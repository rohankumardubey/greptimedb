//! This module can render a `Plan` into actual dataflow
//!
//! TODO(discord9): use batched input/output

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::rc::{self, Rc};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

use datatypes::arrow::array::ArrayAccessor;
use datatypes::value::Value;
use hydroflow::futures::stream::Stream;
use hydroflow::lattices::cc_traits::{GetMut, MapInsert};
use hydroflow::scheduled::graph::Hydroflow;
use hydroflow::scheduled::graph_ext::GraphExt;
use hydroflow::scheduled::handoff::{self, VecHandoff};
use hydroflow::scheduled::port::{Port, PortCtx, RECV, SEND};
use hydroflow::scheduled::SubgraphId;
use itertools::Itertools;
use tokio::sync::mpsc::error::TryRecvError;
use tokio_stream::wrappers::UnboundedReceiverStream;

use self::state::{ScheduledAction, StateId, TemporalFilterState};
use super::types::{Delta, OkErrRecvPort, OkErrSendPort, RawRecvOkErr, RawSendOkErr};
use crate::expr::error::{EvalError, InternalSnafu};
use crate::expr::{
    AggregateExpr, AggregateFunc, GlobalId, Id, LocalId, MapFilterProject, MfpPlan, SafeMfpPlan,
    ScalarExpr,
};
use crate::hydro_compute::render::state::ComputeState;
use crate::hydro_compute::types::{
    BuildDesc, DataflowDescription, DiffRow, Erroff, Hoff, RowMap, VecDiff,
};
use crate::plan::{AccumulablePlan, KeyValPlan, Plan, ReducePlan};
use crate::repr::{self, Diff, Row};
use crate::utils::DiffMap;

mod state;

/// A Thread Local Manager to manage multiple dataflow
pub struct HydroManager {
    /// map from task name to dataflow
    pub dataflows: BTreeMap<String, Hydroflow>,
    /// map from id to input/output
    pub compute_state: ComputeState,
}

/// Build a dataflow from description and connect it with input/output by fetching it
/// from `compute_state`
/// return the `Hydroflow` being built
/// TODO: add compute state for this
pub fn build_compute_dataflow(
    dataflow: DataflowDescription,
    compute_state: &mut ComputeState,
) -> Hydroflow {
    let mut df = Hydroflow::new();
    let mut edge_man = gen_edge_connect_io(&mut df, &dataflow, compute_state);

    for build_desc in &dataflow.objects_to_build {
        let id = build_desc.id;
        let node_name = format!("{:?}", id);
        let mut ctx = Context::new(
            id,
            &mut df,
            &mut edge_man,
            compute_state.current_time.clone(),
        );
        let bundle = ctx.render_plan(build_desc.plan.plan.clone());
        ctx.connect_output(bundle);
    }
    df
}

fn send2dest(
    df: &mut Hydroflow,
    (mut out_send_ok, mut out_send_err): RawSendOkErr,
) -> OkErrSendPort {
    let (send_ok, recv_ok) = df.make_edge::<_, Hoff>("ok");
    let (send_err, recv_err) = df.make_edge::<_, Erroff>("err");

    df.add_subgraph_sink("sink_stream_ok", recv_ok, move |ctx, recv| {
        let data = recv.take_inner();
        for row in data {
            out_send_ok.send(row);
        }
    });
    df.add_subgraph_sink("sink_stream_err", recv_err, move |ctx, recv| {
        let data = recv.take_inner();
        for row in data {
            out_send_err.send(row);
        }
    });
    (send_ok, send_err)
}

fn recv_stream2source(df: &mut Hydroflow, input_okerr_receiver: RawRecvOkErr) -> OkErrRecvPort {
    let (mut input_ok_recv, mut input_err_recv) = input_okerr_receiver;
    let (send_ok, recv_ok) = df.make_edge::<_, Hoff>("ok");
    let (send_err, recv_err) = df.make_edge::<_, Erroff>("err");

    df.add_subgraph_source("source_stream_ok", send_ok, move |context, send| {
        // collect as many recv data as possible
        // first trying to batch input,
        // then use stream to allow
        // waker to make input a external event
        const BATCH_SIZE: usize = 1024;
        let mut count = 0;

        while let Ok(msg) = input_ok_recv.as_mut().try_recv() {
            send.give(Some(msg));
            count += 1;
            if count >= BATCH_SIZE {
                break;
            }
        }

        // support async recv
        let binding = context.waker();
        let mut waker = std::task::Context::from_waker(&binding);
        while let Poll::Ready(data) =
            Stream::poll_next(std::pin::Pin::new(&mut input_ok_recv), &mut waker)
        {
            send.give(data);
        }
    });

    df.add_subgraph_source("source_stream_err", send_err, move |context, send| {
        // use waker to make input a external event
        let binding = context.waker();
        let mut waker = std::task::Context::from_waker(&binding);
        while let Poll::Ready(data) =
            Stream::poll_next(std::pin::Pin::new(&mut input_err_recv), &mut waker)
        {
            send.give(data);
        }
        /*
        while let Ok(data) = input_err_recv.as_mut().try_recv() {
            send.give(Some(data));
        }
        context.schedule_subgraph(context.current_subgraph(), true);*/
    });

    (recv_ok, recv_err)
}

/// The Context for build a Operator with id of `GlobalId`
pub struct Context<'a> {
    pub id: GlobalId,
    pub df: &'a mut Hydroflow,
    /// multiple ports if this operator is used by dst operator multiple times
    /// key being None means this operator is sink
    send_ports: BTreeMap<Option<GlobalId>, Vec<OkErrSendPort>>,
    /// multiple ports if this operator use source operator multiple times
    recv_ports: BTreeMap<GlobalId, Vec<OkErrRecvPort>>,
    /// for each local scope created from `Let`, map from local id to global id
    /// each `CollectionBundle` value is exactly the same and should be take out when use
    local_scope: Vec<HashMap<LocalId, Vec<CollectionBundle>>>,
    /// Frontier before which updates should not be emitted.
    ///
    /// We *must* apply it to sinks, to ensure correct outputs.
    /// We *should* apply it to sources and imported shared state, because it improves performance.
    /// TODO(discord9): use it as current time in temporal filter to get current correct result
    as_of: Rc<RefCell<repr::Timestamp>>,
}

impl<'a> Context<'a> {
    /// Create a Context for build an operator with given id
    fn new(
        id: GlobalId,
        df: &'a mut Hydroflow,
        edge_man: &mut EdgeManager,
        time: Rc<RefCell<repr::Timestamp>>,
    ) -> Self {
        Self {
            id,
            df,
            send_ports: edge_man.take_all_send_port(id),
            recv_ports: edge_man.take_all_recv_port(id),
            local_scope: Default::default(),
            as_of: time,
        }
    }

    /// TODO: shared state between operators Or
    /// At least a way to manage state for multiple operator
    fn add_state<T: std::any::Any>(
        &mut self,
        init: T,
    ) -> (
        hydroflow::scheduled::state::StateHandle<RefCell<T>>,
        StateId,
    ) {
        // TODO(discord9): register trigger in ComputeState
        let state = self.df.add_state(init);
        todo!()
    }

    /// Link this state to subgraph, so manager know when to schedule this subgraph if state require it
    fn register_id(&mut self, state: StateId, subgraph_id: SubgraphId) {
        todo!()
    }

    /// Send to all `send_ports` the content of `bundle`
    /// TODO: better optimization when only one-in one-out
    fn connect_output(&mut self, bundle: CollectionBundle) {
        let send_ports = std::mem::take(&mut self.send_ports);
        let all_send_ports: (Vec<_>, Vec<_>) = send_ports.into_iter().flat_map(|(k, v)| v).unzip();
        self.df.add_subgraph_n_m(
            format!("op_{:?}_output", self.id),
            vec![bundle.ok],
            all_send_ports.0,
            move |ctx, recv, send| {
                let buf = recv[0].take_inner();
                for send in send {
                    send.give(buf.clone());
                }
            },
        );
        self.df.add_subgraph_n_m(
            format!("op_{:?}_output", self.id),
            vec![bundle.err],
            all_send_ports.1,
            move |ctx, recv, send| {
                let buf = recv[0].take_inner();
                for send in send {
                    send.give(buf.clone());
                }
            },
        );
    }
}

/// representation of a Collection, include both ok and err output
///
/// **Expensive** to clone since it need to build a subgraph to clone it's input to multiple output
///
pub struct CollectionBundle {
    /// straight ahead a vector storing this tick's output
    pub ok: Port<RECV, Hoff>,
    pub err: Port<RECV, Erroff>,
}

#[test]
fn test_apply_mfp() {
    const MOCK_SIZE: repr::Timestamp = 10;
    let input: VecDiff = (0..MOCK_SIZE)
        .map(|i| (Row::new(vec![Value::from(1)]), i, 1))
        .collect_vec();
    let mfp = MapFilterProject::new(1).map([ScalarExpr::Column(0)]);
    let mut df = Hydroflow::new();

    let (oks, okr) = df.make_edge::<_, Hoff>("ok");
    let (errs, errr) = df.make_edge::<_, Erroff>("err");
    let input = CollectionBundle::from_ok_err(okr, errr);

    let res = input.apply_mfp(&mfp, &mut df);
    df.run_available();
}

pub fn new_port_pairs<T>(df: &mut Hydroflow) -> (OkErrSendPort<T>, OkErrRecvPort<T>) {
    let (ok_send, ok_recv) = df.make_edge::<_, VecHandoff<T>>("ok");
    let (err_send, err_recv) = df.make_edge::<_, Erroff>("err");
    ((ok_send, err_send), (ok_recv, err_recv))
}

/// TODO: generalize this to replicate any type of port
fn replicate_recv_port(
    (ok, err): OkErrRecvPort,
    times: usize,
    df: &mut Hydroflow,
) -> Vec<OkErrRecvPort> {
    let (oks_send, oks_recv): (Vec<_>, Vec<_>) = (0..times)
        .map(|_| df.make_edge::<_, Hoff>("replicate_ok"))
        .unzip();
    let (errs_send, errs_recv): (Vec<_>, Vec<_>) = (0..times)
        .map(|_| df.make_edge::<_, Erroff>("replicate_err"))
        .unzip();

    df.add_subgraph_n_m(
        "replicate",
        vec![ok],
        oks_send,
        move |ctx, recv_ports, send_ports| {
            let recv = recv_ports[0].take_inner();
            for send in send_ports {
                send.give(recv.clone());
            }
        },
    );

    df.add_subgraph_n_m(
        "replicate",
        vec![err],
        errs_send,
        move |ctx, recv_ports, send_ports| {
            let recv = recv_ports[0].take_inner();
            for send in send_ports {
                send.give(recv.clone());
            }
        },
    );
    oks_recv.into_iter().zip(errs_recv).collect_vec()
}

impl CollectionBundle {
    /// consume self and produce a vec of the same CollectionBundle
    pub fn replicate(self, times: usize, df: &mut Hydroflow) -> Vec<Self> {
        replicate_recv_port((self.ok, self.err), times, df)
            .into_iter()
            .map(|(ok, err)| Self { ok, err })
            .collect_vec()
    }
    pub fn from_ok_err(ok: Port<RECV, Hoff>, err: Port<RECV, Erroff>) -> Self {
        Self { ok, err }
    }
    pub fn apply_mfp(self, mfp: &MapFilterProject, df: &mut Hydroflow) -> CollectionBundle {
        if mfp.is_identity() {
            return self;
        }
        let mfp = mfp.clone();
        let safe_mfp = SafeMfpPlan { mfp };

        let (ok_send, ok_recv) = df.make_edge::<_, Hoff>("mfp_ok");
        let (err_send, err_recv) = df.make_edge::<_, Erroff>("mfp_err");
        df.add_subgraph_2in_2out(
            "MapFilterProject",
            self.ok,
            self.err,
            ok_send,
            err_send,
            move |ctx, ok, err, ok_send, err_send| {
                let mut res_ok = Vec::new();
                let mut res_err = Vec::new();
                let mut row_buf = Row::new(vec![]);
                let mut datum_vec = Vec::new();
                for row_diff in ok.take_inner().iter() {
                    datum_vec.clear();
                    datum_vec.extend(row_diff.0.inner.clone());
                    let res_row = safe_mfp.evaluate_into(&mut datum_vec, &mut row_buf);
                    match res_row {
                        Ok(Some(row)) => res_ok.push((row, row_diff.1, row_diff.2)),
                        Err(err) => res_err.push((err, row_diff.1, row_diff.2)),
                        // row being filtered away, no output row
                        _ => (),
                    }
                }
                ok_send.give(res_ok);

                // error output is concat with new errors
                err_send.give(err.take_inner());
                err_send.give(res_err);
            },
        );

        CollectionBundle::from_ok_err(ok_recv, err_recv)
    }
}

/// Check how many times this `LocalId` is used in given `plan`
fn count_local_id(plan: &Plan, id: LocalId) -> usize {
    match plan {
        Plan::Get { id: Id::Local(cur) } => {
            if *cur == id {
                1
            } else {
                0
            }
        }
        Plan::Get { id: Id::Global(_) } => 0,
        Plan::Let {
            id: nested,
            value,
            body,
        } => {
            let mut res = count_local_id(value, id);
            // prevent shadowing if local id happens to be the same number
            if id != *nested {
                res += count_local_id(body, id);
            }
            res
        }
        Plan::Mfp { input, .. } | Plan::Reduce { input, .. } => count_local_id(input, id),
    }
}

impl<'a> Context<'a> {
    /// Interpret and execute plan
    ///
    /// return the output of this plan
    pub fn render_plan(&mut self, plan: Plan) -> CollectionBundle {
        match plan {
            Plan::Get { id } => {
                // simply replicate recv data to all send ports
                match id {
                    Id::Local(local) => {
                        // search backward in `local_scope`
                        self.local_scope
                            .iter_mut()
                            .rev()
                            .find_map(|scope| {
                                scope.get_mut(&local).map(|bundles| {
                                    bundles.pop().expect("At least one bundle to use")
                                })
                            })
                            .expect("LocalId not found in local_scope, should be plan phase error")
                    }
                    Id::Global(id) => {
                        // ok to unwrap since compile phase will make sure it's right
                        // TODO: get type info
                        let get = self
                            .recv_ports
                            .get_mut(&id)
                            .expect("Not found in recv_ports, should be plan phrase error")
                            .pop()
                            .expect("At least one recv port to use");
                        CollectionBundle::from_ok_err(get.0, get.1)
                    }
                }
            }
            Plan::Let { id, value, body } => {
                let value = self.render_plan(*value);
                let count = count_local_id(&body, id);
                let locals = value.replicate(count, self.df);
                self.local_scope.push([(id, locals)].into());
                let body = self.render_plan(*body);
                self.local_scope.pop();
                body
            }
            Plan::Mfp { input, mfp } => {
                let input = self.render_plan(*input);
                self.render_mfp(input, mfp)
            }
            Plan::Reduce {
                input,
                key_val_plan,
                reduce_plan,
            } => {
                let input = self.render_plan(*input);
                self.render_reduce(input, key_val_plan, reduce_plan)
            }
        }
    }

    pub fn render_mfp(
        &mut self,
        input: CollectionBundle,
        mfp: MapFilterProject,
    ) -> CollectionBundle {
        let (state, state_id) = self.add_state(TemporalFilterState::default());

        let df = &mut self.df;

        // TODO: check and impl temporal filter instead
        if mfp.is_identity() {
            return input;
        }
        let temp_mfp = MfpPlan::create_from(mfp.clone());
        let as_of = self.as_of.clone();

        let (ok_send, ok_recv) = df.make_edge::<_, Hoff>("mfp_ok");
        let (err_send, err_recv) = df.make_edge::<_, Erroff>("mfp_err");
        let sub_id = df.add_subgraph_2in_2out(
            "MapFilterProject",
            input.ok,
            input.err,
            ok_send,
            err_send,
            move |ctx, ok, err, ok_send, err_send| {
                let state_handle = ctx.state_ref(state);

                let as_of = as_of.clone();
                let now = *as_of.borrow();

                ok_send.give(handoff::Iter(
                    state_handle.borrow_mut().trunc_until(now).into_iter(),
                ));

                let temp_mfp = match &temp_mfp {
                    Ok(temp_mfp) => temp_mfp,
                    Err(e) => {
                        let e = InternalSnafu {
                            reason: e.to_string(),
                        }
                        .build();
                        err_send.give(Some((e, now, 1)));
                        return;
                    }
                };
                let mut res_ok = Vec::new();
                let mut res_err = Vec::new();
                let mut row_buf = Row::new(vec![]);
                let mut datum_vec = Vec::new();
                for row_diff in ok.take_inner().iter() {
                    datum_vec.clear();
                    datum_vec.extend(row_diff.0.inner.clone());
                    // TODO(discord9): use temporal filter
                    let deltas = temp_mfp.evaluate::<EvalError>(&mut datum_vec, now, row_diff.2);
                    // for row with time <= now, send it to output, otherwise save it in state
                    for row in deltas {
                        match row {
                            Ok(r) => {
                                if r.1 <= now {
                                    res_ok.push(r)
                                } else {
                                    state_handle.borrow_mut().append_delta_row(Some(r))
                                }
                            }
                            Err(e) => {
                                debug_assert_eq!(e.1, now);
                                res_err.push(e);
                            }
                        }
                    }
                }
                ok_send.give(res_ok);

                // error output is concat with new errors
                err_send.give(err.take_inner());
                err_send.give(res_err);
            },
        );
        self.register_id(state_id, sub_id);

        CollectionBundle::from_ok_err(ok_recv, err_recv)
    }

    pub fn render_reduce(
        &mut self,
        input: CollectionBundle,
        key_val_plan: KeyValPlan,
        reduce_plan: ReducePlan,
    ) -> CollectionBundle {
        /// first assembly key&val that's ((Row, Row), tick, diff)
        /// Then stream kvs through a reduce operator
        let (kv_send, kv_recv) = new_port_pairs::<((Row, Row), repr::Timestamp, Diff)>(self.df);
        self.df.add_subgraph_2in_2out(
            "reduce_get_kv",
            input.ok,
            input.err,
            kv_send.0,
            kv_send.1,
            move |ctx, ok_recv, err_recv, kv_ok_send, kv_err_send| {
                let mut res_ok = Vec::new();
                let mut res_err = Vec::new();
                let mut row_buf = Row::new(vec![]);
                let mut datum_vec = Vec::new();
                for row_diff in ok_recv.take_inner().iter() {
                    datum_vec.clear();
                    datum_vec.extend(row_diff.0.inner.clone());
                    let res_row = key_val_plan
                        .key_plan
                        .evaluate_into(&mut datum_vec, &mut row_buf);
                    let key = match res_row {
                        Ok(Some(row)) => row,
                        /// empty key is also key
                        Ok(None) => Row::new(vec![]),
                        Err(err) => {
                            res_err.push((err, row_diff.1, row_diff.2));
                            continue;
                        }
                    };
                    datum_vec.clear();
                    datum_vec.extend(row_diff.0.inner.clone());
                    let res_row = key_val_plan
                        .val_plan
                        .evaluate_into(&mut datum_vec, &mut row_buf);
                    let val = match res_row {
                        Ok(Some(row)) => row,
                        Ok(None) => Row::new(vec![]),
                        Err(err) => {
                            res_err.push((err, row_diff.1, row_diff.2));
                            continue;
                        }
                    };
                    res_ok.push(((key, val), row_diff.1, row_diff.2));
                }
                kv_ok_send.give(res_ok);

                // error output is concat with new errors
                kv_err_send.give(err_recv.take_inner());
                kv_err_send.give(res_err);
            },
        );

        self.render_reduce_plan(reduce_plan, kv_recv)
    }

    pub fn render_reduce_plan(
        &mut self,
        reduce_plan: ReducePlan,
        (ok, err): OkErrRecvPort<Delta<(Row, Row)>>,
    ) -> CollectionBundle {
        match reduce_plan {
            ReducePlan::Distinct => self.render_distinct((ok, err)),
            ReducePlan::Accumulable(accum_plan) => self.render_accumulable(accum_plan, (ok, err)),
        }
    }

    pub fn render_accumulable(
        &mut self,
        accum_plan: AccumulablePlan,
        (ok, err): OkErrRecvPort<Delta<(Row, Row)>>,
    ) -> CollectionBundle {
        let (reduce_handle, id) = self.add_state::<RowMap>(Default::default());

        let now = self.as_of.clone();

        let (send, recv) = new_port_pairs::<DiffRow>(self.df);

        self.df.add_subgraph_2in_2out(
            "reduce",
            ok,
            err,
            send.0,
            send.1,
            move |ctx, ok_recv, err_recv, send_ok, send_err| {
                let mut reduce_state = ctx.state_ref(reduce_handle).borrow_mut();
                let now = now.clone();
                let now = *now.borrow();

                let buf = ok_recv.take_inner();
                let buf_len = buf.len();

                // assume this same batch have same time
                let mut key2val = BTreeMap::<Row, Vec<(Row, Diff)>>::new();

                // dismantle row into (key -> (columns, tick, diff))
                for ((key, val), tick, diff) in buf {
                    if let Some(kvs) = key2val.get_mut(&key) {
                        kvs.push((val, diff));
                    } else {
                        let mut vals = Vec::with_capacity(buf_len);
                        vals.push((val, diff));
                        key2val.insert(key, vals);
                    }
                }

                for (cur_key_row, mut cur_vals) in key2val {
                    let iter = cur_vals.iter();
                    let mut new_val = BTreeMap::<usize, Value>::new();
                    // first compute aggr result in current batch
                    for (accum_idx, input_idx, aggr) in &accum_plan.simple_aggrs {
                        let accum = reduce_state
                            .get(&cur_key_row)
                            .and_then(|r| r.get(*accum_idx).cloned());
                        let col_iter = iter
                            .clone()
                            .map(|(r, d)| (r.get(*input_idx).unwrap().clone(), *d));
                        let ans = aggr.func.eval_diff_accum(accum, col_iter);
                        match ans {
                            Ok(value) => {
                                new_val.insert(*accum_idx, value);
                            }
                            Err(err) => {
                                send_err.give(Some((err, now, 1)));
                                // early return because can't give full result due to errors being produced
                                return;
                            }
                        }
                    }
                    // build new_val and send it to reduce_state
                    let new_val_row = Row::new(new_val.into_values().collect_vec());
                    reduce_state.insert(cur_key_row, new_val_row);
                }
                let out = reduce_state
                    .gen_diff(now)
                    .into_iter()
                    .map(|((mut k, v), t, d)| {
                        k.extend(v.into_iter());
                        (k, t, d)
                    });
                send_ok.give(handoff::Iter(out));
                // always resend existing errors(if any)
                send_err.give(err_recv.take_inner());
            },
        );

        CollectionBundle::from_ok_err(recv.0, recv.1)
    }

    pub fn render_distinct(
        &mut self,
        (ok, err): OkErrRecvPort<Delta<(Row, Row)>>,
    ) -> CollectionBundle {
        let reduce_handle = self.df.add_state::<Rc<RefCell<RowMap>>>(Default::default());

        let now = self.as_of.clone();

        let (send, recv) = new_port_pairs::<DiffRow>(self.df);

        // TODO: use in_out instead
        self.df.add_subgraph_2in_2out(
            "reduce",
            ok,
            err,
            send.0,
            send.1,
            move |ctx, ok_recv, err_recv, send_ok, send_err| {
                let mut reduce_state = ctx.state_ref(reduce_handle).borrow_mut();

                let now = now.clone();
                let now = *now.borrow();

                let buf = ok_recv.take_inner();
                let key_only = buf.into_iter().map(|row| (row.0 .0, Row::new(vec![])));
                key_only.for_each(|(k, v)| {
                    reduce_state.insert(k, v);
                });

                // distinct hence only retain keys
                let out = reduce_state.gen_diff(now);
                let iter = out.into_iter().map(|r| (r.0 .0, r.1, r.2));

                send_ok.give(handoff::Iter(iter));
                // no errors is produced, resend err
                send_err.give(err_recv.take_inner());
            },
        );

        CollectionBundle::from_ok_err(recv.0, recv.1)
    }
}

/// TODO: now it create edges Ahead of Time, might want to do that only on demand, only notice
/// how many times it will be use and already used
#[derive(Default)]
struct EdgeManager {
    pub src_to_edge: BTreeMap<GlobalId, BTreeSet<(GlobalId, GlobalId)>>,
    pub dst_to_edge: BTreeMap<GlobalId, BTreeSet<(GlobalId, GlobalId)>>,
    /// for each edge, store the actual send ports(may have more than one due to muptiple use)
    pub edge_send_port: BTreeMap<(GlobalId, GlobalId), Vec<OkErrSendPort>>,
    /// for each edge, store the actual recv ports(may have more than one due to muptiple use)
    pub edge_recv_port: BTreeMap<(GlobalId, GlobalId), Vec<OkErrRecvPort>>,
    pub sink_port: BTreeMap<GlobalId, OkErrSendPort>,
}

impl EdgeManager {
    /// init `src_to_edge` and `dst_to_edge` as keys with corresponding `edges` as values
    pub fn new(edges: &[(GlobalId, GlobalId)]) -> Self {
        let mut zelf = Self::default();
        let all_ids = edges
            .iter()
            .flat_map(|(src, dst)| vec![*src, *dst])
            .collect_vec();
        zelf.src_to_edge = all_ids
            .iter()
            .map(|id| (*id, BTreeSet::new()))
            .collect::<BTreeMap<_, _>>();
        zelf.dst_to_edge = all_ids
            .iter()
            .map(|id| (*id, BTreeSet::new()))
            .collect::<BTreeMap<_, _>>();
        for &edge in edges {
            if let Some(edge_list) = zelf.src_to_edge.get_mut(&edge.0) {
                edge_list.insert(edge);
            } else {
                unreachable!("src_to_edge should have all id")
            }

            if let Some(edge_list) = zelf.dst_to_edge.get_mut(&edge.1) {
                edge_list.insert(edge);
            } else {
                unreachable!("dst_to_edge should have all id")
            }
        }
        zelf
    }
    /// Take all send port for this id, that is everything this id send to
    /// return is (id of operator that recv from self, actual send port being used for sending)
    pub fn take_all_send_port(
        &mut self,
        id: GlobalId,
    ) -> BTreeMap<Option<GlobalId>, Vec<OkErrSendPort>> {
        let mut ret: BTreeMap<Option<GlobalId>, Vec<OkErrSendPort>> = self
            .src_to_edge
            .remove(&id)
            .unwrap()
            .into_iter()
            .map(|(src, dst)| {
                let send_port = self.edge_send_port.remove(&(src, dst)).unwrap();

                (Some(dst), send_port)
            })
            .collect();
        if let Some(output_port) = self.sink_port.remove(&id) {
            ret.insert(None, vec![output_port]);
        }
        ret
    }

    /// Take all recv port for this id,
    /// that is everything this operator recv from
    /// return is (id of operator that send to self, actual recv port being used for recving them)
    pub fn take_all_recv_port(&mut self, id: GlobalId) -> BTreeMap<GlobalId, Vec<OkErrRecvPort>> {
        self.dst_to_edge
            .remove(&id)
            .unwrap()
            .into_iter()
            .map(|(src, dst)| {
                let recv_port = self.edge_recv_port.remove(&(src, dst)).unwrap();

                (src, recv_port)
            })
            .collect()
    }
}

/// found all define-use relation in `dataflow` using `build_descs` to build edges
/// FIXME(discord9): check if any replicate edge will cause problem
fn gen_edge_connect_io(
    df: &mut Hydroflow,
    dataflow: &DataflowDescription,
    compute_state: &mut ComputeState,
) -> EdgeManager {
    // input/output being simple id so it's like add new input "virtual" node into this graph
    // but output node is already there for use

    let build_descs = &dataflow.objects_to_build;
    let dest_src_pairs = build_descs
        .iter()
        .map(|desc| (desc.id, find_use_in_plan(&desc.plan.plan)))
        .collect_vec();

    // This edges might have duplicated edges, representing multiple use of same operator
    let edges = dest_src_pairs
        .iter()
        .flat_map(|(dest_id, src_ids)| {
            src_ids
                .iter()
                .map(|src_id| (*src_id, *dest_id))
                .collect_vec()
        })
        .collect_vec();
    let mut man = EdgeManager::new(&edges);

    // make edges and put them into edge_send_port and edge_recv_port
    for edge in edges {
        let (ok_send, ok_recv) = df.make_edge::<_, Hoff>(format!("Ok({:?}->{:?})", edge.0, edge.1));
        let (err_send, err_recv) =
            df.make_edge::<_, Erroff>(format!("Err({:?}->{:?})", edge.0, edge.1));

        if let Some(send_list) = man.edge_send_port.get_mut(&edge) {
            send_list.push((ok_send, err_send));
        } else {
            man.edge_send_port.insert(edge, vec![(ok_send, err_send)]);
        }

        if let Some(recv_list) = man.edge_recv_port.get_mut(&edge) {
            recv_list.push((ok_recv, err_recv));
        } else {
            man.edge_recv_port.insert(edge, vec![(ok_recv, err_recv)]);
        }
    }
    // get input/output from compute_state and replicate it if necessary
    // inputs are virtual nodes need to update edge_recv_port
    // output is real node so only need to update send_ports for output nodes

    // first find out how many times a input node is used
    // then clone input that many times and put recv into edge_recv_port
    for (input_id, edges_of_src) in dataflow
        .inputs
        .iter()
        .map(|id| (id, man.src_to_edge.get(id).unwrap()))
    {
        let input_port = compute_state
            .input_recv
            .get_mut(input_id)
            .unwrap()
            .pop()
            .unwrap();
        let input_port = recv_stream2source(df, input_port);
        let mut input_recv_ports = replicate_recv_port(input_port, edges_of_src.len(), df);
        for edge in edges_of_src {
            let cur_recv = input_recv_ports.pop().unwrap();
            if let Some(recv_list) = man.edge_recv_port.get_mut(edge) {
                recv_list.push(cur_recv);
            } else {
                man.edge_recv_port.insert(*edge, vec![cur_recv]);
            }
        }
    }
    for output_id in &dataflow.outputs {
        let output_port = compute_state
            .output_send
            .get_mut(output_id)
            .unwrap()
            .pop()
            .unwrap();
        let output_port = send2dest(df, output_port);
        man.sink_port.insert(*output_id, output_port);
    }

    man
}

/// extract `GlobalId` being used in a plan, the same id may appear multiple times
fn find_use_in_plan(plan: &Plan) -> Vec<GlobalId> {
    match &plan {
        Plan::Get { id } => match id {
            Id::Global(id) => vec![*id],
            Id::Local(_) => unreachable!("LocalId is only used in Let's body"),
        },
        Plan::Let { id: _, value, body } => {
            let mut part = find_use_in_plan(value);
            part.extend(find_use_in_plan(body));
            part
        }
        Plan::Mfp { input, mfp: _ } => find_use_in_plan(input),
        Plan::Reduce {
            input,
            key_val_plan: _,
            reduce_plan: _,
        } => find_use_in_plan(input),
    }
}

#[test]
fn test_clone_speed() {
    let mut df = Hydroflow::new();
    let (send_ok, recv_ok) = df.make_edge::<_, Hoff>("ok");
    let (send_err, recv_err) = df.make_edge::<_, Erroff>("err");
    replicate_recv_port((recv_ok, recv_err), 3, &mut df);
    df.add_subgraph_source("source", send_ok, |ctx, send| {
        for i in 0..1000 {
            send.give(vec![(Row::new(vec![Value::from(i)]), 0, 1)]);
        }
    });
    for times in 0..10000 {
        df.run_available();
    }
}

#[test]
fn build_df() {
    use datatypes::prelude::ConcreteDataType;

    use crate::plan::TypedPlan;
    use crate::repr::{ColumnType, RelationType};

    let (input_ok, ok_recv) = hydroflow::util::unbounded_channel::<DiffRow>();
    let (input_err, err_recv) = hydroflow::util::unbounded_channel::<Delta<EvalError>>();

    let (send_ok, mut output_ok) = hydroflow::util::unbounded_channel::<DiffRow>();
    let (send_err, output_err) = hydroflow::util::unbounded_channel::<Delta<EvalError>>();

    let mut compute_state = ComputeState {
        input_recv: BTreeMap::from([(GlobalId::User(0), vec![(ok_recv, err_recv)])]),
        output_send: BTreeMap::from([(GlobalId::User(1), vec![(send_ok, send_err)])]),
        current_time: Rc::new(RefCell::new(0)),
    };
    let sum = AggregateExpr {
        func: AggregateFunc::SumUInt16,
        expr: ScalarExpr::Column(0),
        distinct: false,
    };

    let plan = Plan::Reduce {
        input: Box::new(Plan::Get {
            id: Id::Global(GlobalId::User(0)),
        }),
        key_val_plan: KeyValPlan {
            key_plan: SafeMfpPlan {
                mfp: MapFilterProject::new(2).project([0]),
            },
            val_plan: SafeMfpPlan {
                mfp: MapFilterProject::new(2).project([1]),
            },
        },
        reduce_plan: ReducePlan::Accumulable(AccumulablePlan {
            full_aggrs: vec![sum.clone()],
            simple_aggrs: vec![(0, 0, sum)],
            distinct_aggrs: vec![],
        }),
    };
    let typ = RelationType::new(vec![ColumnType::new(
        ConcreteDataType::uint64_datatype(),
        true,
    )]);

    let dd = DataflowDescription {
        inputs: vec![GlobalId::User(0)],
        outputs: vec![GlobalId::User(1)],
        objects_to_build: vec![BuildDesc {
            id: GlobalId::User(1),
            plan: TypedPlan { plan, typ },
        }],
        name: "test_sum".to_string(),
    };
    let mut df = build_compute_dataflow(dd, &mut compute_state);

    let input_sheet = vec![
        (Row::new(vec![Value::UInt64(0), Value::UInt16(1)]), 0, 1),
        (Row::new(vec![Value::UInt64(0), Value::UInt16(2)]), 0, 1),
        (Row::new(vec![Value::UInt64(0), Value::UInt16(3)]), 0, 1),
    ];
    for row in input_sheet {
        input_ok.send(row);
    }
    df.run_available();

    let input_sheet = vec![
        (Row::new(vec![Value::UInt64(0), Value::UInt16(1)]), 1, -1),
        (Row::new(vec![Value::UInt64(1), Value::UInt16(4)]), 1, 1),
        (Row::new(vec![Value::UInt64(1), Value::UInt16(5)]), 1, 1),
        (Row::new(vec![Value::UInt64(1), Value::UInt16(6)]), 1, 1),
    ];
    for row in input_sheet {
        input_ok.send(row);
    }
    df.run_available();

    let mut result = vec![];

    while let Ok(data) = output_ok.as_mut().try_recv() {
        result.push(data)
    }
    assert_eq!(
        result,
        [
            (
                Row {
                    inner: vec![Value::UInt64(0), Value::UInt64(6)],
                },
                0,
                1,
            ),
            (
                Row {
                    inner: vec![Value::UInt64(0), Value::UInt64(6)]
                },
                1,
                -1
            ),
            (
                Row {
                    inner: vec![Value::UInt64(0), Value::UInt64(5)]
                },
                1,
                1
            ),
            (
                Row {
                    inner: vec![Value::UInt64(1), Value::UInt64(15)],
                },
                1,
                1,
            ),
        ]
    );
}
