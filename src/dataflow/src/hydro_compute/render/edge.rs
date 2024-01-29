use std::collections::{BTreeMap, BTreeSet};
use std::task::Poll;

use hydroflow::scheduled::graph::Hydroflow;
use hydroflow::scheduled::graph_ext::GraphExt;
use itertools::Itertools;
use tokio_stream::Stream;

use crate::expr::{GlobalId, Id};
use crate::hydro_compute::render::collection::replicate_recv_port;
use crate::hydro_compute::render::state::ComputeState;
use crate::hydro_compute::types::{
    DataflowDescription, Erroff, Hoff, OkErrRecvPort, OkErrSendPort, RawRecvOkErr, RawSendOkErr,
};
use crate::plan::Plan;

/// TODO: now it create edges Ahead of Time, might want to do that only on demand, only notice
/// how many times it will be use and already used
#[derive(Default)]
pub(crate) struct EdgeManager {
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
pub(crate) fn gen_edge_connect_io(
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
