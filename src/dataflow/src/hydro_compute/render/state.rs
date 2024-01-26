//! various states using in streaming operator
//!

use std::cell::RefCell;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use common_time::time::Time;
use hydroflow::scheduled::SubgraphId;

use crate::expr::error::EvalError;
use crate::expr::{GlobalId, ScalarExpr};
use crate::hydro_compute::types::{Delta, DiffRow, RawRecvOkErr, RawSendOkErr};
use crate::repr::{self, value2internal_ts, Diff, Row, Timestamp};
use crate::utils::{ts_to_duration, DiffMap};

pub struct StateId(usize);

/// Worker-local state that is maintained across dataflows.
/// input/output of a dataflow
/// TODO: use broadcast channel recv for input instead
#[derive(Default)]
pub struct ComputeState {
    /// vec in case of muiltple dataflow needed to be construct at once
    pub input_recv: BTreeMap<GlobalId, Vec<RawRecvOkErr>>,
    /// vec in case of muiltple dataflow needed to be construct at once
    pub output_send: BTreeMap<GlobalId, Vec<RawSendOkErr>>,
    /// current time, updated before run tick to progress dataflow
    pub current_time: Rc<RefCell<repr::Timestamp>>,
    pub state_to_subgraph: BTreeMap<StateId, SubgraphId>,
    pub scheduled_actions: BTreeMap<repr::Timestamp, BTreeSet<SubgraphId>>,
}

impl ComputeState {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn add_input(&mut self, id: GlobalId) -> RawSendOkErr {
        let (input_ok, ok_recv) = hydroflow::util::unbounded_channel::<DiffRow>();
        let (input_err, err_recv) = hydroflow::util::unbounded_channel::<Delta<EvalError>>();

        let recv = (ok_recv, err_recv);
        if let Some(recv_list) = self.input_recv.get_mut(&id) {
            recv_list.push(recv);
        } else if let Some(v) = self.input_recv.insert(id, vec![recv]) {
            panic!("Can't add input more than once for each id {id:?}")
        }

        (input_ok, input_err)
    }

    pub fn add_output(&mut self, id: GlobalId) -> RawRecvOkErr {
        let (send_ok, mut output_ok) = hydroflow::util::unbounded_channel::<DiffRow>();
        let (send_err, output_err) = hydroflow::util::unbounded_channel::<Delta<EvalError>>();

        let send = (send_ok, send_err);
        if let Some(send_list) = self.output_send.get_mut(&id) {
            send_list.push(send);
        } else if let Some(v) = self.output_send.insert(id, vec![send]) {
            panic!("Can't add output more than once for each id {id:?}")
        }
        (output_ok, output_err)
    }
}

/// State need to be schedule after certain time
pub trait ScheduledAction {
    /// Schedule next run at given time
    fn schd_at(&self, now: repr::Timestamp) -> Option<repr::Timestamp>;
}

/// including all the future action to insert/remove rows(because current rows are send forward therefore no need to store)
///
/// Only store row since EvalError should be sent immediately
#[derive(Debug, Default)]
pub struct TemporalFilterState {
    pub spine: BTreeMap<Timestamp, BTreeMap<Row, Diff>>,
}

impl ScheduledAction for TemporalFilterState {
    fn schd_at(&self, now: repr::Timestamp) -> Option<repr::Timestamp> {
        self.spine.iter().next().map(|e| *e.0)
    }
}

impl TemporalFilterState {
    pub fn append_delta_row(&mut self, rows: impl IntoIterator<Item = (Row, Timestamp, Diff)>) {
        for (row, time, diff) in rows {
            let this_time = self.spine.entry(time).or_default();
            let mut row_entry = this_time.entry(row);
            // remove row from spine
            if let Entry::Occupied(mut o) = row_entry {
                *o.get_mut() += diff;
                if *o.get() == 0 {
                    o.remove_entry();
                }
            } else {
                row_entry.or_insert(diff);
            }
        }
    }

    /// trunc all the rows before(including) given time, and send them back
    pub fn trunc_until(&mut self, time: Timestamp) -> Vec<(Row, Timestamp, Diff)> {
        let mut ret = Vec::new();
        for (t, rows) in self.spine.range_mut(..=time) {
            for (row, diff) in std::mem::take(rows).into_iter() {
                ret.push((row, *t, diff));
            }
        }
        ret
    }
}

/// A KV reduce state with timestamp and expire time
///
/// Any keys that are not updated for a long time will be removed from the state
/// and not sending any delta to downstream, since they are simple not used anymore
#[derive(Debug)]
pub struct ReduceState {
    pub inner: DiffMap<Row, Row>,
    pub time2key: BTreeMap<Timestamp, BTreeSet<Row>>,
    pub expire_period: Timestamp,
    /// using this to get timestamp from key row
    pub ts_from_row: ScalarExpr,
}

impl ScheduledAction for ReduceState {
    fn schd_at(&self, now: repr::Timestamp) -> Option<repr::Timestamp> {
        self.time2key
            .iter()
            .next()
            .map(|kv| *kv.0 + self.expire_period)
    }
}

impl ReduceState {
    pub fn get_ts_from_row(&self, row: &Row) -> Result<Timestamp, EvalError> {
        let ts = value2internal_ts(self.ts_from_row.eval(&row.inner)?)?;
        Ok(ts)
    }
    pub fn get_expire_time(&self, current: Timestamp) -> Timestamp {
        current - self.expire_period
    }
    pub fn trunc_expired(&mut self, cur_time: Timestamp) {
        let expire_time = self.get_expire_time(cur_time);
        let mut after = self.time2key.split_off(&expire_time);
        // swap
        std::mem::swap(&mut self.time2key, &mut after);
        let before = after;
        for (_, keys) in before.into_iter() {
            for key in keys.into_iter() {
                // should silently remove from inner
                // w/out producing new delta row
                self.inner.inner.remove(&key);
            }
        }
    }

    pub fn get(&self, k: &Row) -> Option<&Row> {
        self.inner.get(k)
    }

    /// if key row is expired then return expire error
    pub fn insert(&mut self, current: Timestamp, k: Row, v: Row) -> Result<Option<Row>, EvalError> {
        let ts = self.get_ts_from_row(&k)?;
        let expire_at = self.get_expire_time(current);
        if ts < expire_at {
            return Err(EvalError::LateDataDiscarded {
                duration: ts_to_duration(expire_at - ts),
            });
        }

        self.time2key.entry(ts).or_default().insert(k.clone());
        /// this insert should produce delta row if not expired
        let ret = self.inner.insert(k, v);
        Ok(ret)
    }

    pub fn remove(&mut self, current: Timestamp, k: &Row) -> Result<Option<Row>, EvalError> {
        let ts = self.get_ts_from_row(k)?;
        let expire_at = self.get_expire_time(current);
        if ts < expire_at {
            return Err(EvalError::LateDataDiscarded {
                duration: ts_to_duration(expire_at - ts),
            });
        }
        self.time2key.entry(ts).or_default().remove(k);
        Ok(self.inner.remove(k))
    }
}
