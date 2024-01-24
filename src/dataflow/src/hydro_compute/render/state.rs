//! various states using in streaming operator
//!

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};

use common_time::time::Time;

use crate::adapter::error::EvalError;
use crate::expr::ScalarExpr;
use crate::repr::{value2internal_ts, Diff, Row, Timestamp};
use crate::utils::{ts_to_duration, DiffMap};

/// including all the future action to insert/remove rows(because current rows are send forward therefore no need to store)
///
/// Only store row since EvalError should be sent immediately
pub struct TemporalFilterState {
    pub spine: BTreeMap<Timestamp, BTreeMap<Row, Diff>>,
}

impl TemporalFilterState {
    pub fn append_delta_row(&mut self, rows: impl Iterator<Item = (Row, Timestamp, Diff)>) {
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
    pub fn trunc_until(&mut self, time: Timestamp) -> impl Iterator<Item = (Row, Timestamp, Diff)> {
        let mut ret = Vec::new();
        for (t, rows) in self.spine.range_mut(..=time) {
            for (row, diff) in std::mem::take(rows).into_iter() {
                ret.push((row, *t, diff));
            }
        }
        ret.into_iter()
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
