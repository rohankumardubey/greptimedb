use std::borrow::Borrow;
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::expr::error::EvalError;
use crate::repr::{self, Diff, Row};

pub fn ts_to_duration(ts: repr::Timestamp) -> std::time::Duration {
    std::time::Duration::from_millis(ts)
}

/// A BTreeMap which track delta between `gen_diff`
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiffMap<K: Ord, V> {
    pub inner: BTreeMap<K, V>,
    /// key -> remove/add/update
    ///
    /// remove: (old_v, None)
    ///
    /// add: (None, new_v)
    ///
    /// update: (old_v, new_v)
    pub delta: BTreeMap<K, (Option<V>, Option<V>)>,
}

impl<K: Ord, V> DiffMap<K, V> {
    pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        self.inner.get_mut(key)
    }

    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        self.inner.get(key)
    }
    pub fn insert(&mut self, key: K, value: V) -> Option<V>
    where
        K: Ord + Clone,
        V: Clone,
    {
        let old_v = self.inner.insert(key.clone(), value.clone());
        if let Some(diff) = self.delta.get_mut(&key) {
            diff.1 = Some(value);
        } else if let Some(old_v) = old_v.clone() {
            self.delta.insert(key.clone(), (Some(old_v), Some(value)));
        } else {
            self.delta.insert(key.clone(), (None, Some(value)));
        }
        old_v
    }

    pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q> + Ord + Clone,
        Q: Ord + ?Sized + ToOwned<Owned = K>,
        V: Clone,
    {
        let old_v = self.inner.remove(key);
        if let Some(diff) = self.delta.get_mut(key) {
            diff.1 = None;
        } else if let Some(old_v) = old_v.clone() {
            self.delta.insert(key.to_owned(), (Some(old_v), None));
        }
        old_v
    }

    /// generate diff since last call of this function
    pub fn gen_diff(&mut self, tick: repr::Timestamp) -> Vec<((K, V), repr::Timestamp, Diff)>
    where
        K: Clone,
    {
        let mut result = Vec::with_capacity(self.delta.len() * 2);
        let delta = std::mem::take(&mut self.delta);
        for (k, (old_v, new_v)) in delta.into_iter() {
            if let Some(old_v) = old_v {
                result.push(((k.clone(), old_v), tick, -1));
            }
            if let Some(new_v) = new_v {
                result.push(((k.clone(), new_v), tick, 1));
            }
        }
        result
    }
}
