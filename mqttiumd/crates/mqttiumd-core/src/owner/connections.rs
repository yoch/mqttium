//! The online connections that receive outgoing deliveries, indexed by
//! generation and by the work read in flight on each.
use crate::contract::capacity::{AllocationFailed, BoundedVec};
use crate::contract::flat_map::{FlatMap, ScopedFlatMap};
use crate::contract::ids::{ConnectionGeneration, DeliveryId, EffectId, SessionId};
use crate::contract::step::CoreFault;
use core::num::NonZeroU32;

/// A read of outgoing work in flight on a connection.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) struct Read {
    pub(super) effect: EffectId,
    pub(super) session: SessionId,
    pub(super) invalidated: bool,
}

/// What the owner knows of an online connection that receives outgoing
/// deliveries.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) struct Connection {
    pub(super) generation: ConnectionGeneration,
    pub(super) receive_maximum: u16,
    pub(super) maximum_packet_size: u32,
    pub(super) after: Option<DeliveryId>,
    pub(super) loading: Option<Read>,
    pub(super) rescan_needed: bool,
    pub(super) caught_up: bool,
    pub(super) dirty_during_load: bool,
    pub(super) last_qos0_ingress: u64,
}

/// Each row is one connection; the indices are kept here and nowhere else.
#[derive(Debug)]
pub(super) struct Connections {
    rows: BoundedVec<Connection>,
    by_generation: FlatMap<ConnectionGeneration, usize>,
    by_loading_effect: ScopedFlatMap<EffectId, usize>,
}

impl Connections {
    pub(super) fn new(bound: NonZeroU32) -> Result<Self, AllocationFailed> {
        Ok(Self {
            rows: super::table(bound)?,
            by_generation: FlatMap::new(),
            by_loading_effect: ScopedFlatMap::new(),
        })
    }

    pub(super) fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
    /// Whether a new connection would find no row.
    pub(super) fn is_full(&self) -> bool {
        self.rows.len() == self.rows.capacity()
    }
    /// The work a step that visits every connection may cost.
    pub(super) fn visit_work(&self) -> u64 {
        super::capacity_of(self.rows.capacity()).min(16)
    }
    /// The row of a connection.
    pub(super) fn index_of(&self, generation: ConnectionGeneration) -> Option<usize> {
        self.by_generation.get(&generation).copied()
    }
    /// A copy of a connection.
    pub(super) fn of(&self, generation: ConnectionGeneration) -> Option<Connection> {
        self.index_of(generation)
            .map(|index| self.rows.as_slice()[index])
    }
    /// A copy of a row.
    pub(super) fn at(&self, index: usize) -> Option<Connection> {
        self.rows.as_slice().get(index).copied()
    }
    /// A copy of a row that exists.
    pub(super) fn row(&self, index: usize) -> Connection {
        self.rows.as_slice()[index]
    }
    /// The row whose read in flight is `effect` for `session`.
    pub(super) fn reading(&self, effect: EffectId, session: SessionId) -> Option<usize> {
        self.by_loading_effect
            .get(&effect)
            .copied()
            .filter(|&index| {
                self.rows.as_slice()[index]
                    .loading
                    .is_some_and(|r| r.effect == effect && r.session == session)
            })
    }

    pub(super) fn push(&mut self, value: Connection) -> Result<(), CoreFault> {
        let index = self.rows.len();
        self.rows
            .try_push(value)
            .map_err(|_| CoreFault::StalePlan)?;
        self.index(index);
        Ok(())
    }
    /// The ingress order of the latest QoS 0 publication admitted from the
    /// connection at `index`. A row that does not exist is a stale plan.
    pub(super) fn record_qos0_ingress(
        &mut self,
        index: usize,
        order: u64,
    ) -> Result<(), CoreFault> {
        self.rows
            .as_mut_slice()
            .get_mut(index)
            .ok_or(CoreFault::StalePlan)?
            .last_qos0_ingress = order;
        Ok(())
    }
    /// Changes a row and the indices its keys are in. A row that does not
    /// exist is a stale plan.
    pub(super) fn update(
        &mut self,
        index: usize,
        change: impl FnOnce(&mut Connection),
    ) -> Result<(), CoreFault> {
        if index >= self.rows.len() {
            return Err(CoreFault::StalePlan);
        }
        self.unindex(index);
        change(&mut self.rows.as_mut_slice()[index]);
        self.index(index);
        Ok(())
    }
    /// Forgets a connection; the last row takes its place.
    pub(super) fn remove(&mut self, generation: ConnectionGeneration) {
        let Some(index) = self.index_of(generation) else {
            return;
        };
        let last = self.rows.len() - 1;
        self.unindex(index);
        if index != last {
            self.unindex(last);
            self.rows.as_mut_slice().swap(index, last);
        }
        let _ = self.rows.pop();
        if index != last {
            self.index(index);
        }
    }
    /// A delivery of `session` may have changed under a scan: the scan of
    /// its connection, if any, is stale and starts again from the start.
    pub(super) fn invalidate_reads(&mut self, session: SessionId) {
        for c in self.rows.as_mut_slice() {
            if let Some(read) = &mut c.loading
                && read.session == session
            {
                read.invalidated = true;
                c.after = None;
            }
        }
    }
    /// Work may wait for the connection at `index`: it is no longer caught
    /// up, a scan in progress is dirty, and `rescan` asks a new scan.
    pub(super) fn mark_dirty(&mut self, index: usize, rescan: bool) {
        if let Some(c) = self.rows.as_mut_slice().get_mut(index) {
            Self::dirty(c, rescan);
        }
    }
    /// The same, for every connection.
    pub(super) fn mark_all_dirty(&mut self, rescan: bool) {
        for c in self.rows.as_mut_slice() {
            Self::dirty(c, rescan);
        }
    }
    fn dirty(c: &mut Connection, rescan: bool) {
        c.caught_up = false;
        if c.loading.is_some() {
            c.dirty_during_load = true;
        }
        if rescan {
            c.rescan_needed = true;
        }
    }

    fn index(&mut self, index: usize) {
        let c = self.rows.as_slice()[index];
        self.by_generation.insert(c.generation, index);
        if let Some(Read { effect, .. }) = c.loading {
            self.by_loading_effect.insert(effect, index);
        }
    }
    fn unindex(&mut self, index: usize) {
        let c = self.rows.as_slice()[index];
        self.by_generation.remove(&c.generation);
        if let Some(Read { effect, .. }) = c.loading {
            self.by_loading_effect.remove(&effect);
        }
    }
    #[allow(dead_code)]
    pub(super) fn rebuild_indices(&mut self) {
        self.by_generation.clear();
        self.by_loading_effect.clear();
        for index in 0..self.rows.len() {
            self.index(index);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::string::String;
    use alloc::vec::Vec;
    use core::fmt::Write;

    impl Connections {
        /// The rows, in order.
        pub(in crate::owner) fn to_vec(&self) -> Vec<Connection> {
            self.rows.as_slice().to_vec()
        }
        /// A row for a test to rewrite; its keys must not change.
        pub(in crate::owner) fn row_mut(&mut self, index: usize) -> &mut Connection {
            &mut self.rows.as_mut_slice()[index]
        }
        /// The indices kept step by step are those a rebuild from the rows
        /// gives.
        pub(in crate::owner) fn assert_indices_kept(&mut self) {
            fn all<T: core::fmt::Debug>(entries: impl Iterator<Item = T>) -> String {
                let mut entries: Vec<String> = entries
                    .map(|e| {
                        let mut text = String::new();
                        write!(text, "{e:?}").unwrap();
                        text
                    })
                    .collect();
                entries.sort();
                entries.join(";")
            }
            let snapshot =
                |t: &Self| (all(t.by_generation.iter()), all(t.by_loading_effect.iter()));
            let kept = snapshot(self);
            self.rebuild_indices();
            assert_eq!(
                kept,
                snapshot(self),
                "a connection index drifted from its rows"
            );
        }
    }
}
