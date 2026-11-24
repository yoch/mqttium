//! Online connections receiving outgoing deliveries, indexed by generation,
//! active work-page read, and sparse pagination watermarks.
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
    pub(super) after: Option<DeliveryId>,
    pub(super) invalidated: bool,
}

/// Per-connection outgoing delivery settings and catch-up flags.
/// Pagination cursors (`after`, `known_through`, `loading`) live in sparse
/// side maps on [`Connections`] so idle connections remain compact.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) struct Connection {
    pub(super) generation: ConnectionGeneration,
    pub(super) last_qos0_ingress: u64,
    pub(super) maximum_packet_size: u32,
    pub(super) receive_maximum: u16,
    pub(super) rescan_needed: bool,
    pub(super) caught_up: bool,
    pub(super) dirty_during_load: bool,
}

/// Encapsulated table of online outgoing connections and all derived indices.
#[derive(Debug)]
pub(super) struct Connections {
    rows: BoundedVec<Connection>,
    by_generation: FlatMap<ConnectionGeneration, usize>,
    by_loading_effect: ScopedFlatMap<EffectId, usize>,
    after_by_gen: FlatMap<ConnectionGeneration, DeliveryId>,
    known_through_by_gen: FlatMap<ConnectionGeneration, DeliveryId>,
    loading_by_gen: FlatMap<ConnectionGeneration, Read>,
}

impl Connections {
    pub(super) fn new(bound: NonZeroU32) -> Result<Self, AllocationFailed> {
        Ok(Self {
            rows: super::table(bound)?,
            by_generation: FlatMap::new(),
            by_loading_effect: ScopedFlatMap::new(),
            after_by_gen: FlatMap::new(),
            known_through_by_gen: FlatMap::new(),
            loading_by_gen: FlatMap::new(),
        })
    }

    pub(super) fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub(super) fn len(&self) -> usize {
        self.rows.len()
    }

    pub(super) fn capacity(&self) -> usize {
        self.rows.capacity()
    }

    pub(super) fn as_slice(&self) -> &[Connection] {
        self.rows.as_slice()
    }

    pub(super) fn index_of(&self, generation: ConnectionGeneration) -> Option<usize> {
        self.by_generation.get(&generation).copied()
    }

    pub(super) fn after_of(&self, generation: ConnectionGeneration) -> Option<DeliveryId> {
        if self.after_by_gen.is_empty() {
            None
        } else {
            self.after_by_gen.get(&generation).copied()
        }
    }

    pub(super) fn has_after(&self, generation: ConnectionGeneration) -> bool {
        !self.after_by_gen.is_empty() && self.after_by_gen.contains_key(&generation)
    }

    pub(super) fn known_through_of(&self, generation: ConnectionGeneration) -> Option<DeliveryId> {
        if self.known_through_by_gen.is_empty() {
            None
        } else {
            self.known_through_by_gen.get(&generation).copied()
        }
    }

    pub(super) fn loading_of(&self, generation: ConnectionGeneration) -> Option<Read> {
        if self.loading_by_gen.is_empty() {
            None
        } else {
            self.loading_by_gen.get(&generation).copied()
        }
    }

    pub(super) fn is_loading(&self, generation: ConnectionGeneration) -> bool {
        !self.loading_by_gen.is_empty() && self.loading_by_gen.contains_key(&generation)
    }

    pub(super) fn reading(&self, effect: EffectId, session: SessionId) -> Option<usize> {
        self.by_loading_effect.get(&effect).copied().filter(|&idx| {
            let conn_gen = self.rows.as_slice()[idx].generation;
            self.loading_of(conn_gen)
                .is_some_and(|r| r.effect == effect && r.session == session)
        })
    }

    pub(super) fn set_loading(
        &mut self,
        generation: ConnectionGeneration,
        index: usize,
        read: Option<Read>,
    ) {
        if let Some(old) = self.loading_by_gen.remove(&generation) {
            self.by_loading_effect.remove(&old.effect);
        }
        if let Some(read) = read {
            self.loading_by_gen.insert(generation, read);
            self.by_loading_effect.insert(read.effect, index);
        }
    }

    pub(super) fn set_after(
        &mut self,
        generation: ConnectionGeneration,
        after: Option<DeliveryId>,
    ) {
        match after {
            Some(id) => {
                self.after_by_gen.insert(generation, id);
            }
            None => {
                if !self.after_by_gen.is_empty() {
                    self.after_by_gen.remove(&generation);
                }
            }
        }
    }

    pub(super) fn advance_known_through(
        &mut self,
        generation: ConnectionGeneration,
        id: DeliveryId,
    ) {
        let should_update = self
            .known_through_by_gen
            .get(&generation)
            .is_none_or(|&prev| id > prev);
        if should_update {
            self.known_through_by_gen.insert(generation, id);
        }
    }

    pub(super) fn clear_known_through(&mut self, generation: ConnectionGeneration) {
        if !self.known_through_by_gen.is_empty() {
            self.known_through_by_gen.remove(&generation);
        }
    }

    pub(super) fn clear_all_known_through(&mut self) {
        if !self.known_through_by_gen.is_empty() {
            self.known_through_by_gen.clear();
        }
    }

    fn index(&mut self, index: usize) {
        let c = self.rows.as_slice()[index];
        self.by_generation.insert(c.generation, index);
        if let Some(read) = self.loading_of(c.generation) {
            self.by_loading_effect.insert(read.effect, index);
        }
    }

    fn unindex(&mut self, index: usize) {
        let c = self.rows.as_slice()[index];
        self.by_generation.remove(&c.generation);
        if let Some(read) = self.loading_of(c.generation) {
            self.by_loading_effect.remove(&read.effect);
        }
    }

    pub(super) fn push(&mut self, value: Connection) -> Result<(), CoreFault> {
        let index = self.rows.len();
        self.rows
            .try_push(value)
            .map_err(|_| CoreFault::StalePlan)?;
        self.index(index);
        Ok(())
    }

    pub(super) fn replace(&mut self, index: usize, value: Connection) -> Result<(), CoreFault> {
        if index >= self.rows.len() {
            return Err(CoreFault::StalePlan);
        }
        self.unindex(index);
        self.rows.as_mut_slice()[index] = value;
        self.index(index);
        Ok(())
    }

    pub(super) fn record_qos0_ingress(
        &mut self,
        index: usize,
        ingress_order: u64,
    ) -> Result<(), CoreFault> {
        self.rows
            .as_mut_slice()
            .get_mut(index)
            .ok_or(CoreFault::StalePlan)?
            .last_qos0_ingress = ingress_order;
        Ok(())
    }

    pub(super) fn begin_scan(&mut self, index: usize, read: Read) -> Result<(), CoreFault> {
        if index >= self.rows.len() {
            return Err(CoreFault::StalePlan);
        }
        let generation = {
            let state = &mut self.rows.as_mut_slice()[index];
            state.rescan_needed = false;
            state.caught_up = false;
            state.dirty_during_load = false;
            state.generation
        };
        self.set_loading(generation, index, Some(read));
        Ok(())
    }

    pub(super) fn complete_page(
        &mut self,
        index: usize,
        discard: bool,
        next: Option<DeliveryId>,
        no_pending_non_inline: bool,
    ) -> Result<ConnectionGeneration, CoreFault> {
        if index >= self.rows.len() {
            return Err(CoreFault::StalePlan);
        }
        let generation = {
            let c = &mut self.rows.as_mut_slice()[index];
            c.rescan_needed = discard;
            c.caught_up =
                !discard && next.is_none() && !c.dirty_during_load && no_pending_non_inline;
            c.dirty_during_load = false;
            c.generation
        };
        self.set_loading(generation, index, None);
        self.set_after(generation, next);
        Ok(generation)
    }

    pub(super) fn mark_dirty(&mut self, generation: ConnectionGeneration, rescan: bool) {
        if let Some(index) = self.index_of(generation) {
            let is_loading = self.is_loading(generation);
            let c = &mut self.rows.as_mut_slice()[index];
            c.caught_up = false;
            if is_loading {
                c.dirty_during_load = true;
            }
            if rescan {
                c.rescan_needed = true;
            }
        }
    }

    pub(super) fn mark_all_dirty(&mut self, rescan: bool) {
        let has_any_loading = !self.loading_by_gen.is_empty();
        for c in self.rows.as_mut_slice() {
            c.caught_up = false;
            if has_any_loading && self.loading_by_gen.contains_key(&c.generation) {
                c.dirty_during_load = true;
            }
            if rescan {
                c.rescan_needed = true;
            }
        }
        self.clear_all_known_through();
    }

    pub(super) fn remove_generation(&mut self, generation: ConnectionGeneration) {
        if let Some(index) = self.index_of(generation) {
            let last = self.rows.len() - 1;
            let removed_gen = self.rows.as_slice()[index].generation;
            self.unindex(index);
            self.loading_by_gen.remove(&removed_gen);
            self.after_by_gen.remove(&removed_gen);
            self.known_through_by_gen.remove(&removed_gen);
            if index != last {
                self.unindex(last);
                self.rows.as_mut_slice().swap(index, last);
            }
            let _ = self.rows.pop();
            if index != last {
                self.index(index);
            }
        }
    }

    pub(super) fn invalidate_reads(&mut self, session: SessionId) {
        self.invalidate_reads_matching(session, None);
    }

    pub(super) fn invalidate_reads_for(&mut self, session: SessionId, finished_id: DeliveryId) {
        self.invalidate_reads_matching(session, Some(finished_id));
    }

    fn invalidate_reads_matching(&mut self, session: SessionId, finished_id: Option<DeliveryId>) {
        if self.loading_by_gen.is_empty() {
            return;
        }
        let mut invalidated_gens = alloc::vec::Vec::new();
        for (&generation, read) in self.loading_by_gen.iter_mut() {
            let may_contain = match (finished_id, read.after) {
                (Some(done), Some(cursor)) => done >= cursor,
                _ => true,
            };
            if read.session == session && may_contain {
                read.invalidated = true;
                invalidated_gens.push(generation);
            }
        }
        for generation in invalidated_gens {
            self.after_by_gen.remove(&generation);
        }
    }

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
        pub(in crate::owner) fn index_snapshot(&self) -> Vec<String> {
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
            alloc::vec![
                all(self.by_generation.iter().map(|(&g, &i)| (g, i))),
                all(self.by_loading_effect.iter().map(|(e, &i)| (e, i))),
            ]
        }

        pub(in crate::owner) fn as_mut_slice(&mut self) -> &mut [Connection] {
            self.rows.as_mut_slice()
        }
    }
}
