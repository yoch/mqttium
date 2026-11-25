//! O(1) deterministic internal tables for `VolatileStore`.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::Bound;
use std::sync::Arc;

use mqttiumd_contract::command::{FrozenRecipe, Recipe};
use mqttiumd_contract::flat_map::FlatMap;
use mqttiumd_contract::ids::{
    DeliveryId, LogicalOwnerId, OperationId, OperationSeq, Revision, RunEpoch, SessionId,
    StoreIncarnation,
};
use mqttiumd_contract::packet::SharedBytes;
use mqttiumd_contract::scoped_map::{ScopedKey, ScopedMap};
use mqttiumd_contract::session::{SessionDetachment, SessionExpiry};
use mqttiumd_store_api::{Decision, Limits, ReadError, SessionRecord, SessionState};

use crate::witness::{self, IdentityBounds};

/// Stored receipt recipe: live commits retain the immutable [`FrozenRecipe`]
/// directly without serializing canonical bytes; reopened snapshots retain
/// their verified raw canonical bytes.
#[derive(Clone, Debug)]
pub(crate) enum VRecipe {
    Frozen(FrozenRecipe),
    Raw(Arc<[u8]>),
}

impl VRecipe {
    #[inline]
    pub(crate) fn is_open_will(&self) -> bool {
        match self {
            Self::Frozen(frozen) => witness::is_open_will_recipe_obj(frozen.get()),
            Self::Raw(bytes) => witness::is_open_will_recipe(bytes),
        }
    }

    #[inline]
    pub(crate) fn canonical_len(&self) -> usize {
        match self {
            Self::Frozen(frozen) => frozen.canonical_len(),
            Self::Raw(bytes) => bytes.len(),
        }
    }

    #[inline]
    pub(crate) fn matches_frozen(&self, other: &FrozenRecipe) -> bool {
        match self {
            Self::Frozen(stored) => stored == other,
            Self::Raw(bytes) => bytes.as_ref() == other.to_canonical_bytes().as_slice(),
        }
    }

    pub(crate) fn to_canonical_vec(&self) -> Vec<u8> {
        match self {
            Self::Frozen(frozen) => frozen.to_canonical_bytes(),
            Self::Raw(bytes) => bytes.to_vec(),
        }
    }

    pub(crate) fn receipt_bounds(
        &self,
        limits: &Limits,
        operation: OperationId,
        decision: Decision,
    ) -> Result<Option<IdentityBounds>, ReadError> {
        match self {
            Self::Frozen(frozen) => {
                witness::receipt_bounds(limits, operation, &frozen.to_canonical_bytes(), decision)
            }
            Self::Raw(bytes) => witness::receipt_bounds(limits, operation, bytes, decision),
        }
    }
}

const OPENED_NONE: u8 = 0;
const OPENED_HOT: u8 = 1;
const OPENED_FOREIGN: u8 = 2;

/// Compact in-memory session record sharing immutable `client_id` bytes,
/// prefix-stripping active-origin `opened_by` to 8 bytes, and boxing cold
/// detachment metadata (56 B vs 88 B).
#[derive(Clone, Debug)]
pub(crate) struct VSession {
    pub client_id: SharedBytes,
    pub revision: Revision,
    pub expiry: SessionExpiry,
    pub detached: Option<Box<SessionDetachment>>,
    opened_by_seq: u64,
    pub state: SessionState,
    opened_by_kind: u8,
}

impl VSession {
    #[inline]
    pub(crate) fn new(
        client_id: SharedBytes,
        revision: Revision,
        state: SessionState,
        expiry: SessionExpiry,
        detached: Option<Box<SessionDetachment>>,
    ) -> Self {
        Self {
            client_id,
            revision,
            expiry,
            detached,
            opened_by_seq: 0,
            state,
            opened_by_kind: OPENED_NONE,
        }
    }

    #[inline]
    pub(crate) fn detached(&self) -> Option<SessionDetachment> {
        self.detached.as_deref().copied()
    }

    #[inline]
    pub(crate) fn has_opened_by(&self) -> bool {
        self.opened_by_kind != OPENED_NONE
    }
}

/// Dense session storage indexed by a compact `FlatMap<SessionId, u32>` so
/// empty hash slots cost 16 B instead of `size_of::<VSession>()`.
#[derive(Debug)]
pub(crate) struct SessionTable {
    entries: Vec<Option<(SessionId, VSession)>>,
    free: Vec<u32>,
    index: FlatMap<SessionId, u32>,
    len: usize,
    hot_scope: Option<(StoreIncarnation, RunEpoch, LogicalOwnerId)>,
    foreign_opened_by: BTreeMap<SessionId, OperationId>,
}

impl SessionTable {
    pub(crate) fn new() -> Self {
        Self {
            entries: Vec::new(),
            free: Vec::new(),
            index: FlatMap::new(),
            len: 0,
            hot_scope: None,
            foreign_opened_by: BTreeMap::new(),
        }
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub(crate) fn get(&self, id: &SessionId) -> Option<&VSession> {
        let &slot = self.index.get(id)?;
        self.entries[slot as usize].as_ref().map(|(_, v)| v)
    }

    #[inline]
    pub(crate) fn get_mut(&mut self, id: &SessionId) -> Option<&mut VSession> {
        let &slot = self.index.get(id)?;
        self.entries[slot as usize].as_mut().map(|(_, v)| v)
    }

    #[inline]
    pub(crate) fn opened_by(&self, id: SessionId, row: &VSession) -> Option<OperationId> {
        match row.opened_by_kind {
            OPENED_NONE => None,
            OPENED_HOT => {
                let (inc, ep, own) = self.hot_scope?;
                Some(OperationId::new(
                    inc,
                    ep,
                    own,
                    OperationSeq::new(row.opened_by_seq),
                ))
            }
            _ => self.foreign_opened_by.get(&id).copied(),
        }
    }

    #[inline]
    fn encode_opened_by(&mut self, id: SessionId, op: Option<OperationId>) -> (u8, u64) {
        match op {
            None => {
                if !self.foreign_opened_by.is_empty() {
                    self.foreign_opened_by.remove(&id);
                }
                (OPENED_NONE, 0)
            }
            Some(op) => {
                let scope = op.scope();
                let hot = *self.hot_scope.get_or_insert(scope);
                if scope == hot {
                    if !self.foreign_opened_by.is_empty() {
                        self.foreign_opened_by.remove(&id);
                    }
                    (OPENED_HOT, op.sequence().get())
                } else {
                    self.foreign_opened_by.insert(id, op);
                    (OPENED_FOREIGN, 0)
                }
            }
        }
    }

    #[inline]
    pub(crate) fn set_opened_by(&mut self, id: SessionId, op: Option<OperationId>) {
        let (kind, seq) = self.encode_opened_by(id, op);
        if let Some(row) = self.get_mut(&id) {
            row.opened_by_kind = kind;
            row.opened_by_seq = seq;
        }
    }

    pub(crate) fn insert(
        &mut self,
        id: SessionId,
        mut row: VSession,
        opened_by: Option<OperationId>,
    ) {
        let (kind, seq) = self.encode_opened_by(id, opened_by);
        row.opened_by_kind = kind;
        row.opened_by_seq = seq;
        if let Some(&slot) = self.index.get(&id) {
            self.entries[slot as usize] = Some((id, row));
            return;
        }
        let slot = if let Some(free) = self.free.pop() {
            self.entries[free as usize] = Some((id, row));
            free
        } else {
            let slot = self.entries.len() as u32;
            self.entries.push(Some((id, row)));
            slot
        };
        self.index.insert(id, slot);
        self.len += 1;
    }

    pub(crate) fn insert_record(&mut self, id: SessionId, record: SessionRecord) {
        let client_id = SharedBytes::copy_from(&record.client_id, record.client_id.len())
            .expect("in-memory client_id fits its own length");
        let row = VSession::new(
            client_id,
            record.revision,
            record.state,
            record.expiry,
            record.detached.map(Box::new),
        );
        self.insert(id, row, record.opened_by);
    }

    pub(crate) fn to_record(&self, id: SessionId, row: &VSession) -> SessionRecord {
        SessionRecord {
            client_id: row.client_id.as_slice().to_vec(),
            revision: row.revision,
            state: row.state,
            expiry: row.expiry,
            opened_by: self.opened_by(id, row),
            detached: row.detached(),
        }
    }

    pub(crate) fn remove(&mut self, id: &SessionId) -> Option<VSession> {
        let slot = self.index.remove(id)?;
        let (_, removed) = self.entries[slot as usize].take()?;
        if removed.opened_by_kind == OPENED_FOREIGN {
            self.foreign_opened_by.remove(id);
        }
        self.len -= 1;
        if self.len == 0 {
            self.entries.clear();
            self.free.clear();
        } else {
            self.free.push(slot);
        }
        Some(removed)
    }

    #[inline]
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&SessionId, &VSession)> + '_ {
        self.entries
            .iter()
            .filter_map(|slot| slot.as_ref().map(|(id, row)| (id, row)))
    }

    #[inline]
    pub(crate) fn values(&self) -> impl Iterator<Item = &VSession> + '_ {
        self.entries
            .iter()
            .filter_map(|slot| slot.as_ref().map(|(_, row)| row))
    }
}

/// Per-session sorted set of pending `DeliveryId`s.
///
/// Monotonic FIFO QoS 1 traffic stays in `Small(VecDeque)` with O(1) push-back
/// and O(1) pop-front and zero allocations after warm-up, spilling cleanly to
/// `Large(BTreeSet)` only when a session accumulates more than 32 obligations.
#[derive(Clone, Debug)]
pub(crate) enum SessionObligations {
    Small(VecDeque<DeliveryId>),
    Large(BTreeSet<DeliveryId>),
}

impl Default for SessionObligations {
    #[inline]
    fn default() -> Self {
        Self::Small(VecDeque::new())
    }
}

impl SessionObligations {
    const PROMOTE_THRESHOLD: usize = 128;

    #[inline]
    pub(crate) fn insert(&mut self, delivery: DeliveryId) {
        match self {
            Self::Small(q) => {
                if q.back().is_none_or(|b| *b < delivery) {
                    q.push_back(delivery);
                } else if q.back() == Some(&delivery) {
                    return;
                } else {
                    let pos = q.partition_point(|x| *x < delivery);
                    if q.get(pos) == Some(&delivery) {
                        return;
                    }
                    q.insert(pos, delivery);
                }
                if q.len() > Self::PROMOTE_THRESHOLD {
                    let set: BTreeSet<DeliveryId> = q.drain(..).collect();
                    *self = Self::Large(set);
                }
            }
            Self::Large(set) => {
                set.insert(delivery);
            }
        }
    }

    #[inline]
    pub(crate) fn remove(&mut self, delivery: &DeliveryId) -> bool {
        match self {
            Self::Small(q) => {
                if q.front() == Some(delivery) {
                    q.pop_front();
                    return true;
                }
                if q.back() == Some(delivery) {
                    q.pop_back();
                    return true;
                }
                let pos = q.partition_point(|x| x < delivery);
                if q.get(pos) == Some(delivery) {
                    q.remove(pos);
                    true
                } else {
                    false
                }
            }
            Self::Large(set) => set.remove(delivery),
        }
    }

    pub(crate) fn collect_all(&self) -> Vec<DeliveryId> {
        match self {
            Self::Small(q) => q.iter().copied().collect(),
            Self::Large(set) => set.iter().copied().collect(),
        }
    }

    pub(crate) fn iter_after(&self, after: Option<DeliveryId>) -> SessionObligationIter<'_> {
        match self {
            Self::Small(q) => {
                let start = match after {
                    None => 0,
                    Some(prev) => q.partition_point(|x| *x <= prev),
                };
                SessionObligationIter::Small(q, start)
            }
            Self::Large(set) => {
                let range = match after {
                    None => (Bound::Unbounded, Bound::Unbounded),
                    Some(prev) => (Bound::Excluded(prev), Bound::Unbounded),
                };
                SessionObligationIter::Large(set.range(range))
            }
        }
    }
}

pub(crate) enum SessionObligationIter<'a> {
    Small(&'a VecDeque<DeliveryId>, usize),
    Large(std::collections::btree_set::Range<'a, DeliveryId>),
}

impl Iterator for SessionObligationIter<'_> {
    type Item = DeliveryId;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Small(q, idx) => {
                let item = q.get(*idx).copied()?;
                *idx += 1;
                Some(item)
            }
            Self::Large(range) => range.next().copied(),
        }
    }
}

/// One active owner's direct-indexed ring slice inside [`OriginOpTable`].
#[derive(Debug)]
struct OwnerRing<V> {
    scope: (StoreIncarnation, RunEpoch, LogicalOwnerId),
    base: u64,
    ring: VecDeque<Option<V>>,
    len: usize,
}

impl<V> OwnerRing<V> {
    #[inline]
    fn new(scope: (StoreIncarnation, RunEpoch, LogicalOwnerId), seq: u64, value: V) -> Self {
        let mut ring = VecDeque::new();
        ring.push_back(Some(value));
        Self {
            scope,
            base: seq,
            ring,
            len: 1,
        }
    }

    #[inline]
    fn get(&self, seq: u64) -> Option<&V> {
        if seq >= self.base {
            let offset = seq - self.base;
            if offset < self.ring.len() as u64 {
                return self.ring[offset as usize].as_ref();
            }
        }
        None
    }

    #[inline]
    fn remove(&mut self, seq: u64) -> Option<V> {
        if seq >= self.base {
            let offset = seq - self.base;
            if offset < self.ring.len() as u64 {
                let removed = self.ring[offset as usize].take();
                if removed.is_some() {
                    self.len -= 1;
                    while self.ring.front().is_some_and(Option::is_none) {
                        self.ring.pop_front();
                        self.base = self.base.saturating_add(1);
                    }
                    if self.ring.is_empty() {
                        self.ring.clear();
                    }
                    return removed;
                }
            }
        }
        None
    }
}

/// Active-origin direct-indexed ring table for `VolatileStore::operations`
/// supporting multiple concurrent `LogicalOwnerId` producers (e.g. coordinator
/// and sharded delivery owners) with a `ScopedMap` fallback for historical or
/// non-contiguous sequences.
#[derive(Debug)]
pub(crate) struct OriginOpTable<V> {
    hot: Vec<OwnerRing<V>>,
    hot_len: usize,
    cold: ScopedMap<OperationId, V>,
}

impl<V> Default for OriginOpTable<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V> OriginOpTable<V> {
    const MAX_GAP_PAD: u64 = 64;
    const MAX_HOT_OWNERS: usize = 32;

    pub(crate) fn new() -> Self {
        Self {
            hot: Vec::new(),
            hot_len: 0,
            cold: ScopedMap::new(),
        }
    }

    #[inline]
    fn find_hot(&self, scope: (StoreIncarnation, RunEpoch, LogicalOwnerId)) -> Option<usize> {
        // Common case: small number of owners (1..=16) scanned linearly in L1 cache.
        self.hot.iter().position(|r| r.scope == scope)
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.hot_len + self.cold.len()
    }

    #[inline]
    pub(crate) fn get(&self, key: &OperationId) -> Option<&V> {
        let scope = key.scope();
        if let Some(idx) = self.find_hot(scope) {
            if let Some(val) = self.hot[idx].get(key.sequence().get()) {
                return Some(val);
            }
            if self.cold.is_empty() {
                return None;
            }
        }
        if self.cold.is_empty() {
            None
        } else {
            self.cold.get(key)
        }
    }

    #[inline]
    pub(crate) fn contains_key(&self, key: &OperationId) -> bool {
        self.get(key).is_some()
    }

    #[inline]
    pub(crate) fn insert(&mut self, key: OperationId, value: V) -> Option<V> {
        let scope = key.scope();
        let seq = key.sequence().get();
        let idx = if let Some(idx) = self.find_hot(scope) {
            idx
        } else if self.hot.len() < Self::MAX_HOT_OWNERS {
            let prev = if self.cold.is_empty() {
                None
            } else {
                self.cold.remove(&key)
            };
            self.hot.push(OwnerRing::new(scope, seq, value));
            self.hot_len += 1;
            return prev;
        } else {
            return self.cold.insert(key, value);
        };
        let owner_ring = &mut self.hot[idx];
        if owner_ring.ring.is_empty() {
            let prev = if self.cold.is_empty() {
                None
            } else {
                self.cold.remove(&key)
            };
            owner_ring.base = seq;
            owner_ring.ring.push_back(Some(value));
            owner_ring.len = 1;
            self.hot_len += 1;
            return prev;
        }
        let end = owner_ring.base.saturating_add(owner_ring.ring.len() as u64);
        if seq == end {
            let prev = if self.cold.is_empty() {
                None
            } else {
                self.cold.remove(&key)
            };
            owner_ring.ring.push_back(Some(value));
            owner_ring.len += 1;
            self.hot_len += 1;
            return prev;
        }
        if seq >= owner_ring.base && seq < end {
            let slot = &mut owner_ring.ring[(seq - owner_ring.base) as usize];
            let prev = slot.replace(value);
            if prev.is_some() {
                return prev;
            }
            owner_ring.len += 1;
            self.hot_len += 1;
            return if self.cold.is_empty() {
                None
            } else {
                self.cold.remove(&key)
            };
        }
        if seq > end && seq - end <= Self::MAX_GAP_PAD {
            for gap_seq in end..seq {
                if !self.cold.is_empty() {
                    let gap_id =
                        OperationId::new(scope.0, scope.1, scope.2, OperationSeq::new(gap_seq));
                    if let Some(promoted) = self.cold.remove(&gap_id) {
                        owner_ring.ring.push_back(Some(promoted));
                        owner_ring.len += 1;
                        self.hot_len += 1;
                        continue;
                    }
                }
                owner_ring.ring.push_back(None);
            }
            let prev = if self.cold.is_empty() {
                None
            } else {
                self.cold.remove(&key)
            };
            owner_ring.ring.push_back(Some(value));
            owner_ring.len += 1;
            self.hot_len += 1;
            return prev;
        }
        self.cold.insert(key, value)
    }

    #[inline]
    pub(crate) fn remove(&mut self, key: &OperationId) -> Option<V> {
        let scope = key.scope();
        if let Some(idx) = self.find_hot(scope) {
            if let Some(removed) = self.hot[idx].remove(key.sequence().get()) {
                self.hot_len -= 1;
                return Some(removed);
            }
            if self.cold.is_empty() {
                return None;
            }
        }
        if self.cold.is_empty() {
            None
        } else {
            self.cold.remove(key)
        }
    }

    pub(crate) fn for_each_in_scope_range<E>(
        &self,
        scope: (StoreIncarnation, RunEpoch, LogicalOwnerId),
        start: OperationSeq,
        end: OperationSeq,
        mut f: impl FnMut(OperationId, &V) -> Result<(), E>,
    ) -> Result<(), E> {
        let s = start.get();
        let e = end.get();
        if s > e {
            return Ok(());
        }
        let Some(idx) = self.find_hot(scope) else {
            return self.cold.for_each_in_scope_range(scope, start, end, f);
        };
        let owner_ring = &self.hot[idx];
        if owner_ring.len == 0 {
            return self.cold.for_each_in_scope_range(scope, start, end, f);
        }
        if self.cold.is_empty() {
            let ring_end = owner_ring.base.saturating_add(owner_ring.ring.len() as u64);
            let lo = s.max(owner_ring.base);
            let hi = e.min(ring_end.saturating_sub(1));
            if lo <= hi && !owner_ring.ring.is_empty() {
                for seq in lo..=hi {
                    if let Some(val) = owner_ring.ring[(seq - owner_ring.base) as usize].as_ref() {
                        let id =
                            OperationId::new(scope.0, scope.1, scope.2, OperationSeq::new(seq));
                        f(id, val)?;
                    }
                }
            }
            return Ok(());
        }
        // General case: merge ring and cold entries in ascending sequence order.
        let mut items: Vec<(OperationId, &V)> = Vec::new();
        let ring_end = owner_ring.base.saturating_add(owner_ring.ring.len() as u64);
        let lo = s.max(owner_ring.base);
        let hi = e.min(ring_end.saturating_sub(1));
        if lo <= hi && !owner_ring.ring.is_empty() {
            for seq in lo..=hi {
                if let Some(val) = owner_ring.ring[(seq - owner_ring.base) as usize].as_ref() {
                    items.push((
                        OperationId::new(scope.0, scope.1, scope.2, OperationSeq::new(seq)),
                        val,
                    ));
                }
            }
        }
        for (id, val) in self.cold.iter_ordered() {
            if id.scope() == scope && id.sequence() >= start && id.sequence() <= end {
                items.push((id, val));
            }
        }
        items.sort_unstable_by_key(|(id, _)| *id);
        for (id, val) in items {
            f(id, val)?;
        }
        Ok(())
    }

    pub(crate) fn keys(&self) -> Vec<OperationId> {
        let mut out = Vec::with_capacity(self.len());
        for owner_ring in &self.hot {
            let scope = owner_ring.scope;
            for (i, slot) in owner_ring.ring.iter().enumerate() {
                if slot.is_some() {
                    out.push(OperationId::new(
                        scope.0,
                        scope.1,
                        scope.2,
                        OperationSeq::new(owner_ring.base + i as u64),
                    ));
                }
            }
        }
        out.extend(self.cold.keys());
        out
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = &V> + '_ {
        self.hot
            .iter()
            .flat_map(|r| r.ring.iter().filter_map(Option::as_ref))
            .chain(self.cold.values())
    }

    pub(crate) fn iter_ordered(&self) -> Vec<(OperationId, &V)> {
        let mut out = Vec::with_capacity(self.len());
        for owner_ring in &self.hot {
            let scope = owner_ring.scope;
            for (i, slot) in owner_ring.ring.iter().enumerate() {
                if let Some(val) = slot.as_ref() {
                    out.push((
                        OperationId::new(
                            scope.0,
                            scope.1,
                            scope.2,
                            OperationSeq::new(owner_ring.base + i as u64),
                        ),
                        val,
                    ));
                }
            }
        }
        out.extend(self.cold.iter_ordered());
        if self.hot.len() > 1 || !self.cold.is_empty() {
            out.sort_unstable_by_key(|(id, _)| *id);
        }
        out
    }
}

/// Helper to check whether a recipe is an open-will lifecycle command.
#[inline]
pub(crate) fn is_open_will(recipe: &Recipe) -> bool {
    witness::is_open_will_recipe_obj(recipe)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn op(epoch: u64, seq: u64) -> OperationId {
        OperationId::new(
            StoreIncarnation::new(1),
            RunEpoch::new(epoch),
            LogicalOwnerId::new(0),
            OperationSeq::new(seq),
        )
    }

    #[test]
    fn origin_op_table_cold_entry_not_shadowed_when_ring_pads_across_it() {
        let mut table = OriginOpTable::new();
        for s in 1..=559 {
            assert_eq!(table.insert(op(1, s), s), None);
        }
        // Control-lane op arrives > MAX_GAP_PAD (64) ahead of ring end -> spills into cold.
        assert_eq!(table.insert(op(1, 624), 624), None);
        assert_eq!(table.get(&op(1, 624)), Some(&624));

        // Ordinary lane catches up to 623, then 625 arrives padding across slot 624.
        for s in 560..=623 {
            assert_eq!(table.insert(op(1, s), s), None);
        }
        assert_eq!(table.insert(op(1, 625), 625), None);

        // 624 must remain visible, removable, and present in range scans without duplicates.
        assert_eq!(table.get(&op(1, 624)), Some(&624));
        assert_eq!(table.len(), 625);
        let mut range_seqs = Vec::new();
        let _ = table.for_each_in_scope_range(
            op(1, 1).scope(),
            OperationSeq::new(620),
            OperationSeq::new(625),
            |id, &v| -> Result<(), ()> {
                range_seqs.push((id.sequence().get(), v));
                Ok(())
            },
        );
        assert_eq!(
            range_seqs,
            vec![
                (620, 620),
                (621, 621),
                (622, 622),
                (623, 623),
                (624, 624),
                (625, 625)
            ]
        );
        assert_eq!(table.remove(&op(1, 624)), Some(624));
        assert_eq!(table.get(&op(1, 624)), None);
        assert_eq!(table.len(), 624);
    }

    #[test]
    fn origin_op_table_matches_btreemap_under_out_of_order_gaps() {
        let mut table = OriginOpTable::new();
        let mut oracle = BTreeMap::new();
        let mut rng: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut next = || {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            rng
        };
        let mut frontier = 1u64;
        for step in 0..10_000u64 {
            let mode = next() % 10;
            if mode < 6 {
                // Insert near frontier or with an out-of-order jump up to 160.
                let jump = if next() % 5 == 0 {
                    (next() % 160) + 1
                } else {
                    next() % 16
                };
                let seq = frontier + jump;
                let k = op(1, seq);
                assert_eq!(table.insert(k, step), oracle.insert(k, step));
            } else if mode < 9 {
                // Remove from frontier or random recent key.
                let seq = frontier + (next() % 128);
                let k = op(1, seq);
                assert_eq!(table.remove(&k), oracle.remove(&k));
                if next() % 4 == 0 {
                    for s in frontier..frontier + 8 {
                        let fk = op(1, s);
                        assert_eq!(table.remove(&fk), oracle.remove(&fk));
                    }
                    frontier += 8;
                }
            } else {
                let seq = frontier + (next() % 180);
                let k = op(1, seq);
                assert_eq!(table.get(&k), oracle.get(&k));
            }
            assert_eq!(table.len(), oracle.len());
            if step % 500 == 0 {
                let ordered: Vec<_> = table
                    .iter_ordered()
                    .into_iter()
                    .map(|(k, &v)| (k, v))
                    .collect();
                let expected: Vec<_> = oracle.iter().map(|(&k, &v)| (k, v)).collect();
                assert_eq!(ordered, expected);
            }
        }
        let ordered: Vec<_> = table
            .iter_ordered()
            .into_iter()
            .map(|(k, &v)| (k, v))
            .collect();
        let expected: Vec<_> = oracle.iter().map(|(&k, &v)| (k, v)).collect();
        assert_eq!(ordered, expected);
    }

    #[test]
    fn origin_op_table_multi_owner_matches_btreemap_under_concurrent_gaps() {
        let mut table = OriginOpTable::new();
        let mut oracle = BTreeMap::new();
        let mut rng: u64 = 0x517c_c1b7_2722_0a95;
        let mut next = || {
            rng = rng
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            rng
        };
        let make_op = |owner: u64, seq: u64| {
            OperationId::new(
                StoreIncarnation::new(1),
                RunEpoch::new(1),
                LogicalOwnerId::new(owner),
                OperationSeq::new(seq),
            )
        };
        let mut frontiers = [1u64; 4];
        for step in 0..10_000u64 {
            let owner = (next() % 4) as usize;
            let mode = next() % 10;
            if mode < 6 {
                let jump = if next() % 6 == 0 {
                    (next() % 140) + 1
                } else {
                    next() % 12
                };
                let k = make_op(owner as u64, frontiers[owner] + jump);
                assert_eq!(table.insert(k, step), oracle.insert(k, step));
            } else if mode < 9 {
                let k = make_op(owner as u64, frontiers[owner] + (next() % 100));
                assert_eq!(table.remove(&k), oracle.remove(&k));
                if next() % 4 == 0 {
                    for s in frontiers[owner]..frontiers[owner] + 6 {
                        let fk = make_op(owner as u64, s);
                        assert_eq!(table.remove(&fk), oracle.remove(&fk));
                    }
                    frontiers[owner] += 6;
                }
            } else {
                let lo = frontiers[owner];
                let hi = lo + 40;
                let mut got = Vec::new();
                let _ = table.for_each_in_scope_range(
                    make_op(owner as u64, 1).scope(),
                    OperationSeq::new(lo),
                    OperationSeq::new(hi),
                    |id, &v| -> Result<(), ()> {
                        got.push((id, v));
                        Ok(())
                    },
                );
                let want: Vec<_> = oracle
                    .iter()
                    .filter(|(id, _)| {
                        id.owner() == LogicalOwnerId::new(owner as u64)
                            && id.sequence().get() >= lo
                            && id.sequence().get() <= hi
                    })
                    .map(|(&id, &v)| (id, v))
                    .collect();
                assert_eq!(got, want);
            }
            assert_eq!(table.len(), oracle.len());
        }
        let ordered: Vec<_> = table
            .iter_ordered()
            .into_iter()
            .map(|(k, &v)| (k, v))
            .collect();
        let expected: Vec<_> = oracle.iter().map(|(&k, &v)| (k, v)).collect();
        assert_eq!(ordered, expected);
    }
}
