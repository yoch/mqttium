//! Deterministic open-addressed hash map (`FlatMap`), set (`FlatSet`), and
//! prefix-stripped active-scope map (`ScopedFlatMap`).
//!
//! Designed strictly for `#![no_std]` + `alloc` + `#![forbid(unsafe_code)]`
//! point-lookup tables inside the pure core, runtime, and in-memory store:
//! - Uses a deterministic Fibonacci multiply-shift mixer (zero randomness, zero
//!   `std`).
//! - Uses backward-shift deletion instead of tombstones so high-churn steady-
//!   state tables (in-flight slots, deliveries, frames, obligations) never
//!   degrade or allocate after warm-up.
//! - Its iteration order is that of the table, not of the keys: a reader
//!   whose output depends on order sorts what it reads, and a table that
//!   needs range queries stays in `BTreeMap` / `ScopedMap`.

use alloc::collections::BTreeMap;
use alloc::vec::Vec;

use crate::ids::{
    ConnectionGeneration, ConnectionKey, DeadlineToken, DeliveryId, EffectId, EffectSeq,
    InboundPacketId, OperationId, OperationSeq, OutboundPacketId, PayloadId, PayloadSeq,
    PublicationId, PublicationSeq, SessionId,
};
use crate::scoped_map::ScopedKey;

const MIN_CAPACITY: usize = 16;

/// Deterministic 64-bit Fibonacci mixer (2 instructions on ARM64: `mul` +
/// shifted `eor`, zero collisions on sequential IDs in power-of-two tables).
fn mix64(x: u64) -> u64 {
    let h = x.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    h ^ (h >> 32)
}

fn combine64(a: u64, b: u64) -> u64 {
    let h = a.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ b.wrapping_mul(0x517c_c1b7_2722_0a95);
    h ^ (h >> 32)
}

/// Monotonically increasing integer key eligible for direct-indexed ring lookup
/// in [`SeqMap`].
pub trait SeqKey: FlatKey {
    fn seq_u64(self) -> u64;
}

impl SeqKey for u64 {
    fn seq_u64(self) -> u64 {
        self
    }
}

impl SeqKey for usize {
    fn seq_u64(self) -> u64 {
        self as u64
    }
}

impl SeqKey for SessionId {
    fn seq_u64(self) -> u64 {
        self.get()
    }
}

impl SeqKey for ConnectionGeneration {
    fn seq_u64(self) -> u64 {
        self.get()
    }
}

impl SeqKey for DeadlineToken {
    fn seq_u64(self) -> u64 {
        self.get()
    }
}

impl SeqKey for OperationSeq {
    fn seq_u64(self) -> u64 {
        self.get()
    }
}

impl SeqKey for PublicationSeq {
    fn seq_u64(self) -> u64 {
        self.get()
    }
}

impl SeqKey for PayloadSeq {
    fn seq_u64(self) -> u64 {
        self.get()
    }
}

impl SeqKey for EffectSeq {
    fn seq_u64(self) -> u64 {
        self.get()
    }
}

impl SeqKey for (PublicationSeq, u32) {
    fn seq_u64(self) -> u64 {
        self.0
            .get()
            .wrapping_add(u64::from(self.1).wrapping_mul(0x9e37_79b9_7f4a_7c15))
    }
}

/// A compact `Copy + Eq` key with a deterministic 64-bit hash.
pub trait FlatKey: Copy + Eq {
    fn flat_hash(self) -> u64;
}

impl FlatKey for u64 {
    fn flat_hash(self) -> u64 {
        mix64(self)
    }
}

impl FlatKey for usize {
    fn flat_hash(self) -> u64 {
        mix64(self as u64)
    }
}

impl FlatKey for SessionId {
    fn flat_hash(self) -> u64 {
        mix64(self.get())
    }
}

impl FlatKey for ConnectionGeneration {
    fn flat_hash(self) -> u64 {
        mix64(self.get())
    }
}

impl FlatKey for DeadlineToken {
    fn flat_hash(self) -> u64 {
        mix64(self.get())
    }
}

impl FlatKey for OperationSeq {
    fn flat_hash(self) -> u64 {
        mix64(self.get())
    }
}

impl FlatKey for PublicationSeq {
    fn flat_hash(self) -> u64 {
        mix64(self.get())
    }
}

impl FlatKey for PayloadSeq {
    fn flat_hash(self) -> u64 {
        mix64(self.get())
    }
}

impl FlatKey for EffectSeq {
    fn flat_hash(self) -> u64 {
        mix64(self.get())
    }
}

impl FlatKey for InboundPacketId {
    fn flat_hash(self) -> u64 {
        mix64(u64::from(self.get()))
    }
}

impl FlatKey for OutboundPacketId {
    fn flat_hash(self) -> u64 {
        mix64(u64::from(self.get()))
    }
}

impl FlatKey for ConnectionKey {
    fn flat_hash(self) -> u64 {
        combine64(self.session.get(), self.generation.get())
    }
}

impl FlatKey for (SessionId, InboundPacketId) {
    fn flat_hash(self) -> u64 {
        combine64(self.0.get(), u64::from(self.1.get()))
    }
}

impl FlatKey for (SessionId, OutboundPacketId) {
    fn flat_hash(self) -> u64 {
        combine64(self.0.get(), u64::from(self.1.get()))
    }
}

impl FlatKey for (SessionId, ConnectionGeneration) {
    fn flat_hash(self) -> u64 {
        combine64(self.0.get(), self.1.get())
    }
}

impl FlatKey for (ConnectionKey, OutboundPacketId) {
    fn flat_hash(self) -> u64 {
        combine64(
            self.0.generation.get(),
            self.0.session.get().wrapping_shl(16) ^ u64::from(self.1.get()),
        )
    }
}

impl FlatKey for (PublicationSeq, u32) {
    fn flat_hash(self) -> u64 {
        combine64(self.0.get(), u64::from(self.1))
    }
}

impl FlatKey for OperationId {
    fn flat_hash(self) -> u64 {
        combine64(
            self.sequence().get(),
            self.epoch().get() ^ self.owner().get().wrapping_shl(32),
        )
    }
}

impl FlatKey for PublicationId {
    fn flat_hash(self) -> u64 {
        combine64(
            self.sequence().get(),
            self.epoch().get() ^ self.owner().get().wrapping_shl(32),
        )
    }
}

impl FlatKey for PayloadId {
    fn flat_hash(self) -> u64 {
        combine64(
            self.sequence().get(),
            self.epoch().get() ^ self.owner().get().wrapping_shl(32),
        )
    }
}

impl FlatKey for DeliveryId {
    fn flat_hash(self) -> u64 {
        combine64(self.publication.sequence().get(), u64::from(self.index))
    }
}

impl FlatKey for EffectId {
    fn flat_hash(self) -> u64 {
        combine64(
            self.sequence.get(),
            self.epoch.get() ^ self.owner.get().wrapping_shl(32),
        )
    }
}

/// Deterministic open-addressed hash map using linear probing and
/// backward-shift deletion (zero tombstones, zero steady-state allocations).
#[derive(Clone, Debug)]
pub struct FlatMap<K: FlatKey, V> {
    slots: Vec<Option<(K, V)>>,
    len: usize,
}

impl<K: FlatKey, V> Default for FlatMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: FlatKey, V> FlatMap<K, V> {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            slots: Vec::new(),
            len: 0,
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn clear(&mut self) {
        if self.len != 0 {
            for slot in &mut self.slots {
                *slot = None;
            }
            self.len = 0;
        }
    }

    fn grow(&mut self, min_cap: usize) {
        let new_cap = min_cap.max(MIN_CAPACITY).next_power_of_two();
        let mask = new_cap - 1;
        let mut new_slots: Vec<Option<(K, V)>> = Vec::with_capacity(new_cap);
        new_slots.resize_with(new_cap, || None);
        for (k, v) in self.slots.drain(..).flatten() {
            let mut idx = (k.flat_hash() as usize) & mask;
            while new_slots[idx].is_some() {
                idx = (idx + 1) & mask;
            }
            new_slots[idx] = Some((k, v));
        }
        self.slots = new_slots;
    }

    fn ensure_capacity_for_insert(&mut self) {
        let cap = self.slots.len();
        if cap == 0 {
            self.grow(MIN_CAPACITY);
        } else if (self.len + 1) * 2 > cap {
            self.grow(cap * 2);
        }
    }

    fn find_slot(&self, key: K) -> Option<usize> {
        let cap = self.slots.len();
        if cap == 0 || self.len == 0 {
            return None;
        }
        let mask = cap - 1;
        let mut idx = (key.flat_hash() as usize) & mask;
        loop {
            match &self.slots[idx] {
                Some((k, _)) if *k == key => return Some(idx),
                Some(_) => idx = (idx + 1) & mask,
                None => return None,
            }
        }
    }

    #[must_use]
    pub fn contains_key(&self, key: &K) -> bool {
        self.find_slot(*key).is_some()
    }

    #[must_use]
    pub fn get(&self, key: &K) -> Option<&V> {
        let idx = self.find_slot(*key)?;
        self.slots[idx].as_ref().map(|(_, v)| v)
    }

    #[must_use]
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let idx = self.find_slot(*key)?;
        self.slots[idx].as_mut().map(|(_, v)| v)
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.ensure_capacity_for_insert();
        let mask = self.slots.len() - 1;
        let mut idx = (key.flat_hash() as usize) & mask;
        loop {
            match &mut self.slots[idx] {
                Some((k, v)) if *k == key => {
                    return Some(core::mem::replace(v, value));
                }
                Some(_) => idx = (idx + 1) & mask,
                slot @ None => {
                    *slot = Some((key, value));
                    self.len += 1;
                    return None;
                }
            }
        }
    }

    pub fn get_or_insert_with(&mut self, key: K, f: impl FnOnce() -> V) -> &mut V {
        self.ensure_capacity_for_insert();
        let mask = self.slots.len() - 1;
        let mut idx = (key.flat_hash() as usize) & mask;
        loop {
            match &self.slots[idx] {
                Some((k, _)) if *k == key => break,
                Some(_) => idx = (idx + 1) & mask,
                None => {
                    self.slots[idx] = Some((key, f()));
                    self.len += 1;
                    break;
                }
            }
        }
        match &mut self.slots[idx] {
            Some((_, v)) => v,
            None => unreachable_slot(),
        }
    }

    pub fn get_or_insert_default(&mut self, key: K) -> &mut V
    where
        V: Default,
    {
        self.get_or_insert_with(key, V::default)
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        let idx = self.find_slot(*key)?;
        let removed = self.slots[idx].take().map(|(_, v)| v);
        self.len -= 1;
        self.backward_shift_from(idx);
        removed
    }

    fn backward_shift_from(&mut self, mut hole: usize) {
        let mask = self.slots.len() - 1;
        let mut curr = (hole + 1) & mask;
        while let Some((k, _)) = &self.slots[curr] {
            let home = (k.flat_hash() as usize) & mask;
            let dist_curr = curr.wrapping_sub(home) & mask;
            let dist_hole = hole.wrapping_sub(home) & mask;
            if dist_hole < dist_curr {
                self.slots.swap(hole, curr);
                hole = curr;
            }
            curr = (curr + 1) & mask;
        }
    }

    /// Keeps the entries `f` accepts. `f` sees every entry once.
    pub fn retain(&mut self, mut f: impl FnMut(&K, &mut V) -> bool) {
        if self.len == 0 {
            return;
        }
        // The scan starts after an empty slot, and the table always has
        // one: no run of entries then goes round the place it started, so
        // a removal only moves entries the scan has not reached yet.
        let Some(start) = self.slots.iter().position(Option::is_none) else {
            return;
        };
        let mask = self.slots.len() - 1;
        let mut offset = 1;
        while offset <= mask {
            let i = (start + offset) & mask;
            let keep = match &mut self.slots[i] {
                Some((k, v)) => f(k, v),
                None => true,
            };
            if keep {
                offset += 1;
            } else {
                self.slots[i] = None;
                self.len -= 1;
                // The entry that takes this place, if any, is seen next.
                self.backward_shift_from(i);
            }
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> + '_ {
        self.slots
            .iter()
            .filter_map(|slot| slot.as_ref().map(|(k, v)| (k, v)))
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&K, &mut V)> + '_ {
        self.slots
            .iter_mut()
            .filter_map(|slot| slot.as_mut().map(|(k, v)| (&*k, v)))
    }

    pub fn keys(&self) -> impl Iterator<Item = &K> + '_ {
        self.slots
            .iter()
            .filter_map(|slot| slot.as_ref().map(|(k, _)| k))
    }

    pub fn values(&self) -> impl Iterator<Item = &V> + '_ {
        self.slots
            .iter()
            .filter_map(|slot| slot.as_ref().map(|(_, v)| v))
    }

    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut V> + '_ {
        self.slots
            .iter_mut()
            .filter_map(|slot| slot.as_mut().map(|(_, v)| v))
    }

    pub fn into_entries(self) -> impl Iterator<Item = (K, V)> {
        self.slots.into_iter().flatten()
    }
}

impl<K: FlatKey + Ord, V: PartialEq> PartialEq<BTreeMap<K, V>> for FlatMap<K, V> {
    fn eq(&self, other: &BTreeMap<K, V>) -> bool {
        self.len == other.len() && self.iter().all(|(k, v)| other.get(k) == Some(v))
    }
}

impl<K: FlatKey, V: PartialEq> PartialEq for FlatMap<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len && self.iter().all(|(k, v)| other.get(k) == Some(v))
    }
}

impl<K: FlatKey, V: Eq> Eq for FlatMap<K, V> {}

impl<K: FlatKey, V> IntoIterator for FlatMap<K, V> {
    type Item = (K, V);
    type IntoIter = core::iter::Flatten<alloc::vec::IntoIter<Option<(K, V)>>>;

    fn into_iter(self) -> Self::IntoIter {
        self.slots.into_iter().flatten()
    }
}

impl<K: FlatKey, V> core::ops::Index<&K> for FlatMap<K, V> {
    type Output = V;

    fn index(&self, key: &K) -> &Self::Output {
        match self.get(key) {
            Some(v) => v,
            None => panic!("flat_map key not found"),
        }
    }
}

fn unreachable_slot() -> ! {
    panic!("flat_map slot populated above")
}

/// Deterministic open-addressed hash set backed by [`FlatMap`].
#[derive(Clone, Debug)]
pub struct FlatSet<K: FlatKey> {
    inner: FlatMap<K, ()>,
}

impl<K: FlatKey> Default for FlatSet<K> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: FlatKey> FlatSet<K> {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            inner: FlatMap::new(),
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn clear(&mut self) {
        self.inner.clear();
    }

    #[must_use]
    pub fn contains(&self, key: &K) -> bool {
        self.inner.contains_key(key)
    }

    pub fn insert(&mut self, key: K) -> bool {
        self.inner.insert(key, ()).is_none()
    }

    pub fn remove(&mut self, key: &K) -> bool {
        self.inner.remove(key).is_some()
    }

    pub fn iter(&self) -> impl Iterator<Item = &K> + '_ {
        self.inner.keys()
    }
}

/// Prefix-stripped O(1) deterministic point-lookup map for scoped identifiers.
///
/// Active-scope keys live in a compact `FlatMap<K::Local, V>` (8-byte keys,
/// zero steady-state allocations, backward-shift deletion), while keys from
/// recovered prior epochs fall back to a cold `BTreeMap<K, V>`.
///
/// The active scope is that of the first key received, until a key of a
/// later run arrives: its scope then becomes the active one, and what the
/// compact map held moves to the fallback. A table rebuilt after a restart
/// receives what earlier runs left before the keys of its own run.
#[derive(Clone, Debug)]
pub struct ScopedFlatMap<K: ScopedKey, V>
where
    K::Local: FlatKey,
{
    hot_scope: Option<K::Scope>,
    hot: FlatMap<K::Local, V>,
    cold: BTreeMap<K, V>,
}

impl<K: ScopedKey, V> Default for ScopedFlatMap<K, V>
where
    K::Local: FlatKey,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K: ScopedKey, V> ScopedFlatMap<K, V>
where
    K::Local: FlatKey,
{
    #[must_use]
    pub const fn new() -> Self {
        Self {
            hot_scope: None,
            hot: FlatMap::new(),
            cold: BTreeMap::new(),
        }
    }

    #[must_use]
    pub const fn with_scope(scope: K::Scope) -> Self {
        Self {
            hot_scope: Some(scope),
            hot: FlatMap::new(),
            cold: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.hot.len() + self.cold.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.hot.is_empty() && self.cold.is_empty()
    }

    pub fn clear(&mut self) {
        self.hot.clear();
        self.cold.clear();
    }

    #[must_use]
    pub fn contains_key(&self, key: &K) -> bool {
        if self.hot_scope == Some(key.scope()) {
            self.hot.contains_key(&key.local())
        } else {
            self.cold.contains_key(key)
        }
    }

    #[must_use]
    pub fn get(&self, key: &K) -> Option<&V> {
        if self.hot_scope == Some(key.scope()) {
            self.hot.get(&key.local())
        } else {
            self.cold.get(key)
        }
    }

    #[must_use]
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        if self.hot_scope == Some(key.scope()) {
            self.hot.get_mut(&key.local())
        } else {
            self.cold.get_mut(key)
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let scope = key.scope();
        match self.hot_scope {
            Some(hot) if hot == scope => self.hot.insert(key.local(), value),
            Some(hot) if !K::later_run(scope, hot) => self.cold.insert(key, value),
            _ => {
                self.activate(scope);
                self.hot.insert(key.local(), value)
            }
        }
    }

    /// Makes `scope` the active one: none was, or `scope` is of a later
    /// run. No key of `scope` is in the fallback, since the first of them
    /// comes here.
    fn activate(&mut self, scope: K::Scope) {
        if let Some(earlier) = self.hot_scope.replace(scope) {
            for (local, value) in core::mem::take(&mut self.hot).into_entries() {
                self.cold.insert(K::from_parts(earlier, local), value);
            }
        }
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        if self.hot_scope == Some(key.scope()) {
            self.hot.remove(&key.local())
        } else {
            self.cold.remove(key)
        }
    }

    pub fn get_or_insert_default(&mut self, key: K) -> &mut V
    where
        V: Default,
    {
        let scope = key.scope();
        match self.hot_scope {
            Some(hot) if hot == scope => self.hot.get_or_insert_default(key.local()),
            Some(hot) if !K::later_run(scope, hot) => self.cold.entry(key).or_default(),
            _ => {
                self.activate(scope);
                self.hot.get_or_insert_default(key.local())
            }
        }
    }

    pub fn retain(&mut self, mut f: impl FnMut(K, &mut V) -> bool) {
        if let Some(scope) = self.hot_scope {
            self.hot
                .retain(|local, value| f(K::from_parts(scope, *local), value));
        }
        self.cold.retain(|k, value| f(*k, value));
    }

    pub fn iter(&self) -> impl Iterator<Item = (K, &V)> + '_ {
        let hot_scope = self.hot_scope;
        self.hot
            .iter()
            .filter_map(move |(local, value)| {
                hot_scope.map(|scope| (K::from_parts(scope, *local), value))
            })
            .chain(self.cold.iter().map(|(k, v)| (*k, v)))
    }

    pub fn keys(&self) -> impl Iterator<Item = K> + '_ {
        self.iter().map(|(k, _)| k)
    }

    pub fn values(&self) -> impl Iterator<Item = &V> + '_ {
        self.hot.values().chain(self.cold.values())
    }
}

const SEQ_RING_MIN: usize = 64;
const SEQ_RING_MAX: usize = 16_384;

/// Direct-indexed O(1) ring map for monotonically increasing sequence keys,
/// backed by a cold [`FlatMap`] overflow for rare wrap-around collisions.
///
/// Allocates zero bytes when empty (no idle RSS cost). On steady-state
/// monotonic keys, `insert`, `get`, `get_mut`, and `remove` perform a single
/// bitmask array index with zero hashing, zero linear probing, and zero
/// backward-shift deletion.
#[derive(Clone, Debug)]
pub struct SeqMap<K: SeqKey, V> {
    ring: Vec<Option<(K, V)>>,
    ring_len: usize,
    overflow: FlatMap<K, V>,
}

impl<K: SeqKey, V> Default for SeqMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: SeqKey, V> SeqMap<K, V> {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            ring: Vec::new(),
            ring_len: 0,
            overflow: FlatMap::new(),
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.ring_len + self.overflow.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ring_len == 0 && self.overflow.is_empty()
    }

    pub fn clear(&mut self) {
        if self.ring_len != 0 {
            for slot in &mut self.ring {
                *slot = None;
            }
            self.ring_len = 0;
        }
        self.overflow.clear();
    }

    fn grow_ring(&mut self, new_cap: usize) {
        let cap = new_cap
            .clamp(SEQ_RING_MIN, SEQ_RING_MAX)
            .next_power_of_two();
        let mask = cap - 1;
        let mut new_ring: Vec<Option<(K, V)>> = Vec::with_capacity(cap);
        new_ring.resize_with(cap, || None);
        let mut new_overflow = FlatMap::new();
        let mut new_ring_len = 0usize;
        for (k, v) in self.ring.drain(..).flatten() {
            let idx = (k.seq_u64() as usize) & mask;
            if new_ring[idx].is_none() {
                new_ring[idx] = Some((k, v));
                new_ring_len += 1;
            } else {
                new_overflow.insert(k, v);
            }
        }
        for (k, v) in core::mem::take(&mut self.overflow).into_entries() {
            let idx = (k.seq_u64() as usize) & mask;
            if new_ring[idx].is_none() {
                new_ring[idx] = Some((k, v));
                new_ring_len += 1;
            } else {
                new_overflow.insert(k, v);
            }
        }
        self.ring = new_ring;
        self.ring_len = new_ring_len;
        self.overflow = new_overflow;
    }

    #[must_use]
    pub fn contains_key(&self, key: &K) -> bool {
        let cap = self.ring.len();
        if cap == 0 {
            return false;
        }
        let idx = (key.seq_u64() as usize) & (cap - 1);
        if let Some((k, _)) = &self.ring[idx]
            && *k == *key
        {
            return true;
        }
        !self.overflow.is_empty() && self.overflow.contains_key(key)
    }

    #[must_use]
    pub fn get(&self, key: &K) -> Option<&V> {
        let cap = self.ring.len();
        if cap == 0 {
            return None;
        }
        let idx = (key.seq_u64() as usize) & (cap - 1);
        if let Some((k, v)) = &self.ring[idx]
            && *k == *key
        {
            return Some(v);
        }
        if self.overflow.is_empty() {
            None
        } else {
            self.overflow.get(key)
        }
    }

    #[must_use]
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let cap = self.ring.len();
        if cap == 0 {
            return None;
        }
        let idx = (key.seq_u64() as usize) & (cap - 1);
        if matches!(&self.ring[idx], Some((k, _)) if *k == *key) {
            return self.ring[idx].as_mut().map(|(_, v)| v);
        }
        if self.overflow.is_empty() {
            None
        } else {
            self.overflow.get_mut(key)
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        if self.ring.is_empty() {
            self.grow_ring(SEQ_RING_MIN);
        }
        let cap = self.ring.len();
        let idx = (key.seq_u64() as usize) & (cap - 1);
        match &mut self.ring[idx] {
            None => {
                if !self.overflow.is_empty() {
                    let prev = self.overflow.remove(&key);
                    self.ring[idx] = Some((key, value));
                    self.ring_len += 1;
                    return prev;
                }
                self.ring[idx] = Some((key, value));
                self.ring_len += 1;
                None
            }
            Some((k, v)) if *k == key => Some(core::mem::replace(v, value)),
            Some(_) => self.insert_collision(key, value),
        }
    }

    fn insert_collision(&mut self, key: K, value: V) -> Option<V> {
        if let Some(prev) = self.overflow.insert(key, value) {
            return Some(prev);
        }
        let cap = self.ring.len();
        if cap < SEQ_RING_MAX
            && (self.ring_len + self.overflow.len() >= cap / 2 || self.overflow.len() >= 4)
        {
            self.grow_ring(cap * 2);
        }
        None
    }

    pub fn get_or_insert_default(&mut self, key: K) -> &mut V
    where
        V: Default,
    {
        if self.ring.is_empty() {
            self.grow_ring(SEQ_RING_MIN);
        }
        let cap = self.ring.len();
        let idx = (key.seq_u64() as usize) & (cap - 1);
        if matches!(&self.ring[idx], Some((k, _)) if *k == key) {
            return match &mut self.ring[idx] {
                Some((_, v)) => v,
                None => unreachable_slot(),
            };
        }
        if self.ring[idx].is_none() && self.overflow.is_empty() {
            self.ring[idx] = Some((key, V::default()));
            self.ring_len += 1;
            return match &mut self.ring[idx] {
                Some((_, v)) => v,
                None => unreachable_slot(),
            };
        }
        self.get_or_insert_default_slow(key)
    }

    fn get_or_insert_default_slow(&mut self, key: K) -> &mut V
    where
        V: Default,
    {
        let cap = self.ring.len();
        let idx = (key.seq_u64() as usize) & (cap - 1);
        if self.ring[idx].is_none() {
            let val = self.overflow.remove(&key).unwrap_or_default();
            self.ring[idx] = Some((key, val));
            self.ring_len += 1;
            return match &mut self.ring[idx] {
                Some((_, v)) => v,
                None => unreachable_slot(),
            };
        }
        self.overflow.get_or_insert_default(key)
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        let cap = self.ring.len();
        if cap == 0 {
            return None;
        }
        let idx = (key.seq_u64() as usize) & (cap - 1);
        if matches!(&self.ring[idx], Some((k, _)) if *k == *key) {
            self.ring_len -= 1;
            return self.ring[idx].take().map(|(_, v)| v);
        }
        if self.overflow.is_empty() {
            None
        } else {
            self.overflow.remove(key)
        }
    }

    pub fn retain(&mut self, mut f: impl FnMut(&K, &mut V) -> bool) {
        if self.ring_len != 0 {
            for slot in &mut self.ring {
                let drop_slot = match slot {
                    Some((k, v)) => !f(k, v),
                    None => false,
                };
                if drop_slot {
                    *slot = None;
                    self.ring_len -= 1;
                }
            }
        }
        if !self.overflow.is_empty() {
            self.overflow.retain(f);
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> + '_ {
        self.ring
            .iter()
            .filter_map(|s| s.as_ref().map(|(k, v)| (k, v)))
            .chain(self.overflow.iter())
    }

    pub fn values(&self) -> impl Iterator<Item = &V> + '_ {
        self.ring
            .iter()
            .filter_map(|s| s.as_ref().map(|(_, v)| v))
            .chain(self.overflow.values())
    }

    pub fn into_entries(self) -> impl Iterator<Item = (K, V)> {
        self.ring
            .into_iter()
            .flatten()
            .chain(self.overflow.into_entries())
    }
}

/// Prefix-stripped direct-indexed O(1) map for scoped monotonic identifiers.
///
/// Active-scope keys use a zero-hash [`SeqMap<K::Local, V>`] ring; keys from
/// recovered prior epochs fall back to a cold `BTreeMap<K, V>`.
#[derive(Clone, Debug)]
pub struct ScopedSeqMap<K: ScopedKey, V>
where
    K::Local: SeqKey,
{
    hot_scope: Option<K::Scope>,
    hot: SeqMap<K::Local, V>,
    cold: BTreeMap<K, V>,
}

impl<K: ScopedKey, V> Default for ScopedSeqMap<K, V>
where
    K::Local: SeqKey,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K: ScopedKey, V> ScopedSeqMap<K, V>
where
    K::Local: SeqKey,
{
    #[must_use]
    pub const fn new() -> Self {
        Self {
            hot_scope: None,
            hot: SeqMap::new(),
            cold: BTreeMap::new(),
        }
    }

    #[must_use]
    pub const fn with_scope(scope: K::Scope) -> Self {
        Self {
            hot_scope: Some(scope),
            hot: SeqMap::new(),
            cold: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.hot.len() + self.cold.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.hot.is_empty() && self.cold.is_empty()
    }

    pub fn clear(&mut self) {
        self.hot.clear();
        self.cold.clear();
    }

    #[must_use]
    pub fn contains_key(&self, key: &K) -> bool {
        if self.hot_scope == Some(key.scope()) {
            self.hot.contains_key(&key.local())
        } else {
            self.cold.contains_key(key)
        }
    }

    #[must_use]
    pub fn get(&self, key: &K) -> Option<&V> {
        if self.hot_scope == Some(key.scope()) {
            self.hot.get(&key.local())
        } else {
            self.cold.get(key)
        }
    }

    #[must_use]
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        if self.hot_scope == Some(key.scope()) {
            self.hot.get_mut(&key.local())
        } else {
            self.cold.get_mut(key)
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let scope = key.scope();
        match self.hot_scope {
            Some(hot) if hot == scope => self.hot.insert(key.local(), value),
            Some(hot) if !K::later_run(scope, hot) => self.cold.insert(key, value),
            _ => {
                self.activate(scope);
                self.hot.insert(key.local(), value)
            }
        }
    }

    fn activate(&mut self, scope: K::Scope) {
        if let Some(earlier) = self.hot_scope.replace(scope) {
            for (local, value) in core::mem::take(&mut self.hot).into_entries() {
                self.cold.insert(K::from_parts(earlier, local), value);
            }
        }
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        if self.hot_scope == Some(key.scope()) {
            self.hot.remove(&key.local())
        } else {
            self.cold.remove(key)
        }
    }

    pub fn get_or_insert_default(&mut self, key: K) -> &mut V
    where
        V: Default,
    {
        let scope = key.scope();
        match self.hot_scope {
            Some(hot) if hot == scope => self.hot.get_or_insert_default(key.local()),
            Some(hot) if !K::later_run(scope, hot) => self.cold.entry(key).or_default(),
            _ => {
                self.activate(scope);
                self.hot.get_or_insert_default(key.local())
            }
        }
    }

    pub fn retain(&mut self, mut f: impl FnMut(K, &mut V) -> bool) {
        if let Some(scope) = self.hot_scope {
            self.hot
                .retain(|local, value| f(K::from_parts(scope, *local), value));
        }
        self.cold.retain(|k, value| f(*k, value));
    }

    pub fn iter(&self) -> impl Iterator<Item = (K, &V)> + '_ {
        let hot_scope = self.hot_scope;
        self.hot
            .iter()
            .filter_map(move |(local, value)| {
                hot_scope.map(|scope| (K::from_parts(scope, *local), value))
            })
            .chain(self.cold.iter().map(|(k, v)| (*k, v)))
    }

    pub fn keys(&self) -> impl Iterator<Item = K> + '_ {
        self.iter().map(|(k, _)| k)
    }

    pub fn values(&self) -> impl Iterator<Item = &V> + '_ {
        self.hot.values().chain(self.cold.values())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{LogicalOwnerId, RunEpoch, StoreIncarnation};

    #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
    struct CollidingKey(u64);

    impl FlatKey for CollidingKey {
        fn flat_hash(self) -> u64 {
            // Force all keys into only 4 buckets to torture-test linear probing
            // and backward-shift deletion.
            self.0 & 3
        }
    }

    #[test]
    fn collision_torture_and_backward_shift() {
        let mut flat = FlatMap::new();
        let mut oracle = BTreeMap::new();
        let mut rng: u64 = 0x1234_5678_9abc_def0;
        let mut next = || {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            rng
        };

        for step in 0..25_000u64 {
            let key = CollidingKey(next() % 128);
            let op = next() % 10;
            if op < 5 {
                let val = step;
                assert_eq!(flat.insert(key, val), oracle.insert(key, val));
            } else if op < 9 {
                assert_eq!(flat.remove(&key), oracle.remove(&key));
            } else {
                assert_eq!(flat.get(&key), oracle.get(&key));
            }
            assert_eq!(flat.len(), oracle.len());
            if step % 250 == 0 {
                for k in 0..128 {
                    let ck = CollidingKey(k);
                    assert_eq!(flat.get(&ck), oracle.get(&ck));
                }
            }
        }
        flat.retain(|k, v| (k.0 + *v) % 3 == 0);
        oracle.retain(|k, v| (k.0 + *v) % 3 == 0);
        assert_eq!(flat.len(), oracle.len());
        for k in 0..128 {
            let ck = CollidingKey(k);
            assert_eq!(flat.get(&ck), oracle.get(&ck));
        }
    }

    #[test]
    fn differential_randomized_against_btree() {
        let mut flat: FlatMap<u64, u64> = FlatMap::new();
        let mut oracle: BTreeMap<u64, u64> = BTreeMap::new();
        let mut rng: u64 = 0xfedc_ba98_7654_3210;
        let mut next = || {
            rng = rng
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            rng
        };

        for step in 0..50_000u64 {
            let key = next() % 1024;
            match next() % 4 {
                0 | 1 => {
                    assert_eq!(flat.insert(key, step), oracle.insert(key, step));
                }
                2 => {
                    assert_eq!(flat.remove(&key), oracle.remove(&key));
                }
                _ => {
                    assert_eq!(flat.get(&key), oracle.get(&key));
                    assert_eq!(flat.contains_key(&key), oracle.contains_key(&key));
                }
            }
            assert_eq!(flat.len(), oracle.len());
        }
        for (k, v) in &oracle {
            assert_eq!(flat.get(k), Some(v));
        }
        let mut seen = BTreeMap::new();
        for (&k, &v) in flat.iter() {
            assert!(seen.insert(k, v).is_none());
        }
        assert_eq!(seen, oracle);
        flat.clear();
        assert!(flat.is_empty());
        assert_eq!(flat.get(&0), None);
    }

    #[test]
    fn scoped_flat_map_hot_and_cold_scopes() {
        let inc = StoreIncarnation::new(1);
        let owner = LogicalOwnerId::new(1);
        let hot_ep = RunEpoch::new(2);
        let cold_ep = RunEpoch::new(1);
        let mut map: ScopedFlatMap<OperationId, u64> =
            ScopedFlatMap::with_scope((inc, hot_ep, owner));
        let k_hot = OperationId::new(inc, hot_ep, owner, OperationSeq::new(42));
        let k_cold = OperationId::new(inc, cold_ep, owner, OperationSeq::new(42));

        assert_eq!(map.insert(k_hot, 100), None);
        assert_eq!(map.insert(k_cold, 200), None);
        assert_eq!(map.len(), 2);
        assert_eq!(map.get(&k_hot), Some(&100));
        assert_eq!(map.get(&k_cold), Some(&200));

        assert_eq!(map.remove(&k_hot), Some(100));
        assert_eq!(map.get(&k_hot), None);
        assert_eq!(map.get(&k_cold), Some(&200));
        assert_eq!(map.len(), 1);
    }

    #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
    struct LastSlotKey(u64);

    impl FlatKey for LastSlotKey {
        fn flat_hash(self) -> u64 {
            // Every key belongs in the last slot: the run of entries goes
            // round the end of the table.
            u64::MAX
        }
    }

    /// `retain` shows every entry to its predicate once, also when removing
    /// one moves entries from the start of the table to its end.
    #[test]
    fn retain_calls_its_predicate_once_for_each_entry() {
        let mut flat = FlatMap::new();
        for key in 0..8 {
            flat.insert(LastSlotKey(key), ());
        }
        let mut calls = BTreeMap::new();
        flat.retain(|key, ()| {
            *calls.entry(*key).or_insert(0u32) += 1;
            key.0 % 3 != 0
        });
        for key in 0..8 {
            assert_eq!(calls.get(&LastSlotKey(key)), Some(&1));
            assert_eq!(flat.contains_key(&LastSlotKey(key)), key % 3 != 0);
        }
        assert_eq!(flat.len(), 5);
    }

    fn operation(epoch: u64, owner: u64, sequence: u64) -> OperationId {
        OperationId::new(
            StoreIncarnation::new(1),
            RunEpoch::new(epoch),
            LogicalOwnerId::new(owner),
            OperationSeq::new(sequence),
        )
    }

    /// A table rebuilt after a restart first receives what earlier runs
    /// left. The keys the run then creates are the busy ones: they get the
    /// hot storage, whatever was inserted first.
    #[test]
    fn scoped_flat_map_gives_its_hot_storage_to_the_newest_run() {
        let mut map: ScopedFlatMap<OperationId, u64> = ScopedFlatMap::new();
        let left = [
            (operation(1, 0, 7), 17),
            (operation(1, 0, 8), 18),
            (operation(2, 0, 7), 27),
        ];
        for (key, value) in left {
            assert_eq!(map.insert(key, value), None);
        }
        let first_of_the_run = operation(3, 0, 1);
        assert_eq!(map.insert(first_of_the_run, 31), None);
        assert_eq!(map.hot_scope, Some(first_of_the_run.scope()));

        // A key of an earlier run, or of another owner of this run, moves
        // nothing.
        let late = operation(2, 0, 9);
        let other_owner = operation(3, 5, 1);
        assert_eq!(map.insert(late, 29), None);
        *map.get_or_insert_default(other_owner) += 35;
        assert_eq!(map.hot_scope, Some(first_of_the_run.scope()));

        assert_eq!(map.len(), 6);
        for (key, value) in left {
            assert_eq!(map.get(&key), Some(&value));
        }
        assert_eq!(map.get(&first_of_the_run), Some(&31));
        assert_eq!(map.get(&late), Some(&29));
        assert_eq!(map.get(&other_owner), Some(&35));

        // The same through the other way in.
        let mut counted: ScopedFlatMap<OperationId, u64> = ScopedFlatMap::new();
        *counted.get_or_insert_default(operation(1, 0, 7)) += 1;
        *counted.get_or_insert_default(first_of_the_run) += 1;
        assert_eq!(counted.hot_scope, Some(first_of_the_run.scope()));
        assert_eq!(counted.get(&operation(1, 0, 7)), Some(&1));
    }

    #[test]
    fn scoped_flat_map_matches_an_ordered_map_across_runs_and_owners() {
        let mut map: ScopedFlatMap<OperationId, u64> = ScopedFlatMap::new();
        let mut oracle = BTreeMap::new();
        let mut rng: u64 = 0x0dd0_5eed_cafe_f00d;
        let mut next = || {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            rng >> 33
        };
        for step in 0..20_000u64 {
            // Later steps draw from later runs, as a broker restarted
            // several times does, with stragglers of the earlier ones.
            let newest = 1 + step / 5_000;
            let epoch = 1 + next() % newest;
            let key = operation(epoch, next() % 2, next() % 48);
            match next() % 10 {
                0..=4 => assert_eq!(map.insert(key, step), oracle.insert(key, step)),
                5 => *map.get_or_insert_default(key) += 1,
                6..=8 => assert_eq!(map.remove(&key), oracle.remove(&key)),
                _ => assert_eq!(map.get(&key), oracle.get(&key)),
            }
            if let Some(value) = map.get(&key)
                && oracle.get(&key) != Some(value)
            {
                // Only the counted insertion leaves the oracle behind.
                *oracle.entry(key).or_default() += 1;
                assert_eq!(oracle.get(&key), Some(value));
            }
            assert_eq!(map.len(), oracle.len());
        }
        let mut seen: Vec<(OperationId, u64)> = map.iter().map(|(k, v)| (k, *v)).collect();
        seen.sort();
        let expected: Vec<(OperationId, u64)> = oracle.into_iter().collect();
        assert_eq!(seen, expected);
    }

    #[test]
    fn seq_map_matches_btree_under_monotonic_and_colliding_keys() {
        let mut seq: SeqMap<u64, u64> = SeqMap::new();
        let mut oracle: BTreeMap<u64, u64> = BTreeMap::new();
        let mut rng: u64 = 0x55aa_1234_9988_7766;
        let mut next = || {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            rng >> 20
        };
        // Steady-state monotonic window + periodic old stragglers colliding mod ring size.
        for step in 1..40_000u64 {
            let key = if step % 17 == 0 {
                (next() % 16) * 64 + (step % 64)
            } else {
                step.saturating_sub(next() % 96)
            };
            match next() % 5 {
                0 | 1 => assert_eq!(seq.insert(key, step), oracle.insert(key, step)),
                2 | 3 => assert_eq!(seq.remove(&key), oracle.remove(&key)),
                _ => {
                    assert_eq!(seq.get(&key), oracle.get(&key));
                    assert_eq!(seq.contains_key(&key), oracle.contains_key(&key));
                }
            }
            assert_eq!(seq.len(), oracle.len());
        }
        seq.retain(|k, v| (k + *v) % 3 != 0);
        oracle.retain(|k, v| (k + *v) % 3 != 0);
        assert_eq!(seq.len(), oracle.len());
        for (k, v) in &oracle {
            assert_eq!(seq.get(k), Some(v));
        }
        seq.clear();
        assert!(seq.is_empty());
    }

    #[test]
    fn scoped_seq_map_matches_btree_across_epochs_and_owners() {
        let mut map: ScopedSeqMap<OperationId, u64> = ScopedSeqMap::new();
        let mut oracle = BTreeMap::new();
        let mut rng: u64 = 0x0dd0_5eed_1234_abcd;
        let mut next = || {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            rng >> 33
        };
        for step in 0..20_000u64 {
            let newest = 1 + step / 5_000;
            let epoch = 1 + next() % newest;
            let key = operation(epoch, next() % 2, 1 + next() % 128);
            match next() % 10 {
                0..=4 => assert_eq!(map.insert(key, step), oracle.insert(key, step)),
                5 => *map.get_or_insert_default(key) += 1,
                6..=8 => assert_eq!(map.remove(&key), oracle.remove(&key)),
                _ => assert_eq!(map.get(&key), oracle.get(&key)),
            }
            if let Some(value) = map.get(&key)
                && oracle.get(&key) != Some(value)
            {
                *oracle.entry(key).or_default() += 1;
                assert_eq!(oracle.get(&key), Some(value));
            }
            assert_eq!(map.len(), oracle.len());
        }
        let mut seen: Vec<(OperationId, u64)> = map.iter().map(|(k, v)| (k, *v)).collect();
        seen.sort();
        let expected: Vec<(OperationId, u64)> = oracle.into_iter().collect();
        assert_eq!(seen, expected);
    }

    #[test]
    fn scoped_seq_map_delivery_id_handles_pairs_and_fanout() {
        let mut map: ScopedSeqMap<DeliveryId, u64> = ScopedSeqMap::new();
        let mut oracle = BTreeMap::new();
        let inc = StoreIncarnation::new(1);
        let ep = RunEpoch::new(1);
        let owner = LogicalOwnerId::new(0);
        // 1:1 pairs sliding window + 1:16 fanout mixed
        for p in 1..=2000u64 {
            let fanout = if p % 4 == 0 { 16 } else { 1 };
            for idx in 0..fanout {
                let id = DeliveryId {
                    publication: PublicationId::new(inc, ep, owner, PublicationSeq::new(p)),
                    index: idx,
                };
                let val = (p << 8) | u64::from(idx);
                assert_eq!(map.insert(id, val), oracle.insert(id, val));
                assert_eq!(map.get(&id), oracle.get(&id));
            }
            if p > 64 {
                let old_p = p - 64;
                let old_fanout = if old_p % 4 == 0 { 16 } else { 1 };
                for idx in 0..old_fanout {
                    let id = DeliveryId {
                        publication: PublicationId::new(inc, ep, owner, PublicationSeq::new(old_p)),
                        index: idx,
                    };
                    assert_eq!(map.remove(&id), oracle.remove(&id));
                }
            }
            assert_eq!(map.len(), oracle.len());
        }
        let mut seen: Vec<(DeliveryId, u64)> = map.iter().map(|(k, v)| (k, *v)).collect();
        seen.sort();
        let expected: Vec<(DeliveryId, u64)> = oracle.into_iter().collect();
        assert_eq!(seen, expected);
    }
}
