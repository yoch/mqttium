//! Prefix-stripped deterministic ordered map (`ScopedMap`): `no_std +
//! alloc`, without unsafe code, and strictly canonical in `Ord`.
//!
//! Within a run epoch, 24 of the 32 bytes in `OperationId`, `PublicationId`,
//! `PayloadId`, and `DeliveryId` (and 16 of the 24 bytes in `EffectId`) are a
//! constant scope prefix `(incarnation, epoch, owner)`. Storing active-scope
//! keys in a `BTreeMap<K::Local, V>` indexed by their 8-byte `u64` sequence
//! cuts key footprint 4x, improves B-tree cache fanout, and replaces 32-byte
//! comparisons with a single 64-bit compare while retaining a cold `BTreeMap`
//! fallback for recovered prior-epoch keys.

use alloc::collections::BTreeMap;
use core::ops::Bound;

use crate::ids::{
    DeliveryId, EffectId, EffectSeq, LogicalOwnerId, OperationId, OperationSeq, PayloadId,
    PayloadSeq, PublicationId, PublicationSeq, RunEpoch, StoreIncarnation,
};

/// A composite identifier made of a low-cardinality scope prefix and a compact
/// local key whose `Ord` matches the full key's `Ord` within a fixed scope.
pub trait ScopedKey: Ord + Copy {
    type Scope: Ord + Copy;
    type Local: Ord + Copy;

    fn scope(self) -> Self::Scope;
    fn local(self) -> Self::Local;
    fn from_parts(scope: Self::Scope, local: Self::Local) -> Self;
    /// Whether `scope` belongs to a later run of the broker than `than`,
    /// whatever their owners.
    fn later_run(scope: Self::Scope, than: Self::Scope) -> bool;
}

impl ScopedKey for OperationId {
    type Scope = (StoreIncarnation, RunEpoch, LogicalOwnerId);
    type Local = OperationSeq;

    fn scope(self) -> Self::Scope {
        (self.incarnation(), self.epoch(), self.owner())
    }

    fn local(self) -> Self::Local {
        self.sequence()
    }

    fn from_parts(scope: Self::Scope, local: Self::Local) -> Self {
        Self::new(scope.0, scope.1, scope.2, local)
    }

    fn later_run(scope: Self::Scope, than: Self::Scope) -> bool {
        (scope.0, scope.1) > (than.0, than.1)
    }
}

impl ScopedKey for PublicationId {
    type Scope = (StoreIncarnation, RunEpoch, LogicalOwnerId);
    type Local = PublicationSeq;

    fn scope(self) -> Self::Scope {
        (self.incarnation(), self.epoch(), self.owner())
    }

    fn local(self) -> Self::Local {
        self.sequence()
    }

    fn from_parts(scope: Self::Scope, local: Self::Local) -> Self {
        Self::new(scope.0, scope.1, scope.2, local)
    }

    fn later_run(scope: Self::Scope, than: Self::Scope) -> bool {
        (scope.0, scope.1) > (than.0, than.1)
    }
}

impl ScopedKey for PayloadId {
    type Scope = (StoreIncarnation, RunEpoch, LogicalOwnerId);
    type Local = PayloadSeq;

    fn scope(self) -> Self::Scope {
        (self.incarnation(), self.epoch(), self.owner())
    }

    fn local(self) -> Self::Local {
        self.sequence()
    }

    fn from_parts(scope: Self::Scope, local: Self::Local) -> Self {
        Self::new(scope.0, scope.1, scope.2, local)
    }

    fn later_run(scope: Self::Scope, than: Self::Scope) -> bool {
        (scope.0, scope.1) > (than.0, than.1)
    }
}

impl ScopedKey for DeliveryId {
    type Scope = (StoreIncarnation, RunEpoch, LogicalOwnerId);
    type Local = (PublicationSeq, u32);

    fn scope(self) -> Self::Scope {
        (
            self.publication.incarnation(),
            self.publication.epoch(),
            self.publication.owner(),
        )
    }

    fn local(self) -> Self::Local {
        (self.publication.sequence(), self.index)
    }

    fn from_parts(scope: Self::Scope, local: Self::Local) -> Self {
        Self {
            publication: PublicationId::new(scope.0, scope.1, scope.2, local.0),
            index: local.1,
        }
    }

    fn later_run(scope: Self::Scope, than: Self::Scope) -> bool {
        (scope.0, scope.1) > (than.0, than.1)
    }
}

impl ScopedKey for EffectId {
    type Scope = (RunEpoch, LogicalOwnerId);
    type Local = EffectSeq;

    fn scope(self) -> Self::Scope {
        (self.epoch, self.owner)
    }

    fn local(self) -> Self::Local {
        self.sequence
    }

    fn from_parts(scope: Self::Scope, local: Self::Local) -> Self {
        Self {
            epoch: scope.0,
            owner: scope.1,
            sequence: local,
        }
    }

    fn later_run(scope: Self::Scope, than: Self::Scope) -> bool {
        scope.0 > than.0
    }
}

/// Deterministic map with prefix-stripped hot storage for the active scope.
///
/// Invariant: any key whose `scope()` equals `hot_scope` is stored exclusively
/// in `hot` and never in `cold`.
#[derive(Clone, Debug)]
pub struct ScopedMap<K: ScopedKey, V> {
    hot_scope: Option<K::Scope>,
    hot: BTreeMap<K::Local, V>,
    cold: BTreeMap<K, V>,
}

impl<K: ScopedKey, V> Default for ScopedMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: ScopedKey, V> ScopedMap<K, V> {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            hot_scope: None,
            hot: BTreeMap::new(),
            cold: BTreeMap::new(),
        }
    }

    #[must_use]
    pub const fn with_scope(scope: K::Scope) -> Self {
        Self {
            hot_scope: Some(scope),
            hot: BTreeMap::new(),
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
        self.hot_scope = None;
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

    #[must_use]
    pub fn contains_key(&self, key: &K) -> bool {
        if self.hot_scope == Some(key.scope()) {
            self.hot.contains_key(&key.local())
        } else {
            self.cold.contains_key(key)
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let scope = key.scope();
        match self.hot_scope {
            Some(hot) if hot == scope => self.hot.insert(key.local(), value),
            None => {
                self.hot_scope = Some(scope);
                self.hot.insert(key.local(), value)
            }
            Some(_) => self.cold.insert(key, value),
        }
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        if self.hot_scope == Some(key.scope()) {
            let removed = self.hot.remove(&key.local());
            if self.hot.is_empty() && self.cold.is_empty() {
                self.hot_scope = None;
            }
            removed
        } else {
            let removed = self.cold.remove(key);
            if self.hot.is_empty() && self.cold.is_empty() {
                self.hot_scope = None;
            }
            removed
        }
    }

    /// Mutates an existing entry or inserts `default()` when absent.
    pub fn get_or_insert_default(&mut self, key: K) -> &mut V
    where
        V: Default,
    {
        let scope = key.scope();
        match self.hot_scope {
            Some(hot) if hot == scope => self.hot.entry(key.local()).or_default(),
            None => {
                self.hot_scope = Some(scope);
                self.hot.entry(key.local()).or_default()
            }
            Some(_) => self.cold.entry(key).or_default(),
        }
    }

    /// Visits entries in a single-scope inclusive local range `[lo, hi]` in
    /// canonical key order.
    pub fn for_each_in_scope_range<E>(
        &self,
        scope: K::Scope,
        lo: K::Local,
        hi: K::Local,
        mut f: impl FnMut(K, &V) -> Result<(), E>,
    ) -> Result<(), E> {
        if self.hot_scope == Some(scope) {
            for (&local, value) in self.hot.range((Bound::Included(lo), Bound::Included(hi))) {
                f(K::from_parts(scope, local), value)?;
            }
        } else {
            let start = K::from_parts(scope, lo);
            let end = K::from_parts(scope, hi);
            for (&key, value) in self
                .cold
                .range((Bound::Included(start), Bound::Included(end)))
            {
                f(key, value)?;
            }
        }
        Ok(())
    }

    /// Returns whether any entry exists in the single-scope inclusive range
    /// `[lo, hi]`.
    #[must_use]
    pub fn has_any_in_scope_range(&self, scope: K::Scope, lo: K::Local, hi: K::Local) -> bool {
        if self.hot_scope == Some(scope) {
            self.hot
                .range((Bound::Included(lo), Bound::Included(hi)))
                .next()
                .is_some()
        } else {
            let start = K::from_parts(scope, lo);
            let end = K::from_parts(scope, hi);
            self.cold
                .range((Bound::Included(start), Bound::Included(end)))
                .next()
                .is_some()
        }
    }

    /// Iterates values (order is unspecified across hot/cold scopes).
    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.hot.values().chain(self.cold.values())
    }

    /// Iterates keys (order is unspecified across hot/cold scopes).
    pub fn keys(&self) -> impl Iterator<Item = K> + '_ {
        let hot = self.hot_scope.into_iter().flat_map(|scope| {
            self.hot
                .keys()
                .map(move |&local| K::from_parts(scope, local))
        });
        hot.chain(self.cold.keys().copied())
    }

    /// Iterates `(key, &value)` pairs in canonical `Ord` order.
    pub fn iter_ordered(&self) -> OrderedIter<'_, K, V> {
        OrderedIter {
            hot_scope: self.hot_scope,
            hot: self.hot_scope.map(|_| self.hot.range(..).peekable()),
            cold: self.cold.range(..).peekable(),
        }
    }

    /// Iterates `(key, &value)` pairs starting strictly after `after` in
    /// canonical `Ord` order.
    pub fn iter_after(&self, after: Option<K>) -> OrderedIter<'_, K, V> {
        let Some(after) = after else {
            return self.iter_ordered();
        };
        let hot = match self.hot_scope {
            Some(scope) if scope == after.scope() => Some(
                self.hot
                    .range((Bound::Excluded(after.local()), Bound::Unbounded))
                    .peekable(),
            ),
            Some(scope) if scope > after.scope() => Some(self.hot.range(..).peekable()),
            _ => None,
        };
        let cold = self
            .cold
            .range((Bound::Excluded(after), Bound::Unbounded))
            .peekable();
        OrderedIter {
            hot_scope: self.hot_scope,
            hot,
            cold,
        }
    }

    /// Retains only entries satisfying `predicate`.
    pub fn retain(&mut self, mut predicate: impl FnMut(K, &mut V) -> bool) {
        if let Some(scope) = self.hot_scope {
            self.hot
                .retain(|&local, value| predicate(K::from_parts(scope, local), value));
        }
        self.cold.retain(|&key, value| predicate(key, value));
        if self.hot.is_empty() && self.cold.is_empty() {
            self.hot_scope = None;
        }
    }
}

/// Canonical `Ord` merge iterator over `hot` and `cold` partitions of a [`ScopedMap`].
pub struct OrderedIter<'a, K: ScopedKey, V> {
    hot_scope: Option<K::Scope>,
    hot: Option<core::iter::Peekable<alloc::collections::btree_map::Range<'a, K::Local, V>>>,
    cold: core::iter::Peekable<alloc::collections::btree_map::Range<'a, K, V>>,
}

impl<'a, K: ScopedKey, V> Iterator for OrderedIter<'a, K, V> {
    type Item = (K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        let hot_next = self.hot_scope.and_then(|scope| {
            self.hot
                .as_mut()?
                .peek()
                .map(|&(local, _)| K::from_parts(scope, *local))
        });
        let cold_next = self.cold.peek().map(|&(k, _)| *k);
        match (hot_next, cold_next) {
            (Some(hk), Some(ck)) => {
                if hk <= ck {
                    let (_, v) = self.hot.as_mut()?.next()?;
                    Some((hk, v))
                } else {
                    let (&k, v) = self.cold.next()?;
                    Some((k, v))
                }
            }
            (Some(hk), None) => {
                let (_, v) = self.hot.as_mut()?.next()?;
                Some((hk, v))
            }
            (None, Some(_)) => {
                let (&k, v) = self.cold.next()?;
                Some((k, v))
            }
            (None, None) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec::Vec;

    fn op(epoch: u64, seq: u64) -> OperationId {
        OperationId::new(
            StoreIncarnation::new(1),
            RunEpoch::new(epoch),
            LogicalOwnerId::new(0),
            OperationSeq::new(seq),
        )
    }

    #[test]
    fn scoped_map_matches_canonical_order_across_hot_and_cold_scopes() {
        let mut map = ScopedMap::new();
        map.insert(op(2, 10), 10);
        map.insert(op(2, 5), 5);
        map.insert(op(1, 99), 99);
        map.insert(op(3, 1), 1);
        map.insert(op(2, 7), 7);

        let ordered: Vec<_> = map.iter_ordered().map(|(k, &v)| (k, v)).collect();
        assert_eq!(
            ordered,
            alloc::vec![
                (op(1, 99), 99),
                (op(2, 5), 5),
                (op(2, 7), 7),
                (op(2, 10), 10),
                (op(3, 1), 1),
            ]
        );
        let after: Vec<_> = map
            .iter_after(Some(op(2, 5)))
            .map(|(k, &v)| (k, v))
            .collect();
        assert_eq!(
            after,
            alloc::vec![(op(2, 7), 7), (op(2, 10), 10), (op(3, 1), 1)]
        );
        assert_eq!(map.get(&op(2, 7)), Some(&7));
        assert_eq!(map.get(&op(1, 99)), Some(&99));
        assert_eq!(map.remove(&op(2, 7)), Some(7));
        assert_eq!(map.len(), 4);
    }
}
