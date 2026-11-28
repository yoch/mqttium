//! Fixed-capacity stable-index slab and inline small index set for `Owner`.
//!
//! Both structures are `no_std + alloc`, `#![forbid(unsafe_code)]`, and
//! strictly deterministic.

use alloc::vec::Vec;
use core::ops::{Index, IndexMut};

use crate::contract::capacity::AllocationFailed;

/// Fixed-capacity slab with O(1) LIFO free-list reuse and stable indices.
///
/// Removing an element never relocates any other element, eliminating
/// swap-remove copies and secondary-index fixups on hot transitions.
#[derive(Debug)]
pub(crate) struct BoundedSlab<T> {
    entries: Vec<Option<T>>,
    free: Vec<u32>,
    len: usize,
    capacity: usize,
}

impl<T> BoundedSlab<T> {
    /// Reserves storage for up to `capacity` entries up front.
    pub(crate) fn with_capacity(capacity: usize) -> Result<Self, AllocationFailed> {
        if u32::try_from(capacity).is_err() {
            return Err(AllocationFailed);
        }
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(capacity)
            .map_err(|_| AllocationFailed)?;
        let mut free = Vec::new();
        free.try_reserve_exact(capacity)
            .map_err(|_| AllocationFailed)?;
        Ok(Self {
            entries,
            free,
            len: 0,
            capacity,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    pub(crate) fn get(&self, index: usize) -> Option<&T> {
        self.entries.get(index).and_then(Option::as_ref)
    }

    pub(crate) fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        self.entries.get_mut(index).and_then(Option::as_mut)
    }

    pub(crate) fn insert(&mut self, value: T) -> Result<usize, T> {
        if let Some(idx) = self.free.pop() {
            let idx = idx as usize;
            self.entries[idx] = Some(value);
            self.len += 1;
            return Ok(idx);
        }
        if self.entries.len() >= self.capacity {
            return Err(value);
        }
        let idx = self.entries.len();
        self.entries.push(Some(value));
        self.len += 1;
        Ok(idx)
    }

    pub(crate) fn remove(&mut self, index: usize) -> Option<T> {
        let removed = self.entries.get_mut(index)?.take()?;
        self.len -= 1;
        if self.len == 0 {
            self.entries.clear();
            self.free.clear();
        } else {
            self.free.push(index as u32);
        }
        Some(removed)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (usize, &T)> + '_ {
        self.entries
            .iter()
            .enumerate()
            .filter_map(|(i, slot)| slot.as_ref().map(|v| (i, v)))
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = &T> + '_ {
        self.entries.iter().filter_map(Option::as_ref)
    }
}

impl<T> Index<usize> for BoundedSlab<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        self.entries[index].as_ref().expect("occupied slab entry")
    }
}

impl<T> IndexMut<usize> for BoundedSlab<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.entries[index].as_mut().expect("occupied slab entry")
    }
}

const SMALL_SET_INLINE: usize = 8;

/// Inline sorted index set (zero heap allocations for up to 8 active entries
/// per session/window, spilling transparently to a sorted `Vec<usize>` above 8).
#[derive(Clone, PartialEq, Eq, Debug)]
pub(crate) enum SmallIndexSet {
    Inline {
        len: u8,
        items: [u32; SMALL_SET_INLINE],
    },
    Large(Vec<usize>),
}

impl Default for SmallIndexSet {
    fn default() -> Self {
        Self::Inline {
            len: 0,
            items: [0; SMALL_SET_INLINE],
        }
    }
}

impl SmallIndexSet {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Inline { len, .. } => usize::from(*len),
            Self::Large(v) => v.len(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn insert(&mut self, value: usize) -> bool {
        match self {
            Self::Inline { len, items } => {
                let n = usize::from(*len);
                if let Ok(val32) = u32::try_from(value) {
                    let mut pos = 0;
                    while pos < n {
                        let cur = items[pos];
                        if cur == val32 {
                            return false;
                        }
                        if cur > val32 {
                            break;
                        }
                        pos += 1;
                    }
                    if n < SMALL_SET_INLINE {
                        items.copy_within(pos..n, pos + 1);
                        items[pos] = val32;
                        *len += 1;
                        return true;
                    }
                }
                let mut v = Vec::with_capacity(SMALL_SET_INLINE * 2);
                for &x in &items[..n] {
                    v.push(x as usize);
                }
                let inserted = match v.binary_search(&value) {
                    Ok(_) => false,
                    Err(pos) => {
                        v.insert(pos, value);
                        true
                    }
                };
                *self = Self::Large(v);
                inserted
            }
            Self::Large(v) => match v.binary_search(&value) {
                Ok(_) => false,
                Err(pos) => {
                    v.insert(pos, value);
                    true
                }
            },
        }
    }

    pub(crate) fn remove(&mut self, value: &usize) -> bool {
        match self {
            Self::Inline { len, items } => {
                let Ok(val32) = u32::try_from(*value) else {
                    return false;
                };
                let n = usize::from(*len);
                let mut pos = 0;
                while pos < n {
                    if items[pos] == val32 {
                        items.copy_within((pos + 1)..n, pos);
                        *len -= 1;
                        return true;
                    }
                    if items[pos] > val32 {
                        return false;
                    }
                    pos += 1;
                }
                false
            }
            Self::Large(v) => match v.binary_search(value) {
                Ok(pos) => {
                    v.remove(pos);
                    true
                }
                Err(_) => false,
            },
        }
    }

    pub(crate) fn iter(&self) -> SmallIndexIter<'_> {
        match self {
            Self::Inline { len, items } => SmallIndexIter::Inline {
                items: &items[..usize::from(*len)],
                pos: 0,
            },
            Self::Large(v) => SmallIndexIter::Large(v.iter()),
        }
    }
}

pub(crate) enum SmallIndexIter<'a> {
    Inline { items: &'a [u32], pos: usize },
    Large(core::slice::Iter<'a, usize>),
}

impl Iterator for SmallIndexIter<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Inline { items, pos } => {
                let &item = items.get(*pos)?;
                *pos += 1;
                Some(item as usize)
            }
            Self::Large(iter) => iter.next().copied(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Inline { items, pos } => {
                let rem = items.len().saturating_sub(*pos);
                (rem, Some(rem))
            }
            Self::Large(iter) => iter.size_hint(),
        }
    }
}

impl<'a> IntoIterator for &'a SmallIndexSet {
    type Item = usize;
    type IntoIter = SmallIndexIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub(crate) enum SmallIndexIntoIter {
    Inline {
        items: [u32; SMALL_SET_INLINE],
        pos: usize,
        len: usize,
    },
    Large(alloc::vec::IntoIter<usize>),
}

impl Iterator for SmallIndexIntoIter {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Inline { items, pos, len } => {
                if *pos >= *len {
                    return None;
                }
                let item = items[*pos] as usize;
                *pos += 1;
                Some(item)
            }
            Self::Large(iter) => iter.next(),
        }
    }
}

impl IntoIterator for SmallIndexSet {
    type Item = usize;
    type IntoIter = SmallIndexIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Self::Inline { len, items } => SmallIndexIntoIter::Inline {
                items,
                pos: 0,
                len: usize::from(len),
            },
            Self::Large(v) => SmallIndexIntoIter::Large(v.into_iter()),
        }
    }
}

#[cfg(test)]
mod test_support {
    use super::*;

    impl<T> BoundedSlab<T> {
        pub(crate) fn to_vec(&self) -> Vec<T>
        where
            T: Clone,
        {
            self.values().cloned().collect()
        }

        pub(crate) fn last_mut(&mut self) -> Option<&mut T> {
            self.entries.iter_mut().rev().find_map(Option::as_mut)
        }
    }
}
