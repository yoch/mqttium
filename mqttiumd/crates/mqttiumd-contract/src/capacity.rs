//! Typed quantities and bounded containers (DEM-005).
//!
//! Units are visible in the types, and a container cannot grow past the
//! capacity it was created with: a refused insertion hands the item back.

use alloc::collections::VecDeque;
use alloc::vec::Vec;

macro_rules! quantity {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
        pub struct $name(u64);

        impl $name {
            /// No unit at all.
            pub const ZERO: Self = Self(0);

            /// Builds the quantity from a count of its unit.
            #[must_use]
            pub const fn new(amount: u64) -> Self {
                Self(amount)
            }

            /// The count of units.
            #[must_use]
            pub const fn get(self) -> u64 {
                self.0
            }

            /// Sum, or `None` on overflow.
            #[must_use]
            pub const fn checked_add(self, other: Self) -> Option<Self> {
                match self.0.checked_add(other.0) {
                    Some(sum) => Some(Self(sum)),
                    None => None,
                }
            }

            /// Difference, or `None` when `other` exceeds `self`.
            #[must_use]
            pub const fn checked_sub(self, other: Self) -> Option<Self> {
                match self.0.checked_sub(other.0) {
                    Some(difference) => Some(Self(difference)),
                    None => None,
                }
            }
        }
    };
}

quantity!(
    /// Bytes of physical payload, counted until the last reference is gone.
    PayloadBytes
);
quantity!(
    /// References to shared payloads and per-destination metadata.
    References
);
quantity!(
    /// QoS exchange slots.
    ExchangeSlots
);
quantity!(
    /// Places in an owner's effect box.
    EffectSlots
);
quantity!(
    /// Places reserved to receive and handle adapter results.
    ResultSlots
);
quantity!(
    /// Retained command receipts.
    ReceiptSlots
);
quantity!(
    /// Counted continuations.
    Continuations
);
quantity!(
    /// Work a turn may spend. It is not a reservation of slots or memory.
    WorkUnits
);

/// The resource a transition is short of.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Resource {
    PayloadBytes,
    References,
    ExchangeSlots,
    EffectSlots,
    ResultSlots,
    ReceiptSlots,
    Continuations,
    /// Entries of an effect box's reservation table.
    Reservations,
    Work,
}

/// The event that makes retrying a blocked input useful. A blocked input
/// waits for it; it is never retried in a busy loop (DEM-006).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum WakeCondition {
    /// A holder released some of the resource.
    ResourceReleased,
    /// An adapter accepted an effect and freed its slot.
    EffectAccepted,
    /// A result was delivered and its reserve returned.
    ResultDelivered,
    /// The scheduler granted a new turn with a larger work limit.
    TurnGranted,
}

/// What a returned input needs before it can be applied.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct CapacityNeed {
    pub resource: Resource,
    pub amount: u64,
    pub wake: WakeCondition,
}

/// The allocator could not provide the requested capacity.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct AllocationFailed;

/// A vector that never exceeds the capacity fixed at construction.
///
/// The storage is reserved fallibly up front, and no method can push past
/// the bound, so no later reallocation happens. The inner `Vec` is not
/// reachable mutably.
///
/// Equality compares the bound as well as the items: two containers with
/// the same items and different bounds refuse different insertions, so they
/// are not interchangeable. A type whose value is only the list of items,
/// such as a recipe, defines its own equality over `as_slice`.
#[derive(PartialEq, Eq, Debug)]
pub struct BoundedVec<T> {
    items: Vec<T>,
    capacity: usize,
}

impl<T> BoundedVec<T> {
    /// Reserves room for exactly `capacity` items.
    pub fn with_capacity(capacity: usize) -> Result<Self, AllocationFailed> {
        let mut items = Vec::new();
        items
            .try_reserve_exact(capacity)
            .map_err(|_| AllocationFailed)?;
        Ok(Self { items, capacity })
    }

    /// Appends the item, or returns it untouched when the vector is full.
    pub fn try_push(&mut self, item: T) -> Result<(), T> {
        if self.items.len() >= self.capacity {
            return Err(item);
        }
        self.items.push(item);
        Ok(())
    }

    /// Removes and returns the last item.
    pub fn pop(&mut self) -> Option<T> {
        self.items.pop()
    }

    /// Number of items held.
    #[must_use]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Whether no item is held.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Whether the bound is reached.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.items.len() >= self.capacity
    }

    /// The fixed bound.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Read-only view of the items in insertion order.
    #[must_use]
    pub fn as_slice(&self) -> &[T] {
        &self.items
    }

    /// Mutable view of the items held. It cannot add or remove an item, so
    /// the bound still holds.
    #[must_use]
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        &mut self.items
    }
}

impl<T: Clone> BoundedVec<T> {
    /// A bounded vector holding a copy of `items`, with capacity `capacity`.
    /// Returns `None` when the slice exceeds the bound or allocation fails.
    #[must_use]
    pub fn from_slice(items: &[T], capacity: usize) -> Option<Self> {
        if items.len() > capacity {
            return None;
        }
        let mut bounded = Self::with_capacity(capacity).ok()?;
        bounded.items.extend_from_slice(items);
        Some(bounded)
    }
}

/// A first-in first-out queue that never exceeds its fixed capacity.
#[derive(Debug)]
pub struct BoundedQueue<T> {
    items: VecDeque<T>,
    capacity: usize,
}

impl<T> BoundedQueue<T> {
    /// Reserves room for exactly `capacity` items.
    pub fn with_capacity(capacity: usize) -> Result<Self, AllocationFailed> {
        let mut items = VecDeque::new();
        items
            .try_reserve_exact(capacity)
            .map_err(|_| AllocationFailed)?;
        Ok(Self { items, capacity })
    }

    /// Appends at the back, or returns the item untouched when full.
    pub fn try_push_back(&mut self, item: T) -> Result<(), T> {
        if self.items.len() >= self.capacity {
            return Err(item);
        }
        self.items.push_back(item);
        Ok(())
    }

    /// Puts an item back at the front, or returns it untouched when full.
    pub fn try_push_front(&mut self, item: T) -> Result<(), T> {
        if self.items.len() >= self.capacity {
            return Err(item);
        }
        self.items.push_front(item);
        Ok(())
    }

    /// Removes and returns the oldest item.
    pub fn pop_front(&mut self) -> Option<T> {
        self.items.pop_front()
    }

    /// Number of items held.
    #[must_use]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Whether no item is held.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// The fixed bound.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Read-only iteration from oldest to newest.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.items.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantities_check_both_directions() {
        let limit = PayloadBytes::new(u64::MAX);
        assert_eq!(limit.checked_add(PayloadBytes::new(1)), None);
        assert_eq!(
            PayloadBytes::new(5).checked_sub(PayloadBytes::new(5)),
            Some(PayloadBytes::ZERO)
        );
        assert_eq!(PayloadBytes::new(5).checked_sub(PayloadBytes::new(6)), None);
    }

    #[test]
    fn bounded_vec_returns_the_refused_item() {
        let mut items = BoundedVec::with_capacity(2).expect("small allocation");
        assert_eq!(items.try_push("a"), Ok(()));
        assert_eq!(items.try_push("b"), Ok(()));
        assert!(items.is_full());
        assert_eq!(items.try_push("c"), Err("c"));
        assert_eq!(items.as_slice(), &["a", "b"]);
        assert_eq!(items.capacity(), 2);
    }

    #[test]
    fn zero_capacity_refuses_everything() {
        let mut items = BoundedVec::with_capacity(0).expect("empty allocation");
        assert_eq!(items.try_push(1u8), Err(1));
        assert!(items.is_empty());
        let mut queue = BoundedQueue::with_capacity(0).expect("empty allocation");
        assert_eq!(queue.try_push_back(1u8), Err(1));
        assert_eq!(queue.try_push_front(1u8), Err(1));
    }

    #[test]
    fn from_slice_respects_the_bound() {
        assert!(BoundedVec::from_slice(&[1, 2, 3], 2).is_none());
        let exact = BoundedVec::from_slice(&[1, 2], 2).expect("fits");
        assert!(exact.is_full());
    }

    #[test]
    fn bounded_queue_keeps_order_and_bound() {
        let mut queue = BoundedQueue::with_capacity(2).expect("small allocation");
        assert_eq!(queue.try_push_back(1), Ok(()));
        assert_eq!(queue.try_push_back(2), Ok(()));
        assert_eq!(queue.try_push_back(3), Err(3));
        assert_eq!(queue.pop_front(), Some(1));
        assert_eq!(queue.try_push_front(1), Ok(()));
        assert_eq!(queue.iter().copied().collect::<Vec<_>>(), vec![1, 2]);
        assert_eq!(queue.len(), 2);
    }
}
