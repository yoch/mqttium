//! `release_if_current` and `Account::reserve` do not allocate (DEM-021).
//!
//! The global allocator of this test binary counts the allocations of the
//! thread that asked for the count. Each outcome of the function is
//! called once under the count, and the count must stay at zero. A control
//! shows that the counter sees an allocation.
//!
//! This is a run of the function as the product toolchain compiles it, in
//! the test profile: it covers the calls made here, one per outcome, not
//! every input. The proof shows that these are all the outcomes.

// The only `unsafe` of the proof workspace: a global allocator cannot be
// written without it. It forwards every call to the system allocator.
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use mqttiumd_core::contract::capacity::{ExchangeSlots, PayloadBytes, Resource};
use mqttiumd_core::contract::ids::{AccountId, LeaseGeneration, LeaseKey};
use mqttiumd_core::credits::{
    Account, Credits, LeaseRecord, LeaseState, ReleaseError, ReleaseOutcome, ReserveError,
    release_if_current,
};

thread_local! {
    /// `Some(n)` while this thread counts: `n` allocations so far.
    static COUNT: Cell<Option<u64>> = const { Cell::new(None) };
}

struct Counting;

fn note() {
    // During the teardown of a thread the cell may be gone; nothing is
    // being counted then.
    let _ = COUNT.try_with(|count| {
        if let Some(seen) = count.get() {
            count.set(Some(seen + 1));
        }
    });
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note();
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        note();
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        note();
        unsafe { System.realloc(pointer, layout, size) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Runs `call` and returns its result with the number of allocations this
/// thread made meanwhile.
fn counted<T>(call: impl FnOnce() -> T) -> (T, u64) {
    COUNT.set(Some(0));
    let result = call();
    let seen = COUNT.take().expect("the count was started");
    (result, seen)
}

const ACCOUNT: AccountId = AccountId::new(7);

fn credits(bytes: u64, slots: u64) -> Credits {
    Credits::new(PayloadBytes::new(bytes), ExchangeSlots::new(slots))
}

fn key(account: AccountId, generation: u64) -> LeaseKey {
    LeaseKey {
        account,
        slot: 3,
        generation: LeaseGeneration::new(generation),
    }
}

fn lease(cost: Credits, state: LeaseState) -> LeaseRecord {
    LeaseRecord::from_parts(key(ACCOUNT, 5), cost, state)
}

fn account(id: AccountId, used: Credits) -> Account {
    Account::from_parts(id, used, credits(1000, 10))
}

#[test]
fn the_counter_sees_an_allocation() {
    let (boxed, seen) = counted(|| Box::new(1u8));
    assert_eq!(*boxed, 1);
    assert!(seen >= 1, "the counting allocator saw nothing");
}

#[test]
fn no_outcome_of_release_if_current_allocates() {
    let current = key(ACCOUNT, 5);
    // One call per outcome of the function, in the order of its guards.
    let cases = [
        (
            lease(credits(100, 2), LeaseState::Held),
            account(ACCOUNT, credits(250, 6)),
            key(ACCOUNT, 4),
            Ok(ReleaseOutcome::Stale),
        ),
        (
            lease(credits(100, 2), LeaseState::Held),
            account(AccountId::new(8), credits(250, 6)),
            current,
            Err(ReleaseError::WrongAccount),
        ),
        (
            lease(credits(100, 2), LeaseState::Released),
            account(ACCOUNT, credits(250, 6)),
            current,
            Ok(ReleaseOutcome::AlreadyReleased),
        ),
        (
            lease(credits(251, 2), LeaseState::Held),
            account(ACCOUNT, credits(250, 6)),
            current,
            Err(ReleaseError::CostExceedsUsage(Resource::PayloadBytes)),
        ),
        (
            lease(credits(100, 7), LeaseState::Held),
            account(ACCOUNT, credits(250, 6)),
            current,
            Err(ReleaseError::CostExceedsUsage(Resource::ExchangeSlots)),
        ),
        (
            lease(credits(100, 2), LeaseState::Held),
            account(ACCOUNT, credits(250, 6)),
            current,
            Ok(ReleaseOutcome::Released),
        ),
    ];
    for (mut record, mut charged, presented, expected) in cases {
        let (result, seen) = counted(|| release_if_current(&mut record, &mut charged, presented));
        assert_eq!(result, expected);
        assert_eq!(seen, 0, "{expected:?} allocated {seen} times");
    }
}

#[test]
fn no_outcome_of_reserve_allocates() {
    let mut charged = Account::new(ACCOUNT, credits(100, 2));
    let (held, seen) = counted(|| charged.reserve(1, LeaseGeneration::new(0), credits(60, 1)));
    assert!(held.is_ok());
    assert_eq!(seen, 0, "a reservation allocated {seen} times");

    let (refused, seen) = counted(|| charged.reserve(2, LeaseGeneration::new(0), credits(41, 1)));
    assert_eq!(
        refused,
        Err(ReserveError::LimitExceeded(Resource::PayloadBytes))
    );
    assert_eq!(seen, 0, "a refusal for the bytes allocated {seen} times");

    let (refused, seen) = counted(|| charged.reserve(2, LeaseGeneration::new(0), credits(40, 2)));
    assert_eq!(
        refused,
        Err(ReserveError::LimitExceeded(Resource::ExchangeSlots))
    );
    assert_eq!(seen, 0, "a refusal for the slots allocated {seen} times");
}
