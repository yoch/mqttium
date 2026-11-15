//! The lease table of one account: every credit reservation of an owner,
//! held exactly once (DEM-021, DEM-024, record 0010).
//!
//! A lease is charged by [`Account::reserve`] and given back by
//! [`release_if_current`], the function of the pilot proof, and by nothing
//! else: this module writes no second variant of either. The table adds
//! what those two leave to their caller: which slot a lease takes, a
//! reuse generation never used twice for a slot, and one record per held
//! lease.
//!
//! One record per held lease is an invariant of this table, not a check
//! of the function it calls: a slot holds at most one record, a record
//! leaves its slot when it is released, and the only place that rebuilds
//! a record from parts is [`LeaseTable::restore`], which refuses a slot
//! that already holds one.

// Interface only in this commit: the bodies below grant and check nothing
// yet, so some names are not used. Removed with the implementation.
#![allow(dead_code, unused_imports)]

use core::num::NonZeroU32;

use crate::contract::capacity::{AllocationFailed, BoundedVec, Resource};
use crate::contract::ids::{AccountId, LeaseGeneration, LeaseKey};
use crate::credits::{
    Account, Credits, LeaseRecord, LeaseState, ReleaseError, ReleaseOutcome, release_if_current,
};

/// Why a lease was not granted. Nothing changed.
///
/// The two definitive refusals name no wake-up and come first; the two
/// temporary ones end when a lease of this table is released.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LeaseRefused {
    /// The cost alone exceeds the limit of the account in this dimension.
    /// Definitive: no release makes room for it. Bytes are examined
    /// before slots.
    LargerThanAccount(Resource),
    /// No slot of the table can ever be taken again: each is spent, or
    /// holds a lease at its last generation. Definitive.
    TableExhausted,
    /// The account lacks `missing` units of this dimension. Temporary.
    /// Bytes are examined before slots.
    Limit { resource: Resource, missing: u64 },
    /// Every slot that is not spent holds a lease. Temporary.
    TableFull,
}

/// Why a recovered lease was not restored. Nothing changed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RestoreRefused {
    /// The key names another account than the one of this table.
    ForeignAccount,
    /// The key names no slot of this table.
    UnknownSlot,
    /// The slot already holds a lease: a second record of a held lease is
    /// never built.
    SlotHeld,
    /// The generation is older than one this slot already used.
    GenerationReused,
    /// The restored cost does not fit under the limit of the account.
    LimitExceeded(Resource),
}

/// One slot of the table.
#[derive(Debug)]
struct LeaseSlot {
    /// The generation the next lease of this slot takes, or `None` once
    /// the last one was used: the slot is spent.
    next_generation: Option<LeaseGeneration>,
    /// The one record of the lease this slot holds, if it holds one.
    record: Option<LeaseRecord>,
}

/// The leases of one account.
#[derive(Debug)]
pub struct LeaseTable {
    account: Account,
    slots: BoundedVec<LeaseSlot>,
}

impl LeaseTable {
    /// An empty table of exactly `max_leases` slots over an empty account
    /// with the given limits.
    pub fn new(
        account: AccountId,
        limit: Credits,
        max_leases: NonZeroU32,
    ) -> Result<Self, AllocationFailed> {
        let _ = (account, limit, max_leases);
        Err(AllocationFailed)
    }

    /// The account the leases of this table are charged to.
    #[must_use]
    pub fn account(&self) -> &Account {
        &self.account
    }

    /// Number of leases held.
    #[must_use]
    pub fn held(&self) -> usize {
        self.slots
            .as_slice()
            .iter()
            .filter(|slot| slot.record.is_some())
            .count()
    }

    /// Whether `key` names a lease this table holds now.
    #[must_use]
    pub fn holds(&self, key: LeaseKey) -> bool {
        let _ = key;
        false
    }

    /// Whether a lease of this cost would be granted, changing nothing.
    /// A plan asks this; its application then calls [`LeaseTable::reserve`],
    /// which cannot refuse what this accepted.
    pub fn check(&self, cost: Credits) -> Result<(), LeaseRefused> {
        let _ = cost;
        Ok(())
    }

    /// Charges `cost` to the account and holds the lease in a free slot,
    /// under a generation that slot never used. Nothing changes on a
    /// refusal.
    pub fn reserve(&mut self, cost: Credits) -> Result<LeaseKey, LeaseRefused> {
        let _ = cost;
        Err(LeaseRefused::TableExhausted)
    }

    /// Gives back the lease `key` names, through `release_if_current`.
    ///
    /// A key that names no held lease of this table (another account, no
    /// such slot, an empty slot, an older generation) is a late release:
    /// `Ok(Stale)`, nothing changes. On `Released` the record leaves its
    /// slot, which moves to its next generation.
    pub fn release(&mut self, key: LeaseKey) -> Result<ReleaseOutcome, ReleaseError> {
        let _ = key;
        Ok(ReleaseOutcome::Released)
    }

    /// Restores a held lease read from durable data, once per durable row.
    /// This is the one place of the core that rebuilds a record from its
    /// parts; it refuses a slot that already holds one.
    pub fn restore(&mut self, key: LeaseKey, cost: Credits) -> Result<(), RestoreRefused> {
        let _ = (key, cost);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::capacity::{ExchangeSlots, PayloadBytes};

    const ACCOUNT: u64 = 9;

    fn credits(bytes: u64, slots: u64) -> Credits {
        Credits::new(PayloadBytes::new(bytes), ExchangeSlots::new(slots))
    }

    fn table(bytes: u64, slots: u64, max_leases: u32) -> LeaseTable {
        LeaseTable::new(
            AccountId::new(ACCOUNT),
            credits(bytes, slots),
            NonZeroU32::new(max_leases).expect("at least one lease"),
        )
        .expect("small")
    }

    fn key(slot: u32, generation: u64) -> LeaseKey {
        LeaseKey {
            account: AccountId::new(ACCOUNT),
            slot,
            generation: LeaseGeneration::new(generation),
        }
    }

    /// A slot as a test compares it: its next generation and the key of
    /// the lease it holds.
    type SlotView = (Option<u64>, Option<LeaseKey>);

    /// Everything a refusal must leave unchanged.
    fn state(table: &LeaseTable) -> (Credits, Vec<SlotView>) {
        let slots = table
            .slots
            .as_slice()
            .iter()
            .map(|slot| {
                (
                    slot.next_generation.map(LeaseGeneration::get),
                    slot.record.as_ref().map(LeaseRecord::key),
                )
            })
            .collect();
        (table.account.used(), slots)
    }

    /// The usage of the account is the sum of the held leases, and every
    /// record is held, in its own slot, at a generation the slot left.
    fn assert_invariant(table: &LeaseTable) {
        let mut bytes = 0u64;
        let mut slots = 0u64;
        for (index, slot) in table.slots.as_slice().iter().enumerate() {
            let Some(record) = &slot.record else { continue };
            assert_eq!(record.state(), LeaseState::Held);
            assert_eq!(record.key().account, table.account.id());
            assert_eq!(record.key().slot as usize, index);
            if let Some(next) = slot.next_generation {
                assert!(record.key().generation < next, "generation not consumed");
            }
            bytes += record.cost().bytes.get();
            slots += record.cost().slots.get();
        }
        assert_eq!(table.account.used(), credits(bytes, slots));
        assert!(table.account.is_coherent());
    }

    #[test]
    fn a_lease_is_charged_once_and_released_once() {
        let mut table = table(100, 4, 2);
        let lease = table.reserve(credits(30, 1)).expect("room");
        assert_eq!(lease, key(0, 0));
        assert!(table.holds(lease));
        assert_eq!(table.held(), 1);
        assert_eq!(table.account().used(), credits(30, 1));
        assert_invariant(&table);

        assert_eq!(table.release(lease), Ok(ReleaseOutcome::Released));
        assert!(!table.holds(lease));
        assert_eq!(table.held(), 0);
        assert_eq!(table.account().used(), Credits::ZERO);
        assert_invariant(&table);

        // A second release of the same key subtracts nothing.
        let before = state(&table);
        assert_eq!(table.release(lease), Ok(ReleaseOutcome::Stale));
        assert_eq!(state(&table), before);
    }

    #[test]
    fn a_released_slot_is_reused_under_a_new_generation() {
        let mut table = table(100, 4, 1);
        let first = table.reserve(credits(10, 1)).expect("room");
        assert_eq!(table.release(first), Ok(ReleaseOutcome::Released));
        let second = table.reserve(credits(20, 2)).expect("room");
        assert_eq!(second, key(0, 1));
        assert_ne!(first, second);

        // The old key is a late release: it never touches the new lease.
        let before = state(&table);
        assert_eq!(table.release(first), Ok(ReleaseOutcome::Stale));
        assert_eq!(state(&table), before);
        assert!(table.holds(second));
        assert_invariant(&table);
    }

    // Record 0010: one record per held lease. No path builds a second
    // record of a lease that is held.
    #[test]
    fn no_path_builds_a_second_record_of_a_held_lease() {
        let mut table = table(100, 4, 2);
        let held = table.reserve(credits(30, 1)).expect("room");
        let before = state(&table);

        // Recovery presenting the row of a held lease again is refused.
        assert_eq!(
            table.restore(held, credits(30, 1)),
            Err(RestoreRefused::SlotHeld)
        );
        // So is any other generation of the slot that holds it.
        assert_eq!(
            table.restore(key(0, 7), credits(1, 1)),
            Err(RestoreRefused::SlotHeld)
        );
        assert_eq!(state(&table), before);

        // A reservation never takes the slot of a held lease.
        let other = table.reserve(credits(30, 1)).expect("room");
        assert_ne!(other.slot, held.slot);
        assert_eq!(table.held(), 2);
        assert_invariant(&table);

        // One release gives back one cost; the other lease is untouched.
        assert_eq!(table.release(held), Ok(ReleaseOutcome::Released));
        assert_eq!(table.account().used(), credits(30, 1));
        assert_eq!(table.release(held), Ok(ReleaseOutcome::Stale));
        assert_eq!(table.account().used(), credits(30, 1));
        assert!(table.holds(other));
        assert_invariant(&table);
    }

    // Record 0010: nothing but recovery rebuilds a record from its parts.
    // The source of this crate names `LeaseRecord::from_parts` in one
    // function outside `credits.rs`, and names no other release or
    // charge of an account.
    #[test]
    fn the_core_rebuilds_a_record_in_one_place_and_releases_through_one_function() {
        let source = std::fs::read_to_string("src/leases.rs").expect("the source of this module");
        let production = source
            .split("#[cfg(test)]")
            .next()
            .expect("the code before the tests");
        let code: Vec<&str> = production
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .collect();
        let count = |needle: &str| code.iter().filter(|line| line.contains(needle)).count();
        assert_eq!(count("LeaseRecord::from_parts("), 1, "rebuilt elsewhere");
        assert_eq!(count("release_if_current("), 1, "released elsewhere");
        assert_eq!(count(".reserve(slot"), 1, "charged elsewhere");

        // No other module of the core touches a record or an account.
        for entry in std::fs::read_dir("src").expect("the sources of this crate") {
            let path = entry.expect("a source file").path();
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("");
            if name == "leases.rs" || name == "credits.rs" {
                continue;
            }
            let other = std::fs::read_to_string(&path).expect("a source file");
            for needle in ["from_parts(", "release_if_current(", "Account::new("] {
                assert!(!other.contains(needle), "{name} uses {needle}");
            }
        }
    }

    #[test]
    fn a_refused_lease_changes_nothing() {
        let mut table = table(100, 2, 2);
        let _held = table.reserve(credits(60, 1)).expect("room");
        let before = state(&table);
        let cases = [
            // Definitive: the cost alone exceeds a limit.
            (
                credits(101, 1),
                LeaseRefused::LargerThanAccount(Resource::PayloadBytes),
            ),
            (
                credits(1, 3),
                LeaseRefused::LargerThanAccount(Resource::ExchangeSlots),
            ),
            (
                credits(101, 3),
                LeaseRefused::LargerThanAccount(Resource::PayloadBytes),
            ),
            // Temporary: what is held leaves too little.
            (
                credits(50, 1),
                LeaseRefused::Limit {
                    resource: Resource::PayloadBytes,
                    missing: 10,
                },
            ),
            (
                credits(40, 2),
                LeaseRefused::Limit {
                    resource: Resource::ExchangeSlots,
                    missing: 1,
                },
            ),
            (
                credits(50, 2),
                LeaseRefused::Limit {
                    resource: Resource::PayloadBytes,
                    missing: 10,
                },
            ),
        ];
        for (cost, expected) in cases {
            assert_eq!(table.check(cost), Err(expected), "{cost:?}");
            assert_eq!(table.reserve(cost), Err(expected), "{cost:?}");
            assert_eq!(state(&table), before, "{cost:?}");
        }
        // What `check` accepts, `reserve` grants.
        assert_eq!(table.check(credits(40, 1)), Ok(()));
        assert_eq!(state(&table), before, "check changed something");
        assert!(table.reserve(credits(40, 1)).is_ok());
        assert_invariant(&table);
    }

    #[test]
    fn a_full_table_is_a_wait_and_a_spent_table_is_not() {
        let mut table = table(100, 8, 2);
        let first = table.reserve(credits(1, 1)).expect("room");
        let second = table.reserve(credits(1, 1)).expect("room");
        let before = state(&table);
        assert_eq!(table.check(credits(1, 1)), Err(LeaseRefused::TableFull));
        assert_eq!(table.reserve(credits(1, 1)), Err(LeaseRefused::TableFull));
        assert_eq!(state(&table), before);

        // A release ends the wait.
        assert_eq!(table.release(first), Ok(ReleaseOutcome::Released));
        assert!(table.reserve(credits(1, 1)).is_ok());
        assert_eq!(table.release(second), Ok(ReleaseOutcome::Released));

        // Every slot at its last generation: the leases they hold are the
        // last ones, and the table is exhausted at once.
        let mut table = table_at_last_generation(2);
        let last = table.reserve(credits(1, 1)).expect("a generation is left");
        assert_eq!(last.generation, LeaseGeneration::new(u64::MAX));
        let other = table.reserve(credits(1, 1)).expect("a generation is left");
        assert_eq!(
            table.check(credits(1, 1)),
            Err(LeaseRefused::TableExhausted)
        );
        assert_eq!(
            table.reserve(credits(1, 1)),
            Err(LeaseRefused::TableExhausted)
        );
        assert_eq!(table.release(last), Ok(ReleaseOutcome::Released));
        assert_eq!(table.release(other), Ok(ReleaseOutcome::Released));
        assert_eq!(
            table.reserve(credits(1, 1)),
            Err(LeaseRefused::TableExhausted)
        );
        assert_eq!(table.account().used(), Credits::ZERO);
        // A definitive refusal is reported before a temporary one.
        assert_eq!(
            table.reserve(credits(1_000, 1)),
            Err(LeaseRefused::LargerThanAccount(Resource::PayloadBytes))
        );
    }

    fn table_at_last_generation(max_leases: u32) -> LeaseTable {
        let mut table = table(100, 8, max_leases);
        for slot in table.slots.as_mut_slice() {
            slot.next_generation = Some(LeaseGeneration::new(u64::MAX));
        }
        table
    }

    #[test]
    fn a_key_that_names_no_held_lease_is_a_late_release() {
        let mut table = table(100, 4, 2);
        let held = table.reserve(credits(10, 1)).expect("room");
        let before = state(&table);
        let foreign = LeaseKey {
            account: AccountId::new(ACCOUNT + 1),
            ..held
        };
        for late in [foreign, key(1, 0), key(99, 0), key(0, 5)] {
            assert!(!table.holds(late), "{late:?}");
            assert_eq!(table.release(late), Ok(ReleaseOutcome::Stale), "{late:?}");
            assert_eq!(state(&table), before, "{late:?}");
        }
        assert!(table.holds(held));
    }

    #[test]
    fn recovery_restores_each_held_lease_once() {
        let mut table = table(100, 4, 3);
        assert_eq!(table.restore(key(1, 5), credits(40, 1)), Ok(()));
        assert!(table.holds(key(1, 5)));
        assert_eq!(table.account().used(), credits(40, 1));
        assert_invariant(&table);

        // The restored lease is released like any other, and its slot is
        // then reused under a generation the durable row never had.
        assert_eq!(table.release(key(1, 5)), Ok(ReleaseOutcome::Released));
        assert_eq!(table.account().used(), Credits::ZERO);
        let fresh = table.reserve(credits(1, 1)).expect("room");
        assert_eq!(fresh, key(0, 0));
        let reused = table.reserve(credits(1, 1)).expect("room");
        assert_eq!(reused, key(1, 6));
        assert_invariant(&table);
    }

    #[test]
    fn a_refused_restoration_changes_nothing() {
        let mut table = table(100, 2, 2);
        assert_eq!(table.restore(key(0, 3), credits(60, 1)), Ok(()));
        assert_eq!(table.release(key(0, 3)), Ok(ReleaseOutcome::Released));
        assert_eq!(table.restore(key(1, 0), credits(60, 1)), Ok(()));
        let before = state(&table);
        let foreign = LeaseKey {
            account: AccountId::new(ACCOUNT + 1),
            ..key(0, 9)
        };
        let cases = [
            (foreign, credits(1, 1), RestoreRefused::ForeignAccount),
            (key(2, 0), credits(1, 1), RestoreRefused::UnknownSlot),
            (key(1, 1), credits(1, 1), RestoreRefused::SlotHeld),
            // Slot 0 already used generation 3.
            (key(0, 3), credits(1, 1), RestoreRefused::GenerationReused),
            (key(0, 2), credits(1, 1), RestoreRefused::GenerationReused),
            (
                key(0, 4),
                credits(41, 1),
                RestoreRefused::LimitExceeded(Resource::PayloadBytes),
            ),
            (
                key(0, 4),
                credits(1, 2),
                RestoreRefused::LimitExceeded(Resource::ExchangeSlots),
            ),
        ];
        for (key, cost, expected) in cases {
            assert_eq!(table.restore(key, cost), Err(expected), "{key:?}");
            assert_eq!(state(&table), before, "{key:?}");
        }
        assert_eq!(table.restore(key(0, 4), credits(40, 1)), Ok(()));
        assert_invariant(&table);
    }

    #[test]
    fn a_lease_restored_at_the_last_generation_spends_its_slot() {
        let mut table = table(100, 4, 1);
        assert_eq!(table.restore(key(0, u64::MAX), credits(1, 1)), Ok(()));
        assert_eq!(
            table.reserve(credits(1, 1)),
            Err(LeaseRefused::TableExhausted)
        );
        assert_eq!(
            table.release(key(0, u64::MAX)),
            Ok(ReleaseOutcome::Released)
        );
        assert_eq!(
            table.reserve(credits(1, 1)),
            Err(LeaseRefused::TableExhausted)
        );
        assert_eq!(table.account().used(), Credits::ZERO);
    }
}
