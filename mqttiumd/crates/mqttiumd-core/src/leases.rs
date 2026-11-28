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
//! leaves its slot when it is released, and [`LeaseTable::restore`], the
//! one place that adds a lease read from durable data, refuses a slot that
//! already holds one. It charges that lease through [`Account::reserve`]
//! as well, so no record is rebuilt from its parts and no account is
//! charged by hand.

use alloc::vec::Vec;
use core::num::NonZeroU32;

use crate::contract::capacity::{AllocationFailed, BoundedVec, Resource};
use crate::contract::ids::{AccountId, LeaseGeneration, LeaseKey};
use crate::credits::{
    Account, Credits, LeaseRecord, ReleaseError, ReleaseOutcome, release_if_current,
};

/// Two-level O(1) deterministic bitset of free slot indices.
///
/// Replaces a 32,768-node `BTreeSet<usize>` (~1 MiB) with 4 KiB of bit words
/// plus a 64-byte summary line, preserving exact lowest-free-slot selection.
#[derive(Debug)]
struct FreeBitset {
    words: Vec<u64>,
    summary: Vec<u64>,
}

impl FreeBitset {
    fn all_free(count: usize) -> Result<Self, AllocationFailed> {
        let word_count = count.div_ceil(64);
        let summary_count = word_count.div_ceil(64);
        let mut words = Vec::new();
        words
            .try_reserve_exact(word_count)
            .map_err(|_| AllocationFailed)?;
        words.resize(word_count, u64::MAX);
        let rem = count & 63;
        if rem != 0
            && let Some(last) = words.last_mut()
        {
            *last = (1u64 << rem) - 1;
        }
        let mut summary = Vec::new();
        summary
            .try_reserve_exact(summary_count)
            .map_err(|_| AllocationFailed)?;
        summary.resize(summary_count, u64::MAX);
        let srem = word_count & 63;
        if srem != 0
            && let Some(last) = summary.last_mut()
        {
            *last = (1u64 << srem) - 1;
        }
        Ok(Self { words, summary })
    }

    fn first(&self) -> Option<usize> {
        for (si, &sw) in self.summary.iter().enumerate() {
            if sw != 0 {
                let wi = (si << 6) + (sw.trailing_zeros() as usize);
                let w = self.words[wi];
                return Some((wi << 6) + (w.trailing_zeros() as usize));
            }
        }
        None
    }

    fn set(&mut self, index: usize) {
        let wi = index >> 6;
        let bit = 1u64 << (index & 63);
        let was_zero = self.words[wi] == 0;
        self.words[wi] |= bit;
        if was_zero {
            self.summary[wi >> 6] |= 1u64 << (wi & 63);
        }
    }

    fn clear(&mut self, index: usize) {
        let wi = index >> 6;
        let bit = 1u64 << (index & 63);
        self.words[wi] &= !bit;
        if self.words[wi] == 0 {
            self.summary[wi >> 6] &= !(1u64 << (wi & 63));
        }
    }
}

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
    free_slots: FreeBitset,
    held_count: usize,
    exhausted_count: usize,
}

impl LeaseTable {
    /// An empty table of exactly `max_leases` slots over an empty account
    /// with the given limits.
    pub fn new(
        account: AccountId,
        limit: Credits,
        max_leases: NonZeroU32,
    ) -> Result<Self, AllocationFailed> {
        let count = usize::try_from(max_leases.get()).map_err(|_| AllocationFailed)?;
        // Pre-reserve exact capacity without touching backing pages at startup;
        // slots materialize lazily inside the reserved buffer on first use.
        let slots = BoundedVec::with_capacity(count)?;
        let free_slots = FreeBitset::all_free(count)?;
        Ok(Self {
            account: Account::new(account, limit),
            slots,
            free_slots,
            held_count: 0,
            exhausted_count: 0,
        })
    }

    fn ensure_slot(&mut self, index: usize) -> &mut LeaseSlot {
        while self.slots.len() <= index {
            let _ = self.slots.try_push(LeaseSlot {
                next_generation: Some(LeaseGeneration::new(0)),
                record: None,
            });
        }
        &mut self.slots.as_mut_slice()[index]
    }

    /// The slot `key` names when it holds exactly that lease: the whole
    /// key is compared, account and generation included.
    fn locate(&self, key: LeaseKey) -> Option<usize> {
        let index = usize::try_from(key.slot).ok()?;
        let record = self.slots.as_slice().get(index)?.record.as_ref()?;
        (record.key() == key).then_some(index)
    }

    /// The slot the next lease takes, or why there is none.
    fn free_slot(&self) -> Result<usize, LeaseRefused> {
        if let Some(index) = self.free_slots.first() {
            return Ok(index);
        }
        // A held slot is freed by a release unless its lease took the
        // last generation: that slot is spent already.
        if self.exhausted_count < self.slots.capacity() {
            Err(LeaseRefused::TableFull)
        } else {
            Err(LeaseRefused::TableExhausted)
        }
    }

    /// What the account lacks for `cost`, bytes before slots, comparing
    /// against `ceiling`: the limit, or what the limit leaves free.
    fn shortfall(cost: Credits, ceiling: Credits) -> Option<(Resource, u64)> {
        if cost.bytes > ceiling.bytes {
            return Some((
                Resource::PayloadBytes,
                cost.bytes.get() - ceiling.bytes.get(),
            ));
        }
        if cost.slots > ceiling.slots {
            return Some((
                Resource::ExchangeSlots,
                cost.slots.get() - ceiling.slots.get(),
            ));
        }
        None
    }

    /// What the limit leaves free. A coherent account never underflows;
    /// an incoherent one leaves nothing.
    fn free(&self) -> Credits {
        let (used, limit) = (self.account.used(), self.account.limit());
        Credits::new(
            limit.bytes.checked_sub(used.bytes).unwrap_or_default(),
            limit.slots.checked_sub(used.slots).unwrap_or_default(),
        )
    }

    /// The account the leases of this table are charged to.
    #[must_use]
    pub fn account(&self) -> &Account {
        &self.account
    }

    /// Number of leases held.
    #[must_use]
    pub fn held(&self) -> usize {
        self.held_count
    }

    /// Whether `key` names a lease this table holds now.
    #[must_use]
    pub fn holds(&self, key: LeaseKey) -> bool {
        self.locate(key).is_some()
    }

    /// Whether a lease of this cost would be granted, changing nothing.
    /// A plan asks this; its application then calls [`LeaseTable::reserve`],
    /// which cannot refuse what this accepted.
    pub fn check(&self, cost: Credits) -> Result<(), LeaseRefused> {
        self.admit(cost).map(|_| ())
    }

    /// The decisions of `check` and `reserve`, in the order the refusals
    /// are reported: the definitive ones first.
    fn admit(&self, cost: Credits) -> Result<usize, LeaseRefused> {
        if let Some((resource, _)) = Self::shortfall(cost, self.account.limit()) {
            return Err(LeaseRefused::LargerThanAccount(resource));
        }
        let slot = self.free_slot();
        if slot == Err(LeaseRefused::TableExhausted) {
            return slot;
        }
        if let Some((resource, missing)) = Self::shortfall(cost, self.free()) {
            return Err(LeaseRefused::Limit { resource, missing });
        }
        slot
    }

    /// Charges `cost` to the account and holds the lease in a free slot,
    /// under a generation that slot never used. Nothing changes on a
    /// refusal.
    pub fn reserve(&mut self, cost: Credits) -> Result<LeaseKey, LeaseRefused> {
        let index = self.admit(cost)?;
        // `admit` found the slot free and its generation not exhausted.
        let slot = u32::try_from(index).map_err(|_| LeaseRefused::TableExhausted)?;
        let generation = match self.slots.as_slice().get(index) {
            Some(entry) => entry.next_generation.ok_or(LeaseRefused::TableExhausted)?,
            None => LeaseGeneration::new(0),
        };
        let record = match self.account.reserve(slot, generation, cost) {
            Ok(record) => record,
            // `admit` compared the same limits; the account is the authority.
            Err(crate::credits::ReserveError::LimitExceeded(resource)) => {
                return Err(LeaseRefused::Limit {
                    resource,
                    missing: 0,
                });
            }
        };
        let key = record.key();
        let entry = self.ensure_slot(index);
        entry.record = Some(record);
        // The generation is used whatever follows; the last leaves `None`.
        entry.next_generation = generation.checked_next().ok();
        let spent = entry.next_generation.is_none();
        self.free_slots.clear(index);
        self.held_count += 1;
        if spent {
            self.exhausted_count += 1;
        }
        Ok(key)
    }

    /// Gives back the lease `key` names, through `release_if_current`.
    ///
    /// A key that names no held lease of this table (another account, no
    /// such slot, an empty slot, an older generation) is a late release:
    /// `Ok(Stale)`, nothing changes. On `Released` the record leaves its
    /// slot, which moves to its next generation.
    pub fn release(&mut self, key: LeaseKey) -> Result<ReleaseOutcome, ReleaseError> {
        let Some(index) = self.locate(key) else {
            return Ok(ReleaseOutcome::Stale);
        };
        let entry = &mut self.slots.as_mut_slice()[index];
        let Some(record) = entry.record.as_mut() else {
            return Ok(ReleaseOutcome::Stale);
        };
        let outcome = release_if_current(record, &mut self.account, key)?;
        if outcome == ReleaseOutcome::Released {
            // The one record of the lease leaves the table with it.
            entry.record = None;
            self.held_count -= 1;
            if entry.next_generation.is_some() {
                self.free_slots.set(index);
            }
        }
        Ok(outcome)
    }

    /// Replaces the lease `key` names with a lease of `cost` held in the
    /// same slot under the next generation of that slot, and returns the
    /// key of the lease the slot then holds. The old lease leaves through
    /// [`LeaseTable::release`] and the new one is charged as a restored
    /// lease is: the one release path and the one charge path (record
    /// 0010).
    ///
    /// A lease only gets lighter. A `cost` equal to what the lease holds,
    /// or larger in a dimension, changes nothing and returns `key`; so does
    /// a slot whose generations are spent, whose lease stays as it is
    /// rather than leave the table. `None` when `key` names no lease this
    /// table holds: nothing changed.
    pub fn lighten(&mut self, key: LeaseKey, cost: Credits) -> Option<LeaseKey> {
        let index = self.locate(key)?;
        let entry = &self.slots.as_slice()[index];
        let held = entry.record.as_ref()?.cost();
        if cost == held || cost.bytes > held.bytes || cost.slots > held.slots {
            return Some(key);
        }
        let Some(generation) = entry.next_generation else {
            return Some(key);
        };
        if self.release(key) != Ok(ReleaseOutcome::Released) {
            return None;
        }
        // The slot is free at `generation`, and the account was just
        // released of more than it is charged now.
        let lighter = LeaseKey { generation, ..key };
        self.restore(lighter, cost).ok()?;
        Some(lighter)
    }

    /// Restores a held lease read from durable data, once per durable row.
    /// It refuses a slot that already holds one, and charges the account
    /// through `Account::reserve`, under the generation of the row.
    pub fn restore(&mut self, key: LeaseKey, cost: Credits) -> Result<(), RestoreRefused> {
        if key.account != self.account.id() {
            return Err(RestoreRefused::ForeignAccount);
        }
        let index = usize::try_from(key.slot).map_err(|_| RestoreRefused::UnknownSlot)?;
        if index >= self.slots.capacity() {
            return Err(RestoreRefused::UnknownSlot);
        }
        if let Some(entry) = self.slots.as_slice().get(index) {
            if entry.record.is_some() {
                return Err(RestoreRefused::SlotHeld);
            }
            // A spent slot used every generation.
            if entry
                .next_generation
                .is_none_or(|next| key.generation < next)
            {
                return Err(RestoreRefused::GenerationReused);
            }
        }
        // Recovery charges the restored lease like a new one, through the
        // one charge path, under the generation the durable row names.
        let record = match self.account.reserve(key.slot, key.generation, cost) {
            Ok(record) => record,
            Err(crate::credits::ReserveError::LimitExceeded(resource)) => {
                return Err(RestoreRefused::LimitExceeded(resource));
            }
        };
        let entry = self.ensure_slot(index);
        entry.record = Some(record);
        entry.next_generation = key.generation.checked_next().ok();
        let spent = entry.next_generation.is_none();
        self.free_slots.clear(index);
        self.held_count += 1;
        if spent {
            self.exhausted_count += 1;
        }
        Ok(())
    }
}

/// What one slot of a table shows: the generation its next lease takes,
/// or `None` once it is spent, and the lease it holds.
pub type LeaseSlotView = (Option<u64>, Option<LeaseKey>);

/// For the tests of the crate: what a refusal must leave unchanged.
#[cfg(test)]
mod snapshot {
    use super::*;

    impl LeaseTable {
        /// The usage of the account and the view of each slot.
        pub(crate) fn snapshot(&self) -> (Credits, Vec<LeaseSlotView>) {
            let mut slots: Vec<LeaseSlotView> = self
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
            slots.resize(self.slots.capacity(), (Some(0), None));
            (self.account.used(), slots)
        }
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

    /// The usage of the account is the sum of the held leases, and every
    /// record is held, in its own slot, at a generation the slot left.
    fn assert_invariant(table: &LeaseTable) {
        let mut bytes = 0u64;
        let mut slots = 0u64;
        for (index, slot) in table.slots.as_slice().iter().enumerate() {
            let Some(record) = &slot.record else { continue };
            assert_eq!(record.state(), crate::credits::LeaseState::Held);
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
        let before = table.snapshot();
        assert_eq!(table.release(lease), Ok(ReleaseOutcome::Stale));
        assert_eq!(table.snapshot(), before);
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
        let before = table.snapshot();
        assert_eq!(table.release(first), Ok(ReleaseOutcome::Stale));
        assert_eq!(table.snapshot(), before);
        assert!(table.holds(second));
        assert_invariant(&table);
    }

    // Record 0010: one record per held lease. No path builds a second
    // record of a lease that is held.
    #[test]
    fn no_path_builds_a_second_record_of_a_held_lease() {
        let mut table = table(100, 4, 2);
        let held = table.reserve(credits(30, 1)).expect("room");
        let before = table.snapshot();

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
        assert_eq!(table.snapshot(), before);

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

    // Record 0010: one charge path and one release path. The source of
    // this crate rebuilds no record from its parts, charges an account in
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
        // Recovery charges a restored lease through `Account::reserve`,
        // like a new one: there is one charge path, the one the proof
        // covers, and no record is rebuilt from its parts.
        assert_eq!(count("from_parts("), 0, "rebuilt or charged by hand");
        assert_eq!(count("release_if_current("), 1, "released elsewhere");
        assert_eq!(count("self.account.reserve("), 2, "charged elsewhere");

        // No other module of the core touches a record or an account.
        let mut pending = vec![std::path::PathBuf::from("src")];
        while let Some(path) = pending.pop() {
            if path.is_dir() {
                for entry in std::fs::read_dir(path).expect("a source directory") {
                    pending.push(entry.expect("a source entry").path());
                }
                continue;
            }
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
        let before = table.snapshot();
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
            assert_eq!(table.snapshot(), before, "{cost:?}");
        }
        // What `check` accepts, `reserve` grants.
        assert_eq!(table.check(credits(40, 1)), Ok(()));
        assert_eq!(table.snapshot(), before, "check changed something");
        assert!(table.reserve(credits(40, 1)).is_ok());
        assert_invariant(&table);
    }

    #[test]
    fn a_full_table_is_a_wait_and_a_spent_table_is_not() {
        let mut table = table(100, 8, 2);
        let first = table.reserve(credits(1, 1)).expect("room");
        let second = table.reserve(credits(1, 1)).expect("room");
        let before = table.snapshot();
        assert_eq!(table.check(credits(1, 1)), Err(LeaseRefused::TableFull));
        assert_eq!(table.reserve(credits(1, 1)), Err(LeaseRefused::TableFull));
        assert_eq!(table.snapshot(), before);

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

    // Control mutation L7: an exhausted table is reported even when the
    // account also lacks room, since no release ends the exhaustion.
    #[test]
    fn an_exhausted_table_is_reported_before_a_lack_of_credits() {
        let mut table = table_at_last_generation(2);
        let last = table.reserve(credits(60, 1)).expect("a generation is left");
        let _other = table.reserve(credits(30, 1)).expect("a generation is left");
        assert_eq!(table.release(last), Ok(ReleaseOutcome::Released));
        // Thirty bytes are in use: fifty more would be a temporary lack on
        // a table with a slot left.
        let before = table.snapshot();
        assert_eq!(
            table.check(credits(80, 1)),
            Err(LeaseRefused::TableExhausted)
        );
        assert_eq!(
            table.reserve(credits(80, 1)),
            Err(LeaseRefused::TableExhausted)
        );
        assert_eq!(table.snapshot(), before);
    }

    fn table_at_last_generation(max_leases: u32) -> LeaseTable {
        let mut table = table(100, 8, max_leases);
        for index in 0..max_leases as usize {
            table.ensure_slot(index).next_generation = Some(LeaseGeneration::new(u64::MAX));
        }
        table
    }

    #[test]
    fn a_key_that_names_no_held_lease_is_a_late_release() {
        let mut table = table(100, 4, 2);
        let held = table.reserve(credits(10, 1)).expect("room");
        let before = table.snapshot();
        let foreign = LeaseKey {
            account: AccountId::new(ACCOUNT + 1),
            ..held
        };
        for late in [foreign, key(1, 0), key(99, 0), key(0, 5)] {
            assert!(!table.holds(late), "{late:?}");
            assert_eq!(table.release(late), Ok(ReleaseOutcome::Stale), "{late:?}");
            assert_eq!(table.snapshot(), before, "{late:?}");
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
        let before = table.snapshot();
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
            assert_eq!(table.snapshot(), before, "{key:?}");
        }
        assert_eq!(table.restore(key(0, 4), credits(40, 1)), Ok(()));
        assert_invariant(&table);
    }

    // Control mutation L5: a spent slot used every generation, so no
    // durable row can name one it did not use.
    #[test]
    fn a_spent_slot_is_never_restored() {
        let mut table = table(100, 4, 1);
        assert_eq!(table.restore(key(0, u64::MAX), credits(1, 1)), Ok(()));
        assert_eq!(
            table.release(key(0, u64::MAX)),
            Ok(ReleaseOutcome::Released)
        );
        let before = table.snapshot();
        for generation in [0, 7, u64::MAX] {
            assert_eq!(
                table.restore(key(0, generation), credits(1, 1)),
                Err(RestoreRefused::GenerationReused),
                "generation {generation}"
            );
            assert_eq!(table.snapshot(), before);
        }
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

    #[test]
    fn a_lightened_lease_returns_its_bytes_and_keeps_its_slot() {
        let mut table = table(10, 2, 2);
        let heavy = table.reserve(credits(10, 1)).expect("room");
        assert_eq!(
            table.check(credits(1, 1)),
            Err(LeaseRefused::Limit {
                resource: Resource::PayloadBytes,
                missing: 1
            })
        );
        let light = table.lighten(heavy, credits(0, 1)).expect("held");
        assert_eq!(light, key(heavy.slot, 1), "the next generation of the slot");
        assert!(table.holds(light) && !table.holds(heavy));
        assert_eq!(table.held(), 1);
        assert_eq!(table.account().used(), credits(0, 1));
        assert_invariant(&table);
        // The old key is a late release; the bytes serve another lease.
        assert_eq!(table.release(heavy), Ok(ReleaseOutcome::Stale));
        let other = table.reserve(credits(10, 1)).expect("the bytes are back");
        assert_ne!(other.slot, light.slot);
        assert_eq!(table.release(light), Ok(ReleaseOutcome::Released));
        assert_eq!(table.account().used(), credits(10, 1));
        assert_invariant(&table);
    }

    #[test]
    fn lightening_never_grows_a_lease_and_changes_nothing_it_cannot_do() {
        let mut table = table(10, 4, 2);
        let lease = table.reserve(credits(4, 1)).expect("room");
        let before = table.snapshot();
        // The same cost, more bytes, more slots: the lease stays.
        for cost in [credits(4, 1), credits(5, 1), credits(0, 2)] {
            assert_eq!(table.lighten(lease, cost), Some(lease), "{cost:?}");
            assert_eq!(table.snapshot(), before);
        }
        // A key that names no held lease.
        assert_eq!(table.lighten(key(1, 0), credits(0, 1)), None);
        assert_eq!(table.lighten(key(0, 1), credits(0, 1)), None);
        assert_eq!(table.snapshot(), before);

        // A slot at its last generation keeps the lease it holds.
        let mut table = table_at_last_generation(1);
        let last = table.reserve(credits(4, 1)).expect("a generation is left");
        let before = table.snapshot();
        assert_eq!(table.lighten(last, credits(0, 1)), Some(last));
        assert_eq!(table.snapshot(), before);

        // The generation before the last: the lighter lease spends the slot.
        let mut table = table_at_last_generation(1);
        table.slots.as_mut_slice()[0].next_generation = Some(LeaseGeneration::new(u64::MAX - 1));
        let lease = table.reserve(credits(4, 1)).expect("a generation is left");
        let light = table.lighten(lease, credits(0, 1)).expect("held");
        assert_eq!(light.generation, LeaseGeneration::new(u64::MAX));
        assert_invariant(&table);
        assert_eq!(table.release(light), Ok(ReleaseOutcome::Released));
        assert_eq!(
            table.reserve(credits(1, 1)),
            Err(LeaseRefused::TableExhausted)
        );
    }

    #[test]
    fn lazy_slots_initialize_on_demand_without_changing_refusals() {
        let mut table = table(100, 8, 64);
        assert_eq!(table.slots.len(), 0);
        assert_eq!(table.slots.capacity(), 64);
        assert_eq!(
            table.restore(key(10, 0), credits(101, 1)),
            Err(RestoreRefused::LimitExceeded(Resource::PayloadBytes))
        );
        assert_eq!(
            table.slots.len(),
            0,
            "refused restore must not materialize virgin slots"
        );
        let first = table.reserve(credits(10, 1)).expect("room");
        assert_eq!(first, key(0, 0));
        assert_eq!(table.slots.len(), 1);
        assert_eq!(table.restore(key(5, 2), credits(10, 1)), Ok(()));
        assert_eq!(table.slots.len(), 6);
        assert_invariant(&table);
    }
}
