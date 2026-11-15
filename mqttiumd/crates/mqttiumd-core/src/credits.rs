//! Credit accounts, leases and their exact release (DEM-021, RES-001).
//!
//! An [`Account`] counts what is in use, in two dimensions: payload bytes
//! and exchange slots. A [`LeaseRecord`] is one reservation charged to an
//! account: its full identity (account, slot, reuse generation), its cost
//! and whether it is still held. [`release_if_current`] is the only way a
//! lease gives its cost back. It is the function of the pilot proof: the
//! proof under `proofs/credits/` is on this body, not on a copy.
//!
//! Nothing here allocates or does IO.

use crate::contract::capacity::{ExchangeSlots, PayloadBytes, Resource};
use crate::contract::ids::{AccountId, LeaseGeneration, LeaseKey};

/// An amount in the two dimensions an account counts.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Credits {
    pub bytes: PayloadBytes,
    pub slots: ExchangeSlots,
}

impl Credits {
    /// Nothing in either dimension.
    pub const ZERO: Self = Self {
        bytes: PayloadBytes::ZERO,
        slots: ExchangeSlots::ZERO,
    };

    /// Builds the amount from its two dimensions.
    #[must_use]
    pub const fn new(bytes: PayloadBytes, slots: ExchangeSlots) -> Self {
        Self { bytes, slots }
    }
}

/// Whether a lease still holds its cost.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LeaseState {
    /// The cost is counted in the account's usage.
    Held,
    /// The cost was given back, once.
    Released,
}

/// One reservation: its identity, its cost and its state.
///
/// **One record per held lease.** An account counts usage; it does not
/// know its leases. [`release_if_current`] therefore cannot tell a second
/// record of a held lease from the first: releasing both subtracts the
/// cost twice, and the account then admits more than its limit. Whoever
/// owns the lease table holds each record exactly once; that is a
/// precondition of the function, not something it checks.
///
/// The record is neither `Clone` nor `Copy` for that reason.
/// [`LeaseRecord::from_parts`] can still build a second record of the same
/// lease, and must not be used to.
#[derive(PartialEq, Eq, Debug)]
pub struct LeaseRecord {
    key: LeaseKey,
    cost: Credits,
    state: LeaseState,
}

impl LeaseRecord {
    /// Rebuilds a record from its parts, as recovery reads them.
    ///
    /// Two callers have a reason to use it: recovery, which rebuilds each
    /// record once from the durable row that holds it, and the proof
    /// crate, which must state every record, the incoherent ones included.
    /// A transition never calls it: it gets a held lease from
    /// [`Account::reserve`], which charges the account. Building a second
    /// record of a lease that is already held breaks the precondition of
    /// [`release_if_current`].
    ///
    /// Nothing is checked against any account: [`release_if_current`] does
    /// not rely on the record and the account agreeing, and refuses without
    /// mutation when they do not.
    #[must_use]
    pub const fn from_parts(key: LeaseKey, cost: Credits, state: LeaseState) -> Self {
        Self { key, cost, state }
    }

    /// The identity of this reservation: account, slot and generation.
    #[must_use]
    pub const fn key(&self) -> LeaseKey {
        self.key
    }

    /// What the reservation costs in each dimension.
    #[must_use]
    pub const fn cost(&self) -> Credits {
        self.cost
    }

    /// Whether the cost is still held.
    #[must_use]
    pub const fn state(&self) -> LeaseState {
        self.state
    }
}

/// A capacity account: usage and limit in each dimension.
///
/// An account is coherent when its usage does not exceed its limit in
/// either dimension. [`Account::new`] and [`Account::reserve`] keep it so;
/// [`Account::from_parts`] can express an incoherent one.
#[derive(PartialEq, Eq, Debug)]
pub struct Account {
    id: AccountId,
    used: Credits,
    limit: Credits,
}

/// A reservation was refused; nothing changed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReserveError {
    /// The cost does not fit under the limit of this dimension. Bytes are
    /// examined before slots.
    LimitExceeded(Resource),
}

impl Account {
    /// An empty account with the given limits.
    #[must_use]
    pub const fn new(id: AccountId, limit: Credits) -> Self {
        Self {
            id,
            used: Credits::ZERO,
            limit,
        }
    }

    /// Rebuilds an account from its parts, as recovery reads them.
    ///
    /// Two callers have a reason to use it: recovery, and the proof crate,
    /// which must state the accounts whose usage exceeds a limit. A
    /// transition never calls it: an account starts with [`Account::new`]
    /// and changes through [`Account::reserve`] and [`release_if_current`].
    ///
    /// Coherence is not checked: see [`Account::is_coherent`]. An
    /// incoherent account is not refused by [`release_if_current`]: a
    /// release from it is exact or refused whole, and the proof covers
    /// both.
    #[must_use]
    pub const fn from_parts(id: AccountId, used: Credits, limit: Credits) -> Self {
        Self { id, used, limit }
    }

    /// The identity of the account.
    #[must_use]
    pub const fn id(&self) -> AccountId {
        self.id
    }

    /// What is in use.
    #[must_use]
    pub const fn used(&self) -> Credits {
        self.used
    }

    /// The limits.
    #[must_use]
    pub const fn limit(&self) -> Credits {
        self.limit
    }

    /// Whether usage is within the limit in both dimensions.
    #[must_use]
    pub const fn is_coherent(&self) -> bool {
        self.used.bytes.get() <= self.limit.bytes.get()
            && self.used.slots.get() <= self.limit.slots.get()
    }

    /// Charges `cost` to this account and returns the held lease, named by
    /// this account, `slot` and `generation`.
    ///
    /// Both dimensions are checked before any mutation: a refusal changes
    /// nothing. The caller owns the slot table and supplies a generation it
    /// never used for this slot before.
    pub fn reserve(
        &mut self,
        slot: u32,
        generation: LeaseGeneration,
        cost: Credits,
    ) -> Result<LeaseRecord, ReserveError> {
        let bytes = match self.used.bytes.checked_add(cost.bytes) {
            Some(bytes) if bytes.get() <= self.limit.bytes.get() => bytes,
            _ => return Err(ReserveError::LimitExceeded(Resource::PayloadBytes)),
        };
        let slots = match self.used.slots.checked_add(cost.slots) {
            Some(slots) if slots.get() <= self.limit.slots.get() => slots,
            _ => return Err(ReserveError::LimitExceeded(Resource::ExchangeSlots)),
        };
        self.used = Credits { bytes, slots };
        Ok(LeaseRecord {
            key: LeaseKey {
                account: self.id,
                slot,
                generation,
            },
            cost,
            state: LeaseState::Held,
        })
    }
}

/// What a call to [`release_if_current`] did when it found no fault.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReleaseOutcome {
    /// The cost was subtracted from the account, exactly, in both
    /// dimensions, and the lease is now `Released`.
    Released,
    /// The key does not name this record: another slot, another account or
    /// another generation. Nothing changed.
    Stale,
    /// The key is current and the lease was already released. Nothing
    /// changed: there is no second subtraction.
    AlreadyReleased,
}

/// A fault found by [`release_if_current`]; nothing changed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReleaseError {
    /// The key names this record, and the account presented is not the one
    /// the lease is charged to.
    WrongAccount,
    /// The held cost exceeds what the account has in use in this
    /// dimension. Bytes are examined before slots; neither dimension
    /// changed.
    CostExceedsUsage(Resource),
}

/// Releases `lease` from `account` if `key` is its current key.
///
/// | Condition | Result |
/// | - | - |
/// | `key` is not the key of `lease` | `Ok(Stale)`, no mutation |
/// | `account` is not the account of `lease` | `Err(WrongAccount)`, no mutation |
/// | `lease` is already released | `Ok(AlreadyReleased)`, no mutation |
/// | The cost exceeds a usage | `Err(CostExceedsUsage)`, no mutation |
/// | Otherwise | Both usages reduced by the exact cost, `lease` released, `Ok(Released)` |
///
/// Precondition: `lease` is the only record of its reservation (see
/// [`LeaseRecord`]). Nothing else is required of the arguments: an
/// incoherent account, or a held lease its account never counted, is
/// released exactly or refused whole.
///
/// The conditions are examined in this order. A stale key is examined
/// first: after a slot is reused, the old key may name another account
/// than the new reservation, and that is a late release, not a fault.
/// Both subtractions are computed and checked before either usage or the
/// state is written.
pub fn release_if_current(
    lease: &mut LeaseRecord,
    account: &mut Account,
    key: LeaseKey,
) -> Result<ReleaseOutcome, ReleaseError> {
    if key != lease.key {
        return Ok(ReleaseOutcome::Stale);
    }
    if account.id != lease.key.account {
        return Err(ReleaseError::WrongAccount);
    }
    if lease.state == LeaseState::Released {
        return Ok(ReleaseOutcome::AlreadyReleased);
    }
    let Some(bytes) = account.used.bytes.checked_sub(lease.cost.bytes) else {
        return Err(ReleaseError::CostExceedsUsage(Resource::PayloadBytes));
    };
    let Some(slots) = account.used.slots.checked_sub(lease.cost.slots) else {
        return Err(ReleaseError::CostExceedsUsage(Resource::ExchangeSlots));
    };
    account.used = Credits { bytes, slots };
    lease.state = LeaseState::Released;
    Ok(ReleaseOutcome::Released)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ACCOUNT: AccountId = AccountId::new(7);
    const OTHER_ACCOUNT: AccountId = AccountId::new(8);

    fn credits(bytes: u64, slots: u64) -> Credits {
        Credits::new(PayloadBytes::new(bytes), ExchangeSlots::new(slots))
    }

    fn key(account: AccountId, slot: u32, generation: u64) -> LeaseKey {
        LeaseKey {
            account,
            slot,
            generation: LeaseGeneration::new(generation),
        }
    }

    fn account(id: AccountId, used: Credits, limit: Credits) -> Account {
        Account::from_parts(id, used, limit)
    }

    fn held(key: LeaseKey, cost: Credits) -> LeaseRecord {
        LeaseRecord::from_parts(key, cost, LeaseState::Held)
    }

    /// Calls the function and asserts that neither argument changed.
    fn refused(
        lease: LeaseRecord,
        account: Account,
        key: LeaseKey,
    ) -> Result<ReleaseOutcome, ReleaseError> {
        let lease_before = LeaseRecord::from_parts(lease.key(), lease.cost(), lease.state());
        let account_before = Account::from_parts(account.id(), account.used(), account.limit());
        let (mut lease, mut account) = (lease, account);
        let result = release_if_current(&mut lease, &mut account, key);
        assert_eq!(lease, lease_before, "the lease changed");
        assert_eq!(account, account_before, "the account changed");
        result
    }

    #[test]
    fn a_current_key_subtracts_both_dimensions_exactly_and_releases() {
        let current = key(ACCOUNT, 3, 5);
        let mut lease = held(current, credits(100, 2));
        let mut charged = account(ACCOUNT, credits(250, 6), credits(1000, 10));

        assert_eq!(
            release_if_current(&mut lease, &mut charged, current),
            Ok(ReleaseOutcome::Released)
        );
        assert_eq!(charged.used(), credits(150, 4));
        assert_eq!(charged.limit(), credits(1000, 10));
        assert_eq!(charged.id(), ACCOUNT);
        assert_eq!(lease.state(), LeaseState::Released);
        assert_eq!(lease.key(), current);
        assert_eq!(lease.cost(), credits(100, 2));
    }

    #[test]
    fn a_stale_key_mutates_nothing() {
        let current = key(ACCOUNT, 3, 5);
        for stale in [
            key(ACCOUNT, 3, 4),
            key(ACCOUNT, 3, 6),
            key(ACCOUNT, 2, 5),
            key(OTHER_ACCOUNT, 3, 5),
        ] {
            assert_eq!(
                refused(
                    held(current, credits(100, 2)),
                    account(ACCOUNT, credits(250, 6), credits(1000, 10)),
                    stale,
                ),
                Ok(ReleaseOutcome::Stale),
                "{stale:?}"
            );
        }
    }

    #[test]
    fn a_repeated_release_subtracts_nothing_more() {
        let current = key(ACCOUNT, 3, 5);
        let mut lease = held(current, credits(100, 2));
        let mut charged = account(ACCOUNT, credits(250, 6), credits(1000, 10));
        assert_eq!(
            release_if_current(&mut lease, &mut charged, current),
            Ok(ReleaseOutcome::Released)
        );

        assert_eq!(
            refused(lease, charged, current),
            Ok(ReleaseOutcome::AlreadyReleased)
        );
    }

    #[test]
    fn a_repeated_release_does_not_subtract_even_when_it_could() {
        let current = key(ACCOUNT, 3, 5);
        let released = LeaseRecord::from_parts(current, credits(100, 2), LeaseState::Released);
        assert_eq!(
            refused(
                released,
                account(ACCOUNT, credits(250, 6), credits(1000, 10)),
                current
            ),
            Ok(ReleaseOutcome::AlreadyReleased)
        );
    }

    #[test]
    fn a_wrong_account_is_an_explicit_error_and_changes_nothing() {
        let current = key(ACCOUNT, 3, 5);
        assert_eq!(
            refused(
                held(current, credits(100, 2)),
                account(OTHER_ACCOUNT, credits(250, 6), credits(1000, 10)),
                current,
            ),
            Err(ReleaseError::WrongAccount)
        );
    }

    #[test]
    fn a_wrong_account_is_reported_for_a_released_lease_too() {
        let current = key(ACCOUNT, 3, 5);
        assert_eq!(
            refused(
                LeaseRecord::from_parts(current, credits(100, 2), LeaseState::Released),
                account(OTHER_ACCOUNT, credits(250, 6), credits(1000, 10)),
                current,
            ),
            Err(ReleaseError::WrongAccount)
        );
    }

    #[test]
    fn bytes_above_usage_change_neither_dimension() {
        let current = key(ACCOUNT, 3, 5);
        assert_eq!(
            refused(
                held(current, credits(251, 2)),
                account(ACCOUNT, credits(250, 6), credits(1000, 10)),
                current,
            ),
            Err(ReleaseError::CostExceedsUsage(Resource::PayloadBytes))
        );
    }

    #[test]
    fn slots_above_usage_change_neither_dimension() {
        let current = key(ACCOUNT, 3, 5);
        // The bytes would fit: they must not be subtracted on their own.
        assert_eq!(
            refused(
                held(current, credits(100, 7)),
                account(ACCOUNT, credits(250, 6), credits(1000, 10)),
                current,
            ),
            Err(ReleaseError::CostExceedsUsage(Resource::ExchangeSlots))
        );
    }

    #[test]
    fn both_above_usage_report_bytes_and_change_nothing() {
        let current = key(ACCOUNT, 3, 5);
        assert_eq!(
            refused(
                held(current, credits(251, 7)),
                account(ACCOUNT, credits(250, 6), credits(1000, 10)),
                current,
            ),
            Err(ReleaseError::CostExceedsUsage(Resource::PayloadBytes))
        );
    }

    #[test]
    fn an_old_key_leaves_the_reservation_that_reused_the_slot() {
        let mut charged = Account::new(ACCOUNT, credits(1000, 10));
        let mut first = charged
            .reserve(3, LeaseGeneration::new(5), credits(100, 2))
            .expect("fits");
        let old = first.key();
        assert_eq!(
            release_if_current(&mut first, &mut charged, old),
            Ok(ReleaseOutcome::Released)
        );

        // The slot is reused under the next generation.
        let next = old.generation.checked_next().expect("not exhausted");
        let second = charged.reserve(3, next, credits(40, 1)).expect("fits");
        assert_eq!(charged.used(), credits(40, 1));

        assert_eq!(refused(second, charged, old), Ok(ReleaseOutcome::Stale));
    }

    #[test]
    fn an_old_key_of_another_account_is_stale_not_a_fault() {
        // The slot now holds a reservation of another account: the late
        // release names the old account, and is a late release.
        let old = key(ACCOUNT, 3, 5);
        let reused = key(OTHER_ACCOUNT, 3, 6);
        assert_eq!(
            refused(
                held(reused, credits(40, 1)),
                account(ACCOUNT, credits(250, 6), credits(1000, 10)),
                old,
            ),
            Ok(ReleaseOutcome::Stale)
        );
    }

    #[test]
    fn the_integer_limits_are_exact() {
        let current = key(AccountId::new(u64::MAX), u32::MAX, u64::MAX);
        let all = credits(u64::MAX, u64::MAX);

        let mut lease = held(current, all);
        let mut charged = account(AccountId::new(u64::MAX), all, all);
        assert_eq!(
            release_if_current(&mut lease, &mut charged, current),
            Ok(ReleaseOutcome::Released)
        );
        assert_eq!(charged.used(), Credits::ZERO);

        // One unit short in the second dimension only.
        assert_eq!(
            refused(
                held(current, all),
                account(
                    AccountId::new(u64::MAX),
                    credits(u64::MAX, u64::MAX - 1),
                    all
                ),
                current,
            ),
            Err(ReleaseError::CostExceedsUsage(Resource::ExchangeSlots))
        );

        // A zero cost releases without changing the usage.
        let mut free = held(current, Credits::ZERO);
        let mut empty = account(AccountId::new(u64::MAX), Credits::ZERO, Credits::ZERO);
        assert_eq!(
            release_if_current(&mut free, &mut empty, current),
            Ok(ReleaseOutcome::Released)
        );
        assert_eq!(empty.used(), Credits::ZERO);
        assert_eq!(free.state(), LeaseState::Released);
    }

    #[test]
    fn an_incoherent_account_is_released_exactly_or_refused_whole() {
        // Usage above the limit: the release still subtracts exactly.
        let current = key(ACCOUNT, 3, 5);
        let mut lease = held(current, credits(100, 2));
        let mut over = account(ACCOUNT, credits(250, 6), credits(10, 1));
        assert!(!over.is_coherent());
        assert_eq!(
            release_if_current(&mut lease, &mut over, current),
            Ok(ReleaseOutcome::Released)
        );
        assert_eq!(over.used(), credits(150, 4));

        // A held lease the account never counted: refused, nothing changes.
        assert_eq!(
            refused(
                held(current, credits(100, 2)),
                account(ACCOUNT, Credits::ZERO, credits(1000, 10)),
                current,
            ),
            Err(ReleaseError::CostExceedsUsage(Resource::PayloadBytes))
        );
    }

    #[test]
    fn a_release_keeps_a_coherent_account_coherent() {
        let current = key(ACCOUNT, 3, 5);
        let mut lease = held(current, credits(100, 2));
        let mut charged = account(ACCOUNT, credits(1000, 10), credits(1000, 10));
        assert!(charged.is_coherent());
        assert_eq!(
            release_if_current(&mut lease, &mut charged, current),
            Ok(ReleaseOutcome::Released)
        );
        assert!(charged.is_coherent());
    }

    #[test]
    fn a_second_record_of_a_held_lease_is_not_detected() {
        // The precondition, shown: the function cannot see that two
        // records stand for one lease. The owner of the lease table must
        // never build the second one.
        let mut charged = Account::new(ACCOUNT, credits(1000, 10));
        let _other = charged
            .reserve(1, LeaseGeneration::new(0), credits(100, 2))
            .expect("fits");
        let mut first = charged
            .reserve(3, LeaseGeneration::new(5), credits(100, 2))
            .expect("fits");
        let current = first.key();
        let mut second = LeaseRecord::from_parts(first.key(), first.cost(), first.state());

        assert_eq!(
            release_if_current(&mut first, &mut charged, current),
            Ok(ReleaseOutcome::Released)
        );
        assert_eq!(
            release_if_current(&mut second, &mut charged, current),
            Ok(ReleaseOutcome::Released)
        );
        // The lease still held in slot 1 is no longer counted.
        assert_eq!(charged.used(), Credits::ZERO);
    }

    #[test]
    fn reserve_charges_both_dimensions_or_nothing() {
        let mut charged = Account::new(ACCOUNT, credits(100, 2));
        let lease = charged
            .reserve(1, LeaseGeneration::new(0), credits(60, 1))
            .expect("fits");
        assert_eq!(lease.key(), key(ACCOUNT, 1, 0));
        assert_eq!(lease.cost(), credits(60, 1));
        assert_eq!(lease.state(), LeaseState::Held);
        assert_eq!(charged.used(), credits(60, 1));

        assert_eq!(
            charged.reserve(2, LeaseGeneration::new(0), credits(41, 1)),
            Err(ReserveError::LimitExceeded(Resource::PayloadBytes))
        );
        // The bytes fit and the slots do not: the bytes are not charged.
        assert_eq!(
            charged.reserve(2, LeaseGeneration::new(0), credits(40, 2)),
            Err(ReserveError::LimitExceeded(Resource::ExchangeSlots))
        );
        assert_eq!(charged.used(), credits(60, 1));

        // The exact limit is reachable.
        assert!(
            charged
                .reserve(2, LeaseGeneration::new(0), credits(40, 1))
                .is_ok()
        );
        assert_eq!(charged.used(), charged.limit());
        assert!(charged.is_coherent());
    }

    #[test]
    fn reserve_refuses_an_overflow_without_wrapping() {
        let mut charged =
            Account::from_parts(ACCOUNT, credits(u64::MAX, 0), credits(u64::MAX, u64::MAX));
        assert_eq!(
            charged.reserve(1, LeaseGeneration::new(0), credits(1, 0)),
            Err(ReserveError::LimitExceeded(Resource::PayloadBytes))
        );
        assert_eq!(charged.used(), credits(u64::MAX, 0));
    }

    #[test]
    fn reserve_then_release_restores_the_account() {
        let mut charged = Account::from_parts(ACCOUNT, credits(10, 1), credits(100, 4));
        let mut lease = charged
            .reserve(9, LeaseGeneration::new(2), credits(90, 3))
            .expect("fits");
        let current = lease.key();
        assert_eq!(
            release_if_current(&mut lease, &mut charged, current),
            Ok(ReleaseOutcome::Released)
        );
        assert_eq!(charged.used(), credits(10, 1));
    }
}
