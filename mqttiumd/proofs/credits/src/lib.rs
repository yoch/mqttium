//! Kani proof of `mqttiumd_core::credits::release_if_current` (DEM-021).
//!
//! Every harness calls the production function of `mqttiumd-core`, built
//! from the product's sources by path. Nothing here restates its body: the
//! expectations are written over plain integers read from the state before
//! the call.
//!
//! The states are built with the public `from_parts` constructors, which
//! check nothing, so every field is symbolic over its whole type: all of
//! `u64` for identities, generations, costs, usages and limits, all of
//! `u32` for the slot, both lease states. Coherent and incoherent accounts
//! are both in range unless a harness says otherwise.
//!
//! A failed assertion ends its path, as a panic does, so a later assertion
//! is only examined where the earlier ones hold. Within a harness the
//! assertions on the state therefore come first and the assertion on the
//! returned value last: a mutation of the state is reported as such, not
//! hidden behind a wrong return value.
//!
//! One precondition of the function is not a property the harnesses can
//! establish: each held lease has one record. `p12` builds the state the
//! precondition forbids and shows what the function then does.
//!
//! Each assertion and each cover carries a name, `P<n>` or `W<n>`, that
//! `proofs/credits/PROOF.md` lists and `proofs/credits/run.sh` looks for.
//! A cover is a witness of non-vacuity: the run fails unless every one is
//! satisfied, so an assumption that made a branch unreachable would fail
//! the run instead of proving the branch by absence.

#[cfg(kani)]
mod harnesses {
    use mqttiumd_core::contract::capacity::{ExchangeSlots, PayloadBytes, Resource};
    use mqttiumd_core::contract::ids::{AccountId, LeaseGeneration, LeaseKey};
    use mqttiumd_core::credits::{
        Account, Credits, LeaseRecord, LeaseState, ReleaseError, ReleaseOutcome, release_if_current,
    };

    /// The whole state as plain integers.
    #[derive(Clone, Copy)]
    struct Plain {
        lease_account: u64,
        lease_slot: u32,
        lease_generation: u64,
        cost_bytes: u64,
        cost_slots: u64,
        held: bool,
        account: u64,
        used_bytes: u64,
        used_slots: u64,
        limit_bytes: u64,
        limit_slots: u64,
    }

    fn plain(lease: &LeaseRecord, account: &Account) -> Plain {
        Plain {
            lease_account: lease.key().account.get(),
            lease_slot: lease.key().slot,
            lease_generation: lease.key().generation.get(),
            cost_bytes: lease.cost().bytes.get(),
            cost_slots: lease.cost().slots.get(),
            held: matches!(lease.state(), LeaseState::Held),
            account: account.id().get(),
            used_bytes: account.used().bytes.get(),
            used_slots: account.used().slots.get(),
            limit_bytes: account.limit().bytes.get(),
            limit_slots: account.limit().slots.get(),
        }
    }

    /// Every field equal, as integers.
    fn same(a: Plain, b: Plain) -> bool {
        a.lease_account == b.lease_account
            && a.lease_slot == b.lease_slot
            && a.lease_generation == b.lease_generation
            && a.cost_bytes == b.cost_bytes
            && a.cost_slots == b.cost_slots
            && a.held == b.held
            && a.account == b.account
            && a.used_bytes == b.used_bytes
            && a.used_slots == b.used_slots
            && a.limit_bytes == b.limit_bytes
            && a.limit_slots == b.limit_slots
    }

    /// Whether `key` names the lease of `state`, as integers.
    fn names(key: LeaseKey, state: Plain) -> bool {
        key.account.get() == state.lease_account
            && key.slot == state.lease_slot
            && key.generation.get() == state.lease_generation
    }

    fn any_key() -> LeaseKey {
        LeaseKey {
            account: AccountId::new(kani::any()),
            slot: kani::any(),
            generation: LeaseGeneration::new(kani::any()),
        }
    }

    fn any_credits() -> Credits {
        Credits::new(
            PayloadBytes::new(kani::any()),
            ExchangeSlots::new(kani::any()),
        )
    }

    /// Any lease record: any key, any cost, held or released.
    fn any_lease() -> LeaseRecord {
        let state = if kani::any() {
            LeaseState::Held
        } else {
            LeaseState::Released
        };
        LeaseRecord::from_parts(any_key(), any_credits(), state)
    }

    /// Any account, coherent or not.
    fn any_account() -> Account {
        Account::from_parts(AccountId::new(kani::any()), any_credits(), any_credits())
    }

    /// P1. The whole table at once, with no assumption at all: for every
    /// lease, account and key, the result and the final state are the ones
    /// the DEM-021 table gives, and no other outcome exists.
    #[kani::proof]
    fn p1_total_specification() {
        let mut lease = any_lease();
        let mut account = any_account();
        let key = any_key();
        let before = plain(&lease, &account);

        let result = release_if_current(&mut lease, &mut account, key);
        let after = plain(&lease, &account);

        if !names(key, before) {
            assert!(same(before, after), "P1b: a stale key mutates nothing");
            assert!(
                result == Ok(ReleaseOutcome::Stale),
                "P1a: a key that is not current is stale"
            );
            kani::cover!(true, "W1a: a stale key is reached");
        } else if before.account != before.lease_account {
            assert!(same(before, after), "P1d: a wrong account mutates nothing");
            assert!(
                result == Err(ReleaseError::WrongAccount),
                "P1c: a wrong account is an explicit error"
            );
            kani::cover!(true, "W1b: a wrong account is reached");
        } else if !before.held {
            assert!(
                same(before, after),
                "P1f: a repeated release subtracts nothing"
            );
            assert!(
                result == Ok(ReleaseOutcome::AlreadyReleased),
                "P1e: a released lease is reported as already released"
            );
            kani::cover!(true, "W1c: a repeated release is reached");
        } else if before.cost_bytes > before.used_bytes {
            assert!(
                same(before, after),
                "P1h: bytes above the usage change neither dimension"
            );
            assert!(
                result == Err(ReleaseError::CostExceedsUsage(Resource::PayloadBytes)),
                "P1g: bytes above the usage are an explicit error"
            );
            kani::cover!(true, "W1d: bytes above the usage are reached");
        } else if before.cost_slots > before.used_slots {
            assert!(
                same(before, after),
                "P1j: slots above the usage change neither dimension"
            );
            assert!(
                result == Err(ReleaseError::CostExceedsUsage(Resource::ExchangeSlots)),
                "P1i: slots above the usage are an explicit error"
            );
            kani::cover!(true, "W1e: slots above the usage are reached");
        } else {
            let expected = Plain {
                held: false,
                used_bytes: before.used_bytes - before.cost_bytes,
                used_slots: before.used_slots - before.cost_slots,
                ..before
            };
            assert!(
                same(expected, after),
                "P1l: both dimensions are reduced by the exact cost, the lease is released, nothing else changes"
            );
            assert!(
                result == Ok(ReleaseOutcome::Released),
                "P1k: a current key on a held lease releases"
            );
            kani::cover!(true, "W1f: an exact release is reached");
        }
    }

    /// P2. Stale key: no mutation. The key differs from the lease's in at
    /// least one component; each component alone is witnessed.
    #[kani::proof]
    fn p2_stale_key_mutates_nothing() {
        let mut lease = any_lease();
        let mut account = any_account();
        let key = any_key();
        let before = plain(&lease, &account);
        kani::assume(!names(key, before));

        let result = release_if_current(&mut lease, &mut account, key);

        assert!(
            same(before, plain(&lease, &account)),
            "P2b: a stale key changes neither the lease nor the account"
        );
        assert!(
            result == Ok(ReleaseOutcome::Stale),
            "P2a: the result of a stale key is Stale"
        );
        let same_account = key.account.get() == before.lease_account;
        let same_slot = key.slot == before.lease_slot;
        let same_generation = key.generation.get() == before.lease_generation;
        kani::cover!(
            same_account && same_slot && !same_generation,
            "W2a: only the generation differs"
        );
        kani::cover!(
            same_account && !same_slot && same_generation,
            "W2b: only the slot differs"
        );
        kani::cover!(
            !same_account && same_slot && same_generation,
            "W2c: only the account of the key differs"
        );
        kani::cover!(
            same_account
                && same_slot
                && key.generation.get() == u64::MAX
                && before.lease_generation == 0,
            "W2d: the key is of the last generation and the lease of the first"
        );
        kani::cover!(
            same_account
                && same_slot
                && !same_generation
                && before.held
                && before.account == before.lease_account
                && before.cost_bytes <= before.used_bytes
                && before.cost_slots <= before.used_slots
                && before.cost_bytes > 0
                && before.cost_slots > 0,
            "W2e: only the generation stands between the key and a real subtraction"
        );
    }

    /// P3. Repeated release: no second subtraction. Two calls with the
    /// same key; whatever the first did, the second changes nothing, and
    /// after a real release it reports `AlreadyReleased`.
    #[kani::proof]
    fn p3_repeated_release_subtracts_once() {
        let mut lease = any_lease();
        let mut account = any_account();
        let key = any_key();

        let first = release_if_current(&mut lease, &mut account, key);
        let between = plain(&lease, &account);
        let second = release_if_current(&mut lease, &mut account, key);

        assert!(
            same(between, plain(&lease, &account)),
            "P3a: a second call with the same key changes nothing"
        );
        if first == Ok(ReleaseOutcome::Released) {
            assert!(!between.held, "P3c: a released lease is not held");
            assert!(
                second == Ok(ReleaseOutcome::AlreadyReleased),
                "P3b: a release that is repeated is reported as already released"
            );
        }
        kani::cover!(
            first == Ok(ReleaseOutcome::Released)
                && between.used_bytes >= between.cost_bytes
                && between.used_slots >= between.cost_slots
                && between.cost_bytes > 0
                && between.cost_slots > 0,
            "W3a: a real release is repeated while a second subtraction would fit"
        );
        kani::cover!(
            first == Ok(ReleaseOutcome::AlreadyReleased),
            "W3b: a lease that starts released is released again"
        );
    }

    /// P4. Wrong account: explicit error, no change. The key is current
    /// and the account presented is another one.
    #[kani::proof]
    fn p4_wrong_account_is_refused_whole() {
        let mut lease = any_lease();
        let mut account = any_account();
        let key = lease.key();
        let before = plain(&lease, &account);
        kani::assume(before.account != before.lease_account);

        let result = release_if_current(&mut lease, &mut account, key);

        assert!(
            same(before, plain(&lease, &account)),
            "P4b: a wrong account changes neither the lease nor the account"
        );
        assert!(
            result == Err(ReleaseError::WrongAccount),
            "P4a: the result of a wrong account is WrongAccount"
        );
        kani::cover!(
            before.held
                && before.cost_bytes <= before.used_bytes
                && before.cost_slots <= before.used_slots
                && before.cost_bytes > 0
                && before.cost_slots > 0,
            "W4a: the wrong account could have paid the cost"
        );
        kani::cover!(!before.held, "W4b: the lease was already released");
    }

    /// P5. Cost above a usage: explicit error, neither dimension changed.
    /// The key is current, the account is the lease's, the lease is held,
    /// and at least one dimension of the cost exceeds the usage.
    #[kani::proof]
    fn p5_cost_above_usage_changes_neither_dimension() {
        let mut lease = any_lease();
        let mut account = any_account();
        let key = lease.key();
        let before = plain(&lease, &account);
        kani::assume(before.account == before.lease_account && before.held);
        let bytes_short = before.cost_bytes > before.used_bytes;
        let slots_short = before.cost_slots > before.used_slots;
        kani::assume(bytes_short || slots_short);

        let result = release_if_current(&mut lease, &mut account, key);
        let after = plain(&lease, &account);

        assert!(
            after.used_bytes == before.used_bytes,
            "P5b: the bytes in use are unchanged"
        );
        assert!(
            after.used_slots == before.used_slots,
            "P5c: the slots in use are unchanged"
        );
        assert!(after.held, "P5d: the lease is still held");
        assert!(same(before, after), "P5e: nothing at all changed");
        assert!(
            matches!(result, Err(ReleaseError::CostExceedsUsage(_))),
            "P5a: a cost above a usage is an explicit error"
        );
        kani::cover!(
            !bytes_short && slots_short && before.cost_bytes > 0,
            "W5a: the bytes fit, are not zero, and the slots do not fit"
        );
        kani::cover!(
            bytes_short && !slots_short && before.cost_slots > 0,
            "W5b: the slots fit, are not zero, and the bytes do not fit"
        );
        kani::cover!(bytes_short && slots_short, "W5c: neither dimension fits");
        kani::cover!(
            before.cost_slots == u64::MAX && before.used_slots == u64::MAX - 1 && !bytes_short,
            "W5d: one slot short at the top of the range"
        );
        kani::cover!(
            before.cost_bytes == 1 && before.used_bytes == 0,
            "W5e: one byte short at the bottom of the range"
        );
    }

    /// P6. Current key and coherent account: exact subtraction of bytes
    /// and slots, then the state `Released`; the account stays coherent.
    #[kani::proof]
    fn p6_current_key_releases_exactly() {
        let mut lease = any_lease();
        let mut account = any_account();
        let key = lease.key();
        let before = plain(&lease, &account);
        kani::assume(account.is_coherent());
        kani::assume(before.account == before.lease_account && before.held);
        kani::assume(before.cost_bytes <= before.used_bytes);
        kani::assume(before.cost_slots <= before.used_slots);

        let result = release_if_current(&mut lease, &mut account, key);
        let after = plain(&lease, &account);

        assert!(
            after.used_bytes == before.used_bytes - before.cost_bytes,
            "P6b: the bytes in use are reduced by the exact cost"
        );
        assert!(
            after.used_slots == before.used_slots - before.cost_slots,
            "P6c: the slots in use are reduced by the exact cost"
        );
        assert!(!after.held, "P6d: the lease is released");
        assert!(
            after.limit_bytes == before.limit_bytes
                && after.limit_slots == before.limit_slots
                && after.account == before.account
                && names(key, after)
                && after.cost_bytes == before.cost_bytes
                && after.cost_slots == before.cost_slots,
            "P6e: limits, identities and the recorded cost are unchanged"
        );
        assert!(account.is_coherent(), "P6f: the account stays coherent");
        assert!(
            result == Ok(ReleaseOutcome::Released),
            "P6a: the result is Released"
        );
        kani::cover!(
            before.cost_bytes == u64::MAX && before.cost_slots == u64::MAX,
            "W6a: both costs are the largest integer"
        );
        kani::cover!(
            before.cost_bytes == 0 && before.cost_slots == 0 && before.used_bytes > 0,
            "W6b: a zero cost is released from a used account"
        );
        kani::cover!(
            before.used_bytes == before.limit_bytes
                && before.used_slots == before.limit_slots
                && before.cost_bytes > 0
                && before.cost_slots > 0,
            "W6c: the account was full"
        );
        kani::cover!(
            before.lease_generation == u64::MAX
                && before.lease_slot == u32::MAX
                && before.lease_account == u64::MAX,
            "W6d: every identity is the largest integer"
        );
        kani::cover!(
            after.used_bytes > 0 && after.used_slots > 0,
            "W6e: something stays in use after the release"
        );
    }

    /// P7. Old key after the slot was reused: the new reservation is
    /// unchanged. The first lease is released for real, the production
    /// `reserve` charges the same slot again under another generation, and
    /// the old key is presented once more.
    #[kani::proof]
    fn p7_old_key_leaves_the_reused_slot() {
        let mut account = any_account();
        let slot: u32 = kani::any();
        let old_generation = LeaseGeneration::new(kani::any());
        let new_generation = LeaseGeneration::new(kani::any());
        kani::assume(new_generation.get() != old_generation.get());

        let Ok(mut first) = account.reserve(slot, old_generation, any_credits()) else {
            return;
        };
        let old_key = first.key();
        let released = release_if_current(&mut first, &mut account, old_key);
        assert!(
            released == Ok(ReleaseOutcome::Released),
            "P7a: a reservation just made is released by its own key"
        );

        let Ok(mut second) = account.reserve(slot, new_generation, any_credits()) else {
            return;
        };
        let before = plain(&second, &account);

        let result = release_if_current(&mut second, &mut account, old_key);

        assert!(
            same(before, plain(&second, &account)),
            "P7c: the new reservation and its account are unchanged"
        );
        assert!(before.held, "P7d: the new reservation is still held");
        assert!(
            result == Ok(ReleaseOutcome::Stale),
            "P7b: the old key is stale for the reservation that reused the slot"
        );
        kani::cover!(
            before.cost_bytes > 0 && before.cost_slots > 0,
            "W7a: the new reservation has a cost in both dimensions"
        );
        kani::cover!(
            Ok(new_generation) == old_generation.checked_next(),
            "W7b: the slot is reused under the next generation"
        );
        kani::cover!(
            new_generation.get() < old_generation.get(),
            "W7c: a generation that went backwards is refused as well"
        );
    }

    /// P8. Incoherent account, refusal: an account whose usage exceeds a
    /// limit is never partially mutated. Every result other than
    /// `Released` leaves the lease and the account as they were.
    #[kani::proof]
    fn p8_incoherent_account_refusal_mutates_nothing() {
        let mut lease = any_lease();
        let mut account = any_account();
        let key = any_key();
        kani::assume(!account.is_coherent());
        let before = plain(&lease, &account);

        let result = release_if_current(&mut lease, &mut account, key);

        if result != Ok(ReleaseOutcome::Released) {
            assert!(
                same(before, plain(&lease, &account)),
                "P8a: a refusal on an incoherent account mutates nothing"
            );
        }
        kani::cover!(
            matches!(result, Err(ReleaseError::CostExceedsUsage(_))),
            "W8a: an incoherent account refuses a cost above its usage"
        );
        kani::cover!(
            result == Err(ReleaseError::WrongAccount),
            "W8b: an incoherent account refuses as the wrong account"
        );
        kani::cover!(
            result == Ok(ReleaseOutcome::Stale),
            "W8c: an incoherent account sees a stale key"
        );
        kani::cover!(
            result == Ok(ReleaseOutcome::AlreadyReleased),
            "W8d: an incoherent account sees a repeated release"
        );
    }

    /// P9. Incoherent account, release: the subtraction is still exact in
    /// both dimensions and nothing else moves. Coherence is not claimed.
    #[kani::proof]
    fn p9_incoherent_account_release_is_exact() {
        let mut lease = any_lease();
        let mut account = any_account();
        let key = any_key();
        kani::assume(!account.is_coherent());
        let before = plain(&lease, &account);

        let result = release_if_current(&mut lease, &mut account, key);
        let after = plain(&lease, &account);

        if result == Ok(ReleaseOutcome::Released) {
            assert!(
                before.held && names(key, before) && before.account == before.lease_account,
                "P9a: only a held lease is released, by its key, from its account"
            );
            assert!(
                before.cost_bytes <= before.used_bytes && before.cost_slots <= before.used_slots,
                "P9b: a release never happens above a usage"
            );
            let expected = Plain {
                held: false,
                used_bytes: before.used_bytes - before.cost_bytes,
                used_slots: before.used_slots - before.cost_slots,
                ..before
            };
            assert!(
                same(expected, after),
                "P9c: the release on an incoherent account is exact"
            );
        }
        kani::cover!(
            result == Ok(ReleaseOutcome::Released) && !account.is_coherent(),
            "W9a: a release leaves the account incoherent"
        );
        kani::cover!(
            result == Ok(ReleaseOutcome::Released) && account.is_coherent(),
            "W9b: a release makes the account coherent again"
        );
        kani::cover!(
            before.used_bytes > before.limit_bytes && before.used_slots <= before.limit_slots,
            "W9c: only the bytes exceed their limit"
        );
        kani::cover!(
            before.used_bytes <= before.limit_bytes && before.used_slots > before.limit_slots,
            "W9d: only the slots exceed their limit"
        );
    }

    /// P10. Coherence is preserved by every outcome, not only by a
    /// release: a coherent account is coherent after any call.
    #[kani::proof]
    fn p10_coherence_is_preserved() {
        let mut lease = any_lease();
        let mut account = any_account();
        let key = any_key();
        kani::assume(account.is_coherent());

        let result = release_if_current(&mut lease, &mut account, key);

        assert!(
            account.is_coherent(),
            "P10a: a coherent account stays coherent"
        );
        kani::cover!(
            result == Ok(ReleaseOutcome::Released),
            "W10a: a coherent account is released"
        );
        kani::cover!(result.is_err(), "W10b: a coherent account refuses");
    }

    /// P11. Reserve then release gives the account back exactly as it was,
    /// through the two production functions.
    #[kani::proof]
    fn p11_reserve_then_release_restores_the_account() {
        let mut account = any_account();
        let before = (
            account.used().bytes.get(),
            account.used().slots.get(),
            account.limit().bytes.get(),
            account.limit().slots.get(),
            account.id().get(),
        );
        let Ok(mut lease) = account.reserve(
            kani::any(),
            LeaseGeneration::new(kani::any()),
            any_credits(),
        ) else {
            return;
        };
        let charged = lease.cost().bytes.get() > 0 && lease.cost().slots.get() > 0;
        let key = lease.key();

        let result = release_if_current(&mut lease, &mut account, key);

        assert!(
            result == Ok(ReleaseOutcome::Released),
            "P11a: a reservation is released by its own key"
        );
        let after = (
            account.used().bytes.get(),
            account.used().slots.get(),
            account.limit().bytes.get(),
            account.limit().slots.get(),
            account.id().get(),
        );
        assert!(
            before == after,
            "P11b: the account is exactly as it was before the reservation"
        );
        kani::cover!(
            charged,
            "W11a: the reservation had a cost in both dimensions"
        );
    }
    /// P12. The precondition, shown rather than assumed away. The function
    /// requires one record per held lease: an account does not know its
    /// leases. Here a second record of the same held lease is built with
    /// `from_parts`, which is what the precondition forbids. Each release
    /// is exact on its own, and nothing in the function can refuse the
    /// second: when both succeed the cost has left the account twice.
    /// W12a shows that this happens; the owner of the lease table, not
    /// this function, has to make it impossible.
    #[kani::proof]
    fn p12_a_second_record_of_a_held_lease_is_released_too() {
        let mut account = any_account();
        let mut first = any_lease();
        let mut second = LeaseRecord::from_parts(first.key(), first.cost(), first.state());
        let key = first.key();
        let before = plain(&first, &account);

        let first_result = release_if_current(&mut first, &mut account, key);
        let second_result = release_if_current(&mut second, &mut account, key);
        let after = plain(&second, &account);

        if first_result == Ok(ReleaseOutcome::Released)
            && second_result == Ok(ReleaseOutcome::Released)
        {
            assert!(
                before.cost_bytes <= before.used_bytes
                    && before.cost_bytes <= before.used_bytes - before.cost_bytes
                    && before.cost_slots <= before.used_slots
                    && before.cost_slots <= before.used_slots - before.cost_slots,
                "P12a: two releases happen only while the usage covers the cost twice"
            );
            assert!(
                after.used_bytes == before.used_bytes - before.cost_bytes - before.cost_bytes
                    && after.used_slots
                        == before.used_slots - before.cost_slots - before.cost_slots,
                "P12b: the cost of the one lease has left the account twice"
            );
        }
        kani::cover!(
            first_result == Ok(ReleaseOutcome::Released)
                && second_result == Ok(ReleaseOutcome::Released)
                && before.cost_bytes > 0
                && before.cost_slots > 0,
            "W12a: a second record of a held lease is released as well"
        );
        kani::cover!(
            first_result == Ok(ReleaseOutcome::Released)
                && matches!(second_result, Err(ReleaseError::CostExceedsUsage(_))),
            "W12b: the second release is refused only when the usage no longer covers it"
        );
    }
}
