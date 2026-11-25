//! Transition of one restored lease with a second, independently held lease.
#[cfg(kani)]
mod harnesses {
    use core::num::NonZeroU32;
    use mqttiumd_core::contract::capacity::{ExchangeSlots, PayloadBytes};
    use mqttiumd_core::contract::ids::{AccountId, LeaseGeneration, LeaseKey};
    use mqttiumd_core::credits::Credits;
    use mqttiumd_core::leases::LeaseTable;

    fn cost(bytes: u64, slots: u64) -> Credits {
        Credits::new(PayloadBytes::new(bytes), ExchangeSlots::new(slots))
    }

    #[kani::proof]
    #[kani::unwind(5)]
    fn lighten_preserves_the_other_lease_and_accounts_for_both() {
        let old_bytes: u64 = kani::any();
        let old_slots: u64 = kani::any();
        // One unit in each dimension already belongs to the other session.
        kani::assume(old_bytes < u64::MAX && old_slots < u64::MAX);
        let new_bytes: u64 = kani::any();
        let new_slots: u64 = kani::any();
        let generation: u64 = kani::any();
        let owner = AccountId::new(1);
        let mut table =
            LeaseTable::new(owner, cost(u64::MAX, u64::MAX), NonZeroU32::new(2).unwrap()).unwrap();
        let other = table.reserve(cost(1, 1)).unwrap();
        let key = LeaseKey {
            account: owner,
            slot: 1,
            generation: LeaseGeneration::new(generation),
        };
        table.restore(key, cost(old_bytes, old_slots)).unwrap();

        let presented = match kani::any::<u8>() {
            0 => LeaseKey {
                account: AccountId::new(2),
                ..key
            },
            1 => LeaseKey {
                generation: LeaseGeneration::new(kani::any()),
                ..key
            },
            2 => LeaseKey { slot: 2, ..key },
            _ => key,
        };
        let result = table.lighten(presented, cost(new_bytes, new_slots));
        let grows = new_bytes > old_bytes || new_slots > old_slots;
        let equal = new_bytes == old_bytes && new_slots == old_slots;
        let changed = presented == key && !grows && !equal && generation < u64::MAX;
        let expected = if changed {
            cost(new_bytes + 1, new_slots + 1)
        } else {
            cost(old_bytes + 1, old_slots + 1)
        };
        assert!(
            table.account().used() == expected,
            "L1a total credits match the two live leases"
        );
        assert_eq!(table.held(), 2, "both table entries stay held");
        assert!(
            table.holds(other),
            "the other session keeps its original identity"
        );
        if changed {
            let replacement = LeaseKey {
                generation: LeaseGeneration::new(generation + 1),
                ..key
            };
            assert_eq!(
                result,
                Some(replacement),
                "a reduction advances exactly one generation"
            );
            assert!(table.holds(replacement));
            assert!(
                !table.holds(key),
                "the old key cannot release the replacement"
            );
        } else {
            assert_eq!(result, (presented == key).then_some(key));
            assert!(table.holds(key), "refusal leaves the original lease held");
        }
        kani::cover!(
            changed && new_bytes == 0 && new_slots == 1,
            "PUBREC retains one slot and no bytes"
        );
        kani::cover!(changed && new_slots < old_slots, "slot credits can shrink");
        kani::cover!(
            presented == key && !grows && !equal && generation == u64::MAX,
            "generation exhausted"
        );
        kani::cover!(presented == key && grows, "growth refused");
        kani::cover!(presented == key && equal, "unchanged charge");
        kani::cover!(presented.account != owner, "foreign account");
        kani::cover!(presented.generation != key.generation, "stale generation");
        kani::cover!(presented.slot != key.slot, "out-of-range slot");
    }
}
