//! Independent byte expectations for the versioned subscription recipe.
use mqttiumd_contract::capacity::BoundedVec;
use mqttiumd_contract::command::{Recipe, UpdateSubscriptions};
use mqttiumd_contract::ids::{
    LogicalOwnerId, OperationId, OperationSeq, Revision, RunEpoch, SessionId, StoreIncarnation,
};
use mqttiumd_contract::packet::{Qos, SharedBytes};
use mqttiumd_contract::pubsub::{GrantedSubscriptionChange, SubscriptionOptions};

fn recipe(options: SubscriptionOptions, granted: Option<Qos>) -> Recipe {
    let mut changes = BoundedVec::with_capacity(1).unwrap();
    changes
        .try_push(GrantedSubscriptionChange {
            topic: SharedBytes::copy_from(b"a/#", 3).unwrap(),
            granted,
            options,
        })
        .unwrap();
    Recipe::UpdateSubscriptions(UpdateSubscriptions {
        operation: OperationId::new(
            StoreIncarnation::new(1),
            RunEpoch::new(2),
            LogicalOwnerId::new(3),
            OperationSeq::new(4),
        ),
        session: SessionId::new(5),
        session_revision: Revision::new(6),
        route_revision: Revision::new(7),
        changes,
    })
}
fn encode(recipe: &Recipe) -> Vec<u8> {
    let mut bytes = Vec::new();
    recipe.encode_canonical(&mut bytes).unwrap();
    bytes
}
#[test]
fn default_kind7_is_unchanged_and_nondefault_kind9_has_one_explicit_byte() {
    let legacy = encode(&recipe(
        SubscriptionOptions::DEFAULT,
        Some(Qos::AtLeastOnce),
    ));
    let mut expected = b"MQDR\0\x01\x07".to_vec();
    for value in [1u64, 2, 3, 4, 5, 6, 7] {
        expected.extend_from_slice(&value.to_be_bytes());
    }
    expected.extend_from_slice(&[1, 2, 0, 3, b'a', b'/', b'#']);
    assert_eq!(legacy, expected, "legacy_subscription_bytes_changed");
    for bits in [4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44] {
        let command = recipe(
            SubscriptionOptions::from_bits(bits).unwrap(),
            Some(Qos::AtLeastOnce),
        );
        let bytes = encode(&command);
        let mut expected = legacy.clone();
        expected[6] = 9;
        expected.insert(65, bits);
        assert_eq!(bytes, expected, "options_byte_layout_changed");
        assert_eq!(Recipe::decode_canonical(&bytes, 16, 1024).unwrap(), command);
        let mut invalid = bytes.clone();
        invalid[65] = 0;
        assert!(
            Recipe::decode_canonical(&invalid, 16, 1024).is_err(),
            "all_default_kind9_not_canonical"
        );
        for bad in [1, 2, 3, 0x30, 0x40, 0x80, 0xff] {
            invalid[65] = bad;
            assert!(
                Recipe::decode_canonical(&invalid, 16, 1024).is_err(),
                "invalid_options_accepted"
            );
        }
    }
}
#[test]
fn removal_does_not_carry_ignored_subscription_options() {
    assert!(
        recipe(SubscriptionOptions::from_bits(4).unwrap(), None)
            .check()
            .is_err()
    );
    assert!(recipe(SubscriptionOptions::DEFAULT, None).check().is_ok());
}
