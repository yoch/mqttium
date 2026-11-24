//! Byte-level and adversarial checks for the temporal recipe envelopes.
use mqttiumd_contract::capacity::{
    BoundedVec, ExchangeSlots, PayloadBytes, ReceiptSlots, References,
};
use mqttiumd_contract::command::{
    AdmitPublication, Costs, DeliveryCause, ExpireDelivery, ExpiringAdmission, ExpiringPublication,
    FinishDelivery, MarkDeliveryStarted, PayloadRef, Recipe, RecipeError, Source, Target,
    TimedReplaySubscriptions, UpdateSubscriptions,
};
use mqttiumd_contract::ids::{
    ConnectionGeneration, DeliveryId, ExchangeGeneration, ExchangeId, InboundPacketId,
    LogicalOwnerId, OperationId, OperationSeq, OutboundPacketId, PayloadId, PayloadSeq,
    PublicationId, PublicationSeq, RequestId, RequestSeq, Revision, RunEpoch, SessionId,
    StoreIncarnation,
};
use mqttiumd_contract::message_expiry::{MessageExpiry, MessageTime};
use mqttiumd_contract::packet::{Qos, SharedBytes};
use mqttiumd_contract::pubsub::{GrantedSubscriptionChange, SubscriptionOptions};
use mqttiumd_contract::receipts::ReceiptClass;
use mqttiumd_contract::retained::{ReplaySubscriptions, RetainedPublication, RetainedQos0};

fn scope() -> (StoreIncarnation, RunEpoch, LogicalOwnerId) {
    (
        StoreIncarnation::new(1),
        RunEpoch::new(2),
        LogicalOwnerId::new(3),
    )
}
fn operation(n: u64) -> OperationId {
    let (incarnation, epoch, owner) = scope();
    OperationId::new(incarnation, epoch, owner, OperationSeq::new(n))
}
fn publication() -> PublicationId {
    let (incarnation, epoch, owner) = scope();
    PublicationId::new(incarnation, epoch, owner, PublicationSeq::new(5))
}
fn time(tick: u64) -> MessageTime {
    MessageTime {
        domain: [7; 32],
        tick,
    }
}
fn expiry() -> MessageExpiry {
    MessageExpiry {
        observed: time(100),
        interval: 2,
    }
}
fn admission(qos: Qos, retain: bool) -> AdmitPublication {
    let (incarnation, epoch, owner) = scope();
    AdmitPublication {
        operation: operation(4),
        request: RequestId::new(incarnation, epoch, owner, RequestSeq::new(6)),
        publication: publication(),
        source: Source {
            session: SessionId::new(8),
            session_revision: Revision::new(9),
            packet_id: InboundPacketId::new(10).unwrap(),
            exchange: ExchangeId {
                session: SessionId::new(8),
                generation: ExchangeGeneration::new(11),
            },
        },
        route_revision: Revision::new(12),
        qos,
        retain,
        topic: SharedBytes::copy_from(b"t", 1).unwrap(),
        payload: PayloadRef {
            id: PayloadId::new(incarnation, epoch, owner, PayloadSeq::new(13)),
            length: PayloadBytes::new(1),
        },
        costs: Costs {
            payload_bytes: PayloadBytes::new(1),
            references: References::new(1),
            exchange_slots: ExchangeSlots::new(1),
            receipt_slots: ReceiptSlots::new(1),
        },
        targets: mqttiumd_contract::command::TargetVec::from_slice(
            &[Target {
                session: SessionId::new(14),
                session_revision: Revision::new(15),
                delivery: DeliveryId {
                    publication: publication(),
                    index: 0,
                },
                effective_qos: Qos::AtLeastOnce,
                cause: DeliveryCause::LiveExact,
            }],
            16,
        )
        .unwrap(),
    }
}
fn qos0() -> RetainedQos0 {
    let a = admission(Qos::AtMostOnce, true);
    RetainedQos0 {
        operation: a.operation,
        request: a.request,
        publication: a.publication,
        session: a.source.session,
        session_revision: a.source.session_revision,
        connection: ConnectionGeneration::new(16),
        ingress_order: 17,
        route_revision: a.route_revision,
        topic: a.topic,
        payload: a.payload,
        costs: a.costs,
        targets: a.targets.into_bounded(),
        retain_targets: mqttiumd_contract::retained::RetainMask::from_u16(1),
    }
}
fn replay() -> ReplaySubscriptions {
    ReplaySubscriptions {
        update: UpdateSubscriptions {
            operation: operation(4),
            session: SessionId::new(8),
            session_revision: Revision::new(9),
            route_revision: Revision::new(12),
            changes: BoundedVec::from_slice(
                &[GrantedSubscriptionChange {
                    topic: SharedBytes::copy_from(b"t", 1).unwrap(),
                    granted: Some(Qos::AtLeastOnce),
                    options: SubscriptionOptions::DEFAULT,
                }],
                8,
            )
            .unwrap(),
        },
        first_publication: publication(),
    }
}
fn finish() -> FinishDelivery {
    FinishDelivery {
        operation: operation(18),
        delivery: DeliveryId {
            publication: publication(),
            index: 0,
        },
        target: SessionId::new(14),
        target_revision: Revision::new(15),
        packet_id: OutboundPacketId::new(19).unwrap(),
        claim: operation(20),
    }
}
fn encode(recipe: &Recipe) -> Vec<u8> {
    let mut bytes = Vec::new();
    recipe.encode_canonical(&mut bytes).unwrap();
    bytes
}
fn assert_strict(recipe: &Recipe) -> Vec<u8> {
    let bytes = encode(recipe);
    assert_eq!(Recipe::decode_canonical(&bytes, 16, 1).unwrap(), *recipe);
    for cut in 0..bytes.len() {
        assert!(
            Recipe::decode_canonical(&bytes[..cut], 16, 1).is_err(),
            "accepted prefix {cut}"
        );
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert_eq!(
        Recipe::decode_canonical(&trailing, 16, 1),
        Err(RecipeError::TrailingBytes)
    );
    bytes
}
fn envelope(tag: u8, nested: &[u8], timed: bool) -> Vec<u8> {
    let mut bytes = b"MQDR\0\x01".to_vec();
    bytes.push(tag);
    bytes.extend_from_slice(&(nested.len() as u32).to_be_bytes());
    bytes.extend_from_slice(nested);
    bytes.extend_from_slice(&[7; 32]);
    bytes.extend_from_slice(&100u64.to_be_bytes());
    if timed {
        bytes.extend_from_slice(&2u32.to_be_bytes());
    }
    bytes
}

#[test]
fn legacy_admission_and_retained_layouts_have_independent_byte_expectations() {
    fn scoped(out: &mut Vec<u8>, sequence: u64) {
        for n in [1u64, 2, 3, sequence] {
            out.extend_from_slice(&n.to_be_bytes());
        }
    }
    fn tail(out: &mut Vec<u8>, qos0: bool) {
        out.extend_from_slice(&[0, 1, b't']);
        scoped(out, 13);
        for _ in 0..5 {
            out.extend_from_slice(&1u64.to_be_bytes());
        }
        out.extend_from_slice(if qos0 { &[1] } else { &[0, 1] });
        out.extend_from_slice(&14u64.to_be_bytes());
        out.extend_from_slice(&15u64.to_be_bytes());
        out.extend_from_slice(&[0, 0, 0, 0, 1, 1]);
    }
    let mut a = b"MQDR\0\x01\x01".to_vec();
    for sequence in [4, 6, 5] {
        scoped(&mut a, sequence);
    }
    a.extend_from_slice(&8u64.to_be_bytes());
    a.extend_from_slice(&9u64.to_be_bytes());
    a.extend_from_slice(&10u16.to_be_bytes());
    a.extend_from_slice(&11u64.to_be_bytes());
    a.extend_from_slice(&12u64.to_be_bytes());
    a.extend_from_slice(&[1, 0]);
    tail(&mut a, false);
    assert_eq!(
        encode(&Recipe::AdmitPublication(admission(
            Qos::AtLeastOnce,
            false
        ))),
        a
    );
    let mut retained_a = a;
    retained_a[138] = 1;
    let mut retained = b"MQDR\0\x01\x0a".to_vec();
    retained.extend_from_slice(&(retained_a.len() as u32).to_be_bytes());
    retained.extend_from_slice(&retained_a);
    retained.extend_from_slice(&1u16.to_be_bytes());
    assert_eq!(
        encode(&Recipe::RetainedPublication(RetainedPublication {
            admission: admission(Qos::AtLeastOnce, true),
            retain_targets: mqttiumd_contract::retained::RetainMask::from_u16(1)
        })),
        retained
    );
    let mut q0 = b"MQDR\0\x01\x0b".to_vec();
    for sequence in [4, 6, 5] {
        scoped(&mut q0, sequence);
    }
    for n in [8u64, 9, 16, 17, 12] {
        q0.extend_from_slice(&n.to_be_bytes());
    }
    tail(&mut q0, true);
    q0.extend_from_slice(&1u16.to_be_bytes());
    assert_eq!(encode(&Recipe::RetainedQos0(qos0())), q0);
    let mut update = b"MQDR\0\x01\x07".to_vec();
    scoped(&mut update, 4);
    for n in [8u64, 9, 12] {
        update.extend_from_slice(&n.to_be_bytes());
    }
    update.extend_from_slice(&[1, 2, 0, 1, b't']);
    let mut replay_bytes = b"MQDR\0\x01\x0c".to_vec();
    replay_bytes.extend_from_slice(&(update.len() as u32).to_be_bytes());
    replay_bytes.extend_from_slice(&update);
    scoped(&mut replay_bytes, 5);
    assert_eq!(encode(&Recipe::ReplaySubscriptions(replay())), replay_bytes);
}

#[test]
fn tag13_preserves_each_original_admission_and_strict_bounds() {
    for qos in [Qos::AtLeastOnce, Qos::ExactlyOnce] {
        for retain in [false, true] {
            let legacy = if retain {
                Recipe::RetainedPublication(RetainedPublication {
                    admission: admission(qos, retain),
                    retain_targets: mqttiumd_contract::retained::RetainMask::from_u16(1),
                })
            } else {
                Recipe::AdmitPublication(admission(qos, retain))
            };
            let nested = encode(&legacy);
            let recipe = Recipe::ExpiringPublication(ExpiringPublication {
                admission: ExpiringAdmission::Qos12(RetainedPublication {
                    admission: admission(qos, retain),
                    retain_targets: mqttiumd_contract::retained::RetainMask::from_u16(u16::from(
                        retain,
                    )),
                }),
                expiry: expiry(),
            });
            assert_eq!(assert_strict(&recipe), envelope(13, &nested, true));
            assert_eq!(recipe.admission(), legacy.admission());
            assert!(recipe.retained_qos0().is_none());
            assert_eq!(recipe.operation(), legacy.operation());
            assert_eq!(ReceiptClass::of(&recipe), ReceiptClass::Business);
            assert!(Recipe::decode_canonical(&encode(&recipe), 0, 1).is_err());
            assert!(Recipe::decode_canonical(&encode(&recipe), 16, 0).is_err());
        }
    }
    let legacy = Recipe::RetainedQos0(qos0());
    let recipe = Recipe::ExpiringPublication(ExpiringPublication {
        admission: ExpiringAdmission::Qos0(qos0()),
        expiry: expiry(),
    });
    assert_eq!(assert_strict(&recipe), envelope(13, &encode(&legacy), true));
    assert_eq!(recipe.retained_qos0(), legacy.retained_qos0());
    assert!(recipe.admission().is_none());
}

#[test]
fn temporal_wrappers_do_not_admit_recursive_or_noncanonical_aliases() {
    let recipe = Recipe::ExpiringPublication(ExpiringPublication {
        admission: ExpiringAdmission::Qos0(qos0()),
        expiry: expiry(),
    });
    for tag in [2, 7, 8, 9, 12, 13, 14, 15, 16, 255] {
        let mut bytes = encode(&recipe);
        bytes[17] = tag;
        assert!(Recipe::decode_canonical(&bytes, 16, 1).is_err());
    }
    // Retained QoS1/2 is canonical only through nested tag10, never tag1.
    let alias = envelope(
        13,
        &encode(&Recipe::AdmitPublication(admission(Qos::AtLeastOnce, true))),
        true,
    );
    assert!(Recipe::decode_canonical(&alias, 16, 1).is_err());
    let bad_qos = Recipe::ExpiringPublication(ExpiringPublication {
        admission: ExpiringAdmission::Qos12(RetainedPublication {
            admission: admission(Qos::AtMostOnce, false),
            retain_targets: mqttiumd_contract::retained::RetainMask::EMPTY,
        }),
        expiry: expiry(),
    });
    assert!(bad_qos.check().is_err());
    let bad_mask = Recipe::ExpiringPublication(ExpiringPublication {
        admission: ExpiringAdmission::Qos12(RetainedPublication {
            admission: admission(Qos::AtLeastOnce, false),
            retain_targets: mqttiumd_contract::retained::RetainMask::from_u16(1),
        }),
        expiry: expiry(),
    });
    assert!(bad_mask.check().is_err());
    let mut overflow = encode(&recipe);
    let tick = overflow.len() - 12;
    overflow[tick..tick + 8].copy_from_slice(&u64::MAX.to_be_bytes());
    assert!(Recipe::decode_canonical(&overflow, 16, 1).is_err());
}

#[test]
fn tag14_replays_inherit_the_frozen_observation_and_legacy_selection_recipe() {
    let legacy = Recipe::ReplaySubscriptions(replay());
    let recipe = Recipe::TimedReplaySubscriptions(TimedReplaySubscriptions {
        replay: replay(),
        observed: time(100),
    });
    let bytes = assert_strict(&recipe);
    assert_eq!(bytes, envelope(14, &encode(&legacy), false));
    assert_eq!(recipe.replay_subscriptions(), legacy.replay_subscriptions());
    assert_eq!(recipe.operation(), legacy.operation());
    assert_eq!(ReceiptClass::of(&recipe), ReceiptClass::Business);
    for tag in [1, 10, 11, 13, 14, 15, 16] {
        let mut invalid = bytes.clone();
        invalid[17] = tag;
        assert!(Recipe::decode_canonical(&invalid, 16, 1).is_err());
    }
}

#[test]
fn exact_claim_guard_is_preserved_by_started_and_expired_recipes() {
    let f = finish();
    let started = Recipe::MarkDeliveryStarted(MarkDeliveryStarted {
        operation: f.operation,
        delivery: f.delivery,
        target: f.target,
        target_revision: f.target_revision,
        packet_id: f.packet_id,
        claim: f.claim,
    });
    let mut expected = encode(&Recipe::FinishDelivery(f));
    expected[6] = 15;
    assert_eq!(assert_strict(&started), expected);
    assert_eq!(ReceiptClass::of(&started), ReceiptClass::Control);
    let observed = time(expiry().deadline().unwrap());
    let expired = Recipe::ExpireDelivery(ExpireDelivery {
        finish: f,
        expiry: expiry(),
        observed,
    });
    let bytes = assert_strict(&expired);
    expected[6] = 16;
    expected.extend_from_slice(&[7; 32]);
    expected.extend_from_slice(&100u64.to_be_bytes());
    expected.extend_from_slice(&2u32.to_be_bytes());
    expected.extend_from_slice(&[7; 32]);
    expected.extend_from_slice(&observed.tick.to_be_bytes());
    assert_eq!(bytes, expected);
    assert_eq!(ReceiptClass::of(&expired), ReceiptClass::Control);
    assert_eq!(expired.operation(), f.operation);
    for observation in [
        time(99),
        time(100),
        MessageTime {
            domain: [8; 32],
            ..observed
        },
    ] {
        assert!(
            Recipe::ExpireDelivery(ExpireDelivery {
                finish: f,
                expiry: expiry(),
                observed: observation
            })
            .check()
            .is_err()
        );
    }
    for recipe in [started, expired] {
        let mut zero = encode(&recipe);
        zero[91..93].fill(0);
        assert!(Recipe::decode_canonical(&zero, 16, 1).is_err());
    }
}

#[test]
fn changing_temporal_metadata_changes_the_exact_frozen_recipe() {
    let mut encodings = Vec::new();
    for value in [
        expiry(),
        MessageExpiry {
            interval: 3,
            ..expiry()
        },
        MessageExpiry {
            observed: time(101),
            ..expiry()
        },
        MessageExpiry {
            observed: MessageTime {
                domain: [8; 32],
                ..time(100)
            },
            ..expiry()
        },
    ] {
        let recipe = Recipe::ExpiringPublication(ExpiringPublication {
            admission: ExpiringAdmission::Qos0(qos0()),
            expiry: value,
        });
        let bytes = encode(&recipe);
        assert!(!encodings.contains(&bytes));
        encodings.push(bytes);
    }
}
