use mqttiumd_contract::{
    capacity::*, command::*, ids::*, packet::SharedBytes, receipts::ReceiptClass, session::*,
    will::*,
};
fn op(n: u64) -> OperationId {
    OperationId::new(
        StoreIncarnation::new(1),
        RunEpoch::new(2),
        LogicalOwnerId::new(3),
        OperationSeq::new(n),
    )
}
fn bytes(value: &[u8]) -> SharedBytes {
    SharedBytes::copy_from(value, 32768).unwrap()
}
#[test]
fn will_open_end_and_publication_have_distinct_strict_roundtrips() {
    let spec = WillSpec {
        retain: false,
        topic: bytes(b"atelier/capteur/status"),
        payload: bytes(b"offline"),
    };
    let guard = SessionGuard {
        session: SessionId::new(4),
        opened_by: Some(op(1)),
    };
    let mut recipes = vec![
        Recipe::SessionLifecycle(SessionLifecycle {
            operation: op(1),
            action: SessionAction::OpenWillAt {
                client_id: bytes(b"capteur"),
                expected: None,
                clean_start: false,
                expiry: SessionExpiry::OnDisconnect,
                now: 0,
                will: Some(spec.clone()),
            },
        }),
        Recipe::SessionLifecycle(SessionLifecycle {
            operation: op(2),
            action: SessionAction::EndWill {
                guard,
                expiry: SessionExpiry::OnDisconnect,
                observed_at: 5,
                publish: true,
            },
        }),
    ];
    recipes.push(Recipe::WillPublication(WillPublication {
        live_retain: None,
        operation: op(3),
        will: op(1),
        publication: PublicationId::new(
            StoreIncarnation::new(1),
            RunEpoch::new(2),
            LogicalOwnerId::new(3),
            PublicationSeq::new(1),
        ),
        route_revision: Revision::new(1),
        topic: spec.topic.clone(),
        payload: PayloadRef {
            id: PayloadId::new(
                StoreIncarnation::new(1),
                RunEpoch::new(2),
                LogicalOwnerId::new(3),
                PayloadSeq::new(1),
            ),
            length: PayloadBytes::new(7),
        },
        costs: Costs {
            payload_bytes: PayloadBytes::new(0),
            references: References::new(0),
            exchange_slots: ExchangeSlots::new(0),
            receipt_slots: ReceiptSlots::new(1),
        },
        targets: BoundedVec::with_capacity(0).unwrap(),
    }));
    for (i, recipe) in recipes.iter().enumerate() {
        let mut raw = vec![];
        recipe.encode_canonical(&mut raw).unwrap();
        assert_eq!(raw[6], if i == 2 { 17 } else { 8 });
        assert_eq!(Recipe::decode_canonical(&raw, 16, 1024).unwrap(), *recipe);
        if i == 0 {
            assert_eq!(raw[39], 5);
            raw[39] = 7;
            let retained = Recipe::decode_canonical(&raw, 16, 1024).unwrap();
            assert!(
                matches!(&retained, Recipe::SessionLifecycle(SessionLifecycle {
                action: SessionAction::OpenWillAt { will: Some(value), .. }, ..
            }) if value.retain)
            );
            let mut encoded = vec![];
            retained.encode_canonical(&mut encoded).unwrap();
            assert_eq!(raw, encoded);
        } else if i == 2 {
            raw[6] = 18;
            raw.extend_from_slice(&[0, 0]);
            let retained = Recipe::decode_canonical(&raw, 16, 1024).unwrap();
            assert!(
                matches!(&retained, Recipe::WillPublication(value) if value.live_retain == Some(0))
            );
            let mut encoded = vec![];
            retained.encode_canonical(&mut encoded).unwrap();
            assert_eq!(raw, encoded);
            *raw.last_mut().unwrap() = 1;
            assert_eq!(
                Recipe::decode_canonical(&raw, 16, 1024),
                Err(RecipeError::InvalidField("will_live_retain"))
            );
            *raw.last_mut().unwrap() = 0;
        }
        for n in 0..raw.len() {
            assert!(Recipe::decode_canonical(&raw[..n], 16, 1024).is_err());
        }
        assert_eq!(
            ReceiptClass::of(recipe),
            if i == 0 {
                ReceiptClass::Business
            } else {
                ReceiptClass::Control
            }
        );
    }
    let debug = format!("{spec:?}");
    assert!(!debug.contains("offline"));
    assert!(!debug.contains("atelier"));
}
#[test]
fn invalid_will_is_rejected_before_encoding() {
    for topic in [b"".as_slice(), b"a/+", b"a/#", b"a\0b", b"\xff"] {
        assert!(
            WillSpec {
                retain: false,
                topic: bytes(topic),
                payload: bytes(b"secret")
            }
            .check()
            .is_err()
        );
    }
    let payload = vec![1; MAX_WILL_PAYLOAD_BYTES + 1];
    assert!(
        WillSpec {
            retain: false,
            topic: bytes(b"t"),
            payload: SharedBytes::copy_from(&payload, payload.len()).unwrap()
        }
        .check()
        .is_err()
    );
}
