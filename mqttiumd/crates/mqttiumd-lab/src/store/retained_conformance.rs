//! The same retained ownership, replay and retirement checks on both writers.
use super::conformance::{INCARNATION, World, fixture_limits, operation_id, publication_id};
use super::durable::*;
use mqttiumd_contract::capacity::{
    BoundedVec, ExchangeSlots, PayloadBytes, ReceiptSlots, References,
};
use mqttiumd_contract::command::{
    AdmitPublication, Command, Costs, DeliveryCause, FrozenRecipe, PayloadRef, Recipe, Source,
    Target, UpdateSubscriptions,
};
use mqttiumd_contract::ids::{
    ConnectionGeneration, DeliveryId, ExchangeGeneration, ExchangeId, InboundPacketId,
    LogicalOwnerId, PayloadId, PayloadSeq, RequestId, RequestSeq, Revision, SessionId,
    StoreIncarnation,
};
use mqttiumd_contract::outcome::{CommitFacts, RejectReason, StoreOutcome, StoreReply};
use mqttiumd_contract::packet::{Qos, SharedBytes};
use mqttiumd_contract::pubsub::{GrantedSubscriptionChange, SubscriptionOptions};
use mqttiumd_contract::receipts::{
    ReceiptClosure, ReceiptCollection, ReceiptCollectionReply, ReceiptOrigin,
};
use mqttiumd_contract::retained::{ReplaySubscriptions, RetainedPublication, RetainedQos0};

fn limits() -> Limits {
    let mut l = fixture_limits();
    l.payload_bytes = PayloadBytes::new(4 * 1024 * 1024);
    l.references = References::new(128);
    l.exchange_slots = ExchangeSlots::new(128);
    l.receipt_slots = ReceiptSlots::new(8192);
    l.max_payload_bytes = 32768;
    l.max_sessions = 128;
    l
}
fn bytes(value: &[u8]) -> SharedBytes {
    SharedBytes::copy_from(value, 65535).unwrap()
}
fn frozen(recipe: Recipe, payload: Option<&[u8]>) -> Command {
    Command {
        recipe: FrozenRecipe::freeze(recipe).unwrap(),
        payload: payload.map(bytes),
    }
}
fn admitted<S: DurableStore>(
    world: &World<S>,
    seq: u64,
    topic: &[u8],
    payload: &[u8],
    qos: Qos,
    targets: &[SessionId],
) -> Command {
    let operation = operation_id(world.epoch, seq);
    let publication = publication_id(world.epoch, seq);
    let payload_ref = PayloadRef {
        id: PayloadId::new(
            INCARNATION,
            world.epoch,
            LogicalOwnerId::new(0),
            PayloadSeq::new(seq),
        ),
        length: PayloadBytes::new(payload.len() as u64),
    };
    let request = RequestId::new(
        INCARNATION,
        world.epoch,
        LogicalOwnerId::new(0),
        RequestSeq::new(seq),
    );
    let costs = Costs {
        payload_bytes: payload_ref.length,
        references: References::new(targets.len() as u64 + u64::from(!payload.is_empty())),
        exchange_slots: ExchangeSlots::new(u64::from(qos == Qos::ExactlyOnce)),
        receipt_slots: ReceiptSlots::new(1),
    };
    let targets = BoundedVec::from_slice(
        &targets
            .iter()
            .enumerate()
            .map(|(index, target)| Target {
                session: *target,
                session_revision: Revision::new(1),
                delivery: DeliveryId {
                    publication,
                    index: index as u32,
                },
                effective_qos: qos.min(Qos::AtLeastOnce),
                cause: DeliveryCause::LiveExact,
            })
            .collect::<Vec<_>>(),
        16,
    )
    .unwrap();
    let recipe = if qos == Qos::AtMostOnce {
        Recipe::RetainedQos0(RetainedQos0 {
            operation,
            request,
            publication,
            session: world.producer,
            session_revision: Revision::new(1),
            connection: ConnectionGeneration::new(1),
            ingress_order: seq,
            route_revision: world.store.snapshot().unwrap().route_revision,
            topic: bytes(topic),
            payload: payload_ref,
            costs,
            targets,
            retain_targets: 0,
        })
    } else {
        Recipe::RetainedPublication(RetainedPublication {
            admission: AdmitPublication {
                operation,
                request,
                publication,
                source: Source {
                    session: world.producer,
                    session_revision: Revision::new(1),
                    packet_id: InboundPacketId::new(seq as u16).unwrap(),
                    exchange: ExchangeId {
                        session: world.producer,
                        generation: ExchangeGeneration::new(seq),
                    },
                },
                route_revision: world.store.snapshot().unwrap().route_revision,
                qos,
                retain: true,
                topic: bytes(topic),
                payload: payload_ref,
                costs,
                targets,
            },
            retain_targets: 0,
        })
    };
    frozen(recipe, Some(payload))
}
fn subscribe<S: DurableStore>(
    world: &World<S>,
    seq: u64,
    session: SessionId,
    first: u64,
    changes: &[(&[u8], Qos, u8)],
) -> Command {
    frozen(
        Recipe::ReplaySubscriptions(ReplaySubscriptions {
            update: UpdateSubscriptions {
                operation: operation_id(world.epoch, seq),
                session,
                session_revision: Revision::new(1),
                route_revision: world.store.snapshot().unwrap().route_revision,
                changes: BoundedVec::from_slice(
                    &changes
                        .iter()
                        .map(|(topic, qos, options)| GrantedSubscriptionChange {
                            topic: bytes(topic),
                            granted: Some(*qos),
                            options: SubscriptionOptions::from_bits(*options).unwrap(),
                        })
                        .collect::<Vec<_>>(),
                    8,
                )
                .unwrap(),
            },
            first_publication: publication_id(world.epoch, first),
        }),
        None,
    )
}
fn commit<S: DurableStore>(store: &mut S, command: &Command) -> CommitFacts {
    let reply = store.apply(command);
    assert!(
        reply.committed_now,
        "{reply:?} recipe={:?}",
        command.recipe.get()
    );
    match reply.reply {
        StoreReply::Outcome(StoreOutcome::Committed(facts)) => facts,
        other => panic!("not committed: {other:?}"),
    }
}
fn consume<S: DurableStore>(
    world: &mut World<S>,
    seq: u64,
    delivery: DeliveryId,
    target: SessionId,
) {
    commit(
        &mut world.store,
        &frozen(
            Recipe::ConsumeDelivery(mqttiumd_contract::command::ConsumeDelivery {
                operation: operation_id(world.epoch, seq),
                delivery,
                target,
                target_revision: Revision::new(1),
            }),
            None,
        ),
    );
}

pub fn retained_survives_without_targets_and_producer<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    for qos in [Qos::AtMostOnce, Qos::AtLeastOnce, Qos::ExactlyOnce] {
        let mut w = World::with_limits(make, limits());
        let command = admitted(&w, 1, b"atelier/a/temperature", b"21", qos, &[]);
        commit(&mut w.store, &command);
        w.store
            .admin(AdminCommand::DeleteSession {
                session: w.producer,
            })
            .unwrap();
        let state = w.store.snapshot().unwrap();
        assert_eq!(state.retained.len(), 1, "retained_depends_on_subscriber");
        assert_eq!(state.payloads.len(), 1);
        assert_eq!(state.accounts.references.get(), 1);
        assert_eq!(state.accounts.payload_bytes.get(), 2);
        assert!(state.inbound.is_empty());
        let sub = subscribe(
            &w,
            2,
            w.b,
            1000,
            &[(b"atelier/+/temperature", Qos::AtLeastOnce, 0)],
        );
        commit(&mut w.store, &sub);
        let page = w.store.read_work(w.b, None, 8).unwrap();
        assert_eq!(page.items.len(), 1);
        let item = &page.items.as_slice()[0];
        assert_eq!(item.payload.as_slice(), b"21");
        assert!(item.retain);
        assert_eq!(item.qos, qos.min(Qos::AtLeastOnce));
    }
}

pub fn retained_replacement_clear_and_replay_freeze<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let mut w = World::with_limits(make, limits());
    let a = admitted(&w, 1, b"atelier/x", b"old", Qos::ExactlyOnce, &[]);
    let original = commit(&mut w.store, &a);
    let sub = subscribe(&w, 2, w.b, 1000, &[(b"atelier/#", Qos::AtMostOnce, 0)]);
    commit(&mut w.store, &sub);
    let b = admitted(&w, 3, b"atelier/x", b"new", Qos::AtLeastOnce, &[]);
    commit(&mut w.store, &b);
    // An old known operation and its resolver return knowledge, never another mutation.
    assert_eq!(
        w.store.apply(&a).reply,
        StoreReply::Outcome(StoreOutcome::Committed(original))
    );
    assert_eq!(
        w.store.resolve_and_fence(a.recipe.operation()).outcome,
        StoreOutcome::Committed(original)
    );
    let release = w.release(4, 1, 1);
    commit(&mut w.store, &release);
    let state = w.store.snapshot().unwrap();
    assert_eq!(
        state.retained[b"atelier/x".as_slice()].publication,
        publication_id(w.epoch, 3),
        "old_retained_operation_remutated"
    );
    let page = w.store.read_work(w.b, None, 8).unwrap();
    assert_eq!(
        page.items.as_slice()[0].payload.as_slice(),
        b"old",
        "replay_reread_current_retained"
    );
    let clear = admitted(&w, 5, b"atelier/x", b"", Qos::AtLeastOnce, &[]);
    commit(&mut w.store, &clear);
    let state = w.store.snapshot().unwrap();
    assert!(state.retained.is_empty());
    assert_eq!(state.payloads.len(), 1, "clear_released_owed_payload");
    assert_eq!(state.accounts.references.get(), 1);
    assert_eq!(state.accounts.payload_bytes.get(), 3);
    let item = &page.items.as_slice()[0];
    let b = w.b;
    consume(&mut w, 6, item.delivery, b);
    let state = w.store.snapshot().unwrap();
    assert!(state.publications.is_empty());
    assert!(state.payloads.is_empty());
    assert_eq!(state.accounts.references.get(), 0);
    assert_eq!(state.accounts.payload_bytes.get(), 0);
}

pub fn thirty_two_topics_and_ordered_options<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let mut w = World::with_limits(make, limits());
    for n in 1..=32 {
        let command = admitted(
            &w,
            n,
            format!("atelier/{n:02}").as_bytes(),
            &[n as u8],
            Qos::AtLeastOnce,
            &[],
        );
        commit(&mut w.store, &command);
    }
    let sub = subscribe(&w, 33, w.b, 1000, &[(b"atelier/#", Qos::AtMostOnce, 0)]);
    commit(&mut w.store, &sub);
    let state = w.store.snapshot().unwrap();
    assert_eq!(state.retained.len(), 32);
    assert_eq!(
        state.obligations.len(),
        32,
        "replay_was_truncated_to_fanout_limit"
    );
    assert_eq!(state.accounts.references.get(), 64);
    let mut after = None;
    let mut seen = 0;
    let mut deliveries = Vec::new();
    loop {
        let page = w.store.read_work(w.b, after, 8).unwrap();
        for item in page.items.as_slice() {
            assert!(item.retain);
            assert_eq!(item.qos, Qos::AtMostOnce);
            assert_eq!(
                state.publications[&item.delivery.publication].retained_version,
                Some(publication_id(
                    w.epoch,
                    u64::from(item.payload.as_slice()[0])
                ))
            );
            deliveries.push(item.delivery);
            seen += 1;
        }
        after = page.next;
        if after.is_none() {
            break;
        }
    }
    assert_eq!(seen, 32);
    let b = w.b;
    for (index, delivery) in deliveries.iter().enumerate() {
        consume(&mut w, 34 + index as u64, *delivery, b);
    }
    // The unmaterialized tail of the replay range remains protected before GC.
    let mut collision = admitted(&w, 66, b"unused", b"bad", Qos::AtLeastOnce, &[]);
    let mut raw = Vec::new();
    collision.recipe.get().encode_canonical(&mut raw).unwrap();
    let Recipe::RetainedPublication(mut value) = Recipe::decode_canonical(&raw, 16, 256).unwrap()
    else {
        panic!()
    };
    value.admission.publication = publication_id(w.epoch, 1255);
    collision.recipe = FrozenRecipe::freeze(Recipe::RetainedPublication(value)).unwrap();
    assert_eq!(
        w.store.apply(&collision).reply,
        StoreReply::Outcome(StoreOutcome::RejectedCertain(
            RejectReason::PreconditionFailed
        )),
        "unused_replay_identity_reused"
    );
    let collection = ReceiptCollection {
        closure: ReceiptClosure {
            origin: ReceiptOrigin::of(operation_id(w.epoch, 1)),
            through: operation_id(w.epoch, 66).sequence(),
            publication_through: publication_id(w.epoch, 1254).sequence(),
            payload_through: PayloadSeq::new(66),
        },
        limit: 128,
    };
    assert!(
        matches!(
            w.store.collect_receipts(collection).unwrap(),
            ReceiptCollectionReply::Refused(_)
        ),
        "replay_range_not_closed_before_receipt_deletion"
    );
    let closure = ReceiptClosure {
        publication_through: publication_id(w.epoch, 1255).sequence(),
        ..collection.closure
    };
    assert!(matches!(
        w.store
            .collect_receipts(ReceiptCollection {
                closure,
                ..collection
            })
            .unwrap(),
        ReceiptCollectionReply::Applied { .. }
    ));
    assert_eq!(
        w.store.apply(&sub).reply,
        StoreReply::Outcome(StoreOutcome::RetiredOutcomeUnavailable)
    );
    let other = admitted(&w, 67, b"fresh", b"fresh", Qos::AtLeastOnce, &[]);
    assert_eq!(
        w.store.apply(&other).reply,
        StoreReply::Outcome(StoreOutcome::RejectedCertain(
            RejectReason::PreconditionFailed
        )),
        "old_publication_id_reexecuted_after_collection"
    );
    // A new valid range may be issued above the retired publication frontier.
    let sub2 = subscribe(
        &w,
        68,
        w.c,
        2000,
        &[
            (b"atelier/#", Qos::AtMostOnce, 0x20),
            (b"atelier/#", Qos::AtMostOnce, 0x10),
            (b"atelier/#", Qos::AtMostOnce, 0),
        ],
    );
    commit(&mut w.store, &sub2);
    let state = w.store.snapshot().unwrap();
    assert_eq!(
        state.obligations.len(),
        32,
        "ordered_RH_steps_not_preserved"
    );
    assert_eq!(state.accounts.references.get(), 64);
}

pub fn quota_full_replacement_and_no_local_client_identity<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let mut w = World::with_limits(make, limits());
    for n in 1..=32 {
        let c = admitted(
            &w,
            n,
            format!("v/{n}").as_bytes(),
            b"old",
            Qos::AtLeastOnce,
            &[],
        );
        commit(&mut w.store, &c);
    }
    let denied = admitted(&w, 33, b"v/33", b"extra", Qos::AtLeastOnce, &[]);
    assert!(matches!(
        w.store.apply(&denied).reply,
        StoreReply::Outcome(StoreOutcome::RejectedCertain(
            RejectReason::CapacityExceeded(_)
        ))
    ));
    let replacement = admitted(&w, 34, b"v/1", b"replacement", Qos::AtLeastOnce, &[]);
    commit(&mut w.store, &replacement);
    let clear = admitted(&w, 35, b"v/2", b"", Qos::AtLeastOnce, &[]);
    commit(&mut w.store, &clear);
    let fresh = admitted(&w, 36, b"v/33", b"fresh", Qos::AtLeastOnce, &[]);
    commit(&mut w.store, &fresh);
    w.store
        .admin(AdminCommand::DeleteSession {
            session: w.producer,
        })
        .unwrap();
    let AdminDone::SessionCreated {
        session: new_producer,
        ..
    } = w
        .store
        .admin(AdminCommand::CreateSession {
            client_id: b"p1-producer".to_vec(),
        })
        .unwrap()
    else {
        panic!()
    };
    assert_ne!(new_producer, w.producer);
    let local = subscribe(&w, 37, new_producer, 1000, &[(b"v/#", Qos::AtMostOnce, 4)]);
    commit(&mut w.store, &local);
    assert!(
        w.store
            .read_work(new_producer, None, 8)
            .unwrap()
            .items
            .is_empty(),
        "no_local_forgot_producer_client_id"
    );
    let other = subscribe(
        &w,
        38,
        w.b,
        2000,
        &[
            (b"v/#", Qos::AtMostOnce, 0x10),
            (b"v/#", Qos::AtMostOnce, 0x10),
        ],
    );
    commit(&mut w.store, &other);
    assert_eq!(
        w.store.snapshot().unwrap().obligations.len(),
        32,
        "repeated_RH1_replayed_existing_filter"
    );
}

pub fn retained_topic_names_are_not_subscription_filters<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    for qos in [Qos::AtMostOnce, Qos::AtLeastOnce, Qos::ExactlyOnce] {
        let mut w = World::with_limits(make, limits());
        let command = admitted(&w, 1, b"$share/literal", b"state", qos, &[]);
        commit(&mut w.store, &command);
        let snapshot = w.store.snapshot();
        assert!(
            snapshot.is_ok(),
            "retained_topic_name_confused_with_shared_filter: {snapshot:?}"
        );
        assert_eq!(snapshot.unwrap().retained.len(), 1);
    }
}

fn with_ids(
    command: &Command,
    publication: Option<mqttiumd_contract::ids::PublicationId>,
    payload: Option<PayloadId>,
    different: bool,
) -> Command {
    let mut bytes = Vec::new();
    command.recipe.get().encode_canonical(&mut bytes).unwrap();
    let mut recipe = Recipe::decode_canonical(&bytes, 16, 65535).unwrap();
    match &mut recipe {
        Recipe::RetainedPublication(v) => {
            if let Some(id) = publication {
                v.admission.publication = id;
            }
            if let Some(id) = payload {
                v.admission.payload.id = id;
            }
            if different {
                v.admission.topic = super::retained_conformance::bytes(b"different");
            }
        }
        Recipe::RetainedQos0(v) => {
            if let Some(id) = publication {
                v.publication = id;
            }
            if let Some(id) = payload {
                v.payload.id = id;
            }
            if different {
                v.topic = super::retained_conformance::bytes(b"different");
            }
        }
        _ => panic!(),
    }
    Command {
        recipe: FrozenRecipe::freeze(recipe).unwrap(),
        payload: command.payload.clone(),
    }
}

pub fn collected_retained_identities_never_execute_again<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    for qos in [Qos::AtMostOnce, Qos::AtLeastOnce] {
        let mut w = World::with_limits(make, limits());
        let a = admitted(&w, 1, b"retired", b"old", qos, &[]);
        let original = commit(&mut w.store, &a);
        let clear = admitted(&w, 2, b"retired", b"", qos, &[]);
        commit(&mut w.store, &clear);
        assert_eq!(
            w.store.apply(&a).reply,
            StoreReply::Outcome(StoreOutcome::Committed(original))
        );
        let state = w.store.snapshot().unwrap();
        assert!(state.retained.is_empty());
        assert!(state.publications.is_empty());
        assert!(state.payloads.is_empty());
        let same_pub = with_ids(
            &admitted(&w, 3, b"bad-pub", b"new", qos, &[]),
            Some(publication_id(w.epoch, 1)),
            None,
            false,
        );
        let old_payload = PayloadId::new(
            INCARNATION,
            w.epoch,
            LogicalOwnerId::new(0),
            PayloadSeq::new(1),
        );
        let same_payload = with_ids(
            &admitted(&w, 4, b"bad-payload", b"new", qos, &[]),
            None,
            Some(old_payload),
            false,
        );
        for command in [&same_pub, &same_payload] {
            assert_eq!(
                w.store.apply(command).reply,
                StoreReply::Outcome(StoreOutcome::RejectedCertain(
                    RejectReason::PreconditionFailed
                )),
                "retained_identity_reused_before_collection"
            );
        }
        let closure = ReceiptClosure {
            origin: ReceiptOrigin::of(operation_id(w.epoch, 1)),
            through: operation_id(w.epoch, 4).sequence(),
            publication_through: publication_id(w.epoch, 4).sequence(),
            payload_through: PayloadSeq::new(4),
        };
        assert!(matches!(
            w.store
                .collect_receipts(ReceiptCollection {
                    closure,
                    limit: 128
                })
                .unwrap(),
            ReceiptCollectionReply::Applied { .. }
        ));
        for old in [&a, &with_ids(&a, None, None, true)] {
            assert_eq!(
                w.store.apply(old).reply,
                StoreReply::Outcome(StoreOutcome::RetiredOutcomeUnavailable),
                "retired_retained_operation_reexecuted"
            );
        }
        let same_pub = with_ids(
            &admitted(&w, 5, b"bad-pub", b"new", qos, &[]),
            Some(publication_id(w.epoch, 1)),
            None,
            false,
        );
        let same_payload = with_ids(
            &admitted(&w, 6, b"bad-payload", b"new", qos, &[]),
            Some(publication_id(w.epoch, 100)),
            Some(old_payload),
            false,
        );
        for command in [&same_pub, &same_payload] {
            assert_eq!(
                w.store.apply(command).reply,
                StoreReply::Outcome(StoreOutcome::RejectedCertain(
                    RejectReason::PreconditionFailed
                )),
                "retained_identity_reused_after_collection"
            );
        }
        let fresh = with_ids(
            &admitted(&w, 7, b"fresh", b"new", qos, &[]),
            Some(publication_id(w.epoch, 101)),
            None,
            false,
        );
        commit(&mut w.store, &fresh);
        let state = w.store.snapshot().unwrap();
        assert_eq!(state.retained.len(), 1);
        assert_eq!(state.accounts.references.get(), 1);
        assert_eq!(state.accounts.payload_bytes.get(), 3);
    }
}

pub fn rejected_replay_ranges_remain_reserved<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let mut limit = limits();
    limit.references = References::new(1);
    let mut w = World::with_limits(make, limit);
    let a = admitted(&w, 1, b"range/x", b"old", Qos::AtLeastOnce, &[]);
    commit(&mut w.store, &a);
    let sub = subscribe(&w, 2, w.b, 1000, &[(b"range/#", Qos::AtMostOnce, 0)]);
    assert!(matches!(
        w.store.apply(&sub).reply,
        StoreReply::Outcome(StoreOutcome::RejectedCertain(
            RejectReason::CapacityExceeded(_)
        ))
    ));
    let collision = with_ids(
        &admitted(&w, 3, b"range/x", b"new", Qos::AtLeastOnce, &[]),
        Some(publication_id(w.epoch, 1255)),
        None,
        false,
    );
    assert_eq!(
        w.store.apply(&collision).reply,
        StoreReply::Outcome(StoreOutcome::RejectedCertain(
            RejectReason::PreconditionFailed
        )),
        "rejected_replay_range_was_reused"
    );
    let closure = ReceiptClosure {
        origin: ReceiptOrigin::of(operation_id(w.epoch, 1)),
        through: operation_id(w.epoch, 3).sequence(),
        publication_through: publication_id(w.epoch, 1254).sequence(),
        payload_through: PayloadSeq::new(3),
    };
    assert!(
        matches!(
            w.store
                .collect_receipts(ReceiptCollection {
                    closure,
                    limit: 128
                })
                .unwrap(),
            ReceiptCollectionReply::Refused(_)
        ),
        "rejected_replay_range_not_closed"
    );
    let closure = ReceiptClosure {
        publication_through: publication_id(w.epoch, 1255).sequence(),
        ..closure
    };
    assert!(matches!(
        w.store
            .collect_receipts(ReceiptCollection {
                closure,
                limit: 128
            })
            .unwrap(),
        ReceiptCollectionReply::Applied { .. }
    ));
    assert_eq!(
        w.store.apply(&sub).reply,
        StoreReply::Outcome(StoreOutcome::RetiredOutcomeUnavailable)
    );
    let state = w.store.snapshot().unwrap();
    assert_eq!(
        state.retained[b"range/x".as_slice()].publication,
        publication_id(w.epoch, 1)
    );
    assert_eq!(state.accounts.references.get(), 1);
}

pub fn all<S: DurableStore>(make: &dyn Fn(StoreIncarnation, Limits) -> S) {
    rejected_replay_ranges_remain_reserved(make);
    collected_retained_identities_never_execute_again(make);
    retained_topic_names_are_not_subscription_filters(make);
    retained_survives_without_targets_and_producer(make);
    retained_replacement_clear_and_replay_freeze(make);
    thirty_two_topics_and_ordered_options(make);
    quota_full_replacement_and_no_local_client_identity(make);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn deterministic_retained_contract() {
        all(&MemoryStore::create);
    }
}

/// The temporal suite is shared verbatim by the deterministic and SQL writers.
pub mod expiry {
    use super::*;
    use mqttiumd_contract::command::{ClaimDelivery, FinishDelivery};
    use mqttiumd_contract::ids::OutboundPacketId;
    use mqttiumd_contract::message_expiry::{
        ExpireDelivery, ExpiringAdmission, ExpiringPublication, MarkDeliveryStarted, MessageExpiry,
        MessageTime, NANOS_PER_SECOND, TimedReplaySubscriptions,
    };

    pub fn at(seconds: u64) -> MessageTime {
        MessageTime {
            domain: [7; 32],
            tick: seconds * NANOS_PER_SECOND,
        }
    }
    fn clock<S: DurableStore>(store: &mut S, observed: MessageTime) {
        let expected = store.snapshot().unwrap().session_clock;
        store
            .checkpoint_session_clock(
                expected,
                SessionClockReading {
                    domain: observed.domain,
                    tick: observed.tick,
                },
            )
            .unwrap();
    }
    pub fn expiring(command: Command, expiry: MessageExpiry) -> Command {
        let mut canonical = Vec::new();
        command
            .recipe
            .get()
            .encode_canonical(&mut canonical)
            .unwrap();
        let admission = match Recipe::decode_canonical(&canonical, 16, 65535).unwrap() {
            Recipe::RetainedPublication(value) => ExpiringAdmission::Qos12(value),
            Recipe::RetainedQos0(value) => ExpiringAdmission::Qos0(value),
            _ => panic!("expected retained admission"),
        };
        Command {
            recipe: FrozenRecipe::freeze(Recipe::ExpiringPublication(ExpiringPublication {
                admission,
                expiry,
            }))
            .unwrap(),
            payload: command.payload,
        }
    }
    fn timed(command: Command, observed: MessageTime) -> Command {
        let mut canonical = Vec::new();
        command
            .recipe
            .get()
            .encode_canonical(&mut canonical)
            .unwrap();
        let Recipe::ReplaySubscriptions(replay) =
            Recipe::decode_canonical(&canonical, 16, 65535).unwrap()
        else {
            panic!()
        };
        frozen(
            Recipe::TimedReplaySubscriptions(TimedReplaySubscriptions { replay, observed }),
            None,
        )
    }
    fn lifetime(seconds: u32) -> MessageExpiry {
        MessageExpiry {
            observed: at(0),
            interval: seconds,
        }
    }
    fn claim<S: DurableStore>(
        w: &mut World<S>,
        seq: u64,
        delivery: DeliveryId,
        target: SessionId,
        packet: u16,
    ) -> FinishDelivery {
        let packet_id = OutboundPacketId::new(packet).unwrap();
        let operation = operation_id(w.epoch, seq);
        commit(
            &mut w.store,
            &frozen(
                Recipe::ClaimDelivery(ClaimDelivery {
                    operation,
                    delivery,
                    target,
                    target_revision: Revision::new(1),
                    packet_id,
                }),
                None,
            ),
        );
        FinishDelivery {
            operation: operation_id(w.epoch, seq + 1),
            delivery,
            target,
            target_revision: Revision::new(1),
            packet_id,
            claim: operation,
        }
    }

    pub fn shared_refs_replay_and_source_end<S: DurableStore>(
        make: &dyn Fn(StoreIncarnation, Limits) -> S,
    ) {
        let mut w = World::with_limits(make, limits());
        clock(&mut w.store, at(0));
        let expiry = lifetime(2);
        let admitted = expiring(
            admitted(
                &w,
                1,
                b"expiry/value",
                b"original",
                Qos::ExactlyOnce,
                &[w.b],
            ),
            expiry,
        );
        let original = commit(&mut w.store, &admitted);
        let replay = timed(
            subscribe(&w, 2, w.c, 1000, &[(b"expiry/#", Qos::AtLeastOnce, 0)]),
            at(1),
        );
        commit(&mut w.store, &replay);
        w.store
            .admin(AdminCommand::DeleteSession {
                session: w.producer,
            })
            .unwrap();
        let state = w.store.snapshot().unwrap();
        assert_eq!(state.accounts.references.get(), 3);
        assert_eq!(state.accounts.payload_bytes.get(), 8);
        assert_eq!(state.publications.len(), 2);
        for target in [w.b, w.c] {
            assert_eq!(
                w.store.read_work(target, None, 8).unwrap().items.as_slice()[0].expiry,
                Some(expiry),
                "replay_refreshed_expiry_origin"
            );
        }
        let sweep = w.store.expire_messages(at(2), None, 4).unwrap();
        assert_eq!(
            sweep.removed.len(),
            2,
            "expiry_did_not_end_every_unclaimed_copy"
        );
        assert_eq!(sweep.retained_removed, 1);
        let state = w.store.snapshot().unwrap();
        assert_eq!(state.accounts.references.get(), 0);
        assert_eq!(state.accounts.payload_bytes.get(), 0);
        assert!(state.payloads.is_empty());
        assert!(state.publications.is_empty());
        assert_eq!(
            w.store.apply(&admitted).reply,
            StoreReply::Outcome(StoreOutcome::Committed(original))
        );
        assert!(
            w.store.snapshot().unwrap().retained.is_empty(),
            "receipt_replay_renewed_lifetime"
        );
        let retry = w.store.expire_messages(at(2), None, 4).unwrap();
        assert!(retry.removed.is_empty());
        assert_eq!(retry.retained_removed, 0);
    }

    pub fn claimed_copies_require_exact_never_started_guard<S: DurableStore>(
        make: &dyn Fn(StoreIncarnation, Limits) -> S,
    ) {
        let mut w = World::with_limits(make, limits());
        clock(&mut w.store, at(0));
        let expiry = lifetime(1);
        let command = expiring(
            admitted(
                &w,
                1,
                b"expiry/claimed",
                b"payload",
                Qos::AtLeastOnce,
                &[w.b, w.c],
            ),
            expiry,
        );
        commit(&mut w.store, &command);
        let b = w.b;
        let c = w.c;
        let b_id = DeliveryId {
            publication: publication_id(w.epoch, 1),
            index: 0,
        };
        let c_id = DeliveryId {
            publication: publication_id(w.epoch, 1),
            index: 1,
        };
        let b_finish = claim(&mut w, 2, b_id, b, 1);
        let mut c_finish = claim(&mut w, 4, c_id, c, 1);
        commit(
            &mut w.store,
            &frozen(
                Recipe::MarkDeliveryStarted(MarkDeliveryStarted {
                    operation: operation_id(w.epoch, 6),
                    delivery: c_id,
                    target: c,
                    target_revision: Revision::new(1),
                    packet_id: c_finish.packet_id,
                    claim: c_finish.claim,
                }),
                None,
            ),
        );
        let sweep = w.store.expire_messages(at(1), None, 4).unwrap();
        assert!(sweep.removed.is_empty(), "started_claim_payload_released");
        assert_eq!(sweep.retained_removed, 1);
        assert_eq!(
            sweep.quarantined, 1,
            "expired_unstarted_claim_not_reported_uncertain"
        );
        let state = w.store.snapshot().unwrap();
        assert_eq!(state.accounts.references.get(), 2);
        assert!(!state.outbound[&b_id].started);
        assert!(state.outbound[&c_id].started);
        assert!(
            w.store.read_work(c, None, 8).unwrap().items.as_slice()[0]
                .claim
                .unwrap()
                .started
        );
        let mut wrong = b_finish;
        wrong.operation = operation_id(w.epoch, 7);
        wrong.claim = c_finish.claim;
        assert!(matches!(
            w.store
                .apply(&frozen(
                    Recipe::ExpireDelivery(ExpireDelivery {
                        finish: wrong,
                        expiry,
                        observed: at(1)
                    }),
                    None
                ))
                .reply,
            StoreReply::Outcome(StoreOutcome::RejectedCertain(
                RejectReason::PreconditionFailed
            ))
        ));
        commit(
            &mut w.store,
            &frozen(
                Recipe::ExpireDelivery(ExpireDelivery {
                    finish: b_finish,
                    expiry,
                    observed: at(1),
                }),
                None,
            ),
        );
        c_finish.operation = operation_id(w.epoch, 8);
        assert!(matches!(
            w.store
                .apply(&frozen(
                    Recipe::ExpireDelivery(ExpireDelivery {
                        finish: c_finish,
                        expiry,
                        observed: at(1)
                    }),
                    None
                ))
                .reply,
            StoreReply::Outcome(StoreOutcome::RejectedCertain(
                RejectReason::PreconditionFailed
            ))
        ));
        c_finish.operation = operation_id(w.epoch, 9);
        commit(
            &mut w.store,
            &frozen(Recipe::FinishDelivery(c_finish), None),
        );
        assert_eq!(w.store.snapshot().unwrap().accounts.references.get(), 0);
        // A late positive-write observation must never recreate the removed claim.
        let late = frozen(
            Recipe::MarkDeliveryStarted(MarkDeliveryStarted {
                operation: operation_id(w.epoch, 10),
                delivery: c_id,
                target: c,
                target_revision: Revision::new(1),
                packet_id: c_finish.packet_id,
                claim: c_finish.claim,
            }),
            None,
        );
        assert!(matches!(
            w.store.apply(&late).reply,
            StoreReply::Outcome(StoreOutcome::RejectedCertain(
                RejectReason::PreconditionFailed
            ))
        ));
        assert!(w.store.snapshot().unwrap().outbound.is_empty());
    }

    pub fn zero_replacement_at_capacity_and_qos2_continuation<S: DurableStore>(
        make: &dyn Fn(StoreIncarnation, Limits) -> S,
    ) {
        let mut l = limits();
        l.references = References::new(1);
        l.payload_bytes = PayloadBytes::new(3);
        let mut w = World::with_limits(make, l);
        clock(&mut w.store, at(0));
        let old = admitted(&w, 1, b"expiry/value", b"old", Qos::AtLeastOnce, &[]);
        commit(&mut w.store, &old);
        let zero = expiring(
            admitted(
                &w,
                2,
                b"expiry/value",
                b"larger replacement",
                Qos::ExactlyOnce,
                &[w.b],
            ),
            lifetime(0),
        );
        commit(&mut w.store, &zero);
        let state = w.store.snapshot().unwrap();
        assert!(state.retained.is_empty());
        assert!(state.obligations.is_empty());
        assert!(state.payloads.is_empty());
        assert_eq!(state.accounts.references.get(), 0);
        assert_eq!(state.accounts.payload_bytes.get(), 0);
        assert_eq!(state.inbound.len(), 1, "expiry_ended_qos2_handshake");
        let release = w.release(3, 2, 2);
        commit(&mut w.store, &release);
        assert!(w.store.snapshot().unwrap().publications.is_empty());
    }

    pub fn temporal_quarantine_and_observation_refusals<S: DurableStore>(
        make: &dyn Fn(StoreIncarnation, Limits) -> S,
    ) {
        let mut w = World::with_limits(make, limits());
        clock(&mut w.store, at(0));
        let command = expiring(
            admitted(&w, 1, b"expiry/value", b"held", Qos::AtLeastOnce, &[w.b]),
            lifetime(1),
        );
        commit(&mut w.store, &command);
        let foreign = MessageTime {
            domain: [9; 32],
            tick: 50 * NANOS_PER_SECOND,
        };
        clock(&mut w.store, foreign);
        let before = w.store.snapshot().unwrap();
        let sweep = w.store.expire_messages(foreign, None, 4).unwrap();
        assert_eq!(sweep.quarantined, 1);
        assert!(sweep.removed.is_empty());
        assert_eq!(sweep.retained_removed, 0);
        let after = w.store.snapshot().unwrap();
        assert_eq!(after.accounts, before.accounts);
        assert_eq!(after.publications, before.publications);
        let replay = timed(
            subscribe(&w, 2, w.c, 1000, &[(b"expiry/#", Qos::AtMostOnce, 0)]),
            foreign,
        );
        commit(&mut w.store, &replay);
        assert!(
            w.store.read_work(w.c, None, 8).unwrap().items.is_empty(),
            "unknown_origin_replayed_as_fresh"
        );
        let before = w.store.snapshot().unwrap();
        assert_eq!(
            w.store.expire_messages(at(100), None, 4),
            Err(ReadError::InvalidData)
        );
        assert_eq!(w.store.snapshot().unwrap(), before);
        let stale = w
            .store
            .expire_messages(
                MessageTime {
                    tick: foreign.tick - 1,
                    ..foreign
                },
                None,
                4,
            )
            .unwrap();
        assert_eq!(stale.quarantined, 1);
        // Historical tag 12 has no temporal observation and must skip expiring values.
        let legacy = subscribe(&w, 3, w.c, 2000, &[(b"expiry/#", Qos::AtMostOnce, 0)]);
        commit(&mut w.store, &legacy);
        assert!(w.store.read_work(w.c, None, 8).unwrap().items.is_empty());
    }

    pub fn thirty_two_timed_replays_keep_original_ttl<S: DurableStore>(
        make: &dyn Fn(StoreIncarnation, Limits) -> S,
    ) {
        let mut w = World::with_limits(make, limits());
        clock(&mut w.store, at(0));
        let expiry = lifetime(10);
        for n in 1..=32 {
            let c = expiring(
                admitted(
                    &w,
                    n,
                    format!("expiry/{n:02}").as_bytes(),
                    &[n as u8],
                    Qos::AtLeastOnce,
                    &[],
                ),
                expiry,
            );
            commit(&mut w.store, &c);
        }
        let sub = timed(
            subscribe(
                &w,
                33,
                w.b,
                1000,
                &[
                    (b"expiry/#", Qos::AtMostOnce, 0x20),
                    (b"expiry/#", Qos::AtMostOnce, 0x10),
                    (b"expiry/#", Qos::AtMostOnce, 0),
                ],
            ),
            at(9),
        );
        commit(&mut w.store, &sub);
        let state = w.store.snapshot().unwrap();
        assert_eq!(
            state.obligations.len(),
            32,
            "timed_replay_truncated_or_RH_order_lost"
        );
        assert_eq!(state.accounts.references.get(), 64);
        for row in state.publications.values() {
            assert_eq!(row.expiry, Some(expiry), "replay_refreshed_expiry_origin");
        }
        let mut cursor = None;
        let mut removed = 0;
        let mut retained = 0;
        loop {
            let page = w.store.expire_messages(at(10), cursor, 4).unwrap();
            removed += page.removed.len();
            retained += page.retained_removed;
            cursor = page.next;
            if !page.more {
                break;
            }
            assert!(cursor.is_some());
        }
        assert_eq!(removed, 32);
        assert_eq!(retained, 32);
        assert_eq!(w.store.snapshot().unwrap().accounts.references.get(), 0);
        let expired = timed(
            subscribe(&w, 34, w.c, 2000, &[(b"expiry/#", Qos::AtMostOnce, 0)]),
            at(10),
        );
        commit(&mut w.store, &expired);
        assert!(w.store.read_work(w.c, None, 8).unwrap().items.is_empty());
    }

    pub fn expired_retained_values_are_skipped_without_maintenance<S: DurableStore>(
        make: &dyn Fn(StoreIncarnation, Limits) -> S,
    ) {
        let mut w = World::with_limits(make, limits());
        clock(&mut w.store, at(0));
        let c = expiring(
            admitted(&w, 1, b"expiry/value", b"old", Qos::AtLeastOnce, &[]),
            lifetime(1),
        );
        commit(&mut w.store, &c);
        let replay = timed(
            subscribe(&w, 2, w.b, 1000, &[(b"expiry/#", Qos::AtMostOnce, 0)]),
            at(1),
        );
        commit(&mut w.store, &replay);
        assert!(
            w.store.read_work(w.b, None, 8).unwrap().items.is_empty(),
            "expired_retained_replayed"
        );
        assert_eq!(
            w.store.snapshot().unwrap().retained.len(),
            1,
            "selection performed unrequested maintenance"
        );
    }

    pub fn offline_expiry_removes_pending_copies<S: DurableStore>(
        make: &dyn Fn(StoreIncarnation, Limits) -> S,
    ) {
        use mqttiumd_contract::session::{
            SessionAction, SessionExpiry, SessionGuard, SessionLifecycle,
        };
        let mut w = World::with_limits(make, limits());
        clock(&mut w.store, at(0));
        let b = w.b;
        let client = w.store.snapshot().unwrap().sessions[&b].client_id.clone();
        let opened = operation_id(w.epoch, 1);
        commit(
            &mut w.store,
            &frozen(
                Recipe::SessionLifecycle(SessionLifecycle {
                    operation: opened,
                    action: SessionAction::Open {
                        client_id: bytes(&client),
                        expected: Some(SessionGuard {
                            session: b,
                            opened_by: None,
                        }),
                        clean_start: false,
                        expiry: SessionExpiry::Never,
                    },
                }),
                None,
            ),
        );
        let c = expiring(
            admitted(&w, 2, b"expiry/offline", b"pending", Qos::AtLeastOnce, &[b]),
            lifetime(2),
        );
        commit(&mut w.store, &c);
        commit(
            &mut w.store,
            &frozen(
                Recipe::SessionLifecycle(SessionLifecycle {
                    operation: operation_id(w.epoch, 3),
                    action: SessionAction::Detach {
                        guard: SessionGuard {
                            session: b,
                            opened_by: Some(opened),
                        },
                        expiry: SessionExpiry::Never,
                        observed_at: 0,
                    },
                }),
                None,
            ),
        );
        assert!(w.store.snapshot().unwrap().sessions[&b].detached.is_some());
        let result = w.store.expire_messages(at(2), None, 4).unwrap();
        assert_eq!(result.removed.len(), 1, "offline_expiry_ignored");
        assert!(
            w.store.read_work(b, None, 8).unwrap().items.is_empty(),
            "offline_expiry_ignored"
        );
        assert!(
            w.store.snapshot().unwrap().sessions[&b].detached.is_some(),
            "message expiry ended persistent session"
        );
    }

    pub fn frozen_observations_may_precede_latest_checkpoint<S: DurableStore>(
        make: &dyn Fn(StoreIncarnation, Limits) -> S,
    ) {
        let mut w = World::with_limits(make, limits());
        clock(&mut w.store, at(5));
        let expiry = lifetime(10);
        let c = expiring(
            admitted(&w, 1, b"expiry/frozen", b"held", Qos::AtLeastOnce, &[]),
            expiry,
        );
        commit(&mut w.store, &c);
        let replay = timed(
            subscribe(&w, 2, w.b, 1000, &[(b"expiry/#", Qos::AtMostOnce, 0)]),
            at(4),
        );
        commit(&mut w.store, &replay);
        assert_eq!(
            w.store.read_work(w.b, None, 8).unwrap().items.as_slice()[0].expiry,
            Some(expiry),
            "checkpoint_renewed_or_refused_frozen_lifetime"
        );
        let future = MessageExpiry {
            observed: at(20),
            interval: 1,
        };
        let c = expiring(
            admitted(&w, 3, b"expiry/future", b"future", Qos::AtLeastOnce, &[]),
            future,
        );
        commit(&mut w.store, &c);
        let before = w.store.snapshot().unwrap();
        let result = w.store.expire_messages(at(19), None, 4).unwrap();
        assert_eq!(result.retained_removed, 1);
        assert_eq!(result.removed.len(), 1);
        let after = w.store.snapshot().unwrap();
        assert_eq!(
            after.publications.get(&publication_id(w.epoch, 3)),
            before.publications.get(&publication_id(w.epoch, 3)),
            "maintenance_aged_a_later_admission"
        );
        assert_eq!(after.accounts.references.get(), 1);
    }

    pub fn maintenance_retry_skips_admissions_after_the_frozen_observation<S: DurableStore>(
        make: &dyn Fn(StoreIncarnation, Limits) -> S,
    ) {
        let mut w = World::with_limits(make, limits());
        clock(&mut w.store, at(0));
        let old = expiring(
            admitted(&w, 1, b"expiry/old", b"old", Qos::AtLeastOnce, &[w.b]),
            lifetime(1),
        );
        commit(&mut w.store, &old);
        let frozen = at(1);
        let lost = w.store.expire_messages(frozen, None, 4).unwrap();
        assert_eq!(lost.removed.len(), 1);
        assert_eq!(lost.retained_removed, 1);
        clock(&mut w.store, at(2));
        let later_expiry = MessageExpiry {
            observed: at(2),
            interval: 1,
        };
        let new = expiring(
            admitted(&w, 2, b"expiry/later", b"new", Qos::AtLeastOnce, &[w.b]),
            later_expiry,
        );
        commit(&mut w.store, &new);
        let before = w.store.snapshot().unwrap();
        let retry = w
            .store
            .expire_messages(frozen, None, 4)
            .expect("maintenance_retry_mistook_newer_admission_for_clock_regression");
        assert!(retry.removed.is_empty());
        assert_eq!(retry.retained_removed, 0);
        assert_eq!(retry.quarantined, 0);
        assert_eq!(
            w.store.snapshot().unwrap(),
            before,
            "maintenance_retry_changed_later_admission"
        );
        assert_eq!(
            w.store.read_work(w.b, None, 8).unwrap().items.as_slice()[0].expiry,
            Some(later_expiry)
        );
        let current = w.store.expire_messages(at(3), None, 4).unwrap();
        assert_eq!(current.removed.len(), 1);
        assert_eq!(current.retained_removed, 1);
        assert_eq!(w.store.snapshot().unwrap().accounts.references.get(), 0);
    }

    pub fn timed_commands_require_the_current_boot_authority<S: DurableStore>(
        make: &dyn Fn(StoreIncarnation, Limits) -> S,
    ) {
        let mut w = World::with_limits(make, limits());
        clock(&mut w.store, at(0));
        let foreign = MessageTime {
            domain: [9; 32],
            tick: 0,
        };
        let bad = expiring(
            admitted(&w, 1, b"expiry/foreign", b"bad", Qos::AtLeastOnce, &[]),
            MessageExpiry {
                observed: foreign,
                interval: 10,
            },
        );
        assert_eq!(
            w.store.apply(&bad).reply,
            StoreReply::Outcome(StoreOutcome::RejectedCertain(
                RejectReason::PreconditionFailed
            )),
            "foreign_admission_authority_was_not_refused"
        );
        let good = admitted(&w, 2, b"expiry/value", b"current", Qos::AtLeastOnce, &[]);
        commit(&mut w.store, &good);
        let before = w.store.snapshot().unwrap();
        let bad = timed(
            subscribe(&w, 3, w.b, 1000, &[(b"expiry/#", Qos::AtMostOnce, 0)]),
            foreign,
        );
        assert_eq!(
            w.store.apply(&bad).reply,
            StoreReply::Outcome(StoreOutcome::RejectedCertain(
                RejectReason::PreconditionFailed
            )),
            "foreign_replay_authority_was_not_refused"
        );
        let after = w.store.snapshot().unwrap();
        assert_eq!(before.retained, after.retained);
        assert_eq!(before.routes, after.routes);
        assert_eq!(before.obligations, after.obligations);
    }

    pub fn timed_replay_cannot_observe_before_the_message_origin<S: DurableStore>(
        make: &dyn Fn(StoreIncarnation, Limits) -> S,
    ) {
        let mut w = World::with_limits(make, limits());
        clock(&mut w.store, at(0));
        let c = expiring(
            admitted(&w, 1, b"expiry/future", b"future", Qos::AtLeastOnce, &[]),
            MessageExpiry {
                observed: at(20),
                interval: 1,
            },
        );
        commit(&mut w.store, &c);
        let before = w.store.snapshot().unwrap();
        let replay = timed(
            subscribe(&w, 2, w.b, 1000, &[(b"expiry/#", Qos::AtMostOnce, 0)]),
            at(19),
        );
        assert_eq!(
            w.store.apply(&replay).reply,
            StoreReply::Outcome(StoreOutcome::RejectedCertain(
                RejectReason::PreconditionFailed
            )),
            "regressive_replay_observation_was_not_refused"
        );
        let after = w.store.snapshot().unwrap();
        assert_eq!(before.routes, after.routes);
        assert_eq!(before.obligations, after.obligations);
        assert_eq!(before.retained, after.retained);
    }

    pub fn timed_replay_rejection_reserves_the_full_identity_range<S: DurableStore>(
        make: &dyn Fn(StoreIncarnation, Limits) -> S,
    ) {
        let mut l = limits();
        l.references = References::new(1);
        let mut w = World::with_limits(make, l);
        clock(&mut w.store, at(0));
        let old = expiring(
            admitted(&w, 1, b"expiry/value", b"old", Qos::AtLeastOnce, &[]),
            lifetime(100),
        );
        let facts = commit(&mut w.store, &old);
        let before = w.store.snapshot().unwrap();
        let replay = timed(
            subscribe(&w, 2, w.b, 1000, &[(b"expiry/#", Qos::AtMostOnce, 0)]),
            at(1),
        );
        assert!(matches!(
            w.store.apply(&replay).reply,
            StoreReply::Outcome(StoreOutcome::RejectedCertain(
                RejectReason::CapacityExceeded(_)
            ))
        ));
        let after = w.store.snapshot().unwrap();
        assert_eq!(before.routes, after.routes);
        assert_eq!(before.retained, after.retained);
        assert_eq!(before.accounts.references, after.accounts.references);
        let mut collision = expiring(
            admitted(&w, 3, b"expiry/collision", b"new", Qos::AtLeastOnce, &[]),
            lifetime(100),
        );
        let mut raw = Vec::new();
        collision.recipe.get().encode_canonical(&mut raw).unwrap();
        let Recipe::ExpiringPublication(mut value) =
            Recipe::decode_canonical(&raw, 16, 65535).unwrap()
        else {
            panic!()
        };
        let ExpiringAdmission::Qos12(a) = &mut value.admission else {
            panic!()
        };
        a.admission.publication = publication_id(w.epoch, 1255);
        collision.recipe = FrozenRecipe::freeze(Recipe::ExpiringPublication(value)).unwrap();
        assert_eq!(
            w.store.apply(&collision).reply,
            StoreReply::Outcome(StoreOutcome::RejectedCertain(
                RejectReason::PreconditionFailed
            )),
            "rejected_timed_replay_range_reused"
        );
        let closure = ReceiptClosure {
            origin: ReceiptOrigin::of(operation_id(w.epoch, 1)),
            through: operation_id(w.epoch, 3).sequence(),
            publication_through: publication_id(w.epoch, 1254).sequence(),
            payload_through: PayloadSeq::new(3),
        };
        assert!(
            matches!(
                w.store
                    .collect_receipts(ReceiptCollection {
                        closure,
                        limit: 128
                    })
                    .unwrap(),
                ReceiptCollectionReply::Refused(_)
            ),
            "timed_replay_full_range_not_guarded_before_receipt_gc"
        );
        assert_eq!(
            w.store.apply(&old).reply,
            StoreReply::Outcome(StoreOutcome::Committed(facts))
        );
        let closure = ReceiptClosure {
            publication_through: publication_id(w.epoch, 1255).sequence(),
            ..closure
        };
        assert!(matches!(
            w.store
                .collect_receipts(ReceiptCollection {
                    closure,
                    limit: 128
                })
                .unwrap(),
            ReceiptCollectionReply::Applied { .. }
        ));
        assert_eq!(
            w.store.apply(&replay).reply,
            StoreReply::Outcome(StoreOutcome::RetiredOutcomeUnavailable)
        );
        assert_eq!(
            w.store.apply(&old).reply,
            StoreReply::Outcome(StoreOutcome::RetiredOutcomeUnavailable)
        );
        assert_eq!(
            w.store.snapshot().unwrap().retained,
            before.retained,
            "retired_timed_receipt_remutated"
        );
    }

    pub fn empty_retained_clear_still_creates_its_live_publication<S: DurableStore>(
        make: &dyn Fn(StoreIncarnation, Limits) -> S,
    ) {
        let mut w = World::with_limits(make, limits());
        clock(&mut w.store, at(0));
        let old = admitted(&w, 1, b"expiry/value", b"old", Qos::AtLeastOnce, &[]);
        commit(&mut w.store, &old);
        let clear = expiring(
            admitted(&w, 2, b"expiry/value", b"", Qos::AtLeastOnce, &[w.b]),
            lifetime(10),
        );
        commit(&mut w.store, &clear);
        let state = w.store.snapshot().unwrap();
        assert!(state.retained.is_empty());
        assert_eq!(state.accounts.references.get(), 1);
        let page = w.store.read_work(w.b, None, 8).unwrap();
        assert_eq!(
            page.items.len(),
            1,
            "empty_retained_clear_lost_live_delivery"
        );
        assert!(page.items.as_slice()[0].payload.is_empty());
        assert_eq!(page.items.as_slice()[0].expiry, Some(lifetime(10)));
        let result = w.store.expire_messages(at(10), None, 4).unwrap();
        assert_eq!(result.removed.len(), 1);
        assert_eq!(w.store.snapshot().unwrap().accounts.references.get(), 0);
    }

    #[cfg(test)]
    pub fn deterministic_maintenance_respects_the_publication_budget() {
        let mut w = World::with_limits(&MemoryStore::create, limits());
        clock(&mut w.store, at(0));
        for n in 1..=5 {
            let c = expiring(
                admitted(
                    &w,
                    n,
                    format!("expiry/{n}").as_bytes(),
                    b"old",
                    Qos::AtLeastOnce,
                    &[w.b],
                ),
                lifetime(1),
            );
            commit(&mut w.store, &c);
        }
        let mut state = w.store.snapshot().unwrap();
        state
            .publications
            .get_mut(&publication_id(w.epoch, 5))
            .unwrap()
            .expiry
            .as_mut()
            .unwrap()
            .observed
            .tick = u64::MAX;
        w.store = MemoryStore::reopen(state);
        let result = w
            .store
            .expire_messages(at(1), None, 4)
            .expect("sweep_read_past_publication_budget");
        assert_eq!(result.removed.len(), 4);
        assert_eq!(result.retained_removed, 4);
        assert!(result.more);
        assert_eq!(result.next, Some(publication_id(w.epoch, 4)));
        assert_eq!(
            w.store.expire_messages(at(1), result.next, 4),
            Err(ReadError::InvalidData)
        );
    }

    pub fn all<S: DurableStore>(make: &dyn Fn(StoreIncarnation, Limits) -> S) {
        maintenance_retry_skips_admissions_after_the_frozen_observation(make);
        timed_replay_cannot_observe_before_the_message_origin(make);
        timed_commands_require_the_current_boot_authority(make);
        timed_replay_rejection_reserves_the_full_identity_range(make);
        empty_retained_clear_still_creates_its_live_publication(make);
        expired_retained_values_are_skipped_without_maintenance(make);
        offline_expiry_removes_pending_copies(make);
        frozen_observations_may_precede_latest_checkpoint(make);
        shared_refs_replay_and_source_end(make);
        claimed_copies_require_exact_never_started_guard(make);
        zero_replacement_at_capacity_and_qos2_continuation(make);
        temporal_quarantine_and_observation_refusals(make);
        thirty_two_timed_replays_keep_original_ttl(make);
    }
}

#[cfg(test)]
mod expiry_tests {
    #[test]
    fn deterministic_message_expiry_contract() {
        super::expiry::all(&super::MemoryStore::create);
    }
    #[test]
    fn deterministic_maintenance_respects_the_publication_budget() {
        super::expiry::deterministic_maintenance_respects_the_publication_budget();
    }
}
