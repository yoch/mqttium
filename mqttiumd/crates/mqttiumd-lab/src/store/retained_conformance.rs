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

pub fn all<S: DurableStore>(make: &dyn Fn(StoreIncarnation, Limits) -> S) {
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
