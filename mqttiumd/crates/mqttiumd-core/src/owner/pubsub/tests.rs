/// Used by the tests only.
impl super::VolatileQueue {
    pub(in crate::owner) fn to_vec(
        &self,
    ) -> (
        alloc::vec::Vec<super::InFlightVolatile>,
        alloc::vec::Vec<Volatile>,
    ) {
        let inflight: alloc::vec::Vec<super::InFlightVolatile> =
            self.inflight.values().cloned().collect();
        let mut queued = alloc::vec::Vec::with_capacity(self.parked_len + self.pending.len());
        queued.extend(
            self.lanes
                .values()
                .flat_map(|lane| lane.parked.iter().cloned()),
        );
        queued.extend(self.pending.iter().cloned());
        (inflight, queued)
    }
}

/// Used by the tests only.
impl super::State {
    pub(in crate::owner) fn rebuild_indices(&mut self) {
        self.connections.rebuild_indices();
        self.deliveries.rebuild_indices();
    }
}

/// Used by the tests only.
impl super::Owner {
    pub(in crate::owner) fn project_routes(
        &self,
        session: SessionId,
        revision: Revision,
        changes: &[GrantedSubscriptionChange],
    ) -> Result<BoundedVec<Route>, Stop> {
        let mut routes = BoundedVec::with_capacity(self.routes.capacity())?;
        for route in self.routes.as_slice() {
            if !(route.target == session && changes.iter().any(|c| c.topic == route.topic)) {
                routes.try_push(route.clone()).map_err(|_| IMPOSSIBLE)?;
            }
        }
        for (i, change) in changes.iter().enumerate() {
            if change.granted.is_none()
                || changes[i + 1..]
                    .iter()
                    .any(|later| later.topic == change.topic)
            {
                continue;
            }
            routes
                .try_push(Route {
                    topic: change.topic.clone(),
                    target: session,
                    target_revision: revision,
                    max_qos: change.granted.ok_or(IMPOSSIBLE)?,
                    options: change.options,
                })
                .map_err(|_| {
                    Stop::Need(CapacityNeed {
                        resource: Resource::References,
                        amount: 1,
                        wake: WakeCondition::ResourceReleased,
                    })
                })?;
        }
        Ok(routes)
    }
}

use super::*;
use crate::contract::command::StartupNonce;
use crate::contract::effects::{EffectSink, SubmitResult};
use crate::contract::outcome::{EpochOpened, EpochOpening, OpeningOutcome, OpeningReply};
use alloc::vec::Vec;

impl State {
    pub(in crate::owner) fn assert_indices_kept(&mut self) {
        let kept = (
            self.connections.index_snapshot(),
            self.deliveries.index_snapshot(),
        );
        self.rebuild_indices();
        let rebuilt = (
            self.connections.index_snapshot(),
            self.deliveries.index_snapshot(),
        );
        assert_eq!(kept, rebuilt, "a pub/sub index drifted from its rows");
    }
}

impl Owner {
    /// Steps the owner and verifies that every incremental pub/sub index
    /// and counter matches a clean rebuild from the rows, on every outcome.
    pub(in crate::owner) fn step_checked(&mut self, input: Input, grant: TurnGrant) -> StepOutcome {
        let outcome = self.step(input, grant);
        self.pubsub.assert_indices_kept();
        outcome
    }
}

const TARGET: SessionId = SessionId::new(1);
const CONNECTION: ConnectionGeneration = ConnectionGeneration::new(10);
const TURN: TurnGrant = TurnGrant {
    work_limit: WorkUnits::new(100_000),
};

struct Taking(Vec<OwnedEffect>);
impl EffectSink for Taking {
    fn try_submit(&mut self, effect: OwnedEffect) -> SubmitResult {
        self.0.push(effect);
        SubmitResult::Accepted
    }
}

fn bytes(value: &[u8]) -> SharedBytes {
    SharedBytes::copy_from(value, 128).unwrap()
}
fn nonzero(value: u32) -> NonZeroU32 {
    NonZeroU32::new(value).unwrap()
}
fn drain(owner: &mut Owner) -> Vec<(EffectId, Effect)> {
    let mut sink = Taking(Vec::new());
    let _ = owner.dispatch(&mut sink);
    sink.0.into_iter().map(OwnedEffect::into_parts).collect()
}
fn apply(owner: &mut Owner, input: Input) -> Vec<(EffectId, Effect)> {
    owner.pubsub.rebuild_indices();
    let result = owner.step_checked(input, TURN);
    assert!(matches!(result, StepOutcome::Applied { .. }), "{result:?}");
    drain(owner)
}
fn world() -> Owner {
    let incarnation = StoreIncarnation::new(1);
    let epoch = RunEpoch::new(2);
    let mut opening = EpochOpening::new(incarnation, StartupNonce::new([7; 16]));
    let command = opening.submit(RunEpoch::new(1)).unwrap();
    let _ = opening
        .observe(OpeningReply {
            command,
            outcome: OpeningOutcome::Committed(EpochOpened { incarnation, epoch }),
        })
        .unwrap();
    let mut authority =
        OwnerAuthority::from_grant(opening.hand_over().unwrap(), nonzero(1)).unwrap();
    let outbox = authority
        .create_outbox(
            LogicalOwnerId::new(4),
            NonZeroUsize::new(32).unwrap(),
            nonzero(8),
        )
        .unwrap();
    let config = OwnerConfig {
        max_sessions: nonzero(2),
        max_routes: nonzero(2),
        max_exchanges: nonzero(8),
        session_qos2_exchanges: NonZeroU32::MAX,
        account: AccountId::new(1),
        payload_bytes: PayloadBytes::new(1024),
        resolution_delay: LogicalDuration::new(1),
        retained_operations: nonzero(16),
        remembered_connections: nonzero(4),
    };
    let mut owner = Owner::new(config, outbox).unwrap();
    let facts = [
        RecoveredFact::Epoch { incarnation, epoch },
        RecoveredFact::Limits {
            max_targets_per_admission: 2,
            max_topic_bytes: 128,
            max_payload_bytes: PayloadBytes::new(128),
        },
        RecoveredFact::RouteTable {
            revision: Revision::new(0),
        },
        RecoveredFact::Session {
            session: TARGET,
            client_id: bytes(b"subscriber"),
            revision: Revision::new(1),
            expiry: crate::contract::session::SessionExpiry::Never,
            opened_by: None,
            detached: None,
        },
        RecoveredFact::Session {
            session: SessionId::new(2),
            client_id: bytes(b"other"),
            revision: Revision::new(1),
            expiry: crate::contract::session::SessionExpiry::Never,
            opened_by: None,
            detached: None,
        },
    ];
    assert!(
        apply(
            &mut owner,
            Input::Recovery {
                facts: BoundedVec::from_slice(&facts, facts.len()).unwrap(),
                last: true
            }
        )
        .is_empty()
    );
    let _ = apply(
        &mut owner,
        Input::Packet {
            connection: CONNECTION,
            ingress_order: 0,
            packet: InboundPacket::ConnectResume {
                client_id: bytes(b"subscriber"),
            },
        },
    );
    assert!(
        apply(
            &mut owner,
            Input::ConnectionSettings {
                connection: CONNECTION,
                receive_maximum: 8,
                maximum_packet_size: 65536
            }
        )
        .is_empty()
    );
    owner
}
#[test]
fn a_work_notification_during_a_read_survives_the_old_empty_page() {
    // The coordinator and delivery shards use the same work reader.
    // Their runtime schedules WorkAvailable after the page only if the
    // owner still reports ready work.
    for during in [None, Some(false), Some(true)] {
        let mut owner = world();
        owner.mark_session_dirty(TARGET, true);
        let load = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        let [(old_read, Effect::LoadWork { after: None, .. })] = load.as_slice() else {
            panic!("expected the initial read");
        };
        if let Some(rescan) = during {
            owner.mark_session_dirty(TARGET, rescan);
            assert!(apply(&mut owner, Input::WorkAvailable { session: TARGET }).is_empty());
        }
        let page = |effect| Input::WorkPage {
            effect,
            session: TARGET,
            items: BoundedVec::with_capacity(0).unwrap(),
            next: None,
        };
        assert!(apply(&mut owner, page(*old_read)).is_empty());
        assert_eq!(owner.has_ready_work(TARGET), during == Some(true));
        if during == Some(true) {
            let load = apply(&mut owner, Input::WorkAvailable { session: TARGET });
            let [(fresh_read, Effect::LoadWork { after: None, .. })] = load.as_slice() else {
                panic!("the notification must start one fresh read");
            };
            assert_ne!(old_read, fresh_read);
            assert!(apply(&mut owner, page(*fresh_read)).is_empty());
            assert!(
                !owner.has_ready_work(TARGET),
                "a clean empty page goes idle"
            );
        }
    }
}

fn delivery(index: u32) -> DeliveryId {
    DeliveryId {
        publication: PublicationId::new(
            StoreIncarnation::new(1),
            RunEpoch::new(1),
            LogicalOwnerId::new(4),
            PublicationSeq::new(1),
        ),
        index,
    }
}
#[test]
fn outgoing_pi_fallback_finds_the_lowest_gap_above_the_bitmap() {
    let mut table = Deliveries::new(nonzero(514)).unwrap();
    for id in (1u16..=515).filter(|&id| id != 514) {
        let claim = DeliveryClaim {
            operation: OperationId::new(
                StoreIncarnation::new(1),
                RunEpoch::new(1),
                LogicalOwnerId::new(4),
                OperationSeq::new(u64::from(id)),
            ),
            packet_id: OutboundPacketId::new(id).unwrap(),
            started: false,
            await_pubcomp: false,
        };
        table
            .push(Delivery::metadata(
                delivery(u32::from(id)),
                TARGET,
                Revision::new(1),
                Qos::AtLeastOnce,
                Some(claim),
            ))
            .unwrap();
    }
    assert_eq!(table.free_packet_id(TARGET, 513, false, false), None);
    assert_eq!(
        table.free_packet_id(TARGET, u16::MAX, false, false),
        OutboundPacketId::new(514)
    );
    table.remove(table.slot_of(delivery(512)).unwrap());
    assert_eq!(
        table.free_packet_id(TARGET, u16::MAX, false, false),
        OutboundPacketId::new(512)
    );
}

fn queued(owner: &mut Owner, index: u32) {
    let mut item = Delivery::metadata(
        delivery(index),
        TARGET,
        Revision::new(1),
        Qos::AtLeastOnce,
        None,
    );
    item.topic = Some(bytes(b"topic"));
    item.payload = Some(bytes(b"payload"));
    owner.pubsub.deliveries.push(item).unwrap();
}
fn known(owner: &mut Owner, effect: EffectId, command: &Command) -> Vec<(EffectId, Effect)> {
    let facts = match command.recipe.get() {
        Recipe::ClaimDelivery(c) => CommitFacts::DeliveryClaimed {
            delivery: c.delivery,
            target: c.target,
            packet_id: c.packet_id,
            claim: c.operation,
        },
        Recipe::FinishDelivery(c) => CommitFacts::DeliveryFinished {
            delivery: c.delivery,
            target: c.target,
            packet_id: c.packet_id,
            claim: c.claim,
        },
        Recipe::MarkDeliveryStarted(c) => CommitFacts::DeliveryStarted {
            delivery: c.delivery,
            target: c.target,
            packet_id: c.packet_id,
            claim: c.claim,
        },
        Recipe::PreparePubrel(c) => CommitFacts::DeliveryPubrelReady {
            delivery: c.delivery,
            target: c.target,
            packet_id: c.packet_id,
            claim: c.claim,
        },
        Recipe::ExpireDelivery(c) => CommitFacts::DeliveryExpired {
            delivery: c.finish.delivery,
            target: c.finish.target,
            packet_id: c.finish.packet_id,
            claim: c.finish.claim,
        },
        Recipe::ConsumeDelivery(c) => CommitFacts::DeliveryConsumed {
            delivery: c.delivery,
            target: c.target,
        },
        _ => panic!("delivery command"),
    };
    apply(
        owner,
        Input::StoreResult {
            effect,
            operation: command.recipe.operation(),
            reply: StoreReply::Outcome(StoreOutcome::Committed(facts)),
        },
    )
}

#[test]
fn qos2_keeps_window_and_identifier_until_pubcomp_finish_is_known() {
    let mut owner = world();
    owner.pubsub.connections.as_mut_slice()[0].receive_maximum = 1;
    queued(&mut owner, 0);
    owner.pubsub.deliveries.row_mut(0).qos = Qos::ExactlyOnce;
    queued(&mut owner, 1);
    let key = ConnectionKey {
        session: TARGET,
        generation: CONNECTION,
    };
    let send_ack = |owner: &mut Owner, kind, packet_id| {
        let target = owner.qos2_ack_target(key, packet_id, kind);
        apply(
            owner,
            Input::ObservedQos2Ack {
                connection: key,
                kind,
                packet_id,
                reason: 0,
                target,
            },
        )
    };
    let claim = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    let (effect, Effect::Persist { command }) = &claim[0] else {
        panic!("claim submission")
    };
    let publish = known(&mut owner, *effect, command);
    let Effect::EmitPublish {
        packet_id,
        qos: Qos::ExactlyOnce,
        ..
    } = publish[0].1
    else {
        panic!("QoS 2 PUBLISH")
    };
    let prepared = send_ack(&mut owner, Qos2Ack::Pubrec, packet_id);
    let (effect, Effect::Persist { command }) = &prepared[0] else {
        panic!("prepare submission")
    };
    assert!(matches!(command.recipe.get(), Recipe::PreparePubrel(_)));
    let unknown = apply(
        &mut owner,
        Input::StoreResult {
            effect: *effect,
            operation: command.recipe.operation(),
            reply: StoreReply::Outcome(StoreOutcome::PendingOrUnknown),
        },
    );
    assert!(unknown.is_empty() && send_ack(&mut owner, Qos2Ack::Pubrec, packet_id).is_empty());
    assert!(
        apply(&mut owner, Input::WorkAvailable { session: TARGET }).is_empty(),
        "qos2_pubrel_before_commit"
    );
    assert_eq!(owner.pubsub.free_packet_id(TARGET, 1), None);
    assert!(known(&mut owner, *effect, command).is_empty());
    let release = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    assert!(matches!(release[0].1, Effect::EmitFrame {
            frame: OutboundFrame::Pubrel { packet_id: observed }, ..
        } if observed == packet_id));
    assert!(
        apply(&mut owner, Input::WorkAvailable { session: TARGET }).is_empty(),
        "qos2_receive_maximum_released_before_pubcomp"
    );
    let finished = send_ack(&mut owner, Qos2Ack::Pubcomp, packet_id);
    let (effect, Effect::Persist { command }) = &finished[0] else {
        panic!("finish submission")
    };
    assert_eq!(owner.pubsub.free_packet_id(TARGET, 1), None);
    assert!(apply(&mut owner, Input::WorkAvailable { session: TARGET }).is_empty());
    assert!(known(&mut owner, *effect, command).is_empty());
    assert_eq!(owner.pubsub.free_packet_id(TARGET, 1), Some(packet_id));
}

#[test]
fn recovered_qos2_phase_selects_pubrel_even_with_publish_bytes_available() {
    let mut owner = world();
    let packet_id = OutboundPacketId::new(1).unwrap();
    let prior = OperationId::new(
        StoreIncarnation::new(1),
        RunEpoch::new(1),
        LogicalOwnerId::new(4),
        OperationSeq::new(100),
    );
    let claim = DeliveryClaim {
        operation: prior,
        packet_id,
        started: false,
        await_pubcomp: true,
    };
    owner
        .add_outbound_fact(
            delivery(0),
            TARGET,
            Revision::new(1),
            (Qos::ExactlyOnce, Some(claim), None, false),
        )
        .unwrap();
    let item = owner.pubsub.deliveries.row_mut(0);
    item.topic = Some(bytes(b"topic"));
    item.payload = Some(bytes(b"payload"));
    let frames = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    assert!(
        matches!(&frames[..], [(_, Effect::EmitFrame {
            frame: OutboundFrame::Pubrel { packet_id: observed }, ..
        })] if *observed == packet_id),
        "qos2_recovery_replayed_publish_instead_of_pubrel"
    );
}

#[test]
fn resumed_qos2_pubrel_is_independent_of_the_reduced_publish_window() {
    for pubrel_index in [1, 0] {
        let mut owner = world();
        owner.pubsub.connections.as_mut_slice()[0].receive_maximum = 1;
        let key = ConnectionKey {
            session: TARGET,
            generation: CONNECTION,
        };
        let ids = [
            OutboundPacketId::new(1).unwrap(),
            OutboundPacketId::new(2).unwrap(),
        ];
        let operations = [100, 101].map(|seq| {
            OperationId::new(
                StoreIncarnation::new(1),
                RunEpoch::new(1),
                LogicalOwnerId::new(4),
                OperationSeq::new(seq),
            )
        });
        for (index, (packet_id, operation)) in ids.into_iter().zip(operations).enumerate() {
            let claim = DeliveryClaim {
                operation,
                packet_id,
                started: false,
                await_pubcomp: index == pubrel_index,
            };
            owner
                .add_outbound_fact(
                    delivery(index as u32),
                    TARGET,
                    Revision::new(1),
                    (Qos::ExactlyOnce, Some(claim), None, false),
                )
                .unwrap();
            if index != pubrel_index {
                let item = owner.pubsub.deliveries.row_mut(index);
                item.topic = Some(bytes(b"topic"));
                item.payload = Some(bytes(b"payload"));
            }
        }
        for (index, (expected_id, expected_claim)) in ids.into_iter().zip(operations).enumerate() {
            assert!(
                owner.has_ready_work(TARGET),
                "replay blocked before frame {index} with PUBREL at {pubrel_index}"
            );
            let frames = apply(&mut owner, Input::WorkAvailable { session: TARGET });
            let expected_delivery = delivery(index as u32);
            if index == pubrel_index {
                assert!(matches!(&frames[..], [(_, Effect::EmitFrame {
                        frame: OutboundFrame::Pubrel { packet_id },
                        pubrel: Some((delivery, claim)), ..
                    })] if (*packet_id, *delivery, *claim) == (expected_id, expected_delivery, expected_claim)));
            } else {
                assert!(matches!(&frames[..], [(_, Effect::EmitPublish {
                        packet_id, delivery, claim, qos: Qos::ExactlyOnce, dup: true, ..
                    })] if (*packet_id, *delivery, *claim) == (expected_id, expected_delivery, expected_claim)));
            }
        }
        assert_eq!(owner.pubsub.active(CONNECTION), 1);
        assert_eq!(owner.pubsub.free_packet_id(TARGET, 2), None);
        let ack = |owner: &mut Owner, kind, packet_id| {
            let target = owner.qos2_ack_target(key, packet_id, kind);
            assert!(target.is_some());
            let effects = apply(
                owner,
                Input::ObservedQos2Ack {
                    connection: key,
                    kind,
                    packet_id,
                    reason: 0,
                    target,
                },
            );
            let (effect, Effect::Persist { command }) = &effects[0] else {
                panic!("durable outgoing acknowledgment")
            };
            assert!(known(owner, *effect, command).is_empty());
        };
        ack(&mut owner, Qos2Ack::Pubcomp, ids[pubrel_index]);
        assert_eq!(owner.pubsub.active(CONNECTION), 1);
        assert_eq!(
            owner.pubsub.free_packet_id(TARGET, 2),
            Some(ids[pubrel_index])
        );
        let pending_publish = ids[1 - pubrel_index];
        ack(&mut owner, Qos2Ack::Pubrec, pending_publish);
        assert!(
            matches!(&apply(&mut owner, Input::WorkAvailable { session: TARGET })[..],
                [(_, Effect::EmitFrame { frame: OutboundFrame::Pubrel { packet_id }, .. })]
                if *packet_id == pending_publish)
        );
        assert_eq!(owner.pubsub.active(CONNECTION), 1);
        ack(&mut owner, Qos2Ack::Pubcomp, pending_publish);
        assert_eq!(owner.pubsub.active(CONNECTION), 0);
        assert!(owner.pubsub.deliveries.is_empty());
        assert_eq!(owner.pubsub.free_packet_id(TARGET, 2), Some(ids[0]));
    }
}

#[test]
fn an_applied_finish_reuses_a_free_identifier_after_the_historical_ceiling() {
    let mut owner = world();
    // The committed red fixture positioned the old cursor at 65535.
    // There is now no historical allocation cursor to position; both
    // completed cycles below still require a new claim on this socket.
    queued(&mut owner, 0);
    let claim = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    let Effect::Persist { command } = &claim[0].1 else {
        panic!("initial claim")
    };
    let packet_id = match command.recipe.get() {
        Recipe::ClaimDelivery(c) => c.packet_id,
        _ => panic!("claim"),
    };
    let emitted = known(&mut owner, claim[0].0, command);
    assert!(matches!(emitted[0].1, Effect::EmitPublish { .. }));
    let finish = apply(
        &mut owner,
        Input::Packet {
            connection: CONNECTION,
            ingress_order: 1,
            packet: InboundPacket::Puback {
                packet_id,
                reason: 0,
            },
        },
    );
    let Effect::Persist { command } = &finish[0].1 else {
        panic!("finish")
    };
    assert!(known(&mut owner, finish[0].0, command).is_empty());
    queued(&mut owner, 1);
    let next = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    assert!(
        matches!(next.first(),Some((_,Effect::Persist {command})) if matches!(command.recipe.get(),Recipe::ClaimDelivery(_))),
        "outgoing_pi_historical_exhaustion: a free identifier must permit the next delivery on the same connection: {next:?}"
    );
    let Effect::Persist { command } = &next[0].1 else {
        panic!("new claim")
    };
    let Recipe::ClaimDelivery(claim) = command.recipe.get() else {
        panic!("claim")
    };
    assert_eq!(
        claim.packet_id.get(),
        1,
        "the allocator prefers the smallest free number"
    );
}

#[test]
fn lifecycle_retired_claim_finish_and_consume_results_close_receipt_holes_without_emitting() {
    use crate::contract::receipts::ReceiptCollectionReply;
    use crate::contract::session::{SessionCancellations, SessionChange, SessionEndReason};
    for phase in 0..3 {
        let mut owner = world();
        let registration = apply(&mut owner, Input::MaintainReceipts);
        let Effect::CollectReceipts { collection } = registration[0].1 else {
            panic!("register origin")
        };
        apply(
            &mut owner,
            Input::ReceiptsCollected {
                effect: registration[0].0,
                collection,
                reply: ReceiptCollectionReply::Applied {
                    closure: collection.closure,
                    removed: 0,
                    retained: 0,
                    more: false,
                },
            },
        );
        queued(&mut owner, 0);
        if phase == 2 {
            owner.pubsub.deliveries.row_mut(0).qos = Qos::AtMostOnce;
        }
        let mut other = Delivery::metadata(
            delivery(1),
            SessionId::new(2),
            Revision::new(1),
            Qos::AtLeastOnce,
            None,
        );
        other.topic = Some(bytes(b"topic"));
        other.payload = Some(bytes(b"shared-payload"));
        owner.pubsub.deliveries.push(other.clone()).unwrap();
        let mut pending = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        if phase == 1 {
            let Effect::Persist { command } = &pending[0].1 else {
                panic!("claim")
            };
            known(&mut owner, pending[0].0, command);
            pending = apply(
                &mut owner,
                Input::Packet {
                    connection: CONNECTION,
                    ingress_order: 1,
                    packet: InboundPacket::Puback {
                        packet_id: OutboundPacketId::new(1).unwrap(),
                        reason: 0,
                    },
                },
            );
        }
        let (old_effect, command) = pending
            .iter()
            .find_map(|(id, e)| match e {
                Effect::Persist { command } => Some((*id, command.clone())),
                _ => None,
            })
            .unwrap();
        apply(
            &mut owner,
            Input::StoreResult {
                effect: old_effect,
                operation: command.recipe.operation(),
                reply: StoreReply::Outcome(StoreOutcome::PendingOrUnknown),
            },
        );
        let close = apply(
            &mut owner,
            Input::EndSession {
                connection: ConnectionKey {
                    session: TARGET,
                    generation: CONNECTION,
                },
                opened_by: None,
                reason: SessionEndReason::Disconnect,
            },
        );
        let (close_effect, close_operation) = close
            .iter()
            .find_map(|(id, e)| match e {
                Effect::Persist { command } => Some((*id, command.recipe.operation())),
                _ => None,
            })
            .unwrap();
        apply(
            &mut owner,
            Input::StoreResult {
                effect: close_effect,
                operation: close_operation,
                reply: StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::SessionLifecycle(
                    SessionChange::Closed {
                        session: TARGET,
                        route_revision: Revision::new(0),
                        cancelled: SessionCancellations {
                            inbound: 0,
                            deliveries: 1,
                            routes: 0,
                        },
                    },
                ))),
            },
        );
        assert_eq!(owner.session_status().retired_flights, 1);
        let result = known(&mut owner, old_effect, &command);
        assert!(
            !result.iter().any(|(_, e)| matches!(
                e,
                Effect::EmitPublish { .. } | Effect::EmitQos0 { .. } | Effect::Persist { .. }
            )),
            "retired_delivery_result_reemitted phase={phase}"
        );
        assert_eq!(owner.session_status().retired_flights, 0);
        assert_eq!(
            owner.pubsub.deliveries.to_vec().as_slice(),
            &[other],
            "other_destination_responsibility_changed phase={phase}"
        );
        owner.receipts.test_request_sweep();
        let gc = apply(&mut owner, Input::MaintainReceipts);
        let Effect::CollectReceipts { collection } = gc[0].1 else {
            panic!("collect consumed flights")
        };
        assert_eq!(
            collection.closure.through,
            close_operation.sequence(),
            "retired_delivery_receipt_hole_never_closed phase={phase}"
        );
    }
}

fn operation(sequence: u64) -> OperationId {
    OperationId::new(
        StoreIncarnation::new(1),
        RunEpoch::new(2),
        LogicalOwnerId::new(4),
        OperationSeq::new(sequence),
    )
}
fn flight(sequence: u64) -> InFlight {
    InFlight {
        operation: operation(sequence),
        phase: CommandPhase::Indeterminate,
        deadline: DeadlineToken::new(sequence),
        answers: Answers::new(EffectId {
            epoch: RunEpoch::new(2),
            owner: LogicalOwnerId::new(4),
            sequence: EffectSeq::new(sequence),
        }),
    }
}

#[test]
fn the_complete_small_domain_counts_unknown_finish_and_unloaded_recovered_claims() {
    let mut owner = world();
    for raw in 1..=4 {
        let claim = DeliveryClaim {
            started: false,
            await_pubcomp: false,
            operation: operation(u64::from(raw)),
            packet_id: OutboundPacketId::new(raw).unwrap(),
        };
        let mut item = Delivery::metadata(
            delivery(u32::from(raw)),
            TARGET,
            Revision::new(1),
            Qos::AtLeastOnce,
            Some(claim),
        );
        item.phase = match raw {
            1 => Phase::ClaimPending(flight(1)),
            2 => Phase::AwaitPuback,
            3 => Phase::FinishPending(flight(5)),
            _ => Phase::AwaitPuback,
        };
        assert!(
            item.payload.is_none(),
            "recovered claims reserve before payload loading"
        );
        owner.pubsub.deliveries.push(item).unwrap();
    }
    assert_eq!(
        owner.pubsub.free_packet_id(TARGET, 4),
        None,
        "occupied_pi_reused"
    );
    assert_eq!(owner.pubsub.free_packet_id(TARGET, 5).unwrap().get(), 5);
    assert_eq!(
        owner
            .pubsub
            .free_packet_id(SessionId::new(2), 4)
            .unwrap()
            .get(),
        1,
        "destinations have independent sender identifier spaces"
    );
    let mut pure_zero = Delivery::metadata(
        delivery(10),
        TARGET,
        Revision::new(1),
        Qos::AtMostOnce,
        None,
    );
    pure_zero.payload = Some(bytes(b"zero"));
    owner.pubsub.deliveries.push(pure_zero).unwrap();
    assert_eq!(
        owner.pubsub.free_packet_id(TARGET, 4),
        None,
        "QoS0 neither returns nor takes a QoS1 identifier"
    );
    let ping = apply(
        &mut owner,
        Input::Packet {
            connection: CONNECTION,
            ingress_order: 4,
            packet: InboundPacket::Pingreq,
        },
    );
    assert!(
        matches!(
            ping[0].1,
            Effect::EmitFrame {
                frame: OutboundFrame::Pingresp,
                ..
            }
        ),
        "a genuinely full finite domain does not block control transitions"
    );
}

#[test]
fn a_finish_committed_but_not_known_and_its_resolution_keep_the_identifier_reserved() {
    let mut owner = world();
    queued(&mut owner, 0);
    let claim = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    let Effect::Persist { command } = &claim[0].1 else {
        panic!("claim")
    };
    let claim_operation = command.recipe.operation();
    let _ = known(&mut owner, claim[0].0, command);
    let finish = apply(
        &mut owner,
        Input::Packet {
            connection: CONNECTION,
            ingress_order: 1,
            packet: InboundPacket::Puback {
                packet_id: OutboundPacketId::new(1).unwrap(),
                reason: 0x80,
            },
        },
    );
    let Effect::Persist { command } = &finish[0].1 else {
        panic!("negative ACK still has durable termination")
    };
    let finish_operation = command.recipe.operation();
    assert_ne!(claim_operation, finish_operation);
    assert!(
        apply(
            &mut owner,
            Input::StoreResult {
                effect: finish[0].0,
                operation: finish_operation,
                reply: StoreReply::Outcome(StoreOutcome::PendingOrUnknown)
            }
        )
        .is_empty()
    );
    assert_eq!(
        owner.pubsub.free_packet_id(TARGET, 4).unwrap().get(),
        2,
        "finish_unknown_freed_pi"
    );
    let Effect::ArmDeadline { token, .. } = finish[1].1 else {
        panic!("finish deadline")
    };
    let resolution = apply(
        &mut owner,
        Input::Deadline {
            token,
            now: crate::contract::ids::LogicalTime::new(10),
        },
    );
    assert!(
        matches!(resolution[0].1,Effect::ResolveAndFence {operation} if operation==finish_operation)
    );
    assert_eq!(owner.pubsub.free_packet_id(TARGET, 4).unwrap().get(), 2);
    assert!(known(&mut owner, resolution[0].0, command).is_empty());
    assert_eq!(owner.pubsub.free_packet_id(TARGET, 4).unwrap().get(), 1);
    queued(&mut owner, 1);
    let next = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    let Effect::Persist { command } = &next[0].1 else {
        panic!("new claim")
    };
    let Recipe::ClaimDelivery(new) = command.recipe.get() else {
        panic!("claim")
    };
    assert_eq!(new.packet_id.get(), 1);
    assert_ne!(new.operation, claim_operation);
    assert_ne!(new.operation, finish_operation);
}

#[test]
fn a_submitted_unknown_claim_reserves_its_identifier_before_any_publish_or_callback() {
    let mut owner = world();
    queued(&mut owner, 0);
    queued(&mut owner, 1);
    let claim = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    let Effect::Persist { command } = &claim[0].1 else {
        panic!("claim")
    };
    let op = command.recipe.operation();
    assert_eq!(owner.pubsub.free_packet_id(TARGET, 4).unwrap().get(), 2);
    assert!(
        apply(
            &mut owner,
            Input::StoreResult {
                effect: claim[0].0,
                operation: op,
                reply: StoreReply::Outcome(StoreOutcome::PendingOrUnknown)
            }
        )
        .is_empty()
    );
    assert_eq!(
        owner.pubsub.free_packet_id(TARGET, 4).unwrap().get(),
        2,
        "unknown_claim_pi_reused"
    );
    assert!(
        apply(&mut owner, Input::WorkAvailable { session: TARGET }).is_empty(),
        "later delivery cannot overtake the earlier unknown claim"
    );
}

#[test]
fn collecting_a_known_claim_receipt_does_not_free_the_live_packet_identifier() {
    use crate::contract::receipts::ReceiptCollectionReply;
    let mut owner = world();
    let registration = apply(&mut owner, Input::MaintainReceipts);
    let Effect::CollectReceipts { collection } = registration[0].1 else {
        panic!("registration")
    };
    let _ = apply(
        &mut owner,
        Input::ReceiptsCollected {
            effect: registration[0].0,
            collection,
            reply: ReceiptCollectionReply::Applied {
                closure: collection.closure,
                removed: 0,
                retained: 0,
                more: false,
            },
        },
    );
    queued(&mut owner, 0);
    let claim = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    let Effect::Persist { command } = &claim[0].1 else {
        panic!("claim")
    };
    let _ = known(&mut owner, claim[0].0, command);
    owner.receipts.test_request_sweep();
    let gc = apply(&mut owner, Input::MaintainReceipts);
    let Effect::CollectReceipts { collection } = gc[0].1 else {
        panic!("claim receipt collection")
    };
    assert_eq!(collection.closure.through.get(), 1);
    let _ = apply(
        &mut owner,
        Input::ReceiptsCollected {
            effect: gc[0].0,
            collection,
            reply: ReceiptCollectionReply::Applied {
                closure: collection.closure,
                removed: 1,
                retained: 0,
                more: false,
            },
        },
    );
    assert_eq!(owner.pubsub.free_packet_id(TARGET, 4).unwrap().get(), 2);
    let finish = apply(
        &mut owner,
        Input::Packet {
            connection: CONNECTION,
            ingress_order: 1,
            packet: InboundPacket::Puback {
                packet_id: OutboundPacketId::new(1).unwrap(),
                reason: 0,
            },
        },
    );
    let Effect::Persist { command } = &finish[0].1 else {
        panic!("finish after claim receipt retirement")
    };
    assert!(known(&mut owner, finish[0].0, command).is_empty());
    assert_eq!(owner.pubsub.free_packet_id(TARGET, 4).unwrap().get(), 1);
}

#[test]
fn an_observed_puback_target_cannot_be_rebound_after_identifier_reuse() {
    let mut owner = world();
    queued(&mut owner, 0);
    let claim = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    let Effect::Persist { command } = &claim[0].1 else {
        panic!("claim A")
    };
    let _ = known(&mut owner, claim[0].0, command);
    let key = ConnectionKey {
        session: TARGET,
        generation: CONNECTION,
    };
    let packet_id = OutboundPacketId::new(1).unwrap();
    let target = owner.puback_target(key, packet_id).unwrap();
    let finish = apply(
        &mut owner,
        Input::ObservedPuback {
            connection: key,
            packet_id,
            reason: 0,
            target: Some(target),
        },
    );
    let Effect::Persist { command } = &finish[0].1 else {
        panic!("finish A")
    };
    assert!(known(&mut owner, finish[0].0, command).is_empty());
    queued(&mut owner, 1);
    let claim_b = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    let Effect::Persist { command } = &claim_b[0].1 else {
        panic!("claim B")
    };
    let _ = known(&mut owner, claim_b[0].0, command);
    let new_target = owner.puback_target(key, packet_id).unwrap();
    assert_ne!(target, new_target);
    let effects = apply(
        &mut owner,
        Input::ObservedPuback {
            connection: key,
            packet_id,
            reason: 0,
            target: Some(target),
        },
    );
    assert!(
        effects.is_empty(),
        "stale_puback_rebound: retained A observation must not finish B: {effects:?}"
    );
    assert_eq!(owner.puback_target(key, packet_id), Some(new_target));
}

#[test]
fn an_observed_puback_without_a_target_cannot_acquire_a_future_claim() {
    let mut owner = world();
    let key = ConnectionKey {
        session: TARGET,
        generation: CONNECTION,
    };
    let packet_id = OutboundPacketId::new(1).unwrap();
    let observed = owner.puback_target(key, packet_id);
    assert_eq!(observed, None);
    queued(&mut owner, 0);
    let claim = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    let Effect::Persist { command } = &claim[0].1 else {
        panic!("claim")
    };
    let _ = known(&mut owner, claim[0].0, command);
    let effects = apply(
        &mut owner,
        Input::ObservedPuback {
            connection: key,
            packet_id,
            reason: 0,
            target: observed,
        },
    );
    assert!(
        effects.is_empty(),
        "untargeted_puback_rebound: an immutable no-target observation cannot finish a later claim: {effects:?}"
    );
}

#[test]
fn a_valid_puback_can_precede_the_local_write_callback_and_duplicates_do_not_finish_twice() {
    let mut owner = world();
    queued(&mut owner, 0);
    let claim = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    let Effect::Persist { command } = &claim[0].1 else {
        panic!("claim")
    };
    let _ = known(&mut owner, claim[0].0, command);
    assert!(!owner.pubsub.deliveries.row(0).local_write_completed);
    let key = ConnectionKey {
        session: TARGET,
        generation: CONNECTION,
    };
    let pi = OutboundPacketId::new(1).unwrap();
    let target = owner.puback_target(key, pi).unwrap();
    let finish = apply(
        &mut owner,
        Input::ObservedPuback {
            connection: key,
            packet_id: pi,
            reason: 0,
            target: Some(target),
        },
    );
    assert!(matches!(finish[0].1, Effect::Persist { .. }));
    assert_eq!(
        owner.puback_target(key, pi),
        None,
        "FinishPending has no second eligible ACK target"
    );
    assert_eq!(owner.pubsub.active(CONNECTION), 1);
    let counter = owner.counters.operation;
    assert!(
        apply(
            &mut owner,
            Input::ObservedPuback {
                connection: key,
                packet_id: pi,
                reason: 0,
                target: Some(target)
            }
        )
        .is_empty()
    );
    assert_eq!(owner.counters.operation, counter);
    assert_eq!(owner.pubsub.active(CONNECTION), 1);
    let Effect::Persist { command } = &finish[0].1 else {
        panic!("finish")
    };
    assert!(known(&mut owner, finish[0].0, command).is_empty());
    assert_eq!(owner.pubsub.active(CONNECTION), 0);
}

#[test]
fn an_immutable_target_requires_every_causal_identity_and_the_current_connection() {
    let mut owner = world();
    queued(&mut owner, 0);
    let claim = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    let Effect::Persist { command } = &claim[0].1 else {
        panic!("claim")
    };
    let _ = known(&mut owner, claim[0].0, command);
    let key = ConnectionKey {
        session: TARGET,
        generation: CONNECTION,
    };
    let pi = OutboundPacketId::new(1).unwrap();
    let target = owner.puback_target(key, pi).unwrap();
    let wrong_frame = EffectId {
        sequence: EffectSeq::new(999),
        ..target.frame
    };
    let cases = [
        (
            key,
            pi,
            Some(PubackTarget {
                delivery: delivery(99),
                ..target
            }),
        ),
        (
            key,
            pi,
            Some(PubackTarget {
                claim: operation(999),
                ..target
            }),
        ),
        (
            key,
            pi,
            Some(PubackTarget {
                frame: wrong_frame,
                ..target
            }),
        ),
        (key, OutboundPacketId::new(2).unwrap(), Some(target)),
        (
            ConnectionKey {
                session: SessionId::new(2),
                ..key
            },
            pi,
            Some(target),
        ),
        (
            ConnectionKey {
                generation: ConnectionGeneration::new(11),
                ..key
            },
            pi,
            Some(target),
        ),
    ];
    let counter = owner.counters.operation;
    for (connection, packet_id, target) in cases {
        assert!(
            apply(
                &mut owner,
                Input::ObservedPuback {
                    connection,
                    packet_id,
                    reason: 0,
                    target
                }
            )
            .is_empty()
        );
        assert_eq!(owner.counters.operation, counter);
        assert_eq!(owner.pubsub.active(CONNECTION), 1);
    }
    assert_eq!(owner.puback_target(key, pi), Some(target));
}

#[test]
fn old_finish_results_and_frame_callbacks_cannot_change_a_reused_slot() {
    let mut owner = world();
    queued(&mut owner, 0);
    let claim = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    let Effect::Persist { command } = &claim[0].1 else {
        panic!("claim A")
    };
    let emitted = known(&mut owner, claim[0].0, command);
    let key = ConnectionKey {
        session: TARGET,
        generation: CONNECTION,
    };
    let pi = OutboundPacketId::new(1).unwrap();
    let original = owner.puback_target(key, pi).unwrap();
    assert_eq!(emitted[0].0, original.frame);
    let finish = apply(
        &mut owner,
        Input::ObservedPuback {
            connection: key,
            packet_id: pi,
            reason: 0,
            target: Some(original),
        },
    );
    let Effect::Persist { command } = &finish[0].1 else {
        panic!("finish A")
    };
    let _ = known(&mut owner, finish[0].0, command);
    queued(&mut owner, 1);
    let claim_b = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    let Effect::Persist { command: second } = &claim_b[0].1 else {
        panic!("claim B")
    };
    let _ = known(&mut owner, claim_b[0].0, second);
    let current = owner.puback_target(key, pi).unwrap();
    assert_ne!(current.claim, original.claim);
    assert!(
        known(&mut owner, finish[0].0, command).is_empty(),
        "old_finish_released_reused_pi"
    );
    let before_callback = owner.pubsub.deliveries.to_vec();
    let before_counters = owner.counters;
    assert!(
        apply(
            &mut owner,
            Input::FrameResult {
                effect: original.frame,
                connection: key,
                progress: FrameProgress::Written {
                    confirmed: 20,
                    total: 20
                }
            }
        )
        .is_empty()
    );
    assert_eq!(
        owner.pubsub.deliveries.to_vec().as_slice(),
        before_callback.as_slice(),
        "stale_outgoing_frame_result_applied"
    );
    assert_eq!(owner.counters, before_counters);
    assert_eq!(owner.puback_target(key, pi), Some(current));
    assert_eq!(
        owner.pubsub.active(CONNECTION),
        1,
        "old_callback_returned_current_credit"
    );
    assert!(
        !owner.pubsub.deliveries.row(0).local_write_completed,
        "old_callback_rebound: a local callback belongs to its exact frame occurrence"
    );
}

#[test]
fn takeover_keeps_claim_and_number_but_replaces_the_connection_and_emission_target() {
    let mut owner = world();
    queued(&mut owner, 0);
    let claim = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    let Effect::Persist { command } = &claim[0].1 else {
        panic!("claim")
    };
    let _ = known(&mut owner, claim[0].0, command);
    let pi = OutboundPacketId::new(1).unwrap();
    let old_key = ConnectionKey {
        session: TARGET,
        generation: CONNECTION,
    };
    let original = owner.puback_target(old_key, pi).unwrap();
    let replacement = ConnectionGeneration::new(11);
    let _ = apply(
        &mut owner,
        Input::Packet {
            connection: replacement,
            ingress_order: 0,
            packet: InboundPacket::ConnectResume {
                client_id: bytes(b"subscriber"),
            },
        },
    );
    let _ = apply(
        &mut owner,
        Input::ConnectionSettings {
            connection: replacement,
            receive_maximum: 8,
            maximum_packet_size: 65536,
        },
    );
    assert_eq!(owner.puback_target(old_key, pi), None);
    assert_eq!(owner.pubsub.free_packet_id(TARGET, 4).unwrap().get(), 2);
    let replay = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    assert!(
        matches!(replay[0].1,Effect::EmitPublish {packet_id,claim,dup:true,..} if packet_id==pi && claim==original.claim)
    );
    let key = ConnectionKey {
        session: TARGET,
        generation: replacement,
    };
    let current = owner.puback_target(key, pi).unwrap();
    assert_eq!(current.delivery, original.delivery);
    assert_eq!(current.claim, original.claim);
    assert_ne!(current.frame, original.frame);
    assert!(
        apply(
            &mut owner,
            Input::ObservedPuback {
                connection: old_key,
                packet_id: pi,
                reason: 0,
                target: Some(original)
            }
        )
        .is_empty()
    );
    assert!(
        apply(
            &mut owner,
            Input::ObservedPuback {
                connection: key,
                packet_id: pi,
                reason: 0,
                target: Some(original)
            }
        )
        .is_empty()
    );
    assert!(
        apply(
            &mut owner,
            Input::FrameResult {
                effect: original.frame,
                connection: old_key,
                progress: FrameProgress::Written {
                    confirmed: 20,
                    total: 20
                }
            }
        )
        .is_empty()
    );
    assert_eq!(owner.puback_target(key, pi), Some(current));
    let finish = apply(
        &mut owner,
        Input::ObservedPuback {
            connection: key,
            packet_id: pi,
            reason: 0,
            target: Some(current),
        },
    );
    let Effect::Persist { command } = &finish[0].1 else {
        panic!("current target finishes")
    };
    let _ = known(&mut owner, finish[0].0, command);
    assert_eq!(owner.pubsub.free_packet_id(TARGET, 4).unwrap().get(), 1);
}

#[test]
fn an_unknown_claim_keeps_its_packet_identifier_unavailable() {
    let mut owner = world();
    queued(&mut owner, 0);
    let claim = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    let Effect::Persist { command } = &claim[0].1 else {
        panic!("claim")
    };
    assert!(
        apply(
            &mut owner,
            Input::StoreResult {
                effect: claim[0].0,
                operation: command.recipe.operation(),
                reply: StoreReply::Outcome(StoreOutcome::PendingOrUnknown)
            }
        )
        .is_empty()
    );
    assert_eq!(
        owner.pubsub.free_packet_id(TARGET, 4).unwrap().get(),
        2,
        "occupied_unknown_pi_reused"
    );
}
fn retained_zero(order: u64) -> Input {
    Input::Packet {
        connection: CONNECTION,
        ingress_order: order,
        packet: InboundPacket::Publish {
            expiry: None,
            properties: None,
            qos: Qos::AtMostOnce,
            retain: true,
            dup: false,
            packet_id: None,
            topic: bytes(b"retained/value"),
            payload: bytes(b"value"),
        },
    }
}
fn pending_retained(owner: &mut Owner) -> (EffectId, Command, RetainedFlight) {
    owner.lifecycle_enabled = true;
    let effects = apply(owner, retained_zero(1));
    let (effect, command) = effects
        .into_iter()
        .find_map(|(effect, item)| match item {
            Effect::Persist { command } => Some((effect, command)),
            _ => None,
        })
        .unwrap();
    let flight = owner.pubsub.retained.clone().unwrap();
    (effect, command, flight)
}
#[test]
fn retained_qos0_unknown_survives_source_termination_and_resolves_without_an_ack() {
    use crate::contract::session::{SessionCancellations, SessionChange, SessionEndReason};
    let mut owner = world();
    let (effect, command, flight) = pending_retained(&mut owner);
    let operation = command.recipe.operation();
    let Recipe::RetainedQos0(value) = command.recipe.get() else {
        panic!("retained QoS0")
    };
    assert_eq!(value.costs.exchange_slots, ExchangeSlots::ZERO);
    assert_eq!(value.costs.references, References::new(1));
    assert!(owner.slots.is_empty());
    assert_eq!(owner.leases.held(), 0);
    assert!(
        apply(
            &mut owner,
            Input::StoreResult {
                effect,
                operation,
                reply: StoreReply::Outcome(StoreOutcome::PendingOrUnknown)
            }
        )
        .is_empty()
    );
    assert!(matches!(
        owner.step_checked(retained_zero(2), TURN),
        StepOutcome::NeedCapacity {
            need: CapacityNeed {
                resource: Resource::ResultSlots,
                ..
            },
            ..
        }
    ));
    apply(
        &mut owner,
        Input::ConnectionClosed {
            connection: CONNECTION,
        },
    );
    let close = apply(
        &mut owner,
        Input::EndSession {
            connection: ConnectionKey {
                session: TARGET,
                generation: CONNECTION,
            },
            opened_by: None,
            reason: SessionEndReason::Disconnect,
        },
    );
    let (close_effect, close_operation) = close
        .iter()
        .find_map(|(effect, item)| match item {
            Effect::Persist { command } => Some((*effect, command.recipe.operation())),
            _ => None,
        })
        .unwrap();
    apply(
        &mut owner,
        Input::StoreResult {
            effect: close_effect,
            operation: close_operation,
            reply: StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::SessionLifecycle(
                SessionChange::Closed {
                    session: TARGET,
                    route_revision: Revision::new(0),
                    cancelled: SessionCancellations::default(),
                },
            ))),
        },
    );
    assert!(
        owner.pubsub.retained.as_ref().unwrap().retired,
        "source_termination_lost_unknown_retained_qos0"
    );
    assert_eq!(owner.pubsub.retired_flights(), 1);
    let resolution = apply(
        &mut owner,
        Input::Deadline {
            token: flight.in_flight.deadline,
            now: crate::contract::ids::LogicalTime::new(10000),
        },
    );
    let (resolved, actual) = resolution
        .iter()
        .find_map(|(effect, item)| match item {
            Effect::ResolveAndFence { operation } => Some((*effect, *operation)),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        actual, operation,
        "retained_qos0_resolution_changed_operation"
    );
    assert!(
        !resolution
            .iter()
            .any(|(_, item)| matches!(item, Effect::Persist { .. } | Effect::EmitFrame { .. }))
    );
    let done = StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::RetainedQos0Stored {
        publication: flight.publication,
    }));
    assert!(
        apply(
            &mut owner,
            Input::StoreResult {
                effect: resolved,
                operation,
                reply: done
            }
        )
        .is_empty()
    );
    assert!(owner.pubsub.retained.is_none());
    assert_eq!(owner.pubsub.retired_flights(), 0);
    assert!(owner.slots.is_empty());
    assert_eq!(owner.leases.held(), 0);
    apply(
        &mut owner,
        Input::Packet {
            connection: ConnectionGeneration::new(11),
            ingress_order: 0,
            packet: InboundPacket::ConnectResume {
                client_id: bytes(b"other"),
            },
        },
    );
    apply(
        &mut owner,
        Input::ConnectionSettings {
            connection: ConnectionGeneration::new(11),
            receive_maximum: 8,
            maximum_packet_size: 65536,
        },
    );
    let mut next = retained_zero(1);
    if let Input::Packet { connection, .. } = &mut next {
        *connection = ConnectionGeneration::new(11);
    }
    apply(&mut owner, next);
    let current = owner.pubsub.retained.clone().unwrap();
    assert_ne!(current.in_flight.operation, operation);
    assert!(
        apply(
            &mut owner,
            Input::StoreResult {
                effect,
                operation,
                reply: done
            }
        )
        .is_empty()
    );
    assert_eq!(
        owner.pubsub.retained.as_ref(),
        Some(&current),
        "old_retained_qos0_result_rebound"
    );
    apply(
        &mut owner,
        Input::StoreResult {
            effect: current.in_flight.answers.persist_id(),
            operation: current.in_flight.operation,
            reply: StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::RetainedQos0Stored {
                publication: current.publication,
            })),
        },
    );
    assert!(owner.pubsub.retained.is_none());
}
#[test]
fn retained_qos0_certain_capacity_refusal_is_an_explicit_drop_without_mqtt_credit() {
    let mut owner = world();
    let (effect, command, _) = pending_retained(&mut owner);
    let effects = apply(
        &mut owner,
        Input::StoreResult {
            effect,
            operation: command.recipe.operation(),
            reply: StoreReply::Outcome(StoreOutcome::RejectedCertain(
                RejectReason::CapacityExceeded(Resource::References),
            )),
        },
    );
    assert!(
        matches!(
            effects.as_slice(),
            [(
                _,
                Effect::Qos0Dropped {
                    reason: Qos0DropReason::CapacityExceeded,
                    ..
                }
            )]
        ),
        "retained_qos0_refusal_became_success_or_silent_loss"
    );
    assert!(owner.pubsub.retained.is_none());
    assert!(owner.slots.is_empty());
    assert_eq!(owner.leases.held(), 0);
    apply(&mut owner, retained_zero(2));
    assert!(owner.pubsub.retained.is_some());
}
fn product_subscribe() -> Input {
    Input::Packet {
        connection: CONNECTION,
        ingress_order: 1,
        packet: InboundPacket::Subscribe {
            denied: 0,
            packet_id: InboundPacketId::new(3).unwrap(),
            filters: crate::contract::pubsub::FilterList::copy_from(&[
                crate::contract::pubsub::SubscriptionFilter {
                    filter: bytes(b"retained/#"),
                    qos: Qos::AtLeastOnce,
                    options: crate::contract::pubsub::SubscriptionOptions::DEFAULT,
                },
            ])
            .unwrap(),
        },
    }
}
#[test]
fn subscription_replay_reserves_the_whole_range_before_submission_and_confirmation() {
    use crate::contract::receipts::ReceiptCollectionReply;
    let mut owner = world();
    owner.lifecycle_enabled = true;
    let collection = apply(&mut owner, Input::MaintainReceipts);
    let (effect, request) = collection
        .iter()
        .find_map(|(effect, item)| match item {
            Effect::CollectReceipts { collection } => Some((*effect, *collection)),
            _ => None,
        })
        .unwrap();
    apply(
        &mut owner,
        Input::ReceiptsCollected {
            effect,
            collection: request,
            reply: ReceiptCollectionReply::Applied {
                closure: request.closure,
                removed: 0,
                retained: 0,
                more: false,
            },
        },
    );
    let effects = apply(&mut owner, product_subscribe());
    let (effect, command) = effects
        .iter()
        .find_map(|(effect, item)| match item {
            Effect::Persist { command } => Some((*effect, command)),
            _ => None,
        })
        .unwrap();
    let Recipe::ReplaySubscriptions(value) = command.recipe.get() else {
        panic!("product_subscription_did_not_reserve_replay");
    };
    let first = value.first_publication.sequence().get();
    assert_eq!(
        owner.counters.publication.unwrap().get(),
        first + crate::contract::retained::MAX_REPLAYS as u64,
        "replay_range_not_burned_before_submission"
    );
    let operation = command.recipe.operation();
    apply(
        &mut owner,
        Input::StoreResult {
            effect,
            operation,
            reply: StoreReply::Outcome(StoreOutcome::PendingOrUnknown),
        },
    );
    assert!(owner.routes.is_empty());
    apply(
        &mut owner,
        Input::StoreResult {
            effect,
            operation,
            reply: StoreReply::Outcome(StoreOutcome::Committed(
                CommitFacts::SubscriptionsChanged {
                    session: TARGET,
                    route_revision: Revision::new(1),
                },
            )),
        },
    );
    assert_eq!(owner.routes.len(), 1);
    // Result consumption is separate from the replay obligations' lifetime.
    let status = owner.receipt_status().unwrap();
    assert_eq!((status.1, status.2), (1, 0));
    for revision in 2..=64 {
        let effects = apply(&mut owner, product_subscribe());
        let (effect, operation) = effects
            .iter()
            .find_map(|(effect, item)| match item {
                Effect::Persist { command } => Some((*effect, command.recipe.operation())),
                _ => None,
            })
            .unwrap();
        apply(
            &mut owner,
            Input::StoreResult {
                effect,
                operation,
                reply: StoreReply::Outcome(StoreOutcome::Committed(
                    CommitFacts::SubscriptionsChanged {
                        session: TARGET,
                        route_revision: Revision::new(revision),
                    },
                )),
            },
        );
    }
    let maintenance = apply(&mut owner, Input::MaintainReceipts);
    let closure = maintenance
        .iter()
        .find_map(|(_, item)| match item {
            Effect::CollectReceipts { collection } => Some(collection.closure),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        closure.publication_through.get(),
        first + 64 * crate::contract::retained::MAX_REPLAYS as u64 - 1,
        "receipt_closure_lost_burned_replay_suffix"
    );
    // The issue record itself proves the entire range belongs to this op;
    // a later mutation starts after it, never in an unused replay suffix.
    let later = apply(&mut owner, retained_zero(2));
    let Recipe::RetainedQos0(next) = later
        .iter()
        .find_map(|(_, item)| match item {
            Effect::Persist { command } => Some(command.recipe.get()),
            _ => None,
        })
        .unwrap()
    else {
        panic!("retained QoS0")
    };
    assert_eq!(
        next.publication.sequence().get(),
        first + 64 * crate::contract::retained::MAX_REPLAYS as u64
    );
}
#[test]
fn replay_range_exhaustion_refuses_before_any_counter_projection_or_effect_mutation() {
    let mut owner = world();
    owner.lifecycle_enabled = true;
    owner.counters.publication = Some(PublicationSeq::new(u64::MAX - 254));
    let before = owner.counters;
    let result = owner.step_checked(product_subscribe(), TURN);
    assert!(
        matches!(result, StepOutcome::Exhausted { .. }),
        "replay_reservation_wrapped_or_submitted_a_partial_range"
    );
    assert_eq!(owner.counters, before);
    assert!(owner.pubsub.control.is_none() && owner.routes.is_empty());
    assert!(drain(&mut owner).is_empty());
}
#[test]
fn loaded_retained_delivery_flags_survive_claim_and_consume_knowledge() {
    for qos in [Qos::AtMostOnce, Qos::AtLeastOnce] {
        for recovered_metadata in [false, true] {
            let mut owner = world();
            if recovered_metadata {
                owner
                    .pubsub
                    .deliveries
                    .push(Delivery::metadata(
                        delivery(0),
                        TARGET,
                        Revision::new(1),
                        qos,
                        None,
                    ))
                    .unwrap();
            }
            let effects = apply(&mut owner, Input::WorkAvailable { session: TARGET });
            let effect = effects
                .iter()
                .find_map(|(effect, item)| {
                    matches!(item, Effect::LoadWork { .. }).then_some(*effect)
                })
                .unwrap();
            let item = WorkItem {
                cause: DeliveryCause::RetainedReplay,
                expiry: None,
                properties: None,
                delivery: delivery(0),
                target: TARGET,
                target_revision: Revision::new(1),
                qos,
                retain: true,
                topic: bytes(b"retained/replay"),
                payload: bytes(b"frozen-version"),
                claim: None,
            };
            apply(
                &mut owner,
                Input::WorkPage {
                    effect,
                    session: TARGET,
                    items: BoundedVec::from_slice(core::slice::from_ref(&item), 1).unwrap(),
                    next: None,
                },
            );
            assert!(owner.pubsub.deliveries.row(0).retain);
            let effects = apply(&mut owner, Input::WorkAvailable { session: TARGET });
            let (effect, command) = effects
                .iter()
                .find_map(|(effect, item)| match item {
                    Effect::Persist { command } => Some((*effect, command)),
                    _ => None,
                })
                .unwrap();
            let emitted = known(&mut owner, effect, command);
            assert!(
                emitted.iter().any(|(_, item)| matches!(
                    item,
                    Effect::EmitPublish { retain: true, .. }
                        | Effect::EmitQos0 { retain: true, .. }
                )),
                "loaded_retain_flag_was_not_frozen_for_outgoing_delivery"
            );
        }
    }
}
#[test]
fn a_read_started_before_an_older_obligation_revisits_the_lower_range() {
    fn id(sequence: u64) -> DeliveryId {
        DeliveryId {
            publication: PublicationId::new(
                StoreIncarnation::new(1),
                RunEpoch::new(1),
                LogicalOwnerId::new(4),
                PublicationSeq::new(sequence),
            ),
            index: 0,
        }
    }
    fn load(owner: &mut Owner) -> (EffectId, Option<DeliveryId>) {
        apply(owner, Input::WorkAvailable { session: TARGET })
            .into_iter()
            .find_map(|(effect, value)| match value {
                Effect::LoadWork { after, .. } => Some((effect, after)),
                _ => None,
            })
            .expect("work discovery should issue one bounded page read")
    }
    for next in [None, Some(id(60))] {
        let mut owner = world();
        owner.enable_inline_claims();
        message_clock(&mut owner, 30);
        owner
            .pubsub
            .connections
            .advance_known_through(CONNECTION, id(50));
        let (effect, after) = load(&mut owner);
        assert_eq!(after, Some(id(50)));

        // The writer took this page's snapshot before an earlier-reserved
        // publication (40) became an obligation for TARGET.
        owner.shard_notify_dirty(TARGET, true);
        assert!(apply(&mut owner, Input::WorkAvailable { session: TARGET }).is_empty());
        let stale = WorkItem {
            delivery: id(60),
            target: TARGET,
            target_revision: Revision::new(1),
            qos: Qos::AtLeastOnce,
            cause: DeliveryCause::LiveExact,
            retain: false,
            // The stale higher row needs no claim. Its identifier is still
            // a page cursor even when expiry skips materializing the row.
            expiry: Some(message_expiry()),
            properties: None,
            topic: bytes(b"t/a"),
            payload: bytes(b"high"),
            claim: None,
        };
        apply(
            &mut owner,
            Input::WorkPage {
                effect,
                session: TARGET,
                items: BoundedVec::from_slice(&[stale], 1).unwrap(),
                next,
            },
        );

        let mut found_lower = false;
        // One page remains at most. A design can finish that chain before
        // revisiting the start, or rewind immediately.
        for _ in 0..2 {
            if !owner.has_ready_work(TARGET) {
                break;
            }
            let (effect, after) = load(&mut owner);
            if after.is_none_or(|cursor| cursor < id(40)) {
                found_lower = true;
                break;
            }
            apply(
                &mut owner,
                Input::WorkPage {
                    effect,
                    session: TARGET,
                    items: BoundedVec::with_capacity(1).unwrap(),
                    next: None,
                },
            );
        }
        assert!(
            found_lower,
            "a lower obligation remains invisible after next={next:?}"
        );
    }
}

#[test]
fn a_later_work_page_cannot_rewrite_an_already_loaded_retain_flag() {
    let mut owner = world();
    queued(&mut owner, 0);
    owner.pubsub.deliveries.row_mut(0).retain = true;
    let claim = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    let Effect::Persist { command } = &claim[0].1 else {
        panic!("claim")
    };
    known(&mut owner, claim[0].0, command);
    let read = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    let effect = read
        .iter()
        .find_map(|(effect, item)| matches!(item, Effect::LoadWork { .. }).then_some(*effect))
        .unwrap();
    let held = owner.pubsub.deliveries.row(0);
    let changed = WorkItem {
        cause: DeliveryCause::LiveExact,
        expiry: None,
        properties: None,
        delivery: held.id,
        target: held.target,
        target_revision: held.revision,
        qos: held.qos,
        retain: false,
        topic: held.topic.clone().unwrap(),
        payload: held.payload.clone().unwrap(),
        claim: held.claim,
    };
    let result = owner.step_checked(
        Input::WorkPage {
            effect,
            session: TARGET,
            items: BoundedVec::from_slice(&[changed], 1).unwrap(),
            next: None,
        },
        TURN,
    );
    assert!(
        matches!(result, StepOutcome::Fatal(CoreFault::ImpossibleInput)),
        "work_page_rewrote_frozen_retain_flag"
    );
    assert!(owner.pubsub.deliveries.row(0).retain);
}

fn message_time(seconds: u64) -> MessageTime {
    MessageTime {
        domain: [9; 32],
        tick: seconds * crate::contract::message_expiry::NANOS_PER_SECOND,
    }
}
fn message_expiry() -> MessageExpiry {
    MessageExpiry {
        observed: message_time(10),
        interval: 2,
    }
}
fn message_clock(owner: &mut Owner, seconds: u64) {
    assert!(
        apply(
            owner,
            Input::MessageClock {
                now: message_time(seconds)
            }
        )
        .is_empty()
    );
}
fn message_command(effects: Vec<(EffectId, Effect)>) -> (EffectId, Command) {
    effects
        .into_iter()
        .find_map(|(id, effect)| match effect {
            Effect::Persist { command } => Some((id, command)),
            _ => None,
        })
        .expect("durable command")
}
fn message_frame(owner: &mut Owner, index: u32) -> (EffectId, ConnectionKey, OutboundPacketId) {
    queued(owner, index);
    owner.pubsub.deliveries.last_mut().unwrap().expiry = Some(Box::new(message_expiry()));
    let (effect, command) = message_command(apply(owner, Input::WorkAvailable { session: TARGET }));
    let emitted = known(owner, effect, &command);
    let (effect, connection, packet_id) = emitted
        .into_iter()
        .find_map(|(id, effect)| match effect {
            Effect::EmitPublish {
                connection,
                packet_id,
                expiry,
                started,
                ..
            } => {
                assert_eq!(expiry.as_deref().copied(), Some(message_expiry()));
                assert!(!started);
                Some((id, connection, packet_id))
            }
            _ => None,
        })
        .expect("fresh emission");
    (effect, connection, packet_id)
}
fn message_progress(
    owner: &mut Owner,
    effect: EffectId,
    connection: ConnectionKey,
    progress: FrameProgress,
) -> Vec<(EffectId, Effect)> {
    apply(
        owner,
        Input::FrameResult {
            effect,
            connection,
            progress,
        },
    )
}
fn message_puback(
    owner: &mut Owner,
    connection: ConnectionKey,
    packet_id: OutboundPacketId,
) -> Vec<(EffectId, Effect)> {
    let target = owner.puback_target(connection, packet_id);
    apply(
        owner,
        Input::ObservedPuback {
            connection,
            packet_id,
            reason: 0,
            target,
        },
    )
}

#[test]
fn message_expiry_queued_and_unknown_domain_work_never_claims_consumes_or_emits() {
    for qos in [Qos::AtMostOnce, Qos::AtLeastOnce] {
        for unknown in [false, true] {
            let mut owner = world();
            queued(&mut owner, 0);
            let d = owner.pubsub.deliveries.row_mut(0);
            d.expiry = Some(Box::new(message_expiry()));
            d.qos = qos;
            let mut now = message_time(12);
            if unknown {
                now.domain = [7; 32];
            }
            apply(&mut owner, Input::MessageClock { now });
            assert!(!owner.has_ready_work(TARGET), "quarantined_work_busy_loop");
            let effects = apply(&mut owner, Input::WorkAvailable { session: TARGET });
            assert!(
                !effects.iter().any(|(_, e)| matches!(
                    e,
                    Effect::Persist { .. } | Effect::EmitPublish { .. } | Effect::EmitQos0 { .. }
                )),
                "expired_queued_emitted"
            );
            assert_eq!(
                owner.pubsub.deliveries.len(),
                usize::from(unknown),
                "uncertain_cache_was_freed_or_expired_cache_pinned"
            );
            assert!(owner.pubsub.deliveries.values().all(|d| d.claim.is_none()));
        }
    }
}

#[test]
fn message_expiry_every_clocked_product_subscription_freezes_the_observation_and_range() {
    let mut owner = world();
    // The product clock, rather than a particular session entry point,
    // selects the time-aware recipe for every product SUBSCRIBE.
    message_clock(&mut owner, 10);
    let first = owner.counters.publication.unwrap();
    let (effect, command) = message_command(apply(&mut owner, product_subscribe()));
    let Recipe::TimedReplaySubscriptions(timed) = command.recipe.get() else {
        panic!("product_subscribe_omitted_message_time");
    };
    assert_eq!(timed.observed, message_time(10));
    assert_eq!(timed.replay.first_publication.sequence(), first);
    assert_eq!(
        owner.counters.publication.unwrap().get(),
        first.get() + crate::contract::retained::MAX_REPLAYS as u64
    );
    let operation = command.recipe.operation();
    message_clock(&mut owner, 11);
    apply(
        &mut owner,
        Input::StoreResult {
            effect,
            operation,
            reply: StoreReply::Outcome(StoreOutcome::PendingOrUnknown),
        },
    );
    let deadline = owner.pubsub.control.as_ref().unwrap().in_flight.deadline;
    let resolutions = apply(
        &mut owner,
        Input::Deadline {
            token: deadline,
            now: crate::contract::ids::LogicalTime::new(50),
        },
    );
    assert!(
        resolutions.iter().any(
            |(_, e)| matches!(e, Effect::ResolveAndFence { operation: op } if *op == operation)
        )
    );
    assert_eq!(
        timed.observed,
        message_time(10),
        "replay_refreshed_expiry_origin"
    );
}

#[test]
fn closing_shard_with_early_puback_requires_a_reported_first_byte() {
    use crate::owner::ShardAckProgress;

    let mut owner = world();
    message_clock(&mut owner, 10);
    let (frame, key, pi) = message_frame(&mut owner, 0);
    assert!(message_puback(&mut owner, key, pi).is_empty());
    assert!(message_progress(&mut owner, frame, key, FrameProgress::Failed).is_empty());
    assert_eq!(owner.shard_ack_progress(key), ShardAckProgress::Settled);
    owner
        .shard_detach_session(key.session, key.generation)
        .unwrap();
    let current = ConnectionKey {
        session: TARGET,
        generation: ConnectionGeneration::new(11),
    };
    let revision = owner.pubsub.deliveries.row(0).revision;
    owner
        .shard_attach_session(TARGET, revision, current.generation, 8, 65536)
        .unwrap();
    let replay = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    let replay = replay
        .into_iter()
        .find_map(|(id, e)| {
            matches!(e, Effect::EmitPublish { packet_id, .. } if packet_id == pi).then_some(id)
        })
        .expect("same packet identifier on the resumed session");
    let (id, started) = message_command(message_progress(
        &mut owner,
        replay,
        current,
        FrameProgress::Started,
    ));
    assert!(
        known(&mut owner, id, &started).is_empty(),
        "the new transmission requires its own PUBACK"
    );
    let (id, finish) = message_command(message_puback(&mut owner, current, pi));
    assert!(known(&mut owner, id, &finish).is_empty());
}

#[test]
fn closing_shard_settles_a_certainly_rejected_early_finish() {
    use crate::owner::ShardAckProgress;

    let mut owner = world();
    message_clock(&mut owner, 10);
    let (frame, key, pi) = message_frame(&mut owner, 0);
    assert!(message_puback(&mut owner, key, pi).is_empty());
    let (start_id, start) = message_command(message_progress(
        &mut owner,
        frame,
        key,
        FrameProgress::Started,
    ));
    let (id, finish) = message_command(known(&mut owner, start_id, &start));
    assert_eq!(owner.shard_ack_progress(key), ShardAckProgress::Waiting);
    let effects = apply(
        &mut owner,
        Input::StoreResult {
            effect: id,
            operation: finish.recipe.operation(),
            reply: StoreReply::Outcome(StoreOutcome::RejectedCertain(RejectReason::Fenced)),
        },
    );
    assert!(
        effects
            .iter()
            .any(|(_, e)| matches!(e, Effect::CloseConnection { .. }))
    );
    assert_eq!(owner.shard_ack_progress(key), ShardAckProgress::Settled);
}

#[test]
fn message_expiry_puback_before_started_is_held_through_unknown_marker_and_expiration() {
    let mut owner = world();
    message_clock(&mut owner, 10);
    let (frame, key, pi) = message_frame(&mut owner, 0);
    let target = owner.puback_target(key, pi).unwrap();
    assert!(message_puback(&mut owner, key, pi).is_empty());
    assert!(owner.pubsub.deliveries.row(0).early_puback);
    assert!(
        message_progress(
            &mut owner,
            frame,
            key,
            FrameProgress::Written {
                confirmed: 18,
                total: 18
            }
        )
        .is_empty()
    );
    assert!(
        !owner.pubsub.deliveries.row(0).claim.unwrap().started,
        "complete_callback_invented_durable_started"
    );
    message_clock(&mut owner, 12);
    let (started_effect, started) = message_command(message_progress(
        &mut owner,
        frame,
        key,
        FrameProgress::Started,
    ));
    assert!(
        matches!(started.recipe.get(), Recipe::MarkDeliveryStarted(mark) if mark.claim == target.claim && mark.delivery == target.delivery)
    );
    assert_eq!(
        owner.shard_ack_progress(key),
        crate::owner::ShardAckProgress::Waiting
    );
    let operation = started.recipe.operation();
    assert!(
        apply(
            &mut owner,
            Input::StoreResult {
                effect: started_effect,
                operation,
                reply: StoreReply::Outcome(StoreOutcome::PendingOrUnknown)
            }
        )
        .is_empty()
    );
    assert!(message_puback(&mut owner, key, pi).is_empty());
    assert!(message_progress(&mut owner, frame, key, FrameProgress::Started).is_empty());
    let deadline = owner.pubsub.deliveries.row(0).in_flight().unwrap().deadline;
    let effects = apply(
        &mut owner,
        Input::Deadline {
            token: deadline,
            now: crate::contract::ids::LogicalTime::new(20),
        },
    );
    let resolve = effects
        .iter()
        .find_map(|(id, e)| {
            matches!(e, Effect::ResolveAndFence { operation: op } if *op == operation)
                .then_some(*id)
        })
        .unwrap();
    let finish = known(&mut owner, resolve, &started);
    let (finish_effect, finish) = message_command(finish);
    assert!(
        matches!(finish.recipe.get(), Recipe::FinishDelivery(end) if end.claim == target.claim)
    );
    assert!(owner.pubsub.deliveries.row(0).claim.unwrap().started);
    assert_eq!(
        owner.pubsub.free_packet_id(TARGET, 1),
        None,
        "marker_result_freed_pi"
    );
    assert_eq!(
        owner.shard_ack_progress(key),
        crate::owner::ShardAckProgress::Waiting
    );
    assert!(known(&mut owner, finish_effect, &finish).is_empty());
    assert!(owner.pubsub.deliveries.is_empty());
    assert!(message_progress(&mut owner, frame, key, FrameProgress::Started).is_empty());
    assert!(known(&mut owner, started_effect, &started).is_empty());
}

#[test]
fn message_expiry_started_claim_replays_past_ttl_and_old_facts_cannot_hit_reused_pi() {
    let mut owner = world();
    message_clock(&mut owner, 10);
    let (frame, key, pi) = message_frame(&mut owner, 0);
    let original = owner.puback_target(key, pi).unwrap();
    let (start_effect, start) = message_command(message_progress(
        &mut owner,
        frame,
        key,
        FrameProgress::Started,
    ));
    assert!(known(&mut owner, start_effect, &start).is_empty());
    message_clock(&mut owner, 13);
    apply(
        &mut owner,
        Input::ConnectionClosed {
            connection: CONNECTION,
        },
    );
    apply(
        &mut owner,
        Input::Packet {
            connection: ConnectionGeneration::new(11),
            ingress_order: 0,
            packet: InboundPacket::ConnectResume {
                client_id: bytes(b"subscriber"),
            },
        },
    );
    apply(
        &mut owner,
        Input::ConnectionSettings {
            connection: ConnectionGeneration::new(11),
            receive_maximum: 8,
            maximum_packet_size: 65536,
        },
    );
    let replay = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    assert!(replay.iter().any(|(_, e)| matches!(e, Effect::EmitPublish { started: true, expiry: Some(expiry), packet_id, claim, dup: true, .. } if **expiry == message_expiry() && *packet_id == pi && *claim == original.claim)), "known_started_not_replayed_past_ttl");
    let current = ConnectionKey {
        session: TARGET,
        generation: ConnectionGeneration::new(11),
    };
    let (end_effect, end) = message_command(message_puback(&mut owner, current, pi));
    known(&mut owner, end_effect, &end);
    // Reuse the same small-domain PI for another occurrence.
    queued(&mut owner, 1);
    let (claim_effect, claim) =
        message_command(apply(&mut owner, Input::WorkAvailable { session: TARGET }));
    known(&mut owner, claim_effect, &claim);
    let newer = owner.puback_target(current, pi).unwrap();
    let held = owner.pubsub.deliveries.row(0).clone();
    for progress in [
        FrameProgress::Started,
        FrameProgress::NotStarted {
            reason: MessageExpiryBlock::Expired,
        },
        FrameProgress::Written {
            confirmed: 18,
            total: 18,
        },
    ] {
        assert!(message_progress(&mut owner, frame, key, progress).is_empty());
    }
    assert!(known(&mut owner, start_effect, &start).is_empty());
    assert!(known(&mut owner, end_effect, &end).is_empty());
    assert_eq!(owner.puback_target(current, pi), Some(newer));
    assert_eq!(
        owner.pubsub.deliveries.row(0),
        &held,
        "old_fact_changed_reused_pi"
    );
}

#[test]
fn message_expiry_certain_first_not_started_uses_correlated_expiry_and_unknown_resolution() {
    let mut owner = world();
    message_clock(&mut owner, 10);
    let (frame, key, _) = message_frame(&mut owner, 0);
    message_clock(&mut owner, 12);
    let (expire_effect, expire) = message_command(message_progress(
        &mut owner,
        frame,
        key,
        FrameProgress::NotStarted {
            reason: MessageExpiryBlock::Expired,
        },
    ));
    let Recipe::ExpireDelivery(expiry) = expire.recipe.get() else {
        panic!("certain_unsent_not_expired");
    };
    assert_eq!(expiry.expiry, message_expiry());
    assert_eq!(expiry.observed, message_time(12));
    let operation = expire.recipe.operation();
    apply(
        &mut owner,
        Input::StoreResult {
            effect: expire_effect,
            operation,
            reply: StoreReply::Outcome(StoreOutcome::PendingOrUnknown),
        },
    );
    assert_eq!(owner.pubsub.free_packet_id(TARGET, 1), None);
    assert!(message_progress(&mut owner, frame, key, FrameProgress::Started).is_empty());
    let deadline = owner.pubsub.deliveries.row(0).in_flight().unwrap().deadline;
    let effects = apply(
        &mut owner,
        Input::Deadline {
            token: deadline,
            now: crate::contract::ids::LogicalTime::new(20),
        },
    );
    let resolve = effects
        .iter()
        .find_map(|(id, e)| {
            matches!(e, Effect::ResolveAndFence { operation: op } if *op == operation)
                .then_some(*id)
        })
        .unwrap();
    known(&mut owner, resolve, &expire);
    assert!(owner.pubsub.deliveries.is_empty());
    assert!(known(&mut owner, expire_effect, &expire).is_empty());
    assert_eq!(owner.pubsub.free_packet_id(TARGET, 1).unwrap().get(), 1);
}

#[test]
fn message_expiry_complete_write_without_started_callback_is_not_certainly_unsent() {
    let mut owner = world();
    message_clock(&mut owner, 10);
    let (frame, key, _) = message_frame(&mut owner, 0);
    message_progress(
        &mut owner,
        frame,
        key,
        FrameProgress::Written {
            confirmed: 18,
            total: 18,
        },
    );
    message_clock(&mut owner, 12);
    let effects = message_progress(
        &mut owner,
        frame,
        key,
        FrameProgress::NotStarted {
            reason: MessageExpiryBlock::Expired,
        },
    );
    assert!(
        effects.is_empty(),
        "written_frame_treated_as_certainly_unsent"
    );
    assert_eq!(owner.pubsub.deliveries.len(), 1);
}

#[test]
fn message_expiry_recovered_or_previously_uncertain_claim_never_becomes_certainly_unsent() {
    for unknown in [false, true] {
        let mut owner = world();
        message_clock(&mut owner, 10);
        queued(&mut owner, 0);
        let d = owner.pubsub.deliveries.row_mut(0);
        d.expiry = Some(Box::new(message_expiry()));
        d.claim = Some(DeliveryClaim {
            operation: operation(100),
            packet_id: OutboundPacketId::new(1).unwrap(),
            started: false,
            await_pubcomp: false,
        });
        d.phase = Phase::AwaitPuback;
        d.certain_never_started = false;
        d.may_have_started = true;
        let emitted = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        let (frame, key) = emitted
            .into_iter()
            .find_map(|(id, e)| match e {
                Effect::EmitPublish { connection, .. } => Some((id, connection)),
                _ => None,
            })
            .unwrap();
        let mut now = message_time(12);
        if unknown {
            now.domain = [8; 32];
        }
        apply(&mut owner, Input::MessageClock { now });
        let reason = if unknown {
            MessageExpiryBlock::TemporalUncertain
        } else {
            MessageExpiryBlock::Expired
        };
        assert!(
            message_progress(&mut owner, frame, key, FrameProgress::NotStarted { reason })
                .is_empty()
        );
        let effects = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        assert!(
            !effects
                .iter()
                .any(|(_, e)| matches!(e, Effect::Persist { .. } | Effect::EmitPublish { .. }))
        );
        assert_eq!(owner.pubsub.free_packet_id(TARGET, 1), None);
        assert_eq!(owner.pubsub.deliveries.len(), 1);
    }
}

#[test]
fn message_expiry_property_and_remaining_length_are_counted_at_both_qos_levels() {
    let mut owner = world();
    message_clock(&mut owner, 10);
    let settings = &mut owner.pubsub.connections.as_mut_slice()[0];
    settings.maximum_packet_size = 128;
    let key = ConnectionKey {
        session: TARGET,
        generation: CONNECTION,
    };
    let topic = bytes(b"topic");
    let payload = SharedBytes::copy_from(&[1; 115], 128).unwrap();
    assert!(owner.qos0_fits(key, &topic, &payload, None, None));
    assert!(
        !owner.qos0_fits(key, &topic, &payload, Some(message_expiry()), None),
        "qos0_expiry_property_not_charged"
    );
    // The QoS 1 half pinned the closure of the connection. Its purpose
    // is unchanged: the Message Expiry property is charged. The same
    // bytes fit without it (127 bytes) and are discarded with it (133).
    for timed in [false, true] {
        let mut owner = world();
        message_clock(&mut owner, 10);
        owner.pubsub.connections.as_mut_slice()[0].maximum_packet_size = 128;
        queued(&mut owner, 0);
        let d = owner.pubsub.deliveries.row_mut(0);
        d.expiry = timed.then(|| Box::new(message_expiry()));
        d.payload = Some(payload.clone());
        let (effect, command) =
            message_command(apply(&mut owner, Input::WorkAvailable { session: TARGET }));
        let effects = known(&mut owner, effect, &command);
        assert!(
            !effects
                .iter()
                .any(|(_, e)| matches!(e, Effect::CloseConnection { .. })),
            "qos1_packet_limit_closed_the_connection"
        );
        assert_eq!(
            effects
                .iter()
                .any(|(_, e)| matches!(e, Effect::EmitPublish { .. })),
            !timed,
            "qos1_expiry_property_not_charged"
        );
        assert_eq!(owner.pubsub.free_packet_id(TARGET, 1), None);
        if !timed {
            continue;
        }
        let (effect, discard) =
            message_command(apply(&mut owner, Input::WorkAvailable { session: TARGET }));
        assert!(
            matches!(discard.recipe.get(), Recipe::FinishDelivery(f) if f.claim == command.recipe.operation()),
            "qos1_expiry_property_not_charged"
        );
        assert_eq!(owner.pubsub.free_packet_id(TARGET, 1), None);
        let _ = known(&mut owner, effect, &discard);
        assert!(owner.pubsub.deliveries.is_empty());
        assert_eq!(owner.pubsub.free_packet_id(TARGET, 1).unwrap().get(), 1);
    }
}

#[test]
fn message_expiry_sweep_retires_exact_unclaimed_metadata_and_unknown_claim_receipt_once() {
    let mut owner = world();
    message_clock(&mut owner, 10);
    queued(&mut owner, 0);
    let (claim_effect, claim) =
        message_command(apply(&mut owner, Input::WorkAvailable { session: TARGET }));
    let before = owner.leases.account().used();
    let expired = crate::contract::message_expiry::ExpiredDelivery {
        delivery: delivery(0),
        target: TARGET,
    };
    apply(
        &mut owner,
        Input::MessagesExpired {
            deliveries: BoundedVec::from_slice(&[expired], 1).unwrap(),
        },
    );
    assert!(owner.pubsub.deliveries.row(0).retired);
    let operation = claim.recipe.operation();
    assert!(
        apply(
            &mut owner,
            Input::StoreResult {
                effect: claim_effect,
                operation,
                reply: StoreReply::Outcome(StoreOutcome::RejectedCertain(
                    RejectReason::PreconditionFailed
                ))
            }
        )
        .is_empty()
    );
    assert!(owner.pubsub.deliveries.is_empty());
    for _ in 0..2 {
        apply(
            &mut owner,
            Input::MessagesExpired {
                deliveries: BoundedVec::from_slice(&[expired], 1).unwrap(),
            },
        );
    }
    assert_eq!(
        owner.leases.account().used(),
        before,
        "sweep_fabricated_credit_release"
    );
    queued(&mut owner, 1);
    let (effect, command) =
        message_command(apply(&mut owner, Input::WorkAvailable { session: TARGET }));
    known(&mut owner, effect, &command);
    let newer = owner.pubsub.deliveries.row(0).clone();
    apply(
        &mut owner,
        Input::MessagesExpired {
            deliveries: BoundedVec::from_slice(&[expired], 1).unwrap(),
        },
    );
    assert_eq!(owner.pubsub.deliveries.row(0), &newer);
}

#[test]
fn message_expiry_invalid_volatile_deadline_is_refused_before_admission() {
    let mut owner = world();
    message_clock(&mut owner, 10);
    owner
        .push_route(Route {
            topic: bytes(b"topic"),
            target: TARGET,
            target_revision: Revision::new(1),
            max_qos: Qos::AtMostOnce,
            options: crate::contract::pubsub::SubscriptionOptions::DEFAULT,
        })
        .unwrap();
    let input = Input::Packet {
        connection: CONNECTION,
        ingress_order: 1,
        packet: InboundPacket::Publish {
            topic: bytes(b"topic"),
            qos: Qos::AtMostOnce,
            retain: false,
            dup: false,
            packet_id: None,
            properties: None,
            payload: bytes(b"payload"),
            expiry: Some(MessageExpiry {
                observed: MessageTime {
                    domain: [9; 32],
                    tick: u64::MAX,
                },
                interval: 1,
            }),
        },
    };
    assert!(
        matches!(
            owner.step_checked(input, TURN),
            StepOutcome::Fatal(CoreFault::ImpossibleInput)
        ),
        "invalid_qos0_expiry_was_admitted"
    );
    assert!(owner.pubsub.volatile.is_empty());
}

fn message_register_receipts(owner: &mut Owner) {
    let effects = apply(owner, Input::MaintainReceipts);
    let (effect, collection) = effects
        .iter()
        .find_map(|(id, e)| match e {
            Effect::CollectReceipts { collection } => Some((*id, *collection)),
            _ => None,
        })
        .unwrap();
    apply(
        owner,
        Input::ReceiptsCollected {
            effect,
            collection,
            reply: crate::contract::receipts::ReceiptCollectionReply::Applied {
                closure: collection.closure,
                removed: 0,
                retained: 0,
                more: false,
            },
        },
    );
}

#[test]
fn message_expiry_full_receipt_window_consumes_started_before_preparing_early_ack_finish() {
    let mut owner = world();
    message_register_receipts(&mut owner);
    message_clock(&mut owner, 10);
    let (frame, key, pi) = message_frame(&mut owner, 0);
    message_puback(&mut owner, key, pi);
    let (start_effect, start) = message_command(message_progress(
        &mut owner,
        frame,
        key,
        FrameProgress::Started,
    ));
    owner.receipts.test_fill_consumed_window();
    owner.counters.operation = Some(OperationSeq::new(
        crate::contract::receipts::RECEIPT_WINDOW as u64 + 1,
    ));
    let effects = known(&mut owner, start_effect, &start);
    assert!(
        effects.is_empty(),
        "full_window_submitted_unreserved_finish"
    );
    assert_eq!(
        owner.receipt_status().unwrap().2,
        0,
        "full_window_did_not_consume_started"
    );
    let effects = apply(&mut owner, Input::MaintainReceipts);
    let (effect, collection) = effects
        .iter()
        .find_map(|(id, e)| match e {
            Effect::CollectReceipts { collection } => Some((*id, *collection)),
            _ => None,
        })
        .expect("collection makes control room");
    apply(
        &mut owner,
        Input::ReceiptsCollected {
            effect,
            collection,
            reply: crate::contract::receipts::ReceiptCollectionReply::Applied {
                closure: collection.closure,
                removed: collection.limit,
                retained: 0,
                more: false,
            },
        },
    );
    let effects = apply(&mut owner, Input::MaintainReceipts);
    let continuation = effects
        .iter()
        .find_map(|(_, e)| match e {
            Effect::Continue { continuation } => Some(*continuation),
            _ => None,
        })
        .expect("one bounded finish wake");
    let (finish_effect, finish) = message_command(apply(
        &mut owner,
        Input::Continue {
            continuation,
            budget: WorkUnits::new(100_000),
        },
    ));
    assert!(matches!(finish.recipe.get(), Recipe::FinishDelivery(_)));
    known(&mut owner, finish_effect, &finish);
    assert!(owner.pubsub.deliveries.is_empty());
}

#[test]
fn message_expiry_lost_sweep_reply_cannot_pin_cached_expired_unclaimed_work_or_reload_it() {
    let mut owner = world();
    message_clock(&mut owner, 10);
    queued(&mut owner, 0);
    owner.pubsub.deliveries.row_mut(0).expiry = Some(Box::new(message_expiry()));
    let before = owner.leases.account().used();
    // The store's sweep may already have committed, with its removed-key
    // notification lost. Evict only the obsolete cache, not a claim.
    message_clock(&mut owner, 12);
    assert!(
        owner.pubsub.deliveries.is_empty(),
        "lost_sweep_reply_pinned_cached_payload"
    );
    let effects = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    let read = effects
        .iter()
        .find_map(|(id, e)| matches!(e, Effect::LoadWork { .. }).then_some(*id))
        .unwrap();
    // A frozen page produced before the sweep also carries its original
    // expiry. It cannot reacquire cache/payload capacity after the TTL.
    let stale = WorkItem {
        cause: DeliveryCause::LiveExact,
        delivery: delivery(0),
        target: TARGET,
        target_revision: Revision::new(1),
        qos: Qos::AtLeastOnce,
        retain: false,
        expiry: Some(message_expiry()),
        properties: None,
        topic: bytes(b"topic"),
        payload: bytes(b"payload"),
        claim: None,
    };
    apply(
        &mut owner,
        Input::WorkPage {
            effect: read,
            session: TARGET,
            items: BoundedVec::from_slice(&[stale], 1).unwrap(),
            next: None,
        },
    );
    assert!(
        owner.pubsub.deliveries.is_empty(),
        "stale_page_reloaded_expired_cache"
    );
    assert_eq!(owner.leases.account().used(), before);
}

#[test]
fn message_expiry_lost_sweep_race_certain_claim_refusal_retires_cache_without_closing_connection() {
    for qos in [Qos::AtMostOnce, Qos::AtLeastOnce] {
        let mut owner = world();
        message_clock(&mut owner, 10);
        queued(&mut owner, 0);
        let d = owner.pubsub.deliveries.row_mut(0);
        d.expiry = Some(Box::new(message_expiry()));
        d.qos = qos;
        let (effect, command) =
            message_command(apply(&mut owner, Input::WorkAvailable { session: TARGET }));
        message_clock(&mut owner, 12);
        assert_eq!(
            owner.pubsub.deliveries.len(),
            1,
            "clock_discarded_unknown_command"
        );
        let operation = command.recipe.operation();
        let effects = apply(
            &mut owner,
            Input::StoreResult {
                effect,
                operation,
                reply: StoreReply::Outcome(StoreOutcome::RejectedCertain(
                    RejectReason::PreconditionFailed,
                )),
            },
        );
        assert!(effects.is_empty(), "ordinary_expiry_race_closed_connection");
        assert!(
            owner.pubsub.deliveries.is_empty(),
            "rejected_expired_claim_pinned_cache"
        );
        assert_eq!(owner.connection_of(TARGET), Some(CONNECTION));
        assert!(
            apply(
                &mut owner,
                Input::StoreResult {
                    effect,
                    operation,
                    reply: StoreReply::Outcome(StoreOutcome::RejectedCertain(
                        RejectReason::PreconditionFailed
                    ))
                }
            )
            .is_empty()
        );
    }
}

#[test]
fn message_expiry_clock_authority_is_zero_effect_during_recovery_and_regression_is_explicit() {
    let mut owner = world();
    owner.recovery = Some(RecoveryOrder::new());
    let result = owner.step_checked(
        Input::MessageClock {
            now: message_time(10),
        },
        TurnGrant {
            work_limit: WorkUnits::ZERO,
        },
    );
    assert_eq!(
        result,
        StepOutcome::Applied {
            work_used: WorkUnits::ZERO,
            progress: Progress::Complete
        }
    );
    assert!(owner.is_recovering());
    assert!(drain(&mut owner).is_empty());
    let previous = owner.message_now;
    assert_eq!(
        owner.step_checked(
            Input::MessageClock {
                now: message_time(9)
            },
            TurnGrant {
                work_limit: WorkUnits::ZERO
            }
        ),
        StepOutcome::Fatal(CoreFault::ImpossibleInput)
    );
    assert_eq!(owner.message_now, previous);
}

#[test]
fn message_expiry_clocked_subscription_reserves_all_replay_work_before_drawing_identities() {
    let mut owner = world();
    message_clock(&mut owner, 10);
    let counters = owner.counters;
    let result = owner.step_checked(
        product_subscribe(),
        TurnGrant {
            work_limit: WorkUnits::new(200),
        },
    );
    assert!(
        matches!(
            result,
            StepOutcome::NeedCapacity {
                need: CapacityNeed {
                    resource: Resource::Work,
                    ..
                },
                work_used: WorkUnits::ZERO,
                ..
            }
        ),
        "clocked_subscription_exceeded_work_grant"
    );
    assert_eq!(owner.counters, counters);
    assert!(owner.pubsub.control.is_none());
    assert!(drain(&mut owner).is_empty());
}

#[test]
fn message_expiry_partial_positive_progress_never_proves_a_claim_unsent() {
    let mut owner = world();
    message_clock(&mut owner, 10);
    let (frame, key, _) = message_frame(&mut owner, 0);
    assert!(
        message_progress(
            &mut owner,
            frame,
            key,
            FrameProgress::Written {
                confirmed: 1,
                total: 18
            }
        )
        .is_empty()
    );
    assert!(!owner.pubsub.deliveries.row(0).local_write_completed);
    assert!(
        !owner.pubsub.deliveries.row(0).claim.unwrap().started,
        "partial_callback_invented_started_marker"
    );
    message_clock(&mut owner, 12);
    assert!(
        message_progress(
            &mut owner,
            frame,
            key,
            FrameProgress::NotStarted {
                reason: MessageExpiryBlock::Expired
            }
        )
        .is_empty(),
        "partial_positive_write_treated_as_unsent"
    );
    assert_eq!(owner.pubsub.deliveries.len(), 1);
}

#[test]
fn message_expiry_puback_while_started_marker_is_deferred_keeps_its_exact_target() {
    let mut owner = world();
    message_clock(&mut owner, 10);
    let (frame, key, pi) = message_frame(&mut owner, 0);
    let target = owner.puback_target(key, pi).unwrap();
    let (effect, command) = message_command(message_progress(
        &mut owner,
        frame,
        key,
        FrameProgress::Started,
    ));
    apply(
        &mut owner,
        Input::StoreResult {
            effect,
            operation: command.recipe.operation(),
            reply: StoreReply::Outcome(StoreOutcome::RejectedCertain(RejectReason::Fenced)),
        },
    );
    assert_eq!(
        owner.puback_target(key, pi),
        Some(target),
        "fenced_marker_lost_puback_association"
    );
    assert!(message_puback(&mut owner, key, pi).is_empty());
    let (effect, retry) =
        message_command(apply(&mut owner, Input::WorkAvailable { session: TARGET }));
    assert!(
        matches!(retry.recipe.get(), Recipe::MarkDeliveryStarted(mark) if mark.claim == target.claim)
    );
    let (effect, finish) = message_command(known(&mut owner, effect, &retry));
    known(&mut owner, effect, &finish);
    assert!(owner.pubsub.deliveries.is_empty());
}

#[test]
fn message_expiry_regressive_loaded_origin_refuses_the_page_before_mutation() {
    let mut owner = world();
    message_clock(&mut owner, 10);
    let effects = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    let effect = effects
        .iter()
        .find_map(|(id, e)| matches!(e, Effect::LoadWork { .. }).then_some(*id))
        .unwrap();
    let item = WorkItem {
        cause: DeliveryCause::LiveExact,
        delivery: delivery(0),
        target: TARGET,
        target_revision: Revision::new(1),
        qos: Qos::AtLeastOnce,
        retain: false,
        expiry: Some(MessageExpiry {
            observed: message_time(11),
            interval: 2,
        }),
        properties: None,
        topic: bytes(b"topic"),
        payload: bytes(b"payload"),
        claim: None,
    };
    let result = owner.step_checked(
        Input::WorkPage {
            effect,
            session: TARGET,
            items: BoundedVec::from_slice(&[item], 1).unwrap(),
            next: None,
        },
        TURN,
    );
    assert!(
        matches!(result, StepOutcome::Fatal(CoreFault::ImpossibleInput)),
        "regressive_message_time_silently_quarantined"
    );
    assert!(owner.pubsub.deliveries.is_empty());
}

#[test]
fn message_expiry_qos12_admission_freezes_origin_and_handshakes_outlive_ttl() {
    for qos in [Qos::AtLeastOnce, Qos::ExactlyOnce] {
        for retain in [false, true] {
            let mut owner = world();
            owner.lifecycle_enabled = true;
            message_clock(&mut owner, 10);
            let packet = InboundPacket::Publish {
                topic: bytes(b"topic"),
                qos,
                retain,
                dup: false,
                packet_id: Some(InboundPacketId::new(7).unwrap()),
                properties: None,
                payload: bytes(b"payload"),
                expiry: Some(message_expiry()),
            };
            let (effect, command) = message_command(apply(
                &mut owner,
                Input::Packet {
                    connection: CONNECTION,
                    ingress_order: 1,
                    packet,
                },
            ));
            let Recipe::ExpiringPublication(expiring) = command.recipe.get() else {
                panic!("expiry_not_in_canonical_admission");
            };
            assert_eq!(expiring.expiry, message_expiry());
            let crate::contract::command::ExpiringAdmission::Qos12(retained) = &expiring.admission
            else {
                panic!("qos12_admission_kind");
            };
            assert_eq!(retained.admission.retain, retain);
            assert_eq!(retained.admission.qos, qos);
            let publication = retained.admission.publication;
            let exchange = retained.admission.source.exchange;
            let mut canonical = Vec::new();
            command
                .recipe
                .get()
                .encode_canonical(&mut canonical)
                .unwrap();
            let operation = command.recipe.operation();
            apply(
                &mut owner,
                Input::StoreResult {
                    effect,
                    operation,
                    reply: StoreReply::Outcome(StoreOutcome::PendingOrUnknown),
                },
            );
            message_clock(&mut owner, 20);
            // A duplicate reception cannot replace the operation's original
            // canonical timestamp even if the client supplies a new TTL.
            assert!(
                apply(
                    &mut owner,
                    Input::Packet {
                        connection: CONNECTION,
                        ingress_order: 2,
                        packet: InboundPacket::Publish {
                            topic: bytes(b"topic"),
                            qos,
                            retain,
                            dup: true,
                            packet_id: Some(InboundPacketId::new(7).unwrap()),
                            properties: None,
                            payload: bytes(b"payload"),
                            expiry: Some(MessageExpiry {
                                observed: message_time(20),
                                interval: 100
                            })
                        }
                    }
                )
                .is_empty()
            );
            let mut repeated = Vec::new();
            command
                .recipe
                .get()
                .encode_canonical(&mut repeated)
                .unwrap();
            assert_eq!(repeated, canonical);
            let ack = apply(
                &mut owner,
                Input::StoreResult {
                    effect,
                    operation,
                    reply: StoreReply::Outcome(StoreOutcome::Committed(
                        CommitFacts::PublicationAdmitted {
                            publication,
                            exchange,
                        },
                    )),
                },
            );
            assert!(
                ack.iter().any(|(_, e)| matches!(
                    e,
                    Effect::EmitFrame {
                        frame: OutboundFrame::Puback { .. } | OutboundFrame::Pubrec { .. },
                        ..
                    }
                )),
                "expiry_broke_inbound_acknowledgement"
            );
            if qos == Qos::ExactlyOnce {
                let (_, release) = message_command(apply(
                    &mut owner,
                    Input::Packet {
                        connection: CONNECTION,
                        ingress_order: 3,
                        packet: InboundPacket::Pubrel {
                            packet_id: InboundPacketId::new(7).unwrap(),
                        },
                    },
                ));
                assert!(
                    matches!(release.recipe.get(), Recipe::ReleaseInbound(end) if end.exchange == exchange),
                    "late_pubrel_rematerialized_publication"
                );
            }
        }
    }
}

#[test]
fn message_expiry_volatile_qos0_drops_expired_work_without_durable_identity_or_mqtt_credit() {
    let mut owner = world();
    message_clock(&mut owner, 10);
    owner
        .push_route(Route {
            topic: bytes(b"topic"),
            target: TARGET,
            target_revision: Revision::new(1),
            max_qos: Qos::AtMostOnce,
            options: crate::contract::pubsub::SubscriptionOptions::DEFAULT,
        })
        .unwrap();
    let counters = owner.counters;
    let credits = owner.leases.account().used();
    let effects = apply(
        &mut owner,
        Input::Packet {
            connection: CONNECTION,
            ingress_order: 1,
            packet: InboundPacket::Publish {
                topic: bytes(b"topic"),
                qos: Qos::AtMostOnce,
                retain: false,
                dup: false,
                packet_id: None,
                properties: None,
                payload: bytes(b"payload"),
                expiry: Some(message_expiry()),
            },
        },
    );
    assert!(
        !effects
            .iter()
            .any(|(_, e)| matches!(e, Effect::Persist { .. }))
    );
    let continuation = effects
        .iter()
        .find_map(|(_, e)| match e {
            Effect::Continue { continuation } => Some(*continuation),
            _ => None,
        })
        .unwrap();
    message_clock(&mut owner, 12);
    let effects = apply(
        &mut owner,
        Input::Continue {
            continuation,
            budget: WorkUnits::new(100_000),
        },
    );
    assert!(effects.iter().any(|(_, e)| matches!(
        e,
        Effect::Qos0Dropped {
            reason: Qos0DropReason::MessageExpired,
            ..
        }
    )));
    assert!(owner.pubsub.volatile.is_empty());
    assert_eq!(owner.counters.operation, counters.operation);
    assert_eq!(owner.counters.publication, counters.publication);
    assert_eq!(owner.counters.payload, counters.payload);
    assert_eq!(owner.leases.account().used(), credits);
}

#[test]
fn message_expiry_retained_qos0_unknown_result_survives_source_retirement_and_resolves() {
    let mut owner = world();
    owner.lifecycle_enabled = true;
    message_clock(&mut owner, 10);
    let mut packet = retained_zero(1);
    let Input::Packet {
        packet: InboundPacket::Publish { expiry, .. },
        ..
    } = &mut packet
    else {
        panic!("retained packet");
    };
    *expiry = Some(message_expiry());
    let (effect, command) = message_command(apply(&mut owner, packet));
    let Recipe::ExpiringPublication(expiring) = command.recipe.get() else {
        panic!("retained_qos0_expiry_not_canonical");
    };
    let crate::contract::command::ExpiringAdmission::Qos0(retained) = &expiring.admission else {
        panic!("retained_qos0_invented_exchange");
    };
    let publication = retained.publication;
    assert_eq!(retained.costs.exchange_slots, ExchangeSlots::ZERO);
    let operation = command.recipe.operation();
    apply(
        &mut owner,
        Input::StoreResult {
            effect,
            operation,
            reply: StoreReply::Outcome(StoreOutcome::PendingOrUnknown),
        },
    );
    owner.pubsub.retire_session(TARGET);
    apply(
        &mut owner,
        Input::ConnectionClosed {
            connection: CONNECTION,
        },
    );
    message_clock(&mut owner, 12);
    let flight = owner.pubsub.retained.clone().unwrap();
    assert!(flight.retired);
    let effects = apply(
        &mut owner,
        Input::Deadline {
            token: flight.in_flight.deadline,
            now: crate::contract::ids::LogicalTime::new(20),
        },
    );
    let resolve = effects
        .iter()
        .find_map(|(id, e)| {
            matches!(e, Effect::ResolveAndFence { operation: op } if *op == operation)
                .then_some(*id)
        })
        .unwrap();
    assert!(
        apply(
            &mut owner,
            Input::StoreResult {
                effect: resolve,
                operation,
                reply: StoreReply::Outcome(StoreOutcome::Committed(
                    CommitFacts::RetainedQos0Stored { publication }
                ))
            }
        )
        .is_empty()
    );
    assert!(owner.pubsub.retained.is_none());
    assert!(owner.pubsub.deliveries.is_empty());
    assert_eq!(owner.leases.account().used().slots, ExchangeSlots::ZERO);
}

#[test]
fn message_expiry_start_and_exact_expiry_reserve_effects_before_mutating_claim_or_counters() {
    for start in [false, true] {
        let mut owner = world();
        message_clock(&mut owner, 10);
        let (effect, connection, _) = message_frame(&mut owner, 0);
        if !start {
            message_clock(&mut owner, 12);
        }
        let progress = if start {
            FrameProgress::Started
        } else {
            FrameProgress::NotStarted {
                reason: MessageExpiryBlock::Expired,
            }
        };
        let counters = owner.counters;
        let delivery = owner.pubsub.deliveries.row(0).clone();
        let held = owner
            .outbox
            .reserve(NonZeroUsize::new(owner.outbox.free_slots() - 1).unwrap())
            .unwrap();
        let result = owner.step_checked(
            Input::FrameResult {
                effect,
                connection,
                progress,
            },
            TURN,
        );
        assert!(
            matches!(
                result,
                StepOutcome::NeedCapacity {
                    need: CapacityNeed {
                        resource: Resource::EffectSlots,
                        ..
                    },
                    ..
                }
            ),
            "expiry_transition_did_not_reserve_whole_operation"
        );
        assert_eq!(owner.counters, counters);
        assert_eq!(owner.pubsub.deliveries.row(0), &delivery);
        owner.outbox.release(held).unwrap();
        let (_, command) =
            message_command(message_progress(&mut owner, effect, connection, progress));
        assert_eq!(
            command.recipe.operation().sequence(),
            counters.operation.unwrap()
        );
    }
}

#[test]
fn message_expiry_recovery_inventory_keeps_unmarked_claims_quarantined_and_replays_started() {
    for started in [false, true] {
        for unknown in [false, true] {
            let mut owner = world();
            let mut now = message_time(12);
            if unknown {
                now.domain = [8; 32];
            }
            apply(&mut owner, Input::MessageClock { now });
            owner.recovery = Some(RecoveryOrder::new());
            let claim = DeliveryClaim {
                operation: OperationId::new(
                    StoreIncarnation::new(1),
                    RunEpoch::new(1),
                    LogicalOwnerId::new(4),
                    OperationSeq::new(100),
                ),
                packet_id: OutboundPacketId::new(1).unwrap(),
                started,
                await_pubcomp: false,
            };
            let fact = RecoveredFact::OutboundDelivery {
                delivery: delivery(0),
                target: TARGET,
                target_revision: Revision::new(1),
                qos: Qos::AtLeastOnce,
                claim: Some(claim),
                expiry: Some(message_expiry()),
                started,
            };
            apply(
                &mut owner,
                Input::Recovery {
                    facts: BoundedVec::from_slice(&[fact], 1).unwrap(),
                    last: true,
                },
            );
            let effects = apply(&mut owner, Input::WorkAvailable { session: TARGET });
            let read = effects
                .iter()
                .find_map(|(id, e)| matches!(e, Effect::LoadWork { .. }).then_some(*id))
                .unwrap();
            let item = WorkItem {
                cause: DeliveryCause::LiveExact,
                delivery: delivery(0),
                target: TARGET,
                target_revision: Revision::new(1),
                qos: Qos::AtLeastOnce,
                retain: true,
                expiry: Some(message_expiry()),
                properties: None,
                topic: bytes(b"topic"),
                payload: bytes(b"payload"),
                claim: Some(claim),
            };
            apply(
                &mut owner,
                Input::WorkPage {
                    effect: read,
                    session: TARGET,
                    items: BoundedVec::from_slice(&[item], 1).unwrap(),
                    next: None,
                },
            );
            let effects = apply(&mut owner, Input::WorkAvailable { session: TARGET });
            assert_eq!(effects.iter().any(|(_, e)| matches!(e, Effect::EmitPublish { started: true, packet_id, expiry: Some(expiry), .. } if *packet_id == claim.packet_id && **expiry == message_expiry())), started, "recovery_invented_unsent_or_started_authority");
            assert!(
                !effects
                    .iter()
                    .any(|(_, e)| matches!(e, Effect::Persist { .. }))
            );
            assert_eq!(owner.pubsub.free_packet_id(TARGET, 1), None);
            assert!(!owner.pubsub.deliveries.row(0).certain_never_started);
        }
    }
}

#[test]
fn message_expiry_receipt_frontier_tracks_tag13_payload_and_tag14_complete_replay_range() {
    let mut owner = world();
    owner.lifecycle_enabled = true;
    message_register_receipts(&mut owner);
    message_clock(&mut owner, 10);
    let mut packet = retained_zero(1);
    let Input::Packet {
        packet: InboundPacket::Publish { expiry, .. },
        ..
    } = &mut packet
    else {
        panic!("retained packet");
    };
    *expiry = Some(message_expiry());
    let (effect, command) = message_command(apply(&mut owner, packet));
    let retained = command.recipe.get().retained_qos0().unwrap();
    let publication = retained.publication;
    let payload = retained.payload.id.sequence();
    apply(
        &mut owner,
        Input::StoreResult {
            effect,
            operation: command.recipe.operation(),
            reply: StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::RetainedQos0Stored {
                publication,
            })),
        },
    );
    let (effect, replay) = message_command(apply(&mut owner, product_subscribe()));
    let Recipe::TimedReplaySubscriptions(timed) = replay.recipe.get() else {
        panic!("timed replay");
    };
    let last = timed.replay.first_publication.sequence().get()
        + crate::contract::retained::MAX_REPLAYS as u64
        - 1;
    apply(
        &mut owner,
        Input::StoreResult {
            effect,
            operation: replay.recipe.operation(),
            reply: StoreReply::Outcome(StoreOutcome::Committed(
                CommitFacts::SubscriptionsChanged {
                    session: TARGET,
                    route_revision: Revision::new(1),
                },
            )),
        },
    );
    owner.receipts.test_request_sweep();
    let effects = apply(&mut owner, Input::MaintainReceipts);
    let closure = effects
        .iter()
        .find_map(|(_, e)| match e {
            Effect::CollectReceipts { collection } => Some(collection.closure),
            _ => None,
        })
        .unwrap();
    assert_eq!(closure.through, replay.recipe.operation().sequence());
    assert_eq!(
        closure.publication_through.get(),
        last,
        "tag14_receipt_lost_unused_replay_suffix"
    );
    assert_eq!(
        closure.payload_through, payload,
        "tag13_receipt_lost_payload_identity"
    );
}

#[test]
fn message_expiry_target_retirement_consumes_started_and_expired_results_without_receipt_holes() {
    for start in [false, true] {
        let mut owner = world();
        message_register_receipts(&mut owner);
        message_clock(&mut owner, 10);
        let (frame, key, _) = message_frame(&mut owner, 0);
        if !start {
            message_clock(&mut owner, 12);
        }
        let progress = if start {
            FrameProgress::Started
        } else {
            FrameProgress::NotStarted {
                reason: MessageExpiryBlock::Expired,
            }
        };
        let (effect, command) = message_command(message_progress(&mut owner, frame, key, progress));
        owner.pubsub.retire_session(TARGET);
        apply(
            &mut owner,
            Input::ConnectionClosed {
                connection: CONNECTION,
            },
        );
        assert_eq!(owner.receipt_status().unwrap().2, 1);
        assert!(known(&mut owner, effect, &command).is_empty());
        assert!(owner.pubsub.deliveries.is_empty());
        assert_eq!(
            owner.receipt_status().unwrap().2,
            0,
            "retired_expiry_control_left_receipt_hole"
        );
        assert!(known(&mut owner, effect, &command).is_empty());
    }
}

// Maximum Packet Size of the receiving client (MQTT-3.1.2-25, ADR 0025
// as amended): a QoS 1 or 2 delivery whose PUBLISH would exceed it is
// not sent and ends through the finish guarded by its claim.

const OTHER: SessionId = SessionId::new(2);

/// The name of each effect these tests expect, in order.
fn names(effects: &[(EffectId, Effect)]) -> Vec<&'static str> {
    effects
        .iter()
        .map(|(_, effect)| match effect {
            Effect::Persist { .. } => "Persist",
            Effect::ArmDeadline { .. } => "ArmDeadline",
            Effect::EmitPublish { .. } => "EmitPublish",
            Effect::CloseConnection { .. } => "CloseConnection",
            Effect::LoadWork { .. } => "LoadWork",
            Effect::DeliveryDiscarded { .. } => "DeliveryDiscarded",
            _ => "another effect",
        })
        .collect()
}

/// The one observation a committed discard publishes: the delivery, its
/// target and the `FinishDelivery` that ended it.
fn discarded(effects: &[(EffectId, Effect)]) -> (DeliveryId, SessionId, OperationId) {
    let [
        (
            _,
            Effect::DeliveryDiscarded {
                delivery,
                target,
                operation,
                reason: DeliveryDiscardReason::PacketTooLarge,
            },
        ),
    ] = effects
    else {
        panic!("one discard observation: {effects:?}")
    };
    (*delivery, *target, *operation)
}

/// The world, its connection limited to `maximum` bytes per packet and
/// `receive_maximum` unacknowledged PUBLISH packets.
fn limited(maximum: u32, receive_maximum: u16) -> Owner {
    let mut owner = world();
    let settings = &mut owner.pubsub.connections.as_mut_slice()[0];
    settings.maximum_packet_size = maximum;
    settings.receive_maximum = receive_maximum;
    owner
}

/// A loaded, unclaimed delivery of topic `topic`: 12 bytes of PUBLISH
/// besides its payload, 17 with a Message Expiry Interval.
fn sized(owner: &mut Owner, index: u32, qos: Qos, timed: bool, payload: usize) {
    queued(owner, index);
    let d = owner.pubsub.deliveries.last_mut().unwrap();
    d.qos = qos;
    d.expiry = timed.then(|| Box::new(message_expiry()));
    d.payload = Some(SharedBytes::copy_from(&alloc::vec![7; payload], 128).unwrap());
}

/// Submits the claim the next work turn makes and applies its commit.
fn claim_next(owner: &mut Owner, session: SessionId) -> (Command, Vec<(EffectId, Effect)>) {
    let (effect, command) = message_command(apply(owner, Input::WorkAvailable { session }));
    assert!(matches!(command.recipe.get(), Recipe::ClaimDelivery(_)));
    let effects = known(owner, effect, &command);
    (command, effects)
}

/// The work turn that follows the claim of an oversized delivery: the
/// finish guarded by that claim and its deadline, nothing else.
fn discard_next(
    owner: &mut Owner,
    session: SessionId,
    claim: &Command,
) -> (EffectId, Command, DeadlineToken) {
    assert!(owner.has_ready_work(session), "the discard is ready work");
    let effects = apply(owner, Input::WorkAvailable { session });
    assert_eq!(names(&effects), ["Persist", "ArmDeadline"]);
    let Effect::ArmDeadline { token, .. } = effects[1].1 else {
        panic!("deadline of the discard")
    };
    let (effect, command) = message_command(effects);
    let Recipe::ClaimDelivery(claimed) = claim.recipe.get() else {
        panic!("claim")
    };
    assert_eq!(
        command.recipe.get(),
        &Recipe::FinishDelivery(FinishDelivery {
            operation: command.recipe.operation(),
            delivery: claimed.delivery,
            target: claimed.target,
            target_revision: claimed.target_revision,
            packet_id: claimed.packet_id,
            claim: claimed.operation,
        }),
        "the discard is the finish guarded by the claim"
    );
    assert_ne!(command.recipe.operation(), claimed.operation);
    let held = owner
        .pubsub
        .deliveries
        .values()
        .find(|d| d.id == claimed.delivery)
        .expect("the delivery is kept until its finish is known");
    assert!(
        matches!(&held.phase, Phase::DiscardPending(flight) if flight.operation == command.recipe.operation()),
        "{:?}",
        held.phase
    );
    assert!(held.frame.is_none() && !held.retired);
    assert_eq!(
        held.claim.map(|claim| claim.operation),
        Some(claimed.operation)
    );
    (effect, command, token)
}

#[test]
fn a_delivery_is_sent_at_its_exact_packet_size_and_discarded_one_byte_below() {
    for qos in [Qos::AtLeastOnce, Qos::ExactlyOnce] {
        for timed in [false, true] {
            let fixed = 12 + usize::from(timed) * 5;
            // `payload`, then the two sides of the Remaining Length step:
            // a body of 127 bytes has a 2-byte header, one of 128 has 3.
            for (payload, exact) in [
                (7, fixed + 7),
                (127 - (fixed - 2), 129),
                (128 - (fixed - 2), 131),
            ] {
                let case = (qos, timed, payload);
                let mut owner = limited(exact as u32, 8);
                message_clock(&mut owner, 10);
                sized(&mut owner, 0, qos, timed, payload);
                let (_, sent) = claim_next(&mut owner, TARGET);
                assert_eq!(names(&sent), ["EmitPublish"], "{case:?}");

                let mut owner = limited(exact as u32 - 1, 8);
                message_clock(&mut owner, 10);
                sized(&mut owner, 0, qos, timed, payload);
                let (claim, sent) = claim_next(&mut owner, TARGET);
                assert!(
                    sent.is_empty(),
                    "{case:?}: no PUBLISH, no acknowledgement frame, no close: {sent:?}"
                );
                let (effect, discard, _) = discard_next(&mut owner, TARGET, &claim);
                assert!(
                    apply(&mut owner, Input::WorkAvailable { session: TARGET })
                        .iter()
                        .all(|(_, e)| matches!(e, Effect::LoadWork { .. })),
                    "{case:?}: a pending discard is submitted once"
                );
                let observed = known(&mut owner, effect, &discard);
                assert_eq!(names(&observed), ["DeliveryDiscarded"], "{case:?}");
                assert_eq!(
                    discarded(&observed),
                    (delivery(0), TARGET, discard.recipe.operation()),
                    "{case:?}"
                );
                assert!(owner.pubsub.deliveries.is_empty(), "{case:?}");
                assert_eq!(owner.pubsub.active(CONNECTION), 0, "{case:?}");
                assert_eq!(owner.pubsub.free_packet_id(TARGET, 1).unwrap().get(), 1);
                assert!(
                    known(&mut owner, effect, &discard).is_empty(),
                    "{case:?}: a repeated result publishes no second observation"
                );
            }
        }
    }
}

#[test]
fn two_destinations_of_one_publication_are_decided_by_their_own_packet_limits() {
    let mut owner = world();
    let other = ConnectionGeneration::new(11);
    let _ = apply(
        &mut owner,
        Input::Packet {
            connection: other,
            ingress_order: 0,
            packet: InboundPacket::ConnectResume {
                client_id: bytes(b"other"),
            },
        },
    );
    assert!(
        apply(
            &mut owner,
            Input::ConnectionSettings {
                connection: other,
                receive_maximum: 8,
                maximum_packet_size: 18,
            }
        )
        .is_empty()
    );
    queued(&mut owner, 0);
    queued(&mut owner, 1);
    owner.pubsub.deliveries.row_mut(1).target = OTHER;
    let (_, sent) = claim_next(&mut owner, TARGET);
    assert!(matches!(
        &sent[..],
        [(_, Effect::EmitPublish { delivery: d, connection, .. })]
            if *d == delivery(0) && connection.generation == CONNECTION
    ));
    let (claim, sent) = claim_next(&mut owner, OTHER);
    assert!(sent.is_empty(), "{sent:?}");
    let (effect, discard, _) = discard_next(&mut owner, OTHER, &claim);
    let observed = known(&mut owner, effect, &discard);
    assert_eq!(names(&observed), ["DeliveryDiscarded"]);
    assert_eq!(
        discarded(&observed),
        (delivery(1), OTHER, discard.recipe.operation())
    );
    let left = owner.pubsub.deliveries.to_vec();
    assert_eq!(left.len(), 1, "the other destination keeps its delivery");
    assert_eq!((left[0].id, left[0].target), (delivery(0), TARGET));
    assert!(left[0].frame.is_some() && left[0].phase == Phase::AwaitPuback);
    assert_eq!(owner.pubsub.active(CONNECTION), 1);
    assert_eq!(owner.pubsub.active(other), 0);
}

#[test]
fn a_pending_discard_holds_its_window_and_identifier_until_its_finish_is_known() {
    for receive_maximum in [1, 2] {
        let mut owner = limited(18, receive_maximum);
        sized(&mut owner, 0, Qos::AtLeastOnce, false, 7);
        sized(&mut owner, 1, Qos::AtLeastOnce, false, 1);
        let (claim, sent) = claim_next(&mut owner, TARGET);
        assert!(sent.is_empty(), "{sent:?}");
        let (effect, discard, _) = discard_next(&mut owner, TARGET, &claim);
        assert_eq!(owner.pubsub.active(CONNECTION), 1);
        assert_eq!(owner.pubsub.free_packet_id(TARGET, 4).unwrap().get(), 2);
        if receive_maximum == 1 {
            assert!(
                apply(&mut owner, Input::WorkAvailable { session: TARGET }).is_empty(),
                "the full window admits no claim while the discard is pending"
            );
        } else {
            let (next, sent) = claim_next(&mut owner, TARGET);
            assert!(
                matches!(next.recipe.get(), Recipe::ClaimDelivery(c) if c.delivery == delivery(1) && c.packet_id.get() == 2),
                "the free window admits the next delivery beside the pending discard"
            );
            assert!(matches!(
                &sent[..],
                [(_, Effect::EmitPublish { delivery: d, dup: false, .. })] if *d == delivery(1)
            ));
        }
        let observed = known(&mut owner, effect, &discard);
        assert_eq!(names(&observed), ["DeliveryDiscarded"]);
        assert_eq!(
            discarded(&observed),
            (delivery(0), TARGET, discard.recipe.operation())
        );
        if receive_maximum == 1 {
            let (next, sent) = claim_next(&mut owner, TARGET);
            assert!(
                matches!(next.recipe.get(), Recipe::ClaimDelivery(c) if c.delivery == delivery(1) && c.packet_id.get() == 1),
                "the commit of the discard returns the window and the identifier"
            );
            assert!(matches!(
                &sent[..],
                [(_, Effect::EmitPublish { delivery: d, dup: false, .. })] if *d == delivery(1)
            ));
        }
        assert_eq!(owner.pubsub.deliveries.len(), 1);
        assert_eq!(owner.pubsub.active(CONNECTION), 1);
    }
}

/// A claim found in durable data, as after a crash between the commit of
/// the claim and the commit of its discard.
fn recovered_claim(owner: &mut Owner) -> DeliveryClaim {
    let claim = DeliveryClaim {
        operation: OperationId::new(
            StoreIncarnation::new(1),
            RunEpoch::new(1),
            LogicalOwnerId::new(4),
            OperationSeq::new(100),
        ),
        packet_id: OutboundPacketId::new(3).unwrap(),
        started: false,
        await_pubcomp: false,
    };
    owner
        .add_outbound_fact(
            delivery(0),
            TARGET,
            Revision::new(1),
            (Qos::AtLeastOnce, Some(claim), None, false),
        )
        .unwrap();
    let item = owner.pubsub.deliveries.row_mut(0);
    item.topic = Some(bytes(b"topic"));
    item.payload = Some(bytes(b"payload"));
    claim
}

#[test]
fn a_recovered_claim_is_discarded_again_under_its_identity_or_sent_as_a_duplicate() {
    let mut owner = limited(18, 8);
    let claim = recovered_claim(&mut owner);
    assert!(owner.has_ready_work(TARGET));
    let effects = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    assert_eq!(names(&effects), ["Persist", "ArmDeadline"]);
    let (effect, discard) = message_command(effects);
    let Recipe::FinishDelivery(finish) = discard.recipe.get() else {
        panic!("the recovered claim is discarded again: {discard:?}")
    };
    assert_eq!(
        (finish.delivery, finish.packet_id, finish.claim),
        (delivery(0), claim.packet_id, claim.operation),
        "the second discard is guarded by the same claim"
    );
    assert_ne!(finish.operation, claim.operation);
    let observed = known(&mut owner, effect, &discard);
    assert_eq!(names(&observed), ["DeliveryDiscarded"]);
    assert_eq!(
        discarded(&observed),
        (delivery(0), TARGET, discard.recipe.operation())
    );
    assert!(owner.pubsub.deliveries.is_empty());

    let mut owner = limited(19, 8);
    let claim = recovered_claim(&mut owner);
    let sent = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    assert!(
        matches!(
            &sent[..],
            [(_, Effect::EmitPublish { packet_id, claim: op, dup: true, .. })]
                if *packet_id == claim.packet_id && *op == claim.operation
        ),
        "a recovered claim that fits is sent with DUP=1: {sent:?}"
    );
}

/// The subscriber closes `CONNECTION` and resumes on a new generation
/// that accepts 65536 bytes per packet.
fn reconnect_larger(owner: &mut Owner) -> ConnectionGeneration {
    let next = ConnectionGeneration::new(11);
    let _ = apply(
        owner,
        Input::ConnectionClosed {
            connection: CONNECTION,
        },
    );
    let _ = apply(
        owner,
        Input::Packet {
            connection: next,
            ingress_order: 0,
            packet: InboundPacket::ConnectResume {
                client_id: bytes(b"subscriber"),
            },
        },
    );
    assert!(
        apply(
            owner,
            Input::ConnectionSettings {
                connection: next,
                receive_maximum: 8,
                maximum_packet_size: 65536,
            }
        )
        .is_empty()
    );
    next
}

fn sends_nothing(effects: &[(EffectId, Effect)]) -> bool {
    effects
        .iter()
        .all(|(_, e)| matches!(e, Effect::LoadWork { .. }))
}

#[test]
fn a_discard_of_unknown_outcome_is_resolved_and_never_sent_on_a_larger_connection() {
    let mut owner = limited(18, 8);
    sized(&mut owner, 0, Qos::AtLeastOnce, false, 7);
    let (claim, _) = claim_next(&mut owner, TARGET);
    let (effect, discard, deadline) = discard_next(&mut owner, TARGET, &claim);
    let operation = discard.recipe.operation();
    assert!(
        apply(
            &mut owner,
            Input::StoreResult {
                effect,
                operation,
                reply: StoreReply::Outcome(StoreOutcome::PendingOrUnknown),
            }
        )
        .is_empty()
    );
    let _ = reconnect_larger(&mut owner);
    let work = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    assert!(
        sends_nothing(&work),
        "a discard that may have committed is never sent: {work:?}"
    );
    assert_eq!(owner.pubsub.free_packet_id(TARGET, 4).unwrap().get(), 2);
    let resolution = apply(
        &mut owner,
        Input::Deadline {
            token: deadline,
            now: crate::contract::ids::LogicalTime::new(10),
        },
    );
    assert!(
        matches!(resolution[0].1, Effect::ResolveAndFence { operation: resolved } if resolved == operation)
    );
    let observed = known(&mut owner, resolution[0].0, &discard);
    assert_eq!(names(&observed), ["DeliveryDiscarded"]);
    assert_eq!(
        discarded(&observed),
        (delivery(0), TARGET, discard.recipe.operation())
    );
    assert!(owner.pubsub.deliveries.is_empty());
    let work = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    assert!(sends_nothing(&work), "{work:?}");
}

#[test]
fn a_fenced_discard_closes_nothing_and_is_decided_again() {
    for larger in [false, true] {
        let mut owner = limited(18, 8);
        sized(&mut owner, 0, Qos::AtLeastOnce, false, 7);
        let (claim, _) = claim_next(&mut owner, TARGET);
        let (_, discard, deadline) = discard_next(&mut owner, TARGET, &claim);
        let resolution = apply(
            &mut owner,
            Input::Deadline {
                token: deadline,
                now: crate::contract::ids::LogicalTime::new(10),
            },
        );
        assert!(matches!(resolution[0].1, Effect::ResolveAndFence { .. }));
        let fenced = apply(
            &mut owner,
            Input::StoreResult {
                effect: resolution[0].0,
                operation: discard.recipe.operation(),
                reply: StoreReply::Outcome(StoreOutcome::RejectedCertain(RejectReason::Fenced)),
            },
        );
        assert!(
            fenced.is_empty(),
            "a discard certainly not applied closes nothing: {fenced:?}"
        );
        assert_eq!(owner.pubsub.deliveries.len(), 1, "the claim stands");
        assert_eq!(owner.pubsub.free_packet_id(TARGET, 4).unwrap().get(), 2);
        if larger {
            let _ = reconnect_larger(&mut owner);
            let sent = apply(&mut owner, Input::WorkAvailable { session: TARGET });
            assert!(
                matches!(&sent[..], [(_, Effect::EmitPublish { delivery: d, .. })] if *d == delivery(0)),
                "decided again against the connection of that moment: {sent:?}"
            );
        } else {
            let (effect, again, _) = discard_next(&mut owner, TARGET, &claim);
            assert_ne!(again.recipe.operation(), discard.recipe.operation());
            let observed = known(&mut owner, effect, &again);
            assert_eq!(names(&observed), ["DeliveryDiscarded"]);
            assert_eq!(
                discarded(&observed),
                (delivery(0), TARGET, again.recipe.operation())
            );
            assert!(owner.pubsub.deliveries.is_empty());
        }
    }
}

#[test]
fn a_discard_refused_for_another_reason_keeps_the_rule_of_a_refused_finish() {
    let mut owner = limited(18, 8);
    sized(&mut owner, 0, Qos::AtLeastOnce, false, 7);
    let (claim, _) = claim_next(&mut owner, TARGET);
    let (effect, discard, _) = discard_next(&mut owner, TARGET, &claim);
    let refused = apply(
        &mut owner,
        Input::StoreResult {
            effect,
            operation: discard.recipe.operation(),
            reply: StoreReply::Outcome(StoreOutcome::RejectedCertain(
                RejectReason::PreconditionFailed,
            )),
        },
    );
    assert!(matches!(
        &refused[..],
        [(
            _,
            Effect::CloseConnection {
                reason: CloseReason::CapacityExceeded,
                ..
            }
        )]
    ));
    assert_eq!(owner.pubsub.deliveries.len(), 1);
    assert_eq!(owner.pubsub.free_packet_id(TARGET, 4).unwrap().get(), 2);
}

#[test]
fn a_session_retired_during_a_discard_consumes_its_result_without_a_receipt_hole() {
    let mut owner = limited(18, 8);
    message_register_receipts(&mut owner);
    sized(&mut owner, 0, Qos::AtLeastOnce, false, 7);
    let (claim, _) = claim_next(&mut owner, TARGET);
    let (effect, discard, _) = discard_next(&mut owner, TARGET, &claim);
    owner.pubsub.retire_session(TARGET);
    let _ = apply(
        &mut owner,
        Input::ConnectionClosed {
            connection: CONNECTION,
        },
    );
    assert_eq!(owner.receipt_status().unwrap().2, 1);
    let observed = known(&mut owner, effect, &discard);
    assert_eq!(names(&observed), ["DeliveryDiscarded"]);
    assert_eq!(
        discarded(&observed),
        (delivery(0), TARGET, discard.recipe.operation())
    );
    assert!(owner.pubsub.deliveries.is_empty());
    assert_eq!(
        owner.receipt_status().unwrap().2,
        0,
        "retired_discard_left_receipt_hole"
    );
    assert!(known(&mut owner, effect, &discard).is_empty());
}

#[test]
fn an_absent_maximum_packet_size_sends_the_largest_admissible_delivery() {
    // MQTT 3.1.1 and an MQTT 5 CONNECT without the property both reach
    // the core as `u32::MAX`: nothing the limits admit is discarded.
    for qos in [Qos::AtLeastOnce, Qos::ExactlyOnce] {
        let mut owner = limited(u32::MAX, 8);
        message_clock(&mut owner, 10);
        sized(&mut owner, 0, qos, true, 128);
        owner.pubsub.deliveries.row_mut(0).topic = Some(bytes(&[b't'; 128]));
        let (_, sent) = claim_next(&mut owner, TARGET);
        assert_eq!(names(&sent), ["EmitPublish"]);
    }
}
/// The broker sends PUBREL in the order in which it received the
/// PUBRECs (MQTT-4.6.0-4, which MQTT-4.6.0-6 of 3.1.1 puts on a server
/// for its deliveries), whenever the durable preparations of those
/// PUBRELs are known. Here two PUBRECs arrive in order and the store
/// answers their preparations in reverse: the PUBREL of the second
/// waits for that of the first (counter-audit of bbe1453).
#[test]
fn pubrel_leaves_in_the_order_of_the_pubrecs_whatever_the_order_of_the_results() {
    let mut owner = world();
    let key = ConnectionKey {
        session: TARGET,
        generation: CONNECTION,
    };
    let mut pids = Vec::new();
    for index in 0..2 {
        queued(&mut owner, index);
        owner.pubsub.deliveries.row_mut(index as usize).qos = Qos::ExactlyOnce;
        let claim = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        let (id, Effect::Persist { command }) = &claim[0] else {
            panic!("claim")
        };
        let publish = known(&mut owner, *id, command);
        let Effect::EmitPublish { packet_id, .. } = publish[0].1 else {
            panic!("publish")
        };
        pids.push(packet_id);
    }
    let mut preparations = Vec::new();
    for pid in &pids {
        let target = owner.qos2_ack_target(key, *pid, Qos2Ack::Pubrec);
        assert!(target.is_some());
        let effects = apply(
            &mut owner,
            Input::ObservedQos2Ack {
                connection: key,
                kind: Qos2Ack::Pubrec,
                packet_id: *pid,
                reason: 0,
                target,
            },
        );
        let (id, Effect::Persist { command }) = &effects[0] else {
            panic!("prepare")
        };
        assert!(matches!(command.recipe.get(), Recipe::PreparePubrel(_)));
        preparations.push((*id, command.clone()));
    }
    let pubrels = |effects: &[(EffectId, Effect)]| -> Vec<u16> {
        effects
            .iter()
            .filter_map(|(_, effect)| match effect {
                Effect::EmitFrame {
                    frame: OutboundFrame::Pubrel { packet_id },
                    ..
                } => Some(packet_id.get()),
                _ => None,
            })
            .collect()
    };
    // The preparation of the second PUBREL is known first.
    let (id, command) = &preparations[1];
    let mut seen = pubrels(&known(&mut owner, *id, command));
    seen.extend(pubrels(&apply(
        &mut owner,
        Input::WorkAvailable { session: TARGET },
    )));
    assert_eq!(
        seen,
        Vec::<u16>::new(),
        "the second PUBREL left before the first"
    );
    // Then that of the first: both leave, in the order of the PUBRECs.
    let (id, command) = &preparations[0];
    seen.extend(pubrels(&known(&mut owner, *id, command)));
    for _ in 0..2 {
        seen.extend(pubrels(&apply(
            &mut owner,
            Input::WorkAvailable { session: TARGET },
        )));
    }
    assert_eq!(seen, [pids[0].get(), pids[1].get()]);
}

/// Regression witness for MQTT-4.6.0-4 / MQTT-4.6.0-6:
/// Delivery 0 carries a Message Expiry Interval (so `FrameProgress::Started`
/// places it in `Phase::StartPending` while persisting `MarkDeliveryStarted`).
/// Delivery 1 has no expiry. When `PUBREC(0)` arrives while delivery 0 is
/// still `StartPending` and `PUBREC(1)` arrives right after (committing its
/// `PreparePubrel` before delivery 0's `MarkDeliveryStarted` even commits),
/// delivery 0 MUST keep its earlier rank from the very first PUBREC, holding
/// `PUBREL(1)` until `PUBREL(0)` leaves first.
#[test]
fn early_pubrec_during_start_pending_preserves_pubrel_order_against_later_pubrec() {
    let mut owner = world();
    let now = MessageTime {
        domain: [1u8; 32],
        tick: 1_000_000_000,
    };
    let _ = apply(&mut owner, Input::MessageClock { now });
    let key = ConnectionKey {
        session: TARGET,
        generation: CONNECTION,
    };

    // Delivery 0: QoS 2 WITH Message Expiry Interval.
    queued(&mut owner, 0);
    {
        let d0 = owner.pubsub.deliveries.row_mut(0);
        d0.qos = Qos::ExactlyOnce;
        d0.expiry = Some(Box::new(MessageExpiry {
            observed: now,
            interval: 60,
        }));
    }
    let claim0 = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    let (id0, Effect::Persist { command: cmd0 }) = &claim0[0] else {
        panic!("claim0")
    };
    let pub0 = known(&mut owner, *id0, cmd0);
    let (
        frame0,
        Effect::EmitPublish {
            packet_id: pid0, ..
        },
    ) = pub0[0]
    else {
        panic!("publish0")
    };

    // Delivery 1: QoS 2 WITHOUT Message Expiry Interval.
    queued(&mut owner, 1);
    owner.pubsub.deliveries.row_mut(1).qos = Qos::ExactlyOnce;
    let claim1 = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    let (id1, Effect::Persist { command: cmd1 }) = &claim1[0] else {
        panic!("claim1")
    };
    let pub1 = known(&mut owner, *id1, cmd1);
    let Effect::EmitPublish {
        packet_id: pid1, ..
    } = pub1[0].1
    else {
        panic!("publish1")
    };

    // Frame 0 begins writing -> triggers MarkDeliveryStarted (Phase::StartPending).
    let start_effects = apply(
        &mut owner,
        Input::FrameResult {
            effect: frame0,
            connection: key,
            progress: FrameProgress::Started,
        },
    );
    let (start_id, Effect::Persist { command: start_cmd }) = &start_effects[0] else {
        panic!("MarkDeliveryStarted expected")
    };
    assert!(matches!(
        start_cmd.recipe.get(),
        Recipe::MarkDeliveryStarted(_)
    ));

    // 1) PUBREC(pid0) arrives FIRST while delivery 0 is still StartPending.
    let target0 = owner.qos2_ack_target(key, pid0, Qos2Ack::Pubrec);
    assert!(target0.is_some());
    let ack0 = apply(
        &mut owner,
        Input::ObservedQos2Ack {
            connection: key,
            kind: Qos2Ack::Pubrec,
            packet_id: pid0,
            reason: 0,
            target: target0,
        },
    );
    assert!(
        ack0.is_empty(),
        "early PUBREC waits for MarkDeliveryStarted"
    );

    // 2) PUBREC(pid1) arrives SECOND and immediately submits PreparePubrel(1).
    let target1 = owner.qos2_ack_target(key, pid1, Qos2Ack::Pubrec);
    assert!(target1.is_some());
    let prep1_effects = apply(
        &mut owner,
        Input::ObservedQos2Ack {
            connection: key,
            kind: Qos2Ack::Pubrec,
            packet_id: pid1,
            reason: 0,
            target: target1,
        },
    );
    let (prep1_id, Effect::Persist { command: prep1_cmd }) = &prep1_effects[0] else {
        panic!("PreparePubrel(1) expected")
    };
    assert!(matches!(prep1_cmd.recipe.get(), Recipe::PreparePubrel(_)));

    let pubrels = |effects: &[(EffectId, Effect)]| -> Vec<u16> {
        effects
            .iter()
            .filter_map(|(_, effect)| match effect {
                Effect::EmitFrame {
                    frame: OutboundFrame::Pubrel { packet_id },
                    ..
                } => Some(packet_id.get()),
                _ => None,
            })
            .collect()
    };

    // 3) PreparePubrel(1) commits BEFORE MarkDeliveryStarted(0)!
    //    PUBREL(1) MUST stay held behind delivery 0's earlier PUBREC rank.
    let mut seen = pubrels(&known(&mut owner, *prep1_id, prep1_cmd));
    seen.extend(pubrels(&apply(
        &mut owner,
        Input::WorkAvailable { session: TARGET },
    )));
    assert_eq!(
        seen,
        Vec::<u16>::new(),
        "PUBREL(1) must not overtake delivery 0 whose PUBREC arrived earlier during StartPending"
    );

    // 4) MarkDeliveryStarted(0) commits -> immediately submits PreparePubrel(0)
    //    preserving delivery 0's rank (1 < 2).
    let started_effects = known(&mut owner, *start_id, start_cmd);
    let (prep0_id, Effect::Persist { command: prep0_cmd }) = &started_effects[0] else {
        panic!("PreparePubrel(0) expected immediately after DeliveryStarted")
    };
    assert!(matches!(prep0_cmd.recipe.get(), Recipe::PreparePubrel(_)));
    // Still waiting for PreparePubrel(0) to commit -> PUBREL(1) must STILL be held.
    seen.extend(pubrels(&apply(
        &mut owner,
        Input::WorkAvailable { session: TARGET },
    )));
    assert_eq!(
        seen,
        Vec::<u16>::new(),
        "PUBREL(1) must remain held while PreparePubrel(0) is in flight"
    );

    // 5) PreparePubrel(0) commits -> PUBREL(0) leaves strictly before PUBREL(1).
    seen.extend(pubrels(&known(&mut owner, *prep0_id, prep0_cmd)));
    for _ in 0..3 {
        seen.extend(pubrels(&apply(
            &mut owner,
            Input::WorkAvailable { session: TARGET },
        )));
    }
    assert_eq!(seen, [pid0.get(), pid1.get()]);

    // 6) Both PUBCOMPs resolve both deliveries cleanly.
    for pid in [pid0, pid1] {
        let target = owner.qos2_ack_target(key, pid, Qos2Ack::Pubcomp);
        assert!(target.is_some());
        let fin = apply(
            &mut owner,
            Input::ObservedQos2Ack {
                connection: key,
                kind: Qos2Ack::Pubcomp,
                packet_id: pid,
                reason: 0,
                target,
            },
        );
        let (fin_id, Effect::Persist { command: fin_cmd }) = &fin[0] else {
            panic!("FinishDelivery expected")
        };
        let _ = known(&mut owner, *fin_id, fin_cmd);
    }
    assert!(owner.pubsub.deliveries.is_empty());
}

#[test]
fn inline_claim_commits_emits_rejects_and_handles_toolarge_and_retire() {
    let mut owner = world();
    owner.enable_inline_claims();
    let pub_conn = ConnectionGeneration::new(11);
    let _ = apply(
        &mut owner,
        Input::Packet {
            connection: pub_conn,
            ingress_order: 0,
            packet: InboundPacket::ConnectResume {
                client_id: bytes(b"other"),
            },
        },
    );
    // Subscribe TARGET to "fast/topic" QoS 1.
    let filters = crate::contract::pubsub::FilterList::copy_from(&[
        crate::contract::pubsub::SubscriptionFilter {
            filter: bytes(b"fast/topic"),
            qos: Qos::AtLeastOnce,
            options: crate::contract::pubsub::SubscriptionOptions::DEFAULT,
        },
    ])
    .unwrap();
    let sub_eff = apply(
        &mut owner,
        Input::Packet {
            connection: CONNECTION,
            ingress_order: 1,
            packet: InboundPacket::Subscribe {
                denied: 0,
                packet_id: InboundPacketId::new(1).unwrap(),
                filters,
            },
        },
    );
    let (sub_id, Effect::Persist { command: sub_cmd }) = &sub_eff[0] else {
        panic!("subscribe persist expected");
    };
    let _ = apply(
        &mut owner,
        Input::StoreResult {
            effect: *sub_id,
            operation: sub_cmd.recipe.operation(),
            reply: StoreReply::Outcome(StoreOutcome::Committed(
                CommitFacts::SubscriptionsChanged {
                    session: TARGET,
                    route_revision: Revision::new(1),
                },
            )),
        },
    );

    // Catch TARGET up via one empty WorkPage.
    let scan_eff = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    let (scan_id, Effect::LoadWork { .. }) = &scan_eff[0] else {
        panic!("LoadWork expected");
    };
    let _ = apply(
        &mut owner,
        Input::WorkPage {
            effect: *scan_id,
            session: TARGET,
            items: BoundedVec::with_capacity(0).unwrap(),
            next: None,
        },
    );

    let publish = |owner: &mut Owner, order: u64, pid: u16, body: &[u8]| {
        apply(
            owner,
            Input::Packet {
                connection: pub_conn,
                ingress_order: order,
                packet: InboundPacket::Publish {
                    qos: Qos::AtLeastOnce,
                    retain: false,
                    dup: false,
                    packet_id: Some(InboundPacketId::new(pid).unwrap()),
                    topic: bytes(b"fast/topic"),
                    payload: bytes(body),
                    expiry: None,
                    properties: None,
                },
            },
        )
    };

    // 1) Two pipelined QoS 1 publishes BOTH become ClaimedPublication (packet_id 1 & 2).
    let p1 = publish(&mut owner, 1, 1, b"m1");
    let (eff1, Effect::Persist { command: cmd1 }) = &p1[0] else {
        panic!("Persist 1 expected");
    };
    let Recipe::ClaimedPublication(cp1) = cmd1.recipe.get() else {
        panic!(
            "expected ClaimedPublication for m1, got {:?}",
            cmd1.recipe.get()
        );
    };
    assert_eq!(cp1.claim.packet_id.get(), 1);
    assert_eq!(
        owner.inline_claim_target(cmd1.recipe.operation()),
        Some(TARGET)
    );

    let p2 = publish(&mut owner, 2, 2, b"m2");
    let (eff2, Effect::Persist { command: cmd2 }) = &p2[0] else {
        panic!("Persist 2 expected");
    };
    let Recipe::ClaimedPublication(cp2) = cmd2.recipe.get() else {
        panic!("expected ClaimedPublication for m2");
    };
    assert_eq!(cp2.claim.packet_id.get(), 2);

    // 2) Commit #2 BEFORE #1: #2's EmitPublish MUST wait behind #1 (strict wire order).
    let res2 = apply(
        &mut owner,
        Input::StoreResult {
            effect: *eff2,
            operation: cmd2.recipe.operation(),
            reply: StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::PublicationAdmitted {
                publication: cp2.admission.publication,
                exchange: cp2.admission.source.exchange,
            })),
        },
    );
    assert!(
        !res2
            .iter()
            .any(|(_, e)| matches!(e, Effect::EmitPublish { .. })),
        "delivery 2 must not emit before delivery 1 commits"
    );

    // 3) Commit #1: emits publisher PUBACK(1), subscriber EmitPublish(pid=1), and releases PUBACK(2)!
    let res1 = apply(
        &mut owner,
        Input::StoreResult {
            effect: *eff1,
            operation: cmd1.recipe.operation(),
            reply: StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::PublicationAdmitted {
                publication: cp1.admission.publication,
                exchange: cp1.admission.source.exchange,
            })),
        },
    );
    assert!(res1.iter().any(|(_, e)| matches!(
        e,
        Effect::EmitPublish { packet_id, payload, .. }
            if packet_id.get() == 1 && payload.as_slice() == b"m1"
    )));
    assert!(
        owner.has_ready_work(TARGET),
        "delivery 2 is now ready to emit"
    );
    let w2 = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    assert!(w2.iter().any(|(_, e)| matches!(
        e,
        Effect::EmitPublish { packet_id, payload, .. }
            if packet_id.get() == 2 && payload.as_slice() == b"m2"
    )));
    assert!(!owner.has_ready_work(TARGET));

    // 4) Rejection of inline-claimed #3 frees its provisional delivery and PacketId(3).
    let p3 = publish(&mut owner, 3, 3, b"m3");
    let (eff3, Effect::Persist { command: cmd3 }) = &p3[0] else {
        panic!("Persist 3 expected");
    };
    let Recipe::ClaimedPublication(cp3) = cmd3.recipe.get() else {
        panic!("expected ClaimedPublication for m3");
    };
    assert_eq!(cp3.claim.packet_id.get(), 3);
    let _ = apply(
        &mut owner,
        Input::StoreResult {
            effect: *eff3,
            operation: cmd3.recipe.operation(),
            reply: StoreReply::Outcome(StoreOutcome::RejectedCertain(
                RejectReason::PreconditionFailed,
            )),
        },
    );
    assert_eq!(owner.inline_claim_target(cmd3.recipe.operation()), None);
    // Next publish reuses PacketId(3) because #3 was rolled back cleanly.
    let p4 = publish(&mut owner, 4, 4, b"m4");
    let (eff4, Effect::Persist { command: cmd4 }) = &p4[0] else {
        panic!("Persist 4 expected");
    };
    let Recipe::ClaimedPublication(cp4) = cmd4.recipe.get() else {
        panic!("expected ClaimedPublication for m4");
    };
    assert_eq!(cp4.claim.packet_id.get(), 3);

    // 5) Retiring TARGET while #4 is InlineClaimPending tracks retired_flights == 1 -> 0 on result.
    owner.pubsub.retire_session(TARGET);
    assert_eq!(owner.pubsub.retired_flights(), 1);
    let res4 = apply(
        &mut owner,
        Input::StoreResult {
            effect: *eff4,
            operation: cmd4.recipe.operation(),
            reply: StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::PublicationAdmitted {
                publication: cp4.admission.publication,
                exchange: cp4.admission.source.exchange,
            })),
        },
    );
    assert!(
        !res4
            .iter()
            .any(|(_, e)| matches!(e, Effect::EmitPublish { .. }))
    );
    assert_eq!(owner.pubsub.retired_flights(), 0);
}

#[test]
fn multi_target_inline_claim_full_partial_rollback_and_retire() {
    let incarnation = StoreIncarnation::new(1);
    let epoch = RunEpoch::new(2);
    let mut opening = EpochOpening::new(incarnation, StartupNonce::new([7; 16]));
    let command = opening.submit(RunEpoch::new(1)).unwrap();
    let _ = opening
        .observe(OpeningReply {
            command,
            outcome: OpeningOutcome::Committed(EpochOpened { incarnation, epoch }),
        })
        .unwrap();
    let mut authority =
        OwnerAuthority::from_grant(opening.hand_over().unwrap(), nonzero(1)).unwrap();
    let outbox = authority
        .create_outbox(
            LogicalOwnerId::new(4),
            NonZeroUsize::new(32).unwrap(),
            nonzero(8),
        )
        .unwrap();
    let config = OwnerConfig {
        max_sessions: nonzero(4),
        max_routes: nonzero(4),
        max_exchanges: nonzero(16),
        session_qos2_exchanges: NonZeroU32::MAX,
        account: AccountId::new(1),
        payload_bytes: PayloadBytes::new(4096),
        resolution_delay: LogicalDuration::new(1),
        retained_operations: nonzero(16),
        remembered_connections: nonzero(8),
    };
    let mut owner = Owner::new(config, outbox).unwrap();
    owner.enable_inline_claims();
    let s3 = SessionId::new(30);
    let s4 = SessionId::new(40);
    let c3 = ConnectionGeneration::new(30);
    let c4 = ConnectionGeneration::new(40);
    let pub_conn = ConnectionGeneration::new(11);
    let facts = [
        RecoveredFact::Epoch { incarnation, epoch },
        RecoveredFact::Limits {
            max_targets_per_admission: 4,
            max_topic_bytes: 128,
            max_payload_bytes: PayloadBytes::new(128),
        },
        RecoveredFact::RouteTable {
            revision: Revision::new(0),
        },
        RecoveredFact::Session {
            session: TARGET,
            client_id: bytes(b"subscriber"),
            revision: Revision::new(1),
            expiry: crate::contract::session::SessionExpiry::Never,
            opened_by: None,
            detached: None,
        },
        RecoveredFact::Session {
            session: SessionId::new(2),
            client_id: bytes(b"other"),
            revision: Revision::new(1),
            expiry: crate::contract::session::SessionExpiry::Never,
            opened_by: None,
            detached: None,
        },
        RecoveredFact::Session {
            session: s3,
            client_id: bytes(b"sub-3"),
            revision: Revision::new(1),
            expiry: crate::contract::session::SessionExpiry::Never,
            opened_by: None,
            detached: None,
        },
        RecoveredFact::Session {
            session: s4,
            client_id: bytes(b"sub-4"),
            revision: Revision::new(1),
            expiry: crate::contract::session::SessionExpiry::Never,
            opened_by: None,
            detached: None,
        },
    ];
    assert!(
        apply(
            &mut owner,
            Input::Recovery {
                facts: BoundedVec::from_slice(&facts, facts.len()).unwrap(),
                last: true,
            }
        )
        .is_empty()
    );
    for (name, conn) in [
        (b"subscriber".as_slice(), CONNECTION),
        (b"sub-3".as_slice(), c3),
        (b"sub-4".as_slice(), c4),
    ] {
        let _ = apply(
            &mut owner,
            Input::Packet {
                connection: conn,
                ingress_order: 0,
                packet: InboundPacket::ConnectResume {
                    client_id: bytes(name),
                },
            },
        );
        let _ = apply(
            &mut owner,
            Input::ConnectionSettings {
                connection: conn,
                receive_maximum: 4,
                maximum_packet_size: 4_096,
            },
        );
    }
    let _ = apply(
        &mut owner,
        Input::Packet {
            connection: pub_conn,
            ingress_order: 0,
            packet: InboundPacket::ConnectResume {
                client_id: bytes(b"other"),
            },
        },
    );

    let subscribe =
        |owner: &mut Owner, conn: ConnectionGeneration, sess: SessionId, rev: u64, qos: Qos| {
            let filters = crate::contract::pubsub::FilterList::copy_from(&[
                crate::contract::pubsub::SubscriptionFilter {
                    filter: bytes(b"fan/out"),
                    qos,
                    options: crate::contract::pubsub::SubscriptionOptions::DEFAULT,
                },
            ])
            .unwrap();
            let sub_eff = apply(
                owner,
                Input::Packet {
                    connection: conn,
                    ingress_order: 1,
                    packet: InboundPacket::Subscribe {
                        denied: 0,
                        packet_id: InboundPacketId::new(1).unwrap(),
                        filters,
                    },
                },
            );
            let (sub_id, Effect::Persist { command: sub_cmd }) = &sub_eff[0] else {
                panic!("subscribe persist expected");
            };
            let _ = apply(
                owner,
                Input::StoreResult {
                    effect: *sub_id,
                    operation: sub_cmd.recipe.operation(),
                    reply: StoreReply::Outcome(StoreOutcome::Committed(
                        CommitFacts::SubscriptionsChanged {
                            session: sess,
                            route_revision: Revision::new(rev),
                        },
                    )),
                },
            );
        };
    let catch_up = |owner: &mut Owner, sess: SessionId| {
        let scan_eff = apply(owner, Input::WorkAvailable { session: sess });
        let (scan_id, Effect::LoadWork { .. }) = &scan_eff[0] else {
            panic!("LoadWork expected");
        };
        let _ = apply(
            owner,
            Input::WorkPage {
                effect: *scan_id,
                session: sess,
                items: BoundedVec::with_capacity(0).unwrap(),
                next: None,
            },
        );
    };

    subscribe(&mut owner, CONNECTION, TARGET, 1, Qos::AtLeastOnce);
    subscribe(&mut owner, c3, s3, 2, Qos::AtLeastOnce);
    subscribe(&mut owner, c4, s4, 3, Qos::AtLeastOnce);
    catch_up(&mut owner, TARGET);
    catch_up(&mut owner, s3);
    // Leave s4 NOT caught up first to test partial multi-claim (2 of 3 targets!).

    let publish = |owner: &mut Owner, order: u64, pid: u16, body: &[u8]| {
        apply(
            owner,
            Input::Packet {
                connection: pub_conn,
                ingress_order: order,
                packet: InboundPacket::Publish {
                    qos: Qos::AtLeastOnce,
                    retain: false,
                    dup: false,
                    packet_id: Some(InboundPacketId::new(pid).unwrap()),
                    topic: bytes(b"fan/out"),
                    payload: bytes(body),
                    expiry: None,
                    properties: None,
                },
            },
        )
    };

    // 1) Partial MultiClaimedPublication: TARGET and s3 claimed inline, s4 remains unclaimed.
    let p1 = publish(&mut owner, 1, 1, b"partial");
    let (eff1, Effect::Persist { command: cmd1 }) = &p1[0] else {
        panic!("Persist 1 expected");
    };
    let Recipe::MultiClaimedPublication(mc1) = cmd1.recipe.get() else {
        panic!(
            "expected MultiClaimedPublication, got {:?}",
            cmd1.recipe.get()
        );
    };
    assert_eq!(mc1.admission.targets.len(), 3);
    assert_eq!(mc1.claims.len(), 2);
    assert_eq!(owner.inline_claim_targets(cmd1.recipe.operation()).len(), 2);
    assert!(owner.pubsub.non_inline_by_session.contains_key(&s4));
    assert!(!owner.pubsub.non_inline_by_session.contains_key(&TARGET));
    assert!(!owner.pubsub.non_inline_by_session.contains_key(&s3));

    let res1 = apply(
        &mut owner,
        Input::StoreResult {
            effect: *eff1,
            operation: cmd1.recipe.operation(),
            reply: StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::PublicationAdmitted {
                publication: mc1.admission.publication,
                exchange: mc1.admission.source.exchange,
            })),
        },
    );
    // One claimed target emits directly on PublicationAdmitted; the other claimed target
    // emits on its WorkAvailable with ZERO LoadWork/ClaimDelivery, while s4 is marked dirty.
    let direct_emits = res1
        .iter()
        .filter(|(_, e)| matches!(e, Effect::EmitPublish { .. }))
        .count();
    assert_eq!(direct_emits, 1);
    assert!(owner.last_inline_ready_target().is_some());
    let mut ready_targets = alloc::vec::Vec::new();
    if let Some(first) = owner.last_inline_ready_target() {
        ready_targets.push(first);
    }
    ready_targets.extend_from_slice(owner.last_inline_ready_extra());
    assert!(ready_targets.contains(&s4));
    // Pump the second claimed target (whichever of TARGET/s3 did not emit directly).
    let mut second_emitted = 0usize;
    for sess in [TARGET, s3] {
        if owner.has_ready_work(sess) {
            let w = apply(&mut owner, Input::WorkAvailable { session: sess });
            assert!(w.iter().any(|(_, e)| matches!(
                e,
                Effect::EmitPublish { payload, .. } if payload.as_slice() == b"partial"
            )));
            second_emitted += 1;
        }
    }
    assert_eq!(second_emitted, 1);

    // 2) Catch s4 up; now a publish becomes a FULL 3-of-3 MultiClaimedPublication!
    catch_up(&mut owner, s4);
    let p2 = publish(&mut owner, 2, 2, b"full-3");
    let (eff2, Effect::Persist { command: cmd2 }) = &p2[0] else {
        panic!("Persist 2 expected");
    };
    let Recipe::MultiClaimedPublication(mc2) = cmd2.recipe.get() else {
        panic!("expected full MultiClaimedPublication");
    };
    assert_eq!(mc2.claims.len(), 3);
    assert!(owner.pubsub.non_inline_by_session.is_empty());

    // 3) Reject p2 -> all 3 provisional deliveries roll back cleanly, freeing packet IDs.
    let _ = apply(
        &mut owner,
        Input::StoreResult {
            effect: *eff2,
            operation: cmd2.recipe.operation(),
            reply: StoreReply::Outcome(StoreOutcome::RejectedCertain(
                RejectReason::PreconditionFailed,
            )),
        },
    );
    assert!(
        owner
            .inline_claim_targets(cmd2.recipe.operation())
            .is_empty()
    );

    // 4) Next publish claims all 3 again; retire s3 mid-flight -> TARGET & s4 commit, s3 drops.
    let p3 = publish(&mut owner, 3, 3, b"retire-one");
    let (eff3, Effect::Persist { command: cmd3 }) = &p3[0] else {
        panic!("Persist 3 expected");
    };
    let Recipe::MultiClaimedPublication(mc3) = cmd3.recipe.get() else {
        panic!("expected MultiClaimedPublication for p3");
    };
    assert_eq!(mc3.claims.len(), 3);
    owner.pubsub.retire_session(s3);
    assert_eq!(owner.pubsub.retired_flights(), 1);
    let res3 = apply(
        &mut owner,
        Input::StoreResult {
            effect: *eff3,
            operation: cmd3.recipe.operation(),
            reply: StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::PublicationAdmitted {
                publication: mc3.admission.publication,
                exchange: mc3.admission.source.exchange,
            })),
        },
    );
    assert_eq!(owner.pubsub.retired_flights(), 0);
    let mut delivered_p3 = res3
            .iter()
            .filter(|(_, e)| matches!(e, Effect::EmitPublish { payload, .. } if payload.as_slice() == b"retire-one"))
            .count();
    for sess in [TARGET, s4] {
        if owner.has_ready_work(sess) {
            let w = apply(&mut owner, Input::WorkAvailable { session: sess });
            delivered_p3 += w
                    .iter()
                    .filter(|(_, e)| matches!(e, Effect::EmitPublish { payload, .. } if payload.as_slice() == b"retire-one"))
                    .count();
        }
    }
    assert_eq!(delivered_p3, 2);
}

#[test]
fn inline_preclaim_burst_respects_receive_maximum_and_emits_fifo() {
    let mut owner = world();
    owner.enable_inline_claims();
    owner.pubsub.connections.as_mut_slice()[0].receive_maximum = 2;
    let pub_conn = ConnectionGeneration::new(11);
    let _ = apply(
        &mut owner,
        Input::Packet {
            connection: pub_conn,
            ingress_order: 0,
            packet: InboundPacket::ConnectResume {
                client_id: bytes(b"other"),
            },
        },
    );
    let filters = crate::contract::pubsub::FilterList::copy_from(&[
        crate::contract::pubsub::SubscriptionFilter {
            filter: bytes(b"burst/topic"),
            qos: Qos::AtLeastOnce,
            options: crate::contract::pubsub::SubscriptionOptions::DEFAULT,
        },
    ])
    .unwrap();
    let sub_eff = apply(
        &mut owner,
        Input::Packet {
            connection: CONNECTION,
            ingress_order: 1,
            packet: InboundPacket::Subscribe {
                denied: 0,
                packet_id: InboundPacketId::new(1).unwrap(),
                filters,
            },
        },
    );
    let (sub_id, Effect::Persist { command: sub_cmd }) = &sub_eff[0] else {
        panic!("subscribe persist expected");
    };
    let _ = apply(
        &mut owner,
        Input::StoreResult {
            effect: *sub_id,
            operation: sub_cmd.recipe.operation(),
            reply: StoreReply::Outcome(StoreOutcome::Committed(
                CommitFacts::SubscriptionsChanged {
                    session: TARGET,
                    route_revision: Revision::new(1),
                },
            )),
        },
    );
    let scan_eff = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    let (scan_id, Effect::LoadWork { .. }) = &scan_eff[0] else {
        panic!("LoadWork expected");
    };
    let _ = apply(
        &mut owner,
        Input::WorkPage {
            effect: *scan_id,
            session: TARGET,
            items: BoundedVec::with_capacity(0).unwrap(),
            next: None,
        },
    );

    let publish = |owner: &mut Owner, seq: u16| {
        apply(
            owner,
            Input::Packet {
                connection: pub_conn,
                ingress_order: u64::from(seq),
                packet: InboundPacket::Publish {
                    qos: Qos::AtLeastOnce,
                    retain: false,
                    dup: false,
                    packet_id: Some(InboundPacketId::new(seq).unwrap()),
                    topic: bytes(b"burst/topic"),
                    payload: bytes(&[b'0' + seq as u8]),
                    expiry: None,
                    properties: None,
                },
            },
        )
    };

    // RM = 2 -> preclaim_cap = 4: first 4 publishes are ClaimedPublication,
    // 5th falls back to AdmitPublication without breaking strict FIFO or RM=2.
    let mut admitted = alloc::vec::Vec::new();
    for seq in 1..=4u16 {
        let effs = publish(&mut owner, seq);
        let (eff_id, Effect::Persist { command }) = &effs[0] else {
            panic!("persist expected for {seq}");
        };
        let Recipe::ClaimedPublication(cp) = command.recipe.get() else {
            panic!(
                "expected ClaimedPublication for {seq}, got {:?}",
                command.recipe.get()
            );
        };
        assert_eq!(cp.claim.packet_id.get(), seq);
        assert!(
            owner.pubsub.active(CONNECTION) <= 2,
            "active window exceeded receive_maximum=2 after inline submit {seq}"
        );
        admitted.push((
            *eff_id,
            command.recipe.operation(),
            cp.admission.publication,
            cp.admission.source.exchange,
        ));
    }
    // 5th publish exceeds preclaim_cap=4 -> AdmitPublication (non-inline fallback).
    let eff5 = publish(&mut owner, 5);
    let (_, Effect::Persist { command: cmd5 }) = &eff5[0] else {
        panic!("persist expected for 5");
    };
    assert!(
        matches!(cmd5.recipe.get(), Recipe::AdmitPublication(_)),
        "5th publish should fall back to AdmitPublication"
    );

    // Commit all 4 ClaimedPublications: ONLY #1 and #2 may emit on the wire!
    let mut emitted = alloc::vec::Vec::new();
    for (eff_id, op, pub_id, exch) in admitted {
        let res = apply(
            &mut owner,
            Input::StoreResult {
                effect: eff_id,
                operation: op,
                reply: StoreReply::Outcome(StoreOutcome::Committed(
                    CommitFacts::PublicationAdmitted {
                        publication: pub_id,
                        exchange: exch,
                    },
                )),
            },
        );
        for (_, e) in res {
            if let Effect::EmitPublish {
                packet_id, payload, ..
            } = e
            {
                emitted.push((packet_id.get(), payload.as_slice()[0]));
            }
        }
        assert!(
            owner.pubsub.active(CONNECTION) <= 2,
            "active window exceeded receive_maximum=2 on commit"
        );
    }
    assert_eq!(emitted, alloc::vec![(1, b'1'), (2, b'2')]);
    // Ready work must be false while Receive Maximum is full (no spin!).
    assert!(!owner.has_ready_work(TARGET));

    let key = ConnectionKey {
        session: TARGET,
        generation: CONNECTION,
    };
    // Acknowledge #1 -> FinishDelivery commits -> #3 becomes ready and emits in strict FIFO!
    let pid1 = OutboundPacketId::new(1).unwrap();
    let target1 = owner.puback_target(key, pid1).unwrap();
    let ack1 = apply(
        &mut owner,
        Input::ObservedPuback {
            connection: key,
            packet_id: pid1,
            reason: 0,
            target: Some(target1),
        },
    );
    let (fin1_id, Effect::Persist { command: fin1_cmd }) = &ack1[0] else {
        panic!("FinishDelivery 1 expected");
    };
    let Recipe::FinishDelivery(fin1) = fin1_cmd.recipe.get() else {
        panic!("expected FinishDelivery recipe");
    };
    let _ = apply(
        &mut owner,
        Input::StoreResult {
            effect: *fin1_id,
            operation: fin1_cmd.recipe.operation(),
            reply: StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::DeliveryFinished {
                delivery: fin1.delivery,
                target: fin1.target,
                packet_id: fin1.packet_id,
                claim: fin1.claim,
            })),
        },
    );
    assert!(owner.has_ready_work(TARGET));
    let w3 = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    let mut emitted_after = alloc::vec::Vec::new();
    for (_, e) in w3 {
        if let Effect::EmitPublish {
            packet_id, payload, ..
        } = e
        {
            emitted_after.push((packet_id.get(), payload.as_slice()[0]));
        }
    }
    assert_eq!(emitted_after, alloc::vec![(3, b'3')]);
    assert_eq!(owner.pubsub.active(CONNECTION), 2);
    assert!(!owner.has_ready_work(TARGET));

    // Acknowledge #2 -> FinishDelivery commits -> #4 becomes ready and emits in strict FIFO!
    let pid2 = OutboundPacketId::new(2).unwrap();
    let target2 = owner.puback_target(key, pid2).unwrap();
    let ack2 = apply(
        &mut owner,
        Input::ObservedPuback {
            connection: key,
            packet_id: pid2,
            reason: 0,
            target: Some(target2),
        },
    );
    let (fin2_id, Effect::Persist { command: fin2_cmd }) = &ack2[0] else {
        panic!("FinishDelivery 2 expected");
    };
    let Recipe::FinishDelivery(fin2) = fin2_cmd.recipe.get() else {
        panic!("expected FinishDelivery recipe");
    };
    let _ = apply(
        &mut owner,
        Input::StoreResult {
            effect: *fin2_id,
            operation: fin2_cmd.recipe.operation(),
            reply: StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::DeliveryFinished {
                delivery: fin2.delivery,
                target: fin2.target,
                packet_id: fin2.packet_id,
                claim: fin2.claim,
            })),
        },
    );
    assert!(owner.has_ready_work(TARGET));
    let w4 = apply(&mut owner, Input::WorkAvailable { session: TARGET });
    let mut emitted_4 = alloc::vec::Vec::new();
    for (_, e) in w4 {
        if let Effect::EmitPublish {
            packet_id, payload, ..
        } = e
        {
            emitted_4.push((packet_id.get(), payload.as_slice()[0]));
        }
    }
    assert_eq!(emitted_4, alloc::vec![(4, b'4')]);
    assert_eq!(owner.pubsub.active(CONNECTION), 2);
    assert!(!owner.has_ready_work(TARGET));
}

#[test]
fn inline_qos0_direct_emits_within_window_and_spills_in_order() {
    let mut owner = world();
    owner.enable_inline_claims();
    owner.pubsub.volatile = VolatileQueue::new(nonzero(512)).unwrap();
    owner.pubsub.payload_limit = PayloadBytes::new(1_048_576);
    let pub_conn = ConnectionGeneration::new(11);
    let sub_conn = CONNECTION;
    let sub_session = TARGET;
    let _ = apply(
        &mut owner,
        Input::Packet {
            connection: pub_conn,
            ingress_order: 0,
            packet: InboundPacket::ConnectResume {
                client_id: bytes(b"other"),
            },
        },
    );
    let _ = apply(
        &mut owner,
        Input::ConnectionSettings {
            connection: pub_conn,
            receive_maximum: 16,
            maximum_packet_size: 4096,
        },
    );
    let filters = crate::contract::pubsub::FilterList::copy_from(&[
        crate::contract::pubsub::SubscriptionFilter {
            filter: bytes(b"t/direct"),
            qos: Qos::AtMostOnce,
            options: crate::contract::pubsub::SubscriptionOptions::DEFAULT,
        },
    ])
    .unwrap();
    let sub_effects = apply(
        &mut owner,
        Input::Packet {
            connection: sub_conn,
            ingress_order: 1,
            packet: InboundPacket::Subscribe {
                packet_id: InboundPacketId::new(1).unwrap(),
                filters,
                denied: 0,
            },
        },
    );
    let (sub_eff, Effect::Persist { command }) = &sub_effects[0] else {
        panic!("expected subscribe persist");
    };
    let _ = apply(
        &mut owner,
        Input::StoreResult {
            effect: *sub_eff,
            operation: command.recipe.operation(),
            reply: StoreReply::Outcome(StoreOutcome::Committed(
                CommitFacts::SubscriptionsChanged {
                    session: sub_session,
                    route_revision: Revision::new(1),
                },
            )),
        },
    );

    // First QOS0_WINDOW publishes emit Effect::EmitQos0 directly without Effect::Continue.
    let mut emitted_effects = alloc::vec::Vec::new();
    for seq in 1..=(QOS0_WINDOW as u64) {
        let effects = apply(
            &mut owner,
            Input::Packet {
                connection: pub_conn,
                ingress_order: seq,
                packet: InboundPacket::Publish {
                    topic: bytes(b"t/direct"),
                    qos: Qos::AtMostOnce,
                    retain: false,
                    dup: false,
                    packet_id: None,
                    expiry: None,
                    properties: None,
                    payload: bytes(&seq.to_be_bytes()),
                },
            },
        );
        assert_eq!(effects.len(), 1);
        let (eid, Effect::EmitQos0 { payload, .. }) = &effects[0] else {
            panic!("expected direct EmitQos0 at seq {seq}, got {effects:?}");
        };
        assert_eq!(payload.as_slice(), &seq.to_be_bytes());
        emitted_effects.push(*eid);
    }

    // Message QOS0_WINDOW + 1 must spill into VolatileQueue::pending -> park via Continue.
    let spill_seq = (QOS0_WINDOW as u64) + 1;
    let spill = apply(
        &mut owner,
        Input::Packet {
            connection: pub_conn,
            ingress_order: spill_seq,
            packet: InboundPacket::Publish {
                topic: bytes(b"t/direct"),
                qos: Qos::AtMostOnce,
                retain: false,
                dup: false,
                packet_id: None,
                expiry: None,
                properties: None,
                payload: bytes(&spill_seq.to_be_bytes()),
            },
        },
    );
    assert_eq!(spill.len(), 1);
    let (_, Effect::Continue { continuation }) = spill[0] else {
        panic!("expected Continue when window full, got {spill:?}");
    };
    let parked = apply(
        &mut owner,
        Input::Continue {
            continuation,
            budget: WorkUnits::new(64),
        },
    );
    assert!(parked.is_empty());

    // Completing the first emitted frame wakes the parked spillover message in order.
    let done = apply(
        &mut owner,
        Input::FrameResult {
            effect: emitted_effects[0],
            connection: ConnectionKey {
                session: sub_session,
                generation: sub_conn,
            },
            progress: FrameProgress::Written {
                confirmed: 20,
                total: 20,
            },
        },
    );
    assert_eq!(done.len(), 1);
    let (
        _,
        Effect::Continue {
            continuation: wake_c,
        },
    ) = done[0]
    else {
        panic!("expected Continue waking parked delivery, got {done:?}");
    };
    let resumed = apply(
        &mut owner,
        Input::Continue {
            continuation: wake_c,
            budget: WorkUnits::new(64),
        },
    );
    assert_eq!(resumed.len(), 1);
    let (_, Effect::EmitQos0 { payload, .. }) = &resumed[0] else {
        panic!("expected resumed EmitQos0, got {resumed:?}");
    };
    assert_eq!(payload.as_slice(), &spill_seq.to_be_bytes());
}
