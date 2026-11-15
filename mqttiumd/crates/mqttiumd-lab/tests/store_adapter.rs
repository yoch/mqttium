//! The volatile layer around the writer: the named injection points,
//! the bounds of the adapter, the correlation of results (ADR 0003) and
//! what a crash keeps.

use mqttiumd_contract::command::Command;
use mqttiumd_contract::effects::{
    Effect, EffectSink, Input, OwnedEffect, SubmitBlock, SubmitResult,
};
use mqttiumd_contract::ids::OperationId;
use mqttiumd_contract::ids::{
    EffectId, EffectSeq, InboundPacketId, LogicalOwnerId, SessionId, StoreIncarnation,
};
use mqttiumd_contract::outcome::{RejectReason, StoreOutcome, StoreReply};
use mqttiumd_lab::store::adapter::{
    ActionRefused, AdapterLimits, Answer, Delivery, FaultAction, FaultId, FaultPoint, FaultRefused,
    FaultSpec, Performed, SimStore, StoreAction, StoreEvent, Uncorrelated, WorkId, WorkKind,
};
use mqttiumd_lab::store::conformance::{World, admission_in, open, operation_id, publication_id};
use mqttiumd_lab::store::durable::{Durability, DurableStore, Limits, MemoryStore};

type Bench = World<SimStore<MemoryStore>>;

fn memory(incarnation: StoreIncarnation, limits: Limits) -> MemoryStore {
    MemoryStore::create(incarnation, limits)
}

const LIMITS: AdapterLimits = AdapterLimits {
    operations_in_flight: 2,
    result_slots: 3,
    faults: 4,
    events: 256,
};

fn bench_with(limits: AdapterLimits) -> Bench {
    World::new(&memory).wrap(|store| SimStore::new(store, limits))
}

fn bench() -> Bench {
    bench_with(LIMITS)
}

fn effect_id(bench: &Bench, sequence: u64) -> EffectId {
    EffectId {
        epoch: bench.epoch,
        owner: LogicalOwnerId::new(0),
        sequence: EffectSeq::new(sequence),
    }
}

fn persist_effect(bench: &Bench, sequence: u64, command: &Command) -> OwnedEffect {
    OwnedEffect::new(
        effect_id(bench, sequence),
        Effect::Persist {
            command: command.clone(),
        },
    )
}

fn persist(bench: &mut Bench, sequence: u64, command: &Command) -> WorkId {
    let effect = persist_effect(bench, sequence, command);
    let id = WorkId::Effect(effect.id());
    assert_eq!(bench.store.try_submit(effect), SubmitResult::Accepted);
    id
}

fn fault(point: FaultPoint, action: FaultAction) -> FaultSpec {
    FaultSpec {
        point,
        action,
        kind: None,
        source: None,
        occurrence: 1,
    }
}

fn arm(bench: &mut Bench, point: FaultPoint, action: FaultAction) -> FaultId {
    bench.store.arm(fault(point, action)).expect("allowed")
}

fn enter(bench: &mut Bench, id: WorkId) -> Performed {
    bench
        .store
        .perform(StoreAction::EnterWriter(id))
        .expect("eligible")
}

fn deliver(bench: &mut Bench, id: WorkId) -> Performed {
    bench
        .store
        .perform(StoreAction::DeliverResult(id))
        .expect("eligible")
}

fn delivered_reply(performed: Performed) -> StoreReply {
    match performed {
        Performed::Delivered {
            delivery: Delivery::Store(Input::StoreResult { reply, .. }),
            ..
        } => reply,
        other => panic!("not a delivered store result: {other:?}"),
    }
}

/// The hit recorded for `fault`, checked against the catalogue names.
fn hit(events: &[StoreEvent], fault: FaultId, point: &str, action: &str, id: WorkId) {
    let hits: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            StoreEvent::FaultHit {
                fault: reached,
                point,
                action,
                id,
                ..
            } if *reached == fault => Some((point.name(), action.name(), *id)),
            _ => None,
        })
        .collect();
    assert_eq!(hits, [(point, action, id)]);
}

#[test]
fn a_fault_is_armed_only_with_an_action_its_point_allows() {
    let mut bench = bench_with(AdapterLimits {
        faults: 5,
        ..LIMITS
    });
    let points = [
        FaultPoint::CommandTransit,
        FaultPoint::FenceTransit,
        FaultPoint::CommitApplied,
        FaultPoint::ResultReady,
        FaultPoint::ResultConsume,
    ];
    let actions = [
        FaultAction::HoldCommand,
        FaultAction::RefuseFence,
        FaultAction::Barrier,
        FaultAction::DropResult,
        FaultAction::DelayResult,
        FaultAction::DuplicateResult,
    ];
    // The pairs of `contracts/v1/fault-points.json`, read from the file,
    // and no other.
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../contracts/v1/fault-points.json"
    );
    let file: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(path).expect("the catalogue")).expect("JSON");
    let mut catalogue = Vec::new();
    for point in file["points"].as_array().expect("points") {
        let name = point["point"].as_str().expect("a name").to_owned();
        for action in point["actions"].as_array().expect("actions") {
            catalogue.push((name.clone(), action.as_str().expect("a name").to_owned()));
        }
    }
    let named: Vec<_> = points.iter().map(|point| point.name()).collect();
    for (point, _) in &catalogue {
        assert!(named.contains(&point.as_str()), "{point} is not realised");
    }
    let catalogue: Vec<(&str, &str)> = catalogue
        .iter()
        .map(|(point, action)| (point.as_str(), action.as_str()))
        .collect();
    let mut allowed = Vec::new();
    for point in points {
        for action in actions {
            let listed = catalogue.contains(&(point.name(), action.name()));
            assert_eq!(point.actions().contains(&action), listed);
            if listed {
                allowed.push(fault(point, action));
            } else {
                assert_eq!(
                    bench.store.arm(fault(point, action)),
                    Err(FaultRefused::ActionNotAllowed)
                );
            }
        }
    }
    assert_eq!(allowed.len(), catalogue.len());
    assert_eq!(
        bench.store.arm(FaultSpec {
            occurrence: 0,
            ..allowed[0]
        }),
        Err(FaultRefused::ZeroOccurrence)
    );
    // Each on a kind of its own, so that no two select one passage.
    let kinds = [
        WorkKind::AdmitPublication,
        WorkKind::ReleaseInbound,
        WorkKind::ResolveAndFence,
        WorkKind::OpenEpoch,
    ];
    for (index, spec) in allowed.iter().take(5).enumerate() {
        let spec = FaultSpec {
            kind: Some(kinds[index % kinds.len()]),
            ..*spec
        };
        assert_eq!(bench.store.arm(spec), Ok(FaultId(index)));
    }
    assert_eq!(bench.store.arm(allowed[5]), Err(FaultRefused::TableFull));
}

#[test]
fn a_plain_admission_leaves_its_evidence_in_order() {
    let mut bench = bench();
    let command = bench.admission(1, 7, b"payload", &[bench.b, bench.c]);
    let operation = Some(command.recipe.operation());
    let id = persist(&mut bench, 1, &command);
    assert_eq!(bench.store.eligible(), [StoreAction::EnterWriter(id)]);
    assert_eq!(enter(&mut bench, id), Performed::Executed);
    assert_eq!(bench.store.eligible(), [StoreAction::DeliverResult(id)]);
    let reply = delivered_reply(deliver(&mut bench, id));
    assert!(bench.store.eligible().is_empty());

    let answer = Answer::Operation {
        operation: command.recipe.operation(),
        reply,
    };
    assert_eq!(
        bench.store.take_events(),
        [
            StoreEvent::CommandSubmitted {
                id,
                kind: WorkKind::AdmitPublication,
                operation
            },
            StoreEvent::CommitStable { id, operation },
            StoreEvent::ResultReady { id, answer },
            StoreEvent::ResultDelivered { id, answer },
        ]
    );
    assert!(!bench.store.evidence_lost());
    assert_eq!(WorkKind::AdmitPublication.name(), "AdmitPublication");
    assert_eq!(WorkKind::ReleaseInbound.name(), "ReleaseInbound");
}

#[test]
fn a_held_command_does_not_enter_the_writer_until_it_is_released() {
    let mut bench = bench();
    let hold = arm(
        &mut bench,
        FaultPoint::CommandTransit,
        FaultAction::HoldCommand,
    );
    let command = bench.admission(1, 7, b"payload", &[bench.b, bench.c]);
    let id = persist(&mut bench, 1, &command);
    hit(
        &bench.store.take_events(),
        hold,
        "store.command.transit",
        "hold_command",
        id,
    );
    assert_eq!(
        bench.store.perform(StoreAction::EnterWriter(id)),
        Err(ActionRefused::NotEligible)
    );
    assert!(bench.store.store().snapshot().operations.is_empty());

    // A later command is not held: the occurrence was the first one.
    let other = bench.admission(2, 8, b"payload", &[bench.b]);
    let passing = persist(&mut bench, 2, &other);
    assert_eq!(
        bench.store.eligible(),
        [
            StoreAction::ReleaseCommand(id),
            StoreAction::EnterWriter(passing)
        ]
    );
    assert_eq!(enter(&mut bench, passing), Performed::Executed);

    assert_eq!(
        bench.store.perform(StoreAction::ReleaseCommand(id)),
        Ok(Performed::Released)
    );
    assert_eq!(enter(&mut bench, id), Performed::Executed);
    assert_eq!(bench.store.store().snapshot().publications.len(), 2);
    assert_eq!(bench.store.hits(hold), 1);
}

#[test]
fn a_barrier_after_the_commit_stops_before_any_result_exists() {
    let mut bench = bench();
    let barrier = arm(&mut bench, FaultPoint::CommitApplied, FaultAction::Barrier);
    let command = bench.admission(1, 7, b"payload", &[bench.b, bench.c]);
    let id = persist(&mut bench, 1, &command);
    assert_eq!(
        enter(&mut bench, id),
        Performed::Halted(FaultPoint::CommitApplied)
    );
    assert_eq!(bench.store.halted_at(), Some(FaultPoint::CommitApplied));

    // Committed, and no result anywhere.
    assert_eq!(bench.store.store().snapshot().publications.len(), 1);
    let events = bench.store.take_events();
    hit(&events, barrier, "store.commit.applied", "barrier", id);
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, StoreEvent::ResultReady { .. }))
    );

    // Stopped means stopped: no action, no new command.
    assert!(bench.store.eligible().is_empty());
    assert_eq!(
        bench.store.perform(StoreAction::DeliverResult(id)),
        Err(ActionRefused::Halted)
    );
    let other = bench.admission(2, 8, b"payload", &[bench.b]);
    let refused = persist_effect(&bench, 2, &other);
    assert_eq!(
        bench.store.try_submit(persist_effect(&bench, 2, &other)),
        SubmitResult::NotSubmitted {
            effect: refused,
            reason: SubmitBlock::Closed
        }
    );

    // The supervisor crashes the process there: the commit survives.
    let crashed = bench.store.crash();
    assert_eq!(crashed.store.snapshot().publications.len(), 1);
    assert!(!crashed.evidence_lost);
}

#[test]
fn a_barrier_that_is_resumed_goes_on_to_the_result() {
    let mut bench = bench();
    arm(&mut bench, FaultPoint::CommitApplied, FaultAction::Barrier);
    let command = bench.admission(1, 7, b"payload", &[bench.b, bench.c]);
    let id = persist(&mut bench, 1, &command);
    assert_eq!(
        enter(&mut bench, id),
        Performed::Halted(FaultPoint::CommitApplied)
    );
    assert_eq!(bench.store.resume(), Some(FaultPoint::CommitApplied));
    assert_eq!(bench.store.resume(), None);
    assert!(matches!(
        delivered_reply(deliver(&mut bench, id)),
        StoreReply::Outcome(StoreOutcome::Committed(_))
    ));
}

#[test]
fn a_dropped_result_never_reaches_the_core_and_the_commit_is_reconciled() {
    let mut bench = bench();
    // The fault of scenario P1-02, matched on the client's packet.
    let drop = bench
        .store
        .arm(FaultSpec {
            point: FaultPoint::ResultReady,
            action: FaultAction::DropResult,
            kind: Some(WorkKind::AdmitPublication),
            source: Some((bench.producer, InboundPacketId::new(7).expect("non-zero"))),
            occurrence: 1,
        })
        .expect("allowed");
    let command = bench.admission(1, 7, b"payload", &[bench.b, bench.c]);
    let operation = command.recipe.operation();
    let id = persist(&mut bench, 1, &command);
    assert_eq!(enter(&mut bench, id), Performed::Executed);
    hit(
        &bench.store.take_events(),
        drop,
        "store.result.ready",
        "drop_result",
        id,
    );
    assert!(bench.store.eligible().is_empty(), "nothing to deliver");
    assert_eq!(bench.store.store().snapshot().publications.len(), 1);
    // Nothing answers the lost result's effect any more.
    let WorkId::Effect(lost) = id else {
        panic!("an effect");
    };
    assert_eq!(
        bench.store.present_result(
            lost,
            operation,
            StoreReply::Outcome(StoreOutcome::PendingOrUnknown)
        ),
        Ok(Performed::Uncorrelated)
    );

    // The owner reconciles by `ResolveAndFence`, under a new effect.
    let resolve = effect_id(&bench, 2);
    assert_eq!(
        bench.store.try_submit(OwnedEffect::new(
            resolve,
            Effect::ResolveAndFence { operation }
        )),
        SubmitResult::Accepted
    );
    let resolve = WorkId::Effect(resolve);
    assert_eq!(enter(&mut bench, resolve), Performed::Executed);
    assert!(matches!(
        delivered_reply(deliver(&mut bench, resolve)),
        StoreReply::Outcome(StoreOutcome::Committed(_))
    ));
    assert_eq!(bench.store.store().snapshot().publications.len(), 1);
}

#[test]
fn a_delayed_result_waits_for_its_release() {
    let mut bench = bench();
    let delay = arm(
        &mut bench,
        FaultPoint::ResultReady,
        FaultAction::DelayResult,
    );
    let command = bench.admission(1, 7, b"payload", &[bench.b, bench.c]);
    let id = persist(&mut bench, 1, &command);
    assert_eq!(enter(&mut bench, id), Performed::Executed);
    hit(
        &bench.store.take_events(),
        delay,
        "store.result.ready",
        "delay_result",
        id,
    );
    assert_eq!(bench.store.eligible(), [StoreAction::ReleaseResult(id)]);
    assert_eq!(
        bench.store.perform(StoreAction::DeliverResult(id)),
        Err(ActionRefused::NotEligible)
    );
    assert_eq!(
        bench.store.perform(StoreAction::ReleaseResult(id)),
        Ok(Performed::Released)
    );
    assert!(matches!(
        delivered_reply(deliver(&mut bench, id)),
        StoreReply::Outcome(StoreOutcome::Committed(_))
    ));
}

#[test]
fn a_duplicated_result_is_handed_over_once() {
    let mut bench = bench();
    let duplicate = arm(
        &mut bench,
        FaultPoint::ResultReady,
        FaultAction::DuplicateResult,
    );
    let command = bench.admission(1, 7, b"payload", &[bench.b, bench.c]);
    let operation = Some(command.recipe.operation());
    let id = persist(&mut bench, 1, &command);
    assert_eq!(enter(&mut bench, id), Performed::Executed);
    hit(
        &bench.store.take_events(),
        duplicate,
        "store.result.ready",
        "duplicate_result",
        id,
    );
    assert!(matches!(
        deliver(&mut bench, id),
        Performed::Delivered { barrier: false, .. }
    ));
    // The second copy answers nothing in flight any more.
    assert_eq!(bench.store.eligible(), [StoreAction::DeliverResult(id)]);
    assert_eq!(deliver(&mut bench, id), Performed::Uncorrelated);
    assert!(bench.store.eligible().is_empty());
    assert_eq!(
        bench.store.take_events().last(),
        Some(&StoreEvent::UncorrelatedResult {
            id,
            operation,
            reason: Uncorrelated::NoWorkInFlight
        })
    );
}

#[test]
fn a_barrier_at_consumption_stops_once_the_result_is_handed_over() {
    let mut bench = bench();
    let barrier = arm(&mut bench, FaultPoint::ResultConsume, FaultAction::Barrier);
    let command = bench.admission(1, 7, b"payload", &[bench.b, bench.c]);
    let id = persist(&mut bench, 1, &command);
    assert_eq!(enter(&mut bench, id), Performed::Executed);
    assert!(matches!(
        deliver(&mut bench, id),
        Performed::Delivered { barrier: true, .. }
    ));
    assert_eq!(bench.store.halted_at(), Some(FaultPoint::ResultConsume));
    hit(
        &bench.store.take_events(),
        barrier,
        "store.result.consume",
        "barrier",
        id,
    );
    assert_eq!(bench.store.resume(), Some(FaultPoint::ResultConsume));
    assert_eq!(bench.store.halted_at(), None);
}

#[test]
fn a_fault_counts_only_the_passages_it_matches() {
    let mut bench = bench_with(AdapterLimits {
        operations_in_flight: 4,
        result_slots: 8,
        ..LIMITS
    });
    let packet = |raw| InboundPacketId::new(raw).expect("non-zero");
    let second_admission = bench
        .store
        .arm(FaultSpec {
            point: FaultPoint::CommandTransit,
            action: FaultAction::HoldCommand,
            kind: Some(WorkKind::AdmitPublication),
            source: None,
            occurrence: 2,
        })
        .expect("allowed");
    let release_of_7 = bench
        .store
        .arm(FaultSpec {
            point: FaultPoint::CommandTransit,
            action: FaultAction::HoldCommand,
            kind: Some(WorkKind::ReleaseInbound),
            source: Some((bench.producer, packet(7))),
            occurrence: 1,
        })
        .expect("allowed");
    let never = bench
        .store
        .arm(FaultSpec {
            point: FaultPoint::CommandTransit,
            action: FaultAction::HoldCommand,
            kind: Some(WorkKind::ReleaseInbound),
            source: Some((SessionId::new(99), packet(1))),
            occurrence: 1,
        })
        .expect("allowed");

    let first = bench.admission(1, 7, b"payload", &[bench.b]);
    let release = bench.release(2, 7, 1);
    let second = bench.admission(3, 8, b"payload", &[bench.b]);
    let third = bench.admission(4, 9, b"payload", &[bench.b]);
    let ids = [
        persist(&mut bench, 1, &first),
        persist(&mut bench, 2, &release),
        persist(&mut bench, 3, &second),
        persist(&mut bench, 4, &third),
    ];
    assert_eq!(
        bench.store.eligible(),
        [
            StoreAction::EnterWriter(ids[0]),
            StoreAction::ReleaseCommand(ids[1]),
            StoreAction::ReleaseCommand(ids[2]),
            StoreAction::EnterWriter(ids[3]),
        ]
    );
    assert_eq!(bench.store.hits(second_admission), 1);
    assert_eq!(bench.store.hits(release_of_7), 1);
    // A fault that was not reached says so: the run is inconclusive
    // for whoever required it.
    assert_eq!(bench.store.hits(never), 0);
}

#[test]
fn the_adapter_refuses_past_its_bounds_and_hands_the_effect_back() {
    let mut bench = bench();
    let commands = [
        bench.admission(1, 7, b"payload", &[bench.b]),
        bench.admission(2, 8, b"payload", &[bench.b]),
        bench.admission(3, 9, b"payload", &[bench.b]),
        bench.admission(4, 10, b"payload", &[bench.b]),
    ];
    let first = persist(&mut bench, 1, &commands[0]);
    let second = persist(&mut bench, 2, &commands[1]);

    // Two commands are in flight: the bound.
    assert_eq!(
        bench
            .store
            .try_submit(persist_effect(&bench, 3, &commands[2])),
        SubmitResult::NotSubmitted {
            effect: persist_effect(&bench, 3, &commands[2]),
            reason: SubmitBlock::InFlightLimit
        }
    );
    // Results that are ready still hold their slot.
    assert_eq!(enter(&mut bench, first), Performed::Executed);
    assert_eq!(enter(&mut bench, second), Performed::Executed);
    persist(&mut bench, 3, &commands[2]);
    assert_eq!(
        bench
            .store
            .try_submit(persist_effect(&bench, 4, &commands[3])),
        SubmitResult::NotSubmitted {
            effect: persist_effect(&bench, 4, &commands[3]),
            reason: SubmitBlock::QueueFull
        }
    );
    // Delivering a result frees its slot.
    assert!(matches!(
        deliver(&mut bench, first),
        Performed::Delivered { .. }
    ));
    persist(&mut bench, 4, &commands[3]);

    // An effect that is not for the store, and an identity already in
    // flight, come back untouched.
    let foreign_id = effect_id(&bench, 9);
    let target = bench.b;
    let foreign = || {
        OwnedEffect::new(
            foreign_id,
            Effect::LoadWork {
                session: target,
                limit: 4,
            },
        )
    };
    assert_eq!(
        bench.store.try_submit(foreign()),
        SubmitResult::NotSubmitted {
            effect: foreign(),
            reason: SubmitBlock::Closed
        }
    );
    assert_eq!(
        bench
            .store
            .try_submit(persist_effect(&bench, 4, &commands[3])),
        SubmitResult::NotSubmitted {
            effect: persist_effect(&bench, 4, &commands[3]),
            reason: SubmitBlock::Closed
        }
    );
}

#[test]
fn a_result_that_answers_no_effect_in_flight_is_refused_and_recorded() {
    let mut bench = bench();
    let command = bench.admission(1, 7, b"payload", &[bench.b, bench.c]);
    let operation = command.recipe.operation();
    let committed = StoreReply::Outcome(StoreOutcome::Committed(
        mqttiumd_contract::outcome::CommitFacts::InboundReleased {
            exchange: mqttiumd_contract::ids::ExchangeId {
                session: bench.producer,
                generation: mqttiumd_contract::ids::ExchangeGeneration::new(1),
            },
        },
    ));

    // Nothing is in flight.
    let stray = effect_id(&bench, 5);
    assert_eq!(
        bench.store.present_result(stray, operation, committed),
        Ok(Performed::Uncorrelated)
    );
    assert_eq!(
        bench.store.take_events(),
        [StoreEvent::UncorrelatedResult {
            id: WorkId::Effect(stray),
            operation: Some(operation),
            reason: Uncorrelated::NoWorkInFlight
        }]
    );

    // An effect is in flight, for another operation than the result
    // names: the result does not answer it.
    let id = persist(&mut bench, 1, &command);
    let WorkId::Effect(in_flight) = id else {
        panic!("an effect");
    };
    let other = operation_id(bench.epoch, 2);
    assert_eq!(
        bench.store.present_result(in_flight, other, committed),
        Ok(Performed::Uncorrelated)
    );
    // The real result is still expected and still delivered.
    assert_eq!(enter(&mut bench, id), Performed::Executed);
    assert!(matches!(
        deliver(&mut bench, id),
        Performed::Delivered { .. }
    ));
    // Once delivered, the same result presented again answers nothing.
    assert_eq!(
        bench.store.present_result(in_flight, operation, committed),
        Ok(Performed::Uncorrelated)
    );
}

/// DEM-MU-03: the identity the bench remembered before the crash can
/// only come back as a result that answers nothing, and is refused.
#[test]
fn an_identity_remembered_by_the_bench_is_refused_after_a_restart() {
    let mut bench = bench();
    arm(&mut bench, FaultPoint::ResultReady, FaultAction::DropResult);
    let command = bench.admission(1, 7, b"payload", &[bench.b, bench.c]);
    let id = persist(&mut bench, 1, &command);
    assert_eq!(enter(&mut bench, id), Performed::Executed);
    let WorkId::Effect(remembered_effect) = id else {
        panic!("an effect");
    };
    // What a test reading the trace could have kept.
    let remembered = command.recipe.operation();

    let crashed = bench.store.crash();
    let mut restarted = SimStore::new(crashed.store, LIMITS);
    let before = restarted.store().snapshot();
    assert_eq!(
        restarted.present_result(
            remembered_effect,
            remembered,
            StoreReply::Outcome(StoreOutcome::RejectedCertain(RejectReason::Fenced)),
        ),
        Ok(Performed::Uncorrelated)
    );
    assert_eq!(
        restarted.take_events(),
        [StoreEvent::UncorrelatedResult {
            id,
            operation: Some(remembered),
            reason: Uncorrelated::NoWorkInFlight
        }]
    );
    assert_eq!(restarted.store().snapshot(), before);
}

#[test]
fn a_crash_loses_what_is_volatile_and_keeps_what_is_durable() {
    let mut bench = bench();
    arm(
        &mut bench,
        FaultPoint::CommandTransit,
        FaultAction::HoldCommand,
    );
    let held_command = bench.admission(1, 7, b"payload", &[bench.b]);
    let ready_command = bench.admission(2, 8, b"payload", &[bench.b, bench.c]);
    persist(&mut bench, 1, &held_command);
    let ready = persist(&mut bench, 2, &ready_command);
    assert_eq!(enter(&mut bench, ready), Performed::Executed);
    let durable = bench.store.store().snapshot();
    assert_eq!(durable.publications.len(), 1);

    let crashed = bench.store.crash();
    // The evidence recorded before the crash belongs to the collector.
    assert!(
        crashed
            .events
            .iter()
            .any(|event| matches!(event, StoreEvent::CommitStable { id, .. } if *id == ready))
    );
    let mut restarted = SimStore::new(MemoryStore::reopen(crashed.store.snapshot()), LIMITS);
    assert_eq!(restarted.store().snapshot(), durable);
    // No command in transit, no result, no fault, no barrier.
    assert!(restarted.eligible().is_empty());
    assert_eq!(restarted.halted_at(), None);
    assert!(restarted.take_events().is_empty());
    // The held command never ran, and its epoch is closed by the next
    // opening: it is not entered again.
    open(restarted.store_mut());
    assert_eq!(
        restarted.store_mut().apply(&held_command).reply,
        StoreReply::Outcome(StoreOutcome::RejectedCertain(RejectReason::EpochClosed))
    );
}

#[test]
fn evidence_that_cannot_be_kept_is_reported_as_lost() {
    let mut bench = bench_with(AdapterLimits {
        events: 2,
        ..LIMITS
    });
    let command = bench.admission(1, 7, b"payload", &[bench.b, bench.c]);
    let id = persist(&mut bench, 1, &command);
    assert!(!bench.store.evidence_lost());
    assert_eq!(enter(&mut bench, id), Performed::Executed);
    assert!(bench.store.evidence_lost());
    assert_eq!(bench.store.take_events().len(), 2);
}

#[test]
fn recovery_pages_read_through_the_adapter_leave_their_evidence() {
    let mut bench = bench();
    let command = bench.admission(1, 7, b"payload", &[bench.b, bench.c]);
    assert!(bench.store.store_mut().apply(&command).committed_now);
    open(bench.store.store_mut());
    bench.store.take_events();

    let whole = bench.store.store().recover(None, 64).expect("one page");
    let mut cursor = None;
    let mut read = Vec::new();
    let mut pages = 0u64;
    loop {
        let page = bench.store.recover(cursor, 4).expect("a page");
        let last = page.next.is_none();
        assert_eq!(
            bench.store.take_events(),
            [StoreEvent::RecoveryPageRead {
                run_epoch: bench.store.store().ledger().last_opened(),
                page: pages,
                last,
                facts: page.facts.as_slice().to_vec(),
            }]
        );
        read.extend_from_slice(page.facts.as_slice());
        pages += 1;
        match page.next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    assert_eq!(read, whole.facts.as_slice());
    assert!(pages > 1, "the inventory fits one page: nothing was paged");

    // A new inventory starts again at page zero, and a refused read
    // leaves no page behind.
    let first = bench.store.recover(None, 4).expect("a page");
    assert!(matches!(
        bench.store.take_events().as_slice(),
        [StoreEvent::RecoveryPageRead { page: 0, .. }]
    ));
    assert!(bench.store.recover(first.next, 0).is_err());
    assert!(bench.store.take_events().is_empty());
}

fn resolve(bench: &mut Bench, sequence: u64, operation: OperationId) -> WorkId {
    let effect = OwnedEffect::new(
        effect_id(bench, sequence),
        Effect::ResolveAndFence { operation },
    );
    let id = WorkId::Effect(effect.id());
    assert_eq!(bench.store.try_submit(effect), SubmitResult::Accepted);
    id
}

/// A duplication leaves a second copy of a result. The work it answered
/// is done; when the owner submits again under the same identity, the
/// copy answers nothing: the dispatcher hands over the result of the
/// new acceptance and refuses the old one.
#[test]
fn a_left_over_copy_does_not_answer_work_submitted_again() {
    let mut bench = bench();
    arm(
        &mut bench,
        FaultPoint::ResultReady,
        FaultAction::DuplicateResult,
    );
    let operation = operation_id(bench.epoch, 1);
    bench.store.store_mut().set_durability(Durability::Failing);
    let id = resolve(&mut bench, 1, operation);
    assert_eq!(enter(&mut bench, id), Performed::Executed);
    let unknown = StoreReply::Outcome(StoreOutcome::PendingOrUnknown);
    assert_eq!(delivered_reply(deliver(&mut bench, id)), unknown);

    // The owner submits again under the same effect, and the medium
    // writes now. The copy of the first result is still ready.
    bench
        .store
        .store_mut()
        .set_durability(Durability::Available);
    assert_eq!(resolve(&mut bench, 1, operation), id);
    assert_eq!(enter(&mut bench, id), Performed::Executed);
    let _ = bench.store.take_events();
    assert_eq!(deliver(&mut bench, id), Performed::Uncorrelated);
    let fenced = StoreReply::Outcome(StoreOutcome::RejectedCertain(RejectReason::Fenced));
    assert_eq!(delivered_reply(deliver(&mut bench, id)), fenced);
    assert_eq!(
        bench.store.take_events(),
        [
            StoreEvent::UncorrelatedResult {
                id,
                operation: Some(operation),
                reason: Uncorrelated::NoWorkInFlight
            },
            StoreEvent::ResultDelivered {
                id,
                answer: Answer::Operation {
                    operation,
                    reply: fenced
                }
            },
        ]
    );
}

/// Only one fault fires at a passage. Two faults that may select the
/// same passage at one point are refused when the second is armed:
/// otherwise it would count that passage without firing, and never be
/// reached.
#[test]
fn two_faults_that_may_select_one_passage_are_refused() {
    let mut bench = fresh_bench();
    let packet = |raw| InboundPacketId::new(raw).expect("non-zero");
    let first = bench
        .store
        .arm(fault(FaultPoint::ResultReady, FaultAction::DelayResult))
        .expect("allowed");
    let overlapping = [
        fault(FaultPoint::ResultReady, FaultAction::DropResult),
        FaultSpec {
            kind: Some(WorkKind::AdmitPublication),
            occurrence: 2,
            ..fault(FaultPoint::ResultReady, FaultAction::DuplicateResult)
        },
        FaultSpec {
            source: Some((bench.producer, packet(7))),
            ..fault(FaultPoint::ResultReady, FaultAction::DropResult)
        },
        // Another occurrence, but counted over the passages of one
        // source only: which fault acts would depend on the store.
        FaultSpec {
            source: Some((bench.producer, packet(7))),
            occurrence: 2,
            ..fault(FaultPoint::ResultReady, FaultAction::DropResult)
        },
    ];
    for spec in overlapping {
        assert_eq!(
            bench.store.arm(spec),
            Err(FaultRefused::Overlaps(first)),
            "{spec:?}"
        );
    }
    // Another point, or filters that no passage satisfies together, are
    // armed.
    let mut bench = fresh_bench();
    let admissions = bench
        .store
        .arm(FaultSpec {
            kind: Some(WorkKind::AdmitPublication),
            ..fault(FaultPoint::ResultReady, FaultAction::DelayResult)
        })
        .expect("allowed");
    let releases = FaultSpec {
        kind: Some(WorkKind::ReleaseInbound),
        ..fault(FaultPoint::ResultReady, FaultAction::DropResult)
    };
    assert_eq!(bench.store.arm(releases), Ok(FaultId(1)));
    let on_7 = FaultSpec {
        source: Some((bench.producer, packet(7))),
        kind: Some(WorkKind::AdmitPublication),
        ..fault(FaultPoint::CommandTransit, FaultAction::HoldCommand)
    };
    let on_8 = FaultSpec {
        source: Some((bench.producer, packet(8))),
        ..on_7
    };
    assert_eq!(bench.store.arm(on_7), Ok(FaultId(2)));
    assert_eq!(bench.store.arm(on_8), Ok(FaultId(3)));
    assert_eq!(admissions, FaultId(0));
}

/// A fence made durable, and the receipt of a rejection, are committed
/// transactions with no result yet: the point after the commit is
/// passed, and a crash there keeps them and loses the result.
#[test]
fn a_fence_and_a_rejection_receipt_pass_the_point_after_their_commit() {
    // The fence.
    let mut bench = fresh_bench();
    let barrier = bench
        .store
        .arm(FaultSpec {
            kind: Some(WorkKind::ResolveAndFence),
            ..fault(FaultPoint::CommitApplied, FaultAction::Barrier)
        })
        .expect("allowed");
    let operation = operation_id(bench.epoch, 1);
    let id = resolve(&mut bench, 1, operation);
    assert_eq!(
        enter(&mut bench, id),
        Performed::Halted(FaultPoint::CommitApplied)
    );
    assert_eq!(bench.store.hits(barrier), 1);
    hit(
        &bench.store.take_events(),
        barrier,
        "store.commit.applied",
        "barrier",
        id,
    );
    let crashed = bench.store.crash();
    assert!(crashed.store.snapshot().fences.contains(&operation));
    assert!(
        !crashed
            .events
            .iter()
            .any(|event| matches!(event, StoreEvent::ResultReady { .. }))
    );

    // The receipt of a rejection: a command for an epoch not open yet.
    let mut bench = fresh_bench();
    let barrier = bench
        .store
        .arm(FaultSpec {
            kind: Some(WorkKind::AdmitPublication),
            ..fault(FaultPoint::CommitApplied, FaultAction::Barrier)
        })
        .expect("allowed");
    let ahead = bench.epoch.checked_next().expect("not the last epoch");
    let command = admission_in(
        ahead,
        bench.route_revision,
        bench.producer,
        1,
        7,
        b"payload",
        &[bench.b],
    );
    let id = persist(&mut bench, 1, &command);
    assert_eq!(
        enter(&mut bench, id),
        Performed::Halted(FaultPoint::CommitApplied)
    );
    assert_eq!(bench.store.hits(barrier), 1);
    // Nothing was committed as an effect: no commit is recorded.
    assert!(
        !bench
            .store
            .take_events()
            .iter()
            .any(|event| matches!(event, StoreEvent::CommitStable { .. }))
    );
    assert!(
        bench
            .store
            .store()
            .snapshot()
            .operations
            .contains_key(&command.recipe.operation())
    );

    // A repetition writes nothing and does not pass the point.
    let mut bench = fresh_bench();
    let committed = bench.admission(1, 7, b"payload", &[bench.b]);
    let first = persist(&mut bench, 1, &committed);
    assert_eq!(enter(&mut bench, first), Performed::Executed);
    let _ = deliver(&mut bench, first);
    let barrier = arm(&mut bench, FaultPoint::CommitApplied, FaultAction::Barrier);
    let again = persist(&mut bench, 2, &committed);
    assert_eq!(enter(&mut bench, again), Performed::Executed);
    assert_eq!(bench.store.hits(barrier), 0);
}

/// While a barrier holds, nothing reaches the core: not a result of the
/// pipeline, and not a result presented from outside it.
#[test]
fn nothing_is_handed_over_while_a_barrier_holds() {
    let mut bench = bench();
    arm(&mut bench, FaultPoint::ResultConsume, FaultAction::Barrier);
    let first = bench.admission(1, 7, b"payload", &[bench.b]);
    let second = bench.admission(2, 8, b"payload", &[bench.b]);
    let first_id = persist(&mut bench, 1, &first);
    let second_id = persist(&mut bench, 2, &second);
    assert_eq!(enter(&mut bench, first_id), Performed::Executed);
    assert_eq!(enter(&mut bench, second_id), Performed::Executed);
    assert!(matches!(
        deliver(&mut bench, first_id),
        Performed::Delivered { barrier: true, .. }
    ));
    let WorkId::Effect(effect) = second_id else {
        panic!("an effect");
    };
    let committed = StoreReply::Outcome(StoreOutcome::Committed(
        mqttiumd_contract::outcome::CommitFacts::PublicationAdmitted {
            publication: publication_id(bench.epoch, 2),
            exchange: mqttiumd_contract::ids::ExchangeId {
                session: bench.producer,
                generation: mqttiumd_contract::ids::ExchangeGeneration::new(2),
            },
        },
    ));
    let _ = bench.store.take_events();
    assert_eq!(
        bench
            .store
            .present_result(effect, second.recipe.operation(), committed),
        Err(ActionRefused::Halted)
    );
    assert!(bench.store.take_events().is_empty());
    // After the barrier the real result is still expected and delivered.
    assert_eq!(bench.store.resume(), Some(FaultPoint::ResultConsume));
    assert_eq!(delivered_reply(deliver(&mut bench, second_id)), committed);
}

fn fresh_bench() -> Bench {
    bench_with(LIMITS)
}

/// The bench with `result_slots` results owed at most, and more room
/// for commands in flight, so that the result budget is the bound.
fn bench_with_results(result_slots: usize) -> Bench {
    bench_with(AdapterLimits {
        operations_in_flight: result_slots + 2,
        result_slots,
        ..LIMITS
    })
}

fn queue_full(bench: &mut Bench, sequence: u64, command: &Command) {
    assert_eq!(
        bench
            .store
            .try_submit(persist_effect(bench, sequence, command)),
        SubmitResult::NotSubmitted {
            effect: persist_effect(bench, sequence, command),
            reason: SubmitBlock::QueueFull
        }
    );
}

/// The counterexample of the audit (F6): with one result slot, a
/// duplicated result would make two results owed. Its fault cannot be
/// armed, and a run then never owes two results.
#[test]
fn a_duplicate_that_cannot_fit_is_not_armed() {
    let mut bench = bench_with_results(1);
    assert_eq!(
        bench
            .store
            .arm(fault(FaultPoint::ResultReady, FaultAction::DuplicateResult)),
        Err(FaultRefused::NoResultSlot)
    );
    let command = bench.admission(1, 7, b"payload", &[bench.b]);
    let id = persist(&mut bench, 1, &command);
    assert_eq!(bench.store.results_owed(), 1);
    assert_eq!(enter(&mut bench, id), Performed::Executed);
    assert_eq!(bench.store.results_owed(), 1);
    assert!(matches!(
        deliver(&mut bench, id),
        Performed::Delivered { .. }
    ));
    assert_eq!(bench.store.results_owed(), 0);
    assert!(bench.store.eligible().is_empty());
}

/// With two result slots the duplicate fits: its slot is reserved from
/// arming until both copies are delivered, and no more than two results
/// are ever owed. Then both slots are free again.
#[test]
fn a_duplicate_holds_the_slot_it_reserved_until_it_is_delivered() {
    let mut bench = bench_with_results(2);
    let duplicate = arm(
        &mut bench,
        FaultPoint::ResultReady,
        FaultAction::DuplicateResult,
    );
    let commands: Vec<_> = (1..=4)
        .map(|sequence| bench.admission(sequence, 7 + sequence as u16, b"p", &[bench.b]))
        .collect();
    let first = persist(&mut bench, 1, &commands[0]);
    // The reserved slot is taken: no second command.
    queue_full(&mut bench, 2, &commands[1]);
    assert_eq!(enter(&mut bench, first), Performed::Executed);
    assert_eq!(bench.store.hits(duplicate), 1);
    assert_eq!(bench.store.results_owed(), 2);
    queue_full(&mut bench, 2, &commands[1]);
    assert!(matches!(
        deliver(&mut bench, first),
        Performed::Delivered { .. }
    ));
    // One copy is still owed, in the slot the fault reserved.
    assert_eq!(bench.store.results_owed(), 1);
    assert_eq!(deliver(&mut bench, first), Performed::Uncorrelated);
    assert_eq!(bench.store.results_owed(), 0);
    // The fault fired and its copies were delivered: both slots are free.
    let second = persist(&mut bench, 2, &commands[1]);
    let third = persist(&mut bench, 3, &commands[2]);
    queue_full(&mut bench, 4, &commands[3]);
    for id in [second, third] {
        assert_eq!(enter(&mut bench, id), Performed::Executed);
        assert!(bench.store.results_owed() <= 2);
    }
}

/// A crash loses the armed faults and what they reserved: the adapter
/// that starts again owes nothing and has every slot.
#[test]
fn a_crash_gives_back_the_slot_a_duplicate_reserved() {
    let mut bench = bench_with_results(2);
    arm(
        &mut bench,
        FaultPoint::ResultReady,
        FaultAction::DuplicateResult,
    );
    let command = bench.admission(1, 7, b"p", &[bench.b]);
    persist(&mut bench, 1, &command);
    let other = bench.admission(2, 8, b"p", &[bench.b]);
    queue_full(&mut bench, 2, &other);
    let mut bench = bench.wrap(|adapter| {
        SimStore::new(
            adapter.crash().store,
            AdapterLimits {
                operations_in_flight: 4,
                result_slots: 2,
                ..LIMITS
            },
        )
    });
    assert_eq!(bench.store.results_owed(), 0);
    persist(&mut bench, 2, &other);
    let last = bench.admission(3, 9, b"p", &[bench.b]);
    persist(&mut bench, 3, &last);
}

/// Only a duplicate makes a result more than its command did: any other
/// armed fault reserves nothing, and a command takes the single slot.
#[test]
fn only_a_duplicate_reserves_a_result_slot() {
    let mut bench = bench_with_results(1);
    arm(
        &mut bench,
        FaultPoint::CommandTransit,
        FaultAction::HoldCommand,
    );
    arm(
        &mut bench,
        FaultPoint::ResultReady,
        FaultAction::DelayResult,
    );
    let command = bench.admission(1, 7, b"p", &[bench.b]);
    persist(&mut bench, 1, &command);
    assert_eq!(bench.store.results_owed(), 1);
}

fn on_resolutions(point: FaultPoint, action: FaultAction, occurrence: u64) -> FaultSpec {
    FaultSpec {
        kind: Some(WorkKind::ResolveAndFence),
        occurrence,
        ..fault(point, action)
    }
}

fn hits_of(events: &[StoreEvent], fault: FaultId) -> Vec<(u64, WorkId)> {
    events
        .iter()
        .filter_map(|event| match event {
            StoreEvent::FaultHit {
                fault: reached,
                occurrence,
                id,
                ..
            } if *reached == fault => Some((*occurrence, *id)),
            _ => None,
        })
        .collect()
}

/// Faults on one point, for one kind, at different occurrences select
/// different passages (`contracts/README.md`, "One fault per item"):
/// they are armed, and each fires at its own passage.
#[test]
fn faults_of_different_occurrences_each_fire_at_their_passage() {
    let mut bench = bench_with(AdapterLimits {
        operations_in_flight: 4,
        result_slots: 8,
        ..LIMITS
    });
    let faults: Vec<FaultId> = (1..=3)
        .map(|occurrence| {
            bench
                .store
                .arm(on_resolutions(
                    FaultPoint::ResultReady,
                    FaultAction::DelayResult,
                    occurrence,
                ))
                .expect("another occurrence")
        })
        .collect();
    let mut ids = Vec::new();
    for sequence in 1..=3 {
        let operation = operation_id(bench.epoch, 10 + sequence);
        let id = resolve(&mut bench, sequence, operation);
        assert_eq!(enter(&mut bench, id), Performed::Executed);
        ids.push(id);
    }
    let events = bench.store.take_events();
    for (index, fault) in faults.iter().enumerate() {
        let occurrence = index as u64 + 1;
        assert_eq!(hits_of(&events, *fault), [(occurrence, ids[index])]);
    }
    // Each result is delayed: none is handed over before it is released.
    for id in ids {
        assert!(
            bench
                .store
                .eligible()
                .contains(&StoreAction::ReleaseResult(id))
        );
    }
}

/// A fence held at its point passes it again when it enters the writer
/// again. That second passage is the same item: it is not counted, and
/// it takes no occurrence of another fault. The next resolution is the
/// second passage, and the fault of occurrence 2 fires there.
#[test]
fn a_held_fence_passing_again_takes_no_occurrence_of_another_fault() {
    let mut bench = fresh_bench();
    let hold = bench
        .store
        .arm(on_resolutions(
            FaultPoint::FenceTransit,
            FaultAction::HoldCommand,
            1,
        ))
        .expect("allowed");
    let refuse = bench
        .store
        .arm(on_resolutions(
            FaultPoint::FenceTransit,
            FaultAction::RefuseFence,
            2,
        ))
        .expect("another occurrence");
    let absent = operation_id(bench.epoch, 21);
    let first = resolve(&mut bench, 1, absent);
    assert_eq!(
        enter(&mut bench, first),
        Performed::Held(FaultPoint::FenceTransit)
    );
    assert_eq!(
        bench.store.perform(StoreAction::ReleaseCommand(first)),
        Ok(Performed::Released)
    );
    assert_eq!(enter(&mut bench, first), Performed::Executed);
    assert_eq!(
        delivered_reply(deliver(&mut bench, first)),
        StoreReply::Outcome(StoreOutcome::RejectedCertain(RejectReason::Fenced))
    );
    assert_eq!(bench.store.hits(refuse), 0);

    let absent = operation_id(bench.epoch, 22);
    let second = resolve(&mut bench, 2, absent);
    assert_eq!(enter(&mut bench, second), Performed::Executed);
    assert_eq!(
        delivered_reply(deliver(&mut bench, second)),
        StoreReply::Outcome(StoreOutcome::PendingOrUnknown)
    );
    let events = bench.store.take_events();
    assert_eq!(hits_of(&events, hold), [(1, first)]);
    assert_eq!(hits_of(&events, refuse), [(2, second)]);
}
