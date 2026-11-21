//! QABS: a command delayed after an absent read, interleaved with
//! `ResolveAndFence` (DEM-018), and what the store never does to a
//! terminal result (DEM-010).
//!
//! The bench keeps a command before it enters the writer, observes its
//! absence, then orders its delivery and the fence three ways.

use mqttiumd_contract::command::Command;
use mqttiumd_contract::effects::{Effect, EffectSink, Input, OwnedEffect, SubmitResult};
use mqttiumd_contract::ids::{EffectId, EffectSeq, LogicalOwnerId, OperationId, StoreIncarnation};
use mqttiumd_contract::outcome::{CommitFacts, Knowledge, RejectReason, StoreOutcome, StoreReply};
use mqttiumd_lab::store::adapter::{
    AdapterLimits, Delivery, FaultAction, FaultId, FaultPoint, FaultSpec, Performed, SimStore,
    StoreAction, StoreEvent, WorkId, WorkKind,
};
use mqttiumd_lab::store::conformance::{
    World, accounts_match_what_is_held, opening_command, operation_id,
};
use mqttiumd_lab::store::durable::{AdminCommand, Durability, DurableStore, Limits, MemoryStore};

type Bench = World<SimStore<MemoryStore>>;

fn memory(incarnation: StoreIncarnation, limits: Limits) -> MemoryStore {
    MemoryStore::create(incarnation, limits)
}

fn bench() -> Bench {
    World::new(&memory).wrap(|store| {
        SimStore::new(
            store,
            AdapterLimits {
                operations_in_flight: 4,
                result_slots: 8,
                faults: 4,
                events: 256,
            },
        )
    })
}

fn effect_id(bench: &Bench, sequence: u64) -> EffectId {
    EffectId {
        epoch: bench.epoch,
        owner: LogicalOwnerId::new(0),
        sequence: EffectSeq::new(sequence),
    }
}

fn submit(bench: &mut Bench, sequence: u64, effect: Effect) -> WorkId {
    let id = effect_id(bench, sequence);
    assert_eq!(
        bench.store.try_submit(OwnedEffect::new(id, effect)),
        SubmitResult::Accepted
    );
    WorkId::Effect(id)
}

fn persist(bench: &mut Bench, sequence: u64, command: &Command) -> WorkId {
    submit(
        bench,
        sequence,
        Effect::Persist {
            command: command.clone(),
        },
    )
}

fn resolve(bench: &mut Bench, sequence: u64, operation: OperationId) -> WorkId {
    submit(bench, sequence, Effect::ResolveAndFence { operation })
}

fn enter(bench: &mut Bench, id: WorkId) {
    assert_eq!(
        bench.store.perform(StoreAction::EnterWriter(id)),
        Ok(Performed::Executed)
    );
}

/// Delivers the result of `id` and returns the reply the core receives.
fn deliver(bench: &mut Bench, id: WorkId) -> StoreReply {
    match bench.store.perform(StoreAction::DeliverResult(id)) {
        Ok(Performed::Delivered {
            delivery: Delivery::Store(Input::StoreResult { effect, reply, .. }),
            barrier: false,
        }) => {
            assert_eq!(WorkId::Effect(effect), id);
            reply
        }
        other => panic!("no result delivered for {id:?}: {other:?}"),
    }
}

fn outcome(reply: StoreReply) -> StoreOutcome {
    match reply {
        StoreReply::Outcome(outcome) => outcome,
        StoreReply::Violation(violation) => panic!("violation {violation:?}"),
    }
}

/// Holds the admission before the writer and checks that a read finds
/// no trace of it. Returns the bench, the command and its identity in
/// transit.
fn held_command_and_absent_read() -> (Bench, Command, WorkId) {
    let mut bench = bench();
    let hold = bench
        .store
        .arm(FaultSpec {
            point: FaultPoint::CommandTransit,
            action: FaultAction::HoldCommand,
            kind: Some(WorkKind::AdmitPublication),
            source: None,
            occurrence: 1,
        })
        .expect("allowed");
    let command = bench.admission(1, 7, b"payload", &[bench.b, bench.c]);
    let held = persist(&mut bench, 1, &command);
    assert_eq!(bench.store.hits(hold), 1, "the injection was not reached");
    assert_eq!(
        bench.store.eligible(),
        [StoreAction::ReleaseCommand(held)],
        "the command is held before the writer"
    );
    // The absent read: no receipt, no publication, nothing.
    let read = bench
        .store
        .store()
        .snapshot()
        .expect("healthy durable snapshot");
    assert!(read.operations.is_empty());
    assert!(read.publications.is_empty());
    assert!(read.fences.is_empty());
    (bench, command, held)
}

fn release(bench: &mut Bench, held: WorkId) {
    assert_eq!(
        bench.store.perform(StoreAction::ReleaseCommand(held)),
        Ok(Performed::Released)
    );
}

/// The operations for which the writer made a fence durable.
fn fences(events: &[StoreEvent]) -> Vec<(WorkId, OperationId)> {
    events
        .iter()
        .filter_map(|event| match event {
            StoreEvent::FenceStable { id, operation } => Some((*id, *operation)),
            _ => None,
        })
        .collect()
}

fn commits(events: &[StoreEvent]) -> usize {
    events
        .iter()
        .filter(|event| matches!(event, StoreEvent::CommitStable { .. }))
        .count()
}

#[test]
fn qabs_a_the_command_commits_before_the_fence() {
    let (mut bench, command, held) = held_command_and_absent_read();
    let operation = command.recipe.operation();
    let fence = resolve(&mut bench, 2, operation);

    // The command is released and committed first, then the fence runs.
    release(&mut bench, held);
    enter(&mut bench, held);
    enter(&mut bench, fence);

    let resolved = outcome(deliver(&mut bench, fence));
    let StoreOutcome::Committed(facts) = resolved else {
        panic!("the resolution is {resolved:?}, not Committed");
    };
    assert!(matches!(facts, CommitFacts::PublicationAdmitted { .. }));
    assert_eq!(
        outcome(deliver(&mut bench, held)),
        StoreOutcome::Committed(facts)
    );

    // One effect, and only one.
    let state = bench
        .store
        .store()
        .snapshot()
        .expect("healthy durable snapshot");
    assert_eq!(state.publications.len(), 1);
    assert_eq!(state.obligations.len(), 2);
    assert!(state.fences.is_empty());
    let events = bench.store.take_events();
    assert_eq!(commits(&events), 1);
    assert_eq!(fences(&events), []);
}

#[test]
fn qabs_b_the_fence_commits_before_the_command_is_delivered() {
    let (mut bench, command, held) = held_command_and_absent_read();
    let operation = command.recipe.operation();
    let fence = resolve(&mut bench, 2, operation);

    // The fence runs while the command is still held.
    enter(&mut bench, fence);
    let fenced = StoreOutcome::RejectedCertain(RejectReason::Fenced);
    assert_eq!(outcome(deliver(&mut bench, fence)), fenced);
    let after_fence = bench
        .store
        .store()
        .snapshot()
        .expect("healthy durable snapshot");
    assert!(after_fence.fences.contains(&operation));

    // The held command is really delivered to the writer afterwards,
    // and is refused with no effect.
    release(&mut bench, held);
    enter(&mut bench, held);
    assert_eq!(outcome(deliver(&mut bench, held)), fenced);
    assert_eq!(
        bench
            .store
            .store()
            .snapshot()
            .expect("healthy durable snapshot"),
        after_fence
    );
    assert!(after_fence.publications.is_empty());
    assert!(after_fence.obligations.is_empty());
    let events = bench.store.take_events();
    assert_eq!(commits(&events), 0);
    // The fence was made durable once, by the resolution, and the
    // evidence of it precedes the result that announces the rejection.
    assert_eq!(fences(&events), [(fence, operation)]);
    let position =
        |wanted: &dyn Fn(&StoreEvent) -> bool| events.iter().position(wanted).expect("recorded");
    assert!(
        position(&|event| matches!(event, StoreEvent::FenceStable { .. }))
            < position(
                &|event| matches!(event, StoreEvent::ResultReady { id, .. } if *id == fence)
            )
    );
}

#[test]
fn qabs_c_no_fence_can_be_established_and_no_rejection_is_invented() {
    let (mut bench, command, held) = held_command_and_absent_read();
    let operation = command.recipe.operation();

    // The medium refuses writes: the fence cannot be made durable.
    bench.store.store_mut().set_durability(Durability::Failing);
    let fence = resolve(&mut bench, 2, operation);
    enter(&mut bench, fence);
    assert_eq!(
        outcome(deliver(&mut bench, fence)),
        StoreOutcome::PendingOrUnknown,
        "no certain rejection without a fence"
    );
    assert!(
        bench
            .store
            .store()
            .snapshot()
            .expect("healthy durable snapshot")
            .fences
            .is_empty()
    );
    assert_eq!(fences(&bench.store.take_events()), []);

    // Why it had to stay unknown: the command is still held, and once
    // the medium is back it commits.
    bench
        .store
        .store_mut()
        .set_durability(Durability::Available);
    release(&mut bench, held);
    enter(&mut bench, held);
    assert!(matches!(
        outcome(deliver(&mut bench, held)),
        StoreOutcome::Committed(_)
    ));
    assert_eq!(
        bench
            .store
            .store()
            .snapshot()
            .expect("healthy durable snapshot")
            .publications
            .len(),
        1
    );
}

#[test]
fn a_late_unknown_answer_never_lowers_a_terminal_result() {
    let mut bench = bench();
    bench
        .store
        .arm(FaultSpec {
            point: FaultPoint::ResultReady,
            action: FaultAction::DelayResult,
            kind: Some(WorkKind::AdmitPublication),
            source: None,
            occurrence: 1,
        })
        .expect("allowed");
    let command = bench.admission(1, 7, b"payload", &[bench.b, bench.c]);
    let operation = command.recipe.operation();

    // The first attempt meets a medium that refuses writes: its answer
    // is unknown, and it is delayed on its way back.
    bench.store.store_mut().set_durability(Durability::Failing);
    let first = persist(&mut bench, 1, &command);
    enter(&mut bench, first);
    bench
        .store
        .store_mut()
        .set_durability(Durability::Available);

    // Meanwhile the owner resolves and obtains a terminal result.
    let fence = resolve(&mut bench, 2, operation);
    enter(&mut bench, fence);
    let terminal = Knowledge::Unknown
        .observe(outcome(deliver(&mut bench, fence)))
        .expect("no conflict");
    assert_eq!(terminal, Knowledge::RejectedCertain(RejectReason::Fenced));

    // The old unknown answer arrives last.
    assert_eq!(
        bench.store.perform(StoreAction::ReleaseResult(first)),
        Ok(Performed::Released)
    );
    let late = outcome(deliver(&mut bench, first));
    assert_eq!(late, StoreOutcome::PendingOrUnknown);
    assert_eq!(terminal.observe(late), Ok(terminal));
}

#[derive(Clone, Copy, Debug)]
enum Step {
    Apply(usize),
    Resolve(usize),
    Fail,
    Restore,
    OpenEpoch,
    Collect,
    DeleteTarget,
    Release,
}

/// Whatever the order of commands, fences, epoch openings, collections
/// and write failures, the store never reports two different terminal
/// results for one operation, and a terminal result never becomes
/// unknown again: every answer merges into the owner's knowledge.
#[test]
fn the_store_never_contradicts_a_terminal_result() {
    let steps = [
        Step::Apply(0),
        Step::Apply(1),
        Step::Resolve(0),
        Step::Resolve(1),
        Step::Fail,
        Step::Restore,
        Step::OpenEpoch,
        Step::Collect,
        Step::DeleteTarget,
        Step::Release,
    ];
    const DEPTH: u32 = 5;
    let base = steps.len();
    let mut terminal_runs = 0u32;
    for run in 0..base.pow(DEPTH) {
        let mut code = run;
        let mut world = World::new(&memory);
        let first_epoch = world.epoch;
        // Two operations that compete for the same inbound slot: at
        // most one of them can ever commit.
        let commands = [
            world.admission(1, 7, b"payload", &[world.b, world.c]),
            world.admission(2, 7, b"payload", &[world.b]),
        ];
        let mut knowledge = [Knowledge::Unknown; 2];
        let mut released = false;
        let mut observe = |index: usize, reply: StoreOutcome| {
            let before = knowledge[index];
            let after = before
                .observe(reply)
                .unwrap_or_else(|conflict| panic!("contradiction in run {run}: {conflict:?}"));
            if before.is_terminal() {
                assert_eq!(after, before);
            }
            knowledge[index] = after;
        };
        for _ in 0..DEPTH {
            let step = steps[code % base];
            code /= base;
            match step {
                Step::Apply(index) => {
                    observe(index, outcome(world.store.apply(&commands[index]).reply));
                }
                Step::Resolve(index) => {
                    let operation = operation_id(first_epoch, index as u64 + 1);
                    observe(index, world.store.resolve_and_fence(operation).outcome);
                }
                Step::Fail => world.store.set_durability(Durability::Failing),
                Step::Restore => world.store.set_durability(Durability::Available),
                Step::OpenEpoch => {
                    let command = opening_command(&world.store);
                    world
                        .store
                        .open_epoch(&command)
                        .expect("healthy epoch read");
                }
                Step::Collect => {
                    let _ = world.store.admin(AdminCommand::CollectClosedEpochs {
                        through: first_epoch,
                    });
                }
                Step::DeleteTarget => {
                    let _ = world
                        .store
                        .admin(AdminCommand::DeleteSession { session: world.c });
                }
                Step::Release => {
                    // Ends the exchange of whichever admission took the
                    // slot: the other may then take it.
                    let release = world.release(3, 7, 1);
                    let reply = outcome(world.store.apply(&release).reply);
                    released |= matches!(reply, StoreOutcome::Committed(_));
                }
            }
            assert_eq!(
                accounts_match_what_is_held(
                    &world.store.snapshot().expect("healthy durable snapshot")
                ),
                Ok(()),
                "run {run}"
            );
        }
        let committed = knowledge
            .iter()
            .filter(|known| matches!(known, Knowledge::Committed(_)))
            .count();
        assert!(
            committed <= 1 + usize::from(released),
            "two admissions of one inbound exchange"
        );
        assert_eq!(
            world
                .store
                .snapshot()
                .expect("healthy durable snapshot")
                .publications
                .len(),
            committed,
            "an effect without a committed result, or the reverse"
        );
        terminal_runs += u32::from(knowledge.iter().any(|known| known.is_terminal()));
    }
    assert!(
        terminal_runs > 0,
        "the search never reached a terminal result"
    );
}

/// Arms a fault at the fence point, for resolutions.
fn at_the_fence(bench: &mut Bench, action: FaultAction) -> FaultId {
    bench
        .store
        .arm(FaultSpec {
            point: FaultPoint::FenceTransit,
            action,
            kind: Some(WorkKind::ResolveAndFence),
            source: None,
            occurrence: 1,
        })
        .expect("allowed")
}

/// The resolution reads the absence and stops at the fence point,
/// held; what it carries has not entered the writer again.
fn fence_held(bench: &mut Bench, operation: OperationId) -> (WorkId, FaultId) {
    let hold = at_the_fence(bench, FaultAction::HoldCommand);
    let fence = resolve(bench, 2, operation);
    assert_eq!(
        bench.store.perform(StoreAction::EnterWriter(fence)),
        Ok(Performed::Held(FaultPoint::FenceTransit))
    );
    assert_eq!(bench.store.hits(hold), 1, "the fence point was not reached");
    assert!(
        bench
            .store
            .store()
            .snapshot()
            .expect("healthy durable snapshot")
            .fences
            .is_empty()
    );
    assert!(
        bench
            .store
            .eligible()
            .contains(&StoreAction::ReleaseCommand(fence))
    );
    (fence, hold)
}

/// QABS-A through the fence point: the held fence enters the writer
/// after the command committed. It reads again: the operation is no
/// longer absent, so no fence is written and the commit is reported.
#[test]
fn qabs_a_through_the_fence_point_reads_again_before_it_writes() {
    let (mut bench, command, held) = held_command_and_absent_read();
    let operation = command.recipe.operation();
    let (fence, hold) = fence_held(&mut bench, operation);

    release(&mut bench, held);
    enter(&mut bench, held);
    release(&mut bench, fence);
    enter(&mut bench, fence);
    let resolved = outcome(deliver(&mut bench, fence));
    assert!(
        matches!(resolved, StoreOutcome::Committed(_)),
        "{resolved:?}"
    );
    assert_eq!(outcome(deliver(&mut bench, held)), resolved);
    let state = bench
        .store
        .store()
        .snapshot()
        .expect("healthy durable snapshot");
    assert!(state.fences.is_empty());
    assert_eq!(state.publications.len(), 1);
    let events = bench.store.take_events();
    assert_eq!(fences(&events), []);
    // Released once, it is not reached a second time.
    assert_eq!(bench.store.hits(hold), 1);
}

/// QABS-B through the fence point: the held fence enters the writer
/// before the command. It fences; the command is refused afterwards with
/// no effect.
#[test]
fn qabs_b_through_the_fence_point_fences_before_the_command() {
    let (mut bench, command, held) = held_command_and_absent_read();
    let operation = command.recipe.operation();
    let (fence, _) = fence_held(&mut bench, operation);

    release(&mut bench, fence);
    enter(&mut bench, fence);
    let fenced = StoreOutcome::RejectedCertain(RejectReason::Fenced);
    assert_eq!(outcome(deliver(&mut bench, fence)), fenced);
    release(&mut bench, held);
    enter(&mut bench, held);
    assert_eq!(outcome(deliver(&mut bench, held)), fenced);
    let state = bench
        .store
        .store()
        .snapshot()
        .expect("healthy durable snapshot");
    assert!(state.fences.contains(&operation));
    assert!(state.publications.is_empty());
    assert_eq!(fences(&bench.store.take_events()), [(fence, operation)]);
}

/// QABS-C through the fence point: `refuse_fence` makes the write of
/// that one fence impossible while the store stays available. The
/// resolution answers unknown, the trace says the fence was refused
/// where it would say it was stable, the held command then commits, and
/// a second resolution finds it and does not reach the point.
#[test]
fn qabs_c_through_the_fence_point_refuses_one_fence_and_nothing_else() {
    let (mut bench, command, held) = held_command_and_absent_read();
    let operation = command.recipe.operation();
    let refuse = at_the_fence(&mut bench, FaultAction::RefuseFence);
    let fence = resolve(&mut bench, 2, operation);
    let _ = bench.store.take_events();
    enter(&mut bench, fence);
    assert_eq!(
        outcome(deliver(&mut bench, fence)),
        StoreOutcome::PendingOrUnknown
    );
    assert_eq!(bench.store.hits(refuse), 1);
    let events = bench.store.take_events();
    assert_eq!(fences(&events), []);
    let refused = events
        .iter()
        .position(|event| {
            *event
                == StoreEvent::FenceRefused {
                    id: fence,
                    operation,
                }
        })
        .expect("the refusal is recorded");
    let hit = events
        .iter()
        .position(|event| matches!(event, StoreEvent::FaultHit { fault, .. } if *fault == refuse))
        .expect("the hit is recorded");
    let ready = events
        .iter()
        .position(|event| matches!(event, StoreEvent::ResultReady { id, .. } if *id == fence))
        .expect("a result");
    assert!(hit < refused && refused < ready, "{events:?}");
    assert!(
        bench
            .store
            .store()
            .snapshot()
            .expect("healthy durable snapshot")
            .fences
            .is_empty()
    );

    // The store is available: the held command commits.
    release(&mut bench, held);
    enter(&mut bench, held);
    let committed = outcome(deliver(&mut bench, held));
    assert!(matches!(committed, StoreOutcome::Committed(_)));
    // A second resolution finds the receipt before the point.
    let again = resolve(&mut bench, 3, operation);
    enter(&mut bench, again);
    assert_eq!(outcome(deliver(&mut bench, again)), committed);
    assert_eq!(bench.store.hits(refuse), 1);
}

/// Only a resolution that reads an absence reaches the fence point. One
/// that finds a receipt or a fence answers what is established, and the
/// fault armed there waits for the first one that does read an absence.
#[test]
fn a_resolution_that_finds_its_operation_does_not_reach_the_fence_point() {
    let mut bench = bench();
    let refuse = at_the_fence(&mut bench, FaultAction::RefuseFence);
    let command = bench.admission(1, 7, b"payload", &[bench.b]);
    let admitted = persist(&mut bench, 1, &command);
    enter(&mut bench, admitted);
    let committed = outcome(deliver(&mut bench, admitted));
    let fenced = operation_id(bench.epoch, 5);
    assert!(bench.store.store_mut().resolve_and_fence(fenced).fenced_now);

    let found = resolve(&mut bench, 2, command.recipe.operation());
    enter(&mut bench, found);
    assert_eq!(outcome(deliver(&mut bench, found)), committed);
    let already = resolve(&mut bench, 3, fenced);
    enter(&mut bench, already);
    assert_eq!(
        outcome(deliver(&mut bench, already)),
        StoreOutcome::RejectedCertain(RejectReason::Fenced)
    );
    assert_eq!(bench.store.hits(refuse), 0);

    let missing = operation_id(bench.epoch, 6);
    let absent = resolve(&mut bench, 4, missing);
    enter(&mut bench, absent);
    assert_eq!(
        outcome(deliver(&mut bench, absent)),
        StoreOutcome::PendingOrUnknown
    );
    assert_eq!(bench.store.hits(refuse), 1);
}
