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
    AdapterLimits, Delivery, FaultAction, FaultPoint, FaultSpec, Performed, SimStore, StoreAction,
    StoreEvent, WorkId, WorkKind,
};
use mqttiumd_lab::store::conformance::{World, operation_id};
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
    let read = bench.store.store().snapshot();
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
    let state = bench.store.store().snapshot();
    assert_eq!(state.publications.len(), 1);
    assert_eq!(state.obligations.len(), 2);
    assert!(state.fences.is_empty());
    assert_eq!(commits(&bench.store.take_events()), 1);
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
    let after_fence = bench.store.store().snapshot();
    assert!(after_fence.fences.contains(&operation));

    // The held command is really delivered to the writer afterwards,
    // and is refused with no effect.
    release(&mut bench, held);
    enter(&mut bench, held);
    assert_eq!(outcome(deliver(&mut bench, held)), fenced);
    assert_eq!(bench.store.store().snapshot(), after_fence);
    assert!(after_fence.publications.is_empty());
    assert!(after_fence.obligations.is_empty());
    assert_eq!(commits(&bench.store.take_events()), 0);
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
    assert!(bench.store.store().snapshot().fences.is_empty());

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
    assert_eq!(bench.store.store().snapshot().publications.len(), 1);
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
                    observe(index, world.store.resolve_and_fence(operation));
                }
                Step::Fail => world.store.set_durability(Durability::Failing),
                Step::Restore => world.store.set_durability(Durability::Available),
                Step::OpenEpoch => {
                    world.store.open_epoch();
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
            }
        }
        let committed = knowledge
            .iter()
            .filter(|known| matches!(known, Knowledge::Committed(_)))
            .count();
        assert!(committed <= 1, "two admissions of one inbound exchange");
        assert_eq!(
            world.store.snapshot().publications.len(),
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
