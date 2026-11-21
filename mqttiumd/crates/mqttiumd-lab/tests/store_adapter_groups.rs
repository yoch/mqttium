//! The store adapter told to commit in groups (record 0045): which
//! commands of the transit enter the writer together, what evidence each
//! member leaves, and what the faults of the adapter do inside a group.
//!
//! The store here is the deterministic one, which has no group. The
//! function that commits a group applies its commands one after the other
//! and records how it was called: these tests are about the adapter, not
//! about a store. `tests/bench_group.rs` plays the same adapter on
//! `SqliteStore::apply_group`.

use std::cell::RefCell;

use mqttiumd_contract::command::Command;
use mqttiumd_contract::effects::{Effect, EffectSink, OwnedEffect, SubmitResult};
use mqttiumd_contract::ids::{EffectId, EffectSeq, LogicalOwnerId, OperationId, StoreIncarnation};
use mqttiumd_contract::outcome::{StoreOutcome, StoreReply};
use mqttiumd_lab::store::adapter::{
    ActionRefused, AdapterLimits, Answer, FaultAction, FaultPoint, FaultSpec, GroupEvidence,
    Performed, SimStore, StoreAction, StoreEvent, WorkId,
};
use mqttiumd_lab::store::conformance::{World, operation_id};
use mqttiumd_lab::store::durable::{DurableStore, Limits, MemoryStore};
use mqttiumd_store_api::{Applied, ReadError};

type Bench = World<SimStore<MemoryStore>>;

std::thread_local! {
    /// The operations of each group the store was handed, in order.
    static CALLS: RefCell<Vec<Vec<OperationId>>> = const { RefCell::new(Vec::new()) };
}

/// Commits a group as the deterministic store can: one command after the
/// other, with the answers `apply` gives.
fn together(store: &mut MemoryStore, commands: &[&Command]) -> Vec<Applied> {
    CALLS.with(|calls| {
        calls.borrow_mut().push(
            commands
                .iter()
                .map(|command| command.recipe.operation())
                .collect(),
        );
    });
    commands
        .iter()
        .map(|command| store.apply(command))
        .collect()
}

/// A store that answers one member fewer than it was given.
fn short(store: &mut MemoryStore, commands: &[&Command]) -> Vec<Applied> {
    let mut applied = together(store, commands);
    applied.pop();
    applied
}

fn calls() -> Vec<Vec<OperationId>> {
    CALLS.with(|calls| core::mem::take(&mut *calls.borrow_mut()))
}

fn memory(incarnation: StoreIncarnation, limits: Limits) -> MemoryStore {
    MemoryStore::create(incarnation, limits)
}

const LIMITS: AdapterLimits = AdapterLimits {
    operations_in_flight: 4,
    result_slots: 6,
    faults: 4,
    events: 256,
};

/// An adapter that commits in groups of at most `max` commands.
fn bench(max: usize) -> Bench {
    let _ = calls();
    World::new(&memory).wrap(|store| {
        let mut adapter = SimStore::new(store, LIMITS);
        adapter.commit_in_groups(together, max);
        adapter
    })
}

fn effect_id(bench: &Bench, sequence: u64) -> EffectId {
    EffectId {
        epoch: bench.epoch,
        owner: LogicalOwnerId::new(0),
        sequence: EffectSeq::new(sequence),
    }
}

/// Submits the admission numbered `sequence`, from the producer's packet
/// `sequence + 6`.
fn admit(bench: &mut Bench, sequence: u64) -> WorkId {
    let packet = u16::try_from(sequence + 6).expect("fits");
    let command = bench.admission(sequence, packet, b"payload", &[bench.b, bench.c]);
    let effect = OwnedEffect::new(effect_id(bench, sequence), Effect::Persist { command });
    let id = WorkId::Effect(effect.id());
    assert_eq!(bench.store.try_submit(effect), SubmitResult::Accepted);
    id
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

fn enter(bench: &mut Bench, id: WorkId) -> Performed {
    bench
        .store
        .perform(StoreAction::EnterWriter(id))
        .expect("eligible")
}

/// The evidence of the store in `events`: `C` for a commit and `R` for a
/// ready result, each with the work it belongs to.
fn evidence(events: &[StoreEvent]) -> Vec<(char, WorkId)> {
    events
        .iter()
        .filter_map(|event| match event {
            StoreEvent::CommitStable { id, .. } => Some(('C', *id)),
            StoreEvent::ResultReady { id, .. } => Some(('R', *id)),
            _ => None,
        })
        .collect()
}

fn operation(bench: &Bench, sequence: u64) -> OperationId {
    operation_id(bench.epoch, sequence)
}

fn waiting(bench: &Bench) -> Vec<StoreAction> {
    bench
        .store
        .eligible()
        .into_iter()
        .filter(|action| matches!(action, StoreAction::EnterWriter(_)))
        .collect()
}

#[test]
fn the_commands_waiting_together_enter_the_writer_as_one_group() {
    let mut bench = bench(8);
    let ids = [
        admit(&mut bench, 1),
        admit(&mut bench, 2),
        admit(&mut bench, 3),
    ];
    let _ = bench.store.take_events();

    // Whichever of them the scheduler chooses, the group is the three,
    // in their order of acceptance.
    assert_eq!(enter(&mut bench, ids[1]), Performed::Executed);
    assert_eq!(
        calls(),
        [vec![
            operation(&bench, 1),
            operation(&bench, 2),
            operation(&bench, 3)
        ]],
        "one call of the store for the three commands"
    );
    // Every commit is recorded before any result exists.
    assert_eq!(
        evidence(&bench.store.take_events()),
        [
            ('C', ids[0]),
            ('C', ids[1]),
            ('C', ids[2]),
            ('R', ids[0]),
            ('R', ids[1]),
            ('R', ids[2]),
        ]
    );
    assert!(waiting(&bench).is_empty(), "nothing is left in transit");
    assert_eq!(
        bench.store.take_group_evidence(),
        GroupEvidence {
            groups: 1,
            shared: 1,
            commands: 3,
            largest: 3,
        }
    );
    assert_eq!(bench.store.take_group_evidence(), GroupEvidence::default());
    // Each result then travels alone, in any order.
    for id in [ids[2], ids[0], ids[1]] {
        let delivered = bench
            .store
            .perform(StoreAction::DeliverResult(id))
            .expect("eligible");
        assert!(
            matches!(delivered, Performed::Delivered { barrier: false, .. }),
            "{delivered:?}"
        );
    }
    assert_eq!(
        bench
            .store
            .store()
            .snapshot()
            .expect("healthy")
            .publications
            .len(),
        3
    );
}

#[test]
fn an_adapter_not_told_to_commit_in_groups_takes_one_command_at_a_time() {
    let _ = calls();
    let mut bench: Bench = World::new(&memory).wrap(|store| SimStore::new(store, LIMITS));
    let ids = [admit(&mut bench, 1), admit(&mut bench, 2)];
    let _ = bench.store.take_events();
    assert_eq!(enter(&mut bench, ids[0]), Performed::Executed);
    assert_eq!(
        evidence(&bench.store.take_events()),
        [('C', ids[0]), ('R', ids[0])]
    );
    assert_eq!(waiting(&bench), [StoreAction::EnterWriter(ids[1])]);
    assert!(calls().is_empty());
    assert_eq!(bench.store.take_group_evidence(), GroupEvidence::default());
}

#[test]
fn a_resolution_and_a_held_command_end_a_group() {
    // A resolution between two commands: they are not consecutive.
    let mut bench = bench(8);
    let first = admit(&mut bench, 1);
    let absent = operation(&bench, 9);
    let between = resolve(&mut bench, 2, absent);
    let last = admit(&mut bench, 3);
    assert_eq!(enter(&mut bench, last), Performed::Executed);
    assert_eq!(calls(), [vec![operation(&bench, 3)]]);
    assert_eq!(
        waiting(&bench),
        [
            StoreAction::EnterWriter(first),
            StoreAction::EnterWriter(between)
        ]
    );
    // A resolution enters the writer alone, and is no group.
    assert_eq!(enter(&mut bench, between), Performed::Executed);
    assert!(calls().is_empty());
    assert_eq!(enter(&mut bench, first), Performed::Executed);
    assert_eq!(calls(), [vec![operation(&bench, 1)]]);
    assert_eq!(
        bench.store.take_group_evidence(),
        GroupEvidence {
            groups: 2,
            shared: 0,
            commands: 2,
            largest: 1,
        }
    );

    // A command a fault holds stays where it is, and the commands on
    // either side of it are two groups.
    let mut bench = self::bench(8);
    bench
        .store
        .arm(FaultSpec {
            point: FaultPoint::CommandTransit,
            action: FaultAction::HoldCommand,
            kind: None,
            source: None,
            occurrence: 2,
        })
        .expect("allowed");
    let ids = [
        admit(&mut bench, 1),
        admit(&mut bench, 2),
        admit(&mut bench, 3),
    ];
    assert_eq!(enter(&mut bench, ids[0]), Performed::Executed);
    assert_eq!(calls(), [vec![operation(&bench, 1)]]);
    assert_eq!(
        bench.store.perform(StoreAction::EnterWriter(ids[1])),
        Err(ActionRefused::NotEligible),
        "a held command does not enter the writer"
    );
    // Released, it is a member like the one after it.
    assert_eq!(
        bench.store.perform(StoreAction::ReleaseCommand(ids[1])),
        Ok(Performed::Released)
    );
    assert_eq!(enter(&mut bench, ids[2]), Performed::Executed);
    assert_eq!(calls(), [vec![operation(&bench, 2), operation(&bench, 3)]]);
}

#[test]
fn a_group_is_cut_at_the_bound_around_the_chosen_command() {
    let mut bench = bench(2);
    let ids = [
        admit(&mut bench, 1),
        admit(&mut bench, 2),
        admit(&mut bench, 3),
    ];
    assert_eq!(enter(&mut bench, ids[2]), Performed::Executed);
    assert_eq!(calls(), [vec![operation(&bench, 2), operation(&bench, 3)]]);
    assert_eq!(waiting(&bench), [StoreAction::EnterWriter(ids[0])]);
    assert_eq!(enter(&mut bench, ids[0]), Performed::Executed);
    assert_eq!(calls(), [vec![operation(&bench, 1)]]);
    assert_eq!(
        bench.store.take_group_evidence(),
        GroupEvidence {
            groups: 2,
            shared: 1,
            commands: 3,
            largest: 2,
        }
    );
}

/// The barrier after a commit, reached by the second member of a group:
/// the group is durable, the first member has its result, and the results
/// of the member that stopped and of the one after it wait for the
/// barrier to be left.
#[test]
fn a_barrier_inside_a_group_keeps_the_results_of_the_members_after_it() {
    let barrier = FaultSpec {
        point: FaultPoint::CommitApplied,
        action: FaultAction::Barrier,
        kind: None,
        source: None,
        occurrence: 2,
    };
    let mut bench = bench(8);
    bench.store.arm(barrier).expect("allowed");
    let ids = [
        admit(&mut bench, 1),
        admit(&mut bench, 2),
        admit(&mut bench, 3),
    ];
    let _ = bench.store.take_events();
    assert_eq!(
        enter(&mut bench, ids[0]),
        Performed::Halted(FaultPoint::CommitApplied)
    );
    assert_eq!(
        evidence(&bench.store.take_events()),
        [('C', ids[0]), ('C', ids[1]), ('C', ids[2]), ('R', ids[0]),],
        "the three commits, and the result of the member before the barrier"
    );
    assert_eq!(bench.store.halted_at(), Some(FaultPoint::CommitApplied));
    assert_eq!(bench.store.resume(), Some(FaultPoint::CommitApplied));
    assert_eq!(bench.store.halted_at(), None);
    let events = bench.store.take_events();
    assert_eq!(evidence(&events), [('R', ids[1]), ('R', ids[2])]);
    for event in &events {
        if let StoreEvent::ResultReady { answer, .. } = event {
            assert!(matches!(
                answer,
                Answer::Operation {
                    reply: StoreReply::Outcome(StoreOutcome::Committed(_)),
                    ..
                }
            ));
        }
    }

    // A crash at that barrier loses the results that waited there, and
    // keeps the whole group.
    let mut bench = self::bench(8);
    bench.store.arm(barrier).expect("allowed");
    let first = admit(&mut bench, 1);
    admit(&mut bench, 2);
    assert_eq!(
        enter(&mut bench, first),
        Performed::Halted(FaultPoint::CommitApplied)
    );
    let crashed = bench.store.crash();
    assert_eq!(
        crashed
            .store
            .snapshot()
            .expect("healthy")
            .publications
            .len(),
        2
    );
    assert_eq!(
        evidence(&crashed.events).len(),
        3,
        "two commits, one result"
    );
}

#[test]
fn a_store_that_answers_another_number_of_members_stops_the_adapter() {
    let _ = calls();
    let mut bench: Bench = World::new(&memory).wrap(|store| {
        let mut adapter = SimStore::new(store, LIMITS);
        adapter.commit_in_groups(short, 8);
        adapter
    });
    let ids = [admit(&mut bench, 1), admit(&mut bench, 2)];
    let _ = bench.store.take_events();
    assert_eq!(
        bench.store.perform(StoreAction::EnterWriter(ids[0])),
        Err(ActionRefused::Read(ReadError::InvalidData))
    );
    // Nothing is attributed: no evidence, and the commands are where
    // they were.
    assert!(evidence(&bench.store.take_events()).is_empty());
    assert_eq!(waiting(&bench).len(), 2);
    assert_eq!(bench.store.take_group_evidence(), GroupEvidence::default());
}
