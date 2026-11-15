//! Opening an epoch through the path the product takes (ADR 0009):
//! `EpochOpening::submit`, the store adapter, the writer, the reply,
//! `EpochOpening::observe`, then `hand_over`. No test here builds a
//! grant, or a committed reply, by hand.

use mqttiumd_contract::command::{EpochLedger, EpochPhase, OpeningTicket};
use mqttiumd_contract::effects::RecoveredFact;
use mqttiumd_contract::ids::{RunEpoch, StoreIncarnation};
use mqttiumd_contract::outcome::{
    EpochGrant, EpochOpened, EpochOpening, HandOverRefused, Observed, OpeningOutcome,
    OpeningRejection, OpeningReply, OpeningStage, RecoveredEpochCheck, RejectReason, ReplyRefused,
    StoreOutcome, StoreReply, SubmitRefused,
};
use mqttiumd_lab::store::adapter::{
    AdapterLimits, Delivery, FaultAction, FaultPoint, FaultSpec, Performed, SimStore, StoreAction,
    StoreEvent, WorkId, WorkKind,
};
use mqttiumd_lab::store::conformance::{INCARNATION, World, fixture_limits};
use mqttiumd_lab::store::durable::{Durability, DurableStore, Limits, MemoryStore};

const LIMITS: AdapterLimits = AdapterLimits {
    operations_in_flight: 2,
    result_slots: 4,
    faults: 4,
    events: 256,
};

fn memory(incarnation: StoreIncarnation, limits: Limits) -> MemoryStore {
    MemoryStore::create(incarnation, limits)
}

/// A process that just started on an empty data set.
fn started() -> SimStore<MemoryStore> {
    SimStore::new(memory(INCARNATION, fixture_limits()), LIMITS)
}

fn opening_fault(action: FaultAction) -> FaultSpec {
    FaultSpec {
        point: FaultPoint::ResultReady,
        action,
        kind: Some(WorkKind::OpenEpoch),
        source: None,
        occurrence: 1,
    }
}

/// Submits an attempt and runs it in the writer. Returns its identity.
fn attempt(sim: &mut SimStore<MemoryStore>, opening: &mut EpochOpening) -> WorkId {
    let command = opening.submit().expect("an attempt may start");
    sim.submit_opening(command).expect("room");
    let id = WorkId::Opening(command.ticket);
    assert_eq!(
        sim.perform(StoreAction::EnterWriter(id)),
        Ok(Performed::Executed)
    );
    id
}

/// Delivers the reply of `id` as the dispatcher hands it over.
fn reply(sim: &mut SimStore<MemoryStore>, id: WorkId) -> OpeningReply {
    match sim.perform(StoreAction::DeliverResult(id)) {
        Ok(Performed::Delivered {
            delivery: Delivery::Opening(reply),
            barrier: false,
        }) => reply,
        other => panic!("no opening reply delivered: {other:?}"),
    }
}

/// The whole path of one opening that commits. Returns the grant.
fn open_and_hand_over(sim: &mut SimStore<MemoryStore>, opening: &mut EpochOpening) -> EpochGrant {
    let id = attempt(sim, opening);
    let reply = reply(sim, id);
    assert!(matches!(
        opening.observe(reply),
        Ok(Observed::Advanced(OpeningStage::Committed(_)))
    ));
    opening.hand_over().expect("committed")
}

fn ledger(sim: &SimStore<MemoryStore>) -> EpochLedger {
    sim.store().snapshot().ledger
}

/// What start-up concludes when its deadline passes with no reply.
fn deadline_passed(id: WorkId) -> OpeningReply {
    let WorkId::Opening(ticket) = id else {
        panic!("an opening");
    };
    OpeningReply {
        ticket,
        outcome: OpeningOutcome::Unknown,
    }
}

#[test]
fn a_committed_opening_hands_over_one_grant_for_the_epoch_the_store_opened() {
    let mut sim = started();
    let mut opening = EpochOpening::new();
    let grant = open_and_hand_over(&mut sim, &mut opening);
    assert_eq!(grant.incarnation(), INCARNATION);
    assert_eq!(grant.epoch(), RunEpoch::new(1));
    assert_eq!(ledger(&sim).open_epoch(), Some(grant.epoch()));
    assert_eq!(opening.hand_over(), Err(HandOverRefused::AlreadyHandedOver));

    let id = WorkId::Opening(OpeningTicket::new(1));
    let answer =
        mqttiumd_lab::store::adapter::Answer::Opening(OpeningOutcome::Committed(grant.opened()));
    assert_eq!(
        sim.take_events(),
        [
            StoreEvent::CommandSubmitted {
                id,
                kind: WorkKind::OpenEpoch,
                operation: None
            },
            StoreEvent::CommitStable {
                id,
                operation: None
            },
            StoreEvent::ResultReady { id, answer },
            StoreEvent::ResultDelivered { id, answer },
        ]
    );
}

#[test]
fn a_duplicated_reply_gives_no_second_authority() {
    let mut sim = started();
    let duplicate = sim
        .arm(opening_fault(FaultAction::DuplicateResult))
        .expect("allowed");
    let mut opening = EpochOpening::new();
    let id = attempt(&mut sim, &mut opening);
    assert_eq!(sim.hits(duplicate), 1, "the injection was not reached");

    let first = reply(&mut sim, id);
    opening.observe(first).expect("correlated");
    // The second copy answers no attempt in flight: the dispatcher
    // refuses it and records it.
    assert_eq!(
        sim.perform(StoreAction::DeliverResult(id)),
        Ok(Performed::Uncorrelated)
    );
    assert!(sim.take_events().contains(&StoreEvent::UncorrelatedResult {
        id,
        operation: None
    }));

    let grant = opening.hand_over().expect("committed");
    // Even handed straight to the opening, before or after the
    // hand-over, the same reply changes nothing and yields nothing.
    assert_eq!(opening.observe(first), Ok(Observed::Unchanged));
    assert_eq!(opening.hand_over(), Err(HandOverRefused::AlreadyHandedOver));
    assert_eq!(grant.epoch(), RunEpoch::new(1));
    // One opening was committed, not two.
    assert_eq!(ledger(&sim).last_opened(), RunEpoch::new(1));
}

#[test]
fn an_opening_whose_reply_is_lost_gives_no_authority_and_is_opened_again() {
    let mut sim = started();
    let lost = sim
        .arm(opening_fault(FaultAction::DropResult))
        .expect("allowed");
    let mut opening = EpochOpening::new();
    let id = attempt(&mut sim, &mut opening);
    assert_eq!(sim.hits(lost), 1, "the injection was not reached");

    // The store did open epoch 1; this process will never be told.
    assert_eq!(ledger(&sim).open_epoch(), Some(RunEpoch::new(1)));
    assert!(sim.eligible().is_empty());
    assert_eq!(
        opening.observe(deadline_passed(id)),
        Ok(Observed::Advanced(OpeningStage::Unknown))
    );
    assert_eq!(
        opening.hand_over(),
        Err(HandOverRefused::NotCommitted(OpeningStage::Unknown))
    );
    // Reading the durable fact back does not help: epoch 1 is there to
    // be read, and it authorises nothing.
    let page = sim.store().recover(None, 8).expect("a page");
    assert_eq!(
        page.facts.as_slice(),
        [RecoveredFact::Epoch {
            incarnation: INCARNATION,
            epoch: RunEpoch::new(1)
        }]
    );
    assert_eq!(
        opening.check_recovered(INCARNATION, RunEpoch::new(1)),
        RecoveredEpochCheck::NotEstablished
    );
    assert!(opening.hand_over().is_err());

    // The way forward: a new attempt. It opens epoch 2 and closes the
    // epoch the lost attempt had opened, in which no owner ever lived.
    let grant = open_and_hand_over(&mut sim, &mut opening);
    assert_eq!(grant.epoch(), RunEpoch::new(2));
    assert_eq!(ledger(&sim).phase_of(RunEpoch::new(1)), EpochPhase::Closed);
    assert_eq!(opening.hand_over(), Err(HandOverRefused::AlreadyHandedOver));
}

#[test]
fn the_late_reply_of_an_abandoned_attempt_gives_no_authority() {
    let mut sim = started();
    sim.arm(opening_fault(FaultAction::DelayResult))
        .expect("allowed");
    let mut opening = EpochOpening::new();
    let first = attempt(&mut sim, &mut opening);
    opening.observe(deadline_passed(first)).expect("correlated");

    let second = attempt(&mut sim, &mut opening);
    // The reply of the first attempt, committed for epoch 1, arrives
    // now. The dispatcher still expects it; the opening does not.
    assert_eq!(
        sim.perform(StoreAction::ReleaseResult(first)),
        Ok(Performed::Released)
    );
    let late = reply(&mut sim, first);
    assert_eq!(
        late.outcome,
        OpeningOutcome::Committed(EpochOpened {
            incarnation: INCARNATION,
            epoch: RunEpoch::new(1)
        })
    );
    assert_eq!(
        opening.observe(late),
        Err(ReplyRefused::Uncorrelated {
            ticket: late.ticket
        })
    );
    assert!(opening.hand_over().is_err());

    let current = reply(&mut sim, second);
    opening.observe(current).expect("correlated");
    assert_eq!(
        opening.hand_over().expect("committed").epoch(),
        RunEpoch::new(2)
    );
}

#[test]
fn an_opening_the_medium_cannot_write_is_unknown_and_opens_nothing() {
    let mut sim = started();
    sim.store_mut().set_durability(Durability::Failing);
    let mut opening = EpochOpening::new();
    let id = attempt(&mut sim, &mut opening);
    let unknown = reply(&mut sim, id);
    assert_eq!(unknown.outcome, OpeningOutcome::Unknown);
    assert_eq!(
        opening.observe(unknown),
        Ok(Observed::Advanced(OpeningStage::Unknown))
    );
    assert!(opening.hand_over().is_err());
    assert_eq!(ledger(&sim).open_epoch(), None);
    assert!(
        !sim.take_events()
            .iter()
            .any(|event| matches!(event, StoreEvent::CommitStable { .. }))
    );

    sim.store_mut().set_durability(Durability::Available);
    let grant = open_and_hand_over(&mut sim, &mut opening);
    assert_eq!(grant.epoch(), RunEpoch::new(1));
}

#[test]
fn an_exhausted_epoch_counter_refuses_the_opening_for_good() {
    // No sequence of commands reaches the last epoch: this state is
    // written directly, for this test only.
    let mut durable = memory(INCARNATION, fixture_limits()).snapshot();
    durable.ledger = EpochLedger::from_durable(INCARNATION, RunEpoch::new(u64::MAX - 1));
    let mut sim = SimStore::new(MemoryStore::reopen(durable), LIMITS);

    let mut last = EpochOpening::new();
    let grant = open_and_hand_over(&mut sim, &mut last);
    assert_eq!(grant.epoch(), RunEpoch::new(u64::MAX));

    let before = sim.store().snapshot();
    let mut opening = EpochOpening::new();
    let id = attempt(&mut sim, &mut opening);
    let refused = reply(&mut sim, id);
    let exhausted = OpeningRejection::EpochsExhausted;
    assert_eq!(refused.outcome, OpeningOutcome::RejectedCertain(exhausted));
    assert_eq!(
        opening.observe(refused),
        Ok(Observed::Advanced(OpeningStage::Rejected(exhausted)))
    );
    assert!(opening.hand_over().is_err());
    assert_eq!(opening.submit(), Err(SubmitRefused::Rejected(exhausted)));
    // Nothing moved and nothing wrapped.
    assert_eq!(sim.store().snapshot(), before);
}

/// After the volatile state is lost, recovery opens a new epoch and the
/// older one is not entered again (DEM-012).
#[test]
fn after_a_crash_a_new_epoch_is_opened_and_the_older_one_is_not_entered_again() {
    // The first process: fixture, opening, one committed admission and
    // one command still held in transit when it dies.
    let world = World::new(&memory);
    let first_epoch = world.epoch;
    let committed = world.admission(1, 7, b"payload", &[world.b, world.c]);
    let in_transit = world.admission(2, 8, b"payload", &[world.b]);
    let mut world = world.wrap(|store| SimStore::new(store, LIMITS));
    assert!(world.store.store_mut().apply(&committed).committed_now);
    let durable = world.store.store().snapshot();

    let crashed = world.store.crash();
    // The grant, the opening and every owner died with the process.
    // The next one starts from the durable state and from nothing else.
    let mut sim = SimStore::new(MemoryStore::reopen(crashed.store.snapshot()), LIMITS);
    assert_eq!(sim.store().snapshot(), durable);

    let mut opening = EpochOpening::new();
    let grant = open_and_hand_over(&mut sim, &mut opening);
    assert!(grant.epoch() > first_epoch);
    assert_eq!(ledger(&sim).phase_of(first_epoch), EpochPhase::Closed);

    // The inventory describes the epoch this process opened; reading
    // it, page after page, yields no second authority.
    let mut cursor = None;
    let mut facts = Vec::new();
    loop {
        let page = sim.store().recover(cursor, 2).expect("a page");
        for fact in page.facts.as_slice() {
            if let RecoveredFact::Epoch { incarnation, epoch } = fact {
                assert_eq!(
                    opening.check_recovered(*incarnation, *epoch),
                    RecoveredEpochCheck::Confirms
                );
            }
            assert_eq!(opening.hand_over(), Err(HandOverRefused::AlreadyHandedOver));
        }
        facts.extend_from_slice(page.facts.as_slice());
        match page.next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    // The durable effects of the older epoch are still reconciled.
    assert!(facts.iter().any(|fact| matches!(
        fact,
        RecoveredFact::Operation { operation, .. } if *operation == committed.recipe.operation()
    )));

    // The command the dead process had not delivered: its epoch is
    // closed, it executes nothing.
    let before = sim.store().snapshot();
    let late = sim.store_mut().apply(&in_transit);
    assert_eq!(
        late.reply,
        StoreReply::Outcome(StoreOutcome::RejectedCertain(RejectReason::EpochClosed))
    );
    assert!(!late.committed_now);
    assert_eq!(sim.store().snapshot(), before);
}
