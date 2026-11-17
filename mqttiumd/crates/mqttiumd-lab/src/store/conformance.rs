//! Contract tests of a `DurableStore`.
//!
//! Each check takes a factory and uses the trait only, so the SQLite
//! store of lot L7 runs the same checks unchanged: it supplies a function
//! that creates an empty data set with the given incarnation and limits.
//! A check panics, with the usual assertion message, when the store
//! breaks the contract.
//!
//! The builders at the top are public: the tests of the adapter use the
//! same commands.

use mqttiumd_contract::capacity::{
    BoundedVec, ExchangeSlots, PayloadBytes, ReceiptSlots, References, Resource,
};
use mqttiumd_contract::command::{
    AdmitPublication, Command, Costs, DeliveryCause, EpochPhase, FrozenRecipe, OpenEpoch,
    OpeningAttempt, OpeningRejection, OpeningTicket, PayloadRef, Recipe, ReleaseInbound, Source,
    StartupNonce, Target,
};
use mqttiumd_contract::effects::{InboundPhase, RecoveredFact, RecoveryOrder};
use mqttiumd_contract::ids::{
    DeliveryId, ExchangeGeneration, ExchangeId, InboundPacketId, LogicalOwnerId, OperationId,
    OperationSeq, PayloadId, PayloadSeq, PublicationId, PublicationSeq, RequestId, RequestSeq,
    Revision, RunEpoch, SessionId, StoreIncarnation,
};
use mqttiumd_contract::outcome::{
    CommitFacts, EpochOpened, Knowledge, OpeningOutcome, RejectReason, StoreOutcome, StoreReply,
    StoreViolation,
};
use mqttiumd_contract::packet::{Qos, SharedBytes};

use super::durable::{
    Accounts, AdminCommand, AdminDone, AdminRefused, Decision, DurableState, DurableStore, Limits,
    MAX_CLIENT_ID_BYTES, ObligationPhase, ReadBudget, ReadError, RecoveryRefused, Resolved,
};

/// The incarnation every check creates its data set with.
pub const INCARNATION: StoreIncarnation = StoreIncarnation::new(1);

/// The topic of the fixture route.
pub const TOPIC: &[u8] = b"p1/example";

/// The limits of the fixture `three-persistent-sessions`.
#[must_use]
pub fn fixture_limits() -> Limits {
    Limits {
        payload_bytes: PayloadBytes::new(4096),
        references: References::new(16),
        exchange_slots: ExchangeSlots::new(4),
        receipt_slots: ReceiptSlots::new(8),
        max_targets_per_admission: 4,
        max_topic_bytes: 256,
        max_payload_bytes: 1024,
        max_sessions: 8,
        max_route_targets: 4,
    }
}

/// A store prepared like the fixture, through commands only: three
/// persistent sessions, one exact route towards `b` and `c`, and one
/// open epoch.
pub struct World<S> {
    pub store: S,
    pub producer: SessionId,
    pub b: SessionId,
    pub c: SessionId,
    pub epoch: RunEpoch,
    pub route_revision: Revision,
}

fn create_session<S: DurableStore>(store: &mut S, client_id: &[u8]) -> SessionId {
    match store.admin(AdminCommand::CreateSession {
        client_id: client_id.to_vec(),
    }) {
        Ok(AdminDone::SessionCreated { session, revision }) => {
            assert_eq!(revision, Revision::new(1));
            session
        }
        other => panic!("session {client_id:?} was not created: {other:?}"),
    }
}

/// The nonce of the first start-up the builders speak for.
pub const STARTUP: StartupNonce = startup(0);

/// The nonce of the start-up of rank `rank` among those the builders
/// speak for. No two ranks share one: an attempt designates one
/// command, and the store refuses the opener of the last epoch that
/// asks for the next one under the same identity.
#[must_use]
pub const fn startup(rank: u64) -> StartupNonce {
    let rank = rank.to_be_bytes();
    let mut bytes = [0xA5; 16];
    let mut index = 0;
    while index < rank.len() {
        bytes[8 + index] = rank[index];
        index += 1;
    }
    StartupNonce::new(bytes)
}

/// The attempt numbered `ticket` of the start-up `startup`.
#[must_use]
pub fn attempt(startup: StartupNonce, ticket: u64) -> OpeningAttempt {
    OpeningAttempt {
        startup,
        ticket: OpeningTicket::new(ticket),
    }
}

/// The first command of a start-up that just read the ledger of
/// `store`. Each value read is read by a start-up of its own.
pub fn opening_command<S: DurableStore>(store: &S) -> OpenEpoch {
    let ledger = store.ledger().expect("healthy durable ledger");
    OpenEpoch {
        attempt: attempt(startup(ledger.last_opened().get()), 1),
        incarnation: ledger.incarnation(),
        after: ledger.last_opened(),
    }
}

/// Opens the next epoch, on the value just read, and returns it.
pub fn open<S: DurableStore>(store: &mut S) -> RunEpoch {
    let command = opening_command(store);
    match store.open_epoch(&command).expect("healthy epoch read") {
        OpeningOutcome::Committed(EpochOpened { incarnation, epoch }) => {
            assert_eq!(incarnation, INCARNATION);
            epoch
        }
        other => panic!("the epoch was not opened: {other:?}"),
    }
}

impl<S: DurableStore> World<S> {
    /// Sets the fixture up on an empty data set.
    pub fn prepare(mut store: S) -> Self {
        let producer = create_session(&mut store, b"p1-producer");
        let b = create_session(&mut store, b"p1-b");
        let c = create_session(&mut store, b"p1-c");
        let mut route_revision = Revision::new(0);
        for session in [b, c] {
            match store.admin(AdminCommand::AddRoute {
                topic: TOPIC.to_vec(),
                session,
                qos: Qos::AtLeastOnce,
            }) {
                Ok(AdminDone::RouteAdded {
                    route_revision: now,
                }) => route_revision = now,
                other => panic!("the route was not added: {other:?}"),
            }
        }
        let epoch = open(&mut store);
        Self {
            store,
            producer,
            b,
            c,
            epoch,
            route_revision,
        }
    }

    /// Sets the fixture up on what `make` creates with these limits.
    pub fn with_limits(make: &dyn Fn(StoreIncarnation, Limits) -> S, limits: Limits) -> Self {
        Self::prepare(make(INCARNATION, limits))
    }

    /// The fixture with its own limits.
    pub fn new(make: &dyn Fn(StoreIncarnation, Limits) -> S) -> Self {
        Self::with_limits(make, fixture_limits())
    }
}

impl<S> World<S> {
    /// The same fixture around another holder of its store, such as the
    /// adapter that wraps it.
    pub fn wrap<T>(self, wrap: impl FnOnce(S) -> T) -> World<T> {
        World {
            store: wrap(self.store),
            producer: self.producer,
            b: self.b,
            c: self.c,
            epoch: self.epoch,
            route_revision: self.route_revision,
        }
    }

    /// An admission of `payload` from the producer's packet `packet_id`
    /// towards `targets`, with the costs the store expects, in the
    /// current epoch. `sequence` numbers every identity of the recipe.
    #[must_use]
    pub fn admission(
        &self,
        sequence: u64,
        packet_id: u16,
        payload: &[u8],
        targets: &[SessionId],
    ) -> Command {
        admission_in(
            self.epoch,
            self.route_revision,
            self.producer,
            sequence,
            packet_id,
            payload,
            targets,
        )
    }

    /// The release of the producer's packet `packet_id`, whose exchange
    /// generation is the `admitted_as` sequence of its admission.
    #[must_use]
    pub fn release(&self, sequence: u64, packet_id: u16, admitted_as: u64) -> Command {
        let recipe = Recipe::ReleaseInbound(ReleaseInbound {
            operation: operation_id(self.epoch, sequence),
            source_session: self.producer,
            packet_id: InboundPacketId::new(packet_id).expect("non-zero"),
            exchange: ExchangeId {
                session: self.producer,
                generation: ExchangeGeneration::new(admitted_as),
            },
        });
        Command {
            recipe: FrozenRecipe::freeze(recipe).expect("well formed"),
            payload: None,
        }
    }
}

const OWNER: LogicalOwnerId = LogicalOwnerId::new(0);

/// The `OperationId` the builders give to `sequence` in `epoch`.
#[must_use]
pub fn operation_id(epoch: RunEpoch, sequence: u64) -> OperationId {
    OperationId::new(INCARNATION, epoch, OWNER, OperationSeq::new(sequence))
}

/// The `PublicationId` the builders give to `sequence` in `epoch`.
#[must_use]
pub fn publication_id(epoch: RunEpoch, sequence: u64) -> PublicationId {
    PublicationId::new(INCARNATION, epoch, OWNER, PublicationSeq::new(sequence))
}

/// An admission built from explicit parts; see `World::admission`.
#[must_use]
pub fn admission_in(
    epoch: RunEpoch,
    route_revision: Revision,
    producer: SessionId,
    sequence: u64,
    packet_id: u16,
    payload: &[u8],
    targets: &[SessionId],
) -> Command {
    let publication = publication_id(epoch, sequence);
    let length = PayloadBytes::new(payload.len() as u64);
    let targets: Vec<Target> = targets
        .iter()
        .zip(0u32..)
        .map(|(session, index)| Target {
            session: *session,
            session_revision: Revision::new(1),
            delivery: DeliveryId { publication, index },
            effective_qos: Qos::AtLeastOnce,
            cause: DeliveryCause::LiveExact,
        })
        .collect();
    let recipe = Recipe::AdmitPublication(AdmitPublication {
        operation: operation_id(epoch, sequence),
        request: RequestId::new(INCARNATION, epoch, OWNER, RequestSeq::new(sequence)),
        publication,
        source: Source {
            session: producer,
            session_revision: Revision::new(1),
            packet_id: InboundPacketId::new(packet_id).expect("non-zero"),
            exchange: ExchangeId {
                session: producer,
                generation: ExchangeGeneration::new(sequence),
            },
        },
        route_revision,
        qos: Qos::ExactlyOnce,
        retain: false,
        topic: SharedBytes::copy_from(TOPIC, 256).expect("fits"),
        payload: PayloadRef {
            id: PayloadId::new(INCARNATION, epoch, OWNER, PayloadSeq::new(sequence)),
            length,
        },
        costs: Costs {
            payload_bytes: length,
            references: References::new(targets.len() as u64),
            exchange_slots: ExchangeSlots::new(1),
            receipt_slots: ReceiptSlots::new(1),
        },
        targets: BoundedVec::from_slice(&targets, targets.len().max(1)).expect("fits"),
    });
    Command {
        recipe: FrozenRecipe::freeze(recipe).expect("well formed"),
        payload: Some(SharedBytes::copy_from(payload, 1024).expect("fits")),
    }
}

/// Rebuilds a command after changing its admission.
#[must_use]
pub fn altered(command: &Command, change: impl FnOnce(&mut AdmitPublication)) -> Command {
    let mut bytes = Vec::new();
    command
        .recipe
        .get()
        .encode_canonical(&mut bytes)
        .expect("well formed");
    let Ok(Recipe::AdmitPublication(mut admit)) = Recipe::decode_canonical(&bytes, 64, 1024) else {
        panic!("not an admission");
    };
    change(&mut admit);
    Command {
        recipe: FrozenRecipe::freeze(Recipe::AdmitPublication(admit)).expect("well formed"),
        payload: command.payload.clone(),
    }
}

fn outcome(reply: StoreReply) -> StoreOutcome {
    match reply {
        StoreReply::Outcome(outcome) => outcome,
        StoreReply::Violation(violation) => panic!("violation {violation:?}"),
    }
}

/// An outcome that was already established: no fence was written.
fn settled(outcome: StoreOutcome) -> Resolved {
    Resolved {
        outcome,
        fenced_now: false,
    }
}

fn admitted(command: &Command) -> CommitFacts {
    let Recipe::AdmitPublication(admit) = command.recipe.get() else {
        panic!("not an admission");
    };
    CommitFacts::PublicationAdmitted {
        publication: admit.publication,
        exchange: admit.source.exchange,
    }
}

/// The state without what a rejection is allowed to add: its receipt,
/// the receipt slot it holds and the revision of the commit that
/// recorded it.
fn without_receipts(mut state: DurableState) -> DurableState {
    let receipts = state.operations.len() as u64;
    state.accounts.receipt_slots = ReceiptSlots::new(
        state
            .accounts
            .receipt_slots
            .get()
            .checked_sub(receipts)
            .expect("each receipt holds a slot"),
    );
    state.operations.clear();
    state.revision = 0;
    state
}

/// A fresh data set has no open epoch; each opening opens the next one
/// and closes the one before.
pub fn opening_opens_the_next_epoch_and_closes_the_older_ones<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let mut store = make(INCARNATION, fixture_limits());
    assert_eq!(
        store
            .ledger()
            .expect("healthy durable ledger")
            .incarnation(),
        INCARNATION
    );
    assert_eq!(
        store.ledger().expect("healthy durable ledger").open_epoch(),
        None
    );
    assert_eq!(
        store.snapshot().expect("healthy durable snapshot").ledger,
        store.ledger().expect("healthy durable ledger")
    );
    assert_eq!(open(&mut store), RunEpoch::new(1));
    assert_eq!(open(&mut store), RunEpoch::new(2));
    let ledger = store.ledger().expect("healthy durable ledger");
    assert_eq!(
        ledger,
        store.snapshot().expect("healthy durable snapshot").ledger
    );
    assert_eq!(ledger.incarnation(), INCARNATION);
    assert_eq!(ledger.open_epoch(), Some(RunEpoch::new(2)));
    assert_eq!(ledger.phase_of(RunEpoch::new(1)), EpochPhase::Closed);
    assert_eq!(ledger.phase_of(RunEpoch::new(3)), EpochPhase::Unopened);
}

/// An opening commits only while the last opened epoch is the one its
/// command expects. An attempt that arrives after another opening was
/// committed opens nothing and changes nothing: it cannot close the
/// epoch that is open (ADR 0009).
pub fn an_opening_that_expected_another_epoch_opens_nothing<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let mut store = make(INCARNATION, fixture_limits());
    // Two attempts read the same value; one was held on its way.
    let held = opening_command(&store);
    let passing = OpenEpoch {
        attempt: attempt(held.attempt.startup, 2),
        ..held
    };
    let first = OpeningOutcome::Committed(EpochOpened {
        incarnation: INCARNATION,
        epoch: RunEpoch::new(1),
    });
    assert_eq!(
        store.open_epoch(&passing).expect("healthy epoch read"),
        first
    );
    assert_eq!(
        store.ledger().expect("healthy durable ledger").opened_by(),
        Some(passing.attempt)
    );
    let before = store.snapshot().expect("healthy durable snapshot");
    // Another attempt opened the epoch the held one asked for: it is
    // rejected for certain.
    let moved = OpeningOutcome::RejectedCertain(OpeningRejection::PredecessorMoved);
    assert_eq!(store.open_epoch(&held).expect("healthy epoch read"), moved);
    assert_eq!(store.snapshot().expect("healthy durable snapshot"), before);

    // Once a later opening closed that epoch the store no longer knows
    // who had opened it: both attempts are superseded, neither is
    // called a rejection.
    open(&mut store);
    let before = store.snapshot().expect("healthy durable snapshot");
    assert_eq!(
        store.open_epoch(&held).expect("healthy epoch read"),
        OpeningOutcome::Superseded
    );
    assert_eq!(
        store.open_epoch(&passing).expect("healthy epoch read"),
        OpeningOutcome::Superseded
    );
    assert_eq!(
        store.ledger().expect("healthy durable ledger").open_epoch(),
        Some(RunEpoch::new(2))
    );

    // A condition ahead of the store could still come true: nothing
    // certain is said, and nothing is opened.
    let ahead = OpenEpoch {
        after: RunEpoch::new(7),
        ..opening_command(&store)
    };
    assert_eq!(
        store.open_epoch(&ahead).expect("healthy epoch read"),
        OpeningOutcome::Unknown
    );
    // A command for another data set opens nothing here.
    let elsewhere = OpenEpoch {
        incarnation: StoreIncarnation::new(2),
        ..opening_command(&store)
    };
    assert_eq!(
        store.open_epoch(&elsewhere).expect("healthy epoch read"),
        OpeningOutcome::RejectedCertain(OpeningRejection::ForeignIncarnation)
    );
    assert_eq!(store.snapshot().expect("healthy durable snapshot"), before);
}

/// The command that opened the last epoch, delivered again, gets the
/// same commit and changes nothing: a rejection would say that it had
/// opened nothing.
pub fn an_opening_delivered_again_returns_its_commit<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let mut store = make(INCARNATION, fixture_limits());
    open(&mut store);
    let command = opening_command(&store);
    let committed = OpeningOutcome::Committed(EpochOpened {
        incarnation: INCARNATION,
        epoch: RunEpoch::new(2),
    });
    assert_eq!(
        store.open_epoch(&command).expect("healthy epoch read"),
        committed
    );
    let before = store.snapshot().expect("healthy durable snapshot");
    for _delivery in 0..3 {
        assert_eq!(
            store.open_epoch(&command).expect("healthy epoch read"),
            committed
        );
        assert_eq!(store.snapshot().expect("healthy durable snapshot"), before);
    }
}

/// An attempt designates one command. The start-up that opened the
/// last epoch and asks for the next one under the same identity broke
/// the precondition of its nonce: the store opens nothing and says so
/// for certain, and the first command is still answered as the opener.
pub fn the_opener_of_the_last_epoch_cannot_open_the_next_under_its_identity<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let mut store = make(INCARNATION, fixture_limits());
    open(&mut store);
    let opener = opening_command(&store);
    let committed = OpeningOutcome::Committed(EpochOpened {
        incarnation: INCARNATION,
        epoch: RunEpoch::new(2),
    });
    assert_eq!(
        store.open_epoch(&opener).expect("healthy epoch read"),
        committed
    );
    let before = store.snapshot().expect("healthy durable snapshot");
    let reused = OpenEpoch {
        after: RunEpoch::new(2),
        ..opener
    };
    assert_eq!(
        store.open_epoch(&reused).expect("healthy epoch read"),
        OpeningOutcome::RejectedCertain(OpeningRejection::AttemptReused)
    );
    assert_eq!(store.snapshot().expect("healthy durable snapshot"), before);
    assert_eq!(
        store.open_epoch(&opener).expect("healthy epoch read"),
        committed
    );
    // Any other identity opens the next epoch.
    assert_eq!(open(&mut store), RunEpoch::new(3));
}

/// A restarted process starts its tickets again and reads the same
/// value as its dead predecessor. Their first commands differ by the
/// start-up that made them: the store commits one, rejects the other,
/// and answers each its own result, in both orders.
pub fn the_first_command_of_a_restart_is_not_its_predecessors<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let restart = StartupNonce::new([0x5A; 16]);
    for predecessor_first in [true, false] {
        let mut store = make(INCARNATION, fixture_limits());
        let predecessor = opening_command(&store);
        let successor = OpenEpoch {
            attempt: attempt(restart, 1),
            ..predecessor
        };
        let (first, second) = if predecessor_first {
            (predecessor, successor)
        } else {
            (successor, predecessor)
        };
        let committed = OpeningOutcome::Committed(EpochOpened {
            incarnation: INCARNATION,
            epoch: RunEpoch::new(1),
        });
        let moved = OpeningOutcome::RejectedCertain(OpeningRejection::PredecessorMoved);
        assert_eq!(
            store.open_epoch(&first).expect("healthy epoch read"),
            committed
        );
        assert_eq!(
            store.open_epoch(&second).expect("healthy epoch read"),
            moved
        );
        // Each one again, in the other order: the same answers.
        assert_eq!(
            store.open_epoch(&second).expect("healthy epoch read"),
            moved
        );
        assert_eq!(
            store.open_epoch(&first).expect("healthy epoch read"),
            committed
        );
        assert_eq!(
            store
                .ledger()
                .expect("healthy durable ledger")
                .last_opened(),
            RunEpoch::new(1)
        );
        assert_eq!(
            store.ledger().expect("healthy durable ledger").opened_by(),
            Some(first.attempt)
        );
    }
}

/// Nothing executes without an open epoch, and a command prepared for
/// an epoch that is not open yet stays rejected once that epoch opens.
pub fn a_command_of_an_unopened_epoch_never_executes<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let mut world = World::new(make);
    let early = admission_in(
        RunEpoch::new(2),
        world.route_revision,
        world.producer,
        1,
        7,
        b"payload",
        &[world.b, world.c],
    );
    let rejected = StoreOutcome::RejectedCertain(RejectReason::PreconditionFailed);
    assert_eq!(outcome(world.store.apply(&early).reply), rejected);
    assert_eq!(open(&mut world.store), RunEpoch::new(2));
    let applied = world.store.apply(&early);
    assert_eq!(outcome(applied.reply), rejected);
    assert!(!applied.committed_now);
    assert!(
        world
            .store
            .snapshot()
            .expect("healthy durable snapshot")
            .publications
            .is_empty()
    );
}

/// An admission commits publication, payload, obligations, inbound
/// slot, accounts and receipt together (DEM-011, ARC-009).
pub fn an_admission_commits_everything_together<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let mut world = World::new(make);
    let command = world.admission(1, 7, b"payload", &[world.b, world.c]);
    let facts = admitted(&command);
    let applied = world.store.apply(&command);
    assert_eq!(outcome(applied.reply), StoreOutcome::Committed(facts));
    assert!(applied.committed_now);

    let state = world.store.snapshot().expect("healthy durable snapshot");
    let publication = publication_id(world.epoch, 1);
    let record = &state.publications[&publication];
    assert_eq!(record.source, world.producer);
    assert_eq!(record.topic, TOPIC);
    assert_eq!(record.qos, Qos::ExactlyOnce);
    assert_eq!(state.payloads[&record.payload].bytes, b"payload");
    assert_eq!(state.payloads[&record.payload].references, 2);
    let obligations: Vec<_> = state
        .obligations
        .iter()
        .map(|(delivery, obligation)| {
            assert_eq!(delivery.publication, publication);
            assert_eq!(obligation.cause, DeliveryCause::LiveExact);
            assert_eq!(obligation.qos, Qos::AtLeastOnce);
            assert_eq!(obligation.phase, ObligationPhase::Pending);
            (delivery.index, obligation.target)
        })
        .collect();
    assert_eq!(obligations, [(0, world.b), (1, world.c)]);
    let slot = (world.producer, InboundPacketId::new(7).expect("non-zero"));
    assert_eq!(state.inbound[&slot].publication, publication);
    assert_eq!(state.inbound[&slot].phase, InboundPhase::AwaitPubrel);
    assert_eq!(
        state.accounts,
        Accounts {
            payload_bytes: PayloadBytes::new(7),
            references: References::new(2),
            exchange_slots: ExchangeSlots::new(1),
            receipt_slots: ReceiptSlots::new(1),
        }
    );
    let receipt = &state.operations[&operation_id(world.epoch, 1)];
    assert_eq!(receipt.decision, Decision::Committed(facts));
    let mut canonical = Vec::new();
    command
        .recipe
        .get()
        .encode_canonical(&mut canonical)
        .expect("well formed");
    assert_eq!(receipt.recipe, canonical, "the exact canonical bytes");
}

/// A committed command presented again returns the same result and has
/// no second effect (V3-006).
pub fn a_repeated_command_returns_its_result_without_a_second_effect<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let mut world = World::new(make);
    let command = world.admission(1, 7, b"payload", &[world.b, world.c]);
    let first = world.store.apply(&command);
    let before = world.store.snapshot().expect("healthy durable snapshot");
    let second = world.store.apply(&command);
    assert_eq!(second.reply, first.reply);
    assert!(!second.committed_now);
    assert_eq!(
        world.store.snapshot().expect("healthy durable snapshot"),
        before
    );
}

/// A guard that fails leaves no admitted subset, whichever guard it is
/// and however many rows were prepared before it (ADM-002, MU-24).
pub fn a_failed_guard_leaves_no_admitted_subset<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let precondition = StoreOutcome::RejectedCertain(RejectReason::PreconditionFailed);

    // The last target names a revision the session does not have.
    let mut world = World::new(make);
    let before = world.store.snapshot().expect("healthy durable snapshot");
    let stale = altered(
        &world.admission(1, 7, b"payload", &[world.b, world.c]),
        |admit| admit.targets.as_mut_slice()[1].session_revision = Revision::new(9),
    );
    assert_eq!(outcome(world.store.apply(&stale).reply), precondition);
    assert_eq!(
        without_receipts(world.store.snapshot().expect("healthy durable snapshot")),
        without_receipts(before)
    );

    // The last account checked is short. It is the receipt account, so
    // the rejection itself cannot be recorded: nothing certain is said,
    // and nothing at all is written.
    let mut tight = fixture_limits();
    tight.receipt_slots = ReceiptSlots::new(0);
    let mut world = World::with_limits(make, tight);
    let before = world.store.snapshot().expect("healthy durable snapshot");
    let command = world.admission(1, 7, b"payload", &[world.b, world.c]);
    assert_eq!(
        outcome(world.store.apply(&command).reply),
        StoreOutcome::PendingOrUnknown
    );
    assert_eq!(
        world.store.snapshot().expect("healthy durable snapshot"),
        before
    );

    // The inbound slot is taken by an earlier admission: a newcomer on
    // the same packet, under a fresh exchange, admits nothing.
    let mut world = World::new(make);
    let first = world.admission(1, 7, b"payload", &[world.b, world.c]);
    assert!(world.store.apply(&first).committed_now);
    let before = world.store.snapshot().expect("healthy durable snapshot");
    let hidden = world.admission(2, 7, b"payload", &[world.b, world.c]);
    assert_eq!(outcome(world.store.apply(&hidden).reply), precondition);
    let after = world.store.snapshot().expect("healthy durable snapshot");
    assert_eq!(after.publications, before.publications);
    assert_eq!(after.obligations, before.obligations);
    assert_eq!(after.payloads, before.payloads);
    assert_eq!(after.inbound, before.inbound);
    // The only account that moved is the slot of the rejection receipt.
    assert_eq!(
        after.accounts,
        Accounts {
            receipt_slots: ReceiptSlots::new(before.accounts.receipt_slots.get() + 1),
            ..before.accounts
        }
    );
}

/// Each account refuses an admission it cannot hold, naming itself. The
/// rejection is recorded and holds its receipt slot, except when the
/// receipt account is the one that is short: then nothing can be
/// recorded, and nothing certain is said.
pub fn each_account_is_checked<S: DurableStore>(make: &dyn Fn(StoreIncarnation, Limits) -> S) {
    type Tighten = fn(&mut Limits);
    let cases: [(Tighten, Resource); 3] = [
        (
            |limits| limits.payload_bytes = PayloadBytes::new(6),
            Resource::PayloadBytes,
        ),
        (
            |limits| limits.references = References::new(1),
            Resource::References,
        ),
        (
            |limits| limits.exchange_slots = ExchangeSlots::new(0),
            Resource::ExchangeSlots,
        ),
    ];
    for (tighten, resource) in cases {
        let mut limits = fixture_limits();
        tighten(&mut limits);
        let mut world = World::with_limits(make, limits);
        let command = world.admission(1, 7, b"payload", &[world.b, world.c]);
        assert_eq!(
            outcome(world.store.apply(&command).reply),
            StoreOutcome::RejectedCertain(RejectReason::CapacityExceeded(resource))
        );
        assert_eq!(
            world
                .store
                .snapshot()
                .expect("healthy durable snapshot")
                .accounts,
            Accounts::RECEIPT
        );
    }
    let mut limits = fixture_limits();
    limits.receipt_slots = ReceiptSlots::new(0);
    let mut world = World::with_limits(make, limits);
    let before = world.store.snapshot().expect("healthy durable snapshot");
    let command = world.admission(1, 7, b"payload", &[world.b, world.c]);
    assert_eq!(
        outcome(world.store.apply(&command).reply),
        StoreOutcome::PendingOrUnknown
    );
    assert_eq!(
        world.store.snapshot().expect("healthy durable snapshot"),
        before
    );
    // An account at its limit exactly is not exceeded.
    let mut limits = fixture_limits();
    limits.payload_bytes = PayloadBytes::new(7);
    limits.references = References::new(2);
    limits.exchange_slots = ExchangeSlots::new(1);
    limits.receipt_slots = ReceiptSlots::new(1);
    let mut world = World::with_limits(make, limits);
    let command = world.admission(1, 7, b"payload", &[world.b, world.c]);
    assert!(world.store.apply(&command).committed_now);
}

/// A rejection stays a rejection after quota is released; the same
/// request prepared again under a new identity is admitted (P1-11,
/// MU-08, V3-006).
pub fn a_rejection_stays_a_rejection_after_quota_is_released<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let mut limits = fixture_limits();
    limits.references = References::new(2);
    let mut world = World::with_limits(make, limits);
    let holder = world.admission(1, 7, b"payload", &[world.b, world.c]);
    assert!(world.store.apply(&holder).committed_now);

    let refused = world.admission(2, 8, b"payload", &[world.producer]);
    let rejected =
        StoreOutcome::RejectedCertain(RejectReason::CapacityExceeded(Resource::References));
    assert_eq!(outcome(world.store.apply(&refused).reply), rejected);

    // Quota is released: the obligation towards `c` ends with `c`.
    assert_eq!(
        world
            .store
            .admin(AdminCommand::DeleteSession { session: world.c }),
        Ok(AdminDone::SessionDeleted)
    );
    let released = world.store.snapshot().expect("healthy durable snapshot");
    assert_eq!(released.accounts.references, References::new(1));
    // `c` left the route with its session.
    world.route_revision = released.route_revision;

    let before = world.store.snapshot().expect("healthy durable snapshot");
    let again = world.store.apply(&refused);
    assert_eq!(outcome(again.reply), rejected);
    assert!(!again.committed_now);
    assert_eq!(
        world.store.snapshot().expect("healthy durable snapshot"),
        before
    );
    assert_eq!(
        world.store.resolve_and_fence(operation_id(world.epoch, 2)),
        settled(rejected)
    );

    // A new preparation of the same request gets a new identity.
    let prepared_again = world.admission(3, 8, b"payload", &[world.producer]);
    assert!(world.store.apply(&prepared_again).committed_now);
}

/// The same operation with another recipe is a violation with no
/// second effect, whether the first one committed or was rejected
/// (DEM-009, MU-03).
pub fn another_recipe_under_the_same_operation_is_a_violation<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let violation = StoreReply::Violation(StoreViolation::RecipeMismatch);
    let mut world = World::new(make);
    let command = world.admission(1, 7, b"payload", &[world.b, world.c]);
    assert!(world.store.apply(&command).committed_now);
    let before = world.store.snapshot().expect("healthy durable snapshot");

    let changes: [fn(&mut AdmitPublication); 4] = [
        |admit| {
            admit.targets.pop();
            admit.costs.references = References::new(1);
        },
        |admit| admit.retain = true,
        |admit| admit.topic = SharedBytes::copy_from(b"p1/other", 256).expect("fits"),
        |admit| admit.targets.as_mut_slice()[0].effective_qos = Qos::AtMostOnce,
    ];
    for change in changes {
        let other = altered(&command, change);
        let applied = world.store.apply(&other);
        assert_eq!(applied.reply, violation);
        assert!(!applied.committed_now);
        assert_eq!(
            world.store.snapshot().expect("healthy durable snapshot"),
            before
        );
    }
    // The same recipe with other payload bytes of the same length.
    let other_bytes = Command {
        recipe: command.recipe.clone(),
        payload: Some(SharedBytes::copy_from(b"PAYLOAD", 1024).expect("fits")),
    };
    assert_eq!(world.store.apply(&other_bytes).reply, violation);
    assert_eq!(
        world.store.snapshot().expect("healthy durable snapshot"),
        before
    );

    // After a rejection as well.
    let rejected = altered(&world.admission(2, 8, b"payload", &[world.b]), |admit| {
        admit.source.session_revision = Revision::new(9);
    });
    assert!(matches!(
        world.store.apply(&rejected).reply,
        StoreReply::Outcome(StoreOutcome::RejectedCertain(_))
    ));
    let before = world.store.snapshot().expect("healthy durable snapshot");
    let repaired = world.admission(2, 8, b"payload", &[world.b]);
    assert_eq!(world.store.apply(&repaired).reply, violation);
    assert_eq!(
        world.store.snapshot().expect("healthy durable snapshot"),
        before
    );
}

/// `ResolveAndFence` returns the result of a committed operation, and
/// fences an absent one durably before it announces a rejection: the
/// command that arrives afterwards is refused with no effect (DEM-010).
pub fn resolve_returns_a_commit_and_fences_an_absence<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let mut world = World::new(make);
    let committed = world.admission(1, 7, b"payload", &[world.b, world.c]);
    assert!(world.store.apply(&committed).committed_now);
    assert_eq!(
        world.store.resolve_and_fence(operation_id(world.epoch, 1)),
        Resolved {
            outcome: StoreOutcome::Committed(admitted(&committed)),
            fenced_now: false
        }
    );

    let absent = world.admission(2, 8, b"payload", &[world.b]);
    let fenced = StoreOutcome::RejectedCertain(RejectReason::Fenced);
    assert_eq!(
        world.store.resolve_and_fence(operation_id(world.epoch, 2)),
        Resolved {
            outcome: fenced,
            fenced_now: true
        }
    );
    let before = world.store.snapshot().expect("healthy durable snapshot");
    assert!(before.fences.contains(&operation_id(world.epoch, 2)));

    let late = world.store.apply(&absent);
    assert_eq!(outcome(late.reply), fenced);
    assert!(!late.committed_now);
    assert_eq!(
        world.store.snapshot().expect("healthy durable snapshot"),
        before
    );
    // The fence is established once.
    assert_eq!(
        world.store.resolve_and_fence(operation_id(world.epoch, 2)),
        Resolved {
            outcome: fenced,
            fenced_now: false
        }
    );
    assert_eq!(
        world.store.snapshot().expect("healthy durable snapshot"),
        before
    );
}

/// `ReleaseInbound` ends the exchange of a committed admission once,
/// and only that exchange.
pub fn a_release_ends_the_inbound_exchange<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let precondition = StoreOutcome::RejectedCertain(RejectReason::PreconditionFailed);
    let mut world = World::new(make);
    let command = world.admission(1, 7, b"payload", &[world.b, world.c]);
    assert!(world.store.apply(&command).committed_now);

    // Another packet, then the right packet under another exchange.
    assert_eq!(
        outcome(world.store.apply(&world.release(2, 8, 1)).reply),
        precondition
    );
    assert_eq!(
        outcome(world.store.apply(&world.release(3, 7, 99)).reply),
        precondition
    );
    assert_eq!(
        world
            .store
            .snapshot()
            .expect("healthy durable snapshot")
            .inbound
            .len(),
        1
    );

    let release = world.release(4, 7, 1);
    let released = StoreOutcome::Committed(CommitFacts::InboundReleased {
        exchange: ExchangeId {
            session: world.producer,
            generation: ExchangeGeneration::new(1),
        },
    });
    let applied = world.store.apply(&release);
    assert_eq!(outcome(applied.reply), released);
    assert!(applied.committed_now);
    let state = world.store.snapshot().expect("healthy durable snapshot");
    assert!(state.inbound.is_empty());
    assert_eq!(state.accounts.exchange_slots, ExchangeSlots::ZERO);
    // The release creates no distribution and ends none.
    assert_eq!(state.obligations.len(), 2);
    assert_eq!(state.publications.len(), 1);

    let again = world.store.apply(&release);
    assert_eq!(outcome(again.reply), released);
    assert!(!again.committed_now);
    assert_eq!(
        world.store.snapshot().expect("healthy durable snapshot"),
        state
    );

    // A second release of the exchange, under a new identity, finds no
    // slot to end.
    assert_eq!(
        outcome(world.store.apply(&world.release(5, 7, 1)).reply),
        precondition
    );
}

/// Every account equals what the state holds: a receipt slot per receipt
/// and per fence, an exchange slot per inbound marker or outgoing claim, a reference per
/// obligation, the bytes of the payloads kept. Returns the first account
/// that differs.
pub fn accounts_match_what_is_held(state: &DurableState) -> Result<(), &'static str> {
    let count = |n: usize| u64::try_from(n).expect("a count fits u64");
    let held = Accounts {
        payload_bytes: PayloadBytes::new(
            state
                .payloads
                .values()
                .map(|payload| count(payload.bytes.len()))
                .sum(),
        ),
        references: References::new(count(state.obligations.len())),
        exchange_slots: ExchangeSlots::new(count(state.inbound.len() + state.outbound.len())),
        receipt_slots: ReceiptSlots::new(count(state.operations.len() + state.fences.len())),
    };
    let accounts = state.accounts;
    if accounts.payload_bytes != held.payload_bytes {
        return Err("payload bytes");
    }
    if accounts.references != held.references {
        return Err("references");
    }
    if accounts.exchange_slots != held.exchange_slots {
        return Err("exchange slots");
    }
    if accounts.receipt_slots != held.receipt_slots {
        return Err("receipt slots");
    }
    Ok(())
}

fn inbound_released<S: DurableStore>(world: &World<S>) -> StoreOutcome {
    StoreOutcome::Committed(CommitFacts::InboundReleased {
        exchange: ExchangeId {
            session: world.producer,
            generation: ExchangeGeneration::new(1),
        },
    })
}

/// A release writes a receipt, and that receipt holds a receipt slot like
/// every other: the slots counted are exactly the receipts and fences
/// held (audit F1, case a).
pub fn a_release_holds_the_slot_of_its_receipt<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let mut world = World::new(make);
    let command = world.admission(1, 7, b"payload", &[world.b]);
    assert!(world.store.apply(&command).committed_now);
    let applied = world.store.apply(&world.release(2, 7, 1));
    assert_eq!(outcome(applied.reply), inbound_released(&world));
    let state = world.store.snapshot().expect("healthy durable snapshot");
    let held = state.operations.len() + state.fences.len();
    assert_eq!(held, 2);
    assert_eq!(
        state.accounts.receipt_slots,
        ReceiptSlots::new(u64::try_from(held).expect("small"))
    );
}

/// With no receipt slot left, a release is not recorded, and nothing
/// changes, the inbound marker included: no slot for its receipt, none
/// for a rejection either (audit F1, case b).
pub fn a_release_without_a_receipt_slot_changes_nothing<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let mut limits = fixture_limits();
    limits.receipt_slots = ReceiptSlots::new(1);
    let mut world = World::with_limits(make, limits);
    let command = world.admission(1, 7, b"payload", &[world.b]);
    assert!(world.store.apply(&command).committed_now);
    let full = world.store.snapshot().expect("healthy durable snapshot");
    assert_eq!(full.inbound.len(), 1);
    let applied = world.store.apply(&world.release(2, 7, 1));
    assert_eq!(outcome(applied.reply), StoreOutcome::PendingOrUnknown);
    assert!(!applied.committed_now);
    assert!(!applied.wrote);
    assert_eq!(
        world.store.snapshot().expect("healthy durable snapshot"),
        full
    );
}

/// Collecting a closed epoch gives back exactly the slots its receipts
/// held, the receipt of a release included (audit F1, case c).
pub fn collecting_an_epoch_gives_back_the_slots_of_its_releases<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let mut world = World::new(make);
    let command = world.admission(1, 7, b"payload", &[world.b]);
    assert!(world.store.apply(&command).committed_now);
    let release = world.release(2, 7, 1);
    assert_eq!(
        outcome(world.store.apply(&release).reply),
        inbound_released(&world)
    );
    let closed = world.epoch;
    open(&mut world.store);
    assert_eq!(
        world
            .store
            .admin(AdminCommand::CollectClosedEpochs { through: closed }),
        Ok(AdminDone::Collected)
    );
    let state = world.store.snapshot().expect("healthy durable snapshot");
    assert!(state.operations.is_empty());
    assert_eq!(state.accounts.receipt_slots, ReceiptSlots::ZERO);
}

/// Deleting the producing session deletes no obligation of another
/// session; an obligation ends by its destination, with its accounts
/// (DEM-011, MU-07).
pub fn deleting_the_producer_deletes_no_obligation_of_another_session<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let mut world = World::new(make);
    let command = world.admission(1, 7, b"payload", &[world.b, world.c]);
    assert!(world.store.apply(&command).committed_now);
    let before = world.store.snapshot().expect("healthy durable snapshot");

    assert_eq!(
        world.store.admin(AdminCommand::DeleteSession {
            session: world.producer
        }),
        Ok(AdminDone::SessionDeleted)
    );
    let state = world.store.snapshot().expect("healthy durable snapshot");
    assert!(!state.sessions.contains_key(&world.producer));
    assert_eq!(state.obligations, before.obligations);
    assert_eq!(state.publications, before.publications);
    assert_eq!(state.payloads, before.payloads);
    assert_eq!(state.accounts.references, References::new(2));
    assert_eq!(state.accounts.payload_bytes, PayloadBytes::new(7));
    // Its own inbound slot went with it.
    assert!(state.inbound.is_empty());
    assert_eq!(state.accounts.exchange_slots, ExchangeSlots::ZERO);

    // Deleting a destination ends its obligation and nothing else.
    assert_eq!(
        world
            .store
            .admin(AdminCommand::DeleteSession { session: world.b }),
        Ok(AdminDone::SessionDeleted)
    );
    let state = world.store.snapshot().expect("healthy durable snapshot");
    let left: Vec<_> = state
        .obligations
        .values()
        .map(|obligation| obligation.target)
        .collect();
    assert_eq!(left, [world.c]);
    assert_eq!(state.accounts.references, References::new(1));
    assert_eq!(state.accounts.payload_bytes, PayloadBytes::new(7));

    // The payload goes with its last reference.
    assert_eq!(
        world
            .store
            .admin(AdminCommand::DeleteSession { session: world.c }),
        Ok(AdminDone::SessionDeleted)
    );
    let state = world.store.snapshot().expect("healthy durable snapshot");
    assert!(state.obligations.is_empty());
    assert!(state.payloads.is_empty());
    assert_eq!(state.accounts.references, References::ZERO);
    assert_eq!(state.accounts.payload_bytes, PayloadBytes::ZERO);
    assert_eq!(
        world
            .store
            .admin(AdminCommand::DeleteSession { session: world.c }),
        Err(AdminRefused::UnknownSession)
    );
}

/// Opening a new epoch closes the older one for execution and erases
/// none of its results: a committed operation still answers its
/// result, an absent one is refused and never entered again (DEM-012).
pub fn an_older_epoch_is_not_entered_again<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let mut world = World::new(make);
    let committed = world.admission(1, 7, b"payload", &[world.b, world.c]);
    assert!(world.store.apply(&committed).committed_now);
    let never_arrived = world.admission(2, 8, b"payload", &[world.b]);
    let old_epoch = world.epoch;

    let new_epoch = open(&mut world.store);
    assert!(new_epoch > old_epoch);
    let before = world.store.snapshot().expect("healthy durable snapshot");

    let repeated = world.store.apply(&committed);
    assert_eq!(
        outcome(repeated.reply),
        StoreOutcome::Committed(admitted(&committed))
    );
    assert!(!repeated.committed_now);

    let closed = StoreOutcome::RejectedCertain(RejectReason::EpochClosed);
    let late = world.store.apply(&never_arrived);
    assert_eq!(outcome(late.reply), closed);
    assert!(!late.committed_now);
    assert_eq!(
        world.store.resolve_and_fence(operation_id(old_epoch, 2)),
        settled(closed)
    );
    assert_eq!(
        world.store.snapshot().expect("healthy durable snapshot"),
        before
    );

    // The same request prepared in the new epoch is admitted, and the
    // obligations of the older epoch are still there.
    world.epoch = new_epoch;
    let prepared_again = world.admission(2, 8, b"payload", &[world.b]);
    assert!(world.store.apply(&prepared_again).committed_now);
    assert_eq!(
        world
            .store
            .snapshot()
            .expect("healthy durable snapshot")
            .obligations
            .len(),
        3
    );
}

/// The fourth outcome: once the receipts of a closed epoch are
/// collected, its identities can never execute and the store does not
/// pretend to know what they did (DEM-010, ARC-019).
pub fn a_collected_identity_is_retired_not_rejected<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let mut world = World::new(make);
    let committed = world.admission(1, 7, b"payload", &[world.b, world.c]);
    assert!(world.store.apply(&committed).committed_now);
    let never_arrived = world.admission(2, 8, b"payload", &[world.b]);
    let old_epoch = world.epoch;

    // An open epoch is not collected.
    assert_eq!(
        world
            .store
            .admin(AdminCommand::CollectClosedEpochs { through: old_epoch }),
        Err(AdminRefused::EpochNotClosed)
    );
    open(&mut world.store);
    assert_eq!(
        world
            .store
            .admin(AdminCommand::CollectClosedEpochs { through: old_epoch }),
        Ok(AdminDone::Collected)
    );
    let state = world.store.snapshot().expect("healthy durable snapshot");
    assert!(state.operations.is_empty());
    assert_eq!(state.accounts.receipt_slots, ReceiptSlots::ZERO);
    // Collecting receipts ends no responsibility.
    assert_eq!(state.obligations.len(), 2);

    let retired = StoreOutcome::RetiredOutcomeUnavailable;
    for command in [&committed, &never_arrived] {
        let applied = world.store.apply(command);
        assert_eq!(outcome(applied.reply), retired);
        assert!(!applied.committed_now);
        assert_eq!(
            world.store.resolve_and_fence(command.recipe.operation()),
            settled(retired)
        );
    }
    assert_eq!(
        world.store.snapshot().expect("healthy durable snapshot"),
        state
    );
}

/// An identity of another data set never executes here.
pub fn an_identity_of_another_data_set_is_refused<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let mut world = World::new(make);
    let foreign = OperationId::new(
        StoreIncarnation::new(2),
        world.epoch,
        OWNER,
        OperationSeq::new(1),
    );
    let before = world.store.snapshot().expect("healthy durable snapshot");
    assert_eq!(
        world.store.resolve_and_fence(foreign),
        settled(StoreOutcome::RejectedCertain(
            RejectReason::PreconditionFailed
        ))
    );
    assert_eq!(
        world.store.snapshot().expect("healthy durable snapshot"),
        before
    );
}

/// Recovery reads bounded pages from durable state only: the epoch, the
/// sessions, the inbound slot that links a client replay to its
/// publication, and the operations of the older epoch (DEM-012).
pub fn recovery_reads_bounded_pages_of_durable_facts<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let mut world = World::new(make);
    let committed = world.admission(1, 7, b"payload", &[world.b, world.c]);
    assert!(world.store.apply(&committed).committed_now);
    let rejected = altered(&world.admission(2, 8, b"payload", &[world.b]), |admit| {
        admit.source.session_revision = Revision::new(9);
    });
    world.store.apply(&rejected);
    let fenced = operation_id(world.epoch, 3);
    world.store.resolve_and_fence(fenced);
    let old_epoch = world.epoch;
    let new_epoch = open(&mut world.store);

    let whole = world.store.recover(None, 64).expect("one page");
    assert_eq!(whole.next, None);
    let facts = whole.facts.as_slice();
    assert_eq!(
        facts[0],
        RecoveredFact::Epoch {
            incarnation: INCARNATION,
            epoch: new_epoch
        }
    );
    let sessions: Vec<_> = facts
        .iter()
        .filter_map(|fact| match fact {
            RecoveredFact::Session {
                session, client_id, ..
            } => Some((*session, client_id.as_slice().to_vec())),
            _ => None,
        })
        .collect();
    assert_eq!(
        sessions,
        [
            (world.producer, b"p1-producer".to_vec()),
            (world.b, b"p1-b".to_vec()),
            (world.c, b"p1-c".to_vec()),
        ]
    );
    assert!(facts.contains(&RecoveredFact::InboundExchange {
        exchange: ExchangeId {
            session: world.producer,
            generation: ExchangeGeneration::new(1),
        },
        packet_id: InboundPacketId::new(7).expect("non-zero"),
        publication: publication_id(old_epoch, 1),
        phase: InboundPhase::AwaitPubrel,
    }));
    let operations: Vec<_> = facts
        .iter()
        .filter_map(|fact| match fact {
            RecoveredFact::Operation {
                operation,
                knowledge,
            } => Some((*operation, *knowledge)),
            _ => None,
        })
        .collect();
    assert_eq!(
        operations,
        [
            (
                operation_id(old_epoch, 1),
                Knowledge::Committed(admitted(&committed))
            ),
            (
                operation_id(old_epoch, 2),
                Knowledge::RejectedCertain(RejectReason::PreconditionFailed)
            ),
            (fenced, Knowledge::RejectedCertain(RejectReason::Fenced)),
        ]
    );
    let outbound: Vec<_> = facts
        .iter()
        .filter_map(|fact| match fact {
            RecoveredFact::OutboundDelivery {
                delivery,
                target,
                target_revision,
                qos: Qos::AtLeastOnce,
                claim,
                ..
            } => Some((*delivery, *target, *target_revision, *claim)),
            _ => None,
        })
        .collect();
    assert_eq!(
        outbound,
        [
            (
                DeliveryId {
                    publication: publication_id(old_epoch, 1),
                    index: 0
                },
                world.b,
                Revision::new(1),
                None
            ),
            (
                DeliveryId {
                    publication: publication_id(old_epoch, 1),
                    index: 1
                },
                world.c,
                Revision::new(1),
                None
            ),
        ]
    );
    // The epoch, limits, route table and two targets, three sessions,
    // one inbound exchange, two queued deliveries and three operations.
    assert_eq!(facts.len(), 1 + 1 + 1 + 2 + 3 + 1 + 2 + 3);

    // The same inventory in pages of every size, never above the limit.
    for limit in 1..=facts.len() + 1 {
        let mut collected = Vec::new();
        let mut cursor = None;
        loop {
            let page = world.store.recover(cursor, limit).expect("a page");
            assert!(page.facts.len() <= limit);
            assert!(!page.facts.is_empty());
            collected.extend_from_slice(page.facts.as_slice());
            match page.next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        assert_eq!(collected, facts, "pages of {limit}");
    }

    assert_eq!(
        world.store.recover(None, 0),
        Err(RecoveryRefused::EmptyPage)
    );
    // A cursor does not survive a commit: the frontier moved.
    let first = world.store.recover(None, 2).expect("a page");
    let cursor = first.next.expect("more than two facts");
    world.epoch = new_epoch;
    let command = world.admission(4, 9, b"payload", &[world.b]);
    assert!(world.store.apply(&command).committed_now);
    assert_eq!(
        world.store.recover(Some(cursor), 2),
        Err(RecoveryRefused::FrontierMoved)
    );
}

/// Administration refuses what it cannot do, and changes nothing then.
pub fn administration_refusals_change_nothing<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let mut limits = fixture_limits();
    limits.max_sessions = 3;
    limits.max_route_targets = 2;
    let mut world = World::with_limits(make, limits);
    let before = world.store.snapshot().expect("healthy durable snapshot");
    let refusals = [
        (
            AdminCommand::CreateSession {
                client_id: b"p1-b".to_vec(),
            },
            AdminRefused::DuplicateClientId,
        ),
        (
            AdminCommand::CreateSession {
                client_id: b"p1-d".to_vec(),
            },
            AdminRefused::TableFull,
        ),
        (
            AdminCommand::AddRoute {
                topic: TOPIC.to_vec(),
                session: world.b,
                qos: Qos::AtLeastOnce,
            },
            AdminRefused::DuplicateTarget,
        ),
        (
            AdminCommand::AddRoute {
                topic: TOPIC.to_vec(),
                session: world.producer,
                qos: Qos::AtLeastOnce,
            },
            AdminRefused::TableFull,
        ),
        (
            AdminCommand::AddRoute {
                topic: TOPIC.to_vec(),
                session: SessionId::new(99),
                qos: Qos::AtLeastOnce,
            },
            AdminRefused::UnknownSession,
        ),
        (
            AdminCommand::AddRoute {
                topic: vec![b'x'; 257],
                session: world.b,
                qos: Qos::AtLeastOnce,
            },
            AdminRefused::TopicTooLong,
        ),
        (
            AdminCommand::DeleteSession {
                session: SessionId::new(99),
            },
            AdminRefused::UnknownSession,
        ),
    ];
    for (command, refusal) in refusals {
        assert_eq!(
            world.store.admin(command.clone()),
            Err(refusal),
            "{command:?}"
        );
        assert_eq!(
            world.store.snapshot().expect("healthy durable snapshot"),
            before
        );
    }
}

/// An admission towards no target, a topic without a route, keeps no
/// payload: nothing would ever release its bytes. It is charged and
/// released in the same transaction, so such admissions never exhaust
/// the payload account.
pub fn an_admission_without_targets_holds_no_payload_bytes<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let mut limits = fixture_limits();
    limits.payload_bytes = PayloadBytes::new(1500);
    limits.receipt_slots = ReceiptSlots::new(16);
    let mut world = World::with_limits(make, limits);
    let payload = [7u8; 1000];
    for round in 1..=4u64 {
        let admit = 2 * round - 1;
        let packet = u16::try_from(round).expect("small");
        let command = world.admission(admit, packet, &payload, &[]);
        let applied = world.store.apply(&command);
        assert_eq!(
            outcome(applied.reply),
            StoreOutcome::Committed(admitted(&command)),
            "round {round}"
        );
        let state = world.store.snapshot().expect("healthy durable snapshot");
        assert_eq!(state.accounts.payload_bytes, PayloadBytes::ZERO);
        assert!(state.payloads.is_empty());
        let release = world.release(admit + 1, packet, admit);
        assert!(world.store.apply(&release).committed_now, "round {round}");
    }
    // A payload identity still names one payload: another admission
    // under the identity of a payload that was not kept is refused.
    let reused = altered(&world.admission(9, 9, &payload, &[]), |admit| {
        admit.payload.id = PayloadId::new(INCARNATION, world.epoch, OWNER, PayloadSeq::new(1));
    });
    assert_eq!(
        outcome(world.store.apply(&reused).reply),
        StoreOutcome::RejectedCertain(RejectReason::PreconditionFailed)
    );
    // With a target the bytes are kept and charged as before.
    let kept = world.admission(10, 10, &payload, &[world.b]);
    assert!(world.store.apply(&kept).committed_now);
    assert_eq!(
        world
            .store
            .snapshot()
            .expect("healthy durable snapshot")
            .accounts
            .payload_bytes,
        PayloadBytes::new(1000)
    );
}

/// Once PUBREL ends an unrouted exchange, no live fact needs its publication
/// metadata. The admission receipt still resolves replay and forbids reusing
/// either immutable identity; reusing the packet identifier starts a new exchange.
pub fn a_completed_unrouted_exchange_releases_its_publication<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let mut world = World::new(make);
    let admission = world.admission(1, 7, b"payload", &[]);
    assert!(world.store.apply(&admission).committed_now);
    let admitted_state = world.store.snapshot().expect("healthy snapshot");
    assert_eq!(admitted_state.publications.len(), 1);
    assert_eq!(admitted_state.inbound.len(), 1);
    assert!(admitted_state.payloads.is_empty());
    assert!(world.store.apply(&world.release(2, 7, 1)).committed_now);
    let completed = world.store.snapshot().expect("healthy snapshot");
    assert!(
        completed.publications.is_empty(),
        "a completed unrouted exchange must not retain publication metadata"
    );
    assert!(completed.inbound.is_empty());
    assert!(completed.payloads.is_empty());
    assert!(completed.obligations.is_empty());
    assert_eq!(accounts_match_what_is_held(&completed), Ok(()));

    let replay = world.store.apply(&admission);
    assert_eq!(
        outcome(replay.reply),
        StoreOutcome::Committed(admitted(&admission))
    );
    assert!(!replay.committed_now);
    assert_eq!(world.store.snapshot().expect("healthy snapshot"), completed);

    let publication_reused = altered(&world.admission(3, 7, b"new", &[]), |admit| {
        admit.publication = publication_id(world.epoch, 1);
    });
    assert_eq!(
        outcome(world.store.apply(&publication_reused).reply),
        StoreOutcome::RejectedCertain(RejectReason::PreconditionFailed),
        "collecting metadata must not make a publication identity reusable"
    );
    let payload_reused = altered(&world.admission(4, 7, b"new", &[]), |admit| {
        admit.payload.id = PayloadId::new(INCARNATION, world.epoch, OWNER, PayloadSeq::new(1));
    });
    assert_eq!(
        outcome(world.store.apply(&payload_reused).reply),
        StoreOutcome::RejectedCertain(RejectReason::PreconditionFailed),
        "collecting metadata must not make a payload identity reusable"
    );
    let new_exchange = world.admission(5, 7, b"new", &[]);
    assert!(world.store.apply(&new_exchange).committed_now);
    assert!(world.store.apply(&world.release(6, 7, 5)).committed_now);
    let completed_again = world.store.snapshot().expect("healthy snapshot");
    assert!(completed_again.publications.is_empty());
    assert_eq!(accounts_match_what_is_held(&completed_again), Ok(()));
}

/// Metadata lives until both relations end, irrespective of their order. A
/// producer deletion also ends its own marker, but never another session's
/// obligation (which the existing deletion test checks separately).
pub fn publication_metadata_lives_until_its_last_relation_ends<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    for release_first in [false, true] {
        let mut world = World::new(make);
        let admission = world.admission(1, 7, b"payload", &[world.b]);
        assert!(world.store.apply(&admission).committed_now);
        if release_first {
            assert!(world.store.apply(&world.release(2, 7, 1)).committed_now);
        } else {
            assert_eq!(
                world
                    .store
                    .admin(AdminCommand::DeleteSession { session: world.b }),
                Ok(AdminDone::SessionDeleted)
            );
        }
        let partial = world.store.snapshot().expect("healthy snapshot");
        assert_eq!(partial.publications.len(), 1, "one relation still exists");
        if release_first {
            assert_eq!(
                world
                    .store
                    .admin(AdminCommand::DeleteSession { session: world.b }),
                Ok(AdminDone::SessionDeleted)
            );
        } else {
            assert!(world.store.apply(&world.release(2, 7, 1)).committed_now);
        }
        let complete = world.store.snapshot().expect("healthy snapshot");
        assert!(complete.publications.is_empty(), "both relations ended");
        assert!(complete.payloads.is_empty());
        assert_eq!(accounts_match_what_is_held(&complete), Ok(()));
    }

    let mut world = World::new(make);
    let admission = world.admission(1, 7, b"payload", &[]);
    assert!(world.store.apply(&admission).committed_now);
    assert_eq!(
        world.store.admin(AdminCommand::DeleteSession {
            session: world.producer
        }),
        Ok(AdminDone::SessionDeleted)
    );
    let complete = world.store.snapshot().expect("healthy snapshot");
    assert!(
        complete.publications.is_empty(),
        "deletion ended the last marker"
    );
    assert_eq!(accounts_match_what_is_held(&complete), Ok(()));
}

/// Closed-epoch collection can forget a completed admission's receipt: its
/// publication and payload identities cannot have the scope of a new operation.
pub fn collected_publication_identities_never_enter_a_new_epoch<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let mut world = World::new(make);
    let admission = world.admission(1, 7, b"payload", &[]);
    assert!(world.store.apply(&admission).committed_now);
    assert!(world.store.apply(&world.release(2, 7, 1)).committed_now);
    let closed = world.epoch;
    world.epoch = open(&mut world.store);
    assert_eq!(
        world
            .store
            .admin(AdminCommand::CollectClosedEpochs { through: closed }),
        Ok(AdminDone::Collected)
    );
    let state = world.store.snapshot().expect("healthy snapshot");
    assert!(state.operations.is_empty());
    assert!(state.publications.is_empty());

    for (sequence, payload) in [(3, false), (4, true)] {
        let reused = altered(&world.admission(sequence, 7, b"new", &[]), |admit| {
            if payload {
                admit.payload.id = PayloadId::new(INCARNATION, closed, OWNER, PayloadSeq::new(1));
            } else {
                admit.publication = publication_id(closed, 1);
            }
        });
        assert_eq!(
            outcome(world.store.apply(&reused).reply),
            StoreOutcome::RejectedCertain(RejectReason::PreconditionFailed)
        );
    }
    assert!(
        world
            .store
            .apply(&world.admission(5, 7, b"new", &[]))
            .committed_now
    );
}

/// `max_sessions` bounds the sessions that are active. A deleted session
/// is terminal and frees its place; its identity is never used again.
pub fn a_deleted_session_frees_its_place<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let mut limits = fixture_limits();
    limits.max_sessions = 3;
    let mut world = World::with_limits(make, limits);
    assert_eq!(
        world
            .store
            .admin(AdminCommand::DeleteSession { session: world.c }),
        Ok(AdminDone::SessionDeleted)
    );
    let again = world.store.admin(AdminCommand::CreateSession {
        client_id: b"p1-c".to_vec(),
    });
    let Ok(AdminDone::SessionCreated { session, .. }) = again else {
        panic!("the place of a deleted session was not freed: {again:?}");
    };
    assert_ne!(session, world.c);
    assert_eq!(
        world.store.admin(AdminCommand::CreateSession {
            client_id: b"p1-d".to_vec(),
        }),
        Err(AdminRefused::TableFull)
    );
}

/// A ClientID is at most what MQTT can carry. A longer one makes no
/// session, so no recovered fact has to carry it.
pub fn a_client_id_longer_than_mqtt_allows_is_refused<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let mut world = World::new(make);
    let before = world.store.snapshot().expect("healthy durable snapshot");
    assert_eq!(
        world.store.admin(AdminCommand::CreateSession {
            client_id: vec![b'x'; MAX_CLIENT_ID_BYTES + 1],
        }),
        Err(AdminRefused::ClientIdTooLong)
    );
    assert_eq!(
        world.store.snapshot().expect("healthy durable snapshot"),
        before
    );
    assert!(matches!(
        world.store.admin(AdminCommand::CreateSession {
            client_id: vec![b'x'; MAX_CLIENT_ID_BYTES],
        }),
        Ok(AdminDone::SessionCreated { .. })
    ));
}

/// Epoch 0 is never opened, so never closed and never collected: what is
/// established about its identities stays, whatever is collected.
pub fn what_is_established_in_epoch_zero_is_never_collected<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let mut world = World::new(make);
    let zero = RunEpoch::new(0);
    let fenced = operation_id(zero, 1);
    let fence = world.store.resolve_and_fence(fenced);
    let rejected_fence = StoreOutcome::RejectedCertain(RejectReason::Fenced);
    assert_eq!(fence.outcome, rejected_fence);
    assert!(fence.fenced_now);
    let early = admission_in(zero, world.route_revision, world.producer, 2, 7, b"p", &[]);
    let precondition = StoreOutcome::RejectedCertain(RejectReason::PreconditionFailed);
    assert_eq!(outcome(world.store.apply(&early).reply), precondition);

    let closed = world.epoch;
    open(&mut world.store);
    assert_eq!(
        world
            .store
            .admin(AdminCommand::CollectClosedEpochs { through: closed }),
        Ok(AdminDone::Collected)
    );
    let state = world.store.snapshot().expect("healthy durable snapshot");
    assert!(state.fences.contains(&fenced));
    assert!(state.operations.contains_key(&early.recipe.operation()));
    // The same answers, and no second fence.
    assert_eq!(
        world.store.resolve_and_fence(fenced),
        settled(rejected_fence)
    );
    let fenced_command = admission_in(zero, world.route_revision, world.producer, 1, 8, b"p", &[]);
    assert_eq!(
        outcome(world.store.apply(&fenced_command).reply),
        rejected_fence
    );
    assert_eq!(outcome(world.store.apply(&early).reply), precondition);
    assert_eq!(
        world.store.snapshot().expect("healthy durable snapshot"),
        state
    );
}

/// Every receipt and every fence holds one receipt slot, whatever it
/// says and whatever epoch it belongs to. When none is left, nothing is
/// written and nothing certain is said: durable state never grows past
/// its accounts. Collection gives the slots back.
pub fn every_receipt_and_fence_holds_a_receipt_slot<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let mut limits = fixture_limits();
    limits.receipt_slots = ReceiptSlots::new(3);
    let mut world = World::with_limits(make, limits);
    let slots = |world: &World<S>| {
        world
            .store
            .snapshot()
            .expect("healthy durable snapshot")
            .accounts
            .receipt_slots
    };
    let ahead = world.epoch.checked_next().expect("not the last epoch");
    let fenced = StoreOutcome::RejectedCertain(RejectReason::Fenced);
    let precondition = StoreOutcome::RejectedCertain(RejectReason::PreconditionFailed);

    assert_eq!(
        world
            .store
            .resolve_and_fence(operation_id(world.epoch, 1))
            .outcome,
        fenced
    );
    assert_eq!(slots(&world), ReceiptSlots::new(1));
    let future = |sequence, packet| {
        admission_in(
            ahead,
            world.route_revision,
            world.producer,
            sequence,
            packet,
            b"p",
            &[],
        )
    };
    let early = future(2, 7);
    assert_eq!(outcome(world.store.apply(&early).reply), precondition);
    assert_eq!(slots(&world), ReceiptSlots::new(2));
    let refused = altered(&world.admission(3, 8, b"payload", &[world.b]), |admit| {
        admit.costs.receipt_slots = ReceiptSlots::new(2);
    });
    assert_eq!(outcome(world.store.apply(&refused).reply), precondition);
    assert_eq!(slots(&world), ReceiptSlots::new(3));

    // No slot is left.
    let full = world.store.snapshot().expect("healthy durable snapshot");
    let resolved = world.store.resolve_and_fence(operation_id(world.epoch, 4));
    assert_eq!(resolved.outcome, StoreOutcome::PendingOrUnknown);
    assert!(!resolved.fenced_now);
    let applied = world.store.apply(&future(5, 9));
    assert_eq!(outcome(applied.reply), StoreOutcome::PendingOrUnknown);
    assert!(!applied.wrote);
    assert_eq!(
        world.store.snapshot().expect("healthy durable snapshot"),
        full
    );

    // Collecting the closed epoch gives back the slots of its fence and
    // of its receipt; the receipt of the epoch now open stays.
    let closed = world.epoch;
    open(&mut world.store);
    assert_eq!(
        world
            .store
            .admin(AdminCommand::CollectClosedEpochs { through: closed }),
        Ok(AdminDone::Collected)
    );
    assert_eq!(slots(&world), ReceiptSlots::new(1));
    assert_eq!(outcome(world.store.apply(&early).reply), precondition);
}

/// The inventory is in the order of record 0014: the epoch, the limits,
/// the route table, the routes sorted by the bytes of their topic with
/// the targets of one topic in the order they were added, the sessions,
/// the inbound exchanges, the outgoing deliveries, the operations. `RecoveryOrder::admit` takes
/// every fact, page by page from a fresh rule, and over the whole
/// inventory with the rule carried from page to page.
pub fn recovered_facts_follow_the_order_of_an_inventory<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let mut world = World::new(make);
    // A topic that sorts before the fixture's, whose targets are added
    // in the order `c`, then `producer`.
    for session in [world.c, world.producer] {
        let added = world.store.admin(AdminCommand::AddRoute {
            topic: b"a/first".to_vec(),
            session,
            qos: Qos::ExactlyOnce,
        });
        let Ok(AdminDone::RouteAdded { route_revision }) = added else {
            panic!("the route was not added: {added:?}");
        };
        world.route_revision = route_revision;
    }
    let committed = world.admission(1, 7, b"payload", &[world.b]);
    assert!(world.store.apply(&committed).committed_now);
    open(&mut world.store);

    let whole = world.store.recover(None, 64).expect("one page");
    assert_eq!(whole.next, None);
    let facts = whole.facts.as_slice();
    let limits = fixture_limits();
    let route = |topic: &[u8], target, max_qos| RecoveredFact::Route {
        options: mqttiumd_contract::pubsub::SubscriptionOptions::DEFAULT,
        topic: SharedBytes::copy_from(topic, 256).expect("fits"),
        target,
        target_revision: Revision::new(1),
        max_qos,
    };
    assert_eq!(
        &facts[1..7],
        [
            RecoveredFact::Limits {
                max_targets_per_admission: limits.max_targets_per_admission as u32,
                max_topic_bytes: limits.max_topic_bytes as u32,
                max_payload_bytes: PayloadBytes::new(limits.max_payload_bytes as u64),
            },
            RecoveredFact::RouteTable {
                revision: world.route_revision
            },
            route(b"a/first", world.c, Qos::ExactlyOnce),
            route(b"a/first", world.producer, Qos::ExactlyOnce),
            route(TOPIC, world.b, Qos::AtLeastOnce),
            route(TOPIC, world.c, Qos::AtLeastOnce),
        ]
    );
    for limit in 1..=facts.len() {
        let mut carried = RecoveryOrder::new();
        let mut cursor = None;
        loop {
            let page = world.store.recover(cursor, limit).expect("a page");
            let mut fresh = RecoveryOrder::new();
            for fact in page.facts.as_slice() {
                assert_eq!(fresh.admit(fact), Ok(()), "pages of {limit}: {fact:?}");
                assert_eq!(carried.admit(fact), Ok(()), "pages of {limit}: {fact:?}");
            }
            match page.next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
    }
}

/// Snapshot refusals preserve durable state and distinguish a configured
/// bound from an allocation failure. The healthy explicit/default reads agree.
pub fn snapshot_limits_refuse_without_changing_the_store<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let world = World::new(make);
    let before = world.store.snapshot().expect("healthy durable snapshot");
    assert_eq!(
        world.store.snapshot_with_budget(ReadBudget::SNAPSHOT),
        Ok(before.clone())
    );
    for budget in [
        ReadBudget {
            max_rows: 0,
            ..ReadBudget::SNAPSHOT
        },
        ReadBudget {
            max_bytes: 0,
            ..ReadBudget::SNAPSHOT
        },
        // Every fixture field and row fits individually. Their combined
        // logical size still exceeds this snapshot budget.
        ReadBudget {
            max_bytes: 512,
            ..ReadBudget::SNAPSHOT
        },
        ReadBudget {
            max_field_bytes: 1,
            ..ReadBudget::SNAPSHOT
        },
    ] {
        assert_eq!(
            world.store.snapshot_with_budget(budget),
            Err(ReadError::LimitExceeded),
            "snapshot budget {budget:?}"
        );
        assert_eq!(
            world.store.snapshot().expect("healthy durable snapshot"),
            before
        );
    }
}

/// Refusing a page does not claim that inventory ended. The same cursor can
/// still read its next valid fact, and neither refusal nor inspection writes.
pub fn recovery_limits_refuse_without_claiming_the_inventory_ends<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let world = World::new(make);
    let before = world.store.snapshot().expect("healthy durable snapshot");
    assert_eq!(
        world.store.recover(None, 0),
        Err(RecoveryRefused::EmptyPage)
    );
    assert_eq!(
        world.store.recover(None, usize::MAX),
        Err(RecoveryRefused::Read(ReadError::LimitExceeded))
    );
    let first = world
        .store
        .recover_with_budget(None, 3, ReadBudget::PAGE)
        .expect("metadata page");
    assert_eq!(first.facts.as_slice().len(), 3);
    let cursor = first
        .next
        .expect("the inventory includes routes and sessions");
    for budget in [
        ReadBudget {
            max_rows: 0,
            ..ReadBudget::PAGE
        },
        ReadBudget {
            max_bytes: 0,
            ..ReadBudget::PAGE
        },
        ReadBudget {
            max_field_bytes: 1,
            ..ReadBudget::PAGE
        },
    ] {
        assert_eq!(
            world.store.recover_with_budget(Some(cursor), 1, budget),
            Err(RecoveryRefused::Read(ReadError::LimitExceeded)),
            "recovery budget {budget:?}"
        );
    }
    let next = world
        .store
        .recover(Some(cursor), 1)
        .expect("the refused fact remains readable");
    assert!(matches!(
        next.facts.as_slice(),
        [RecoveredFact::Route { .. }]
    ));
    assert_eq!(
        world.store.snapshot().expect("healthy durable snapshot"),
        before
    );
}

/// A snapshot includes the 24-byte opening attempt even when it has no
/// publication or operation identity. Its field budget covers that metadata.
pub fn snapshot_fields_include_the_opening_identity<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let world = World::new(make);
    let before = world.store.snapshot().expect("healthy durable snapshot");
    assert!(before.ledger.opened_by().is_some());
    assert!(before.operations.is_empty());
    assert!(before.publications.is_empty());
    for max_field_bytes in [8, 16, 23] {
        assert_eq!(
            world.store.snapshot_with_budget(ReadBudget {
                max_field_bytes,
                ..ReadBudget::SNAPSHOT
            }),
            Err(ReadError::LimitExceeded),
            "snapshot field budget {max_field_bytes}"
        );
    }
    assert_eq!(
        world.store.snapshot_with_budget(ReadBudget {
            max_field_bytes: 24,
            ..ReadBudget::SNAPSHOT
        }),
        Ok(before)
    );
}

/// A page charges only the fields it delivers. The opening attempt belongs
/// to the snapshot; it does not prevent a metadata page of eight-byte fields.
pub fn recovery_field_limits_apply_to_the_selected_page<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let world = World::new(make);
    let metadata = world
        .store
        .recover_with_budget(
            None,
            3,
            ReadBudget {
                max_field_bytes: 8,
                ..ReadBudget::PAGE
            },
        )
        .expect("the metadata facts contain only fields of at most eight bytes");
    assert!(matches!(
        metadata.facts.as_slice(),
        [
            RecoveredFact::Epoch { .. },
            RecoveredFact::Limits { .. },
            RecoveredFact::RouteTable { .. }
        ]
    ));
    let cursor = metadata.next.expect("the fixture has routes");
    assert!(TOPIC.len() > 8 && TOPIC.len() <= 16);
    assert_eq!(
        world.store.recover_with_budget(
            Some(cursor),
            1,
            ReadBudget {
                max_field_bytes: 8,
                ..ReadBudget::PAGE
            },
        ),
        Err(RecoveryRefused::Read(ReadError::LimitExceeded))
    );
    let route = world
        .store
        .recover_with_budget(
            Some(cursor),
            1,
            ReadBudget {
                max_field_bytes: 16,
                ..ReadBudget::PAGE
            },
        )
        .expect("the route topic fits within sixteen bytes");
    assert!(matches!(
        route.facts.as_slice(),
        [RecoveredFact::Route { topic, .. }] if topic.as_slice() == TOPIC
    ));
}

/// Fixed durable identities count as fields too. A refused identity fact is
/// still read at the same cursor when its complete 32-byte field fits.
pub fn recovery_field_limits_include_fixed_identities<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let mut world = World::new(make);
    let command = world.admission(1, 7, b"payload", &[world.b]);
    assert!(world.store.apply(&command).committed_now);
    assert!(
        world
            .store
            .resolve_and_fence(operation_id(world.epoch, 2))
            .fenced_now
    );
    open(&mut world.store);
    let before = world.store.snapshot().expect("healthy durable snapshot");
    let inventory = world.store.recover(None, 64).expect("one complete page");
    assert!(inventory.next.is_none());
    let mut cursor = None;
    let mut inbound = 0;
    let mut committed = 0;
    let mut fenced = 0;
    for expected in inventory.facts.as_slice() {
        let is_identity = match expected {
            RecoveredFact::InboundExchange { .. } => {
                inbound += 1;
                true
            }
            RecoveredFact::Operation {
                knowledge: Knowledge::Committed(_),
                ..
            } => {
                committed += 1;
                true
            }
            RecoveredFact::Operation {
                knowledge: Knowledge::RejectedCertain(RejectReason::Fenced),
                ..
            } => {
                fenced += 1;
                true
            }
            _ => false,
        };
        if is_identity {
            for max_field_bytes in [16, 31] {
                assert_eq!(
                    world.store.recover_with_budget(
                        cursor,
                        1,
                        ReadBudget {
                            max_field_bytes,
                            ..ReadBudget::PAGE
                        },
                    ),
                    Err(RecoveryRefused::Read(ReadError::LimitExceeded)),
                    "field budget {max_field_bytes} before {expected:?}"
                );
            }
        }
        let page = world
            .store
            .recover_with_budget(
                cursor,
                1,
                ReadBudget {
                    max_field_bytes: 32,
                    ..ReadBudget::PAGE
                },
            )
            .expect("every selected field fits within thirty-two bytes");
        assert_eq!(page.facts.as_slice(), std::slice::from_ref(expected));
        cursor = page.next;
    }
    assert_eq!((inbound, committed, fenced), (1, 1, 1));
    assert!(cursor.is_none());
    assert_eq!(
        world.store.snapshot().expect("healthy durable snapshot"),
        before
    );
}

/// Every check of this module, in order.
pub fn all<S: DurableStore>(make: &dyn Fn(StoreIncarnation, Limits) -> S) {
    live_receipt_collection_is_bounded_and_never_reexecutes(make);
    live_receipt_collection_refuses_holes_and_preserves_identity_guards(make);
    live_receipt_collection_has_a_control_reserve(make);
    live_receipt_registration_preserves_legacy_zero(make);
    recovery_omits_logically_retired_receipts_but_preserves_live_claims(make);
    live_receipt_frontiers_validate_registration_and_bounds(make);
    collection_at_full_quota_preserves_business_and_startup_collapses_origins(make);
    qos0_grants_and_consumption_preserve_durable_delivery_identity(make);
    qos0_consumption_preserves_unknown_and_atomic_capacity(make);
    replacing_and_removing_qos0_routes_preserves_older_work(make);
    subscriptions_replace_and_remove_without_duplicate_or_hole(make);
    qos1_claim_and_finish_are_conditional_and_idempotent(make);
    outgoing_work_is_bounded_and_independent_of_notifications(make);
    outgoing_capacity_and_filter_guards_are_atomic(make);
    deleting_a_claimed_target_releases_only_its_resources(make);
    outgoing_packet_reuse_preserves_claim_identity(make);
    recycled_packet_ids_preserve_occurrences_after_receipt_collection(make);
    lost_claim_results_preserve_packet_reservations_until_guarded_finish(make);
    opening_opens_the_next_epoch_and_closes_the_older_ones(make);
    an_opening_that_expected_another_epoch_opens_nothing(make);
    an_opening_delivered_again_returns_its_commit(make);
    the_opener_of_the_last_epoch_cannot_open_the_next_under_its_identity(make);
    the_first_command_of_a_restart_is_not_its_predecessors(make);
    a_command_of_an_unopened_epoch_never_executes(make);
    an_admission_commits_everything_together(make);
    a_repeated_command_returns_its_result_without_a_second_effect(make);
    a_failed_guard_leaves_no_admitted_subset(make);
    each_account_is_checked(make);
    a_rejection_stays_a_rejection_after_quota_is_released(make);
    another_recipe_under_the_same_operation_is_a_violation(make);
    resolve_returns_a_commit_and_fences_an_absence(make);
    a_release_ends_the_inbound_exchange(make);
    a_release_holds_the_slot_of_its_receipt(make);
    a_release_without_a_receipt_slot_changes_nothing(make);
    collecting_an_epoch_gives_back_the_slots_of_its_releases(make);
    deleting_the_producer_deletes_no_obligation_of_another_session(make);
    an_older_epoch_is_not_entered_again(make);
    a_collected_identity_is_retired_not_rejected(make);
    an_identity_of_another_data_set_is_refused(make);
    recovery_reads_bounded_pages_of_durable_facts(make);
    administration_refusals_change_nothing(make);
    an_admission_without_targets_holds_no_payload_bytes(make);
    a_completed_unrouted_exchange_releases_its_publication(make);
    publication_metadata_lives_until_its_last_relation_ends(make);
    collected_publication_identities_never_enter_a_new_epoch(make);
    a_deleted_session_frees_its_place(make);
    a_client_id_longer_than_mqtt_allows_is_refused(make);
    what_is_established_in_epoch_zero_is_never_collected(make);
    every_receipt_and_fence_holds_a_receipt_slot(make);
    recovered_facts_follow_the_order_of_an_inventory(make);
    snapshot_limits_refuse_without_changing_the_store(make);
    recovery_limits_refuse_without_claiming_the_inventory_ends(make);
    snapshot_fields_include_the_opening_identity(make);
    recovery_field_limits_apply_to_the_selected_page(make);
    recovery_field_limits_include_fixed_identities(make);
}

/// Typed grants replace atomically; an old obligation keeps its admitted QoS.
/// QoS 0 consumption has a receipt, but no outgoing packet/claim/exchange slot.
pub fn qos0_grants_and_consumption_preserve_durable_delivery_identity<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    use mqttiumd_contract::command::{
        ChangeSubscriptions, ClaimDelivery, ConsumeDelivery, FinishDelivery, UpdateSubscriptions,
    };
    use mqttiumd_contract::ids::OutboundPacketId;
    use mqttiumd_contract::pubsub::{GrantedSubscriptionChange, SubscriptionChange};
    let mut limits = fixture_limits();
    limits.receipt_slots = ReceiptSlots::new(64);
    let mut world = World::with_limits(make, limits);
    let freeze = |recipe| Command {
        recipe: FrozenRecipe::freeze(recipe).unwrap(),
        payload: None,
    };
    let update = |seq, revision, granted| {
        freeze(Recipe::UpdateSubscriptions(UpdateSubscriptions {
            operation: operation_id(world.epoch, seq),
            session: world.b,
            session_revision: Revision::new(1),
            route_revision: revision,
            changes: BoundedVec::from_slice(
                &[GrantedSubscriptionChange {
                    options: mqttiumd_contract::pubsub::SubscriptionOptions::DEFAULT,
                    topic: SharedBytes::copy_from(TOPIC, 256).unwrap(),
                    granted,
                }],
                8,
            )
            .unwrap(),
        }))
    };
    let first = update(1, world.route_revision, Some(Qos::AtMostOnce));
    assert!(
        world.store.apply(&first).committed_now,
        "typed QoS0 subscription must commit"
    );
    world.route_revision = world.route_revision.checked_next().unwrap();
    let stable = world.store.snapshot().unwrap();
    assert_eq!(
        stable.routes[TOPIC]
            .iter()
            .find(|r| r.session == world.b)
            .unwrap()
            .qos,
        Qos::AtMostOnce
    );
    assert!(!world.store.apply(&first).committed_now);
    assert_eq!(world.store.snapshot().unwrap(), stable);
    // Old kind3 keeps its original grant1 meaning even after kind7 exists.
    let legacy = freeze(Recipe::ChangeSubscriptions(ChangeSubscriptions {
        operation: operation_id(world.epoch, 2),
        session: world.b,
        session_revision: Revision::new(1),
        route_revision: world.route_revision,
        changes: BoundedVec::from_slice(
            &[SubscriptionChange {
                topic: SharedBytes::copy_from(TOPIC, 256).unwrap(),
                subscribe: true,
            }],
            8,
        )
        .unwrap(),
    }));
    assert!(world.store.apply(&legacy).committed_now);
    world.route_revision = world.route_revision.checked_next().unwrap();
    assert_eq!(
        world.store.snapshot().unwrap().routes[TOPIC]
            .iter()
            .find(|r| r.session == world.b)
            .unwrap()
            .qos,
        Qos::AtLeastOnce
    );
    assert!(
        world
            .store
            .apply(&update(3, world.route_revision, Some(Qos::AtMostOnce)))
            .committed_now
    );
    world.route_revision = world.route_revision.checked_next().unwrap();
    let admission = altered(&world.admission(4, 7, b"first", &[world.b, world.c]), |a| {
        a.targets.as_mut_slice()[0].effective_qos = Qos::AtMostOnce
    });
    assert!(world.store.apply(&admission).committed_now);
    let old = DeliveryId {
        publication: publication_id(world.epoch, 4),
        index: 0,
    };
    assert!(
        world
            .store
            .apply(&update(5, world.route_revision, Some(Qos::AtLeastOnce)))
            .committed_now
    );
    world.route_revision = world.route_revision.checked_next().unwrap();
    let second = world.admission(6, 8, b"second", &[world.b]);
    assert!(world.store.apply(&second).committed_now);
    let newer = DeliveryId {
        publication: publication_id(world.epoch, 6),
        index: 0,
    };
    let state = world.store.snapshot().unwrap();
    assert_eq!(state.obligations[&old].qos, Qos::AtMostOnce);
    assert_eq!(state.obligations[&newer].qos, Qos::AtLeastOnce);
    let page = world.store.read_work(world.b, None, 8).unwrap();
    assert_eq!(
        page.items
            .as_slice()
            .iter()
            .map(|i| (i.delivery, i.qos, i.claim))
            .collect::<Vec<_>>(),
        [
            (old, Qos::AtMostOnce, None),
            (newer, Qos::AtLeastOnce, None)
        ]
    );
    let recovery = world.store.recover(None, 64).unwrap();
    assert!(recovery.facts.as_slice().iter().any(|f|matches!(f,RecoveredFact::OutboundDelivery{delivery,qos:Qos::AtMostOnce,claim:None,..} if *delivery==old)));
    let consume = |seq, delivery, target| {
        freeze(Recipe::ConsumeDelivery(ConsumeDelivery {
            operation: operation_id(world.epoch, seq),
            delivery,
            target,
            target_revision: Revision::new(1),
        }))
    };
    let wrong_type = consume(7, newer, world.b);
    let before = world.store.snapshot().unwrap();
    assert_eq!(
        outcome(world.store.apply(&wrong_type).reply),
        StoreOutcome::RejectedCertain(RejectReason::PreconditionFailed)
    );
    let after = world.store.snapshot().unwrap();
    assert_eq!(after.obligations, before.obligations);
    assert_eq!(after.payloads, before.payloads);
    assert_eq!(
        after.accounts.exchange_slots,
        before.accounts.exchange_slots
    );
    let consumed = consume(8, old, world.b);
    let applied = world.store.apply(&consumed);
    assert_eq!(
        outcome(applied.reply),
        StoreOutcome::Committed(CommitFacts::DeliveryConsumed {
            delivery: old,
            target: world.b
        })
    );
    assert!(applied.committed_now);
    let after = world.store.snapshot().unwrap();
    assert!(!after.obligations.contains_key(&old));
    assert!(after.outbound.is_empty());
    assert_eq!(
        after.accounts.exchange_slots,
        before.accounts.exchange_slots
    );
    assert_eq!(after.accounts.references.get(), 2);
    assert_eq!(accounts_match_what_is_held(&after), Ok(()));
    assert!(!world.store.apply(&consumed).committed_now);
    assert_eq!(world.store.snapshot().unwrap(), after);
    assert_eq!(
        world
            .store
            .resolve_and_fence(consumed.recipe.operation())
            .outcome,
        StoreOutcome::Committed(CommitFacts::DeliveryConsumed {
            delivery: old,
            target: world.b
        })
    );
    assert_eq!(
        outcome(world.store.apply(&consume(9, old, world.b)).reply),
        StoreOutcome::RejectedCertain(RejectReason::PreconditionFailed)
    );
    let c = DeliveryId {
        publication: old.publication,
        index: 1,
    };
    let claim = freeze(Recipe::ClaimDelivery(ClaimDelivery {
        operation: operation_id(world.epoch, 10),
        delivery: c,
        target: world.c,
        target_revision: Revision::new(1),
        packet_id: OutboundPacketId::new(7).unwrap(),
    }));
    assert!(world.store.apply(&claim).committed_now);
    let finish = freeze(Recipe::FinishDelivery(FinishDelivery {
        operation: operation_id(world.epoch, 11),
        delivery: c,
        target: world.c,
        target_revision: Revision::new(1),
        packet_id: OutboundPacketId::new(7).unwrap(),
        claim: claim.recipe.operation(),
    }));
    assert!(world.store.apply(&finish).committed_now);
    let after = world.store.snapshot().unwrap();
    assert_eq!(after.payloads.len(), 1);
    assert_eq!(after.accounts.payload_bytes.get(), 6);
    assert_eq!(accounts_match_what_is_held(&after), Ok(()));
    assert!(world.store.apply(&world.release(12, 7, 4)).committed_now);
    assert!(
        !world
            .store
            .snapshot()
            .unwrap()
            .publications
            .contains_key(&old.publication)
    );
    assert_eq!(
        world
            .store
            .read_work(world.b, None, 8)
            .unwrap()
            .items
            .as_slice()[0]
            .delivery,
        newer
    );
}

/// Failure/unknown outcomes preserve the same operation and release no subset.
pub fn qos0_consumption_preserves_unknown_and_atomic_capacity<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    use mqttiumd_contract::command::ConsumeDelivery;
    for receipts in [1, 8] {
        let mut limits = fixture_limits();
        limits.receipt_slots = ReceiptSlots::new(receipts);
        let mut world = World::with_limits(make, limits);
        let admission = altered(&world.admission(1, 7, b"payload", &[world.b]), |a| {
            a.qos = Qos::AtLeastOnce;
            a.costs.exchange_slots = ExchangeSlots::ZERO;
            a.targets.as_mut_slice()[0].effective_qos = Qos::AtMostOnce;
        });
        assert!(world.store.apply(&admission).committed_now);
        let delivery = DeliveryId {
            publication: publication_id(world.epoch, 1),
            index: 0,
        };
        let consume = Command {
            recipe: FrozenRecipe::freeze(Recipe::ConsumeDelivery(ConsumeDelivery {
                operation: operation_id(world.epoch, 2),
                delivery,
                target: world.b,
                target_revision: Revision::new(1),
            }))
            .unwrap(),
            payload: None,
        };
        let before = world.store.snapshot().unwrap();
        if receipts == 1 {
            assert_eq!(
                outcome(world.store.apply(&consume).reply),
                StoreOutcome::PendingOrUnknown
            );
            assert_eq!(
                world.store.snapshot().unwrap(),
                before,
                "full receipt account cannot partially consume"
            );
        } else {
            assert!(world.store.apply(&consume).committed_now);
            assert_eq!(
                world
                    .store
                    .resolve_and_fence(consume.recipe.operation())
                    .outcome,
                StoreOutcome::Committed(CommitFacts::DeliveryConsumed {
                    delivery,
                    target: world.b
                })
            );
            let after = world.store.snapshot().unwrap();
            assert!(
                after.obligations.is_empty()
                    && after.outbound.is_empty()
                    && after.publications.is_empty()
                    && after.payloads.is_empty()
            );
            assert_eq!(after.accounts.references, References::ZERO);
            assert_eq!(after.accounts.exchange_slots, ExchangeSlots::ZERO);
            assert!(!world.store.apply(&consume).committed_now);
            assert_eq!(world.store.snapshot().unwrap(), after);
        }
    }
}

/// Both replacement directions keep admitted QoS immutable. Removing the
/// route does not prevent either old delivery cycle from completing.
pub fn replacing_and_removing_qos0_routes_preserves_older_work<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    use mqttiumd_contract::command::{
        ClaimDelivery, ConsumeDelivery, FinishDelivery, UpdateSubscriptions,
    };
    use mqttiumd_contract::ids::OutboundPacketId;
    use mqttiumd_contract::pubsub::GrantedSubscriptionChange;
    let mut limits = fixture_limits();
    limits.receipt_slots = ReceiptSlots::new(64);
    let mut world = World::with_limits(make, limits);
    let first = world.admission(1, 7, b"older-qos1", &[world.b]);
    assert!(world.store.apply(&first).committed_now);
    let freeze = |recipe| Command {
        recipe: FrozenRecipe::freeze(recipe).unwrap(),
        payload: None,
    };
    let update = |seq, revision, granted| {
        freeze(Recipe::UpdateSubscriptions(UpdateSubscriptions {
            operation: operation_id(world.epoch, seq),
            session: world.b,
            session_revision: Revision::new(1),
            route_revision: revision,
            changes: BoundedVec::from_slice(
                &[GrantedSubscriptionChange {
                    options: mqttiumd_contract::pubsub::SubscriptionOptions::DEFAULT,
                    topic: SharedBytes::copy_from(TOPIC, 256).unwrap(),
                    granted,
                }],
                8,
            )
            .unwrap(),
        }))
    };
    let original_revision = world.route_revision;
    let downgraded = update(2, original_revision, Some(Qos::AtMostOnce));
    assert!(world.store.apply(&downgraded).committed_now);
    world.route_revision = world.route_revision.checked_next().unwrap();
    let frozen = world.store.snapshot().unwrap();
    let mismatch = update(2, original_revision, Some(Qos::AtLeastOnce));
    assert_eq!(
        world.store.apply(&mismatch).reply,
        StoreReply::Violation(StoreViolation::RecipeMismatch)
    );
    assert_eq!(
        world.store.snapshot().unwrap(),
        frozen,
        "granted QoS is part of the canonical operation identity"
    );
    let second = altered(&world.admission(3, 8, b"newer-qos0", &[world.b]), |a| {
        a.targets.as_mut_slice()[0].effective_qos = Qos::AtMostOnce
    });
    assert!(world.store.apply(&second).committed_now);
    assert!(
        world
            .store
            .apply(&update(4, world.route_revision, None))
            .committed_now
    );
    world.route_revision = world.route_revision.checked_next().unwrap();
    let state = world.store.snapshot().unwrap();
    assert_eq!(
        state.routes[TOPIC]
            .iter()
            .map(|r| r.session)
            .collect::<Vec<_>>(),
        [world.c]
    );
    let first_delivery = DeliveryId {
        publication: publication_id(world.epoch, 1),
        index: 0,
    };
    let second_delivery = DeliveryId {
        publication: publication_id(world.epoch, 3),
        index: 0,
    };
    assert_eq!(state.obligations[&first_delivery].qos, Qos::AtLeastOnce);
    assert_eq!(state.obligations[&second_delivery].qos, Qos::AtMostOnce);
    assert_eq!(
        world
            .store
            .read_work(world.b, None, 8)
            .unwrap()
            .items
            .as_slice()
            .iter()
            .map(|w| (w.delivery, w.qos))
            .collect::<Vec<_>>(),
        [
            (first_delivery, Qos::AtLeastOnce),
            (second_delivery, Qos::AtMostOnce)
        ]
    );
    let claim = freeze(Recipe::ClaimDelivery(ClaimDelivery {
        operation: operation_id(world.epoch, 5),
        delivery: first_delivery,
        target: world.b,
        target_revision: Revision::new(1),
        packet_id: OutboundPacketId::new(7).unwrap(),
    }));
    assert!(world.store.apply(&claim).committed_now);
    let consume = freeze(Recipe::ConsumeDelivery(ConsumeDelivery {
        operation: operation_id(world.epoch, 6),
        delivery: second_delivery,
        target: world.b,
        target_revision: Revision::new(1),
    }));
    assert!(world.store.apply(&consume).committed_now);
    let after = world.store.snapshot().unwrap();
    assert_eq!(after.outbound.len(), 1);
    assert_eq!(after.accounts.exchange_slots.get(), 3);
    assert_eq!(accounts_match_what_is_held(&after), Ok(()));
    let finish = freeze(Recipe::FinishDelivery(FinishDelivery {
        operation: operation_id(world.epoch, 7),
        delivery: first_delivery,
        target: world.b,
        target_revision: Revision::new(1),
        packet_id: OutboundPacketId::new(7).unwrap(),
        claim: claim.recipe.operation(),
    }));
    assert!(world.store.apply(&finish).committed_now);
    assert!(world.store.apply(&world.release(8, 7, 1)).committed_now);
    assert!(world.store.apply(&world.release(9, 8, 3)).committed_now);
    let ended = world.store.snapshot().unwrap();
    assert!(
        ended.obligations.is_empty()
            && ended.outbound.is_empty()
            && ended.payloads.is_empty()
            && ended.publications.is_empty()
            && ended.inbound.is_empty()
    );
    assert_eq!(accounts_match_what_is_held(&ended), Ok(()));
}

/// A network subscription mutation is a normal receipted operation: replacing
/// an existing target has no duplicate, and removing it preserves old work.
pub fn subscriptions_replace_and_remove_without_duplicate_or_hole<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    use mqttiumd_contract::command::ChangeSubscriptions;
    use mqttiumd_contract::pubsub::SubscriptionChange;
    let mut world = World::new(make);
    let before = world.store.snapshot().unwrap();
    let mut changes = BoundedVec::with_capacity(1).unwrap();
    changes
        .try_push(SubscriptionChange {
            topic: SharedBytes::copy_from(TOPIC, 128).unwrap(),
            subscribe: true,
        })
        .unwrap();
    let command = Command {
        recipe: FrozenRecipe::freeze(Recipe::ChangeSubscriptions(ChangeSubscriptions {
            operation: operation_id(world.epoch, 91),
            session: world.b,
            session_revision: Revision::new(1),
            route_revision: before.route_revision,
            changes,
        }))
        .unwrap(),
        payload: None,
    };
    let reply = world.store.apply(&command);
    let expected =
        StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::SubscriptionsChanged {
            session: world.b,
            route_revision: before.route_revision.checked_next().unwrap(),
        }));
    assert_eq!(
        reply.reply, expected,
        "subscription upsert must commit through the ordinary operation pipeline"
    );
    let replaced = world.store.snapshot().unwrap();
    assert_eq!(
        replaced.routes[TOPIC].len(),
        2,
        "resubscribe must not duplicate a destination"
    );
    assert_eq!(
        replaced.sessions[&world.b].revision,
        before.sessions[&world.b].revision
    );
    assert_eq!(world.store.apply(&command).reply, expected);
    assert_eq!(
        world.store.snapshot().unwrap(),
        replaced,
        "repeated OperationId is idempotent"
    );
    world.route_revision = replaced.route_revision;
    let admission = world.admission(92, 7, b"held", &[world.b, world.c]);
    assert!(matches!(
        world.store.apply(&admission).reply,
        StoreReply::Outcome(StoreOutcome::Committed(_))
    ));
    let mut changes = BoundedVec::with_capacity(1).unwrap();
    changes
        .try_push(SubscriptionChange {
            topic: SharedBytes::copy_from(TOPIC, 128).unwrap(),
            subscribe: false,
        })
        .unwrap();
    let remove = Command {
        recipe: FrozenRecipe::freeze(Recipe::ChangeSubscriptions(ChangeSubscriptions {
            operation: operation_id(world.epoch, 93),
            session: world.b,
            session_revision: Revision::new(1),
            route_revision: replaced.route_revision,
            changes,
        }))
        .unwrap(),
        payload: None,
    };
    assert!(matches!(
        world.store.apply(&remove).reply,
        StoreReply::Outcome(StoreOutcome::Committed(_))
    ));
    let removed = world.store.snapshot().unwrap();
    assert_eq!(
        removed.routes[TOPIC]
            .iter()
            .map(|r| r.session)
            .collect::<Vec<_>>(),
        vec![world.c]
    );
    assert_eq!(
        removed.obligations.len(),
        2,
        "unsubscribe must retain both already admitted obligations"
    );
}

/// Claims reserve a directional wire identifier until guarded durable finish;
/// neither a repeat nor an obsolete claim can decrement resources twice.
pub fn qos1_claim_and_finish_are_conditional_and_idempotent<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    use mqttiumd_contract::command::{ClaimDelivery, FinishDelivery};
    use mqttiumd_contract::ids::OutboundPacketId;
    let mut world = World::new(make);
    let admission = world.admission(1, 7, b"payload", &[world.b, world.c]);
    assert!(matches!(
        world.store.apply(&admission).reply,
        StoreReply::Outcome(StoreOutcome::Committed(_))
    ));
    let delivery = |index| DeliveryId {
        publication: publication_id(world.epoch, 1),
        index,
    };
    let claim = |seq, target, index| Command {
        recipe: FrozenRecipe::freeze(Recipe::ClaimDelivery(ClaimDelivery {
            operation: operation_id(world.epoch, seq),
            delivery: delivery(index),
            target,
            target_revision: Revision::new(1),
            packet_id: OutboundPacketId::new(7).unwrap(),
        }))
        .unwrap(),
        payload: None,
    };
    let first = claim(2, world.b, 0);
    assert_eq!(
        world.store.apply(&first).reply,
        StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::DeliveryClaimed {
            delivery: delivery(0),
            target: world.b,
            packet_id: OutboundPacketId::new(7).unwrap(),
            claim: operation_id(world.epoch, 2),
        })),
        "claim must durably reserve its own operation and packet identifier"
    );
    let before = world.store.snapshot().unwrap();
    assert_eq!(
        before.accounts.exchange_slots.get(),
        2,
        "one incoming and one outgoing slot coexist"
    );
    assert!(!world.store.apply(&first).committed_now);
    assert_eq!(
        world.store.snapshot().unwrap(),
        before,
        "claim replay changes nothing"
    );
    assert!(matches!(
        world.store.apply(&claim(3, world.c, 1)).reply,
        StoreReply::Outcome(StoreOutcome::Committed(_))
    ));
    let finish = |seq, target, index, claimed_as| Command {
        recipe: FrozenRecipe::freeze(Recipe::FinishDelivery(FinishDelivery {
            operation: operation_id(world.epoch, seq),
            delivery: delivery(index),
            target,
            target_revision: Revision::new(1),
            packet_id: OutboundPacketId::new(7).unwrap(),
            claim: operation_id(world.epoch, claimed_as),
        }))
        .unwrap(),
        payload: None,
    };
    assert_eq!(
        world.store.apply(&finish(4, world.b, 0, 99)).reply,
        StoreReply::Outcome(StoreOutcome::RejectedCertain(
            RejectReason::PreconditionFailed
        ))
    );
    let correct = finish(5, world.b, 0, 2);
    assert!(matches!(
        world.store.apply(&correct).reply,
        StoreReply::Outcome(StoreOutcome::Committed(_))
    ));
    let one = world.store.snapshot().unwrap();
    assert_eq!(one.obligations.len(), 1);
    assert_eq!(one.payloads.len(), 1);
    assert_eq!(one.accounts.references.get(), 1);
    assert_eq!(one.accounts.exchange_slots.get(), 2);
    assert!(!world.store.apply(&correct).committed_now);
    assert_eq!(world.store.snapshot().unwrap(), one);
    let release = world.release(6, 7, 1);
    assert!(matches!(
        world.store.apply(&release).reply,
        StoreReply::Outcome(StoreOutcome::Committed(_))
    ));
    assert!(matches!(
        world.store.apply(&finish(7, world.c, 1, 3)).reply,
        StoreReply::Outcome(StoreOutcome::Committed(_))
    ));
    let ended = world.store.snapshot().unwrap();
    assert!(
        ended.obligations.is_empty() && ended.payloads.is_empty() && ended.publications.is_empty()
    );
    assert_eq!(
        (
            ended.accounts.references.get(),
            ended.accounts.payload_bytes.get(),
            ended.accounts.exchange_slots.get()
        ),
        (0, 0, 0)
    );
    assert_eq!(
        world.store.apply(&finish(8, world.b, 0, 2)).reply,
        StoreReply::Outcome(StoreOutcome::RejectedCertain(
            RejectReason::PreconditionFailed
        ))
    );
}

/// A live work scan needs no notification, copies no more than its budget, and
/// reconstructs queued and claimed identities from committed facts alone.
pub fn outgoing_work_is_bounded_and_independent_of_notifications<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    use mqttiumd_contract::command::ClaimDelivery;
    use mqttiumd_contract::ids::OutboundPacketId;
    use mqttiumd_contract::pubsub::DeliveryClaim;
    let mut limits = fixture_limits();
    limits.receipt_slots = ReceiptSlots::new(32);
    let mut world = World::with_limits(make, limits);
    for sequence in 1..=9 {
        let command = altered(
            &world.admission(sequence, 7, b"payload", &[world.b]),
            |admit| {
                admit.qos = Qos::AtLeastOnce;
                admit.costs.exchange_slots = ExchangeSlots::ZERO;
            },
        );
        assert!(world.store.apply(&command).committed_now);
    }
    let first_delivery = DeliveryId {
        publication: publication_id(world.epoch, 1),
        index: 0,
    };
    let claim = DeliveryClaim {
        started: false,
        operation: operation_id(world.epoch, 20),
        packet_id: OutboundPacketId::new(7).unwrap(),
    };
    let command = Command {
        recipe: FrozenRecipe::freeze(Recipe::ClaimDelivery(ClaimDelivery {
            operation: claim.operation,
            delivery: first_delivery,
            target: world.b,
            target_revision: Revision::new(1),
            packet_id: claim.packet_id,
        }))
        .unwrap(),
        payload: None,
    };
    assert!(world.store.apply(&command).committed_now);
    let before = world.store.snapshot().unwrap();
    let page = world.store.read_work(world.b, None, 8).unwrap();
    assert_eq!(page.items.len(), 8);
    let first = &page.items.as_slice()[0];
    assert_eq!(first.delivery, first_delivery);
    assert_eq!(first.claim, Some(claim));
    assert_eq!(first.target, world.b);
    assert_eq!(first.target_revision, Revision::new(1));
    assert_eq!(first.topic.as_slice(), TOPIC);
    assert_eq!(first.payload.as_slice(), b"payload");
    for (rank, item) in page.items.as_slice().iter().enumerate() {
        assert_eq!(
            item.delivery.publication,
            publication_id(world.epoch, (rank + 1) as u64)
        );
        assert_eq!(item.claim.is_some(), rank == 0);
    }
    assert_eq!(page.next, Some(page.items.as_slice()[7].delivery));
    let last = world.store.read_work(world.b, page.next, 8).unwrap();
    assert_eq!(last.items.len(), 1);
    assert_eq!(
        last.items.as_slice()[0].delivery.publication,
        publication_id(world.epoch, 9)
    );
    assert!(last.next.is_none());
    assert!(
        world
            .store
            .read_work(world.c, None, 8)
            .unwrap()
            .items
            .is_empty()
    );
    for limit in [0, 9, usize::MAX] {
        assert_eq!(
            world.store.read_work(world.b, None, limit),
            Err(ReadError::LimitExceeded)
        );
    }
    for budget in [
        ReadBudget {
            max_rows: 0,
            ..ReadBudget::PAGE
        },
        ReadBudget {
            max_bytes: 1,
            ..ReadBudget::PAGE
        },
        ReadBudget {
            max_field_bytes: 6,
            ..ReadBudget::PAGE
        },
    ] {
        assert_eq!(
            world.store.read_work_with_budget(world.b, None, 8, budget),
            Err(ReadError::LimitExceeded)
        );
    }
    assert_eq!(
        world.store.snapshot().unwrap(),
        before,
        "failed reads do not claim or discard durable work"
    );
    assert_eq!(accounts_match_what_is_held(&before), Ok(()));
    world.epoch = open(&mut world.store);
    let recovery = world.store.recover(None, 64).unwrap();
    let recovered: Vec<_> = recovery
        .facts
        .as_slice()
        .iter()
        .filter_map(|fact| match fact {
            RecoveredFact::OutboundDelivery {
                delivery,
                target,
                target_revision,
                qos: Qos::AtLeastOnce,
                claim,
                ..
            } => Some((*delivery, *target, *target_revision, *claim)),
            _ => None,
        })
        .collect();
    assert_eq!(recovered.len(), 9);
    assert_eq!(
        recovered[0],
        (first_delivery, world.b, Revision::new(1), Some(claim))
    );
    assert_eq!(
        world
            .store
            .read_work(world.b, None, 8)
            .unwrap()
            .items
            .as_slice()[0]
            .claim,
        Some(claim)
    );
}

/// Subscription and slot capacity guards are atomic. A failed batch cannot
/// leave an earlier filter installed, and a rejected claim owns no slot.
pub fn outgoing_capacity_and_filter_guards_are_atomic<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    use mqttiumd_contract::command::{ChangeSubscriptions, ClaimDelivery};
    use mqttiumd_contract::ids::OutboundPacketId;
    use mqttiumd_contract::pubsub::SubscriptionChange;
    let mut limits = fixture_limits();
    limits.exchange_slots = ExchangeSlots::new(1);
    limits.max_route_targets = 2;
    let mut world = World::with_limits(make, limits);
    let changed = Command {
        recipe: FrozenRecipe::freeze(Recipe::ChangeSubscriptions(ChangeSubscriptions {
            operation: operation_id(world.epoch, 1),
            session: world.producer,
            session_revision: Revision::new(1),
            route_revision: world.route_revision,
            changes: BoundedVec::from_slice(
                &[
                    SubscriptionChange {
                        topic: SharedBytes::copy_from(b"new/filter", 256).unwrap(),
                        subscribe: true,
                    },
                    SubscriptionChange {
                        topic: SharedBytes::copy_from(TOPIC, 256).unwrap(),
                        subscribe: true,
                    },
                ],
                8,
            )
            .unwrap(),
        }))
        .unwrap(),
        payload: None,
    };
    let before = world.store.snapshot().unwrap();
    assert_eq!(
        outcome(world.store.apply(&changed).reply),
        StoreOutcome::RejectedCertain(RejectReason::CapacityExceeded(Resource::References))
    );
    let refused = world.store.snapshot().unwrap();
    assert_eq!(refused.routes, before.routes);
    assert_eq!(refused.route_revision, before.route_revision);
    let admission = world.admission(2, 7, b"payload", &[world.b]);
    assert!(world.store.apply(&admission).committed_now);
    let claim = Command {
        recipe: FrozenRecipe::freeze(Recipe::ClaimDelivery(ClaimDelivery {
            operation: operation_id(world.epoch, 3),
            delivery: DeliveryId {
                publication: publication_id(world.epoch, 2),
                index: 0,
            },
            target: world.b,
            target_revision: Revision::new(1),
            packet_id: OutboundPacketId::new(7).unwrap(),
        }))
        .unwrap(),
        payload: None,
    };
    let before = world.store.snapshot().unwrap();
    assert_eq!(
        outcome(world.store.apply(&claim).reply),
        StoreOutcome::RejectedCertain(RejectReason::CapacityExceeded(Resource::ExchangeSlots))
    );
    let refused = world.store.snapshot().unwrap();
    assert!(refused.outbound.is_empty());
    assert_eq!(refused.obligations, before.obligations);
    assert_eq!(refused.payloads, before.payloads);
    assert_eq!(
        refused.accounts.exchange_slots,
        before.accounts.exchange_slots
    );
    assert_eq!(accounts_match_what_is_held(&refused), Ok(()));
}

/// Deleting the target releases a claimed slot once, preserving references
/// belonging to another target and the producer's independent inbound slot.
pub fn deleting_a_claimed_target_releases_only_its_resources<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    use mqttiumd_contract::command::ClaimDelivery;
    use mqttiumd_contract::ids::OutboundPacketId;
    let mut world = World::new(make);
    let admission = world.admission(1, 7, b"payload", &[world.b, world.c]);
    assert!(world.store.apply(&admission).committed_now);
    let claim = Command {
        recipe: FrozenRecipe::freeze(Recipe::ClaimDelivery(ClaimDelivery {
            operation: operation_id(world.epoch, 2),
            delivery: DeliveryId {
                publication: publication_id(world.epoch, 1),
                index: 0,
            },
            target: world.b,
            target_revision: Revision::new(1),
            packet_id: OutboundPacketId::new(7).unwrap(),
        }))
        .unwrap(),
        payload: None,
    };
    assert!(world.store.apply(&claim).committed_now);
    assert_eq!(
        world
            .store
            .admin(AdminCommand::DeleteSession { session: world.b }),
        Ok(AdminDone::SessionDeleted)
    );
    let after = world.store.snapshot().unwrap();
    assert!(after.outbound.is_empty());
    assert_eq!(after.obligations.len(), 1);
    assert_eq!(after.obligations.values().next().unwrap().target, world.c);
    assert_eq!(after.accounts.exchange_slots.get(), 1);
    assert_eq!(after.payloads.values().next().unwrap().references, 1);
    assert_eq!(accounts_match_what_is_held(&after), Ok(()));
    assert_eq!(
        world
            .store
            .admin(AdminCommand::DeleteSession { session: world.b }),
        Err(AdminRefused::UnknownSession)
    );
    assert_eq!(world.store.snapshot().unwrap(), after);
}

/// One target cannot own two live slots with the same directional wire ID.
/// Once finish is committed that ID can serve another delivery; an obsolete
/// claim or a replayed old terminal result cannot free the newer occurrence.
pub fn outgoing_packet_reuse_preserves_claim_identity<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    use mqttiumd_contract::command::{ClaimDelivery, FinishDelivery};
    use mqttiumd_contract::ids::OutboundPacketId;
    let mut world = World::new(make);
    for sequence in 1..=2 {
        let admission = altered(
            &world.admission(sequence, 7, b"same payload", &[world.b]),
            |admit| {
                admit.qos = Qos::AtLeastOnce;
                admit.costs.exchange_slots = ExchangeSlots::ZERO;
            },
        );
        assert!(world.store.apply(&admission).committed_now);
    }
    let claim = |seq, published_as| Command {
        recipe: FrozenRecipe::freeze(Recipe::ClaimDelivery(ClaimDelivery {
            operation: operation_id(world.epoch, seq),
            delivery: DeliveryId {
                publication: publication_id(world.epoch, published_as),
                index: 0,
            },
            target: world.b,
            target_revision: Revision::new(1),
            packet_id: OutboundPacketId::new(7).unwrap(),
        }))
        .unwrap(),
        payload: None,
    };
    let finish = |seq, published_as, claimed_as| Command {
        recipe: FrozenRecipe::freeze(Recipe::FinishDelivery(FinishDelivery {
            operation: operation_id(world.epoch, seq),
            delivery: DeliveryId {
                publication: publication_id(world.epoch, published_as),
                index: 0,
            },
            target: world.b,
            target_revision: Revision::new(1),
            packet_id: OutboundPacketId::new(7).unwrap(),
            claim: operation_id(world.epoch, claimed_as),
        }))
        .unwrap(),
        payload: None,
    };
    assert!(world.store.apply(&claim(3, 1)).committed_now);
    let collision = claim(4, 2);
    assert_eq!(
        outcome(world.store.apply(&collision).reply),
        StoreOutcome::RejectedCertain(RejectReason::PreconditionFailed)
    );
    let first_finish = finish(5, 1, 3);
    assert!(world.store.apply(&first_finish).committed_now);
    assert_eq!(
        outcome(world.store.apply(&collision).reply),
        StoreOutcome::RejectedCertain(RejectReason::PreconditionFailed),
        "a certain rejection stays rejected after the slot is freed"
    );
    assert!(world.store.apply(&claim(6, 2)).committed_now);
    let before = world.store.snapshot().unwrap();
    assert!(!world.store.apply(&first_finish).committed_now);
    assert_eq!(
        world.store.snapshot().unwrap(),
        before,
        "a replayed old result owns no newer slot"
    );
    assert_eq!(
        outcome(world.store.apply(&finish(7, 2, 3)).reply),
        StoreOutcome::RejectedCertain(RejectReason::PreconditionFailed)
    );
    let refused = world.store.snapshot().unwrap();
    assert_eq!(refused.outbound, before.outbound);
    assert_eq!(refused.obligations, before.obligations);
    assert_eq!(refused.payloads, before.payloads);
    assert_eq!(
        refused.accounts.exchange_slots,
        before.accounts.exchange_slots
    );
    assert_eq!(accounts_match_what_is_held(&refused), Ok(()));
    assert!(world.store.apply(&finish(8, 2, 6)).committed_now);
    let ended = world.store.snapshot().unwrap();
    assert!(
        ended.outbound.is_empty()
            && ended.obligations.is_empty()
            && ended.publications.is_empty()
            && ended.payloads.is_empty()
    );
    assert_eq!(accounts_match_what_is_held(&ended), Ok(()));
}

/// Opted-in live origins retire compact prefixes without ending live work.
/// The same contract is exercised against both durable implementations.
pub fn live_receipt_collection_is_bounded_and_never_reexecutes<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    use mqttiumd_contract::receipts::{
        ReceiptClosure, ReceiptCollection, ReceiptCollectionReply, ReceiptOrigin,
    };
    let mut limits = fixture_limits();
    limits.receipt_slots = ReceiptSlots::new(2048);
    let mut world = World::with_limits(make, limits);
    let origin = ReceiptOrigin::of(operation_id(world.epoch, 1));
    let empty = ReceiptClosure::empty(origin);
    let collect = |closure, limit| ReceiptCollection { closure, limit };
    assert_eq!(
        world.store.collect_receipts(collect(empty, 1)),
        Ok(ReceiptCollectionReply::Applied {
            closure: empty,
            removed: 0,
            retained: 0,
            more: false,
        })
    );
    let admission = |world: &World<S>, seq| {
        altered(&world.admission(seq, 7, b"same", &[]), |admit| {
            admit.qos = Qos::AtLeastOnce;
            admit.costs.exchange_slots = ExchangeSlots::ZERO;
        })
    };
    let first = admission(&world, 1);
    assert!(world.store.apply(&first).committed_now);
    assert!(
        world
            .store
            .resolve_and_fence(operation_id(world.epoch, 2))
            .fenced_now
    );
    for seq in 3..=4 {
        assert!(world.store.apply(&admission(&world, seq)).committed_now);
    }
    let closure = ReceiptClosure {
        through: OperationSeq::new(4),
        publication_through: PublicationSeq::new(4),
        payload_through: PayloadSeq::new(4),
        ..empty
    };
    for remaining in (0..=3).rev() {
        assert_eq!(
            world.store.collect_receipts(collect(closure, 1)),
            Ok(ReceiptCollectionReply::Applied {
                closure,
                removed: 1,
                retained: remaining,
                more: remaining != 0,
            })
        );
        assert_eq!(world.store.receipt_frontier(origin), Ok(Some(closure)));
        assert_eq!(
            world.store.established(first.recipe.operation()),
            Ok(Some(StoreOutcome::RetiredOutcomeUnavailable))
        );
        assert_eq!(
            outcome(world.store.apply(&first).reply),
            StoreOutcome::RetiredOutcomeUnavailable
        );
        let different = altered(&first, |admit| admit.retain = true);
        assert_eq!(
            outcome(world.store.apply(&different).reply),
            StoreOutcome::RetiredOutcomeUnavailable
        );
        assert_eq!(
            world.store.snapshot().unwrap().accounts.receipt_slots.get(),
            remaining
        );
    }
    let before = world.store.snapshot().unwrap();
    assert_eq!(
        world.store.collect_receipts(collect(closure, 1)),
        Ok(ReceiptCollectionReply::Applied {
            closure,
            removed: 0,
            retained: 0,
            more: false,
        })
    );
    assert_eq!(world.store.snapshot().unwrap().accounts, before.accounts);
    assert!(
        world.store.apply(&admission(&world, 5)).committed_now,
        "the epoch stays live"
    );
}

/// No absent sequence can be closed on the strength of a later receipt.
pub fn live_receipt_collection_refuses_holes_and_preserves_identity_guards<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    use mqttiumd_contract::receipts::{
        ReceiptClosure, ReceiptCollection, ReceiptCollectionRefusal, ReceiptCollectionReply,
        ReceiptOrigin,
    };
    let mut limits = fixture_limits();
    limits.receipt_slots = ReceiptSlots::new(2048);
    let mut world = World::with_limits(make, limits);
    let empty = ReceiptClosure::empty(ReceiptOrigin::of(operation_id(world.epoch, 1)));
    let request = |closure| ReceiptCollection {
        closure,
        limit: 128,
    };
    assert!(matches!(
        world.store.collect_receipts(request(empty)),
        Ok(ReceiptCollectionReply::Applied { .. })
    ));
    let admission = |world: &World<S>, seq| {
        altered(&world.admission(seq, 7, b"payload", &[]), |admit| {
            admit.qos = Qos::AtLeastOnce;
            admit.costs.exchange_slots = ExchangeSlots::ZERO;
        })
    };
    for seq in [3, 1] {
        assert!(world.store.apply(&admission(&world, seq)).committed_now);
    }
    let closure = ReceiptClosure {
        through: OperationSeq::new(3),
        publication_through: PublicationSeq::new(3),
        payload_through: PayloadSeq::new(3),
        ..empty
    };
    let before = world.store.snapshot().unwrap();
    assert_eq!(
        world.store.collect_receipts(request(closure)),
        Ok(ReceiptCollectionReply::Refused(
            ReceiptCollectionRefusal::Gap
        ))
    );
    assert_eq!(world.store.snapshot().unwrap(), before);
    assert!(
        world
            .store
            .resolve_and_fence(operation_id(world.epoch, 2))
            .fenced_now
    );
    assert!(matches!(
        world.store.collect_receipts(request(closure)),
        Ok(ReceiptCollectionReply::Applied { removed: 3, .. })
    ));
    for payload in [false, true] {
        let sequence = if payload { 5 } else { 4 };
        let alias = altered(&admission(&world, sequence), |admit| {
            if payload {
                admit.payload.id =
                    PayloadId::new(INCARNATION, world.epoch, OWNER, PayloadSeq::new(1));
            } else {
                admit.publication = publication_id(world.epoch, 1);
            }
        });
        assert_eq!(
            outcome(world.store.apply(&alias).reply),
            StoreOutcome::RejectedCertain(RejectReason::PreconditionFailed)
        );
        assert!(
            world
                .store
                .snapshot()
                .unwrap()
                .operations
                .contains_key(&alias.recipe.operation()),
            "an alias refusal itself closes its new operation durably"
        );
        assert_eq!(
            world.store.apply(&admission(&world, sequence)).reply,
            StoreReply::Violation(StoreViolation::RecipeMismatch)
        );
    }
    assert!(world.store.apply(&admission(&world, 6)).committed_now);
    assert_eq!(
        accounts_match_what_is_held(&world.store.snapshot().unwrap()),
        Ok(())
    );
}

/// Full business capacity cannot consume the room required to fence a hole;
/// maintenance uses no receipt and recovers the exact charged row count.
pub fn live_receipt_collection_has_a_control_reserve<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    use mqttiumd_contract::receipts::{
        RECEIPT_CONTROL_RESERVE, ReceiptClosure, ReceiptCollection, ReceiptCollectionReply,
        ReceiptOrigin,
    };
    let mut limits = fixture_limits();
    limits.receipt_slots = ReceiptSlots::new(RECEIPT_CONTROL_RESERVE + 2);
    let mut world = World::with_limits(make, limits);
    let empty = ReceiptClosure::empty(ReceiptOrigin::of(operation_id(world.epoch, 1)));
    assert!(matches!(
        world.store.collect_receipts(ReceiptCollection {
            closure: empty,
            limit: 128
        }),
        Ok(ReceiptCollectionReply::Applied { .. })
    ));
    let admission = |world: &World<S>, seq| {
        altered(&world.admission(seq, 7, b"p", &[]), |admit| {
            admit.qos = Qos::AtLeastOnce;
            admit.costs.exchange_slots = ExchangeSlots::ZERO;
        })
    };
    for seq in 1..=2 {
        assert!(world.store.apply(&admission(&world, seq)).committed_now);
    }
    assert_eq!(
        outcome(world.store.apply(&admission(&world, 3)).reply),
        StoreOutcome::PendingOrUnknown
    );
    assert_eq!(
        world.store.snapshot().unwrap().accounts.receipt_slots.get(),
        2
    );
    assert!(
        world
            .store
            .resolve_and_fence(operation_id(world.epoch, 3))
            .fenced_now
    );
    let closure = ReceiptClosure {
        through: OperationSeq::new(3),
        publication_through: PublicationSeq::new(3),
        payload_through: PayloadSeq::new(3),
        ..empty
    };
    assert_eq!(
        world.store.collect_receipts(ReceiptCollection {
            closure,
            limit: 128
        }),
        Ok(ReceiptCollectionReply::Applied {
            closure,
            removed: 3,
            retained: 0,
            more: false
        })
    );
    assert!(world.store.apply(&admission(&world, 4)).committed_now);
}

/// Legacy sequence zero must never be silently covered by an opt-in empty prefix.
pub fn live_receipt_registration_preserves_legacy_zero<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    use mqttiumd_contract::receipts::{
        ReceiptClosure, ReceiptCollection, ReceiptCollectionRefusal, ReceiptCollectionReply,
        ReceiptOrigin,
    };
    let mut world = World::new(make);
    let zero = altered(&world.admission(0, 7, b"gone", &[]), |admit| {
        admit.qos = Qos::AtLeastOnce;
        admit.costs.exchange_slots = ExchangeSlots::ZERO;
    });
    assert!(world.store.apply(&zero).committed_now);
    let before = world.store.snapshot().unwrap();
    assert!(before.publications.is_empty() && before.payloads.is_empty());
    let closure = ReceiptClosure::empty(ReceiptOrigin::of(zero.recipe.operation()));
    assert_eq!(
        world.store.collect_receipts(ReceiptCollection {
            closure,
            limit: 128
        }),
        Ok(ReceiptCollectionReply::Refused(
            ReceiptCollectionRefusal::InvalidFrontier
        ))
    );
    assert_eq!(world.store.snapshot().unwrap(), before);
    assert!(matches!(
        outcome(world.store.apply(&zero).reply),
        StoreOutcome::Committed(_)
    ));
}

/// A compact closure supersedes any physical receipt left by a partial sweep.
/// Recovery must not republish that retained row as an unconsumed terminal fact.
pub fn recovery_omits_logically_retired_receipts_but_preserves_live_claims<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    use mqttiumd_contract::command::{ClaimDelivery, FinishDelivery};
    use mqttiumd_contract::ids::OutboundPacketId;
    use mqttiumd_contract::receipts::{
        ReceiptClosure, ReceiptCollection, ReceiptCollectionReply, ReceiptOrigin,
    };
    let mut limits = fixture_limits();
    limits.receipt_slots = ReceiptSlots::new(2048);
    let mut world = World::with_limits(make, limits);
    let old_epoch = world.epoch;
    let origin = ReceiptOrigin::of(operation_id(old_epoch, 1));
    let empty = ReceiptClosure::empty(origin);
    assert!(matches!(
        world.store.collect_receipts(ReceiptCollection {
            closure: empty,
            limit: 128
        }),
        Ok(ReceiptCollectionReply::Applied { .. })
    ));
    let admission = world.admission(1, 7, b"alive", &[world.b, world.c]);
    assert!(world.store.apply(&admission).committed_now);
    let delivery = DeliveryId {
        publication: publication_id(old_epoch, 1),
        index: 0,
    };
    let claim = Command {
        recipe: FrozenRecipe::freeze(Recipe::ClaimDelivery(ClaimDelivery {
            operation: operation_id(old_epoch, 2),
            delivery,
            target: world.b,
            target_revision: Revision::new(1),
            packet_id: OutboundPacketId::new(9).unwrap(),
        }))
        .unwrap(),
        payload: None,
    };
    assert!(world.store.apply(&claim).committed_now);
    let before = world.store.snapshot().unwrap();
    let closure = ReceiptClosure {
        through: OperationSeq::new(2),
        publication_through: PublicationSeq::new(1),
        payload_through: PayloadSeq::new(1),
        ..empty
    };
    assert!(matches!(
        world
            .store
            .collect_receipts(ReceiptCollection { closure, limit: 1 }),
        Ok(ReceiptCollectionReply::Applied {
            removed: 1,
            more: true,
            ..
        })
    ));
    let after = world.store.snapshot().unwrap();
    assert_eq!(after.obligations, before.obligations);
    assert_eq!(after.outbound, before.outbound);
    assert_eq!(after.inbound, before.inbound);
    assert_eq!(after.payloads, before.payloads);
    world.epoch = open(&mut world.store);
    let inventory = world.store.recover(None, 64).unwrap();
    assert!(
        inventory
            .facts
            .as_slice()
            .contains(&RecoveredFact::ReceiptFrontier(closure))
    );
    assert!(!inventory.facts.as_slice().iter().any(|fact| matches!(fact,
        RecoveredFact::Operation { operation, .. } if ReceiptOrigin::of(*operation) == origin && operation.sequence() <= closure.through
    )), "a physical leftover is already logically retired");
    let finish = Command {
        recipe: FrozenRecipe::freeze(Recipe::FinishDelivery(FinishDelivery {
            operation: operation_id(world.epoch, 1),
            delivery,
            target: world.b,
            target_revision: Revision::new(1),
            packet_id: OutboundPacketId::new(9).unwrap(),
            claim: operation_id(old_epoch, 2),
        }))
        .unwrap(),
        payload: None,
    };
    assert!(
        world.store.apply(&finish).committed_now,
        "the old claim's receipt is not its live identity guard"
    );
    let finished = world.store.snapshot().unwrap();
    assert_eq!(finished.obligations.len(), 1);
    assert!(finished.outbound.is_empty());
    assert_eq!(finished.payloads.values().next().unwrap().references, 1);
    assert_eq!(accounts_match_what_is_held(&finished), Ok(()));
}

/// Floors cannot jump over missing receipts, silently reinterpret sequence
/// zero, or lose admission identity evidence by closing too small an ID floor.
pub fn live_receipt_frontiers_validate_registration_and_bounds<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    use mqttiumd_contract::receipts::{
        MAX_RECEIPT_ORIGINS, ReceiptClosure, ReceiptCollection, ReceiptCollectionRefusal,
        ReceiptCollectionReply, ReceiptOrigin,
    };
    let mut limits = fixture_limits();
    limits.receipt_slots = ReceiptSlots::new(2048);
    let mut world = World::with_limits(make, limits);
    let empty = ReceiptClosure::empty(ReceiptOrigin::of(operation_id(world.epoch, 1)));
    let request = |closure, limit| ReceiptCollection { closure, limit };
    assert!(matches!(
        world.store.collect_receipts(request(empty, 128)),
        Ok(ReceiptCollectionReply::Applied { .. })
    ));
    let before = world.store.snapshot().unwrap();
    for (request, why) in [
        (request(empty, 0), ReceiptCollectionRefusal::LimitExceeded),
        (request(empty, 129), ReceiptCollectionRefusal::LimitExceeded),
        (
            request(
                ReceiptClosure {
                    through: OperationSeq::new(129),
                    ..empty
                },
                128,
            ),
            ReceiptCollectionRefusal::LimitExceeded,
        ),
        (
            request(
                ReceiptClosure {
                    publication_through: PublicationSeq::new(1),
                    ..empty
                },
                128,
            ),
            ReceiptCollectionRefusal::InvalidFrontier,
        ),
        (
            request(
                ReceiptClosure {
                    origin: ReceiptOrigin {
                        incarnation: StoreIncarnation::new(2),
                        ..empty.origin
                    },
                    ..empty
                },
                128,
            ),
            ReceiptCollectionRefusal::OriginMismatch,
        ),
        (
            request(
                ReceiptClosure {
                    origin: ReceiptOrigin {
                        epoch: RunEpoch::new(2),
                        ..empty.origin
                    },
                    ..empty
                },
                128,
            ),
            ReceiptCollectionRefusal::FutureEpoch,
        ),
    ] {
        assert_eq!(
            world.store.collect_receipts(request),
            Ok(ReceiptCollectionReply::Refused(why))
        );
        assert_eq!(world.store.snapshot().unwrap(), before);
    }
    let far = altered(&world.admission(769, 7, b"p", &[]), |admit| {
        admit.qos = Qos::AtLeastOnce;
        admit.costs.exchange_slots = ExchangeSlots::ZERO;
    });
    assert_eq!(
        outcome(world.store.apply(&far).reply),
        StoreOutcome::PendingOrUnknown
    );
    assert_eq!(
        world
            .store
            .resolve_and_fence(operation_id(world.epoch, 1025))
            .outcome,
        StoreOutcome::PendingOrUnknown
    );
    assert_eq!(world.store.snapshot().unwrap(), before);
    for owner in 1..MAX_RECEIPT_ORIGINS {
        let closure = ReceiptClosure::empty(ReceiptOrigin {
            owner: LogicalOwnerId::new(owner as u64),
            ..empty.origin
        });
        assert!(matches!(
            world.store.collect_receipts(request(closure, 128)),
            Ok(ReceiptCollectionReply::Applied { .. })
        ));
    }
    let full = world.store.snapshot().unwrap();
    let extra = ReceiptClosure::empty(ReceiptOrigin {
        owner: LogicalOwnerId::new(MAX_RECEIPT_ORIGINS as u64),
        ..empty.origin
    });
    assert_eq!(
        world.store.collect_receipts(request(extra, 128)),
        Ok(ReceiptCollectionReply::Refused(
            ReceiptCollectionRefusal::OriginCapacity
        ))
    );
    assert_eq!(world.store.snapshot().unwrap(), full);
    for payload in [false, true] {
        let mut legacy = World::new(make);
        let zero_id = altered(&legacy.admission(1, 7, b"gone", &[]), |admit| {
            admit.qos = Qos::AtLeastOnce;
            admit.costs.exchange_slots = ExchangeSlots::ZERO;
            if payload {
                admit.payload.id =
                    PayloadId::new(INCARNATION, legacy.epoch, OWNER, PayloadSeq::new(0));
            } else {
                admit.publication = publication_id(legacy.epoch, 0);
            }
        });
        assert!(legacy.store.apply(&zero_id).committed_now);
        let before = legacy.store.snapshot().unwrap();
        let closure = ReceiptClosure::empty(ReceiptOrigin::of(zero_id.recipe.operation()));
        assert_eq!(
            legacy.store.collect_receipts(request(closure, 128)),
            Ok(ReceiptCollectionReply::Refused(
                ReceiptCollectionRefusal::InvalidFrontier
            ))
        );
        assert_eq!(legacy.store.snapshot().unwrap(), before);
    }
}

/// A full receipt budget still permits collection; live inbound markers do
/// not pin their old receipts, and a release uses the resulting control room.
pub fn collection_at_full_quota_preserves_business_and_startup_collapses_origins<
    S: DurableStore,
>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    use mqttiumd_contract::receipts::{
        ClosedReceiptCollection, ClosedReceiptReply, RECEIPT_CONTROL_RESERVE, ReceiptClosure,
        ReceiptCollection, ReceiptCollectionReply, ReceiptOrigin,
    };
    let mut limits = fixture_limits();
    limits.receipt_slots = ReceiptSlots::new(RECEIPT_CONTROL_RESERVE + 2);
    let mut world = World::with_limits(make, limits);
    let old_epoch = world.epoch;
    let empty = ReceiptClosure::empty(ReceiptOrigin::of(operation_id(old_epoch, 1)));
    assert!(matches!(
        world.store.collect_receipts(ReceiptCollection {
            closure: empty,
            limit: 128
        }),
        Ok(ReceiptCollectionReply::Applied { .. })
    ));
    for seq in 1..=2 {
        assert!(
            world
                .store
                .apply(&world.admission(seq, seq as u16 + 6, b"alive", &[]))
                .committed_now
        );
    }
    for seq in 3..=RECEIPT_CONTROL_RESERVE + 2 {
        assert!(
            world
                .store
                .resolve_and_fence(operation_id(old_epoch, seq))
                .fenced_now
        );
    }
    let full = world.store.snapshot().unwrap();
    assert_eq!(full.accounts.receipt_slots, limits.receipt_slots);
    let closure = ReceiptClosure {
        through: OperationSeq::new(128),
        publication_through: PublicationSeq::new(2),
        payload_through: PayloadSeq::new(2),
        ..empty
    };
    assert!(matches!(
        world.store.collect_receipts(ReceiptCollection {
            closure,
            limit: 128
        }),
        Ok(ReceiptCollectionReply::Applied { removed: 128, .. })
    ));
    let after = world.store.snapshot().unwrap();
    assert_eq!(after.inbound, full.inbound);
    assert_eq!(after.publications, full.publications);
    assert!(world.store.apply(&world.release(515, 7, 1)).committed_now);
    world.epoch = open(&mut world.store);
    let before = world.store.snapshot().unwrap();
    let mut passes = 0;
    loop {
        passes += 1;
        let reply = world
            .store
            .collect_closed_receipts(ClosedReceiptCollection {
                through: old_epoch,
                limit: 17,
            })
            .unwrap();
        let ClosedReceiptReply::Applied {
            removed,
            removed_origins,
            more,
            ..
        } = reply
        else {
            panic!("{reply:?}");
        };
        assert!(u32::from(removed) + u32::from(removed_origins) <= 17);
        let state = world.store.snapshot().unwrap();
        assert_eq!(state.inbound, before.inbound);
        assert_eq!(state.publications, before.publications);
        assert_eq!(state.payloads, before.payloads);
        assert_eq!(state.obligations, before.obligations);
        assert_eq!(accounts_match_what_is_held(&state), Ok(()));
        if !more {
            break;
        }
        assert!(passes < 40, "bounded physical progress");
    }
    let collected = world.store.snapshot().unwrap();
    assert!(collected.receipt_frontiers.is_empty());
    assert_eq!(collected.accounts.receipt_slots.get(), 0);
    assert_eq!(collected.collected_through, old_epoch);
    assert_eq!(
        world.store.established(operation_id(old_epoch, 514)),
        Ok(Some(StoreOutcome::RetiredOutcomeUnavailable))
    );
    assert!(world.store.apply(&world.release(1, 8, 2)).committed_now);
}

/// The largest legal product subscription recipes still fit a full 128-step
/// collection pass after a held/lost response lets consumed work accumulate.
pub fn a_maximum_product_subscription_prefix_fits_the_collection_budget<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    use mqttiumd_contract::command::UpdateSubscriptions;
    use mqttiumd_contract::pubsub::{GrantedSubscriptionChange, MAX_FILTERS};
    use mqttiumd_contract::receipts::{
        ReceiptClosure, ReceiptCollection, ReceiptCollectionReply, ReceiptOrigin,
    };
    let limits = Limits {
        payload_bytes: PayloadBytes::new(4 * 1024 * 1024),
        references: References::new(128),
        exchange_slots: ExchangeSlots::new(128),
        receipt_slots: ReceiptSlots::new(8192),
        max_targets_per_admission: 16,
        max_topic_bytes: 1024,
        max_payload_bytes: 32768,
        max_sessions: 128,
        max_route_targets: 16,
    };
    let mut world = World::with_limits(make, limits);
    let empty = ReceiptClosure::empty(ReceiptOrigin::of(operation_id(world.epoch, 1)));
    assert!(matches!(
        world.store.collect_receipts(ReceiptCollection {
            closure: empty,
            limit: 128
        }),
        Ok(ReceiptCollectionReply::Applied { .. })
    ));
    let filters: Vec<_> = (0..MAX_FILTERS)
        .map(|index| {
            let mut topic = vec![b'a'; 1024];
            topic[1023] = b'0' + u8::try_from(index).unwrap();
            GrantedSubscriptionChange {
                options: mqttiumd_contract::pubsub::SubscriptionOptions::DEFAULT,
                topic: SharedBytes::copy_from(&topic, 1024).unwrap(),
                granted: Some(Qos::AtLeastOnce),
            }
        })
        .collect();
    let mut metered_bytes = 0usize;
    for sequence in 1..=128 {
        let command = Command {
            recipe: FrozenRecipe::freeze(Recipe::UpdateSubscriptions(UpdateSubscriptions {
                operation: operation_id(world.epoch, sequence),
                session: world.b,
                session_revision: Revision::new(1),
                route_revision: world.route_revision,
                changes: BoundedVec::from_slice(&filters, MAX_FILTERS).unwrap(),
            }))
            .unwrap(),
            payload: None,
        };
        let mut canonical = Vec::new();
        command
            .recipe
            .get()
            .encode_canonical(&mut canonical)
            .unwrap();
        metered_bytes = metered_bytes.checked_add(128 + canonical.len()).unwrap();
        let applied = world.store.apply(&command);
        let StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::SubscriptionsChanged {
            route_revision,
            ..
        })) = applied.reply
        else {
            panic!("legal product replacement {sequence} failed: {applied:?}");
        };
        assert!(applied.committed_now);
        world.route_revision = route_revision;
    }
    assert!(
        metered_bytes > ReadBudget::PAGE.max_bytes,
        "the witness must exceed the ordinary page budget"
    );
    let before = world.store.snapshot().unwrap();
    assert_eq!(before.operations.len(), 128);
    assert_eq!(before.accounts.receipt_slots.get(), 128);
    let closure = ReceiptClosure {
        through: OperationSeq::new(128),
        ..empty
    };
    assert_eq!(
        world.store.collect_receipts(ReceiptCollection {
            closure,
            limit: 128
        }),
        Ok(ReceiptCollectionReply::Applied {
            closure,
            removed: 128,
            retained: 0,
            more: false
        }),
        "{metered_bytes} metered bytes are inside the supported product profile"
    );
    let after = world.store.snapshot().unwrap();
    assert_eq!(after.routes, before.routes);
    assert_eq!(after.route_revision, before.route_revision);
    assert!(after.operations.is_empty());
}

/// Recycled wire IDs do not recycle a delivery/claim identity, even after
/// terminal receipts have been collected. This uses only the durable contract.
pub fn recycled_packet_ids_preserve_occurrences_after_receipt_collection<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    use mqttiumd_contract::command::{ClaimDelivery, FinishDelivery};
    use mqttiumd_contract::ids::OutboundPacketId;
    use mqttiumd_contract::receipts::{
        ReceiptClosure, ReceiptCollection, ReceiptCollectionReply, ReceiptOrigin,
    };
    let mut limits = fixture_limits();
    limits.receipt_slots = ReceiptSlots::new(8192);
    let mut world = World::with_limits(make, limits);
    let empty = ReceiptClosure::empty(ReceiptOrigin::of(operation_id(world.epoch, 1)));
    assert!(matches!(
        world.store.collect_receipts(ReceiptCollection {
            closure: empty,
            limit: 128
        }),
        Ok(ReceiptCollectionReply::Applied { .. })
    ));
    let delivery = |published_as| DeliveryId {
        publication: publication_id(world.epoch, published_as),
        index: 0,
    };
    let claim = |sequence, published_as| Command {
        recipe: FrozenRecipe::freeze(Recipe::ClaimDelivery(ClaimDelivery {
            operation: operation_id(world.epoch, sequence),
            delivery: delivery(published_as),
            target: world.b,
            target_revision: Revision::new(1),
            packet_id: OutboundPacketId::new(1).unwrap(),
        }))
        .unwrap(),
        payload: None,
    };
    let finish = |sequence, published_as, claimed_as| Command {
        recipe: FrozenRecipe::freeze(Recipe::FinishDelivery(FinishDelivery {
            operation: operation_id(world.epoch, sequence),
            delivery: delivery(published_as),
            target: world.b,
            target_revision: Revision::new(1),
            packet_id: OutboundPacketId::new(1).unwrap(),
            claim: operation_id(world.epoch, claimed_as),
        }))
        .unwrap(),
        payload: None,
    };
    let (epoch, route_revision, producer, target) =
        (world.epoch, world.route_revision, world.producer, world.b);
    let admit = |published_as| {
        altered(
            &admission_in(
                epoch,
                route_revision,
                producer,
                published_as,
                1,
                b"same bytes",
                &[target],
            ),
            |admit| {
                admit.qos = Qos::AtLeastOnce;
                admit.costs.exchange_slots = ExchangeSlots::ZERO;
            },
        )
    };
    let a = admit(1);
    let a_claim = claim(2, 1);
    let a_finish = finish(3, 1, 2);
    for command in [&a, &a_claim, &a_finish] {
        assert!(world.store.apply(command).committed_now);
    }
    let closure = ReceiptClosure {
        through: OperationSeq::new(3),
        publication_through: PublicationSeq::new(1),
        payload_through: PayloadSeq::new(1),
        ..empty
    };
    assert!(matches!(
        world.store.collect_receipts(ReceiptCollection {
            closure,
            limit: 128
        }),
        Ok(ReceiptCollectionReply::Applied {
            removed: 3,
            retained: 0,
            ..
        })
    ));
    assert!(world.store.snapshot().unwrap().operations.is_empty());
    assert!(world.store.apply(&admit(4)).committed_now);
    assert!(world.store.apply(&claim(5, 4)).committed_now);
    let b = world.store.snapshot().unwrap();
    assert_eq!(b.outbound[&delivery(4)].packet_id.get(), 1);
    assert_eq!(b.outbound[&delivery(4)].claim, operation_id(world.epoch, 5));
    for obsolete in [&a_finish, &finish(3, 4, 5)] {
        assert_eq!(
            outcome(world.store.apply(obsolete).reply),
            StoreOutcome::RetiredOutcomeUnavailable
        );
        assert_eq!(
            world.store.snapshot().unwrap(),
            b,
            "a retired operation cannot own the recycled PI"
        );
    }
    for forged in [finish(6, 1, 2), finish(7, 4, 2)] {
        assert_eq!(
            outcome(world.store.apply(&forged).reply),
            StoreOutcome::RejectedCertain(RejectReason::PreconditionFailed)
        );
        let guarded = world.store.snapshot().unwrap();
        assert_eq!(guarded.outbound, b.outbound);
        assert_eq!(guarded.obligations, b.obligations);
        assert_eq!(guarded.payloads, b.payloads);
        assert_eq!(guarded.accounts.exchange_slots, b.accounts.exchange_slots);
        assert_eq!(guarded.accounts.references, b.accounts.references);
        assert_eq!(guarded.accounts.payload_bytes, b.accounts.payload_bytes);
    }
    let b_finish = finish(8, 4, 5);
    assert!(world.store.apply(&b_finish).committed_now);
    let terminal = world.store.snapshot().unwrap();
    assert!(
        terminal.outbound.is_empty()
            && terminal.obligations.is_empty()
            && terminal.payloads.is_empty()
            && terminal.publications.is_empty()
    );
    assert_eq!(terminal.accounts.exchange_slots, ExchangeSlots::ZERO);
    assert_eq!(accounts_match_what_is_held(&terminal), Ok(()));
    assert!(!world.store.apply(&b_finish).committed_now);
    assert_eq!(
        world.store.snapshot().unwrap(),
        terminal,
        "a duplicate Finish never returns credit twice"
    );
}

/// A committed Claim with a lost result still owns the directional wire ID.
/// Resolution uses the same OperationId; a separate fresh claim can reuse only
/// after the old obligation's Finish is durably established.
pub fn lost_claim_results_preserve_packet_reservations_until_guarded_finish<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    use mqttiumd_contract::command::{ClaimDelivery, FinishDelivery};
    use mqttiumd_contract::ids::OutboundPacketId;
    let mut limits = fixture_limits();
    limits.receipt_slots = ReceiptSlots::new(8192);
    let mut world = World::with_limits(make, limits);
    let delivery = |published_as| DeliveryId {
        publication: publication_id(world.epoch, published_as),
        index: 0,
    };
    let claim = |sequence, published_as| Command {
        recipe: FrozenRecipe::freeze(Recipe::ClaimDelivery(ClaimDelivery {
            operation: operation_id(world.epoch, sequence),
            delivery: delivery(published_as),
            target: world.b,
            target_revision: Revision::new(1),
            packet_id: OutboundPacketId::new(1).unwrap(),
        }))
        .unwrap(),
        payload: None,
    };
    for seq in 1..=2 {
        let admission = altered(&world.admission(seq, 1, b"pending", &[world.b]), |admit| {
            admit.qos = Qos::AtLeastOnce;
            admit.costs.exchange_slots = ExchangeSlots::ZERO;
        });
        assert!(world.store.apply(&admission).committed_now);
    }
    let a_claim = claim(3, 1);
    let _lost_claim_reply = world.store.apply(&a_claim);
    let reserved = world.store.snapshot().unwrap();
    assert_eq!(
        reserved.outbound[&delivery(1)].claim,
        a_claim.recipe.operation()
    );
    assert_eq!(
        outcome(world.store.apply(&claim(4, 2)).reply),
        StoreOutcome::RejectedCertain(RejectReason::PreconditionFailed)
    );
    assert_eq!(world.store.snapshot().unwrap().outbound, reserved.outbound);
    let resolved_claim = world.store.resolve_and_fence(a_claim.recipe.operation());
    assert_eq!(
        resolved_claim.outcome,
        StoreOutcome::Committed(CommitFacts::DeliveryClaimed {
            delivery: delivery(1),
            target: world.b,
            packet_id: OutboundPacketId::new(1).unwrap(),
            claim: operation_id(world.epoch, 3)
        })
    );
    assert!(!resolved_claim.fenced_now);
    let a_finish = Command {
        recipe: FrozenRecipe::freeze(Recipe::FinishDelivery(FinishDelivery {
            operation: operation_id(world.epoch, 5),
            delivery: delivery(1),
            target: world.b,
            target_revision: Revision::new(1),
            packet_id: OutboundPacketId::new(1).unwrap(),
            claim: operation_id(world.epoch, 3),
        }))
        .unwrap(),
        payload: None,
    };
    // The store can establish durability, not whether an owner applied a reply.
    // The consumer holds its volatile reservation until this same-op lookup.
    let _lost_finish_reply = world.store.apply(&a_finish);
    let after_finish = world.store.snapshot().unwrap();
    let resolved = world.store.resolve_and_fence(a_finish.recipe.operation());
    assert!(
        matches!(resolved.outcome,StoreOutcome::Committed(CommitFacts::DeliveryFinished {claim,..}) if claim==operation_id(world.epoch,3))
    );
    assert!(!resolved.fenced_now);
    assert_eq!(
        world.store.snapshot().unwrap(),
        after_finish,
        "resolution adds no recursive receipt/fence"
    );
    assert!(world.store.apply(&claim(6, 2)).committed_now);
    let reused = world.store.snapshot().unwrap();
    assert!(!world.store.apply(&a_finish).committed_now);
    assert_eq!(world.store.snapshot().unwrap(), reused);
    assert_eq!(
        reused.outbound[&delivery(2)].claim,
        operation_id(world.epoch, 6)
    );
}
