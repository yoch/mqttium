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
    AdmitPublication, Command, Costs, DeliveryCause, EpochPhase, FrozenRecipe, PayloadRef, Recipe,
    ReleaseInbound, Source, Target,
};
use mqttiumd_contract::effects::{InboundPhase, RecoveredFact};
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
    ObligationPhase, RecoveryRefused, Resolved, SessionState,
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

/// Opens the next epoch and returns it.
pub fn open<S: DurableStore>(store: &mut S) -> RunEpoch {
    match store.open_epoch() {
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

/// The state without what a rejection is allowed to add: its receipt
/// and the revision of the commit that recorded it.
fn without_receipts(mut state: DurableState) -> DurableState {
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
    assert_eq!(store.snapshot().ledger.open_epoch(), None);
    assert_eq!(open(&mut store), RunEpoch::new(1));
    assert_eq!(open(&mut store), RunEpoch::new(2));
    let ledger = store.snapshot().ledger;
    assert_eq!(ledger.incarnation(), INCARNATION);
    assert_eq!(ledger.open_epoch(), Some(RunEpoch::new(2)));
    assert_eq!(ledger.phase_of(RunEpoch::new(1)), EpochPhase::Closed);
    assert_eq!(ledger.phase_of(RunEpoch::new(3)), EpochPhase::Unopened);
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
    assert!(world.store.snapshot().publications.is_empty());
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

    let state = world.store.snapshot();
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
    let before = world.store.snapshot();
    let second = world.store.apply(&command);
    assert_eq!(second.reply, first.reply);
    assert!(!second.committed_now);
    assert_eq!(world.store.snapshot(), before);
}

/// A guard that fails leaves no admitted subset, whichever guard it is
/// and however many rows were prepared before it (ADM-002, MU-24).
pub fn a_failed_guard_leaves_no_admitted_subset<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    let precondition = StoreOutcome::RejectedCertain(RejectReason::PreconditionFailed);

    // The last target names a revision the session does not have.
    let mut world = World::new(make);
    let before = world.store.snapshot();
    let stale = altered(
        &world.admission(1, 7, b"payload", &[world.b, world.c]),
        |admit| admit.targets.as_mut_slice()[1].session_revision = Revision::new(9),
    );
    assert_eq!(outcome(world.store.apply(&stale).reply), precondition);
    assert_eq!(
        without_receipts(world.store.snapshot()),
        without_receipts(before)
    );

    // The last account checked is short.
    let mut tight = fixture_limits();
    tight.receipt_slots = ReceiptSlots::new(0);
    let mut world = World::with_limits(make, tight);
    let before = world.store.snapshot();
    let command = world.admission(1, 7, b"payload", &[world.b, world.c]);
    assert_eq!(
        outcome(world.store.apply(&command).reply),
        StoreOutcome::RejectedCertain(RejectReason::CapacityExceeded(Resource::ReceiptSlots))
    );
    assert_eq!(
        without_receipts(world.store.snapshot()),
        without_receipts(before)
    );

    // The inbound slot is taken by an earlier admission: a newcomer on
    // the same packet, under a fresh exchange, admits nothing.
    let mut world = World::new(make);
    let first = world.admission(1, 7, b"payload", &[world.b, world.c]);
    assert!(world.store.apply(&first).committed_now);
    let before = world.store.snapshot();
    let hidden = world.admission(2, 7, b"payload", &[world.b, world.c]);
    assert_eq!(outcome(world.store.apply(&hidden).reply), precondition);
    let after = world.store.snapshot();
    assert_eq!(after.publications, before.publications);
    assert_eq!(after.obligations, before.obligations);
    assert_eq!(after.payloads, before.payloads);
    assert_eq!(after.inbound, before.inbound);
    assert_eq!(after.accounts, before.accounts);
}

/// Each account refuses an admission it cannot hold, naming itself.
pub fn each_account_is_checked<S: DurableStore>(make: &dyn Fn(StoreIncarnation, Limits) -> S) {
    type Tighten = fn(&mut Limits);
    let cases: [(Tighten, Resource); 4] = [
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
        (
            |limits| limits.receipt_slots = ReceiptSlots::new(0),
            Resource::ReceiptSlots,
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
        assert_eq!(world.store.snapshot().accounts, Accounts::default());
    }
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
    let released = world.store.snapshot();
    assert_eq!(released.accounts.references, References::new(1));
    // `c` left the route with its session.
    world.route_revision = released.route_revision;

    let before = world.store.snapshot();
    let again = world.store.apply(&refused);
    assert_eq!(outcome(again.reply), rejected);
    assert!(!again.committed_now);
    assert_eq!(world.store.snapshot(), before);
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
    let before = world.store.snapshot();

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
        assert_eq!(world.store.snapshot(), before);
    }
    // The same recipe with other payload bytes of the same length.
    let other_bytes = Command {
        recipe: command.recipe.clone(),
        payload: Some(SharedBytes::copy_from(b"PAYLOAD", 1024).expect("fits")),
    };
    assert_eq!(world.store.apply(&other_bytes).reply, violation);
    assert_eq!(world.store.snapshot(), before);

    // After a rejection as well.
    let rejected = altered(&world.admission(2, 8, b"payload", &[world.b]), |admit| {
        admit.source.session_revision = Revision::new(9);
    });
    assert!(matches!(
        world.store.apply(&rejected).reply,
        StoreReply::Outcome(StoreOutcome::RejectedCertain(_))
    ));
    let before = world.store.snapshot();
    let repaired = world.admission(2, 8, b"payload", &[world.b]);
    assert_eq!(world.store.apply(&repaired).reply, violation);
    assert_eq!(world.store.snapshot(), before);
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
    let before = world.store.snapshot();
    assert!(before.fences.contains(&operation_id(world.epoch, 2)));

    let late = world.store.apply(&absent);
    assert_eq!(outcome(late.reply), fenced);
    assert!(!late.committed_now);
    assert_eq!(world.store.snapshot(), before);
    // The fence is established once.
    assert_eq!(
        world.store.resolve_and_fence(operation_id(world.epoch, 2)),
        Resolved {
            outcome: fenced,
            fenced_now: false
        }
    );
    assert_eq!(world.store.snapshot(), before);
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
    assert_eq!(world.store.snapshot().inbound.len(), 1);

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
    let state = world.store.snapshot();
    assert!(state.inbound.is_empty());
    assert_eq!(state.accounts.exchange_slots, ExchangeSlots::ZERO);
    // The release creates no distribution and ends none.
    assert_eq!(state.obligations.len(), 2);
    assert_eq!(state.publications.len(), 1);

    let again = world.store.apply(&release);
    assert_eq!(outcome(again.reply), released);
    assert!(!again.committed_now);
    assert_eq!(world.store.snapshot(), state);

    // A second release of the exchange, under a new identity, finds no
    // slot to end.
    assert_eq!(
        outcome(world.store.apply(&world.release(5, 7, 1)).reply),
        precondition
    );
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
    let before = world.store.snapshot();

    assert_eq!(
        world.store.admin(AdminCommand::DeleteSession {
            session: world.producer
        }),
        Ok(AdminDone::SessionDeleted)
    );
    let state = world.store.snapshot();
    assert_eq!(
        state.sessions[&world.producer].state,
        SessionState::Terminal
    );
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
    let state = world.store.snapshot();
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
    let state = world.store.snapshot();
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
    let before = world.store.snapshot();

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
    assert_eq!(world.store.snapshot(), before);

    // The same request prepared in the new epoch is admitted, and the
    // obligations of the older epoch are still there.
    world.epoch = new_epoch;
    let prepared_again = world.admission(2, 8, b"payload", &[world.b]);
    assert!(world.store.apply(&prepared_again).committed_now);
    assert_eq!(world.store.snapshot().obligations.len(), 3);
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
    let state = world.store.snapshot();
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
    assert_eq!(world.store.snapshot(), state);
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
    let before = world.store.snapshot();
    assert_eq!(
        world.store.resolve_and_fence(foreign),
        settled(StoreOutcome::RejectedCertain(
            RejectReason::PreconditionFailed
        ))
    );
    assert_eq!(world.store.snapshot(), before);
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
    assert_eq!(facts.len(), 1 + 3 + 1 + 3);

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
    let before = world.store.snapshot();
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
        assert_eq!(world.store.snapshot(), before);
    }
}

/// Every check of this module, in order.
pub fn all<S: DurableStore>(make: &dyn Fn(StoreIncarnation, Limits) -> S) {
    opening_opens_the_next_epoch_and_closes_the_older_ones(make);
    a_command_of_an_unopened_epoch_never_executes(make);
    an_admission_commits_everything_together(make);
    a_repeated_command_returns_its_result_without_a_second_effect(make);
    a_failed_guard_leaves_no_admitted_subset(make);
    each_account_is_checked(make);
    a_rejection_stays_a_rejection_after_quota_is_released(make);
    another_recipe_under_the_same_operation_is_a_violation(make);
    resolve_returns_a_commit_and_fences_an_absence(make);
    a_release_ends_the_inbound_exchange(make);
    deleting_the_producer_deletes_no_obligation_of_another_session(make);
    an_older_epoch_is_not_entered_again(make);
    a_collected_identity_is_retired_not_rejected(make);
    an_identity_of_another_data_set_is_refused(make);
    recovery_reads_bounded_pages_of_durable_facts(make);
    administration_refusals_change_nothing(make);
}
