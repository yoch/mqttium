//! The durable state of the store and the commands that change it.
//!
//! `DurableStore` is the interface of the writer: every method is one
//! step in the single order of authority (DEM-011). `MemoryStore` is its
//! deterministic realisation; the SQLite store of lot L7 implements the
//! same trait and passes the same suite (`conformance`).
//!
//! A command is prepared against the committed state, on a staged copy
//! that nothing else can see, and the copy replaces the committed state
//! only when every guard passed. A guard that fails drops the copy, so no
//! subset of an admission is ever admitted (ADM-002).

use std::collections::{BTreeMap, BTreeSet};

use mqttiumd_contract::capacity::{
    BoundedVec, ExchangeSlots, PayloadBytes, ReceiptSlots, References, Resource,
};
use mqttiumd_contract::command::{
    AdmitPublication, Command, DeliveryCause, EpochLedger, EpochPhase, LedgerOpening, OpenEpoch,
    Recipe, ReleaseInbound,
};
use mqttiumd_contract::effects::{InboundPhase, RecoveredFact};
use mqttiumd_contract::ids::{
    DeliveryId, ExchangeId, InboundPacketId, OperationId, PayloadId, PublicationId, Revision,
    RunEpoch, SessionId, StoreIncarnation,
};
use mqttiumd_contract::outcome::{
    CommitFacts, Knowledge, OpeningOutcome, RejectReason, StoreOutcome, StoreReply, StoreViolation,
};
use mqttiumd_contract::packet::{Qos, SharedBytes};

/// The longest ClientID MQTT can carry: a UTF-8 string with a two-byte
/// length. A session never holds a longer one, and a recovered session
/// fact is built within this budget.
pub const MAX_CLIENT_ID_BYTES: usize = 65_535;

/// The durable limits of a data set, fixed when it is created.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Limits {
    pub payload_bytes: PayloadBytes,
    pub references: References,
    pub exchange_slots: ExchangeSlots,
    pub receipt_slots: ReceiptSlots,
    pub max_targets_per_admission: usize,
    pub max_topic_bytes: usize,
    pub max_payload_bytes: usize,
    /// Active sessions at a time. Terminal ones are not counted: their
    /// identity is never used again, and they hold nothing.
    pub max_sessions: usize,
    pub max_route_targets: usize,
}

/// What the accounts currently charge. Never above the limits.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Accounts {
    pub payload_bytes: PayloadBytes,
    pub references: References,
    pub exchange_slots: ExchangeSlots,
    pub receipt_slots: ReceiptSlots,
}

impl Accounts {
    /// What one receipt or one fence holds.
    pub const RECEIPT: Self = Self::receipt();

    const fn receipt() -> Self {
        Self {
            payload_bytes: PayloadBytes::ZERO,
            references: References::ZERO,
            exchange_slots: ExchangeSlots::ZERO,
            receipt_slots: ReceiptSlots::new(1),
        }
    }
}

/// Life cycle of a session incarnation.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SessionState {
    Active,
    /// The session was deleted. Its identity is never used again.
    Terminal,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SessionRecord {
    pub client_id: Vec<u8>,
    pub revision: Revision,
    pub state: SessionState,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct RouteTarget {
    pub session: SessionId,
    pub qos: Qos,
}

/// The durable decision about one operation.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Decision {
    Committed(CommitFacts),
    Rejected(RejectReason),
}

/// The receipt of an operation: its exact recipe and what was decided.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct OperationRecord {
    /// The canonical bytes of the recipe, compared byte for byte when the
    /// operation is presented again (DEM-009).
    pub recipe: Vec<u8>,
    pub decision: Decision,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PublicationRecord {
    pub operation: OperationId,
    pub source: SessionId,
    pub topic: Vec<u8>,
    pub qos: Qos,
    pub retain: bool,
    pub payload: PayloadId,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PayloadRecord {
    pub bytes: Vec<u8>,
    /// Live obligations that still refer to the payload. Never zero: a
    /// payload no obligation refers to is not kept, and its bytes are
    /// not charged.
    pub references: u64,
}

/// Phase of an obligation. S0 excludes emission towards recipients, so
/// an obligation stays pending until its destination is deleted.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ObligationPhase {
    Pending,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ObligationRecord {
    pub target: SessionId,
    pub target_revision: Revision,
    pub qos: Qos,
    pub cause: DeliveryCause,
    pub phase: ObligationPhase,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct InboundRecord {
    pub exchange: ExchangeId,
    pub publication: PublicationId,
    pub phase: InboundPhase,
}

/// Everything that survives a crash. It is also the snapshot of a store:
/// two stores in the same durable state compare equal, and every
/// collection is ordered by its key.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DurableState {
    pub ledger: EpochLedger,
    /// Closed epochs up to this one had their receipts collected; zero
    /// when nothing was collected.
    pub collected_through: RunEpoch,
    /// Advances at every commit. A recovery cursor names the revision it
    /// reads, and refuses to continue on another one.
    pub revision: u64,
    pub limits: Limits,
    pub accounts: Accounts,
    pub next_session: SessionId,
    pub route_revision: Revision,
    pub sessions: BTreeMap<SessionId, SessionRecord>,
    pub routes: BTreeMap<Vec<u8>, Vec<RouteTarget>>,
    /// Every receipt, admitted or rejected, holds one receipt slot.
    pub operations: BTreeMap<OperationId, OperationRecord>,
    /// Identities closed by `ResolveAndFence` before any command arrived.
    /// Each holds one receipt slot, like a receipt.
    pub fences: BTreeSet<OperationId>,
    pub publications: BTreeMap<PublicationId, PublicationRecord>,
    pub payloads: BTreeMap<PayloadId, PayloadRecord>,
    pub obligations: BTreeMap<DeliveryId, ObligationRecord>,
    pub inbound: BTreeMap<(SessionId, InboundPacketId), InboundRecord>,
}

/// Commands that prepare or administer a data set. A fixture is set up
/// through these, never by writing state.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum AdminCommand {
    /// Creates a persistent session for a ClientID no active session has.
    CreateSession { client_id: Vec<u8> },
    /// Adds one destination to the exact-topic route of `topic`.
    AddRoute {
        topic: Vec<u8>,
        session: SessionId,
        qos: Qos,
    },
    /// Makes a session terminal. Its inbound markers go with it, and the
    /// obligations *towards* it end with their accounts released. The
    /// obligations of the publications it produced, towards other
    /// sessions, are not touched.
    DeleteSession { session: SessionId },
    /// Collects the receipts of closed epochs up to `through`.
    CollectClosedEpochs { through: RunEpoch },
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AdminDone {
    SessionCreated {
        session: SessionId,
        revision: Revision,
    },
    RouteAdded {
        route_revision: Revision,
    },
    SessionDeleted,
    Collected,
}

/// Why an administration command changed nothing.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AdminRefused {
    DuplicateClientId,
    UnknownSession,
    DuplicateTarget,
    TableFull,
    TopicTooLong,
    /// Longer than `MAX_CLIENT_ID_BYTES`.
    ClientIdTooLong,
    /// A counter reached its end; nothing wraps.
    Exhausted,
    /// The epoch is open or was never opened: nothing to collect.
    EpochNotClosed,
    /// The durable medium refused the write.
    WriteFailed,
    /// The store found its own accounting broken. Nothing was committed.
    Inconsistent,
}

/// What applying a command did.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Applied {
    pub reply: StoreReply,
    /// Whether this application is the one that committed the effect of
    /// the operation. False for a repetition, a rejection or a violation.
    pub committed_now: bool,
    /// Whether this application made anything durable: the effect, or
    /// the receipt of a rejection. The point after a commit is passed
    /// exactly when this is true.
    pub wrote: bool,
}

/// What `ResolveAndFence` established.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Resolved {
    pub outcome: StoreOutcome,
    /// Whether this call is the one that made the fence durable. False
    /// when the outcome was already established, and when no fence
    /// could be written.
    pub fenced_now: bool,
}

/// A position in the recovery inventory of one state revision.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct RecoveryCursor {
    revision: u64,
    position: usize,
}

#[derive(PartialEq, Eq, Debug)]
pub struct RecoveryPage {
    pub facts: BoundedVec<RecoveredFact>,
    /// Where the next page starts; `None` when this page is the last.
    pub next: Option<RecoveryCursor>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RecoveryRefused {
    /// The state changed since the cursor was issued: the inventory is
    /// not coherent any more and is read again from the start.
    FrontierMoved,
    /// A page of zero facts makes no progress.
    EmptyPage,
    /// A page, or a fact on it, could not be built within its budget.
    /// Nothing is said about the facts after it: the inventory is not
    /// complete, and it is not presented as complete.
    AllocationFailed,
}

/// The writer of a store. Each method is one step in the single order
/// of authority; a method either commits entirely or changes nothing.
pub trait DurableStore {
    /// The incarnation of the data set and the last epoch opened in it:
    /// what start-up reads before it opens anything.
    fn ledger(&self) -> EpochLedger;

    /// Opens the epoch after the one the command expects and closes
    /// every older one, atomically, or changes nothing. The command
    /// that opened the last epoch, presented again, gets the same
    /// commit; see `EpochLedger::open` for every other case.
    fn open_epoch(&mut self, command: &OpenEpoch) -> OpeningOutcome;

    /// Applies a frozen command.
    fn apply(&mut self, command: &Command) -> Applied;

    /// Reports what is established about `operation`. When it is absent
    /// from a space that can still execute, a durable fence is
    /// established before a certain rejection is announced; when the
    /// fence cannot be established the outcome stays unknown.
    fn resolve_and_fence(&mut self, operation: OperationId) -> Resolved;

    /// Applies an administration command.
    fn admin(&mut self, command: AdminCommand) -> Result<AdminDone, AdminRefused>;

    /// Reads one bounded page of the recovery inventory, from durable
    /// state only.
    fn recover(
        &self,
        cursor: Option<RecoveryCursor>,
        limit: usize,
    ) -> Result<RecoveryPage, RecoveryRefused>;

    /// The durable state, for inspection.
    fn snapshot(&self) -> DurableState;
}

/// Whether the durable medium accepts writes. Controlled by the bench:
/// it is an availability, not a decision of the store (DEM-013).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Durability {
    Available,
    /// Every commit fails. Reads still work.
    Failing,
}

/// The deterministic store: its durable state, and whether the medium
/// currently accepts writes.
#[derive(Debug)]
pub struct MemoryStore {
    committed: DurableState,
    durability: Durability,
}

/// A guard failed while a command was being prepared.
type Guard<T> = Result<T, RejectReason>;

impl MemoryStore {
    /// Creates an empty data set. No epoch is open in it.
    #[must_use]
    pub fn create(incarnation: StoreIncarnation, limits: Limits) -> Self {
        Self {
            committed: DurableState {
                ledger: EpochLedger::fresh(incarnation),
                collected_through: RunEpoch::new(0),
                revision: 0,
                limits,
                accounts: Accounts::default(),
                next_session: SessionId::new(1),
                route_revision: Revision::new(0),
                sessions: BTreeMap::new(),
                routes: BTreeMap::new(),
                operations: BTreeMap::new(),
                fences: BTreeSet::new(),
                publications: BTreeMap::new(),
                payloads: BTreeMap::new(),
                obligations: BTreeMap::new(),
                inbound: BTreeMap::new(),
            },
            durability: Durability::Available,
        }
    }

    /// Reopens a data set from what survived: the durable state and
    /// nothing else.
    #[must_use]
    pub fn reopen(durable: DurableState) -> Self {
        Self {
            committed: durable,
            durability: Durability::Available,
        }
    }

    /// Sets whether the medium accepts writes.
    pub fn set_durability(&mut self, durability: Durability) {
        self.durability = durability;
    }

    /// Commits a staged copy: it becomes the committed state, one
    /// revision later, or nothing changes.
    fn commit(&mut self, mut staged: DurableState) -> bool {
        if self.durability == Durability::Failing {
            return false;
        }
        let Some(revision) = staged.revision.checked_add(1) else {
            return false;
        };
        staged.revision = revision;
        self.committed = staged;
        true
    }

    /// Commits a receipt that records a certain rejection, so that the
    /// operation stays rejected whatever changes later. The receipt holds
    /// one receipt slot. Returns the outcome and whether it was written.
    fn commit_rejection(
        &mut self,
        operation: OperationId,
        recipe: Vec<u8>,
        reason: RejectReason,
    ) -> (StoreOutcome, bool) {
        let mut staged = self.committed.clone();
        // No slot, no receipt: the rejection could not be recorded, and
        // a later presentation could still execute, so nothing certain
        // is announced. Durable state never grows past its accounts.
        if staged.charge(Accounts::receipt()).is_err() {
            return (StoreOutcome::PendingOrUnknown, false);
        }
        staged.operations.insert(
            operation,
            OperationRecord {
                recipe,
                decision: Decision::Rejected(reason),
            },
        );
        if self.commit(staged) {
            (StoreOutcome::RejectedCertain(reason), true)
        } else {
            (StoreOutcome::PendingOrUnknown, false)
        }
    }
}

/// What is already established about an operation, before any guard.
enum Established {
    Outcome(StoreOutcome),
    /// Nothing is recorded, in a space that may still execute.
    Absent(EpochPhase),
}

impl DurableState {
    fn established(&self, operation: OperationId) -> Established {
        if operation.incarnation() != self.ledger.incarnation() {
            // Another data set: it can never execute here.
            return Established::Outcome(StoreOutcome::RejectedCertain(
                RejectReason::PreconditionFailed,
            ));
        }
        if operation.epoch() <= self.collected_through && operation.epoch().get() != 0 {
            return Established::Outcome(StoreOutcome::RetiredOutcomeUnavailable);
        }
        if let Some(record) = self.operations.get(&operation) {
            return Established::Outcome(match record.decision {
                Decision::Committed(facts) => StoreOutcome::Committed(facts),
                Decision::Rejected(reason) => StoreOutcome::RejectedCertain(reason),
            });
        }
        if self.fences.contains(&operation) {
            return Established::Outcome(StoreOutcome::RejectedCertain(RejectReason::Fenced));
        }
        match self.ledger.phase_of(operation.epoch()) {
            // Receipts of a closed epoch that was not collected are all
            // here: absent means it never committed, and the closure
            // forbids it from now on.
            EpochPhase::Closed => {
                Established::Outcome(StoreOutcome::RejectedCertain(RejectReason::EpochClosed))
            }
            phase => Established::Absent(phase),
        }
    }

    fn active_session(&self, session: SessionId, revision: Revision) -> Guard<()> {
        match self.sessions.get(&session) {
            Some(record) if record.state == SessionState::Active && record.revision == revision => {
                Ok(())
            }
            _ => Err(RejectReason::PreconditionFailed),
        }
    }

    /// Charges one account, or names the resource that is short.
    fn charge(&mut self, costs: Accounts) -> Guard<()> {
        let payload_bytes = self
            .accounts
            .payload_bytes
            .checked_add(costs.payload_bytes)
            .filter(|total| *total <= self.limits.payload_bytes)
            .ok_or(RejectReason::CapacityExceeded(Resource::PayloadBytes))?;
        let references = self
            .accounts
            .references
            .checked_add(costs.references)
            .filter(|total| *total <= self.limits.references)
            .ok_or(RejectReason::CapacityExceeded(Resource::References))?;
        let exchange_slots = self
            .accounts
            .exchange_slots
            .checked_add(costs.exchange_slots)
            .filter(|total| *total <= self.limits.exchange_slots)
            .ok_or(RejectReason::CapacityExceeded(Resource::ExchangeSlots))?;
        let receipt_slots = self
            .accounts
            .receipt_slots
            .checked_add(costs.receipt_slots)
            .filter(|total| *total <= self.limits.receipt_slots)
            .ok_or(RejectReason::CapacityExceeded(Resource::ReceiptSlots))?;
        self.accounts = Accounts {
            payload_bytes,
            references,
            exchange_slots,
            receipt_slots,
        };
        Ok(())
    }

    /// Stages an admission. Every row is written in the staged copy as
    /// its guard passes; the caller drops the copy on the first failure.
    fn stage_admission(
        &mut self,
        admit: &AdmitPublication,
        payload: Option<&SharedBytes>,
    ) -> Guard<CommitFacts> {
        let failed = RejectReason::PreconditionFailed;
        let operation = admit.operation;
        let same_scope = |incarnation, epoch, owner| {
            incarnation == operation.incarnation()
                && epoch == operation.epoch()
                && owner == operation.owner()
        };
        if !same_scope(
            admit.publication.incarnation(),
            admit.publication.epoch(),
            admit.publication.owner(),
        ) || !same_scope(
            admit.payload.id.incarnation(),
            admit.payload.id.epoch(),
            admit.payload.id.owner(),
        ) || !same_scope(
            admit.request.incarnation(),
            admit.request.epoch(),
            admit.request.owner(),
        ) {
            return Err(failed);
        }
        let targets = admit.targets.as_slice();
        if targets.len() > self.limits.max_targets_per_admission
            || admit.topic.len() > self.limits.max_topic_bytes
            || admit.route_revision != self.route_revision
        {
            return Err(failed);
        }
        let bytes = payload.ok_or(failed)?;
        let length = u64::try_from(bytes.len()).map_err(|_| failed)?;
        if length != admit.payload.length.get() || bytes.len() > self.limits.max_payload_bytes {
            return Err(failed);
        }
        // The costs are part of the frozen recipe. They must be the
        // costs of what the recipe admits: the store releases exactly
        // what it charged, row by row.
        let exactly_once = admit.qos == Qos::ExactlyOnce;
        let reference_count = u64::try_from(targets.len()).map_err(|_| failed)?;
        if admit.costs.payload_bytes != admit.payload.length
            || admit.costs.references.get() != reference_count
            || admit.costs.exchange_slots.get() != u64::from(exactly_once)
            || admit.costs.receipt_slots.get() != 1
        {
            return Err(failed);
        }
        self.active_session(admit.source.session, admit.source.session_revision)?;

        // A payload identity names one payload, kept or not.
        if self.publications.contains_key(&admit.publication)
            || self.payloads.contains_key(&admit.payload.id)
            || self
                .publications
                .values()
                .any(|publication| publication.payload == admit.payload.id)
        {
            return Err(failed);
        }
        self.publications.insert(
            admit.publication,
            PublicationRecord {
                operation,
                source: admit.source.session,
                topic: admit.topic.as_slice().to_vec(),
                qos: admit.qos,
                retain: admit.retain,
                payload: admit.payload.id,
            },
        );
        // Its bytes are kept while an obligation refers to them, and only
        // then charged. Without a target nothing refers to them: they are
        // charged and released in this same transaction, so nothing is
        // left that no obligation would ever release.
        if reference_count > 0 {
            self.payloads.insert(
                admit.payload.id,
                PayloadRecord {
                    bytes: bytes.as_slice().to_vec(),
                    references: reference_count,
                },
            );
        }
        for target in targets {
            self.active_session(target.session, target.session_revision)?;
            let obligation = ObligationRecord {
                target: target.session,
                target_revision: target.session_revision,
                qos: target.effective_qos,
                cause: target.cause,
                phase: ObligationPhase::Pending,
            };
            if self
                .obligations
                .insert(target.delivery, obligation)
                .is_some()
            {
                return Err(failed);
            }
        }
        if exactly_once {
            let slot = (admit.source.session, admit.source.packet_id);
            let marker = InboundRecord {
                exchange: admit.source.exchange,
                publication: admit.publication,
                phase: InboundPhase::AwaitPubrel,
            };
            // One admission per inbound exchange: an occupied slot is
            // never taken over, whatever generation the newcomer claims.
            if self.inbound.insert(slot, marker).is_some() {
                return Err(failed);
            }
        }
        self.charge(Accounts {
            payload_bytes: admit.costs.payload_bytes,
            references: admit.costs.references,
            exchange_slots: admit.costs.exchange_slots,
            receipt_slots: admit.costs.receipt_slots,
        })?;
        if reference_count == 0 {
            self.accounts.payload_bytes = self
                .accounts
                .payload_bytes
                .checked_sub(admit.costs.payload_bytes)
                .ok_or(failed)?;
        }
        Ok(CommitFacts::PublicationAdmitted {
            publication: admit.publication,
            exchange: admit.source.exchange,
        })
    }

    fn stage_release(&mut self, release: &ReleaseInbound) -> Guard<CommitFacts> {
        let failed = RejectReason::PreconditionFailed;
        let slot = (release.source_session, release.packet_id);
        match self.inbound.get(&slot) {
            Some(marker)
                if marker.exchange == release.exchange
                    && marker.phase == InboundPhase::AwaitPubrel => {}
            _ => return Err(failed),
        }
        self.inbound.remove(&slot);
        self.accounts.exchange_slots = self
            .accounts
            .exchange_slots
            .checked_sub(ExchangeSlots::new(1))
            .ok_or(failed)?;
        Ok(CommitFacts::InboundReleased {
            exchange: release.exchange,
        })
    }

    /// Ends one obligation and releases exactly what it held.
    fn end_obligation(&mut self, delivery: DeliveryId) -> Result<(), AdminRefused> {
        let broken = AdminRefused::Inconsistent;
        self.obligations.remove(&delivery).ok_or(broken)?;
        self.accounts.references = self
            .accounts
            .references
            .checked_sub(References::new(1))
            .ok_or(broken)?;
        let payload_id = self
            .publications
            .get(&delivery.publication)
            .ok_or(broken)?
            .payload;
        let payload = self.payloads.get_mut(&payload_id).ok_or(broken)?;
        payload.references = payload.references.checked_sub(1).ok_or(broken)?;
        if payload.references == 0 {
            let length = u64::try_from(payload.bytes.len()).map_err(|_| broken)?;
            self.payloads.remove(&payload_id);
            self.accounts.payload_bytes = self
                .accounts
                .payload_bytes
                .checked_sub(PayloadBytes::new(length))
                .ok_or(broken)?;
        }
        Ok(())
    }

    fn stage_admin(&mut self, command: AdminCommand) -> Result<AdminDone, AdminRefused> {
        match command {
            AdminCommand::CreateSession { client_id } => {
                if client_id.len() > MAX_CLIENT_ID_BYTES {
                    return Err(AdminRefused::ClientIdTooLong);
                }
                let active = || {
                    self.sessions
                        .values()
                        .filter(|record| record.state == SessionState::Active)
                };
                if active().any(|record| record.client_id == client_id) {
                    return Err(AdminRefused::DuplicateClientId);
                }
                if active().count() >= self.limits.max_sessions {
                    return Err(AdminRefused::TableFull);
                }
                let session = self.next_session;
                self.next_session = session
                    .checked_next()
                    .map_err(|_| AdminRefused::Exhausted)?;
                let revision = Revision::new(1);
                self.sessions.insert(
                    session,
                    SessionRecord {
                        client_id,
                        revision,
                        state: SessionState::Active,
                    },
                );
                Ok(AdminDone::SessionCreated { session, revision })
            }
            AdminCommand::AddRoute {
                topic,
                session,
                qos,
            } => {
                if topic.len() > self.limits.max_topic_bytes {
                    return Err(AdminRefused::TopicTooLong);
                }
                match self.sessions.get(&session) {
                    Some(record) if record.state == SessionState::Active => {}
                    _ => return Err(AdminRefused::UnknownSession),
                }
                let targets = self.routes.entry(topic).or_default();
                if targets.iter().any(|target| target.session == session) {
                    return Err(AdminRefused::DuplicateTarget);
                }
                if targets.len() >= self.limits.max_route_targets {
                    return Err(AdminRefused::TableFull);
                }
                targets.push(RouteTarget { session, qos });
                self.route_revision = self
                    .route_revision
                    .checked_next()
                    .map_err(|_| AdminRefused::Exhausted)?;
                Ok(AdminDone::RouteAdded {
                    route_revision: self.route_revision,
                })
            }
            AdminCommand::DeleteSession { session } => {
                let record = match self.sessions.get_mut(&session) {
                    Some(record) if record.state == SessionState::Active => record,
                    _ => return Err(AdminRefused::UnknownSession),
                };
                record.revision = record
                    .revision
                    .checked_next()
                    .map_err(|_| AdminRefused::Exhausted)?;
                record.state = SessionState::Terminal;
                // The session's own inbound markers, with their slots.
                let markers: Vec<_> = self
                    .inbound
                    .keys()
                    .filter(|(owner, _)| *owner == session)
                    .copied()
                    .collect();
                for slot in markers {
                    self.inbound.remove(&slot);
                    self.accounts.exchange_slots = self
                        .accounts
                        .exchange_slots
                        .checked_sub(ExchangeSlots::new(1))
                        .ok_or(AdminRefused::Inconsistent)?;
                }
                // Obligations end by their destination, never by the
                // session that produced the publication.
                let ended: Vec<_> = self
                    .obligations
                    .iter()
                    .filter(|(_, obligation)| obligation.target == session)
                    .map(|(delivery, _)| *delivery)
                    .collect();
                for delivery in ended {
                    self.end_obligation(delivery)?;
                }
                let mut routes_changed = false;
                for targets in self.routes.values_mut() {
                    let before = targets.len();
                    targets.retain(|target| target.session != session);
                    routes_changed |= targets.len() != before;
                }
                self.routes.retain(|_, targets| !targets.is_empty());
                if routes_changed {
                    self.route_revision = self
                        .route_revision
                        .checked_next()
                        .map_err(|_| AdminRefused::Exhausted)?;
                }
                Ok(AdminDone::SessionDeleted)
            }
            AdminCommand::CollectClosedEpochs { through } => {
                if self.ledger.phase_of(through) != EpochPhase::Closed {
                    return Err(AdminRefused::EpochNotClosed);
                }
                if through > self.collected_through {
                    self.collected_through = through;
                }
                let collected = self.collected_through;
                // Epoch 0 is never opened, so never closed: its receipts
                // and fences are never retired, and they stay.
                let retired = |operation: &OperationId| {
                    operation.epoch() <= collected && operation.epoch().get() != 0
                };
                // Every receipt and every fence holds one receipt slot.
                let mut freed = 0u64;
                let held = self.operations.keys().chain(self.fences.iter());
                for operation in held {
                    if retired(operation) {
                        freed = freed.checked_add(1).ok_or(AdminRefused::Exhausted)?;
                    }
                }
                self.accounts.receipt_slots = self
                    .accounts
                    .receipt_slots
                    .checked_sub(ReceiptSlots::new(freed))
                    .ok_or(AdminRefused::Inconsistent)?;
                self.operations.retain(|operation, _| !retired(operation));
                self.fences.retain(|operation| !retired(operation));
                Ok(AdminDone::Collected)
            }
        }
    }

    /// The recovery inventory, in a fixed order: the epoch, the active
    /// sessions, the inbound markers, then what is established about the
    /// operations of older epochs. A fact that cannot be built within its
    /// budget is an error in its place, never the end of the inventory.
    fn inventory(&self) -> impl Iterator<Item = Result<RecoveredFact, RecoveryRefused>> + '_ {
        let incarnation = self.ledger.incarnation();
        let open = self.ledger.open_epoch();
        let older = move |operation: &OperationId| Some(operation.epoch()) != open;
        let epoch = open.map(|epoch| Ok(RecoveredFact::Epoch { incarnation, epoch }));
        let sessions = self
            .sessions
            .iter()
            .filter(|(_, record)| record.state == SessionState::Active)
            .map(|(session, record)| {
                let client_id = SharedBytes::copy_from(&record.client_id, MAX_CLIENT_ID_BYTES)
                    .map_err(|_| RecoveryRefused::AllocationFailed)?;
                Ok(RecoveredFact::Session {
                    session: *session,
                    client_id,
                    revision: record.revision,
                })
            });
        let inbound = self.inbound.iter().map(|((_, packet_id), marker)| {
            Ok(RecoveredFact::InboundExchange {
                exchange: marker.exchange,
                packet_id: *packet_id,
                publication: marker.publication,
                phase: marker.phase,
            })
        });
        let receipts = self
            .operations
            .iter()
            .filter(move |(operation, _)| older(operation))
            .map(|(operation, record)| {
                Ok(RecoveredFact::Operation {
                    operation: *operation,
                    knowledge: match record.decision {
                        Decision::Committed(facts) => Knowledge::Committed(facts),
                        Decision::Rejected(reason) => Knowledge::RejectedCertain(reason),
                    },
                })
            });
        let fences = self
            .fences
            .iter()
            .filter(move |operation| older(operation))
            .map(|operation| {
                Ok(RecoveredFact::Operation {
                    operation: *operation,
                    knowledge: Knowledge::RejectedCertain(RejectReason::Fenced),
                })
            });
        epoch
            .into_iter()
            .chain(sessions)
            .chain(inbound)
            .chain(receipts)
            .chain(fences)
    }
}

impl DurableStore for MemoryStore {
    fn ledger(&self) -> EpochLedger {
        self.committed.ledger
    }

    fn open_epoch(&mut self, command: &OpenEpoch) -> OpeningOutcome {
        // The decision is the contract's: the same function decides for
        // every store. Only an opening writes; the command that opened
        // the last epoch, presented again, reads its own result.
        if let LedgerOpening::Opens { ledger } = self.committed.ledger.open(command) {
            let mut staged = self.committed.clone();
            staged.ledger = ledger;
            // A write that fails leaves the ledger as it was, and the
            // answer is read from it: no commit is reported.
            let _written = self.commit(staged);
        }
        // The answer comes from what is durable now, never from the
        // decision to open.
        OpeningOutcome::read(&self.committed.ledger, command)
    }

    fn apply(&mut self, command: &Command) -> Applied {
        let settled = |outcome| Applied {
            reply: StoreReply::Outcome(outcome),
            committed_now: false,
            wrote: false,
        };
        let rejected = |(outcome, wrote)| Applied {
            reply: StoreReply::Outcome(outcome),
            committed_now: false,
            wrote,
        };
        let recipe = command.recipe.get();
        let operation = recipe.operation();
        let mut canonical = Vec::new();
        if recipe.encode_canonical(&mut canonical).is_err() {
            // A frozen recipe is well formed; one that is not never ran.
            return settled(StoreOutcome::RejectedCertain(
                RejectReason::PreconditionFailed,
            ));
        }

        // 1. Recognise an operation that was already decided.
        if let Some(record) = self.committed.operations.get(&operation) {
            let same_payload = match (recipe, &command.payload, record.decision) {
                (Recipe::AdmitPublication(admit), Some(bytes), Decision::Committed(_)) => self
                    .committed
                    .payloads
                    .get(&admit.payload.id)
                    .is_none_or(|stored| stored.bytes == bytes.as_slice()),
                _ => true,
            };
            if record.recipe != canonical || !same_payload {
                return Applied {
                    reply: StoreReply::Violation(StoreViolation::RecipeMismatch),
                    committed_now: false,
                    wrote: false,
                };
            }
        }
        let phase = match self.committed.established(operation) {
            Established::Outcome(outcome) => return settled(outcome),
            Established::Absent(phase) => phase,
        };
        if phase != EpochPhase::Open {
            // Its epoch is not open yet. It must not become executable
            // when that epoch opens: the rejection is recorded.
            return rejected(self.commit_rejection(
                operation,
                canonical,
                RejectReason::PreconditionFailed,
            ));
        }

        // 2 to 5. Guards, capacities and every row, on a staged copy.
        let mut staged = self.committed.clone();
        let facts = match recipe {
            Recipe::AdmitPublication(admit) => {
                staged.stage_admission(admit, command.payload.as_ref())
            }
            Recipe::ReleaseInbound(release) => staged.stage_release(release),
        };
        match facts {
            Ok(facts) => {
                staged.operations.insert(
                    operation,
                    OperationRecord {
                        recipe: canonical,
                        decision: Decision::Committed(facts),
                    },
                );
                if self.commit(staged) {
                    Applied {
                        reply: StoreReply::Outcome(StoreOutcome::Committed(facts)),
                        committed_now: true,
                        wrote: true,
                    }
                } else {
                    settled(StoreOutcome::PendingOrUnknown)
                }
            }
            // The staged copy, with whatever rows it already held, is
            // dropped here.
            Err(reason) => rejected(self.commit_rejection(operation, canonical, reason)),
        }
    }

    fn resolve_and_fence(&mut self, operation: OperationId) -> Resolved {
        let settled = |outcome| Resolved {
            outcome,
            fenced_now: false,
        };
        match self.committed.established(operation) {
            Established::Outcome(outcome) => settled(outcome),
            Established::Absent(_) => {
                let mut staged = self.committed.clone();
                // A fence holds a receipt slot. Without one no fence is
                // written, and absence proves nothing: unknown.
                if staged.charge(Accounts::receipt()).is_err() {
                    return settled(StoreOutcome::PendingOrUnknown);
                }
                staged.fences.insert(operation);
                if self.commit(staged) {
                    Resolved {
                        outcome: StoreOutcome::RejectedCertain(RejectReason::Fenced),
                        fenced_now: true,
                    }
                } else {
                    settled(StoreOutcome::PendingOrUnknown)
                }
            }
        }
    }

    fn admin(&mut self, command: AdminCommand) -> Result<AdminDone, AdminRefused> {
        let mut staged = self.committed.clone();
        let done = staged.stage_admin(command)?;
        if self.commit(staged) {
            Ok(done)
        } else {
            Err(AdminRefused::WriteFailed)
        }
    }

    fn recover(
        &self,
        cursor: Option<RecoveryCursor>,
        limit: usize,
    ) -> Result<RecoveryPage, RecoveryRefused> {
        if limit == 0 {
            return Err(RecoveryRefused::EmptyPage);
        }
        let revision = self.committed.revision;
        let start = match cursor {
            Some(cursor) if cursor.revision != revision => {
                return Err(RecoveryRefused::FrontierMoved);
            }
            Some(cursor) => cursor.position,
            None => 0,
        };
        let mut facts =
            BoundedVec::with_capacity(limit).map_err(|_| RecoveryRefused::AllocationFailed)?;
        // One pass over the inventory per page, from where the cursor
        // stands; the fact after the page tells whether there is a next.
        let mut inventory = self.committed.inventory().skip(start);
        let mut position = start;
        while !facts.is_full() {
            let Some(fact) = inventory.next() else {
                return Ok(RecoveryPage { facts, next: None });
            };
            if facts.try_push(fact?).is_err() {
                return Err(RecoveryRefused::AllocationFailed);
            }
            position = position
                .checked_add(1)
                .ok_or(RecoveryRefused::AllocationFailed)?;
        }
        let next = inventory
            .next()
            .is_some()
            .then_some(RecoveryCursor { revision, position });
        Ok(RecoveryPage { facts, next })
    }

    fn snapshot(&self) -> DurableState {
        self.committed.clone()
    }
}
