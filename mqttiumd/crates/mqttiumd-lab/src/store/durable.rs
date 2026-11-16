//! The deterministic store, and the interface of every store.
//!
//! `DurableStore` and the types it speaks in live in `mqttiumd-store-api`
//! and are re-exported here, so that every path `store::durable::…`
//! keeps naming them. `MemoryStore` is the deterministic realisation of
//! the trait; the SQLite store of lot L7 realises it in
//! `mqttiumd-store-sqlite`, and both pass the same suite (`conformance`).
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
    AdmitPublication, ChangeSubscriptions, ClaimDelivery, Command, ConsumeDelivery, EpochLedger,
    EpochPhase, FinishDelivery, LedgerOpening, OpenEpoch, Recipe, RecipeError, ReleaseInbound,
    UpdateSubscriptions,
};
use mqttiumd_contract::effects::{InboundPhase, RecoveredFact};
use mqttiumd_contract::ids::{
    DeliveryId, OperationId, OperationSeq, PublicationId, Revision, RunEpoch, SessionId,
    StoreIncarnation,
};
use mqttiumd_contract::outcome::{
    CommitFacts, Knowledge, OpeningOutcome, RejectReason, StoreOutcome, StoreReply, StoreViolation,
};
use mqttiumd_contract::packet::{Qos, SharedBytes};
use mqttiumd_contract::pubsub::{DeliveryClaim, MAX_SUBSCRIPTIONS, WORK_PAGE_LIMIT, WorkItem};

pub use mqttiumd_store_api::*;

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
                receipt_frontiers: BTreeMap::new(),
                publications: BTreeMap::new(),
                payloads: BTreeMap::new(),
                obligations: BTreeMap::new(),
                inbound: BTreeMap::new(),
                outbound: BTreeMap::new(),
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

    /// Completed metadata is collectible, but the exact admission receipt
    /// remains an identity witness until its epoch is closed and collected.
    /// Check the whole relevant scope before treating an identity as unused.
    fn admission_identity_was_used(&self, admit: &AdmitPublication) -> Result<bool, ReadError> {
        let operation = admit.operation;
        let bound = |sequence| {
            OperationId::new(
                operation.incarnation(),
                operation.epoch(),
                operation.owner(),
                OperationSeq::new(sequence),
            )
        };
        let mut meter = ReadMeter::new(ReadBudget::SNAPSHOT);
        for (id, record) in self.committed.operations.range(bound(0)..=bound(u64::MAX)) {
            meter.field(record.recipe.len(), ReadBudget::SNAPSHOT.max_field_bytes)?;
            meter.charge(
                256usize
                    .checked_add(record.recipe.len())
                    .ok_or(ReadError::LimitExceeded)?,
            )?;
            if let Decision::Committed(CommitFacts::PublicationAdmitted {
                publication,
                exchange,
            }) = record.decision
            {
                let stored = Recipe::decode_canonical(
                    &record.recipe,
                    self.committed.limits.max_targets_per_admission,
                    self.committed.limits.max_topic_bytes,
                )
                .map_err(|error| match error {
                    RecipeError::AllocationFailed => ReadError::AllocationFailed,
                    RecipeError::TooManyTargets { .. } | RecipeError::TopicTooLong(_) => {
                        ReadError::LimitExceeded
                    }
                    _ => ReadError::InvalidData,
                })?;
                let Recipe::AdmitPublication(stored) = stored else {
                    return Err(ReadError::InvalidData);
                };
                if stored.operation != *id
                    || stored.publication != publication
                    || stored.source.exchange != exchange
                {
                    return Err(ReadError::InvalidData);
                }
                if stored.publication == admit.publication || stored.payload.id == admit.payload.id
                {
                    return Ok(true);
                }
            }
        }
        Ok(false)
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
        if staged.charge(Accounts::RECEIPT).is_err() {
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

/// A raw inventory reference never allocates its dynamic fields.
enum FactRef<'a> {
    Epoch(StoreIncarnation, RunEpoch),
    Limits,
    Table,
    Route(&'a [u8], &'a RouteTarget),
    Session(SessionId, &'a SessionRecord),
    Inbound(mqttiumd_contract::ids::InboundPacketId, &'a InboundRecord),
    Operation(OperationId, &'a OperationRecord),
    Fence(OperationId),
    Outbound(DeliveryId, &'a ObligationRecord),
}

struct ReadMeter {
    budget: ReadBudget,
    rows: usize,
    bytes: usize,
}
impl ReadMeter {
    fn new(budget: ReadBudget) -> Self {
        Self {
            budget,
            rows: 0,
            bytes: 0,
        }
    }
    fn charge(&mut self, bytes: usize) -> Result<(), ReadError> {
        self.rows = self.rows.checked_add(1).ok_or(ReadError::LimitExceeded)?;
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or(ReadError::LimitExceeded)?;
        self.budget.check_totals(self.rows, self.bytes)
    }
    fn field(&self, bytes: usize, maximum: usize) -> Result<(), ReadError> {
        if bytes > maximum {
            return Err(ReadError::LimitExceeded);
        }
        self.budget.check_field(bytes)
    }
}

impl FactRef<'_> {
    fn build(
        self,
        state: &DurableState,
        meter: &mut ReadMeter,
    ) -> Result<RecoveredFact, ReadError> {
        let dynamic = match self {
            Self::Route(topic, _) => topic.len(),
            Self::Session(_, record) => record.client_id.len(),
            _ => 0,
        };
        meter.charge(
            128usize
                .checked_add(dynamic)
                .ok_or(ReadError::LimitExceeded)?,
        )?;
        Ok(match self {
            Self::Epoch(incarnation, epoch) => RecoveredFact::Epoch { incarnation, epoch },
            Self::Limits => RecoveredFact::Limits {
                max_targets_per_admission: u32::try_from(state.limits.max_targets_per_admission)
                    .map_err(|_| ReadError::LimitExceeded)?,
                max_topic_bytes: u32::try_from(state.limits.max_topic_bytes)
                    .map_err(|_| ReadError::LimitExceeded)?,
                max_payload_bytes: PayloadBytes::new(
                    u64::try_from(state.limits.max_payload_bytes)
                        .map_err(|_| ReadError::LimitExceeded)?,
                ),
            },
            Self::Table => RecoveredFact::RouteTable {
                revision: state.route_revision,
            },
            Self::Route(topic, target) => {
                meter.field(topic.len(), state.limits.max_topic_bytes)?;
                let session = state
                    .sessions
                    .get(&target.session)
                    .filter(|record| record.state == SessionState::Active)
                    .ok_or(ReadError::InvalidData)?;
                RecoveredFact::Route {
                    topic: SharedBytes::copy_from(topic, state.limits.max_topic_bytes)
                        .map_err(|_| ReadError::LimitExceeded)?,
                    target: target.session,
                    target_revision: session.revision,
                    max_qos: target.qos,
                }
            }
            Self::Session(session, record) => {
                meter.field(record.client_id.len(), MAX_CLIENT_ID_BYTES)?;
                RecoveredFact::Session {
                    session,
                    client_id: SharedBytes::copy_from(&record.client_id, MAX_CLIENT_ID_BYTES)
                        .map_err(|_| ReadError::LimitExceeded)?,
                    revision: record.revision,
                }
            }
            Self::Inbound(packet_id, marker) => {
                meter.field(32, 32)?;
                RecoveredFact::InboundExchange {
                    exchange: marker.exchange,
                    packet_id,
                    publication: marker.publication,
                    phase: marker.phase,
                }
            }
            Self::Operation(operation, record) => {
                meter.field(32, 32)?;
                RecoveredFact::Operation {
                    operation,
                    knowledge: match record.decision {
                        Decision::Committed(facts) => Knowledge::Committed(facts),
                        Decision::Rejected(reason) => Knowledge::RejectedCertain(reason),
                    },
                }
            }
            Self::Outbound(delivery, record) => {
                meter.field(32, 32)?;
                let claim = delivery_claim(state, delivery, record)?;
                RecoveredFact::OutboundDelivery {
                    delivery,
                    target: record.target,
                    target_revision: record.target_revision,
                    qos: record.qos,
                    claim,
                }
            }
            Self::Fence(operation) => {
                meter.field(32, 32)?;
                RecoveredFact::Operation {
                    operation,
                    knowledge: Knowledge::RejectedCertain(RejectReason::Fenced),
                }
            }
        })
    }
}

/// Validate the relationships before exposing an outgoing intent as recoverable.
fn delivery_claim(
    state: &DurableState,
    delivery: DeliveryId,
    record: &ObligationRecord,
) -> Result<Option<DeliveryClaim>, ReadError> {
    if !state.sessions.get(&record.target).is_some_and(|session| {
        session.state == SessionState::Active && session.revision == record.target_revision
    }) {
        return Err(ReadError::InvalidData);
    }
    let Some(claim) = state.outbound.get(&delivery) else {
        return Ok(None);
    };
    if claim.target != record.target
        || claim.target_revision != record.target_revision
        || record.qos != Qos::AtLeastOnce
        || claim.claim.incarnation() != state.ledger.incarnation()
    {
        return Err(ReadError::InvalidData);
    }
    Ok(Some(DeliveryClaim {
        operation: claim.claim,
        packet_id: claim.packet_id,
    }))
}

fn check_inventory_bound(state: &DurableState) -> Result<(), ReadError> {
    let mut rows = 3usize;
    for size in [
        state.routes.len(),
        state.sessions.len(),
        state.inbound.len(),
        state.obligations.len(),
        state.outbound.len(),
        state.operations.len(),
        state.fences.len(),
    ] {
        if size > MAX_RECOVERY_INVENTORY_ROWS {
            return Err(ReadError::LimitExceeded);
        }
    }
    for size in [
        state.sessions.len(),
        state.inbound.len(),
        state.obligations.len(),
        state.operations.len(),
        state.fences.len(),
    ] {
        rows = rows.checked_add(size).ok_or(ReadError::LimitExceeded)?;
    }
    for targets in state.routes.values() {
        rows = rows
            .checked_add(targets.len())
            .ok_or(ReadError::LimitExceeded)?;
        if rows > MAX_RECOVERY_INVENTORY_ROWS {
            return Err(ReadError::LimitExceeded);
        }
    }
    if rows > MAX_RECOVERY_INVENTORY_ROWS {
        return Err(ReadError::LimitExceeded);
    }
    for delivery in state.outbound.keys() {
        let record = state
            .obligations
            .get(delivery)
            .ok_or(ReadError::InvalidData)?;
        delivery_claim(state, *delivery, record)?;
    }
    Ok(())
}

fn check_snapshot(state: &DurableState, budget: ReadBudget) -> Result<(), ReadError> {
    let mut meter = ReadMeter::new(budget);
    budget.check_field(8)?;
    if state.ledger.opened_by().is_some() {
        budget.check_field(24)?;
    }
    meter.charge(256)?;
    for record in state.sessions.values() {
        meter.field(record.client_id.len(), MAX_CLIENT_ID_BYTES)?;
        meter.charge(
            152usize
                .checked_add(record.client_id.len())
                .ok_or(ReadError::LimitExceeded)?,
        )?;
    }
    for (topic, targets) in &state.routes {
        meter.field(topic.len(), state.limits.max_topic_bytes)?;
        for _ in targets {
            meter.charge(
                144usize
                    .checked_add(topic.len())
                    .ok_or(ReadError::LimitExceeded)?,
            )?;
        }
    }
    for record in state.operations.values() {
        meter.field(32, 32)?;
        meter.field(record.recipe.len(), budget.max_field_bytes)?;
        let decision_bytes = match record.decision {
            Decision::Committed(CommitFacts::PublicationAdmitted { .. }) => 56,
            Decision::Committed(CommitFacts::InboundReleased { .. }) => 24,
            Decision::Committed(_) => 96,
            Decision::Rejected(RejectReason::CapacityExceeded(_)) => 24,
            Decision::Rejected(_) => 16,
        };
        meter.charge(
            160usize
                .checked_add(decision_bytes)
                .and_then(|n| n.checked_add(record.recipe.len()))
                .ok_or(ReadError::LimitExceeded)?,
        )?;
    }
    for _ in &state.fences {
        meter.field(32, 32)?;
        meter.charge(160)?;
    }
    for record in state.publications.values() {
        meter.field(32, 32)?;
        meter.field(record.topic.len(), state.limits.max_topic_bytes)?;
        meter.charge(
            248usize
                .checked_add(record.topic.len())
                .ok_or(ReadError::LimitExceeded)?,
        )?;
    }
    for record in state.payloads.values() {
        meter.field(32, 32)?;
        meter.field(record.bytes.len(), state.limits.max_payload_bytes)?;
        meter.charge(
            168usize
                .checked_add(record.bytes.len())
                .ok_or(ReadError::LimitExceeded)?,
        )?;
    }
    for _ in state.obligations.values() {
        meter.field(32, 32)?;
        meter.charge(208)?;
    }
    for delivery in state.outbound.keys() {
        meter.field(32, 32)?;
        meter.charge(232)?;
        let record = state
            .obligations
            .get(delivery)
            .ok_or(ReadError::InvalidData)?;
        delivery_claim(state, *delivery, record)?;
    }
    for _ in state.inbound.values() {
        meter.field(32, 32)?;
        meter.charge(200)?;
    }
    Ok(())
}

/// The steps of the deterministic store on its staged copy. A trait of
/// this module because `DurableState` is a type of `mqttiumd-store-api`.
trait Staging {
    fn established(&self, operation: OperationId) -> Established;
    fn active_session(&self, session: SessionId, revision: Revision) -> Guard<()>;
    fn charge(&mut self, costs: Accounts) -> Guard<()>;
    fn stage_admission(
        &mut self,
        admit: &AdmitPublication,
        payload: Option<&SharedBytes>,
    ) -> Guard<CommitFacts>;
    fn stage_release(&mut self, release: &ReleaseInbound) -> Guard<CommitFacts>;
    fn stage_subscriptions(&mut self, command: &ChangeSubscriptions) -> Guard<CommitFacts>;
    fn stage_updates(&mut self, command: &UpdateSubscriptions) -> Guard<CommitFacts>;
    fn stage_route_updates<'a>(
        &mut self,
        session: SessionId,
        revision: Revision,
        route_revision: Revision,
        changes: impl Iterator<Item = (&'a SharedBytes, Option<Qos>)>,
    ) -> Guard<CommitFacts>;
    fn stage_claim(&mut self, command: &ClaimDelivery) -> Guard<CommitFacts>;
    fn stage_finish(&mut self, command: &FinishDelivery) -> Guard<CommitFacts>;
    fn stage_consume(&mut self, command: &ConsumeDelivery) -> Guard<CommitFacts>;
    fn discard_completed_publication(&mut self, publication: PublicationId);
    fn end_obligation(&mut self, delivery: DeliveryId) -> Result<(), AdminRefused>;
    fn stage_admin(&mut self, command: AdminCommand) -> Result<AdminDone, AdminRefused>;
    fn inventory(&self) -> impl Iterator<Item = FactRef<'_>>;
}

impl Staging for DurableState {
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
        self.discard_completed_publication(admit.publication);
        Ok(CommitFacts::PublicationAdmitted {
            publication: admit.publication,
            exchange: admit.source.exchange,
        })
    }

    fn stage_subscriptions(&mut self, command: &ChangeSubscriptions) -> Guard<CommitFacts> {
        self.stage_route_updates(
            command.session,
            command.session_revision,
            command.route_revision,
            command
                .changes
                .as_slice()
                .iter()
                .map(|c| (&c.topic, c.subscribe.then_some(Qos::AtLeastOnce))),
        )
    }
    fn stage_updates(&mut self, command: &UpdateSubscriptions) -> Guard<CommitFacts> {
        self.stage_route_updates(
            command.session,
            command.session_revision,
            command.route_revision,
            command
                .changes
                .as_slice()
                .iter()
                .map(|c| (&c.topic, c.granted)),
        )
    }
    fn stage_route_updates<'a>(
        &mut self,
        session: SessionId,
        revision: Revision,
        route_revision: Revision,
        changes: impl Iterator<Item = (&'a SharedBytes, Option<Qos>)>,
    ) -> Guard<CommitFacts> {
        let failed = RejectReason::PreconditionFailed;
        self.active_session(session, revision)?;
        if route_revision != self.route_revision {
            return Err(failed);
        }
        for (topic, granted) in changes {
            if topic.len() > self.limits.max_topic_bytes {
                return Err(failed);
            }
            if let Some(qos) = granted {
                if !matches!(qos, Qos::AtMostOnce | Qos::AtLeastOnce) {
                    return Err(failed);
                }
                let targets = self.routes.entry(topic.as_slice().to_vec()).or_default();
                if let Some(target) = targets.iter_mut().find(|target| target.session == session) {
                    target.qos = qos;
                } else {
                    if targets.len() >= self.limits.max_route_targets
                        || targets.len() >= self.limits.max_targets_per_admission
                    {
                        return Err(RejectReason::CapacityExceeded(Resource::References));
                    }
                    targets.push(RouteTarget { session, qos });
                }
            } else if let Some(targets) = self.routes.get_mut(topic.as_slice()) {
                targets.retain(|target| target.session != session);
                if targets.is_empty() {
                    self.routes.remove(topic.as_slice());
                }
            }
        }
        let total = self
            .routes
            .values()
            .try_fold(0usize, |n, t| n.checked_add(t.len()))
            .ok_or(failed)?;
        if total > MAX_SUBSCRIPTIONS {
            return Err(RejectReason::CapacityExceeded(Resource::References));
        }
        self.route_revision = self.route_revision.checked_next().map_err(|_| failed)?;
        self.charge(Accounts::RECEIPT)?;
        Ok(CommitFacts::SubscriptionsChanged {
            session,
            route_revision: self.route_revision,
        })
    }

    fn stage_claim(&mut self, command: &ClaimDelivery) -> Guard<CommitFacts> {
        let failed = RejectReason::PreconditionFailed;
        self.active_session(command.target, command.target_revision)?;
        let record = self.obligations.get(&command.delivery).ok_or(failed)?;
        if record.target != command.target
            || record.target_revision != command.target_revision
            || record.qos != Qos::AtLeastOnce
            || self.outbound.contains_key(&command.delivery)
            || self
                .outbound
                .values()
                .any(|slot| slot.target == command.target && slot.packet_id == command.packet_id)
        {
            return Err(failed);
        }
        self.charge(Accounts {
            exchange_slots: ExchangeSlots::new(1),
            receipt_slots: ReceiptSlots::new(1),
            ..Accounts::default()
        })?;
        self.outbound.insert(
            command.delivery,
            OutboundRecord {
                target: command.target,
                target_revision: command.target_revision,
                packet_id: command.packet_id,
                claim: command.operation,
            },
        );
        Ok(CommitFacts::DeliveryClaimed {
            delivery: command.delivery,
            target: command.target,
            packet_id: command.packet_id,
            claim: command.operation,
        })
    }

    fn stage_finish(&mut self, command: &FinishDelivery) -> Guard<CommitFacts> {
        let failed = RejectReason::PreconditionFailed;
        self.active_session(command.target, command.target_revision)?;
        let expected = OutboundRecord {
            target: command.target,
            target_revision: command.target_revision,
            packet_id: command.packet_id,
            claim: command.claim,
        };
        if self.outbound.get(&command.delivery) != Some(&expected) {
            return Err(failed);
        }
        self.end_obligation(command.delivery).map_err(|_| failed)?;
        self.charge(Accounts::RECEIPT)?;
        Ok(CommitFacts::DeliveryFinished {
            delivery: command.delivery,
            target: command.target,
            packet_id: command.packet_id,
            claim: command.claim,
        })
    }

    fn stage_consume(&mut self, command: &ConsumeDelivery) -> Guard<CommitFacts> {
        let failed = RejectReason::PreconditionFailed;
        self.active_session(command.target, command.target_revision)?;
        let record = self.obligations.get(&command.delivery).ok_or(failed)?;
        if record.target != command.target
            || record.target_revision != command.target_revision
            || record.qos != Qos::AtMostOnce
            || self.outbound.contains_key(&command.delivery)
        {
            return Err(failed);
        }
        self.end_obligation(command.delivery).map_err(|_| failed)?;
        self.charge(Accounts::RECEIPT)?;
        Ok(CommitFacts::DeliveryConsumed {
            delivery: command.delivery,
            target: command.target,
        })
    }

    fn stage_release(&mut self, release: &ReleaseInbound) -> Guard<CommitFacts> {
        let failed = RejectReason::PreconditionFailed;
        let slot = (release.source_session, release.packet_id);
        let publication = match self.inbound.get(&slot) {
            Some(marker)
                if marker.exchange == release.exchange
                    && marker.phase == InboundPhase::AwaitPubrel =>
            {
                marker.publication
            }
            _ => return Err(failed),
        };
        self.inbound.remove(&slot);
        self.accounts.exchange_slots = self
            .accounts
            .exchange_slots
            .checked_sub(ExchangeSlots::new(1))
            .ok_or(failed)?;
        // The receipt of the release holds a slot like every receipt.
        // Short of one, the guard fails in the staged copy, and the
        // rejection finds no slot either: nothing is written.
        self.charge(Accounts::RECEIPT)?;
        self.discard_completed_publication(publication);
        Ok(CommitFacts::InboundReleased {
            exchange: release.exchange,
        })
    }

    fn discard_completed_publication(&mut self, publication: PublicationId) {
        if !self
            .inbound
            .values()
            .any(|marker| marker.publication == publication)
            && !self
                .obligations
                .keys()
                .any(|delivery| delivery.publication == publication)
        {
            self.publications.remove(&publication);
        }
    }

    /// Ends one obligation and releases exactly what it held.
    fn end_obligation(&mut self, delivery: DeliveryId) -> Result<(), AdminRefused> {
        let broken = AdminRefused::Inconsistent;
        self.obligations.remove(&delivery).ok_or(broken)?;
        if self.outbound.remove(&delivery).is_some() {
            self.accounts.exchange_slots = self
                .accounts
                .exchange_slots
                .checked_sub(ExchangeSlots::new(1))
                .ok_or(broken)?;
        }
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
        self.discard_completed_publication(delivery.publication);
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
                    let marker = self
                        .inbound
                        .remove(&slot)
                        .ok_or(AdminRefused::Inconsistent)?;
                    self.accounts.exchange_slots = self
                        .accounts
                        .exchange_slots
                        .checked_sub(ExchangeSlots::new(1))
                        .ok_or(AdminRefused::Inconsistent)?;
                    self.discard_completed_publication(marker.publication);
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

    /// The recovery inventory, in the order of record 0014: the epoch,
    /// the limits, the route table, its routes sorted by the bytes of
    /// their topic with the targets of one topic in the order they were
    /// added, the active sessions, the inbound markers, then what is
    /// established about the operations of older epochs. A fact that
    /// cannot be built within its budget is an error in its place, never
    /// the end of the inventory.
    fn inventory(&self) -> impl Iterator<Item = FactRef<'_>> {
        let incarnation = self.ledger.incarnation();
        let open = self.ledger.open_epoch();
        let older = move |operation: &OperationId| Some(operation.epoch()) != open;
        let epoch = open.map(|epoch| FactRef::Epoch(incarnation, epoch));
        let routes = self.routes.iter().flat_map(|(topic, targets)| {
            targets
                .iter()
                .map(move |target| FactRef::Route(topic, target))
        });
        let sessions = self
            .sessions
            .iter()
            .filter(|(_, record)| record.state == SessionState::Active)
            .map(|(session, record)| FactRef::Session(*session, record));
        let inbound = self
            .inbound
            .iter()
            .map(|((_, packet), marker)| FactRef::Inbound(*packet, marker));
        let operations = self
            .operations
            .iter()
            .filter(move |(operation, _)| older(operation))
            .map(|(operation, record)| FactRef::Operation(*operation, record));
        let fences = self
            .fences
            .iter()
            .filter(move |operation| older(operation))
            .map(|operation| FactRef::Fence(*operation));
        epoch
            .into_iter()
            .chain([FactRef::Limits, FactRef::Table])
            .chain(routes)
            .chain(sessions)
            .chain(inbound)
            .chain(
                self.obligations
                    .iter()
                    .map(|(id, record)| FactRef::Outbound(*id, record)),
            )
            .chain(operations)
            .chain(fences)
    }
}

impl DurableStore for MemoryStore {
    fn ledger(&self) -> Result<EpochLedger, ReadError> {
        Ok(self.committed.ledger)
    }

    fn open_epoch(&mut self, command: &OpenEpoch) -> Result<OpeningOutcome, ReadError> {
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
        Ok(OpeningOutcome::read(&self.committed.ledger, command))
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

        if let Recipe::AdmitPublication(admit) = recipe {
            match self.admission_identity_was_used(admit) {
                Ok(false) => {}
                Ok(true) => {
                    return rejected(self.commit_rejection(
                        operation,
                        canonical,
                        RejectReason::PreconditionFailed,
                    ));
                }
                Err(_) => return settled(StoreOutcome::PendingOrUnknown),
            }
        }

        // 2 to 5. Guards, capacities and every row, on a staged copy.
        let mut staged = self.committed.clone();
        let facts = match recipe {
            Recipe::AdmitPublication(admit) => {
                staged.stage_admission(admit, command.payload.as_ref())
            }
            Recipe::ReleaseInbound(release) => staged.stage_release(release),
            Recipe::ChangeSubscriptions(command) => staged.stage_subscriptions(command),
            Recipe::ClaimDelivery(command) => staged.stage_claim(command),
            Recipe::FinishDelivery(command) => staged.stage_finish(command),
            Recipe::ConsumeDelivery(command) => staged.stage_consume(command),
            Recipe::UpdateSubscriptions(command) => staged.stage_updates(command),
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

    fn established(&self, operation: OperationId) -> Result<Option<StoreOutcome>, ReadError> {
        Ok(match self.committed.established(operation) {
            Established::Outcome(outcome) => Some(outcome),
            Established::Absent(_) => None,
        })
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
                if staged.charge(Accounts::RECEIPT).is_err() {
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

    fn recover_with_budget(
        &self,
        cursor: Option<RecoveryCursor>,
        limit: usize,
        budget: ReadBudget,
    ) -> Result<RecoveryPage, RecoveryRefused> {
        let refused = RecoveryRefused::Read;
        if limit == 0 {
            return Err(RecoveryRefused::EmptyPage);
        }
        budget.check_field(8).map_err(refused)?;
        budget
            .check_totals(
                limit,
                limit
                    .checked_mul(128)
                    .ok_or_else(|| refused(ReadError::LimitExceeded))?,
            )
            .map_err(refused)?;
        check_inventory_bound(&self.committed).map_err(refused)?;
        let revision = self.committed.revision;
        let start = match cursor {
            Some(cursor) if cursor.revision() != revision => {
                return Err(RecoveryRefused::FrontierMoved);
            }
            Some(cursor) => cursor.position(),
            None => 0,
        };
        if start > MAX_RECOVERY_INVENTORY_ROWS {
            return Err(refused(ReadError::LimitExceeded));
        }
        let total = self.committed.inventory().count();
        if start > total {
            return Err(refused(ReadError::InvalidData));
        }
        let mut facts =
            BoundedVec::with_capacity(limit).map_err(|_| refused(ReadError::AllocationFailed))?;
        let mut meter = ReadMeter::new(budget);
        // Skip borrowed raw entries, not built facts: no old page is copied.
        let mut inventory = self.committed.inventory().skip(start);
        let mut position = start;
        while !facts.is_full() {
            let Some(raw) = inventory.next() else {
                return Ok(RecoveryPage { facts, next: None });
            };
            let fact = raw.build(&self.committed, &mut meter).map_err(refused)?;
            facts
                .try_push(fact)
                .map_err(|_| refused(ReadError::LimitExceeded))?;
            position = position
                .checked_add(1)
                .ok_or_else(|| refused(ReadError::LimitExceeded))?;
        }
        let next = inventory
            .next()
            .is_some()
            .then_some(RecoveryCursor::new(revision, position));
        Ok(RecoveryPage { facts, next })
    }

    fn read_work_with_budget(
        &self,
        session: SessionId,
        after: Option<DeliveryId>,
        limit: usize,
        budget: ReadBudget,
    ) -> Result<WorkPage, ReadError> {
        if limit == 0 || limit > WORK_PAGE_LIMIT as usize {
            return Err(ReadError::LimitExceeded);
        }
        let state = &self.committed;
        check_inventory_bound(state)?;
        let mut meter = ReadMeter::new(budget);
        let mut items =
            BoundedVec::with_capacity(limit).map_err(|_| ReadError::AllocationFailed)?;
        let mut more = false;
        for (delivery, record) in &state.obligations {
            if after.is_some_and(|after| *delivery <= after)
                || record.target != session
                || !matches!(record.qos, Qos::AtMostOnce | Qos::AtLeastOnce)
            {
                continue;
            }
            if items.len() == limit {
                more = true;
                break;
            }
            let publication = state
                .publications
                .get(&delivery.publication)
                .ok_or(ReadError::InvalidData)?;
            let payload = state
                .payloads
                .get(&publication.payload)
                .ok_or(ReadError::InvalidData)?;
            meter.field(publication.topic.len(), state.limits.max_topic_bytes)?;
            meter.field(payload.bytes.len(), state.limits.max_payload_bytes)?;
            meter.charge(
                256usize
                    .checked_add(publication.topic.len())
                    .and_then(|n| n.checked_add(payload.bytes.len()))
                    .ok_or(ReadError::LimitExceeded)?,
            )?;
            let claim = delivery_claim(state, *delivery, record)?;
            items
                .try_push(WorkItem {
                    delivery: *delivery,
                    target: record.target,
                    target_revision: record.target_revision,
                    qos: record.qos,
                    topic: SharedBytes::copy_from(&publication.topic, state.limits.max_topic_bytes)
                        .map_err(|_| ReadError::LimitExceeded)?,
                    payload: SharedBytes::copy_from(&payload.bytes, state.limits.max_payload_bytes)
                        .map_err(|_| ReadError::LimitExceeded)?,
                    claim,
                })
                .map_err(|_| ReadError::LimitExceeded)?;
        }
        let next = if more {
            items.as_slice().last().map(|item| item.delivery)
        } else {
            None
        };
        Ok(WorkPage { items, next })
    }

    fn snapshot_with_budget(&self, budget: ReadBudget) -> Result<DurableState, ReadError> {
        check_snapshot(&self.committed, budget)?;
        Ok(self.committed.clone())
    }
}
