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
#[path = "retained.rs"]
mod retained_memory;
#[path = "will.rs"]
mod will_memory;

use mqttiumd_contract::capacity::{
    BoundedVec, ExchangeSlots, PayloadBytes, ReceiptSlots, References, Resource,
};
use mqttiumd_contract::command::{
    AdmitPublication, ChangeSubscriptions, ClaimDelivery, Command, ConsumeDelivery, EpochLedger,
    EpochPhase, FinishDelivery, InlineClaim, LedgerOpening, OpenEpoch, Recipe, RecipeError,
    ReleaseInbound, UpdateSubscriptions,
};
use mqttiumd_contract::effects::{InboundPhase, RecoveredFact};
use mqttiumd_contract::ids::{
    DeliveryId, LogicalOwnerId, OperationId, OperationSeq, PayloadId, PayloadSeq, PublicationId,
    PublicationSeq, Revision, RunEpoch, SessionId, StoreIncarnation,
};
use mqttiumd_contract::message_expiry::{
    ExpireDelivery, ExpiringAdmission, ExpiryState, MarkDeliveryStarted, MessageExpiry, MessageTime,
};
use mqttiumd_contract::outcome::{
    CommitFacts, Knowledge, OpeningOutcome, RejectReason, StoreOutcome, StoreReply, StoreViolation,
};
use mqttiumd_contract::packet::{Qos, SharedBytes};
use mqttiumd_contract::pubsub::{
    DeliveryClaim, MAX_SUBSCRIPTIONS, SubscriptionOptions, WORK_PAGE_LIMIT, WorkItem,
};
use mqttiumd_contract::receipts::{
    ClosedReceiptCollection, ClosedReceiptReply, MAX_COLLECTION_ROWS, MAX_RECEIPT_ORIGINS,
    RECEIPT_CONTROL_RESERVE, RECEIPT_DATA_WINDOW, RECEIPT_WINDOW, ReceiptClass, ReceiptClosure,
    ReceiptCollection, ReceiptCollectionRefusal, ReceiptCollectionReply, ReceiptOrigin,
};
use mqttiumd_contract::session::{
    SessionAction, SessionCancellations, SessionChange, SessionDetachment, SessionEndReason,
    SessionExpiry, SessionGuard, SessionLifecycle,
};

pub use mqttiumd_store_api::*;

/// The deterministic store: its durable state, and whether the medium
/// currently accepts writes.
#[derive(Debug)]
pub struct MemoryStore {
    committed: DurableState,
    durability: Durability,
}

#[derive(Clone, Copy)]
struct Admission<'a> {
    expiry: Option<MessageExpiry>,
    properties: Option<&'a std::sync::Arc<mqttiumd_contract::properties::PublishProperties>>,
    operation: OperationId,
    request: Option<mqttiumd_contract::ids::RequestId>,
    will_source: Option<&'a [u8]>,
    publication: PublicationId,
    source_session: SessionId,
    source_revision: Revision,
    inbound: Option<(
        mqttiumd_contract::ids::InboundPacketId,
        mqttiumd_contract::ids::ExchangeId,
    )>,
    route_revision: Revision,
    qos: Qos,
    retain: bool,
    topic: &'a SharedBytes,
    payload: mqttiumd_contract::command::PayloadRef,
    costs: mqttiumd_contract::command::Costs,
    targets: &'a [mqttiumd_contract::command::Target],
}
impl<'a> From<&'a AdmitPublication> for Admission<'a> {
    fn from(a: &'a AdmitPublication) -> Self {
        Self {
            expiry: None,
            properties: None,
            operation: a.operation,
            request: Some(a.request),
            will_source: None,
            publication: a.publication,
            source_session: a.source.session,
            source_revision: a.source.session_revision,
            inbound: Some((a.source.packet_id, a.source.exchange)),
            route_revision: a.route_revision,
            qos: a.qos,
            retain: a.retain,
            topic: &a.topic,
            payload: a.payload,
            costs: a.costs,
            targets: a.targets.as_slice(),
        }
    }
}
impl<'a> From<&'a mqttiumd_contract::retained::RetainedQos0> for Admission<'a> {
    fn from(a: &'a mqttiumd_contract::retained::RetainedQos0) -> Self {
        Self {
            expiry: None,
            properties: None,
            operation: a.operation,
            request: Some(a.request),
            will_source: None,
            publication: a.publication,
            source_session: a.session,
            source_revision: a.session_revision,
            inbound: None,
            route_revision: a.route_revision,
            qos: Qos::AtMostOnce,
            retain: true,
            topic: &a.topic,
            payload: a.payload,
            costs: a.costs,
            targets: a.targets.as_slice(),
        }
    }
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
                session_clock: None,
                route_revision: Revision::new(0),
                sessions: BTreeMap::new(),
                routes: BTreeMap::new(),
                operations: BTreeMap::new(),
                fences: BTreeSet::new(),
                receipt_frontiers: BTreeMap::new(),
                retained: BTreeMap::new(),
                wills: BTreeMap::new(),
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
    ReceiptFrontier(ReceiptClosure),
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
            Self::ReceiptFrontier(closure) => RecoveredFact::ReceiptFrontier(closure),
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
                    options: target.options,
                }
            }
            Self::Session(session, record) => {
                meter.field(record.client_id.len(), MAX_CLIENT_ID_BYTES)?;
                RecoveredFact::Session {
                    session,
                    client_id: SharedBytes::copy_from(&record.client_id, MAX_CLIENT_ID_BYTES)
                        .map_err(|_| ReadError::LimitExceeded)?,
                    revision: record.revision,
                    expiry: record.expiry,
                    opened_by: record.opened_by,
                    detached: record.detached,
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
                    expiry: state
                        .publications
                        .get(&delivery.publication)
                        .ok_or(ReadError::InvalidData)?
                        .expiry,
                    started: claim.is_some_and(|c| c.started),
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
        return if record.phase == ObligationPhase::Pending {
            Ok(None)
        } else {
            Err(ReadError::InvalidData)
        };
    };
    if claim.target != record.target
        || claim.target_revision != record.target_revision
        || claim.claim.incarnation() != state.ledger.incarnation()
    {
        return Err(ReadError::InvalidData);
    }
    let claim = DeliveryClaim {
        operation: claim.claim,
        packet_id: claim.packet_id,
        started: claim.started,
        await_pubcomp: record.phase == ObligationPhase::AwaitPubcomp,
    };
    let timed = state
        .publications
        .get(&delivery.publication)
        .is_some_and(|p| p.expiry.is_some());
    if !claim.valid_for(record.qos, timed) {
        return Err(ReadError::InvalidData);
    }
    Ok(Some(claim))
}

fn check_inventory_bound(state: &DurableState) -> Result<(), ReadError> {
    will_memory::validate(state)?;
    for publication in state.publications.values() {
        if let Some(expiry) = publication.expiry {
            if expiry.observed.domain == [0; 32] {
                return Err(ReadError::InvalidData);
            }
            expiry.deadline().map_err(|_| ReadError::InvalidData)?;
        }
    }

    for row in state.sessions.values() {
        if matches!(row.expiry, SessionExpiry::After(_)) && state.session_clock.is_none() {
            return Err(ReadError::InvalidData);
        }
        if let SessionExpiry::After(seconds) = row.expiry
            && (seconds == 0 || seconds == u32::MAX)
        {
            return Err(ReadError::InvalidData);
        }
        if let Some(detached) = row.detached
            && (row.opened_by.is_none()
                || detached.operation.incarnation() != state.ledger.incarnation()
                || detached.operation.epoch().get() == 0
                || detached.operation.epoch() > state.ledger.last_opened()
                || !detached.valid_for(row.expiry))
        {
            return Err(ReadError::InvalidData);
        }
        if row.opened_by.is_some_and(|id| {
            id.incarnation() != state.ledger.incarnation()
                || id.epoch().get() == 0
                || id.epoch() > state.ledger.last_opened()
        }) {
            return Err(ReadError::InvalidData);
        }
    }
    if state.receipt_frontiers.len() > MAX_RECEIPT_ORIGINS {
        return Err(ReadError::LimitExceeded);
    }
    for (origin, closure) in &state.receipt_frontiers {
        if *origin != closure.origin
            || origin.incarnation != state.ledger.incarnation()
            || origin.epoch.get() == 0
            || origin.epoch > state.ledger.last_opened()
        {
            return Err(ReadError::InvalidData);
        }
    }
    let mut rows = 3usize;
    for size in [
        state.routes.len(),
        state.sessions.len(),
        state.inbound.len(),
        state.obligations.len(),
        state.outbound.len(),
        state.operations.len(),
        state.fences.len(),
        state.receipt_frontiers.len(),
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
        state.receipt_frontiers.len(),
    ] {
        rows = rows.checked_add(size).ok_or(ReadError::LimitExceeded)?;
    }
    for (filter, targets) in &state.routes {
        if filter.len() > state.limits.max_topic_bytes {
            return Err(ReadError::LimitExceeded);
        }
        if !mqttiumd_contract::topic::valid_filter(filter)
            || targets.iter().any(|target| {
                SubscriptionOptions::from_bits(target.options.bits()) != Some(target.options)
            })
        {
            return Err(ReadError::InvalidData);
        }
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

fn check_delivery_metadata(
    state: &DurableState,
    delivery: DeliveryId,
    record: &ObligationRecord,
) -> Result<(), ReadError> {
    let publication = state
        .publications
        .get(&delivery.publication)
        .ok_or(ReadError::InvalidData)?;
    if let Some(expiry) = publication.expiry {
        expiry.deadline().map_err(|_| ReadError::InvalidData)?;
    }
    if (record.retain && !publication.retain)
        || (record.cause == mqttiumd_contract::command::DeliveryCause::RetainedReplay
            && (!record.retain
                || publication.retained_version.is_none()
                || publication.retained_version == Some(delivery.publication)))
        || (record.cause == mqttiumd_contract::command::DeliveryCause::LiveExact
            && publication.retained_version.is_some())
    {
        return Err(ReadError::InvalidData);
    }
    Ok(())
}

fn check_snapshot(state: &DurableState, budget: ReadBudget) -> Result<(), ReadError> {
    check_inventory_bound(state)?;
    retained_memory::validate(state, budget)?;
    let mut meter = ReadMeter::new(budget);
    budget.check_field(8)?;
    if state.ledger.opened_by().is_some() {
        budget.check_field(24)?;
    }
    meter.charge(256)?;
    if state.session_clock.is_some() {
        budget.check_field(32)?;
        meter.charge(176)?;
    }
    for intent in state.wills.values() {
        for field in [&intent.client_id, &intent.spec.topic, &intent.spec.payload] {
            budget.check_field(field.len())?;
        }
        meter.charge(
            128usize
                .checked_add(intent.client_id.len())
                .and_then(|n| n.checked_add(intent.spec.topic.len()))
                .and_then(|n| n.checked_add(intent.spec.payload.len()))
                .ok_or(ReadError::LimitExceeded)?,
        )?;
    }
    for record in state.sessions.values() {
        meter.field(record.client_id.len(), MAX_CLIENT_ID_BYTES)?;
        meter.charge(
            160usize
                .checked_add(if record.detached.is_some() { 40 } else { 0 })
                .and_then(|n| n.checked_add(if record.opened_by.is_some() { 32 } else { 0 }))
                .and_then(|n| n.checked_add(record.client_id.len()))
                .ok_or(ReadError::LimitExceeded)?,
        )?;
    }
    for (topic, targets) in &state.routes {
        meter.field(topic.len(), state.limits.max_topic_bytes)?;
        for _ in targets {
            meter.charge(
                152usize
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
    if state.receipt_frontiers.len() > MAX_RECEIPT_ORIGINS {
        return Err(ReadError::LimitExceeded);
    }
    for (origin, closure) in &state.receipt_frontiers {
        meter.field(8, 8)?;
        meter.charge(168)?;
        if *origin != closure.origin
            || origin.incarnation != state.ledger.incarnation()
            || origin.epoch.get() == 0
            || origin.epoch > state.ledger.last_opened()
        {
            return Err(ReadError::InvalidData);
        }
    }
    for (topic, record) in &state.retained {
        meter.field(topic.len(), state.limits.max_topic_bytes)?;
        meter.field(record.producer_client_id.len(), MAX_CLIENT_ID_BYTES)?;
        meter.charge(
            256usize
                .checked_add(topic.len())
                .and_then(|n| n.checked_add(record.producer_client_id.len()))
                .ok_or(ReadError::LimitExceeded)?,
        )?;
    }
    for record in state.publications.values() {
        if let Some(expiry) = record.expiry {
            expiry.deadline().map_err(|_| ReadError::InvalidData)?;
        }
        meter.field(32, 32)?;
        meter.field(record.topic.len(), state.limits.max_topic_bytes)?;
        meter.charge(
            248usize
                .checked_add(if record.expiry.is_some() { 44 } else { 0 })
                .and_then(|n| n.checked_add(record.topic.len()))
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
    for (delivery, record) in &state.obligations {
        check_delivery_metadata(state, *delivery, record)?;
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
    fn stage_publication(
        &mut self,
        admit: Admission<'_>,
        retained_mask: Option<&mqttiumd_contract::retained::RetainMask>,
        payload: Option<&SharedBytes>,
    ) -> Guard<CommitFacts>;
    fn stage_publication_with_claim(
        &mut self,
        admit: Admission<'_>,
        retained_mask: Option<&mqttiumd_contract::retained::RetainMask>,
        payload: Option<&SharedBytes>,
        inline_claim: Option<InlineClaim>,
    ) -> Guard<CommitFacts>;
    fn stage_release(&mut self, release: &ReleaseInbound) -> Guard<CommitFacts>;
    fn stage_subscriptions(&mut self, command: &ChangeSubscriptions) -> Guard<CommitFacts>;
    fn stage_updates(&mut self, command: &UpdateSubscriptions) -> Guard<CommitFacts>;
    fn stage_route_updates<'a>(
        &mut self,
        session: SessionId,
        revision: Revision,
        route_revision: Revision,
        changes: impl Iterator<Item = (&'a SharedBytes, Option<Qos>, SubscriptionOptions)>,
    ) -> Guard<CommitFacts>;
    fn stage_claim(&mut self, command: &ClaimDelivery) -> Guard<CommitFacts>;
    fn stage_finish(&mut self, command: &FinishDelivery) -> Guard<CommitFacts>;
    fn stage_pubrel(&mut self, command: &FinishDelivery) -> Guard<CommitFacts>;
    fn stage_started(&mut self, command: &MarkDeliveryStarted) -> Guard<CommitFacts>;
    fn stage_expired(&mut self, command: &ExpireDelivery) -> Guard<CommitFacts>;
    fn stage_consume(&mut self, command: &ConsumeDelivery) -> Guard<CommitFacts>;
    fn discard_completed_publication(&mut self, publication: PublicationId);
    fn end_obligation(&mut self, delivery: DeliveryId) -> Result<(), AdminRefused>;
    fn stage_admin(&mut self, command: AdminCommand) -> Result<AdminDone, AdminRefused>;
    /// A change of session. One that opens or ends a will goes to the
    /// will memory, which changes the session through
    /// [`Staging::change_session`]; any other is that change.
    fn stage_session(&mut self, command: &SessionLifecycle) -> Guard<CommitFacts>;
    /// A change of session that carries no will: open, detach, close and
    /// their timed forms. The will memory builds on it; a will action
    /// given here is refused.
    fn change_session(&mut self, command: &SessionLifecycle) -> Guard<CommitFacts>;
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
        if self
            .receipt_frontiers
            .get(&ReceiptOrigin::of(operation))
            .is_some_and(|closure| operation.sequence() <= closure.through)
        {
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
        self.stage_publication(Admission::from(admit), None, payload)
    }
    fn stage_publication(
        &mut self,
        admit: Admission<'_>,
        retained_mask: Option<&mqttiumd_contract::retained::RetainMask>,
        payload: Option<&SharedBytes>,
    ) -> Guard<CommitFacts> {
        self.stage_publication_with_claim(admit, retained_mask, payload, None)
    }
    fn stage_publication_with_claim(
        &mut self,
        admit: Admission<'_>,
        retained_mask: Option<&mqttiumd_contract::retained::RetainMask>,
        payload: Option<&SharedBytes>,
        inline_claim: Option<InlineClaim>,
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
        ) || admit.request.is_some_and(|request| {
            !same_scope(request.incarnation(), request.epoch(), request.owner())
        }) {
            return Err(failed);
        }
        let expired = if let Some(expiry) = admit.expiry {
            check_message_observation(self, expiry.observed).map_err(|_| failed)?;
            expiry.deadline().map_err(|_| failed)?;
            let anchor = self.session_clock.ok_or(failed)?.reading;
            expiry.interval == 0
                || (anchor.tick >= expiry.observed.tick
                    && matches!(
                        expiry.state(MessageTime {
                            domain: anchor.domain,
                            tick: anchor.tick
                        }),
                        Ok(ExpiryState::Expired)
                    ))
        } else {
            false
        };
        let targets = admit.targets;
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
        if let Some(props) = admit.properties
            && (props.is_empty()
                || props.check().is_err()
                || props.check_payload(bytes.as_slice()).is_err())
        {
            return Err(failed);
        }
        // The costs are part of the frozen recipe. They must be the
        // costs of what the recipe admits: the store releases exactly
        // what it charged, row by row.
        let exactly_once = admit.qos == Qos::ExactlyOnce;
        let reference_count = u64::try_from(targets.len()).map_err(|_| failed)?
            + u64::from(retained_mask.is_some() && length > 0);
        if admit.costs.payload_bytes != admit.payload.length
            || admit.costs.references.get() != reference_count
            || admit.costs.exchange_slots.get() != u64::from(exactly_once)
            || admit.costs.receipt_slots.get() != 1
        {
            return Err(failed);
        }
        if admit.will_source.is_none() {
            self.active_session(admit.source_session, admit.source_revision)?;
        }

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
        if retained_mask.is_some() {
            retained_memory::prepare_mutation(
                self,
                admit.topic.as_slice(),
                if expired { 0 } else { length },
            )?;
        }
        let reference_count = if expired { 0 } else { reference_count };
        self.publications.insert(
            admit.publication,
            PublicationRecord {
                expiry: admit.expiry,
                properties: admit.properties.cloned(),
                retained_version: None,
                operation,
                source: admit.source_session,
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
        for (target_index, target) in targets.iter().enumerate() {
            self.active_session(target.session, target.session_revision)?;
            if expired {
                continue;
            }
            let obligation = ObligationRecord {
                retain: retained_mask.is_some_and(|mask| mask.is_set(target_index)),
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
        let claimed_target = if let Some(claim) = inline_claim {
            if expired {
                return Err(failed);
            }
            let target = targets.get(usize::from(claim.target_index)).ok_or(failed)?;
            if target.effective_qos == Qos::AtMostOnce
                || self.outbound.contains_key(&target.delivery)
                || self
                    .outbound
                    .values()
                    .any(|slot| slot.target == target.session && slot.packet_id == claim.packet_id)
            {
                return Err(failed);
            }
            self.outbound.insert(
                target.delivery,
                OutboundRecord {
                    started: false,
                    target: target.session,
                    target_revision: target.session_revision,
                    packet_id: claim.packet_id,
                    claim: operation,
                },
            );
            true
        } else {
            false
        };
        if exactly_once {
            let (packet_id, exchange) = admit.inbound.ok_or(failed)?;
            let slot = (admit.source_session, packet_id);
            let marker = InboundRecord {
                exchange,
                publication: admit.publication,
                phase: InboundPhase::AwaitPubrel,
            };
            // One admission per inbound exchange: an occupied slot is
            // never taken over, whatever generation the newcomer claims.
            if self.inbound.insert(slot, marker).is_some() {
                return Err(failed);
            }
        }
        let exchange_slots = ExchangeSlots::new(
            admit
                .costs
                .exchange_slots
                .get()
                .checked_add(u64::from(claimed_target))
                .ok_or(failed)?,
        );
        self.charge(Accounts {
            payload_bytes: if expired {
                PayloadBytes::new(0)
            } else {
                admit.costs.payload_bytes
            },
            references: References::new(reference_count),
            exchange_slots,
            receipt_slots: admit.costs.receipt_slots,
        })?;
        if reference_count == 0 && !expired {
            self.accounts.payload_bytes = self
                .accounts
                .payload_bytes
                .checked_sub(admit.costs.payload_bytes)
                .ok_or(failed)?;
        }
        if retained_mask.is_some() && length > 0 && !expired {
            retained_memory::install(
                self,
                admit.topic.as_slice(),
                admit.publication,
                admit.source_session,
                admit.will_source,
            )?;
        }
        self.discard_completed_publication(admit.publication);
        Ok(match admit.inbound {
            Some((_, exchange)) => CommitFacts::PublicationAdmitted {
                publication: admit.publication,
                exchange,
            },
            None => CommitFacts::RetainedQos0Stored {
                publication: admit.publication,
            },
        })
    }

    fn stage_subscriptions(&mut self, command: &ChangeSubscriptions) -> Guard<CommitFacts> {
        self.stage_route_updates(
            command.session,
            command.session_revision,
            command.route_revision,
            command.changes.as_slice().iter().map(|c| {
                (
                    &c.topic,
                    c.subscribe.then_some(Qos::AtLeastOnce),
                    SubscriptionOptions::DEFAULT,
                )
            }),
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
                .map(|c| (&c.topic, c.granted, c.options)),
        )
    }
    fn stage_route_updates<'a>(
        &mut self,
        session: SessionId,
        revision: Revision,
        route_revision: Revision,
        changes: impl Iterator<Item = (&'a SharedBytes, Option<Qos>, SubscriptionOptions)>,
    ) -> Guard<CommitFacts> {
        let failed = RejectReason::PreconditionFailed;
        self.active_session(session, revision)?;
        if route_revision != self.route_revision {
            return Err(failed);
        }
        for (topic, granted, options) in changes {
            if topic.len() > self.limits.max_topic_bytes
                || !mqttiumd_contract::topic::valid_filter(topic.as_slice())
                || SubscriptionOptions::from_bits(options.bits()) != Some(options)
                || (granted.is_none() && options != SubscriptionOptions::DEFAULT)
            {
                return Err(failed);
            }
            if let Some(qos) = granted {
                let targets = self.routes.entry(topic.as_slice().to_vec()).or_default();
                if let Some(target) = targets.iter_mut().find(|target| target.session == session) {
                    target.qos = qos;
                    target.options = options;
                } else {
                    if targets.len() >= self.limits.max_route_targets
                        || targets.len() >= self.limits.max_targets_per_admission
                    {
                        return Err(RejectReason::CapacityExceeded(Resource::References));
                    }
                    targets.push(RouteTarget {
                        session,
                        qos,
                        options,
                    });
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
            || record.qos == Qos::AtMostOnce
            || record.phase != ObligationPhase::Pending
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
                started: false,
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
            started: self.outbound.get(&command.delivery).ok_or(failed)?.started,
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

    fn stage_started(&mut self, command: &MarkDeliveryStarted) -> Guard<CommitFacts> {
        let failed = RejectReason::PreconditionFailed;
        self.active_session(command.target, command.target_revision)?;
        if self
            .publications
            .get(&command.delivery.publication)
            .and_then(|publication| publication.expiry)
            .is_none()
        {
            return Err(failed);
        }
        if self
            .obligations
            .get(&command.delivery)
            .is_none_or(|record| record.phase != ObligationPhase::Pending)
        {
            return Err(failed);
        }
        let slot = self.outbound.get_mut(&command.delivery).ok_or(failed)?;
        if slot.target != command.target
            || slot.target_revision != command.target_revision
            || slot.packet_id != command.packet_id
            || slot.claim != command.claim
        {
            return Err(failed);
        }
        slot.started = true;
        self.charge(Accounts::RECEIPT)?;
        Ok(CommitFacts::DeliveryStarted {
            delivery: command.delivery,
            target: command.target,
            packet_id: command.packet_id,
            claim: command.claim,
        })
    }

    fn stage_pubrel(&mut self, command: &FinishDelivery) -> Guard<CommitFacts> {
        let failed = RejectReason::PreconditionFailed;
        self.active_session(command.target, command.target_revision)?;
        let slot = self.outbound.get(&command.delivery).ok_or(failed)?;
        if slot.target != command.target
            || slot.target_revision != command.target_revision
            || slot.packet_id != command.packet_id
            || slot.claim != command.claim
        {
            return Err(failed);
        }
        let record = self.obligations.get_mut(&command.delivery).ok_or(failed)?;
        if record.qos != Qos::ExactlyOnce || record.phase != ObligationPhase::Pending {
            return Err(failed);
        }
        record.phase = ObligationPhase::AwaitPubcomp;
        self.charge(Accounts::RECEIPT)?;
        Ok(CommitFacts::DeliveryPubrelReady {
            delivery: command.delivery,
            target: command.target,
            packet_id: command.packet_id,
            claim: command.claim,
        })
    }

    fn stage_expired(&mut self, command: &ExpireDelivery) -> Guard<CommitFacts> {
        let failed = RejectReason::PreconditionFailed;
        check_message_observation(self, command.observed).map_err(|_| failed)?;
        let slot = self.outbound.get(&command.finish.delivery).ok_or(failed)?;
        let publication = self
            .publications
            .get(&command.finish.delivery.publication)
            .ok_or(failed)?;
        if slot.started
            || self
                .obligations
                .get(&command.finish.delivery)
                .is_none_or(|record| record.phase != ObligationPhase::Pending)
            || publication.expiry != Some(command.expiry)
            || command.expiry.state(command.observed) != Ok(ExpiryState::Expired)
        {
            return Err(failed);
        }
        self.stage_finish(&command.finish)?;
        Ok(CommitFacts::DeliveryExpired {
            delivery: command.finish.delivery,
            target: command.finish.target,
            packet_id: command.finish.packet_id,
            claim: command.finish.claim,
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
            && !self
                .retained
                .values()
                .any(|entry| entry.publication == publication)
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
                        expiry: SessionExpiry::Never,
                        opened_by: None,
                        detached: None,
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
                if !mqttiumd_contract::topic::valid_filter(&topic) {
                    return Err(AdminRefused::Inconsistent);
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
                targets.push(RouteTarget {
                    session,
                    qos,
                    options: SubscriptionOptions::DEFAULT,
                });
                self.route_revision = self
                    .route_revision
                    .checked_next()
                    .map_err(|_| AdminRefused::Exhausted)?;
                Ok(AdminDone::RouteAdded {
                    route_revision: self.route_revision,
                })
            }
            AdminCommand::DeleteSession { session } => {
                cancel_session(self, session)?;
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

    fn stage_session(&mut self, command: &SessionLifecycle) -> Guard<CommitFacts> {
        match command.action {
            SessionAction::OpenWillAt { .. } => will_memory::open(self, command),
            SessionAction::EndWill { .. } => will_memory::end(self, command),
            _ => self.change_session(command),
        }
    }

    fn change_session(&mut self, command: &SessionLifecycle) -> Guard<CommitFacts> {
        let failed = RejectReason::PreconditionFailed;
        let change = match &command.action {
            SessionAction::OpenWillAt { .. } | SessionAction::EndWill { .. } => return Err(failed),
            SessionAction::Open {
                client_id,
                expected,
                clean_start,
                expiry,
            }
            | SessionAction::OpenAt {
                client_id,
                expected,
                clean_start,
                expiry,
                ..
            } => {
                if matches!(expiry, SessionExpiry::After(_)) && self.session_clock.is_none() {
                    return Err(failed);
                }
                if self.wills.values().any(|i| i.client_id == *client_id) {
                    return Err(failed);
                }
                let mut found = self.sessions.iter().filter(|(_, r)| {
                    r.state == SessionState::Active && r.client_id == client_id.as_slice()
                });
                let current = found.next().map(|(id, record)| (*id, record.clone()));
                if found.next().is_some()
                    || current.as_ref().map(|(session, record)| SessionGuard {
                        session: *session,
                        opened_by: record.opened_by,
                    }) != *expected
                {
                    return Err(failed);
                }
                let now = match command.action {
                    SessionAction::OpenAt { now, .. } => Some(now),
                    _ => None,
                };
                if now.is_none()
                    && current
                        .as_ref()
                        .is_some_and(|(_, row)| row.detached.and_then(|d| d.deadline).is_some())
                {
                    return Err(failed);
                }
                let replace = current.as_ref().is_some_and(|(_, r)| {
                    *clean_start
                        || r.expiry == SessionExpiry::OnDisconnect
                        || r.detached
                            .and_then(|d| d.deadline)
                            .zip(now)
                            .is_some_and(|(deadline, now)| deadline <= now)
                });
                let mut cancelled = SessionCancellations::default();
                let retired = if replace {
                    let id = current.as_ref().ok_or(failed)?.0;
                    cancelled = cancel_session(self, id).map_err(|_| failed)?;
                    Some(id)
                } else {
                    None
                };
                let (session, revision, session_present) =
                    if let Some((id, record)) = current.filter(|_| !replace) {
                        let row = self.sessions.get_mut(&id).ok_or(failed)?;
                        row.expiry = *expiry;
                        row.opened_by = Some(command.operation);
                        row.detached = None;
                        (id, record.revision, true)
                    } else {
                        if self
                            .sessions
                            .values()
                            .filter(|r| r.state == SessionState::Active)
                            .count()
                            >= self.limits.max_sessions
                        {
                            return Err(RejectReason::SessionCapacityExceeded);
                        }
                        let session = self.next_session;
                        self.next_session = session.checked_next().map_err(|_| failed)?;
                        let mut bytes = Vec::new();
                        bytes
                            .try_reserve_exact(client_id.len())
                            .map_err(|_| failed)?;
                        bytes.extend_from_slice(client_id.as_slice());
                        let revision = Revision::new(1);
                        self.sessions.insert(
                            session,
                            SessionRecord {
                                client_id: bytes,
                                revision,
                                state: SessionState::Active,
                                expiry: *expiry,
                                opened_by: Some(command.operation),
                                detached: None,
                            },
                        );
                        (session, revision, false)
                    };
                SessionChange::Opened {
                    session,
                    revision,
                    session_present,
                    expiry: *expiry,
                    retired,
                    route_revision: self.route_revision,
                    cancelled,
                }
            }
            SessionAction::Detach {
                guard,
                expiry,
                observed_at,
            } => {
                if matches!(expiry, SessionExpiry::After(_)) && self.session_clock.is_none() {
                    return Err(failed);
                }
                let row = self
                    .sessions
                    .get(&guard.session)
                    .filter(|r| r.state == SessionState::Active)
                    .ok_or(failed)?;
                if row.opened_by.is_none()
                    || row.opened_by != guard.opened_by
                    || row.detached.is_some()
                {
                    return Err(failed);
                }
                let deadline = expiry.deadline(*observed_at).ok_or(failed)?;
                if *expiry == SessionExpiry::OnDisconnect {
                    let cancelled = cancel_session(self, guard.session).map_err(|_| failed)?;
                    SessionChange::Closed {
                        session: guard.session,
                        route_revision: self.route_revision,
                        cancelled,
                    }
                } else {
                    let detached = SessionDetachment {
                        operation: command.operation,
                        deadline,
                    };
                    let row = self.sessions.get_mut(&guard.session).ok_or(failed)?;
                    row.expiry = *expiry;
                    row.detached = Some(detached);
                    SessionChange::Detached {
                        session: guard.session,
                        expiry: *expiry,
                        detached,
                    }
                }
            }
            SessionAction::Expire {
                guard,
                detached,
                now,
            } => {
                let row = self
                    .sessions
                    .get(&guard.session)
                    .filter(|r| r.state == SessionState::Active)
                    .ok_or(failed)?;
                if row.opened_by != guard.opened_by
                    || row.detached != Some(*detached)
                    || !matches!(row.expiry, SessionExpiry::After(_))
                    || !detached.deadline.is_some_and(|deadline| *now >= deadline)
                {
                    return Err(failed);
                }
                let cancelled = cancel_session(self, guard.session).map_err(|_| failed)?;
                SessionChange::Closed {
                    session: guard.session,
                    route_revision: self.route_revision,
                    cancelled,
                }
            }
            SessionAction::Close { guard, reason } => {
                let row = self
                    .sessions
                    .get(&guard.session)
                    .filter(|r| r.state == SessionState::Active)
                    .ok_or(failed)?;
                if row.opened_by != guard.opened_by
                    || (matches!(
                        reason,
                        SessionEndReason::TransportLost | SessionEndReason::Startup
                    ) && row.expiry != SessionExpiry::OnDisconnect)
                {
                    return Err(failed);
                }
                let cancelled = cancel_session(self, guard.session).map_err(|_| failed)?;
                SessionChange::Closed {
                    session: guard.session,
                    route_revision: self.route_revision,
                    cancelled,
                }
            }
        };
        let observed = match command.action {
            SessionAction::OpenAt { now, .. } | SessionAction::Expire { now, .. } => Some(now),
            SessionAction::Detach { observed_at, .. } => Some(observed_at),
            _ => None,
        };
        if let (Some(now), Some(anchor)) = (observed, self.session_clock.as_mut())
            && now > anchor.logical
        {
            let delta = now.checked_sub(anchor.logical).ok_or(failed)?;
            anchor.reading.tick = anchor.reading.tick.checked_add(delta).ok_or(failed)?;
            anchor.logical = now;
        }
        self.charge(Accounts::RECEIPT)?;
        Ok(CommitFacts::SessionLifecycle(change))
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
        let older = move |operation: &OperationId| {
            Some(operation.epoch()) != open
                && !(operation.epoch().get() != 0 && operation.epoch() <= self.collected_through)
                && !self
                    .receipt_frontiers
                    .get(&ReceiptOrigin::of(*operation))
                    .is_some_and(|closure| operation.sequence() <= closure.through)
        };
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
            .chain(
                self.receipt_frontiers
                    .values()
                    .copied()
                    .map(FactRef::ReceiptFrontier),
            )
            .chain(operations)
            .chain(fences)
    }
}

/// Bounded target-only cancellation, shared by explicit administration and
/// receipted lifecycle commands. A staged caller rolls back every partial error.
fn cancel_session(
    state: &mut DurableState,
    session: SessionId,
) -> Result<SessionCancellations, AdminRefused> {
    let broken = AdminRefused::Inconsistent;
    for intent in state.wills.values_mut().filter(|i| i.session == session) {
        intent.state = mqttiumd_contract::will::WillState::Pending;
    }
    let row = state
        .sessions
        .get(&session)
        .filter(|r| r.state == SessionState::Active)
        .ok_or(AdminRefused::UnknownSession)?;
    row.revision
        .checked_next()
        .map_err(|_| AdminRefused::Exhausted)?;
    let markers = state
        .inbound
        .keys()
        .filter(|(owner, _)| *owner == session)
        .count();
    let deliveries = state
        .obligations
        .values()
        .filter(|r| r.target == session)
        .count();
    let routes = state
        .routes
        .values()
        .flat_map(|targets| targets.iter())
        .filter(|t| t.session == session)
        .count();
    let total = markers
        .checked_add(deliveries)
        .and_then(|n| n.checked_add(routes))
        .ok_or(broken)?;
    if total > MAX_RECOVERY_INVENTORY_ROWS
        || u64::try_from(markers).map_err(|_| broken)? > state.limits.exchange_slots.get()
        || u64::try_from(deliveries).map_err(|_| broken)? > state.limits.references.get()
    {
        return Err(broken);
    }
    let mut marker_keys = Vec::new();
    marker_keys
        .try_reserve_exact(markers)
        .map_err(|_| AdminRefused::WriteFailed)?;
    marker_keys.extend(
        state
            .inbound
            .keys()
            .filter(|(owner, _)| *owner == session)
            .copied(),
    );
    let mut delivery_keys = Vec::new();
    delivery_keys
        .try_reserve_exact(deliveries)
        .map_err(|_| AdminRefused::WriteFailed)?;
    delivery_keys.extend(
        state
            .obligations
            .iter()
            .filter(|(_, r)| r.target == session)
            .map(|(id, _)| *id),
    );
    for key in marker_keys {
        let marker = state.inbound.remove(&key).ok_or(broken)?;
        state.accounts.exchange_slots = state
            .accounts
            .exchange_slots
            .checked_sub(ExchangeSlots::new(1))
            .ok_or(broken)?;
        state.discard_completed_publication(marker.publication);
    }
    for delivery in delivery_keys {
        state.end_obligation(delivery)?;
    }
    for targets in state.routes.values_mut() {
        targets.retain(|target| target.session != session);
    }
    state.routes.retain(|_, targets| !targets.is_empty());
    if routes != 0 {
        state.route_revision = state
            .route_revision
            .checked_next()
            .map_err(|_| AdminRefused::Exhausted)?;
    }
    state.sessions.remove(&session).ok_or(broken)?;
    Ok(SessionCancellations {
        inbound: u32::try_from(markers).map_err(|_| broken)?,
        deliveries: u32::try_from(deliveries).map_err(|_| broken)?,
        routes: u32::try_from(routes).map_err(|_| broken)?,
    })
}

fn check_message_candidate(state: &DurableState, id: PublicationId) -> Result<(), ReadError> {
    let publication = state.publications.get(&id).ok_or(ReadError::InvalidData)?;
    if let Some(expiry) = publication.expiry {
        if expiry.observed.domain == [0; 32] {
            return Err(ReadError::InvalidData);
        }
        expiry.deadline().map_err(|_| ReadError::InvalidData)?;
    }
    if publication.topic.is_empty()
        || publication.topic.len() > state.limits.max_topic_bytes
        || publication.topic.contains(&0)
        || publication.topic.iter().any(|b| matches!(b, b'+' | b'#'))
        || std::str::from_utf8(&publication.topic).is_err()
    {
        return Err(ReadError::InvalidData);
    }
    let range = DeliveryId {
        publication: id,
        index: 0,
    }..=DeliveryId {
        publication: id,
        index: u32::MAX,
    };
    let mut references = 0u64;
    for (delivery, record) in state.obligations.range(range.clone()) {
        references = references.checked_add(1).ok_or(ReadError::LimitExceeded)?;
        if references > MESSAGE_EXPIRY_SWEEP_DELIVERIES as u64 {
            return Err(ReadError::LimitExceeded);
        }
        check_delivery_metadata(state, *delivery, record)?;
        delivery_claim(state, *delivery, record)?;
    }
    for (delivery, _) in state.outbound.range(range) {
        if !state.obligations.contains_key(delivery) {
            return Err(ReadError::InvalidData);
        }
    }
    if state.retained.len() > mqttiumd_contract::retained::MAX_RETAINED_TOPICS {
        return Err(ReadError::LimitExceeded);
    }
    let mut retained = false;
    for (topic, value) in &state.retained {
        if value.publication != id {
            continue;
        }
        if retained
            || !publication.retain
            || publication.retained_version.is_some()
            || *topic != publication.topic
            || value.producer_client_id.is_empty()
            || value.producer_client_id.len() > MAX_CLIENT_ID_BYTES
            || value.producer_client_id.contains(&0)
            || std::str::from_utf8(&value.producer_client_id).is_err()
        {
            return Err(ReadError::InvalidData);
        }
        retained = true;
        references = references.checked_add(1).ok_or(ReadError::LimitExceeded)?;
    }
    if references > 0 {
        let payload = state
            .payloads
            .get(&publication.payload)
            .ok_or(ReadError::InvalidData)?;
        if payload.references < references
            || payload.references > state.limits.references.get()
            || payload.bytes.len() > state.limits.max_payload_bytes
            || retained && payload.bytes.is_empty()
        {
            return Err(ReadError::InvalidData);
        }
    }
    Ok(())
}

fn check_message_observation(state: &DurableState, now: MessageTime) -> Result<(), ReadError> {
    let anchor = state.session_clock.ok_or(ReadError::InvalidData)?;
    if now.domain == [0; 32] || now.domain != anchor.reading.domain {
        return Err(ReadError::InvalidData);
    }
    Ok(())
}

impl DurableStore for MemoryStore {
    fn read_wills(
        &self,
        after: Option<OperationId>,
        limit: usize,
        budget: ReadBudget,
    ) -> Result<WillPage, ReadError> {
        will_memory::page(&self.committed, after, limit, budget)
    }

    fn expire_messages(
        &mut self,
        now: MessageTime,
        after: Option<PublicationId>,
        limit: u32,
    ) -> Result<MessageExpirySweep, ReadError> {
        if limit == 0 || limit > MESSAGE_EXPIRY_SWEEP_PUBLICATIONS {
            return Err(ReadError::LimitExceeded);
        }
        check_message_observation(&self.committed, now)?;
        let mut result = MessageExpirySweep {
            removed: BoundedVec::with_capacity(MESSAGE_EXPIRY_SWEEP_DELIVERIES)
                .map_err(|_| ReadError::AllocationFailed)?,
            ..MessageExpirySweep::default()
        };
        let mut candidates = Vec::new();
        candidates
            .try_reserve_exact(limit as usize)
            .map_err(|_| ReadError::AllocationFailed)?;
        for (id, publication) in self
            .committed
            .publications
            .range((
                after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
                std::ops::Bound::Unbounded,
            ))
            .take(limit as usize)
        {
            check_message_candidate(&self.committed, *id)?;
            candidates.push((*id, publication.expiry));
        }
        result.more = candidates.last().is_some_and(|(last, _)| {
            self.committed
                .publications
                .range((std::ops::Bound::Excluded(*last), std::ops::Bound::Unbounded))
                .next()
                .is_some()
        });
        result.next = result
            .more
            .then(|| candidates.last().map(|(id, _)| *id))
            .flatten();
        let mut staged = self.committed.clone();
        for (id, expiry) in candidates {
            let Some(expiry) = expiry else {
                continue;
            };
            // This request may be a retry of an earlier frozen observation.
            // Later admissions are outside its as-of range, not clock regressions.
            if expiry.observed.domain == now.domain && expiry.observed.tick > now.tick {
                continue;
            }
            match expiry.state(now).map_err(|_| ReadError::InvalidData)? {
                ExpiryState::Fresh { .. } => (),
                ExpiryState::TemporalUncertain => {
                    result.quarantined += 1;
                }
                ExpiryState::Expired => {
                    let uncertain = staged
                        .outbound
                        .range(
                            DeliveryId {
                                publication: id,
                                index: 0,
                            }..=DeliveryId {
                                publication: id,
                                index: u32::MAX,
                            },
                        )
                        .filter(|(_, record)| !record.started)
                        .count();
                    result.quarantined = result
                        .quarantined
                        .checked_add(
                            u32::try_from(uncertain).map_err(|_| ReadError::LimitExceeded)?,
                        )
                        .ok_or(ReadError::LimitExceeded)?;
                    if let Some(topic) = staged
                        .retained
                        .iter()
                        .find_map(|(topic, r)| (r.publication == id).then(|| topic.clone()))
                    {
                        retained_memory::release(&mut staged, &topic, id)
                            .map_err(|_| ReadError::InvalidData)?;
                        result.retained_removed += 1;
                    }
                    let mut deliveries = BoundedVec::with_capacity(MESSAGE_EXPIRY_SWEEP_DELIVERIES)
                        .map_err(|_| ReadError::AllocationFailed)?;
                    for (delivery, record) in staged.obligations.range(
                        DeliveryId {
                            publication: id,
                            index: 0,
                        }..=DeliveryId {
                            publication: id,
                            index: u32::MAX,
                        },
                    ) {
                        if !staged.outbound.contains_key(delivery) {
                            deliveries
                                .try_push((*delivery, record.target))
                                .map_err(|_| ReadError::LimitExceeded)?;
                        }
                    }
                    for &(delivery, target) in deliveries.as_slice() {
                        result
                            .removed
                            .try_push(ExpiredDelivery { delivery, target })
                            .map_err(|_| ReadError::LimitExceeded)?;
                        staged
                            .end_obligation(delivery)
                            .map_err(|_| ReadError::InvalidData)?;
                    }
                }
            }
        }
        if result.removed.is_empty() && result.retained_removed == 0 {
            return Ok(result);
        }
        if self.commit(staged) {
            Ok(result)
        } else {
            Err(ReadError::Unavailable)
        }
    }

    fn checkpoint_session_clock(
        &mut self,
        expected: Option<SessionClockAnchor>,
        reading: SessionClockReading,
    ) -> Result<SessionClockAnchor, ReadError> {
        let current = self.committed.session_clock;
        if current != expected {
            return current.ok_or(ReadError::InvalidData);
        }
        let next = SessionClockAnchor::advance(current, reading)?;
        let mut staged = self.committed.clone();
        staged.session_clock = Some(next);
        if !self.commit(staged) {
            return Err(ReadError::Unavailable);
        }
        Ok(next)
    }

    fn collect_receipts(
        &mut self,
        collection: ReceiptCollection,
    ) -> Result<ReceiptCollectionReply, ReadError> {
        self.collect_receipts_inner(collection)
    }

    fn receipt_frontier(&self, origin: ReceiptOrigin) -> Result<Option<ReceiptClosure>, ReadError> {
        if origin.incarnation != self.committed.ledger.incarnation() {
            return Err(ReadError::InvalidData);
        }
        Ok(self.committed.receipt_frontiers.get(&origin).copied())
    }

    fn collect_closed_receipts(
        &mut self,
        collection: ClosedReceiptCollection,
    ) -> Result<ClosedReceiptReply, ReadError> {
        self.collect_closed_receipts_inner(collection)
    }

    fn ledger(&self) -> Result<EpochLedger, ReadError> {
        Ok(self.committed.ledger)
    }

    fn open_epoch(&mut self, command: &OpenEpoch) -> Result<OpeningOutcome, ReadError> {
        // The decision is the contract's: the same function decides for
        // every store. Only an opening writes; the command that opened
        // the last epoch, presented again, reads its own result.
        if let LedgerOpening::Opens { ledger } = self.committed.ledger.open(command) {
            check_inventory_bound(&self.committed)?;
            let mut staged = self.committed.clone();
            staged.ledger = ledger;
            for intent in staged.wills.values_mut() {
                intent.state = mqttiumd_contract::will::WillState::Pending;
            }
            // Previous transports are gone. Already-detached deadlines never
            // restart; attached finite sessions get the first reliable startup
            // observation exactly once in this new-epoch transaction.
            if staged.sessions.len() > staged.limits.max_sessions
                || staged.sessions.len() > MAX_RECOVERY_INVENTORY_ROWS
            {
                return Err(ReadError::LimitExceeded);
            }
            let now = staged.session_clock.map(|anchor| anchor.logical);
            let mut expired = Vec::new();
            expired
                .try_reserve_exact(staged.sessions.len())
                .map_err(|_| ReadError::AllocationFailed)?;
            for (id, row) in &mut staged.sessions {
                match row.expiry {
                    SessionExpiry::OnDisconnect => expired.push(*id),
                    SessionExpiry::Never => {}
                    SessionExpiry::After(_) => {
                        let now = now.ok_or(ReadError::InvalidData)?;
                        if let Some(detached) = row.detached {
                            if detached.deadline.is_some_and(|deadline| deadline <= now) {
                                expired.push(*id)
                            }
                        } else {
                            row.detached = Some(SessionDetachment {
                                operation: row.opened_by.ok_or(ReadError::InvalidData)?,
                                deadline: Some(
                                    row.expiry
                                        .deadline(now)
                                        .flatten()
                                        .ok_or(ReadError::InvalidData)?,
                                ),
                            });
                        }
                    }
                }
            }
            for session in expired {
                cancel_session(&mut staged, session).map_err(|_| ReadError::InvalidData)?;
            }
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
        if matches!(
            self.committed.established(operation),
            Established::Outcome(StoreOutcome::RetiredOutcomeUnavailable)
        ) {
            return settled(StoreOutcome::RetiredOutcomeUnavailable);
        }
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
                (recipe, Some(bytes), Decision::Committed(_))
                    if retained_memory::bounds(recipe)
                        .ok()
                        .flatten()
                        .is_some_and(|b| b.payload.is_some()) =>
                {
                    self.committed
                        .payloads
                        .get(
                            &retained_memory::bounds(recipe)
                                .ok()
                                .flatten()
                                .and_then(|b| b.payload)
                                .expect("guarded publication payload"),
                        )
                        .is_none_or(|stored| stored.bytes == bytes.as_slice())
                }
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
        if !self.receipt_capacity(operation, Some(ReceiptClass::of(recipe))) {
            return settled(StoreOutcome::PendingOrUnknown);
        }
        if !matches!(
            will_memory::receipt_capacity(&self.committed, &canonical),
            Ok(true)
        ) {
            return settled(StoreOutcome::PendingOrUnknown);
        }
        if phase != EpochPhase::Open {
            // Its epoch is not open yet. It must not become executable
            // when that epoch opens: the rejection is recorded.
            return rejected(self.commit_rejection(
                operation,
                canonical,
                RejectReason::PreconditionFailed,
            ));
        }

        {
            match retained_memory::identity_was_used(&self.committed, recipe) {
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
            Recipe::AnnotatedPublication(annotated) => {
                let props = Some(&annotated.properties);
                match annotated.inner.as_ref() {
                    Recipe::AdmitPublication(admit) => {
                        let mut a = Admission::from(admit);
                        a.properties = props;
                        staged.stage_publication(a, None, command.payload.as_ref())
                    }
                    Recipe::ClaimedPublication(claimed) => {
                        let mut a = Admission::from(&claimed.admission);
                        a.properties = props;
                        staged.stage_publication_with_claim(
                            a,
                            None,
                            command.payload.as_ref(),
                            Some(claimed.claim),
                        )
                    }
                    Recipe::RetainedPublication(value) => {
                        let mut a = Admission::from(&value.admission);
                        a.properties = props;
                        staged.stage_publication(
                            a,
                            Some(&value.retain_targets),
                            command.payload.as_ref(),
                        )
                    }
                    Recipe::RetainedQos0(value) => {
                        let mut a = Admission::from(value);
                        a.properties = props;
                        staged.stage_publication(
                            a,
                            Some(&value.retain_targets),
                            command.payload.as_ref(),
                        )
                    }
                    Recipe::ExpiringPublication(value) => {
                        let (mut admit, mask) = match &value.admission {
                            ExpiringAdmission::Qos12(v) => (
                                Admission::from(&v.admission),
                                v.admission.retain.then_some(&v.retain_targets),
                            ),
                            ExpiringAdmission::Qos0(v) => {
                                (Admission::from(v), Some(&v.retain_targets))
                            }
                        };
                        admit.expiry = Some(value.expiry);
                        admit.properties = props;
                        staged.stage_publication(admit, mask, command.payload.as_ref())
                    }
                    _ => Err(RejectReason::PreconditionFailed),
                }
            }
            Recipe::WillPublication(value) => {
                will_memory::publish(&mut staged, value, command.payload.as_ref())
            }
            Recipe::AdmitPublication(admit) => {
                staged.stage_admission(admit, command.payload.as_ref())
            }
            Recipe::ClaimedPublication(claimed) => staged.stage_publication_with_claim(
                Admission::from(&claimed.admission),
                None,
                command.payload.as_ref(),
                Some(claimed.claim),
            ),
            Recipe::RetainedPublication(value) => staged.stage_publication(
                Admission::from(&value.admission),
                Some(&value.retain_targets),
                command.payload.as_ref(),
            ),
            Recipe::RetainedQos0(value) => staged.stage_publication(
                Admission::from(value),
                Some(&value.retain_targets),
                command.payload.as_ref(),
            ),
            Recipe::ReplaySubscriptions(value) => {
                retained_memory::subscribe(&mut staged, value, None)
            }
            Recipe::TimedReplaySubscriptions(value) => {
                retained_memory::subscribe(&mut staged, &value.replay, Some(value.observed))
            }
            Recipe::ExpiringPublication(value) => {
                let (mut admit, mask) = match &value.admission {
                    ExpiringAdmission::Qos12(v) => (
                        Admission::from(&v.admission),
                        v.admission.retain.then_some(&v.retain_targets),
                    ),
                    ExpiringAdmission::Qos0(v) => (Admission::from(v), Some(&v.retain_targets)),
                };
                admit.expiry = Some(value.expiry);
                staged.stage_publication(admit, mask, command.payload.as_ref())
            }
            Recipe::MarkDeliveryStarted(command) => staged.stage_started(command),
            Recipe::ExpireDelivery(command) => staged.stage_expired(command),
            Recipe::ReleaseInbound(release) => staged.stage_release(release),
            Recipe::ChangeSubscriptions(command) => staged.stage_subscriptions(command),
            Recipe::ClaimDelivery(command) => staged.stage_claim(command),
            Recipe::FinishDelivery(command) => staged.stage_finish(command),
            Recipe::PreparePubrel(command) => staged.stage_pubrel(command),
            Recipe::ConsumeDelivery(command) => staged.stage_consume(command),
            Recipe::UpdateSubscriptions(command) => staged.stage_updates(command),
            Recipe::SessionLifecycle(command) => staged.stage_session(command),
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
                if !self.receipt_capacity(operation, None) {
                    return settled(StoreOutcome::PendingOrUnknown);
                }
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
        for (delivery, record) in &state.obligations {
            if record.target == session {
                check_delivery_metadata(state, *delivery, record)?;
            }
        }
        let mut meter = ReadMeter::new(budget);
        let mut items =
            BoundedVec::with_capacity(limit).map_err(|_| ReadError::AllocationFailed)?;
        let mut more = false;
        for (delivery, record) in &state.obligations {
            if after.is_some_and(|after| *delivery <= after) || record.target != session {
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
            let props_bytes = publication.properties.as_ref().map_or(0, |p| p.wire_len());
            if props_bytes > 0 {
                meter.field(
                    props_bytes,
                    mqttiumd_contract::properties::MAX_PROPERTY_BYTES + 1024,
                )?;
            }
            meter.charge(
                256usize
                    .checked_add(publication.topic.len())
                    .and_then(|n| n.checked_add(payload.bytes.len()))
                    .and_then(|n| n.checked_add(props_bytes))
                    .ok_or(ReadError::LimitExceeded)?,
            )?;
            check_delivery_metadata(state, *delivery, record)?;
            let claim = delivery_claim(state, *delivery, record)?;
            items
                .try_push(WorkItem {
                    delivery: *delivery,
                    target: record.target,
                    target_revision: record.target_revision,
                    qos: record.qos,
                    cause: record.cause,
                    retain: record.retain,
                    expiry: publication.expiry,
                    properties: publication.properties.clone(),
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

impl MemoryStore {
    fn receipt_capacity(&self, operation: OperationId, class: Option<ReceiptClass>) -> bool {
        let state = &self.committed;
        if let Some(closure) = state.receipt_frontiers.get(&ReceiptOrigin::of(operation)) {
            let span = operation
                .sequence()
                .get()
                .checked_sub(closure.through.get());
            let window = if class == Some(ReceiptClass::Business) {
                RECEIPT_DATA_WINDOW
            } else {
                RECEIPT_WINDOW
            };
            if span.is_none_or(|span| span > window as u64) {
                return false;
            }
        }
        class != Some(ReceiptClass::Business)
            || state.receipt_frontiers.is_empty()
            || state.accounts.receipt_slots.get()
                < state
                    .limits
                    .receipt_slots
                    .get()
                    .saturating_sub(RECEIPT_CONTROL_RESERVE)
    }

    fn initial_receipt_origin_is_compatible(
        &self,
        origin: ReceiptOrigin,
    ) -> Result<bool, ReadError> {
        let id = |seq| {
            OperationId::new(
                origin.incarnation,
                origin.epoch,
                origin.owner,
                OperationSeq::new(seq),
            )
        };
        let zero_pub = PublicationId::new(
            origin.incarnation,
            origin.epoch,
            origin.owner,
            PublicationSeq::new(0),
        );
        let zero_payload = PayloadId::new(
            origin.incarnation,
            origin.epoch,
            origin.owner,
            PayloadSeq::new(0),
        );
        if self.committed.operations.contains_key(&id(0))
            || self.committed.fences.contains(&id(0))
            || self.committed.publications.contains_key(&zero_pub)
            || self.committed.payloads.contains_key(&zero_payload)
        {
            return Ok(false);
        }
        let mut meter = ReadMeter::new(ReadBudget::SNAPSHOT);
        for (operation, record) in self.committed.operations.range(id(0)..=id(u64::MAX)) {
            meter.field(record.recipe.len(), ReadBudget::SNAPSHOT.max_field_bytes)?;
            meter.charge(
                128usize
                    .checked_add(record.recipe.len())
                    .ok_or(ReadError::LimitExceeded)?,
            )?;
            if let Some(admit) =
                retained_memory::receipt_bounds(&self.committed, *operation, record)?
                && (admit.first.sequence().get() == 0
                    || admit.payload.is_some_and(|p| p.sequence().get() == 0))
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn collect_receipts_inner(
        &mut self,
        collection: ReceiptCollection,
    ) -> Result<ReceiptCollectionReply, ReadError> {
        let refused = |why| Ok(ReceiptCollectionReply::Refused(why));
        let request = collection.closure;
        let origin = request.origin;
        if collection.limit == 0 || collection.limit > MAX_COLLECTION_ROWS {
            return refused(ReceiptCollectionRefusal::LimitExceeded);
        }
        if origin.incarnation != self.committed.ledger.incarnation() {
            return refused(ReceiptCollectionRefusal::OriginMismatch);
        }
        if origin.epoch > self.committed.ledger.last_opened() {
            return refused(ReceiptCollectionRefusal::FutureEpoch);
        }
        if origin.epoch.get() == 0 || origin.epoch <= self.committed.collected_through {
            return refused(ReceiptCollectionRefusal::InvalidFrontier);
        }
        let stored = self.committed.receipt_frontiers.get(&origin).copied();
        if stored.is_none() {
            if self.committed.receipt_frontiers.len() >= MAX_RECEIPT_ORIGINS {
                return refused(ReceiptCollectionRefusal::OriginCapacity);
            }
            if !self.initial_receipt_origin_is_compatible(origin)? {
                return refused(ReceiptCollectionRefusal::InvalidFrontier);
            }
        }
        let prior = stored.unwrap_or(ReceiptClosure::empty(origin));
        let closure = if request.through <= prior.through {
            if request.publication_through > prior.publication_through
                || request.payload_through > prior.payload_through
            {
                return refused(ReceiptCollectionRefusal::InvalidFrontier);
            }
            prior
        } else {
            let span = request
                .through
                .get()
                .checked_sub(prior.through.get())
                .ok_or(ReadError::InvalidData)?;
            if span > u64::from(MAX_COLLECTION_ROWS) {
                return refused(ReceiptCollectionRefusal::LimitExceeded);
            }
            if request.publication_through < prior.publication_through
                || request.payload_through < prior.payload_through
            {
                return refused(ReceiptCollectionRefusal::InvalidFrontier);
            }
            let mut meter = ReadMeter::new(ReadBudget::COLLECTION);
            for seq in prior.through.get() + 1..=request.through.get() {
                let operation = OperationId::new(
                    origin.incarnation,
                    origin.epoch,
                    origin.owner,
                    OperationSeq::new(seq),
                );
                let record = self.committed.operations.get(&operation);
                let fence = self.committed.fences.contains(&operation);
                if record.is_some() && fence {
                    return Err(ReadError::InvalidData);
                }
                let Some(record) = record else {
                    if !fence {
                        return refused(ReceiptCollectionRefusal::Gap);
                    }
                    meter.charge(32)?;
                    continue;
                };
                meter.field(record.recipe.len(), ReadBudget::COLLECTION.max_field_bytes)?;
                meter.charge(
                    128usize
                        .checked_add(record.recipe.len())
                        .ok_or(ReadError::LimitExceeded)?,
                )?;
                if let Some(admit) =
                    retained_memory::receipt_bounds(&self.committed, operation, record)?
                    && (admit.last.sequence() > request.publication_through
                        || admit
                            .payload
                            .is_some_and(|p| p.sequence() > request.payload_through))
                {
                    return refused(ReceiptCollectionRefusal::InvalidFrontier);
                }
            }
            request
        };
        let first = OperationId::new(
            origin.incarnation,
            origin.epoch,
            origin.owner,
            OperationSeq::new(0),
        );
        let last = OperationId::new(
            origin.incarnation,
            origin.epoch,
            origin.owner,
            closure.through,
        );
        let mut keys = Vec::new();
        let ceiling = usize::from(collection.limit) + 1;
        keys.try_reserve_exact(ceiling * 2)
            .map_err(|_| ReadError::AllocationFailed)?;
        keys.extend(
            self.committed
                .operations
                .range(first..=last)
                .take(ceiling)
                .map(|(id, _)| (*id, false)),
        );
        keys.extend(
            self.committed
                .fences
                .range(first..=last)
                .take(ceiling)
                .map(|id| (*id, true)),
        );
        keys.sort_unstable();
        if keys.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return Err(ReadError::InvalidData);
        }
        let more = keys.len() > usize::from(collection.limit);
        keys.truncate(usize::from(collection.limit));
        let removed = u16::try_from(keys.len()).map_err(|_| ReadError::LimitExceeded)?;
        let retained = self
            .committed
            .accounts
            .receipt_slots
            .get()
            .checked_sub(u64::from(removed))
            .ok_or(ReadError::InvalidData)?;
        if stored == Some(closure) && removed == 0 {
            return Ok(ReceiptCollectionReply::Applied {
                closure,
                removed,
                retained,
                more,
            });
        }
        // Prepare only compact metadata (at most 64 origins), not a copy of
        // every live business object. No fallible work follows the mutation.
        let mut frontiers = self.committed.receipt_frontiers.clone();
        frontiers.insert(origin, closure);
        let Some(revision) = self.committed.revision.checked_add(1) else {
            return refused(ReceiptCollectionRefusal::Exhausted);
        };
        if self.durability == Durability::Failing {
            return Ok(ReceiptCollectionReply::PendingOrUnknown);
        }
        self.committed.receipt_frontiers = frontiers;
        for (operation, fence) in keys {
            if fence {
                self.committed.fences.remove(&operation);
            } else {
                self.committed.operations.remove(&operation);
            }
        }
        self.committed.accounts.receipt_slots = ReceiptSlots::new(retained);
        self.committed.revision = revision;
        Ok(ReceiptCollectionReply::Applied {
            closure,
            removed,
            retained,
            more,
        })
    }

    fn collect_closed_receipts_inner(
        &mut self,
        collection: ClosedReceiptCollection,
    ) -> Result<ClosedReceiptReply, ReadError> {
        let refused = |why| Ok(ClosedReceiptReply::Refused(why));
        if collection.limit == 0 || collection.limit > MAX_COLLECTION_ROWS {
            return refused(ReceiptCollectionRefusal::LimitExceeded);
        }
        if self.committed.ledger.phase_of(collection.through) != EpochPhase::Closed {
            return refused(ReceiptCollectionRefusal::FutureEpoch);
        }
        let through = collection.through.max(self.committed.collected_through);
        let incarnation = self.committed.ledger.incarnation();
        let start = OperationId::new(
            incarnation,
            RunEpoch::new(1),
            LogicalOwnerId::new(0),
            OperationSeq::new(0),
        );
        let end = OperationId::new(
            incarnation,
            through,
            LogicalOwnerId::new(u64::MAX),
            OperationSeq::new(u64::MAX),
        );
        let mut keys = Vec::new();
        let ceiling = usize::from(collection.limit) + 1;
        keys.try_reserve_exact(ceiling * 2)
            .map_err(|_| ReadError::AllocationFailed)?;
        keys.extend(
            self.committed
                .operations
                .range(start..=end)
                .take(ceiling)
                .map(|(id, _)| (*id, false)),
        );
        keys.extend(
            self.committed
                .fences
                .range(start..=end)
                .take(ceiling)
                .map(|id| (*id, true)),
        );
        keys.sort_unstable();
        if keys.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return Err(ReadError::InvalidData);
        }
        let origin_start = ReceiptOrigin {
            incarnation,
            epoch: RunEpoch::new(1),
            owner: LogicalOwnerId::new(0),
        };
        let origin_end = ReceiptOrigin {
            incarnation,
            epoch: through,
            owner: LogicalOwnerId::new(u64::MAX),
        };
        let mut origins = Vec::new();
        origins
            .try_reserve_exact(ceiling)
            .map_err(|_| ReadError::AllocationFailed)?;
        origins.extend(
            self.committed
                .receipt_frontiers
                .range(origin_start..=origin_end)
                .take(ceiling)
                .map(|(origin, _)| *origin),
        );
        let more = keys
            .len()
            .checked_add(origins.len())
            .ok_or(ReadError::LimitExceeded)?
            > usize::from(collection.limit);
        keys.truncate(usize::from(collection.limit));
        origins.truncate(usize::from(collection.limit) - keys.len());
        let removed = u16::try_from(keys.len()).map_err(|_| ReadError::LimitExceeded)?;
        let removed_origins = u16::try_from(origins.len()).map_err(|_| ReadError::LimitExceeded)?;
        let retained = self
            .committed
            .accounts
            .receipt_slots
            .get()
            .checked_sub(u64::from(removed))
            .ok_or(ReadError::InvalidData)?;
        if through == self.committed.collected_through && removed == 0 && removed_origins == 0 {
            return Ok(ClosedReceiptReply::Applied {
                through,
                removed,
                removed_origins,
                retained,
                more,
            });
        }
        let Some(revision) = self.committed.revision.checked_add(1) else {
            return refused(ReceiptCollectionRefusal::Exhausted);
        };
        if self.durability == Durability::Failing {
            return Ok(ClosedReceiptReply::PendingOrUnknown);
        }
        self.committed.collected_through = through;
        for (id, fence) in keys {
            if fence {
                self.committed.fences.remove(&id);
            } else {
                self.committed.operations.remove(&id);
            }
        }
        for origin in origins {
            self.committed.receipt_frontiers.remove(&origin);
        }
        self.committed.accounts.receipt_slots = ReceiptSlots::new(retained);
        self.committed.revision = revision;
        Ok(ClosedReceiptReply::Applied {
            through,
            removed,
            removed_origins,
            retained,
            more,
        })
    }
}
