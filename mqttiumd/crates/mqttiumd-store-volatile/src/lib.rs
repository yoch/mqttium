//! High-throughput indexed in-memory store (`VolatileStore`, ADR 0052) with
//! optional periodic atomic Schema-14 / Schema-15 SQLite snapshots.
//!
//! Preserves 100% of the `DurableStore` contract (including exact guards,
//! accounting, receipt retirement, retained replay, message expiry, and Will
//! semantics) while eliminating per-command disk fsyncs and full-state clones.
//! Every write command executes check-then-mutate atomically in O(1)/O(log N)
//! time using maintained secondary indices and zero-copy `SharedBytes` blobs.

#![forbid(unsafe_code)]

pub mod log;
mod read;
mod snapshot;
mod witness;

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

use mqttiumd_contract::capacity::{
    BoundedVec, ExchangeSlots, PayloadBytes, ReceiptSlots, References, Resource,
};
use mqttiumd_contract::command::{
    AdmitPublication, ChangeSubscriptions, ClaimDelivery, Command, ConsumeDelivery, DeliveryCause,
    EpochLedger, EpochPhase, FinishDelivery, InlineClaim, LedgerOpening, OpenEpoch, Recipe,
    ReleaseInbound, UpdateSubscriptions,
};
use mqttiumd_contract::effects::InboundPhase;
use mqttiumd_contract::flat_map::{FlatSet, ScopedFlatMap};
use mqttiumd_contract::ids::{
    DeliveryId, InboundPacketId, LogicalOwnerId, OperationId, OperationSeq, OutboundPacketId,
    PayloadId, PayloadSeq, PublicationId, PublicationSeq, Revision, RunEpoch, SessionId,
    StoreIncarnation,
};
use mqttiumd_contract::message_expiry::{
    ExpireDelivery, ExpiringAdmission, ExpiryState, MarkDeliveryStarted, MessageExpiry, MessageTime,
};
use mqttiumd_contract::outcome::{
    CommitFacts, OpeningOutcome, RejectReason, StoreOutcome, StoreReply, StoreViolation,
};
use mqttiumd_contract::packet::{Qos, SharedBytes};
use mqttiumd_contract::pubsub::{MAX_SUBSCRIPTIONS, SubscriptionOptions};
use mqttiumd_contract::receipts::{
    ClosedReceiptCollection, ClosedReceiptReply, MAX_COLLECTION_ROWS, MAX_RECEIPT_ORIGINS,
    RECEIPT_CONTROL_RESERVE, RECEIPT_DATA_WINDOW, RECEIPT_WINDOW, ReceiptClass, ReceiptClosure,
    ReceiptCollection, ReceiptCollectionRefusal, ReceiptCollectionReply, ReceiptOrigin,
};
use mqttiumd_contract::retained::{
    MAX_REPLAYS, MAX_RETAINED_BYTES, MAX_RETAINED_TOPICS, ReplaySubscriptions, RetainMask,
};
use mqttiumd_contract::scoped_map::{ScopedKey, ScopedMap};
use mqttiumd_contract::session::{
    SessionAction, SessionCancellations, SessionChange, SessionDetachment, SessionEndReason,
    SessionExpiry, SessionGuard, SessionLifecycle,
};
use mqttiumd_contract::will::{
    MAX_WILL_BYTES, MAX_WILL_INTENTS, WillIntent, WillPublication, WillState,
};
use mqttiumd_store_api::{
    Accounts, AdminCommand, AdminDone, AdminRefused, Applied, Decision, Durability, DurableState,
    DurableStore, ExpiredDelivery, InboundRecord, Limits, MAX_CLIENT_ID_BYTES,
    MAX_RECOVERY_INVENTORY_ROWS, MESSAGE_EXPIRY_SWEEP_DELIVERIES,
    MESSAGE_EXPIRY_SWEEP_PUBLICATIONS, MessageExpirySweep, ObligationPhase, ObligationRecord,
    OutboundRecord, ReadBudget, ReadError, RecoveryCursor, RecoveryPage, RecoveryRefused, Resolved,
    RetainedRecord, RouteTarget, SessionClockAnchor, SessionClockReading, SessionRecord,
    SessionState, WillPage, WorkPage,
};

pub use log::{LOG_MAX_GROUP, LogStore};
pub use mqttiumd_store_sqlite::{GroupRefused, MAX_GROUP, OpenError, Settings, StoragePolicy};
pub use snapshot::VolatileHandle;

use witness::{IdentityBounds, OriginWitness};

const UNKNOWN: Applied = Applied {
    reply: StoreReply::Outcome(StoreOutcome::PendingOrUnknown),
    committed_now: false,
    wrote: false,
};

#[derive(Clone, Debug)]
pub(crate) struct VPublication {
    pub expiry: Option<MessageExpiry>,
    pub properties: Option<std::sync::Arc<mqttiumd_contract::properties::PublishProperties>>,
    pub retained_version: Option<PublicationId>,
    pub operation: OperationId,
    pub source: SessionId,
    pub topic: SharedBytes,
    pub qos: Qos,
    pub retain: bool,
    pub payload: PayloadId,
}

#[derive(Clone, Debug)]
pub(crate) struct VPayload {
    pub bytes: SharedBytes,
    pub references: u64,
}

/// Zero-copy receipt stored in `VolatileStore::operations` (`Arc<[u8]>` shared
/// directly with `FrozenRecipe`) with optional precomputed `IdentityBounds`
/// so `collect_receipts` skips canonical decoding on locally committed rows.
#[derive(Clone, Debug)]
pub(crate) struct VOperation {
    pub recipe: std::sync::Arc<[u8]>,
    pub decision: Decision,
    pub cached_bounds: Option<Option<IdentityBounds>>,
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
    inbound: Option<(InboundPacketId, mqttiumd_contract::ids::ExchangeId)>,
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

enum Established {
    Outcome(StoreOutcome),
    Absent,
}

#[derive(Clone, Copy)]
enum CommitErr {
    Refused(RejectReason),
    NotWritten,
}

type Step<T> = Result<T, CommitErr>;

fn check_charge(used: Accounts, limits: Limits, costs: Accounts) -> Result<Accounts, RejectReason> {
    let payload_bytes = used
        .payload_bytes
        .checked_add(costs.payload_bytes)
        .filter(|total| *total <= limits.payload_bytes)
        .ok_or(RejectReason::CapacityExceeded(Resource::PayloadBytes))?;
    let references = used
        .references
        .checked_add(costs.references)
        .filter(|total| *total <= limits.references)
        .ok_or(RejectReason::CapacityExceeded(Resource::References))?;
    let exchange_slots = used
        .exchange_slots
        .checked_add(costs.exchange_slots)
        .filter(|total| *total <= limits.exchange_slots)
        .ok_or(RejectReason::CapacityExceeded(Resource::ExchangeSlots))?;
    let receipt_slots = used
        .receipt_slots
        .checked_add(costs.receipt_slots)
        .filter(|total| *total <= limits.receipt_slots)
        .ok_or(RejectReason::CapacityExceeded(Resource::ReceiptSlots))?;
    Ok(Accounts {
        payload_bytes,
        references,
        exchange_slots,
        receipt_slots,
    })
}

/// Indexed, zero-clone in-memory realisation of `DurableStore`.
#[derive(Debug)]
pub struct VolatileStore {
    pub(crate) ledger: EpochLedger,
    pub(crate) collected_through: RunEpoch,
    pub(crate) revision: u64,
    pub(crate) limits: Limits,
    pub(crate) accounts: Accounts,
    pub(crate) next_session: SessionId,
    pub(crate) session_clock: Option<SessionClockAnchor>,
    pub(crate) route_revision: Revision,
    pub(crate) sessions: BTreeMap<SessionId, SessionRecord>,
    pub(crate) routes: BTreeMap<Vec<u8>, Vec<RouteTarget>>,
    pub(crate) operations: ScopedMap<OperationId, VOperation>,
    pub(crate) fences: BTreeSet<OperationId>,
    pub(crate) receipt_frontiers: BTreeMap<ReceiptOrigin, ReceiptClosure>,
    pub(crate) retained: BTreeMap<Vec<u8>, RetainedRecord>,
    pub(crate) wills: BTreeMap<OperationId, WillIntent>,
    pub(crate) publications: BTreeMap<PublicationId, VPublication>,
    pub(crate) payloads: ScopedFlatMap<PayloadId, VPayload>,
    pub(crate) obligations: BTreeMap<DeliveryId, ObligationRecord>,
    pub(crate) inbound: BTreeMap<(SessionId, InboundPacketId), InboundRecord>,
    pub(crate) outbound: ScopedFlatMap<DeliveryId, OutboundRecord>,
    // Maintained secondary indices for O(1)/O(log N) operations:
    pub(crate) active_sessions: usize,
    pub(crate) active_by_client: BTreeMap<Vec<u8>, SessionId>,
    pub(crate) total_routes: usize,
    pub(crate) routes_by_session: BTreeMap<SessionId, BTreeSet<Vec<u8>>>,
    pub(crate) obligations_by_target: BTreeSet<(SessionId, DeliveryId)>,
    pub(crate) obligations_by_pub: ScopedFlatMap<PublicationId, u32>,
    pub(crate) inbound_by_pub: ScopedFlatMap<PublicationId, u32>,
    pub(crate) outbound_by_target_pi: FlatSet<(SessionId, OutboundPacketId)>,
    pub(crate) publications_by_payload: ScopedFlatMap<PayloadId, u32>,
    pub(crate) retained_bytes: u64,
    pub(crate) retained_by_pub: ScopedFlatMap<PublicationId, Vec<u8>>,
    pub(crate) wills_bytes: u64,
    pub(crate) wills_by_client: BTreeMap<Vec<u8>, usize>,
    pub(crate) witnesses: BTreeMap<ReceiptOrigin, OriginWitness>,
    pub(crate) open_will_receipt_bytes: usize,
    pub(crate) validated: bool,
    pub(crate) durability: Durability,
}

impl VolatileStore {
    /// Creates an empty volatile store. No epoch is open in it.
    #[must_use]
    pub fn create(incarnation: StoreIncarnation, limits: Limits) -> Self {
        Self {
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
            operations: ScopedMap::new(),
            fences: BTreeSet::new(),
            receipt_frontiers: BTreeMap::new(),
            retained: BTreeMap::new(),
            wills: BTreeMap::new(),
            publications: BTreeMap::new(),
            payloads: ScopedFlatMap::new(),
            obligations: BTreeMap::new(),
            inbound: BTreeMap::new(),
            outbound: ScopedFlatMap::new(),
            active_sessions: 0,
            active_by_client: BTreeMap::new(),
            total_routes: 0,
            routes_by_session: BTreeMap::new(),
            obligations_by_target: BTreeSet::new(),
            obligations_by_pub: ScopedFlatMap::new(),
            inbound_by_pub: ScopedFlatMap::new(),
            outbound_by_target_pi: FlatSet::new(),
            publications_by_payload: ScopedFlatMap::new(),
            retained_bytes: 0,
            retained_by_pub: ScopedFlatMap::new(),
            wills_bytes: 0,
            wills_by_client: BTreeMap::new(),
            witnesses: BTreeMap::new(),
            open_will_receipt_bytes: 0,
            validated: true,
            durability: Durability::Available,
        }
    }

    /// Reopens a store from a `DurableState` (such as a SQLite snapshot).
    #[must_use]
    pub fn reopen(state: DurableState) -> Self {
        let mut active_sessions = 0usize;
        let mut active_by_client = BTreeMap::new();
        let mut dup_active_client = false;
        for (&id, row) in &state.sessions {
            if row.state == SessionState::Active {
                active_sessions += 1;
                if active_by_client.insert(row.client_id.clone(), id).is_some() {
                    dup_active_client = true;
                }
            }
        }
        let mut total_routes = 0usize;
        let mut routes_by_session: BTreeMap<SessionId, BTreeSet<Vec<u8>>> = BTreeMap::new();
        for (topic, targets) in &state.routes {
            total_routes = total_routes.saturating_add(targets.len());
            for t in targets {
                routes_by_session
                    .entry(t.session)
                    .or_default()
                    .insert(topic.clone());
            }
        }
        let mut publications = BTreeMap::new();
        let mut publications_by_payload: ScopedFlatMap<PayloadId, u32> = ScopedFlatMap::new();
        for (id, row) in state.publications {
            let topic = SharedBytes::copy_from(&row.topic, row.topic.len())
                .expect("in-memory topic fits its own length");
            *publications_by_payload.get_or_insert_default(row.payload) += 1;
            publications.insert(
                id,
                VPublication {
                    expiry: row.expiry,
                    properties: row.properties,
                    retained_version: row.retained_version,
                    operation: row.operation,
                    source: row.source,
                    topic,
                    qos: row.qos,
                    retain: row.retain,
                    payload: row.payload,
                },
            );
        }
        let mut payloads = ScopedFlatMap::new();
        for (id, row) in state.payloads {
            let bytes = SharedBytes::copy_from(&row.bytes, row.bytes.len())
                .expect("in-memory payload fits its own length");
            payloads.insert(
                id,
                VPayload {
                    bytes,
                    references: row.references,
                },
            );
        }
        let mut obligations_by_target = BTreeSet::new();
        let mut obligations_by_pub: ScopedFlatMap<PublicationId, u32> = ScopedFlatMap::new();
        for (&delivery, row) in &state.obligations {
            obligations_by_target.insert((row.target, delivery));
            *obligations_by_pub.get_or_insert_default(delivery.publication) += 1;
        }
        let mut inbound_by_pub: ScopedFlatMap<PublicationId, u32> = ScopedFlatMap::new();
        for row in state.inbound.values() {
            *inbound_by_pub.get_or_insert_default(row.publication) += 1;
        }
        let mut outbound_by_target_pi = FlatSet::new();
        let mut outbound = ScopedFlatMap::new();
        for (delivery, row) in state.outbound {
            outbound_by_target_pi.insert((row.target, row.packet_id));
            outbound.insert(delivery, row);
        }
        let mut retained_bytes = 0u64;
        let mut retained_by_pub = ScopedFlatMap::new();
        let mut dup_retained_pub = false;
        for (topic, row) in &state.retained {
            if retained_by_pub
                .insert(row.publication, topic.clone())
                .is_some()
            {
                dup_retained_pub = true;
            }
            if let Some(p) = publications.get(&row.publication)
                && let Some(payload) = payloads.get(&p.payload)
            {
                retained_bytes = retained_bytes.saturating_add(payload.bytes.len() as u64);
            }
        }
        let mut wills_bytes = 0u64;
        let mut wills_by_client: BTreeMap<Vec<u8>, usize> = BTreeMap::new();
        for intent in state.wills.values() {
            wills_bytes = wills_bytes.saturating_add(intent.spec.payload.len() as u64);
            *wills_by_client
                .entry(intent.client_id.as_slice().to_vec())
                .or_insert(0) += 1;
        }
        let mut witnesses: BTreeMap<ReceiptOrigin, OriginWitness> = BTreeMap::new();
        let mut open_will_receipt_bytes = 0usize;
        let mut all_receipts_valid = true;
        let mut operations = ScopedMap::new();
        for (op, record) in state.operations {
            if witness::is_open_will_recipe(&record.recipe) {
                open_will_receipt_bytes =
                    open_will_receipt_bytes.saturating_add(record.recipe.len());
            }
            let cached_bounds =
                match witness::receipt_bounds(&state.limits, op, &record.recipe, record.decision) {
                    Ok(bounds) => {
                        if let Some(b) = bounds {
                            witnesses
                                .entry(ReceiptOrigin::of(op))
                                .or_default()
                                .record(b);
                        }
                        Some(bounds)
                    }
                    Err(_) => {
                        all_receipts_valid = false;
                        None
                    }
                };
            operations.insert(
                op,
                VOperation {
                    recipe: std::sync::Arc::from(record.recipe),
                    decision: record.decision,
                    cached_bounds,
                },
            );
        }
        let mut store = Self {
            ledger: state.ledger,
            collected_through: state.collected_through,
            revision: state.revision,
            limits: state.limits,
            accounts: state.accounts,
            next_session: state.next_session,
            session_clock: state.session_clock,
            route_revision: state.route_revision,
            sessions: state.sessions,
            routes: state.routes,
            operations,
            fences: state.fences,
            receipt_frontiers: state.receipt_frontiers,
            retained: state.retained,
            wills: state.wills,
            publications,
            payloads,
            obligations: state.obligations,
            inbound: state.inbound,
            outbound,
            active_sessions,
            active_by_client,
            total_routes,
            routes_by_session,
            obligations_by_target,
            obligations_by_pub,
            inbound_by_pub,
            outbound_by_target_pi,
            publications_by_payload,
            retained_bytes,
            retained_by_pub,
            wills_bytes,
            wills_by_client,
            witnesses,
            open_will_receipt_bytes,
            validated: false,
            durability: Durability::Available,
        };
        if !dup_active_client
            && !dup_retained_pub
            && all_receipts_valid
            && read::check_inventory_bound(&store).is_ok()
            && read::validate_retained(&store, ReadBudget::SNAPSHOT).is_ok()
        {
            store.validated = true;
        }
        store
    }

    /// Sets whether the medium accepts writes.
    pub fn set_durability(&mut self, durability: Durability) {
        self.durability = durability;
    }

    /// Bounded attachment barrier, O(log N) via `wills_by_client`.
    pub fn has_will_for_client(&self, client: &[u8]) -> Result<bool, ReadError> {
        if !self.validated {
            read::validate_wills(self)?;
        }
        Ok(self.wills_by_client.get(client).copied().unwrap_or(0) > 0)
    }

    /// Applies up to `MAX_GROUP` commands in order with exact parity to
    /// `SqliteStore::apply_group`.
    pub fn apply_group(&mut self, commands: &[&Command]) -> Result<Vec<Applied>, GroupRefused> {
        if commands.len() > MAX_GROUP {
            return Err(GroupRefused::TooLarge {
                commands: commands.len(),
                max: MAX_GROUP,
            });
        }
        match commands {
            [] => return Ok(Vec::new()),
            [only] => return Ok(vec![self.apply(only)]),
            _ => {}
        }
        if self.durability == Durability::Failing {
            let replies: Vec<Applied> = commands.iter().map(|c| self.apply(c)).collect();
            if replies
                .iter()
                .any(|r| matches!(r.reply, StoreReply::Outcome(StoreOutcome::PendingOrUnknown)))
            {
                return Ok(vec![UNKNOWN; commands.len()]);
            }
            return Ok(replies);
        }
        Ok(commands.iter().map(|c| self.apply(c)).collect())
    }

    fn next_revision(&self) -> Result<u64, CommitErr> {
        if self.durability == Durability::Failing {
            return Err(CommitErr::NotWritten);
        }
        self.revision.checked_add(1).ok_or(CommitErr::NotWritten)
    }

    fn established_inner(&self, operation: OperationId) -> Established {
        if operation.incarnation() != self.ledger.incarnation() {
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
            EpochPhase::Closed => {
                Established::Outcome(StoreOutcome::RejectedCertain(RejectReason::EpochClosed))
            }
            _ => Established::Absent,
        }
    }

    pub(crate) fn is_command_already_settled(&self, command: &Command) -> bool {
        let recipe = command.recipe.get();
        let operation = recipe.operation();
        match self.established_inner(operation) {
            Established::Outcome(_) => true,
            Established::Absent => {
                !self.receipt_capacity(operation, Some(ReceiptClass::of(recipe)))
                    || !matches!(
                        self.will_receipt_capacity(command.recipe.canonical_bytes()),
                        Ok(true)
                    )
            }
        }
    }

    pub(crate) fn can_fence(&self, operation: OperationId) -> bool {
        matches!(self.established_inner(operation), Established::Absent)
            && self.receipt_capacity(operation, None)
            && check_charge(self.accounts, self.limits, Accounts::RECEIPT).is_ok()
            && self.revision.checked_add(1).is_some()
    }

    fn active_session(&self, session: SessionId, revision: Revision) -> Result<(), RejectReason> {
        match self.sessions.get(&session) {
            Some(record) if record.state == SessionState::Active && record.revision == revision => {
                Ok(())
            }
            _ => Err(RejectReason::PreconditionFailed),
        }
    }

    fn receipt_capacity(&self, operation: OperationId, class: Option<ReceiptClass>) -> bool {
        if let Some(closure) = self.receipt_frontiers.get(&ReceiptOrigin::of(operation)) {
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
            || self.receipt_frontiers.is_empty()
            || self.accounts.receipt_slots.get()
                < self
                    .limits
                    .receipt_slots
                    .get()
                    .saturating_sub(RECEIPT_CONTROL_RESERVE)
    }

    fn will_receipt_capacity(&self, canonical: &[u8]) -> Result<bool, ReadError> {
        const MAX_BYTES: usize = 8 * 1024 * 1024;
        if self.open_will_receipt_bytes > MAX_BYTES {
            return Err(ReadError::LimitExceeded);
        }
        let is_open = witness::is_open_will_recipe(canonical);
        Ok(!is_open
            || self
                .open_will_receipt_bytes
                .checked_add(canonical.len())
                .is_some_and(|n| n <= MAX_BYTES))
    }

    fn identity_was_used(&self, recipe: &Recipe) -> Result<bool, ReadError> {
        let Some(incoming) = witness::bounds(recipe)? else {
            return Ok(false);
        };
        let operation = recipe.operation();
        let origin = ReceiptOrigin::of(operation);
        if self.receipt_frontiers.get(&origin).is_some_and(|c| {
            incoming.first.sequence() <= c.publication_through
                || incoming
                    .payload
                    .is_some_and(|p| p.sequence() <= c.payload_through)
        }) {
            return Ok(true);
        }
        if self
            .publications
            .range(incoming.first..=incoming.last)
            .next()
            .is_some()
            || incoming.payload.is_some_and(|p| {
                self.payloads.contains_key(&p) || self.publications_by_payload.contains_key(&p)
            })
        {
            return Ok(true);
        }
        if !self.validated {
            // Re-scan operations in origin when reopened from unvalidated state.
            let scope = operation.scope();
            let mut meter = read::ReadMeter::new(ReadBudget::SNAPSHOT);
            let mut found = false;
            self.operations.for_each_in_scope_range(
                scope,
                OperationSeq::new(0),
                OperationSeq::new(u64::MAX),
                |op, record| {
                    if found {
                        return Ok(());
                    }
                    meter.field(record.recipe.len(), ReadBudget::SNAPSHOT.max_field_bytes)?;
                    meter.charge(
                        256usize
                            .checked_add(record.recipe.len())
                            .ok_or(ReadError::LimitExceeded)?,
                    )?;
                    let bounds = match record.cached_bounds {
                        Some(cached) => cached,
                        None => witness::receipt_bounds(
                            &self.limits,
                            op,
                            &record.recipe,
                            record.decision,
                        )?,
                    };
                    if let Some(old) = bounds
                        && (incoming.first <= old.last && old.first <= incoming.last
                            || incoming.payload.is_some() && incoming.payload == old.payload)
                    {
                        found = true;
                    }
                    Ok(())
                },
            )?;
            return Ok(found);
        }
        Ok(self
            .witnesses
            .get(&origin)
            .is_some_and(|w| w.matches(origin, incoming.first, incoming.last, incoming.payload)))
    }

    fn record_operation(
        &mut self,
        operation: OperationId,
        canonical: std::sync::Arc<[u8]>,
        decision: Decision,
        witness_bounds: Option<IdentityBounds>,
    ) {
        if witness::is_open_will_recipe(&canonical) {
            self.open_will_receipt_bytes =
                self.open_will_receipt_bytes.saturating_add(canonical.len());
        }
        if let Some(bounds) = witness_bounds {
            self.witnesses
                .entry(ReceiptOrigin::of(operation))
                .or_default()
                .record(bounds);
        }
        self.operations.insert(
            operation,
            VOperation {
                recipe: canonical,
                decision,
                cached_bounds: Some(witness_bounds),
            },
        );
    }

    fn remove_operation(&mut self, operation: &OperationId) {
        if let Some(record) = self.operations.remove(operation)
            && witness::is_open_will_recipe(&record.recipe)
        {
            self.open_will_receipt_bytes = self
                .open_will_receipt_bytes
                .saturating_sub(record.recipe.len());
        }
    }

    fn commit_rejection(
        &mut self,
        recipe: &Recipe,
        operation: OperationId,
        canonical: std::sync::Arc<[u8]>,
        reason: RejectReason,
    ) -> (StoreOutcome, bool) {
        let Ok(next_rev) = self.next_revision() else {
            return (StoreOutcome::PendingOrUnknown, false);
        };
        let Ok(next_accounts) = check_charge(self.accounts, self.limits, Accounts::RECEIPT) else {
            return (StoreOutcome::PendingOrUnknown, false);
        };
        let witness_bounds = if recipe.replay_subscriptions().is_some() {
            witness::bounds(recipe).ok().flatten()
        } else {
            None
        };
        self.accounts = next_accounts;
        self.record_operation(
            operation,
            canonical,
            Decision::Rejected(reason),
            witness_bounds,
        );
        self.revision = next_rev;
        (StoreOutcome::RejectedCertain(reason), true)
    }

    fn discard_completed_publication(&mut self, publication: PublicationId) {
        let has_inbound = self.inbound_by_pub.contains_key(&publication);
        let has_obligation = self.obligations_by_pub.contains_key(&publication);
        let has_retained = self.retained_by_pub.contains_key(&publication);
        if !has_inbound
            && !has_obligation
            && !has_retained
            && let Some(removed) = self.publications.remove(&publication)
            && let Some(count) = self.publications_by_payload.get_mut(&removed.payload)
        {
            *count -= 1;
            if *count == 0 {
                self.publications_by_payload.remove(&removed.payload);
            }
        }
    }

    fn dec_obligation_pub(&mut self, publication: PublicationId) {
        if let Some(count) = self.obligations_by_pub.get_mut(&publication) {
            *count -= 1;
            if *count == 0 {
                self.obligations_by_pub.remove(&publication);
            }
        }
    }

    fn dec_inbound_pub(&mut self, publication: PublicationId) {
        if let Some(count) = self.inbound_by_pub.get_mut(&publication) {
            *count -= 1;
            if *count == 0 {
                self.inbound_by_pub.remove(&publication);
            }
        }
    }

    fn release_retained_in_place(&mut self, topic: &[u8]) {
        if let Some(old) = self.retained.remove(topic) {
            self.retained_by_pub.remove(&old.publication);
            let payload_id = self
                .publications
                .get(&old.publication)
                .expect("validated retained publication")
                .payload;
            let (payload_len, free_payload) = {
                let payload = self
                    .payloads
                    .get_mut(&payload_id)
                    .expect("validated retained payload");
                payload.references -= 1;
                (payload.bytes.len() as u64, payload.references == 0)
            };
            self.retained_bytes -= payload_len;
            self.accounts.references = References::new(self.accounts.references.get() - 1);
            if free_payload {
                self.payloads.remove(&payload_id);
                self.accounts.payload_bytes =
                    PayloadBytes::new(self.accounts.payload_bytes.get() - payload_len);
            }
            self.discard_completed_publication(old.publication);
        }
    }

    fn end_obligation_in_place(&mut self, delivery: DeliveryId) {
        let record = self
            .obligations
            .remove(&delivery)
            .expect("preflighted obligation");
        self.obligations_by_target
            .remove(&(record.target, delivery));
        self.dec_obligation_pub(delivery.publication);
        if let Some(claim) = self.outbound.remove(&delivery) {
            self.outbound_by_target_pi
                .remove(&(claim.target, claim.packet_id));
            self.accounts.exchange_slots =
                ExchangeSlots::new(self.accounts.exchange_slots.get() - 1);
        }
        self.accounts.references = References::new(self.accounts.references.get() - 1);
        let payload_id = self
            .publications
            .get(&delivery.publication)
            .expect("preflighted publication")
            .payload;
        let (length, free_payload) = {
            let payload = self
                .payloads
                .get_mut(&payload_id)
                .expect("preflighted payload");
            payload.references -= 1;
            (payload.bytes.len() as u64, payload.references == 0)
        };
        if free_payload {
            self.payloads.remove(&payload_id);
            self.accounts.payload_bytes =
                PayloadBytes::new(self.accounts.payload_bytes.get() - length);
        }
        self.discard_completed_publication(delivery.publication);
    }

    fn preflight_end_obligation(&self, delivery: DeliveryId) -> Result<(), AdminRefused> {
        let broken = AdminRefused::Inconsistent;
        if !self.obligations.contains_key(&delivery) {
            return Err(broken);
        }
        if self.outbound.contains_key(&delivery)
            && self
                .accounts
                .exchange_slots
                .checked_sub(ExchangeSlots::new(1))
                .is_none()
        {
            return Err(broken);
        }
        if self
            .accounts
            .references
            .checked_sub(References::new(1))
            .is_none()
        {
            return Err(broken);
        }
        let payload_id = self
            .publications
            .get(&delivery.publication)
            .ok_or(broken)?
            .payload;
        let payload = self.payloads.get(&payload_id).ok_or(broken)?;
        let next_refs = payload.references.checked_sub(1).ok_or(broken)?;
        if next_refs == 0 {
            let length = u64::try_from(payload.bytes.len()).map_err(|_| broken)?;
            self.accounts
                .payload_bytes
                .checked_sub(PayloadBytes::new(length))
                .ok_or(broken)?;
        }
        Ok(())
    }

    #[allow(clippy::type_complexity)]
    fn preflight_cancel_session(
        &self,
        session: SessionId,
    ) -> Result<(Vec<(SessionId, InboundPacketId)>, Vec<DeliveryId>, usize), AdminRefused> {
        let broken = AdminRefused::Inconsistent;
        let row = self
            .sessions
            .get(&session)
            .filter(|r| r.state == SessionState::Active)
            .ok_or(AdminRefused::UnknownSession)?;
        row.revision
            .checked_next()
            .map_err(|_| AdminRefused::Exhausted)?;
        let marker_keys: Vec<_> = self
            .inbound
            .range(
                (session, InboundPacketId::new(1).unwrap())
                    ..=(session, InboundPacketId::new(u16::MAX).unwrap()),
            )
            .map(|(k, _)| *k)
            .collect();
        let delivery_keys: Vec<_> = self
            .obligations_by_target
            .range(
                (
                    session,
                    DeliveryId {
                        publication: PublicationId::new(
                            StoreIncarnation::new(0),
                            RunEpoch::new(0),
                            LogicalOwnerId::new(0),
                            PublicationSeq::new(0),
                        ),
                        index: 0,
                    },
                )
                    ..=(
                        session,
                        DeliveryId {
                            publication: PublicationId::new(
                                StoreIncarnation::new(u64::MAX),
                                RunEpoch::new(u64::MAX),
                                LogicalOwnerId::new(u64::MAX),
                                PublicationSeq::new(u64::MAX),
                            ),
                            index: u32::MAX,
                        },
                    ),
            )
            .map(|&(_, d)| d)
            .collect();
        let routes_count = self
            .routes_by_session
            .get(&session)
            .map_or(0, BTreeSet::len);
        let markers = marker_keys.len();
        let deliveries = delivery_keys.len();
        let total = markers
            .checked_add(deliveries)
            .and_then(|n| n.checked_add(routes_count))
            .ok_or(broken)?;
        if total > MAX_RECOVERY_INVENTORY_ROWS
            || u64::try_from(markers).map_err(|_| broken)? > self.limits.exchange_slots.get()
            || u64::try_from(deliveries).map_err(|_| broken)? > self.limits.references.get()
        {
            return Err(broken);
        }
        if routes_count != 0 {
            self.route_revision
                .checked_next()
                .map_err(|_| AdminRefused::Exhausted)?;
        }
        // Check aggregate decrements don't underflow.
        let mut ex_sub = markers as u64;
        let mut ref_sub = 0u64;
        let mut payload_drops: BTreeMap<PayloadId, u64> = BTreeMap::new();
        for &delivery in &delivery_keys {
            if !self.obligations.contains_key(&delivery) {
                return Err(broken);
            }
            if self.outbound.contains_key(&delivery) {
                ex_sub = ex_sub.checked_add(1).ok_or(broken)?;
            }
            ref_sub = ref_sub.checked_add(1).ok_or(broken)?;
            let pid = self
                .publications
                .get(&delivery.publication)
                .ok_or(broken)?
                .payload;
            *payload_drops.entry(pid).or_insert(0) += 1;
        }
        if self.accounts.exchange_slots.get() < ex_sub || self.accounts.references.get() < ref_sub {
            return Err(broken);
        }
        let mut bytes_sub = 0u64;
        for (pid, drops) in payload_drops {
            let payload = self.payloads.get(&pid).ok_or(broken)?;
            if payload.references < drops {
                return Err(broken);
            }
            if payload.references == drops {
                bytes_sub = bytes_sub
                    .checked_add(payload.bytes.len() as u64)
                    .ok_or(broken)?;
            }
        }
        if self.accounts.payload_bytes.get() < bytes_sub {
            return Err(broken);
        }
        Ok((marker_keys, delivery_keys, routes_count))
    }

    fn cancel_session_in_place(
        &mut self,
        session: SessionId,
        marker_keys: Vec<(SessionId, InboundPacketId)>,
        delivery_keys: Vec<DeliveryId>,
        routes_count: usize,
    ) -> SessionCancellations {
        for intent in self.wills.values_mut().filter(|i| i.session == session) {
            intent.state = WillState::Pending;
        }
        let markers = marker_keys.len() as u32;
        let deliveries = delivery_keys.len() as u32;
        for key in marker_keys {
            let marker = self.inbound.remove(&key).expect("preflighted inbound");
            self.dec_inbound_pub(marker.publication);
            self.accounts.exchange_slots =
                ExchangeSlots::new(self.accounts.exchange_slots.get() - 1);
            self.discard_completed_publication(marker.publication);
        }
        for delivery in delivery_keys {
            self.end_obligation_in_place(delivery);
        }
        if let Some(topics) = self.routes_by_session.remove(&session) {
            for topic in topics {
                if let Some(targets) = self.routes.get_mut(&topic) {
                    let before = targets.len();
                    targets.retain(|t| t.session != session);
                    let removed = before - targets.len();
                    self.total_routes -= removed;
                    if targets.is_empty() {
                        self.routes.remove(&topic);
                    }
                }
            }
        }
        if routes_count != 0 {
            self.route_revision = self
                .route_revision
                .checked_next()
                .expect("preflighted route_revision");
        }
        if let Some(removed) = self.sessions.remove(&session)
            && removed.state == SessionState::Active
        {
            self.active_sessions -= 1;
            if self.active_by_client.get(&removed.client_id) == Some(&session) {
                self.active_by_client.remove(&removed.client_id);
            }
        }
        SessionCancellations {
            inbound: markers,
            deliveries,
            routes: routes_count as u32,
        }
    }

    fn exec_publication(
        &mut self,
        admit: Admission<'_>,
        retained_mask: Option<&RetainMask>,
        payload: Option<&SharedBytes>,
        remove_will: Option<OperationId>,
    ) -> Step<CommitFacts> {
        self.exec_publication_with_claim(admit, retained_mask, payload, remove_will, None)
    }

    fn exec_publication_with_claim(
        &mut self,
        admit: Admission<'_>,
        retained_mask: Option<&RetainMask>,
        payload: Option<&SharedBytes>,
        remove_will: Option<OperationId>,
        inline_claim: Option<InlineClaim>,
    ) -> Step<CommitFacts> {
        let failed = CommitErr::Refused(RejectReason::PreconditionFailed);
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
            read::check_message_observation(self, expiry.observed).map_err(|_| failed)?;
            expiry.deadline().map_err(|_| failed)?;
            let anchor = self.session_clock.ok_or(failed)?.reading;
            expiry.interval == 0
                || (anchor.tick >= expiry.observed.tick
                    && matches!(
                        expiry.state(MessageTime {
                            domain: anchor.domain,
                            tick: anchor.tick,
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
        let exactly_once = admit.qos == Qos::ExactlyOnce;
        let raw_ref_count = u64::try_from(targets.len()).map_err(|_| failed)?
            + u64::from(retained_mask.is_some() && length > 0);
        if admit.costs.payload_bytes != admit.payload.length
            || admit.costs.references.get() != raw_ref_count
            || admit.costs.exchange_slots.get() != u64::from(exactly_once)
            || admit.costs.receipt_slots.get() != 1
        {
            return Err(failed);
        }
        if admit.will_source.is_none() {
            self.active_session(admit.source_session, admit.source_revision)
                .map_err(CommitErr::Refused)?;
        }
        if self.publications.contains_key(&admit.publication)
            || self.payloads.contains_key(&admit.payload.id)
            || self.publications_by_payload.contains_key(&admit.payload.id)
        {
            return Err(failed);
        }
        // Retained preflight & release simulation.
        let mut accounts_after_release = self.accounts;
        let mut release_old_retained = false;
        if retained_mask.is_some() {
            if !self.validated {
                read::validate_retained(self, ReadBudget::SNAPSHOT).map_err(|_| failed)?;
            }
            let new_len = if expired { 0 } else { length };
            let existing = self.retained.get(admit.topic.as_slice());
            if new_len > 0 && existing.is_none() && self.retained.len() >= MAX_RETAINED_TOPICS {
                return Err(CommitErr::Refused(RejectReason::CapacityExceeded(
                    Resource::References,
                )));
            }
            let mut old_len = 0u64;
            let mut old_free_payload = false;
            if let Some(old) = existing {
                let old_pub = self.publications.get(&old.publication).ok_or(failed)?;
                let old_payload = self.payloads.get(&old_pub.payload).ok_or(failed)?;
                old_len = old_payload.bytes.len() as u64;
                old_free_payload = old_payload.references == 1;
                if old_payload.references == 0 {
                    return Err(failed);
                }
                release_old_retained = true;
            }
            let base_retained_bytes = self.retained_bytes.checked_sub(old_len).ok_or(failed)?;
            if base_retained_bytes
                .checked_add(new_len)
                .is_none_or(|n| n > MAX_RETAINED_BYTES)
            {
                return Err(CommitErr::Refused(RejectReason::CapacityExceeded(
                    Resource::PayloadBytes,
                )));
            }
            if release_old_retained {
                accounts_after_release.references = accounts_after_release
                    .references
                    .checked_sub(References::new(1))
                    .ok_or(failed)?;
                if old_free_payload {
                    accounts_after_release.payload_bytes = accounts_after_release
                        .payload_bytes
                        .checked_sub(PayloadBytes::new(old_len))
                        .ok_or(failed)?;
                }
            }
        }
        // Target preflight.
        for target in targets {
            self.active_session(target.session, target.session_revision)
                .map_err(CommitErr::Refused)?;
            if !expired && self.obligations.contains_key(&target.delivery) {
                return Err(failed);
            }
        }
        if !expired && targets.len() > 1 {
            if targets.len() <= 8 {
                for i in 0..targets.len() {
                    for j in (i + 1)..targets.len() {
                        if targets[i].delivery == targets[j].delivery {
                            return Err(failed);
                        }
                    }
                }
            } else {
                let mut seen = BTreeSet::new();
                for target in targets {
                    if !seen.insert(target.delivery) {
                        return Err(failed);
                    }
                }
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
                    .outbound_by_target_pi
                    .contains(&(target.session, claim.packet_id))
            {
                return Err(failed);
            }
            Some((*target, claim.packet_id))
        } else {
            None
        };
        let inbound_slot = if exactly_once {
            let (packet_id, exchange) = admit.inbound.ok_or(failed)?;
            let slot = (admit.source_session, packet_id);
            if self.inbound.contains_key(&slot) {
                return Err(failed);
            }
            Some((slot, exchange))
        } else {
            None
        };
        let reference_count = if expired { 0 } else { raw_ref_count };
        let exchange_slots = ExchangeSlots::new(
            admit
                .costs
                .exchange_slots
                .get()
                .checked_add(u64::from(claimed_target.is_some()))
                .ok_or(failed)?,
        );
        let charged = check_charge(
            accounts_after_release,
            self.limits,
            Accounts {
                payload_bytes: if expired {
                    PayloadBytes::new(0)
                } else {
                    admit.costs.payload_bytes
                },
                references: References::new(reference_count),
                exchange_slots,
                receipt_slots: admit.costs.receipt_slots,
            },
        )
        .map_err(CommitErr::Refused)?;
        let final_accounts = if reference_count == 0 && !expired {
            Accounts {
                payload_bytes: charged
                    .payload_bytes
                    .checked_sub(admit.costs.payload_bytes)
                    .ok_or(failed)?,
                ..charged
            }
        } else {
            charged
        };
        let retained_producer: Option<Vec<u8>> =
            if retained_mask.is_some() && length > 0 && !expired {
                let client = if let Some(client) = admit.will_source {
                    client.to_vec()
                } else {
                    self.sessions
                        .get(&admit.source_session)
                        .ok_or(failed)?
                        .client_id
                        .clone()
                };
                if client.is_empty() || client.contains(&0) || std::str::from_utf8(&client).is_err()
                {
                    return Err(failed);
                }
                Some(client)
            } else {
                None
            };
        if let Some(will_id) = remove_will
            && !self.wills.contains_key(&will_id)
        {
            return Err(failed);
        }
        let _next_rev = self.next_revision()?;

        // --- Infallible mutation phase ---
        if release_old_retained {
            self.release_retained_in_place(admit.topic.as_slice());
        }
        self.publications.insert(
            admit.publication,
            VPublication {
                expiry: admit.expiry,
                properties: admit.properties.cloned(),
                retained_version: None,
                operation,
                source: admit.source_session,
                topic: admit.topic.clone(),
                qos: admit.qos,
                retain: admit.retain,
                payload: admit.payload.id,
            },
        );
        *self
            .publications_by_payload
            .get_or_insert_default(admit.payload.id) += 1;
        if reference_count > 0 {
            self.payloads.insert(
                admit.payload.id,
                VPayload {
                    bytes: bytes.clone(),
                    references: reference_count,
                },
            );
        }
        if !expired {
            for (target_index, target) in targets.iter().enumerate() {
                let obligation = ObligationRecord {
                    retain: retained_mask.is_some_and(|mask| mask.is_set(target_index)),
                    target: target.session,
                    target_revision: target.session_revision,
                    qos: target.effective_qos,
                    cause: target.cause,
                    phase: ObligationPhase::Pending,
                };
                self.obligations.insert(target.delivery, obligation);
                self.obligations_by_target
                    .insert((target.session, target.delivery));
                *self
                    .obligations_by_pub
                    .get_or_insert_default(target.delivery.publication) += 1;
            }
        }
        if let Some((target, packet_id)) = claimed_target {
            self.outbound.insert(
                target.delivery,
                OutboundRecord {
                    started: false,
                    target: target.session,
                    target_revision: target.session_revision,
                    packet_id,
                    claim: operation,
                },
            );
            self.outbound_by_target_pi
                .insert((target.session, packet_id));
        }
        if let Some((slot, exchange)) = inbound_slot {
            self.inbound.insert(
                slot,
                InboundRecord {
                    exchange,
                    publication: admit.publication,
                    phase: InboundPhase::AwaitPubrel,
                },
            );
            *self.inbound_by_pub.get_or_insert_default(admit.publication) += 1;
        }
        self.accounts = final_accounts;
        if let Some(producer_client_id) = retained_producer {
            let topic_vec = admit.topic.as_slice().to_vec();
            self.retained.insert(
                topic_vec.clone(),
                RetainedRecord {
                    publication: admit.publication,
                    producer_client_id,
                },
            );
            self.retained_by_pub.insert(admit.publication, topic_vec);
            self.retained_bytes += length;
        }
        self.discard_completed_publication(admit.publication);
        if let Some(will_id) = remove_will
            && let Some(removed) = self.wills.remove(&will_id)
        {
            self.wills_bytes -= removed.spec.payload.len() as u64;
            if let Some(count) = self.wills_by_client.get_mut(removed.client_id.as_slice()) {
                *count -= 1;
                if *count == 0 {
                    self.wills_by_client.remove(removed.client_id.as_slice());
                }
            }
        }
        Ok(match (remove_will, admit.inbound) {
            (Some(will), _) => CommitFacts::WillPublished {
                will,
                publication: admit.publication,
            },
            (None, Some((_, exchange))) => CommitFacts::PublicationAdmitted {
                publication: admit.publication,
                exchange,
            },
            (None, None) => CommitFacts::RetainedQos0Stored {
                publication: admit.publication,
            },
        })
    }

    fn exec_will_publish(
        &mut self,
        value: &WillPublication,
        payload: Option<&SharedBytes>,
    ) -> Step<CommitFacts> {
        let failed = CommitErr::Refused(RejectReason::PreconditionFailed);
        let intent = self.wills.get(&value.will).ok_or(failed)?;
        if intent.state != WillState::Pending
            || value.live_retain.is_some() != intent.spec.retain
            || value.topic != intent.spec.topic
            || payload != Some(&intent.spec.payload)
            || value.payload.length.get() != intent.spec.payload.len() as u64
        {
            return Err(failed);
        }
        let source_session = intent.session;
        let source_client = intent.client_id.clone();
        let retain = intent.spec.retain;
        self.exec_publication(
            Admission {
                expiry: None,
                properties: None,
                operation: value.operation,
                request: None,
                will_source: Some(source_client.as_slice()),
                publication: value.publication,
                source_session,
                source_revision: Revision::new(0),
                inbound: None,
                route_revision: value.route_revision,
                qos: Qos::AtLeastOnce,
                retain,
                topic: &value.topic,
                payload: value.payload,
                costs: value.costs,
                targets: value.targets.as_slice(),
            },
            value.live_retain.as_ref(),
            payload,
            Some(value.will),
        )
    }

    fn exec_route_updates(
        &mut self,
        session: SessionId,
        revision: Revision,
        route_revision: Revision,
        changes: &[(SharedBytes, Option<Qos>, SubscriptionOptions)],
        replay: Option<(OperationId, PublicationId, Option<MessageTime>)>,
    ) -> Step<CommitFacts> {
        let failed = CommitErr::Refused(RejectReason::PreconditionFailed);
        if let Some((_, _, Some(now))) = replay {
            read::check_message_observation(self, now).map_err(|_| failed)?;
        }
        self.active_session(session, revision)
            .map_err(CommitErr::Refused)?;
        if replay.is_some() && !self.validated {
            read::validate_retained(self, ReadBudget::SNAPSHOT).map_err(|_| failed)?;
        }
        // Compute retained replay plans BEFORE checking route_revision?
        // Wait: MemoryStore computes plans first (checking observation, active_session,
        // retained validate, expiry state), then calls stage_updates (checking
        // route_revision, change validity, capacity). Preserving exact order:
        let mut plans: Vec<(PublicationId, Qos)> = Vec::new();
        if let Some((_, _, observed)) = replay {
            let client = self
                .sessions
                .get(&session)
                .ok_or(failed)?
                .client_id
                .as_slice();
            let reserve = self
                .retained
                .len()
                .saturating_mul(changes.len())
                .min(MAX_REPLAYS);
            plans.try_reserve_exact(reserve).map_err(|_| {
                CommitErr::Refused(RejectReason::CapacityExceeded(Resource::References))
            })?;
            for (step, (topic, granted, options)) in changes.iter().enumerate() {
                let existed = changes[..step]
                    .iter()
                    .rfind(|(t, _, _)| t == topic)
                    .map_or_else(
                        || {
                            self.routes
                                .get(topic.as_slice())
                                .is_some_and(|targets| targets.iter().any(|t| t.session == session))
                        },
                        |(_, g, _)| g.is_some(),
                    );
                if let Some(granted) = *granted
                    && (options.retain_handling() == 0
                        || (options.retain_handling() == 1 && !existed))
                {
                    for (rtopic, value) in &self.retained {
                        if mqttiumd_contract::topic::matches(topic.as_slice(), rtopic)
                            && !(options.no_local()
                                && value.producer_client_id.as_slice() == client)
                        {
                            let expiry = self
                                .publications
                                .get(&value.publication)
                                .ok_or(failed)?
                                .expiry;
                            let eligible = match (expiry, observed) {
                                (None, _) => true,
                                (Some(expiry), Some(now)) => matches!(
                                    expiry.state(now).map_err(|_| failed)?,
                                    ExpiryState::Fresh { .. }
                                ),
                                (Some(_), None) => false,
                            };
                            if eligible {
                                plans.push((value.publication, granted));
                            }
                        }
                    }
                }
            }
        }
        if route_revision != self.route_revision {
            return Err(failed);
        }
        // Simulate route table changes on a tiny local map (<= 8 topics).
        let mut local_routes: BTreeMap<&[u8], Vec<RouteTarget>> = BTreeMap::new();
        let mut delta_total: isize = 0;
        for (topic, granted, options) in changes {
            if topic.len() > self.limits.max_topic_bytes
                || !mqttiumd_contract::topic::valid_filter(topic.as_slice())
                || SubscriptionOptions::from_bits(options.bits()) != Some(*options)
                || (granted.is_none() && *options != SubscriptionOptions::DEFAULT)
            {
                return Err(failed);
            }
            let entry = local_routes.entry(topic.as_slice()).or_insert_with(|| {
                self.routes
                    .get(topic.as_slice())
                    .cloned()
                    .unwrap_or_default()
            });
            if let Some(qos) = *granted {
                if let Some(target) = entry.iter_mut().find(|t| t.session == session) {
                    target.qos = qos;
                    target.options = *options;
                } else {
                    if entry.len() >= self.limits.max_route_targets
                        || entry.len() >= self.limits.max_targets_per_admission
                    {
                        return Err(CommitErr::Refused(RejectReason::CapacityExceeded(
                            Resource::References,
                        )));
                    }
                    entry.push(RouteTarget {
                        session,
                        qos,
                        options: *options,
                    });
                    delta_total += 1;
                }
            } else {
                let before = entry.len();
                entry.retain(|t| t.session != session);
                delta_total -= (before - entry.len()) as isize;
            }
        }
        let next_total_routes = self
            .total_routes
            .checked_add_signed(delta_total)
            .ok_or(failed)?;
        if next_total_routes > MAX_SUBSCRIPTIONS {
            return Err(CommitErr::Refused(RejectReason::CapacityExceeded(
                Resource::References,
            )));
        }
        let next_route_rev = self.route_revision.checked_next().map_err(|_| failed)?;
        // First charge RECEIPT (stage_updates), then charge replay references!
        let accounts_after_receipt = check_charge(self.accounts, self.limits, Accounts::RECEIPT)
            .map_err(CommitErr::Refused)?;
        let final_accounts = if !plans.is_empty() {
            check_charge(
                accounts_after_receipt,
                self.limits,
                Accounts {
                    references: References::new(plans.len() as u64),
                    ..Accounts::default()
                },
            )
            .map_err(CommitErr::Refused)?
        } else {
            accounts_after_receipt
        };
        // Preflight replay IDs and payload refcount increments.
        let mut replay_ids: Vec<(PublicationId, PublicationId, Qos)> = Vec::new();
        if let Some((_, first, _)) = replay {
            replay_ids.reserve(plans.len());
            let mut payload_adds: BTreeMap<PayloadId, u64> = BTreeMap::new();
            for (index, (version, granted)) in plans.into_iter().enumerate() {
                let pub_id = PublicationId::new(
                    first.incarnation(),
                    first.epoch(),
                    first.owner(),
                    PublicationSeq::new(
                        first
                            .sequence()
                            .get()
                            .checked_add(index as u64)
                            .ok_or(failed)?,
                    ),
                );
                if self.publications.contains_key(&pub_id)
                    || self.obligations.contains_key(&DeliveryId {
                        publication: pub_id,
                        index: 0,
                    })
                {
                    return Err(failed);
                }
                let src = self.publications.get(&version).ok_or(failed)?;
                *payload_adds.entry(src.payload).or_insert(0) += 1;
                replay_ids.push((pub_id, version, src.qos.min(granted)));
            }
            for (pid, add) in payload_adds {
                let payload = self.payloads.get(&pid).ok_or(failed)?;
                payload.references.checked_add(add).ok_or(failed)?;
            }
        }
        let _next_rev = self.next_revision()?;

        // --- Infallible mutation phase ---
        for (topic_slice, targets) in local_routes {
            let has_session = targets.iter().any(|t| t.session == session);
            if targets.is_empty() {
                self.routes.remove(topic_slice);
            } else {
                self.routes.insert(topic_slice.to_vec(), targets);
            }
            if has_session {
                self.routes_by_session
                    .entry(session)
                    .or_default()
                    .insert(topic_slice.to_vec());
            } else if let Some(set) = self.routes_by_session.get_mut(&session) {
                set.remove(topic_slice);
                if set.is_empty() {
                    self.routes_by_session.remove(&session);
                }
            }
        }
        self.total_routes = next_total_routes;
        self.route_revision = next_route_rev;
        self.accounts = final_accounts;
        if let Some((op, _, _)) = replay {
            for (pub_id, version, qos) in replay_ids {
                let mut source = self
                    .publications
                    .get(&version)
                    .expect("preflighted version")
                    .clone();
                source.operation = op;
                source.retained_version = Some(version);
                source.retain = true;
                let payload_id = source.payload;
                self.payloads
                    .get_mut(&payload_id)
                    .expect("preflighted payload")
                    .references += 1;
                *self
                    .publications_by_payload
                    .get_or_insert_default(payload_id) += 1;
                self.publications.insert(pub_id, source);
                let delivery = DeliveryId {
                    publication: pub_id,
                    index: 0,
                };
                self.obligations.insert(
                    delivery,
                    ObligationRecord {
                        retain: true,
                        target: session,
                        target_revision: revision,
                        qos,
                        cause: DeliveryCause::RetainedReplay,
                        phase: ObligationPhase::Pending,
                    },
                );
                self.obligations_by_target.insert((session, delivery));
                *self.obligations_by_pub.get_or_insert_default(pub_id) += 1;
            }
        }
        Ok(CommitFacts::SubscriptionsChanged {
            session,
            route_revision: self.route_revision,
        })
    }

    fn exec_claim(&mut self, command: &ClaimDelivery) -> Step<CommitFacts> {
        let failed = CommitErr::Refused(RejectReason::PreconditionFailed);
        self.active_session(command.target, command.target_revision)
            .map_err(CommitErr::Refused)?;
        let record = self.obligations.get(&command.delivery).ok_or(failed)?;
        if record.target != command.target
            || record.target_revision != command.target_revision
            || record.qos == Qos::AtMostOnce
            || record.phase != ObligationPhase::Pending
            || self.outbound.contains_key(&command.delivery)
            || self
                .outbound_by_target_pi
                .contains(&(command.target, command.packet_id))
        {
            return Err(failed);
        }
        let next_accounts = check_charge(
            self.accounts,
            self.limits,
            Accounts {
                exchange_slots: ExchangeSlots::new(1),
                receipt_slots: ReceiptSlots::new(1),
                ..Accounts::default()
            },
        )
        .map_err(CommitErr::Refused)?;
        let _next_rev = self.next_revision()?;

        self.accounts = next_accounts;
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
        self.outbound_by_target_pi
            .insert((command.target, command.packet_id));
        Ok(CommitFacts::DeliveryClaimed {
            delivery: command.delivery,
            target: command.target,
            packet_id: command.packet_id,
            claim: command.operation,
        })
    }

    fn exec_finish(&mut self, command: &FinishDelivery) -> Step<CommitFacts> {
        let failed = CommitErr::Refused(RejectReason::PreconditionFailed);
        self.active_session(command.target, command.target_revision)
            .map_err(CommitErr::Refused)?;
        let slot = self.outbound.get(&command.delivery).ok_or(failed)?;
        if slot.target != command.target
            || slot.target_revision != command.target_revision
            || slot.packet_id != command.packet_id
            || slot.claim != command.claim
        {
            return Err(failed);
        }
        self.preflight_end_obligation(command.delivery)
            .map_err(|_| failed)?;
        // Note: end_obligation frees exchange_slot + reference (+ payload bytes),
        // THEN charges RECEIPT! So receipt_slots check is on current receipt_slots!
        let _check_receipt = check_charge(
            Accounts {
                payload_bytes: PayloadBytes::new(0),
                references: References::new(0),
                exchange_slots: ExchangeSlots::new(0),
                receipt_slots: self.accounts.receipt_slots,
            },
            self.limits,
            Accounts::RECEIPT,
        )
        .map_err(CommitErr::Refused)?;
        let _next_rev = self.next_revision()?;

        self.end_obligation_in_place(command.delivery);
        self.accounts.receipt_slots = ReceiptSlots::new(self.accounts.receipt_slots.get() + 1);
        Ok(CommitFacts::DeliveryFinished {
            delivery: command.delivery,
            target: command.target,
            packet_id: command.packet_id,
            claim: command.claim,
        })
    }

    fn exec_started(&mut self, command: &MarkDeliveryStarted) -> Step<CommitFacts> {
        let failed = CommitErr::Refused(RejectReason::PreconditionFailed);
        self.active_session(command.target, command.target_revision)
            .map_err(CommitErr::Refused)?;
        if self
            .publications
            .get(&command.delivery.publication)
            .and_then(|p| p.expiry)
            .is_none()
        {
            return Err(failed);
        }
        if self
            .obligations
            .get(&command.delivery)
            .is_none_or(|r| r.phase != ObligationPhase::Pending)
        {
            return Err(failed);
        }
        let slot = self.outbound.get(&command.delivery).ok_or(failed)?;
        if slot.target != command.target
            || slot.target_revision != command.target_revision
            || slot.packet_id != command.packet_id
            || slot.claim != command.claim
        {
            return Err(failed);
        }
        let next_accounts = check_charge(self.accounts, self.limits, Accounts::RECEIPT)
            .map_err(CommitErr::Refused)?;
        let _next_rev = self.next_revision()?;

        self.outbound
            .get_mut(&command.delivery)
            .expect("preflighted")
            .started = true;
        self.accounts = next_accounts;
        Ok(CommitFacts::DeliveryStarted {
            delivery: command.delivery,
            target: command.target,
            packet_id: command.packet_id,
            claim: command.claim,
        })
    }

    fn exec_pubrel(&mut self, command: &FinishDelivery) -> Step<CommitFacts> {
        let failed = CommitErr::Refused(RejectReason::PreconditionFailed);
        self.active_session(command.target, command.target_revision)
            .map_err(CommitErr::Refused)?;
        let slot = self.outbound.get(&command.delivery).ok_or(failed)?;
        if slot.target != command.target
            || slot.target_revision != command.target_revision
            || slot.packet_id != command.packet_id
            || slot.claim != command.claim
        {
            return Err(failed);
        }
        let record = self.obligations.get(&command.delivery).ok_or(failed)?;
        if record.qos != Qos::ExactlyOnce || record.phase != ObligationPhase::Pending {
            return Err(failed);
        }
        let next_accounts = check_charge(self.accounts, self.limits, Accounts::RECEIPT)
            .map_err(CommitErr::Refused)?;
        let _next_rev = self.next_revision()?;

        self.obligations
            .get_mut(&command.delivery)
            .expect("preflighted")
            .phase = ObligationPhase::AwaitPubcomp;
        self.accounts = next_accounts;
        Ok(CommitFacts::DeliveryPubrelReady {
            delivery: command.delivery,
            target: command.target,
            packet_id: command.packet_id,
            claim: command.claim,
        })
    }

    fn exec_expired(&mut self, command: &ExpireDelivery) -> Step<CommitFacts> {
        let failed = CommitErr::Refused(RejectReason::PreconditionFailed);
        read::check_message_observation(self, command.observed).map_err(|_| failed)?;
        let slot = self.outbound.get(&command.finish.delivery).ok_or(failed)?;
        let publication = self
            .publications
            .get(&command.finish.delivery.publication)
            .ok_or(failed)?;
        if slot.started
            || self
                .obligations
                .get(&command.finish.delivery)
                .is_none_or(|r| r.phase != ObligationPhase::Pending)
            || publication.expiry != Some(command.expiry)
            || command.expiry.state(command.observed) != Ok(ExpiryState::Expired)
        {
            return Err(failed);
        }
        self.exec_finish(&command.finish)?;
        Ok(CommitFacts::DeliveryExpired {
            delivery: command.finish.delivery,
            target: command.finish.target,
            packet_id: command.finish.packet_id,
            claim: command.finish.claim,
        })
    }

    fn exec_consume(&mut self, command: &ConsumeDelivery) -> Step<CommitFacts> {
        let failed = CommitErr::Refused(RejectReason::PreconditionFailed);
        self.active_session(command.target, command.target_revision)
            .map_err(CommitErr::Refused)?;
        let record = self.obligations.get(&command.delivery).ok_or(failed)?;
        if record.target != command.target
            || record.target_revision != command.target_revision
            || record.qos != Qos::AtMostOnce
            || self.outbound.contains_key(&command.delivery)
        {
            return Err(failed);
        }
        self.preflight_end_obligation(command.delivery)
            .map_err(|_| failed)?;
        let _check_receipt = check_charge(
            Accounts {
                payload_bytes: PayloadBytes::new(0),
                references: References::new(0),
                exchange_slots: ExchangeSlots::new(0),
                receipt_slots: self.accounts.receipt_slots,
            },
            self.limits,
            Accounts::RECEIPT,
        )
        .map_err(CommitErr::Refused)?;
        let _next_rev = self.next_revision()?;

        self.end_obligation_in_place(command.delivery);
        self.accounts.receipt_slots = ReceiptSlots::new(self.accounts.receipt_slots.get() + 1);
        Ok(CommitFacts::DeliveryConsumed {
            delivery: command.delivery,
            target: command.target,
        })
    }

    fn exec_release(&mut self, release: &ReleaseInbound) -> Step<CommitFacts> {
        let failed = CommitErr::Refused(RejectReason::PreconditionFailed);
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
        let next_ex = self
            .accounts
            .exchange_slots
            .checked_sub(ExchangeSlots::new(1))
            .ok_or(failed)?;
        let next_accounts = check_charge(
            Accounts {
                exchange_slots: next_ex,
                ..self.accounts
            },
            self.limits,
            Accounts::RECEIPT,
        )
        .map_err(CommitErr::Refused)?;
        let _next_rev = self.next_revision()?;

        self.inbound.remove(&slot);
        self.dec_inbound_pub(publication);
        self.accounts = next_accounts;
        self.discard_completed_publication(publication);
        Ok(CommitFacts::InboundReleased {
            exchange: release.exchange,
        })
    }

    fn exec_session(&mut self, command: &SessionLifecycle) -> Step<CommitFacts> {
        let failed = CommitErr::Refused(RejectReason::PreconditionFailed);
        let observed = match &command.action {
            SessionAction::OpenAt { now, .. }
            | SessionAction::OpenWillAt { now, .. }
            | SessionAction::Expire { now, .. } => Some(*now),
            SessionAction::Detach { observed_at, .. }
            | SessionAction::EndWill { observed_at, .. } => Some(*observed_at),
            _ => None,
        };
        let next_clock = if let (Some(now), Some(mut anchor)) = (observed, self.session_clock)
            && now > anchor.logical
        {
            let delta = now.checked_sub(anchor.logical).ok_or(failed)?;
            anchor.reading.tick = anchor.reading.tick.checked_add(delta).ok_or(failed)?;
            anchor.logical = now;
            Some(anchor)
        } else {
            self.session_clock
        };
        // Check receipt slot before mutations. Note: cancel_session only frees
        // exchange_slots/references/payload_bytes, NEVER receipt_slots.
        let _check_receipt = check_charge(
            Accounts {
                payload_bytes: PayloadBytes::new(0),
                references: References::new(0),
                exchange_slots: ExchangeSlots::new(0),
                receipt_slots: self.accounts.receipt_slots,
            },
            self.limits,
            Accounts::RECEIPT,
        )
        .map_err(CommitErr::Refused)?;

        match &command.action {
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
            }
            | SessionAction::OpenWillAt {
                client_id,
                expected,
                clean_start,
                expiry,
                ..
            } => {
                let will = match &command.action {
                    SessionAction::OpenWillAt { will, .. } => will.as_ref(),
                    _ => None,
                };
                if self
                    .wills_by_client
                    .get(client_id.as_slice())
                    .copied()
                    .unwrap_or(0)
                    > 0
                {
                    return Err(failed);
                }
                if let Some(spec) = will {
                    if spec.topic.len() > self.limits.max_topic_bytes
                        || spec.payload.len() > self.limits.max_payload_bytes
                    {
                        return Err(failed);
                    }
                    if self.wills.len() >= MAX_WILL_INTENTS
                        || self
                            .wills_bytes
                            .checked_add(spec.payload.len() as u64)
                            .is_none_or(|n| n > MAX_WILL_BYTES)
                    {
                        return Err(CommitErr::Refused(RejectReason::CapacityExceeded(
                            Resource::PayloadBytes,
                        )));
                    }
                }
                if matches!(expiry, SessionExpiry::After(_)) && self.session_clock.is_none() {
                    return Err(failed);
                }
                let current = if self.validated {
                    self.active_by_client
                        .get(client_id.as_slice())
                        .and_then(|&id| self.sessions.get(&id).map(|r| (id, r)))
                } else {
                    let mut found = self.sessions.iter().filter(|(_, r)| {
                        r.state == SessionState::Active && r.client_id == client_id.as_slice()
                    });
                    let first = found.next().map(|(id, r)| (*id, r));
                    if found.next().is_some() {
                        return Err(failed);
                    }
                    first
                };
                if current.map(|(session, record)| SessionGuard {
                    session,
                    opened_by: record.opened_by,
                }) != *expected
                {
                    return Err(failed);
                }
                let now = match command.action {
                    SessionAction::OpenAt { now, .. } | SessionAction::OpenWillAt { now, .. } => {
                        Some(now)
                    }
                    _ => None,
                };
                if now.is_none()
                    && current
                        .is_some_and(|(_, row)| row.detached.and_then(|d| d.deadline).is_some())
                {
                    return Err(failed);
                }
                let replace = current.is_some_and(|(_, r)| {
                    *clean_start
                        || r.expiry == SessionExpiry::OnDisconnect
                        || r.detached
                            .and_then(|d| d.deadline)
                            .zip(now)
                            .is_some_and(|(deadline, now)| deadline <= now)
                });
                let current_id = current.map(|(id, _)| id);
                let cancel_plan = if replace {
                    let id = current_id.ok_or(failed)?;
                    Some((id, self.preflight_cancel_session(id).map_err(|_| failed)?))
                } else {
                    None
                };
                let creating_new = current_id.is_none() || replace;
                let active_after_cancel = self.active_sessions - usize::from(replace);
                if creating_new {
                    if active_after_cancel >= self.limits.max_sessions {
                        return Err(CommitErr::Refused(RejectReason::SessionCapacityExceeded));
                    }
                    self.next_session.checked_next().map_err(|_| failed)?;
                }
                let _next_rev = self.next_revision()?;

                // --- Infallible mutation phase ---
                let mut cancelled = SessionCancellations::default();
                let retired = if let Some((id, (markers, deliveries, routes_count))) = cancel_plan {
                    cancelled = self.cancel_session_in_place(id, markers, deliveries, routes_count);
                    Some(id)
                } else {
                    None
                };
                let (session, revision, session_present) = if !creating_new {
                    let id = current_id.expect("present");
                    let row = self.sessions.get_mut(&id).expect("present");
                    row.expiry = *expiry;
                    row.opened_by = Some(command.operation);
                    row.detached = None;
                    (id, row.revision, true)
                } else {
                    let session = self.next_session;
                    self.next_session = session.checked_next().expect("preflighted");
                    let revision = Revision::new(1);
                    let cid_vec = client_id.as_slice().to_vec();
                    self.sessions.insert(
                        session,
                        SessionRecord {
                            client_id: cid_vec.clone(),
                            revision,
                            state: SessionState::Active,
                            expiry: *expiry,
                            opened_by: Some(command.operation),
                            detached: None,
                        },
                    );
                    self.active_sessions += 1;
                    self.active_by_client.insert(cid_vec, session);
                    (session, revision, false)
                };
                if let Some(spec) = will {
                    self.wills_bytes += spec.payload.len() as u64;
                    *self
                        .wills_by_client
                        .entry(client_id.as_slice().to_vec())
                        .or_insert(0) += 1;
                    self.wills.insert(
                        command.operation,
                        WillIntent {
                            id: command.operation,
                            session,
                            client_id: client_id.clone(),
                            spec: spec.clone(),
                            state: WillState::Armed,
                        },
                    );
                }
                self.session_clock = next_clock;
                self.accounts.receipt_slots =
                    ReceiptSlots::new(self.accounts.receipt_slots.get() + 1);
                Ok(CommitFacts::SessionLifecycle(SessionChange::Opened {
                    session,
                    revision,
                    session_present,
                    expiry: *expiry,
                    retired,
                    route_revision: self.route_revision,
                    cancelled,
                }))
            }
            SessionAction::Detach {
                guard,
                expiry,
                observed_at,
            }
            | SessionAction::EndWill {
                guard,
                expiry,
                observed_at,
                ..
            } => {
                let will_publish = match command.action {
                    SessionAction::EndWill { publish, .. } => Some(publish),
                    _ => None,
                };
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
                if will_publish.is_some() {
                    let will_id = guard.opened_by.ok_or(failed)?;
                    if let Some(intent) = self.wills.get(&will_id)
                        && (intent.session != guard.session || intent.state != WillState::Armed)
                    {
                        return Err(failed);
                    }
                }
                let deadline = expiry.deadline(*observed_at).ok_or(failed)?;
                let cancel_plan = if *expiry == SessionExpiry::OnDisconnect {
                    Some(
                        self.preflight_cancel_session(guard.session)
                            .map_err(|_| failed)?,
                    )
                } else {
                    None
                };
                let _next_rev = self.next_revision()?;

                // --- Infallible mutation phase ---
                if let Some(publish) = will_publish {
                    let will_id = guard.opened_by.expect("checked");
                    if self.wills.contains_key(&will_id) {
                        if publish {
                            self.wills.get_mut(&will_id).expect("present").state =
                                WillState::Pending;
                        } else if let Some(removed) = self.wills.remove(&will_id) {
                            self.wills_bytes -= removed.spec.payload.len() as u64;
                            if let Some(c) =
                                self.wills_by_client.get_mut(removed.client_id.as_slice())
                            {
                                *c -= 1;
                                if *c == 0 {
                                    self.wills_by_client.remove(removed.client_id.as_slice());
                                }
                            }
                        }
                    }
                }
                let change = if let Some((markers, deliveries, routes_count)) = cancel_plan {
                    let cancelled = self.cancel_session_in_place(
                        guard.session,
                        markers,
                        deliveries,
                        routes_count,
                    );
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
                    let row = self.sessions.get_mut(&guard.session).expect("present");
                    row.expiry = *expiry;
                    row.detached = Some(detached);
                    SessionChange::Detached {
                        session: guard.session,
                        expiry: *expiry,
                        detached,
                    }
                };
                self.session_clock = next_clock;
                self.accounts.receipt_slots =
                    ReceiptSlots::new(self.accounts.receipt_slots.get() + 1);
                Ok(CommitFacts::SessionLifecycle(change))
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
                let (markers, deliveries, routes_count) = self
                    .preflight_cancel_session(guard.session)
                    .map_err(|_| failed)?;
                let _next_rev = self.next_revision()?;

                let cancelled =
                    self.cancel_session_in_place(guard.session, markers, deliveries, routes_count);
                self.session_clock = next_clock;
                self.accounts.receipt_slots =
                    ReceiptSlots::new(self.accounts.receipt_slots.get() + 1);
                Ok(CommitFacts::SessionLifecycle(SessionChange::Closed {
                    session: guard.session,
                    route_revision: self.route_revision,
                    cancelled,
                }))
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
                let (markers, deliveries, routes_count) = self
                    .preflight_cancel_session(guard.session)
                    .map_err(|_| failed)?;
                let _next_rev = self.next_revision()?;

                let cancelled =
                    self.cancel_session_in_place(guard.session, markers, deliveries, routes_count);
                self.session_clock = next_clock;
                self.accounts.receipt_slots =
                    ReceiptSlots::new(self.accounts.receipt_slots.get() + 1);
                Ok(CommitFacts::SessionLifecycle(SessionChange::Closed {
                    session: guard.session,
                    route_revision: self.route_revision,
                    cancelled,
                }))
            }
        }
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
        if self.operations.contains_key(&id(0))
            || self.fences.contains(&id(0))
            || self.publications.contains_key(&zero_pub)
            || self.payloads.contains_key(&zero_payload)
        {
            return Ok(false);
        }
        let mut meter = read::ReadMeter::new(ReadBudget::SNAPSHOT);
        let scope = (origin.incarnation, origin.epoch, origin.owner);
        let mut compatible = true;
        self.operations.for_each_in_scope_range(
            scope,
            OperationSeq::new(0),
            OperationSeq::new(u64::MAX),
            |operation, record| {
                if !compatible {
                    return Ok(());
                }
                meter.field(record.recipe.len(), ReadBudget::SNAPSHOT.max_field_bytes)?;
                meter.charge(
                    128usize
                        .checked_add(record.recipe.len())
                        .ok_or(ReadError::LimitExceeded)?,
                )?;
                let admit = match record.cached_bounds {
                    Some(cached) => cached,
                    None => witness::receipt_bounds(
                        &self.limits,
                        operation,
                        &record.recipe,
                        record.decision,
                    )?,
                };
                if let Some(admit) = admit
                    && (admit.first.sequence().get() == 0
                        || admit.payload.is_some_and(|p| p.sequence().get() == 0))
                {
                    compatible = false;
                }
                Ok(())
            },
        )?;
        Ok(compatible)
    }
}

impl DurableStore for VolatileStore {
    fn read_wills(
        &self,
        after: Option<OperationId>,
        limit: usize,
        budget: ReadBudget,
    ) -> Result<WillPage, ReadError> {
        read::will_page(self, after, limit, budget)
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
        read::check_message_observation(self, now)?;
        let mut result = MessageExpirySweep {
            removed: BoundedVec::with_capacity(MESSAGE_EXPIRY_SWEEP_DELIVERIES)
                .map_err(|_| ReadError::AllocationFailed)?,
            ..MessageExpirySweep::default()
        };
        let mut candidates = Vec::new();
        candidates
            .try_reserve_exact(limit as usize)
            .map_err(|_| ReadError::AllocationFailed)?;
        for (&id, publication) in self
            .publications
            .range((
                after.map_or(Bound::Unbounded, Bound::Excluded),
                Bound::Unbounded,
            ))
            .take(limit as usize)
        {
            read::check_message_candidate(self, id)?;
            candidates.push((id, publication.expiry));
        }
        result.more = candidates.last().is_some_and(|(last, _)| {
            self.publications
                .range((Bound::Excluded(*last), Bound::Unbounded))
                .next()
                .is_some()
        });
        result.next = result
            .more
            .then(|| candidates.last().map(|(id, _)| *id))
            .flatten();

        // Preflight all mutations before touching `self`.
        let mut retained_topics_to_release: Vec<Vec<u8>> = Vec::new();
        let mut deliveries_to_end: Vec<(DeliveryId, SessionId)> = Vec::new();
        for (id, expiry) in candidates {
            let Some(expiry) = expiry else {
                continue;
            };
            if expiry.observed.domain == now.domain && expiry.observed.tick > now.tick {
                continue;
            }
            match expiry.state(now).map_err(|_| ReadError::InvalidData)? {
                ExpiryState::Fresh { .. } => (),
                ExpiryState::TemporalUncertain => {
                    result.quarantined += 1;
                }
                ExpiryState::Expired => {
                    let range = DeliveryId {
                        publication: id,
                        index: 0,
                    }..=DeliveryId {
                        publication: id,
                        index: u32::MAX,
                    };
                    let uncertain = self
                        .outbound
                        .iter()
                        .filter(|(delivery, record)| delivery.publication == id && !record.started)
                        .count();
                    result.quarantined = result
                        .quarantined
                        .checked_add(
                            u32::try_from(uncertain).map_err(|_| ReadError::LimitExceeded)?,
                        )
                        .ok_or(ReadError::LimitExceeded)?;
                    if let Some(topic) = self.retained_by_pub.get(&id).cloned() {
                        retained_topics_to_release.push(topic);
                        result.retained_removed += 1;
                    }
                    for (&delivery, record) in self.obligations.range(range) {
                        if !self.outbound.contains_key(&delivery) {
                            result
                                .removed
                                .try_push(ExpiredDelivery {
                                    delivery,
                                    target: record.target,
                                })
                                .map_err(|_| ReadError::LimitExceeded)?;
                            deliveries_to_end.push((delivery, record.target));
                        }
                    }
                }
            }
        }
        if result.removed.is_empty() && result.retained_removed == 0 {
            return Ok(result);
        }
        let next_rev = self.next_revision().map_err(|_| ReadError::Unavailable)?;
        for topic in retained_topics_to_release {
            self.release_retained_in_place(&topic);
        }
        for (delivery, _) in deliveries_to_end {
            self.end_obligation_in_place(delivery);
        }
        self.revision = next_rev;
        Ok(result)
    }

    fn checkpoint_session_clock(
        &mut self,
        expected: Option<SessionClockAnchor>,
        reading: SessionClockReading,
    ) -> Result<SessionClockAnchor, ReadError> {
        let current = self.session_clock;
        if current != expected {
            return current.ok_or(ReadError::InvalidData);
        }
        let next = SessionClockAnchor::advance(current, reading)?;
        let next_rev = self.next_revision().map_err(|_| ReadError::Unavailable)?;
        self.session_clock = Some(next);
        self.revision = next_rev;
        Ok(next)
    }

    fn collect_receipts(
        &mut self,
        collection: ReceiptCollection,
    ) -> Result<ReceiptCollectionReply, ReadError> {
        let refused = |why| Ok(ReceiptCollectionReply::Refused(why));
        let request = collection.closure;
        let origin = request.origin;
        if collection.limit == 0 || collection.limit > MAX_COLLECTION_ROWS {
            return refused(ReceiptCollectionRefusal::LimitExceeded);
        }
        if origin.incarnation != self.ledger.incarnation() {
            return refused(ReceiptCollectionRefusal::OriginMismatch);
        }
        if origin.epoch > self.ledger.last_opened() {
            return refused(ReceiptCollectionRefusal::FutureEpoch);
        }
        if origin.epoch.get() == 0 || origin.epoch <= self.collected_through {
            return refused(ReceiptCollectionRefusal::InvalidFrontier);
        }
        let stored = self.receipt_frontiers.get(&origin).copied();
        if stored.is_none() {
            if self.receipt_frontiers.len() >= MAX_RECEIPT_ORIGINS {
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
            let mut meter = read::ReadMeter::new(ReadBudget::COLLECTION);
            for seq in prior.through.get() + 1..=request.through.get() {
                let operation = OperationId::new(
                    origin.incarnation,
                    origin.epoch,
                    origin.owner,
                    OperationSeq::new(seq),
                );
                let record = self.operations.get(&operation);
                let fence = self.fences.contains(&operation);
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
                let admit = match record.cached_bounds {
                    Some(cached) => cached,
                    None => witness::receipt_bounds(
                        &self.limits,
                        operation,
                        &record.recipe,
                        record.decision,
                    )?,
                };
                if let Some(admit) = admit
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
        let scope = (origin.incarnation, origin.epoch, origin.owner);
        let _ = self.operations.for_each_in_scope_range(
            scope,
            OperationSeq::new(0),
            closure.through,
            |id, _| -> Result<(), ()> {
                if keys.len() < ceiling {
                    keys.push((id, false));
                }
                Ok(())
            },
        );
        keys.extend(
            self.fences
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
        let Some(revision) = self.revision.checked_add(1) else {
            return refused(ReceiptCollectionRefusal::Exhausted);
        };
        if self.durability == Durability::Failing {
            return Ok(ReceiptCollectionReply::PendingOrUnknown);
        }
        self.receipt_frontiers.insert(origin, closure);
        for (operation, fence) in keys {
            if fence {
                self.fences.remove(&operation);
            } else {
                self.remove_operation(&operation);
            }
        }
        if let Some(w) = self.witnesses.get_mut(&origin) {
            w.trim_through(
                closure.publication_through.get(),
                closure.payload_through.get(),
            );
        }
        self.accounts.receipt_slots = ReceiptSlots::new(retained);
        self.revision = revision;
        Ok(ReceiptCollectionReply::Applied {
            closure,
            removed,
            retained,
            more,
        })
    }

    fn receipt_frontier(&self, origin: ReceiptOrigin) -> Result<Option<ReceiptClosure>, ReadError> {
        if origin.incarnation != self.ledger.incarnation() {
            return Err(ReadError::InvalidData);
        }
        Ok(self.receipt_frontiers.get(&origin).copied())
    }

    fn collect_closed_receipts(
        &mut self,
        collection: ClosedReceiptCollection,
    ) -> Result<ClosedReceiptReply, ReadError> {
        let refused = |why| Ok(ClosedReceiptReply::Refused(why));
        if collection.limit == 0 || collection.limit > MAX_COLLECTION_ROWS {
            return refused(ReceiptCollectionRefusal::LimitExceeded);
        }
        if self.ledger.phase_of(collection.through) != EpochPhase::Closed {
            return refused(ReceiptCollectionRefusal::FutureEpoch);
        }
        let through = collection.through.max(self.collected_through);
        let incarnation = self.ledger.incarnation();
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
            self.operations
                .iter_ordered()
                .skip_while(|(id, _)| *id < start)
                .take_while(|(id, _)| *id <= end)
                .take(ceiling)
                .map(|(id, _)| (id, false)),
        );
        keys.extend(
            self.fences
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
            self.receipt_frontiers
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
            .accounts
            .receipt_slots
            .get()
            .checked_sub(u64::from(removed))
            .ok_or(ReadError::InvalidData)?;
        if through == self.collected_through && removed == 0 && removed_origins == 0 {
            return Ok(ClosedReceiptReply::Applied {
                through,
                removed,
                removed_origins,
                retained,
                more,
            });
        }
        let Some(revision) = self.revision.checked_add(1) else {
            return refused(ReceiptCollectionRefusal::Exhausted);
        };
        if self.durability == Durability::Failing {
            return Ok(ClosedReceiptReply::PendingOrUnknown);
        }
        self.collected_through = through;
        for (id, fence) in keys {
            if fence {
                self.fences.remove(&id);
            } else {
                self.remove_operation(&id);
            }
        }
        for origin in origins {
            self.receipt_frontiers.remove(&origin);
            self.witnesses.remove(&origin);
        }
        self.accounts.receipt_slots = ReceiptSlots::new(retained);
        self.revision = revision;
        Ok(ClosedReceiptReply::Applied {
            through,
            removed,
            removed_origins,
            retained,
            more,
        })
    }

    fn ledger(&self) -> Result<EpochLedger, ReadError> {
        Ok(self.ledger)
    }

    fn open_epoch(&mut self, command: &OpenEpoch) -> Result<OpeningOutcome, ReadError> {
        if let LedgerOpening::Opens { ledger } = self.ledger.open(command) {
            if !self.validated {
                read::check_inventory_bound(self)?;
            }
            if self.sessions.len() > self.limits.max_sessions
                || self.sessions.len() > MAX_RECOVERY_INVENTORY_ROWS
            {
                return Err(ReadError::LimitExceeded);
            }
            let now = self.session_clock.map(|anchor| anchor.logical);
            let mut expired = Vec::new();
            expired
                .try_reserve_exact(self.sessions.len())
                .map_err(|_| ReadError::AllocationFailed)?;
            let mut to_detach: Vec<(SessionId, SessionDetachment)> = Vec::new();
            for (&id, row) in &self.sessions {
                match row.expiry {
                    SessionExpiry::OnDisconnect => expired.push(id),
                    SessionExpiry::Never => {}
                    SessionExpiry::After(_) => {
                        let now = now.ok_or(ReadError::InvalidData)?;
                        if let Some(detached) = row.detached {
                            if detached.deadline.is_some_and(|deadline| deadline <= now) {
                                expired.push(id);
                            }
                        } else {
                            let detach = SessionDetachment {
                                operation: row.opened_by.ok_or(ReadError::InvalidData)?,
                                deadline: Some(
                                    row.expiry
                                        .deadline(now)
                                        .flatten()
                                        .ok_or(ReadError::InvalidData)?,
                                ),
                            };
                            to_detach.push((id, detach));
                        }
                    }
                }
            }
            // Preflight all session cancellations before mutating.
            let mut cancel_plans = Vec::with_capacity(expired.len());
            for session in expired {
                let plan = self
                    .preflight_cancel_session(session)
                    .map_err(|_| ReadError::InvalidData)?;
                cancel_plans.push((session, plan));
            }
            if let Ok(next_rev) = self.next_revision() {
                self.ledger = ledger;
                for intent in self.wills.values_mut() {
                    intent.state = WillState::Pending;
                }
                for (id, detach) in to_detach {
                    if let Some(row) = self.sessions.get_mut(&id) {
                        row.detached = Some(detach);
                    }
                }
                for (session, (markers, deliveries, routes_count)) in cancel_plans {
                    self.cancel_session_in_place(session, markers, deliveries, routes_count);
                }
                self.revision = next_rev;
            }
        }
        Ok(OpeningOutcome::read(&self.ledger, command))
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
        let canonical = command.recipe.canonical_bytes();
        // Single-pass recognition (replaces 3 separate map lookups in the prior path).
        if operation.incarnation() != self.ledger.incarnation() {
            return settled(StoreOutcome::RejectedCertain(
                RejectReason::PreconditionFailed,
            ));
        }
        if (operation.epoch() <= self.collected_through && operation.epoch().get() != 0)
            || self
                .receipt_frontiers
                .get(&ReceiptOrigin::of(operation))
                .is_some_and(|closure| operation.sequence() <= closure.through)
        {
            return settled(StoreOutcome::RetiredOutcomeUnavailable);
        }
        if let Some(record) = self.operations.get(&operation) {
            let same_payload = match (recipe, &command.payload, record.decision) {
                (recipe, Some(bytes), Decision::Committed(_))
                    if witness::bounds(recipe)
                        .ok()
                        .flatten()
                        .is_some_and(|b| b.payload.is_some()) =>
                {
                    let pid = witness::bounds(recipe)
                        .ok()
                        .flatten()
                        .and_then(|b| b.payload)
                        .expect("guarded publication payload");
                    self.payloads
                        .get(&pid)
                        .is_none_or(|stored| stored.bytes.as_slice() == bytes.as_slice())
                }
                _ => true,
            };
            if record.recipe.as_ref() != canonical || !same_payload {
                return Applied {
                    reply: StoreReply::Violation(StoreViolation::RecipeMismatch),
                    committed_now: false,
                    wrote: false,
                };
            }
            return settled(match record.decision {
                Decision::Committed(facts) => StoreOutcome::Committed(facts),
                Decision::Rejected(reason) => StoreOutcome::RejectedCertain(reason),
            });
        }
        if self.fences.contains(&operation) {
            return settled(StoreOutcome::RejectedCertain(RejectReason::Fenced));
        }
        let phase = match self.ledger.phase_of(operation.epoch()) {
            EpochPhase::Closed => {
                return settled(StoreOutcome::RejectedCertain(RejectReason::EpochClosed));
            }
            phase => phase,
        };
        if !self.receipt_capacity(operation, Some(ReceiptClass::of(recipe))) {
            return settled(StoreOutcome::PendingOrUnknown);
        }
        if !matches!(self.will_receipt_capacity(canonical), Ok(true)) {
            return settled(StoreOutcome::PendingOrUnknown);
        }
        if phase != EpochPhase::Open {
            return rejected(self.commit_rejection(
                recipe,
                operation,
                command.recipe.canonical_arc(),
                RejectReason::PreconditionFailed,
            ));
        }
        match self.identity_was_used(recipe) {
            Ok(false) => {}
            Ok(true) => {
                return rejected(self.commit_rejection(
                    recipe,
                    operation,
                    command.recipe.canonical_arc(),
                    RejectReason::PreconditionFailed,
                ));
            }
            Err(_) => return settled(StoreOutcome::PendingOrUnknown),
        }

        let result = match recipe {
            Recipe::AnnotatedPublication(annotated) => {
                let props = Some(&annotated.properties);
                match annotated.inner.as_ref() {
                    Recipe::AdmitPublication(admit) => {
                        let mut a = Admission::from(admit);
                        a.properties = props;
                        self.exec_publication(a, None, command.payload.as_ref(), None)
                    }
                    Recipe::ClaimedPublication(claimed) => {
                        let mut a = Admission::from(&claimed.admission);
                        a.properties = props;
                        self.exec_publication_with_claim(
                            a,
                            None,
                            command.payload.as_ref(),
                            None,
                            Some(claimed.claim),
                        )
                    }
                    Recipe::RetainedPublication(value) => {
                        let mut a = Admission::from(&value.admission);
                        a.properties = props;
                        self.exec_publication(
                            a,
                            Some(&value.retain_targets),
                            command.payload.as_ref(),
                            None,
                        )
                    }
                    Recipe::RetainedQos0(value) => {
                        let mut a = Admission::from(value);
                        a.properties = props;
                        self.exec_publication(
                            a,
                            Some(&value.retain_targets),
                            command.payload.as_ref(),
                            None,
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
                        self.exec_publication(admit, mask, command.payload.as_ref(), None)
                    }
                    _ => Err(CommitErr::Refused(RejectReason::PreconditionFailed)),
                }
            }
            Recipe::WillPublication(value) => {
                self.exec_will_publish(value, command.payload.as_ref())
            }
            Recipe::AdmitPublication(admit) => {
                self.exec_publication(Admission::from(admit), None, command.payload.as_ref(), None)
            }
            Recipe::ClaimedPublication(claimed) => self.exec_publication_with_claim(
                Admission::from(&claimed.admission),
                None,
                command.payload.as_ref(),
                None,
                Some(claimed.claim),
            ),
            Recipe::RetainedPublication(value) => self.exec_publication(
                Admission::from(&value.admission),
                Some(&value.retain_targets),
                command.payload.as_ref(),
                None,
            ),
            Recipe::RetainedQos0(value) => self.exec_publication(
                Admission::from(value),
                Some(&value.retain_targets),
                command.payload.as_ref(),
                None,
            ),
            Recipe::ReplaySubscriptions(ReplaySubscriptions {
                update,
                first_publication,
            }) => {
                let changes: Vec<_> = update
                    .changes
                    .as_slice()
                    .iter()
                    .map(|c| (c.topic.clone(), c.granted, c.options))
                    .collect();
                self.exec_route_updates(
                    update.session,
                    update.session_revision,
                    update.route_revision,
                    &changes,
                    Some((update.operation, *first_publication, None)),
                )
            }
            Recipe::TimedReplaySubscriptions(value) => {
                let update = &value.replay.update;
                let changes: Vec<_> = update
                    .changes
                    .as_slice()
                    .iter()
                    .map(|c| (c.topic.clone(), c.granted, c.options))
                    .collect();
                self.exec_route_updates(
                    update.session,
                    update.session_revision,
                    update.route_revision,
                    &changes,
                    Some((
                        update.operation,
                        value.replay.first_publication,
                        Some(value.observed),
                    )),
                )
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
                self.exec_publication(admit, mask, command.payload.as_ref(), None)
            }
            Recipe::MarkDeliveryStarted(cmd) => self.exec_started(cmd),
            Recipe::ExpireDelivery(cmd) => self.exec_expired(cmd),
            Recipe::ReleaseInbound(release) => self.exec_release(release),
            Recipe::ChangeSubscriptions(ChangeSubscriptions {
                session,
                session_revision,
                route_revision,
                changes,
                ..
            }) => {
                let list: Vec<_> = changes
                    .as_slice()
                    .iter()
                    .map(|c| {
                        (
                            c.topic.clone(),
                            c.subscribe.then_some(Qos::AtLeastOnce),
                            SubscriptionOptions::DEFAULT,
                        )
                    })
                    .collect();
                self.exec_route_updates(*session, *session_revision, *route_revision, &list, None)
            }
            Recipe::UpdateSubscriptions(UpdateSubscriptions {
                session,
                session_revision,
                route_revision,
                changes,
                ..
            }) => {
                let list: Vec<_> = changes
                    .as_slice()
                    .iter()
                    .map(|c| (c.topic.clone(), c.granted, c.options))
                    .collect();
                self.exec_route_updates(*session, *session_revision, *route_revision, &list, None)
            }
            Recipe::ClaimDelivery(cmd) => self.exec_claim(cmd),
            Recipe::FinishDelivery(cmd) => self.exec_finish(cmd),
            Recipe::PreparePubrel(cmd) => self.exec_pubrel(cmd),
            Recipe::ConsumeDelivery(cmd) => self.exec_consume(cmd),
            Recipe::SessionLifecycle(cmd) => self.exec_session(cmd),
        };
        match result {
            Ok(facts) => {
                let witness_bounds = witness::bounds(recipe).ok().flatten();
                self.record_operation(
                    operation,
                    command.recipe.canonical_arc(),
                    Decision::Committed(facts),
                    witness_bounds,
                );
                self.revision += 1;
                Applied {
                    reply: StoreReply::Outcome(StoreOutcome::Committed(facts)),
                    committed_now: true,
                    wrote: true,
                }
            }
            Err(CommitErr::Refused(reason)) => rejected(self.commit_rejection(
                recipe,
                operation,
                command.recipe.canonical_arc(),
                reason,
            )),
            Err(CommitErr::NotWritten) => settled(StoreOutcome::PendingOrUnknown),
        }
    }

    fn established(&self, operation: OperationId) -> Result<Option<StoreOutcome>, ReadError> {
        Ok(match self.established_inner(operation) {
            Established::Outcome(outcome) => Some(outcome),
            Established::Absent => None,
        })
    }

    fn resolve_and_fence(&mut self, operation: OperationId) -> Resolved {
        let settled = |outcome| Resolved {
            outcome,
            fenced_now: false,
        };
        match self.established_inner(operation) {
            Established::Outcome(outcome) => settled(outcome),
            Established::Absent => {
                if !self.receipt_capacity(operation, None) {
                    return settled(StoreOutcome::PendingOrUnknown);
                }
                let Ok(next_accounts) = check_charge(self.accounts, self.limits, Accounts::RECEIPT)
                else {
                    return settled(StoreOutcome::PendingOrUnknown);
                };
                let Ok(next_rev) = self.next_revision() else {
                    return settled(StoreOutcome::PendingOrUnknown);
                };
                self.accounts = next_accounts;
                self.fences.insert(operation);
                self.revision = next_rev;
                Resolved {
                    outcome: StoreOutcome::RejectedCertain(RejectReason::Fenced),
                    fenced_now: true,
                }
            }
        }
    }

    fn admin(&mut self, command: AdminCommand) -> Result<AdminDone, AdminRefused> {
        match command {
            AdminCommand::CreateSession { client_id } => {
                if client_id.len() > MAX_CLIENT_ID_BYTES {
                    return Err(AdminRefused::ClientIdTooLong);
                }
                if self.active_by_client.contains_key(&client_id) {
                    return Err(AdminRefused::DuplicateClientId);
                }
                if self.active_sessions >= self.limits.max_sessions {
                    return Err(AdminRefused::TableFull);
                }
                let session = self.next_session;
                let next_session = session
                    .checked_next()
                    .map_err(|_| AdminRefused::Exhausted)?;
                let next_rev = self
                    .next_revision()
                    .map_err(|_| AdminRefused::WriteFailed)?;
                let revision = Revision::new(1);
                self.next_session = next_session;
                self.sessions.insert(
                    session,
                    SessionRecord {
                        client_id: client_id.clone(),
                        revision,
                        state: SessionState::Active,
                        expiry: SessionExpiry::Never,
                        opened_by: None,
                        detached: None,
                    },
                );
                self.active_sessions += 1;
                self.active_by_client.insert(client_id, session);
                self.revision = next_rev;
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
                if let Some(targets) = self.routes.get(&topic) {
                    if targets.iter().any(|t| t.session == session) {
                        return Err(AdminRefused::DuplicateTarget);
                    }
                    if targets.len() >= self.limits.max_route_targets {
                        return Err(AdminRefused::TableFull);
                    }
                }
                let next_route_rev = self
                    .route_revision
                    .checked_next()
                    .map_err(|_| AdminRefused::Exhausted)?;
                let next_rev = self
                    .next_revision()
                    .map_err(|_| AdminRefused::WriteFailed)?;
                self.routes
                    .entry(topic.clone())
                    .or_default()
                    .push(RouteTarget {
                        session,
                        qos,
                        options: SubscriptionOptions::DEFAULT,
                    });
                self.total_routes += 1;
                self.routes_by_session
                    .entry(session)
                    .or_default()
                    .insert(topic);
                self.route_revision = next_route_rev;
                self.revision = next_rev;
                Ok(AdminDone::RouteAdded {
                    route_revision: self.route_revision,
                })
            }
            AdminCommand::DeleteSession { session } => {
                let (markers, deliveries, routes_count) = self.preflight_cancel_session(session)?;
                let next_rev = self
                    .next_revision()
                    .map_err(|_| AdminRefused::WriteFailed)?;
                self.cancel_session_in_place(session, markers, deliveries, routes_count);
                self.revision = next_rev;
                Ok(AdminDone::SessionDeleted)
            }
            AdminCommand::CollectClosedEpochs { through } => {
                if self.ledger.phase_of(through) != EpochPhase::Closed {
                    return Err(AdminRefused::EpochNotClosed);
                }
                let collected = through.max(self.collected_through);
                let retired = |operation: &OperationId| {
                    operation.epoch() <= collected && operation.epoch().get() != 0
                };
                let mut freed = 0u64;
                for operation in self.operations.keys().chain(self.fences.iter().copied()) {
                    if retired(&operation) {
                        freed = freed.checked_add(1).ok_or(AdminRefused::Exhausted)?;
                    }
                }
                let next_receipts = self
                    .accounts
                    .receipt_slots
                    .checked_sub(ReceiptSlots::new(freed))
                    .ok_or(AdminRefused::Inconsistent)?;
                let next_rev = self
                    .next_revision()
                    .map_err(|_| AdminRefused::WriteFailed)?;
                self.collected_through = collected;
                self.accounts.receipt_slots = next_receipts;
                let retired_ops: Vec<OperationId> =
                    self.operations.keys().filter(|op| retired(op)).collect();
                for op in retired_ops {
                    self.remove_operation(&op);
                }
                self.fences.retain(|op| !retired(op));
                self.witnesses
                    .retain(|origin, _| !(origin.epoch <= collected && origin.epoch.get() != 0));
                self.revision = next_rev;
                Ok(AdminDone::Collected)
            }
        }
    }

    fn recover_with_budget(
        &self,
        cursor: Option<RecoveryCursor>,
        limit: usize,
        budget: ReadBudget,
    ) -> Result<RecoveryPage, RecoveryRefused> {
        read::recover_page(self, cursor, limit, budget)
    }

    fn read_work_with_budget(
        &self,
        session: SessionId,
        after: Option<DeliveryId>,
        limit: usize,
        budget: ReadBudget,
    ) -> Result<WorkPage, ReadError> {
        read::work_page(self, session, after, limit, budget)
    }

    fn snapshot_with_budget(&self, budget: ReadBudget) -> Result<DurableState, ReadError> {
        read::build_snapshot(self, budget)
    }
}
