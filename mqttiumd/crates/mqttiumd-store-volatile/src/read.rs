//! Bounded validation, recovery inventory, work paging, and snapshot builders
//! for `VolatileStore`, matching `MemoryStore` and `SqliteStore` semantics.

use std::collections::BTreeMap;
use std::ops::Bound;

use mqttiumd_contract::capacity::{BoundedVec, PayloadBytes};
use mqttiumd_contract::command::DeliveryCause;
use mqttiumd_contract::effects::RecoveredFact;
use mqttiumd_contract::ids::{DeliveryId, OperationId, PublicationId, SessionId};
use mqttiumd_contract::message_expiry::MessageTime;
use mqttiumd_contract::outcome::{CommitFacts, Knowledge, RejectReason};
use mqttiumd_contract::packet::SharedBytes;
use mqttiumd_contract::pubsub::{DeliveryClaim, SubscriptionOptions, WORK_PAGE_LIMIT, WorkItem};
use mqttiumd_contract::receipts::{MAX_RECEIPT_ORIGINS, ReceiptClosure, ReceiptOrigin};
use mqttiumd_contract::retained::MAX_RETAINED_BYTES;
use mqttiumd_contract::retained::MAX_RETAINED_TOPICS;
use mqttiumd_contract::session::SessionExpiry;
use mqttiumd_contract::will::{
    MAX_WILL_BYTES, MAX_WILL_INTENTS, MAX_WILL_PAYLOAD_BYTES, MAX_WILL_TOPIC_BYTES, WillState,
};
use mqttiumd_store_api::{
    Decision, DurableState, InboundRecord, MAX_CLIENT_ID_BYTES, MAX_RECOVERY_INVENTORY_ROWS,
    MESSAGE_EXPIRY_SWEEP_DELIVERIES, ObligationPhase, ObligationRecord, OperationRecord,
    PayloadRecord, PublicationRecord, ReadBudget, ReadError, RecoveryCursor, RecoveryPage,
    RecoveryRefused, RouteTarget, SessionState, WillPage, WorkPage,
};

use crate::VolatileStore;
use crate::tables::VSession;

pub(crate) struct ReadMeter {
    budget: ReadBudget,
    rows: usize,
    bytes: usize,
}

impl ReadMeter {
    pub(crate) fn new(budget: ReadBudget) -> Self {
        Self {
            budget,
            rows: 0,
            bytes: 0,
        }
    }

    pub(crate) fn charge(&mut self, bytes: usize) -> Result<(), ReadError> {
        self.rows = self.rows.checked_add(1).ok_or(ReadError::LimitExceeded)?;
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or(ReadError::LimitExceeded)?;
        self.budget.check_totals(self.rows, self.bytes)
    }

    pub(crate) fn field(&self, bytes: usize, maximum: usize) -> Result<(), ReadError> {
        if bytes > maximum {
            return Err(ReadError::LimitExceeded);
        }
        self.budget.check_field(bytes)
    }
}

pub(crate) fn delivery_claim(
    store: &VolatileStore,
    delivery: DeliveryId,
    record: &ObligationRecord,
) -> Result<Option<DeliveryClaim>, ReadError> {
    if !store.sessions.get(&record.target).is_some_and(|session| {
        session.state == SessionState::Active && session.revision == record.target_revision
    }) {
        return Err(ReadError::InvalidData);
    }
    let Some(claim) = store.outbound.get(&delivery) else {
        return if record.phase == ObligationPhase::Pending {
            Ok(None)
        } else {
            Err(ReadError::InvalidData)
        };
    };
    if claim.target != record.target
        || claim.target_revision != record.target_revision
        || claim.claim.incarnation() != store.ledger.incarnation()
    {
        return Err(ReadError::InvalidData);
    }
    let claim = DeliveryClaim {
        operation: claim.claim,
        packet_id: claim.packet_id,
        started: claim.started,
        await_pubcomp: record.phase == ObligationPhase::AwaitPubcomp,
    };
    let timed = store
        .publications
        .get(&delivery.publication)
        .is_some_and(|p| p.expiry.is_some());
    if !claim.valid_for(record.qos, timed) {
        return Err(ReadError::InvalidData);
    }
    Ok(Some(claim))
}

pub(crate) fn check_delivery_metadata(
    store: &VolatileStore,
    delivery: DeliveryId,
    record: &ObligationRecord,
) -> Result<(), ReadError> {
    let publication = store
        .publications
        .get(&delivery.publication)
        .ok_or(ReadError::InvalidData)?;
    if let Some(expiry) = publication.expiry() {
        expiry.deadline().map_err(|_| ReadError::InvalidData)?;
    }
    if (record.retain && !publication.retain)
        || (record.cause == DeliveryCause::RetainedReplay
            && (!record.retain
                || publication.retained_version.is_none()
                || publication.retained_version == Some(delivery.publication)))
        || (record.cause == DeliveryCause::LiveExact && publication.retained_version.is_some())
    {
        return Err(ReadError::InvalidData);
    }
    Ok(())
}

pub(crate) fn validate_wills(store: &VolatileStore) -> Result<(), ReadError> {
    const MAX_WILL_RECEIPT_BYTES: usize = 8 * 1024 * 1024;
    if store.open_will_receipt_bytes > MAX_WILL_RECEIPT_BYTES {
        return Err(ReadError::LimitExceeded);
    }
    if store.wills.len() > MAX_WILL_INTENTS {
        return Err(ReadError::LimitExceeded);
    }
    let mut bytes = 0u64;
    for (id, intent) in &store.wills {
        if intent.client_id.len() > mqttiumd_contract::session::MAX_SESSION_CLIENT_BYTES
            || intent.spec.topic.len() > MAX_WILL_TOPIC_BYTES
            || intent.spec.payload.len() > MAX_WILL_PAYLOAD_BYTES
        {
            return Err(ReadError::LimitExceeded);
        }
        if *id != intent.id
            || id.incarnation() != store.ledger.incarnation()
            || id.sequence().get() == 0
            || id.epoch().get() == 0
            || id.epoch() > store.ledger.last_opened()
            || intent.session.get() == 0
            || intent.session >= store.next_session
            || intent.client_id.is_empty()
            || intent.client_id.as_slice().contains(&0)
            || std::str::from_utf8(intent.client_id.as_slice()).is_err()
            || intent.spec.check().is_err()
        {
            return Err(ReadError::InvalidData);
        }
        bytes = bytes
            .checked_add(intent.spec.payload.len() as u64)
            .ok_or(ReadError::LimitExceeded)?;
        if bytes > MAX_WILL_BYTES {
            return Err(ReadError::LimitExceeded);
        }
        if intent.state == WillState::Armed
            && !store.sessions.get(&intent.session).is_some_and(|s| {
                s.state == SessionState::Active
                    && store.sessions.opened_by(intent.session, s) == Some(*id)
                    && s.client_id.as_slice() == intent.client_id.as_slice()
                    && s.detached.is_none()
            })
        {
            return Err(ReadError::InvalidData);
        }
    }
    Ok(())
}

pub(crate) fn validate_retained(
    store: &VolatileStore,
    budget: ReadBudget,
) -> Result<(), ReadError> {
    if store.retained.len() > MAX_RETAINED_TOPICS {
        return Err(ReadError::LimitExceeded);
    }
    let mut total = 0u64;
    let mut meter = ReadMeter::new(budget);
    for (topic, record) in &store.retained {
        meter.field(
            topic.len(),
            store.limits.max_topic_bytes.min(usize::from(u16::MAX)),
        )?;
        meter.field(record.producer_client_id.len(), MAX_CLIENT_ID_BYTES)?;
        meter.charge(
            256usize
                .checked_add(topic.len())
                .and_then(|n| n.checked_add(record.producer_client_id.len()))
                .ok_or(ReadError::LimitExceeded)?,
        )?;
        let publication = store
            .publications
            .get(&record.publication)
            .ok_or(ReadError::InvalidData)?;
        if let Some(expiry) = publication.expiry() {
            if expiry.observed.domain == [0; 32] {
                return Err(ReadError::InvalidData);
            }
            expiry.deadline().map_err(|_| ReadError::InvalidData)?;
        }
        let payload = store
            .payloads
            .get(&publication.payload)
            .ok_or(ReadError::InvalidData)?;
        if topic.is_empty()
            || topic.contains(&0)
            || std::str::from_utf8(topic).is_err()
            || topic.iter().any(|b| matches!(*b, b'+' | b'#'))
            || record.producer_client_id.is_empty()
            || record.producer_client_id.contains(&0)
            || std::str::from_utf8(&record.producer_client_id).is_err()
            || publication.topic.as_slice() != topic.as_slice()
            || !publication.retain
            || publication.retained_version.is_some()
            || payload.bytes.is_empty()
            || payload.references == 0
        {
            return Err(ReadError::InvalidData);
        }
        if payload.bytes.len() > store.limits.max_payload_bytes {
            return Err(ReadError::LimitExceeded);
        }
        total = total
            .checked_add(payload.bytes.len() as u64)
            .ok_or(ReadError::LimitExceeded)?;
        if total > MAX_RETAINED_BYTES {
            return Err(ReadError::LimitExceeded);
        }
    }
    Ok(())
}

pub(crate) fn check_inventory_bound(store: &VolatileStore) -> Result<(), ReadError> {
    validate_wills(store)?;
    for publication in store.publications.values() {
        if let Some(expiry) = publication.expiry() {
            if expiry.observed.domain == [0; 32] {
                return Err(ReadError::InvalidData);
            }
            expiry.deadline().map_err(|_| ReadError::InvalidData)?;
        }
    }
    for (&id, row) in store.sessions.iter() {
        let opened_by = store.sessions.opened_by(id, row);
        if matches!(row.expiry, SessionExpiry::After(_)) && store.session_clock.is_none() {
            return Err(ReadError::InvalidData);
        }
        if let SessionExpiry::After(seconds) = row.expiry
            && (seconds == 0 || seconds == u32::MAX)
        {
            return Err(ReadError::InvalidData);
        }
        if let Some(detached) = row.detached()
            && (opened_by.is_none()
                || detached.operation.incarnation() != store.ledger.incarnation()
                || detached.operation.epoch().get() == 0
                || detached.operation.epoch() > store.ledger.last_opened()
                || !detached.valid_for(row.expiry))
        {
            return Err(ReadError::InvalidData);
        }
        if opened_by.is_some_and(|id| {
            id.incarnation() != store.ledger.incarnation()
                || id.epoch().get() == 0
                || id.epoch() > store.ledger.last_opened()
        }) {
            return Err(ReadError::InvalidData);
        }
    }
    if store.receipt_frontiers.len() > MAX_RECEIPT_ORIGINS {
        return Err(ReadError::LimitExceeded);
    }
    for (origin, closure) in &store.receipt_frontiers {
        if *origin != closure.origin
            || origin.incarnation != store.ledger.incarnation()
            || origin.epoch.get() == 0
            || origin.epoch > store.ledger.last_opened()
        {
            return Err(ReadError::InvalidData);
        }
    }
    let mut rows = 3usize;
    for size in [
        store.routes.len(),
        store.sessions.len(),
        store.inbound.len(),
        store.obligations.len(),
        store.outbound.len(),
        store.operations.len(),
        store.fences.len(),
        store.receipt_frontiers.len(),
    ] {
        if size > MAX_RECOVERY_INVENTORY_ROWS {
            return Err(ReadError::LimitExceeded);
        }
    }
    for size in [
        store.sessions.len(),
        store.inbound.len(),
        store.obligations.len(),
        store.operations.len(),
        store.fences.len(),
        store.receipt_frontiers.len(),
    ] {
        rows = rows.checked_add(size).ok_or(ReadError::LimitExceeded)?;
    }
    for (filter, targets) in &store.routes {
        if filter.len() > store.limits.max_topic_bytes {
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
    for (delivery, _) in store.outbound.iter() {
        let record = store
            .obligations
            .get(&delivery)
            .ok_or(ReadError::InvalidData)?;
        delivery_claim(store, delivery, record)?;
    }
    Ok(())
}

pub(crate) fn check_message_observation(
    store: &VolatileStore,
    now: MessageTime,
) -> Result<(), ReadError> {
    let anchor = store.session_clock.ok_or(ReadError::InvalidData)?;
    if now.domain == [0; 32] || now.domain != anchor.reading.domain {
        return Err(ReadError::InvalidData);
    }
    Ok(())
}

pub(crate) fn check_message_candidate(
    store: &VolatileStore,
    id: PublicationId,
) -> Result<(), ReadError> {
    let publication = store.publications.get(&id).ok_or(ReadError::InvalidData)?;
    if let Some(expiry) = publication.expiry() {
        if expiry.observed.domain == [0; 32] {
            return Err(ReadError::InvalidData);
        }
        expiry.deadline().map_err(|_| ReadError::InvalidData)?;
    }
    let topic = publication.topic.as_slice();
    if topic.is_empty()
        || topic.len() > store.limits.max_topic_bytes
        || topic.contains(&0)
        || topic.iter().any(|b| matches!(b, b'+' | b'#'))
        || std::str::from_utf8(topic).is_err()
    {
        return Err(ReadError::InvalidData);
    }
    let mut matching_obligations: Vec<(DeliveryId, &ObligationRecord)> = store
        .obligations
        .iter()
        .filter(|(delivery, _)| delivery.publication == id)
        .collect();
    matching_obligations.sort_unstable_by_key(|(delivery, _)| *delivery);
    let mut references = 0u64;
    for (delivery, record) in matching_obligations {
        references = references.checked_add(1).ok_or(ReadError::LimitExceeded)?;
        if references > MESSAGE_EXPIRY_SWEEP_DELIVERIES as u64 {
            return Err(ReadError::LimitExceeded);
        }
        check_delivery_metadata(store, delivery, record)?;
        delivery_claim(store, delivery, record)?;
    }
    if !store.validated {
        for (delivery, _) in store.outbound.iter() {
            if delivery.publication == id && !store.obligations.contains_key(&delivery) {
                return Err(ReadError::InvalidData);
            }
        }
    }
    if store.retained.len() > MAX_RETAINED_TOPICS {
        return Err(ReadError::LimitExceeded);
    }
    let mut retained = false;
    if store.validated {
        if let Some(rtopic) = store.retained_by_pub.get(&id)
            && let Some(value) = store.retained.get(rtopic)
        {
            if !publication.retain
                || publication.retained_version.is_some()
                || rtopic.as_slice() != topic
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
    } else {
        for (rtopic, value) in &store.retained {
            if value.publication != id {
                continue;
            }
            if retained
                || !publication.retain
                || publication.retained_version.is_some()
                || rtopic.as_slice() != topic
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
    }
    if references > 0 {
        let payload = store
            .payloads
            .get(&publication.payload)
            .ok_or(ReadError::InvalidData)?;
        if payload.references < references
            || payload.references > store.limits.references.get()
            || payload.bytes.len() > store.limits.max_payload_bytes
            || retained && payload.bytes.is_empty()
        {
            return Err(ReadError::InvalidData);
        }
    }
    Ok(())
}

pub(crate) fn will_page(
    store: &VolatileStore,
    after: Option<OperationId>,
    limit: usize,
    budget: ReadBudget,
) -> Result<WillPage, ReadError> {
    if !store.validated {
        validate_wills(store)?;
    }
    if limit == 0 || limit > MAX_WILL_INTENTS || limit > budget.max_rows {
        return Err(ReadError::LimitExceeded);
    }
    let mut items = BoundedVec::with_capacity(limit).map_err(|_| ReadError::AllocationFailed)?;
    let mut meter = ReadMeter::new(budget);
    let mut more = false;
    let range = (
        after.map_or(Bound::Unbounded, Bound::Excluded),
        Bound::Unbounded,
    );
    for (_, intent) in store.wills.range(range) {
        if intent.state != WillState::Pending {
            continue;
        }
        if items.len() == limit {
            more = true;
            break;
        }
        let bytes = 128usize
            .checked_add(intent.client_id.len())
            .and_then(|n| n.checked_add(intent.spec.topic.len()))
            .and_then(|n| n.checked_add(intent.spec.payload.len()))
            .ok_or(ReadError::LimitExceeded)?;
        for field in [&intent.client_id, &intent.spec.topic, &intent.spec.payload] {
            budget.check_field(field.len())?;
        }
        meter.charge(bytes)?;
        items
            .try_push(intent.clone())
            .map_err(|_| ReadError::AllocationFailed)?;
    }
    let next = more
        .then(|| items.as_slice().last().map(|i| i.id))
        .flatten();
    Ok(WillPage { items, next })
}

pub(crate) fn work_page(
    store: &VolatileStore,
    session: SessionId,
    after: Option<DeliveryId>,
    limit: usize,
    budget: ReadBudget,
) -> Result<WorkPage, ReadError> {
    if limit == 0 || limit > WORK_PAGE_LIMIT as usize {
        return Err(ReadError::LimitExceeded);
    }
    if !store.validated {
        check_inventory_bound(store)?;
        for (delivery, record) in store.obligations.iter() {
            if record.target == session {
                check_delivery_metadata(store, delivery, record)?;
            }
        }
    }
    let mut meter = ReadMeter::new(budget);
    let mut items = BoundedVec::with_capacity(limit).map_err(|_| ReadError::AllocationFailed)?;
    let mut more = false;
    if let Some(set) = store.obligations_by_target.get(&session) {
        for delivery in set.iter_after(after) {
            if items.len() == limit {
                more = true;
                break;
            }
            let record = store
                .obligations
                .get(&delivery)
                .ok_or(ReadError::InvalidData)?;
            let publication = store
                .publications
                .get(&delivery.publication)
                .ok_or(ReadError::InvalidData)?;
            let payload = store
                .payloads
                .get(&publication.payload)
                .ok_or(ReadError::InvalidData)?;
            meter.field(publication.topic.len(), store.limits.max_topic_bytes)?;
            meter.field(payload.bytes.len(), store.limits.max_payload_bytes)?;
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
            if !store.validated {
                check_delivery_metadata(store, delivery, record)?;
            }
            let claim = delivery_claim(store, delivery, record)?;
            items
                .try_push(WorkItem {
                    delivery,
                    target: record.target,
                    target_revision: record.target_revision,
                    qos: record.qos,
                    cause: record.cause,
                    retain: record.retain,
                    expiry: publication.expiry(),
                    properties: publication.properties.clone(),
                    // Zero-copy Arc clone on the hot delivery path!
                    topic: publication.topic.clone(),
                    payload: payload.bytes.clone(),
                    claim,
                })
                .map_err(|_| ReadError::LimitExceeded)?;
        }
    }
    let next = if more {
        items.as_slice().last().map(|item| item.delivery)
    } else {
        None
    };
    Ok(WorkPage { items, next })
}

enum FactRef<'a> {
    Epoch(
        mqttiumd_contract::ids::StoreIncarnation,
        mqttiumd_contract::ids::RunEpoch,
    ),
    Limits,
    Table,
    Route(&'a [u8], &'a RouteTarget),
    Session(SessionId, &'a VSession),
    Inbound(mqttiumd_contract::ids::InboundPacketId, &'a InboundRecord),
    Operation(OperationId, &'a crate::VOperation),
    Fence(OperationId),
    Outbound(DeliveryId, &'a ObligationRecord),
    ReceiptFrontier(ReceiptClosure),
}

impl FactRef<'_> {
    fn build(
        self,
        store: &VolatileStore,
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
                max_targets_per_admission: u32::try_from(store.limits.max_targets_per_admission)
                    .map_err(|_| ReadError::LimitExceeded)?,
                max_topic_bytes: u32::try_from(store.limits.max_topic_bytes)
                    .map_err(|_| ReadError::LimitExceeded)?,
                max_payload_bytes: PayloadBytes::new(
                    u64::try_from(store.limits.max_payload_bytes)
                        .map_err(|_| ReadError::LimitExceeded)?,
                ),
            },
            Self::Table => RecoveredFact::RouteTable {
                revision: store.route_revision,
            },
            Self::Route(topic, target) => {
                meter.field(topic.len(), store.limits.max_topic_bytes)?;
                let session = store
                    .sessions
                    .get(&target.session)
                    .filter(|record| record.state == SessionState::Active)
                    .ok_or(ReadError::InvalidData)?;
                RecoveredFact::Route {
                    topic: SharedBytes::copy_from(topic, store.limits.max_topic_bytes)
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
                    client_id: record.client_id.clone(),
                    revision: record.revision,
                    expiry: record.expiry,
                    opened_by: store.sessions.opened_by(session, record),
                    detached: record.detached(),
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
                let claim = delivery_claim(store, delivery, record)?;
                RecoveredFact::OutboundDelivery {
                    delivery,
                    target: record.target,
                    target_revision: record.target_revision,
                    qos: record.qos,
                    expiry: store
                        .publications
                        .get(&delivery.publication)
                        .ok_or(ReadError::InvalidData)?
                        .expiry(),
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

fn inventory(store: &VolatileStore) -> impl Iterator<Item = FactRef<'_>> {
    let incarnation = store.ledger.incarnation();
    let open = store.ledger.open_epoch();
    let older = move |operation: &OperationId| {
        Some(operation.epoch()) != open
            && !(operation.epoch().get() != 0 && operation.epoch() <= store.collected_through)
            && !store
                .receipt_frontiers
                .get(&ReceiptOrigin::of(*operation))
                .is_some_and(|closure| operation.sequence() <= closure.through)
    };
    let epoch = open.map(|epoch| FactRef::Epoch(incarnation, epoch));
    let routes = store.routes.iter().flat_map(|(topic, targets)| {
        targets
            .iter()
            .map(move |target| FactRef::Route(topic, target))
    });
    let mut sessions: Vec<(SessionId, &VSession)> = store
        .sessions
        .iter()
        .filter(|(_, record)| record.state == SessionState::Active)
        .map(|(session, record)| (*session, record))
        .collect();
    sessions.sort_unstable_by_key(|(id, _)| *id);
    let sessions = sessions
        .into_iter()
        .map(|(session, record)| FactRef::Session(session, record));
    let inbound = store
        .inbound
        .iter()
        .map(|((_, packet), marker)| FactRef::Inbound(*packet, marker));
    let mut obligations: Vec<(DeliveryId, &ObligationRecord)> = store.obligations.iter().collect();
    obligations.sort_unstable_by_key(|(id, _)| *id);
    let obligations = obligations
        .into_iter()
        .map(|(id, record)| FactRef::Outbound(id, record));
    let operations = store
        .operations
        .iter_ordered()
        .into_iter()
        .filter(move |(operation, _)| older(operation))
        .map(|(operation, record)| FactRef::Operation(operation, record));
    let fences = store
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
        .chain(obligations)
        .chain(
            store
                .receipt_frontiers
                .values()
                .copied()
                .map(FactRef::ReceiptFrontier),
        )
        .chain(operations)
        .chain(fences)
}

pub(crate) fn recover_page(
    store: &VolatileStore,
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
    check_inventory_bound(store).map_err(refused)?;
    let revision = store.revision;
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
    let total = inventory(store).count();
    if start > total {
        return Err(refused(ReadError::InvalidData));
    }
    let mut facts =
        BoundedVec::with_capacity(limit).map_err(|_| refused(ReadError::AllocationFailed))?;
    let mut meter = ReadMeter::new(budget);
    let mut iter = inventory(store).skip(start);
    let mut position = start;
    while !facts.is_full() {
        let Some(raw) = iter.next() else {
            return Ok(RecoveryPage { facts, next: None });
        };
        let fact = raw.build(store, &mut meter).map_err(refused)?;
        facts
            .try_push(fact)
            .map_err(|_| refused(ReadError::LimitExceeded))?;
        position = position
            .checked_add(1)
            .ok_or_else(|| refused(ReadError::LimitExceeded))?;
    }
    let next = iter
        .next()
        .is_some()
        .then_some(RecoveryCursor::new(revision, position));
    Ok(RecoveryPage { facts, next })
}

pub(crate) fn build_snapshot(
    store: &VolatileStore,
    budget: ReadBudget,
) -> Result<DurableState, ReadError> {
    check_inventory_bound(store)?;
    validate_retained(store, budget)?;
    let mut meter = ReadMeter::new(budget);
    budget.check_field(8)?;
    if store.ledger.opened_by().is_some() {
        budget.check_field(24)?;
    }
    meter.charge(256)?;
    if store.session_clock.is_some() {
        budget.check_field(32)?;
        meter.charge(176)?;
    }
    for intent in store.wills.values() {
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
    for record in store.sessions.values() {
        meter.field(record.client_id.len(), MAX_CLIENT_ID_BYTES)?;
        meter.charge(
            160usize
                .checked_add(if record.detached.is_some() { 40 } else { 0 })
                .and_then(|n| n.checked_add(if record.has_opened_by() { 32 } else { 0 }))
                .and_then(|n| n.checked_add(record.client_id.len()))
                .ok_or(ReadError::LimitExceeded)?,
        )?;
    }
    for (topic, targets) in &store.routes {
        meter.field(topic.len(), store.limits.max_topic_bytes)?;
        for _ in targets {
            meter.charge(
                152usize
                    .checked_add(topic.len())
                    .ok_or(ReadError::LimitExceeded)?,
            )?;
        }
    }
    for record in store.operations.values() {
        let rlen = record.canonical_len as usize;
        meter.field(32, 32)?;
        meter.field(rlen, budget.max_field_bytes)?;
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
                .and_then(|n| n.checked_add(rlen))
                .ok_or(ReadError::LimitExceeded)?,
        )?;
    }
    for _ in &store.fences {
        meter.field(32, 32)?;
        meter.charge(160)?;
    }
    if store.receipt_frontiers.len() > MAX_RECEIPT_ORIGINS {
        return Err(ReadError::LimitExceeded);
    }
    for (origin, closure) in &store.receipt_frontiers {
        meter.field(8, 8)?;
        meter.charge(168)?;
        if *origin != closure.origin
            || origin.incarnation != store.ledger.incarnation()
            || origin.epoch.get() == 0
            || origin.epoch > store.ledger.last_opened()
        {
            return Err(ReadError::InvalidData);
        }
    }
    for (topic, record) in &store.retained {
        meter.field(topic.len(), store.limits.max_topic_bytes)?;
        meter.field(record.producer_client_id.len(), MAX_CLIENT_ID_BYTES)?;
        meter.charge(
            256usize
                .checked_add(topic.len())
                .and_then(|n| n.checked_add(record.producer_client_id.len()))
                .ok_or(ReadError::LimitExceeded)?,
        )?;
    }
    let mut publications = BTreeMap::new();
    for (id, record) in store.publications.iter() {
        let expiry = record.expiry();
        if let Some(expiry) = expiry {
            expiry.deadline().map_err(|_| ReadError::InvalidData)?;
        }
        meter.field(32, 32)?;
        meter.field(record.topic.len(), store.limits.max_topic_bytes)?;
        let props_bytes = record.properties.as_ref().map_or(0, |p| p.wire_len());
        if props_bytes > 0 {
            meter.field(
                props_bytes,
                mqttiumd_contract::properties::MAX_PROPERTY_BYTES + 1024,
            )?;
        }
        meter.charge(
            248usize
                .checked_add(if expiry.is_some() { 44 } else { 0 })
                .and_then(|n| n.checked_add(record.topic.len()))
                .and_then(|n| n.checked_add(props_bytes))
                .ok_or(ReadError::LimitExceeded)?,
        )?;
        publications.insert(
            id,
            PublicationRecord {
                expiry,
                properties: record.properties.clone(),
                retained_version: record.retained_version,
                operation: record.operation,
                source: record.source,
                topic: record.topic.as_slice().to_vec(),
                qos: record.qos,
                retain: record.retain,
                payload: record.payload,
            },
        );
    }
    let mut payloads = BTreeMap::new();
    for (id, record) in store.payloads.iter() {
        meter.field(32, 32)?;
        meter.field(record.bytes.len(), store.limits.max_payload_bytes)?;
        meter.charge(
            168usize
                .checked_add(record.bytes.len())
                .ok_or(ReadError::LimitExceeded)?,
        )?;
        payloads.insert(
            id,
            PayloadRecord {
                bytes: record.bytes.as_slice().to_vec(),
                references: record.references,
            },
        );
    }
    let mut obligations = BTreeMap::new();
    for (delivery, record) in store.obligations.iter() {
        check_delivery_metadata(store, delivery, record)?;
        meter.field(32, 32)?;
        meter.charge(208)?;
        obligations.insert(delivery, *record);
    }
    let mut outbound = BTreeMap::new();
    for (delivery, out) in store.outbound.iter() {
        meter.field(32, 32)?;
        meter.charge(232)?;
        let record = store
            .obligations
            .get(&delivery)
            .ok_or(ReadError::InvalidData)?;
        delivery_claim(store, delivery, record)?;
        outbound.insert(delivery, *out);
    }
    for _ in store.inbound.values() {
        meter.field(32, 32)?;
        meter.charge(200)?;
    }
    let mut operations = BTreeMap::new();
    for (op, record) in store.operations.iter_ordered() {
        operations.insert(
            op,
            OperationRecord {
                recipe: record.recipe.to_canonical_vec(),
                decision: record.decision,
            },
        );
    }
    let mut sessions = BTreeMap::new();
    for (&id, row) in store.sessions.iter() {
        sessions.insert(id, store.sessions.to_record(id, row));
    }
    Ok(DurableState {
        ledger: store.ledger,
        collected_through: store.collected_through,
        revision: store.revision,
        limits: store.limits,
        accounts: store.accounts,
        next_session: store.next_session,
        session_clock: store.session_clock,
        route_revision: store.route_revision,
        sessions,
        routes: store.routes.clone(),
        operations,
        fences: store.fences.clone(),
        receipt_frontiers: store.receipt_frontiers.clone(),
        retained: store.retained.clone(),
        wills: store.wills.clone(),
        publications,
        payloads,
        obligations,
        inbound: store.inbound.clone(),
        outbound,
    })
}
