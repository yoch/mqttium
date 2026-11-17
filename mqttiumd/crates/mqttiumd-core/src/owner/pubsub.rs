//! Hierarchical subscriptions and outgoing QoS 1 under the same owner authority.
use super::*;
use crate::contract::command::{
    ClaimDelivery, ConsumeDelivery, ExpireDelivery, FinishDelivery, MarkDeliveryStarted,
    UpdateSubscriptions,
};
use crate::contract::ids::{ContinuationId, OutboundPacketId};
use crate::contract::pubsub::{
    DeliveryClaim, FilterResults, GrantedSubscriptionChange, MAX_FILTERS, PubackTarget,
    Qos0DropReason, Qos0Origin, VolatilePublicationId, WORK_PAGE_LIMIT, WorkItem,
};

// Isolated transition mutants. Production builds retain these false values.
const MUTANT_EARLY_PI_RELEASE: bool = false;
const MUTANT_IGNORE_OCCUPIED_PI: bool = false;
const MUTANT_STALE_OUTGOING_FRAME_RESULT: bool = false;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) struct Read {
    effect: EffectId,
    session: SessionId,
    invalidated: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) struct Connection {
    generation: ConnectionGeneration,
    receive_maximum: u16,
    maximum_packet_size: u32,
    after: Option<DeliveryId>,
    loading: Option<Read>,
    rescan_needed: bool,
    last_qos0_ingress: u64,
}

#[derive(Debug)]
pub(super) struct Control {
    pub(super) session: SessionId,
    pub(super) connection: ConnectionKey,
    pub(super) packet_id: InboundPacketId,
    pub(super) packet: InboundPacket,
    pub(super) results: FilterResults,
    pub(super) projected: BoundedVec<Route>,
    pub(super) in_flight: InFlight,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) struct RetainedFlight {
    publication: PublicationId,
    origin: VolatilePublicationId,
    in_flight: InFlight,
    retired: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Phase {
    Queued,
    ClaimPending(InFlight),
    AwaitPuback,
    FinishPending(InFlight),
    StartPending(InFlight),
    ExpirePending(InFlight),
    StartRetry,
    ExpireRetry,
    ConsumePending(InFlight, ConnectionKey),
    Qos0Writing(Finished),
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub(super) struct Delivery {
    id: DeliveryId,
    target: SessionId,
    revision: Revision,
    qos: Qos,
    retain: bool,
    expiry: Option<MessageExpiry>,
    claim: Option<DeliveryClaim>,
    topic: Option<SharedBytes>,
    payload: Option<SharedBytes>,
    phase: Phase,
    claim_result: Option<Finished>,
    window: Option<ConnectionGeneration>,
    frame: Option<(EffectId, ConnectionKey)>,
    local_write_completed: bool,
    may_have_started: bool,
    /// Only a fresh claim in this process, with no earlier uncertain attempt,
    /// can turn an exact first-frame NotStarted observation into deletion.
    certain_never_started: bool,
    early_puback: bool,
    retired: bool,
}

impl Delivery {
    fn waiting_to_send(&self, generation: ConnectionGeneration, now: Option<MessageTime>) -> bool {
        if self.retired || !self.expiry_allows_send(now) {
            return false;
        }
        match self.phase {
            Phase::FinishPending(_)
            | Phase::StartPending(_)
            | Phase::ExpirePending(_)
            | Phase::StartRetry
            | Phase::ExpireRetry
            | Phase::Qos0Writing(_) => false,
            Phase::AwaitPuback => {
                self.window != Some(generation)
                    || self
                        .frame
                        .is_none_or(|(_, key)| key.generation != generation)
            }
            Phase::Queued | Phase::ClaimPending(_) | Phase::ConsumePending(_, _) => true,
        }
    }
    fn expiry_allows_send(&self, now: Option<MessageTime>) -> bool {
        self.claim.is_some_and(|claim| claim.started)
            || self.expiry.is_none_or(|expiry| {
                now.is_some_and(|now| matches!(expiry.state(now), Ok(ExpiryState::Fresh { .. })))
            })
    }
    fn control_ready(&self, now: Option<MessageTime>) -> bool {
        !self.retired
            && match self.phase {
                Phase::StartRetry => true,
                Phase::AwaitPuback
                    if self.early_puback && self.claim.is_some_and(|claim| claim.started) =>
                {
                    true
                }
                Phase::AwaitPuback | Phase::ExpireRetry => {
                    self.certain_never_started
                        && self.frame.is_none()
                        && !self.early_puback
                        && self.expiry.is_some_and(|expiry| {
                            now.is_some_and(|now| expiry.state(now) == Ok(ExpiryState::Expired))
                        })
                }
                _ => false,
            }
    }
    fn in_flight(&self) -> Option<InFlight> {
        match self.phase {
            Phase::ClaimPending(f)
            | Phase::FinishPending(f)
            | Phase::StartPending(f)
            | Phase::ExpirePending(f)
            | Phase::ConsumePending(f, _) => Some(f),
            _ => None,
        }
    }
    fn metadata(
        id: DeliveryId,
        target: SessionId,
        revision: Revision,
        qos: Qos,
        claim: Option<DeliveryClaim>,
    ) -> Self {
        Self {
            id,
            target,
            revision,
            qos,
            retain: false,
            expiry: None,
            claim,
            topic: None,
            payload: None,
            phase: if claim.is_some() {
                Phase::AwaitPuback
            } else {
                Phase::Queued
            },
            claim_result: None,
            window: None,
            frame: None,
            local_write_completed: false,
            may_have_started: claim.is_some(),
            certain_never_started: claim.is_none(),
            early_puback: false,
            retired: false,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub(super) struct Volatile {
    origin: Qos0Origin,
    target: SessionId,
    connection: Option<ConnectionKey>,
    topic: SharedBytes,
    payload: SharedBytes,
    expiry: Option<MessageExpiry>,
    frame: Option<EffectId>,
}

#[derive(Debug)]
pub(super) struct State {
    pub(super) connections: BoundedVec<Connection>,
    pub(super) deliveries: BoundedVec<Delivery>,
    pub(super) control: Option<Control>,
    pub(super) retained: Option<RetainedFlight>,
    payload_limit: PayloadBytes,
    pub(super) volatile: BoundedVec<Volatile>,
    pub(super) continuation: Option<ContinuationId>,
    pub(super) continuations_started: bool,
}

#[derive(Debug)]
pub(super) enum Change {
    RetainedSubmit {
        connection: usize,
        flight: RetainedFlight,
    },
    RetainedDone {
        finished: Finished,
    },
    Qos0Admit {
        connection: usize,
        ingress_order: u64,
        records: BoundedVec<Volatile>,
        continuation: Option<ContinuationId>,
    },
    Qos0Send {
        slot: usize,
        effect: EffectId,
        continuation: Option<ContinuationId>,
    },
    Qos0Drop {
        slot: usize,
        continuation: Option<ContinuationId>,
    },
    Qos0Done {
        slot: usize,
    },
    Qos0Idle,
    Consume {
        slot: usize,
        in_flight: InFlight,
        connection: ConnectionKey,
    },
    Consumed {
        slot: usize,
        finished: Finished,
        emission: Option<(EffectId, ConnectionKey)>,
    },

    Settings {
        index: Option<usize>,
        value: Connection,
    },
    Control(Control),
    ControlEnd {
        revision: Option<Revision>,
        finished: Finished,
    },
    Scan {
        connection: usize,
        read: Read,
    },
    Page {
        connection: usize,
        discard: bool,
        next: Option<DeliveryId>,
    },
    Claim {
        slot: usize,
        claim: DeliveryClaim,
        in_flight: InFlight,
        connection: usize,
    },
    Claimed {
        slot: usize,
        finished: Finished,
        emission: Option<(EffectId, ConnectionKey)>,
    },
    Emit {
        slot: usize,
        effect: EffectId,
        connection: ConnectionKey,
    },
    Finish {
        slot: usize,
        in_flight: InFlight,
    },
    Finished {
        slot: usize,
        finished: Finished,
    },
    Refused {
        slot: usize,
        finished: Finished,
        claiming: bool,
    },
    Await {
        operation: OperationId,
        phase: CommandPhase,
        resolution: Option<(DeadlineToken, EffectId)>,
    },
    Written {
        slot: usize,
    },
    PubackObserved {
        slot: usize,
    },
    Start {
        slot: usize,
        in_flight: InFlight,
    },
    Started {
        slot: usize,
        finished: Finished,
        finish: Option<InFlight>,
    },
    Expire {
        slot: usize,
        in_flight: InFlight,
    },
    NotStarted {
        slot: usize,
    },
    MessagesExpired,
    RetryDeferred {
        slot: usize,
        finished: Finished,
        expiring: bool,
    },
}

impl State {
    pub(super) fn retired_flights(&self) -> usize {
        self.deliveries
            .as_slice()
            .iter()
            .filter(|d| d.retired && d.in_flight().is_some())
            .count()
            + usize::from(self.retained.is_some_and(|flight| flight.retired))
    }
    pub(super) fn retire_session(&mut self, session: SessionId) {
        if let Some(flight) = self
            .retained
            .as_mut()
            .filter(|f| f.origin.source.session == session)
        {
            flight.retired = true;
        }
        let mut index = 0;
        while index < self.deliveries.len() {
            let d = &mut self.deliveries.as_mut_slice()[index];
            if d.target != session {
                index += 1;
                continue;
            }
            if d.in_flight().is_some() || matches!(d.phase, Phase::Qos0Writing(_)) {
                d.retired = true;
                d.window = None;
                d.topic = None;
                d.payload = None;
                index += 1;
            } else {
                let last = self.deliveries.len() - 1;
                self.deliveries.as_mut_slice().swap(index, last);
                let _ = self.deliveries.pop();
            }
        }
        // Volatile attempts retain their existing callback/continuation:
        // that bounded path records their explicit closed-destination drop.
    }
    fn reserves_packet_id(delivery: &Delivery, session: SessionId) -> bool {
        delivery.target == session
            && delivery.claim.is_some()
            && !(MUTANT_EARLY_PI_RELEASE && matches!(delivery.phase, Phase::FinishPending(_)))
    }

    /// Packet Identifiers are recyclable target-local resources. Every
    /// retained claim reserves its number, including unknown submissions,
    /// unconfirmed finishes and recovered metadata without loaded bytes.
    /// Among N live claims, a free value is found in the first N+1 values
    /// or the finite domain is genuinely full. No traffic-history scan.
    fn free_packet_id(&self, session: SessionId, maximum: u16) -> Option<OutboundPacketId> {
        let occupied = self
            .deliveries
            .as_slice()
            .iter()
            .filter(|delivery| Self::reserves_packet_id(delivery, session))
            .count();
        let candidates = occupied.saturating_add(1).min(usize::from(maximum));
        for raw in 1..=candidates {
            let packet_id = u16::try_from(raw).ok().and_then(OutboundPacketId::new)?;
            if MUTANT_IGNORE_OCCUPIED_PI
                || !self.deliveries.as_slice().iter().any(|delivery| {
                    Self::reserves_packet_id(delivery, session)
                        && delivery
                            .claim
                            .is_some_and(|claim| claim.packet_id == packet_id)
                })
            {
                return Some(packet_id);
            }
        }
        None
    }

    pub(super) fn work_bound(&self, input: &Input, routes: usize) -> u64 {
        if let Input::MessagesExpired { deliveries } = input {
            let count = capacity_of(deliveries.len());
            return capacity_of(self.deliveries.capacity())
                .saturating_mul(count.saturating_add(1))
                .saturating_add(count.saturating_mul(count));
        }
        let new = matches!(
            input,
            Input::ObservedPuback { .. }
                | Input::ConnectionSettings { .. }
                | Input::WorkPage { .. }
                | Input::Packet {
                    packet: InboundPacket::Subscribe { .. }
                        | InboundPacket::Unsubscribe { .. }
                        | InboundPacket::Puback { .. },
                    ..
                }
        );
        if !new
            && self.connections.is_empty()
            && self.control.is_none()
            && self.volatile.is_empty()
            && self.deliveries.is_empty()
        {
            return 0;
        }
        let slots = capacity_of(self.deliveries.capacity());
        let connection_work = capacity_of(self.connections.capacity());
        if let Input::WorkPage { items, .. } = input {
            let count = capacity_of(items.len());
            return connection_work
                .saturating_add(slots.saturating_mul(count.saturating_add(1)))
                .saturating_add(count.saturating_mul(count));
        }
        let filters = match input {
            Input::Packet {
                packet: InboundPacket::Subscribe { filters, .. },
                ..
            } => capacity_of(filters.len()),
            Input::Packet {
                packet: InboundPacket::Unsubscribe { filters, .. },
                ..
            } => capacity_of(filters.len()),
            _ => 0,
        };
        // Packet allocation skips at most the bounded number of restored
        // claims. Each candidate is checked against that same table.
        connection_work
            .saturating_add(slots.saturating_mul(slots.saturating_add(4)))
            .saturating_add(capacity_of(routes).saturating_mul(filters))
            .saturating_add(filters.saturating_mul(filters))
    }
    pub(super) fn new(config: OwnerConfig) -> Result<Self, AllocationFailed> {
        Ok(Self {
            connections: table(config.max_sessions)?,
            deliveries: table(config.max_exchanges)?,
            control: None,
            retained: None,
            payload_limit: config.payload_bytes,
            volatile: table(config.max_exchanges)?,
            continuation: None,
            continuations_started: false,
        })
    }
    fn connection(&self, generation: ConnectionGeneration) -> Option<usize> {
        self.connections
            .as_slice()
            .iter()
            .position(|c| c.generation == generation)
    }
    fn delivery(&self, id: DeliveryId) -> Option<usize> {
        self.deliveries.as_slice().iter().position(|d| d.id == id)
    }
    fn active(&self, generation: ConnectionGeneration) -> usize {
        self.deliveries
            .as_slice()
            .iter()
            .filter(|d| d.window == Some(generation))
            .count()
    }
    fn loaded_bytes(&self) -> Option<u64> {
        self.deliveries.as_slice().iter().try_fold(0u64, |sum, d| {
            sum.checked_add(d.payload.as_ref().map_or(0, |p| p.len() as u64))
        })
    }
    pub(super) fn evict_expired_cache(&mut self, now: MessageTime) {
        let mut index = 0;
        while index < self.deliveries.len() {
            let d = &self.deliveries.as_slice()[index];
            if d.claim.is_none()
                && d.phase == Phase::Queued
                && d.expiry
                    .is_some_and(|expiry| expiry.state(now) == Ok(ExpiryState::Expired))
            {
                let last = self.deliveries.len() - 1;
                self.deliveries.as_mut_slice().swap(index, last);
                let _ = self.deliveries.pop();
            } else {
                index += 1;
            }
        }
    }
    pub(super) fn has_control_work(&self, now: Option<MessageTime>) -> bool {
        self.deliveries
            .as_slice()
            .iter()
            .any(|d| d.control_ready(now))
    }
    pub(super) fn close_work(&self) -> u64 {
        if self.connections.is_empty() {
            0
        } else {
            capacity_of(self.connections.capacity())
                .saturating_add(capacity_of(self.deliveries.capacity()))
        }
    }
    pub(super) fn close(&mut self, generation: ConnectionGeneration) {
        if let Some(index) = self.connection(generation) {
            let last = self.connections.len() - 1;
            self.connections.as_mut_slice().swap(index, last);
            let _ = self.connections.pop();
        }
        for delivery in self.deliveries.as_mut_slice() {
            if delivery.window == Some(generation) {
                delivery.window = None;
                if delivery.frame.is_some() {
                    delivery.certain_never_started = false;
                }
            }
        }
    }
    pub(super) fn deadline_is_live(&self, token: DeadlineToken) -> bool {
        self.retained.is_some_and(|f| f.in_flight.deadline == token)
            || self
                .control
                .as_ref()
                .is_some_and(|c| c.in_flight.deadline == token)
            || self
                .deliveries
                .as_slice()
                .iter()
                .filter_map(Delivery::in_flight)
                .any(|f| f.deadline == token)
    }

    pub(super) fn known(&self, operation: OperationId) -> Option<Finished> {
        self.deliveries
            .as_slice()
            .iter()
            .filter_map(|d| d.claim_result)
            .find(|f| f.operation == operation)
    }
    pub(super) fn control_pending(&self) -> bool {
        self.control.is_some()
    }
    fn invalidate_reads(&mut self, session: SessionId) {
        for c in self.connections.as_mut_slice() {
            if let Some(read) = &mut c.loading
                && read.session == session
            {
                read.invalidated = true;
                c.after = None;
            }
        }
    }
}

fn changed(change: Change) -> super::Change {
    super::Change::Pubsub(change)
}
fn blocked() -> Stop {
    Stop::Need(CapacityNeed {
        resource: Resource::ResultSlots,
        amount: 1,
        wake: WakeCondition::ResultDelivered,
    })
}

impl Owner {
    pub(super) fn retained_work_bound(&self, input: &Input) -> u64 {
        if self.lifecycle_enabled
            && matches!(
                input,
                Input::Packet {
                    packet: InboundPacket::Subscribe { .. },
                    ..
                }
            )
        {
            crate::contract::retained::MAX_REPLAYS as u64
        } else {
            0
        }
    }

    /// Whether a bounded work wake-up can advance already known online work.
    /// Empty scans are deliberately excluded; external notifications and the
    /// periodic fallback supply the authority to start a new discovery scan.
    #[must_use]
    pub fn has_ready_work(&self, session: SessionId) -> bool {
        if self.recovery.is_some() {
            return false;
        }
        if self.receipts.control_available()
            && self
                .pubsub
                .deliveries
                .as_slice()
                .iter()
                .any(|d| d.target == session && d.control_ready(self.message_now))
        {
            return true;
        }
        let Some(generation) = self.connection_of(session) else {
            return false;
        };
        let Some(index) = self.pubsub.connection(generation) else {
            return false;
        };
        let settings = self.pubsub.connections.as_slice()[index];
        let room = self.pubsub.active(generation) < usize::from(settings.receive_maximum);
        let next = self
            .pubsub
            .deliveries
            .as_slice()
            .iter()
            .filter(|d| d.target == session && d.waiting_to_send(generation, self.message_now))
            .min_by_key(|d| d.id);
        if let Some(delivery) = next {
            match delivery.phase {
                Phase::ClaimPending(_) | Phase::ConsumePending(_, _) => return false,
                Phase::Queued
                    if (room || delivery.qos == Qos::AtMostOnce) && delivery.payload.is_some() =>
                {
                    return true;
                }
                Phase::AwaitPuback
                    if delivery.payload.is_some()
                        && (room || delivery.window == Some(generation)) =>
                {
                    return true;
                }
                _ => {}
            }
        }
        room && settings.loading.is_none() && (settings.after.is_some() || settings.rescan_needed)
    }

    fn qos0_continuation(&self, draft: &mut Draft) -> Result<ContinuationId, Stop> {
        let continuation = draw(
            &mut draft.counters.continuation,
            ContinuationId::checked_next,
        )?;
        draft.emit(Effect::Continue { continuation })?;
        Ok(continuation)
    }

    pub(super) fn plan_retained_qos0(
        &self,
        session: usize,
        origin: (ConnectionGeneration, u64),
        packet: &InboundPacket,
        draft: &mut Draft,
    ) -> Result<super::Change, Stop> {
        let (generation, ingress_order) = origin;
        let Some(connection) = self.pubsub.connection(generation) else {
            return Err(IMPOSSIBLE);
        };
        let InboundPacket::Publish {
            qos: Qos::AtMostOnce,
            retain: true,
            dup: false,
            packet_id: None,
            topic,
            payload,
            expiry,
        } = packet
        else {
            return Self::plan_close(generation, Some(session), CloseReason::ProtocolError, draft);
        };
        if ingress_order <= self.pubsub.connections.as_slice()[connection].last_qos0_ingress {
            return Self::plan_close(generation, Some(session), CloseReason::ProtocolError, draft);
        }
        if self.pubsub.retained.is_some() {
            return Err(blocked());
        }
        let limits = self.limits.ok_or(IMPOSSIBLE)?;
        if topic.len() as u64 > u64::from(limits.max_topic_bytes)
            || payload.len() as u64 > limits.max_payload_bytes.get()
        {
            return Self::plan_close(
                generation,
                Some(session),
                CloseReason::CapacityExceeded,
                draft,
            );
        }
        let source = &self.sessions.as_slice()[session];
        let selected = match super::routing::select(
            self.routes.as_slice(),
            self.sessions.as_slice(),
            source.id,
            topic.as_slice(),
            Qos::AtMostOnce,
            limits.max_targets,
        ) {
            Ok(selected) => selected,
            Err(super::routing::Refusal::Revision) => return Err(IMPOSSIBLE),
            Err(super::routing::Refusal::Capacity) => {
                draft.emit(Effect::Qos0Dropped {
                    origin: Qos0Origin::Volatile {
                        publication: VolatilePublicationId {
                            source: ConnectionKey {
                                session: source.id,
                                generation,
                            },
                            ingress_order,
                        },
                    },
                    target: None,
                    reason: Qos0DropReason::CapacityExceeded,
                })?;
                return Ok(changed(Change::Qos0Admit {
                    connection,
                    ingress_order,
                    records: BoundedVec::with_capacity(0)?,
                    continuation: None,
                }));
            }
        };
        let operation = self.operation(draft)?;
        let request = RequestId::new(
            self.incarnation,
            self.epoch,
            self.id,
            draw(&mut draft.counters.request, RequestSeq::checked_next)?,
        );
        let publication = PublicationId::new(
            self.incarnation,
            self.epoch,
            self.id,
            draw(
                &mut draft.counters.publication,
                PublicationSeq::checked_next,
            )?,
        );
        let payload_id = PayloadId::new(
            self.incarnation,
            self.epoch,
            self.id,
            draw(&mut draft.counters.payload, PayloadSeq::checked_next)?,
        );
        let mut targets = BoundedVec::with_capacity(selected.len())?;
        let mut retain_targets = 0u16;
        for (index, target) in selected.iter().enumerate() {
            targets
                .try_push(Target {
                    session: target.session,
                    session_revision: target.revision,
                    delivery: DeliveryId {
                        publication,
                        index: u32::try_from(index).map_err(|_| IMPOSSIBLE)?,
                    },
                    effective_qos: Qos::AtMostOnce,
                    cause: DeliveryCause::LiveExact,
                })
                .map_err(|_| IMPOSSIBLE)?;
            if target.rap {
                retain_targets |= 1u16 << index;
            }
        }
        let retained = crate::contract::retained::RetainedQos0 {
            operation,
            request,
            publication,
            session: source.id,
            session_revision: source.revision,
            connection: generation,
            ingress_order,
            route_revision: self.route_revision.ok_or(IMPOSSIBLE)?,
            topic: topic.clone(),
            payload: PayloadRef {
                id: payload_id,
                length: PayloadBytes::new(payload.len() as u64),
            },
            costs: Costs {
                payload_bytes: PayloadBytes::new(payload.len() as u64),
                references: References::new(
                    (selected.len() as u64)
                        .checked_add(u64::from(!payload.is_empty()))
                        .ok_or(IMPOSSIBLE)?,
                ),
                exchange_slots: ExchangeSlots::ZERO,
                receipt_slots: ReceiptSlots::new(1),
            },
            targets,
            retain_targets,
        };
        let recipe = if let Some(expiry) = expiry {
            Recipe::ExpiringPublication(crate::contract::command::ExpiringPublication {
                admission: crate::contract::command::ExpiringAdmission::Qos0(retained),
                expiry: *expiry,
            })
        } else {
            Recipe::RetainedQos0(retained)
        };
        let in_flight = self.plan_submission(operation, recipe, Some(payload.clone()), draft)?;
        Ok(changed(Change::RetainedSubmit {
            connection,
            flight: RetainedFlight {
                publication,
                origin: VolatilePublicationId {
                    source: ConnectionKey {
                        session: source.id,
                        generation,
                    },
                    ingress_order,
                },
                in_flight,
                retired: false,
            },
        }))
    }
    fn plan_retained_qos0_result(
        &self,
        effect: EffectId,
        operation: OperationId,
        reply: &StoreReply,
        draft: &mut Draft,
    ) -> Result<Option<super::Change>, Stop> {
        let Some(flight) = self
            .pubsub
            .retained
            .filter(|f| f.in_flight.operation == operation)
        else {
            return Ok(None);
        };
        if !flight.in_flight.answers.names(effect) {
            return Ok(Some(super::Change::Nothing));
        }
        let outcome = match reply {
            StoreReply::Outcome(outcome) => *outcome,
            StoreReply::Violation(StoreViolation::RecipeMismatch) => {
                return Err(Stop::Fatal(CoreFault::RecipeMismatch { operation }));
            }
        };
        let verdict = flight
            .in_flight
            .phase
            .on(CommandEvent::StoreResult(OutcomeClass::of(&outcome)));
        let (phase, facts) = match (verdict, outcome) {
            (Verdict::Transition(CommandPhase::Committed), StoreOutcome::Committed(facts)) => {
                if facts
                    != (CommitFacts::RetainedQos0Stored {
                        publication: flight.publication,
                    })
                {
                    return Err(Stop::Fatal(CoreFault::ContradictoryOutcome { operation }));
                }
                (CommandPhase::Consumed, Some(facts))
            }
            (
                Verdict::Transition(CommandPhase::RejectedCertain),
                StoreOutcome::RejectedCertain(reason),
            ) => {
                draft.emit(Effect::Qos0Dropped {
                    origin: Qos0Origin::Volatile {
                        publication: flight.origin,
                    },
                    target: None,
                    reason: if matches!(reason, RejectReason::CapacityExceeded(_)) {
                        Qos0DropReason::CapacityExceeded
                    } else {
                        Qos0DropReason::WriteFailed
                    },
                })?;
                (CommandPhase::RejectedCertain, None)
            }
            (Verdict::Transition(CommandPhase::Indeterminate), _) => {
                return Ok(Some(changed(Change::Await {
                    operation,
                    phase: CommandPhase::Indeterminate,
                    resolution: None,
                })));
            }
            _ => return Err(IMPOSSIBLE),
        };
        draft.receipt_consumed = Some(operation);
        Ok(Some(changed(Change::RetainedDone {
            finished: Finished {
                operation,
                answers: flight.in_flight.answers,
                phase,
                facts,
            },
        })))
    }
    pub(super) fn plan_qos0_packet(
        &self,
        session: usize,
        origin: (ConnectionGeneration, u64),
        packet: &InboundPacket,
        draft: &mut Draft,
    ) -> Result<super::Change, Stop> {
        let (generation, ingress_order) = origin;
        let Some(connection) = self.pubsub.connection(generation) else {
            return Self::plan_close(generation, Some(session), CloseReason::ProtocolError, draft);
        };
        let InboundPacket::Publish {
            qos: Qos::AtMostOnce,
            dup: false,
            retain: false,
            packet_id: None,
            topic,
            payload,
            expiry,
        } = packet
        else {
            return Self::plan_close(generation, Some(session), CloseReason::ProtocolError, draft);
        };
        if expiry.is_some_and(|expiry| expiry.deadline().is_err()) {
            return Err(IMPOSSIBLE);
        }
        if ingress_order <= self.pubsub.connections.as_slice()[connection].last_qos0_ingress {
            return Self::plan_close(generation, Some(session), CloseReason::ProtocolError, draft);
        }
        let limits = self.limits.ok_or(IMPOSSIBLE)?;
        if topic.len() as u64 > u64::from(limits.max_topic_bytes)
            || payload.len() as u64 > limits.max_payload_bytes.get()
        {
            return Self::plan_close(
                generation,
                Some(session),
                CloseReason::CapacityExceeded,
                draft,
            );
        }
        let publication = VolatilePublicationId {
            source: ConnectionKey {
                session: self.sessions.as_slice()[session].id,
                generation,
            },
            ingress_order,
        };
        let origin = Qos0Origin::Volatile { publication };
        let selected = match super::routing::select(
            self.routes.as_slice(),
            self.sessions.as_slice(),
            publication.source.session,
            topic.as_slice(),
            Qos::AtMostOnce,
            limits.max_targets,
        ) {
            Ok(selected) => Some(selected),
            Err(super::routing::Refusal::Capacity) => None,
            Err(super::routing::Refusal::Revision) => return Err(IMPOSSIBLE),
        };
        let count = selected.as_ref().map_or(0, super::routing::Selection::len);
        let used = self
            .pubsub
            .volatile
            .as_slice()
            .iter()
            .try_fold(0u64, |sum, v| sum.checked_add(v.payload.len() as u64))
            .ok_or(IMPOSSIBLE)?;
        let fits = selected.is_some()
            && count <= self.pubsub.volatile.capacity() - self.pubsub.volatile.len()
            && (payload.len() as u64)
                .checked_mul(count as u64)
                .and_then(|n| n.checked_add(used))
                .is_some_and(|n| n <= self.pubsub.payload_limit.get());
        if count == 0 || !fits {
            draft.emit(Effect::Qos0Dropped {
                origin,
                target: None,
                reason: if count == 0 && selected.is_some() {
                    Qos0DropReason::NoSubscribers
                } else {
                    Qos0DropReason::CapacityExceeded
                },
            })?;
            return Ok(changed(Change::Qos0Admit {
                connection,
                ingress_order,
                records: BoundedVec::with_capacity(0)?,
                continuation: None,
            }));
        }
        let mut records = BoundedVec::with_capacity(count)?;
        for target in selected.ok_or(IMPOSSIBLE)?.iter() {
            let connection = self
                .connection_of(target.session)
                .map(|generation| ConnectionKey {
                    session: target.session,
                    generation,
                });
            records
                .try_push(Volatile {
                    origin,
                    target: target.session,
                    connection,
                    topic: topic.clone(),
                    payload: payload.clone(),
                    expiry: *expiry,
                    frame: None,
                })
                .map_err(|_| IMPOSSIBLE)?;
        }
        let continuation = if self.pubsub.continuation.is_none() {
            Some(self.qos0_continuation(draft)?)
        } else {
            None
        };
        Ok(changed(Change::Qos0Admit {
            connection,
            ingress_order,
            records,
            continuation,
        }))
    }

    fn qos0_fits(
        &self,
        connection: ConnectionKey,
        topic: &SharedBytes,
        payload: &SharedBytes,
        expiry: Option<MessageExpiry>,
    ) -> bool {
        let Some(settings) = self
            .pubsub
            .connection(connection.generation)
            .map(|i| self.pubsub.connections.as_slice()[i])
        else {
            return false;
        };
        let Some(body) = topic
            .len()
            .checked_add(payload.len())
            .and_then(|n| n.checked_add(3 + usize::from(expiry.is_some()) * 5))
        else {
            return false;
        };
        let remaining = if body < 128 {
            1
        } else if body < 16384 {
            2
        } else if body < 2097152 {
            3
        } else {
            4
        };
        body.checked_add(remaining + 1)
            .is_some_and(|n| n <= settings.maximum_packet_size as usize)
    }

    pub(super) fn plan_qos0_continue(
        &self,
        continuation: ContinuationId,
        draft: &mut Draft,
    ) -> Result<super::Change, Stop> {
        if self.pubsub.continuation != Some(continuation) {
            return if self.pubsub.continuations_started {
                Ok(super::Change::Nothing)
            } else {
                Err(IMPOSSIBLE)
            };
        }
        let Some((slot, v)) = self
            .pubsub
            .volatile
            .as_slice()
            .iter()
            .enumerate()
            .find(|(_, v)| v.frame.is_none())
        else {
            return Ok(changed(Change::Qos0Idle));
        };
        let next = if self.pubsub.volatile.as_slice()[slot + 1..]
            .iter()
            .any(|v| v.frame.is_none())
        {
            Some(self.qos0_continuation(draft)?)
        } else {
            None
        };
        let reason = if let Some(expiry) = v.expiry {
            match self.message_now.map(|now| expiry.state(now)) {
                Some(Ok(ExpiryState::Fresh { .. })) => None,
                Some(Ok(ExpiryState::Expired)) => Some(Qos0DropReason::MessageExpired),
                Some(Err(_)) => return Err(IMPOSSIBLE),
                _ => Some(Qos0DropReason::TemporalUncertain),
            }
        } else {
            None
        };
        let reason = reason.or(match v.connection {
            None => Some(Qos0DropReason::DestinationAbsent),
            Some(key) if self.connection_of(v.target) != Some(key.generation) => {
                Some(Qos0DropReason::ConnectionClosed)
            }
            Some(key) if !self.qos0_fits(key, &v.topic, &v.payload, v.expiry) => {
                Some(Qos0DropReason::PacketTooLarge)
            }
            _ => None,
        });
        if let Some(reason) = reason {
            draft.emit(Effect::Qos0Dropped {
                origin: v.origin,
                target: Some(v.target),
                reason,
            })?;
            return Ok(changed(Change::Qos0Drop {
                slot,
                continuation: next,
            }));
        }
        let connection = v.connection.ok_or(IMPOSSIBLE)?;
        let effect = draft.emit(Effect::EmitQos0 {
            connection,
            origin: v.origin,
            retain: false,
            topic: v.topic.clone(),
            payload: v.payload.clone(),
            expiry: v.expiry,
        })?;
        Ok(changed(Change::Qos0Send {
            slot,
            effect,
            continuation: next,
        }))
    }

    fn plan_qos0_terminal(
        &self,
        effect: EffectId,
        connection: ConnectionKey,
        reason: Option<Qos0DropReason>,
        draft: &mut Draft,
    ) -> Result<Option<super::Change>, Stop> {
        if let Some((slot, v)) = self
            .pubsub
            .volatile
            .as_slice()
            .iter()
            .enumerate()
            .find(|(_, v)| v.frame == Some(effect) && v.connection == Some(connection))
        {
            if let Some(reason) = reason {
                draft.emit(Effect::Qos0Dropped {
                    origin: v.origin,
                    target: Some(v.target),
                    reason,
                })?;
            }
            return Ok(Some(changed(Change::Qos0Done { slot })));
        }
        if let Some((slot, d)) =
            self.pubsub
                .deliveries
                .as_slice()
                .iter()
                .enumerate()
                .find(|(_, d)| {
                    matches!(d.phase, Phase::Qos0Writing(_))
                        && d.frame == Some((effect, connection))
                })
        {
            let Phase::Qos0Writing(finished) = d.phase else {
                return Err(IMPOSSIBLE);
            };
            if let Some(reason) = reason {
                draft.emit(Effect::Qos0Dropped {
                    origin: Qos0Origin::Consumed {
                        delivery: d.id,
                        operation: finished.operation,
                    },
                    target: Some(d.target),
                    reason,
                })?;
            }
            return Ok(Some(changed(Change::Finished { slot, finished })));
        }
        Ok(None)
    }

    pub(super) fn plan_qos0_refusal(
        &self,
        effect: EffectId,
        connection: ConnectionKey,
        reason: Qos0DropReason,
        draft: &mut Draft,
    ) -> Result<super::Change, Stop> {
        Ok(self
            .plan_qos0_terminal(effect, connection, Some(reason), draft)?
            .unwrap_or(super::Change::Nothing))
    }

    pub(super) fn plan_qos0_frame(
        &self,
        effect: EffectId,
        connection: ConnectionKey,
        progress: FrameProgress,
        draft: &mut Draft,
    ) -> Result<Option<super::Change>, Stop> {
        match progress {
            FrameProgress::NotStarted { reason } => self.plan_qos0_terminal(
                effect,
                connection,
                Some(match reason {
                    MessageExpiryBlock::Expired => Qos0DropReason::MessageExpired,
                    MessageExpiryBlock::TemporalUncertain => Qos0DropReason::TemporalUncertain,
                }),
                draft,
            ),
            FrameProgress::Failed => self.plan_qos0_terminal(
                effect,
                connection,
                Some(Qos0DropReason::WriteFailed),
                draft,
            ),
            FrameProgress::Written { confirmed, total } if total > 0 && confirmed == total => {
                self.plan_qos0_terminal(effect, connection, None, draft)
            }
            _ => Ok(None),
        }
    }

    pub(super) fn plan_settings(
        &self,
        connection: ConnectionGeneration,
        receive_maximum: u16,
        maximum_packet_size: u32,
        draft: &mut Draft,
    ) -> Result<super::Change, Stop> {
        if self.closed.iter().any(|c| *c == connection) {
            return Ok(super::Change::Nothing);
        }
        if receive_maximum == 0 || maximum_packet_size == 0 {
            return Err(IMPOSSIBLE);
        }
        let index = self.pubsub.connection(connection);
        if index.is_none() && self.pubsub.connections.len() == self.pubsub.connections.capacity() {
            draft.close(connection, CloseReason::CapacityExceeded)?;
            return Ok(super::Change::Nothing);
        }
        let mut value = Connection {
            generation: connection,
            receive_maximum: receive_maximum.min(8),
            maximum_packet_size,
            after: None,
            loading: None,
            rescan_needed: false,
            last_qos0_ingress: 0,
        };
        if let Some(index) = index {
            let current = self.pubsub.connections.as_slice()[index];
            if current.receive_maximum != value.receive_maximum
                || current.maximum_packet_size != value.maximum_packet_size
            {
                return Err(IMPOSSIBLE);
            }
            value = current;
        }
        Ok(changed(Change::Settings { index, value }))
    }

    pub(super) fn control_collision(
        &self,
        connection: ConnectionGeneration,
        packet: &InboundPacket,
    ) -> bool {
        let Some(control) = &self.pubsub.control else {
            return false;
        };
        if control.connection.generation != connection {
            return false;
        }
        match packet {
            InboundPacket::Publish {
                packet_id: Some(id),
                ..
            }
            | InboundPacket::Pubrel { packet_id: id } => *id == control.packet_id,
            _ => false,
        }
    }

    /// Correlates an ACK to the current occurrence eligible on this exact
    /// generation. Local write callbacks are not prerequisites for MQTT ACKs.
    #[must_use]
    pub fn puback_target(
        &self,
        connection: ConnectionKey,
        packet_id: OutboundPacketId,
    ) -> Option<PubackTarget> {
        if self.connection_of(connection.session) != Some(connection.generation) {
            return None;
        }
        self.pubsub
            .deliveries
            .as_slice()
            .iter()
            .find_map(|delivery| {
                let claim = delivery.claim?;
                let (frame, sent) = delivery.frame?;
                (delivery.target == connection.session
                    && delivery.qos == Qos::AtLeastOnce
                    && matches!(delivery.phase, Phase::AwaitPuback | Phase::StartPending(_))
                    && delivery.window == Some(connection.generation)
                    && sent == connection
                    && claim.packet_id == packet_id)
                    .then_some(PubackTarget {
                        delivery: delivery.id,
                        claim: claim.operation,
                        frame,
                    })
            })
    }

    pub(super) fn plan_observed_puback(
        &self,
        connection: ConnectionKey,
        packet_id: OutboundPacketId,
        reason: u8,
        target: Option<PubackTarget>,
        draft: &mut Draft,
    ) -> Result<super::Change, Stop> {
        if self.connection_of(connection.session) != Some(connection.generation) {
            return Ok(super::Change::Nothing);
        }
        if !matches!(
            reason,
            0 | 0x10 | 0x80 | 0x83 | 0x87 | 0x90 | 0x91 | 0x97 | 0x99
        ) {
            return Self::plan_close(
                connection.generation,
                self.session_of(connection.generation),
                CloseReason::ProtocolError,
                draft,
            );
        }
        let Some(target) = target else {
            return Ok(super::Change::Nothing);
        };
        if self.puback_target(connection, packet_id) != Some(target) {
            return Ok(super::Change::Nothing);
        }
        let slot = self.pubsub.delivery(target.delivery).ok_or(IMPOSSIBLE)?;
        let delivery = &self.pubsub.deliveries.as_slice()[slot];
        if delivery.expiry.is_some() && !delivery.claim.is_some_and(|claim| claim.started) {
            return Ok(changed(Change::PubackObserved { slot }));
        }
        let operation = self.operation(draft)?;
        let recipe = Recipe::FinishDelivery(FinishDelivery {
            operation,
            delivery: target.delivery,
            target: delivery.target,
            target_revision: delivery.revision,
            packet_id,
            claim: target.claim,
        });
        let in_flight = self.plan_submission(operation, recipe, None, draft)?;
        Ok(changed(Change::Finish { slot, in_flight }))
    }

    pub(super) fn plan_pubsub_packet(
        &self,
        session_index: usize,
        connection: ConnectionGeneration,
        packet: &InboundPacket,
        draft: &mut Draft,
    ) -> Result<super::Change, Stop> {
        let session = &self.sessions.as_slice()[session_index];
        let key = ConnectionKey {
            session: session.id,
            generation: connection,
        };
        if let InboundPacket::Puback { packet_id, reason } = packet {
            // Legacy synchronous adapters associate and consume in this
            // single transition. Product input freezes the target earlier.
            return self.plan_observed_puback(
                key,
                *packet_id,
                *reason,
                self.puback_target(key, *packet_id),
                draft,
            );
        }
        if self.lifecycle.is_some() {
            return Err(blocked());
        }
        let (packet_id, subscribing) = match packet {
            InboundPacket::Subscribe { packet_id, .. } => (*packet_id, true),
            InboundPacket::Unsubscribe { packet_id, .. } => (*packet_id, false),
            _ => return Err(IMPOSSIBLE),
        };
        if self.slot_of(session.id, packet_id).is_some() {
            return Self::plan_close(
                connection,
                Some(session_index),
                CloseReason::ProtocolError,
                draft,
            );
        }
        if let Some(control) = &self.pubsub.control {
            if control.connection == key && control.packet_id == packet_id {
                return Self::plan_close(
                    connection,
                    Some(session_index),
                    CloseReason::ProtocolError,
                    draft,
                );
            }
            return Err(blocked());
        }
        if self.pubsub.connection(connection).is_none() {
            return Self::plan_close(
                connection,
                Some(session_index),
                CloseReason::ProtocolError,
                draft,
            );
        }
        let (changes, results) = self.subscription_changes(session.id, packet)?;
        if changes.is_empty() {
            draft.frame(
                key,
                if subscribing {
                    OutboundFrame::Suback { packet_id, results }
                } else {
                    OutboundFrame::Unsuback { packet_id, results }
                },
            )?;
            return Ok(super::Change::Nothing);
        }
        // Reserve a complete replacement projection before committing its
        // intent. No postcommit allocation or capacity refusal is needed.
        let projected = match self.project_routes(session.id, session.revision, changes.as_slice())
        {
            Ok(routes) => routes,
            Err(Stop::Need(_)) => {
                let mut refused = results;
                for code in &mut refused.codes[..usize::from(refused.len)] {
                    if *code < 128 {
                        *code = if subscribing { 0x97 } else { 0x83 };
                    }
                }
                draft.frame(
                    key,
                    if subscribing {
                        OutboundFrame::Suback {
                            packet_id,
                            results: refused,
                        }
                    } else {
                        OutboundFrame::Unsuback {
                            packet_id,
                            results: refused,
                        }
                    },
                )?;
                return Ok(super::Change::Nothing);
            }
            Err(error) => return Err(error),
        };
        let operation = self.operation(draft)?;
        let update = UpdateSubscriptions {
            operation,
            session: session.id,
            session_revision: session.revision,
            route_revision: self.route_revision.ok_or(IMPOSSIBLE)?,
            changes,
        };
        let recipe = if (self.lifecycle_enabled || self.message_now.is_some()) && subscribing {
            let first = draw(
                &mut draft.counters.publication,
                PublicationSeq::checked_next,
            )?;
            for _ in 1..crate::contract::retained::MAX_REPLAYS {
                let _ = draw(
                    &mut draft.counters.publication,
                    PublicationSeq::checked_next,
                )?;
            }
            let replay = crate::contract::retained::ReplaySubscriptions {
                update,
                first_publication: PublicationId::new(self.incarnation, self.epoch, self.id, first),
            };
            if let Some(observed) = self.message_now {
                Recipe::TimedReplaySubscriptions(
                    crate::contract::command::TimedReplaySubscriptions { replay, observed },
                )
            } else {
                Recipe::ReplaySubscriptions(replay)
            }
        } else {
            Recipe::UpdateSubscriptions(update)
        };
        let in_flight = self.plan_submission(operation, recipe, None, draft)?;
        Ok(changed(Change::Control(Control {
            session: session.id,
            connection: key,
            packet_id,
            packet: packet.clone(),
            results,
            projected,
            in_flight,
        })))
    }

    fn subscription_changes(
        &self,
        session: SessionId,
        packet: &InboundPacket,
    ) -> Result<(BoundedVec<GrantedSubscriptionChange>, FilterResults), Stop> {
        let mut changes = BoundedVec::with_capacity(MAX_FILTERS)?;
        let mut codes = [0u8; MAX_FILTERS];
        let mut len = 0usize;
        match packet {
            InboundPacket::Subscribe { filters, .. } => {
                for item in filters.as_slice() {
                    let allowed = crate::contract::topic::valid_filter(item.filter.as_slice());
                    codes[len] = if allowed {
                        item.qos.min(Qos::AtLeastOnce).to_wire()
                    } else if item.filter.as_slice().starts_with(b"$share/") {
                        0x9e
                    } else {
                        0x8f
                    };
                    len += 1;
                    if allowed {
                        changes
                            .try_push(GrantedSubscriptionChange {
                                topic: item.filter.clone(),
                                granted: Some(item.qos.min(Qos::AtLeastOnce)),
                                options: item.options,
                            })
                            .map_err(|_| IMPOSSIBLE)?;
                    }
                }
            }
            InboundPacket::Unsubscribe { filters, .. } => {
                for topic in filters.as_slice() {
                    let allowed = crate::contract::topic::valid_filter(topic.as_slice());
                    let removed_before = changes
                        .as_slice()
                        .iter()
                        .any(|c| c.topic == *topic && c.granted.is_none());
                    let present = !removed_before
                        && self
                            .routes
                            .as_slice()
                            .iter()
                            .any(|r| r.target == session && r.topic == *topic);
                    codes[len] = if !allowed {
                        0x8f
                    } else if present {
                        0
                    } else {
                        0x11
                    };
                    len += 1;
                    if allowed {
                        changes
                            .try_push(GrantedSubscriptionChange {
                                topic: topic.clone(),
                                granted: None,
                                options: crate::contract::pubsub::SubscriptionOptions::DEFAULT,
                            })
                            .map_err(|_| IMPOSSIBLE)?;
                    }
                }
            }
            _ => return Err(IMPOSSIBLE),
        }
        if len == 0 || len > MAX_FILTERS {
            return Err(IMPOSSIBLE);
        }
        if let Some(limits) = self.limits
            && changes
                .as_slice()
                .iter()
                .any(|c| c.topic.len() as u64 > u64::from(limits.max_topic_bytes))
        {
            return Err(IMPOSSIBLE);
        }
        Ok((
            changes,
            FilterResults::new(&codes[..len]).map_err(|_| IMPOSSIBLE)?,
        ))
    }

    fn project_routes(
        &self,
        session: SessionId,
        revision: Revision,
        changes: &[GrantedSubscriptionChange],
    ) -> Result<BoundedVec<Route>, Stop> {
        let mut routes = BoundedVec::with_capacity(self.routes.capacity())?;
        for route in self.routes.as_slice() {
            if !(route.target == session && changes.iter().any(|c| c.topic == route.topic)) {
                routes.try_push(route.clone()).map_err(|_| IMPOSSIBLE)?;
            }
        }
        for (i, change) in changes.iter().enumerate() {
            if change.granted.is_none()
                || changes[i + 1..]
                    .iter()
                    .any(|later| later.topic == change.topic)
            {
                continue;
            }
            routes
                .try_push(Route {
                    topic: change.topic.clone(),
                    target: session,
                    target_revision: revision,
                    max_qos: change.granted.ok_or(IMPOSSIBLE)?,
                    options: change.options,
                })
                .map_err(|_| {
                    Stop::Need(CapacityNeed {
                        resource: Resource::References,
                        amount: 1,
                        wake: WakeCondition::ResourceReleased,
                    })
                })?;
        }
        Ok(routes)
    }

    pub(super) fn plan_pubsub_control_work(
        &self,
        session: Option<SessionId>,
        draft: &mut Draft,
    ) -> Result<Option<super::Change>, Stop> {
        if !self.receipts.control_available() {
            return Ok(None);
        }
        let Some((slot, d)) = self
            .pubsub
            .deliveries
            .as_slice()
            .iter()
            .enumerate()
            .filter(|(_, d)| {
                session.is_none_or(|session| d.target == session)
                    && d.control_ready(self.message_now)
            })
            .min_by_key(|(_, d)| d.id)
        else {
            return Ok(None);
        };
        let change = if d.phase == Phase::StartRetry {
            Change::Start {
                slot,
                in_flight: self.submit_start(d, draft)?,
            }
        } else if d.early_puback && d.claim.is_some_and(|claim| claim.started) {
            let operation = self.operation(draft)?;
            let in_flight = self.plan_submission(
                operation,
                Recipe::FinishDelivery(self.finish_recipe(d, operation)?),
                None,
                draft,
            )?;
            Change::Finish { slot, in_flight }
        } else {
            Change::Expire {
                slot,
                in_flight: self.submit_expiry(d, draft)?,
            }
        };
        Ok(Some(changed(change)))
    }

    pub(super) fn plan_work(
        &self,
        session: SessionId,
        draft: &mut Draft,
    ) -> Result<super::Change, Stop> {
        if let Some(change) = self.plan_pubsub_control_work(Some(session), draft)? {
            return Ok(change);
        }
        let Some(generation) = self.connection_of(session) else {
            return Ok(super::Change::Nothing);
        };
        let Some(connection) = self.pubsub.connection(generation) else {
            return Ok(super::Change::Nothing);
        };
        let settings = self.pubsub.connections.as_slice()[connection];
        let room = self.pubsub.active(generation) < usize::from(settings.receive_maximum);
        let next = self
            .pubsub
            .deliveries
            .as_slice()
            .iter()
            .enumerate()
            .filter(|(_, d)| d.target == session && d.waiting_to_send(generation, self.message_now))
            .min_by_key(|(_, d)| d.id);
        if let Some((_, d)) = next
            && matches!(
                d.phase,
                Phase::ClaimPending(_) | Phase::ConsumePending(_, _)
            )
        {
            // A lost claim result must be reconciled before a later
            // publication can become eligible on this destination.
            return Ok(super::Change::Nothing);
        }
        if let Some((slot, d)) = next
            && d.phase == Phase::AwaitPuback
            && d.payload.is_some()
            && (room || d.window == Some(generation))
        {
            let emission = self.plan_delivery_emit(d, draft)?;
            if let Some((effect, connection)) = emission {
                return Ok(changed(Change::Emit {
                    slot,
                    effect,
                    connection,
                }));
            }
            if draft.closed.is_some() {
                return Ok(super::Change::Nothing);
            }
        }
        if let Some((slot, d)) = next
            && d.phase == Phase::Queued
            && d.qos == Qos::AtMostOnce
            && d.payload.is_some()
        {
            let operation = self.operation(draft)?;
            let recipe = Recipe::ConsumeDelivery(ConsumeDelivery {
                operation,
                delivery: d.id,
                target: d.target,
                target_revision: d.revision,
            });
            let in_flight = self.plan_submission(operation, recipe, None, draft)?;
            return Ok(changed(Change::Consume {
                slot,
                in_flight,
                connection: ConnectionKey {
                    session,
                    generation,
                },
            }));
        }
        if room
            && let Some((slot, d)) = next
            && d.phase == Phase::Queued
            && d.payload.is_some()
        {
            let Some(packet_id) = self.pubsub.free_packet_id(session, u16::MAX) else {
                draft.close(generation, CloseReason::CapacityExceeded)?;
                return Ok(super::Change::Nothing);
            };
            let operation = self.operation(draft)?;
            let claim = DeliveryClaim {
                operation,
                packet_id,
                started: false,
            };
            let recipe = Recipe::ClaimDelivery(ClaimDelivery {
                operation,
                delivery: d.id,
                target: d.target,
                target_revision: d.revision,
                packet_id,
            });
            let in_flight = self.plan_submission(operation, recipe, None, draft)?;
            return Ok(changed(Change::Claim {
                slot,
                claim,
                in_flight,
                connection,
            }));
        }
        if room && settings.loading.is_none() {
            let effect = draft.emit(Effect::LoadWork {
                session,
                limit: WORK_PAGE_LIMIT,
                after: settings.after,
            })?;
            return Ok(changed(Change::Scan {
                connection,
                read: Read {
                    effect,
                    session,
                    invalidated: false,
                },
            }));
        }
        Ok(super::Change::Nothing)
    }

    pub(super) fn plan_work_page(
        &self,
        effect: EffectId,
        session: SessionId,
        items: &[WorkItem],
        next: Option<DeliveryId>,
    ) -> Result<super::Change, Stop> {
        let Some(connection) = self.pubsub.connections.as_slice().iter().position(|c| {
            c.loading
                .is_some_and(|r| r.effect == effect && r.session == session)
        }) else {
            return Ok(super::Change::Nothing);
        };
        let settings = self.pubsub.connections.as_slice()[connection];
        let read = settings.loading.ok_or(IMPOSSIBLE)?;
        if read.invalidated || self.connection_of(session) != Some(settings.generation) {
            return Ok(changed(Change::Page {
                connection,
                discard: true,
                next: None,
            }));
        }
        if items.len() > WORK_PAGE_LIMIT as usize
            || items.windows(2).any(|w| w[0].delivery >= w[1].delivery)
            || settings
                .after
                .is_some_and(|after| items.first().is_some_and(|first| first.delivery <= after))
            || next.is_some_and(|id| items.last().is_none_or(|last| last.delivery != id))
        {
            return Err(IMPOSSIBLE);
        }
        let target = self
            .sessions
            .as_slice()
            .iter()
            .find(|s| s.id == session)
            .ok_or(IMPOSSIBLE)?;
        let mut total = self.pubsub.loaded_bytes().ok_or(IMPOSSIBLE)?;
        let mut added = 0usize;
        let limits = self.limits.ok_or(IMPOSSIBLE)?;
        for (position, item) in items.iter().enumerate() {
            if item.target != session
                || item.target_revision != target.revision
                || item.topic.len() as u64 > u64::from(limits.max_topic_bytes)
                || item.payload.len() as u64 > limits.max_payload_bytes.get()
                || item.qos == Qos::ExactlyOnce
                || (item.claim.is_some() && item.qos != Qos::AtLeastOnce)
                || item.expiry.is_some_and(|expiry| expiry.deadline().is_err())
                || item
                    .claim
                    .is_some_and(|claim| claim.started && item.expiry.is_none())
            {
                return Err(IMPOSSIBLE);
            }
            if let Some(claim) = item.claim
                && (claim.operation.incarnation() != self.incarnation
                    || claim.operation.epoch() > self.epoch
                    || self.pubsub.deliveries.as_slice().iter().any(|d| {
                        d.id != item.delivery
                            && d.claim.is_some_and(|other| {
                                other.operation == claim.operation
                                    || (d.target == item.target
                                        && other.packet_id == claim.packet_id)
                            })
                    })
                    || items[..position].iter().any(|other| {
                        other.claim.is_some_and(|other| {
                            other.operation == claim.operation || other.packet_id == claim.packet_id
                        })
                    }))
            {
                return Err(IMPOSSIBLE);
            }
            let known = self
                .pubsub
                .delivery(item.delivery)
                .map(|i| &self.pubsub.deliveries.as_slice()[i]);
            if let Some(known) = known {
                if known.target != item.target
                    || known.revision != item.target_revision
                    || known.qos != item.qos
                    || known.expiry != item.expiry
                    || (known.topic.is_some() && known.retain != item.retain)
                    || known
                        .topic
                        .as_ref()
                        .is_some_and(|topic| *topic != item.topic)
                    || known
                        .payload
                        .as_ref()
                        .is_some_and(|payload| *payload != item.payload)
                {
                    return Err(IMPOSSIBLE);
                }
                if let Some(claim) = item.claim
                    && known.claim.is_none_or(|held| {
                        held.operation != claim.operation || held.packet_id != claim.packet_id
                    })
                {
                    return Err(IMPOSSIBLE);
                }
            } else if !self.expired_unclaimed_item(item) {
                added += 1;
            }
            if !self.expired_unclaimed_item(item) && known.is_none_or(|d| d.payload.is_none()) {
                total = total
                    .checked_add(item.payload.len() as u64)
                    .ok_or(IMPOSSIBLE)?;
            }
        }
        if self.pubsub.deliveries.len() + added > self.pubsub.deliveries.capacity()
            || total > self.pubsub.payload_limit.get()
        {
            return Err(Stop::Need(CapacityNeed {
                resource: Resource::PayloadBytes,
                amount: 1,
                wake: WakeCondition::ResourceReleased,
            }));
        }
        Ok(changed(Change::Page {
            connection,
            discard: false,
            next,
        }))
    }

    fn expired_unclaimed_item(&self, item: &WorkItem) -> bool {
        item.claim.is_none()
            && item.expiry.is_some_and(|expiry| {
                self.message_now
                    .is_some_and(|now| expiry.state(now) == Ok(ExpiryState::Expired))
            })
    }

    fn plan_delivery_emit(
        &self,
        delivery: &Delivery,
        draft: &mut Draft,
    ) -> Result<Option<(EffectId, ConnectionKey)>, Stop> {
        if !delivery.expiry_allows_send(self.message_now) {
            return Ok(None);
        }
        let Some(generation) = self.connection_of(delivery.target) else {
            return Ok(None);
        };
        let Some(settings) = self
            .pubsub
            .connection(generation)
            .map(|i| self.pubsub.connections.as_slice()[i])
        else {
            return Ok(None);
        };
        if delivery.window != Some(generation)
            && self.pubsub.active(generation) >= usize::from(settings.receive_maximum)
        {
            return Ok(None);
        }
        if self.pubsub.deliveries.as_slice().iter().any(|earlier| {
            earlier.target == delivery.target
                && earlier.id < delivery.id
                && earlier.waiting_to_send(generation, self.message_now)
        }) {
            // This also protects a claim result delivered after takeover:
            // restored earlier exchanges are replayed before newer ones.
            return Ok(None);
        }
        let (Some(topic), Some(payload), Some(claim)) =
            (&delivery.topic, &delivery.payload, delivery.claim)
        else {
            return Ok(None);
        };
        let body = topic
            .len()
            .checked_add(payload.len())
            .and_then(|n| n.checked_add(5 + usize::from(delivery.expiry.is_some()) * 5))
            .ok_or(IMPOSSIBLE)?;
        let remaining = if body < 128 {
            1
        } else if body < 16384 {
            2
        } else if body < 2097152 {
            3
        } else {
            4
        };
        if body
            .checked_add(remaining + 1)
            .is_none_or(|n| n > settings.maximum_packet_size as usize)
        {
            draft.close(generation, CloseReason::CapacityExceeded)?;
            return Ok(None);
        }
        let connection = ConnectionKey {
            session: delivery.target,
            generation,
        };
        let effect = draft.emit(Effect::EmitPublish {
            connection,
            delivery: delivery.id,
            packet_id: claim.packet_id,
            claim: claim.operation,
            topic: topic.clone(),
            payload: payload.clone(),
            dup: delivery.may_have_started,
            retain: delivery.retain,
            expiry: delivery.expiry,
            started: claim.started,
        })?;
        Ok(Some((effect, connection)))
    }

    pub(super) fn plan_pubsub_result(
        &self,
        effect: EffectId,
        operation: OperationId,
        reply: &StoreReply,
        draft: &mut Draft,
    ) -> Result<Option<super::Change>, Stop> {
        if let Some(change) = self.plan_retained_qos0_result(effect, operation, reply, draft)? {
            return Ok(Some(change));
        }
        let control = self
            .pubsub
            .control
            .as_ref()
            .filter(|c| c.in_flight.operation == operation);
        let delivery = self
            .pubsub
            .deliveries
            .as_slice()
            .iter()
            .enumerate()
            .find(|(_, d)| d.in_flight().is_some_and(|f| f.operation == operation));
        let in_flight = if let Some(c) = control {
            c.in_flight
        } else if let Some((_, d)) = delivery {
            d.in_flight().ok_or(IMPOSSIBLE)?
        } else {
            return Ok(None);
        };
        if !in_flight.answers.names(effect) {
            return Ok(Some(super::Change::Nothing));
        }
        let outcome = match reply {
            StoreReply::Outcome(o) => *o,
            StoreReply::Violation(StoreViolation::RecipeMismatch) => {
                return Err(Stop::Fatal(CoreFault::RecipeMismatch { operation }));
            }
        };
        let verdict = in_flight
            .phase
            .on(CommandEvent::StoreResult(OutcomeClass::of(&outcome)));
        let finished = |phase, facts| Finished {
            operation,
            answers: in_flight.answers,
            phase,
            facts,
        };
        match (verdict, outcome) {
            (Verdict::Transition(CommandPhase::Committed), StoreOutcome::Committed(facts)) => {
                draft.receipt_consumed = Some(operation);
                let done = finished(CommandPhase::Consumed, Some(facts));
                if let Some(c) = control {
                    let revision = self
                        .route_revision
                        .ok_or(IMPOSSIBLE)?
                        .checked_next()
                        .map_err(|_| Stop::Exhausted(Exhaustion::IdentityCounter))?;
                    if facts
                        != (CommitFacts::SubscriptionsChanged {
                            session: c.session,
                            route_revision: revision,
                        })
                    {
                        return Err(Stop::Fatal(CoreFault::ContradictoryOutcome { operation }));
                    }
                    self.control_answer(c, c.results, draft)?;
                    return Ok(Some(changed(Change::ControlEnd {
                        revision: Some(revision),
                        finished: done,
                    })));
                }
                let (slot, d) = delivery.ok_or(IMPOSSIBLE)?;
                if matches!(d.phase, Phase::StartPending(_) | Phase::ExpirePending(_)) {
                    let claim = d.claim.ok_or(IMPOSSIBLE)?;
                    let expected = if matches!(d.phase, Phase::StartPending(_)) {
                        CommitFacts::DeliveryStarted {
                            delivery: d.id,
                            target: d.target,
                            packet_id: claim.packet_id,
                            claim: claim.operation,
                        }
                    } else {
                        CommitFacts::DeliveryExpired {
                            delivery: d.id,
                            target: d.target,
                            packet_id: claim.packet_id,
                            claim: claim.operation,
                        }
                    };
                    if facts != expected {
                        return Err(Stop::Fatal(CoreFault::ContradictoryOutcome { operation }));
                    }
                    if d.retired || matches!(d.phase, Phase::ExpirePending(_)) {
                        return Ok(Some(changed(Change::Finished {
                            slot,
                            finished: done,
                        })));
                    }
                    let finish = if d.early_puback && self.receipts.control_available() {
                        let operation = self.operation(draft)?;
                        Some(self.plan_submission(
                            operation,
                            Recipe::FinishDelivery(self.finish_recipe(d, operation)?),
                            None,
                            draft,
                        )?)
                    } else {
                        None
                    };
                    return Ok(Some(changed(Change::Started {
                        slot,
                        finished: done,
                        finish,
                    })));
                }
                if d.retired {
                    let expected = match d.phase {
                        Phase::ConsumePending(_, _) => CommitFacts::DeliveryConsumed {
                            delivery: d.id,
                            target: d.target,
                        },
                        Phase::ClaimPending(_) => {
                            let claim = d.claim.ok_or(IMPOSSIBLE)?;
                            CommitFacts::DeliveryClaimed {
                                delivery: d.id,
                                target: d.target,
                                packet_id: claim.packet_id,
                                claim: claim.operation,
                            }
                        }
                        Phase::FinishPending(_) => {
                            let claim = d.claim.ok_or(IMPOSSIBLE)?;
                            CommitFacts::DeliveryFinished {
                                delivery: d.id,
                                target: d.target,
                                packet_id: claim.packet_id,
                                claim: claim.operation,
                            }
                        }
                        _ => return Err(IMPOSSIBLE),
                    };
                    if facts != expected {
                        return Err(Stop::Fatal(CoreFault::ContradictoryOutcome { operation }));
                    }
                    if matches!(d.phase, Phase::ConsumePending(_, _)) {
                        draft.emit(Effect::Qos0Dropped {
                            origin: Qos0Origin::Consumed {
                                delivery: d.id,
                                operation,
                            },
                            target: Some(d.target),
                            reason: Qos0DropReason::ConnectionClosed,
                        })?;
                    }
                    return Ok(Some(changed(Change::Finished {
                        slot,
                        finished: done,
                    })));
                }
                if let Phase::ConsumePending(_, connection) = d.phase {
                    if facts
                        != (CommitFacts::DeliveryConsumed {
                            delivery: d.id,
                            target: d.target,
                        })
                    {
                        return Err(Stop::Fatal(CoreFault::ContradictoryOutcome { operation }));
                    }
                    let (Some(topic), Some(payload)) = (&d.topic, &d.payload) else {
                        return Err(IMPOSSIBLE);
                    };
                    let origin = Qos0Origin::Consumed {
                        delivery: d.id,
                        operation,
                    };
                    let reason = if d.expiry.is_some() && !d.expiry_allows_send(self.message_now) {
                        Some(
                            if d.expiry.is_some_and(|expiry| {
                                self.message_now.is_some_and(|now| {
                                    expiry.state(now) == Ok(ExpiryState::Expired)
                                })
                            }) {
                                Qos0DropReason::MessageExpired
                            } else {
                                Qos0DropReason::TemporalUncertain
                            },
                        )
                    } else if self.connection_of(d.target) != Some(connection.generation) {
                        Some(Qos0DropReason::ConnectionClosed)
                    } else if !self.qos0_fits(connection, topic, payload, d.expiry) {
                        Some(Qos0DropReason::PacketTooLarge)
                    } else {
                        None
                    };
                    let emission = if let Some(reason) = reason {
                        draft.emit(Effect::Qos0Dropped {
                            origin,
                            target: Some(d.target),
                            reason,
                        })?;
                        None
                    } else {
                        Some((
                            draft.emit(Effect::EmitQos0 {
                                connection,
                                origin,
                                retain: d.retain,
                                topic: topic.clone(),
                                payload: payload.clone(),
                                expiry: d.expiry,
                            })?,
                            connection,
                        ))
                    };
                    return Ok(Some(changed(Change::Consumed {
                        slot,
                        finished: done,
                        emission,
                    })));
                }
                let claim = d.claim.ok_or(IMPOSSIBLE)?;
                if matches!(d.phase, Phase::ClaimPending(_)) {
                    if facts
                        != (CommitFacts::DeliveryClaimed {
                            delivery: d.id,
                            target: d.target,
                            packet_id: claim.packet_id,
                            claim: claim.operation,
                        })
                    {
                        return Err(Stop::Fatal(CoreFault::ContradictoryOutcome { operation }));
                    }
                    let emission = self.plan_delivery_emit(d, draft)?;
                    Ok(Some(changed(Change::Claimed {
                        slot,
                        finished: done,
                        emission,
                    })))
                } else {
                    if facts
                        != (CommitFacts::DeliveryFinished {
                            delivery: d.id,
                            target: d.target,
                            packet_id: claim.packet_id,
                            claim: claim.operation,
                        })
                    {
                        return Err(Stop::Fatal(CoreFault::ContradictoryOutcome { operation }));
                    }
                    Ok(Some(changed(Change::Finished {
                        slot,
                        finished: done,
                    })))
                }
            }
            (
                Verdict::Transition(CommandPhase::RejectedCertain),
                StoreOutcome::RejectedCertain(reason),
            ) => {
                draft.receipt_consumed = Some(operation);
                let done = finished(CommandPhase::RejectedCertain, None);
                if let Some(c) = control {
                    let mut results = c.results;
                    let subscribe = matches!(c.packet, InboundPacket::Subscribe { .. });
                    let code = if subscribe && matches!(reason, RejectReason::CapacityExceeded(_)) {
                        0x97
                    } else {
                        0x83
                    };
                    for code_at in &mut results.codes[..usize::from(results.len)] {
                        if *code_at < 128 {
                            *code_at = code;
                        }
                    }
                    self.control_answer(c, results, draft)?;
                    return Ok(Some(changed(Change::ControlEnd {
                        revision: None,
                        finished: done,
                    })));
                }
                let (slot, d) = delivery.ok_or(IMPOSSIBLE)?;
                if d.retired {
                    return Ok(Some(changed(Change::Finished {
                        slot,
                        finished: done,
                    })));
                }
                if reason == RejectReason::PreconditionFailed
                    && matches!(
                        d.phase,
                        Phase::ClaimPending(_) | Phase::ConsumePending(_, _)
                    )
                    && d.expiry.is_some_and(|expiry| {
                        self.message_now
                            .is_some_and(|now| expiry.state(now) == Ok(ExpiryState::Expired))
                    })
                {
                    return Ok(Some(changed(Change::Finished {
                        slot,
                        finished: done,
                    })));
                }
                if matches!(d.phase, Phase::StartPending(_) | Phase::ExpirePending(_)) {
                    if reason != RejectReason::Fenced {
                        return Err(IMPOSSIBLE);
                    }
                    return Ok(Some(changed(Change::RetryDeferred {
                        slot,
                        finished: done,
                        expiring: matches!(d.phase, Phase::ExpirePending(_)),
                    })));
                }
                if let Some(connection) = self.connection_of(d.target)
                    && !matches!(d.phase,Phase::ConsumePending(_,key) if key.generation!=connection)
                {
                    draft.close(connection, CloseReason::CapacityExceeded)?;
                }
                Ok(Some(changed(Change::Refused {
                    slot,
                    finished: done,
                    claiming: matches!(
                        d.phase,
                        Phase::ClaimPending(_) | Phase::ConsumePending(_, _)
                    ),
                })))
            }
            (Verdict::Transition(CommandPhase::Indeterminate), _) => {
                Ok(Some(changed(Change::Await {
                    operation,
                    phase: CommandPhase::Indeterminate,
                    resolution: None,
                })))
            }
            _ => Err(IMPOSSIBLE),
        }
    }

    fn control_answer(
        &self,
        control: &Control,
        results: FilterResults,
        draft: &mut Draft,
    ) -> Result<(), Stop> {
        if self.connection_of(control.session) != Some(control.connection.generation) {
            return Ok(());
        }
        let frame = if matches!(control.packet, InboundPacket::Subscribe { .. }) {
            OutboundFrame::Suback {
                packet_id: control.packet_id,
                results,
            }
        } else {
            OutboundFrame::Unsuback {
                packet_id: control.packet_id,
                results,
            }
        };
        draft.frame(control.connection, frame)
    }

    pub(super) fn plan_pubsub_deadline(
        &self,
        token: DeadlineToken,
        draft: &mut Draft,
    ) -> Result<Option<super::Change>, Stop> {
        let waiting = self
            .pubsub
            .retained
            .map(|f| f.in_flight)
            .filter(|f| f.deadline == token)
            .or_else(|| {
                self.pubsub
                    .control
                    .as_ref()
                    .map(|c| c.in_flight)
                    .filter(|f| f.deadline == token)
                    .or_else(|| {
                        self.pubsub
                            .deliveries
                            .as_slice()
                            .iter()
                            .filter_map(Delivery::in_flight)
                            .find(|f| f.deadline == token)
                    })
            });
        let Some(in_flight) = waiting else {
            return Ok(None);
        };
        let Verdict::Transition(phase) = in_flight.phase.on(CommandEvent::ResultTimeout) else {
            return Err(IMPOSSIBLE);
        };
        let effect = draft.emit(Effect::ResolveAndFence {
            operation: in_flight.operation,
        })?;
        let deadline = draw(&mut draft.counters.deadline, DeadlineToken::checked_next)?;
        draft.emit(Effect::ArmDeadline {
            token: deadline,
            after: self.resolution_delay,
        })?;
        Ok(Some(changed(Change::Await {
            operation: in_flight.operation,
            phase,
            resolution: Some((deadline, effect)),
        })))
    }

    fn finish_recipe(
        &self,
        delivery: &Delivery,
        operation: OperationId,
    ) -> Result<FinishDelivery, Stop> {
        let claim = delivery.claim.ok_or(IMPOSSIBLE)?;
        Ok(FinishDelivery {
            operation,
            delivery: delivery.id,
            target: delivery.target,
            target_revision: delivery.revision,
            packet_id: claim.packet_id,
            claim: claim.operation,
        })
    }

    fn submit_start(&self, delivery: &Delivery, draft: &mut Draft) -> Result<InFlight, Stop> {
        let operation = self.operation(draft)?;
        let finish = self.finish_recipe(delivery, operation)?;
        self.plan_submission(
            operation,
            Recipe::MarkDeliveryStarted(MarkDeliveryStarted {
                operation,
                delivery: finish.delivery,
                target: finish.target,
                target_revision: finish.target_revision,
                packet_id: finish.packet_id,
                claim: finish.claim,
            }),
            None,
            draft,
        )
    }

    fn submit_expiry(&self, delivery: &Delivery, draft: &mut Draft) -> Result<InFlight, Stop> {
        let expiry = delivery.expiry.ok_or(IMPOSSIBLE)?;
        let observed = self.message_now.ok_or(IMPOSSIBLE)?;
        if expiry.state(observed) != Ok(ExpiryState::Expired) || !delivery.certain_never_started {
            return Err(IMPOSSIBLE);
        }
        let operation = self.operation(draft)?;
        self.plan_submission(
            operation,
            Recipe::ExpireDelivery(ExpireDelivery {
                finish: self.finish_recipe(delivery, operation)?,
                expiry,
                observed,
            }),
            None,
            draft,
        )
    }

    pub(super) fn plan_pubsub_frame(
        &self,
        effect: EffectId,
        connection: ConnectionKey,
        progress: FrameProgress,
        draft: &mut Draft,
    ) -> Result<Option<super::Change>, Stop> {
        let Some((slot, d)) =
            self.pubsub
                .deliveries
                .as_slice()
                .iter()
                .enumerate()
                .find(|(_, d)| {
                    !d.retired
                        && d.frame.is_some_and(|(frame, sent)| {
                            sent == connection
                                && (frame == effect || MUTANT_STALE_OUTGOING_FRAME_RESULT)
                        })
                })
        else {
            return Ok(None);
        };
        // A callback for a replaced generation never establishes transmission
        // authority for the new connection or for another reusable PI.
        if self.connection_of(d.target) != Some(connection.generation) {
            return Ok(None);
        }
        let change = match progress {
            FrameProgress::Started
                if d.expiry.is_some()
                    && d.phase == Phase::AwaitPuback
                    && !d.claim.is_some_and(|claim| claim.started) =>
            {
                Change::Start {
                    slot,
                    in_flight: self.submit_start(d, draft)?,
                }
            }
            FrameProgress::NotStarted {
                reason: MessageExpiryBlock::Expired,
            } if d.phase == Phase::AwaitPuback
                && d.certain_never_started
                && !d.early_puback
                && d.expiry.is_some_and(|expiry| {
                    self.message_now
                        .is_some_and(|now| expiry.state(now) == Ok(ExpiryState::Expired))
                }) =>
            {
                Change::Expire {
                    slot,
                    in_flight: self.submit_expiry(d, draft)?,
                }
            }
            FrameProgress::NotStarted { .. } if d.phase == Phase::AwaitPuback => {
                Change::NotStarted { slot }
            }
            FrameProgress::Written { confirmed, total } if total > 0 && confirmed == total => {
                Change::Written { slot }
            }
            _ => return Ok(None),
        };
        Ok(Some(changed(change)))
    }

    pub(super) fn plan_messages_expired(
        &self,
        deliveries: &[crate::contract::message_expiry::ExpiredDelivery],
    ) -> Result<super::Change, Stop> {
        if deliveries.len() > 128 {
            return Err(IMPOSSIBLE);
        }
        if deliveries
            .iter()
            .enumerate()
            .any(|(index, expired)| deliveries[..index].contains(expired))
        {
            return Err(IMPOSSIBLE);
        }
        Ok(changed(Change::MessagesExpired))
    }

    pub(super) fn valid_outbound_fact(
        &self,
        delivery: DeliveryId,
        target: SessionId,
        revision: Revision,
        kind: (Qos, Option<DeliveryClaim>, Option<MessageExpiry>, bool),
        same: &[RecoveredFact],
        sessions: &[RecoveredFact],
    ) -> bool {
        let (qos, claim, expiry, started) = kind;
        if expiry.is_some_and(|expiry| expiry.deadline().is_err())
            || started != claim.is_some_and(|claim| claim.started)
            || (started && expiry.is_none())
        {
            return false;
        }
        let session_ok=self.sessions.as_slice().iter().any(|s|s.id==target&&s.revision==revision)||sessions.iter().any(|f|matches!(f,RecoveredFact::Session{session,revision:rev,..}if *session==target&&*rev==revision));
        let conflict = |id: DeliveryId, to: SessionId, other: Option<DeliveryClaim>| {
            id == delivery
                || (to == target
                    && claim.is_some()
                    && other.is_some_and(|c| Some(c.packet_id) == claim.map(|c| c.packet_id)))
                || claim.is_some()
                    && other.is_some_and(|c| Some(c.operation) == claim.map(|c| c.operation))
        };
        (claim.is_none()||qos==Qos::AtLeastOnce)&&session_ok&&self.pubsub.deliveries.len()+same.len()<self.pubsub.deliveries.capacity()
            &&claim.is_none_or(|c|c.operation.incarnation()==self.incarnation&&c.operation.epoch()<self.epoch)
            &&!self.pubsub.deliveries.as_slice().iter().any(|d|conflict(d.id,d.target,d.claim))
            &&!same.iter().any(|f|matches!(f,RecoveredFact::OutboundDelivery{delivery:id,target:to,claim:other,..}if conflict(*id,*to,*other)))
    }

    pub(super) fn add_outbound_fact(
        &mut self,
        id: DeliveryId,
        target: SessionId,
        revision: Revision,
        kind: (Qos, Option<DeliveryClaim>, Option<MessageExpiry>, bool),
    ) -> Result<(), CoreFault> {
        let (qos, claim, expiry, _) = kind;
        let mut delivery = Delivery::metadata(id, target, revision, qos, claim);
        delivery.expiry = expiry;
        self.pubsub
            .deliveries
            .try_push(delivery)
            .map_err(|_| CoreFault::StalePlan)
    }

    pub(super) fn apply_pubsub(&mut self, change: Change, input: Input) -> Result<(), CoreFault> {
        let stale = CoreFault::StalePlan;
        if matches!(&input, Input::Continue { continuation, .. } if self.release_continuation == Some(*continuation))
        {
            self.release_continuation = None;
        }
        match change {
            Change::RetainedSubmit { connection, flight } => {
                self.pubsub
                    .connections
                    .as_mut_slice()
                    .get_mut(connection)
                    .ok_or(stale)?
                    .last_qos0_ingress = flight.origin.ingress_order;
                self.pubsub.retained = Some(flight);
            }
            Change::RetainedDone { finished } => {
                self.pubsub.retained.take().ok_or(stale)?;
                remember(&mut self.finished, finished)?;
            }
            Change::Qos0Admit {
                connection,
                ingress_order,
                records,
                continuation,
            } => {
                self.pubsub
                    .connections
                    .as_mut_slice()
                    .get_mut(connection)
                    .ok_or(stale)?
                    .last_qos0_ingress = ingress_order;
                for record in records.as_slice() {
                    self.pubsub
                        .volatile
                        .try_push(record.clone())
                        .map_err(|_| stale)?;
                }
                if let Some(c) = continuation {
                    self.pubsub.continuation = Some(c);
                    self.pubsub.continuations_started = true;
                }
            }
            Change::Qos0Send {
                slot,
                effect,
                continuation,
            } => {
                self.pubsub
                    .volatile
                    .as_mut_slice()
                    .get_mut(slot)
                    .ok_or(stale)?
                    .frame = Some(effect);
                self.pubsub.continuation = continuation;
            }
            Change::Qos0Drop { slot, continuation } => {
                self.pubsub
                    .volatile
                    .as_mut_slice()
                    .get_mut(slot..)
                    .ok_or(stale)?
                    .rotate_left(1);
                let _ = self.pubsub.volatile.pop();
                self.pubsub.continuation = continuation;
            }
            Change::Qos0Done { slot } => {
                self.pubsub
                    .volatile
                    .as_mut_slice()
                    .get_mut(slot..)
                    .ok_or(stale)?
                    .rotate_left(1);
                let _ = self.pubsub.volatile.pop();
            }
            Change::Qos0Idle => self.pubsub.continuation = None,
            Change::Consume {
                slot,
                in_flight,
                connection,
            } => {
                self.pubsub
                    .deliveries
                    .as_mut_slice()
                    .get_mut(slot)
                    .ok_or(stale)?
                    .phase = Phase::ConsumePending(in_flight, connection);
            }
            Change::Consumed {
                slot,
                finished,
                emission,
            } => {
                let d = self
                    .pubsub
                    .deliveries
                    .as_mut_slice()
                    .get_mut(slot)
                    .ok_or(stale)?;
                d.phase = Phase::Qos0Writing(finished);
                d.claim_result = Some(finished);
                d.frame = emission;
                let target = d.target;
                self.pubsub.invalidate_reads(target);
                if emission.is_none() {
                    remember(&mut self.finished, finished)?;
                    let last = self.pubsub.deliveries.len() - 1;
                    self.pubsub.deliveries.as_mut_slice().swap(slot, last);
                    let _ = self.pubsub.deliveries.pop();
                }
            }
            Change::Settings { index, value } => match index {
                Some(i) => {
                    *self
                        .pubsub
                        .connections
                        .as_mut_slice()
                        .get_mut(i)
                        .ok_or(stale)? = value
                }
                None => self.pubsub.connections.try_push(value).map_err(|_| stale)?,
            },
            Change::Control(control) => self.pubsub.control = Some(control),
            Change::ControlEnd { revision, finished } => {
                let control = self.pubsub.control.take().ok_or(stale)?;
                if let Some(revision) = revision {
                    self.routes = control.projected;
                    self.route_revision = Some(revision);
                }
                remember(&mut self.finished, finished)?;
            }
            Change::Scan { connection, read } => {
                let state = self
                    .pubsub
                    .connections
                    .as_mut_slice()
                    .get_mut(connection)
                    .ok_or(stale)?;
                state.loading = Some(read);
                state.rescan_needed = false;
            }
            Change::Page {
                connection,
                discard,
                next,
            } => {
                let c = self
                    .pubsub
                    .connections
                    .as_mut_slice()
                    .get_mut(connection)
                    .ok_or(stale)?;
                c.loading = None;
                c.after = next;
                c.rescan_needed = discard;
                if !discard {
                    let Input::WorkPage { items, .. } = input else {
                        return Err(stale);
                    };
                    for item in items.as_slice() {
                        if let Some(i) = self.pubsub.delivery(item.delivery) {
                            let d = &mut self.pubsub.deliveries.as_mut_slice()[i];
                            if d.topic.is_none() {
                                d.retain = item.retain;
                                d.topic = Some(item.topic.clone());
                                d.payload = Some(item.payload.clone());
                            }
                        } else if !self.expired_unclaimed_item(item) {
                            let mut d = Delivery::metadata(
                                item.delivery,
                                item.target,
                                item.target_revision,
                                item.qos,
                                item.claim,
                            );
                            d.expiry = item.expiry;
                            d.retain = item.retain;
                            d.topic = Some(item.topic.clone());
                            d.payload = Some(item.payload.clone());
                            self.pubsub.deliveries.try_push(d).map_err(|_| stale)?;
                        }
                    }
                }
            }
            Change::Claim {
                slot,
                claim,
                in_flight,
                connection,
            } => {
                let generation = self
                    .pubsub
                    .connections
                    .as_slice()
                    .get(connection)
                    .ok_or(stale)?
                    .generation;
                let d = self
                    .pubsub
                    .deliveries
                    .as_mut_slice()
                    .get_mut(slot)
                    .ok_or(stale)?;
                d.claim = Some(claim);
                d.phase = Phase::ClaimPending(in_flight);
                d.window = Some(generation);
            }
            Change::Claimed {
                slot,
                finished,
                emission,
            } => {
                let d = self
                    .pubsub
                    .deliveries
                    .as_mut_slice()
                    .get_mut(slot)
                    .ok_or(stale)?;
                d.claim_result = Some(finished);
                d.phase = Phase::AwaitPuback;
                if let Some((effect, connection)) = emission {
                    d.window = Some(connection.generation);
                    if d.frame.is_some() {
                        d.certain_never_started = false;
                    }
                    d.frame = Some((effect, connection));
                    d.may_have_started = true;
                    d.local_write_completed = false;
                }
            }
            Change::Emit {
                slot,
                effect,
                connection,
            } => {
                let d = self
                    .pubsub
                    .deliveries
                    .as_mut_slice()
                    .get_mut(slot)
                    .ok_or(stale)?;
                d.window = Some(connection.generation);
                if d.frame.is_some() {
                    d.certain_never_started = false;
                }
                d.frame = Some((effect, connection));
                d.may_have_started = true;
                d.local_write_completed = false;
            }
            Change::Finish { slot, in_flight } => {
                self.pubsub
                    .deliveries
                    .as_mut_slice()
                    .get_mut(slot)
                    .ok_or(stale)?
                    .phase = Phase::FinishPending(in_flight)
            }
            Change::Finished { slot, finished } => {
                let d = self
                    .pubsub
                    .deliveries
                    .as_slice()
                    .get(slot)
                    .ok_or(stale)?
                    .clone();
                if let Some(known) = d.claim_result
                    && known.operation != finished.operation
                {
                    remember(&mut self.finished, known)?;
                }
                remember(&mut self.finished, finished)?;
                let last = self.pubsub.deliveries.len() - 1;
                self.pubsub.deliveries.as_mut_slice().swap(slot, last);
                let _ = self.pubsub.deliveries.pop();
                self.pubsub.invalidate_reads(d.target);
            }
            Change::Refused {
                slot,
                finished,
                claiming,
            } => {
                let d = self
                    .pubsub
                    .deliveries
                    .as_mut_slice()
                    .get_mut(slot)
                    .ok_or(stale)?;
                d.window = None;
                if claiming {
                    d.claim = None;
                    d.phase = Phase::Queued;
                } else {
                    d.phase = Phase::AwaitPuback;
                }
                remember(&mut self.finished, finished)?;
            }
            Change::Await {
                operation,
                phase,
                resolution,
            } => {
                let held = if let Some(control) = self
                    .pubsub
                    .control
                    .as_mut()
                    .filter(|c| c.in_flight.operation == operation)
                {
                    &mut control.in_flight
                } else if let Some(flight) = self
                    .pubsub
                    .retained
                    .as_mut()
                    .filter(|f| f.in_flight.operation == operation)
                {
                    &mut flight.in_flight
                } else {
                    let d = self
                        .pubsub
                        .deliveries
                        .as_mut_slice()
                        .iter_mut()
                        .find(|d| d.in_flight().is_some_and(|f| f.operation == operation))
                        .ok_or(stale)?;
                    match &mut d.phase {
                        Phase::ClaimPending(f)
                        | Phase::FinishPending(f)
                        | Phase::StartPending(f)
                        | Phase::ExpirePending(f)
                        | Phase::ConsumePending(f, _) => f,
                        _ => return Err(stale),
                    }
                };
                held.phase = phase;
                if let Some((deadline, effect)) = resolution {
                    held.deadline = deadline;
                    held.answers = held.answers.with(effect);
                }
            }
            Change::PubackObserved { slot } => {
                self.pubsub
                    .deliveries
                    .as_mut_slice()
                    .get_mut(slot)
                    .ok_or(stale)?
                    .early_puback = true;
            }
            Change::Start { slot, in_flight } => {
                let d = self
                    .pubsub
                    .deliveries
                    .as_mut_slice()
                    .get_mut(slot)
                    .ok_or(stale)?;
                d.certain_never_started = false;
                d.phase = Phase::StartPending(in_flight);
            }
            Change::Started {
                slot,
                finished,
                finish,
            } => {
                let d = self
                    .pubsub
                    .deliveries
                    .as_mut_slice()
                    .get_mut(slot)
                    .ok_or(stale)?;
                d.claim.as_mut().ok_or(stale)?.started = true;
                d.phase = finish.map_or(Phase::AwaitPuback, Phase::FinishPending);
                remember(&mut self.finished, finished)?;
            }
            Change::Expire { slot, in_flight } => {
                self.pubsub
                    .deliveries
                    .as_mut_slice()
                    .get_mut(slot)
                    .ok_or(stale)?
                    .phase = Phase::ExpirePending(in_flight);
            }
            Change::RetryDeferred {
                slot,
                finished,
                expiring,
            } => {
                let d = self
                    .pubsub
                    .deliveries
                    .as_mut_slice()
                    .get_mut(slot)
                    .ok_or(stale)?;
                d.phase = if expiring {
                    Phase::ExpireRetry
                } else {
                    Phase::StartRetry
                };
                if expiring {
                    d.frame = None;
                    d.window = None;
                }
                remember(&mut self.finished, finished)?;
            }
            Change::NotStarted { slot } => {
                let d = self
                    .pubsub
                    .deliveries
                    .as_mut_slice()
                    .get_mut(slot)
                    .ok_or(stale)?;
                d.frame = None;
                d.window = None;
                if d.certain_never_started {
                    d.may_have_started = false;
                }
            }
            Change::MessagesExpired => {
                let Input::MessagesExpired { deliveries } = input else {
                    return Err(stale);
                };
                for expired in deliveries.as_slice() {
                    let Some(slot) = self.pubsub.delivery(expired.delivery) else {
                        continue;
                    };
                    let d = &mut self.pubsub.deliveries.as_mut_slice()[slot];
                    // A stale maintenance answer cannot act on a now-known
                    // claim. A queued uncertain submission waits for its
                    // own correlated result and consumes its receipt once.
                    if d.target != expired.target
                        || (d.claim.is_some() && !matches!(d.phase, Phase::ClaimPending(_)))
                    {
                        continue;
                    }
                    let target = d.target;
                    if d.in_flight().is_some() {
                        d.retired = true;
                        d.window = None;
                        d.topic = None;
                        d.payload = None;
                    } else {
                        let last = self.pubsub.deliveries.len() - 1;
                        self.pubsub.deliveries.as_mut_slice().swap(slot, last);
                        let _ = self.pubsub.deliveries.pop();
                    }
                    self.pubsub.invalidate_reads(target);
                }
            }
            Change::Written { slot } => {
                let d = self
                    .pubsub
                    .deliveries
                    .as_mut_slice()
                    .get_mut(slot)
                    .ok_or(stale)?;
                d.local_write_completed = true;
                d.certain_never_started = false;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::command::StartupNonce;
    use crate::contract::effects::{EffectSink, SubmitResult};
    use crate::contract::outcome::{EpochOpened, EpochOpening, OpeningOutcome, OpeningReply};
    use alloc::vec::Vec;

    const TARGET: SessionId = SessionId::new(1);
    const CONNECTION: ConnectionGeneration = ConnectionGeneration::new(10);
    const TURN: TurnGrant = TurnGrant {
        work_limit: WorkUnits::new(100_000),
    };

    struct Taking(Vec<OwnedEffect>);
    impl EffectSink for Taking {
        fn try_submit(&mut self, effect: OwnedEffect) -> SubmitResult {
            self.0.push(effect);
            SubmitResult::Accepted
        }
    }

    fn bytes(value: &[u8]) -> SharedBytes {
        SharedBytes::copy_from(value, 128).unwrap()
    }
    fn nonzero(value: u32) -> NonZeroU32 {
        NonZeroU32::new(value).unwrap()
    }
    fn drain(owner: &mut Owner) -> Vec<(EffectId, Effect)> {
        let mut sink = Taking(Vec::new());
        let _ = owner.dispatch(&mut sink);
        sink.0.into_iter().map(OwnedEffect::into_parts).collect()
    }
    fn apply(owner: &mut Owner, input: Input) -> Vec<(EffectId, Effect)> {
        let result = owner.step(input, TURN);
        assert!(matches!(result, StepOutcome::Applied { .. }), "{result:?}");
        drain(owner)
    }
    fn world() -> Owner {
        let incarnation = StoreIncarnation::new(1);
        let epoch = RunEpoch::new(2);
        let mut opening = EpochOpening::new(incarnation, StartupNonce::new([7; 16]));
        let command = opening.submit(RunEpoch::new(1)).unwrap();
        let _ = opening
            .observe(OpeningReply {
                command,
                outcome: OpeningOutcome::Committed(EpochOpened { incarnation, epoch }),
            })
            .unwrap();
        let mut authority =
            OwnerAuthority::from_grant(opening.hand_over().unwrap(), nonzero(1)).unwrap();
        let outbox = authority
            .create_outbox(
                LogicalOwnerId::new(4),
                NonZeroUsize::new(32).unwrap(),
                nonzero(8),
            )
            .unwrap();
        let config = OwnerConfig {
            max_sessions: nonzero(2),
            max_routes: nonzero(2),
            max_exchanges: nonzero(8),
            account: AccountId::new(1),
            payload_bytes: PayloadBytes::new(1024),
            resolution_delay: LogicalDuration::new(1),
            retained_operations: nonzero(16),
            remembered_connections: nonzero(4),
        };
        let mut owner = Owner::new(config, outbox).unwrap();
        let facts = [
            RecoveredFact::Epoch { incarnation, epoch },
            RecoveredFact::Limits {
                max_targets_per_admission: 2,
                max_topic_bytes: 128,
                max_payload_bytes: PayloadBytes::new(128),
            },
            RecoveredFact::RouteTable {
                revision: Revision::new(0),
            },
            RecoveredFact::Session {
                session: TARGET,
                client_id: bytes(b"subscriber"),
                revision: Revision::new(1),
                expiry: crate::contract::session::SessionExpiry::Never,
                opened_by: None,
                detached: None,
            },
            RecoveredFact::Session {
                session: SessionId::new(2),
                client_id: bytes(b"other"),
                revision: Revision::new(1),
                expiry: crate::contract::session::SessionExpiry::Never,
                opened_by: None,
                detached: None,
            },
        ];
        assert!(
            apply(
                &mut owner,
                Input::Recovery {
                    facts: BoundedVec::from_slice(&facts, facts.len()).unwrap(),
                    last: true
                }
            )
            .is_empty()
        );
        let _ = apply(
            &mut owner,
            Input::Packet {
                connection: CONNECTION,
                ingress_order: 0,
                packet: InboundPacket::ConnectResume {
                    client_id: bytes(b"subscriber"),
                },
            },
        );
        assert!(
            apply(
                &mut owner,
                Input::ConnectionSettings {
                    connection: CONNECTION,
                    receive_maximum: 8,
                    maximum_packet_size: 65536
                }
            )
            .is_empty()
        );
        owner
    }
    fn delivery(index: u32) -> DeliveryId {
        DeliveryId {
            publication: PublicationId::new(
                StoreIncarnation::new(1),
                RunEpoch::new(1),
                LogicalOwnerId::new(4),
                PublicationSeq::new(1),
            ),
            index,
        }
    }
    fn queued(owner: &mut Owner, index: u32) {
        let mut item = Delivery::metadata(
            delivery(index),
            TARGET,
            Revision::new(1),
            Qos::AtLeastOnce,
            None,
        );
        item.topic = Some(bytes(b"topic"));
        item.payload = Some(bytes(b"payload"));
        owner.pubsub.deliveries.try_push(item).unwrap();
    }
    fn known(owner: &mut Owner, effect: EffectId, command: &Command) -> Vec<(EffectId, Effect)> {
        let facts = match command.recipe.get() {
            Recipe::ClaimDelivery(c) => CommitFacts::DeliveryClaimed {
                delivery: c.delivery,
                target: c.target,
                packet_id: c.packet_id,
                claim: c.operation,
            },
            Recipe::FinishDelivery(c) => CommitFacts::DeliveryFinished {
                delivery: c.delivery,
                target: c.target,
                packet_id: c.packet_id,
                claim: c.claim,
            },
            Recipe::MarkDeliveryStarted(c) => CommitFacts::DeliveryStarted {
                delivery: c.delivery,
                target: c.target,
                packet_id: c.packet_id,
                claim: c.claim,
            },
            Recipe::ExpireDelivery(c) => CommitFacts::DeliveryExpired {
                delivery: c.finish.delivery,
                target: c.finish.target,
                packet_id: c.finish.packet_id,
                claim: c.finish.claim,
            },
            Recipe::ConsumeDelivery(c) => CommitFacts::DeliveryConsumed {
                delivery: c.delivery,
                target: c.target,
            },
            _ => panic!("delivery command"),
        };
        apply(
            owner,
            Input::StoreResult {
                effect,
                operation: command.recipe.operation(),
                reply: StoreReply::Outcome(StoreOutcome::Committed(facts)),
            },
        )
    }

    #[test]
    fn an_applied_finish_reuses_a_free_identifier_after_the_historical_ceiling() {
        let mut owner = world();
        // The committed red fixture positioned the old cursor at 65535.
        // There is now no historical allocation cursor to position; both
        // completed cycles below still require a new claim on this socket.
        queued(&mut owner, 0);
        let claim = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        let Effect::Persist { command } = &claim[0].1 else {
            panic!("initial claim")
        };
        let packet_id = match command.recipe.get() {
            Recipe::ClaimDelivery(c) => c.packet_id,
            _ => panic!("claim"),
        };
        let emitted = known(&mut owner, claim[0].0, command);
        assert!(matches!(emitted[0].1, Effect::EmitPublish { .. }));
        let finish = apply(
            &mut owner,
            Input::Packet {
                connection: CONNECTION,
                ingress_order: 1,
                packet: InboundPacket::Puback {
                    packet_id,
                    reason: 0,
                },
            },
        );
        let Effect::Persist { command } = &finish[0].1 else {
            panic!("finish")
        };
        assert!(known(&mut owner, finish[0].0, command).is_empty());
        queued(&mut owner, 1);
        let next = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        assert!(
            matches!(next.first(),Some((_,Effect::Persist {command})) if matches!(command.recipe.get(),Recipe::ClaimDelivery(_))),
            "outgoing_pi_historical_exhaustion: a free identifier must permit the next delivery on the same connection: {next:?}"
        );
        let Effect::Persist { command } = &next[0].1 else {
            panic!("new claim")
        };
        let Recipe::ClaimDelivery(claim) = command.recipe.get() else {
            panic!("claim")
        };
        assert_eq!(
            claim.packet_id.get(),
            1,
            "the allocator prefers the smallest free number"
        );
    }

    #[test]
    fn lifecycle_retired_claim_finish_and_consume_results_close_receipt_holes_without_emitting() {
        use crate::contract::receipts::ReceiptCollectionReply;
        use crate::contract::session::{SessionCancellations, SessionChange, SessionEndReason};
        for phase in 0..3 {
            let mut owner = world();
            let registration = apply(&mut owner, Input::MaintainReceipts);
            let Effect::CollectReceipts { collection } = registration[0].1 else {
                panic!("register origin")
            };
            apply(
                &mut owner,
                Input::ReceiptsCollected {
                    effect: registration[0].0,
                    collection,
                    reply: ReceiptCollectionReply::Applied {
                        closure: collection.closure,
                        removed: 0,
                        retained: 0,
                        more: false,
                    },
                },
            );
            queued(&mut owner, 0);
            if phase == 2 {
                owner.pubsub.deliveries.as_mut_slice()[0].qos = Qos::AtMostOnce;
            }
            let mut other = Delivery::metadata(
                delivery(1),
                SessionId::new(2),
                Revision::new(1),
                Qos::AtLeastOnce,
                None,
            );
            other.topic = Some(bytes(b"topic"));
            other.payload = Some(bytes(b"shared-payload"));
            owner.pubsub.deliveries.try_push(other.clone()).unwrap();
            let mut pending = apply(&mut owner, Input::WorkAvailable { session: TARGET });
            if phase == 1 {
                let Effect::Persist { command } = &pending[0].1 else {
                    panic!("claim")
                };
                known(&mut owner, pending[0].0, command);
                pending = apply(
                    &mut owner,
                    Input::Packet {
                        connection: CONNECTION,
                        ingress_order: 1,
                        packet: InboundPacket::Puback {
                            packet_id: OutboundPacketId::new(1).unwrap(),
                            reason: 0,
                        },
                    },
                );
            }
            let (old_effect, command) = pending
                .iter()
                .find_map(|(id, e)| match e {
                    Effect::Persist { command } => Some((*id, command.clone())),
                    _ => None,
                })
                .unwrap();
            apply(
                &mut owner,
                Input::StoreResult {
                    effect: old_effect,
                    operation: command.recipe.operation(),
                    reply: StoreReply::Outcome(StoreOutcome::PendingOrUnknown),
                },
            );
            let close = apply(
                &mut owner,
                Input::EndSession {
                    connection: ConnectionKey {
                        session: TARGET,
                        generation: CONNECTION,
                    },
                    opened_by: None,
                    reason: SessionEndReason::Disconnect,
                },
            );
            let (close_effect, close_operation) = close
                .iter()
                .find_map(|(id, e)| match e {
                    Effect::Persist { command } => Some((*id, command.recipe.operation())),
                    _ => None,
                })
                .unwrap();
            apply(
                &mut owner,
                Input::StoreResult {
                    effect: close_effect,
                    operation: close_operation,
                    reply: StoreReply::Outcome(StoreOutcome::Committed(
                        CommitFacts::SessionLifecycle(SessionChange::Closed {
                            session: TARGET,
                            route_revision: Revision::new(0),
                            cancelled: SessionCancellations {
                                inbound: 0,
                                deliveries: 1,
                                routes: 0,
                            },
                        }),
                    )),
                },
            );
            assert_eq!(owner.session_status().retired_flights, 1);
            let result = known(&mut owner, old_effect, &command);
            assert!(
                !result.iter().any(|(_, e)| matches!(
                    e,
                    Effect::EmitPublish { .. } | Effect::EmitQos0 { .. } | Effect::Persist { .. }
                )),
                "retired_delivery_result_reemitted phase={phase}"
            );
            assert_eq!(owner.session_status().retired_flights, 0);
            assert_eq!(
                owner.pubsub.deliveries.as_slice(),
                &[other],
                "other_destination_responsibility_changed phase={phase}"
            );
            owner.receipts.test_request_sweep();
            let gc = apply(&mut owner, Input::MaintainReceipts);
            let Effect::CollectReceipts { collection } = gc[0].1 else {
                panic!("collect consumed flights")
            };
            assert_eq!(
                collection.closure.through,
                close_operation.sequence(),
                "retired_delivery_receipt_hole_never_closed phase={phase}"
            );
        }
    }

    fn operation(sequence: u64) -> OperationId {
        OperationId::new(
            StoreIncarnation::new(1),
            RunEpoch::new(2),
            LogicalOwnerId::new(4),
            OperationSeq::new(sequence),
        )
    }
    fn flight(sequence: u64) -> InFlight {
        InFlight {
            operation: operation(sequence),
            phase: CommandPhase::Indeterminate,
            deadline: DeadlineToken::new(sequence),
            answers: Answers::new(EffectId {
                epoch: RunEpoch::new(2),
                owner: LogicalOwnerId::new(4),
                sequence: EffectSeq::new(sequence),
            }),
        }
    }

    #[test]
    fn the_complete_small_domain_counts_unknown_finish_and_unloaded_recovered_claims() {
        let mut owner = world();
        for raw in 1..=4 {
            let claim = DeliveryClaim {
                started: false,
                operation: operation(u64::from(raw)),
                packet_id: OutboundPacketId::new(raw).unwrap(),
            };
            let mut item = Delivery::metadata(
                delivery(u32::from(raw)),
                TARGET,
                Revision::new(1),
                Qos::AtLeastOnce,
                Some(claim),
            );
            item.phase = match raw {
                1 => Phase::ClaimPending(flight(1)),
                2 => Phase::AwaitPuback,
                3 => Phase::FinishPending(flight(5)),
                _ => Phase::AwaitPuback,
            };
            assert!(
                item.payload.is_none(),
                "recovered claims reserve before payload loading"
            );
            owner.pubsub.deliveries.try_push(item).unwrap();
        }
        assert_eq!(
            owner.pubsub.free_packet_id(TARGET, 4),
            None,
            "occupied_pi_reused"
        );
        assert_eq!(owner.pubsub.free_packet_id(TARGET, 5).unwrap().get(), 5);
        assert_eq!(
            owner
                .pubsub
                .free_packet_id(SessionId::new(2), 4)
                .unwrap()
                .get(),
            1,
            "destinations have independent sender identifier spaces"
        );
        let mut pure_zero = Delivery::metadata(
            delivery(10),
            TARGET,
            Revision::new(1),
            Qos::AtMostOnce,
            None,
        );
        pure_zero.payload = Some(bytes(b"zero"));
        owner.pubsub.deliveries.try_push(pure_zero).unwrap();
        assert_eq!(
            owner.pubsub.free_packet_id(TARGET, 4),
            None,
            "QoS0 neither returns nor takes a QoS1 identifier"
        );
        let ping = apply(
            &mut owner,
            Input::Packet {
                connection: CONNECTION,
                ingress_order: 4,
                packet: InboundPacket::Pingreq,
            },
        );
        assert!(
            matches!(
                ping[0].1,
                Effect::EmitFrame {
                    frame: OutboundFrame::Pingresp,
                    ..
                }
            ),
            "a genuinely full finite domain does not block control transitions"
        );
    }

    #[test]
    fn a_finish_committed_but_not_known_and_its_resolution_keep_the_identifier_reserved() {
        let mut owner = world();
        queued(&mut owner, 0);
        let claim = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        let Effect::Persist { command } = &claim[0].1 else {
            panic!("claim")
        };
        let claim_operation = command.recipe.operation();
        let _ = known(&mut owner, claim[0].0, command);
        let finish = apply(
            &mut owner,
            Input::Packet {
                connection: CONNECTION,
                ingress_order: 1,
                packet: InboundPacket::Puback {
                    packet_id: OutboundPacketId::new(1).unwrap(),
                    reason: 0x80,
                },
            },
        );
        let Effect::Persist { command } = &finish[0].1 else {
            panic!("negative ACK still has durable termination")
        };
        let finish_operation = command.recipe.operation();
        assert_ne!(claim_operation, finish_operation);
        assert!(
            apply(
                &mut owner,
                Input::StoreResult {
                    effect: finish[0].0,
                    operation: finish_operation,
                    reply: StoreReply::Outcome(StoreOutcome::PendingOrUnknown)
                }
            )
            .is_empty()
        );
        assert_eq!(
            owner.pubsub.free_packet_id(TARGET, 4).unwrap().get(),
            2,
            "finish_unknown_freed_pi"
        );
        let Effect::ArmDeadline { token, .. } = finish[1].1 else {
            panic!("finish deadline")
        };
        let resolution = apply(
            &mut owner,
            Input::Deadline {
                token,
                now: crate::contract::ids::LogicalTime::new(10),
            },
        );
        assert!(
            matches!(resolution[0].1,Effect::ResolveAndFence {operation} if operation==finish_operation)
        );
        assert_eq!(owner.pubsub.free_packet_id(TARGET, 4).unwrap().get(), 2);
        assert!(known(&mut owner, resolution[0].0, command).is_empty());
        assert_eq!(owner.pubsub.free_packet_id(TARGET, 4).unwrap().get(), 1);
        queued(&mut owner, 1);
        let next = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        let Effect::Persist { command } = &next[0].1 else {
            panic!("new claim")
        };
        let Recipe::ClaimDelivery(new) = command.recipe.get() else {
            panic!("claim")
        };
        assert_eq!(new.packet_id.get(), 1);
        assert_ne!(new.operation, claim_operation);
        assert_ne!(new.operation, finish_operation);
    }

    #[test]
    fn a_submitted_unknown_claim_reserves_its_identifier_before_any_publish_or_callback() {
        let mut owner = world();
        queued(&mut owner, 0);
        queued(&mut owner, 1);
        let claim = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        let Effect::Persist { command } = &claim[0].1 else {
            panic!("claim")
        };
        let op = command.recipe.operation();
        assert_eq!(owner.pubsub.free_packet_id(TARGET, 4).unwrap().get(), 2);
        assert!(
            apply(
                &mut owner,
                Input::StoreResult {
                    effect: claim[0].0,
                    operation: op,
                    reply: StoreReply::Outcome(StoreOutcome::PendingOrUnknown)
                }
            )
            .is_empty()
        );
        assert_eq!(
            owner.pubsub.free_packet_id(TARGET, 4).unwrap().get(),
            2,
            "unknown_claim_pi_reused"
        );
        assert!(
            apply(&mut owner, Input::WorkAvailable { session: TARGET }).is_empty(),
            "later delivery cannot overtake the earlier unknown claim"
        );
    }

    #[test]
    fn collecting_a_known_claim_receipt_does_not_free_the_live_packet_identifier() {
        use crate::contract::receipts::ReceiptCollectionReply;
        let mut owner = world();
        let registration = apply(&mut owner, Input::MaintainReceipts);
        let Effect::CollectReceipts { collection } = registration[0].1 else {
            panic!("registration")
        };
        let _ = apply(
            &mut owner,
            Input::ReceiptsCollected {
                effect: registration[0].0,
                collection,
                reply: ReceiptCollectionReply::Applied {
                    closure: collection.closure,
                    removed: 0,
                    retained: 0,
                    more: false,
                },
            },
        );
        queued(&mut owner, 0);
        let claim = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        let Effect::Persist { command } = &claim[0].1 else {
            panic!("claim")
        };
        let _ = known(&mut owner, claim[0].0, command);
        owner.receipts.test_request_sweep();
        let gc = apply(&mut owner, Input::MaintainReceipts);
        let Effect::CollectReceipts { collection } = gc[0].1 else {
            panic!("claim receipt collection")
        };
        assert_eq!(collection.closure.through.get(), 1);
        let _ = apply(
            &mut owner,
            Input::ReceiptsCollected {
                effect: gc[0].0,
                collection,
                reply: ReceiptCollectionReply::Applied {
                    closure: collection.closure,
                    removed: 1,
                    retained: 0,
                    more: false,
                },
            },
        );
        assert_eq!(owner.pubsub.free_packet_id(TARGET, 4).unwrap().get(), 2);
        let finish = apply(
            &mut owner,
            Input::Packet {
                connection: CONNECTION,
                ingress_order: 1,
                packet: InboundPacket::Puback {
                    packet_id: OutboundPacketId::new(1).unwrap(),
                    reason: 0,
                },
            },
        );
        let Effect::Persist { command } = &finish[0].1 else {
            panic!("finish after claim receipt retirement")
        };
        assert!(known(&mut owner, finish[0].0, command).is_empty());
        assert_eq!(owner.pubsub.free_packet_id(TARGET, 4).unwrap().get(), 1);
    }

    #[test]
    fn an_observed_puback_target_cannot_be_rebound_after_identifier_reuse() {
        let mut owner = world();
        queued(&mut owner, 0);
        let claim = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        let Effect::Persist { command } = &claim[0].1 else {
            panic!("claim A")
        };
        let _ = known(&mut owner, claim[0].0, command);
        let key = ConnectionKey {
            session: TARGET,
            generation: CONNECTION,
        };
        let packet_id = OutboundPacketId::new(1).unwrap();
        let target = owner.puback_target(key, packet_id).unwrap();
        let finish = apply(
            &mut owner,
            Input::ObservedPuback {
                connection: key,
                packet_id,
                reason: 0,
                target: Some(target),
            },
        );
        let Effect::Persist { command } = &finish[0].1 else {
            panic!("finish A")
        };
        assert!(known(&mut owner, finish[0].0, command).is_empty());
        queued(&mut owner, 1);
        let claim_b = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        let Effect::Persist { command } = &claim_b[0].1 else {
            panic!("claim B")
        };
        let _ = known(&mut owner, claim_b[0].0, command);
        let new_target = owner.puback_target(key, packet_id).unwrap();
        assert_ne!(target, new_target);
        let effects = apply(
            &mut owner,
            Input::ObservedPuback {
                connection: key,
                packet_id,
                reason: 0,
                target: Some(target),
            },
        );
        assert!(
            effects.is_empty(),
            "stale_puback_rebound: retained A observation must not finish B: {effects:?}"
        );
        assert_eq!(owner.puback_target(key, packet_id), Some(new_target));
    }

    #[test]
    fn an_observed_puback_without_a_target_cannot_acquire_a_future_claim() {
        let mut owner = world();
        let key = ConnectionKey {
            session: TARGET,
            generation: CONNECTION,
        };
        let packet_id = OutboundPacketId::new(1).unwrap();
        let observed = owner.puback_target(key, packet_id);
        assert_eq!(observed, None);
        queued(&mut owner, 0);
        let claim = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        let Effect::Persist { command } = &claim[0].1 else {
            panic!("claim")
        };
        let _ = known(&mut owner, claim[0].0, command);
        let effects = apply(
            &mut owner,
            Input::ObservedPuback {
                connection: key,
                packet_id,
                reason: 0,
                target: observed,
            },
        );
        assert!(
            effects.is_empty(),
            "untargeted_puback_rebound: an immutable no-target observation cannot finish a later claim: {effects:?}"
        );
    }

    #[test]
    fn a_valid_puback_can_precede_the_local_write_callback_and_duplicates_do_not_finish_twice() {
        let mut owner = world();
        queued(&mut owner, 0);
        let claim = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        let Effect::Persist { command } = &claim[0].1 else {
            panic!("claim")
        };
        let _ = known(&mut owner, claim[0].0, command);
        assert!(!owner.pubsub.deliveries.as_slice()[0].local_write_completed);
        let key = ConnectionKey {
            session: TARGET,
            generation: CONNECTION,
        };
        let pi = OutboundPacketId::new(1).unwrap();
        let target = owner.puback_target(key, pi).unwrap();
        let finish = apply(
            &mut owner,
            Input::ObservedPuback {
                connection: key,
                packet_id: pi,
                reason: 0,
                target: Some(target),
            },
        );
        assert!(matches!(finish[0].1, Effect::Persist { .. }));
        assert_eq!(
            owner.puback_target(key, pi),
            None,
            "FinishPending has no second eligible ACK target"
        );
        assert_eq!(owner.pubsub.active(CONNECTION), 1);
        let counter = owner.counters.operation;
        assert!(
            apply(
                &mut owner,
                Input::ObservedPuback {
                    connection: key,
                    packet_id: pi,
                    reason: 0,
                    target: Some(target)
                }
            )
            .is_empty()
        );
        assert_eq!(owner.counters.operation, counter);
        assert_eq!(owner.pubsub.active(CONNECTION), 1);
        let Effect::Persist { command } = &finish[0].1 else {
            panic!("finish")
        };
        assert!(known(&mut owner, finish[0].0, command).is_empty());
        assert_eq!(owner.pubsub.active(CONNECTION), 0);
    }

    #[test]
    fn an_immutable_target_requires_every_causal_identity_and_the_current_connection() {
        let mut owner = world();
        queued(&mut owner, 0);
        let claim = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        let Effect::Persist { command } = &claim[0].1 else {
            panic!("claim")
        };
        let _ = known(&mut owner, claim[0].0, command);
        let key = ConnectionKey {
            session: TARGET,
            generation: CONNECTION,
        };
        let pi = OutboundPacketId::new(1).unwrap();
        let target = owner.puback_target(key, pi).unwrap();
        let wrong_frame = EffectId {
            sequence: EffectSeq::new(999),
            ..target.frame
        };
        let cases = [
            (
                key,
                pi,
                Some(PubackTarget {
                    delivery: delivery(99),
                    ..target
                }),
            ),
            (
                key,
                pi,
                Some(PubackTarget {
                    claim: operation(999),
                    ..target
                }),
            ),
            (
                key,
                pi,
                Some(PubackTarget {
                    frame: wrong_frame,
                    ..target
                }),
            ),
            (key, OutboundPacketId::new(2).unwrap(), Some(target)),
            (
                ConnectionKey {
                    session: SessionId::new(2),
                    ..key
                },
                pi,
                Some(target),
            ),
            (
                ConnectionKey {
                    generation: ConnectionGeneration::new(11),
                    ..key
                },
                pi,
                Some(target),
            ),
        ];
        let counter = owner.counters.operation;
        for (connection, packet_id, target) in cases {
            assert!(
                apply(
                    &mut owner,
                    Input::ObservedPuback {
                        connection,
                        packet_id,
                        reason: 0,
                        target
                    }
                )
                .is_empty()
            );
            assert_eq!(owner.counters.operation, counter);
            assert_eq!(owner.pubsub.active(CONNECTION), 1);
        }
        assert_eq!(owner.puback_target(key, pi), Some(target));
    }

    #[test]
    fn old_finish_results_and_frame_callbacks_cannot_change_a_reused_slot() {
        let mut owner = world();
        queued(&mut owner, 0);
        let claim = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        let Effect::Persist { command } = &claim[0].1 else {
            panic!("claim A")
        };
        let emitted = known(&mut owner, claim[0].0, command);
        let key = ConnectionKey {
            session: TARGET,
            generation: CONNECTION,
        };
        let pi = OutboundPacketId::new(1).unwrap();
        let original = owner.puback_target(key, pi).unwrap();
        assert_eq!(emitted[0].0, original.frame);
        let finish = apply(
            &mut owner,
            Input::ObservedPuback {
                connection: key,
                packet_id: pi,
                reason: 0,
                target: Some(original),
            },
        );
        let Effect::Persist { command } = &finish[0].1 else {
            panic!("finish A")
        };
        let _ = known(&mut owner, finish[0].0, command);
        queued(&mut owner, 1);
        let claim_b = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        let Effect::Persist { command: second } = &claim_b[0].1 else {
            panic!("claim B")
        };
        let _ = known(&mut owner, claim_b[0].0, second);
        let current = owner.puback_target(key, pi).unwrap();
        assert_ne!(current.claim, original.claim);
        assert!(
            known(&mut owner, finish[0].0, command).is_empty(),
            "old_finish_released_reused_pi"
        );
        let before_callback = owner.pubsub.deliveries.as_slice().to_vec();
        let before_counters = owner.counters;
        assert!(
            apply(
                &mut owner,
                Input::FrameResult {
                    effect: original.frame,
                    connection: key,
                    progress: FrameProgress::Written {
                        confirmed: 20,
                        total: 20
                    }
                }
            )
            .is_empty()
        );
        assert_eq!(
            owner.pubsub.deliveries.as_slice(),
            before_callback.as_slice(),
            "stale_outgoing_frame_result_applied"
        );
        assert_eq!(owner.counters, before_counters);
        assert_eq!(owner.puback_target(key, pi), Some(current));
        assert_eq!(
            owner.pubsub.active(CONNECTION),
            1,
            "old_callback_returned_current_credit"
        );
        assert!(
            !owner.pubsub.deliveries.as_slice()[0].local_write_completed,
            "old_callback_rebound: a local callback belongs to its exact frame occurrence"
        );
    }

    #[test]
    fn takeover_keeps_claim_and_number_but_replaces_the_connection_and_emission_target() {
        let mut owner = world();
        queued(&mut owner, 0);
        let claim = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        let Effect::Persist { command } = &claim[0].1 else {
            panic!("claim")
        };
        let _ = known(&mut owner, claim[0].0, command);
        let pi = OutboundPacketId::new(1).unwrap();
        let old_key = ConnectionKey {
            session: TARGET,
            generation: CONNECTION,
        };
        let original = owner.puback_target(old_key, pi).unwrap();
        let replacement = ConnectionGeneration::new(11);
        let _ = apply(
            &mut owner,
            Input::Packet {
                connection: replacement,
                ingress_order: 0,
                packet: InboundPacket::ConnectResume {
                    client_id: bytes(b"subscriber"),
                },
            },
        );
        let _ = apply(
            &mut owner,
            Input::ConnectionSettings {
                connection: replacement,
                receive_maximum: 8,
                maximum_packet_size: 65536,
            },
        );
        assert_eq!(owner.puback_target(old_key, pi), None);
        assert_eq!(owner.pubsub.free_packet_id(TARGET, 4).unwrap().get(), 2);
        let replay = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        assert!(
            matches!(replay[0].1,Effect::EmitPublish {packet_id,claim,dup:true,..} if packet_id==pi && claim==original.claim)
        );
        let key = ConnectionKey {
            session: TARGET,
            generation: replacement,
        };
        let current = owner.puback_target(key, pi).unwrap();
        assert_eq!(current.delivery, original.delivery);
        assert_eq!(current.claim, original.claim);
        assert_ne!(current.frame, original.frame);
        assert!(
            apply(
                &mut owner,
                Input::ObservedPuback {
                    connection: old_key,
                    packet_id: pi,
                    reason: 0,
                    target: Some(original)
                }
            )
            .is_empty()
        );
        assert!(
            apply(
                &mut owner,
                Input::ObservedPuback {
                    connection: key,
                    packet_id: pi,
                    reason: 0,
                    target: Some(original)
                }
            )
            .is_empty()
        );
        assert!(
            apply(
                &mut owner,
                Input::FrameResult {
                    effect: original.frame,
                    connection: old_key,
                    progress: FrameProgress::Written {
                        confirmed: 20,
                        total: 20
                    }
                }
            )
            .is_empty()
        );
        assert_eq!(owner.puback_target(key, pi), Some(current));
        let finish = apply(
            &mut owner,
            Input::ObservedPuback {
                connection: key,
                packet_id: pi,
                reason: 0,
                target: Some(current),
            },
        );
        let Effect::Persist { command } = &finish[0].1 else {
            panic!("current target finishes")
        };
        let _ = known(&mut owner, finish[0].0, command);
        assert_eq!(owner.pubsub.free_packet_id(TARGET, 4).unwrap().get(), 1);
    }

    #[test]
    fn an_unknown_claim_keeps_its_packet_identifier_unavailable() {
        let mut owner = world();
        queued(&mut owner, 0);
        let claim = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        let Effect::Persist { command } = &claim[0].1 else {
            panic!("claim")
        };
        assert!(
            apply(
                &mut owner,
                Input::StoreResult {
                    effect: claim[0].0,
                    operation: command.recipe.operation(),
                    reply: StoreReply::Outcome(StoreOutcome::PendingOrUnknown)
                }
            )
            .is_empty()
        );
        assert_eq!(
            owner.pubsub.free_packet_id(TARGET, 4).unwrap().get(),
            2,
            "occupied_unknown_pi_reused"
        );
    }
    fn retained_zero(order: u64) -> Input {
        Input::Packet {
            connection: CONNECTION,
            ingress_order: order,
            packet: InboundPacket::Publish {
                expiry: None,
                qos: Qos::AtMostOnce,
                retain: true,
                dup: false,
                packet_id: None,
                topic: bytes(b"retained/value"),
                payload: bytes(b"value"),
            },
        }
    }
    fn pending_retained(owner: &mut Owner) -> (EffectId, Command, RetainedFlight) {
        owner.lifecycle_enabled = true;
        let effects = apply(owner, retained_zero(1));
        let (effect, command) = effects
            .into_iter()
            .find_map(|(effect, item)| match item {
                Effect::Persist { command } => Some((effect, command)),
                _ => None,
            })
            .unwrap();
        let flight = owner.pubsub.retained.unwrap();
        (effect, command, flight)
    }
    #[test]
    fn retained_qos0_unknown_survives_source_termination_and_resolves_without_an_ack() {
        use crate::contract::session::{SessionCancellations, SessionChange, SessionEndReason};
        let mut owner = world();
        let (effect, command, flight) = pending_retained(&mut owner);
        let operation = command.recipe.operation();
        let Recipe::RetainedQos0(value) = command.recipe.get() else {
            panic!("retained QoS0")
        };
        assert_eq!(value.costs.exchange_slots, ExchangeSlots::ZERO);
        assert_eq!(value.costs.references, References::new(1));
        assert!(owner.slots.is_empty());
        assert_eq!(owner.leases.held(), 0);
        assert!(
            apply(
                &mut owner,
                Input::StoreResult {
                    effect,
                    operation,
                    reply: StoreReply::Outcome(StoreOutcome::PendingOrUnknown)
                }
            )
            .is_empty()
        );
        assert!(matches!(
            owner.step(retained_zero(2), TURN),
            StepOutcome::NeedCapacity {
                need: CapacityNeed {
                    resource: Resource::ResultSlots,
                    ..
                },
                ..
            }
        ));
        apply(
            &mut owner,
            Input::ConnectionClosed {
                connection: CONNECTION,
            },
        );
        let close = apply(
            &mut owner,
            Input::EndSession {
                connection: ConnectionKey {
                    session: TARGET,
                    generation: CONNECTION,
                },
                opened_by: None,
                reason: SessionEndReason::Disconnect,
            },
        );
        let (close_effect, close_operation) = close
            .iter()
            .find_map(|(effect, item)| match item {
                Effect::Persist { command } => Some((*effect, command.recipe.operation())),
                _ => None,
            })
            .unwrap();
        apply(
            &mut owner,
            Input::StoreResult {
                effect: close_effect,
                operation: close_operation,
                reply: StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::SessionLifecycle(
                    SessionChange::Closed {
                        session: TARGET,
                        route_revision: Revision::new(0),
                        cancelled: SessionCancellations::default(),
                    },
                ))),
            },
        );
        assert!(
            owner.pubsub.retained.unwrap().retired,
            "source_termination_lost_unknown_retained_qos0"
        );
        assert_eq!(owner.pubsub.retired_flights(), 1);
        let resolution = apply(
            &mut owner,
            Input::Deadline {
                token: flight.in_flight.deadline,
                now: crate::contract::ids::LogicalTime::new(10000),
            },
        );
        let (resolved, actual) = resolution
            .iter()
            .find_map(|(effect, item)| match item {
                Effect::ResolveAndFence { operation } => Some((*effect, *operation)),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            actual, operation,
            "retained_qos0_resolution_changed_operation"
        );
        assert!(
            !resolution
                .iter()
                .any(|(_, item)| matches!(item, Effect::Persist { .. } | Effect::EmitFrame { .. }))
        );
        let done = StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::RetainedQos0Stored {
            publication: flight.publication,
        }));
        assert!(
            apply(
                &mut owner,
                Input::StoreResult {
                    effect: resolved,
                    operation,
                    reply: done
                }
            )
            .is_empty()
        );
        assert!(owner.pubsub.retained.is_none());
        assert_eq!(owner.pubsub.retired_flights(), 0);
        assert!(owner.slots.is_empty());
        assert_eq!(owner.leases.held(), 0);
        apply(
            &mut owner,
            Input::Packet {
                connection: ConnectionGeneration::new(11),
                ingress_order: 0,
                packet: InboundPacket::ConnectResume {
                    client_id: bytes(b"other"),
                },
            },
        );
        apply(
            &mut owner,
            Input::ConnectionSettings {
                connection: ConnectionGeneration::new(11),
                receive_maximum: 8,
                maximum_packet_size: 65536,
            },
        );
        let mut next = retained_zero(1);
        if let Input::Packet { connection, .. } = &mut next {
            *connection = ConnectionGeneration::new(11);
        }
        apply(&mut owner, next);
        let current = owner.pubsub.retained.unwrap();
        assert_ne!(current.in_flight.operation, operation);
        assert!(
            apply(
                &mut owner,
                Input::StoreResult {
                    effect,
                    operation,
                    reply: done
                }
            )
            .is_empty()
        );
        assert_eq!(
            owner.pubsub.retained,
            Some(current),
            "old_retained_qos0_result_rebound"
        );
        apply(
            &mut owner,
            Input::StoreResult {
                effect: current.in_flight.answers.persist,
                operation: current.in_flight.operation,
                reply: StoreReply::Outcome(StoreOutcome::Committed(
                    CommitFacts::RetainedQos0Stored {
                        publication: current.publication,
                    },
                )),
            },
        );
        assert!(owner.pubsub.retained.is_none());
    }
    #[test]
    fn retained_qos0_certain_capacity_refusal_is_an_explicit_drop_without_mqtt_credit() {
        let mut owner = world();
        let (effect, command, _) = pending_retained(&mut owner);
        let effects = apply(
            &mut owner,
            Input::StoreResult {
                effect,
                operation: command.recipe.operation(),
                reply: StoreReply::Outcome(StoreOutcome::RejectedCertain(
                    RejectReason::CapacityExceeded(Resource::References),
                )),
            },
        );
        assert!(
            matches!(
                effects.as_slice(),
                [(
                    _,
                    Effect::Qos0Dropped {
                        reason: Qos0DropReason::CapacityExceeded,
                        ..
                    }
                )]
            ),
            "retained_qos0_refusal_became_success_or_silent_loss"
        );
        assert!(owner.pubsub.retained.is_none());
        assert!(owner.slots.is_empty());
        assert_eq!(owner.leases.held(), 0);
        apply(&mut owner, retained_zero(2));
        assert!(owner.pubsub.retained.is_some());
    }
    fn product_subscribe() -> Input {
        Input::Packet {
            connection: CONNECTION,
            ingress_order: 1,
            packet: InboundPacket::Subscribe {
                packet_id: InboundPacketId::new(3).unwrap(),
                filters: crate::contract::pubsub::FilterList::copy_from(&[
                    crate::contract::pubsub::SubscriptionFilter {
                        filter: bytes(b"retained/#"),
                        qos: Qos::AtLeastOnce,
                        options: crate::contract::pubsub::SubscriptionOptions::DEFAULT,
                    },
                ])
                .unwrap(),
            },
        }
    }
    #[test]
    fn subscription_replay_reserves_the_whole_range_before_submission_and_confirmation() {
        use crate::contract::receipts::ReceiptCollectionReply;
        let mut owner = world();
        owner.lifecycle_enabled = true;
        let collection = apply(&mut owner, Input::MaintainReceipts);
        let (effect, request) = collection
            .iter()
            .find_map(|(effect, item)| match item {
                Effect::CollectReceipts { collection } => Some((*effect, *collection)),
                _ => None,
            })
            .unwrap();
        apply(
            &mut owner,
            Input::ReceiptsCollected {
                effect,
                collection: request,
                reply: ReceiptCollectionReply::Applied {
                    closure: request.closure,
                    removed: 0,
                    retained: 0,
                    more: false,
                },
            },
        );
        let effects = apply(&mut owner, product_subscribe());
        let (effect, command) = effects
            .iter()
            .find_map(|(effect, item)| match item {
                Effect::Persist { command } => Some((*effect, command)),
                _ => None,
            })
            .unwrap();
        let Recipe::ReplaySubscriptions(value) = command.recipe.get() else {
            panic!("product_subscription_did_not_reserve_replay");
        };
        let first = value.first_publication.sequence().get();
        assert_eq!(
            owner.counters.publication.unwrap().get(),
            first + crate::contract::retained::MAX_REPLAYS as u64,
            "replay_range_not_burned_before_submission"
        );
        let operation = command.recipe.operation();
        apply(
            &mut owner,
            Input::StoreResult {
                effect,
                operation,
                reply: StoreReply::Outcome(StoreOutcome::PendingOrUnknown),
            },
        );
        assert!(owner.routes.is_empty());
        apply(
            &mut owner,
            Input::StoreResult {
                effect,
                operation,
                reply: StoreReply::Outcome(StoreOutcome::Committed(
                    CommitFacts::SubscriptionsChanged {
                        session: TARGET,
                        route_revision: Revision::new(1),
                    },
                )),
            },
        );
        assert_eq!(owner.routes.len(), 1);
        // Result consumption is separate from the replay obligations' lifetime.
        let status = owner.receipt_status().unwrap();
        assert_eq!((status.1, status.2), (1, 0));
        for revision in 2..=64 {
            let effects = apply(&mut owner, product_subscribe());
            let (effect, operation) = effects
                .iter()
                .find_map(|(effect, item)| match item {
                    Effect::Persist { command } => Some((*effect, command.recipe.operation())),
                    _ => None,
                })
                .unwrap();
            apply(
                &mut owner,
                Input::StoreResult {
                    effect,
                    operation,
                    reply: StoreReply::Outcome(StoreOutcome::Committed(
                        CommitFacts::SubscriptionsChanged {
                            session: TARGET,
                            route_revision: Revision::new(revision),
                        },
                    )),
                },
            );
        }
        let maintenance = apply(&mut owner, Input::MaintainReceipts);
        let closure = maintenance
            .iter()
            .find_map(|(_, item)| match item {
                Effect::CollectReceipts { collection } => Some(collection.closure),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            closure.publication_through.get(),
            first + 64 * crate::contract::retained::MAX_REPLAYS as u64 - 1,
            "receipt_closure_lost_burned_replay_suffix"
        );
        // The issue record itself proves the entire range belongs to this op;
        // a later mutation starts after it, never in an unused replay suffix.
        let later = apply(&mut owner, retained_zero(2));
        let Recipe::RetainedQos0(next) = later
            .iter()
            .find_map(|(_, item)| match item {
                Effect::Persist { command } => Some(command.recipe.get()),
                _ => None,
            })
            .unwrap()
        else {
            panic!("retained QoS0")
        };
        assert_eq!(
            next.publication.sequence().get(),
            first + 64 * crate::contract::retained::MAX_REPLAYS as u64
        );
    }
    #[test]
    fn replay_range_exhaustion_refuses_before_any_counter_projection_or_effect_mutation() {
        let mut owner = world();
        owner.lifecycle_enabled = true;
        owner.counters.publication = Some(PublicationSeq::new(u64::MAX - 254));
        let before = owner.counters;
        let result = owner.step(product_subscribe(), TURN);
        assert!(
            matches!(result, StepOutcome::Exhausted { .. }),
            "replay_reservation_wrapped_or_submitted_a_partial_range"
        );
        assert_eq!(owner.counters, before);
        assert!(owner.pubsub.control.is_none() && owner.routes.is_empty());
        assert!(drain(&mut owner).is_empty());
    }
    #[test]
    fn loaded_retained_delivery_flags_survive_claim_and_consume_knowledge() {
        for qos in [Qos::AtMostOnce, Qos::AtLeastOnce] {
            for recovered_metadata in [false, true] {
                let mut owner = world();
                if recovered_metadata {
                    owner
                        .pubsub
                        .deliveries
                        .try_push(Delivery::metadata(
                            delivery(0),
                            TARGET,
                            Revision::new(1),
                            qos,
                            None,
                        ))
                        .unwrap();
                }
                let effects = apply(&mut owner, Input::WorkAvailable { session: TARGET });
                let effect = effects
                    .iter()
                    .find_map(|(effect, item)| {
                        matches!(item, Effect::LoadWork { .. }).then_some(*effect)
                    })
                    .unwrap();
                let item = WorkItem {
                    expiry: None,
                    delivery: delivery(0),
                    target: TARGET,
                    target_revision: Revision::new(1),
                    qos,
                    retain: true,
                    topic: bytes(b"retained/replay"),
                    payload: bytes(b"frozen-version"),
                    claim: None,
                };
                apply(
                    &mut owner,
                    Input::WorkPage {
                        effect,
                        session: TARGET,
                        items: BoundedVec::from_slice(core::slice::from_ref(&item), 1).unwrap(),
                        next: None,
                    },
                );
                assert!(owner.pubsub.deliveries.as_slice()[0].retain);
                let effects = apply(&mut owner, Input::WorkAvailable { session: TARGET });
                let (effect, command) = effects
                    .iter()
                    .find_map(|(effect, item)| match item {
                        Effect::Persist { command } => Some((*effect, command)),
                        _ => None,
                    })
                    .unwrap();
                let emitted = known(&mut owner, effect, command);
                assert!(
                    emitted.iter().any(|(_, item)| matches!(
                        item,
                        Effect::EmitPublish { retain: true, .. }
                            | Effect::EmitQos0 { retain: true, .. }
                    )),
                    "loaded_retain_flag_was_not_frozen_for_outgoing_delivery"
                );
            }
        }
    }
    #[test]
    fn a_later_work_page_cannot_rewrite_an_already_loaded_retain_flag() {
        let mut owner = world();
        queued(&mut owner, 0);
        owner.pubsub.deliveries.as_mut_slice()[0].retain = true;
        let claim = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        let Effect::Persist { command } = &claim[0].1 else {
            panic!("claim")
        };
        known(&mut owner, claim[0].0, command);
        let read = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        let effect = read
            .iter()
            .find_map(|(effect, item)| matches!(item, Effect::LoadWork { .. }).then_some(*effect))
            .unwrap();
        let held = &owner.pubsub.deliveries.as_slice()[0];
        let changed = WorkItem {
            expiry: None,
            delivery: held.id,
            target: held.target,
            target_revision: held.revision,
            qos: held.qos,
            retain: false,
            topic: held.topic.clone().unwrap(),
            payload: held.payload.clone().unwrap(),
            claim: held.claim,
        };
        let result = owner.step(
            Input::WorkPage {
                effect,
                session: TARGET,
                items: BoundedVec::from_slice(&[changed], 1).unwrap(),
                next: None,
            },
            TURN,
        );
        assert!(
            matches!(result, StepOutcome::Fatal(CoreFault::ImpossibleInput)),
            "work_page_rewrote_frozen_retain_flag"
        );
        assert!(owner.pubsub.deliveries.as_slice()[0].retain);
    }

    fn message_time(seconds: u64) -> MessageTime {
        MessageTime {
            domain: [9; 32],
            tick: seconds * crate::contract::message_expiry::NANOS_PER_SECOND,
        }
    }
    fn message_expiry() -> MessageExpiry {
        MessageExpiry {
            observed: message_time(10),
            interval: 2,
        }
    }
    fn message_clock(owner: &mut Owner, seconds: u64) {
        assert!(
            apply(
                owner,
                Input::MessageClock {
                    now: message_time(seconds)
                }
            )
            .is_empty()
        );
    }
    fn message_command(effects: Vec<(EffectId, Effect)>) -> (EffectId, Command) {
        effects
            .into_iter()
            .find_map(|(id, effect)| match effect {
                Effect::Persist { command } => Some((id, command)),
                _ => None,
            })
            .expect("durable command")
    }
    fn message_frame(owner: &mut Owner, index: u32) -> (EffectId, ConnectionKey, OutboundPacketId) {
        queued(owner, index);
        owner
            .pubsub
            .deliveries
            .as_mut_slice()
            .last_mut()
            .unwrap()
            .expiry = Some(message_expiry());
        let (effect, command) =
            message_command(apply(owner, Input::WorkAvailable { session: TARGET }));
        let emitted = known(owner, effect, &command);
        let (effect, connection, packet_id) = emitted
            .into_iter()
            .find_map(|(id, effect)| match effect {
                Effect::EmitPublish {
                    connection,
                    packet_id,
                    expiry,
                    started,
                    ..
                } => {
                    assert_eq!(expiry, Some(message_expiry()));
                    assert!(!started);
                    Some((id, connection, packet_id))
                }
                _ => None,
            })
            .expect("fresh emission");
        (effect, connection, packet_id)
    }
    fn message_progress(
        owner: &mut Owner,
        effect: EffectId,
        connection: ConnectionKey,
        progress: FrameProgress,
    ) -> Vec<(EffectId, Effect)> {
        apply(
            owner,
            Input::FrameResult {
                effect,
                connection,
                progress,
            },
        )
    }
    fn message_puback(
        owner: &mut Owner,
        connection: ConnectionKey,
        packet_id: OutboundPacketId,
    ) -> Vec<(EffectId, Effect)> {
        let target = owner.puback_target(connection, packet_id);
        apply(
            owner,
            Input::ObservedPuback {
                connection,
                packet_id,
                reason: 0,
                target,
            },
        )
    }

    #[test]
    fn message_expiry_queued_and_unknown_domain_work_never_claims_consumes_or_emits() {
        for qos in [Qos::AtMostOnce, Qos::AtLeastOnce] {
            for unknown in [false, true] {
                let mut owner = world();
                queued(&mut owner, 0);
                let d = &mut owner.pubsub.deliveries.as_mut_slice()[0];
                d.expiry = Some(message_expiry());
                d.qos = qos;
                let mut now = message_time(12);
                if unknown {
                    now.domain = [7; 32];
                }
                apply(&mut owner, Input::MessageClock { now });
                assert!(!owner.has_ready_work(TARGET), "quarantined_work_busy_loop");
                let effects = apply(&mut owner, Input::WorkAvailable { session: TARGET });
                assert!(
                    !effects.iter().any(|(_, e)| matches!(
                        e,
                        Effect::Persist { .. }
                            | Effect::EmitPublish { .. }
                            | Effect::EmitQos0 { .. }
                    )),
                    "expired_queued_emitted"
                );
                assert_eq!(
                    owner.pubsub.deliveries.len(),
                    usize::from(unknown),
                    "uncertain_cache_was_freed_or_expired_cache_pinned"
                );
                assert!(
                    owner
                        .pubsub
                        .deliveries
                        .as_slice()
                        .iter()
                        .all(|d| d.claim.is_none())
                );
            }
        }
    }

    #[test]
    fn message_expiry_every_clocked_product_subscription_freezes_the_observation_and_range() {
        let mut owner = world();
        // The product clock, rather than a particular session entry point,
        // selects the time-aware recipe for every product SUBSCRIBE.
        message_clock(&mut owner, 10);
        let first = owner.counters.publication.unwrap();
        let (effect, command) = message_command(apply(&mut owner, product_subscribe()));
        let Recipe::TimedReplaySubscriptions(timed) = command.recipe.get() else {
            panic!("product_subscribe_omitted_message_time");
        };
        assert_eq!(timed.observed, message_time(10));
        assert_eq!(timed.replay.first_publication.sequence(), first);
        assert_eq!(
            owner.counters.publication.unwrap().get(),
            first.get() + crate::contract::retained::MAX_REPLAYS as u64
        );
        let operation = command.recipe.operation();
        message_clock(&mut owner, 11);
        apply(
            &mut owner,
            Input::StoreResult {
                effect,
                operation,
                reply: StoreReply::Outcome(StoreOutcome::PendingOrUnknown),
            },
        );
        let deadline = owner.pubsub.control.as_ref().unwrap().in_flight.deadline;
        let resolutions = apply(
            &mut owner,
            Input::Deadline {
                token: deadline,
                now: crate::contract::ids::LogicalTime::new(50),
            },
        );
        assert!(resolutions.iter().any(
            |(_, e)| matches!(e, Effect::ResolveAndFence { operation: op } if *op == operation)
        ));
        assert_eq!(
            timed.observed,
            message_time(10),
            "replay_refreshed_expiry_origin"
        );
    }

    #[test]
    fn message_expiry_puback_before_started_is_held_through_unknown_marker_and_expiration() {
        let mut owner = world();
        message_clock(&mut owner, 10);
        let (frame, key, pi) = message_frame(&mut owner, 0);
        let target = owner.puback_target(key, pi).unwrap();
        assert!(message_puback(&mut owner, key, pi).is_empty());
        assert!(owner.pubsub.deliveries.as_slice()[0].early_puback);
        assert!(
            message_progress(
                &mut owner,
                frame,
                key,
                FrameProgress::Written {
                    confirmed: 18,
                    total: 18
                }
            )
            .is_empty()
        );
        assert!(
            !owner.pubsub.deliveries.as_slice()[0].claim.unwrap().started,
            "complete_callback_invented_durable_started"
        );
        message_clock(&mut owner, 12);
        let (started_effect, started) = message_command(message_progress(
            &mut owner,
            frame,
            key,
            FrameProgress::Started,
        ));
        assert!(
            matches!(started.recipe.get(), Recipe::MarkDeliveryStarted(mark) if mark.claim == target.claim && mark.delivery == target.delivery)
        );
        let operation = started.recipe.operation();
        assert!(
            apply(
                &mut owner,
                Input::StoreResult {
                    effect: started_effect,
                    operation,
                    reply: StoreReply::Outcome(StoreOutcome::PendingOrUnknown)
                }
            )
            .is_empty()
        );
        assert!(message_puback(&mut owner, key, pi).is_empty());
        assert!(message_progress(&mut owner, frame, key, FrameProgress::Started).is_empty());
        let deadline = owner.pubsub.deliveries.as_slice()[0]
            .in_flight()
            .unwrap()
            .deadline;
        let effects = apply(
            &mut owner,
            Input::Deadline {
                token: deadline,
                now: crate::contract::ids::LogicalTime::new(20),
            },
        );
        let resolve = effects
            .iter()
            .find_map(|(id, e)| {
                matches!(e, Effect::ResolveAndFence { operation: op } if *op == operation)
                    .then_some(*id)
            })
            .unwrap();
        let finish = known(&mut owner, resolve, &started);
        let (finish_effect, finish) = message_command(finish);
        assert!(
            matches!(finish.recipe.get(), Recipe::FinishDelivery(end) if end.claim == target.claim)
        );
        assert!(owner.pubsub.deliveries.as_slice()[0].claim.unwrap().started);
        assert_eq!(
            owner.pubsub.free_packet_id(TARGET, 1),
            None,
            "marker_result_freed_pi"
        );
        assert!(known(&mut owner, finish_effect, &finish).is_empty());
        assert!(owner.pubsub.deliveries.is_empty());
        assert!(message_progress(&mut owner, frame, key, FrameProgress::Started).is_empty());
        assert!(known(&mut owner, started_effect, &started).is_empty());
    }

    #[test]
    fn message_expiry_started_claim_replays_past_ttl_and_old_facts_cannot_hit_reused_pi() {
        let mut owner = world();
        message_clock(&mut owner, 10);
        let (frame, key, pi) = message_frame(&mut owner, 0);
        let original = owner.puback_target(key, pi).unwrap();
        let (start_effect, start) = message_command(message_progress(
            &mut owner,
            frame,
            key,
            FrameProgress::Started,
        ));
        assert!(known(&mut owner, start_effect, &start).is_empty());
        message_clock(&mut owner, 13);
        apply(
            &mut owner,
            Input::ConnectionClosed {
                connection: CONNECTION,
            },
        );
        apply(
            &mut owner,
            Input::Packet {
                connection: ConnectionGeneration::new(11),
                ingress_order: 0,
                packet: InboundPacket::ConnectResume {
                    client_id: bytes(b"subscriber"),
                },
            },
        );
        apply(
            &mut owner,
            Input::ConnectionSettings {
                connection: ConnectionGeneration::new(11),
                receive_maximum: 8,
                maximum_packet_size: 65536,
            },
        );
        let replay = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        assert!(replay.iter().any(|(_, e)| matches!(e, Effect::EmitPublish { started: true, expiry: Some(expiry), packet_id, claim, dup: true, .. } if *expiry == message_expiry() && *packet_id == pi && *claim == original.claim)), "known_started_not_replayed_past_ttl");
        let current = ConnectionKey {
            session: TARGET,
            generation: ConnectionGeneration::new(11),
        };
        let (end_effect, end) = message_command(message_puback(&mut owner, current, pi));
        known(&mut owner, end_effect, &end);
        // Reuse the same small-domain PI for another occurrence.
        queued(&mut owner, 1);
        let (claim_effect, claim) =
            message_command(apply(&mut owner, Input::WorkAvailable { session: TARGET }));
        known(&mut owner, claim_effect, &claim);
        let newer = owner.puback_target(current, pi).unwrap();
        let held = owner.pubsub.deliveries.as_slice()[0].clone();
        for progress in [
            FrameProgress::Started,
            FrameProgress::NotStarted {
                reason: MessageExpiryBlock::Expired,
            },
            FrameProgress::Written {
                confirmed: 18,
                total: 18,
            },
        ] {
            assert!(message_progress(&mut owner, frame, key, progress).is_empty());
        }
        assert!(known(&mut owner, start_effect, &start).is_empty());
        assert!(known(&mut owner, end_effect, &end).is_empty());
        assert_eq!(owner.puback_target(current, pi), Some(newer));
        assert_eq!(
            owner.pubsub.deliveries.as_slice()[0],
            held,
            "old_fact_changed_reused_pi"
        );
    }

    #[test]
    fn message_expiry_certain_first_not_started_uses_correlated_expiry_and_unknown_resolution() {
        let mut owner = world();
        message_clock(&mut owner, 10);
        let (frame, key, _) = message_frame(&mut owner, 0);
        message_clock(&mut owner, 12);
        let (expire_effect, expire) = message_command(message_progress(
            &mut owner,
            frame,
            key,
            FrameProgress::NotStarted {
                reason: MessageExpiryBlock::Expired,
            },
        ));
        let Recipe::ExpireDelivery(expiry) = expire.recipe.get() else {
            panic!("certain_unsent_not_expired");
        };
        assert_eq!(expiry.expiry, message_expiry());
        assert_eq!(expiry.observed, message_time(12));
        let operation = expire.recipe.operation();
        apply(
            &mut owner,
            Input::StoreResult {
                effect: expire_effect,
                operation,
                reply: StoreReply::Outcome(StoreOutcome::PendingOrUnknown),
            },
        );
        assert_eq!(owner.pubsub.free_packet_id(TARGET, 1), None);
        assert!(message_progress(&mut owner, frame, key, FrameProgress::Started).is_empty());
        let deadline = owner.pubsub.deliveries.as_slice()[0]
            .in_flight()
            .unwrap()
            .deadline;
        let effects = apply(
            &mut owner,
            Input::Deadline {
                token: deadline,
                now: crate::contract::ids::LogicalTime::new(20),
            },
        );
        let resolve = effects
            .iter()
            .find_map(|(id, e)| {
                matches!(e, Effect::ResolveAndFence { operation: op } if *op == operation)
                    .then_some(*id)
            })
            .unwrap();
        known(&mut owner, resolve, &expire);
        assert!(owner.pubsub.deliveries.is_empty());
        assert!(known(&mut owner, expire_effect, &expire).is_empty());
        assert_eq!(owner.pubsub.free_packet_id(TARGET, 1).unwrap().get(), 1);
    }

    #[test]
    fn message_expiry_complete_write_without_started_callback_is_not_certainly_unsent() {
        let mut owner = world();
        message_clock(&mut owner, 10);
        let (frame, key, _) = message_frame(&mut owner, 0);
        message_progress(
            &mut owner,
            frame,
            key,
            FrameProgress::Written {
                confirmed: 18,
                total: 18,
            },
        );
        message_clock(&mut owner, 12);
        let effects = message_progress(
            &mut owner,
            frame,
            key,
            FrameProgress::NotStarted {
                reason: MessageExpiryBlock::Expired,
            },
        );
        assert!(
            effects.is_empty(),
            "written_frame_treated_as_certainly_unsent"
        );
        assert_eq!(owner.pubsub.deliveries.len(), 1);
    }

    #[test]
    fn message_expiry_recovered_or_previously_uncertain_claim_never_becomes_certainly_unsent() {
        for unknown in [false, true] {
            let mut owner = world();
            message_clock(&mut owner, 10);
            queued(&mut owner, 0);
            let d = &mut owner.pubsub.deliveries.as_mut_slice()[0];
            d.expiry = Some(message_expiry());
            d.claim = Some(DeliveryClaim {
                operation: operation(100),
                packet_id: OutboundPacketId::new(1).unwrap(),
                started: false,
            });
            d.phase = Phase::AwaitPuback;
            d.certain_never_started = false;
            d.may_have_started = true;
            let emitted = apply(&mut owner, Input::WorkAvailable { session: TARGET });
            let (frame, key) = emitted
                .into_iter()
                .find_map(|(id, e)| match e {
                    Effect::EmitPublish { connection, .. } => Some((id, connection)),
                    _ => None,
                })
                .unwrap();
            let mut now = message_time(12);
            if unknown {
                now.domain = [8; 32];
            }
            apply(&mut owner, Input::MessageClock { now });
            let reason = if unknown {
                MessageExpiryBlock::TemporalUncertain
            } else {
                MessageExpiryBlock::Expired
            };
            assert!(
                message_progress(&mut owner, frame, key, FrameProgress::NotStarted { reason })
                    .is_empty()
            );
            let effects = apply(&mut owner, Input::WorkAvailable { session: TARGET });
            assert!(
                !effects
                    .iter()
                    .any(|(_, e)| matches!(e, Effect::Persist { .. } | Effect::EmitPublish { .. }))
            );
            assert_eq!(owner.pubsub.free_packet_id(TARGET, 1), None);
            assert_eq!(owner.pubsub.deliveries.len(), 1);
        }
    }

    #[test]
    fn message_expiry_property_and_remaining_length_are_counted_at_both_qos_levels() {
        let mut owner = world();
        message_clock(&mut owner, 10);
        let settings = &mut owner.pubsub.connections.as_mut_slice()[0];
        settings.maximum_packet_size = 128;
        let key = ConnectionKey {
            session: TARGET,
            generation: CONNECTION,
        };
        let topic = bytes(b"topic");
        let payload = SharedBytes::copy_from(&[1; 115], 128).unwrap();
        assert!(owner.qos0_fits(key, &topic, &payload, None));
        assert!(
            !owner.qos0_fits(key, &topic, &payload, Some(message_expiry())),
            "qos0_expiry_property_not_charged"
        );
        queued(&mut owner, 0);
        let d = &mut owner.pubsub.deliveries.as_mut_slice()[0];
        d.expiry = Some(message_expiry());
        d.payload = Some(payload);
        let (effect, command) =
            message_command(apply(&mut owner, Input::WorkAvailable { session: TARGET }));
        let effects = known(&mut owner, effect, &command);
        assert!(
            effects.iter().any(|(_, e)| matches!(
                e,
                Effect::CloseConnection {
                    reason: CloseReason::CapacityExceeded,
                    ..
                }
            )),
            "qos1_expiry_property_not_charged"
        );
        assert!(
            !effects
                .iter()
                .any(|(_, e)| matches!(e, Effect::EmitPublish { .. }))
        );
        assert_eq!(owner.pubsub.free_packet_id(TARGET, 1), None);
    }

    #[test]
    fn message_expiry_sweep_retires_exact_unclaimed_metadata_and_unknown_claim_receipt_once() {
        let mut owner = world();
        message_clock(&mut owner, 10);
        queued(&mut owner, 0);
        let (claim_effect, claim) =
            message_command(apply(&mut owner, Input::WorkAvailable { session: TARGET }));
        let before = owner.leases.account().used();
        let expired = crate::contract::message_expiry::ExpiredDelivery {
            delivery: delivery(0),
            target: TARGET,
        };
        apply(
            &mut owner,
            Input::MessagesExpired {
                deliveries: BoundedVec::from_slice(&[expired], 1).unwrap(),
            },
        );
        assert!(owner.pubsub.deliveries.as_slice()[0].retired);
        let operation = claim.recipe.operation();
        assert!(
            apply(
                &mut owner,
                Input::StoreResult {
                    effect: claim_effect,
                    operation,
                    reply: StoreReply::Outcome(StoreOutcome::RejectedCertain(
                        RejectReason::PreconditionFailed
                    ))
                }
            )
            .is_empty()
        );
        assert!(owner.pubsub.deliveries.is_empty());
        for _ in 0..2 {
            apply(
                &mut owner,
                Input::MessagesExpired {
                    deliveries: BoundedVec::from_slice(&[expired], 1).unwrap(),
                },
            );
        }
        assert_eq!(
            owner.leases.account().used(),
            before,
            "sweep_fabricated_credit_release"
        );
        queued(&mut owner, 1);
        let (effect, command) =
            message_command(apply(&mut owner, Input::WorkAvailable { session: TARGET }));
        known(&mut owner, effect, &command);
        let newer = owner.pubsub.deliveries.as_slice()[0].clone();
        apply(
            &mut owner,
            Input::MessagesExpired {
                deliveries: BoundedVec::from_slice(&[expired], 1).unwrap(),
            },
        );
        assert_eq!(owner.pubsub.deliveries.as_slice()[0], newer);
    }

    #[test]
    fn message_expiry_invalid_volatile_deadline_is_refused_before_admission() {
        let mut owner = world();
        message_clock(&mut owner, 10);
        owner
            .routes
            .try_push(Route {
                topic: bytes(b"topic"),
                target: TARGET,
                target_revision: Revision::new(1),
                max_qos: Qos::AtMostOnce,
                options: crate::contract::pubsub::SubscriptionOptions::DEFAULT,
            })
            .unwrap();
        let input = Input::Packet {
            connection: CONNECTION,
            ingress_order: 1,
            packet: InboundPacket::Publish {
                topic: bytes(b"topic"),
                qos: Qos::AtMostOnce,
                retain: false,
                dup: false,
                packet_id: None,
                payload: bytes(b"payload"),
                expiry: Some(MessageExpiry {
                    observed: MessageTime {
                        domain: [9; 32],
                        tick: u64::MAX,
                    },
                    interval: 1,
                }),
            },
        };
        assert!(
            matches!(
                owner.step(input, TURN),
                StepOutcome::Fatal(CoreFault::ImpossibleInput)
            ),
            "invalid_qos0_expiry_was_admitted"
        );
        assert!(owner.pubsub.volatile.is_empty());
    }

    fn message_register_receipts(owner: &mut Owner) {
        let effects = apply(owner, Input::MaintainReceipts);
        let (effect, collection) = effects
            .iter()
            .find_map(|(id, e)| match e {
                Effect::CollectReceipts { collection } => Some((*id, *collection)),
                _ => None,
            })
            .unwrap();
        apply(
            owner,
            Input::ReceiptsCollected {
                effect,
                collection,
                reply: crate::contract::receipts::ReceiptCollectionReply::Applied {
                    closure: collection.closure,
                    removed: 0,
                    retained: 0,
                    more: false,
                },
            },
        );
    }

    #[test]
    fn message_expiry_full_receipt_window_consumes_started_before_preparing_early_ack_finish() {
        let mut owner = world();
        message_register_receipts(&mut owner);
        message_clock(&mut owner, 10);
        let (frame, key, pi) = message_frame(&mut owner, 0);
        message_puback(&mut owner, key, pi);
        let (start_effect, start) = message_command(message_progress(
            &mut owner,
            frame,
            key,
            FrameProgress::Started,
        ));
        owner.receipts.test_fill_consumed_window();
        owner.counters.operation = Some(OperationSeq::new(
            crate::contract::receipts::RECEIPT_WINDOW as u64 + 1,
        ));
        let effects = known(&mut owner, start_effect, &start);
        assert!(
            effects.is_empty(),
            "full_window_submitted_unreserved_finish"
        );
        assert_eq!(
            owner.receipt_status().unwrap().2,
            0,
            "full_window_did_not_consume_started"
        );
        let effects = apply(&mut owner, Input::MaintainReceipts);
        let (effect, collection) = effects
            .iter()
            .find_map(|(id, e)| match e {
                Effect::CollectReceipts { collection } => Some((*id, *collection)),
                _ => None,
            })
            .expect("collection makes control room");
        apply(
            &mut owner,
            Input::ReceiptsCollected {
                effect,
                collection,
                reply: crate::contract::receipts::ReceiptCollectionReply::Applied {
                    closure: collection.closure,
                    removed: collection.limit,
                    retained: 0,
                    more: false,
                },
            },
        );
        let effects = apply(&mut owner, Input::MaintainReceipts);
        let continuation = effects
            .iter()
            .find_map(|(_, e)| match e {
                Effect::Continue { continuation } => Some(*continuation),
                _ => None,
            })
            .expect("one bounded finish wake");
        let (finish_effect, finish) = message_command(apply(
            &mut owner,
            Input::Continue {
                continuation,
                budget: WorkUnits::new(100_000),
            },
        ));
        assert!(matches!(finish.recipe.get(), Recipe::FinishDelivery(_)));
        known(&mut owner, finish_effect, &finish);
        assert!(owner.pubsub.deliveries.is_empty());
    }

    #[test]
    fn message_expiry_lost_sweep_reply_cannot_pin_cached_expired_unclaimed_work_or_reload_it() {
        let mut owner = world();
        message_clock(&mut owner, 10);
        queued(&mut owner, 0);
        owner.pubsub.deliveries.as_mut_slice()[0].expiry = Some(message_expiry());
        let before = owner.leases.account().used();
        // The store's sweep may already have committed, with its removed-key
        // notification lost. Evict only the obsolete cache, not a claim.
        message_clock(&mut owner, 12);
        assert!(
            owner.pubsub.deliveries.is_empty(),
            "lost_sweep_reply_pinned_cached_payload"
        );
        let effects = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        let read = effects
            .iter()
            .find_map(|(id, e)| matches!(e, Effect::LoadWork { .. }).then_some(*id))
            .unwrap();
        // A frozen page produced before the sweep also carries its original
        // expiry. It cannot reacquire cache/payload capacity after the TTL.
        let stale = WorkItem {
            delivery: delivery(0),
            target: TARGET,
            target_revision: Revision::new(1),
            qos: Qos::AtLeastOnce,
            retain: false,
            expiry: Some(message_expiry()),
            topic: bytes(b"topic"),
            payload: bytes(b"payload"),
            claim: None,
        };
        apply(
            &mut owner,
            Input::WorkPage {
                effect: read,
                session: TARGET,
                items: BoundedVec::from_slice(&[stale], 1).unwrap(),
                next: None,
            },
        );
        assert!(
            owner.pubsub.deliveries.is_empty(),
            "stale_page_reloaded_expired_cache"
        );
        assert_eq!(owner.leases.account().used(), before);
    }

    #[test]
    fn message_expiry_lost_sweep_race_certain_claim_refusal_retires_cache_without_closing_connection()
     {
        for qos in [Qos::AtMostOnce, Qos::AtLeastOnce] {
            let mut owner = world();
            message_clock(&mut owner, 10);
            queued(&mut owner, 0);
            let d = &mut owner.pubsub.deliveries.as_mut_slice()[0];
            d.expiry = Some(message_expiry());
            d.qos = qos;
            let (effect, command) =
                message_command(apply(&mut owner, Input::WorkAvailable { session: TARGET }));
            message_clock(&mut owner, 12);
            assert_eq!(
                owner.pubsub.deliveries.len(),
                1,
                "clock_discarded_unknown_command"
            );
            let operation = command.recipe.operation();
            let effects = apply(
                &mut owner,
                Input::StoreResult {
                    effect,
                    operation,
                    reply: StoreReply::Outcome(StoreOutcome::RejectedCertain(
                        RejectReason::PreconditionFailed,
                    )),
                },
            );
            assert!(effects.is_empty(), "ordinary_expiry_race_closed_connection");
            assert!(
                owner.pubsub.deliveries.is_empty(),
                "rejected_expired_claim_pinned_cache"
            );
            assert_eq!(owner.connection_of(TARGET), Some(CONNECTION));
            assert!(
                apply(
                    &mut owner,
                    Input::StoreResult {
                        effect,
                        operation,
                        reply: StoreReply::Outcome(StoreOutcome::RejectedCertain(
                            RejectReason::PreconditionFailed
                        ))
                    }
                )
                .is_empty()
            );
        }
    }

    #[test]
    fn message_expiry_clock_authority_is_zero_effect_during_recovery_and_regression_is_explicit() {
        let mut owner = world();
        owner.recovery = Some(RecoveryOrder::new());
        let result = owner.step(
            Input::MessageClock {
                now: message_time(10),
            },
            TurnGrant {
                work_limit: WorkUnits::ZERO,
            },
        );
        assert_eq!(
            result,
            StepOutcome::Applied {
                work_used: WorkUnits::ZERO,
                progress: Progress::Complete
            }
        );
        assert!(owner.is_recovering());
        assert!(drain(&mut owner).is_empty());
        let previous = owner.message_now;
        assert_eq!(
            owner.step(
                Input::MessageClock {
                    now: message_time(9)
                },
                TurnGrant {
                    work_limit: WorkUnits::ZERO
                }
            ),
            StepOutcome::Fatal(CoreFault::ImpossibleInput)
        );
        assert_eq!(owner.message_now, previous);
    }
}
