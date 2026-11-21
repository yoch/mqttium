//! Hierarchical subscriptions and outgoing QoS 1/2 under the same owner authority.
use super::*;
use crate::contract::command::{
    ClaimDelivery, ConsumeDelivery, ExpireDelivery, FinishDelivery, MarkDeliveryStarted,
    UpdateSubscriptions,
};
use crate::contract::ids::{ContinuationId, OutboundPacketId};
use crate::contract::packet::Qos2Ack;
use crate::contract::pubsub::{
    DeliveryClaim, DeliveryDiscardReason, FilterResults, GrantedSubscriptionChange, MAX_FILTERS,
    PubackTarget, Qos0DropReason, Qos0Origin, VolatilePublicationId, WORK_PAGE_LIMIT, WorkItem,
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
    /// The guarded finish of a claimed delivery whose PUBLISH would exceed
    /// the Maximum Packet Size of its connection. Nothing was emitted.
    DiscardPending(InFlight),
    PubrelPending(InFlight),
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
    cause: DeliveryCause,
    retain: bool,
    expiry: Option<MessageExpiry>,
    claim: Option<DeliveryClaim>,
    topic: Option<SharedBytes>,
    payload: Option<SharedBytes>,
    phase: Phase,
    claim_result: Option<Finished>,
    /// Send quota reserved for a PUBLISH on this generation. Its PUBREL
    /// retains this reservation through PUBCOMP. A resumed PUBREL has none.
    window: Option<ConnectionGeneration>,
    frame: Option<(EffectId, ConnectionKey)>,
    local_write_completed: bool,
    may_have_started: bool,
    /// Only a fresh claim in this process, with no earlier uncertain attempt,
    /// can turn an exact first-frame NotStarted observation into deletion.
    certain_never_started: bool,
    early_puback: bool,
    early_pubrec: Option<u8>,
    /// The preparation of this delivery's PUBREL, submitted when its PUBREC
    /// was received: operations are numbered as they are submitted, so it
    /// orders the PUBRECs of a subscriber. `None` for a PUBREL recovered
    /// from an earlier run, which precedes those of this one.
    pubrec: Option<OperationId>,
    retired: bool,
}

impl Delivery {
    fn awaits_pubcomp(&self) -> bool {
        self.claim.is_some_and(|claim| claim.await_pubcomp)
    }

    fn waiting_to_send(&self, generation: ConnectionGeneration, now: Option<MessageTime>) -> bool {
        if self.retired || !self.expiry_allows_send(now) {
            return false;
        }
        match self.phase {
            Phase::FinishPending(_)
            | Phase::DiscardPending(_)
            | Phase::PubrelPending(_)
            | Phase::StartPending(_)
            | Phase::ExpirePending(_)
            | Phase::StartRetry
            | Phase::ExpireRetry
            | Phase::Qos0Writing(_) => false,
            Phase::AwaitPuback => self
                .frame
                .is_none_or(|(_, key)| key.generation != generation),
            Phase::Queued | Phase::ClaimPending(_) | Phase::ConsumePending(_, _) => true,
        }
    }

    fn ready_to_send(&self, generation: ConnectionGeneration, room: bool) -> bool {
        match self.phase {
            Phase::Queued => self.payload.is_some() && (room || self.qos == Qos::AtMostOnce),
            Phase::AwaitPuback => {
                self.awaits_pubcomp()
                    || (self.payload.is_some() && (room || self.window == Some(generation)))
            }
            _ => false,
        }
    }
    fn expiry_allows_send(&self, now: Option<MessageTime>) -> bool {
        self.claim
            .is_some_and(|claim| claim.started || claim.await_pubcomp)
            || self.expiry.is_none_or(|expiry| {
                now.is_some_and(|now| matches!(expiry.state(now), Ok(ExpiryState::Fresh { .. })))
            })
    }
    fn control_ready(&self, now: Option<MessageTime>) -> bool {
        !self.retired
            && match self.phase {
                Phase::StartRetry => true,
                Phase::AwaitPuback
                    if self.early_pubrec.is_some()
                        || (self.early_puback && self.claim.is_some_and(|claim| claim.started)) =>
                {
                    true
                }
                Phase::AwaitPuback | Phase::ExpireRetry => {
                    self.certain_never_started
                        && self.frame.is_none()
                        && !self.early_puback
                        && self.early_pubrec.is_none()
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
            | Phase::DiscardPending(f)
            | Phase::PubrelPending(f)
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
            cause: DeliveryCause::LiveExact,
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
            early_pubrec: None,
            pubrec: None,
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
    Discard {
        slot: usize,
        in_flight: InFlight,
    },
    PreparePubrel {
        slot: usize,
        in_flight: InFlight,
    },
    PubrelReady {
        slot: usize,
        finished: Finished,
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
        complete: bool,
    },
    PubackObserved {
        slot: usize,
    },
    PubrecObserved {
        slot: usize,
        reason: u8,
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
                | Input::ObservedQos2Ack { .. }
                | Input::ConnectionSettings { .. }
                | Input::WorkPage { .. }
                | Input::Packet {
                    packet: InboundPacket::Subscribe { .. }
                        | InboundPacket::Unsubscribe { .. }
                        | InboundPacket::Puback { .. }
                        | InboundPacket::Qos2Ack { .. },
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

    fn next_send(
        &self,
        session: SessionId,
        generation: ConnectionGeneration,
        now: Option<MessageTime>,
        room: bool,
    ) -> Option<(usize, &Delivery)> {
        let mut publish: Option<(usize, &Delivery)> = None;
        let mut pubrel: Option<(usize, &Delivery)> = None;
        // PUBREL leaves in the order of the PUBRECs (MQTT-4.6.0-4). The
        // earliest PUBREC of this session whose PUBREL is still being
        // prepared holds the PUBRELs of the later ones, whenever their own
        // preparations became known.
        let preparing = self
            .deliveries
            .as_slice()
            .iter()
            .filter(|delivery| delivery.target == session)
            .filter_map(|delivery| match delivery.phase {
                Phase::PubrelPending(in_flight) => Some(in_flight.operation.sequence()),
                _ => None,
            })
            .min();
        let received = |delivery: &Delivery| delivery.pubrec.map(OperationId::sequence);
        for (index, delivery) in self.deliveries.as_slice().iter().enumerate() {
            if delivery.target != session || !delivery.waiting_to_send(generation, now) {
                continue;
            }
            if delivery.awaits_pubcomp() {
                if received(delivery)
                    .is_some_and(|own| preparing.is_some_and(|earlier| earlier < own))
                {
                    continue;
                }
                if pubrel.is_none_or(|(_, previous)| {
                    (received(delivery), delivery.id) < (received(previous), previous.id)
                }) {
                    pubrel = Some((index, delivery));
                }
            } else if publish.is_none_or(|(_, previous)| delivery.id < previous.id) {
                publish = Some((index, delivery));
            }
        }
        // Keep PUBLISH order even when the first claim is unresolved. PUBREL
        // can pass that barrier, since it completes an earlier PUBLISH and
        // is exempt from the new connection's PUBLISH quota (MQTT 5 §4.9).
        match (publish, pubrel) {
            (Some((_, p)), Some((_, r)))
                if p.id < r.id
                    && (p.ready_to_send(generation, room)
                        || (room && p.phase == Phase::AwaitPuback)) =>
            {
                publish
            }
            (_, Some(_)) => pubrel,
            _ => publish,
        }
    }
    fn loaded_bytes(&self) -> Option<u64> {
        self.deliveries.as_slice().iter().try_fold(0u64, |sum, d| {
            sum.checked_add(d.payload.as_ref().map_or(0, |p| p.len() as u64))
        })
    }
    pub(super) fn accepts_message_time(&self, now: MessageTime) -> bool {
        self.deliveries
            .as_slice()
            .iter()
            .filter_map(|d| d.expiry)
            .chain(self.volatile.as_slice().iter().filter_map(|d| d.expiry))
            .all(|expiry| expiry.state(now).is_ok())
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

/// What the owner may do now with a claimed delivery.
enum Emission {
    /// The frame was published in the draft.
    Frame(EffectId, ConnectionKey),
    /// A gate holds the delivery; nothing was decided about it.
    Held,
    /// Its PUBLISH exceeds the Maximum Packet Size of its connection.
    TooLarge,
}

/// The size of the PUBLISH the profile sends for these bytes. There is one
/// encoding: fixed header, minimal Remaining Length, Topic Name, the Packet
/// Identifier above QoS 0, a one-byte Property Length and the Message
/// Expiry Interval when the message has one (MQTT-3.3.2-6), payload. No
/// property is optional, so nothing can be left out to make a packet fit.
/// `None` is an arithmetic overflow.
fn publish_packet_size(
    topic: usize,
    payload: usize,
    packet_identifier: bool,
    expiry: bool,
) -> Option<usize> {
    let body = topic
        .checked_add(payload)?
        .checked_add(3 + usize::from(packet_identifier) * 2 + usize::from(expiry) * 5)?;
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
        if (self.lifecycle_enabled || self.message_now.is_some())
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
        if let Some((_, delivery)) =
            self.pubsub
                .next_send(session, generation, self.message_now, room)
        {
            if delivery.ready_to_send(generation, room) {
                return true;
            }
            if matches!(
                delivery.phase,
                Phase::ClaimPending(_) | Phase::ConsumePending(_, _)
            ) {
                return false;
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
        self.check_message_expiry(*expiry)?;
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
        self.check_message_expiry(*expiry)?;
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
        publish_packet_size(topic.len(), payload.len(), false, expiry.is_some())
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
            cause: DeliveryCause::LiveExact,
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
        self.outgoing_ack_target(connection, packet_id, |delivery, _| {
            delivery.qos == Qos::AtLeastOnce
                && matches!(
                    delivery.phase,
                    Phase::AwaitPuback | Phase::StartPending(_) | Phase::StartRetry
                )
        })
    }

    #[must_use]
    pub fn qos2_ack_target(
        &self,
        connection: ConnectionKey,
        packet_id: OutboundPacketId,
        kind: Qos2Ack,
    ) -> Option<PubackTarget> {
        self.outgoing_ack_target(connection, packet_id, |delivery, claim| {
            delivery.qos == Qos::ExactlyOnce
                && match kind {
                    Qos2Ack::Pubrec => matches!(
                        delivery.phase,
                        Phase::AwaitPuback | Phase::StartPending(_) | Phase::StartRetry
                    ),
                    Qos2Ack::Pubcomp => claim.await_pubcomp && delivery.phase == Phase::AwaitPuback,
                }
        })
    }

    fn outgoing_ack_target(
        &self,
        connection: ConnectionKey,
        packet_id: OutboundPacketId,
        eligible: impl Fn(&Delivery, DeliveryClaim) -> bool,
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
                    && eligible(delivery, claim)
                    && sent == connection
                    && claim.packet_id == packet_id)
                    .then_some(PubackTarget {
                        delivery: delivery.id,
                        claim: claim.operation,
                        frame,
                    })
            })
    }

    pub(super) fn plan_observed_qos2_ack(
        &self,
        connection: ConnectionKey,
        kind: Qos2Ack,
        packet_id: OutboundPacketId,
        reason: u8,
        target: Option<PubackTarget>,
        draft: &mut Draft,
    ) -> Result<super::Change, Stop> {
        if self.connection_of(connection.session) != Some(connection.generation) {
            return Ok(super::Change::Nothing);
        }
        if !kind.accepts(reason) {
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
        if self.qos2_ack_target(connection, packet_id, kind) != Some(target) {
            return Ok(super::Change::Nothing);
        }
        let slot = self.pubsub.delivery(target.delivery).ok_or(IMPOSSIBLE)?;
        let delivery = &self.pubsub.deliveries.as_slice()[slot];
        if kind == Qos2Ack::Pubrec {
            if delivery.claim.is_some_and(|c| c.await_pubcomp) {
                if reason < 0x80 {
                    draft.frame(connection, OutboundFrame::Pubrel { packet_id })?;
                }
                return Ok(super::Change::Nothing);
            }
            if matches!(delivery.phase, Phase::StartPending(_) | Phase::StartRetry) {
                return if delivery.early_pubrec.is_none() {
                    Ok(changed(Change::PubrecObserved { slot, reason }))
                } else {
                    Ok(super::Change::Nothing)
                };
            }
            return Ok(changed(self.after_pubrec(slot, delivery, reason, draft)?));
        }
        let operation = self.operation(draft)?;
        let recipe = Recipe::FinishDelivery(self.finish_recipe(delivery, operation)?);
        let in_flight = self.plan_submission(operation, recipe, None, draft)?;
        Ok(changed(Change::Finish { slot, in_flight }))
    }

    fn after_pubrec(
        &self,
        slot: usize,
        delivery: &Delivery,
        reason: u8,
        draft: &mut Draft,
    ) -> Result<Change, Stop> {
        let operation = self.operation(draft)?;
        let guarded = self.finish_recipe(delivery, operation)?;
        // A negative PUBREC terminates the exchange without sending PUBREL.
        if reason >= 0x80 {
            let in_flight =
                self.plan_submission(operation, Recipe::FinishDelivery(guarded), None, draft)?;
            return Ok(Change::Finish { slot, in_flight });
        }
        let in_flight =
            self.plan_submission(operation, Recipe::PreparePubrel(guarded), None, draft)?;
        Ok(Change::PreparePubrel { slot, in_flight })
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
        if let InboundPacket::Qos2Ack {
            kind,
            packet_id,
            reason,
        } = packet
        {
            return self.plan_observed_qos2_ack(
                key,
                *kind,
                *packet_id,
                *reason,
                self.qos2_ack_target(key, *packet_id, *kind),
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
            InboundPacket::Subscribe {
                filters, denied, ..
            } => {
                let mut remaining = self.routes.capacity() - self.routes.len();
                for item in filters.as_slice() {
                    if denied & (1 << len) != 0 {
                        codes[len] = 0x87;
                        len += 1;
                        continue;
                    }
                    if !crate::contract::topic::valid_filter(item.filter.as_slice()) {
                        codes[len] = if item.filter.as_slice().starts_with(b"$share/") {
                            0x9e
                        } else {
                            0x8f
                        };
                        len += 1;
                        continue;
                    }
                    let replacing = changes
                        .as_slice()
                        .iter()
                        .any(|c: &GrantedSubscriptionChange| c.topic == item.filter)
                        || self
                            .routes
                            .as_slice()
                            .iter()
                            .any(|r| r.target == session && r.topic == item.filter);
                    if !replacing {
                        if remaining == 0 {
                            codes[len] = 0x97;
                            len += 1;
                            continue;
                        }
                        remaining -= 1;
                    }
                    let granted = item.qos;
                    codes[len] = granted.to_wire();
                    len += 1;
                    changes
                        .try_push(GrantedSubscriptionChange {
                            topic: item.filter.clone(),
                            granted: Some(granted),
                            options: item.options,
                        })
                        .map_err(|_| IMPOSSIBLE)?;
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
        } else if let Some(reason) = d.early_pubrec {
            self.after_pubrec(slot, d, reason, draft)?
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
            .next_send(session, generation, self.message_now, room);
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
            && d.ready_to_send(generation, room)
        {
            match self.plan_delivery_emit(d, draft)? {
                Emission::Frame(effect, connection) => {
                    return Ok(changed(Change::Emit {
                        slot,
                        effect,
                        connection,
                    }));
                }
                Emission::TooLarge => {
                    // MQTT-3.1.2-25: the PUBLISH is not sent and the delivery
                    // ends as a completed one. The finish guarded by its
                    // claim ends the claim and the obligation together, once.
                    let operation = self.operation(draft)?;
                    let in_flight = self.plan_submission(
                        operation,
                        Recipe::FinishDelivery(self.finish_recipe(d, operation)?),
                        None,
                        draft,
                    )?;
                    return Ok(changed(Change::Discard { slot, in_flight }));
                }
                Emission::Held => {}
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
                await_pubcomp: false,
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
                || (item.cause == DeliveryCause::RetainedReplay && !item.retain)
                || self.check_message_expiry(item.expiry).is_err()
                || item
                    .claim
                    .is_some_and(|claim| !claim.valid_for(item.qos, item.expiry.is_some()))
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
                    || (known.topic.is_some()
                        && (known.retain != item.retain || known.cause != item.cause))
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

    /// Every gate that holds a delivery stands ahead of the size test, so a
    /// delivery is found too large exactly where its PUBLISH would have been
    /// emitted, in delivery order and on the connection of that moment.
    fn plan_delivery_emit(&self, delivery: &Delivery, draft: &mut Draft) -> Result<Emission, Stop> {
        if !delivery.expiry_allows_send(self.message_now) {
            return Ok(Emission::Held);
        }
        let Some(generation) = self.connection_of(delivery.target) else {
            return Ok(Emission::Held);
        };
        let Some(settings) = self
            .pubsub
            .connection(generation)
            .map(|i| self.pubsub.connections.as_slice()[i])
        else {
            return Ok(Emission::Held);
        };
        let Some(claim) = delivery.claim else {
            return Ok(Emission::Held);
        };
        let connection = ConnectionKey {
            session: delivery.target,
            generation,
        };
        if claim.await_pubcomp {
            let effect = draft.emit(Effect::EmitFrame {
                connection,
                frame: OutboundFrame::Pubrel {
                    packet_id: claim.packet_id,
                },
                publish_order: None,
                pubrel: Some((delivery.id, claim.operation)),
            })?;
            return Ok(Emission::Frame(effect, connection));
        }
        if delivery.window != Some(generation)
            && self.pubsub.active(generation) >= usize::from(settings.receive_maximum)
        {
            return Ok(Emission::Held);
        }
        if self.pubsub.deliveries.as_slice().iter().any(|earlier| {
            earlier.target == delivery.target
                && earlier.id < delivery.id
                && earlier.waiting_to_send(generation, self.message_now)
        }) {
            // This also protects a claim result delivered after takeover:
            // restored earlier exchanges are replayed before newer ones.
            return Ok(Emission::Held);
        }
        let (Some(topic), Some(payload)) = (&delivery.topic, &delivery.payload) else {
            return Ok(Emission::Held);
        };
        let size = publish_packet_size(topic.len(), payload.len(), true, delivery.expiry.is_some())
            .ok_or(IMPOSSIBLE)?;
        if size > settings.maximum_packet_size as usize {
            return Ok(Emission::TooLarge);
        }
        let effect = draft.emit(Effect::EmitPublish {
            connection,
            delivery: delivery.id,
            cause: delivery.cause,
            qos: delivery.qos,
            packet_id: claim.packet_id,
            claim: claim.operation,
            topic: topic.clone(),
            payload: payload.clone(),
            dup: delivery.may_have_started,
            retain: delivery.retain,
            expiry: delivery.expiry,
            started: claim.started,
        })?;
        Ok(Emission::Frame(effect, connection))
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
                if matches!(d.phase, Phase::PubrelPending(_)) {
                    let claim = d.claim.ok_or(IMPOSSIBLE)?;
                    if facts
                        != (CommitFacts::DeliveryPubrelReady {
                            delivery: d.id,
                            target: d.target,
                            packet_id: claim.packet_id,
                            claim: claim.operation,
                        })
                    {
                        return Err(Stop::Fatal(CoreFault::ContradictoryOutcome { operation }));
                    }
                    let change = if d.retired {
                        Change::Finished {
                            slot,
                            finished: done,
                        }
                    } else {
                        Change::PubrelReady {
                            slot,
                            finished: done,
                        }
                    };
                    return Ok(Some(changed(change)));
                }
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
                        Phase::FinishPending(_) | Phase::DiscardPending(_) => {
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
                    if matches!(d.phase, Phase::DiscardPending(_)) {
                        draft.emit(Effect::DeliveryDiscarded {
                            delivery: d.id,
                            target: d.target,
                            operation,
                            reason: DeliveryDiscardReason::PacketTooLarge,
                        })?;
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
                                cause: d.cause,
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
                    // A delivery found too large is not decided here: it
                    // keeps its claim and its window, and the work turn
                    // that follows this commit submits its discard.
                    let emission = match self.plan_delivery_emit(d, draft)? {
                        Emission::Frame(effect, connection) => Some((effect, connection)),
                        Emission::Held | Emission::TooLarge => None,
                    };
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
                    if matches!(d.phase, Phase::DiscardPending(_)) {
                        draft.emit(Effect::DeliveryDiscarded {
                            delivery: d.id,
                            target: d.target,
                            operation,
                            reason: DeliveryDiscardReason::PacketTooLarge,
                        })?;
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
                // A fenced discard was certainly not applied: the claim
                // stands and the next work turn decides again, against the
                // connection of that moment. The client did nothing wrong,
                // so its connection is not closed.
                let fenced_discard =
                    matches!(d.phase, Phase::DiscardPending(_)) && reason == RejectReason::Fenced;
                if !fenced_discard
                    && let Some(connection) = self.connection_of(d.target)
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
                    && !d
                        .claim
                        .is_some_and(|claim| claim.started || claim.await_pubcomp) =>
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
            FrameProgress::Written { confirmed, total } if confirmed > 0 && confirmed <= total => {
                Change::Written {
                    slot,
                    complete: confirmed == total,
                }
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
        if self.check_message_expiry(expiry).is_err()
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
        claim.is_none_or(|claim| claim.valid_for(qos, expiry.is_some()))&&session_ok&&self.pubsub.deliveries.len()+same.len()<self.pubsub.deliveries.capacity()
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
                                d.cause = item.cause;
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
                            d.cause = item.cause;
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
                if !d.awaits_pubcomp() {
                    d.window = Some(connection.generation);
                }
                if d.frame.is_some() {
                    d.certain_never_started = false;
                }
                d.frame = Some((effect, connection));
                d.may_have_started = true;
                d.local_write_completed = false;
            }
            Change::Finish { slot, in_flight } => {
                let d = self
                    .pubsub
                    .deliveries
                    .as_mut_slice()
                    .get_mut(slot)
                    .ok_or(stale)?;
                d.early_pubrec = None;
                d.phase = Phase::FinishPending(in_flight);
            }
            Change::Discard { slot, in_flight } => {
                self.pubsub
                    .deliveries
                    .as_mut_slice()
                    .get_mut(slot)
                    .ok_or(stale)?
                    .phase = Phase::DiscardPending(in_flight);
            }
            Change::PreparePubrel { slot, in_flight } => {
                let d = self
                    .pubsub
                    .deliveries
                    .as_mut_slice()
                    .get_mut(slot)
                    .ok_or(stale)?;
                d.early_pubrec = None;
                d.pubrec = Some(in_flight.operation);
                d.phase = Phase::PubrelPending(in_flight);
            }
            Change::PubrelReady { slot, finished } => {
                let d = self
                    .pubsub
                    .deliveries
                    .as_mut_slice()
                    .get_mut(slot)
                    .ok_or(stale)?;
                d.claim.as_mut().ok_or(stale)?.await_pubcomp = true;
                d.phase = Phase::AwaitPuback;
                d.frame = None;
                d.certain_never_started = false;
                remember(&mut self.finished, finished)?;
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
                        | Phase::DiscardPending(f)
                        | Phase::PubrelPending(f)
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
            Change::PubrecObserved { slot, reason } => {
                self.pubsub
                    .deliveries
                    .as_mut_slice()
                    .get_mut(slot)
                    .ok_or(stale)?
                    .early_pubrec = Some(reason);
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
            Change::Written { slot, complete } => {
                let d = self
                    .pubsub
                    .deliveries
                    .as_mut_slice()
                    .get_mut(slot)
                    .ok_or(stale)?;
                d.local_write_completed |= complete;
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
            Recipe::PreparePubrel(c) => CommitFacts::DeliveryPubrelReady {
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
    fn qos2_keeps_window_and_identifier_until_pubcomp_finish_is_known() {
        let mut owner = world();
        owner.pubsub.connections.as_mut_slice()[0].receive_maximum = 1;
        queued(&mut owner, 0);
        owner.pubsub.deliveries.as_mut_slice()[0].qos = Qos::ExactlyOnce;
        queued(&mut owner, 1);
        let key = ConnectionKey {
            session: TARGET,
            generation: CONNECTION,
        };
        let send_ack = |owner: &mut Owner, kind, packet_id| {
            let target = owner.qos2_ack_target(key, packet_id, kind);
            apply(
                owner,
                Input::ObservedQos2Ack {
                    connection: key,
                    kind,
                    packet_id,
                    reason: 0,
                    target,
                },
            )
        };
        let claim = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        let (effect, Effect::Persist { command }) = &claim[0] else {
            panic!("claim submission")
        };
        let publish = known(&mut owner, *effect, command);
        let Effect::EmitPublish {
            packet_id,
            qos: Qos::ExactlyOnce,
            ..
        } = publish[0].1
        else {
            panic!("QoS 2 PUBLISH")
        };
        let prepared = send_ack(&mut owner, Qos2Ack::Pubrec, packet_id);
        let (effect, Effect::Persist { command }) = &prepared[0] else {
            panic!("prepare submission")
        };
        assert!(matches!(command.recipe.get(), Recipe::PreparePubrel(_)));
        let unknown = apply(
            &mut owner,
            Input::StoreResult {
                effect: *effect,
                operation: command.recipe.operation(),
                reply: StoreReply::Outcome(StoreOutcome::PendingOrUnknown),
            },
        );
        assert!(unknown.is_empty() && send_ack(&mut owner, Qos2Ack::Pubrec, packet_id).is_empty());
        assert!(
            apply(&mut owner, Input::WorkAvailable { session: TARGET }).is_empty(),
            "qos2_pubrel_before_commit"
        );
        assert_eq!(owner.pubsub.free_packet_id(TARGET, 1), None);
        assert!(known(&mut owner, *effect, command).is_empty());
        let release = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        assert!(matches!(release[0].1, Effect::EmitFrame {
            frame: OutboundFrame::Pubrel { packet_id: observed }, ..
        } if observed == packet_id));
        assert!(
            apply(&mut owner, Input::WorkAvailable { session: TARGET }).is_empty(),
            "qos2_receive_maximum_released_before_pubcomp"
        );
        let finished = send_ack(&mut owner, Qos2Ack::Pubcomp, packet_id);
        let (effect, Effect::Persist { command }) = &finished[0] else {
            panic!("finish submission")
        };
        assert_eq!(owner.pubsub.free_packet_id(TARGET, 1), None);
        assert!(apply(&mut owner, Input::WorkAvailable { session: TARGET }).is_empty());
        assert!(known(&mut owner, *effect, command).is_empty());
        assert_eq!(owner.pubsub.free_packet_id(TARGET, 1), Some(packet_id));
    }

    #[test]
    fn recovered_qos2_phase_selects_pubrel_even_with_publish_bytes_available() {
        let mut owner = world();
        let packet_id = OutboundPacketId::new(1).unwrap();
        let prior = OperationId::new(
            StoreIncarnation::new(1),
            RunEpoch::new(1),
            LogicalOwnerId::new(4),
            OperationSeq::new(100),
        );
        let claim = DeliveryClaim {
            operation: prior,
            packet_id,
            started: false,
            await_pubcomp: true,
        };
        owner
            .add_outbound_fact(
                delivery(0),
                TARGET,
                Revision::new(1),
                (Qos::ExactlyOnce, Some(claim), None, false),
            )
            .unwrap();
        let item = &mut owner.pubsub.deliveries.as_mut_slice()[0];
        item.topic = Some(bytes(b"topic"));
        item.payload = Some(bytes(b"payload"));
        let frames = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        assert!(
            matches!(&frames[..], [(_, Effect::EmitFrame {
            frame: OutboundFrame::Pubrel { packet_id: observed }, ..
        })] if *observed == packet_id),
            "qos2_recovery_replayed_publish_instead_of_pubrel"
        );
    }

    #[test]
    fn resumed_qos2_pubrel_is_independent_of_the_reduced_publish_window() {
        for pubrel_index in [1, 0] {
            let mut owner = world();
            owner.pubsub.connections.as_mut_slice()[0].receive_maximum = 1;
            let key = ConnectionKey {
                session: TARGET,
                generation: CONNECTION,
            };
            let ids = [
                OutboundPacketId::new(1).unwrap(),
                OutboundPacketId::new(2).unwrap(),
            ];
            let operations = [100, 101].map(|seq| {
                OperationId::new(
                    StoreIncarnation::new(1),
                    RunEpoch::new(1),
                    LogicalOwnerId::new(4),
                    OperationSeq::new(seq),
                )
            });
            for (index, (packet_id, operation)) in ids.into_iter().zip(operations).enumerate() {
                let claim = DeliveryClaim {
                    operation,
                    packet_id,
                    started: false,
                    await_pubcomp: index == pubrel_index,
                };
                owner
                    .add_outbound_fact(
                        delivery(index as u32),
                        TARGET,
                        Revision::new(1),
                        (Qos::ExactlyOnce, Some(claim), None, false),
                    )
                    .unwrap();
                if index != pubrel_index {
                    let item = &mut owner.pubsub.deliveries.as_mut_slice()[index];
                    item.topic = Some(bytes(b"topic"));
                    item.payload = Some(bytes(b"payload"));
                }
            }
            for (index, (expected_id, expected_claim)) in
                ids.into_iter().zip(operations).enumerate()
            {
                assert!(
                    owner.has_ready_work(TARGET),
                    "replay blocked before frame {index} with PUBREL at {pubrel_index}"
                );
                let frames = apply(&mut owner, Input::WorkAvailable { session: TARGET });
                let expected_delivery = delivery(index as u32);
                if index == pubrel_index {
                    assert!(matches!(&frames[..], [(_, Effect::EmitFrame {
                        frame: OutboundFrame::Pubrel { packet_id },
                        pubrel: Some((delivery, claim)), ..
                    })] if (*packet_id, *delivery, *claim) == (expected_id, expected_delivery, expected_claim)));
                } else {
                    assert!(matches!(&frames[..], [(_, Effect::EmitPublish {
                        packet_id, delivery, claim, qos: Qos::ExactlyOnce, dup: true, ..
                    })] if (*packet_id, *delivery, *claim) == (expected_id, expected_delivery, expected_claim)));
                }
            }
            assert_eq!(owner.pubsub.active(CONNECTION), 1);
            assert_eq!(owner.pubsub.free_packet_id(TARGET, 2), None);
            let ack = |owner: &mut Owner, kind, packet_id| {
                let target = owner.qos2_ack_target(key, packet_id, kind);
                assert!(target.is_some());
                let effects = apply(
                    owner,
                    Input::ObservedQos2Ack {
                        connection: key,
                        kind,
                        packet_id,
                        reason: 0,
                        target,
                    },
                );
                let (effect, Effect::Persist { command }) = &effects[0] else {
                    panic!("durable outgoing acknowledgment")
                };
                assert!(known(owner, *effect, command).is_empty());
            };
            ack(&mut owner, Qos2Ack::Pubcomp, ids[pubrel_index]);
            assert_eq!(owner.pubsub.active(CONNECTION), 1);
            assert_eq!(
                owner.pubsub.free_packet_id(TARGET, 2),
                Some(ids[pubrel_index])
            );
            let pending_publish = ids[1 - pubrel_index];
            ack(&mut owner, Qos2Ack::Pubrec, pending_publish);
            assert!(
                matches!(&apply(&mut owner, Input::WorkAvailable { session: TARGET })[..],
                [(_, Effect::EmitFrame { frame: OutboundFrame::Pubrel { packet_id }, .. })]
                if *packet_id == pending_publish)
            );
            assert_eq!(owner.pubsub.active(CONNECTION), 1);
            ack(&mut owner, Qos2Ack::Pubcomp, pending_publish);
            assert_eq!(owner.pubsub.active(CONNECTION), 0);
            assert!(owner.pubsub.deliveries.is_empty());
            assert_eq!(owner.pubsub.free_packet_id(TARGET, 2), Some(ids[0]));
        }
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
                await_pubcomp: false,
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
                denied: 0,
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
                    cause: DeliveryCause::RetainedReplay,
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
            cause: DeliveryCause::LiveExact,
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
                await_pubcomp: false,
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
        // The QoS 1 half pinned the closure of the connection. Its purpose
        // is unchanged: the Message Expiry property is charged. The same
        // bytes fit without it (127 bytes) and are discarded with it (133).
        for timed in [false, true] {
            let mut owner = world();
            message_clock(&mut owner, 10);
            owner.pubsub.connections.as_mut_slice()[0].maximum_packet_size = 128;
            queued(&mut owner, 0);
            let d = &mut owner.pubsub.deliveries.as_mut_slice()[0];
            d.expiry = timed.then(message_expiry);
            d.payload = Some(payload.clone());
            let (effect, command) =
                message_command(apply(&mut owner, Input::WorkAvailable { session: TARGET }));
            let effects = known(&mut owner, effect, &command);
            assert!(
                !effects
                    .iter()
                    .any(|(_, e)| matches!(e, Effect::CloseConnection { .. })),
                "qos1_packet_limit_closed_the_connection"
            );
            assert_eq!(
                effects
                    .iter()
                    .any(|(_, e)| matches!(e, Effect::EmitPublish { .. })),
                !timed,
                "qos1_expiry_property_not_charged"
            );
            assert_eq!(owner.pubsub.free_packet_id(TARGET, 1), None);
            if !timed {
                continue;
            }
            let (effect, discard) =
                message_command(apply(&mut owner, Input::WorkAvailable { session: TARGET }));
            assert!(
                matches!(discard.recipe.get(), Recipe::FinishDelivery(f) if f.claim == command.recipe.operation()),
                "qos1_expiry_property_not_charged"
            );
            assert_eq!(owner.pubsub.free_packet_id(TARGET, 1), None);
            let _ = known(&mut owner, effect, &discard);
            assert!(owner.pubsub.deliveries.is_empty());
            assert_eq!(owner.pubsub.free_packet_id(TARGET, 1).unwrap().get(), 1);
        }
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
            cause: DeliveryCause::LiveExact,
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

    #[test]
    fn message_expiry_clocked_subscription_reserves_all_replay_work_before_drawing_identities() {
        let mut owner = world();
        message_clock(&mut owner, 10);
        let counters = owner.counters;
        let result = owner.step(
            product_subscribe(),
            TurnGrant {
                work_limit: WorkUnits::new(200),
            },
        );
        assert!(
            matches!(
                result,
                StepOutcome::NeedCapacity {
                    need: CapacityNeed {
                        resource: Resource::Work,
                        ..
                    },
                    work_used: WorkUnits::ZERO,
                    ..
                }
            ),
            "clocked_subscription_exceeded_work_grant"
        );
        assert_eq!(owner.counters, counters);
        assert!(owner.pubsub.control.is_none());
        assert!(drain(&mut owner).is_empty());
    }

    #[test]
    fn message_expiry_partial_positive_progress_never_proves_a_claim_unsent() {
        let mut owner = world();
        message_clock(&mut owner, 10);
        let (frame, key, _) = message_frame(&mut owner, 0);
        assert!(
            message_progress(
                &mut owner,
                frame,
                key,
                FrameProgress::Written {
                    confirmed: 1,
                    total: 18
                }
            )
            .is_empty()
        );
        assert!(!owner.pubsub.deliveries.as_slice()[0].local_write_completed);
        assert!(
            !owner.pubsub.deliveries.as_slice()[0].claim.unwrap().started,
            "partial_callback_invented_started_marker"
        );
        message_clock(&mut owner, 12);
        assert!(
            message_progress(
                &mut owner,
                frame,
                key,
                FrameProgress::NotStarted {
                    reason: MessageExpiryBlock::Expired
                }
            )
            .is_empty(),
            "partial_positive_write_treated_as_unsent"
        );
        assert_eq!(owner.pubsub.deliveries.len(), 1);
    }

    #[test]
    fn message_expiry_puback_while_started_marker_is_deferred_keeps_its_exact_target() {
        let mut owner = world();
        message_clock(&mut owner, 10);
        let (frame, key, pi) = message_frame(&mut owner, 0);
        let target = owner.puback_target(key, pi).unwrap();
        let (effect, command) = message_command(message_progress(
            &mut owner,
            frame,
            key,
            FrameProgress::Started,
        ));
        apply(
            &mut owner,
            Input::StoreResult {
                effect,
                operation: command.recipe.operation(),
                reply: StoreReply::Outcome(StoreOutcome::RejectedCertain(RejectReason::Fenced)),
            },
        );
        assert_eq!(
            owner.puback_target(key, pi),
            Some(target),
            "fenced_marker_lost_puback_association"
        );
        assert!(message_puback(&mut owner, key, pi).is_empty());
        let (effect, retry) =
            message_command(apply(&mut owner, Input::WorkAvailable { session: TARGET }));
        assert!(
            matches!(retry.recipe.get(), Recipe::MarkDeliveryStarted(mark) if mark.claim == target.claim)
        );
        let (effect, finish) = message_command(known(&mut owner, effect, &retry));
        known(&mut owner, effect, &finish);
        assert!(owner.pubsub.deliveries.is_empty());
    }

    #[test]
    fn message_expiry_regressive_loaded_origin_refuses_the_page_before_mutation() {
        let mut owner = world();
        message_clock(&mut owner, 10);
        let effects = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        let effect = effects
            .iter()
            .find_map(|(id, e)| matches!(e, Effect::LoadWork { .. }).then_some(*id))
            .unwrap();
        let item = WorkItem {
            cause: DeliveryCause::LiveExact,
            delivery: delivery(0),
            target: TARGET,
            target_revision: Revision::new(1),
            qos: Qos::AtLeastOnce,
            retain: false,
            expiry: Some(MessageExpiry {
                observed: message_time(11),
                interval: 2,
            }),
            topic: bytes(b"topic"),
            payload: bytes(b"payload"),
            claim: None,
        };
        let result = owner.step(
            Input::WorkPage {
                effect,
                session: TARGET,
                items: BoundedVec::from_slice(&[item], 1).unwrap(),
                next: None,
            },
            TURN,
        );
        assert!(
            matches!(result, StepOutcome::Fatal(CoreFault::ImpossibleInput)),
            "regressive_message_time_silently_quarantined"
        );
        assert!(owner.pubsub.deliveries.is_empty());
    }

    #[test]
    fn message_expiry_qos12_admission_freezes_origin_and_handshakes_outlive_ttl() {
        for qos in [Qos::AtLeastOnce, Qos::ExactlyOnce] {
            for retain in [false, true] {
                let mut owner = world();
                owner.lifecycle_enabled = true;
                message_clock(&mut owner, 10);
                let packet = InboundPacket::Publish {
                    topic: bytes(b"topic"),
                    qos,
                    retain,
                    dup: false,
                    packet_id: Some(InboundPacketId::new(7).unwrap()),
                    payload: bytes(b"payload"),
                    expiry: Some(message_expiry()),
                };
                let (effect, command) = message_command(apply(
                    &mut owner,
                    Input::Packet {
                        connection: CONNECTION,
                        ingress_order: 1,
                        packet,
                    },
                ));
                let Recipe::ExpiringPublication(expiring) = command.recipe.get() else {
                    panic!("expiry_not_in_canonical_admission");
                };
                assert_eq!(expiring.expiry, message_expiry());
                let crate::contract::command::ExpiringAdmission::Qos12(retained) =
                    &expiring.admission
                else {
                    panic!("qos12_admission_kind");
                };
                assert_eq!(retained.admission.retain, retain);
                assert_eq!(retained.admission.qos, qos);
                let publication = retained.admission.publication;
                let exchange = retained.admission.source.exchange;
                let mut canonical = Vec::new();
                command
                    .recipe
                    .get()
                    .encode_canonical(&mut canonical)
                    .unwrap();
                let operation = command.recipe.operation();
                apply(
                    &mut owner,
                    Input::StoreResult {
                        effect,
                        operation,
                        reply: StoreReply::Outcome(StoreOutcome::PendingOrUnknown),
                    },
                );
                message_clock(&mut owner, 20);
                // A duplicate reception cannot replace the operation's original
                // canonical timestamp even if the client supplies a new TTL.
                assert!(
                    apply(
                        &mut owner,
                        Input::Packet {
                            connection: CONNECTION,
                            ingress_order: 2,
                            packet: InboundPacket::Publish {
                                topic: bytes(b"topic"),
                                qos,
                                retain,
                                dup: true,
                                packet_id: Some(InboundPacketId::new(7).unwrap()),
                                payload: bytes(b"payload"),
                                expiry: Some(MessageExpiry {
                                    observed: message_time(20),
                                    interval: 100
                                })
                            }
                        }
                    )
                    .is_empty()
                );
                let mut repeated = Vec::new();
                command
                    .recipe
                    .get()
                    .encode_canonical(&mut repeated)
                    .unwrap();
                assert_eq!(repeated, canonical);
                let ack = apply(
                    &mut owner,
                    Input::StoreResult {
                        effect,
                        operation,
                        reply: StoreReply::Outcome(StoreOutcome::Committed(
                            CommitFacts::PublicationAdmitted {
                                publication,
                                exchange,
                            },
                        )),
                    },
                );
                assert!(
                    ack.iter().any(|(_, e)| matches!(
                        e,
                        Effect::EmitFrame {
                            frame: OutboundFrame::Puback { .. } | OutboundFrame::Pubrec { .. },
                            ..
                        }
                    )),
                    "expiry_broke_inbound_acknowledgement"
                );
                if qos == Qos::ExactlyOnce {
                    let (_, release) = message_command(apply(
                        &mut owner,
                        Input::Packet {
                            connection: CONNECTION,
                            ingress_order: 3,
                            packet: InboundPacket::Pubrel {
                                packet_id: InboundPacketId::new(7).unwrap(),
                            },
                        },
                    ));
                    assert!(
                        matches!(release.recipe.get(), Recipe::ReleaseInbound(end) if end.exchange == exchange),
                        "late_pubrel_rematerialized_publication"
                    );
                }
            }
        }
    }

    #[test]
    fn message_expiry_volatile_qos0_drops_expired_work_without_durable_identity_or_mqtt_credit() {
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
        let counters = owner.counters;
        let credits = owner.leases.account().used();
        let effects = apply(
            &mut owner,
            Input::Packet {
                connection: CONNECTION,
                ingress_order: 1,
                packet: InboundPacket::Publish {
                    topic: bytes(b"topic"),
                    qos: Qos::AtMostOnce,
                    retain: false,
                    dup: false,
                    packet_id: None,
                    payload: bytes(b"payload"),
                    expiry: Some(message_expiry()),
                },
            },
        );
        assert!(
            !effects
                .iter()
                .any(|(_, e)| matches!(e, Effect::Persist { .. }))
        );
        let continuation = effects
            .iter()
            .find_map(|(_, e)| match e {
                Effect::Continue { continuation } => Some(*continuation),
                _ => None,
            })
            .unwrap();
        message_clock(&mut owner, 12);
        let effects = apply(
            &mut owner,
            Input::Continue {
                continuation,
                budget: WorkUnits::new(100_000),
            },
        );
        assert!(effects.iter().any(|(_, e)| matches!(
            e,
            Effect::Qos0Dropped {
                reason: Qos0DropReason::MessageExpired,
                ..
            }
        )));
        assert!(owner.pubsub.volatile.is_empty());
        assert_eq!(owner.counters.operation, counters.operation);
        assert_eq!(owner.counters.publication, counters.publication);
        assert_eq!(owner.counters.payload, counters.payload);
        assert_eq!(owner.leases.account().used(), credits);
    }

    #[test]
    fn message_expiry_retained_qos0_unknown_result_survives_source_retirement_and_resolves() {
        let mut owner = world();
        owner.lifecycle_enabled = true;
        message_clock(&mut owner, 10);
        let mut packet = retained_zero(1);
        let Input::Packet {
            packet: InboundPacket::Publish { expiry, .. },
            ..
        } = &mut packet
        else {
            panic!("retained packet");
        };
        *expiry = Some(message_expiry());
        let (effect, command) = message_command(apply(&mut owner, packet));
        let Recipe::ExpiringPublication(expiring) = command.recipe.get() else {
            panic!("retained_qos0_expiry_not_canonical");
        };
        let crate::contract::command::ExpiringAdmission::Qos0(retained) = &expiring.admission
        else {
            panic!("retained_qos0_invented_exchange");
        };
        let publication = retained.publication;
        assert_eq!(retained.costs.exchange_slots, ExchangeSlots::ZERO);
        let operation = command.recipe.operation();
        apply(
            &mut owner,
            Input::StoreResult {
                effect,
                operation,
                reply: StoreReply::Outcome(StoreOutcome::PendingOrUnknown),
            },
        );
        owner.pubsub.retire_session(TARGET);
        apply(
            &mut owner,
            Input::ConnectionClosed {
                connection: CONNECTION,
            },
        );
        message_clock(&mut owner, 12);
        let flight = owner.pubsub.retained.unwrap();
        assert!(flight.retired);
        let effects = apply(
            &mut owner,
            Input::Deadline {
                token: flight.in_flight.deadline,
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
        assert!(
            apply(
                &mut owner,
                Input::StoreResult {
                    effect: resolve,
                    operation,
                    reply: StoreReply::Outcome(StoreOutcome::Committed(
                        CommitFacts::RetainedQos0Stored { publication }
                    ))
                }
            )
            .is_empty()
        );
        assert!(owner.pubsub.retained.is_none());
        assert!(owner.pubsub.deliveries.is_empty());
        assert_eq!(owner.leases.account().used().slots, ExchangeSlots::ZERO);
    }

    #[test]
    fn message_expiry_start_and_exact_expiry_reserve_effects_before_mutating_claim_or_counters() {
        for start in [false, true] {
            let mut owner = world();
            message_clock(&mut owner, 10);
            let (effect, connection, _) = message_frame(&mut owner, 0);
            if !start {
                message_clock(&mut owner, 12);
            }
            let progress = if start {
                FrameProgress::Started
            } else {
                FrameProgress::NotStarted {
                    reason: MessageExpiryBlock::Expired,
                }
            };
            let counters = owner.counters;
            let delivery = owner.pubsub.deliveries.as_slice()[0].clone();
            let held = owner
                .outbox
                .reserve(NonZeroUsize::new(owner.outbox.free_slots() - 1).unwrap())
                .unwrap();
            let result = owner.step(
                Input::FrameResult {
                    effect,
                    connection,
                    progress,
                },
                TURN,
            );
            assert!(
                matches!(
                    result,
                    StepOutcome::NeedCapacity {
                        need: CapacityNeed {
                            resource: Resource::EffectSlots,
                            ..
                        },
                        ..
                    }
                ),
                "expiry_transition_did_not_reserve_whole_operation"
            );
            assert_eq!(owner.counters, counters);
            assert_eq!(owner.pubsub.deliveries.as_slice()[0], delivery);
            owner.outbox.release(held).unwrap();
            let (_, command) =
                message_command(message_progress(&mut owner, effect, connection, progress));
            assert_eq!(
                command.recipe.operation().sequence(),
                counters.operation.unwrap()
            );
        }
    }

    #[test]
    fn message_expiry_recovery_inventory_keeps_unmarked_claims_quarantined_and_replays_started() {
        for started in [false, true] {
            for unknown in [false, true] {
                let mut owner = world();
                let mut now = message_time(12);
                if unknown {
                    now.domain = [8; 32];
                }
                apply(&mut owner, Input::MessageClock { now });
                owner.recovery = Some(RecoveryOrder::new());
                let claim = DeliveryClaim {
                    operation: OperationId::new(
                        StoreIncarnation::new(1),
                        RunEpoch::new(1),
                        LogicalOwnerId::new(4),
                        OperationSeq::new(100),
                    ),
                    packet_id: OutboundPacketId::new(1).unwrap(),
                    started,
                    await_pubcomp: false,
                };
                let fact = RecoveredFact::OutboundDelivery {
                    delivery: delivery(0),
                    target: TARGET,
                    target_revision: Revision::new(1),
                    qos: Qos::AtLeastOnce,
                    claim: Some(claim),
                    expiry: Some(message_expiry()),
                    started,
                };
                apply(
                    &mut owner,
                    Input::Recovery {
                        facts: BoundedVec::from_slice(&[fact], 1).unwrap(),
                        last: true,
                    },
                );
                let effects = apply(&mut owner, Input::WorkAvailable { session: TARGET });
                let read = effects
                    .iter()
                    .find_map(|(id, e)| matches!(e, Effect::LoadWork { .. }).then_some(*id))
                    .unwrap();
                let item = WorkItem {
                    cause: DeliveryCause::LiveExact,
                    delivery: delivery(0),
                    target: TARGET,
                    target_revision: Revision::new(1),
                    qos: Qos::AtLeastOnce,
                    retain: true,
                    expiry: Some(message_expiry()),
                    topic: bytes(b"topic"),
                    payload: bytes(b"payload"),
                    claim: Some(claim),
                };
                apply(
                    &mut owner,
                    Input::WorkPage {
                        effect: read,
                        session: TARGET,
                        items: BoundedVec::from_slice(&[item], 1).unwrap(),
                        next: None,
                    },
                );
                let effects = apply(&mut owner, Input::WorkAvailable { session: TARGET });
                assert_eq!(effects.iter().any(|(_, e)| matches!(e, Effect::EmitPublish { started: true, packet_id, expiry: Some(expiry), .. } if *packet_id == claim.packet_id && *expiry == message_expiry())), started, "recovery_invented_unsent_or_started_authority");
                assert!(
                    !effects
                        .iter()
                        .any(|(_, e)| matches!(e, Effect::Persist { .. }))
                );
                assert_eq!(owner.pubsub.free_packet_id(TARGET, 1), None);
                assert!(!owner.pubsub.deliveries.as_slice()[0].certain_never_started);
            }
        }
    }

    #[test]
    fn message_expiry_receipt_frontier_tracks_tag13_payload_and_tag14_complete_replay_range() {
        let mut owner = world();
        owner.lifecycle_enabled = true;
        message_register_receipts(&mut owner);
        message_clock(&mut owner, 10);
        let mut packet = retained_zero(1);
        let Input::Packet {
            packet: InboundPacket::Publish { expiry, .. },
            ..
        } = &mut packet
        else {
            panic!("retained packet");
        };
        *expiry = Some(message_expiry());
        let (effect, command) = message_command(apply(&mut owner, packet));
        let retained = command.recipe.get().retained_qos0().unwrap();
        let publication = retained.publication;
        let payload = retained.payload.id.sequence();
        apply(
            &mut owner,
            Input::StoreResult {
                effect,
                operation: command.recipe.operation(),
                reply: StoreReply::Outcome(StoreOutcome::Committed(
                    CommitFacts::RetainedQos0Stored { publication },
                )),
            },
        );
        let (effect, replay) = message_command(apply(&mut owner, product_subscribe()));
        let Recipe::TimedReplaySubscriptions(timed) = replay.recipe.get() else {
            panic!("timed replay");
        };
        let last = timed.replay.first_publication.sequence().get()
            + crate::contract::retained::MAX_REPLAYS as u64
            - 1;
        apply(
            &mut owner,
            Input::StoreResult {
                effect,
                operation: replay.recipe.operation(),
                reply: StoreReply::Outcome(StoreOutcome::Committed(
                    CommitFacts::SubscriptionsChanged {
                        session: TARGET,
                        route_revision: Revision::new(1),
                    },
                )),
            },
        );
        owner.receipts.test_request_sweep();
        let effects = apply(&mut owner, Input::MaintainReceipts);
        let closure = effects
            .iter()
            .find_map(|(_, e)| match e {
                Effect::CollectReceipts { collection } => Some(collection.closure),
                _ => None,
            })
            .unwrap();
        assert_eq!(closure.through, replay.recipe.operation().sequence());
        assert_eq!(
            closure.publication_through.get(),
            last,
            "tag14_receipt_lost_unused_replay_suffix"
        );
        assert_eq!(
            closure.payload_through, payload,
            "tag13_receipt_lost_payload_identity"
        );
    }

    #[test]
    fn message_expiry_target_retirement_consumes_started_and_expired_results_without_receipt_holes()
    {
        for start in [false, true] {
            let mut owner = world();
            message_register_receipts(&mut owner);
            message_clock(&mut owner, 10);
            let (frame, key, _) = message_frame(&mut owner, 0);
            if !start {
                message_clock(&mut owner, 12);
            }
            let progress = if start {
                FrameProgress::Started
            } else {
                FrameProgress::NotStarted {
                    reason: MessageExpiryBlock::Expired,
                }
            };
            let (effect, command) =
                message_command(message_progress(&mut owner, frame, key, progress));
            owner.pubsub.retire_session(TARGET);
            apply(
                &mut owner,
                Input::ConnectionClosed {
                    connection: CONNECTION,
                },
            );
            assert_eq!(owner.receipt_status().unwrap().2, 1);
            assert!(known(&mut owner, effect, &command).is_empty());
            assert!(owner.pubsub.deliveries.is_empty());
            assert_eq!(
                owner.receipt_status().unwrap().2,
                0,
                "retired_expiry_control_left_receipt_hole"
            );
            assert!(known(&mut owner, effect, &command).is_empty());
        }
    }

    // Maximum Packet Size of the receiving client (MQTT-3.1.2-25, ADR 0025
    // as amended): a QoS 1 or 2 delivery whose PUBLISH would exceed it is
    // not sent and ends through the finish guarded by its claim.

    const OTHER: SessionId = SessionId::new(2);

    /// The name of each effect these tests expect, in order.
    fn names(effects: &[(EffectId, Effect)]) -> Vec<&'static str> {
        effects
            .iter()
            .map(|(_, effect)| match effect {
                Effect::Persist { .. } => "Persist",
                Effect::ArmDeadline { .. } => "ArmDeadline",
                Effect::EmitPublish { .. } => "EmitPublish",
                Effect::CloseConnection { .. } => "CloseConnection",
                Effect::LoadWork { .. } => "LoadWork",
                Effect::DeliveryDiscarded { .. } => "DeliveryDiscarded",
                _ => "another effect",
            })
            .collect()
    }

    /// The one observation a committed discard publishes: the delivery, its
    /// target and the `FinishDelivery` that ended it.
    fn discarded(effects: &[(EffectId, Effect)]) -> (DeliveryId, SessionId, OperationId) {
        let [
            (
                _,
                Effect::DeliveryDiscarded {
                    delivery,
                    target,
                    operation,
                    reason: DeliveryDiscardReason::PacketTooLarge,
                },
            ),
        ] = effects
        else {
            panic!("one discard observation: {effects:?}")
        };
        (*delivery, *target, *operation)
    }

    /// The world, its connection limited to `maximum` bytes per packet and
    /// `receive_maximum` unacknowledged PUBLISH packets.
    fn limited(maximum: u32, receive_maximum: u16) -> Owner {
        let mut owner = world();
        let settings = &mut owner.pubsub.connections.as_mut_slice()[0];
        settings.maximum_packet_size = maximum;
        settings.receive_maximum = receive_maximum;
        owner
    }

    /// A loaded, unclaimed delivery of topic `topic`: 12 bytes of PUBLISH
    /// besides its payload, 17 with a Message Expiry Interval.
    fn sized(owner: &mut Owner, index: u32, qos: Qos, timed: bool, payload: usize) {
        queued(owner, index);
        let d = owner.pubsub.deliveries.as_mut_slice().last_mut().unwrap();
        d.qos = qos;
        d.expiry = timed.then(message_expiry);
        d.payload = Some(SharedBytes::copy_from(&alloc::vec![7; payload], 128).unwrap());
    }

    /// Submits the claim the next work turn makes and applies its commit.
    fn claim_next(owner: &mut Owner, session: SessionId) -> (Command, Vec<(EffectId, Effect)>) {
        let (effect, command) = message_command(apply(owner, Input::WorkAvailable { session }));
        assert!(matches!(command.recipe.get(), Recipe::ClaimDelivery(_)));
        let effects = known(owner, effect, &command);
        (command, effects)
    }

    /// The work turn that follows the claim of an oversized delivery: the
    /// finish guarded by that claim and its deadline, nothing else.
    fn discard_next(
        owner: &mut Owner,
        session: SessionId,
        claim: &Command,
    ) -> (EffectId, Command, DeadlineToken) {
        assert!(owner.has_ready_work(session), "the discard is ready work");
        let effects = apply(owner, Input::WorkAvailable { session });
        assert_eq!(names(&effects), ["Persist", "ArmDeadline"]);
        let Effect::ArmDeadline { token, .. } = effects[1].1 else {
            panic!("deadline of the discard")
        };
        let (effect, command) = message_command(effects);
        let Recipe::ClaimDelivery(claimed) = claim.recipe.get() else {
            panic!("claim")
        };
        assert_eq!(
            command.recipe.get(),
            &Recipe::FinishDelivery(FinishDelivery {
                operation: command.recipe.operation(),
                delivery: claimed.delivery,
                target: claimed.target,
                target_revision: claimed.target_revision,
                packet_id: claimed.packet_id,
                claim: claimed.operation,
            }),
            "the discard is the finish guarded by the claim"
        );
        assert_ne!(command.recipe.operation(), claimed.operation);
        let held = owner
            .pubsub
            .deliveries
            .as_slice()
            .iter()
            .find(|d| d.id == claimed.delivery)
            .expect("the delivery is kept until its finish is known");
        assert!(
            matches!(held.phase, Phase::DiscardPending(flight) if flight.operation == command.recipe.operation()),
            "{:?}",
            held.phase
        );
        assert!(held.frame.is_none() && !held.retired);
        assert_eq!(
            held.claim.map(|claim| claim.operation),
            Some(claimed.operation)
        );
        (effect, command, token)
    }

    #[test]
    fn a_delivery_is_sent_at_its_exact_packet_size_and_discarded_one_byte_below() {
        for qos in [Qos::AtLeastOnce, Qos::ExactlyOnce] {
            for timed in [false, true] {
                let fixed = 12 + usize::from(timed) * 5;
                // `payload`, then the two sides of the Remaining Length step:
                // a body of 127 bytes has a 2-byte header, one of 128 has 3.
                for (payload, exact) in [
                    (7, fixed + 7),
                    (127 - (fixed - 2), 129),
                    (128 - (fixed - 2), 131),
                ] {
                    let case = (qos, timed, payload);
                    let mut owner = limited(exact as u32, 8);
                    message_clock(&mut owner, 10);
                    sized(&mut owner, 0, qos, timed, payload);
                    let (_, sent) = claim_next(&mut owner, TARGET);
                    assert_eq!(names(&sent), ["EmitPublish"], "{case:?}");

                    let mut owner = limited(exact as u32 - 1, 8);
                    message_clock(&mut owner, 10);
                    sized(&mut owner, 0, qos, timed, payload);
                    let (claim, sent) = claim_next(&mut owner, TARGET);
                    assert!(
                        sent.is_empty(),
                        "{case:?}: no PUBLISH, no acknowledgement frame, no close: {sent:?}"
                    );
                    let (effect, discard, _) = discard_next(&mut owner, TARGET, &claim);
                    assert!(
                        apply(&mut owner, Input::WorkAvailable { session: TARGET })
                            .iter()
                            .all(|(_, e)| matches!(e, Effect::LoadWork { .. })),
                        "{case:?}: a pending discard is submitted once"
                    );
                    let observed = known(&mut owner, effect, &discard);
                    assert_eq!(names(&observed), ["DeliveryDiscarded"], "{case:?}");
                    assert_eq!(
                        discarded(&observed),
                        (delivery(0), TARGET, discard.recipe.operation()),
                        "{case:?}"
                    );
                    assert!(owner.pubsub.deliveries.is_empty(), "{case:?}");
                    assert_eq!(owner.pubsub.active(CONNECTION), 0, "{case:?}");
                    assert_eq!(owner.pubsub.free_packet_id(TARGET, 1).unwrap().get(), 1);
                    assert!(
                        known(&mut owner, effect, &discard).is_empty(),
                        "{case:?}: a repeated result publishes no second observation"
                    );
                }
            }
        }
    }

    #[test]
    fn two_destinations_of_one_publication_are_decided_by_their_own_packet_limits() {
        let mut owner = world();
        let other = ConnectionGeneration::new(11);
        let _ = apply(
            &mut owner,
            Input::Packet {
                connection: other,
                ingress_order: 0,
                packet: InboundPacket::ConnectResume {
                    client_id: bytes(b"other"),
                },
            },
        );
        assert!(
            apply(
                &mut owner,
                Input::ConnectionSettings {
                    connection: other,
                    receive_maximum: 8,
                    maximum_packet_size: 18,
                }
            )
            .is_empty()
        );
        queued(&mut owner, 0);
        queued(&mut owner, 1);
        owner.pubsub.deliveries.as_mut_slice()[1].target = OTHER;
        let (_, sent) = claim_next(&mut owner, TARGET);
        assert!(matches!(
            &sent[..],
            [(_, Effect::EmitPublish { delivery: d, connection, .. })]
                if *d == delivery(0) && connection.generation == CONNECTION
        ));
        let (claim, sent) = claim_next(&mut owner, OTHER);
        assert!(sent.is_empty(), "{sent:?}");
        let (effect, discard, _) = discard_next(&mut owner, OTHER, &claim);
        let observed = known(&mut owner, effect, &discard);
        assert_eq!(names(&observed), ["DeliveryDiscarded"]);
        assert_eq!(
            discarded(&observed),
            (delivery(1), OTHER, discard.recipe.operation())
        );
        let left = owner.pubsub.deliveries.as_slice();
        assert_eq!(left.len(), 1, "the other destination keeps its delivery");
        assert_eq!((left[0].id, left[0].target), (delivery(0), TARGET));
        assert!(left[0].frame.is_some() && left[0].phase == Phase::AwaitPuback);
        assert_eq!(owner.pubsub.active(CONNECTION), 1);
        assert_eq!(owner.pubsub.active(other), 0);
    }

    #[test]
    fn a_pending_discard_holds_its_window_and_identifier_until_its_finish_is_known() {
        for receive_maximum in [1, 2] {
            let mut owner = limited(18, receive_maximum);
            sized(&mut owner, 0, Qos::AtLeastOnce, false, 7);
            sized(&mut owner, 1, Qos::AtLeastOnce, false, 1);
            let (claim, sent) = claim_next(&mut owner, TARGET);
            assert!(sent.is_empty(), "{sent:?}");
            let (effect, discard, _) = discard_next(&mut owner, TARGET, &claim);
            assert_eq!(owner.pubsub.active(CONNECTION), 1);
            assert_eq!(owner.pubsub.free_packet_id(TARGET, 4).unwrap().get(), 2);
            if receive_maximum == 1 {
                assert!(
                    apply(&mut owner, Input::WorkAvailable { session: TARGET }).is_empty(),
                    "the full window admits no claim while the discard is pending"
                );
            } else {
                let (next, sent) = claim_next(&mut owner, TARGET);
                assert!(
                    matches!(next.recipe.get(), Recipe::ClaimDelivery(c) if c.delivery == delivery(1) && c.packet_id.get() == 2),
                    "the free window admits the next delivery beside the pending discard"
                );
                assert!(matches!(
                    &sent[..],
                    [(_, Effect::EmitPublish { delivery: d, dup: false, .. })] if *d == delivery(1)
                ));
            }
            let observed = known(&mut owner, effect, &discard);
            assert_eq!(names(&observed), ["DeliveryDiscarded"]);
            assert_eq!(
                discarded(&observed),
                (delivery(0), TARGET, discard.recipe.operation())
            );
            if receive_maximum == 1 {
                let (next, sent) = claim_next(&mut owner, TARGET);
                assert!(
                    matches!(next.recipe.get(), Recipe::ClaimDelivery(c) if c.delivery == delivery(1) && c.packet_id.get() == 1),
                    "the commit of the discard returns the window and the identifier"
                );
                assert!(matches!(
                    &sent[..],
                    [(_, Effect::EmitPublish { delivery: d, dup: false, .. })] if *d == delivery(1)
                ));
            }
            assert_eq!(owner.pubsub.deliveries.len(), 1);
            assert_eq!(owner.pubsub.active(CONNECTION), 1);
        }
    }

    /// A claim found in durable data, as after a crash between the commit of
    /// the claim and the commit of its discard.
    fn recovered_claim(owner: &mut Owner) -> DeliveryClaim {
        let claim = DeliveryClaim {
            operation: OperationId::new(
                StoreIncarnation::new(1),
                RunEpoch::new(1),
                LogicalOwnerId::new(4),
                OperationSeq::new(100),
            ),
            packet_id: OutboundPacketId::new(3).unwrap(),
            started: false,
            await_pubcomp: false,
        };
        owner
            .add_outbound_fact(
                delivery(0),
                TARGET,
                Revision::new(1),
                (Qos::AtLeastOnce, Some(claim), None, false),
            )
            .unwrap();
        let item = &mut owner.pubsub.deliveries.as_mut_slice()[0];
        item.topic = Some(bytes(b"topic"));
        item.payload = Some(bytes(b"payload"));
        claim
    }

    #[test]
    fn a_recovered_claim_is_discarded_again_under_its_identity_or_sent_as_a_duplicate() {
        let mut owner = limited(18, 8);
        let claim = recovered_claim(&mut owner);
        assert!(owner.has_ready_work(TARGET));
        let effects = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        assert_eq!(names(&effects), ["Persist", "ArmDeadline"]);
        let (effect, discard) = message_command(effects);
        let Recipe::FinishDelivery(finish) = discard.recipe.get() else {
            panic!("the recovered claim is discarded again: {discard:?}")
        };
        assert_eq!(
            (finish.delivery, finish.packet_id, finish.claim),
            (delivery(0), claim.packet_id, claim.operation),
            "the second discard is guarded by the same claim"
        );
        assert_ne!(finish.operation, claim.operation);
        let observed = known(&mut owner, effect, &discard);
        assert_eq!(names(&observed), ["DeliveryDiscarded"]);
        assert_eq!(
            discarded(&observed),
            (delivery(0), TARGET, discard.recipe.operation())
        );
        assert!(owner.pubsub.deliveries.is_empty());

        let mut owner = limited(19, 8);
        let claim = recovered_claim(&mut owner);
        let sent = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        assert!(
            matches!(
                &sent[..],
                [(_, Effect::EmitPublish { packet_id, claim: op, dup: true, .. })]
                    if *packet_id == claim.packet_id && *op == claim.operation
            ),
            "a recovered claim that fits is sent with DUP=1: {sent:?}"
        );
    }

    /// The subscriber closes `CONNECTION` and resumes on a new generation
    /// that accepts 65536 bytes per packet.
    fn reconnect_larger(owner: &mut Owner) -> ConnectionGeneration {
        let next = ConnectionGeneration::new(11);
        let _ = apply(
            owner,
            Input::ConnectionClosed {
                connection: CONNECTION,
            },
        );
        let _ = apply(
            owner,
            Input::Packet {
                connection: next,
                ingress_order: 0,
                packet: InboundPacket::ConnectResume {
                    client_id: bytes(b"subscriber"),
                },
            },
        );
        assert!(
            apply(
                owner,
                Input::ConnectionSettings {
                    connection: next,
                    receive_maximum: 8,
                    maximum_packet_size: 65536,
                }
            )
            .is_empty()
        );
        next
    }

    fn sends_nothing(effects: &[(EffectId, Effect)]) -> bool {
        effects
            .iter()
            .all(|(_, e)| matches!(e, Effect::LoadWork { .. }))
    }

    #[test]
    fn a_discard_of_unknown_outcome_is_resolved_and_never_sent_on_a_larger_connection() {
        let mut owner = limited(18, 8);
        sized(&mut owner, 0, Qos::AtLeastOnce, false, 7);
        let (claim, _) = claim_next(&mut owner, TARGET);
        let (effect, discard, deadline) = discard_next(&mut owner, TARGET, &claim);
        let operation = discard.recipe.operation();
        assert!(
            apply(
                &mut owner,
                Input::StoreResult {
                    effect,
                    operation,
                    reply: StoreReply::Outcome(StoreOutcome::PendingOrUnknown),
                }
            )
            .is_empty()
        );
        let _ = reconnect_larger(&mut owner);
        let work = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        assert!(
            sends_nothing(&work),
            "a discard that may have committed is never sent: {work:?}"
        );
        assert_eq!(owner.pubsub.free_packet_id(TARGET, 4).unwrap().get(), 2);
        let resolution = apply(
            &mut owner,
            Input::Deadline {
                token: deadline,
                now: crate::contract::ids::LogicalTime::new(10),
            },
        );
        assert!(
            matches!(resolution[0].1, Effect::ResolveAndFence { operation: resolved } if resolved == operation)
        );
        let observed = known(&mut owner, resolution[0].0, &discard);
        assert_eq!(names(&observed), ["DeliveryDiscarded"]);
        assert_eq!(
            discarded(&observed),
            (delivery(0), TARGET, discard.recipe.operation())
        );
        assert!(owner.pubsub.deliveries.is_empty());
        let work = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        assert!(sends_nothing(&work), "{work:?}");
    }

    #[test]
    fn a_fenced_discard_closes_nothing_and_is_decided_again() {
        for larger in [false, true] {
            let mut owner = limited(18, 8);
            sized(&mut owner, 0, Qos::AtLeastOnce, false, 7);
            let (claim, _) = claim_next(&mut owner, TARGET);
            let (_, discard, deadline) = discard_next(&mut owner, TARGET, &claim);
            let resolution = apply(
                &mut owner,
                Input::Deadline {
                    token: deadline,
                    now: crate::contract::ids::LogicalTime::new(10),
                },
            );
            assert!(matches!(resolution[0].1, Effect::ResolveAndFence { .. }));
            let fenced = apply(
                &mut owner,
                Input::StoreResult {
                    effect: resolution[0].0,
                    operation: discard.recipe.operation(),
                    reply: StoreReply::Outcome(StoreOutcome::RejectedCertain(RejectReason::Fenced)),
                },
            );
            assert!(
                fenced.is_empty(),
                "a discard certainly not applied closes nothing: {fenced:?}"
            );
            assert_eq!(owner.pubsub.deliveries.len(), 1, "the claim stands");
            assert_eq!(owner.pubsub.free_packet_id(TARGET, 4).unwrap().get(), 2);
            if larger {
                let _ = reconnect_larger(&mut owner);
                let sent = apply(&mut owner, Input::WorkAvailable { session: TARGET });
                assert!(
                    matches!(&sent[..], [(_, Effect::EmitPublish { delivery: d, .. })] if *d == delivery(0)),
                    "decided again against the connection of that moment: {sent:?}"
                );
            } else {
                let (effect, again, _) = discard_next(&mut owner, TARGET, &claim);
                assert_ne!(again.recipe.operation(), discard.recipe.operation());
                let observed = known(&mut owner, effect, &again);
                assert_eq!(names(&observed), ["DeliveryDiscarded"]);
                assert_eq!(
                    discarded(&observed),
                    (delivery(0), TARGET, again.recipe.operation())
                );
                assert!(owner.pubsub.deliveries.is_empty());
            }
        }
    }

    #[test]
    fn a_discard_refused_for_another_reason_keeps_the_rule_of_a_refused_finish() {
        let mut owner = limited(18, 8);
        sized(&mut owner, 0, Qos::AtLeastOnce, false, 7);
        let (claim, _) = claim_next(&mut owner, TARGET);
        let (effect, discard, _) = discard_next(&mut owner, TARGET, &claim);
        let refused = apply(
            &mut owner,
            Input::StoreResult {
                effect,
                operation: discard.recipe.operation(),
                reply: StoreReply::Outcome(StoreOutcome::RejectedCertain(
                    RejectReason::PreconditionFailed,
                )),
            },
        );
        assert!(matches!(
            &refused[..],
            [(
                _,
                Effect::CloseConnection {
                    reason: CloseReason::CapacityExceeded,
                    ..
                }
            )]
        ));
        assert_eq!(owner.pubsub.deliveries.len(), 1);
        assert_eq!(owner.pubsub.free_packet_id(TARGET, 4).unwrap().get(), 2);
    }

    #[test]
    fn a_session_retired_during_a_discard_consumes_its_result_without_a_receipt_hole() {
        let mut owner = limited(18, 8);
        message_register_receipts(&mut owner);
        sized(&mut owner, 0, Qos::AtLeastOnce, false, 7);
        let (claim, _) = claim_next(&mut owner, TARGET);
        let (effect, discard, _) = discard_next(&mut owner, TARGET, &claim);
        owner.pubsub.retire_session(TARGET);
        let _ = apply(
            &mut owner,
            Input::ConnectionClosed {
                connection: CONNECTION,
            },
        );
        assert_eq!(owner.receipt_status().unwrap().2, 1);
        let observed = known(&mut owner, effect, &discard);
        assert_eq!(names(&observed), ["DeliveryDiscarded"]);
        assert_eq!(
            discarded(&observed),
            (delivery(0), TARGET, discard.recipe.operation())
        );
        assert!(owner.pubsub.deliveries.is_empty());
        assert_eq!(
            owner.receipt_status().unwrap().2,
            0,
            "retired_discard_left_receipt_hole"
        );
        assert!(known(&mut owner, effect, &discard).is_empty());
    }

    #[test]
    fn an_absent_maximum_packet_size_sends_the_largest_admissible_delivery() {
        // MQTT 3.1.1 and an MQTT 5 CONNECT without the property both reach
        // the core as `u32::MAX`: nothing the limits admit is discarded.
        for qos in [Qos::AtLeastOnce, Qos::ExactlyOnce] {
            let mut owner = limited(u32::MAX, 8);
            message_clock(&mut owner, 10);
            sized(&mut owner, 0, qos, true, 128);
            owner.pubsub.deliveries.as_mut_slice()[0].topic = Some(bytes(&[b't'; 128]));
            let (_, sent) = claim_next(&mut owner, TARGET);
            assert_eq!(names(&sent), ["EmitPublish"]);
        }
    }
    /// The broker sends PUBREL in the order in which it received the
    /// PUBRECs (MQTT-4.6.0-4, which MQTT-4.6.0-6 of 3.1.1 puts on a server
    /// for its deliveries), whenever the durable preparations of those
    /// PUBRELs are known. Here two PUBRECs arrive in order and the store
    /// answers their preparations in reverse: the PUBREL of the second
    /// waits for that of the first (counter-audit of bbe1453).
    #[test]
    fn pubrel_leaves_in_the_order_of_the_pubrecs_whatever_the_order_of_the_results() {
        let mut owner = world();
        let key = ConnectionKey {
            session: TARGET,
            generation: CONNECTION,
        };
        let mut pids = Vec::new();
        for index in 0..2 {
            queued(&mut owner, index);
            owner.pubsub.deliveries.as_mut_slice()[index as usize].qos = Qos::ExactlyOnce;
            let claim = apply(&mut owner, Input::WorkAvailable { session: TARGET });
            let (id, Effect::Persist { command }) = &claim[0] else {
                panic!("claim")
            };
            let publish = known(&mut owner, *id, command);
            let Effect::EmitPublish { packet_id, .. } = publish[0].1 else {
                panic!("publish")
            };
            pids.push(packet_id);
        }
        let mut preparations = Vec::new();
        for pid in &pids {
            let target = owner.qos2_ack_target(key, *pid, Qos2Ack::Pubrec);
            assert!(target.is_some());
            let effects = apply(
                &mut owner,
                Input::ObservedQos2Ack {
                    connection: key,
                    kind: Qos2Ack::Pubrec,
                    packet_id: *pid,
                    reason: 0,
                    target,
                },
            );
            let (id, Effect::Persist { command }) = &effects[0] else {
                panic!("prepare")
            };
            assert!(matches!(command.recipe.get(), Recipe::PreparePubrel(_)));
            preparations.push((*id, command.clone()));
        }
        let pubrels = |effects: &[(EffectId, Effect)]| -> Vec<u16> {
            effects
                .iter()
                .filter_map(|(_, effect)| match effect {
                    Effect::EmitFrame {
                        frame: OutboundFrame::Pubrel { packet_id },
                        ..
                    } => Some(packet_id.get()),
                    _ => None,
                })
                .collect()
        };
        // The preparation of the second PUBREL is known first.
        let (id, command) = &preparations[1];
        let mut seen = pubrels(&known(&mut owner, *id, command));
        seen.extend(pubrels(&apply(
            &mut owner,
            Input::WorkAvailable { session: TARGET },
        )));
        assert_eq!(
            seen,
            Vec::<u16>::new(),
            "the second PUBREL left before the first"
        );
        // Then that of the first: both leave, in the order of the PUBRECs.
        let (id, command) = &preparations[0];
        seen.extend(pubrels(&known(&mut owner, *id, command)));
        for _ in 0..2 {
            seen.extend(pubrels(&apply(
                &mut owner,
                Input::WorkAvailable { session: TARGET },
            )));
        }
        assert_eq!(seen, [pids[0].get(), pids[1].get()]);
    }
}
