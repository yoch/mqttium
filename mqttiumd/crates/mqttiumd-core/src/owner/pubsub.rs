//! Hierarchical subscriptions and outgoing QoS 1/2 under the same owner authority.
pub(super) use super::connections::Connection;
use super::connections::{Connections, Read};
pub(super) use super::deliveries::Delivery;
use super::deliveries::{Deliveries, Phase};
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
use alloc::collections::{BTreeSet, VecDeque};

// Isolated transition mutants. Production builds retain these false values.
const MUTANT_EARLY_PI_RELEASE: bool = false;
const MUTANT_IGNORE_OCCUPIED_PI: bool = false;
const MUTANT_STALE_OUTGOING_FRAME_RESULT: bool = false;

#[derive(Debug)]
pub(super) struct Control {
    pub(super) session: SessionId,
    pub(super) session_revision: Revision,
    pub(super) connection: ConnectionKey,
    pub(super) packet_id: InboundPacketId,
    pub(super) packet: InboundPacket,
    pub(super) results: FilterResults,
    pub(super) changes: BoundedVec<GrantedSubscriptionChange>,
    pub(super) in_flight: InFlight,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub(super) struct RetainedFlight {
    publication: PublicationId,
    origin: VolatilePublicationId,
    in_flight: InFlight,
    retired: bool,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub(super) struct Volatile {
    pub(super) origin: Qos0Origin,
    pub(super) target: SessionId,
    pub(super) connection: Option<ConnectionKey>,
    pub(super) topic: SharedBytes,
    pub(super) payload: SharedBytes,
    pub(super) expiry: Option<alloc::boxed::Box<MessageExpiry>>,
    pub(super) properties: Option<alloc::sync::Arc<crate::contract::properties::PublishProperties>>,
    pub(super) frame: Option<EffectId>,
}

impl Volatile {
    fn expiry(&self) -> Option<MessageExpiry> {
        self.expiry.as_deref().copied()
    }
}

/// Volatile QoS 0 frames the owner lets one connection have emitted and
/// not yet written, failed or refused (record 0063). The output queue of a
/// connection is bounded and drops a QoS 0 frame it cannot hold: the owner
/// emits a window and the next frames as their writes complete. The
/// product's queue has 256 places; the window leaves thirty-two of them to
/// acknowledgements and to QoS 1 and 2 publications. It is several times
/// what one publisher adds in a turn of the runtime, so a connection that
/// fell behind catches up.
pub const QOS0_WINDOW: usize = 224;

/// Product cap on the outgoing QoS 1/2 PUBLISH window negotiated with each peer.
pub const MAX_OUTGOING_WINDOW: u16 = 256;

/// Volatile QoS 0 messages kept waiting for the window of one connection.
/// Past it the newest is dropped with `OutputSaturated`.
pub(super) const QOS0_PARKED: usize = 1_024;

/// Consumed QoS 0 deliveries one connection may have being written. Each
/// is consumed in the store before its frame is emitted, so its frame must
/// find a place in the output queue.
pub const QOS0_CONSUMED_WINDOW: usize = 16;

/// The volatile deliveries of one connection generation: how many are in
/// flight, and those that wait for the window, oldest first.
#[derive(Debug, Default)]
struct Lane {
    window: usize,
    parked: VecDeque<Volatile>,
    /// Whether the lane is in `VolatileQueue::ready`.
    ready: bool,
}

#[derive(Debug)]
pub(super) struct VolatileQueue {
    capacity: usize,
    pending: VecDeque<Volatile>,
    inflight: ScopedFlatMap<EffectId, Volatile>,
    lanes: FlatMap<ConnectionGeneration, Lane>,
    /// The lanes whose oldest waiting delivery can leave or be dropped, in
    /// the order they became so: each is served one delivery in its turn.
    ready: VecDeque<ConnectionGeneration>,
    parked_len: usize,
    parked_bytes: u64,
    payload_bytes: u64,
    expiring_count: usize,
}

impl VolatileQueue {
    fn new(bound: NonZeroU32) -> Result<Self, AllocationFailed> {
        let capacity = usize::try_from(bound.get()).map_err(|_| AllocationFailed)?;
        Ok(Self {
            capacity,
            pending: VecDeque::new(),
            inflight: ScopedFlatMap::new(),
            lanes: FlatMap::new(),
            ready: VecDeque::new(),
            parked_len: 0,
            parked_bytes: 0,
            payload_bytes: 0,
            expiring_count: 0,
        })
    }

    pub(super) fn capacity(&self) -> usize {
        self.capacity
    }

    pub(super) fn len(&self) -> usize {
        self.pending.len() + self.inflight.len() + self.parked_len
    }

    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(super) fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }

    pub(super) fn push_pending(&mut self, item: Volatile) -> Result<(), CoreFault> {
        if self.len() >= self.capacity {
            return Err(CoreFault::StalePlan);
        }
        self.payload_bytes = self
            .payload_bytes
            .checked_add(item.payload.len() as u64)
            .ok_or(CoreFault::StalePlan)?;
        if item.expiry.is_some() {
            self.expiring_count += 1;
        }
        self.pending.push_back(item);
        Ok(())
    }

    fn front_pending(&self) -> Option<&Volatile> {
        self.pending.front()
    }

    fn has_more_pending(&self) -> bool {
        self.pending.len() > 1
    }

    fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Frames of `generation` in flight.
    fn window(&self, generation: ConnectionGeneration) -> usize {
        self.lanes.get(&generation).map_or(0, |lane| lane.window)
    }

    /// Deliveries of `generation` that wait for its window.
    fn parked(&self, generation: ConnectionGeneration) -> usize {
        self.lanes
            .get(&generation)
            .map_or(0, |lane| lane.parked.len())
    }

    /// Whether one more delivery of `bytes` may wait for a window: within
    /// what one connection may keep waiting, and within half of the queue
    /// and of its payload, so that connections that wait never take the
    /// place of the publications of the others.
    fn may_park(&self, generation: ConnectionGeneration, bytes: usize, payload_limit: u64) -> bool {
        self.parked(generation) < QOS0_PARKED
            && self.parked_len < self.capacity / 2
            && self
                .parked_bytes
                .checked_add(bytes as u64)
                .is_some_and(|total| total <= payload_limit / 2)
    }

    /// The lane to serve next and its oldest waiting delivery.
    fn front_ready(&self) -> Option<(ConnectionGeneration, &Volatile)> {
        let generation = *self.ready.front()?;
        let item = self.lanes.get(&generation)?.parked.front()?;
        Some((generation, item))
    }

    fn other_ready(&self) -> bool {
        self.ready.len() > 1
    }

    fn generation_of(item: &Volatile) -> Result<ConnectionGeneration, CoreFault> {
        item.connection
            .map(|key| key.generation)
            .ok_or(CoreFault::StalePlan)
    }

    fn forget(&mut self, item: &Volatile) {
        self.payload_bytes = self.payload_bytes.saturating_sub(item.payload.len() as u64);
        if item.expiry.is_some() {
            self.expiring_count = self.expiring_count.saturating_sub(1);
        }
    }

    fn lane_done(&mut self, generation: ConnectionGeneration) {
        if self
            .lanes
            .get(&generation)
            .is_some_and(|lane| lane.window == 0 && lane.parked.is_empty() && !lane.ready)
        {
            self.lanes.remove(&generation);
        }
    }

    fn promote_front_to_inflight(&mut self, effect: EffectId) -> Result<(), CoreFault> {
        let mut item = self.pending.pop_front().ok_or(CoreFault::StalePlan)?;
        let generation = Self::generation_of(&item)?;
        item.frame = Some(effect);
        self.inflight.insert(effect, item);
        self.lanes.get_or_insert_default(generation).window += 1;
        Ok(())
    }

    fn drop_front_pending(&mut self) -> Result<(), CoreFault> {
        let item = self.pending.pop_front().ok_or(CoreFault::StalePlan)?;
        self.forget(&item);
        Ok(())
    }

    /// The oldest pending delivery waits behind the window of its
    /// connection.
    fn park_front_pending(&mut self) -> Result<(), CoreFault> {
        let item = self.pending.pop_front().ok_or(CoreFault::StalePlan)?;
        let generation = Self::generation_of(&item)?;
        self.parked_len += 1;
        self.parked_bytes = self
            .parked_bytes
            .checked_add(item.payload.len() as u64)
            .ok_or(CoreFault::StalePlan)?;
        self.lanes
            .get_or_insert_default(generation)
            .parked
            .push_back(item);
        Ok(())
    }

    /// The oldest waiting delivery of the lane served next leaves it: with
    /// `effect` it is in flight, without it it was dropped. `stays` keeps
    /// the lane among those to serve.
    fn serve_ready(&mut self, effect: Option<EffectId>, stays: bool) -> Result<(), CoreFault> {
        let generation = self.ready.pop_front().ok_or(CoreFault::StalePlan)?;
        let lane = self
            .lanes
            .get_mut(&generation)
            .ok_or(CoreFault::StalePlan)?;
        let mut item = lane.parked.pop_front().ok_or(CoreFault::StalePlan)?;
        lane.ready = stays;
        if effect.is_some() {
            lane.window += 1;
        }
        self.parked_len = self.parked_len.saturating_sub(1);
        self.parked_bytes = self.parked_bytes.saturating_sub(item.payload.len() as u64);
        if stays {
            self.ready.push_back(generation);
        }
        match effect {
            Some(effect) => {
                item.frame = Some(effect);
                self.inflight.insert(effect, item);
            }
            None => self.forget(&item),
        }
        self.lane_done(generation);
        Ok(())
    }

    fn get_inflight(&self, effect: EffectId) -> Option<&Volatile> {
        self.inflight.get(&effect)
    }

    /// Whether the end of the frame `effect` lets a waiting delivery of its
    /// connection be served: the lane has one and is not already among
    /// those to serve.
    fn wakes(&self, effect: EffectId) -> bool {
        self.inflight
            .get(&effect)
            .and_then(|item| item.connection)
            .and_then(|key| self.lanes.get(&key.generation))
            .is_some_and(|lane| !lane.parked.is_empty() && !lane.ready)
    }

    fn remove_inflight(&mut self, effect: EffectId) -> Result<(), CoreFault> {
        let item = self.inflight.remove(&effect).ok_or(CoreFault::StalePlan)?;
        self.forget(&item);
        let generation = Self::generation_of(&item)?;
        if let Some(lane) = self.lanes.get_mut(&generation) {
            lane.window = lane.window.saturating_sub(1);
            if !lane.parked.is_empty() && !lane.ready {
                lane.ready = true;
                self.ready.push_back(generation);
            }
        }
        self.lane_done(generation);
        Ok(())
    }

    fn accepts_message_time(&self, now: MessageTime) -> bool {
        if self.expiring_count == 0 {
            return true;
        }
        self.pending
            .iter()
            .chain(self.inflight.values())
            .chain(self.lanes.values().flat_map(|lane| lane.parked.iter()))
            .filter_map(Volatile::expiry)
            .all(|expiry| expiry.state(now).is_ok())
    }
}

#[derive(Debug)]
pub(super) struct State {
    pub(super) connections: Connections,
    pub(super) deliveries: Deliveries,
    pub(super) non_inline_by_session: FlatMap<SessionId, u32>,
    pub(super) pending_non_inline_pubs: BTreeSet<PublicationId>,
    pub(super) control: Option<Control>,
    pub(super) retained: Option<RetainedFlight>,
    payload_limit: PayloadBytes,
    pub(super) volatile: VolatileQueue,
    pub(super) continuation: Option<ContinuationId>,
    pub(super) continuations_started: bool,
    next_pubrec_rank: u64,
}

#[derive(Debug)]
pub(super) enum StartFollowUp {
    Finish(InFlight),
    PreparePubrel { in_flight: InFlight, rank: u64 },
}

#[derive(Debug)]
pub(super) enum Change {
    RetainedSubmit {
        connection: usize,
        flight: alloc::boxed::Box<RetainedFlight>,
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
    Qos0Delegated {
        connection: usize,
        ingress_order: u64,
        batch: alloc::boxed::Box<super::DelegatedQos0Batch>,
    },
    Qos0Send {
        effect: EffectId,
        continuation: Option<ContinuationId>,
    },
    Qos0Drop {
        continuation: Option<ContinuationId>,
    },
    /// The oldest pending delivery waits for the window of its connection.
    Qos0Park {
        continuation: Option<ContinuationId>,
    },
    /// The lane served next gives its oldest waiting delivery: emitted as
    /// `effect`, or dropped.
    Qos0Serve {
        effect: Option<EffectId>,
        stays: bool,
        continuation: Option<ContinuationId>,
    },
    Qos0Done {
        effect: EffectId,
        continuation: Option<ContinuationId>,
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
    Control(alloc::boxed::Box<Control>),
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
        rank: u64,
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
        rank: Option<u64>,
    },
    Start {
        slot: usize,
        in_flight: InFlight,
    },
    Started {
        slot: usize,
        finished: Finished,
        follow_up: Option<alloc::boxed::Box<StartFollowUp>>,
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
    pub(super) fn new(config: OwnerConfig) -> Result<Self, AllocationFailed> {
        Ok(Self {
            connections: Connections::new(config.max_sessions)?,
            deliveries: Deliveries::new(config.max_exchanges)?,
            non_inline_by_session: FlatMap::new(),
            pending_non_inline_pubs: BTreeSet::new(),
            control: None,
            retained: None,
            payload_limit: config.payload_bytes,
            volatile: VolatileQueue::new(config.max_exchanges)?,
            continuation: None,
            continuations_started: false,
            next_pubrec_rank: 1,
        })
    }

    pub(super) fn payload_limit(&self) -> u64 {
        self.payload_limit.get()
    }

    pub(super) fn work_bound(&self, input: &Input) -> u64 {
        if let Input::MessagesExpired { deliveries } = input {
            let count = capacity_of(deliveries.len());
            return count.saturating_mul(16).saturating_add(16);
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
        let slots = capacity_of(self.deliveries.capacity()).min(32);
        let connection_work = capacity_of(self.connections.capacity()).min(16);
        if let Input::WorkPage { items, .. } = input {
            let count = capacity_of(items.len());
            return connection_work
                .saturating_add(count.saturating_mul(16))
                .saturating_add(slots);
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
        connection_work
            .saturating_add(slots)
            .saturating_add(filters.saturating_mul(8))
    }

    pub(super) fn connection(&self, generation: ConnectionGeneration) -> Option<usize> {
        self.connections.index_of(generation)
    }

    pub(super) fn delivery(&self, id: DeliveryId) -> Option<usize> {
        self.deliveries.slot_of(id)
    }

    fn active(&self, generation: ConnectionGeneration) -> usize {
        self.deliveries.active(generation)
    }

    fn qos0_writing(&self, session: SessionId, generation: ConnectionGeneration) -> usize {
        self.deliveries.qos0_writing(session, generation)
    }

    fn next_send(
        &self,
        session: SessionId,
        generation: ConnectionGeneration,
        now: Option<MessageTime>,
        room: bool,
    ) -> Option<(usize, &Delivery)> {
        self.deliveries.next_send(session, generation, now, room)
    }

    fn next_pipelined_claim(
        &self,
        session: SessionId,
        generation: ConnectionGeneration,
        now: Option<MessageTime>,
        room: bool,
    ) -> (Option<(usize, &Delivery)>, bool) {
        self.deliveries
            .next_pipelined_claim(session, generation, now, room)
    }

    fn free_packet_id(&self, session: SessionId, maximum: u16) -> Option<OutboundPacketId> {
        self.deliveries.free_packet_id(
            session,
            maximum,
            MUTANT_EARLY_PI_RELEASE,
            MUTANT_IGNORE_OCCUPIED_PI,
        )
    }

    fn loaded_bytes(&self) -> u64 {
        self.deliveries.loaded_bytes()
    }

    pub(super) fn accepts_message_time(&self, now: MessageTime) -> bool {
        self.deliveries.accepts_message_time(now) && self.volatile.accepts_message_time(now)
    }

    pub(super) fn evict_expired_cache(&mut self, now: MessageTime) {
        self.deliveries.evict_expired(now);
    }

    pub(super) fn has_control_work(&self, now: Option<MessageTime>) -> bool {
        self.deliveries.has_control_work(now)
    }

    pub(super) fn close_work(&self) -> u64 {
        if self.connections.is_empty() {
            0
        } else {
            capacity_of(self.connections.capacity())
                .min(16)
                .saturating_add(capacity_of(self.deliveries.capacity()).min(32))
        }
    }

    pub(super) fn drop_session_cache(&mut self, session: SessionId) {
        self.deliveries.drop_session_cache(session);
    }

    pub(super) fn retired_flights(&self) -> usize {
        self.deliveries.retired_flights()
            + usize::from(self.retained.as_ref().is_some_and(|flight| flight.retired))
    }

    pub(super) fn retire_session(&mut self, session: SessionId) {
        if let Some(flight) = self
            .retained
            .as_mut()
            .filter(|f| f.origin.source.session == session)
        {
            flight.retired = true;
        }
        self.deliveries.retire_session(session);
    }

    pub(super) fn close(&mut self, generation: ConnectionGeneration) {
        self.connections.remove_generation(generation);
        self.deliveries.close_generation(generation);
    }

    pub(super) fn deadline_is_live(&self, token: DeadlineToken) -> bool {
        self.retained
            .as_ref()
            .is_some_and(|f| f.in_flight.deadline == token)
            || self
                .control
                .as_ref()
                .is_some_and(|c| c.in_flight.deadline == token)
            || self.deliveries.deadline_is_live(token)
    }

    pub(super) fn known(&self, operation: OperationId) -> Option<&Finished> {
        self.deliveries.known_claim_result(operation)
    }

    pub(super) fn control_pending(&self) -> bool {
        self.control.is_some()
    }
}

/// What the owner may do now with a claimed delivery.
pub(super) enum Emission {
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
    props_wire_len: usize,
) -> Option<usize> {
    let prop_bytes = (usize::from(expiry) * 5).checked_add(props_wire_len)?;
    let prop_section =
        crate::contract::properties::varint_len(prop_bytes).checked_add(prop_bytes)?;
    let body = topic
        .checked_add(payload)?
        .checked_add(2 + usize::from(packet_identifier) * 2)?
        .checked_add(prop_section)?;
    let remaining = crate::contract::properties::varint_len(body);
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
            // Constant-time identity-range reservation (ADR 0051); stays above
            // the unit test's 200-unit minimum work guard while fitting well
            // inside runtime TurnGrant.
            256
        } else {
            0
        }
    }

    /// Whether a bounded work wake-up can advance already known online work.
    /// Empty scans are deliberately excluded; external notifications and the
    /// periodic fallback supply the authority to start a new discovery scan.
    #[must_use]
    pub fn has_ready_work(&self, session: SessionId) -> bool {
        self.ready_work_generation(session).is_some()
    }

    /// Returns the active connection generation of `session` when a bounded
    /// work wake-up can advance already known online work, avoiding duplicate
    /// session-binding lookups in the runtime.
    #[must_use]
    pub fn ready_work_generation(&self, session: SessionId) -> Option<ConnectionGeneration> {
        if self.recovery.is_some() {
            return None;
        }
        let generation = self.connection_of(session);
        let sd = self.pubsub.deliveries.session(session);
        if self.receipts.control_available()
            && !self.pubsub.deliveries.control_candidates().is_empty()
            && sd.is_some_and(|sd| {
                sd.all.iter().any(|idx| {
                    self.pubsub
                        .deliveries
                        .row(idx)
                        .control_ready(self.message_now)
                })
            })
        {
            return generation;
        }
        let generation = generation?;
        let index = self.pubsub.connection(generation)?;
        let settings = self.pubsub.connections.as_slice()[index];
        let room = self.pubsub.active(generation) < usize::from(settings.receive_maximum);
        let needs_scan = settings.rescan_needed || self.pubsub.connections.has_after(generation);
        if let Some(sd) = sd
            && !sd.all.is_empty()
        {
            let sent = sd.sent_on(generation, self.pubsub.deliveries.slab());
            let busy = usize::from(sd.busy);
            let inline_pending = sd.inline_pending.len();
            // O(1) fast path for steady-state inline-claimed QoS 1: when every
            // delivery is already sent, busy, or awaiting inline admission and
            // no rescan/page cursor is pending, nothing can be emitted or claimed.
            if sent + busy + inline_pending == sd.all.len() {
                if !needs_scan {
                    return None;
                }
                if inline_pending == 0 {
                    return (room && !self.read_held(session, generation)).then_some(generation);
                }
            }
            if let Some((_, delivery)) =
                self.pubsub
                    .next_send(session, generation, self.message_now, room)
            {
                if delivery.phase == Phase::Queued
                    && delivery.qos == Qos::AtMostOnce
                    && self.pubsub.qos0_writing(session, generation) >= QOS0_CONSUMED_WINDOW
                {
                    // Held until a write of the connection ends (record 0063).
                    return None;
                }
                if delivery.ready_to_send(generation, room) {
                    return Some(generation);
                }
                if matches!(
                    delivery.phase,
                    Phase::ClaimPending(_)
                        | Phase::ConsumePending(_, _)
                        | Phase::InlineClaimPending(_)
                ) {
                    if sd.queued == 0 && !needs_scan {
                        return None;
                    }
                    let (candidate, can_prefetch) = self.pubsub.next_pipelined_claim(
                        session,
                        generation,
                        self.message_now,
                        room,
                    );
                    if candidate.is_some() {
                        return Some(generation);
                    }
                    if !can_prefetch {
                        return None;
                    }
                }
            }
        }
        (room && !self.read_held(session, generation) && needs_scan).then_some(generation)
    }

    /// Whether no read of `session`'s work may start now: one is in flight,
    /// or the delivery shard that owns the session fences it
    /// (`Owner::shard_fence_scans`). A held session waits as during a read,
    /// and is not ready work.
    fn read_held(&self, session: SessionId, generation: ConnectionGeneration) -> bool {
        self.pubsub.connections.is_loading(generation)
            || (!self.scan_fences.is_empty() && self.scan_fences.contains(&session))
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
            properties,
        } = packet
        else {
            return Self::plan_close(generation, Some(session), CloseReason::ProtocolError, draft);
        };
        if let Some(props) = properties
            && (props.is_empty()
                || props.check().is_err()
                || props.check_payload(payload.as_slice()).is_err())
        {
            return Err(IMPOSSIBLE);
        }
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
        let selected = match self.route_index.select(
            self.routes.as_slice(),
            self.sessions.as_slice(),
            &self.session_by_id,
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
        }
        let mut rap_iter = selected.iter();
        let retain_targets =
            crate::contract::retained::RetainMask::from_predicate(selected.len(), |_| {
                rap_iter.next().is_some_and(|target| target.rap)
            })
            .map_err(|_| IMPOSSIBLE)?;
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
        let recipe = if let Some(props) = properties {
            Recipe::AnnotatedPublication(crate::contract::command::AnnotatedPublication {
                inner: alloc::boxed::Box::new(recipe),
                properties: props.clone(),
            })
        } else {
            recipe
        };
        let in_flight = self.plan_submission(operation, recipe, Some(payload.clone()), draft)?;
        Ok(changed(Change::RetainedSubmit {
            connection,
            flight: alloc::boxed::Box::new(RetainedFlight {
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
            }),
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
            .as_ref()
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
            finished: Finished::new(operation, flight.in_flight.answers.clone(), phase, facts),
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
            properties,
        } = packet
        else {
            return Self::plan_close(generation, Some(session), CloseReason::ProtocolError, draft);
        };
        if let Some(props) = properties
            && (props.is_empty()
                || props.check().is_err()
                || props.check_payload(payload.as_slice()).is_err())
        {
            return Err(IMPOSSIBLE);
        }
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
        let selected = match self.route_index.select(
            self.routes.as_slice(),
            self.sessions.as_slice(),
            &self.session_by_id,
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
        if self.delegated_deliveries {
            if count == 0 || selected.is_none() {
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
            let mut targets = alloc::vec::Vec::with_capacity(count);
            for target in selected.ok_or(IMPOSSIBLE)?.iter() {
                let target_conn =
                    self.connection_of(target.session)
                        .map(|generation| ConnectionKey {
                            session: target.session,
                            generation,
                        });
                targets.push(super::DelegatedQos0Target {
                    target: target.session,
                    connection: target_conn,
                });
            }
            return Ok(changed(Change::Qos0Delegated {
                connection,
                ingress_order,
                batch: alloc::boxed::Box::new(super::DelegatedQos0Batch {
                    origin,
                    topic: topic.clone(),
                    payload: payload.clone(),
                    expiry: *expiry,
                    properties: properties.clone(),
                    targets,
                }),
            }));
        }
        let used = self.pubsub.volatile.payload_bytes();
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
                    expiry: expiry.map(alloc::boxed::Box::new),
                    properties: properties.clone(),
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
        properties: Option<&crate::contract::properties::PublishProperties>,
    ) -> bool {
        let Some(settings) = self
            .pubsub
            .connection(connection.generation)
            .map(|i| self.pubsub.connections.as_slice()[i])
        else {
            return false;
        };
        publish_packet_size(
            topic.len(),
            payload.len(),
            false,
            expiry.is_some(),
            properties.map_or(0, crate::contract::properties::PublishProperties::wire_len),
        )
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
        // A connection whose window has room is served first, one
        // delivery in its turn: what waited leaves before what came after.
        if let Some((generation, v)) = self.pubsub.volatile.front_ready() {
            let closed = self.connection_of(v.target) != Some(generation);
            let window = self.pubsub.volatile.window(generation);
            if !closed && window >= QOS0_WINDOW {
                return Err(IMPOSSIBLE);
            }
            let reason = self.qos0_drop_reason(v)?;
            let window = window + usize::from(reason.is_none());
            let stays =
                self.pubsub.volatile.parked(generation) > 1 && (closed || window < QOS0_WINDOW);
            let next = if stays
                || self.pubsub.volatile.other_ready()
                || self.pubsub.volatile.has_pending()
            {
                Some(self.qos0_continuation(draft)?)
            } else {
                None
            };
            let effect = match reason {
                Some(reason) => {
                    draft.emit(Effect::Qos0Dropped {
                        origin: v.origin,
                        target: Some(v.target),
                        reason,
                    })?;
                    None
                }
                None => Some(self.emit_volatile(v, draft)?),
            };
            return Ok(changed(Change::Qos0Serve {
                effect,
                stays,
                continuation: next,
            }));
        }
        let Some(v) = self.pubsub.volatile.front_pending() else {
            return Ok(changed(Change::Qos0Idle));
        };
        let next = if self.pubsub.volatile.has_more_pending() {
            Some(self.qos0_continuation(draft)?)
        } else {
            None
        };
        if let Some(reason) = self.qos0_drop_reason(v)? {
            draft.emit(Effect::Qos0Dropped {
                origin: v.origin,
                target: Some(v.target),
                reason,
            })?;
            return Ok(changed(Change::Qos0Drop { continuation: next }));
        }
        let generation = v.connection.ok_or(IMPOSSIBLE)?.generation;
        // The window of the connection is full, or earlier deliveries wait
        // for it: this one waits behind them, in its order. Past what a
        // connection may keep waiting, it is dropped.
        if self.pubsub.volatile.parked(generation) > 0
            || self.pubsub.volatile.window(generation) >= QOS0_WINDOW
        {
            if self.pubsub.volatile.may_park(
                generation,
                v.payload.len(),
                self.pubsub.payload_limit.get(),
            ) {
                return Ok(changed(Change::Qos0Park { continuation: next }));
            }
            draft.emit(Effect::Qos0Dropped {
                origin: v.origin,
                target: Some(v.target),
                reason: Qos0DropReason::OutputSaturated,
            })?;
            return Ok(changed(Change::Qos0Drop { continuation: next }));
        }
        let effect = self.emit_volatile(v, draft)?;
        Ok(changed(Change::Qos0Send {
            effect,
            continuation: next,
        }))
    }

    /// Why a volatile delivery cannot be sent now, if it cannot.
    fn qos0_drop_reason(&self, v: &Volatile) -> Result<Option<Qos0DropReason>, Stop> {
        let expiry = v.expiry();
        let reason = if let Some(expiry) = expiry {
            match self.message_now.map(|now| expiry.state(now)) {
                Some(Ok(ExpiryState::Fresh { .. })) => None,
                Some(Ok(ExpiryState::Expired)) => Some(Qos0DropReason::MessageExpired),
                Some(Err(_)) => return Err(IMPOSSIBLE),
                _ => Some(Qos0DropReason::TemporalUncertain),
            }
        } else {
            None
        };
        Ok(reason.or(match v.connection {
            None => Some(Qos0DropReason::DestinationAbsent),
            Some(key) if self.connection_of(v.target) != Some(key.generation) => {
                Some(Qos0DropReason::ConnectionClosed)
            }
            Some(key)
                if !self.qos0_fits(key, &v.topic, &v.payload, expiry, v.properties.as_deref()) =>
            {
                Some(Qos0DropReason::PacketTooLarge)
            }
            _ => None,
        }))
    }

    fn emit_volatile(&self, v: &Volatile, draft: &mut Draft) -> Result<EffectId, Stop> {
        let connection = v.connection.ok_or(IMPOSSIBLE)?;
        draft.emit(Effect::EmitQos0 {
            connection,
            origin: v.origin,
            cause: DeliveryCause::LiveExact,
            retain: false,
            topic: v.topic.clone(),
            payload: v.payload.clone(),
            expiry: v.expiry.clone(),
            properties: v.properties.clone(),
        })
    }

    fn plan_qos0_terminal(
        &self,
        effect: EffectId,
        connection: ConnectionKey,
        reason: Option<Qos0DropReason>,
        draft: &mut Draft,
    ) -> Result<Option<super::Change>, Stop> {
        if !self.pubsub.volatile.is_empty()
            && let Some(v) = self
                .pubsub
                .volatile
                .get_inflight(effect)
                .filter(|v| v.connection == Some(connection))
        {
            if let Some(reason) = reason {
                draft.emit(Effect::Qos0Dropped {
                    origin: v.origin,
                    target: Some(v.target),
                    reason,
                })?;
            }
            // A place of the window returns: a delivery that waited for it
            // is served, by the continuation if one runs, else by a new one.
            let continuation =
                if self.pubsub.volatile.wakes(effect) && self.pubsub.continuation.is_none() {
                    Some(self.qos0_continuation(draft)?)
                } else {
                    None
                };
            return Ok(Some(changed(Change::Qos0Done {
                effect,
                continuation,
            })));
        }
        if let Some(slot) = self.pubsub.deliveries.slot_by_frame(effect) {
            let d = self.pubsub.deliveries.row(slot);
            if let Phase::Qos0Writing(finished) = &d.phase
                && d.frame == Some((effect, connection))
            {
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
                return Ok(Some(changed(Change::Finished {
                    slot,
                    finished: (**finished).clone(),
                })));
            }
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

    pub(super) fn plan_qos0_or_pubsub_frame(
        &self,
        effect: EffectId,
        connection: ConnectionKey,
        progress: FrameProgress,
        draft: &mut Draft,
    ) -> Result<Option<super::Change>, Stop> {
        let qos0_terminal = match progress {
            FrameProgress::NotStarted { reason } => Some(Some(match reason {
                MessageExpiryBlock::Expired => Qos0DropReason::MessageExpired,
                MessageExpiryBlock::TemporalUncertain => Qos0DropReason::TemporalUncertain,
            })),
            FrameProgress::Failed => Some(Some(Qos0DropReason::WriteFailed)),
            FrameProgress::Written { confirmed, total } if total > 0 && confirmed == total => {
                Some(None)
            }
            _ => None,
        };
        if let Some(reason) = qos0_terminal
            && !self.pubsub.volatile.is_empty()
            && let Some(v) = self
                .pubsub
                .volatile
                .get_inflight(effect)
                .filter(|v| v.connection == Some(connection))
        {
            if let Some(reason) = reason {
                draft.emit(Effect::Qos0Dropped {
                    origin: v.origin,
                    target: Some(v.target),
                    reason,
                })?;
            }
            let continuation =
                if self.pubsub.volatile.wakes(effect) && self.pubsub.continuation.is_none() {
                    Some(self.qos0_continuation(draft)?)
                } else {
                    None
                };
            return Ok(Some(changed(Change::Qos0Done {
                effect,
                continuation,
            })));
        }
        let found = if MUTANT_STALE_OUTGOING_FRAME_RESULT {
            self.pubsub
                .deliveries
                .iter()
                .find(|(_, d)| !d.retired && d.frame.is_some_and(|(_, sent)| sent == connection))
        } else {
            self.pubsub
                .deliveries
                .slot_by_frame(effect)
                .and_then(|slot| {
                    let d = self.pubsub.deliveries.row(slot);
                    (d.frame == Some((effect, connection))).then_some((slot, d))
                })
        };
        let Some((slot, d)) = found else {
            return Ok(None);
        };
        if let Some(reason) = qos0_terminal
            && let Phase::Qos0Writing(finished) = &d.phase
        {
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
            return Ok(Some(changed(Change::Finished {
                slot,
                finished: (**finished).clone(),
            })));
        }
        if d.retired {
            return Ok(None);
        }
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
                && d.expiry().is_some_and(|expiry| {
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

    pub(super) fn plan_settings(
        &self,
        connection: ConnectionGeneration,
        receive_maximum: u16,
        maximum_packet_size: u32,
        draft: &mut Draft,
    ) -> Result<super::Change, Stop> {
        if self.is_closed(connection) {
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
            last_qos0_ingress: 0,
            maximum_packet_size,
            receive_maximum: receive_maximum.min(MAX_OUTGOING_WINDOW),
            rescan_needed: false,
            caught_up: false,
            dirty_during_load: false,
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

    /// Fused check used by runtime outgoing-binding pruning: verifies in a
    /// single lookup whether `(connection, packet_id)` still resolves to
    /// `target` for either QoS 1 PUBACK or QoS 2 PUBREC.
    #[must_use]
    pub fn outgoing_binding_is_live(
        &self,
        connection: ConnectionKey,
        packet_id: OutboundPacketId,
        target: PubackTarget,
    ) -> bool {
        if self.connection_of(connection.session) != Some(connection.generation) {
            return false;
        }
        if !MUTANT_IGNORE_OCCUPIED_PI && !MUTANT_EARLY_PI_RELEASE {
            let Some(idx) = self
                .pubsub
                .deliveries
                .slot_by_target_pi(connection.session, packet_id)
            else {
                return false;
            };
            let delivery = self.pubsub.deliveries.row(idx);
            let Some(claim) = delivery.claim else {
                return false;
            };
            let Some((frame, sent)) = delivery.frame else {
                return false;
            };
            return sent == connection
                && claim.packet_id == packet_id
                && delivery.id == target.delivery
                && claim.operation == target.claim
                && frame == target.frame
                && matches!(delivery.qos, Qos::AtLeastOnce | Qos::ExactlyOnce)
                && matches!(
                    delivery.phase,
                    Phase::AwaitPuback | Phase::StartPending(_) | Phase::StartRetry
                );
        }
        self.puback_target(connection, packet_id) == Some(target)
            || self.qos2_ack_target(connection, packet_id, Qos2Ack::Pubrec) == Some(target)
    }

    /// Whether the owner currently holds any expiring deliveries or volatile
    /// publications requiring up-to-date message clock observations.
    #[must_use]
    pub fn has_expiring_messages(&self) -> bool {
        !!self.pubsub.deliveries.has_expiring() || self.pubsub.volatile.expiring_count > 0
    }

    fn outgoing_ack_slot(
        &self,
        connection: ConnectionKey,
        packet_id: OutboundPacketId,
        eligible: impl Fn(&Delivery, DeliveryClaim) -> bool,
    ) -> Option<(usize, PubackTarget)> {
        if self.connection_of(connection.session) != Some(connection.generation) {
            return None;
        }
        if !MUTANT_IGNORE_OCCUPIED_PI && !MUTANT_EARLY_PI_RELEASE {
            let idx = self
                .pubsub
                .deliveries
                .slot_by_target_pi(connection.session, packet_id)?;
            let delivery = self.pubsub.deliveries.row(idx);
            let claim = delivery.claim?;
            let (frame, sent) = delivery.frame?;
            return (eligible(delivery, claim)
                && sent == connection
                && claim.packet_id == packet_id)
                .then_some((
                    idx,
                    PubackTarget {
                        delivery: delivery.id,
                        claim: claim.operation,
                        frame,
                    },
                ));
        }
        let sd = self.pubsub.deliveries.session(connection.session)?;
        sd.all.iter().find_map(|idx| {
            let delivery = self.pubsub.deliveries.row(idx);
            let claim = delivery.claim?;
            let (frame, sent) = delivery.frame?;
            (eligible(delivery, claim) && sent == connection && claim.packet_id == packet_id)
                .then_some((
                    idx,
                    PubackTarget {
                        delivery: delivery.id,
                        claim: claim.operation,
                        frame,
                    },
                ))
        })
    }

    fn outgoing_ack_target(
        &self,
        connection: ConnectionKey,
        packet_id: OutboundPacketId,
        eligible: impl Fn(&Delivery, DeliveryClaim) -> bool,
    ) -> Option<PubackTarget> {
        self.outgoing_ack_slot(connection, packet_id, eligible)
            .map(|(_, target)| target)
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
        let delivery = self.pubsub.deliveries.row(slot);
        if kind == Qos2Ack::Pubrec {
            if delivery.claim.is_some_and(|c| c.await_pubcomp) {
                if reason < 0x80 && delivery.frame.is_some_and(|(_, sent)| sent == connection) {
                    draft.frame(connection, OutboundFrame::Pubrel { packet_id })?;
                }
                return Ok(super::Change::Nothing);
            }
            if matches!(delivery.phase, Phase::StartPending(_) | Phase::StartRetry) {
                return if delivery.early_pubrec.is_none() {
                    let rank = if reason < 0x80 {
                        Some(delivery.pubrec_rank.unwrap_or(self.pubsub.next_pubrec_rank))
                    } else {
                        None
                    };
                    Ok(changed(Change::PubrecObserved { slot, reason, rank }))
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
        let rank = delivery.pubrec_rank.unwrap_or(self.pubsub.next_pubrec_rank);
        let in_flight =
            self.plan_submission(operation, Recipe::PreparePubrel(guarded), None, draft)?;
        Ok(Change::PreparePubrel {
            slot,
            in_flight,
            rank,
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
        let Some((slot, matched)) = self.outgoing_ack_slot(connection, packet_id, |delivery, _| {
            delivery.qos == Qos::AtLeastOnce
                && matches!(
                    delivery.phase,
                    Phase::AwaitPuback | Phase::StartPending(_) | Phase::StartRetry
                )
        }) else {
            return Ok(super::Change::Nothing);
        };
        if matched != target {
            return Ok(super::Change::Nothing);
        }
        let delivery = self.pubsub.deliveries.row(slot);
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
        // Validate route capacity without cloning the entire route table.
        if self.projected_route_count(session.id, changes.as_slice()) > self.routes.capacity() {
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
        let mut control_changes = BoundedVec::with_capacity(changes.len())?;
        for c in changes.as_slice() {
            control_changes
                .try_push(c.clone())
                .map_err(|_| IMPOSSIBLE)?;
        }
        let operation = self.operation(draft)?;
        let update = UpdateSubscriptions {
            operation,
            session: session.id,
            session_revision: session.revision,
            route_revision: self.route_revision.ok_or(IMPOSSIBLE)?,
            changes,
        };
        let recipe = if (self.lifecycle_enabled || self.message_now.is_some()) && subscribing {
            let first = draft
                .counters
                .publication
                .ok_or(Stop::Exhausted(Exhaustion::IdentityCounter))?;
            let span = crate::contract::retained::MAX_REPLAYS as u64;
            let last = first
                .get()
                .checked_add(span - 1)
                .ok_or(Stop::Exhausted(Exhaustion::IdentityCounter))?;
            draft.counters.publication = last.checked_add(1).map(PublicationSeq::new);
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
        Ok(changed(Change::Control(alloc::boxed::Box::new(Control {
            session: session.id,
            session_revision: session.revision,
            connection: key,
            packet_id,
            packet: packet.clone(),
            results,
            changes: control_changes,
            in_flight,
        }))))
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
                            .route_by_key
                            .contains_key(&(session, item.filter.clone()));
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
                        && self.route_by_key.contains_key(&(session, topic.clone()));
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

    fn projected_route_count(
        &self,
        session: SessionId,
        changes: &[GrantedSubscriptionChange],
    ) -> usize {
        let mut count = self.routes.len();
        for (i, change) in changes.iter().enumerate() {
            if changes[i + 1..]
                .iter()
                .any(|later| later.topic == change.topic)
            {
                continue;
            }
            let existed = self
                .route_by_key
                .contains_key(&(session, change.topic.clone()));
            match (existed, change.granted.is_some()) {
                (true, false) => count = count.saturating_sub(1),
                (false, true) => count = count.saturating_add(1),
                _ => {}
            }
        }
        count
    }

    pub(super) fn plan_pubsub_control_work(
        &self,
        session: Option<SessionId>,
        draft: &mut Draft,
    ) -> Result<Option<super::Change>, Stop> {
        if !self.receipts.control_available()
            || self.pubsub.deliveries.control_candidates().is_empty()
        {
            return Ok(None);
        }
        let Some((slot, d)) = self
            .pubsub
            .deliveries
            .control_candidates()
            .iter()
            .map(|&slot| (slot, self.pubsub.deliveries.row(slot)))
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
        let after = self.pubsub.connections.after_of(generation);
        let loading = self.read_held(session, generation);
        let needs_scan = settings.rescan_needed || after.is_some();
        if let Some(sd) = self.pubsub.deliveries.session(session)
            && !sd.all.is_empty()
        {
            let sent = sd.sent_on(generation, self.pubsub.deliveries.slab());
            let busy = usize::from(sd.busy);
            let inline_pending = sd.inline_pending.len();
            if sent + busy + inline_pending == sd.all.len() {
                if !room || loading || (inline_pending > 0 && !needs_scan) {
                    return Ok(super::Change::Nothing);
                }
                if inline_pending == 0 {
                    let cursor =
                        after.or_else(|| self.pubsub.connections.known_through_of(generation));
                    let effect = draft.emit(Effect::LoadWork {
                        session,
                        limit: WORK_PAGE_LIMIT,
                        after: cursor,
                    })?;
                    return Ok(changed(Change::Scan {
                        connection,
                        read: Read {
                            effect,
                            session,
                            after: cursor,
                            invalidated: false,
                        },
                    }));
                }
            }
        }
        let next = self
            .pubsub
            .next_send(session, generation, self.message_now, room);
        let mut claim_target = None;
        let mut allow_prefetch = true;
        if let Some((_, d)) = next
            && matches!(
                d.phase,
                Phase::ClaimPending(_) | Phase::ConsumePending(_, _) | Phase::InlineClaimPending(_)
            )
        {
            let has_queued = self
                .pubsub
                .deliveries
                .session(session)
                .is_some_and(|sd| sd.queued > 0);
            if !has_queued && (!needs_scan || loading) {
                return Ok(super::Change::Nothing);
            }
            // A lost (Indeterminate) claim result or a pending QoS 0 consume
            // must be reconciled before a later publication can be claimed on
            // this destination. Healthy in-flight Submitted claims allow
            // pipelining further QoS 1/2 claims within Receive Maximum while
            // `plan_delivery_emit` continues to enforce strict wire order.
            let (candidate, can_prefetch) =
                self.pubsub
                    .next_pipelined_claim(session, generation, self.message_now, room);
            if let Some(pair) = candidate {
                claim_target = Some(pair);
            } else if can_prefetch && !loading && needs_scan {
                allow_prefetch = true;
            } else {
                return Ok(super::Change::Nothing);
            }
        } else {
            allow_prefetch = false;
        }
        if claim_target.is_none()
            && let Some((slot, d)) = next
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
        if claim_target.is_none()
            && let Some((slot, d)) = next
            && d.phase == Phase::Queued
            && d.qos == Qos::AtMostOnce
            && d.payload.is_some()
        {
            // The frame of a consumed delivery must find a place in the
            // output queue: the store no longer owes the message. No more
            // are consumed than the connection may have being written
            // (record 0063); the end of a write brings this work back.
            if self.pubsub.qos0_writing(session, generation) >= QOS0_CONSUMED_WINDOW {
                return Ok(super::Change::Nothing);
            }
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
        let claim_candidate = claim_target.or_else(|| {
            if room {
                next.filter(|(_, d)| d.phase == Phase::Queued && d.payload.is_some())
            } else {
                None
            }
        });
        if room && let Some((slot, d)) = claim_candidate {
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
        if room
            && !loading
            && (allow_prefetch
                || next.is_none_or(|(_, d)| {
                    !matches!(
                        d.phase,
                        Phase::ClaimPending(_)
                            | Phase::ConsumePending(_, _)
                            | Phase::InlineClaimPending(_)
                    )
                }))
        {
            let cursor = after.or_else(|| self.pubsub.connections.known_through_of(generation));
            let effect = draft.emit(Effect::LoadWork {
                session,
                limit: WORK_PAGE_LIMIT,
                after: cursor,
            })?;
            return Ok(changed(Change::Scan {
                connection,
                read: Read {
                    effect,
                    session,
                    after: cursor,
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
        let Some(connection) = self.pubsub.connections.reading(effect, session) else {
            return Ok(super::Change::Nothing);
        };
        let settings = self.pubsub.connections.as_slice()[connection];
        let read = self
            .pubsub
            .connections
            .loading_of(settings.generation)
            .ok_or(IMPOSSIBLE)?;
        if read.invalidated || self.connection_of(session) != Some(settings.generation) {
            return Ok(changed(Change::Page {
                connection,
                discard: true,
                next: None,
            }));
        }
        if items.len() > WORK_PAGE_LIMIT as usize
            || items.windows(2).any(|w| w[0].delivery >= w[1].delivery)
            || read
                .after
                .is_some_and(|after| items.first().is_some_and(|first| first.delivery <= after))
            || next.is_some_and(|id| items.last().is_none_or(|last| last.delivery != id))
        {
            return Err(IMPOSSIBLE);
        }
        let target = self
            .session_by_id
            .get(&session)
            .and_then(|&idx| self.sessions.as_slice().get(idx))
            .ok_or(IMPOSSIBLE)?;
        let mut total = self.pubsub.loaded_bytes();
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
                || item
                    .properties
                    .as_ref()
                    .is_some_and(|p| p.is_empty() || p.check().is_err())
            {
                return Err(IMPOSSIBLE);
            }
            if let Some(claim) = item.claim {
                let op_conflict = self
                    .pubsub
                    .deliveries
                    .slots_by_claim_op(claim.operation)
                    .is_some_and(|slots| {
                        slots.iter().any(|idx| {
                            let d = self.pubsub.deliveries.row(idx);
                            d.id != item.delivery
                                && (d.target == item.target
                                    || d.id.publication != item.delivery.publication)
                        })
                    });
                let pi_conflict = self
                    .pubsub
                    .deliveries
                    .session(item.target)
                    .is_some_and(|sd| {
                        sd.all.iter().any(|idx| {
                            let d = self.pubsub.deliveries.row(idx);
                            d.id != item.delivery
                                && d.claim
                                    .is_some_and(|other| other.packet_id == claim.packet_id)
                        })
                    });
                if claim.operation.incarnation() != self.incarnation
                    || claim.operation.epoch() > self.epoch
                    || op_conflict
                    || pi_conflict
                    || items[..position].iter().any(|other| {
                        other.claim.is_some_and(|other| {
                            other.operation == claim.operation || other.packet_id == claim.packet_id
                        })
                    })
                {
                    return Err(IMPOSSIBLE);
                }
            }
            let known = self
                .pubsub
                .delivery(item.delivery)
                .map(|i| self.pubsub.deliveries.row(i));
            if let Some(known) = known {
                if known.target != item.target
                    || known.revision != item.target_revision
                    || known.qos != item.qos
                    || known.expiry() != item.expiry
                    || (known.topic.is_some()
                        && (known.retain != item.retain
                            || known.cause != item.cause
                            || known.properties != item.properties))
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
    pub(super) fn plan_delivery_emit(
        &self,
        delivery: &Delivery,
        draft: &mut Draft,
    ) -> Result<Emission, Stop> {
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
        if let Some(sd) = self.pubsub.deliveries.session(delivery.target)
            && !sd.all.is_empty()
        {
            let sent = sd.sent_on(generation, self.pubsub.deliveries.slab());
            let busy = usize::from(sd.busy);
            let self_uncounted = !Deliveries::is_busy_delivery(delivery)
                && Deliveries::sent_gen(delivery) != Some((delivery.target, generation));
            let only_self_waiting = self_uncounted && sent + busy + 1 == sd.all.len();
            if !only_self_waiting {
                let inline_len = sd.inline_pending.len();
                if sent + busy + inline_len == sd.all.len() {
                    if sd.inline_pending.iter().any(|idx| {
                        let earlier = self.pubsub.deliveries.row(idx);
                        earlier.id < delivery.id
                            && earlier.waiting_to_send(generation, self.message_now)
                    }) {
                        return Ok(Emission::Held);
                    }
                } else if sd.waiting_candidates(generation).iter().any(|idx| {
                    let earlier = self.pubsub.deliveries.row(idx);
                    earlier.id < delivery.id
                        && earlier.waiting_to_send(generation, self.message_now)
                }) {
                    // This also protects a claim result delivered after takeover:
                    // restored earlier exchanges are replayed before newer ones.
                    return Ok(Emission::Held);
                }
            }
        }
        let (Some(topic), Some(payload)) = (&delivery.topic, &delivery.payload) else {
            return Ok(Emission::Held);
        };
        let size = publish_packet_size(
            topic.len(),
            payload.len(),
            true,
            delivery.expiry.is_some(),
            delivery
                .properties
                .as_deref()
                .map_or(0, crate::contract::properties::PublishProperties::wire_len),
        )
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
            expiry: delivery.expiry.clone(),
            properties: delivery.properties.clone(),
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
        if self.pubsub.retained.is_some()
            && let Some(change) = self.plan_retained_qos0_result(effect, operation, reply, draft)?
        {
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
            .slot_by_op(operation)
            .map(|slot| (slot, self.pubsub.deliveries.row(slot)));
        let in_flight = if let Some(c) = control {
            &c.in_flight
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
        let finished =
            |phase, facts| Finished::new(operation, in_flight.answers.clone(), phase, facts);
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
                    let follow_up = if d.early_puback && self.receipts.control_available() {
                        let operation = self.operation(draft)?;
                        Some(alloc::boxed::Box::new(StartFollowUp::Finish(
                            self.plan_submission(
                                operation,
                                Recipe::FinishDelivery(self.finish_recipe(d, operation)?),
                                None,
                                draft,
                            )?,
                        )))
                    } else if let Some(reason) = d.early_pubrec
                        && self.receipts.control_available()
                    {
                        let operation = self.operation(draft)?;
                        let guarded = self.finish_recipe(d, operation)?;
                        if reason >= 0x80 {
                            Some(alloc::boxed::Box::new(StartFollowUp::Finish(
                                self.plan_submission(
                                    operation,
                                    Recipe::FinishDelivery(guarded),
                                    None,
                                    draft,
                                )?,
                            )))
                        } else {
                            let rank = d.pubrec_rank.unwrap_or(self.pubsub.next_pubrec_rank);
                            Some(alloc::boxed::Box::new(StartFollowUp::PreparePubrel {
                                in_flight: self.plan_submission(
                                    operation,
                                    Recipe::PreparePubrel(guarded),
                                    None,
                                    draft,
                                )?,
                                rank,
                            }))
                        }
                    } else {
                        None
                    };
                    return Ok(Some(changed(Change::Started {
                        slot,
                        finished: done,
                        follow_up,
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
                            if d.expiry().is_some_and(|expiry| {
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
                    } else if !self.qos0_fits(
                        connection,
                        topic,
                        payload,
                        d.expiry(),
                        d.properties.as_deref(),
                    ) {
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
                                expiry: d.expiry.clone(),
                                properties: d.properties.clone(),
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
                    && d.expiry().is_some_and(|expiry| {
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
            .as_ref()
            .map(|f| &f.in_flight)
            .filter(|f| f.deadline == token)
            .or_else(|| {
                self.pubsub
                    .control
                    .as_ref()
                    .map(|c| &c.in_flight)
                    .filter(|f| f.deadline == token)
                    .or_else(|| {
                        self.pubsub
                            .deliveries
                            .slot_by_deadline(token)
                            .and_then(|idx| self.pubsub.deliveries.row(idx).in_flight())
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
        let expiry = delivery.expiry().ok_or(IMPOSSIBLE)?;
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

    pub(super) fn plan_messages_expired(
        &self,
        deliveries: &[crate::contract::message_expiry::ExpiredDelivery],
    ) -> Result<super::Change, Stop> {
        if deliveries.len() > 4_096 {
            return Err(IMPOSSIBLE);
        }
        let mut seen = BTreeSet::new();
        for expired in deliveries {
            if !seen.insert((expired.delivery, expired.target)) {
                return Err(IMPOSSIBLE);
            }
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
                || (claim.is_some()
                    && other.is_some_and(|c| {
                        Some(c.operation) == claim.map(|c| c.operation)
                            && (to == target || id.publication != delivery.publication)
                    }))
        };
        claim.is_none_or(|claim| claim.valid_for(qos, expiry.is_some()))&&session_ok&&self.pubsub.deliveries.len()+same.len()<self.pubsub.deliveries.capacity()
            &&claim.is_none_or(|c|c.operation.incarnation()==self.incarnation&&c.operation.epoch()<self.epoch)
            &&!self.pubsub.deliveries.values().any(|d|conflict(d.id,d.target,d.claim))
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
        delivery.expiry = expiry.map(alloc::boxed::Box::new);
        self.pubsub.deliveries.push(delivery)
    }

    /// Returns the first target session of a pending inline-claimed admission, if any.
    #[must_use]
    pub fn inline_claim_target(&self, operation: OperationId) -> Option<SessionId> {
        let slots = self.pubsub.deliveries.slots_by_claim_op(operation)?;
        slots.iter().find_map(|slot| {
            let d = self.pubsub.deliveries.row(slot);
            matches!(d.phase, Phase::InlineClaimPending(op) if op == operation).then_some(d.target)
        })
    }

    /// Returns all target sessions of a pending inline-claimed admission.
    #[must_use]
    pub fn inline_claim_targets(&self, operation: OperationId) -> alloc::vec::Vec<SessionId> {
        let Some(slots) = self.pubsub.deliveries.slots_by_claim_op(operation) else {
            return alloc::vec::Vec::new();
        };
        slots
            .iter()
            .filter_map(|slot| {
                let d = self.pubsub.deliveries.row(slot);
                matches!(d.phase, Phase::InlineClaimPending(op) if op == operation)
                    .then_some(d.target)
            })
            .collect()
    }

    pub(super) fn mark_session_dirty(&mut self, session: SessionId, rescan: bool) {
        if let Some(generation) = self.connection_of(session) {
            self.pubsub.connections.mark_dirty(generation, rescan);
        }
    }

    pub(super) fn mark_all_sessions_dirty(&mut self, rescan: bool) {
        self.pubsub.connections.mark_all_dirty(rescan);
    }

    pub(super) fn inline_claims_globally_allowed(&self) -> bool {
        self.inline_claim_enabled
            && self.pubsub.control.is_none()
            && self.pubsub.retained.is_none()
            && !self.will_status().admission_pending
    }

    pub(super) fn plan_inline_claim(
        &self,
        selected: routing::Selected,
        length: u64,
    ) -> Option<(ConnectionGeneration, OutboundPacketId)> {
        if !self.inline_claims_globally_allowed()
            || self.pubsub.deliveries.len() >= self.pubsub.deliveries.capacity()
            || self
                .pubsub
                .deliveries
                .loaded_bytes()
                .checked_add(length)
                .is_none_or(|total| total > self.pubsub.payload_limit.get())
        {
            return None;
        }
        self.plan_inline_claim_target(selected)
    }

    pub(super) fn plan_inline_claim_target(
        &self,
        selected: routing::Selected,
    ) -> Option<(ConnectionGeneration, OutboundPacketId)> {
        if selected.qos == Qos::AtMostOnce
            || self
                .pubsub
                .non_inline_by_session
                .contains_key(&selected.session)
        {
            return None;
        }
        let generation = self.connection_of(selected.session)?;
        let conn_idx = self.pubsub.connection(generation)?;
        let settings = self.pubsub.connections.as_slice()[conn_idx];
        let rm = usize::from(settings.receive_maximum);
        let preclaim_cap = rm.saturating_add(rm.min(256));
        let session_deliveries = self
            .pubsub
            .deliveries
            .session(selected.session)
            .map_or(0, |sd| sd.all.len());
        if !settings.caught_up
            || self.pubsub.connections.is_loading(generation)
            || self.pubsub.connections.has_after(generation)
            || settings.rescan_needed
            || session_deliveries >= preclaim_cap
        {
            return None;
        }
        if let Some(sd) = self.pubsub.deliveries.session(selected.session)
            && !sd.all.is_empty()
        {
            let sent = sd.sent_on(generation, self.pubsub.deliveries.slab());
            let busy = usize::from(sd.busy);
            let inline_len = sd.inline_pending.len();
            if sent + busy + inline_len == sd.all.len() {
                for idx in &sd.inline_pending {
                    let d = self.pubsub.deliveries.row(idx);
                    if let Phase::InlineClaimPending(op) = &d.phase {
                        if !self.slot_by_op.get(op).is_some_and(|&slot_idx| {
                            matches!(
                                &self.slots[slot_idx].state,
                                SlotState::AdmissionPending(f) if f.phase == CommandPhase::Submitted
                            )
                        }) {
                            return None;
                        }
                    } else {
                        return None;
                    }
                }
            } else {
                for idx in sd.waiting_candidates(generation) {
                    let d = self.pubsub.deliveries.row(idx);
                    if !d.waiting_to_send(generation, self.message_now) || d.awaits_pubcomp() {
                        continue;
                    }
                    match &d.phase {
                        Phase::AwaitPuback => {}
                        Phase::ClaimPending(f) if f.phase == CommandPhase::Submitted => {}
                        Phase::InlineClaimPending(op)
                            if self.slot_by_op.get(op).is_some_and(|&slot_idx| {
                                matches!(
                                    &self.slots[slot_idx].state,
                                    SlotState::AdmissionPending(f) if f.phase == CommandPhase::Submitted
                                )
                            }) => {}
                        _ => return None,
                    }
                }
            }
        }
        let packet_id = self.pubsub.free_packet_id(selected.session, u16::MAX)?;
        Some((generation, packet_id))
    }

    pub(super) fn plan_inline_claim_result(
        &self,
        operation: OperationId,
        committed: bool,
        draft: &mut Draft,
    ) -> Result<Option<super::InlineOutcome>, Stop> {
        let Some(slots) = self.pubsub.deliveries.slots_by_claim_op(operation) else {
            return Ok(None);
        };
        if slots.len() == 1 {
            let slot = slots.iter().next().ok_or(IMPOSSIBLE)?;
            let d = self.pubsub.deliveries.row(slot);
            if !matches!(d.phase, Phase::InlineClaimPending(op) if op == operation) {
                return Ok(None);
            }
            if !committed || d.retired {
                return Ok(Some(super::InlineOutcome::One(
                    super::InlineSlotOutcome::Remove(slot),
                )));
            }
            let emission = if draft.effects.count + 1 < super::MAX_TRANSITION_EFFECTS {
                match self.plan_delivery_emit(d, draft)? {
                    Emission::Frame(effect, connection) => Some((effect, connection)),
                    Emission::Held | Emission::TooLarge => None,
                }
            } else {
                None
            };
            return Ok(Some(super::InlineOutcome::One(
                super::InlineSlotOutcome::Committed { slot, emission },
            )));
        }
        let mut outcomes = alloc::vec::Vec::with_capacity(slots.len());
        for slot in slots.iter() {
            let d = self.pubsub.deliveries.row(slot);
            if !matches!(d.phase, Phase::InlineClaimPending(op) if op == operation) {
                continue;
            }
            if !committed || d.retired {
                outcomes.push(super::InlineSlotOutcome::Remove(slot));
                continue;
            }
            let emission = if draft.effects.count + 1 < super::MAX_TRANSITION_EFFECTS {
                match self.plan_delivery_emit(d, draft)? {
                    Emission::Frame(effect, connection) => Some((effect, connection)),
                    Emission::Held | Emission::TooLarge => None,
                }
            } else {
                None
            };
            outcomes.push(super::InlineSlotOutcome::Committed { slot, emission });
        }
        if outcomes.is_empty() {
            Ok(None)
        } else {
            Ok(Some(super::InlineOutcome::Many(outcomes)))
        }
    }

    pub(super) fn apply_inline_claim_insert(
        &mut self,
        operation: OperationId,
        publication: PublicationId,
        plan: super::InlineClaimPlan,
        topic: SharedBytes,
        payload: SharedBytes,
        properties: Option<alloc::sync::Arc<crate::contract::properties::PublishProperties>>,
    ) -> Result<(), CoreFault> {
        let claim = DeliveryClaim {
            operation,
            packet_id: plan.packet_id,
            started: false,
            await_pubcomp: false,
        };
        let delivery_id = DeliveryId {
            publication,
            index: u32::from(plan.target_index),
        };
        let mut d = Delivery::metadata(
            delivery_id,
            plan.target,
            plan.target_revision,
            plan.qos,
            Some(claim),
        );
        let has_room = self.pubsub.connection(plan.generation).is_some_and(|i| {
            let rm = usize::from(self.pubsub.connections.as_slice()[i].receive_maximum);
            self.pubsub.active(plan.generation) < rm
        });
        let has_unwindowed_unsent = self
            .pubsub
            .deliveries
            .session(plan.target)
            .is_some_and(|sd| {
                sd.waiting_candidates(plan.generation).iter().any(|idx| {
                    let existing = self.pubsub.deliveries.row(idx);
                    existing.window != Some(plan.generation)
                        && existing.waiting_to_send(plan.generation, self.message_now)
                })
            });
        d.phase = Phase::InlineClaimPending(operation);
        if has_room && !has_unwindowed_unsent {
            d.window = Some(plan.generation);
        }
        d.may_have_started = false;
        d.certain_never_started = true;
        d.topic = Some(topic);
        d.payload = Some(payload);
        d.properties = properties;
        self.pubsub.deliveries.push(d)?;
        if self.inline_claim_enabled
            && self
                .pubsub
                .connection(plan.generation)
                .is_some_and(|i| self.pubsub.connections.as_slice()[i].caught_up)
        {
            self.pubsub
                .connections
                .advance_known_through(plan.generation, delivery_id);
        }
        Ok(())
    }

    /// Fast O(1) check whether `session` definitely has no ready outbound work
    /// right now (no control candidates, no rescan/after cursor, and all
    /// deliveries already sent, busy, or inline-pending).
    pub(super) fn session_provably_idle_work(&self, session: SessionId) -> bool {
        if !self.pubsub.deliveries.control_candidates().is_empty() {
            return false;
        }
        let Some(generation) = self.connection_of(session) else {
            return true;
        };
        let Some(index) = self.pubsub.connection(generation) else {
            return true;
        };
        let settings = self.pubsub.connections.as_slice()[index];
        if settings.rescan_needed || self.pubsub.connections.has_after(generation) {
            return false;
        }
        let Some(sd) = self.pubsub.deliveries.session(session) else {
            return true;
        };
        if sd.all.is_empty() {
            return true;
        }
        let sent = sd.sent_on(generation, self.pubsub.deliveries.slab());
        sent + usize::from(sd.busy) + sd.inline_pending.len() == sd.all.len()
    }

    pub(super) fn apply_inline_claim_outcome(
        &mut self,
        outcome: super::InlineOutcome,
        has_non_inline: bool,
    ) -> Result<(), CoreFault> {
        let mut all_idle = !has_non_inline;
        let apply_one = |owner: &mut Owner,
                         slot_outcome: super::InlineSlotOutcome,
                         all_idle: &mut bool|
         -> Result<(), CoreFault> {
            match slot_outcome {
                super::InlineSlotOutcome::Remove(slot) => {
                    let Some(d) = owner.pubsub.deliveries.get(slot) else {
                        return Err(CoreFault::StalePlan);
                    };
                    let target = d.target;
                    owner.pubsub.deliveries.remove(slot);
                    if !owner.session_provably_idle_work(target) {
                        *all_idle = false;
                        owner.push_inline_ready_target(target);
                    }
                    Ok(())
                }
                super::InlineSlotOutcome::Committed { slot, emission } => {
                    let Some(d) = owner.pubsub.deliveries.get(slot) else {
                        return Err(CoreFault::StalePlan);
                    };
                    let target = d.target;
                    owner.pubsub.deliveries.mutate(slot, |d| {
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
                    });
                    if emission.is_none() || !owner.session_provably_idle_work(target) {
                        *all_idle = false;
                        owner.push_inline_ready_target(target);
                    }
                    Ok(())
                }
            }
        };
        match outcome {
            super::InlineOutcome::One(single) => apply_one(self, single, &mut all_idle)?,
            super::InlineOutcome::Many(many) => {
                for slot_outcome in many {
                    apply_one(self, slot_outcome, &mut all_idle)?;
                }
            }
        }
        if all_idle {
            self.last_step_may_unblock_work = false;
        }
        Ok(())
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
                    .record_qos0_ingress(connection, flight.origin.ingress_order)?;
                self.pubsub.retained = Some(*flight);
            }
            Change::RetainedDone { finished } => {
                self.pubsub.retained.take().ok_or(stale)?;
                self.mark_all_sessions_dirty(true);
                self.remember_finished(finished)?;
            }
            Change::Qos0Admit {
                connection,
                ingress_order,
                records,
                continuation,
            } => {
                self.pubsub
                    .connections
                    .record_qos0_ingress(connection, ingress_order)?;
                for record in records.as_slice() {
                    self.pubsub.volatile.push_pending(record.clone())?;
                }
                if let Some(c) = continuation {
                    self.pubsub.continuation = Some(c);
                    self.pubsub.continuations_started = true;
                }
            }
            Change::Qos0Delegated {
                connection,
                ingress_order,
                batch,
            } => {
                self.pubsub
                    .connections
                    .record_qos0_ingress(connection, ingress_order)?;
                self.last_delegated_qos0 = Some(*batch);
                self.last_step_may_unblock_work = false;
            }
            Change::Qos0Send {
                effect,
                continuation,
            } => {
                self.pubsub.volatile.promote_front_to_inflight(effect)?;
                self.pubsub.continuation = continuation;
            }
            Change::Qos0Drop { continuation } => {
                self.pubsub.volatile.drop_front_pending()?;
                self.pubsub.continuation = continuation;
            }
            Change::Qos0Park { continuation } => {
                self.pubsub.volatile.park_front_pending()?;
                self.pubsub.continuation = continuation;
            }
            Change::Qos0Serve {
                effect,
                stays,
                continuation,
            } => {
                self.pubsub.volatile.serve_ready(effect, stays)?;
                self.pubsub.continuation = continuation;
            }
            Change::Qos0Done {
                effect,
                continuation,
            } => {
                self.pubsub.volatile.remove_inflight(effect)?;
                if let Some(c) = continuation {
                    self.pubsub.continuation = Some(c);
                    self.pubsub.continuations_started = true;
                }
            }
            Change::Qos0Idle => self.pubsub.continuation = None,
            Change::Consume {
                slot,
                in_flight,
                connection,
            } => {
                if self.pubsub.deliveries.get(slot).is_none() {
                    return Err(stale);
                }
                self.pubsub.deliveries.mutate(slot, |d| {
                    d.phase = Phase::ConsumePending(in_flight, connection);
                });
            }
            Change::Consumed {
                slot,
                finished,
                emission,
            } => {
                let (target, delivery_id) = {
                    let d = self.pubsub.deliveries.get(slot).ok_or(stale)?;
                    (d.target, d.id)
                };
                if emission.is_none() {
                    self.remember_finished(finished)?;
                    self.pubsub.deliveries.remove(slot);
                } else {
                    self.pubsub.deliveries.mutate(slot, |d| {
                        d.phase = Phase::Qos0Writing(alloc::boxed::Box::new(finished.clone()));
                        d.claim_result = Some(alloc::boxed::Box::new(finished));
                        d.frame = emission;
                    });
                }
                self.pubsub
                    .connections
                    .invalidate_reads_for(target, delivery_id);
            }
            Change::Settings { index, value } => match index {
                Some(i) => self.pubsub.connections.replace(i, value)?,
                None => self.pubsub.connections.push(value)?,
            },
            Change::Control(control) => self.pubsub.control = Some(*control),
            Change::ControlEnd { revision, finished } => {
                let control = self.pubsub.control.take().ok_or(stale)?;
                if let Some(revision) = revision {
                    let mut removed_any = false;
                    let changes = control.changes.as_slice();
                    let mut idx = 0;
                    while idx < self.routes.len() {
                        let r = &self.routes.as_slice()[idx];
                        if r.target == control.session && changes.iter().any(|c| c.topic == r.topic)
                        {
                            self.routes.as_mut_slice()[idx..].rotate_left(1);
                            let _ = self.routes.pop();
                            removed_any = true;
                        } else {
                            idx += 1;
                        }
                    }
                    if removed_any {
                        self.rebuild_route_index();
                    }
                    for (i, change) in changes.iter().enumerate() {
                        if change.granted.is_none()
                            || changes[i + 1..]
                                .iter()
                                .any(|later| later.topic == change.topic)
                        {
                            continue;
                        }
                        self.push_route(Route {
                            topic: change.topic.clone(),
                            target: control.session,
                            target_revision: control.session_revision,
                            max_qos: change.granted.ok_or(stale)?,
                            options: change.options,
                        })?;
                    }
                    self.route_revision = Some(revision);
                    if let Some(generation) = self.connection_of(control.session) {
                        self.pubsub.connections.clear_known_through(generation);
                    }
                    self.mark_session_dirty(control.session, true);
                }
                self.remember_finished(finished)?;
            }
            Change::Scan { connection, read } => {
                self.pubsub.connections.begin_scan(connection, read)?;
            }
            Change::Page {
                connection,
                discard,
                next,
            } => {
                let page_session = match &input {
                    Input::WorkPage { session, .. } => Some(*session),
                    _ => None,
                };
                let no_pending_non_inline = page_session
                    .is_some_and(|s| !self.pubsub.non_inline_by_session.contains_key(&s))
                    && self.pubsub.control.is_none()
                    && self.pubsub.retained.is_none()
                    && !self.will_status().admission_pending;
                let generation = self.pubsub.connections.complete_page(
                    connection,
                    discard,
                    next,
                    no_pending_non_inline,
                )?;
                if !discard {
                    let Input::WorkPage { items, .. } = input else {
                        return Err(stale);
                    };
                    for item in items.as_slice() {
                        if let Some(i) = self.pubsub.delivery(item.delivery) {
                            if self.pubsub.deliveries.row(i).topic.is_none() {
                                self.pubsub.deliveries.rewrite(i, |d| {
                                    d.cause = item.cause;
                                    d.retain = item.retain;
                                    d.properties = item.properties.clone();
                                    d.topic = Some(item.topic.clone());
                                    d.payload = Some(item.payload.clone());
                                });
                            }
                        } else if !self.expired_unclaimed_item(item) {
                            let mut d = Delivery::metadata(
                                item.delivery,
                                item.target,
                                item.target_revision,
                                item.qos,
                                item.claim,
                            );
                            d.expiry = item.expiry.map(alloc::boxed::Box::new);
                            d.properties = item.properties.clone();
                            d.cause = item.cause;
                            d.retain = item.retain;
                            d.topic = Some(item.topic.clone());
                            d.payload = Some(item.payload.clone());
                            self.pubsub.deliveries.push(d)?;
                        }
                    }
                    // The returned items are valid. Their high-water mark
                    // cannot cover obligations made visible since the read.
                    if !self.pubsub.connections.as_slice()[connection].rescan_needed
                        && self.inline_claim_enabled
                        && self.pubsub.control.is_none()
                        && self.pubsub.retained.is_none()
                        && !self.will_status().admission_pending
                        && let Some(high) =
                            next.or_else(|| items.as_slice().last().map(|item| item.delivery))
                        && self
                            .pubsub
                            .pending_non_inline_pubs
                            .first()
                            .is_none_or(|&min_p| min_p > high.publication)
                    {
                        self.pubsub
                            .connections
                            .advance_known_through(generation, high);
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
                if self.pubsub.deliveries.get(slot).is_none() {
                    return Err(stale);
                }
                self.pubsub.deliveries.mutate(slot, |d| {
                    d.claim = Some(claim);
                    d.phase = Phase::ClaimPending(in_flight);
                    d.window = Some(generation);
                });
            }
            Change::Claimed {
                slot,
                finished,
                emission,
            } => {
                if self.pubsub.deliveries.get(slot).is_none() {
                    return Err(stale);
                }
                self.pubsub.deliveries.mutate(slot, |d| {
                    d.claim_result = Some(alloc::boxed::Box::new(finished));
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
                });
            }
            Change::Emit {
                slot,
                effect,
                connection,
            } => {
                if self.pubsub.deliveries.get(slot).is_none() {
                    return Err(stale);
                }
                self.pubsub.deliveries.mutate(slot, |d| {
                    if !d.awaits_pubcomp() {
                        d.window = Some(connection.generation);
                    }
                    if d.frame.is_some() {
                        d.certain_never_started = false;
                    }
                    d.frame = Some((effect, connection));
                    d.may_have_started = true;
                    d.local_write_completed = false;
                });
            }
            Change::Finish { slot, in_flight } => {
                if self.pubsub.deliveries.get(slot).is_none() {
                    return Err(stale);
                }
                self.pubsub.deliveries.mutate(slot, |d| {
                    d.early_puback = false;
                    d.early_pubrec = None;
                    d.phase = Phase::FinishPending(in_flight);
                });
                if matches!(input, Input::ObservedPuback { .. }) {
                    self.last_step_may_unblock_work = false;
                }
            }
            Change::Discard { slot, in_flight } => {
                if self.pubsub.deliveries.get(slot).is_none() {
                    return Err(stale);
                }
                self.pubsub.deliveries.mutate(slot, |d| {
                    d.phase = Phase::DiscardPending(in_flight);
                });
            }
            Change::PreparePubrel {
                slot,
                in_flight,
                rank,
            } => {
                if self.pubsub.deliveries.get(slot).is_none() {
                    return Err(stale);
                }
                let mut advance_rank = false;
                self.pubsub.deliveries.mutate(slot, |d| {
                    d.early_pubrec = None;
                    if d.pubrec_rank.is_none() {
                        d.pubrec_rank = Some(rank);
                        advance_rank = true;
                    }
                    d.phase = Phase::PubrelPending(in_flight);
                });
                if advance_rank {
                    self.pubsub.next_pubrec_rank =
                        self.pubsub.next_pubrec_rank.max(rank.saturating_add(1));
                }
            }
            Change::PubrelReady { slot, finished } => {
                if self
                    .pubsub
                    .deliveries
                    .get(slot)
                    .is_none_or(|d| d.claim.is_none())
                {
                    return Err(stale);
                }
                self.pubsub.deliveries.mutate(slot, |d| {
                    if let Some(claim) = d.claim.as_mut() {
                        claim.await_pubcomp = true;
                    }
                    d.phase = Phase::AwaitPuback;
                    d.frame = None;
                    d.certain_never_started = false;
                });
                self.remember_finished(finished)?;
            }
            Change::Finished { slot, finished } => {
                if self.pubsub.deliveries.get(slot).is_none() {
                    return Err(stale);
                }
                let (target, delivery_id, finished_pi) = {
                    let row = self.pubsub.deliveries.row(slot);
                    (row.target, row.id, row.claim.map(|c| c.packet_id))
                };
                let claim_result = self.pubsub.deliveries.row(slot).claim_result.clone();
                if let Some(known) = claim_result
                    && known.operation != finished.operation
                {
                    self.remember_finished(*known)?;
                }
                self.remember_finished(finished)?;
                self.pubsub.deliveries.remove(slot);
                self.last_finished_delivery = Some((target, finished_pi));
                self.pubsub
                    .connections
                    .invalidate_reads_for(target, delivery_id);
                if self.session_provably_idle_work(target) {
                    self.last_step_may_unblock_work = false;
                }
            }
            Change::Refused {
                slot,
                finished,
                claiming,
            } => {
                if self.pubsub.deliveries.get(slot).is_none() {
                    return Err(stale);
                }
                self.pubsub.deliveries.mutate(slot, |d| {
                    d.window = None;
                    if claiming {
                        d.claim = None;
                        d.phase = Phase::Queued;
                    } else {
                        d.phase = Phase::AwaitPuback;
                    }
                });
                self.remember_finished(finished)?;
            }
            Change::Await {
                operation,
                phase,
                resolution,
            } => {
                if let Some(control) = self
                    .pubsub
                    .control
                    .as_mut()
                    .filter(|c| c.in_flight.operation == operation)
                {
                    control.in_flight.phase = phase;
                    if let Some((deadline, effect)) = resolution {
                        control.in_flight.deadline = deadline;
                        let prev = core::mem::replace(
                            &mut control.in_flight.answers,
                            Answers::new(effect),
                        );
                        control.in_flight.answers = prev.with(effect);
                    }
                } else if let Some(flight) = self
                    .pubsub
                    .retained
                    .as_mut()
                    .filter(|f| f.in_flight.operation == operation)
                {
                    flight.in_flight.phase = phase;
                    if let Some((deadline, effect)) = resolution {
                        flight.in_flight.deadline = deadline;
                        let prev =
                            core::mem::replace(&mut flight.in_flight.answers, Answers::new(effect));
                        flight.in_flight.answers = prev.with(effect);
                    }
                } else {
                    let slot = self.pubsub.deliveries.slot_by_op(operation).ok_or(stale)?;
                    if self.pubsub.deliveries.row(slot).in_flight().is_none() {
                        return Err(stale);
                    }
                    self.pubsub.deliveries.mutate(slot, |d| {
                        if let Some(held) = d.in_flight_mut() {
                            held.phase = phase;
                            if let Some((deadline, effect)) = resolution {
                                held.deadline = deadline;
                                let prev =
                                    core::mem::replace(&mut held.answers, Answers::new(effect));
                                held.answers = prev.with(effect);
                            }
                        }
                    });
                }
            }
            Change::PubackObserved { slot } => {
                if self.pubsub.deliveries.get(slot).is_none() {
                    return Err(stale);
                }
                self.pubsub.deliveries.mutate(slot, |d| {
                    d.early_puback = true;
                });
            }
            Change::PubrecObserved { slot, reason, rank } => {
                if self.pubsub.deliveries.get(slot).is_none() {
                    return Err(stale);
                }
                let mut advance_rank = false;
                self.pubsub.deliveries.mutate(slot, |d| {
                    d.early_pubrec = Some(reason);
                    if let Some(rank) = rank
                        && d.pubrec_rank.is_none()
                    {
                        d.pubrec_rank = Some(rank);
                        advance_rank = true;
                    }
                });
                if advance_rank && let Some(rank) = rank {
                    self.pubsub.next_pubrec_rank =
                        self.pubsub.next_pubrec_rank.max(rank.saturating_add(1));
                }
            }
            Change::Start { slot, in_flight } => {
                if self.pubsub.deliveries.get(slot).is_none() {
                    return Err(stale);
                }
                self.pubsub.deliveries.mutate(slot, |d| {
                    d.certain_never_started = false;
                    d.phase = Phase::StartPending(in_flight);
                });
            }
            Change::Started {
                slot,
                finished,
                follow_up,
            } => {
                if self
                    .pubsub
                    .deliveries
                    .get(slot)
                    .is_none_or(|d| d.claim.is_none())
                {
                    return Err(stale);
                }
                let mut advance_rank: Option<u64> = None;
                self.pubsub.deliveries.mutate(slot, |d| {
                    if let Some(claim) = d.claim.as_mut() {
                        claim.started = true;
                    }
                    match follow_up.map(|b| *b) {
                        None => d.phase = Phase::AwaitPuback,
                        Some(StartFollowUp::Finish(in_flight)) => {
                            d.early_puback = false;
                            d.early_pubrec = None;
                            d.phase = Phase::FinishPending(in_flight);
                        }
                        Some(StartFollowUp::PreparePubrel { in_flight, rank }) => {
                            d.early_pubrec = None;
                            if d.pubrec_rank.is_none() {
                                d.pubrec_rank = Some(rank);
                                advance_rank = Some(rank);
                            }
                            d.phase = Phase::PubrelPending(in_flight);
                        }
                    }
                });
                if let Some(rank) = advance_rank {
                    self.pubsub.next_pubrec_rank =
                        self.pubsub.next_pubrec_rank.max(rank.saturating_add(1));
                }
                self.remember_finished(finished)?;
            }
            Change::Expire { slot, in_flight } => {
                if self.pubsub.deliveries.get(slot).is_none() {
                    return Err(stale);
                }
                self.pubsub.deliveries.mutate(slot, |d| {
                    d.phase = Phase::ExpirePending(in_flight);
                });
            }
            Change::RetryDeferred {
                slot,
                finished,
                expiring,
            } => {
                if self.pubsub.deliveries.get(slot).is_none() {
                    return Err(stale);
                }
                self.pubsub.deliveries.mutate(slot, |d| {
                    d.phase = if expiring {
                        Phase::ExpireRetry
                    } else {
                        Phase::StartRetry
                    };
                    if expiring {
                        d.frame = None;
                        d.window = None;
                    }
                });
                self.remember_finished(finished)?;
            }
            Change::NotStarted { slot } => {
                if self.pubsub.deliveries.get(slot).is_none() {
                    return Err(stale);
                }
                self.pubsub.deliveries.mutate(slot, |d| {
                    d.frame = None;
                    d.window = None;
                    if d.certain_never_started {
                        d.may_have_started = false;
                    }
                });
            }
            Change::MessagesExpired => {
                let Input::MessagesExpired { deliveries } = input else {
                    return Err(stale);
                };
                for expired in deliveries.as_slice() {
                    let Some(slot) = self.pubsub.delivery(expired.delivery) else {
                        continue;
                    };
                    let (target, skip, retire) = {
                        let d = self.pubsub.deliveries.row(slot);
                        let skip = d.target != expired.target
                            || (d.claim.is_some() && !matches!(d.phase, Phase::ClaimPending(_)));
                        (d.target, skip, d.in_flight().is_some())
                    };
                    if skip {
                        continue;
                    }
                    if retire {
                        self.pubsub.deliveries.rewrite(slot, |d| {
                            d.retired = true;
                            d.window = None;
                            d.topic = None;
                            d.payload = None;
                        });
                    } else {
                        self.pubsub.deliveries.remove(slot);
                    }
                    self.pubsub.connections.invalidate_reads(target);
                }
            }
            Change::Written { slot, complete } => {
                if self.pubsub.deliveries.get(slot).is_none() {
                    return Err(stale);
                }
                self.pubsub.deliveries.mutate(slot, |d| {
                    d.local_write_completed |= complete;
                    d.certain_never_started = false;
                });
                self.last_step_may_unblock_work = false;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
