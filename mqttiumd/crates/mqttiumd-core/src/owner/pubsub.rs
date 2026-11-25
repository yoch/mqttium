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

    #[allow(dead_code)]
    pub(super) fn to_vec(&self) -> alloc::vec::Vec<Volatile> {
        let mut out = alloc::vec::Vec::with_capacity(self.len());
        out.extend(self.inflight.values().cloned());
        out.extend(
            self.lanes
                .values()
                .flat_map(|lane| lane.parked.iter().cloned()),
        );
        out.extend(self.pending.iter().cloned());
        out
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

    pub(super) fn work_bound(&self, input: &Input, _routes: usize) -> u64 {
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

    fn loaded_bytes(&self) -> Option<u64> {
        Some(self.deliveries.loaded_bytes())
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

    #[allow(dead_code)]
    pub(super) fn rebuild_indices(&mut self) {
        self.connections.rebuild_indices();
        self.deliveries.rebuild_indices();
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
                    return (room && !self.pubsub.connections.is_loading(generation))
                        .then_some(generation);
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
        (room && !self.pubsub.connections.is_loading(generation) && needs_scan)
            .then_some(generation)
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

    #[allow(dead_code)]
    pub(super) fn project_routes(
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
        let loading = self.pubsub.connections.is_loading(generation);
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
mod tests {
    use super::*;
    use crate::contract::command::StartupNonce;
    use crate::contract::effects::{EffectSink, SubmitResult};
    use crate::contract::outcome::{EpochOpened, EpochOpening, OpeningOutcome, OpeningReply};
    use alloc::vec::Vec;

    impl State {
        pub(in crate::owner) fn assert_indices_kept(&mut self) {
            let kept = (
                self.connections.index_snapshot(),
                self.deliveries.index_snapshot(),
            );
            self.rebuild_indices();
            let rebuilt = (
                self.connections.index_snapshot(),
                self.deliveries.index_snapshot(),
            );
            assert_eq!(kept, rebuilt, "a pub/sub index drifted from its rows");
        }
    }

    impl Owner {
        /// Steps the owner and verifies that every incremental pub/sub index
        /// and counter matches a clean rebuild from the rows, on every outcome.
        pub(in crate::owner) fn step_checked(
            &mut self,
            input: Input,
            grant: TurnGrant,
        ) -> StepOutcome {
            let outcome = self.step(input, grant);
            self.pubsub.assert_indices_kept();
            outcome
        }
    }

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
        owner.pubsub.rebuild_indices();
        let result = owner.step_checked(input, TURN);
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
            session_qos2_exchanges: NonZeroU32::MAX,
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
    #[test]
    fn a_work_notification_during_a_read_survives_the_old_empty_page() {
        // The coordinator and delivery shards use the same work reader.
        // Their runtime schedules WorkAvailable after the page only if the
        // owner still reports ready work.
        for during in [None, Some(false), Some(true)] {
            let mut owner = world();
            owner.mark_session_dirty(TARGET, true);
            let load = apply(&mut owner, Input::WorkAvailable { session: TARGET });
            let [(old_read, Effect::LoadWork { after: None, .. })] = load.as_slice() else {
                panic!("expected the initial read");
            };
            if let Some(rescan) = during {
                owner.mark_session_dirty(TARGET, rescan);
                assert!(apply(&mut owner, Input::WorkAvailable { session: TARGET }).is_empty());
            }
            let page = |effect| Input::WorkPage {
                effect,
                session: TARGET,
                items: BoundedVec::with_capacity(0).unwrap(),
                next: None,
            };
            assert!(apply(&mut owner, page(*old_read)).is_empty());
            assert_eq!(owner.has_ready_work(TARGET), during == Some(true));
            if during == Some(true) {
                let load = apply(&mut owner, Input::WorkAvailable { session: TARGET });
                let [(fresh_read, Effect::LoadWork { after: None, .. })] = load.as_slice() else {
                    panic!("the notification must start one fresh read");
                };
                assert_ne!(old_read, fresh_read);
                assert!(apply(&mut owner, page(*fresh_read)).is_empty());
                assert!(
                    !owner.has_ready_work(TARGET),
                    "a clean empty page goes idle"
                );
            }
        }
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
    #[test]
    fn outgoing_pi_fallback_finds_the_lowest_gap_above_the_bitmap() {
        let mut table = Deliveries::new(nonzero(514)).unwrap();
        for id in (1u16..=515).filter(|&id| id != 514) {
            let claim = DeliveryClaim {
                operation: OperationId::new(
                    StoreIncarnation::new(1),
                    RunEpoch::new(1),
                    LogicalOwnerId::new(4),
                    OperationSeq::new(u64::from(id)),
                ),
                packet_id: OutboundPacketId::new(id).unwrap(),
                started: false,
                await_pubcomp: false,
            };
            table
                .push(Delivery::metadata(
                    delivery(u32::from(id)),
                    TARGET,
                    Revision::new(1),
                    Qos::AtLeastOnce,
                    Some(claim),
                ))
                .unwrap();
        }
        assert_eq!(table.free_packet_id(TARGET, 513, false, false), None);
        assert_eq!(
            table.free_packet_id(TARGET, u16::MAX, false, false),
            OutboundPacketId::new(514)
        );
        table.remove(table.slot_of(delivery(512)).unwrap());
        assert_eq!(
            table.free_packet_id(TARGET, u16::MAX, false, false),
            OutboundPacketId::new(512)
        );
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
        owner.pubsub.deliveries.push(item).unwrap();
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
        owner.pubsub.deliveries.row_mut(0).qos = Qos::ExactlyOnce;
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
        let item = owner.pubsub.deliveries.row_mut(0);
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
                    let item = owner.pubsub.deliveries.row_mut(index);
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
                owner.pubsub.deliveries.row_mut(0).qos = Qos::AtMostOnce;
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
            owner.pubsub.deliveries.push(other.clone()).unwrap();
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
                owner.pubsub.deliveries.to_vec().as_slice(),
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
            owner.pubsub.deliveries.push(item).unwrap();
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
        owner.pubsub.deliveries.push(pure_zero).unwrap();
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
        assert!(!owner.pubsub.deliveries.row(0).local_write_completed);
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
        let before_callback = owner.pubsub.deliveries.to_vec();
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
            owner.pubsub.deliveries.to_vec().as_slice(),
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
            !owner.pubsub.deliveries.row(0).local_write_completed,
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
                properties: None,
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
        let flight = owner.pubsub.retained.clone().unwrap();
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
            owner.step_checked(retained_zero(2), TURN),
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
            owner.pubsub.retained.as_ref().unwrap().retired,
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
        let current = owner.pubsub.retained.clone().unwrap();
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
            owner.pubsub.retained.as_ref(),
            Some(&current),
            "old_retained_qos0_result_rebound"
        );
        apply(
            &mut owner,
            Input::StoreResult {
                effect: current.in_flight.answers.persist_id(),
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
        let result = owner.step_checked(product_subscribe(), TURN);
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
                        .push(Delivery::metadata(
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
                    properties: None,
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
                assert!(owner.pubsub.deliveries.row(0).retain);
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
    fn a_read_started_before_an_older_obligation_revisits_the_lower_range() {
        fn id(sequence: u64) -> DeliveryId {
            DeliveryId {
                publication: PublicationId::new(
                    StoreIncarnation::new(1),
                    RunEpoch::new(1),
                    LogicalOwnerId::new(4),
                    PublicationSeq::new(sequence),
                ),
                index: 0,
            }
        }
        fn load(owner: &mut Owner) -> (EffectId, Option<DeliveryId>) {
            apply(owner, Input::WorkAvailable { session: TARGET })
                .into_iter()
                .find_map(|(effect, value)| match value {
                    Effect::LoadWork { after, .. } => Some((effect, after)),
                    _ => None,
                })
                .expect("work discovery should issue one bounded page read")
        }
        for next in [None, Some(id(60))] {
            let mut owner = world();
            owner.enable_inline_claims();
            message_clock(&mut owner, 30);
            owner
                .pubsub
                .connections
                .advance_known_through(CONNECTION, id(50));
            let (effect, after) = load(&mut owner);
            assert_eq!(after, Some(id(50)));

            // The writer took this page's snapshot before an earlier-reserved
            // publication (40) became an obligation for TARGET.
            owner.shard_notify_dirty(TARGET, true);
            assert!(apply(&mut owner, Input::WorkAvailable { session: TARGET }).is_empty());
            let stale = WorkItem {
                delivery: id(60),
                target: TARGET,
                target_revision: Revision::new(1),
                qos: Qos::AtLeastOnce,
                cause: DeliveryCause::LiveExact,
                retain: false,
                // The stale higher row needs no claim. Its identifier is still
                // a page cursor even when expiry skips materializing the row.
                expiry: Some(message_expiry()),
                properties: None,
                topic: bytes(b"t/a"),
                payload: bytes(b"high"),
                claim: None,
            };
            apply(
                &mut owner,
                Input::WorkPage {
                    effect,
                    session: TARGET,
                    items: BoundedVec::from_slice(&[stale], 1).unwrap(),
                    next,
                },
            );

            let mut found_lower = false;
            // One page remains at most. A design can finish that chain before
            // revisiting the start, or rewind immediately.
            for _ in 0..2 {
                if !owner.has_ready_work(TARGET) {
                    break;
                }
                let (effect, after) = load(&mut owner);
                if after.is_none_or(|cursor| cursor < id(40)) {
                    found_lower = true;
                    break;
                }
                apply(
                    &mut owner,
                    Input::WorkPage {
                        effect,
                        session: TARGET,
                        items: BoundedVec::with_capacity(1).unwrap(),
                        next: None,
                    },
                );
            }
            assert!(
                found_lower,
                "a lower obligation remains invisible after next={next:?}"
            );
        }
    }

    #[test]
    fn a_later_work_page_cannot_rewrite_an_already_loaded_retain_flag() {
        let mut owner = world();
        queued(&mut owner, 0);
        owner.pubsub.deliveries.row_mut(0).retain = true;
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
        let held = owner.pubsub.deliveries.row(0);
        let changed = WorkItem {
            cause: DeliveryCause::LiveExact,
            expiry: None,
            properties: None,
            delivery: held.id,
            target: held.target,
            target_revision: held.revision,
            qos: held.qos,
            retain: false,
            topic: held.topic.clone().unwrap(),
            payload: held.payload.clone().unwrap(),
            claim: held.claim,
        };
        let result = owner.step_checked(
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
        assert!(owner.pubsub.deliveries.row(0).retain);
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
        owner.pubsub.deliveries.last_mut().unwrap().expiry = Some(Box::new(message_expiry()));
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
                    assert_eq!(expiry.as_deref().copied(), Some(message_expiry()));
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
                let d = owner.pubsub.deliveries.row_mut(0);
                d.expiry = Some(Box::new(message_expiry()));
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
                assert!(owner.pubsub.deliveries.values().all(|d| d.claim.is_none()));
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
    fn closing_shard_with_early_puback_requires_a_reported_first_byte() {
        use crate::owner::ShardAckProgress;

        let mut owner = world();
        message_clock(&mut owner, 10);
        let (frame, key, pi) = message_frame(&mut owner, 0);
        assert!(message_puback(&mut owner, key, pi).is_empty());
        assert!(message_progress(&mut owner, frame, key, FrameProgress::Failed).is_empty());
        assert_eq!(owner.shard_ack_progress(key), ShardAckProgress::Settled);
        owner
            .shard_detach_session(key.session, key.generation)
            .unwrap();
        let current = ConnectionKey {
            session: TARGET,
            generation: ConnectionGeneration::new(11),
        };
        let revision = owner.pubsub.deliveries.row(0).revision;
        owner
            .shard_attach_session(TARGET, revision, current.generation, 8, 65536)
            .unwrap();
        let replay = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        let replay = replay
            .into_iter()
            .find_map(|(id, e)| {
                matches!(e, Effect::EmitPublish { packet_id, .. } if packet_id == pi).then_some(id)
            })
            .expect("same packet identifier on the resumed session");
        let (id, started) = message_command(message_progress(
            &mut owner,
            replay,
            current,
            FrameProgress::Started,
        ));
        assert!(
            known(&mut owner, id, &started).is_empty(),
            "the new transmission requires its own PUBACK"
        );
        let (id, finish) = message_command(message_puback(&mut owner, current, pi));
        assert!(known(&mut owner, id, &finish).is_empty());
    }

    #[test]
    fn closing_shard_settles_a_certainly_rejected_early_finish() {
        use crate::owner::ShardAckProgress;

        let mut owner = world();
        message_clock(&mut owner, 10);
        let (frame, key, pi) = message_frame(&mut owner, 0);
        assert!(message_puback(&mut owner, key, pi).is_empty());
        let (start_id, start) = message_command(message_progress(
            &mut owner,
            frame,
            key,
            FrameProgress::Started,
        ));
        let (id, finish) = message_command(known(&mut owner, start_id, &start));
        assert_eq!(owner.shard_ack_progress(key), ShardAckProgress::Waiting);
        let effects = apply(
            &mut owner,
            Input::StoreResult {
                effect: id,
                operation: finish.recipe.operation(),
                reply: StoreReply::Outcome(StoreOutcome::RejectedCertain(RejectReason::Fenced)),
            },
        );
        assert!(
            effects
                .iter()
                .any(|(_, e)| matches!(e, Effect::CloseConnection { .. }))
        );
        assert_eq!(owner.shard_ack_progress(key), ShardAckProgress::Settled);
    }

    #[test]
    fn message_expiry_puback_before_started_is_held_through_unknown_marker_and_expiration() {
        let mut owner = world();
        message_clock(&mut owner, 10);
        let (frame, key, pi) = message_frame(&mut owner, 0);
        let target = owner.puback_target(key, pi).unwrap();
        assert!(message_puback(&mut owner, key, pi).is_empty());
        assert!(owner.pubsub.deliveries.row(0).early_puback);
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
            !owner.pubsub.deliveries.row(0).claim.unwrap().started,
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
        assert_eq!(
            owner.shard_ack_progress(key),
            crate::owner::ShardAckProgress::Waiting
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
        let deadline = owner.pubsub.deliveries.row(0).in_flight().unwrap().deadline;
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
        assert!(owner.pubsub.deliveries.row(0).claim.unwrap().started);
        assert_eq!(
            owner.pubsub.free_packet_id(TARGET, 1),
            None,
            "marker_result_freed_pi"
        );
        assert_eq!(
            owner.shard_ack_progress(key),
            crate::owner::ShardAckProgress::Waiting
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
        assert!(replay.iter().any(|(_, e)| matches!(e, Effect::EmitPublish { started: true, expiry: Some(expiry), packet_id, claim, dup: true, .. } if **expiry == message_expiry() && *packet_id == pi && *claim == original.claim)), "known_started_not_replayed_past_ttl");
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
        let held = owner.pubsub.deliveries.row(0).clone();
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
            owner.pubsub.deliveries.row(0),
            &held,
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
        let deadline = owner.pubsub.deliveries.row(0).in_flight().unwrap().deadline;
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
            let d = owner.pubsub.deliveries.row_mut(0);
            d.expiry = Some(Box::new(message_expiry()));
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
        assert!(owner.qos0_fits(key, &topic, &payload, None, None));
        assert!(
            !owner.qos0_fits(key, &topic, &payload, Some(message_expiry()), None),
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
            let d = owner.pubsub.deliveries.row_mut(0);
            d.expiry = timed.then(|| Box::new(message_expiry()));
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
        assert!(owner.pubsub.deliveries.row(0).retired);
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
        let newer = owner.pubsub.deliveries.row(0).clone();
        apply(
            &mut owner,
            Input::MessagesExpired {
                deliveries: BoundedVec::from_slice(&[expired], 1).unwrap(),
            },
        );
        assert_eq!(owner.pubsub.deliveries.row(0), &newer);
    }

    #[test]
    fn message_expiry_invalid_volatile_deadline_is_refused_before_admission() {
        let mut owner = world();
        message_clock(&mut owner, 10);
        owner
            .push_route(Route {
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
                properties: None,
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
                owner.step_checked(input, TURN),
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
        owner.pubsub.deliveries.row_mut(0).expiry = Some(Box::new(message_expiry()));
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
            properties: None,
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
            let d = owner.pubsub.deliveries.row_mut(0);
            d.expiry = Some(Box::new(message_expiry()));
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
        let result = owner.step_checked(
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
            owner.step_checked(
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
        let result = owner.step_checked(
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
        assert!(!owner.pubsub.deliveries.row(0).local_write_completed);
        assert!(
            !owner.pubsub.deliveries.row(0).claim.unwrap().started,
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
            properties: None,
            topic: bytes(b"topic"),
            payload: bytes(b"payload"),
            claim: None,
        };
        let result = owner.step_checked(
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
                    properties: None,
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
                                properties: None,
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
            .push_route(Route {
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
                    properties: None,
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
        let flight = owner.pubsub.retained.clone().unwrap();
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
            let delivery = owner.pubsub.deliveries.row(0).clone();
            let held = owner
                .outbox
                .reserve(NonZeroUsize::new(owner.outbox.free_slots() - 1).unwrap())
                .unwrap();
            let result = owner.step_checked(
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
            assert_eq!(owner.pubsub.deliveries.row(0), &delivery);
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
                    properties: None,
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
                assert_eq!(effects.iter().any(|(_, e)| matches!(e, Effect::EmitPublish { started: true, packet_id, expiry: Some(expiry), .. } if *packet_id == claim.packet_id && **expiry == message_expiry())), started, "recovery_invented_unsent_or_started_authority");
                assert!(
                    !effects
                        .iter()
                        .any(|(_, e)| matches!(e, Effect::Persist { .. }))
                );
                assert_eq!(owner.pubsub.free_packet_id(TARGET, 1), None);
                assert!(!owner.pubsub.deliveries.row(0).certain_never_started);
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
        let d = owner.pubsub.deliveries.last_mut().unwrap();
        d.qos = qos;
        d.expiry = timed.then(|| Box::new(message_expiry()));
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
            .values()
            .find(|d| d.id == claimed.delivery)
            .expect("the delivery is kept until its finish is known");
        assert!(
            matches!(&held.phase, Phase::DiscardPending(flight) if flight.operation == command.recipe.operation()),
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
        owner.pubsub.deliveries.row_mut(1).target = OTHER;
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
        let left = owner.pubsub.deliveries.to_vec();
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
        let item = owner.pubsub.deliveries.row_mut(0);
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
            owner.pubsub.deliveries.row_mut(0).topic = Some(bytes(&[b't'; 128]));
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
            owner.pubsub.deliveries.row_mut(index as usize).qos = Qos::ExactlyOnce;
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

    /// Regression witness for MQTT-4.6.0-4 / MQTT-4.6.0-6:
    /// Delivery 0 carries a Message Expiry Interval (so `FrameProgress::Started`
    /// places it in `Phase::StartPending` while persisting `MarkDeliveryStarted`).
    /// Delivery 1 has no expiry. When `PUBREC(0)` arrives while delivery 0 is
    /// still `StartPending` and `PUBREC(1)` arrives right after (committing its
    /// `PreparePubrel` before delivery 0's `MarkDeliveryStarted` even commits),
    /// delivery 0 MUST keep its earlier rank from the very first PUBREC, holding
    /// `PUBREL(1)` until `PUBREL(0)` leaves first.
    #[test]
    fn early_pubrec_during_start_pending_preserves_pubrel_order_against_later_pubrec() {
        let mut owner = world();
        let now = MessageTime {
            domain: [1u8; 32],
            tick: 1_000_000_000,
        };
        let _ = apply(&mut owner, Input::MessageClock { now });
        let key = ConnectionKey {
            session: TARGET,
            generation: CONNECTION,
        };

        // Delivery 0: QoS 2 WITH Message Expiry Interval.
        queued(&mut owner, 0);
        {
            let d0 = owner.pubsub.deliveries.row_mut(0);
            d0.qos = Qos::ExactlyOnce;
            d0.expiry = Some(Box::new(MessageExpiry {
                observed: now,
                interval: 60,
            }));
        }
        let claim0 = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        let (id0, Effect::Persist { command: cmd0 }) = &claim0[0] else {
            panic!("claim0")
        };
        let pub0 = known(&mut owner, *id0, cmd0);
        let (
            frame0,
            Effect::EmitPublish {
                packet_id: pid0, ..
            },
        ) = pub0[0]
        else {
            panic!("publish0")
        };

        // Delivery 1: QoS 2 WITHOUT Message Expiry Interval.
        queued(&mut owner, 1);
        owner.pubsub.deliveries.row_mut(1).qos = Qos::ExactlyOnce;
        let claim1 = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        let (id1, Effect::Persist { command: cmd1 }) = &claim1[0] else {
            panic!("claim1")
        };
        let pub1 = known(&mut owner, *id1, cmd1);
        let Effect::EmitPublish {
            packet_id: pid1, ..
        } = pub1[0].1
        else {
            panic!("publish1")
        };

        // Frame 0 begins writing -> triggers MarkDeliveryStarted (Phase::StartPending).
        let start_effects = apply(
            &mut owner,
            Input::FrameResult {
                effect: frame0,
                connection: key,
                progress: FrameProgress::Started,
            },
        );
        let (start_id, Effect::Persist { command: start_cmd }) = &start_effects[0] else {
            panic!("MarkDeliveryStarted expected")
        };
        assert!(matches!(
            start_cmd.recipe.get(),
            Recipe::MarkDeliveryStarted(_)
        ));

        // 1) PUBREC(pid0) arrives FIRST while delivery 0 is still StartPending.
        let target0 = owner.qos2_ack_target(key, pid0, Qos2Ack::Pubrec);
        assert!(target0.is_some());
        let ack0 = apply(
            &mut owner,
            Input::ObservedQos2Ack {
                connection: key,
                kind: Qos2Ack::Pubrec,
                packet_id: pid0,
                reason: 0,
                target: target0,
            },
        );
        assert!(
            ack0.is_empty(),
            "early PUBREC waits for MarkDeliveryStarted"
        );

        // 2) PUBREC(pid1) arrives SECOND and immediately submits PreparePubrel(1).
        let target1 = owner.qos2_ack_target(key, pid1, Qos2Ack::Pubrec);
        assert!(target1.is_some());
        let prep1_effects = apply(
            &mut owner,
            Input::ObservedQos2Ack {
                connection: key,
                kind: Qos2Ack::Pubrec,
                packet_id: pid1,
                reason: 0,
                target: target1,
            },
        );
        let (prep1_id, Effect::Persist { command: prep1_cmd }) = &prep1_effects[0] else {
            panic!("PreparePubrel(1) expected")
        };
        assert!(matches!(prep1_cmd.recipe.get(), Recipe::PreparePubrel(_)));

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

        // 3) PreparePubrel(1) commits BEFORE MarkDeliveryStarted(0)!
        //    PUBREL(1) MUST stay held behind delivery 0's earlier PUBREC rank.
        let mut seen = pubrels(&known(&mut owner, *prep1_id, prep1_cmd));
        seen.extend(pubrels(&apply(
            &mut owner,
            Input::WorkAvailable { session: TARGET },
        )));
        assert_eq!(
            seen,
            Vec::<u16>::new(),
            "PUBREL(1) must not overtake delivery 0 whose PUBREC arrived earlier during StartPending"
        );

        // 4) MarkDeliveryStarted(0) commits -> immediately submits PreparePubrel(0)
        //    preserving delivery 0's rank (1 < 2).
        let started_effects = known(&mut owner, *start_id, start_cmd);
        let (prep0_id, Effect::Persist { command: prep0_cmd }) = &started_effects[0] else {
            panic!("PreparePubrel(0) expected immediately after DeliveryStarted")
        };
        assert!(matches!(prep0_cmd.recipe.get(), Recipe::PreparePubrel(_)));
        // Still waiting for PreparePubrel(0) to commit -> PUBREL(1) must STILL be held.
        seen.extend(pubrels(&apply(
            &mut owner,
            Input::WorkAvailable { session: TARGET },
        )));
        assert_eq!(
            seen,
            Vec::<u16>::new(),
            "PUBREL(1) must remain held while PreparePubrel(0) is in flight"
        );

        // 5) PreparePubrel(0) commits -> PUBREL(0) leaves strictly before PUBREL(1).
        seen.extend(pubrels(&known(&mut owner, *prep0_id, prep0_cmd)));
        for _ in 0..3 {
            seen.extend(pubrels(&apply(
                &mut owner,
                Input::WorkAvailable { session: TARGET },
            )));
        }
        assert_eq!(seen, [pid0.get(), pid1.get()]);

        // 6) Both PUBCOMPs resolve both deliveries cleanly.
        for pid in [pid0, pid1] {
            let target = owner.qos2_ack_target(key, pid, Qos2Ack::Pubcomp);
            assert!(target.is_some());
            let fin = apply(
                &mut owner,
                Input::ObservedQos2Ack {
                    connection: key,
                    kind: Qos2Ack::Pubcomp,
                    packet_id: pid,
                    reason: 0,
                    target,
                },
            );
            let (fin_id, Effect::Persist { command: fin_cmd }) = &fin[0] else {
                panic!("FinishDelivery expected")
            };
            let _ = known(&mut owner, *fin_id, fin_cmd);
        }
        assert!(owner.pubsub.deliveries.is_empty());
    }

    #[test]
    fn inline_claim_commits_emits_rejects_and_handles_toolarge_and_retire() {
        let mut owner = world();
        owner.enable_inline_claims();
        let pub_conn = ConnectionGeneration::new(11);
        let _ = apply(
            &mut owner,
            Input::Packet {
                connection: pub_conn,
                ingress_order: 0,
                packet: InboundPacket::ConnectResume {
                    client_id: bytes(b"other"),
                },
            },
        );
        // Subscribe TARGET to "fast/topic" QoS 1.
        let filters = crate::contract::pubsub::FilterList::copy_from(&[
            crate::contract::pubsub::SubscriptionFilter {
                filter: bytes(b"fast/topic"),
                qos: Qos::AtLeastOnce,
                options: crate::contract::pubsub::SubscriptionOptions::DEFAULT,
            },
        ])
        .unwrap();
        let sub_eff = apply(
            &mut owner,
            Input::Packet {
                connection: CONNECTION,
                ingress_order: 1,
                packet: InboundPacket::Subscribe {
                    denied: 0,
                    packet_id: InboundPacketId::new(1).unwrap(),
                    filters,
                },
            },
        );
        let (sub_id, Effect::Persist { command: sub_cmd }) = &sub_eff[0] else {
            panic!("subscribe persist expected");
        };
        let _ = apply(
            &mut owner,
            Input::StoreResult {
                effect: *sub_id,
                operation: sub_cmd.recipe.operation(),
                reply: StoreReply::Outcome(StoreOutcome::Committed(
                    CommitFacts::SubscriptionsChanged {
                        session: TARGET,
                        route_revision: Revision::new(1),
                    },
                )),
            },
        );

        // Catch TARGET up via one empty WorkPage.
        let scan_eff = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        let (scan_id, Effect::LoadWork { .. }) = &scan_eff[0] else {
            panic!("LoadWork expected");
        };
        let _ = apply(
            &mut owner,
            Input::WorkPage {
                effect: *scan_id,
                session: TARGET,
                items: BoundedVec::with_capacity(0).unwrap(),
                next: None,
            },
        );

        let publish = |owner: &mut Owner, order: u64, pid: u16, body: &[u8]| {
            apply(
                owner,
                Input::Packet {
                    connection: pub_conn,
                    ingress_order: order,
                    packet: InboundPacket::Publish {
                        qos: Qos::AtLeastOnce,
                        retain: false,
                        dup: false,
                        packet_id: Some(InboundPacketId::new(pid).unwrap()),
                        topic: bytes(b"fast/topic"),
                        payload: bytes(body),
                        expiry: None,
                        properties: None,
                    },
                },
            )
        };

        // 1) Two pipelined QoS 1 publishes BOTH become ClaimedPublication (packet_id 1 & 2).
        let p1 = publish(&mut owner, 1, 1, b"m1");
        let (eff1, Effect::Persist { command: cmd1 }) = &p1[0] else {
            panic!("Persist 1 expected");
        };
        let Recipe::ClaimedPublication(cp1) = cmd1.recipe.get() else {
            panic!(
                "expected ClaimedPublication for m1, got {:?}",
                cmd1.recipe.get()
            );
        };
        assert_eq!(cp1.claim.packet_id.get(), 1);
        assert_eq!(
            owner.inline_claim_target(cmd1.recipe.operation()),
            Some(TARGET)
        );

        let p2 = publish(&mut owner, 2, 2, b"m2");
        let (eff2, Effect::Persist { command: cmd2 }) = &p2[0] else {
            panic!("Persist 2 expected");
        };
        let Recipe::ClaimedPublication(cp2) = cmd2.recipe.get() else {
            panic!("expected ClaimedPublication for m2");
        };
        assert_eq!(cp2.claim.packet_id.get(), 2);

        // 2) Commit #2 BEFORE #1: #2's EmitPublish MUST wait behind #1 (strict wire order).
        let res2 = apply(
            &mut owner,
            Input::StoreResult {
                effect: *eff2,
                operation: cmd2.recipe.operation(),
                reply: StoreReply::Outcome(StoreOutcome::Committed(
                    CommitFacts::PublicationAdmitted {
                        publication: cp2.admission.publication,
                        exchange: cp2.admission.source.exchange,
                    },
                )),
            },
        );
        assert!(
            !res2
                .iter()
                .any(|(_, e)| matches!(e, Effect::EmitPublish { .. })),
            "delivery 2 must not emit before delivery 1 commits"
        );

        // 3) Commit #1: emits publisher PUBACK(1), subscriber EmitPublish(pid=1), and releases PUBACK(2)!
        let res1 = apply(
            &mut owner,
            Input::StoreResult {
                effect: *eff1,
                operation: cmd1.recipe.operation(),
                reply: StoreReply::Outcome(StoreOutcome::Committed(
                    CommitFacts::PublicationAdmitted {
                        publication: cp1.admission.publication,
                        exchange: cp1.admission.source.exchange,
                    },
                )),
            },
        );
        assert!(res1.iter().any(|(_, e)| matches!(
            e,
            Effect::EmitPublish { packet_id, payload, .. }
                if packet_id.get() == 1 && payload.as_slice() == b"m1"
        )));
        assert!(
            owner.has_ready_work(TARGET),
            "delivery 2 is now ready to emit"
        );
        let w2 = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        assert!(w2.iter().any(|(_, e)| matches!(
            e,
            Effect::EmitPublish { packet_id, payload, .. }
                if packet_id.get() == 2 && payload.as_slice() == b"m2"
        )));
        assert!(!owner.has_ready_work(TARGET));

        // 4) Rejection of inline-claimed #3 frees its provisional delivery and PacketId(3).
        let p3 = publish(&mut owner, 3, 3, b"m3");
        let (eff3, Effect::Persist { command: cmd3 }) = &p3[0] else {
            panic!("Persist 3 expected");
        };
        let Recipe::ClaimedPublication(cp3) = cmd3.recipe.get() else {
            panic!("expected ClaimedPublication for m3");
        };
        assert_eq!(cp3.claim.packet_id.get(), 3);
        let _ = apply(
            &mut owner,
            Input::StoreResult {
                effect: *eff3,
                operation: cmd3.recipe.operation(),
                reply: StoreReply::Outcome(StoreOutcome::RejectedCertain(
                    RejectReason::PreconditionFailed,
                )),
            },
        );
        assert_eq!(owner.inline_claim_target(cmd3.recipe.operation()), None);
        // Next publish reuses PacketId(3) because #3 was rolled back cleanly.
        let p4 = publish(&mut owner, 4, 4, b"m4");
        let (eff4, Effect::Persist { command: cmd4 }) = &p4[0] else {
            panic!("Persist 4 expected");
        };
        let Recipe::ClaimedPublication(cp4) = cmd4.recipe.get() else {
            panic!("expected ClaimedPublication for m4");
        };
        assert_eq!(cp4.claim.packet_id.get(), 3);

        // 5) Retiring TARGET while #4 is InlineClaimPending tracks retired_flights == 1 -> 0 on result.
        owner.pubsub.retire_session(TARGET);
        assert_eq!(owner.pubsub.retired_flights(), 1);
        let res4 = apply(
            &mut owner,
            Input::StoreResult {
                effect: *eff4,
                operation: cmd4.recipe.operation(),
                reply: StoreReply::Outcome(StoreOutcome::Committed(
                    CommitFacts::PublicationAdmitted {
                        publication: cp4.admission.publication,
                        exchange: cp4.admission.source.exchange,
                    },
                )),
            },
        );
        assert!(
            !res4
                .iter()
                .any(|(_, e)| matches!(e, Effect::EmitPublish { .. }))
        );
        assert_eq!(owner.pubsub.retired_flights(), 0);
    }

    #[test]
    fn multi_target_inline_claim_full_partial_rollback_and_retire() {
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
            max_sessions: nonzero(4),
            max_routes: nonzero(4),
            max_exchanges: nonzero(16),
            session_qos2_exchanges: NonZeroU32::MAX,
            account: AccountId::new(1),
            payload_bytes: PayloadBytes::new(4096),
            resolution_delay: LogicalDuration::new(1),
            retained_operations: nonzero(16),
            remembered_connections: nonzero(8),
        };
        let mut owner = Owner::new(config, outbox).unwrap();
        owner.enable_inline_claims();
        let s3 = SessionId::new(30);
        let s4 = SessionId::new(40);
        let c3 = ConnectionGeneration::new(30);
        let c4 = ConnectionGeneration::new(40);
        let pub_conn = ConnectionGeneration::new(11);
        let facts = [
            RecoveredFact::Epoch { incarnation, epoch },
            RecoveredFact::Limits {
                max_targets_per_admission: 4,
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
            RecoveredFact::Session {
                session: s3,
                client_id: bytes(b"sub-3"),
                revision: Revision::new(1),
                expiry: crate::contract::session::SessionExpiry::Never,
                opened_by: None,
                detached: None,
            },
            RecoveredFact::Session {
                session: s4,
                client_id: bytes(b"sub-4"),
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
                    last: true,
                }
            )
            .is_empty()
        );
        for (name, conn) in [
            (b"subscriber".as_slice(), CONNECTION),
            (b"sub-3".as_slice(), c3),
            (b"sub-4".as_slice(), c4),
        ] {
            let _ = apply(
                &mut owner,
                Input::Packet {
                    connection: conn,
                    ingress_order: 0,
                    packet: InboundPacket::ConnectResume {
                        client_id: bytes(name),
                    },
                },
            );
            let _ = apply(
                &mut owner,
                Input::ConnectionSettings {
                    connection: conn,
                    receive_maximum: 4,
                    maximum_packet_size: 4_096,
                },
            );
        }
        let _ = apply(
            &mut owner,
            Input::Packet {
                connection: pub_conn,
                ingress_order: 0,
                packet: InboundPacket::ConnectResume {
                    client_id: bytes(b"other"),
                },
            },
        );

        let subscribe =
            |owner: &mut Owner, conn: ConnectionGeneration, sess: SessionId, rev: u64, qos: Qos| {
                let filters = crate::contract::pubsub::FilterList::copy_from(&[
                    crate::contract::pubsub::SubscriptionFilter {
                        filter: bytes(b"fan/out"),
                        qos,
                        options: crate::contract::pubsub::SubscriptionOptions::DEFAULT,
                    },
                ])
                .unwrap();
                let sub_eff = apply(
                    owner,
                    Input::Packet {
                        connection: conn,
                        ingress_order: 1,
                        packet: InboundPacket::Subscribe {
                            denied: 0,
                            packet_id: InboundPacketId::new(1).unwrap(),
                            filters,
                        },
                    },
                );
                let (sub_id, Effect::Persist { command: sub_cmd }) = &sub_eff[0] else {
                    panic!("subscribe persist expected");
                };
                let _ = apply(
                    owner,
                    Input::StoreResult {
                        effect: *sub_id,
                        operation: sub_cmd.recipe.operation(),
                        reply: StoreReply::Outcome(StoreOutcome::Committed(
                            CommitFacts::SubscriptionsChanged {
                                session: sess,
                                route_revision: Revision::new(rev),
                            },
                        )),
                    },
                );
            };
        let catch_up = |owner: &mut Owner, sess: SessionId| {
            let scan_eff = apply(owner, Input::WorkAvailable { session: sess });
            let (scan_id, Effect::LoadWork { .. }) = &scan_eff[0] else {
                panic!("LoadWork expected");
            };
            let _ = apply(
                owner,
                Input::WorkPage {
                    effect: *scan_id,
                    session: sess,
                    items: BoundedVec::with_capacity(0).unwrap(),
                    next: None,
                },
            );
        };

        subscribe(&mut owner, CONNECTION, TARGET, 1, Qos::AtLeastOnce);
        subscribe(&mut owner, c3, s3, 2, Qos::AtLeastOnce);
        subscribe(&mut owner, c4, s4, 3, Qos::AtLeastOnce);
        catch_up(&mut owner, TARGET);
        catch_up(&mut owner, s3);
        // Leave s4 NOT caught up first to test partial multi-claim (2 of 3 targets!).

        let publish = |owner: &mut Owner, order: u64, pid: u16, body: &[u8]| {
            apply(
                owner,
                Input::Packet {
                    connection: pub_conn,
                    ingress_order: order,
                    packet: InboundPacket::Publish {
                        qos: Qos::AtLeastOnce,
                        retain: false,
                        dup: false,
                        packet_id: Some(InboundPacketId::new(pid).unwrap()),
                        topic: bytes(b"fan/out"),
                        payload: bytes(body),
                        expiry: None,
                        properties: None,
                    },
                },
            )
        };

        // 1) Partial MultiClaimedPublication: TARGET and s3 claimed inline, s4 remains unclaimed.
        let p1 = publish(&mut owner, 1, 1, b"partial");
        let (eff1, Effect::Persist { command: cmd1 }) = &p1[0] else {
            panic!("Persist 1 expected");
        };
        let Recipe::MultiClaimedPublication(mc1) = cmd1.recipe.get() else {
            panic!(
                "expected MultiClaimedPublication, got {:?}",
                cmd1.recipe.get()
            );
        };
        assert_eq!(mc1.admission.targets.len(), 3);
        assert_eq!(mc1.claims.len(), 2);
        assert_eq!(owner.inline_claim_targets(cmd1.recipe.operation()).len(), 2);
        assert!(owner.pubsub.non_inline_by_session.contains_key(&s4));
        assert!(!owner.pubsub.non_inline_by_session.contains_key(&TARGET));
        assert!(!owner.pubsub.non_inline_by_session.contains_key(&s3));

        let res1 = apply(
            &mut owner,
            Input::StoreResult {
                effect: *eff1,
                operation: cmd1.recipe.operation(),
                reply: StoreReply::Outcome(StoreOutcome::Committed(
                    CommitFacts::PublicationAdmitted {
                        publication: mc1.admission.publication,
                        exchange: mc1.admission.source.exchange,
                    },
                )),
            },
        );
        // One claimed target emits directly on PublicationAdmitted; the other claimed target
        // emits on its WorkAvailable with ZERO LoadWork/ClaimDelivery, while s4 is marked dirty.
        let direct_emits = res1
            .iter()
            .filter(|(_, e)| matches!(e, Effect::EmitPublish { .. }))
            .count();
        assert_eq!(direct_emits, 1);
        assert!(owner.last_inline_ready_target().is_some());
        let mut ready_targets = alloc::vec::Vec::new();
        if let Some(first) = owner.last_inline_ready_target() {
            ready_targets.push(first);
        }
        ready_targets.extend_from_slice(owner.last_inline_ready_extra());
        assert!(ready_targets.contains(&s4));
        // Pump the second claimed target (whichever of TARGET/s3 did not emit directly).
        let mut second_emitted = 0usize;
        for sess in [TARGET, s3] {
            if owner.has_ready_work(sess) {
                let w = apply(&mut owner, Input::WorkAvailable { session: sess });
                assert!(w.iter().any(|(_, e)| matches!(
                    e,
                    Effect::EmitPublish { payload, .. } if payload.as_slice() == b"partial"
                )));
                second_emitted += 1;
            }
        }
        assert_eq!(second_emitted, 1);

        // 2) Catch s4 up; now a publish becomes a FULL 3-of-3 MultiClaimedPublication!
        catch_up(&mut owner, s4);
        let p2 = publish(&mut owner, 2, 2, b"full-3");
        let (eff2, Effect::Persist { command: cmd2 }) = &p2[0] else {
            panic!("Persist 2 expected");
        };
        let Recipe::MultiClaimedPublication(mc2) = cmd2.recipe.get() else {
            panic!("expected full MultiClaimedPublication");
        };
        assert_eq!(mc2.claims.len(), 3);
        assert!(owner.pubsub.non_inline_by_session.is_empty());

        // 3) Reject p2 -> all 3 provisional deliveries roll back cleanly, freeing packet IDs.
        let _ = apply(
            &mut owner,
            Input::StoreResult {
                effect: *eff2,
                operation: cmd2.recipe.operation(),
                reply: StoreReply::Outcome(StoreOutcome::RejectedCertain(
                    RejectReason::PreconditionFailed,
                )),
            },
        );
        assert!(
            owner
                .inline_claim_targets(cmd2.recipe.operation())
                .is_empty()
        );

        // 4) Next publish claims all 3 again; retire s3 mid-flight -> TARGET & s4 commit, s3 drops.
        let p3 = publish(&mut owner, 3, 3, b"retire-one");
        let (eff3, Effect::Persist { command: cmd3 }) = &p3[0] else {
            panic!("Persist 3 expected");
        };
        let Recipe::MultiClaimedPublication(mc3) = cmd3.recipe.get() else {
            panic!("expected MultiClaimedPublication for p3");
        };
        assert_eq!(mc3.claims.len(), 3);
        owner.pubsub.retire_session(s3);
        assert_eq!(owner.pubsub.retired_flights(), 1);
        let res3 = apply(
            &mut owner,
            Input::StoreResult {
                effect: *eff3,
                operation: cmd3.recipe.operation(),
                reply: StoreReply::Outcome(StoreOutcome::Committed(
                    CommitFacts::PublicationAdmitted {
                        publication: mc3.admission.publication,
                        exchange: mc3.admission.source.exchange,
                    },
                )),
            },
        );
        assert_eq!(owner.pubsub.retired_flights(), 0);
        let mut delivered_p3 = res3
            .iter()
            .filter(|(_, e)| matches!(e, Effect::EmitPublish { payload, .. } if payload.as_slice() == b"retire-one"))
            .count();
        for sess in [TARGET, s4] {
            if owner.has_ready_work(sess) {
                let w = apply(&mut owner, Input::WorkAvailable { session: sess });
                delivered_p3 += w
                    .iter()
                    .filter(|(_, e)| matches!(e, Effect::EmitPublish { payload, .. } if payload.as_slice() == b"retire-one"))
                    .count();
            }
        }
        assert_eq!(delivered_p3, 2);
    }

    #[test]
    fn inline_preclaim_burst_respects_receive_maximum_and_emits_fifo() {
        let mut owner = world();
        owner.enable_inline_claims();
        owner.pubsub.connections.as_mut_slice()[0].receive_maximum = 2;
        let pub_conn = ConnectionGeneration::new(11);
        let _ = apply(
            &mut owner,
            Input::Packet {
                connection: pub_conn,
                ingress_order: 0,
                packet: InboundPacket::ConnectResume {
                    client_id: bytes(b"other"),
                },
            },
        );
        let filters = crate::contract::pubsub::FilterList::copy_from(&[
            crate::contract::pubsub::SubscriptionFilter {
                filter: bytes(b"burst/topic"),
                qos: Qos::AtLeastOnce,
                options: crate::contract::pubsub::SubscriptionOptions::DEFAULT,
            },
        ])
        .unwrap();
        let sub_eff = apply(
            &mut owner,
            Input::Packet {
                connection: CONNECTION,
                ingress_order: 1,
                packet: InboundPacket::Subscribe {
                    denied: 0,
                    packet_id: InboundPacketId::new(1).unwrap(),
                    filters,
                },
            },
        );
        let (sub_id, Effect::Persist { command: sub_cmd }) = &sub_eff[0] else {
            panic!("subscribe persist expected");
        };
        let _ = apply(
            &mut owner,
            Input::StoreResult {
                effect: *sub_id,
                operation: sub_cmd.recipe.operation(),
                reply: StoreReply::Outcome(StoreOutcome::Committed(
                    CommitFacts::SubscriptionsChanged {
                        session: TARGET,
                        route_revision: Revision::new(1),
                    },
                )),
            },
        );
        let scan_eff = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        let (scan_id, Effect::LoadWork { .. }) = &scan_eff[0] else {
            panic!("LoadWork expected");
        };
        let _ = apply(
            &mut owner,
            Input::WorkPage {
                effect: *scan_id,
                session: TARGET,
                items: BoundedVec::with_capacity(0).unwrap(),
                next: None,
            },
        );

        let publish = |owner: &mut Owner, seq: u16| {
            apply(
                owner,
                Input::Packet {
                    connection: pub_conn,
                    ingress_order: u64::from(seq),
                    packet: InboundPacket::Publish {
                        qos: Qos::AtLeastOnce,
                        retain: false,
                        dup: false,
                        packet_id: Some(InboundPacketId::new(seq).unwrap()),
                        topic: bytes(b"burst/topic"),
                        payload: bytes(&[b'0' + seq as u8]),
                        expiry: None,
                        properties: None,
                    },
                },
            )
        };

        // RM = 2 -> preclaim_cap = 4: first 4 publishes are ClaimedPublication,
        // 5th falls back to AdmitPublication without breaking strict FIFO or RM=2.
        let mut admitted = alloc::vec::Vec::new();
        for seq in 1..=4u16 {
            let effs = publish(&mut owner, seq);
            let (eff_id, Effect::Persist { command }) = &effs[0] else {
                panic!("persist expected for {seq}");
            };
            let Recipe::ClaimedPublication(cp) = command.recipe.get() else {
                panic!(
                    "expected ClaimedPublication for {seq}, got {:?}",
                    command.recipe.get()
                );
            };
            assert_eq!(cp.claim.packet_id.get(), seq);
            assert!(
                owner.pubsub.active(CONNECTION) <= 2,
                "active window exceeded receive_maximum=2 after inline submit {seq}"
            );
            admitted.push((
                *eff_id,
                command.recipe.operation(),
                cp.admission.publication,
                cp.admission.source.exchange,
            ));
        }
        // 5th publish exceeds preclaim_cap=4 -> AdmitPublication (non-inline fallback).
        let eff5 = publish(&mut owner, 5);
        let (_, Effect::Persist { command: cmd5 }) = &eff5[0] else {
            panic!("persist expected for 5");
        };
        assert!(
            matches!(cmd5.recipe.get(), Recipe::AdmitPublication(_)),
            "5th publish should fall back to AdmitPublication"
        );

        // Commit all 4 ClaimedPublications: ONLY #1 and #2 may emit on the wire!
        let mut emitted = alloc::vec::Vec::new();
        for (eff_id, op, pub_id, exch) in admitted {
            let res = apply(
                &mut owner,
                Input::StoreResult {
                    effect: eff_id,
                    operation: op,
                    reply: StoreReply::Outcome(StoreOutcome::Committed(
                        CommitFacts::PublicationAdmitted {
                            publication: pub_id,
                            exchange: exch,
                        },
                    )),
                },
            );
            for (_, e) in res {
                if let Effect::EmitPublish {
                    packet_id, payload, ..
                } = e
                {
                    emitted.push((packet_id.get(), payload.as_slice()[0]));
                }
            }
            assert!(
                owner.pubsub.active(CONNECTION) <= 2,
                "active window exceeded receive_maximum=2 on commit"
            );
        }
        assert_eq!(emitted, alloc::vec![(1, b'1'), (2, b'2')]);
        // Ready work must be false while Receive Maximum is full (no spin!).
        assert!(!owner.has_ready_work(TARGET));

        let key = ConnectionKey {
            session: TARGET,
            generation: CONNECTION,
        };
        // Acknowledge #1 -> FinishDelivery commits -> #3 becomes ready and emits in strict FIFO!
        let pid1 = OutboundPacketId::new(1).unwrap();
        let target1 = owner.puback_target(key, pid1).unwrap();
        let ack1 = apply(
            &mut owner,
            Input::ObservedPuback {
                connection: key,
                packet_id: pid1,
                reason: 0,
                target: Some(target1),
            },
        );
        let (fin1_id, Effect::Persist { command: fin1_cmd }) = &ack1[0] else {
            panic!("FinishDelivery 1 expected");
        };
        let Recipe::FinishDelivery(fin1) = fin1_cmd.recipe.get() else {
            panic!("expected FinishDelivery recipe");
        };
        let _ = apply(
            &mut owner,
            Input::StoreResult {
                effect: *fin1_id,
                operation: fin1_cmd.recipe.operation(),
                reply: StoreReply::Outcome(StoreOutcome::Committed(
                    CommitFacts::DeliveryFinished {
                        delivery: fin1.delivery,
                        target: fin1.target,
                        packet_id: fin1.packet_id,
                        claim: fin1.claim,
                    },
                )),
            },
        );
        assert!(owner.has_ready_work(TARGET));
        let w3 = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        let mut emitted_after = alloc::vec::Vec::new();
        for (_, e) in w3 {
            if let Effect::EmitPublish {
                packet_id, payload, ..
            } = e
            {
                emitted_after.push((packet_id.get(), payload.as_slice()[0]));
            }
        }
        assert_eq!(emitted_after, alloc::vec![(3, b'3')]);
        assert_eq!(owner.pubsub.active(CONNECTION), 2);
        assert!(!owner.has_ready_work(TARGET));

        // Acknowledge #2 -> FinishDelivery commits -> #4 becomes ready and emits in strict FIFO!
        let pid2 = OutboundPacketId::new(2).unwrap();
        let target2 = owner.puback_target(key, pid2).unwrap();
        let ack2 = apply(
            &mut owner,
            Input::ObservedPuback {
                connection: key,
                packet_id: pid2,
                reason: 0,
                target: Some(target2),
            },
        );
        let (fin2_id, Effect::Persist { command: fin2_cmd }) = &ack2[0] else {
            panic!("FinishDelivery 2 expected");
        };
        let Recipe::FinishDelivery(fin2) = fin2_cmd.recipe.get() else {
            panic!("expected FinishDelivery recipe");
        };
        let _ = apply(
            &mut owner,
            Input::StoreResult {
                effect: *fin2_id,
                operation: fin2_cmd.recipe.operation(),
                reply: StoreReply::Outcome(StoreOutcome::Committed(
                    CommitFacts::DeliveryFinished {
                        delivery: fin2.delivery,
                        target: fin2.target,
                        packet_id: fin2.packet_id,
                        claim: fin2.claim,
                    },
                )),
            },
        );
        assert!(owner.has_ready_work(TARGET));
        let w4 = apply(&mut owner, Input::WorkAvailable { session: TARGET });
        let mut emitted_4 = alloc::vec::Vec::new();
        for (_, e) in w4 {
            if let Effect::EmitPublish {
                packet_id, payload, ..
            } = e
            {
                emitted_4.push((packet_id.get(), payload.as_slice()[0]));
            }
        }
        assert_eq!(emitted_4, alloc::vec![(4, b'4')]);
        assert_eq!(owner.pubsub.active(CONNECTION), 2);
        assert!(!owner.has_ready_work(TARGET));
    }
}
