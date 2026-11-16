//! Exact subscriptions and outgoing QoS 1 under the same owner authority.
use super::*;
use crate::contract::command::{
    ClaimDelivery, ConsumeDelivery, FinishDelivery, UpdateSubscriptions,
};
use crate::contract::ids::{ContinuationId, OutboundPacketId};
use crate::contract::pubsub::{
    DeliveryClaim, FilterResults, GrantedSubscriptionChange, MAX_FILTERS, PubackTarget,
    Qos0DropReason, Qos0Origin, VolatilePublicationId, WORK_PAGE_LIMIT, WorkItem,
};

// Isolated transition mutants. Production builds retain these false values.
const MUTANT_EARLY_PI_RELEASE: bool = false;

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
enum Phase {
    Queued,
    ClaimPending(InFlight),
    AwaitPuback,
    FinishPending(InFlight),
    ConsumePending(InFlight, ConnectionKey),
    Qos0Writing(Finished),
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub(super) struct Delivery {
    id: DeliveryId,
    target: SessionId,
    revision: Revision,
    qos: Qos,
    claim: Option<DeliveryClaim>,
    topic: Option<SharedBytes>,
    payload: Option<SharedBytes>,
    phase: Phase,
    claim_result: Option<Finished>,
    window: Option<ConnectionGeneration>,
    frame: Option<(EffectId, ConnectionKey)>,
    local_write_completed: bool,
    may_have_started: bool,
}

impl Delivery {
    fn waiting_to_send(&self, generation: ConnectionGeneration) -> bool {
        match self.phase {
            Phase::FinishPending(_) | Phase::Qos0Writing(_) => false,
            Phase::AwaitPuback => {
                self.window != Some(generation)
                    || self
                        .frame
                        .is_none_or(|(_, key)| key.generation != generation)
            }
            Phase::Queued | Phase::ClaimPending(_) | Phase::ConsumePending(_, _) => true,
        }
    }
    fn in_flight(&self) -> Option<InFlight> {
        match self.phase {
            Phase::ClaimPending(f) | Phase::FinishPending(f) | Phase::ConsumePending(f, _) => {
                Some(f)
            }
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
    frame: Option<EffectId>,
}

#[derive(Debug)]
pub(super) struct State {
    pub(super) connections: BoundedVec<Connection>,
    pub(super) deliveries: BoundedVec<Delivery>,
    pub(super) control: Option<Control>,
    payload_limit: PayloadBytes,
    pub(super) volatile: BoundedVec<Volatile>,
    pub(super) continuation: Option<ContinuationId>,
    pub(super) continuations_started: bool,
}

#[derive(Debug)]
pub(super) enum Change {
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
}

impl State {
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
            if !self.deliveries.as_slice().iter().any(|delivery| {
                Self::reserves_packet_id(delivery, session)
                    && delivery
                        .claim
                        .is_some_and(|claim| claim.packet_id == packet_id)
            }) {
                return Some(packet_id);
            }
        }
        None
    }

    pub(super) fn work_bound(&self, input: &Input, routes: usize) -> u64 {
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
        if !new && self.connections.is_empty() && self.control.is_none() && self.volatile.is_empty()
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
            }
        }
    }
    pub(super) fn deadline_is_live(&self, token: DeadlineToken) -> bool {
        self.control
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
fn exact(topic: &SharedBytes) -> bool {
    !topic.is_empty()
        && !topic.as_slice().contains(&b'+')
        && !topic.as_slice().contains(&b'#')
        && !topic.as_slice().starts_with(b"$share/")
}

impl Owner {
    /// Whether a bounded work wake-up can advance already known online work.
    /// Empty scans are deliberately excluded; external notifications and the
    /// periodic fallback supply the authority to start a new discovery scan.
    #[must_use]
    pub fn has_ready_work(&self, session: SessionId) -> bool {
        if self.recovery.is_some() {
            return false;
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
            .filter(|d| d.target == session && d.waiting_to_send(generation))
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
        } = packet
        else {
            return Self::plan_close(generation, Some(session), CloseReason::ProtocolError, draft);
        };
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
        let count = self
            .routes
            .as_slice()
            .iter()
            .filter(|r| r.topic == *topic)
            .count();
        let used = self
            .pubsub
            .volatile
            .as_slice()
            .iter()
            .try_fold(0u64, |sum, v| sum.checked_add(v.payload.len() as u64))
            .ok_or(IMPOSSIBLE)?;
        let fits = count <= self.pubsub.volatile.capacity() - self.pubsub.volatile.len()
            && (payload.len() as u64)
                .checked_mul(count as u64)
                .and_then(|n| n.checked_add(used))
                .is_some_and(|n| n <= self.pubsub.payload_limit.get());
        if count == 0 || !fits {
            draft.emit(Effect::Qos0Dropped {
                origin,
                target: None,
                reason: if count == 0 {
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
        for route in self.routes.as_slice().iter().filter(|r| r.topic == *topic) {
            let connection = self
                .connection_of(route.target)
                .map(|generation| ConnectionKey {
                    session: route.target,
                    generation,
                });
            records
                .try_push(Volatile {
                    origin,
                    target: route.target,
                    connection,
                    topic: topic.clone(),
                    payload: payload.clone(),
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
            .and_then(|n| n.checked_add(3))
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
        let reason = match v.connection {
            None => Some(Qos0DropReason::DestinationAbsent),
            Some(key) if self.connection_of(v.target) != Some(key.generation) => {
                Some(Qos0DropReason::ConnectionClosed)
            }
            Some(key) if !self.qos0_fits(key, &v.topic, &v.payload) => {
                Some(Qos0DropReason::PacketTooLarge)
            }
            _ => None,
        };
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
            topic: v.topic.clone(),
            payload: v.payload.clone(),
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
                    && delivery.phase == Phase::AwaitPuback
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
        let recipe = Recipe::UpdateSubscriptions(UpdateSubscriptions {
            operation,
            session: session.id,
            session_revision: session.revision,
            route_revision: self.route_revision.ok_or(IMPOSSIBLE)?,
            changes,
        });
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
                    let allowed = exact(&item.filter);
                    codes[len] = if allowed {
                        item.qos.min(Qos::AtLeastOnce).to_wire()
                    } else if item.filter.as_slice().starts_with(b"$share/") {
                        0x9e
                    } else if !exact(&item.filter) {
                        0xa2
                    } else {
                        0x83
                    };
                    len += 1;
                    if allowed {
                        changes
                            .try_push(GrantedSubscriptionChange {
                                topic: item.filter.clone(),
                                granted: Some(item.qos.min(Qos::AtLeastOnce)),
                            })
                            .map_err(|_| IMPOSSIBLE)?;
                    }
                }
            }
            InboundPacket::Unsubscribe { filters, .. } => {
                for topic in filters.as_slice() {
                    let allowed = exact(topic);
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
                        0x83
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

    pub(super) fn plan_work(
        &self,
        session: SessionId,
        draft: &mut Draft,
    ) -> Result<super::Change, Stop> {
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
            .filter(|(_, d)| d.target == session && d.waiting_to_send(generation))
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
                if item.claim.is_some() && known.claim != item.claim {
                    return Err(IMPOSSIBLE);
                }
            } else {
                added += 1;
            }
            if known.is_none_or(|d| d.payload.is_none()) {
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

    fn plan_delivery_emit(
        &self,
        delivery: &Delivery,
        draft: &mut Draft,
    ) -> Result<Option<(EffectId, ConnectionKey)>, Stop> {
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
                && earlier.waiting_to_send(generation)
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
            .and_then(|n| n.checked_add(5))
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
                    let reason = if self.connection_of(d.target) != Some(connection.generation) {
                        Some(Qos0DropReason::ConnectionClosed)
                    } else if !self.qos0_fits(connection, topic, payload) {
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
                                topic: topic.clone(),
                                payload: payload.clone(),
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

    pub(super) fn plan_pubsub_frame(
        &self,
        effect: EffectId,
        connection: ConnectionKey,
        progress: FrameProgress,
    ) -> Option<super::Change> {
        if !matches!(progress,FrameProgress::Written{confirmed,total} if total>0&&confirmed==total)
        {
            return None;
        }
        self.pubsub
            .deliveries
            .as_slice()
            .iter()
            .position(|d| d.frame == Some((effect, connection)))
            .map(|slot| changed(Change::Written { slot }))
    }

    pub(super) fn valid_outbound_fact(
        &self,
        delivery: DeliveryId,
        target: SessionId,
        revision: Revision,
        kind: (Qos, Option<DeliveryClaim>),
        same: &[RecoveredFact],
        sessions: &[RecoveredFact],
    ) -> bool {
        let (qos, claim) = kind;
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
        qos: Qos,
        claim: Option<DeliveryClaim>,
    ) -> Result<(), CoreFault> {
        self.pubsub
            .deliveries
            .try_push(Delivery::metadata(id, target, revision, qos, claim))
            .map_err(|_| CoreFault::StalePlan)
    }

    pub(super) fn apply_pubsub(&mut self, change: Change, input: Input) -> Result<(), CoreFault> {
        let stale = CoreFault::StalePlan;
        match change {
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
                                d.topic = Some(item.topic.clone());
                                d.payload = Some(item.payload.clone());
                            }
                        } else {
                            let mut d = Delivery::metadata(
                                item.delivery,
                                item.target,
                                item.target_revision,
                                item.qos,
                                item.claim,
                            );
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
            Change::Written { slot } => {
                self.pubsub
                    .deliveries
                    .as_mut_slice()
                    .get_mut(slot)
                    .ok_or(stale)?
                    .local_write_completed = true
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
            },
            RecoveredFact::Session {
                session: SessionId::new(2),
                client_id: bytes(b"other"),
                revision: Revision::new(1),
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
}
