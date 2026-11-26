//! Pure deterministic hooks for multi-owner Delivery Shards (Lot 10).
//!
//! A Coordinator owner (`LogicalOwnerId(0)`) owns ingress, routing, sessions,
//! wills, and publication admission. Outgoing delivery work (QoS 1/2 emit,
//! PUBACK/PUBREC/PUBCOMP handling, `LoadWork`/`ClaimDelivery`/`FinishDelivery`,
//! and volatile QoS 0 pacing) may be delegated to resident `DeliveryShard`
//! `Owner` instances (`LogicalOwnerId(1..=K)`), each backed by its own
//! disjoint `Outbox` and `ReceiptOrigin`.
//!
//! All types in this module remain `no_std + alloc` and pure deterministic:
//! no threads, atomics, sockets, or clocks live in `mqttiumd-core`.

use alloc::boxed::Box;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::num::NonZeroUsize;

use super::connections::Connection;
use super::deliveries::{Delivery, Phase};
use super::pubsub::{Emission, Volatile};
use super::{Draft, Effects, Limits, Owner, OwnerConfig, OwnerRefused, Stop, draw};
use crate::contract::capacity::{CapacityNeed, PayloadBytes, Resource, WakeCondition};
use crate::contract::effects::{Effect, EffectSink, Input, Outbox};
use crate::contract::ids::{
    ConnectionGeneration, ConnectionKey, ContinuationId, DeliveryId, OperationId, OutboundPacketId,
    Revision, SessionId,
};
use crate::contract::message_expiry::MessageExpiry;
use crate::contract::packet::{Qos, SharedBytes};
use crate::contract::properties::PublishProperties;
use crate::contract::pubsub::{DeliveryClaim, Qos0DropReason, Qos0Origin};
use crate::contract::session::SessionExpiry;
use crate::contract::step::{CoreFault, Exhaustion, Progress, StepOutcome, TurnGrant};

/// Recovered store limits needed to construct a standalone `DeliveryShard`
/// owner without running a recovery inventory pass.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ShardLimits {
    pub max_targets: u32,
    pub max_topic_bytes: u32,
    pub max_payload_bytes: PayloadBytes,
}

/// Progress required for this generation's already observed delivery ACKs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShardAckProgress {
    Settled,
    Waiting,
    WorkReady,
}

/// External lease oracle consulted by a Coordinator owner when outgoing
/// deliveries are delegated to separate resident `DeliveryShard` owners.
///
/// The trait is pure and synchronous (`no_std + alloc`); production runtimes
/// implement it with lock-free per-session lease slots, while deterministic
/// tests may supply an in-memory oracle.
pub trait InlineClaimOracle {
    /// Attempts to reserve an inline claim slot and a free outbound Packet
    /// Identifier (`1..=512`) for `session` to receive a live QoS 1/2
    /// publication of `payload_bytes`.
    fn try_reserve(
        &mut self,
        session: SessionId,
        qos: Qos,
        payload_bytes: u64,
    ) -> Option<(ConnectionGeneration, OutboundPacketId)>;

    /// Marks `session` ineligible for further inline claims until the owning
    /// shard completes a store catch-up scan and re-publishes eligibility.
    /// Invoked only when an admission actually applies (never on a refused plan).
    fn mark_ineligible(&mut self, session: SessionId);

    /// Undoes a provisional reservation made during a transition plan that was
    /// refused before applying or whose store admission returned
    /// `RejectedCertain`.
    fn rollback(&mut self, session: SessionId, packet_id: OutboundPacketId);
}

/// One inline-claimed delivery handed from the Coordinator owner to the
/// target session's owning `DeliveryShard` after `PublicationAdmitted` commits.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct DelegatedClaim {
    pub delivery: DeliveryId,
    pub target: SessionId,
    pub target_revision: Revision,
    pub qos: Qos,
    pub claim_operation: OperationId,
    pub packet_id: OutboundPacketId,
    pub generation: ConnectionGeneration,
}

/// All inline-claimed deliveries belonging to one committed (or rejected)
/// Coordinator publication admission.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DelegatedInlineBatch {
    pub operation: OperationId,
    pub topic: SharedBytes,
    pub payload: SharedBytes,
    pub properties: Option<Arc<PublishProperties>>,
    pub claims: Vec<DelegatedClaim>,
}

/// One target selected by the Coordinator for a live volatile QoS 0 publish.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct DelegatedQos0Target {
    pub target: SessionId,
    pub connection: Option<ConnectionKey>,
}

/// Routed volatile QoS 0 publication handed from the Coordinator to delivery
/// shards for per-connection windowing and emission.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DelegatedQos0Batch {
    pub origin: Qos0Origin,
    pub topic: SharedBytes,
    pub payload: SharedBytes,
    pub expiry: Option<MessageExpiry>,
    pub properties: Option<Arc<PublishProperties>>,
    pub targets: Vec<DelegatedQos0Target>,
}

/// Outcome of a direct shard-only step (`step_shard_inline_committed` or
/// `step_shard_qos0`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ShardStepOutcome {
    /// Applied completely; `accepted` counts effects delivered immediately to
    /// `sink`, and `emitted` indicates whether an outbound frame effect was
    /// produced in this step.
    Applied { accepted: usize, emitted: bool },
    /// Refused without mutating state because a local bound was reached.
    NeedCapacity(CapacityNeed),
    /// Refused because an identity counter or outbox reservation table ran out.
    Exhausted(Exhaustion),
    /// Internal invariant violation.
    Fatal(CoreFault),
}

impl Owner {
    /// Constructs a resident `DeliveryShard` owner that already considers
    /// recovery closed and uses `limits` discovered by the Coordinator.
    #[expect(
        clippy::result_large_err,
        reason = "a refusal hands the box back by value, matching Owner::new"
    )]
    pub fn new_delivery_shard(
        config: OwnerConfig,
        outbox: Outbox,
        limits: ShardLimits,
    ) -> Result<Self, OwnerRefused> {
        let mut owner = Self::new(config, outbox)?;
        owner.recovery = None;
        owner.epoch_recovered = true;
        owner.limits = Some(Limits {
            max_targets: limits.max_targets,
            max_topic_bytes: limits.max_topic_bytes,
            max_payload_bytes: limits.max_payload_bytes,
        });
        owner.route_revision = Some(Revision::new(0));
        owner.inline_claim_enabled = true;
        Ok(owner)
    }

    /// Returns the recovered store limits of this owner, if recovery has
    /// supplied them.
    #[must_use]
    pub fn shard_limits(&self) -> Option<ShardLimits> {
        self.limits.map(|l| ShardLimits {
            max_targets: l.max_targets,
            max_topic_bytes: l.max_topic_bytes,
            max_payload_bytes: l.max_payload_bytes,
        })
    }

    /// Configures this Coordinator owner to delegate outgoing deliveries
    /// (inline QoS 1/2 claims and volatile QoS 0 batches) to external
    /// `DeliveryShard` owners.
    pub fn enable_delegated_deliveries(&mut self) {
        self.delegated_deliveries = true;
    }

    /// Whether this owner delegates outgoing deliveries to external shards.
    #[must_use]
    pub fn delegated_deliveries_enabled(&self) -> bool {
        self.delegated_deliveries
    }

    /// Applies `input` with an external `InlineClaimOracle`, without immediate
    /// sink dispatch.
    pub fn step_with_oracle(
        &mut self,
        input: Input,
        grant: TurnGrant,
        oracle: &mut dyn InlineClaimOracle,
    ) -> StepOutcome {
        self.step_with_sink_and_oracle(input, grant, None, Some(oracle))
            .0
    }

    /// Applies `input` with an external `InlineClaimOracle` and immediate
    /// `sink` dispatch when the outbox is empty.
    pub fn step_and_dispatch_with_oracle(
        &mut self,
        input: Input,
        grant: TurnGrant,
        sink: &mut dyn EffectSink,
        oracle: &mut dyn InlineClaimOracle,
    ) -> (StepOutcome, usize) {
        self.step_with_sink_and_oracle(input, grant, Some(sink), Some(oracle))
    }

    /// Takes the inline-claimed delivery batch committed by the last applied
    /// `PublicationAdmitted` result, if any.
    pub fn take_last_delegated_committed(&mut self) -> Option<DelegatedInlineBatch> {
        self.last_delegated_committed.take()
    }

    /// Takes the inline-claimed delivery batch rolled back by the last applied
    /// `RejectedCertain` result, if any.
    pub fn take_last_delegated_rejected(&mut self) -> Option<DelegatedInlineBatch> {
        self.last_delegated_rejected.take()
    }

    /// Takes the routed volatile QoS 0 batch produced by the last applied
    /// QoS 0 `Publish` packet, if any.
    pub fn take_last_delegated_qos0(&mut self) -> Option<DelegatedQos0Batch> {
        self.last_delegated_qos0.take()
    }

    /// Returns `(target_session, packet_id)` if the last applied transition
    /// removed a finished delivery from `pubsub.deliveries`.
    #[must_use]
    pub fn last_finished_delivery(&self) -> Option<(SessionId, Option<OutboundPacketId>)> {
        self.last_finished_delivery
    }

    /// Number of in-flight delegated inline-claimed admissions currently
    /// targeting `session` on this Coordinator.
    #[must_use]
    pub fn pending_delegated_inline_count(&self, session: SessionId) -> usize {
        self.delegated_inline_by_session
            .get(&session)
            .copied()
            .unwrap_or(0)
    }

    /// Installs or rebinds `session` on a `DeliveryShard` owner with its
    /// active connection settings and marks it needing a store scan before
    /// inline eligibility can be advertised.
    pub fn shard_attach_session(
        &mut self,
        session: SessionId,
        revision: Revision,
        connection: ConnectionGeneration,
        receive_maximum: u16,
        maximum_packet_size: u32,
    ) -> Result<(), CoreFault> {
        if receive_maximum == 0 || maximum_packet_size == 0 {
            return Err(CoreFault::ImpossibleInput);
        }
        if let Some(&idx) = self.session_by_id.get(&session) {
            let old_revision = self.sessions.as_slice()[idx].revision;
            let old_conn = self.sessions.as_slice()[idx].connection;
            if old_revision != revision {
                self.pubsub.retire_session(session);
                if let Some(old_gen) = old_conn
                    && old_gen != connection
                {
                    self.pubsub.close(old_gen);
                    if !self.is_closed(old_gen) {
                        self.remember_closed(old_gen)?;
                    }
                }
                let client_id = SharedBytes::copy_from(&session.get().to_be_bytes(), 8)
                    .map_err(|_| CoreFault::AllocationFailed)?;
                self.replace_session(
                    idx,
                    super::Session {
                        id: session,
                        client_id,
                        revision,
                        connection: Some(connection),
                        expiry: SessionExpiry::Never,
                        opened_by: None,
                        detached: None,
                        will_managed: false,
                    },
                )?;
            } else {
                if let Some(old_gen) = old_conn
                    && old_gen != connection
                {
                    self.pubsub.close(old_gen);
                    if !self.is_closed(old_gen) {
                        self.remember_closed(old_gen)?;
                    }
                }
                self.set_session_connection(idx, Some(connection))?;
            }
        } else {
            let client_id = SharedBytes::copy_from(&session.get().to_be_bytes(), 8)
                .map_err(|_| CoreFault::AllocationFailed)?;
            self.push_session(super::Session {
                id: session,
                client_id,
                revision,
                connection: Some(connection),
                expiry: SessionExpiry::Never,
                opened_by: None,
                detached: None,
                will_managed: false,
            })?;
        }
        let settings = Connection {
            generation: connection,
            last_qos0_ingress: 0,
            maximum_packet_size,
            receive_maximum: receive_maximum.min(super::MAX_OUTGOING_WINDOW),
            rescan_needed: true,
            caught_up: false,
            dirty_during_load: false,
        };
        if let Some(cidx) = self.pubsub.connection(connection) {
            self.pubsub.connections.replace(cidx, settings)?;
            self.pubsub.connections.clear_known_through(connection);
        } else {
            self.pubsub.connections.push(settings)?;
        }
        Ok(())
    }

    /// Called only after the stopped transport's observations have been applied.
    /// An expiring delivery can complete an early PUBACK only if its first
    /// byte was reported. An unresolved start already has a store command or
    /// a retry, while no later first-byte report can arrive for this generation.
    pub fn shard_ack_progress(&self, connection: ConnectionKey) -> ShardAckProgress {
        if self
            .connection_of(connection.session)
            .is_some_and(|live| live != connection.generation)
        {
            return ShardAckProgress::Settled;
        }
        let Some(session) = self.pubsub.deliveries.session(connection.session) else {
            return ShardAckProgress::Settled;
        };
        let mut progress = ShardAckProgress::Settled;
        for index in session.all.iter() {
            let d = self.pubsub.deliveries.row(index);
            if d.retired {
                continue;
            }
            let early = d.early_puback || d.early_pubrec.is_some();
            match d.phase {
                Phase::FinishPending(_) | Phase::PubrelPending(_) => {
                    progress = ShardAckProgress::Waiting;
                }
                Phase::StartPending(_) if early => progress = ShardAckProgress::Waiting,
                Phase::StartRetry | Phase::AwaitPuback
                    if early && d.control_ready(self.message_now) =>
                {
                    if self.receipts.control_available() {
                        return ShardAckProgress::WorkReady;
                    }
                    progress = ShardAckProgress::Waiting;
                }
                _ => {}
            }
        }
        progress
    }

    /// Detaches `connection` from `session` on a `DeliveryShard` owner while
    /// keeping durable session deliveries ready for a later reconnect.
    pub fn shard_detach_session(
        &mut self,
        session: SessionId,
        connection: ConnectionGeneration,
    ) -> Result<(), CoreFault> {
        if self
            .connection_of(session)
            .is_none_or(|live| live == connection)
            && let Some(rows) = self.pubsub.deliveries.session(session)
        {
            // Without a first byte on the old transport, an early PUBACK
            // cannot be paired with the first byte of a later retransmission.
            let stale: Vec<_> = rows
                .all
                .iter()
                .filter(|&index| {
                    let d = self.pubsub.deliveries.row(index);
                    d.early_puback
                        && matches!(d.phase, Phase::AwaitPuback)
                        && d.claim.is_some_and(|claim| !claim.started)
                })
                .collect();
            for index in stale {
                self.pubsub
                    .deliveries
                    .mutate(index, |d| d.early_puback = false);
            }
        }
        self.pubsub.close(connection);
        if let Some(&idx) = self.session_by_id.get(&session)
            && self.sessions.as_slice()[idx].connection == Some(connection)
        {
            self.set_session_connection(idx, None)?;
        }
        if !self.is_closed(connection) {
            self.remember_closed(connection)?;
        }
        Ok(())
    }

    /// Retires `session` and all of its deliveries on a `DeliveryShard` owner
    /// (in-flight store commands stay retired until their result arrives).
    pub fn shard_retire_session(&mut self, session: SessionId) -> Result<(), CoreFault> {
        self.pubsub.retire_session(session);
        if let Some(&idx) = self.session_by_id.get(&session) {
            if let Some(conn) = self.sessions.as_slice()[idx].connection {
                self.pubsub.close(conn);
                if !self.is_closed(conn) {
                    self.remember_closed(conn)?;
                }
            }
            let _ = self.remove_session_at(idx)?;
        }
        Ok(())
    }

    /// Marks `session` dirty on a `DeliveryShard` owner so the next work scan
    /// loads newly committed non-inline deliveries. Set `clear_known_through`
    /// when a subscription/retained replay may have created a delivery with an
    /// older `PublicationId`.
    pub fn shard_notify_dirty(&mut self, session: SessionId, clear_known_through: bool) {
        if clear_known_through && let Some(generation) = self.connection_of(session) {
            self.pubsub.connections.clear_known_through(generation);
        }
        self.mark_session_dirty(session, true);
    }

    /// The packet identifiers another owner may hold for `session`, as the
    /// shared lease shows them: those the coordinator reserved for inline
    /// claims whose admission is pending. The claims this owner makes for the
    /// session avoid them. All zero forgets them.
    pub fn shard_set_foreign_packet_ids(&mut self, session: SessionId, mask: [u64; 8]) {
        self.pubsub.deliveries.set_foreign_packet_ids(session, mask);
    }

    /// Returns `Some((in_flight_count, used_pi_mask))` when `session` on
    /// `generation` is safe for Coordinator inline-claim leasing: online,
    /// caught up with no pending scan/rescan/cursor, zero queued/unclaimed or
    /// claiming deliveries, and every occupied Packet Identifier within
    /// `1..=512`.
    #[must_use]
    pub fn shard_inline_eligibility(
        &self,
        session: SessionId,
        generation: ConnectionGeneration,
    ) -> Option<(usize, [u64; 8])> {
        if self.connection_of(session) != Some(generation) {
            return None;
        }
        let cidx = self.pubsub.connection(generation)?;
        let settings = self.pubsub.connections.as_slice()[cidx];
        if !settings.caught_up
            || settings.rescan_needed
            || self.pubsub.connections.is_loading(generation)
            || self.pubsub.connections.has_after(generation)
        {
            return None;
        }
        let Some(sd) = self.pubsub.deliveries.session(session) else {
            return Some((0, [0u64; 8]));
        };
        if sd.queued > 0 {
            return None;
        }
        for idx in &sd.all {
            let d = self.pubsub.deliveries.row(idx);
            if matches!(
                d.phase,
                Phase::ClaimPending(_) | Phase::ConsumePending(_, _)
            ) {
                return None;
            }
            if d.claim.is_some_and(|claim| claim.packet_id.get() > 512) {
                return None;
            }
        }
        Some((sd.all.len(), sd.used_pi_mask()))
    }

    /// Admits an already-committed Coordinator inline claim directly into this
    /// `DeliveryShard` owner and immediately emits `Effect::EmitPublish` if
    /// the target connection is online, has `Receive Maximum` room, no earlier
    /// delivery waits ahead of it, and the packet fits `Maximum Packet Size`.
    pub fn step_shard_inline_committed(
        &mut self,
        claim: &DelegatedClaim,
        topic: SharedBytes,
        payload: SharedBytes,
        properties: Option<Arc<PublishProperties>>,
        sink: Option<&mut dyn EffectSink>,
    ) -> ShardStepOutcome {
        self.last_observed_puback_ignored = false;
        self.last_step_may_unblock_work = true;
        self.last_inline_ready_target = None;
        if !self.last_inline_ready_extra.is_empty() {
            self.last_inline_ready_extra.clear();
        }
        self.last_finished_delivery = None;

        let Ok(next_revision) = self.revision.checked_next() else {
            return ShardStepOutcome::Exhausted(Exhaustion::IdentityCounter);
        };
        let Some(&sidx) = self.session_by_id.get(&claim.target) else {
            self.revision = next_revision;
            self.last_step_may_unblock_work = false;
            return ShardStepOutcome::Applied {
                accepted: 0,
                emitted: false,
            };
        };
        if self.sessions.as_slice()[sidx].revision != claim.target_revision {
            self.revision = next_revision;
            self.last_step_may_unblock_work = false;
            return ShardStepOutcome::Applied {
                accepted: 0,
                emitted: false,
            };
        }
        if let Some(existing_slot) = self.pubsub.delivery(claim.delivery) {
            if self.pubsub.deliveries.row(existing_slot).topic.is_none() {
                self.pubsub.deliveries.rewrite(existing_slot, |d| {
                    d.topic = Some(topic);
                    d.payload = Some(payload);
                    d.properties = properties;
                });
            }
            self.revision = next_revision;
            return ShardStepOutcome::Applied {
                accepted: 0,
                emitted: false,
            };
        }
        if self.pubsub.deliveries.len() >= self.pubsub.deliveries.capacity()
            || self
                .pubsub
                .deliveries
                .loaded_bytes()
                .checked_add(payload.len() as u64)
                .is_none_or(|total| total > self.pubsub.payload_limit())
        {
            return ShardStepOutcome::NeedCapacity(CapacityNeed {
                resource: Resource::PayloadBytes,
                amount: 1,
                wake: WakeCondition::ResourceReleased,
            });
        }
        let delivery_claim = DeliveryClaim {
            operation: claim.claim_operation,
            packet_id: claim.packet_id,
            started: false,
            await_pubcomp: false,
        };
        let mut delivery = Delivery::metadata(
            claim.delivery,
            claim.target,
            claim.target_revision,
            claim.qos,
            Some(delivery_claim),
        );
        delivery.phase = Phase::AwaitPuback;
        delivery.may_have_started = false;
        delivery.certain_never_started = true;
        delivery.topic = Some(topic);
        delivery.payload = Some(payload);
        delivery.properties = properties;

        let mut draft = Draft {
            epoch: self.epoch,
            owner: self.id,
            counters: self.counters,
            effects: Effects {
                items: [None, None, None],
                count: 0,
            },
            progress: Progress::Complete,
            closed: None,
            receipt_issued: None,
            receipt_consumed: None,
            released: None,
            inline_outcome: None,
            delegated_inline_resolved: None,
            non_inline_resolved: None,
        };
        let emission = match self.plan_delivery_emit(&delivery, &mut draft) {
            Ok(Emission::Frame(effect, connection)) => Some((effect, connection)),
            Ok(Emission::Held | Emission::TooLarge) => None,
            Err(Stop::Need(need)) => return ShardStepOutcome::NeedCapacity(need),
            Err(Stop::Exhausted(ex)) => return ShardStepOutcome::Exhausted(ex),
            Err(Stop::Fatal(fault)) => return ShardStepOutcome::Fatal(fault),
        };
        if self.receipts.enabled
            && draft.effects.count > 0
            && self.outbox.free_slots() < draft.effects.count.saturating_add(2)
        {
            return ShardStepOutcome::NeedCapacity(CapacityNeed {
                resource: Resource::EffectSlots,
                amount: u64::try_from(
                    draft.effects.count.saturating_add(2) - self.outbox.free_slots(),
                )
                .unwrap_or(u64::MAX),
                wake: WakeCondition::EffectAccepted,
            });
        }
        let direct_sink =
            sink.filter(|_| self.outbox.is_empty() && !self.outbox.has_live_reservations());
        let reservation = match (
            NonZeroUsize::new(draft.effects.count),
            direct_sink.is_some(),
        ) {
            (None, _) => None,
            (Some(slots), true) => {
                if let Err(refusal) = self.outbox.check_reserve(slots) {
                    return Self::shard_reservation_refused(refusal);
                }
                None
            }
            (Some(slots), false) => match self.outbox.reserve(slots) {
                Ok(key) => Some(key),
                Err(refusal) => return Self::shard_reservation_refused(refusal),
            },
        };
        if let Some((effect, connection)) = emission {
            delivery.window = Some(connection.generation);
            delivery.frame = Some((effect, connection));
            delivery.may_have_started = true;
            delivery.local_write_completed = false;
        }
        if let Err(fault) = self.pubsub.deliveries.push(delivery) {
            if let Some(key) = reservation {
                let _ = self.outbox.release(key);
            }
            return ShardStepOutcome::Fatal(fault);
        }
        if self
            .pubsub
            .connection(claim.generation)
            .is_some_and(|i| self.pubsub.connections.as_slice()[i].caught_up)
        {
            self.pubsub
                .connections
                .advance_known_through(claim.generation, claim.delivery);
        }
        self.counters = draft.counters;
        self.revision = next_revision;
        let mut accepted = 0;
        if let Some(sink) = direct_sink {
            if draft.effects.count > 0 {
                match self
                    .outbox
                    .dispatch_immediate(&mut draft.effects.items[..draft.effects.count], sink)
                {
                    Ok(n) => accepted = n,
                    Err(_) => return ShardStepOutcome::Fatal(CoreFault::ReservationOverrun),
                }
            }
        } else if let Some(key) = reservation {
            for slot in &mut draft.effects.items[..draft.effects.count] {
                if let Some(effect) = slot.take()
                    && self.outbox.publish(key, effect).is_err()
                {
                    return ShardStepOutcome::Fatal(CoreFault::ReservationOverrun);
                }
            }
        }
        if emission.is_some() && self.session_provably_idle_work(claim.target) {
            self.last_step_may_unblock_work = false;
        }
        ShardStepOutcome::Applied {
            accepted,
            emitted: emission.is_some(),
        }
    }

    /// Enqueues one delegated volatile QoS 0 target into this `DeliveryShard`
    /// owner's `VolatileQueue` with full ADR 0063 windowing and drop semantics.
    #[expect(
        clippy::too_many_arguments,
        reason = "mirrors the fields of one routed volatile QoS 0 delivery without extra boxing"
    )]
    pub fn step_shard_qos0(
        &mut self,
        origin: Qos0Origin,
        target: SessionId,
        connection: Option<ConnectionKey>,
        topic: SharedBytes,
        payload: SharedBytes,
        expiry: Option<MessageExpiry>,
        properties: Option<Arc<PublishProperties>>,
        sink: Option<&mut dyn EffectSink>,
    ) -> ShardStepOutcome {
        self.last_observed_puback_ignored = false;
        self.last_step_may_unblock_work = false;
        self.last_inline_ready_target = None;
        if !self.last_inline_ready_extra.is_empty() {
            self.last_inline_ready_extra.clear();
        }
        self.last_finished_delivery = None;

        let Ok(next_revision) = self.revision.checked_next() else {
            return ShardStepOutcome::Exhausted(Exhaustion::IdentityCounter);
        };
        let mut draft = Draft {
            epoch: self.epoch,
            owner: self.id,
            counters: self.counters,
            effects: Effects {
                items: [None, None, None],
                count: 0,
            },
            progress: Progress::Complete,
            closed: None,
            receipt_issued: None,
            receipt_consumed: None,
            released: None,
            inline_outcome: None,
            delegated_inline_resolved: None,
            non_inline_resolved: None,
        };
        let used = self.pubsub.volatile.payload_bytes();
        let fits = self.pubsub.volatile.len() < self.pubsub.volatile.capacity()
            && used
                .checked_add(payload.len() as u64)
                .is_some_and(|total| total <= self.pubsub.payload_limit());
        let mut schedule_continuation = None;
        if !fits {
            if let Err(stop) = draft.emit(Effect::Qos0Dropped {
                origin,
                target: Some(target),
                reason: Qos0DropReason::CapacityExceeded,
            }) {
                return Self::shard_stop(stop);
            }
        } else if self.pubsub.continuation.is_none() {
            let continuation = match draw(
                &mut draft.counters.continuation,
                ContinuationId::checked_next,
            ) {
                Ok(c) => c,
                Err(stop) => return Self::shard_stop(stop),
            };
            if let Err(stop) = draft.emit(Effect::Continue { continuation }) {
                return Self::shard_stop(stop);
            }
            schedule_continuation = Some(continuation);
        }
        if self.receipts.enabled
            && draft.effects.count > 0
            && self.outbox.free_slots() < draft.effects.count.saturating_add(2)
        {
            return ShardStepOutcome::NeedCapacity(CapacityNeed {
                resource: Resource::EffectSlots,
                amount: u64::try_from(
                    draft.effects.count.saturating_add(2) - self.outbox.free_slots(),
                )
                .unwrap_or(u64::MAX),
                wake: WakeCondition::EffectAccepted,
            });
        }
        let direct_sink =
            sink.filter(|_| self.outbox.is_empty() && !self.outbox.has_live_reservations());
        let reservation = match (
            NonZeroUsize::new(draft.effects.count),
            direct_sink.is_some(),
        ) {
            (None, _) => None,
            (Some(slots), true) => {
                if let Err(refusal) = self.outbox.check_reserve(slots) {
                    return Self::shard_reservation_refused(refusal);
                }
                None
            }
            (Some(slots), false) => match self.outbox.reserve(slots) {
                Ok(key) => Some(key),
                Err(refusal) => return Self::shard_reservation_refused(refusal),
            },
        };
        if fits {
            let item = Volatile {
                origin,
                target,
                connection,
                topic,
                payload,
                expiry: expiry.map(Box::new),
                properties,
                frame: None,
            };
            if let Err(fault) = self.pubsub.volatile.push_pending(item) {
                if let Some(key) = reservation {
                    let _ = self.outbox.release(key);
                }
                return ShardStepOutcome::Fatal(fault);
            }
            if let Some(c) = schedule_continuation {
                self.pubsub.continuation = Some(c);
                self.pubsub.continuations_started = true;
            }
        }
        self.counters = draft.counters;
        self.revision = next_revision;
        let mut accepted = 0;
        if let Some(sink) = direct_sink {
            if draft.effects.count > 0 {
                match self
                    .outbox
                    .dispatch_immediate(&mut draft.effects.items[..draft.effects.count], sink)
                {
                    Ok(n) => accepted = n,
                    Err(_) => return ShardStepOutcome::Fatal(CoreFault::ReservationOverrun),
                }
            }
        } else if let Some(key) = reservation {
            for slot in &mut draft.effects.items[..draft.effects.count] {
                if let Some(effect) = slot.take()
                    && self.outbox.publish(key, effect).is_err()
                {
                    return ShardStepOutcome::Fatal(CoreFault::ReservationOverrun);
                }
            }
        }
        ShardStepOutcome::Applied {
            accepted,
            emitted: false,
        }
    }

    fn shard_stop(stop: Stop) -> ShardStepOutcome {
        match stop {
            Stop::Need(need) => ShardStepOutcome::NeedCapacity(need),
            Stop::Exhausted(ex) => ShardStepOutcome::Exhausted(ex),
            Stop::Fatal(fault) => ShardStepOutcome::Fatal(fault),
        }
    }

    fn shard_reservation_refused(
        refusal: crate::contract::effects::ReserveRefused,
    ) -> ShardStepOutcome {
        match refusal {
            crate::contract::effects::ReserveRefused::Need(need) => {
                ShardStepOutcome::NeedCapacity(need)
            }
            crate::contract::effects::ReserveRefused::LargerThanBox {
                requested,
                capacity,
            } => ShardStepOutcome::Fatal(CoreFault::BoxTooSmall {
                requested,
                capacity,
            }),
            crate::contract::effects::ReserveRefused::TableExhausted => {
                ShardStepOutcome::Exhausted(Exhaustion::ReservationTable)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::capacity::WorkUnits;
    use crate::contract::command::{Recipe, StartupNonce};
    use crate::contract::effects::{
        EffectSink, OwnedEffect, OwnerAuthority, RecoveredFact, SubmitResult,
    };
    use crate::contract::ids::{
        AccountId, InboundPacketId, LogicalDuration, LogicalOwnerId, RunEpoch, StoreIncarnation,
    };
    use crate::contract::outcome::{
        CommitFacts, EpochOpened, EpochOpening, OpeningOutcome, OpeningReply, RejectReason,
        StoreOutcome, StoreReply,
    };
    use crate::contract::packet::{InboundPacket, Qos2Ack};
    use crate::contract::pubsub::{SubscriptionOptions, WorkItem};
    use alloc::collections::BTreeMap;

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

    #[derive(Default)]
    struct TestLease {
        eligible_gen: Option<ConnectionGeneration>,
        in_flight: usize,
        preclaim_cap: usize,
        used_pi: [u64; 8],
    }

    struct TestOracle {
        leases: BTreeMap<SessionId, TestLease>,
        rollbacks: usize,
        ineligible_marks: usize,
    }

    impl TestOracle {
        fn new() -> Self {
            Self {
                leases: BTreeMap::new(),
                rollbacks: 0,
                ineligible_marks: 0,
            }
        }

        fn set_eligible(
            &mut self,
            session: SessionId,
            generation: ConnectionGeneration,
            preclaim_cap: usize,
            in_flight: usize,
            used_pi: [u64; 8],
        ) {
            self.leases.insert(
                session,
                TestLease {
                    eligible_gen: Some(generation),
                    in_flight,
                    preclaim_cap,
                    used_pi,
                },
            );
        }

        fn release_pi(&mut self, session: SessionId, pi: OutboundPacketId) {
            if let Some(lease) = self.leases.get_mut(&session) {
                let id = pi.get();
                if id <= 512 {
                    let zb = usize::from(id - 1);
                    lease.used_pi[zb / 64] &= !(1u64 << (zb % 64));
                }
                lease.in_flight = lease.in_flight.saturating_sub(1);
            }
        }
    }

    impl InlineClaimOracle for TestOracle {
        fn try_reserve(
            &mut self,
            session: SessionId,
            qos: Qos,
            _payload_bytes: u64,
        ) -> Option<(ConnectionGeneration, OutboundPacketId)> {
            if qos == Qos::AtMostOnce {
                return None;
            }
            let lease = self.leases.get_mut(&session)?;
            let generation = lease.eligible_gen?;
            if lease.in_flight >= lease.preclaim_cap {
                return None;
            }
            for (word_idx, word) in lease.used_pi.iter_mut().enumerate() {
                if *word != u64::MAX {
                    let bit = word.trailing_ones() as usize;
                    let candidate = (word_idx * 64 + bit + 1) as u16;
                    *word |= 1u64 << bit;
                    lease.in_flight += 1;
                    return OutboundPacketId::new(candidate).map(|pi| (generation, pi));
                }
            }
            None
        }

        fn mark_ineligible(&mut self, session: SessionId) {
            self.ineligible_marks += 1;
            if let Some(lease) = self.leases.get_mut(&session) {
                lease.eligible_gen = None;
            }
        }

        fn rollback(&mut self, session: SessionId, packet_id: OutboundPacketId) {
            self.rollbacks += 1;
            self.release_pi(session, packet_id);
        }
    }

    fn bytes(v: &[u8]) -> SharedBytes {
        SharedBytes::copy_from(v, 128).unwrap()
    }

    fn nz(v: u32) -> core::num::NonZeroU32 {
        core::num::NonZeroU32::new(v).unwrap()
    }

    fn test_config() -> OwnerConfig {
        OwnerConfig {
            max_sessions: nz(16),
            max_routes: nz(32),
            max_exchanges: nz(64),
            session_qos2_exchanges: nz(16),
            account: AccountId::new(1),
            payload_bytes: PayloadBytes::new(64 * 1024),
            resolution_delay: LogicalDuration::new(10),
            retained_operations: nz(64),
            remembered_connections: nz(32),
        }
    }

    /// Builds a Coordinator (`Owner 0`) recovered with producer `S1` and
    /// subscribers `S2` (Shard 1) and `S3` (Shard 2) on topic `t/a`, plus two
    /// `DeliveryShard` owners (`Owner 1`, `Owner 2`).
    fn build_cluster() -> (Owner, Owner, Owner) {
        let inc = StoreIncarnation::new(1);
        let epoch = RunEpoch::new(2);
        let mut opening = EpochOpening::new(inc, StartupNonce::new([9; 16]));
        let cmd = opening.submit(RunEpoch::new(1)).unwrap();
        let _ = opening
            .observe(OpeningReply {
                command: cmd,
                outcome: OpeningOutcome::Committed(EpochOpened {
                    incarnation: inc,
                    epoch,
                }),
            })
            .unwrap();
        let mut authority =
            OwnerAuthority::from_grant(opening.hand_over().unwrap(), nz(3)).unwrap();
        let slots = NonZeroUsize::new(64).unwrap();
        let box0 = authority
            .create_outbox(LogicalOwnerId::new(0), slots, nz(8))
            .unwrap();
        let box1 = authority
            .create_outbox(LogicalOwnerId::new(1), slots, nz(8))
            .unwrap();
        let box2 = authority
            .create_outbox(LogicalOwnerId::new(2), slots, nz(8))
            .unwrap();

        let mut coord = Owner::new(test_config(), box0).unwrap();
        coord.enable_session_lifecycle();
        coord.enable_inline_claims();
        coord.enable_delegated_deliveries();

        let mut facts = crate::contract::capacity::BoundedVec::with_capacity(16).unwrap();
        facts
            .try_push(RecoveredFact::Epoch {
                incarnation: inc,
                epoch,
            })
            .unwrap();
        facts
            .try_push(RecoveredFact::Limits {
                max_targets_per_admission: 16,
                max_topic_bytes: 128,
                max_payload_bytes: PayloadBytes::new(4096),
            })
            .unwrap();
        facts
            .try_push(RecoveredFact::RouteTable {
                revision: Revision::new(1),
            })
            .unwrap();
        for sid in [2u64, 3u64] {
            facts
                .try_push(RecoveredFact::Route {
                    topic: bytes(b"t/a"),
                    target: SessionId::new(sid),
                    target_revision: Revision::new(1),
                    max_qos: Qos::ExactlyOnce,
                    options: SubscriptionOptions::DEFAULT,
                })
                .unwrap();
        }
        for (sid, cid) in [(1u64, b"pub1" as &[u8]), (2, b"sub2"), (3, b"sub3")] {
            facts
                .try_push(RecoveredFact::Session {
                    session: SessionId::new(sid),
                    client_id: bytes(cid),
                    revision: Revision::new(1),
                    expiry: SessionExpiry::Never,
                    opened_by: None,
                    detached: None,
                })
                .unwrap();
        }
        assert!(matches!(
            coord.step(Input::Recovery { facts, last: true }, TURN),
            StepOutcome::Applied { .. }
        ));
        let mut sink = Taking(Vec::new());
        // Bind producer S1 on gen 10, subscribers S2 on gen 20, S3 on gen 30.
        for (cid, g) in [(b"pub1" as &[u8], 10u64), (b"sub2", 20), (b"sub3", 30)] {
            let conn_gen = ConnectionGeneration::new(g);
            assert!(matches!(
                coord.step_and_dispatch(
                    Input::ConnectionSettings {
                        connection: conn_gen,
                        receive_maximum: 2,
                        maximum_packet_size: 4096,
                    },
                    TURN,
                    &mut sink,
                ),
                (StepOutcome::Applied { .. }, _)
            ));
            assert!(matches!(
                coord.step_and_dispatch(
                    Input::Packet {
                        connection: conn_gen,
                        ingress_order: 1,
                        packet: InboundPacket::ConnectResume {
                            client_id: bytes(cid),
                        },
                    },
                    TURN,
                    &mut sink,
                ),
                (StepOutcome::Applied { .. }, _)
            ));
        }
        let limits = coord.shard_limits().unwrap();
        let mut shard1 = Owner::new_delivery_shard(test_config(), box1, limits).unwrap();
        let mut shard2 = Owner::new_delivery_shard(test_config(), box2, limits).unwrap();
        shard1
            .shard_attach_session(
                SessionId::new(2),
                Revision::new(1),
                ConnectionGeneration::new(20),
                2,
                4096,
            )
            .unwrap();
        shard2
            .shard_attach_session(
                SessionId::new(3),
                Revision::new(1),
                ConnectionGeneration::new(30),
                2,
                4096,
            )
            .unwrap();
        // Complete initial empty WorkPage scan on both shards so they become caught up.
        for (shard, sid) in [
            (&mut shard1, SessionId::new(2)),
            (&mut shard2, SessionId::new(3)),
        ] {
            let mut s = Taking(Vec::new());
            assert!(matches!(
                shard.step_and_dispatch(Input::WorkAvailable { session: sid }, TURN, &mut s),
                (StepOutcome::Applied { .. }, _)
            ));
            let load_eff =
                s.0.iter()
                    .find_map(|e| matches!(e.effect(), Effect::LoadWork { .. }).then_some(e.id()))
                    .unwrap();
            let empty = crate::contract::capacity::BoundedVec::with_capacity(0).unwrap();
            assert!(matches!(
                shard.step_and_dispatch(
                    Input::WorkPage {
                        effect: load_eff,
                        session: sid,
                        items: empty,
                        next: None,
                    },
                    TURN,
                    &mut s,
                ),
                (StepOutcome::Applied { .. }, _)
            ));
        }
        (coord, shard1, shard2)
    }

    #[test]
    fn coordinator_and_two_shards_inline_fanout_rm_and_finish() {
        let (mut coord, mut shard1, mut shard2) = build_cluster();
        let mut oracle = TestOracle::new();
        let (c1, m1) = shard1
            .shard_inline_eligibility(SessionId::new(2), ConnectionGeneration::new(20))
            .unwrap();
        let (c2, m2) = shard2
            .shard_inline_eligibility(SessionId::new(3), ConnectionGeneration::new(30))
            .unwrap();
        assert_eq!((c1, c2), (0, 0));
        oracle.set_eligible(SessionId::new(2), ConnectionGeneration::new(20), 4, c1, m1);
        oracle.set_eligible(SessionId::new(3), ConnectionGeneration::new(30), 4, c2, m2);

        // Publish 3 QoS 1 messages to t/a -> RM=2 on both shards so 2 emit immediately and 1 waits in FIFO!
        let mut s1_emitted = Vec::new();
        let mut s2_emitted = Vec::new();
        for seq in 1u16..=3 {
            let mut sink = Taking(Vec::new());
            let (out, _) = coord.step_and_dispatch_with_oracle(
                Input::Packet {
                    connection: ConnectionGeneration::new(10),
                    ingress_order: u64::from(seq) + 1,
                    packet: InboundPacket::Publish {
                        qos: Qos::AtLeastOnce,
                        dup: false,
                        retain: false,
                        packet_id: Some(InboundPacketId::new(seq).unwrap()),
                        topic: bytes(b"t/a"),
                        payload: bytes(&[seq as u8; 8]),
                        expiry: None,
                        properties: None,
                    },
                },
                TURN,
                &mut sink,
                &mut oracle,
            );
            assert!(matches!(out, StepOutcome::Applied { .. }), "{out:?}");
            assert_eq!(coord.pending_delegated_inline_count(SessionId::new(2)), 1);
            assert_eq!(coord.pending_delegated_inline_count(SessionId::new(3)), 1);
            let (eff_id, op, pub_id, ex) = sink
                .0
                .iter()
                .find_map(|e| match e.effect() {
                    Effect::Persist { command } => match command.recipe.get() {
                        Recipe::MultiClaimedPublication(m) => {
                            assert_eq!(m.claims.len(), 2);
                            Some((
                                e.id(),
                                m.admission.operation,
                                m.admission.publication,
                                m.admission.source.exchange,
                            ))
                        }
                        other => panic!("expected MultiClaimedPublication, got {other:?}"),
                    },
                    _ => None,
                })
                .unwrap();

            // Commit admission on Coordinator.
            let mut csink = Taking(Vec::new());
            let (out, _) = coord.step_and_dispatch_with_oracle(
                Input::StoreResult {
                    effect: eff_id,
                    operation: op,
                    reply: StoreReply::Outcome(StoreOutcome::Committed(
                        CommitFacts::PublicationAdmitted {
                            publication: pub_id,
                            exchange: ex,
                        },
                    )),
                },
                TURN,
                &mut csink,
                &mut oracle,
            );
            assert!(matches!(out, StepOutcome::Applied { .. }));
            assert_eq!(coord.pending_delegated_inline_count(SessionId::new(2)), 0);
            let batch = coord.take_last_delegated_committed().unwrap();
            assert_eq!(batch.claims.len(), 2);

            // Hand off each claim to its owning shard.
            for claim in &batch.claims {
                let mut sh_sink = Taking(Vec::new());
                let (shard, emitted_log) = if claim.target == SessionId::new(2) {
                    (&mut shard1, &mut s1_emitted)
                } else {
                    (&mut shard2, &mut s2_emitted)
                };
                let res = shard.step_shard_inline_committed(
                    claim,
                    batch.topic.clone(),
                    batch.payload.clone(),
                    batch.properties.clone(),
                    Some(&mut sh_sink),
                );
                let expected_emitted = seq <= 2;
                assert_eq!(
                    res,
                    ShardStepOutcome::Applied {
                        accepted: usize::from(expected_emitted),
                        emitted: expected_emitted,
                    }
                );
                for eff in sh_sink.0 {
                    if let Effect::EmitPublish {
                        packet_id,
                        delivery,
                        claim: cl,
                        ..
                    } = eff.effect()
                    {
                        emitted_log.push((*packet_id, *delivery, *cl, eff.id()));
                    }
                }
            }
        }
        assert_eq!(s1_emitted.len(), 2);
        assert_eq!(s2_emitted.len(), 2);

        // PUBACK first delivery on shard1 -> FinishDelivery -> commits -> shard1 emits 3rd delivery!
        let (pi1, del1, cl1, frame1) = s1_emitted[0];
        let conn20 = ConnectionKey {
            session: SessionId::new(2),
            generation: ConnectionGeneration::new(20),
        };
        let target1 = shard1.puback_target(conn20, pi1).unwrap();
        assert_eq!(target1.delivery, del1);
        assert_eq!(target1.claim, cl1);
        assert_eq!(target1.frame, frame1);

        let mut sink1 = Taking(Vec::new());
        assert!(matches!(
            shard1.step_and_dispatch(
                Input::ObservedPuback {
                    connection: conn20,
                    packet_id: pi1,
                    reason: 0,
                    target: Some(target1),
                },
                TURN,
                &mut sink1,
            ),
            (StepOutcome::Applied { .. }, _)
        ));
        let (fin_eff, fin_op) = sink1
            .0
            .iter()
            .find_map(|e| match e.effect() {
                Effect::Persist { command } => match command.recipe.get() {
                    Recipe::FinishDelivery(f) => {
                        assert_eq!(f.operation.owner(), LogicalOwnerId::new(1));
                        assert_eq!(f.claim.owner(), LogicalOwnerId::new(0));
                        assert_eq!(f.delivery.publication.owner(), LogicalOwnerId::new(0));
                        Some((e.id(), f.operation))
                    }
                    _ => None,
                },
                _ => None,
            })
            .unwrap();

        let mut sink1b = Taking(Vec::new());
        assert!(matches!(
            shard1.step_and_dispatch(
                Input::StoreResult {
                    effect: fin_eff,
                    operation: fin_op,
                    reply: StoreReply::Outcome(StoreOutcome::Committed(
                        CommitFacts::DeliveryFinished {
                            delivery: del1,
                            target: SessionId::new(2),
                            packet_id: pi1,
                            claim: cl1,
                        },
                    )),
                },
                TURN,
                &mut sink1b,
            ),
            (StepOutcome::Applied { .. }, _)
        ));
        assert_eq!(
            shard1.last_finished_delivery(),
            Some((SessionId::new(2), Some(pi1)))
        );
        oracle.release_pi(SessionId::new(2), pi1);

        // Now WorkAvailable emits the held 3rd delivery!
        let mut sink1c = Taking(Vec::new());
        assert!(matches!(
            shard1.step_and_dispatch(
                Input::WorkAvailable {
                    session: SessionId::new(2),
                },
                TURN,
                &mut sink1c,
            ),
            (StepOutcome::Applied { .. }, _)
        ));
        assert!(
            sink1c
                .0
                .iter()
                .any(|e| matches!(e.effect(), Effect::EmitPublish { .. }))
        );

        coord.pubsub.assert_indices_kept();
        shard1.pubsub.assert_indices_kept();
        shard2.pubsub.assert_indices_kept();
    }

    #[test]
    fn delegated_refusal_and_rejected_certain_roll_back_oracle_atomically() {
        let (mut coord, shard1, shard2) = build_cluster();
        let mut oracle = TestOracle::new();
        let (c1, m1) = shard1
            .shard_inline_eligibility(SessionId::new(2), ConnectionGeneration::new(20))
            .unwrap();
        let (c2, m2) = shard2
            .shard_inline_eligibility(SessionId::new(3), ConnectionGeneration::new(30))
            .unwrap();
        oracle.set_eligible(SessionId::new(2), ConnectionGeneration::new(20), 4, c1, m1);
        oracle.set_eligible(SessionId::new(3), ConnectionGeneration::new(30), 4, c2, m2);

        // 1. Refuse due to tiny work_limit -> oracle MUST roll back cleanly (in_flight == 0).
        let pub_input = Input::Packet {
            connection: ConnectionGeneration::new(10),
            ingress_order: 2,
            packet: InboundPacket::Publish {
                qos: Qos::AtLeastOnce,
                dup: false,
                retain: false,
                packet_id: Some(InboundPacketId::new(1).unwrap()),
                topic: bytes(b"t/a"),
                payload: bytes(b"hello"),
                expiry: None,
                properties: None,
            },
        };
        let tiny = TurnGrant {
            work_limit: WorkUnits::new(1),
        };
        let out = coord.step_with_oracle(pub_input, tiny, &mut oracle);
        let StepOutcome::NeedCapacity { input, .. } = out else {
            panic!("expected NeedCapacity, got {out:?}");
        };
        assert_eq!(oracle.leases[&SessionId::new(2)].in_flight, 0);
        assert_eq!(oracle.leases[&SessionId::new(3)].in_flight, 0);

        // 2. Apply with full grant, then reject with RejectedCertain -> rolls back oracle.
        let mut sink = Taking(Vec::new());
        let (out, _) = coord.step_and_dispatch_with_oracle(input, TURN, &mut sink, &mut oracle);
        assert!(matches!(out, StepOutcome::Applied { .. }));
        assert_eq!(oracle.leases[&SessionId::new(2)].in_flight, 1);
        assert_eq!(oracle.leases[&SessionId::new(3)].in_flight, 1);

        let (eff_id, op) = sink
            .0
            .iter()
            .find_map(|e| match e.effect() {
                Effect::Persist { command } => Some((e.id(), command.recipe.operation())),
                _ => None,
            })
            .unwrap();
        let mut sink2 = Taking(Vec::new());
        let (out, _) = coord.step_and_dispatch_with_oracle(
            Input::StoreResult {
                effect: eff_id,
                operation: op,
                reply: StoreReply::Outcome(StoreOutcome::RejectedCertain(
                    RejectReason::CapacityExceeded(Resource::PayloadBytes),
                )),
            },
            TURN,
            &mut sink2,
            &mut oracle,
        );
        assert!(matches!(out, StepOutcome::Applied { .. }));
        assert!(coord.take_last_delegated_committed().is_none());
        let rejected = coord.take_last_delegated_rejected().unwrap();
        assert_eq!(rejected.claims.len(), 2);
        assert_eq!(oracle.leases[&SessionId::new(2)].in_flight, 0);
        assert_eq!(oracle.leases[&SessionId::new(3)].in_flight, 0);
        assert_eq!(oracle.leases[&SessionId::new(2)].used_pi, [0u64; 8]);
        assert_eq!(oracle.leases[&SessionId::new(3)].used_pi, [0u64; 8]);
    }

    #[test]
    fn partial_inline_claim_and_qos0_delegation_and_too_large_discard() {
        let (mut coord, mut shard1, mut shard2) = build_cluster();
        let mut oracle = TestOracle::new();
        // Only S2 is eligible; S3 is NOT eligible -> partial ClaimedPublication for S2 + dirty S3.
        oracle.set_eligible(
            SessionId::new(2),
            ConnectionGeneration::new(20),
            4,
            0,
            [0u64; 8],
        );

        let mut sink = Taking(Vec::new());
        let (out, _) = coord.step_and_dispatch_with_oracle(
            Input::Packet {
                connection: ConnectionGeneration::new(10),
                ingress_order: 2,
                packet: InboundPacket::Publish {
                    qos: Qos::ExactlyOnce,
                    dup: false,
                    retain: false,
                    packet_id: Some(InboundPacketId::new(7).unwrap()),
                    topic: bytes(b"t/a"),
                    payload: bytes(b"qos2payload"),
                    expiry: None,
                    properties: None,
                },
            },
            TURN,
            &mut sink,
            &mut oracle,
        );
        assert!(matches!(out, StepOutcome::Applied { .. }));
        let (eff_id, op, pub_id, ex) = sink
            .0
            .iter()
            .find_map(|e| match e.effect() {
                Effect::Persist { command } => match command.recipe.get() {
                    Recipe::ClaimedPublication(c) => {
                        assert_eq!(c.claim.target_index, 0);
                        Some((
                            e.id(),
                            c.admission.operation,
                            c.admission.publication,
                            c.admission.source.exchange,
                        ))
                    }
                    other => panic!("expected ClaimedPublication, got {other:?}"),
                },
                _ => None,
            })
            .unwrap();

        let mut sink2 = Taking(Vec::new());
        let (out, _) = coord.step_and_dispatch_with_oracle(
            Input::StoreResult {
                effect: eff_id,
                operation: op,
                reply: StoreReply::Outcome(StoreOutcome::Committed(
                    CommitFacts::PublicationAdmitted {
                        publication: pub_id,
                        exchange: ex,
                    },
                )),
            },
            TURN,
            &mut sink2,
            &mut oracle,
        );
        assert!(matches!(out, StepOutcome::Applied { .. }));
        assert_eq!(coord.last_inline_ready_target(), Some(SessionId::new(3)));
        let batch = coord.take_last_delegated_committed().unwrap();
        assert_eq!(batch.claims.len(), 1);
        assert_eq!(batch.claims[0].target, SessionId::new(2));

        // Drive QoS 2 end-to-end on shard1: EmitPublish -> PUBREC -> PreparePubrel -> PUBREL -> PUBCOMP -> FinishDelivery.
        let mut s1_sink = Taking(Vec::new());
        assert_eq!(
            shard1.step_shard_inline_committed(
                &batch.claims[0],
                batch.topic.clone(),
                batch.payload.clone(),
                None,
                Some(&mut s1_sink),
            ),
            ShardStepOutcome::Applied {
                accepted: 1,
                emitted: true,
            }
        );
        let conn20 = ConnectionKey {
            session: SessionId::new(2),
            generation: ConnectionGeneration::new(20),
        };
        let pi = batch.claims[0].packet_id;
        let pubrec_target = shard1.qos2_ack_target(conn20, pi, Qos2Ack::Pubrec).unwrap();
        let mut s1_rec = Taking(Vec::new());
        assert!(matches!(
            shard1.step_and_dispatch(
                Input::ObservedQos2Ack {
                    connection: conn20,
                    kind: Qos2Ack::Pubrec,
                    packet_id: pi,
                    reason: 0,
                    target: Some(pubrec_target),
                },
                TURN,
                &mut s1_rec,
            ),
            (StepOutcome::Applied { .. }, _)
        ));
        let (prep_eff, prep_op) = s1_rec
            .0
            .iter()
            .find_map(|e| match e.effect() {
                Effect::Persist { command } => Some((e.id(), command.recipe.operation())),
                _ => None,
            })
            .unwrap();
        let mut s1_rel = Taking(Vec::new());
        assert!(matches!(
            shard1.step_and_dispatch(
                Input::StoreResult {
                    effect: prep_eff,
                    operation: prep_op,
                    reply: StoreReply::Outcome(StoreOutcome::Committed(
                        CommitFacts::DeliveryPubrelReady {
                            delivery: batch.claims[0].delivery,
                            target: SessionId::new(2),
                            packet_id: pi,
                            claim: batch.claims[0].claim_operation,
                        },
                    )),
                },
                TURN,
                &mut s1_rel,
            ),
            (StepOutcome::Applied { .. }, _)
        ));
        // WorkAvailable emits PUBREL.
        assert!(matches!(
            shard1.step_and_dispatch(
                Input::WorkAvailable {
                    session: SessionId::new(2),
                },
                TURN,
                &mut s1_rel,
            ),
            (StepOutcome::Applied { .. }, _)
        ));
        assert!(s1_rel.0.iter().any(|e| matches!(
            e.effect(),
            Effect::EmitFrame {
                frame: crate::contract::packet::OutboundFrame::Pubrel { .. },
                ..
            }
        )));
        let pubcomp_target = shard1
            .qos2_ack_target(conn20, pi, Qos2Ack::Pubcomp)
            .unwrap();
        let mut s1_comp = Taking(Vec::new());
        assert!(matches!(
            shard1.step_and_dispatch(
                Input::ObservedQos2Ack {
                    connection: conn20,
                    kind: Qos2Ack::Pubcomp,
                    packet_id: pi,
                    reason: 0,
                    target: Some(pubcomp_target),
                },
                TURN,
                &mut s1_comp,
            ),
            (StepOutcome::Applied { .. }, _)
        ));
        assert!(s1_comp.0.iter().any(|e| matches!(
            e.effect(),
            Effect::Persist { command } if matches!(command.recipe.get(), Recipe::FinishDelivery(_))
        )));

        // Unclaimed S3 loads via WorkPage on shard2!
        shard2.shard_notify_dirty(SessionId::new(3), false);
        let mut s2_sink = Taking(Vec::new());
        assert!(matches!(
            shard2.step_and_dispatch(
                Input::WorkAvailable {
                    session: SessionId::new(3),
                },
                TURN,
                &mut s2_sink,
            ),
            (StepOutcome::Applied { .. }, _)
        ));
        let load_eff = s2_sink
            .0
            .iter()
            .find_map(|e| matches!(e.effect(), Effect::LoadWork { .. }).then_some(e.id()))
            .unwrap();
        let mut page = crate::contract::capacity::BoundedVec::with_capacity(1).unwrap();
        page.try_push(WorkItem {
            delivery: DeliveryId {
                publication: pub_id,
                index: 1,
            },
            target: SessionId::new(3),
            target_revision: Revision::new(1),
            qos: Qos::ExactlyOnce,
            cause: crate::contract::command::DeliveryCause::LiveExact,
            retain: false,
            expiry: None,
            properties: None,
            topic: bytes(b"t/a"),
            payload: bytes(b"qos2payload"),
            claim: None,
        })
        .unwrap();
        assert!(matches!(
            shard2.step_and_dispatch(
                Input::WorkPage {
                    effect: load_eff,
                    session: SessionId::new(3),
                    items: page,
                    next: None,
                },
                TURN,
                &mut s2_sink,
            ),
            (StepOutcome::Applied { .. }, _)
        ));
        // Because S3 has a Queued delivery, shard_inline_eligibility MUST return None!
        assert!(
            shard2
                .shard_inline_eligibility(SessionId::new(3), ConnectionGeneration::new(30))
                .is_none()
        );

        // Test QoS 0 delegation from Coordinator -> shard1 -> EmitQos0.
        let mut q0_sink = Taking(Vec::new());
        let (out, _) = coord.step_and_dispatch_with_oracle(
            Input::Packet {
                connection: ConnectionGeneration::new(10),
                ingress_order: 3,
                packet: InboundPacket::Publish {
                    qos: Qos::AtMostOnce,
                    dup: false,
                    retain: false,
                    packet_id: None,
                    topic: bytes(b"t/a"),
                    payload: bytes(b"fire-and-forget"),
                    expiry: None,
                    properties: None,
                },
            },
            TURN,
            &mut q0_sink,
            &mut oracle,
        );
        assert!(matches!(out, StepOutcome::Applied { .. }));
        let q0_batch = coord.take_last_delegated_qos0().unwrap();
        assert_eq!(q0_batch.targets.len(), 2);
        let t0 = q0_batch.targets[0];
        let mut sh_q0 = Taking(Vec::new());
        assert_eq!(
            shard1.step_shard_qos0(
                q0_batch.origin,
                t0.target,
                t0.connection,
                q0_batch.topic.clone(),
                q0_batch.payload.clone(),
                q0_batch.expiry,
                q0_batch.properties.clone(),
                Some(&mut sh_q0),
            ),
            ShardStepOutcome::Applied {
                accepted: 1,
                emitted: false,
            }
        );
        let cont = sh_q0
            .0
            .iter()
            .find_map(|e| match e.effect() {
                Effect::Continue { continuation } => Some(*continuation),
                _ => None,
            })
            .unwrap();
        let mut sh_q0_emit = Taking(Vec::new());
        assert!(matches!(
            shard1.step_and_dispatch(
                Input::Continue {
                    continuation: cont,
                    budget: WorkUnits::new(100),
                },
                TURN,
                &mut sh_q0_emit,
            ),
            (StepOutcome::Applied { .. }, _)
        ));
        assert!(
            sh_q0_emit
                .0
                .iter()
                .any(|e| matches!(e.effect(), Effect::EmitQos0 { .. }))
        );

        // Test TooLarge discard + detach/retire cleanly on shard2.
        shard2
            .shard_attach_session(
                SessionId::new(3),
                Revision::new(1),
                ConnectionGeneration::new(31),
                2,
                10, // tiny max_packet_size
            )
            .unwrap();
        let big_claim = DelegatedClaim {
            delivery: DeliveryId {
                publication: crate::contract::ids::PublicationId::new(
                    StoreIncarnation::new(1),
                    RunEpoch::new(2),
                    LogicalOwnerId::new(0),
                    crate::contract::ids::PublicationSeq::new(999),
                ),
                index: 0,
            },
            target: SessionId::new(3),
            target_revision: Revision::new(1),
            qos: Qos::AtLeastOnce,
            claim_operation: OperationId::new(
                StoreIncarnation::new(1),
                RunEpoch::new(2),
                LogicalOwnerId::new(0),
                crate::contract::ids::OperationSeq::new(999),
            ),
            packet_id: OutboundPacketId::new(42).unwrap(),
            generation: ConnectionGeneration::new(31),
        };
        let mut big_sink = Taking(Vec::new());
        assert_eq!(
            shard2.step_shard_inline_committed(
                &big_claim,
                bytes(b"t/a"),
                bytes(b"payload-longer-than-10-bytes"),
                None,
                Some(&mut big_sink),
            ),
            ShardStepOutcome::Applied {
                accepted: 0,
                emitted: false,
            }
        );
        // Retire session on shard1 (while FinishPending is in flight) and shard2.
        shard1.shard_retire_session(SessionId::new(2)).unwrap();
        shard2
            .shard_detach_session(SessionId::new(3), ConnectionGeneration::new(31))
            .unwrap();
        shard2.shard_retire_session(SessionId::new(3)).unwrap();

        coord.pubsub.assert_indices_kept();
        shard1.pubsub.assert_indices_kept();
        shard2.pubsub.assert_indices_kept();
    }
}
