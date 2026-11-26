//! Encapsulated table of outgoing QoS 1/2 deliveries and all secondary indices,
//! backed by a slot-stable [`BoundedSlab`] and consolidated per-session
//! [`SessionDeliveries`] index entries.
use super::*;
use crate::contract::ids::OutboundPacketId;
use crate::contract::pubsub::DeliveryClaim;
use alloc::collections::BTreeSet;

const PI_BITMAP_WORDS: usize = 8;
/// Number of low outgoing Packet Identifiers indexed in the per-session bitmap.
pub const OUTGOING_PI_BITMAP_BITS: u16 = (PI_BITMAP_WORDS * u64::BITS as usize) as u16;

/// Highest candidate the fallback needs to examine with this many distinct occupied IDs.
#[must_use]
pub fn outgoing_pi_candidate_limit(occupied: usize, maximum: u16) -> usize {
    occupied.saturating_add(1).min(usize::from(maximum))
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub(super) enum Phase {
    Queued,
    ClaimPending(InFlight),
    /// Provisionally claimed atomically by a `Recipe::ClaimedPublication`
    /// whose `InFlight` lives on `Owner::slots`.
    InlineClaimPending(OperationId),
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
    Qos0Writing(alloc::boxed::Box<Finished>),
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub(super) struct Delivery {
    pub(super) id: DeliveryId,
    pub(super) target: SessionId,
    pub(super) revision: Revision,
    pub(super) qos: Qos,
    pub(super) cause: DeliveryCause,
    pub(super) retain: bool,
    pub(super) expiry: Option<alloc::boxed::Box<MessageExpiry>>,
    pub(super) properties: Option<alloc::sync::Arc<crate::contract::properties::PublishProperties>>,
    pub(super) claim: Option<DeliveryClaim>,
    pub(super) topic: Option<SharedBytes>,
    pub(super) payload: Option<SharedBytes>,
    pub(super) phase: Phase,
    pub(super) claim_result: Option<alloc::boxed::Box<Finished>>,
    /// Send quota reserved for a PUBLISH on this generation. Its PUBREL
    /// retains this reservation through PUBCOMP. A resumed PUBREL has none.
    pub(super) window: Option<ConnectionGeneration>,
    pub(super) frame: Option<(EffectId, ConnectionKey)>,
    pub(super) local_write_completed: bool,
    pub(super) may_have_started: bool,
    /// Only a fresh claim in this process, with no earlier uncertain attempt,
    /// can turn an exact first-frame NotStarted observation into deletion.
    pub(super) certain_never_started: bool,
    pub(super) early_puback: bool,
    pub(super) early_pubrec: Option<u8>,
    /// Monotone rank assigned on the first positive PUBREC observed for this
    /// delivery, even while `StartPending` / `StartRetry` is still running
    /// (MQTT-4.6.0-4 / MQTT-4.6.0-6).
    pub(super) pubrec_rank: Option<u64>,
    pub(super) retired: bool,
}

impl Delivery {
    pub(super) fn expiry(&self) -> Option<MessageExpiry> {
        self.expiry.as_deref().copied()
    }

    pub(super) fn awaits_pubcomp(&self) -> bool {
        self.claim.is_some_and(|claim| claim.await_pubcomp)
    }

    pub(super) fn waiting_to_send(
        &self,
        generation: ConnectionGeneration,
        now: Option<MessageTime>,
    ) -> bool {
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
            Phase::Queued
            | Phase::ClaimPending(_)
            | Phase::InlineClaimPending(_)
            | Phase::ConsumePending(_, _) => true,
        }
    }

    pub(super) fn ready_to_send(&self, generation: ConnectionGeneration, room: bool) -> bool {
        match self.phase {
            Phase::Queued => self.payload.is_some() && (room || self.qos == Qos::AtMostOnce),
            Phase::AwaitPuback => {
                self.awaits_pubcomp()
                    || (self.payload.is_some() && (room || self.window == Some(generation)))
            }
            _ => false,
        }
    }

    pub(super) fn expiry_allows_send(&self, now: Option<MessageTime>) -> bool {
        self.claim
            .is_some_and(|claim| claim.started || claim.await_pubcomp)
            || self.expiry().is_none_or(|expiry| {
                now.is_some_and(|now| matches!(expiry.state(now), Ok(ExpiryState::Fresh { .. })))
            })
    }

    pub(super) fn control_ready(&self, now: Option<MessageTime>) -> bool {
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
                        && self.expiry().is_some_and(|expiry| {
                            now.is_some_and(|now| expiry.state(now) == Ok(ExpiryState::Expired))
                        })
                }
                _ => false,
            }
    }

    pub(super) fn in_flight(&self) -> Option<&InFlight> {
        match &self.phase {
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

    pub(super) fn in_flight_mut(&mut self) -> Option<&mut InFlight> {
        match &mut self.phase {
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

    pub(super) fn metadata(
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
            properties: None,
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
            pubrec_rank: None,
            retired: false,
        }
    }
}

/// Per-session delivery index and O(1) counters consolidated into a single
/// `FlatMap` entry and kept warm across 0<->1 steady-state transitions until
/// session removal.
#[derive(Default, Debug)]
pub(super) struct SessionDeliveries {
    pub(super) all: SmallIndexSet,
    pub(super) inline_pending: SmallIndexSet,
    /// Non-retired, non-busy deliveries that are not yet transmitted
    /// (`!(phase == AwaitPuback && frame.is_some())`). When all sent frames
    /// belong to the current generation, every delivery satisfying
    /// `waiting_to_send` lives in this small set.
    pub(super) unsent: SmallIndexSet,
    pub(super) busy: u16,
    pub(super) queued: u16,
    sent_gen: Option<ConnectionGeneration>,
    sent_count: u16,
    sent_mixed: u16,
    used_pi_mask: [u64; PI_BITMAP_WORDS],
    pi_collisions: u16,
}

impl SessionDeliveries {
    pub(super) fn waiting_candidates(&self, generation: ConnectionGeneration) -> &SmallIndexSet {
        if self.sent_mixed == 0 && (self.sent_count == 0 || self.sent_gen == Some(generation)) {
            &self.unsent
        } else {
            &self.all
        }
    }

    pub(super) fn used_pi_mask(&self) -> [u64; PI_BITMAP_WORDS] {
        self.used_pi_mask
    }

    fn add_sent(&mut self, generation: ConnectionGeneration) {
        if self.sent_gen == Some(generation) {
            self.sent_count = self.sent_count.saturating_add(1);
        } else if self.sent_count == 0 && self.sent_mixed == 0 {
            self.sent_gen = Some(generation);
            self.sent_count = 1;
        } else {
            self.sent_mixed = self.sent_mixed.saturating_add(1);
        }
    }

    fn sub_sent(&mut self, generation: ConnectionGeneration) {
        if self.sent_gen == Some(generation) && self.sent_count > 0 {
            self.sent_count -= 1;
            if self.sent_count == 0 && self.sent_mixed == 0 {
                self.sent_gen = None;
            }
        } else {
            self.sent_mixed = self.sent_mixed.saturating_sub(1);
        }
    }

    pub(super) fn sent_on(
        &self,
        generation: ConnectionGeneration,
        deliveries: &BoundedSlab<Delivery>,
    ) -> usize {
        if self.sent_mixed == 0 {
            if self.sent_gen == Some(generation) {
                usize::from(self.sent_count)
            } else {
                0
            }
        } else {
            self.all
                .iter()
                .filter(|&idx| {
                    let d = &deliveries[idx];
                    Deliveries::sent_gen(d).is_some_and(|(_, g)| g == generation)
                })
                .count()
        }
    }

    fn add_pi(&mut self, packet_id: OutboundPacketId) {
        let id = packet_id.get();
        if id <= OUTGOING_PI_BITMAP_BITS {
            let zero_based = usize::from(id - 1);
            let word = zero_based / 64;
            let bit = 1u64 << (zero_based % 64);
            if self.used_pi_mask[word] & bit != 0 {
                self.pi_collisions = self.pi_collisions.saturating_add(1);
            } else {
                self.used_pi_mask[word] |= bit;
            }
        }
    }

    fn sub_pi(&mut self, packet_id: OutboundPacketId, deliveries: &BoundedSlab<Delivery>) {
        let id = packet_id.get();
        if id <= OUTGOING_PI_BITMAP_BITS {
            if self.pi_collisions == 0 {
                let zero_based = usize::from(id - 1);
                self.used_pi_mask[zero_based / 64] &= !(1u64 << (zero_based % 64));
            } else {
                let mut mask = [0u64; PI_BITMAP_WORDS];
                let mut collisions = 0u16;
                for idx in &self.all {
                    if let Some(claim) = deliveries[idx].claim {
                        let cid = claim.packet_id.get();
                        if cid <= OUTGOING_PI_BITMAP_BITS {
                            let zero_based = usize::from(cid - 1);
                            let word = zero_based / 64;
                            let bit = 1u64 << (zero_based % 64);
                            if mask[word] & bit != 0 {
                                collisions = collisions.saturating_add(1);
                            } else {
                                mask[word] |= bit;
                            }
                        }
                    }
                }
                self.used_pi_mask = mask;
                self.pi_collisions = collisions;
            }
        }
    }
}

/// Encapsulated table of outgoing QoS 1/2 deliveries and all secondary indices.
#[derive(Debug)]
pub(super) struct Deliveries {
    rows: BoundedSlab<Delivery>,
    by_id: ScopedFlatMap<DeliveryId, usize>,
    by_op: ScopedFlatMap<OperationId, usize>,
    by_deadline: FlatMap<DeadlineToken, usize>,
    by_frame: ScopedFlatMap<EffectId, usize>,
    by_claim_op: ScopedFlatMap<OperationId, SmallIndexSet>,
    by_claim_result_op: ScopedFlatMap<OperationId, usize>,
    by_target_pi: FlatMap<(SessionId, OutboundPacketId), usize>,
    by_session: FlatMap<SessionId, SessionDeliveries>,
    by_window: FlatMap<ConnectionGeneration, SmallIndexSet>,
    control_candidates: BTreeSet<usize>,
    expiring: BTreeSet<usize>,
    pubrec_pending_count: usize,
    loaded_payload_bytes: u64,
}

impl Deliveries {
    pub(super) fn new(bound: NonZeroU32) -> Result<Self, AllocationFailed> {
        Ok(Self {
            rows: super::slab(bound)?,
            by_id: ScopedFlatMap::new(),
            by_op: ScopedFlatMap::new(),
            by_deadline: FlatMap::new(),
            by_frame: ScopedFlatMap::new(),
            by_claim_op: ScopedFlatMap::new(),
            by_claim_result_op: ScopedFlatMap::new(),
            by_target_pi: FlatMap::new(),
            by_session: FlatMap::new(),
            by_window: FlatMap::new(),
            control_candidates: BTreeSet::new(),
            expiring: BTreeSet::new(),
            pubrec_pending_count: 0,
            loaded_payload_bytes: 0,
        })
    }

    pub(super) fn is_control_candidate(d: &Delivery) -> bool {
        !d.retired
            && (matches!(d.phase, Phase::StartRetry | Phase::ExpireRetry)
                || (d.phase == Phase::AwaitPuback
                    && (d.early_pubrec.is_some()
                        || d.early_puback
                        || (d.certain_never_started && d.frame.is_none() && d.expiry.is_some()))))
    }

    pub(super) fn is_pubrec_pending(d: &Delivery) -> bool {
        !d.retired
            && !d.awaits_pubcomp()
            && (matches!(d.phase, Phase::PubrelPending(_))
                || d.early_pubrec.is_some_and(|reason| reason < 0x80))
    }

    pub(super) fn sent_gen(d: &Delivery) -> Option<(SessionId, ConnectionGeneration)> {
        if !d.retired && d.phase == Phase::AwaitPuback {
            d.frame.map(|(_, key)| (d.target, key.generation))
        } else {
            None
        }
    }

    pub(super) fn is_busy_delivery(d: &Delivery) -> bool {
        d.retired
            || matches!(
                d.phase,
                Phase::FinishPending(_)
                    | Phase::DiscardPending(_)
                    | Phase::PubrelPending(_)
                    | Phase::StartPending(_)
                    | Phase::ExpirePending(_)
                    | Phase::StartRetry
                    | Phase::ExpireRetry
                    | Phase::Qos0Writing(_)
            )
    }

    fn is_inline_pending(d: &Delivery) -> bool {
        !d.retired && matches!(d.phase, Phase::InlineClaimPending(_))
    }

    fn is_queued_delivery(d: &Delivery) -> bool {
        !d.retired && d.phase == Phase::Queued
    }

    fn is_unsent_delivery(d: &Delivery) -> bool {
        !d.retired
            && !Self::is_busy_delivery(d)
            && !(d.phase == Phase::AwaitPuback && d.frame.is_some())
    }

    pub(super) fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub(super) fn len(&self) -> usize {
        self.rows.len()
    }

    pub(super) fn capacity(&self) -> usize {
        self.rows.capacity()
    }

    pub(super) fn get(&self, slot: usize) -> Option<&Delivery> {
        self.rows.get(slot)
    }

    pub(super) fn row(&self, slot: usize) -> &Delivery {
        &self.rows[slot]
    }

    pub(super) fn slab(&self) -> &BoundedSlab<Delivery> {
        &self.rows
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (usize, &Delivery)> {
        self.rows.iter()
    }

    pub(super) fn values(&self) -> impl Iterator<Item = &Delivery> {
        self.rows.values()
    }

    #[allow(dead_code)]
    pub(super) fn to_vec(&self) -> alloc::vec::Vec<Delivery> {
        self.rows.values().cloned().collect()
    }

    pub(super) fn slot_of(&self, id: DeliveryId) -> Option<usize> {
        self.by_id.get(&id).copied()
    }

    pub(super) fn slot_by_op(&self, op: OperationId) -> Option<usize> {
        self.by_op.get(&op).copied()
    }

    pub(super) fn slot_by_deadline(&self, token: DeadlineToken) -> Option<usize> {
        self.by_deadline.get(&token).copied()
    }

    pub(super) fn slot_by_frame(&self, effect: EffectId) -> Option<usize> {
        self.by_frame.get(&effect).copied()
    }

    pub(super) fn slots_by_claim_op(&self, op: OperationId) -> Option<&SmallIndexSet> {
        self.by_claim_op.get(&op)
    }

    pub(super) fn slot_by_target_pi(
        &self,
        session: SessionId,
        packet_id: OutboundPacketId,
    ) -> Option<usize> {
        self.by_target_pi.get(&(session, packet_id)).copied()
    }

    pub(super) fn session(&self, session: SessionId) -> Option<&SessionDeliveries> {
        self.by_session.get(&session)
    }

    pub(super) fn control_candidates(&self) -> &BTreeSet<usize> {
        &self.control_candidates
    }

    pub(super) fn has_expiring(&self) -> bool {
        !self.expiring.is_empty()
    }

    pub(super) fn loaded_bytes(&self) -> u64 {
        self.loaded_payload_bytes
    }

    pub(super) fn active(&self, generation: ConnectionGeneration) -> usize {
        self.by_window
            .get(&generation)
            .map_or(0, SmallIndexSet::len)
    }

    pub(super) fn deadline_is_live(&self, token: DeadlineToken) -> bool {
        self.by_deadline.contains_key(&token)
    }

    pub(super) fn known_claim_result(&self, operation: OperationId) -> Option<&Finished> {
        self.by_claim_result_op
            .get(&operation)
            .and_then(|&idx| self.rows.get(idx))
            .and_then(|d| d.claim_result.as_deref())
    }

    pub(super) fn qos0_writing(
        &self,
        session: SessionId,
        generation: ConnectionGeneration,
    ) -> usize {
        let Some(sd) = self.by_session.get(&session) else {
            return 0;
        };
        sd.all
            .iter()
            .filter(|&index| {
                let d = &self.rows[index];
                matches!(d.phase, Phase::Qos0Writing(_))
                    && d.frame.is_some_and(|(_, key)| key.generation == generation)
            })
            .count()
    }

    pub(super) fn accepts_message_time(&self, now: MessageTime) -> bool {
        self.expiring.is_empty()
            || self
                .expiring
                .iter()
                .filter_map(|&idx| self.rows[idx].expiry())
                .all(|expiry| expiry.state(now).is_ok())
    }

    pub(super) fn has_control_work(&self, now: Option<MessageTime>) -> bool {
        self.control_candidates
            .iter()
            .any(|&idx| self.rows[idx].control_ready(now))
    }

    pub(super) fn retired_flights(&self) -> usize {
        self.rows
            .values()
            .filter(|d| {
                d.retired
                    && (d.in_flight().is_some() || matches!(d.phase, Phase::InlineClaimPending(_)))
            })
            .count()
    }

    fn index(&mut self, index: usize) {
        let d = &self.rows[index];
        self.by_id.insert(d.id, index);
        if let Some(f) = d.in_flight() {
            self.by_op.insert(f.operation, index);
            self.by_deadline.insert(f.deadline, index);
        }
        if let Some((effect, _)) = d.frame {
            self.by_frame.insert(effect, index);
        }
        if let Some(claim) = d.claim {
            self.by_claim_op
                .get_or_insert_default(claim.operation)
                .insert(index);
            if !d.retired {
                self.by_target_pi.insert((d.target, claim.packet_id), index);
            }
        }
        if let Some(res) = d.claim_result.as_deref() {
            self.by_claim_result_op.insert(res.operation, index);
        }
        if let Some(generation) = d.window {
            self.by_window
                .get_or_insert_default(generation)
                .insert(index);
        }
        {
            let sd = self.by_session.get_or_insert_default(d.target);
            sd.all.insert(index);
            if let Some(claim) = d.claim {
                sd.add_pi(claim.packet_id);
            }
            if let Some((_, generation)) = Self::sent_gen(d) {
                sd.add_sent(generation);
            }
            if Self::is_busy_delivery(d) {
                sd.busy = sd.busy.saturating_add(1);
            }
            if Self::is_inline_pending(d) {
                sd.inline_pending.insert(index);
            }
            if Self::is_unsent_delivery(d) {
                sd.unsent.insert(index);
            }
            if Self::is_queued_delivery(d) {
                sd.queued = sd.queued.saturating_add(1);
            }
        }
        if let Some(payload) = &d.payload {
            self.loaded_payload_bytes = self
                .loaded_payload_bytes
                .saturating_add(payload.len() as u64);
        }
        if Self::is_control_candidate(d) {
            self.control_candidates.insert(index);
        }
        if d.expiry.is_some() {
            self.expiring.insert(index);
        }
        if Self::is_pubrec_pending(d) {
            self.pubrec_pending_count += 1;
        }
    }

    fn unindex(&mut self, index: usize) {
        let d = &self.rows[index];
        self.by_id.remove(&d.id);
        if let Some(f) = d.in_flight() {
            self.by_op.remove(&f.operation);
            self.by_deadline.remove(&f.deadline);
        }
        if let Some((effect, _)) = d.frame {
            self.by_frame.remove(&effect);
        }
        let removed_pi = if let Some(claim) = d.claim {
            if let Some(set) = self.by_claim_op.get_mut(&claim.operation) {
                set.remove(&index);
                if set.is_empty() {
                    self.by_claim_op.remove(&claim.operation);
                }
            }
            if !d.retired {
                self.by_target_pi.remove(&(d.target, claim.packet_id));
            }
            Some(claim.packet_id)
        } else {
            None
        };
        if let Some(res) = d.claim_result.as_deref() {
            self.by_claim_result_op.remove(&res.operation);
        }
        if let Some(generation) = d.window
            && let Some(set) = self.by_window.get_mut(&generation)
        {
            set.remove(&index);
        }
        let target = d.target;
        let sent_gen = Self::sent_gen(d).map(|(_, g)| g);
        let was_busy = Self::is_busy_delivery(d);
        let was_inline = Self::is_inline_pending(d);
        let was_unsent = Self::is_unsent_delivery(d);
        let was_queued = Self::is_queued_delivery(d);
        let payload_len = d.payload.as_ref().map_or(0, |p| p.len() as u64);
        let was_pubrec = Self::is_pubrec_pending(d);
        if let Some(sd) = self.by_session.get_mut(&target) {
            sd.all.remove(&index);
            if let Some(pi) = removed_pi {
                sd.sub_pi(pi, &self.rows);
            }
            if let Some(generation) = sent_gen {
                sd.sub_sent(generation);
            }
            if was_busy {
                sd.busy = sd.busy.saturating_sub(1);
            }
            if was_inline {
                sd.inline_pending.remove(&index);
            }
            if was_unsent {
                sd.unsent.remove(&index);
            }
            if was_queued {
                sd.queued = sd.queued.saturating_sub(1);
            }
        }
        if payload_len != 0 {
            self.loaded_payload_bytes = self.loaded_payload_bytes.saturating_sub(payload_len);
        }
        self.control_candidates.remove(&index);
        self.expiring.remove(&index);
        if was_pubrec {
            self.pubrec_pending_count = self.pubrec_pending_count.saturating_sub(1);
        }
    }

    pub(super) fn push(&mut self, delivery: Delivery) -> Result<(), CoreFault> {
        let index = self
            .rows
            .insert(delivery)
            .map_err(|_| CoreFault::StalePlan)?;
        self.index(index);
        Ok(())
    }

    pub(super) fn remove(&mut self, index: usize) {
        self.unindex(index);
        let _ = self.rows.remove(index);
    }

    /// Rewrites arbitrary fields of a delivery row, fully refreshing all
    /// secondary indices around the change.
    pub(super) fn rewrite(&mut self, index: usize, f: impl FnOnce(&mut Delivery)) {
        self.unindex(index);
        f(&mut self.rows[index]);
        self.index(index);
    }

    /// Updates a `Delivery` in place when `id`, `target`, `payload`, and `expiry`
    /// are unchanged, touching only the secondary indices whose keys changed.
    pub(super) fn mutate(&mut self, index: usize, f: impl FnOnce(&mut Delivery)) {
        let d = &self.rows[index];
        let old_flight = d.in_flight().map(|fl| (fl.operation, fl.deadline));
        let old_frame = d.frame.map(|(e, _)| e);
        let old_claim = d.claim.map(|c| c.operation);
        let old_pi = d.claim.map(|c| c.packet_id);
        let old_target_pi = (!d.retired)
            .then(|| d.claim.map(|c| (d.target, c.packet_id)))
            .flatten();
        let old_claim_res = d.claim_result.as_deref().map(|r| r.operation);
        let old_window = d.window;
        let old_sent = Self::sent_gen(d).map(|(_, g)| g);
        let old_busy = Self::is_busy_delivery(d);
        let old_inline = Self::is_inline_pending(d);
        let old_unsent = Self::is_unsent_delivery(d);
        let old_queued = Self::is_queued_delivery(d);
        let old_ctrl = Self::is_control_candidate(d);
        let old_pubrec = Self::is_pubrec_pending(d);

        f(&mut self.rows[index]);

        let d = &self.rows[index];
        let target = d.target;
        let new_flight = d.in_flight().map(|fl| (fl.operation, fl.deadline));
        let new_frame = d.frame.map(|(e, _)| e);
        let new_claim = d.claim.map(|c| c.operation);
        let new_pi = d.claim.map(|c| c.packet_id);
        let new_target_pi = (!d.retired)
            .then(|| d.claim.map(|c| (d.target, c.packet_id)))
            .flatten();
        let new_claim_res = d.claim_result.as_deref().map(|r| r.operation);
        let new_window = d.window;
        let new_sent = Self::sent_gen(d).map(|(_, g)| g);
        let new_busy = Self::is_busy_delivery(d);
        let new_inline = Self::is_inline_pending(d);
        let new_unsent = Self::is_unsent_delivery(d);
        let new_queued = Self::is_queued_delivery(d);
        let new_ctrl = Self::is_control_candidate(d);
        let new_pubrec = Self::is_pubrec_pending(d);

        if old_flight != new_flight {
            if let Some((op, dl)) = old_flight {
                self.by_op.remove(&op);
                self.by_deadline.remove(&dl);
            }
            if let Some((op, dl)) = new_flight {
                self.by_op.insert(op, index);
                self.by_deadline.insert(dl, index);
            }
        }
        if old_frame != new_frame {
            if let Some(e) = old_frame {
                self.by_frame.remove(&e);
            }
            if let Some(e) = new_frame {
                self.by_frame.insert(e, index);
            }
        }
        if old_claim != new_claim {
            if let Some(op) = old_claim
                && let Some(set) = self.by_claim_op.get_mut(&op)
            {
                set.remove(&index);
                if set.is_empty() {
                    self.by_claim_op.remove(&op);
                }
            }
            if let Some(op) = new_claim {
                self.by_claim_op.get_or_insert_default(op).insert(index);
            }
        }
        if old_target_pi != new_target_pi {
            if let Some(key) = old_target_pi {
                self.by_target_pi.remove(&key);
            }
            if let Some(key) = new_target_pi {
                self.by_target_pi.insert(key, index);
            }
        }
        if old_claim_res != new_claim_res {
            if let Some(op) = old_claim_res {
                self.by_claim_result_op.remove(&op);
            }
            if let Some(op) = new_claim_res {
                self.by_claim_result_op.insert(op, index);
            }
        }
        if old_window != new_window {
            if let Some(generation) = old_window
                && let Some(set) = self.by_window.get_mut(&generation)
            {
                set.remove(&index);
            }
            if let Some(generation) = new_window {
                self.by_window
                    .get_or_insert_default(generation)
                    .insert(index);
            }
        }
        if (old_pi != new_pi
            || old_sent != new_sent
            || old_busy != new_busy
            || old_inline != new_inline
            || old_unsent != new_unsent
            || old_queued != new_queued)
            && let Some(sd) = self.by_session.get_mut(&target)
        {
            if old_pi != new_pi {
                if let Some(pi) = old_pi {
                    sd.sub_pi(pi, &self.rows);
                }
                if let Some(pi) = new_pi {
                    sd.add_pi(pi);
                }
            }
            if old_sent != new_sent {
                if let Some(generation) = old_sent {
                    sd.sub_sent(generation);
                }
                if let Some(generation) = new_sent {
                    sd.add_sent(generation);
                }
            }
            if old_busy != new_busy {
                if old_busy {
                    sd.busy = sd.busy.saturating_sub(1);
                }
                if new_busy {
                    sd.busy = sd.busy.saturating_add(1);
                }
            }
            if old_inline != new_inline {
                if old_inline {
                    sd.inline_pending.remove(&index);
                }
                if new_inline {
                    sd.inline_pending.insert(index);
                }
            }
            if old_unsent != new_unsent {
                if old_unsent {
                    sd.unsent.remove(&index);
                }
                if new_unsent {
                    sd.unsent.insert(index);
                }
            }
            if old_queued != new_queued {
                if old_queued {
                    sd.queued = sd.queued.saturating_sub(1);
                }
                if new_queued {
                    sd.queued = sd.queued.saturating_add(1);
                }
            }
        }
        if old_ctrl != new_ctrl {
            if new_ctrl {
                self.control_candidates.insert(index);
            } else {
                self.control_candidates.remove(&index);
            }
        }
        if old_pubrec != new_pubrec {
            if new_pubrec {
                self.pubrec_pending_count += 1;
            } else {
                self.pubrec_pending_count = self.pubrec_pending_count.saturating_sub(1);
            }
        }
    }

    pub(super) fn drop_session_cache(&mut self, session: SessionId) {
        if self
            .by_session
            .get(&session)
            .is_some_and(|sd| sd.all.is_empty())
        {
            self.by_session.remove(&session);
        }
    }

    pub(super) fn close_generation(&mut self, generation: ConnectionGeneration) {
        if let Some(indices) = self.by_window.remove(&generation) {
            for idx in indices {
                self.rewrite(idx, |delivery| {
                    if delivery.window == Some(generation) {
                        delivery.window = None;
                        if delivery.frame.is_some() {
                            delivery.certain_never_started = false;
                        }
                    }
                });
            }
        }
    }

    pub(super) fn retire_session(&mut self, session: SessionId) {
        let indices: alloc::vec::Vec<usize> = self
            .by_session
            .get(&session)
            .map(|sd| sd.all.iter().collect())
            .unwrap_or_default();
        for index in indices {
            let keep = {
                let d = &self.rows[index];
                d.in_flight().is_some()
                    || matches!(
                        d.phase,
                        Phase::Qos0Writing(_) | Phase::InlineClaimPending(_)
                    )
            };
            if keep {
                self.rewrite(index, |d| {
                    d.retired = true;
                    d.window = None;
                    d.topic = None;
                    // A consumed retained replay can still be held by its
                    // adapter. Keep its shared payload charged until the
                    // exact write or refusal removes this retired row.
                    if d.cause != DeliveryCause::RetainedReplay
                        || !matches!(d.phase, Phase::Qos0Writing(_))
                    {
                        d.payload = None;
                    }
                    d.properties = None;
                });
            } else {
                self.remove(index);
            }
        }
    }

    pub(super) fn evict_expired(&mut self, now: MessageTime) {
        if self.expiring.is_empty() {
            return;
        }
        let expired: alloc::vec::Vec<usize> = self
            .expiring
            .iter()
            .copied()
            .filter(|&idx| {
                let d = &self.rows[idx];
                d.claim.is_none()
                    && d.phase == Phase::Queued
                    && d.expiry()
                        .is_some_and(|expiry| expiry.state(now) == Ok(ExpiryState::Expired))
            })
            .collect();
        for index in expired {
            self.remove(index);
        }
    }

    pub(super) fn free_packet_id(
        &self,
        session: SessionId,
        maximum: u16,
        mutant_early_pi_release: bool,
        mutant_ignore_occupied_pi: bool,
    ) -> Option<OutboundPacketId> {
        let Some(sd) = self.by_session.get(&session) else {
            return OutboundPacketId::new(1).filter(|id| id.get() <= maximum);
        };
        if sd.all.is_empty() || mutant_ignore_occupied_pi {
            return OutboundPacketId::new(1).filter(|id| id.get() <= maximum);
        }
        if !mutant_early_pi_release && sd.pi_collisions == 0 {
            for (word_idx, &word) in sd.used_pi_mask.iter().enumerate() {
                if word != u64::MAX {
                    let candidate = (word_idx * 64 + word.trailing_ones() as usize + 1) as u16;
                    if candidate <= maximum {
                        return OutboundPacketId::new(candidate);
                    }
                    return None;
                }
            }
            if maximum <= OUTGOING_PI_BITMAP_BITS {
                return None;
            }
        }
        let reserves = |delivery: &Delivery| {
            delivery.target == session
                && delivery.claim.is_some()
                && !(mutant_early_pi_release && matches!(delivery.phase, Phase::FinishPending(_)))
        };
        let mut mask = [0u64; PI_BITMAP_WORDS];
        for idx in &sd.all {
            let delivery = &self.rows[idx];
            if reserves(delivery)
                && let Some(claim) = delivery.claim
            {
                let id = claim.packet_id.get();
                if id <= OUTGOING_PI_BITMAP_BITS {
                    let zero_based = usize::from(id - 1);
                    mask[zero_based / 64] |= 1u64 << (zero_based % 64);
                }
            }
        }
        for (word_idx, &word) in mask.iter().enumerate() {
            if word != u64::MAX {
                let candidate = (word_idx * 64 + word.trailing_ones() as usize + 1) as u16;
                if candidate <= maximum {
                    return OutboundPacketId::new(candidate);
                }
                return None;
            }
        }
        if maximum <= OUTGOING_PI_BITMAP_BITS {
            return None;
        }
        let mut used = BTreeSet::new();
        for idx in &sd.all {
            let delivery = &self.rows[idx];
            if reserves(delivery)
                && let Some(claim) = delivery.claim
            {
                used.insert(claim.packet_id.get());
            }
        }
        let candidates = outgoing_pi_candidate_limit(used.len(), maximum);
        for raw in usize::from(OUTGOING_PI_BITMAP_BITS) + 1..=candidates {
            let id = u16::try_from(raw).ok()?;
            if !used.contains(&id) {
                return OutboundPacketId::new(id);
            }
        }
        None
    }

    pub(super) fn next_send(
        &self,
        session: SessionId,
        generation: ConnectionGeneration,
        now: Option<MessageTime>,
        room: bool,
    ) -> Option<(usize, &Delivery)> {
        let sd = self.by_session.get(&session)?;
        if sd.all.is_empty() {
            return None;
        }
        let sent = sd.sent_on(generation, &self.rows);
        if sent + usize::from(sd.busy) == sd.all.len() {
            return None;
        }
        let mut publish: Option<(usize, &Delivery)> = None;
        let mut pubrel: Option<(usize, &Delivery)> = None;
        let preparing = if self.pubrec_pending_count == 0 {
            None
        } else {
            sd.all
                .iter()
                .filter_map(|idx| {
                    let d = &self.rows[idx];
                    if Self::is_pubrec_pending(d) {
                        d.pubrec_rank
                    } else {
                        None
                    }
                })
                .min()
        };
        let received = |delivery: &Delivery| delivery.pubrec_rank;
        for index in sd.waiting_candidates(generation) {
            let delivery = &self.rows[index];
            if !delivery.waiting_to_send(generation, now) {
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

    pub(super) fn next_pipelined_claim(
        &self,
        session: SessionId,
        generation: ConnectionGeneration,
        now: Option<MessageTime>,
        room: bool,
    ) -> (Option<(usize, &Delivery)>, bool) {
        if !room {
            return (None, false);
        }
        let Some(sd) = self.by_session.get(&session) else {
            return (None, true);
        };
        let indices = sd.waiting_candidates(generation);
        let mut blocker: Option<DeliveryId> = None;
        let mut best_queued: Option<(usize, &Delivery)> = None;
        let mut blocked_all = false;
        for idx in indices {
            let d = &self.rows[idx];
            if !d.waiting_to_send(generation, now) || d.awaits_pubcomp() {
                continue;
            }
            match &d.phase {
                Phase::ClaimPending(f) if f.phase == CommandPhase::Submitted => {}
                Phase::AwaitPuback if d.window == Some(generation) => {}
                Phase::Queued if d.qos != Qos::AtMostOnce && d.payload.is_some() => {
                    if best_queued.is_none_or(|(_, prev)| d.id < prev.id) {
                        best_queued = Some((idx, d));
                    }
                }
                _ => {
                    blocked_all = true;
                    if blocker.is_none_or(|prev| d.id < prev) {
                        blocker = Some(d.id);
                    }
                }
            }
        }
        if let Some((idx, d)) = best_queued {
            if blocker.is_none_or(|b| d.id < b) {
                return (Some((idx, d)), false);
            }
            return (None, false);
        }
        (None, !blocked_all)
    }

    pub(super) fn rebuild_indices(&mut self) {
        self.by_id.clear();
        self.by_op.clear();
        self.by_deadline.clear();
        self.by_frame.clear();
        self.by_claim_op.clear();
        self.by_claim_result_op.clear();
        self.by_target_pi.clear();
        self.by_session.clear();
        self.by_window.clear();
        self.control_candidates.clear();
        self.expiring.clear();
        self.pubrec_pending_count = 0;
        self.loaded_payload_bytes = 0;
        let live: alloc::vec::Vec<usize> = self.rows.iter().map(|(idx, _)| idx).collect();
        for index in live {
            self.index(index);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::string::String;
    use alloc::vec::Vec;
    use core::fmt::Write;

    type SessionDeliveriesSnapshot = (
        Vec<usize>,
        Vec<usize>,
        Vec<usize>,
        u16,
        u16,
        [u64; PI_BITMAP_WORDS],
        u16,
        Vec<(ConnectionGeneration, usize)>,
    );

    impl SessionDeliveries {
        fn is_clean_empty(&self) -> bool {
            self.all.is_empty()
                && self.inline_pending.is_empty()
                && self.unsent.is_empty()
                && self.busy == 0
                && self.queued == 0
                && self.sent_count == 0
                && self.sent_mixed == 0
                && self.used_pi_mask == [0u64; PI_BITMAP_WORDS]
                && self.pi_collisions == 0
        }

        fn snapshot_parts(&self, deliveries: &BoundedSlab<Delivery>) -> SessionDeliveriesSnapshot {
            let mut all: Vec<usize> = self.all.iter().collect();
            all.sort_unstable();
            let mut inline_pending: Vec<usize> = self.inline_pending.iter().collect();
            inline_pending.sort_unstable();
            let mut unsent: Vec<usize> = self.unsent.iter().collect();
            unsent.sort_unstable();
            let mut gens = BTreeSet::new();
            for idx in &self.all {
                if let Some((_, g)) = Deliveries::sent_gen(&deliveries[idx]) {
                    gens.insert(g);
                }
            }
            let sent: Vec<(ConnectionGeneration, usize)> = gens
                .into_iter()
                .map(|g| (g, self.sent_on(g, deliveries)))
                .collect();
            (
                all,
                inline_pending,
                unsent,
                self.busy,
                self.queued,
                self.used_pi_mask,
                self.pi_collisions,
                sent,
            )
        }
    }

    impl Deliveries {
        pub(in crate::owner) fn index_snapshot(&self) -> Vec<String> {
            fn all<T: core::fmt::Debug>(entries: impl Iterator<Item = T>) -> String {
                let mut entries: Vec<String> = entries
                    .map(|e| {
                        let mut text = String::new();
                        write!(text, "{e:?}").unwrap();
                        text
                    })
                    .collect();
                entries.sort();
                entries.join(";")
            }
            fn one(value: impl core::fmt::Debug) -> String {
                let mut text = String::new();
                write!(text, "{value:?}").unwrap();
                text
            }
            for (&session, sd) in self.by_session.iter() {
                if sd.all.is_empty() {
                    assert!(
                        sd.is_clean_empty(),
                        "warm empty SessionDeliveries drifted for {session:?}: {sd:?}"
                    );
                }
            }
            let sessions = all(self
                .by_session
                .iter()
                .filter(|(_, sd)| !sd.all.is_empty())
                .map(|(&s, sd)| (s, sd.snapshot_parts(&self.rows))));
            let windows = all(self.by_window.iter().filter_map(|(&g, set)| {
                if set.is_empty() {
                    None
                } else {
                    let mut v: Vec<usize> = set.iter().collect();
                    v.sort_unstable();
                    Some((g, v))
                }
            }));
            let claim_ops = all(self.by_claim_op.iter().map(|(k, set)| {
                assert!(!set.is_empty(), "empty by_claim_op bucket leaked for {k:?}");
                let mut v: Vec<usize> = set.iter().collect();
                v.sort_unstable();
                (k, v)
            }));
            alloc::vec![
                all(self.by_id.iter().map(|(k, &v)| (k, v))),
                all(self.by_op.iter().map(|(k, &v)| (k, v))),
                all(self.by_deadline.iter().map(|(&k, &v)| (k, v))),
                all(self.by_frame.iter().map(|(k, &v)| (k, v))),
                claim_ops,
                all(self.by_claim_result_op.iter().map(|(k, &v)| (k, v))),
                all(self.by_target_pi.iter().map(|(&k, &v)| (k, v))),
                sessions,
                windows,
                one(&self.control_candidates),
                one(&self.expiring),
                one(self.pubrec_pending_count),
                one(self.loaded_payload_bytes),
            ]
        }

        pub(in crate::owner) fn row_mut(&mut self, slot: usize) -> &mut Delivery {
            &mut self.rows[slot]
        }

        pub(in crate::owner) fn last_mut(&mut self) -> Option<&mut Delivery> {
            self.rows.last_mut()
        }
    }

    const TARGET: SessionId = SessionId::new(7);

    fn claimed(index: u32, packet_id: u16) -> Delivery {
        let publication = PublicationId::new(
            StoreIncarnation::new(1),
            RunEpoch::new(1),
            LogicalOwnerId::new(0),
            PublicationSeq::new(1),
        );
        Delivery {
            id: DeliveryId { publication, index },
            target: TARGET,
            revision: Revision::new(1),
            qos: Qos::AtLeastOnce,
            cause: DeliveryCause::LiveExact,
            retain: false,
            expiry: None,
            properties: None,
            claim: Some(DeliveryClaim {
                operation: OperationId::new(
                    StoreIncarnation::new(1),
                    RunEpoch::new(1),
                    LogicalOwnerId::new(0),
                    OperationSeq::new(u64::from(index) + 1),
                ),
                packet_id: OutboundPacketId::new(packet_id).unwrap(),
                started: false,
                await_pubcomp: false,
            }),
            topic: None,
            payload: None,
            phase: Phase::AwaitPuback,
            claim_result: None,
            window: None,
            frame: None,
            local_write_completed: false,
            may_have_started: false,
            certain_never_started: false,
            early_puback: false,
            early_pubrec: None,
            pubrec_rank: None,
            retired: false,
        }
    }

    /// A session whose rows hold one identifier twice counts one collision.
    /// Moving another of its rows to a new identifier does not add one.
    #[test]
    fn a_new_identifier_counts_no_collision_it_does_not_make() {
        let mut deliveries = Deliveries::new(NonZeroU32::new(8).unwrap()).unwrap();
        for (index, packet_id) in [(0, 1), (1, 1), (2, 2)] {
            deliveries.push(claimed(index, packet_id)).unwrap();
        }
        let moved = *deliveries.by_id.get(&claimed(2, 2).id).unwrap();
        deliveries.mutate(moved, |row| {
            if let Some(claim) = row.claim.as_mut() {
                claim.packet_id = OutboundPacketId::new(3).unwrap();
            }
        });
        assert_eq!(deliveries.by_session[&TARGET].pi_collisions, 1);
        let kept = deliveries.index_snapshot();
        deliveries.rebuild_indices();
        assert_eq!(kept, deliveries.index_snapshot());
    }
}
