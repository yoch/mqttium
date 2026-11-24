//! The outgoing QoS 1 and 2 deliveries of the owner and their indices.
use super::connections::Connections;
use super::*;
use crate::contract::ids::OutboundPacketId;
use crate::contract::pubsub::DeliveryClaim;
use alloc::collections::BTreeSet;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
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
    Qos0Writing(Finished),
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub(super) struct Delivery {
    pub(super) id: DeliveryId,
    pub(super) target: SessionId,
    pub(super) revision: Revision,
    pub(super) qos: Qos,
    pub(super) cause: DeliveryCause,
    pub(super) retain: bool,
    pub(super) expiry: Option<MessageExpiry>,
    pub(super) properties: Option<alloc::sync::Arc<crate::contract::properties::PublishProperties>>,
    pub(super) claim: Option<DeliveryClaim>,
    pub(super) topic: Option<SharedBytes>,
    pub(super) payload: Option<SharedBytes>,
    pub(super) phase: Phase,
    pub(super) claim_result: Option<Finished>,
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
    /// (MQTT-4.6.0-4 / MQTT-4.6.0-6). Never overwritten by later retries or
    /// duplicate PUBRECs; `None` for a PUBREL recovered from an earlier run,
    /// which precedes those of this run.
    pub(super) pubrec_rank: Option<u64>,
    pub(super) retired: bool,
}

impl Delivery {
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
            || self.expiry.is_none_or(|expiry| {
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
                        && self.expiry.is_some_and(|expiry| {
                            now.is_some_and(|now| expiry.state(now) == Ok(ExpiryState::Expired))
                        })
                }
                _ => false,
            }
    }
    pub(super) fn in_flight(&self) -> Option<InFlight> {
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

/// The outgoing QoS 1 and 2 deliveries, and every index and count derived
/// from them. Each change of a row goes through the table, which keeps them.
#[derive(Debug)]
pub(super) struct Deliveries {
    rows: BoundedVec<Delivery>,
    delivery_by_id: ScopedFlatMap<DeliveryId, usize>,
    delivery_by_op: ScopedFlatMap<OperationId, usize>,
    delivery_by_deadline: FlatMap<DeadlineToken, usize>,
    delivery_by_frame: ScopedFlatMap<EffectId, usize>,
    delivery_by_claim_op: ScopedFlatMap<OperationId, usize>,
    delivery_by_claim_result_op: ScopedFlatMap<OperationId, usize>,
    delivery_by_target_pi: FlatMap<(SessionId, OutboundPacketId), usize>,
    deliveries_by_session: FlatMap<SessionId, SmallIndexSet>,
    deliveries_by_window: FlatMap<ConnectionGeneration, SmallIndexSet>,
    sent_on_gen: FlatMap<(SessionId, ConnectionGeneration), usize>,
    busy_by_session: FlatMap<SessionId, usize>,
    control_candidates: BTreeSet<usize>,
    expiring_deliveries: BTreeSet<usize>,
    pubrec_pending_count: usize,
    active_by_gen: FlatMap<ConnectionGeneration, usize>,
    loaded_payload_bytes: u64,
    next_pubrec_rank: u64,
}

impl Deliveries {
    pub(super) fn new(config: OwnerConfig) -> Result<Self, AllocationFailed> {
        Ok(Self {
            rows: table(config.max_exchanges)?,
            delivery_by_id: ScopedFlatMap::new(),
            delivery_by_op: ScopedFlatMap::new(),
            delivery_by_deadline: FlatMap::new(),
            delivery_by_frame: ScopedFlatMap::new(),
            delivery_by_claim_op: ScopedFlatMap::new(),
            delivery_by_claim_result_op: ScopedFlatMap::new(),
            delivery_by_target_pi: FlatMap::new(),
            deliveries_by_session: FlatMap::new(),
            deliveries_by_window: FlatMap::new(),
            sent_on_gen: FlatMap::new(),
            busy_by_session: FlatMap::new(),
            control_candidates: BTreeSet::new(),
            expiring_deliveries: BTreeSet::new(),
            pubrec_pending_count: 0,
            active_by_gen: FlatMap::new(),
            loaded_payload_bytes: 0,
            next_pubrec_rank: 1,
        })
    }

    /// The rows, read only. Every change goes through the table.
    pub(super) fn rows(&self) -> &[Delivery] {
        self.rows.as_slice()
    }
    pub(super) fn len(&self) -> usize {
        self.rows.len()
    }
    pub(super) fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
    /// The rows the table can hold.
    pub(super) fn capacity(&self) -> usize {
        self.rows.capacity()
    }
    /// Whether a new delivery would find no row.
    pub(super) fn is_full(&self) -> bool {
        self.rows.len() >= self.rows.capacity()
    }
    /// The rows of the deliveries that target `session`.
    pub(super) fn of_session(&self, session: SessionId) -> Option<&SmallIndexSet> {
        self.deliveries_by_session.get(&session)
    }
    /// The row whose command in flight is `operation`.
    pub(super) fn index_of_op(&self, operation: OperationId) -> Option<usize> {
        self.delivery_by_op.get(&operation).copied()
    }
    /// The row whose command in flight waits for `token`.
    pub(super) fn index_of_deadline(&self, token: DeadlineToken) -> Option<usize> {
        self.delivery_by_deadline.get(&token).copied()
    }
    /// The row whose frame is `effect`.
    pub(super) fn index_of_frame(&self, effect: EffectId) -> Option<usize> {
        self.delivery_by_frame.get(&effect).copied()
    }
    /// The row claimed by `operation`.
    pub(super) fn index_of_claim_op(&self, operation: OperationId) -> Option<usize> {
        self.delivery_by_claim_op.get(&operation).copied()
    }
    /// The live row that holds a packet identifier of a session.
    pub(super) fn index_of_target_pi(&self, key: (SessionId, OutboundPacketId)) -> Option<usize> {
        self.delivery_by_target_pi.get(&key).copied()
    }
    /// Whether a delivery may need a control command.
    pub(super) fn has_control_candidates(&self) -> bool {
        !self.control_candidates.is_empty()
    }
    /// The rows that may need a control command, in order.
    pub(super) fn control_candidates(&self) -> impl Iterator<Item = usize> + '_ {
        self.control_candidates.iter().copied()
    }
    /// Whether a delivery carries a Message Expiry Interval.
    pub(super) fn has_expiring(&self) -> bool {
        !self.expiring_deliveries.is_empty()
    }
    /// Whether `length` more loaded payload bytes stay within `limit`.
    pub(super) fn payload_fits(&self, length: u64, limit: u64) -> bool {
        self.loaded_payload_bytes
            .checked_add(length)
            .is_some_and(|total| total <= limit)
    }
    /// The rank the next first positive PUBREC takes.
    pub(super) fn next_rank(&self) -> u64 {
        self.next_pubrec_rank
    }
    /// A PUBREC took `rank`: the next one ranks after it.
    pub(super) fn advance_rank(&mut self, rank: u64) {
        self.next_pubrec_rank = self.next_pubrec_rank.max(rank.saturating_add(1));
    }
    /// Changes a row and every index and count its keys are in.
    pub(super) fn rewrite<R>(
        &mut self,
        index: usize,
        change: impl FnOnce(&mut Delivery) -> R,
    ) -> R {
        self.unindex_delivery(index);
        let result = change(&mut self.rows.as_mut_slice()[index]);
        self.index_delivery(index);
        result
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

    pub(super) fn index_delivery(&mut self, index: usize) {
        let d = &self.rows.as_slice()[index];
        self.delivery_by_id.insert(d.id, index);
        self.deliveries_by_session
            .get_or_insert_default(d.target)
            .insert(index);
        if let Some(f) = d.in_flight() {
            self.delivery_by_op.insert(f.operation, index);
            self.delivery_by_deadline.insert(f.deadline, index);
        }
        if let Some((effect, _)) = d.frame {
            self.delivery_by_frame.insert(effect, index);
        }
        if let Some(claim) = d.claim {
            self.delivery_by_claim_op.insert(claim.operation, index);
            if !d.retired {
                self.delivery_by_target_pi
                    .insert((d.target, claim.packet_id), index);
            }
        }
        if let Some(res) = d.claim_result {
            self.delivery_by_claim_result_op
                .insert(res.operation, index);
        }
        if let Some(generation) = d.window {
            *self.active_by_gen.get_or_insert_default(generation) += 1;
            self.deliveries_by_window
                .get_or_insert_default(generation)
                .insert(index);
        }
        if let Some(key) = Self::sent_gen(d) {
            *self.sent_on_gen.get_or_insert_default(key) += 1;
        }
        if Self::is_busy_delivery(d) {
            *self.busy_by_session.get_or_insert_default(d.target) += 1;
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
            self.expiring_deliveries.insert(index);
        }
        if Self::is_pubrec_pending(d) {
            self.pubrec_pending_count += 1;
        }
    }

    pub(super) fn unindex_delivery(&mut self, index: usize) {
        let d = &self.rows.as_slice()[index];
        self.delivery_by_id.remove(&d.id);
        if let Some(set) = self.deliveries_by_session.get_mut(&d.target) {
            set.remove(&index);
            if set.is_empty() {
                self.deliveries_by_session.remove(&d.target);
            }
        }
        if let Some(f) = d.in_flight() {
            self.delivery_by_op.remove(&f.operation);
            self.delivery_by_deadline.remove(&f.deadline);
        }
        if let Some((effect, _)) = d.frame {
            self.delivery_by_frame.remove(&effect);
        }
        if let Some(claim) = d.claim {
            self.delivery_by_claim_op.remove(&claim.operation);
            if !d.retired {
                self.delivery_by_target_pi
                    .remove(&(d.target, claim.packet_id));
            }
        }
        if let Some(res) = d.claim_result {
            self.delivery_by_claim_result_op.remove(&res.operation);
        }
        if let Some(generation) = d.window {
            if let Some(count) = self.active_by_gen.get_mut(&generation) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    self.active_by_gen.remove(&generation);
                }
            }
            if let Some(set) = self.deliveries_by_window.get_mut(&generation) {
                set.remove(&index);
                if set.is_empty() {
                    self.deliveries_by_window.remove(&generation);
                }
            }
        }
        if let Some(key) = Self::sent_gen(d)
            && let Some(count) = self.sent_on_gen.get_mut(&key)
        {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.sent_on_gen.remove(&key);
            }
        }
        if Self::is_busy_delivery(d)
            && let Some(count) = self.busy_by_session.get_mut(&d.target)
        {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.busy_by_session.remove(&d.target);
            }
        }
        if let Some(payload) = &d.payload {
            self.loaded_payload_bytes = self
                .loaded_payload_bytes
                .saturating_sub(payload.len() as u64);
        }
        self.control_candidates.remove(&index);
        self.expiring_deliveries.remove(&index);
        if Self::is_pubrec_pending(d) {
            self.pubrec_pending_count = self.pubrec_pending_count.saturating_sub(1);
        }
    }

    pub(super) fn reindex_moved_delivery(&mut self, from: usize, to: usize) {
        let d = &self.rows.as_slice()[to];
        self.delivery_by_id.insert(d.id, to);
        if let Some(set) = self.deliveries_by_session.get_mut(&d.target) {
            set.replace(from, to);
        }
        if let Some(f) = d.in_flight() {
            self.delivery_by_op.insert(f.operation, to);
            self.delivery_by_deadline.insert(f.deadline, to);
        }
        if let Some((effect, _)) = d.frame {
            self.delivery_by_frame.insert(effect, to);
        }
        if let Some(claim) = d.claim {
            self.delivery_by_claim_op.insert(claim.operation, to);
            if !d.retired {
                self.delivery_by_target_pi
                    .insert((d.target, claim.packet_id), to);
            }
        }
        if let Some(res) = d.claim_result {
            self.delivery_by_claim_result_op.insert(res.operation, to);
        }
        if let Some(generation) = d.window
            && let Some(set) = self.deliveries_by_window.get_mut(&generation)
        {
            set.replace(from, to);
        }
        if Self::is_control_candidate(d) {
            self.control_candidates.remove(&from);
            self.control_candidates.insert(to);
        }
        if d.expiry.is_some() {
            self.expiring_deliveries.remove(&from);
            self.expiring_deliveries.insert(to);
        }
    }

    /// Updates a `Delivery` in place when `id`, `target`, `payload`, and `expiry`
    /// are unchanged, touching only the secondary indices whose keys changed.
    pub(super) fn mutate_delivery(&mut self, index: usize, f: impl FnOnce(&mut Delivery)) {
        let d = &self.rows.as_slice()[index];
        let old_flight = d.in_flight().map(|fl| (fl.operation, fl.deadline));
        let old_frame = d.frame.map(|(e, _)| e);
        let old_claim = d.claim.map(|c| c.operation);
        let old_target_pi = (!d.retired)
            .then(|| d.claim.map(|c| (d.target, c.packet_id)))
            .flatten();
        let old_claim_res = d.claim_result.map(|r| r.operation);
        let old_window = d.window;
        let old_sent = Self::sent_gen(d);
        let old_busy = Self::is_busy_delivery(d);
        let old_ctrl = Self::is_control_candidate(d);
        let old_pubrec = Self::is_pubrec_pending(d);

        f(&mut self.rows.as_mut_slice()[index]);

        let d = &self.rows.as_slice()[index];
        let target = d.target;
        let new_flight = d.in_flight().map(|fl| (fl.operation, fl.deadline));
        let new_frame = d.frame.map(|(e, _)| e);
        let new_claim = d.claim.map(|c| c.operation);
        let new_target_pi = (!d.retired)
            .then(|| d.claim.map(|c| (d.target, c.packet_id)))
            .flatten();
        let new_claim_res = d.claim_result.map(|r| r.operation);
        let new_window = d.window;
        let new_sent = Self::sent_gen(d);
        let new_busy = Self::is_busy_delivery(d);
        let new_ctrl = Self::is_control_candidate(d);
        let new_pubrec = Self::is_pubrec_pending(d);

        if old_flight != new_flight {
            if let Some((op, dl)) = old_flight {
                self.delivery_by_op.remove(&op);
                self.delivery_by_deadline.remove(&dl);
            }
            if let Some((op, dl)) = new_flight {
                self.delivery_by_op.insert(op, index);
                self.delivery_by_deadline.insert(dl, index);
            }
        }
        if old_frame != new_frame {
            if let Some(e) = old_frame {
                self.delivery_by_frame.remove(&e);
            }
            if let Some(e) = new_frame {
                self.delivery_by_frame.insert(e, index);
            }
        }
        if old_claim != new_claim {
            if let Some(op) = old_claim {
                self.delivery_by_claim_op.remove(&op);
            }
            if let Some(op) = new_claim {
                self.delivery_by_claim_op.insert(op, index);
            }
        }
        if old_target_pi != new_target_pi {
            if let Some(key) = old_target_pi {
                self.delivery_by_target_pi.remove(&key);
            }
            if let Some(key) = new_target_pi {
                self.delivery_by_target_pi.insert(key, index);
            }
        }
        if old_claim_res != new_claim_res {
            if let Some(op) = old_claim_res {
                self.delivery_by_claim_result_op.remove(&op);
            }
            if let Some(op) = new_claim_res {
                self.delivery_by_claim_result_op.insert(op, index);
            }
        }
        if old_window != new_window {
            if let Some(generation) = old_window {
                if let Some(count) = self.active_by_gen.get_mut(&generation) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        self.active_by_gen.remove(&generation);
                    }
                }
                if let Some(set) = self.deliveries_by_window.get_mut(&generation) {
                    set.remove(&index);
                    if set.is_empty() {
                        self.deliveries_by_window.remove(&generation);
                    }
                }
            }
            if let Some(generation) = new_window {
                *self.active_by_gen.get_or_insert_default(generation) += 1;
                self.deliveries_by_window
                    .get_or_insert_default(generation)
                    .insert(index);
            }
        }
        if old_sent != new_sent {
            if let Some(key) = old_sent
                && let Some(count) = self.sent_on_gen.get_mut(&key)
            {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    self.sent_on_gen.remove(&key);
                }
            }
            if let Some(key) = new_sent {
                *self.sent_on_gen.get_or_insert_default(key) += 1;
            }
        }
        if old_busy != new_busy {
            if old_busy && let Some(count) = self.busy_by_session.get_mut(&target) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    self.busy_by_session.remove(&target);
                }
            }
            if new_busy {
                *self.busy_by_session.get_or_insert_default(target) += 1;
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

    pub(super) fn push_delivery(&mut self, delivery: Delivery) -> Result<(), CoreFault> {
        let index = self.rows.len();
        self.rows
            .try_push(delivery)
            .map_err(|_| CoreFault::StalePlan)?;
        self.index_delivery(index);
        Ok(())
    }

    pub(super) fn remove_delivery_at(&mut self, index: usize) {
        let last = self.rows.len() - 1;
        self.unindex_delivery(index);
        if index != last {
            self.rows.as_mut_slice().swap(index, last);
        }
        let _ = self.rows.pop();
        if index != last {
            self.reindex_moved_delivery(last, index);
        }
    }

    pub(super) fn retired_flights(&self) -> usize {
        self.rows
            .as_slice()
            .iter()
            .filter(|d| {
                d.retired
                    && (d.in_flight().is_some() || matches!(d.phase, Phase::InlineClaimPending(_)))
            })
            .count()
    }
    pub(super) fn retire_session(&mut self, session: SessionId) {
        let indices: alloc::vec::Vec<usize> = self
            .deliveries_by_session
            .get(&session)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default();
        // Process in descending index order so swap-removals preserve lower indices.
        for index in indices.into_iter().rev() {
            let keep = {
                let d = &self.rows.as_slice()[index];
                d.in_flight().is_some()
                    || matches!(
                        d.phase,
                        Phase::Qos0Writing(_) | Phase::InlineClaimPending(_)
                    )
            };
            if keep {
                self.unindex_delivery(index);
                let d = &mut self.rows.as_mut_slice()[index];
                d.retired = true;
                d.window = None;
                d.topic = None;
                d.payload = None;
                d.properties = None;
                self.index_delivery(index);
            } else {
                self.remove_delivery_at(index);
            }
        }
        // Volatile attempts retain their existing callback/continuation:
        // that bounded path records their explicit closed-destination drop.
    }
    pub(super) fn delivery(&self, id: DeliveryId) -> Option<usize> {
        self.delivery_by_id.get(&id).copied()
    }
    pub(super) fn active(&self, generation: ConnectionGeneration) -> usize {
        self.active_by_gen.get(&generation).copied().unwrap_or(0)
    }

    /// Consumed QoS 0 deliveries of `session` whose frame is being written
    /// on `generation`.
    pub(super) fn qos0_writing(
        &self,
        session: SessionId,
        generation: ConnectionGeneration,
    ) -> usize {
        let Some(indices) = self.deliveries_by_session.get(&session) else {
            return 0;
        };
        let slice = self.rows.as_slice();
        indices
            .iter()
            .filter(|&&index| {
                let d = &slice[index];
                matches!(d.phase, Phase::Qos0Writing(_))
                    && d.frame.is_some_and(|(_, key)| key.generation == generation)
            })
            .count()
    }

    pub(super) fn next_send(
        &self,
        session: SessionId,
        generation: ConnectionGeneration,
        now: Option<MessageTime>,
        room: bool,
    ) -> Option<(usize, &Delivery)> {
        let indices = self.deliveries_by_session.get(&session)?;
        // O(1) fast path: when every delivery of this session is either already
        // transmitted on `generation` (AwaitPuback with frame on `generation`)
        // or in a non-sending terminal/busy phase, none can satisfy
        // `waiting_to_send(generation, now)`.
        let sent = self
            .sent_on_gen
            .get(&(session, generation))
            .copied()
            .unwrap_or(0);
        let busy = self.busy_by_session.get(&session).copied().unwrap_or(0);
        if sent + busy == indices.len() {
            return None;
        }
        let slice = self.rows.as_slice();
        let mut publish: Option<(usize, &Delivery)> = None;
        let mut pubrel: Option<(usize, &Delivery)> = None;
        // PUBREL leaves in the order of the first positive PUBREC
        // (MQTT-4.6.0-4 / MQTT-4.6.0-6). Any delivery of this session that
        // already received a positive PUBREC — whether waiting for
        // MarkDeliveryStarted before PreparePubrel or waiting for
        // PreparePubrel to commit — holds later PUBRELs behind its rank.
        let preparing = if self.pubrec_pending_count == 0 {
            None
        } else {
            indices
                .iter()
                .filter_map(|&idx| {
                    let d = &slice[idx];
                    if Self::is_pubrec_pending(d) {
                        d.pubrec_rank
                    } else {
                        None
                    }
                })
                .min()
        };
        let received = |delivery: &Delivery| delivery.pubrec_rank;
        for &index in indices {
            let delivery = &slice[index];
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

    /// Finds the next queued QoS 1/2 delivery eligible for pipelined claiming
    /// when every earlier waiting delivery on this destination is a healthy
    /// in-flight `ClaimPending(Submitted)` or an `AwaitPuback` waiting behind
    /// such a claim. Any `Indeterminate` (unknown) claim, `ConsumePending`,
    /// QoS 0 delivery, or unloaded delivery forms a strict barrier ahead of
    /// later deliveries.
    ///
    /// Also returns whether all waiting non-PUBREL deliveries are healthy
    /// in-flight claims (allowing `LoadWork` prefetch when `after`/`rescan`
    /// indicates more work in the store).
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
        let Some(indices) = self.deliveries_by_session.get(&session) else {
            return (None, true);
        };
        let slice = self.rows.as_slice();
        let mut blocker: Option<DeliveryId> = None;
        let mut best_queued: Option<(usize, &Delivery)> = None;
        let mut blocked_all = false;
        for &idx in indices {
            let d = &slice[idx];
            if !d.waiting_to_send(generation, now) || d.awaits_pubcomp() {
                continue;
            }
            match d.phase {
                Phase::ClaimPending(f) if f.phase == CommandPhase::Submitted => {}
                Phase::AwaitPuback => {}
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
    pub(super) fn loaded_bytes(&self) -> Option<u64> {
        Some(self.loaded_payload_bytes)
    }
    /// Whether `now` keeps every expiring delivery in a known state.
    pub(super) fn accepts_message_time(&self, now: MessageTime) -> bool {
        if !self.expiring_deliveries.is_empty() {
            let slice = self.rows.as_slice();
            if !self
                .expiring_deliveries
                .iter()
                .filter_map(|&idx| slice[idx].expiry)
                .all(|expiry| expiry.state(now).is_ok())
            {
                return false;
            }
        }
        true
    }
    pub(super) fn evict_expired_cache(&mut self, now: MessageTime) {
        if self.expiring_deliveries.is_empty() {
            return;
        }
        let expired: alloc::vec::Vec<usize> = self
            .expiring_deliveries
            .iter()
            .copied()
            .filter(|&idx| {
                let d = &self.rows.as_slice()[idx];
                d.claim.is_none()
                    && d.phase == Phase::Queued
                    && d.expiry
                        .is_some_and(|expiry| expiry.state(now) == Ok(ExpiryState::Expired))
            })
            .collect();
        for index in expired.into_iter().rev() {
            self.remove_delivery_at(index);
        }
    }
    pub(super) fn has_control_work(&self, now: Option<MessageTime>) -> bool {
        let slice = self.rows.as_slice();
        self.control_candidates
            .iter()
            .any(|&idx| slice[idx].control_ready(now))
    }
    pub(super) fn close_work(&self, connections: &Connections) -> u64 {
        if connections.is_empty() {
            0
        } else {
            connections
                .visit_work()
                .saturating_add(capacity_of(self.rows.capacity()).min(32))
        }
    }
    #[allow(dead_code)]
    pub(super) fn rebuild_indices(&mut self) {
        self.delivery_by_id.clear();
        self.delivery_by_op.clear();
        self.delivery_by_deadline.clear();
        self.delivery_by_frame.clear();
        self.delivery_by_claim_op.clear();
        self.delivery_by_claim_result_op.clear();
        self.delivery_by_target_pi.clear();
        self.deliveries_by_session.clear();
        self.deliveries_by_window.clear();
        self.sent_on_gen.clear();
        self.busy_by_session.clear();
        self.control_candidates.clear();
        self.expiring_deliveries.clear();
        self.pubrec_pending_count = 0;
        self.active_by_gen.clear();
        self.loaded_payload_bytes = 0;
        for index in 0..self.rows.len() {
            self.index_delivery(index);
        }
    }
    /// Releases the windows the deliveries held on a closed connection.
    pub(super) fn close_windows(&mut self, generation: ConnectionGeneration) {
        if let Some(indices) = self.deliveries_by_window.remove(&generation) {
            self.active_by_gen.remove(&generation);
            for idx in indices {
                self.unindex_delivery(idx);
                let delivery = &mut self.rows.as_mut_slice()[idx];
                if delivery.window == Some(generation) {
                    delivery.window = None;
                    if delivery.frame.is_some() {
                        delivery.certain_never_started = false;
                    }
                }
                self.index_delivery(idx);
            }
        }
    }
    pub(super) fn holds_deadline(&self, token: DeadlineToken) -> bool {
        self.delivery_by_deadline.contains_key(&token)
    }

    pub(super) fn known(&self, operation: OperationId) -> Option<Finished> {
        self.delivery_by_claim_result_op
            .get(&operation)
            .and_then(|&idx| self.rows.as_slice().get(idx))
            .and_then(|d| d.claim_result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::ids::{LogicalOwnerId, OperationSeq, PublicationId, PublicationSeq};
    use crate::contract::message_expiry::{MessageExpiry, MessageTime};

    impl Deliveries {
        /// Every index and count of the state that a rebuild derives from the
        /// rows, in an order that does not depend on how they were filled.
        fn index_snapshot(&self) -> alloc::vec::Vec<alloc::string::String> {
            use alloc::string::String;
            use alloc::vec::Vec;
            use core::fmt::Write;
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
            alloc::vec![
                all(self.delivery_by_id.iter()),
                all(self.delivery_by_op.iter()),
                all(self.delivery_by_deadline.iter()),
                all(self.delivery_by_frame.iter()),
                all(self.delivery_by_claim_op.iter()),
                all(self.delivery_by_claim_result_op.iter()),
                all(self.delivery_by_target_pi.iter()),
                all(self.deliveries_by_session.iter()),
                all(self.deliveries_by_window.iter()),
                all(self.sent_on_gen.iter()),
                all(self.busy_by_session.iter()),
                all(self.active_by_gen.iter()),
                one(&self.control_candidates),
                one(&self.expiring_deliveries),
                one(self.pubrec_pending_count),
                one(self.loaded_payload_bytes),
            ]
        }
        pub(in crate::owner) fn assert_indices_kept(&mut self) {
            let kept = self.index_snapshot();
            self.rebuild_indices();
            assert_eq!(
                kept,
                self.index_snapshot(),
                "an index drifted from its rows"
            );
        }
    }

    impl Deliveries {
        /// The rows, for a test to rewrite. A rewrite that changes a key
        /// leaves the indices stale: the test rebuilds them, or steps through
        /// `apply`, which does.
        pub(in crate::owner) fn rows_mut(&mut self) -> &mut [Delivery] {
            self.rows.as_mut_slice()
        }
    }

    const TARGET: SessionId = SessionId::new(1);
    const CONNECTION: ConnectionGeneration = ConnectionGeneration::new(10);
    fn nonzero(value: u32) -> NonZeroU32 {
        NonZeroU32::new(value).unwrap()
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
    fn message_expiry() -> MessageExpiry {
        MessageExpiry {
            observed: MessageTime {
                domain: [9; 32],
                tick: 10 * crate::contract::message_expiry::NANOS_PER_SECOND,
            },
            interval: 2,
        }
    }
    /// The table alone, with no owner around it: its rows are written by
    /// hand.
    fn bare_state() -> Deliveries {
        Deliveries::new(OwnerConfig {
            max_sessions: nonzero(2),
            max_routes: nonzero(2),
            max_exchanges: nonzero(8),
            session_qos2_exchanges: NonZeroU32::MAX,
            account: AccountId::new(1),
            payload_bytes: PayloadBytes::new(1024),
            resolution_delay: LogicalDuration::new(1),
            retained_operations: nonzero(16),
            remembered_connections: nonzero(4),
        })
        .unwrap()
    }
    #[test]
    fn a_swap_removal_reindexes_the_row_it_moves_under_every_index() {
        let key = ConnectionKey {
            session: TARGET,
            generation: CONNECTION,
        };
        let claim = |packet_id| DeliveryClaim {
            operation: operation(50 + u64::from(packet_id)),
            packet_id: OutboundPacketId::new(packet_id).unwrap(),
            started: true,
            await_pubcomp: false,
        };
        // The row removed: sent on the connection and a control candidate.
        let mut sent = Delivery::metadata(
            delivery(0),
            TARGET,
            Revision::new(1),
            Qos::AtLeastOnce,
            Some(claim(1)),
        );
        let frame = EffectId {
            epoch: RunEpoch::new(2),
            owner: LogicalOwnerId::new(4),
            sequence: EffectSeq::new(90),
        };
        sent.frame = Some((frame, key));
        sent.early_pubrec = Some(0x10);
        // The rows moved into its slot, one at a time: in flight and
        // expiring, then retrying its start.
        let mut in_flight = Delivery::metadata(
            delivery(1),
            TARGET,
            Revision::new(1),
            Qos::AtLeastOnce,
            Some(claim(2)),
        );
        in_flight.phase = Phase::StartPending(flight(2));
        in_flight.expiry = Some(message_expiry());
        let mut retrying = Delivery::metadata(
            delivery(2),
            TARGET,
            Revision::new(1),
            Qos::AtLeastOnce,
            Some(claim(3)),
        );
        retrying.phase = Phase::StartRetry;
        for moved in [in_flight, retrying] {
            let mut state = bare_state();
            state.push_delivery(sent.clone()).unwrap();
            state.push_delivery(moved.clone()).unwrap();
            assert_eq!(state.sent_on_gen.get(&(TARGET, CONNECTION)), Some(&1));
            assert!(state.control_candidates.contains(&0));

            state.remove_delivery_at(0);

            assert_eq!(state.delivery(moved.id), Some(0));
            assert_eq!(state.sent_on_gen.get(&(TARGET, CONNECTION)), None);
            assert_eq!(
                state.control_candidates.contains(&0),
                Deliveries::is_control_candidate(&moved)
            );
            assert_eq!(
                state.expiring_deliveries.contains(&0),
                moved.expiry.is_some()
            );
            if let Some(f) = moved.in_flight() {
                assert_eq!(state.delivery_by_op.get(&f.operation), Some(&0));
            }
            state.assert_indices_kept();
        }
    }
    #[test]
    fn a_mutation_that_changes_a_claim_moves_its_packet_identifier_key() {
        let mut state = bare_state();
        let claim = |packet_id| DeliveryClaim {
            operation: operation(60),
            packet_id: OutboundPacketId::new(packet_id).unwrap(),
            started: false,
            await_pubcomp: false,
        };
        state
            .push_delivery(Delivery::metadata(
                delivery(0),
                TARGET,
                Revision::new(1),
                Qos::AtLeastOnce,
                Some(claim(4)),
            ))
            .unwrap();

        state.mutate_delivery(0, |d| d.claim = Some(claim(5)));

        let pi = |id| (TARGET, OutboundPacketId::new(id).unwrap());
        assert_eq!(state.delivery_by_target_pi.get(&pi(4)), None);
        assert_eq!(state.delivery_by_target_pi.get(&pi(5)), Some(&0));
        state.assert_indices_kept();
    }
}
