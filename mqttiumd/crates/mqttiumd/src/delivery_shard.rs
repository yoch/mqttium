//! Resident multi-core `DeliveryShard` worker threads and lock-free
//! `InlineClaimLease` oracle (Lot 10).
//!
//! Each shard owns an independent deterministic `Owner` (`LogicalOwnerId(1..=K)`),
//! a dedicated SPSC `ShardStoreHandle` to the single exclusive `Writer`, a
//! direct `ShardInbox` fed by I/O reactor threads (`PUBACK`/`PUBREC`/`PUBCOMP`
//! and shard-owned `Written` receipts), and cloneable `OutgoingHandle`s to
//! enqueue outbound `PUBLISH`/`PUBREL` frames straight onto subscriber sockets.

mod notices;

use notices::Notices;
use std::collections::{BTreeMap, VecDeque};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, TryRecvError, TrySendError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use mqttiumd_contract::capacity::{BoundedVec, PayloadBytes, WorkUnits};
use mqttiumd_contract::command::DeliveryCause;
use mqttiumd_contract::effects::{
    CloseReason, Effect, EffectSink, FrameProgress, Input, Outbox, OwnedEffect, SubmitBlock,
    SubmitResult,
};
use mqttiumd_contract::flat_map::{FlatMap, FlatSet, ScopedFlatMap};
use mqttiumd_contract::ids::{
    AccountId, ConnectionGeneration, ConnectionKey, DeadlineToken, EffectId, LogicalDuration,
    LogicalOwnerId, LogicalTime, OperationId, OutboundPacketId, Revision, SessionId,
};
use mqttiumd_contract::message_expiry::{ExpiredDelivery, MessageExpiry, MessageTime};
use mqttiumd_contract::outcome::{CommitFacts, StoreOutcome, StoreReply};
use mqttiumd_contract::packet::{InboundPacket, OutboundFrame, SharedBytes};
use mqttiumd_contract::properties::PublishProperties;
use mqttiumd_contract::pubsub::{PubackTarget, Qos0DropReason, Qos0Origin};
use mqttiumd_contract::step::{StepOutcome, TurnGrant};
use mqttiumd_core::owner::{
    DelegatedClaim, DelegatedInlineBatch, DelegatedQos0Batch, DelegatedQos0Target,
    InlineClaimOracle, Owner, OwnerConfig, ShardAckProgress, ShardLimits, ShardStepOutcome,
};
use mqttiumd_store_api::ReadPhase;
use serde_json::json;

use crate::codec::{self, Bounds, Packet, Version};
use crate::events::Events;
use crate::net::{
    self, OutgoingHandle, ShardInbox, ShardIoEvent, ShardIoRoute, Write, WriteExpiry, WriteReport,
    Written,
};
use crate::runtime::Limits;
use crate::session_time::SessionClock;
use crate::wake::{Signalled, Wake};
use crate::writer::{
    ControlRequest, ControlResponse, Failure, Request, Response, ShardStoreHandle,
};

/// Maximum configurable resident delivery shards (`--delivery-shards 0..=16`).
pub const MAX_DELIVERY_SHARDS: usize = 16;

const COORD_CHANNEL_CAP: usize = 4_096;
/// Deliveries of delegated QoS 0 a shard keeps while its core cannot take
/// them: what one full lane of the coordinator brings.
const PENDING_QOS0_LIMIT: usize = COORD_CHANNEL_CAP;
const IDLE_TICK: Duration = Duration::from_millis(20);
const BACKPRESSURE_TICK: Duration = Duration::from_millis(1);
const BOUNDS: Bounds = Bounds {
    max_packet: net::MAX_PACKET_BYTES,
    max_client_id: crate::writer::MAX_CLIENT,
    max_topic: crate::writer::MAX_TOPIC,
    max_payload: crate::writer::MAX_PAYLOAD,
};

/// Cache-line-isolated lock-free lease slot shared between Coordinator (owner
/// of `dirty_seq`, sole allocator of `used_pi` bits when eligible) and one
/// `DeliveryShard` (owner of `acked_dirty_seq` and bit release on finish).
#[repr(align(64))]
pub(crate) struct InlineClaimLease {
    dirty_seq: AtomicU64,
    acked_dirty_seq: AtomicU64,
    eligible_gen: AtomicU64,
    in_flight: AtomicU32,
    preclaim_cap: AtomicU32,
    used_pi: [AtomicU64; 8],
}

impl InlineClaimLease {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            dirty_seq: AtomicU64::new(0),
            acked_dirty_seq: AtomicU64::new(0),
            eligible_gen: AtomicU64::new(0),
            in_flight: AtomicU32::new(0),
            preclaim_cap: AtomicU32::new(0),
            used_pi: std::array::from_fn(|_| AtomicU64::new(0)),
        })
    }

    #[inline]
    fn mark_dirty(&self) -> u64 {
        self.eligible_gen.store(0, Ordering::Release);
        self.dirty_seq.fetch_add(1, Ordering::Release) + 1
    }

    #[inline]
    fn try_reserve(&self) -> Option<(ConnectionGeneration, OutboundPacketId)> {
        let dirty = self.dirty_seq.load(Ordering::Relaxed);
        if dirty == 0 || self.acked_dirty_seq.load(Ordering::Acquire) != dirty {
            return None;
        }
        let generation = self.eligible_gen.load(Ordering::Acquire);
        if generation == 0 {
            return None;
        }
        let cap = self.preclaim_cap.load(Ordering::Relaxed);
        if self.in_flight.load(Ordering::Acquire) >= cap {
            return None;
        }
        for (word_idx, word) in self.used_pi.iter().enumerate() {
            let val = word.load(Ordering::Acquire);
            if val != u64::MAX {
                let bit = val.trailing_ones() as usize;
                let mask = 1u64 << bit;
                let prev = word.fetch_or(mask, Ordering::AcqRel);
                if prev & mask == 0 {
                    self.in_flight.fetch_add(1, Ordering::AcqRel);
                    let candidate = (word_idx * 64 + bit + 1) as u16;
                    return OutboundPacketId::new(candidate)
                        .map(|pi| (ConnectionGeneration::new(generation), pi));
                }
            }
        }
        None
    }

    #[inline]
    fn release_pi(&self, packet_id: OutboundPacketId) {
        let id = packet_id.get();
        if id <= 512 {
            let zb = usize::from(id - 1);
            let mask = !(1u64 << (zb % 64));
            let prev = self.used_pi[zb / 64].fetch_and(mask, Ordering::Release);
            if prev & (1u64 << (zb % 64)) != 0 {
                self.release_in_flight();
            }
        } else {
            // Above the bitmap nothing records whether this identifier was
            // counted: the count is lowered, never below zero.
            self.release_in_flight();
        }
    }

    #[inline]
    fn release_in_flight(&self) {
        let _ = self
            .in_flight
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1));
    }

    /// The identifiers the lease holds now. Read after a dirty mark, it holds
    /// every identifier the coordinator reserved and has not handed over: the
    /// mark ends its reservations.
    fn reserved_snapshot(&self) -> [u64; 8] {
        std::array::from_fn(|word| self.used_pi[word].load(Ordering::Acquire))
    }

    /// Invoked exclusively by the owning `DeliveryShard` when Coordinator has
    /// zero pending inline claims (`dirty_seq` barrier satisfied) and the
    /// session is caught up with zero queued/claiming deliveries.
    fn publish_eligible(
        &self,
        generation: ConnectionGeneration,
        preclaim_cap: usize,
        in_flight: usize,
        used_pi: [u64; 8],
        acked_seq: u64,
    ) {
        for (slot, bits) in self.used_pi.iter().zip(used_pi) {
            slot.store(bits, Ordering::Relaxed);
        }
        self.in_flight.store(
            u32::try_from(in_flight).unwrap_or(u32::MAX),
            Ordering::Relaxed,
        );
        self.preclaim_cap.store(
            u32::try_from(preclaim_cap).unwrap_or(u32::MAX),
            Ordering::Relaxed,
        );
        self.eligible_gen.store(generation.get(), Ordering::Release);
        self.acked_dirty_seq.store(acked_seq, Ordering::Release);
    }
}

/// Control & delivery commands sent from the Coordinator thread to one
/// resident `DeliveryShard` worker in strict FIFO order.
pub(crate) enum CoordToShard {
    Attach {
        session: SessionId,
        revision: Revision,
        generation: ConnectionGeneration,
        receive_maximum: u16,
        maximum_packet_size: u32,
        version: Version,
        outgoing: OutgoingHandle,
        lease: Arc<InlineClaimLease>,
        dirty_seq: u64,
    },
    Detach {
        connection: ConnectionKey,
        dirty_seq: u64,
        completed: Arc<AtomicBool>,
    },
    Retire {
        session: SessionId,
    },
    NotifyDirty {
        session: SessionId,
        dirty_seq: u64,
        clear_known_through: bool,
    },
    DirtyBarrier {
        session: SessionId,
        dirty_seq: u64,
    },
    InlineCommitted {
        topic: SharedBytes,
        payload: SharedBytes,
        properties: Option<Arc<PublishProperties>>,
        claims: Vec<DelegatedClaim>,
    },
    Qos0Deliver {
        origin: Qos0Origin,
        topic: SharedBytes,
        payload: SharedBytes,
        expiry: Option<MessageExpiry>,
        properties: Option<Arc<PublishProperties>>,
        targets: Vec<DelegatedQos0Target>,
    },
    /// What a test asks of the shard where it reads this message.
    #[cfg(test)]
    Test(ShardTest),
    MessagesExpired {
        deliveries: Vec<ExpiredDelivery>,
    },
}

/// What a test asks of a shard, at the place of its message in the lane.
#[cfg(test)]
pub(crate) enum ShardTest {
    /// Panic.
    Panic,
    /// Wait until the sender is dropped or sends.
    Pause(mpsc::Receiver<()>),
    /// Nothing: a message that only takes a place in the lane.
    Nothing,
}

#[cfg(test)]
impl ShardTest {
    fn run(self) {
        match self {
            Self::Panic => panic!("a test asked this delivery shard to panic"),
            Self::Pause(release) => {
                let _ = release.recv();
            }
            Self::Nothing => {}
        }
    }
}

/// Rare notices sent from a `DeliveryShard` worker back to the Coordinator.
pub(crate) enum ShardToCoord {
    CloseConnection {
        generation: ConnectionGeneration,
        code: &'static str,
        disconnect_reason: Option<u8>,
    },
    Fatal(Failure),
}

struct ShardClient {
    owner_id: LogicalOwnerId,
    tx: Signalled<CoordToShard>,
    inbox: Arc<ShardInbox>,
    /// Asked for by `ShardPool::shutdown`. A flag rather than a message of
    /// the lane, so that a full lane cannot lose it; the shard reads it
    /// when its lane is empty, after every message sent before it.
    stop: Arc<AtomicBool>,
    wake: crate::wake::Wake,
    handle: Option<JoinHandle<Result<(), Failure>>>,
}

/// Coordinator-side handle managing `K` resident `DeliveryShard` workers and
/// implementing `InlineClaimOracle` for `Owner::step_and_dispatch_with_oracle`.
pub(crate) struct ShardPool {
    shards: Vec<ShardClient>,
    notices: Arc<Notices>,
    leases: FlatMap<SessionId, Arc<InlineClaimLease>>,
    pending_dirty_barriers: FlatMap<SessionId, u64>,
    /// Committed inline claims not yet handed to their shard: an inline
    /// admission of their session has an unknown outcome. In the order of
    /// their admissions; at most the lease's room per session.
    held_claims: FlatMap<SessionId, Vec<HeldClaim>>,
    last_fatal: Option<Failure>,
}

/// A committed inline claim the coordinator holds back.
struct HeldClaim {
    claim: DelegatedClaim,
    topic: SharedBytes,
    payload: SharedBytes,
    properties: Option<Arc<PublishProperties>>,
}

impl ShardPool {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn spawn(
        outboxes: Vec<Outbox>,
        store_handles: Vec<ShardStoreHandle>,
        shard_limits: ShardLimits,
        limits: Limits,
        events: Events,
        clock: Arc<dyn SessionClock>,
        coord_wake: Wake,
        cancel: Arc<AtomicBool>,
    ) -> Result<Self, Failure> {
        if outboxes.len() != store_handles.len() || outboxes.is_empty() {
            return Err(Failure::plain("shard_pool_mismatch"));
        }
        let notices = Arc::new(Notices::new(coord_wake));
        let mut shards = Vec::with_capacity(outboxes.len());
        let mut ready_rxs = Vec::with_capacity(outboxes.len());

        let max_exchanges = crate::writer::SESSIONS * crate::receiving::MAXIMUM;
        let shard_owner_config = OwnerConfig {
            max_sessions: NonZeroU32::new(crate::writer::SESSIONS as u32).expect("nonzero"),
            max_routes: NonZeroU32::new(1).expect("nonzero"),
            max_exchanges: NonZeroU32::new(max_exchanges as u32).expect("nonzero"),
            session_qos2_exchanges: NonZeroU32::new(1).expect("nonzero"),
            account: AccountId::new(1),
            payload_bytes: PayloadBytes::new(64 * 1024 * 1024),
            resolution_delay: LogicalDuration::new(10_000),
            retained_operations: NonZeroU32::new(128).expect("nonzero"),
            remembered_connections: NonZeroU32::new(8).expect("nonzero"),
        };

        for (idx, (outbox, store_handle)) in outboxes.into_iter().zip(store_handles).enumerate() {
            let owner_id = outbox.id().owner();
            let shard_wake = store_handle.wake.clone();
            let (coord_tx, coord_rx) = crate::wake::signalled(COORD_CHANNEL_CAP, &shard_wake);
            let inbox = ShardInbox::new(shard_wake.clone(), crate::writer::QUEUE, limits.frames);
            let inbox_worker = Arc::clone(&inbox);
            let (ready_tx, ready_rx) = mpsc::sync_channel(1);
            let worker_notices = Arc::clone(&notices);
            let panic_notices = Arc::clone(&notices);
            let worker_events = events.clone();
            let worker_clock = Arc::clone(&clock);
            let worker_cancel = Arc::clone(&cancel);
            let stop = Arc::new(AtomicBool::new(false));
            let worker_stop = Arc::clone(&stop);

            let owner = Owner::new_delivery_shard(shard_owner_config, outbox, shard_limits)
                .map_err(|_| Failure::plain("shard_owner_creation"))?;

            let handle = thread::Builder::new()
                .name(format!("mqtt-deliv-{idx}"))
                .spawn(move || -> Result<(), Failure> {
                    let mut worker = DeliveryShardWorker::new(
                        owner,
                        store_handle,
                        coord_rx,
                        inbox_worker,
                        worker_notices,
                        worker_events,
                        worker_clock,
                        limits,
                        worker_cancel,
                        worker_stop,
                    );
                    match worker.register_receipts(Duration::from_secs(30)) {
                        Ok(()) => {
                            let _ = ready_tx.send(Ok(()));
                        }
                        Err(err) => {
                            let _ = ready_tx.send(Err(err.clone()));
                            return Err(err);
                        }
                    }
                    // A panic leaves the worker's state unknown: nothing of it
                    // is used again, and the coordinator stops the broker as
                    // for any fault of a shard.
                    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        worker.run_loop()
                    }))
                    .unwrap_or_else(|_| Err(Failure::plain("delivery_shard_panicked")));
                    if let Err(err) = &res {
                        panic_notices.publish(ShardToCoord::Fatal(err.clone()));
                    }
                    res
                })
                .map_err(|_| Failure::plain("shard_spawn"))?;

            ready_rxs.push(ready_rx);
            shards.push(ShardClient {
                owner_id,
                tx: coord_tx,
                inbox,
                stop,
                wake: shard_wake,
                handle: Some(handle),
            });
        }

        // Wait for every shard to register its ReceiptOrigin before listening.
        for rx in ready_rxs {
            rx.recv_timeout(Duration::from_secs(30))
                .map_err(|_| Failure::plain("shard_startup_deadline"))??;
        }

        Ok(Self {
            shards,
            notices,
            leases: FlatMap::new(),
            pending_dirty_barriers: FlatMap::new(),
            held_claims: FlatMap::new(),
            last_fatal: None,
        })
    }

    #[inline]
    pub(crate) fn shard_index(&self, session: SessionId) -> usize {
        (session.get() as usize) % self.shards.len()
    }

    pub(crate) fn route_for(&self, connection: ConnectionKey) -> ShardIoRoute {
        let idx = self.shard_index(connection.session);
        let shard = &self.shards[idx];
        ShardIoRoute::new(shard.owner_id, connection, Arc::clone(&shard.inbox))
    }

    fn lease_of(&mut self, session: SessionId) -> Arc<InlineClaimLease> {
        if let Some(existing) = self.leases.get(&session) {
            return Arc::clone(existing);
        }
        let lease = InlineClaimLease::new();
        self.leases.insert(session, Arc::clone(&lease));
        lease
    }

    fn send_to(&mut self, shard_idx: usize, msg: CoordToShard) {
        if self.shards[shard_idx].tx.send(msg).is_err() && self.last_fatal.is_none() {
            self.last_fatal = Some(Failure::plain("delivery_shard_disconnected"));
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn attach_session(
        &mut self,
        session: SessionId,
        revision: Revision,
        generation: ConnectionGeneration,
        receive_maximum: u16,
        maximum_packet_size: u32,
        version: Version,
        outgoing: OutgoingHandle,
        pending_inline_count: usize,
    ) {
        self.notices.attach(generation);
        let lease = self.lease_of(session);
        let seq = lease.mark_dirty();
        let dirty_seq = if pending_inline_count == 0 {
            self.pending_dirty_barriers.remove(&session);
            seq
        } else {
            self.pending_dirty_barriers.insert(session, seq);
            0
        };
        let idx = self.shard_index(session);
        self.send_to(
            idx,
            CoordToShard::Attach {
                session,
                revision,
                generation,
                receive_maximum,
                maximum_packet_size,
                version,
                outgoing,
                lease,
                dirty_seq,
            },
        );
    }

    pub(crate) fn detach_session(
        &mut self,
        session: SessionId,
        generation: ConnectionGeneration,
        pending_inline_count: usize,
    ) -> Arc<AtomicBool> {
        // The transport has finished, so its close request has been satisfied.
        self.notices.detach(generation);
        let completed = Arc::<AtomicBool>::default();
        let lease = self.lease_of(session);
        let seq = lease.mark_dirty();
        let dirty_seq = if pending_inline_count == 0 {
            self.pending_dirty_barriers.remove(&session);
            seq
        } else {
            self.pending_dirty_barriers.insert(session, seq);
            0
        };
        let idx = self.shard_index(session);
        self.send_to(
            idx,
            CoordToShard::Detach {
                connection: ConnectionKey {
                    session,
                    generation,
                },
                dirty_seq,
                completed: Arc::clone(&completed),
            },
        );
        completed
    }

    pub(crate) fn retire_session(&mut self, session: SessionId) {
        if let Some(lease) = self.leases.remove(&session) {
            let _ = lease.mark_dirty();
        }
        self.pending_dirty_barriers.remove(&session);
        self.held_claims.remove(&session);
        let idx = self.shard_index(session);
        self.send_to(idx, CoordToShard::Retire { session });
    }

    pub(crate) fn notify_dirty(
        &mut self,
        session: SessionId,
        clear_known_through: bool,
        pending_inline_count: usize,
    ) {
        let lease = self.lease_of(session);
        let seq = lease.mark_dirty();
        let dirty_seq = if pending_inline_count == 0 {
            self.pending_dirty_barriers.remove(&session);
            seq
        } else {
            self.pending_dirty_barriers.insert(session, seq);
            0
        };
        let idx = self.shard_index(session);
        self.send_to(
            idx,
            CoordToShard::NotifyDirty {
                session,
                dirty_seq,
                clear_known_through,
            },
        );
    }

    pub(crate) fn flush_dirty_barrier_if_pending(&mut self, session: SessionId) {
        if let Some(dirty_seq) = self.pending_dirty_barriers.remove(&session) {
            let idx = self.shard_index(session);
            self.send_to(idx, CoordToShard::DirtyBarrier { session, dirty_seq });
        }
    }

    pub(crate) fn flush_idle_dirty_barriers(
        &mut self,
        mut pending_count: impl FnMut(SessionId) -> usize,
    ) {
        if self.pending_dirty_barriers.is_empty() {
            return;
        }
        let ready: Vec<(SessionId, u64)> = self
            .pending_dirty_barriers
            .iter()
            .filter_map(|(&session, &seq)| (pending_count(session) == 0).then_some((session, seq)))
            .collect();
        for (session, dirty_seq) in ready {
            self.pending_dirty_barriers.remove(&session);
            let idx = self.shard_index(session);
            self.send_to(idx, CoordToShard::DirtyBarrier { session, dirty_seq });
        }
    }

    /// Hands the committed inline claims of `batch` to their shards. A
    /// claim whose session has an inline admission of unknown outcome
    /// (`unknown`), or claims already held, is held in admission order
    /// until `release_held_claims`: the earlier publication may still
    /// commit, and the subscriber receives it first.
    pub(crate) fn send_inline_committed(
        &mut self,
        mut batch: DelegatedInlineBatch,
        unknown: impl Fn(SessionId) -> bool,
    ) {
        if !self.held_claims.is_empty() || batch.claims.iter().any(|claim| unknown(claim.target)) {
            let held = &mut self.held_claims;
            batch.claims.retain(|claim| {
                if !held.contains_key(&claim.target) && !unknown(claim.target) {
                    return true;
                }
                let claims = held.get_or_insert_default(claim.target);
                let order = |claim: &DelegatedClaim| claim.claim_operation.sequence().get();
                let at = claims.partition_point(|item| order(&item.claim) < order(claim));
                claims.insert(
                    at,
                    HeldClaim {
                        claim: *claim,
                        topic: batch.topic.clone(),
                        payload: batch.payload.clone(),
                        properties: batch.properties.clone(),
                    },
                );
                false
            });
            if batch.claims.is_empty() {
                return;
            }
        }
        self.send_claims(batch);
    }

    /// Hands the held claims of `session` to its shard, in admission order,
    /// once no inline admission of the session has an unknown outcome. The
    /// runtime calls it before it flushes the session's barrier: the shard
    /// reads the session only after these claims.
    pub(crate) fn release_held_claims(&mut self, session: SessionId, unknown: bool) {
        if unknown || self.held_claims.is_empty() {
            return;
        }
        let Some(claims) = self.held_claims.remove(&session) else {
            return;
        };
        let idx = self.shard_index(session);
        for held in claims {
            self.send_to(
                idx,
                CoordToShard::InlineCommitted {
                    topic: held.topic,
                    payload: held.payload,
                    properties: held.properties,
                    claims: vec![held.claim],
                },
            );
        }
    }

    fn send_claims(&mut self, batch: DelegatedInlineBatch) {
        let k = self.shards.len();
        if k == 1 || batch.claims.len() <= 1 {
            if let Some(first) = batch.claims.first() {
                let idx = self.shard_index(first.target);
                self.send_to(
                    idx,
                    CoordToShard::InlineCommitted {
                        topic: batch.topic,
                        payload: batch.payload,
                        properties: batch.properties,
                        claims: batch.claims,
                    },
                );
            }
            return;
        }
        let mut per_shard: Vec<Vec<DelegatedClaim>> = (0..k).map(|_| Vec::new()).collect();
        for claim in batch.claims {
            let idx = (claim.target.get() as usize) % k;
            per_shard[idx].push(claim);
        }
        for (idx, claims) in per_shard.into_iter().enumerate() {
            if !claims.is_empty() {
                self.send_to(
                    idx,
                    CoordToShard::InlineCommitted {
                        topic: batch.topic.clone(),
                        payload: batch.payload.clone(),
                        properties: batch.properties.clone(),
                        claims,
                    },
                );
            }
        }
    }

    pub(crate) fn send_qos0(&mut self, batch: DelegatedQos0Batch) {
        let k = self.shards.len();
        if k == 1 || batch.targets.len() <= 1 {
            if let Some(first) = batch.targets.first() {
                let idx = self.shard_index(first.target);
                self.send_to(
                    idx,
                    CoordToShard::Qos0Deliver {
                        origin: batch.origin,
                        topic: batch.topic,
                        payload: batch.payload,
                        expiry: batch.expiry,
                        properties: batch.properties,
                        targets: batch.targets,
                    },
                );
            }
            return;
        }
        let mut per_shard: Vec<Vec<DelegatedQos0Target>> = (0..k).map(|_| Vec::new()).collect();
        for target in batch.targets {
            let idx = (target.target.get() as usize) % k;
            per_shard[idx].push(target);
        }
        for (idx, targets) in per_shard.into_iter().enumerate() {
            if !targets.is_empty() {
                self.send_to(
                    idx,
                    CoordToShard::Qos0Deliver {
                        origin: batch.origin,
                        topic: batch.topic.clone(),
                        payload: batch.payload.clone(),
                        expiry: batch.expiry,
                        properties: batch.properties.clone(),
                        targets,
                    },
                );
            }
        }
    }

    pub(crate) fn send_messages_expired(&mut self, deliveries: &[ExpiredDelivery]) {
        if deliveries.is_empty() {
            return;
        }
        let k = self.shards.len();
        let mut per_shard: Vec<Vec<ExpiredDelivery>> = (0..k).map(|_| Vec::new()).collect();
        for &item in deliveries {
            let idx = (item.target.get() as usize) % k;
            per_shard[idx].push(item);
        }
        for (idx, items) in per_shard.into_iter().enumerate() {
            if !items.is_empty() {
                self.send_to(idx, CoordToShard::MessagesExpired { deliveries: items });
            }
        }
    }

    /// Whether the coordinator may reserve for `session` now: the predicate
    /// of `InlineClaimLease::try_reserve`, without its room check.
    #[cfg(test)]
    pub(crate) fn lease_is_eligible(&self, session: SessionId) -> bool {
        self.leases.get(&session).is_some_and(|lease| {
            let dirty = lease.dirty_seq.load(Ordering::Acquire);
            dirty != 0
                && lease.acked_dirty_seq.load(Ordering::Acquire) == dirty
                && lease.eligible_gen.load(Ordering::Acquire) != 0
        })
    }

    #[cfg(test)]
    pub(crate) fn send_for_test(&mut self, shard_idx: usize, test: ShardTest) {
        self.send_to(shard_idx, CoordToShard::Test(test));
    }

    /// Whether the lane of the shard took the message, without waiting.
    #[cfg(test)]
    pub(crate) fn try_send_for_test(&mut self, shard_idx: usize, test: ShardTest) -> bool {
        self.shards[shard_idx]
            .tx
            .try_send(CoordToShard::Test(test))
            .is_ok()
    }

    pub(crate) fn drain_notices(&mut self, out: &mut Vec<ShardToCoord>) -> Result<(), Failure> {
        if let Some(err) = self.notices.drain_into(out).or(self.last_fatal.take()) {
            return Err(err);
        }
        Ok(())
    }

    /// Clean ordered shutdown: stops every delivery shard and joins its thread
    /// so all in-flight shard store commands drain before the `Writer` stops.
    pub(crate) fn shutdown(mut self, budget: Duration) -> Result<(), Failure> {
        for shard in &self.shards {
            shard.stop.store(true, Ordering::Release);
            shard.wake.wake();
        }
        let deadline = Instant::now() + budget;
        let mut first_err = None;
        for shard in &mut self.shards {
            if let Some(handle) = shard.handle.take() {
                while !handle.is_finished() && Instant::now() < deadline {
                    thread::sleep(Duration::from_millis(2));
                }
                if handle.is_finished() {
                    match handle.join() {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => {
                            first_err.get_or_insert(err);
                        }
                        Err(_) => {
                            first_err.get_or_insert_with(|| Failure::plain("shard_panicked"));
                        }
                    }
                } else {
                    first_err.get_or_insert_with(|| Failure::plain("shard_shutdown_deadline"));
                }
            }
        }
        // A joined worker returns the original fault, even if sending to its
        // closed input lane also failed during shutdown.
        match first_err.or(self.last_fatal) {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

impl InlineClaimOracle for ShardPool {
    fn try_reserve(
        &mut self,
        session: SessionId,
        qos: mqttiumd_contract::packet::Qos,
        _payload_bytes: u64,
    ) -> Option<(ConnectionGeneration, OutboundPacketId)> {
        if qos == mqttiumd_contract::packet::Qos::AtMostOnce {
            return None;
        }
        self.leases.get(&session)?.try_reserve()
    }

    fn mark_ineligible(&mut self, session: SessionId) {
        if let Some(lease) = self.leases.get(&session) {
            let seq = lease.mark_dirty();
            self.pending_dirty_barriers.insert(session, seq);
        }
    }

    fn rollback(&mut self, session: SessionId, packet_id: OutboundPacketId) {
        if let Some(lease) = self.leases.get(&session) {
            lease.release_pi(packet_id);
        }
    }
}

struct ShardSession {
    generation: ConnectionGeneration,
    version: Version,
    max_packet: u32,
    receive_maximum: u16,
    outgoing: OutgoingHandle,
    lease: Arc<InlineClaimLease>,
    seen_dirty_seq: u64,
    scan_started_dirty_seq: u64,
    published_dirty_seq: u64,
    lease_published: bool,
    in_flight_load: Option<(EffectId, u64)>,
}

struct ShardPendingFrame {
    connection: ConnectionKey,
    kind: &'static str,
}

struct PendingInlineItem {
    claim: DelegatedClaim,
    topic: SharedBytes,
    payload: SharedBytes,
    properties: Option<Arc<PublishProperties>>,
}

struct PendingQos0Item {
    origin: Qos0Origin,
    target: SessionId,
    connection: Option<ConnectionKey>,
    topic: SharedBytes,
    payload: SharedBytes,
    expiry: Option<MessageExpiry>,
    properties: Option<Arc<PublishProperties>>,
}

struct DeliveryShardWorker {
    owner: Owner,
    store: ShardStoreHandle,
    coord_rx: Receiver<CoordToShard>,
    io_inbox: Arc<ShardInbox>,
    notices: Arc<Notices>,
    events: Events,
    clock: Arc<dyn SessionClock>,
    limits: Limits,
    cancel: Arc<AtomicBool>,
    started: Instant,
    progress: u64,
    stopping: bool,
    stop: Arc<AtomicBool>,
    #[cfg(test)]
    wakes_held: bool,

    sessions: FlatMap<SessionId, ShardSession>,
    // The coordinator retains the closing peer until its own receipt is set.
    // This bounds the number of fences by the process connection limit.
    pending_detaches: BTreeMap<ConnectionKey, (u64, Arc<AtomicBool>)>,
    outgoing_bindings: FlatMap<(ConnectionKey, OutboundPacketId), PubackTarget>,
    retained_output: crate::retained_output::RetainedOutput,
    frames: ScopedFlatMap<EffectId, ShardPendingFrame>,
    inflight: ScopedFlatMap<EffectId, OperationId>,
    work_reads: ScopedFlatMap<EffectId, SessionId>,
    timers: BTreeMap<DeadlineToken, Instant>,
    next_timer_due: Option<Instant>,

    inputs: VecDeque<Input>,
    store_inputs: VecDeque<Input>,
    ack_inputs: VecDeque<(Input, net::ShardAckCredit)>,
    control_inputs: VecDeque<Input>,
    terminal_inputs: VecDeque<Input>,
    queued_work_sessions: FlatSet<SessionId>,
    /// Sessions not read from the store while a committed inline claim of
    /// theirs waits in `pending_inline` (`fence_after_message`).
    inline_fenced: FlatSet<SessionId>,
    pending_inline: VecDeque<PendingInlineItem>,
    pending_qos0: VecDeque<PendingQos0Item>,
    io_scratch: VecDeque<ShardIoEvent>,

    message_last_time: Option<MessageTime>,
    last_message_clock_sync: Instant,
    message_clock_needed: bool,
    maintenance_needed: bool,
    receipt_pressure_blocked: bool,
    last_maintenance_flush: Instant,
    timers_dirty: bool,
}

impl DeliveryShardWorker {
    #[allow(clippy::too_many_arguments)]
    fn new(
        owner: Owner,
        store: ShardStoreHandle,
        coord_rx: Receiver<CoordToShard>,
        io_inbox: Arc<ShardInbox>,
        notices: Arc<Notices>,
        events: Events,
        clock: Arc<dyn SessionClock>,
        limits: Limits,
        cancel: Arc<AtomicBool>,
        stop: Arc<AtomicBool>,
    ) -> Self {
        let now = Instant::now();
        Self {
            stop,
            #[cfg(test)]
            wakes_held: false,
            owner,
            store,
            coord_rx,
            io_inbox,
            notices,
            events,
            clock,
            limits,
            cancel,
            started: now,
            progress: 0,
            stopping: false,
            sessions: FlatMap::new(),
            pending_detaches: BTreeMap::new(),
            outgoing_bindings: FlatMap::new(),
            retained_output: Default::default(),
            frames: ScopedFlatMap::new(),
            inflight: ScopedFlatMap::new(),
            work_reads: ScopedFlatMap::new(),
            timers: BTreeMap::new(),
            next_timer_due: None,
            inputs: VecDeque::new(),
            store_inputs: VecDeque::new(),
            ack_inputs: VecDeque::new(),
            control_inputs: VecDeque::new(),
            terminal_inputs: VecDeque::new(),
            queued_work_sessions: FlatSet::new(),
            inline_fenced: FlatSet::new(),
            pending_inline: VecDeque::new(),
            pending_qos0: VecDeque::new(),
            io_scratch: VecDeque::new(),
            message_last_time: None,
            last_message_clock_sync: now,
            message_clock_needed: true,
            maintenance_needed: false,
            receipt_pressure_blocked: false,
            last_maintenance_flush: now,
            timers_dirty: false,
        }
    }

    fn notify_coord(&self, notice: ShardToCoord) {
        self.notices.publish(notice);
    }

    fn register_receipts(&mut self, budget: Duration) -> Result<(), Failure> {
        let deadline = Instant::now() + budget;
        self.control_inputs.push_back(Input::MaintainReceipts);
        while !self.owner.collection_ready() {
            if self.cancel.load(Ordering::Relaxed) {
                return Err(Failure::plain("startup_cancelled"));
            }
            if Instant::now() >= deadline {
                return Err(Failure::plain("shard_receipt_registration_deadline"));
            }
            self.drain_store_replies()?;
            self.due_deadlines(Instant::now())?;
            self.drain_inputs()?;
            self.dispatch()?;
            if !self.owner.collection_ready() {
                self.store.wake.wait(Duration::from_millis(2));
            }
        }
        Ok(())
    }

    fn run_loop(&mut self) -> Result<(), Failure> {
        loop {
            let before = self.progress;
            let now = Instant::now();
            {
                // The wake-ups this pass gives the reactors are coalesced
                // and given when the guard drops, before any wait.
                let _wakes = net::hold_wakes();
                #[cfg(test)]
                {
                    self.wakes_held = true;
                }
                self.drain_store_replies()?;
                if !self.owner.outbox_is_empty() {
                    self.dispatch()?;
                }
                self.drain_io_events()?;
                self.drain_inputs()?;
                self.drain_coord_messages()?;
                self.drain_pending_inline()?;
                self.drain_pending_qos0()?;
                self.due_deadlines(now)?;
                self.drain_inputs()?;
                self.dispatch()?;
                self.schedule_maintenance();
            }
            #[cfg(test)]
            {
                self.wakes_held = false;
            }

            if self.stopping {
                if self.inflight.is_empty()
                    && self.work_reads.is_empty()
                    && self.control_inputs.is_empty()
                    && self.owner.outbox_is_empty()
                    && self.retained_output.is_empty()
                {
                    return Ok(());
                }
                if self.cancel.load(Ordering::Relaxed) && self.inflight.is_empty() {
                    return Ok(());
                }
            }

            if self.progress == before {
                let wait = if !self.owner.outbox_is_empty()
                    || !self.retained_output.is_empty()
                    || !self.pending_inline.is_empty()
                    || !self.pending_qos0.is_empty()
                    || self.stopping
                {
                    BACKPRESSURE_TICK
                } else {
                    self.idle_wait(now)
                };
                // A hold kept through the wait would suppress the kicks of
                // every producer that holds nothing (record 0044, model
                // reactor-wake).
                #[cfg(test)]
                assert!(
                    !self.wakes_held,
                    "a delivery shard waits while it holds the reactors' wake-ups"
                );
                self.store.wake.wait(wait);
            }
        }
    }

    fn idle_wait(&self, now: Instant) -> Duration {
        self.next_timer_due
            .map_or(IDLE_TICK, |due| due.saturating_duration_since(now))
            .min(IDLE_TICK)
    }

    fn message_now(&mut self) -> Result<MessageTime, Failure> {
        let reading = self
            .clock
            .read()
            .map_err(|error| Failure::plain(error.code()))?;
        let now = MessageTime {
            domain: reading.domain,
            tick: reading.tick,
        };
        if self
            .message_last_time
            .is_some_and(|last| last.domain == now.domain && now.tick < last.tick)
        {
            return Err(Failure::plain("message_clock_regressed"));
        }
        self.message_last_time = Some(now);
        Ok(now)
    }

    fn queue_work(&mut self, session: SessionId) {
        if self.queued_work_sessions.insert(session) {
            self.inputs.push_back(Input::WorkAvailable { session });
        }
    }

    fn queue_work_front(&mut self, session: SessionId) {
        if self.queued_work_sessions.insert(session) {
            self.inputs.push_front(Input::WorkAvailable { session });
        }
    }

    /// Gives the core the identifiers the coordinator may hold for
    /// `session`, read from the lease after the dirty mark that came with the
    /// message just read: the claims the shard makes avoid them.
    fn refresh_foreign_packet_ids(&mut self, session: SessionId) {
        if let Some(sess) = self.sessions.get(&session) {
            let reserved = sess.lease.reserved_snapshot();
            self.owner.shard_set_foreign_packet_ids(session, reserved);
        }
    }

    /// Fences `session` from reads of the store, or opens it, after a dirty
    /// message of the coordinator. A message without sequence means inline
    /// claims are pending: one may commit, and a read would deliver its
    /// row before the claim arrives, then again. A message with a sequence
    /// comes after every claim committed for the session; those still
    /// waiting in `pending_inline` are applied before the session is read.
    fn fence_after_message(&mut self, session: SessionId, dirty_seq: u64) {
        let pending = dirty_seq == 0;
        let waiting = !pending
            && self
                .pending_inline
                .iter()
                .any(|item| item.claim.target == session);
        if waiting {
            self.inline_fenced.insert(session);
        } else if !self.inline_fenced.is_empty() {
            self.inline_fenced.remove(&session);
        }
        self.owner.shard_fence_scans(session, pending || waiting);
    }

    /// Opens the sessions whose waiting claims are all applied.
    fn open_drained_fences(&mut self) {
        let drained: Vec<SessionId> = self
            .inline_fenced
            .iter()
            .copied()
            .filter(|&session| {
                !self
                    .pending_inline
                    .iter()
                    .any(|item| item.claim.target == session)
            })
            .collect();
        for session in drained {
            self.inline_fenced.remove(&session);
            self.owner.shard_fence_scans(session, false);
            self.queue_work(session);
        }
    }

    fn try_publish_lease(&mut self, session: SessionId) {
        let Some(sess) = self.sessions.get_mut(&session) else {
            return;
        };
        if sess.lease_published || sess.seen_dirty_seq == 0 {
            return;
        }
        if sess.scan_started_dirty_seq < sess.seen_dirty_seq {
            return;
        }
        // A committed inline claim not yet applied is no row of the core:
        // published now, the lease would not count its identifier.
        if self
            .pending_inline
            .iter()
            .any(|item| item.claim.target == session)
        {
            return;
        }
        let generation = sess.generation;
        let rm = usize::from(
            sess.receive_maximum
                .min(mqttiumd_core::owner::MAX_OUTGOING_WINDOW),
        );
        let preclaim_cap = 2 * rm;
        if let Some((in_flight, used_pi)) = self.owner.shard_inline_eligibility(session, generation)
        {
            sess.lease.publish_eligible(
                generation,
                preclaim_cap,
                in_flight,
                used_pi,
                sess.seen_dirty_seq,
            );
            sess.published_dirty_seq = sess.seen_dirty_seq;
            sess.lease_published = true;
            // The lease now holds the rows' view; the coordinator reserves
            // from it, and a later dirty mark gives the shard its bits again.
            self.owner.shard_set_foreign_packet_ids(session, [0; 8]);
        }
    }

    fn drain_store_replies(&mut self) -> Result<(), Failure> {
        for _ in 0..crate::writer::CONTROL_QUEUE {
            match self.store.control_receiver.try_recv() {
                Ok(ControlResponse::Collected {
                    effect,
                    collection,
                    result,
                }) => {
                    self.progress = self.progress.wrapping_add(1);
                    let reply = result?;
                    if self.owner.collection_reply_is_live(effect, collection) {
                        self.control_inputs.push_back(Input::ReceiptsCollected {
                            effect,
                            collection,
                            reply,
                        });
                    }
                }
                Ok(ControlResponse::Failed(err)) => return Err(err),
                Ok(_) => {}
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    if self.stopping || self.cancel.load(Ordering::Relaxed) {
                        self.stopping = true;
                        break;
                    }
                    return Err(Failure::plain("shard_writer_control_disconnected"));
                }
            }
        }

        for _ in 0..crate::writer::QUEUE {
            match self.store.receiver.try_recv() {
                Ok(Response::Store {
                    effect,
                    operation,
                    reply,
                    ..
                }) => {
                    self.progress = self.progress.wrapping_add(1);
                    if self.inflight.remove(&effect) != Some(operation) {
                        return Err(Failure::plain("shard_uncorrelated_store_result"));
                    }
                    self.store_inputs.push_back(Input::StoreResult {
                        effect,
                        operation,
                        reply,
                    });
                }
                Ok(Response::Work {
                    effect,
                    session,
                    result,
                }) => {
                    self.progress = self.progress.wrapping_add(1);
                    if self.work_reads.remove(&effect) != Some(session) {
                        return Err(Failure::plain("shard_uncorrelated_work_page"));
                    }
                    let page = result.map_err(|e| Failure::read(e, ReadPhase::Work))?;
                    if !self.sessions.contains_key(&session) {
                        continue;
                    }
                    if let Some(sess) = self.sessions.get_mut(&session)
                        && sess.in_flight_load.is_some_and(|(eid, _)| eid == effect)
                    {
                        let (_, started_seq) = sess.in_flight_load.take().expect("checked");
                        if page.next.is_none() {
                            sess.scan_started_dirty_seq =
                                sess.scan_started_dirty_seq.max(started_seq);
                        }
                    }
                    self.message_clock_needed = true;
                    self.store_inputs.push_back(Input::WorkPage {
                        effect,
                        session,
                        items: page.items,
                        next: page.next,
                    });
                }
                Ok(Response::Stopped) => {
                    self.stopping = true;
                    break;
                }
                Ok(_) => {}
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    if self.stopping || self.cancel.load(Ordering::Relaxed) {
                        self.stopping = true;
                        break;
                    }
                    return Err(Failure::plain("shard_writer_disconnected"));
                }
            }
        }
        Ok(())
    }

    fn drain_io_events(&mut self) -> Result<(), Failure> {
        let mut scratch = std::mem::take(&mut self.io_scratch);
        scratch.clear();
        let taken = self.io_inbox.drain_into(&mut scratch, 1024);
        if taken == 0 {
            self.io_scratch = scratch;
            return Ok(());
        }
        self.progress = self.progress.wrapping_add(taken as u64);
        while let Some(event) = scratch.pop_front() {
            match event {
                ShardIoEvent::AckPacket {
                    credit,
                    connection,
                    first,
                    body,
                } => {
                    let Some(sess) = self.sessions.get(&connection.session) else {
                        continue;
                    };
                    if sess.generation != connection.generation {
                        continue;
                    }
                    let version = sess.version;
                    if version == Version::V5
                        && codec::wire_size(body.len()) > codec::ANNOUNCED_PACKET_BYTES as usize
                    {
                        self.notify_coord(ShardToCoord::CloseConnection {
                            generation: connection.generation,
                            code: "packet_too_large",
                            disconnect_reason: Some(0x95),
                        });
                        continue;
                    }
                    match codec::decode_for_with_cache(version, first, &body, BOUNDS, None) {
                        Ok(Packet::Core(InboundPacket::Puback { packet_id, reason })) => {
                            let target = self
                                .outgoing_bindings
                                .get(&(connection, packet_id))
                                .copied();
                            self.ack_inputs.push_back((
                                Input::ObservedPuback {
                                    connection,
                                    packet_id,
                                    reason,
                                    target,
                                },
                                credit,
                            ));
                        }
                        Ok(Packet::Core(InboundPacket::Qos2Ack {
                            kind,
                            packet_id,
                            reason,
                        })) => {
                            let target = self
                                .outgoing_bindings
                                .get(&(connection, packet_id))
                                .copied();
                            self.ack_inputs.push_back((
                                Input::ObservedQos2Ack {
                                    connection,
                                    kind,
                                    packet_id,
                                    reason,
                                    target,
                                },
                                credit,
                            ));
                        }
                        Ok(_) => {}
                        Err(error) => {
                            self.notify_coord(ShardToCoord::CloseConnection {
                                generation: connection.generation,
                                code: "packet_refused",
                                disconnect_reason: (version == Version::V5)
                                    .then_some(error.reason_code()),
                            });
                        }
                    }
                }
                ShardIoEvent::Written {
                    generation,
                    written,
                    ..
                } => {
                    self.handle_written(generation, written);
                }
            }
        }
        self.io_scratch = scratch;
        Ok(())
    }

    fn handle_written(&mut self, _generation: ConnectionGeneration, written: Written) {
        match written {
            Written::Started {
                correlation: Some((effect, connection)),
            } => {
                if self.frames.contains_key(&effect) {
                    self.terminal_inputs.push_back(Input::FrameResult {
                        effect,
                        connection,
                        progress: FrameProgress::Started,
                    });
                }
            }
            Written::NotStarted {
                correlation: Some((effect, connection)),
                reason,
            } => {
                if let Some(frame) = self.frames.remove(&effect) {
                    if frame.kind == "PUBLISH0" {
                        let drop_reason = match reason {
                            mqttiumd_contract::message_expiry::MessageExpiryBlock::Expired => {
                                Qos0DropReason::MessageExpired
                            }
                            mqttiumd_contract::message_expiry::MessageExpiryBlock::TemporalUncertain => {
                                Qos0DropReason::TemporalUncertain
                            }
                        };
                        self.terminal_inputs.push_back(Input::Qos0EmitRefused {
                            effect,
                            connection,
                            reason: drop_reason,
                        });
                    } else {
                        self.terminal_inputs.push_back(Input::FrameResult {
                            effect,
                            connection,
                            progress: FrameProgress::NotStarted { reason },
                        });
                    }
                }
            }
            Written::Complete {
                correlation: Some((effect, connection)),
                bytes,
            } => {
                if self.frames.remove(&effect).is_some() {
                    self.terminal_inputs.push_back(Input::FrameResult {
                        effect,
                        connection,
                        progress: FrameProgress::Written {
                            confirmed: bytes as u64,
                            total: bytes as u64,
                        },
                    });
                }
            }
            Written::Failed {
                correlation: Some((effect, connection)),
                ..
            } => {
                if let Some(frame) = self.frames.remove(&effect) {
                    if frame.kind == "PUBLISH0" {
                        self.terminal_inputs.push_back(Input::Qos0EmitRefused {
                            effect,
                            connection,
                            reason: Qos0DropReason::ConnectionClosed,
                        });
                    } else {
                        self.terminal_inputs.push_back(Input::FrameResult {
                            effect,
                            connection,
                            progress: FrameProgress::Failed,
                        });
                    }
                }
            }
            _ => {}
        }
    }

    fn finish_detaches(&mut self) -> Result<(), Failure> {
        if self.pending_detaches.is_empty() {
            return Ok(());
        }
        // The reactor task has already finished. No later packet or write
        // report can enter behind this generation's final observation.
        let mut actionable = Vec::new();
        for &key in self.pending_detaches.keys() {
            let io_pending = self.sessions.get(&key.session).is_some_and(|session| {
                session.generation == key.generation
                    && (session.outgoing.pending_shard_acks() != 0
                        || session.outgoing.pending_shard_written() != 0)
            });
            let terminal_pending = self.terminal_inputs.iter().any(|input| {
                matches!(input,
                Input::FrameResult {connection,..} | Input::Qos0EmitRefused {connection,..}
                    if *connection == key)
            });
            if !io_pending && !terminal_pending {
                let progress = self.owner.shard_ack_progress(key);
                if progress != ShardAckProgress::Waiting {
                    actionable.push((key, progress));
                }
            }
        }
        for (key, progress) in actionable {
            if progress == ShardAckProgress::WorkReady {
                self.queue_work(key.session);
                continue;
            }
            let (dirty_seq, completed) =
                self.pending_detaches.remove(&key).expect("selected fence");
            self.cleanup_generation_state(key.generation);
            self.owner
                .shard_detach_session(key.session, key.generation)
                .map_err(|_| Failure::plain("shard_detach_fault"))?;
            if let Some(sess) = self.sessions.get_mut(&key.session)
                && sess.generation == key.generation
            {
                sess.lease_published = false;
                sess.seen_dirty_seq = dirty_seq;
                sess.in_flight_load = None;
            }
            completed.store(true, Ordering::Release);
            self.notices.wake_coordinator();
            self.progress = self.progress.wrapping_add(1);
        }
        Ok(())
    }

    fn drain_coord_messages(&mut self) -> Result<(), Failure> {
        for _ in 0..256 {
            match self.coord_rx.try_recv() {
                Ok(msg) => {
                    self.progress = self.progress.wrapping_add(1);
                    match msg {
                        CoordToShard::Attach {
                            session,
                            revision,
                            generation,
                            receive_maximum,
                            maximum_packet_size,
                            version,
                            outgoing,
                            lease,
                            dirty_seq,
                        } => {
                            let old_gen = self.sessions.get(&session).map(|s| s.generation);
                            if let Some(old_gen) = old_gen
                                && old_gen != generation
                            {
                                // A resumed connection cannot open until the old
                                // generation's detach receipt releases the peer.
                                self.cleanup_generation_state(old_gen);
                            }
                            self.owner
                                .shard_attach_session(
                                    session,
                                    revision,
                                    generation,
                                    receive_maximum,
                                    maximum_packet_size,
                                )
                                .map_err(|_| Failure::plain("shard_attach_fault"))?;
                            self.sessions.insert(
                                session,
                                ShardSession {
                                    generation,
                                    version,
                                    max_packet: maximum_packet_size,
                                    receive_maximum,
                                    outgoing,
                                    lease,
                                    seen_dirty_seq: dirty_seq,
                                    scan_started_dirty_seq: 0,
                                    published_dirty_seq: 0,
                                    lease_published: false,
                                    in_flight_load: None,
                                },
                            );
                            self.refresh_foreign_packet_ids(session);
                            self.fence_after_message(session, dirty_seq);
                            self.queue_work(session);
                        }
                        CoordToShard::Detach {
                            connection,
                            dirty_seq,
                            completed,
                        } => {
                            self.pending_detaches
                                .insert(connection, (dirty_seq, completed));
                        }
                        CoordToShard::Retire { session } => {
                            if let Some(sess) = self.sessions.remove(&session) {
                                self.cleanup_generation_state(sess.generation);
                            }
                            self.outgoing_bindings
                                .retain(|(conn, _), _| conn.session != session);
                            self.owner
                                .shard_retire_session(session)
                                .map_err(|_| Failure::plain("shard_retire_fault"))?;
                            self.owner.shard_set_foreign_packet_ids(session, [0; 8]);
                            self.inline_fenced.remove(&session);
                        }
                        CoordToShard::NotifyDirty {
                            session,
                            dirty_seq,
                            clear_known_through,
                        } => {
                            if let Some(sess) = self.sessions.get_mut(&session) {
                                sess.lease_published = false;
                                if dirty_seq > 0 {
                                    sess.seen_dirty_seq = sess.seen_dirty_seq.max(dirty_seq);
                                } else {
                                    sess.seen_dirty_seq = 0;
                                }
                            }
                            self.owner.shard_notify_dirty(session, clear_known_through);
                            self.refresh_foreign_packet_ids(session);
                            self.fence_after_message(session, dirty_seq);
                            self.queue_work(session);
                        }
                        CoordToShard::DirtyBarrier { session, dirty_seq } => {
                            if let Some(sess) = self.sessions.get_mut(&session) {
                                sess.lease_published = false;
                                sess.seen_dirty_seq = sess.seen_dirty_seq.max(dirty_seq);
                            }
                            self.owner.shard_notify_dirty(session, false);
                            self.refresh_foreign_packet_ids(session);
                            self.fence_after_message(session, dirty_seq);
                            self.queue_work(session);
                        }
                        CoordToShard::InlineCommitted {
                            topic,
                            payload,
                            properties,
                            claims,
                        } => {
                            for claim in claims {
                                self.pending_inline.push_back(PendingInlineItem {
                                    claim,
                                    topic: topic.clone(),
                                    payload: payload.clone(),
                                    properties: properties.clone(),
                                });
                            }
                        }
                        CoordToShard::Qos0Deliver {
                            origin,
                            topic,
                            payload,
                            expiry,
                            properties,
                            targets,
                        } => {
                            for target in targets {
                                if self.pending_qos0.len() >= PENDING_QOS0_LIMIT {
                                    self.shed_qos0(origin, target.target);
                                    continue;
                                }
                                self.pending_qos0.push_back(PendingQos0Item {
                                    origin,
                                    target: target.target,
                                    connection: target.connection,
                                    topic: topic.clone(),
                                    payload: payload.clone(),
                                    expiry,
                                    properties: properties.clone(),
                                });
                            }
                        }
                        CoordToShard::MessagesExpired { deliveries } => {
                            if !deliveries.is_empty()
                                && let Some(bv) =
                                    BoundedVec::from_slice(&deliveries, deliveries.len())
                            {
                                self.message_clock_needed = true;
                                self.store_inputs
                                    .push_back(Input::MessagesExpired { deliveries: bv });
                            }
                        }
                        #[cfg(test)]
                        CoordToShard::Test(test) => test.run(),
                    }
                }
                Err(TryRecvError::Empty) => {
                    // Stored after the coordinator's last message: seen
                    // here, nothing it sent before the stop is still queued.
                    if self.stop.load(Ordering::Acquire) {
                        self.stopping = true;
                    }
                    break;
                }
                Err(TryRecvError::Disconnected) => {
                    self.stopping = true;
                    break;
                }
            }
        }
        self.finish_detaches()
    }

    fn cleanup_generation_state(&mut self, generation: ConnectionGeneration) {
        self.outgoing_bindings
            .retain(|(conn, _), _| conn.generation != generation);
        let stale: Vec<EffectId> = self
            .frames
            .iter()
            .filter(|(_, f)| f.connection.generation == generation)
            .map(|(id, _)| id)
            .collect();
        for id in stale {
            if let Some(frame) = self.frames.remove(&id) {
                if frame.kind == "PUBLISH0" {
                    self.terminal_inputs.push_back(Input::Qos0EmitRefused {
                        effect: id,
                        connection: frame.connection,
                        reason: Qos0DropReason::ConnectionClosed,
                    });
                } else {
                    self.terminal_inputs.push_back(Input::FrameResult {
                        effect: id,
                        connection: frame.connection,
                        progress: FrameProgress::Failed,
                    });
                }
            }
        }
    }

    fn drain_pending_inline(&mut self) -> Result<(), Failure> {
        let mut budget = 1024usize;
        while budget > 0
            && let Some(item) = self.pending_inline.pop_front()
        {
            budget -= 1;
            let mut close_notices = Vec::new();
            let mut sink = ShardSink {
                store: &mut self.store,
                sessions: &mut self.sessions,
                outgoing_bindings: &mut self.outgoing_bindings,
                frames: &mut self.frames,
                inflight: &mut self.inflight,
                work_reads: &mut self.work_reads,
                timers: &mut self.timers,
                next_timer_due: &mut self.next_timer_due,
                inputs: &mut self.inputs,
                terminal_inputs: &mut self.terminal_inputs,
                events: &self.events,
                limits: self.limits,
                close_notices: &mut close_notices,
                error: None,
            };
            let outcome = self.owner.step_shard_inline_committed(
                &item.claim,
                item.topic.clone(),
                item.payload.clone(),
                item.properties.clone(),
                Some(&mut sink),
            );
            let sink_err = sink.error.take();
            for notice in close_notices {
                self.notify_coord(notice);
            }
            if let Some(err) = sink_err {
                return Err(err);
            }
            match outcome {
                ShardStepOutcome::Applied { .. } => {
                    self.progress = self.progress.wrapping_add(1);
                    if self.owner.last_step_may_unblock_work()
                        && self.owner.has_ready_work(item.claim.target)
                    {
                        self.queue_work_front(item.claim.target);
                    }
                    // The claim is a row now: the lease it held back may be
                    // published, with its identifier.
                    self.try_publish_lease(item.claim.target);
                }
                ShardStepOutcome::NeedCapacity(_) => {
                    self.pending_inline.push_front(item);
                    break;
                }
                ShardStepOutcome::Exhausted(_) => return Err(Failure::plain("shard_exhausted")),
                ShardStepOutcome::Fatal(_) => return Err(Failure::plain("shard_core_fault")),
            }
        }
        if !self.inline_fenced.is_empty() {
            self.open_drained_fences();
        }
        Ok(())
    }

    /// Drops a delegated QoS 0 delivery the shard has no room to keep, as
    /// the core drops one its volatile queue cannot hold, with the same
    /// journal entry.
    fn shed_qos0(&self, origin: Qos0Origin, target: SessionId) {
        let _ = self.events.emit_with("qos0_dropped", || {
            json!({
                "details": {
                    "target": target.get().to_string(),
                    "reason": Qos0DropReason::CapacityExceeded.code(),
                    "origin_kind": match origin {
                        Qos0Origin::Volatile { .. } => "volatile",
                        Qos0Origin::Consumed { .. } => "consumed",
                    }
                }
            })
        });
    }

    fn drain_pending_qos0(&mut self) -> Result<(), Failure> {
        let mut budget = 1024usize;
        while budget > 0
            && let Some(item) = self.pending_qos0.pop_front()
        {
            budget -= 1;
            let mut close_notices = Vec::new();
            let mut sink = ShardSink {
                store: &mut self.store,
                sessions: &mut self.sessions,
                outgoing_bindings: &mut self.outgoing_bindings,
                frames: &mut self.frames,
                inflight: &mut self.inflight,
                work_reads: &mut self.work_reads,
                timers: &mut self.timers,
                next_timer_due: &mut self.next_timer_due,
                inputs: &mut self.inputs,
                terminal_inputs: &mut self.terminal_inputs,
                events: &self.events,
                limits: self.limits,
                close_notices: &mut close_notices,
                error: None,
            };
            let outcome = self.owner.step_shard_qos0(
                item.origin,
                item.target,
                item.connection,
                item.topic.clone(),
                item.payload.clone(),
                item.expiry,
                item.properties.clone(),
                Some(&mut sink),
            );
            let sink_err = sink.error.take();
            for notice in close_notices {
                self.notify_coord(notice);
            }
            if let Some(err) = sink_err {
                return Err(err);
            }
            match outcome {
                ShardStepOutcome::Applied { .. } => {
                    self.progress = self.progress.wrapping_add(1);
                }
                ShardStepOutcome::NeedCapacity(_) => {
                    self.pending_qos0.push_front(item);
                    break;
                }
                ShardStepOutcome::Exhausted(_) => return Err(Failure::plain("shard_exhausted")),
                ShardStepOutcome::Fatal(_) => return Err(Failure::plain("shard_core_fault")),
            }
        }
        Ok(())
    }

    fn due_deadlines(&mut self, now: Instant) -> Result<(), Failure> {
        if self.next_timer_due.is_none_or(|next| now < next) {
            return Ok(());
        }
        let due: Vec<DeadlineToken> = self
            .timers
            .iter()
            .filter(|(_, d)| **d <= now)
            .map(|(t, _)| *t)
            .collect();
        for token in due {
            let logical = LogicalTime::new(
                self.started
                    .elapsed()
                    .as_millis()
                    .try_into()
                    .map_err(|_| Failure::plain("clock_exhausted"))?,
            );
            let input = Input::Deadline {
                token,
                now: logical,
            };
            if self.owner.collection_deadline_is_live(token) {
                self.control_inputs.push_back(input);
            } else {
                self.inputs.push_back(input);
            }
            self.timers.remove(&token);
        }
        self.next_timer_due = self.timers.values().min().copied();
        Ok(())
    }

    fn drain_inputs(&mut self) -> Result<(), Failure> {
        if !(self.inputs.is_empty()
            && self.store_inputs.is_empty()
            && self.ack_inputs.is_empty()
            && self.control_inputs.is_empty()
            && self.terminal_inputs.is_empty())
        {
            let needs_clock = self.message_clock_needed
                || self.message_last_time.is_none()
                || self.owner.has_expiring_messages()
                || self.last_message_clock_sync.elapsed() >= IDLE_TICK;
            if needs_clock {
                let now = self.message_now()?;
                self.message_clock_needed = false;
                self.last_message_clock_sync = Instant::now();
                if !matches!(
                    self.owner.step(
                        Input::MessageClock { now },
                        TurnGrant {
                            work_limit: WorkUnits::new(65_536)
                        }
                    ),
                    StepOutcome::Applied { .. }
                ) {
                    return Err(Failure::plain("shard_message_clock_not_applied"));
                }
            }
        }

        let mut step_budget = (self.inputs.len()
            + self.store_inputs.len()
            + self.ack_inputs.len()
            + self.control_inputs.len()
            + self.terminal_inputs.len())
        .min(self.limits.drain_steps);
        let mut steps_taken = 0usize;
        let mut control_blocked = false;
        let mut terminal_blocked = false;
        let mut store_blocked = false;
        let mut ack_blocked = false;
        let mut inputs_blocked = false;

        while steps_taken < step_budget {
            let mut ack_credit = None;
            let Some(input) = (!control_blocked)
                .then(|| self.control_inputs.pop_front())
                .flatten()
                .or_else(|| {
                    (!terminal_blocked)
                        .then(|| self.terminal_inputs.pop_front())
                        .flatten()
                })
                .or_else(|| {
                    (!store_blocked)
                        .then(|| self.store_inputs.pop_front())
                        .flatten()
                })
                .or_else(|| {
                    (!ack_blocked)
                        .then(|| self.ack_inputs.pop_front())
                        .flatten()
                        .map(|(input, credit)| {
                            ack_credit = Some(credit);
                            input
                        })
                })
                .or_else(|| {
                    if inputs_blocked {
                        return None;
                    }
                    let popped = self.inputs.pop_front()?;
                    if let Input::WorkAvailable { session } = &popped {
                        self.queued_work_sessions.remove(session);
                    }
                    Some(popped)
                })
            else {
                break;
            };
            steps_taken += 1;

            let ready_hint = match &input {
                Input::StoreResult {
                    reply:
                        StoreReply::Outcome(StoreOutcome::Committed(
                            CommitFacts::DeliveryFinished { target, .. }
                            | CommitFacts::DeliveryClaimed { target, .. }
                            | CommitFacts::DeliveryConsumed { target, .. }
                            | CommitFacts::DeliveryStarted { target, .. }
                            | CommitFacts::DeliveryPubrelReady { target, .. }
                            | CommitFacts::DeliveryExpired { target, .. },
                        )),
                    ..
                } => Some(*target),
                Input::ObservedPuback { connection, .. }
                | Input::ObservedQos2Ack { connection, .. }
                | Input::FrameResult { connection, .. }
                | Input::Qos0EmitRefused { connection, .. } => Some(connection.session),
                Input::WorkAvailable { session } | Input::WorkPage { session, .. } => {
                    Some(*session)
                }
                _ => None,
            };
            let prune_key = match &input {
                Input::ObservedPuback {
                    connection,
                    packet_id,
                    ..
                }
                | Input::ObservedQos2Ack {
                    connection,
                    packet_id,
                    ..
                } => Some((*connection, *packet_id)),
                Input::StoreResult {
                    reply:
                        StoreReply::Outcome(StoreOutcome::Committed(
                            CommitFacts::DeliveryFinished {
                                target, packet_id, ..
                            }
                            | CommitFacts::DeliveryPubrelReady {
                                target, packet_id, ..
                            }
                            | CommitFacts::DeliveryExpired {
                                target, packet_id, ..
                            },
                        )),
                    ..
                } => self.owner.connection_of(*target).map(|generation| {
                    (
                        ConnectionKey {
                            session: *target,
                            generation,
                        },
                        *packet_id,
                    )
                }),
                _ => None,
            };
            let is_store_or_receipt = matches!(
                input,
                Input::StoreResult { .. } | Input::ReceiptsCollected { .. }
            );
            let is_receipt_collected = matches!(input, Input::ReceiptsCollected { .. });
            let receipt_more = matches!(
                input,
                Input::ReceiptsCollected {
                    reply: mqttiumd_contract::receipts::ReceiptCollectionReply::Applied {
                        more: true,
                        ..
                    },
                    ..
                }
            );
            let inputs_before = self.inputs.len();

            let mut close_notices = Vec::new();
            let mut sink = ShardSink {
                store: &mut self.store,
                sessions: &mut self.sessions,
                outgoing_bindings: &mut self.outgoing_bindings,
                frames: &mut self.frames,
                inflight: &mut self.inflight,
                work_reads: &mut self.work_reads,
                timers: &mut self.timers,
                next_timer_due: &mut self.next_timer_due,
                inputs: &mut self.inputs,
                terminal_inputs: &mut self.terminal_inputs,
                events: &self.events,
                limits: self.limits,
                close_notices: &mut close_notices,
                error: None,
            };
            let (outcome, _accepted) = self.owner.step_and_dispatch(
                input,
                TurnGrant {
                    work_limit: WorkUnits::new(65_536),
                },
                &mut sink,
            );
            let sink_err = sink.error.take();
            for notice in close_notices {
                self.notify_coord(notice);
            }
            if let Some(err) = sink_err {
                return Err(err);
            }

            match outcome {
                StepOutcome::Applied { .. } => {
                    self.progress = self.progress.wrapping_add(1);
                    if is_receipt_collected {
                        self.receipt_pressure_blocked = false;
                        ack_blocked = false;
                        inputs_blocked = false;
                        if receipt_more
                            || self.owner.receipt_status().is_some_and(|(_, span, holes)| {
                                span >= mqttiumd_contract::receipts::RECEIPT_COLLECTION_TRIGGER
                                    && span > holes
                            })
                        {
                            self.maintenance_needed = true;
                        }
                    }
                    if is_store_or_receipt {
                        self.timers_dirty = true;
                        ack_blocked = false;
                        inputs_blocked = false;
                        if self.owner.collection_ready() {
                            self.maintenance_needed = true;
                        }
                    }
                    if let Some((finished_session, Some(finished_pi))) =
                        self.owner.last_finished_delivery()
                        && let Some(sess) = self.sessions.get(&finished_session)
                        && sess.lease_published
                    {
                        sess.lease.release_pi(finished_pi);
                    }
                    if let Some((conn, pi)) = prune_key
                        && let Some(&target) = self.outgoing_bindings.get(&(conn, pi))
                        && !self.owner.outgoing_binding_is_live(conn, pi, target)
                    {
                        self.outgoing_bindings.remove(&(conn, pi));
                    }
                    if let Some(session) = ready_hint {
                        if self.owner.last_step_may_unblock_work()
                            && self.owner.has_ready_work(session)
                        {
                            self.queue_work_front(session);
                        }
                        self.try_publish_lease(session);
                    }
                    if self.inputs.len() > inputs_before {
                        step_budget = step_budget
                            .saturating_add(self.inputs.len() - inputs_before)
                            .min(self.limits.drain_steps);
                    }
                }
                StepOutcome::NeedCapacity { input, need, .. } => {
                    if need.resource == mqttiumd_contract::capacity::Resource::ReceiptSlots {
                        self.receipt_pressure_blocked = true;
                        self.maintenance_needed = true;
                        self.schedule_maintenance();
                        control_blocked = false;
                    }
                    match input {
                        Input::MaintainReceipts | Input::ReceiptsCollected { .. } => {
                            self.control_inputs.push_front(input);
                            control_blocked = true;
                        }
                        Input::FrameResult { .. } | Input::Qos0EmitRefused { .. } => {
                            self.terminal_inputs.push_front(input);
                            terminal_blocked = true;
                        }
                        Input::StoreResult { .. }
                        | Input::WorkPage { .. }
                        | Input::MessagesExpired { .. } => {
                            self.store_inputs.push_front(input);
                            store_blocked = true;
                        }
                        Input::ObservedPuback { .. } | Input::ObservedQos2Ack { .. } => {
                            self.ack_inputs.push_front((
                                input,
                                ack_credit.take().expect("ACK input has credit"),
                            ));
                            ack_blocked = true;
                        }
                        other => {
                            if let Input::WorkAvailable { session } = &other {
                                self.queued_work_sessions.insert(*session);
                            }
                            self.inputs.push_front(other);
                            inputs_blocked = true;
                        }
                    }
                }
                StepOutcome::Exhausted { .. } => return Err(Failure::plain("shard_exhausted")),
                StepOutcome::Fatal(_) => return Err(Failure::plain("shard_core_fault")),
            }
        }

        if self.timers_dirty && self.timers.len() >= 1024 {
            self.timers_dirty = false;
            self.timers
                .retain(|token, _| self.owner.deadline_is_live(*token));
            self.next_timer_due = self.timers.values().min().copied();
        }
        Ok(())
    }

    fn dispatch(&mut self) -> Result<(), Failure> {
        if self.owner.outbox_is_empty() && self.retained_output.is_empty() {
            return Ok(());
        }
        let mut close_notices = Vec::new();
        let mut sink = ShardSink {
            store: &mut self.store,
            sessions: &mut self.sessions,
            outgoing_bindings: &mut self.outgoing_bindings,
            frames: &mut self.frames,
            inflight: &mut self.inflight,
            work_reads: &mut self.work_reads,
            timers: &mut self.timers,
            next_timer_due: &mut self.next_timer_due,
            inputs: &mut self.inputs,
            terminal_inputs: &mut self.terminal_inputs,
            events: &self.events,
            limits: self.limits,
            close_notices: &mut close_notices,
            error: None,
        };
        let retried = self.retained_output.retry(&mut sink);
        let report = self
            .owner
            .dispatch(&mut self.retained_output.sink(&mut sink));
        let sink_err = sink.error.take();
        for notice in close_notices {
            self.notify_coord(notice);
        }
        if report.accepted + retried > 0 {
            self.progress = self
                .progress
                .wrapping_add((report.accepted + retried) as u64);
        }
        if let Some(err) = sink_err {
            return Err(err);
        }
        Ok(())
    }

    fn schedule_maintenance(&mut self) {
        if !self.maintenance_needed {
            return;
        }
        let urgent = self.receipt_pressure_blocked
            || self.last_maintenance_flush.elapsed() >= IDLE_TICK
            || self.owner.receipt_status().is_some_and(|(_, span, _)| {
                span >= usize::from(mqttiumd_contract::receipts::MAX_COLLECTION_ROWS)
            });
        if !urgent {
            return;
        }
        if self.control_inputs.len() < crate::writer::CONTROL_QUEUE
            && !self
                .control_inputs
                .iter()
                .any(|i| matches!(i, Input::MaintainReceipts))
        {
            self.control_inputs.push_back(Input::MaintainReceipts);
            self.maintenance_needed = false;
            self.last_maintenance_flush = Instant::now();
        }
    }
}

struct ShardSink<'a> {
    store: &'a mut ShardStoreHandle,
    sessions: &'a mut FlatMap<SessionId, ShardSession>,
    outgoing_bindings: &'a mut FlatMap<(ConnectionKey, OutboundPacketId), PubackTarget>,
    frames: &'a mut ScopedFlatMap<EffectId, ShardPendingFrame>,
    inflight: &'a mut ScopedFlatMap<EffectId, OperationId>,
    work_reads: &'a mut ScopedFlatMap<EffectId, SessionId>,
    timers: &'a mut BTreeMap<DeadlineToken, Instant>,
    next_timer_due: &'a mut Option<Instant>,
    inputs: &'a mut VecDeque<Input>,
    terminal_inputs: &'a mut VecDeque<Input>,
    events: &'a Events,
    limits: Limits,
    close_notices: &'a mut Vec<ShardToCoord>,
    error: Option<Failure>,
}

impl EffectSink for ShardSink<'_> {
    fn try_submit(&mut self, effect: OwnedEffect) -> SubmitResult {
        let id = effect.id();
        let refused = |effect, reason| SubmitResult::NotSubmitted { effect, reason };
        if self.error.is_some() {
            return refused(effect, SubmitBlock::Closed);
        }
        if matches!(
            effect.effect(),
            Effect::EmitPublish { .. } | Effect::EmitFrame { .. } | Effect::EmitQos0 { .. }
        ) && (self.frames.len() >= self.limits.frames
            || self
                .terminal_inputs
                .len()
                .saturating_add(2 * (self.frames.len() + 1))
                > self.limits.terminal_inputs)
        {
            return refused(effect, SubmitBlock::QueueFull);
        }
        match effect.effect() {
            Effect::Persist { command } => {
                if self.inflight.len() >= self.limits.in_flight {
                    return refused(effect, SubmitBlock::InFlightLimit);
                }
                let op = command.recipe.operation();
                match self.store.sender.try_send(Request::Effect(effect)) {
                    Ok(()) => {
                        self.inflight.insert(id, op);
                        SubmitResult::Accepted
                    }
                    Err(TrySendError::Full(Request::Effect(effect))) => {
                        refused(effect, SubmitBlock::QueueFull)
                    }
                    Err(TrySendError::Disconnected(Request::Effect(effect))) => {
                        self.error = Some(Failure::plain("shard_writer_disconnected"));
                        refused(effect, SubmitBlock::Closed)
                    }
                    _ => unreachable!(),
                }
            }
            Effect::ResolveAndFence { operation } => {
                if self.inflight.len() >= self.limits.in_flight {
                    return refused(effect, SubmitBlock::InFlightLimit);
                }
                let op = *operation;
                match self.store.sender.try_send(Request::Effect(effect)) {
                    Ok(()) => {
                        self.inflight.insert(id, op);
                        SubmitResult::Accepted
                    }
                    Err(TrySendError::Full(Request::Effect(effect))) => {
                        refused(effect, SubmitBlock::QueueFull)
                    }
                    Err(TrySendError::Disconnected(Request::Effect(effect))) => {
                        self.error = Some(Failure::plain("shard_writer_disconnected"));
                        refused(effect, SubmitBlock::Closed)
                    }
                    _ => unreachable!(),
                }
            }
            Effect::LoadWork { session, .. } => {
                if self.work_reads.len() >= self.limits.in_flight {
                    return refused(effect, SubmitBlock::InFlightLimit);
                }
                let session = *session;
                match self.store.sender.try_send(Request::Effect(effect)) {
                    Ok(()) => {
                        self.work_reads.insert(id, session);
                        if let Some(sess) = self.sessions.get_mut(&session) {
                            sess.in_flight_load = Some((id, sess.seen_dirty_seq));
                        }
                        SubmitResult::Accepted
                    }
                    Err(TrySendError::Full(Request::Effect(effect))) => {
                        refused(effect, SubmitBlock::QueueFull)
                    }
                    Err(TrySendError::Disconnected(Request::Effect(effect))) => {
                        self.error = Some(Failure::plain("shard_writer_disconnected"));
                        refused(effect, SubmitBlock::Closed)
                    }
                    _ => unreachable!(),
                }
            }
            Effect::CollectReceipts { collection } => {
                let collection = *collection;
                match self.store.control_sender.try_send(ControlRequest::Collect {
                    effect: id,
                    collection,
                }) {
                    Ok(()) => {
                        let _ = effect.into_parts();
                        SubmitResult::Accepted
                    }
                    Err(TrySendError::Full(_)) => refused(effect, SubmitBlock::QueueFull),
                    Err(TrySendError::Disconnected(_)) => {
                        self.error = Some(Failure::plain("shard_writer_control_disconnected"));
                        refused(effect, SubmitBlock::Closed)
                    }
                }
            }
            Effect::ArmDeadline { token, after } => {
                if self.timers.len() >= self.limits.timers {
                    return refused(effect, SubmitBlock::QueueFull);
                }
                let Some(due) = Instant::now().checked_add(Duration::from_millis(after.get()))
                else {
                    self.error = Some(Failure::plain("timer_overflow"));
                    return refused(effect, SubmitBlock::Closed);
                };
                self.timers.insert(*token, due);
                *self.next_timer_due = Some(match *self.next_timer_due {
                    Some(cur) => cur.min(due),
                    None => due,
                });
                let _ = effect.into_parts();
                SubmitResult::Accepted
            }
            Effect::Continue { continuation } => {
                self.inputs.push_front(Input::Continue {
                    continuation: *continuation,
                    budget: WorkUnits::new(65_536),
                });
                let _ = effect.into_parts();
                SubmitResult::Accepted
            }
            Effect::EmitPublish {
                expiry,
                properties,
                started,
                connection,
                delivery,
                cause,
                qos,
                packet_id,
                claim,
                topic,
                payload,
                dup,
                retain,
            } => {
                let (connection, delivery, packet_id, claim, dup, retain) =
                    (*connection, *delivery, *packet_id, *claim, *dup, *retain);
                let Some(sess) = self.sessions.get(&connection.session).filter(|s| {
                    s.generation == connection.generation && !s.outgoing.is_cancelled()
                }) else {
                    let _ = effect.into_parts();
                    self.terminal_inputs.push_back(Input::FrameResult {
                        effect: id,
                        connection,
                        progress: FrameProgress::Failed,
                    });
                    return SubmitResult::Accepted;
                };
                let version = sess.version;
                let retain =
                    retain && (version == Version::V5 || *cause == DeliveryCause::RetainedReplay);
                let expiry_val = expiry.as_deref().copied();
                let encoded = match codec::encode_delivery_segments(
                    version,
                    (packet_id, *qos),
                    topic,
                    payload,
                    dup,
                    retain,
                    expiry_val.map(|v| v.interval),
                    properties.as_deref(),
                ) {
                    Ok(encoded) if encoded.total_len <= sess.max_packet as usize => encoded,
                    _ => {
                        self.close_notices.push(ShardToCoord::CloseConnection {
                            generation: connection.generation,
                            code: "outgoing_packet_limit",
                            disconnect_reason: None,
                        });
                        let _ = effect.into_parts();
                        self.terminal_inputs.push_back(Input::FrameResult {
                            effect: id,
                            connection,
                            progress: FrameProgress::Failed,
                        });
                        return SubmitResult::Accepted;
                    }
                };
                let write_expiry = expiry_val.map(|expiry| {
                    Box::new(WriteExpiry {
                        expiry,
                        value_offset: encoded.expiry_value_offset,
                        started: *started,
                    })
                });
                let binding_key = (connection, packet_id);
                let binding = PubackTarget {
                    delivery,
                    claim,
                    frame: id,
                };
                if self.outgoing_bindings.contains_key(&binding_key) {
                    self.error = Some(Failure::plain("shard_outgoing_binding_conflict"));
                    return refused(effect, SubmitBlock::InFlightLimit);
                }
                self.outgoing_bindings.insert(binding_key, binding);
                let track_written = write_expiry.is_some() || self.events.records("frame_written");
                match sess.outgoing.try_send_outgoing(Write {
                    expiry: write_expiry,
                    correlation: track_written.then_some((id, connection)),
                    bytes: encoded.header,
                    payload: encoded.payload,
                    report: if track_written {
                        WriteReport::Complete
                    } else {
                        WriteReport::None
                    },
                }) {
                    Ok(()) => {
                        if track_written {
                            self.frames.insert(
                                id,
                                ShardPendingFrame {
                                    connection,
                                    kind: "PUBLISH",
                                },
                            );
                        }
                        let _ = effect.into_parts();
                        SubmitResult::Accepted
                    }
                    Err(TrySendError::Full(_)) => {
                        self.outgoing_bindings.remove(&binding_key);
                        refused(effect, SubmitBlock::TransportBackpressure)
                    }
                    Err(TrySendError::Disconnected(_)) => {
                        self.outgoing_bindings.remove(&binding_key);
                        let _ = effect.into_parts();
                        self.terminal_inputs.push_back(Input::FrameResult {
                            effect: id,
                            connection,
                            progress: FrameProgress::Failed,
                        });
                        SubmitResult::Accepted
                    }
                }
            }
            Effect::EmitFrame {
                connection,
                frame,
                pubrel,
                ..
            } => {
                let (connection, frame) = (*connection, *frame);
                let binding = match (*pubrel, frame) {
                    (Some((delivery, claim)), OutboundFrame::Pubrel { packet_id }) => Some((
                        (connection, packet_id),
                        PubackTarget {
                            delivery,
                            claim,
                            frame: id,
                        },
                    )),
                    _ => None,
                };
                let Some(sess) = self.sessions.get(&connection.session).filter(|s| {
                    s.generation == connection.generation && !s.outgoing.is_cancelled()
                }) else {
                    let _ = effect.into_parts();
                    self.terminal_inputs.push_back(Input::FrameResult {
                        effect: id,
                        connection,
                        progress: FrameProgress::Failed,
                    });
                    return SubmitResult::Accepted;
                };
                let Ok(bytes) = codec::encode_frame_bytes(sess.version, frame) else {
                    let _ = effect.into_parts();
                    self.terminal_inputs.push_back(Input::FrameResult {
                        effect: id,
                        connection,
                        progress: FrameProgress::Failed,
                    });
                    return SubmitResult::Accepted;
                };
                if bytes.len() > sess.max_packet as usize {
                    self.close_notices.push(ShardToCoord::CloseConnection {
                        generation: connection.generation,
                        code: "outgoing_packet_limit",
                        disconnect_reason: None,
                    });
                    let _ = effect.into_parts();
                    self.terminal_inputs.push_back(Input::FrameResult {
                        effect: id,
                        connection,
                        progress: FrameProgress::Failed,
                    });
                    return SubmitResult::Accepted;
                }
                if let Some((key, target)) = binding {
                    self.outgoing_bindings.insert(key, target);
                }
                match sess.outgoing.try_send_outgoing(Write {
                    expiry: None,
                    correlation: Some((id, connection)),
                    bytes,
                    payload: None,
                    report: WriteReport::Complete,
                }) {
                    Ok(()) => {
                        self.frames.insert(
                            id,
                            ShardPendingFrame {
                                connection,
                                kind: "PUBREL",
                            },
                        );
                        let _ = effect.into_parts();
                        SubmitResult::Accepted
                    }
                    Err(TrySendError::Full(_)) => {
                        if let Some((key, _)) = binding {
                            self.outgoing_bindings.remove(&key);
                        }
                        refused(effect, SubmitBlock::TransportBackpressure)
                    }
                    Err(TrySendError::Disconnected(_)) => {
                        if let Some((key, _)) = binding {
                            self.outgoing_bindings.remove(&key);
                        }
                        let _ = effect.into_parts();
                        self.terminal_inputs.push_back(Input::FrameResult {
                            effect: id,
                            connection,
                            progress: FrameProgress::Failed,
                        });
                        SubmitResult::Accepted
                    }
                }
            }
            Effect::EmitQos0 {
                expiry,
                properties,
                connection,
                cause,
                retain,
                topic,
                payload,
                ..
            } => {
                let connection = *connection;
                let result = if let Some(sess) = self
                    .sessions
                    .get(&connection.session)
                    .filter(|s| s.generation == connection.generation && !s.outgoing.is_cancelled())
                {
                    // A previously closed transport can still own pending
                    // write receipts. Retain the effect while that shared
                    // budget drains; only socket pressure is a QoS 0 drop.
                    if !sess.outgoing.has_write_receipt_credit() {
                        return refused(effect, SubmitBlock::QueueFull);
                    }
                    let version = sess.version;
                    let expiry_val = expiry.as_deref().copied();
                    match codec::encode_qos0_segments(
                        version,
                        topic,
                        payload,
                        *retain
                            && (version == Version::V5 || *cause == DeliveryCause::RetainedReplay),
                        expiry_val.map(|v| v.interval),
                        properties.as_deref(),
                    ) {
                        Ok(encoded) if encoded.total_len <= sess.max_packet as usize => {
                            let write_expiry = expiry_val.map(|expiry| {
                                Box::new(WriteExpiry {
                                    expiry,
                                    value_offset: encoded.expiry_value_offset,
                                    started: false,
                                })
                            });
                            match sess.outgoing.try_send_outgoing(Write {
                                expiry: write_expiry,
                                correlation: Some((id, connection)),
                                bytes: encoded.header,
                                payload: encoded.payload,
                                report: WriteReport::Complete,
                            }) {
                                Ok(()) => Ok(()),
                                Err(TrySendError::Full(_))
                                    if *cause == DeliveryCause::RetainedReplay =>
                                {
                                    return refused(effect, SubmitBlock::TransportBackpressure);
                                }
                                Err(TrySendError::Full(_)) => Err(Qos0DropReason::OutputSaturated),
                                Err(TrySendError::Disconnected(_)) => {
                                    Err(Qos0DropReason::ConnectionClosed)
                                }
                            }
                        }
                        _ => Err(Qos0DropReason::PacketTooLarge),
                    }
                } else {
                    Err(Qos0DropReason::ConnectionClosed)
                };
                match result {
                    Ok(()) => {
                        self.frames.insert(
                            id,
                            ShardPendingFrame {
                                connection,
                                kind: "PUBLISH0",
                            },
                        );
                    }
                    Err(reason) => {
                        self.terminal_inputs.push_back(Input::Qos0EmitRefused {
                            effect: id,
                            connection,
                            reason,
                        });
                    }
                }
                let _ = effect.into_parts();
                SubmitResult::Accepted
            }
            Effect::CloseConnection { connection, reason } => {
                let (code, disconnect_reason) = match reason {
                    CloseReason::ProtocolError => ("protocol_error", None),
                    CloseReason::SessionTakenOver => ("session_taken_over", Some(0x8e)),
                    CloseReason::OutcomeUnknown => ("outcome_unknown", None),
                    CloseReason::CapacityExceeded => ("capacity_exceeded", Some(0x97)),
                };
                self.close_notices.push(ShardToCoord::CloseConnection {
                    generation: *connection,
                    code,
                    disconnect_reason,
                });
                let _ = effect.into_parts();
                SubmitResult::Accepted
            }
            Effect::Qos0Dropped {
                origin,
                target,
                reason,
            } => {
                let (origin, target, reason) = (*origin, *target, *reason);
                let _ = self.events.emit_with("qos0_dropped", || {
                    json!({
                        "details": {
                            "target": target.map(|t| t.get().to_string()).unwrap_or_default(),
                            "reason": reason.code(),
                            "origin_kind": match origin {
                                Qos0Origin::Volatile { .. } => "volatile",
                                Qos0Origin::Consumed { .. } => "consumed",
                            }
                        }
                    })
                });
                let _ = effect.into_parts();
                SubmitResult::Accepted
            }
            Effect::DeliveryDiscarded {
                delivery,
                target,
                operation: finished,
                reason,
            } => {
                let (delivery, target, finished, reason) = (*delivery, *target, *finished, *reason);
                let _ = self.events.emit_with("delivery_discarded", || {
                    json!({
                        "operation": crate::events::operation(finished),
                        "command_kind": "Finish",
                        "details": {
                            "delivery": format!(
                                "{}/{}/{}/{}/{}",
                                delivery.publication.incarnation().get(),
                                delivery.publication.epoch().get(),
                                delivery.publication.owner().get(),
                                delivery.publication.sequence().get(),
                                delivery.index
                            ),
                            "target": target.get().to_string(),
                            "reason": reason.code(),
                        }
                    })
                });
                let _ = effect.into_parts();
                SubmitResult::Accepted
            }
            Effect::SessionChanged { .. }
            | Effect::SessionRefused { .. }
            | Effect::LoadWills { .. } => {
                let _ = effect.into_parts();
                SubmitResult::Accepted
            }
        }
    }
}

#[cfg(test)]
mod fence_tests;
#[cfg(test)]
mod notice_tests;
