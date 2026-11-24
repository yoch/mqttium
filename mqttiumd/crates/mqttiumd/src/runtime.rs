//! One bounded owner reactor, independent socket tasks and a single SQLite writer.
use crate::session_time::{SessionClock, logical_now};
use crate::{
    codec::{self, Bounds, Packet, Version},
    events::{Events, operation},
    net,
    receiving::Terminal,
    security::{self, Security, TlsFiles},
    writer::{
        Boot, ControlRequest, ControlResponse, DirectWriter, Failure, Hooks, Request, Response,
        Session, StoreHandle, Writer,
    },
};
use mqttiumd_contract::capacity::{BoundedVec, PayloadBytes, Resource, WakeCondition, WorkUnits};
use mqttiumd_contract::command::{DeliveryCause, Recipe};
use mqttiumd_contract::effects::{
    CloseReason, Effect, EffectSink, FrameProgress, Input, OwnedEffect, OwnerAuthority,
    RecoveredFact, SubmitBlock, SubmitResult,
};
use mqttiumd_contract::flat_map::{FlatMap, FlatSet, ScopedFlatMap};
use mqttiumd_contract::ids::{
    AccountId, ConnectionGeneration, ConnectionKey, DeadlineToken, EffectId, LogicalDuration,
    LogicalOwnerId, LogicalTime, OperationId, OutboundPacketId, PublicationId, SessionId,
};
use mqttiumd_contract::message_expiry::{MessageExpiry, MessageTime};
use mqttiumd_contract::outcome::{CommitFacts, StoreOutcome, StoreReply};
use mqttiumd_contract::packet::{InboundPacket, OutboundFrame, Qos2Ack};
use mqttiumd_contract::pubsub::{PubackTarget, Qos0DropReason, Qos0Origin};
use mqttiumd_contract::receipts::{
    ClosedReceiptCollection, ClosedReceiptReply, ReceiptCollectionReply,
};
use mqttiumd_contract::session::{
    SessionAction, SessionChange, SessionDetachment, SessionEndReason, SessionExpiry, SessionGuard,
};
use mqttiumd_contract::step::{StepOutcome, TurnGrant};
#[cfg(feature = "test-hooks")]
use mqttiumd_contract::{ids::DeliveryId, pubsub::WorkItem};
use mqttiumd_core::owner::{Owner, OwnerConfig};
use mqttiumd_store_api::{ReadError, ReadPhase, SessionClockAnchor, SessionClockReading};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::net::{SocketAddr, TcpListener};
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc::{TryRecvError, TrySendError},
};
use std::thread;
use std::time::{Duration, Instant};

/// The connection cap of the unit tests and of the frozen bounded profile.
#[cfg(test)]
const CONNECTIONS: usize = Limits::BOUNDED.connections;
const IN_FLIGHT: usize = 16;
const TIMERS: usize = 512;
// One FIFO with reserved capacity for accepted writer requests and due deadlines.
const WRITER_INPUTS: usize = crate::writer::QUEUE;
const DEADLINE_INPUTS: usize = 4;
const ORDINARY_INPUTS: usize = 92;
const OUTGOING_BINDINGS: usize = 128;
const LIFECYCLE_IN_FLIGHT: usize = 1;
/// Closing and terminal facts applied by one pass over the inputs. With
/// the ordinary inputs of the same pass they stay under the effect box.
const PASS_FACTS: usize = 16;
/// Upper bound of an idle wait: connection deadlines and expiry scans are
/// wall-clock checks without a producer to signal them.
const IDLE_TICK: Duration = Duration::from_millis(20);
/// How long a packet may wait for a credit while the broker returns none
/// and applies no answer of the store, before its connection is closed
/// with `input_capacity`: as long as the budget of a partial packet.
const CAPACITY_WAIT: Duration = Duration::from_secs(5);
/// A busy reactor reports its suppressed observations at this interval.
const COUNTERS_INTERVAL: Duration = Duration::from_secs(1);
const MESSAGE_EXPIRY_RETRIES: u8 = 3;
#[cfg(test)]
const TERMINAL_INPUTS: usize = Limits::BOUNDED.terminal_inputs;
#[cfg(test)]
const CLOSING_INPUTS: usize = Limits::BOUNDED.closing_inputs;

/// What the operator may size. Every queue that grows with the number of
/// connections is derived here, once.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// Transports open at once, the retiring ones included.
    pub connections: usize,
    closing_inputs: usize,
    /// Per transport: the queued frames, one active and eight terminal
    /// receipts.
    frames: usize,
    /// A close can retire every accepted frame while an outbox-sized set of
    /// synchronously refused volatile effects also awaits its exact owner
    /// fact.
    terminal_inputs: usize,
    /// Packets and facts waiting for the core, the writer's and the
    /// deadlines' reserved shares excluded.
    ordinary_inputs: usize,
    /// Effects the core may hold between two dispatches.
    effect_box: usize,
    /// Outgoing packet identifiers in flight: eight per connection.
    outgoing_bindings: usize,
    timers: usize,
    /// Concurrent business commands/work reads allowed in flight to the writer.
    in_flight: usize,
    /// Closing and terminal facts applied by one pass over the inputs.
    pass_facts: usize,
    /// Maximum step transitions applied by one pass over the inputs.
    drain_steps: usize,
}

impl Limits {
    /// A session is durable and the store holds this many: more transports
    /// than sessions could never all be established.
    pub const MAX_CONNECTIONS: usize = crate::writer::SESSIONS;
    /// The profile every reception before record 0044 ran with.
    const BOUNDED_CONNECTIONS: usize = 16;
    pub const BOUNDED: Self = Self::new(Self::BOUNDED_CONNECTIONS);
    pub const DEFAULT: Self = Self::new(Self::MAX_CONNECTIONS);

    /// The places of the ordinary lane that business packets leave free:
    /// an eighth of it. Acknowledgements, PINGREQ and DISCONNECT are read
    /// down to the last two places.
    const fn control_reserve(&self) -> usize {
        self.ordinary_inputs / 8
    }

    /// `connections` is between 1 and [`Limits::MAX_CONNECTIONS`]; the
    /// command line checks it.
    /// A value outside that range is brought to its nearest bound: no
    /// limit builds a runtime without room for one connection.
    #[must_use]
    pub const fn new(connections: usize) -> Self {
        let connections = if connections == 0 {
            1
        } else if connections > Self::MAX_CONNECTIONS {
            Self::MAX_CONNECTIONS
        } else {
            connections
        };
        let frames = connections * (net::OUTPUT_SLOTS + 9);
        // The first profile sized these for sixteen connections; they grow
        // with every further sixteen.
        let scale = connections.div_ceil(Self::BOUNDED_CONNECTIONS);
        let effect_box = if 128 * scale > 4_096 {
            4_096
        } else {
            128 * scale
        };
        let in_flight = if IN_FLIGHT * scale > 512 {
            512
        } else {
            IN_FLIGHT * scale
        };
        let pass_facts = if PASS_FACTS * scale > 4_096 {
            4_096
        } else {
            PASS_FACTS * scale
        };
        let drain_steps = if 512 * scale > 32_768 {
            32_768
        } else {
            512 * scale
        };
        Self {
            connections,
            closing_inputs: 2 * connections,
            frames,
            terminal_inputs: 2 * frames + effect_box,
            // Every connection may have its announced window of
            // publications waiting for a credit at once; the lane holds
            // them all beside its ordinary share, so its control reserve
            // is never what keeps a window-respecting client waiting.
            ordinary_inputs: ORDINARY_INPUTS * scale + connections * crate::receiving::MAXIMUM,
            effect_box,
            outgoing_bindings: OUTGOING_BINDINGS * scale,
            timers: TIMERS * scale,
            in_flight,
            pass_facts,
            drain_steps,
        }
    }
}
const BOUNDS: Bounds = Bounds {
    max_packet: codec::MAX_PACKET_BYTES,
    max_client_id: crate::writer::MAX_CLIENT,
    max_topic: crate::writer::MAX_TOPIC,
    max_payload: crate::writer::MAX_PAYLOAD,
};
// The announced Maximum Packet Size follows the payload limit.
const _: () = assert!(codec::ANNOUNCED_PACKET_BYTES as usize == BOUNDS.max_payload + 8);
#[cfg(feature = "test-hooks")]
const MUTANT_EARLY_ACK: bool = false;
#[cfg(feature = "test-hooks")]
const MUTANT_ALLOW_STALE: bool = false;
#[cfg(feature = "test-hooks")]
const MUTANT_EARLY_PUBACK: bool = false;
#[cfg(feature = "test-hooks")]
const MUTANT_STALE_RECEIPT: bool = false;
#[cfg(feature = "test-hooks")]
const MUTANT_EARLY_OUTGOING: bool = false;
#[cfg(feature = "test-hooks")]
const MUTANT_EARLY_FINISH: bool = false;
#[cfg(feature = "test-hooks")]
const MUTANT_NOTIFICATIONS_ONLY: bool = false;
#[cfg(feature = "test-hooks")]
const MUTANT_EARLY_QOS0: bool = false;
#[cfg(feature = "test-hooks")]
const MUTANT_REPLAY_QOS0: bool = false;
#[cfg(feature = "test-hooks")]
const MUTANT_QOS0_ACCOUNTING: bool = false;
#[cfg(feature = "test-hooks")]
const MUTANT_REBIND_OBSERVED_PUBACK: bool = false;

pub struct Config {
    pub db: PathBuf,
    pub require_existing_db: bool,
    pub storage_mode: crate::writer::StorageMode,
    pub listen: SocketAddr,
    pub events: PathBuf,
    pub event_level: crate::events::Level,
    pub limits: Limits,
    pub hooks: Hooks,
    pub tls: Option<TlsFiles>,
}
struct Peer {
    io: net::Connection,
    created: Instant,
    last_packet: Instant,
    connected: bool,
    version: Option<Version>,
    session: Option<SessionId>,
    opened_by: Option<OperationId>,
    will_managed: bool,
    will_disconnect: Option<bool>,
    connect_expiry: SessionExpiry,
    disconnect_expiry: Option<SessionExpiry>,
    close_observed_at: Option<u64>,
    /// The next packet of this connection, taken from its lane and not
    /// yet given to the core: a packet behind a CONNECT that is still
    /// being opened, or a business packet the lane has no place for. The
    /// packets behind it stay with the transport, in order.
    held_packet: Option<(u8, codec::FrameBytes, net::FrameObservation)>,
    /// Since when a packet of this connection waits for a credit that
    /// other work holds. The results of the store return such credits; a
    /// wait during which the broker returned none for [`CAPACITY_WAIT`] is
    /// not pressure any more and ends the connection.
    capacity_wait: Option<Instant>,
    requests: Option<Requests>,
    pending: bool,
    keep_alive: u16,
    max_packet: usize,
    ingress: u64,
    receiving: crate::receiving::Window,
    closing: Option<(&'static str, Instant)>,
    deferred_publish: Option<DeferredPublish>,
    /// The packets read past `deferred_publish`, in their order, with their
    /// bytes counted. The reader goes on behind a publication that waits
    /// for a slot of the window, because the PUBREL that frees one is
    /// behind it in the stream; what is not such a PUBREL waits here.
    behind: VecDeque<(u8, codec::FrameBytes, net::FrameObservation)>,
    behind_bytes: usize,
    blocked_output: Option<EffectId>,
    retire_code: Option<&'static str>,
}
/// The packets kept behind a waiting publication: one more lane of them,
/// and no more bytes than one packet of the largest size.
const BEHIND_PACKETS: usize = net::INPUT_SLOTS;
const BEHIND_BYTES: usize = codec::MAX_PACKET_BYTES;

/// The Packet Identifier of a raw PUBLISH above QoS 0. A packet too short
/// to hold one has none: the codec refuses it when its turn comes.
fn raw_publish_id(first: u8, body: &[u8]) -> Option<u16> {
    if first >> 4 != 3 || first & 0x06 == 0 {
        return None;
    }
    let topic = usize::from(u16::from_be_bytes([*body.first()?, *body.get(1)?]));
    Some(u16::from_be_bytes([
        *body.get(2 + topic)?,
        *body.get(3 + topic)?,
    ]))
}

impl Peer {
    /// Whether this packet goes ahead of the publication that waits for a
    /// slot: only a PUBREL, which completes another exchange and frees its
    /// slot, and commutes with every packet but the PUBLISH of its own
    /// identifier. Everything else keeps its place in the stream.
    fn overtakes_deferred(&self, first: u8, body: &[u8]) -> bool {
        if first >> 4 != 6 || body.len() < 2 {
            return false;
        }
        let id = u16::from_be_bytes([body[0], body[1]]);
        let waiting = self
            .deferred_publish
            .as_ref()
            .and_then(|deferred| match &deferred.packet {
                InboundPacket::Publish { packet_id, .. } => packet_id.map(|id| id.get()),
                _ => None,
            });
        waiting != Some(id)
            && self
                .behind
                .iter()
                .all(|(first, body, _)| raw_publish_id(*first, body) != Some(id))
    }

    /// Keeps a packet behind the waiting publication, or hands it back
    /// when the bound is reached: it then stays the held packet and the
    /// rest stays with the transport.
    fn keep_behind(
        &mut self,
        packet: (u8, codec::FrameBytes, net::FrameObservation),
    ) -> Result<(), (u8, codec::FrameBytes, net::FrameObservation)> {
        let bytes = packet.1.len();
        if self.behind.len() >= BEHIND_PACKETS || self.behind_bytes + bytes > BEHIND_BYTES {
            return Err(packet);
        }
        self.behind_bytes += bytes;
        self.behind.push_back(packet);
        Ok(())
    }

    fn submitted_output(&mut self, effect: EffectId) -> bool {
        if self.blocked_output == Some(effect) {
            self.blocked_output = None;
            true
        } else {
            false
        }
    }

    #[inline]
    fn needs_poll(&self) -> bool {
        self.retire_code.is_some()
            || self.held_packet.is_some()
            || self.deferred_publish.is_some()
            || !self.behind.is_empty()
            || self.requests.is_some()
            || self.blocked_output.is_some()
            || self.closing.is_some()
            || self.pending
            || !self.connected
            || self.capacity_wait.is_some()
    }
}
/// A SUBSCRIBE or UNSUBSCRIBE served as several requests of the core, in
/// order, and acknowledged once (codec `Packet::Requests`). No other packet
/// of its connection is taken while it lasts.
struct Requests {
    packet_id: mqttiumd_contract::ids::InboundPacketId,
    unsubscribe: bool,
    /// The unit requests the core has not been given yet.
    parts: VecDeque<InboundPacket>,
    /// One entry per filter of the request: a code the codec decided, or
    /// `None` for a filter a unit request answers.
    answers: Vec<Option<u8>>,
    /// The codes the unit requests were answered with so far, in order, as
    /// this protocol version writes them.
    served: Vec<u8>,
    /// A unit request is with the core and its answer has not come back.
    awaiting: bool,
}
struct DeferredPublish {
    packet: InboundPacket,
    ingress_order: u64,
    since: Instant,
    /// The window is held by a frame whose write is not yet reported: a
    /// short, bounded wait. Otherwise the publication waits for the
    /// acknowledgements of a 3.1.1 client's earlier ones.
    wire: bool,
}
struct PendingFrame {
    connection: ConnectionKey,
    kind: &'static str,
    packet_id: Option<u16>,
    receive_serial: Option<u64>,
    terminal: Option<Terminal>,
    wire_released: bool,
    was_retained: bool,
}
struct PendingWillConnect {
    connect: codec::Connect,
    previous: Option<ConnectionGeneration>,
    awaiting: bool,
    reported_pending: bool,
    retry_at: Instant,
}
struct PendingSession {
    client_id: mqttiumd_contract::packet::SharedBytes,
    connection: ConnectionGeneration,
    operation: Option<OperationId>,
    close_intent: Option<CloseIntent>,
}
#[derive(Clone, Copy)]
struct CloseIntent {
    expiry: SessionExpiry,
    observed_at: u64,
    explicit: bool,
    will: Option<bool>,
}
struct ExpiryTimer {
    guard: SessionGuard,
    detached: SessionDetachment,
    queued: bool,
}
#[derive(Clone)]
struct AppliedSessionChange {
    operation: OperationId,
    connection: Option<ConnectionGeneration>,
    client_id: mqttiumd_contract::packet::SharedBytes,
    change: SessionChange,
}
struct Runtime<'a> {
    /// Counts what a pass achieved: a value taken from a lane, an applied
    /// transition, an effect accepted by an adapter. A pass that leaves it
    /// unchanged has nothing to do until a producer signals `wake`.
    progress: u64,
    wake: crate::wake::Wake,
    limits: Limits,
    security: Option<Security>,
    message_expiry_request: Option<(MessageTime, Option<PublicationId>)>,
    message_expiry_retry: bool,
    message_expiry_retry_attempts: u8,
    message_expiry_observation: Option<MessageTime>,
    message_expiry_after: Option<PublicationId>,
    message_expiry_scan: Instant,
    message_expiry_input: Option<Input>,
    message_last_time: Option<MessageTime>,
    last_message_clock_sync: Instant,
    message_clock_needed: bool,
    clock: Arc<dyn SessionClock>,
    clock_anchor: SessionClockAnchor,
    clock_checkpoint: Option<(SessionClockReading, Instant)>,
    last_clock_checkpoint: Instant,
    checkpoint_degraded: bool,
    expiry_timers: BTreeMap<SessionId, ExpiryTimer>,
    expiry_inputs: VecDeque<Input>,
    last_expiry_inventory: Option<(usize, bool)>,
    owner: Owner,
    _authority: OwnerAuthority,
    writer: StoreHandle<'a>,
    events: Events,
    peers: FlatMap<ConnectionGeneration, Peer>,
    ready_peers: Arc<net::ReadyQueue>,
    ready_raw: Vec<u64>,
    pending_peers: FlatSet<ConnectionGeneration>,
    capacity_waiters: usize,
    retiring: Vec<net::Connection>,
    sessions: BTreeMap<Vec<u8>, Session>,
    session_ids: BTreeSet<SessionId>,
    session_requests: BTreeMap<u64, PendingSession>,
    will_connects: BTreeMap<ConnectionGeneration, PendingWillConnect>,
    will_reads: Option<EffectId>,
    last_will_deferred: Option<(OperationId, mqttiumd_core::owner::WillDeferredReason)>,
    next_session_request: u64,
    generation: ConnectionGeneration,
    peer_cursor: usize,
    peer_keys: Vec<ConnectionGeneration>,
    last_peer_deadline_scan: Instant,
    inputs: VecDeque<Input>,
    control_inputs: VecDeque<Input>,
    closing_inputs: VecDeque<Input>,
    terminal_inputs: VecDeque<Input>,
    terminal_effects: ScopedFlatMap<EffectId, u32>,
    deferred_inputs: VecDeque<Input>,
    deferred_by_conn: FlatMap<ConnectionGeneration, usize>,
    /// A packet waits in `deferred_inputs` for room in the effect box.
    effects_awaited: bool,
    /// How many deferred packets may be tried again, from the oldest,
    /// because what they wait for may be there: an answer of the store, the
    /// terminal receipt of a frame or a closed connection (each can end an
    /// exchange and return its credit), or effects the adapters accepted.
    /// The head of `deferred_inputs` is served ahead of everything else;
    /// when it still cannot be served it keeps its place and the retries
    /// stop, so a wake costs one failed attempt whatever the number of
    /// packets that wait, and nothing behind the head overtakes it.
    deferred_retries: usize,
    /// When the broker last ended an exchange or applied an answer of the
    /// store. A connection that waits for a credit is closed only when
    /// nothing of the kind happened for [`CAPACITY_WAIT`]: a wait behind
    /// other work that progresses is pressure, however long it lasts.
    credit_progress: Instant,
    timers: BTreeMap<DeadlineToken, Instant>,
    inflight: ScopedFlatMap<EffectId, OperationId>,
    lifecycle_inflight: BTreeMap<EffectId, OperationId>,
    last_session_change: Option<OperationId>,
    session_cardinality_dirty: bool,
    last_session_cardinality: Option<[usize; 13]>,
    work_reads: ScopedFlatMap<EffectId, SessionId>,
    pending_work: VecDeque<SessionId>,
    pending_work_set: BTreeSet<SessionId>,
    queued_work_sessions: FlatSet<SessionId>,
    cached_bindings: FlatMap<SessionId, ConnectionGeneration>,
    bindings_dirty: bool,
    stale_frames_dirty: bool,
    timers_dirty: bool,
    receipt_pressure_blocked: bool,
    last_work_scan: Instant,
    last_will_scan: Instant,
    last_maintenance_flush: Instant,
    maintenance_needed: bool,
    #[cfg(feature = "test-hooks")]
    work_cache: BTreeMap<DeliveryId, WorkItem>,
    #[cfg(feature = "test-hooks")]
    deferred_work: VecDeque<Response>,
    #[cfg(feature = "test-hooks")]
    emission_barriers: VecDeque<HeldEmission>,
    #[cfg(feature = "test-hooks")]
    last_consume_result: Option<(EffectId, OperationId, StoreReply)>,
    started: Instant,
    #[cfg(feature = "test-hooks")]
    captured: Option<(ConnectionGeneration, codec::Connect)>,
    #[cfg(feature = "test-hooks")]
    hooks: Hooks,
    #[cfg(feature = "test-hooks")]
    deferred_ack: Option<(EffectId, ConnectionKey, usize, Instant)>,
    #[cfg(feature = "test-hooks")]
    deferred_ack_used: bool,
    frames: ScopedFlatMap<EffectId, PendingFrame>,
    outgoing_bindings: FlatMap<(ConnectionKey, OutboundPacketId), PubackTarget>,
    puback_observation: u64,
    #[cfg(feature = "test-hooks")]
    copied_puback: Option<CopiedPuback>,
    #[cfg(feature = "test-hooks")]
    deferred_outgoing_written: Option<(EffectId, ConnectionKey, usize, Instant)>,
    #[cfg(feature = "test-hooks")]
    copied_outgoing_frame: Option<(EffectId, ConnectionKey, FrameProgress)>,
    #[cfg(feature = "test-hooks")]
    last_finish_result: Option<(EffectId, OperationId, StoreReply)>,
    #[cfg(feature = "test-hooks")]
    copied_session_change: Option<AppliedSessionChange>,
    #[cfg(feature = "test-hooks")]
    copied_session_close: Option<SessionCloseObservation>,
}
fn ordinary_room(inputs: &VecDeque<Input>, reservations: usize, limit: usize) -> usize {
    // Most turns use only a small fraction of the ordinary lane.
    let upper = inputs.len() + reservations;
    if upper + 2 <= limit {
        return limit - upper;
    }
    let occupied = inputs
        .iter()
        .filter(|input| !Runtime::writer_input(input) && !matches!(input, Input::Deadline { .. }))
        .count();
    limit.saturating_sub(occupied + reservations)
}
impl Runtime<'_> {
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

    #[cfg(test)]
    fn expire_messages(&mut self) -> Result<(), Failure> {
        self.expire_messages_at(Instant::now())
    }

    /// One bounded scan request; a completed page rearms the next page without
    /// allocating a historical timer or an operation receipt.
    fn expire_messages_at(&mut self, now_instant: Instant) -> Result<(), Failure> {
        if let Some((now, after)) = self.message_expiry_request {
            if self.message_expiry_retry {
                if self.message_expiry_retry_attempts >= MESSAGE_EXPIRY_RETRIES {
                    self.events
                        .emit(
                            "message_expiry_retry_exhausted",
                            json!({"details":crate::writer::message_expiry_details(now, after)}),
                        )
                        .map_err(event_failure)?;
                    return Err(Failure::plain("message_expiry_retry_exhausted"));
                }
                match self.writer.try_send_control(ControlRequest::MessageExpiry {
                    now,
                    after,
                    limit: 4,
                }) {
                    Ok(()) => {
                        self.message_expiry_retry = false;
                        self.message_expiry_retry_attempts = self
                            .message_expiry_retry_attempts
                            .checked_add(1)
                            .ok_or_else(|| Failure::plain("message_expiry_retry_exhausted"))?;
                        let mut details = crate::writer::message_expiry_details(now, after);
                        details["attempt"] = json!(self.message_expiry_retry_attempts);
                        self.events
                            .emit("message_expiry_retry", json!({"details":details}))
                            .map_err(event_failure)?;
                    }
                    Err(TrySendError::Full(_)) => {}
                    Err(TrySendError::Disconnected(_)) => {
                        return Err(Failure::plain("writer_control_disconnected"));
                    }
                }
            }
            return Ok(());
        }
        if self.message_expiry_input.is_some()
            || (self.message_expiry_after.is_none()
                && now_instant.saturating_duration_since(self.message_expiry_scan)
                    < Duration::from_millis(100))
        {
            return Ok(());
        }
        let after = self.message_expiry_after;
        let now = if after.is_some() {
            self.message_expiry_observation
                .ok_or_else(|| Failure::plain("message_expiry_scan_observation"))?
        } else {
            let now = self.message_now()?;
            if self.message_expiry_observation == Some(now) {
                // Reusing a completed header could give an old duplicated page
                // authority over a later scan while the provider is stopped.
                self.message_expiry_scan = now_instant;
                return Ok(());
            }
            now
        };
        match self.writer.try_send_control(ControlRequest::MessageExpiry {
            now,
            after,
            limit: 4,
        }) {
            Ok(()) => {
                self.message_expiry_request = Some((now, after));
                self.message_expiry_observation = Some(now);
                self.message_expiry_retry_attempts = 0;
            }
            Err(TrySendError::Full(_)) => {}
            Err(TrySendError::Disconnected(_)) => {
                return Err(Failure::plain("writer_control_disconnected"));
            }
        }
        Ok(())
    }
    fn emit_session_clock_anchor(&self) -> Result<(), Failure> {
        let domain: String = self
            .clock_anchor
            .reading
            .domain
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        self.events
            .emit(
                "session_clock_anchor",
                json!({"details":{
                    "domain":domain,"tick":self.clock_anchor.reading.tick.to_string(),
                    "logical":self.clock_anchor.logical.to_string(),
                    "origin_status":self.clock.origin_status()
                }}),
            )
            .map_err(event_failure)
    }
    fn session_now(&self) -> Result<u64, Failure> {
        let reading = self.clock.read().map_err(|e| Failure::plain(e.code()))?;
        logical_now(self.clock_anchor, reading).map_err(|e| Failure::plain(e.code()))
    }

    fn closure_input(
        session: SessionId,
        generation: ConnectionGeneration,
        opened_by: Option<OperationId>,
        intent: CloseIntent,
    ) -> Option<Input> {
        let connection = ConnectionKey {
            session,
            generation,
        };
        if let Some(publish) = intent.will {
            return Some(Input::EndSessionWill {
                connection,
                opened_by,
                expiry: intent.expiry,
                observed_at: intent.observed_at,
                publish,
            });
        }
        match intent.expiry {
            SessionExpiry::OnDisconnect => Some(Input::EndSession {
                connection,
                opened_by,
                reason: if intent.explicit {
                    SessionEndReason::Disconnect
                } else {
                    SessionEndReason::TransportLost
                },
            }),
            SessionExpiry::After(_) => Some(Input::DetachSession {
                connection,
                opened_by,
                expiry: intent.expiry,
                observed_at: intent.observed_at,
            }),
            SessionExpiry::Never if intent.explicit => Some(Input::DetachSession {
                connection,
                opened_by,
                expiry: intent.expiry,
                observed_at: intent.observed_at,
            }),
            SessionExpiry::Never => None,
        }
    }

    /// One keyed wakeup per live detached session, four due inputs per turn.
    /// The periodic work scan and socket controls retain their own budgets.
    fn expire_sessions(&mut self) -> Result<(), Failure> {
        if !self.expiry_timers.is_empty() {
            let now = self.session_now()?;
            let mut budget = 4;
            for timer in self.expiry_timers.values_mut() {
                if budget == 0 {
                    break;
                }
                if !timer.queued
                    && timer
                        .detached
                        .deadline
                        .is_some_and(|deadline| now >= deadline)
                {
                    if self.expiry_inputs.len() >= crate::writer::SESSIONS {
                        return Err(Failure::plain("expiry_input_budget"));
                    }
                    self.expiry_inputs.push_back(Input::ExpireSession {
                        guard: timer.guard,
                        detached: timer.detached,
                        now,
                    });
                    timer.queued = true;
                    budget -= 1;
                }
            }
        }
        if !self.expiry_timers.is_empty()
            && self.last_clock_checkpoint.elapsed() >= Duration::from_secs(30)
        {
            if self.clock_checkpoint.is_none() {
                let reading = self.clock.read().map_err(|e| Failure::plain(e.code()))?;
                match self
                    .writer
                    .try_send_control(ControlRequest::ClockCheckpoint {
                        expected: self.clock_anchor,
                        reading,
                    }) {
                    Ok(()) => self.clock_checkpoint = Some((reading, Instant::now())),
                    Err(TrySendError::Full(_)) => {}
                    Err(TrySendError::Disconnected(_)) => {
                        return Err(Failure::plain("writer_control_disconnected"));
                    }
                }
            }
            if self.last_clock_checkpoint.elapsed() > Duration::from_secs(31)
                && !self.checkpoint_degraded
            {
                self.checkpoint_degraded = true;
                self.events.emit("session_clock_degraded", json!({"details":{"code":"checkpoint_late","age_ms":self.last_clock_checkpoint.elapsed().as_millis().to_string()}})).map_err(event_failure)?;
            }
        }
        let inventory = (self.expiry_timers.len(), self.clock_checkpoint.is_some());
        if self.last_expiry_inventory != Some(inventory) {
            self.events.emit("expiry_inventory", json!({"details":{"finite_timers":inventory.0,"checkpoint_inflight":inventory.1,"queued":self.expiry_inputs.len(),"period_ms":30000}})).map_err(event_failure)?;
            self.last_expiry_inventory = Some(inventory);
        }
        Ok(())
    }

    /// One pass over every source, in the fixed order of the reactor.
    fn turn(&mut self) -> Result<(), Failure> {
        // However many lanes the turn serves, each reactor thread is woken
        // once, when the turn ends.
        let _wakes = net::hold_wakes();
        let now = Instant::now();
        self.writer_results()?;
        self.connections_at(now)?;
        self.discover_work_at(now);
        self.drain_pending_work();
        self.expire_messages_at(now)?;
        self.expire_sessions()?;
        self.due_deadlines_at(now)?;
        #[cfg(feature = "test-hooks")]
        self.controls()?;
        self.drain_inputs()?;
        self.drain_pending_work();
        self.dispatch()?;
        self.writer.maybe_periodic_snapshot(now);
        Ok(())
    }

    /// How long an idle reactor may wait for a signal. A due core deadline
    /// bounds it exactly. The connection deadlines, the expiry scans and the
    /// file-polled test controls have no producer and are bounded by a tick.
    fn idle_wait(&self) -> Duration {
        #[cfg(feature = "test-hooks")]
        if self.hooks.directory.is_some() {
            return Duration::from_millis(1);
        }
        let now = Instant::now();
        self.timers
            .values()
            .map(|due| due.saturating_duration_since(now))
            .min()
            .unwrap_or(IDLE_TICK)
            .min(IDLE_TICK)
    }

    /// Whether another transport may start its handshake now. While the
    /// handshake budget is spent, pending transports wait in the listener's
    /// backlog instead of being closed.
    fn admits_handshake(&self) -> bool {
        self.security.is_none()
            || self
                .peers
                .values()
                .map(|peer| &peer.io)
                .chain(self.retiring.iter())
                .filter(|io| io.is_authenticating())
                .count()
                < security::MAX_UNAUTHENTICATED
    }

    fn accept(&mut self, stream: std::net::TcpStream) -> Result<bool, Failure> {
        // Retiring threads count against the same hard connection budget.
        let close_reserved = self
            .peers
            .len()
            .checked_add(1)
            .and_then(|peers| peers.checked_mul(2))
            .and_then(|future| future.checked_add(self.closing_inputs.len()))
            .is_some_and(|required| required <= self.limits.closing_inputs);
        let code = if self.peers.len() + self.retiring.len() >= self.limits.connections
            || !close_reserved
            || self.session_requests.len() >= self.limits.connections
        {
            Some("connection_limit")
        } else if self.security.is_some()
            && self
                .peers
                .values()
                .map(|peer| &peer.io)
                .chain(self.retiring.iter())
                .filter(|io| io.is_authenticating())
                .count()
                >= security::MAX_UNAUTHENTICATED
        {
            Some("tls_handshake_limit")
        } else {
            None
        };
        if let Some(code) = code {
            let _ = stream.shutdown(std::net::Shutdown::Both);
            self.events
                .emit("connection_refused", json!({"details":{"code":code}}))
                .map_err(event_failure)?;
            return Ok(false);
        }
        self.generation = self
            .generation
            .checked_next()
            .map_err(|_| Failure::plain("generation_exhausted"))?;
        let ready = (self.ready_peers.clone(), self.generation.get());
        let io = match &self.security {
            Some(security) => net::spawn_tls(
                stream,
                security.tls.clone(),
                security.policy.clone(),
                self.clock.clone(),
                self.wake.clone(),
                Some(ready),
                #[cfg(feature = "test-hooks")]
                (self.hooks.clone(), self.events.clone()),
            ),
            None => net::spawn_with_ready(stream, self.clock.clone(), self.wake.clone(), ready),
        }
        .map_err(|_| Failure::plain("socket_spawn"))?;
        let sendbuf = io.send_buffer_bytes();
        self.pending_peers.insert(self.generation);
        self.peers.insert(
            self.generation,
            Peer {
                io,
                created: Instant::now(),
                last_packet: Instant::now(),
                connected: false,
                version: None,
                session: None,
                opened_by: None,
                will_managed: false,
                will_disconnect: None,
                connect_expiry: SessionExpiry::OnDisconnect,
                disconnect_expiry: None,
                close_observed_at: None,
                held_packet: None,
                capacity_wait: None,
                requests: None,
                pending: false,
                keep_alive: 0,
                max_packet: 268435455,
                ingress: 0,
                receiving: crate::receiving::Window::default(),
                closing: None,
                deferred_publish: None,
                behind: VecDeque::new(),
                behind_bytes: 0,
                blocked_output: None,
                retire_code: None,
            },
        );
        self.events.emit("connection_opened", json!({"connection":self.generation.get().to_string(),"details":{"send_buffer_bytes":sendbuf}})).map_err(event_failure)?;
        Ok(true)
    }
    fn queue(&mut self, input: Input) -> Result<(), Failure> {
        if matches!(
            input,
            Input::WorkPage { .. }
                | Input::MessagesExpired { .. }
                | Input::Recovery { .. }
                | Input::Packet {
                    packet: InboundPacket::Subscribe { .. },
                    ..
                }
        ) {
            self.message_clock_needed = true;
        }
        if matches!(input, Input::ExpireSession { .. }) {
            if !self.expiry_inputs.iter().any(|old| old == &input) {
                if self.expiry_inputs.len() >= crate::writer::SESSIONS {
                    return Err(Failure::plain("expiry_input_budget"));
                }
                self.expiry_inputs.push_back(input);
            }
            return Ok(());
        }
        if Self::terminal_input(&input) {
            let eid = Self::terminal_effect(&input);
            if eid.is_some_and(|id| self.terminal_effects.contains_key(&id))
                && self.terminal_inputs.iter().any(|queued| queued == &input)
            {
                return Ok(());
            }
            if self.terminal_inputs.len() >= self.limits.terminal_inputs {
                return Err(Failure::plain("terminal_input_budget"));
            }
            if let Some(id) = eid {
                *self.terminal_effects.get_or_insert_default(id) += 1;
            }
            self.terminal_inputs.push_back(input);
            self.session_cardinality_dirty = true;
            return Ok(());
        }
        if Self::closing_input(&input) {
            // At most one close and one explicit end per configured transport.
            // Retried facts remain here while another lifecycle command runs.
            if self.closing_inputs.iter().any(|queued| queued == &input) {
                return Ok(());
            }
            if self.closing_inputs.len() >= self.limits.closing_inputs {
                return Err(Failure::plain("closing_input_budget"));
            }
            self.closing_inputs.push_back(input);
            self.session_cardinality_dirty = true;
            return Ok(());
        }
        if self.maintenance_input(&input) {
            if self.control_inputs.len() >= crate::writer::CONTROL_QUEUE {
                return Err(Failure::plain("core_control_input_budget"));
            }
            self.control_inputs.push_back(input);
            return Ok(());
        }
        if Self::writer_input(&input) {
            if self.inputs.len() >= WRITER_INPUTS && self.writer_input_count() >= WRITER_INPUTS {
                return Err(Failure::plain("writer_input_budget"));
            }
        } else if matches!(input, Input::Deadline { .. }) {
            if self.inputs.len() >= DEADLINE_INPUTS
                && self.ordinary_deadline_count() >= DEADLINE_INPUTS
            {
                return Err(Failure::plain("deadline_input_budget"));
            }
        } else if self.ordinary_room() == 0 {
            return Err(Failure::plain("core_input_budget"));
        }
        self.inputs.push_back(input);
        Ok(())
    }
    /// Whether a business packet of this connection must wait with its
    /// transport. The last places of the ordinary lane are kept for the
    /// packets that release capacity, and a connection has at most its
    /// announced window of business packets waiting in the core: beyond
    /// it the backlog stays in its socket, where it slows its peer, and
    /// cannot fill the lane (record 0046). Within the window the
    /// publications of a client that respects Receive Maximum are all
    /// taken, so the PUBREL or PUBACK it sends after them is never held
    /// behind one of them.
    fn business_waits(&self, generation: ConnectionGeneration) -> bool {
        self.ordinary_room() < self.limits.control_reserve()
            || self.deferred_by_conn.get(&generation).copied().unwrap_or(0)
                >= crate::receiving::MAXIMUM
    }

    fn note_deferred_push(&mut self, input: &Input) {
        if let Some(conn) = business_connection(input) {
            *self.deferred_by_conn.get_or_insert_default(conn) += 1;
        }
    }

    fn note_deferred_pop(&mut self, input: &Input) {
        if let Some(conn) = business_connection(input)
            && let Some(count) = self.deferred_by_conn.get_mut(&conn)
        {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.deferred_by_conn.remove(&conn);
            }
        }
    }

    fn push_deferred_back(&mut self, input: Input) {
        self.note_deferred_push(&input);
        self.deferred_inputs.push_back(input);
    }

    fn push_deferred_front(&mut self, input: Input) {
        self.note_deferred_push(&input);
        self.deferred_inputs.push_front(input);
    }

    fn pop_deferred_front(&mut self) -> Option<Input> {
        let popped = self.deferred_inputs.pop_front()?;
        self.note_deferred_pop(&popped);
        Some(popped)
    }

    fn retain_deferred(&mut self, mut keep: impl FnMut(&Input) -> bool) {
        let counts = &mut self.deferred_by_conn;
        self.deferred_inputs.retain(|input| {
            let kept = keep(input);
            if !kept
                && let Some(conn) = business_connection(input)
                && let Some(count) = counts.get_mut(&conn)
            {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    counts.remove(&conn);
                }
            }
            kept
        });
    }
    /// Advances a SUBSCRIBE or UNSUBSCRIBE served as several requests of
    /// the core: gives the core the next unit request once the previous one
    /// is answered, then sends the one acknowledgement of the whole. What
    /// cannot be done now (no room in the lane, a full output lane) is done
    /// on a later turn.
    fn serve_requests(&mut self, generation: ConnectionGeneration) -> Result<(), Failure> {
        let room = self.ordinary_room();
        let Some(p) = self.peers.get_mut(&generation) else {
            return Ok(());
        };
        let Some(requests) = p.requests.as_mut() else {
            return Ok(());
        };
        if requests.awaiting || p.closing.is_some() {
            return Ok(());
        }
        if !requests.parts.is_empty() {
            if room < 2 {
                return Ok(());
            }
            let packet = requests.parts.pop_front().expect("checked above");
            requests.awaiting = true;
            p.ingress = p
                .ingress
                .checked_add(1)
                .ok_or_else(|| Failure::plain("ingress_exhausted"))?;
            let ingress_order = p.ingress;
            self.progress = self.progress.wrapping_add(1);
            return self.queue(Input::Packet {
                connection: generation,
                ingress_order,
                packet,
            });
        }
        // Every unit request is answered: one code per filter, in the order
        // of the request.
        let version = p.version.unwrap_or_default();
        let mut served = requests.served.iter().copied();
        let codes: Vec<u8> = requests
            .answers
            .iter()
            .filter_map(|answer| answer.or_else(|| served.next()))
            .collect();
        let silent = requests.unsubscribe && version == codec::Version::V311;
        if (!silent && codes.len() != requests.answers.len()) || served.next().is_some() {
            return Err(Failure::plain("requests_answers_mismatch"));
        }
        let Ok(bytes) =
            codec::encode_filter_ack(version, requests.packet_id, requests.unsubscribe, &codes)
        else {
            return self.close(generation, "failure_without_wire_ack");
        };
        if bytes.len() > p.max_packet {
            return self.close(generation, "outgoing_packet_limit");
        }
        if p.io
            .outgoing
            .try_send(net::Write {
                expiry: None,
                correlation: None,
                bytes: codec::FrameBytes::from(bytes),
                payload: None,
                report: net::WriteReport::None,
            })
            .is_ok()
        {
            p.requests = None;
            self.progress = self.progress.wrapping_add(1);
        }
        Ok(())
    }

    /// A packet the core handed back waits in order. One that was the
    /// retried head goes back to the head, and the retries stop: what is
    /// behind it waits for the same thing or for its turn.
    fn defer(&mut self, packet: Input, retried: bool) {
        if retried {
            self.deferred_retries = 0;
            self.push_deferred_front(packet);
        } else {
            self.push_deferred_back(packet);
        }
    }
    fn ordinary_room(&self) -> usize {
        ordinary_room(
            &self.inputs,
            self.deferred_inputs.len() + self.will_connects.len(),
            self.limits.ordinary_inputs,
        )
    }
    fn writer_input(input: &Input) -> bool {
        matches!(
            input,
            Input::StoreResult { .. } | Input::WorkPage { .. } | Input::WillPage { .. }
        )
    }
    fn writer_input_count(&self) -> usize {
        self.inputs
            .iter()
            .filter(|input| Self::writer_input(input))
            .count()
    }
    fn ordinary_deadline_count(&self) -> usize {
        self.inputs
            .iter()
            .filter(|input| matches!(input, Input::Deadline { .. }))
            .count()
    }
    fn terminal_input(input: &Input) -> bool {
        matches!(
            input,
            Input::FrameResult { .. } | Input::Qos0EmitRefused { .. }
        )
    }
    #[inline]
    fn terminal_effect(input: &Input) -> Option<EffectId> {
        match input {
            Input::FrameResult { effect, .. } | Input::Qos0EmitRefused { effect, .. } => {
                Some(*effect)
            }
            _ => None,
        }
    }
    fn pop_terminal_input(&mut self) -> Option<Input> {
        let popped = self.terminal_inputs.pop_front()?;
        if let Some(id) = Self::terminal_effect(&popped)
            && let Some(count) = self.terminal_effects.get_mut(&id)
        {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.terminal_effects.remove(&id);
            }
        }
        Some(popped)
    }
    fn closing_input(input: &Input) -> bool {
        matches!(
            input,
            Input::EndSessionWill { .. }
                | Input::EndSession { .. }
                | Input::DetachSession { .. }
                | Input::ConnectionClosed { .. }
        )
    }
    fn maintenance_input(&self, input: &Input) -> bool {
        matches!(
            input,
            Input::DiscoverWills | Input::ReceiptsCollected { .. } | Input::MaintainReceipts
        ) || matches!(input, Input::StoreResult { operation, .. } if self.owner.lifecycle_operation_is_live(*operation) || self.owner.will_operation_is_live(*operation))
            || matches!(input,Input::Deadline {token,..} if self.owner.collection_deadline_is_live(*token) || self.owner.lifecycle_dispatch().is_some_and(|(_,deadline)| deadline == *token) || self.owner.will_dispatch().is_some_and(|(_,deadline)| deadline == *token))
    }
    fn close(
        &mut self,
        generation: ConnectionGeneration,
        code: &'static str,
    ) -> Result<(), Failure> {
        let Some(peer) = self.peers.get_mut(&generation) else {
            return Ok(());
        };
        if peer.retire_code.is_some() {
            return Ok(());
        }
        peer.retire_code = Some(code);
        peer.closing.get_or_insert((code, Instant::now()));
        self.pending_peers.insert(generation);
        if self.peers[&generation].close_observed_at.is_none() {
            let observed = self.session_now()?;
            self.peers
                .get_mut(&generation)
                .expect("live retiring peer")
                .close_observed_at = Some(observed);
        }
        self.peers[&generation].io.close();
        let must_preserve_write_facts = self
            .frames
            .values()
            .any(|frame| frame.connection.generation == generation && frame.kind == "PUBLISH")
            || self.terminal_inputs.iter().any(|input| {
                matches!(input,
            Input::FrameResult {connection,..} | Input::Qos0EmitRefused {connection,..}
                if connection.generation == generation)
            });
        if must_preserve_write_facts {
            self.collect_written(generation)?;
            self.drain_inputs()?;
            // An accepted QoS 1 frame retains its authoritative generation
            // until its stopped producer has published all network facts.
            if self
                .frames
                .values()
                .any(|frame| frame.connection.generation == generation)
                || self.terminal_inputs.iter().any(|input| {
                    matches!(input,
                    Input::FrameResult {connection,..} | Input::Qos0EmitRefused {connection,..}
                        if connection.generation == generation)
                })
            {
                return Ok(());
            }
        }
        // A TLS task ends its own transport: close_notify, then the
        // shutdown. Its peer keeps its place until the task has finished,
        // so nothing is bound to another generation while the old socket
        // can still write. The retiring pass of `connections` finishes it.
        if self.peers[&generation].io.is_tls() && !self.peers[&generation].io.is_finished() {
            return Ok(());
        }
        self.finish_close(generation, code)
    }

    fn finish_close(
        &mut self,
        generation: ConnectionGeneration,
        code: &'static str,
    ) -> Result<(), Failure> {
        if let Some(peer) = self.peers.remove(&generation) {
            self.pending_peers.remove(&generation);
            if peer.capacity_wait.is_some() {
                self.capacity_waiters = self.capacity_waiters.saturating_sub(1);
            }
            self.bindings_dirty = true;
            self.stale_frames_dirty = true;
            let observed_monotonic_ns = peer
                .io
                .terminal_observed_ns()
                .or_else(crate::events::monotonic_ns)
                .map(|ns| ns.to_string());
            self.session_cardinality_dirty = true;
            self.will_connects.remove(&generation);
            #[cfg(feature = "test-hooks")]
            if let Some(session) = peer.session
                && self
                    .hooks
                    .directory
                    .as_ref()
                    .is_some_and(|dir| consume_control(&dir.join("capture-next-session-close")))
            {
                let observation = SessionCloseObservation {
                    generation,
                    session,
                    opened_by: peer.opened_by,
                };
                self.events
                    .emit("session_close_copied", observation.fields())
                    .map_err(event_failure)?;
                self.copied_session_close = Some(observation);
            }
            // Freeze the observation before any delayed Open/COMMIT result.
            let observed_at = match peer.close_observed_at {
                Some(value) => value,
                None => self.session_now()?,
            };
            let intent = CloseIntent {
                expiry: peer.disconnect_expiry.unwrap_or(peer.connect_expiry),
                observed_at,
                explicit: peer.disconnect_expiry.is_some(),
                will: peer
                    .will_managed
                    .then_some(peer.will_disconnect.unwrap_or(true)),
            };
            for pending in self
                .session_requests
                .values_mut()
                .filter(|p| p.connection == generation)
            {
                if pending.close_intent.is_none() {
                    pending.close_intent = Some(intent);
                }
            }
            if let Some(session) = peer.session
                && let Some(input) =
                    Self::closure_input(session, generation, peer.opened_by, intent)
            {
                self.queue(input)?;
            }
            self.session_requests.retain(|_, pending| {
                pending.connection != generation || pending.operation.is_some()
            });
            // Accepted writes may have completed or failed before their local
            // receipt was collected. Closure ends each exact attempt, never
            // granting a volatile retry or abandoning its bounded owner row.
            let pending: Vec<_> = self
                .frames
                .iter()
                .filter(|(_, frame)| frame.connection.generation == generation)
                .map(|(id, _)| id)
                .collect();
            for id in pending {
                let frame = self
                    .frames
                    .remove(&id)
                    .ok_or_else(|| Failure::plain("frame_missing_on_close"))?;
                self.retained_write_failed(id, &frame, code)?;
                if frame.kind == "PUBLISH0" {
                    self.queue(Input::Qos0EmitRefused {
                        effect: id,
                        connection: frame.connection,
                        reason: Qos0DropReason::ConnectionClosed,
                    })?;
                } else {
                    self.queue(Input::FrameResult {
                        effect: id,
                        connection: frame.connection,
                        progress: FrameProgress::Failed,
                    })?;
                }
            }
            self.retain_deferred(|input| input_connection(input) != Some(generation));
            self.outgoing_bindings
                .retain(|(connection, _), _| connection.generation != generation);
            peer.io.close();
            self.retiring.push(peer.io);
            let mut fields =
                json!({"connection":generation.get().to_string(),"details":{"code":code}});
            if let Some(ns) = observed_monotonic_ns {
                fields["details"]["observed_monotonic_ns"] = json!(ns);
            }
            self.events
                .emit("connection_closed", fields)
                .map_err(event_failure)?;
            self.queue(Input::ConnectionClosed {
                connection: generation,
            })?;
        }
        Ok(())
    }

    /// Orderly process shutdown closes transports first, then gives exact
    /// zero-session ends and already accepted commands a bounded final turn.
    /// An unresolved command remains uncertain; startup has its durable policy.
    fn shutdown_sessions(&mut self, budget: Duration) -> Result<(), Failure> {
        if self.owner.is_recovering() {
            return Ok(());
        }
        let deadline = Instant::now()
            .checked_add(budget)
            .ok_or_else(|| Failure::plain("shutdown_deadline_overflow"))?;
        let mut generations: Vec<_> = self.peers.keys().copied().collect();
        generations.sort_unstable();
        for generation in generations {
            self.close(generation, "shutdown")?;
        }
        loop {
            self.writer_results()?;
            self.connections()?;
            self.due_deadlines()?;
            self.drain_inputs()?;
            self.dispatch()?;
            let status = self.owner.session_status();
            let complete = !status.lifecycle_pending
                && status.retired_flights == 0
                && self.sessions.values().all(|session| {
                    session.expiry == SessionExpiry::Never
                        || (matches!(session.expiry, SessionExpiry::After(_))
                            && session.detached.is_some())
                })
                && self.inputs.is_empty()
                && self.deferred_inputs.is_empty()
                && self.control_inputs.is_empty()
                && self.clock_checkpoint.is_none()
                && self.expiry_inputs.is_empty()
                && self.closing_inputs.is_empty()
                && self.terminal_inputs.is_empty()
                && self.session_requests.is_empty()
                && self.lifecycle_inflight.is_empty()
                && self.inflight.is_empty()
                && self.work_reads.is_empty()
                && self.frames.is_empty()
                && self.timers.is_empty()
                && self.owner.outbox().unwrap().is_empty();
            if complete {
                self.events.emit("session_shutdown_completed", json!({"details":{"persistent_sessions":self.sessions.len(),"owner_bindings":status.bindings}})).map_err(event_failure)?;
                return Ok(());
            }
            if Instant::now() >= deadline {
                let mut fields = json!({"details":{"code":"pending_or_unknown","phase":"Shutdown","lifecycle_pending":status.lifecycle_pending,"retired_flights":status.retired_flights,"in_flight":self.inflight.len()+self.lifecycle_inflight.len(),"closing_inputs":self.closing_inputs.len()}});
                if let Some((pending, _)) = self.owner.lifecycle_dispatch() {
                    fields["operation"] = operation(pending);
                }
                self.events
                    .emit("shutdown_incomplete", fields)
                    .map_err(event_failure)?;
                return Err(Failure::plain("shutdown_incomplete"));
            }
            thread::sleep(Duration::from_millis(1));
        }
    }

    /// Apply the adapter responsibility before any following network effect
    /// can leave the outbox. The pure owner has already applied this exact
    /// known result; its non-reused operation also fences copied old effects.
    fn session_changed(&mut self, accepted: AppliedSessionChange) -> Result<(), Failure> {
        let AppliedSessionChange {
            operation: changed_by,
            connection,
            client_id,
            change,
        } = accepted;
        if let Some(previous) = self.last_session_change {
            if previous.incarnation() != changed_by.incarnation()
                || previous.epoch() != changed_by.epoch()
                || previous.owner() != changed_by.owner()
            {
                return Err(Failure::plain("lifecycle_origin_mismatch"));
            }
            if changed_by.sequence() <= previous.sequence() {
                self.events.emit("session_change_ignored", json!({"operation":operation(changed_by),"details":{"code":"stale_lifecycle_result"}})).map_err(event_failure)?;
                return Ok(());
            }
        }
        #[cfg(feature = "test-hooks")]
        if self
            .hooks
            .directory
            .as_ref()
            .is_some_and(|dir| consume_control(&dir.join("capture-next-session-change")))
        {
            let copied = AppliedSessionChange {
                operation: changed_by,
                connection,
                client_id: client_id.clone(),
                change,
            };
            self.events
                .emit("session_change_copied", copied.fields())
                .map_err(event_failure)?;
            self.copied_session_change = Some(copied);
        }
        match change {
            SessionChange::Opened {
                session,
                revision,
                session_present,
                expiry,
                retired,
                ..
            } => {
                self.bindings_dirty = true;
                if let Some(old) = retired {
                    self.retire_session(old)?;
                }
                if self.sessions.len() >= crate::writer::SESSIONS
                    && !self.sessions.contains_key(client_id.as_slice())
                {
                    return Err(Failure::plain("session_cache_budget"));
                }
                if let Some(replaced) = self.sessions.insert(
                    client_id.as_slice().to_vec(),
                    Session {
                        id: session,
                        revision,
                        client: client_id.clone(),
                        present: session_present,
                        expiry,
                        opened_by: Some(changed_by),
                        detached: None,
                    },
                ) {
                    self.session_ids.remove(&replaced.id);
                }
                self.session_ids.insert(session);
                if let Some(generation) = connection {
                    let close_intent = self
                        .session_requests
                        .values()
                        .find(|pending| pending.connection == generation)
                        .and_then(|pending| pending.close_intent);
                    self.session_requests
                        .retain(|_, pending| pending.connection != generation);
                    if let Some(peer) = self.peers.get_mut(&generation) {
                        if peer.session.is_some_and(|original| original != session)
                            || peer
                                .opened_by
                                .is_some_and(|original| original != changed_by)
                        {
                            return Err(Failure::plain("transport_session_rebound"));
                        }
                        peer.session = Some(session);
                        peer.opened_by = Some(changed_by);
                        if let Some(intent) = close_intent {
                            peer.will_disconnect = intent.will;
                            peer.disconnect_expiry = Some(intent.expiry);
                            peer.close_observed_at = Some(intent.observed_at);
                        }
                        peer.connected = true;
                        // The ordered CONNACK, not cache installation, releases
                        // the bounded pre-CONNACK ingress gate.
                        peer.pending = true;
                    } else {
                        if let Some(intent) = close_intent.or_else(|| {
                            (expiry == SessionExpiry::OnDisconnect).then_some(CloseIntent {
                                expiry,
                                observed_at: 0,
                                explicit: false,
                                will: None,
                            })
                        }) && let Some(input) =
                            Self::closure_input(session, generation, Some(changed_by), intent)
                        {
                            self.queue(input)?;
                        }
                        self.queue(Input::ConnectionClosed {
                            connection: generation,
                        })?;
                    }
                }
                self.expiry_timers.remove(&session);

                let mut fields = json!({"operation":operation(changed_by),"details":{"session":session.get().to_string(),"session_present":session_present,"expiry":expiry_json(expiry),"source":"lifecycle_commit"}});
                if let Some(generation) = connection {
                    fields["connection"] = json!(generation.get().to_string());
                }
                self.events
                    .emit("session_established", fields)
                    .map_err(event_failure)?;
                self.rearm_ready_work(session, true);
            }
            SessionChange::Closed { session, .. } => self.retire_session(session)?,
            SessionChange::Detached {
                session,
                expiry,
                detached,
            } => {
                let value = self
                    .sessions
                    .values_mut()
                    .find(|s| s.id == session)
                    .ok_or_else(|| Failure::plain("detached_session_missing"))?;
                value.expiry = expiry;
                value.detached = Some(detached);
                if detached.deadline.is_some() {
                    self.expiry_timers.insert(
                        session,
                        ExpiryTimer {
                            guard: SessionGuard {
                                session,
                                opened_by: value.opened_by,
                            },
                            detached,
                            queued: false,
                        },
                    );
                } else {
                    self.expiry_timers.remove(&session);
                }
            }
        }
        self.last_session_change = Some(changed_by);
        let mut details = match change {
            SessionChange::Opened {
                session,
                revision,
                session_present,
                expiry,
                retired,
                route_revision,
                cancelled,
            } => json!({
                "action":"Opened","session":session.get().to_string(),"revision":revision.get().to_string(),
                "session_present":session_present,"expiry":expiry_json(expiry),
                "retired_session":retired.map(|id|id.get().to_string()),"route_revision":route_revision.get().to_string(),
                "cancelled_inbound":cancelled.inbound,"cancelled_deliveries":cancelled.deliveries,"cancelled_routes":cancelled.routes
            }),
            SessionChange::Detached {
                session,
                expiry,
                detached,
            } => {
                let mut details = json!({"action":"Detached","session":session.get().to_string(),"expiry":expiry_json(expiry),"detached_by":operation_text(detached.operation)});
                if let Some(deadline) = detached.deadline {
                    details["deadline"] = json!(deadline.to_string());
                }
                details
            }
            SessionChange::Closed {
                session,
                route_revision,
                cancelled,
            } => json!({
                "action":"Closed","session":session.get().to_string(),"route_revision":route_revision.get().to_string(),
                "cancelled_inbound":cancelled.inbound,"cancelled_deliveries":cancelled.deliveries,"cancelled_routes":cancelled.routes
            }),
        };
        // No ClientID or payload bytes enter bounded lifecycle diagnostics.
        let mut fields = json!({"operation":operation(changed_by),"details":details.take()});
        if let Some(generation) = connection {
            fields["connection"] = json!(generation.get().to_string());
        }
        self.events
            .emit("session_changed", fields)
            .map_err(event_failure)?;
        self.session_cardinality_dirty = true;
        self.emit_session_cardinality()
    }

    fn emit_session_cardinality(&mut self) -> Result<(), Failure> {
        if !self.session_cardinality_dirty {
            return Ok(());
        }
        if !self.events.records("session_cardinality") {
            // Computed for the audit record only.
            self.session_cardinality_dirty = false;
            return Ok(());
        }
        let owner = self.owner.session_status();
        let ready = self
            .inputs
            .iter()
            .chain(self.deferred_inputs.iter())
            .filter(|input| matches!(input, Input::WorkAvailable { .. }))
            .count();
        let values = [
            self.sessions.len(),
            owner.sessions,
            owner.bindings,
            self.work_reads.len(),
            ready,
            self.outgoing_bindings.len(),
            self.session_requests.len()
                + self.lifecycle_inflight.len()
                + self.closing_inputs.len()
                + usize::from(owner.lifecycle_pending),
            owner.closed_generations,
            owner.retired_flights,
            owner.inbound,
            owner.outgoing,
            self.frames.len(),
            self.timers.len(),
        ];
        self.session_cardinality_dirty = false;
        if self.last_session_cardinality == Some(values) {
            return Ok(());
        }
        self.last_session_cardinality = Some(values);
        self.events.emit("session_cardinality", json!({"details":{
            "session_cache":values[0],"owner_sessions":values[1],"owner_bindings":values[2],"work_requests":values[3],"work_ready":values[4],
            "emit_registrations":values[5],"lifecycle_requests":values[6],"closed_generations":values[7],"retired_flights":values[8],
            "owner_inbound":values[9],"owner_outgoing":values[10],"frames":values[11],"timers":values[12]
        }})).map_err(event_failure)
    }

    /// Already collected TCP facts precede the logical close. The transport
    /// itself is already shut down, and no later bytes are admitted this way.
    fn queued_generation_is_current(
        &self,
        generation: ConnectionGeneration,
    ) -> Result<bool, Failure> {
        if self.closing_inputs.iter().any(|input| matches!(input, Input::ConnectionClosed { connection } if *connection == generation)) {
            return Ok(true);
        }
        self.admit_generation(generation)
    }

    fn pop_closing_input(&mut self) -> Option<Input> {
        let input = self.closing_inputs.pop_front()?;
        let generation = match &input {
            Input::ConnectionClosed { connection } => *connection,
            Input::EndSessionWill { connection, .. }
            | Input::EndSession { connection, .. }
            | Input::DetachSession { connection, .. } => connection.generation,
            _ => unreachable!("closing lane classification"),
        };
        if self
            .inputs
            .iter()
            .chain(self.deferred_inputs.iter())
            .any(|older| input_connection(older) == Some(generation))
        {
            self.closing_inputs.push_back(input);
            None
        } else {
            Some(input)
        }
    }

    fn retire_session(&mut self, retired: SessionId) -> Result<(), Failure> {
        self.bindings_dirty = true;
        self.expiry_timers.remove(&retired);
        self.expiry_inputs
            .retain(|i| !matches!(i, Input::ExpireSession {guard,..} if guard.session == retired));
        self.session_cardinality_dirty = true;
        self.sessions.retain(|_, cached| cached.id != retired);
        self.session_ids.remove(&retired);
        if self.pending_work_set.remove(&retired) {
            self.pending_work.retain(|s| *s != retired);
        }
        self.outgoing_bindings
            .retain(|(connection, _), _| connection.session != retired);
        self.inputs.retain(
            |input| !matches!(input, Input::WorkAvailable { session } if *session == retired),
        );
        self.queued_work_sessions.remove(&retired);
        self.retain_deferred(
            |input| !matches!(input, Input::WorkAvailable { session } if *session == retired),
        );
        #[cfg(feature = "test-hooks")]
        self.work_cache.retain(|_, item| item.target != retired);
        // Accepted reads retain their bounded effect correlation until their
        // reply arrives. SessionId is never reused, so that reply can then be
        // disposed without resurrecting work or retaining a tombstone.
        Ok(())
    }
    fn drain_inputs(&mut self) -> Result<(), Failure> {
        // Clock observations support actual work; an empty or blocked ready
        // set must not manufacture owner transitions on every reactor poll.
        if !(self.inputs.is_empty()
            && self.control_inputs.is_empty()
            && self.closing_inputs.is_empty()
            && self.terminal_inputs.is_empty()
            && self.expiry_inputs.is_empty()
            && self.message_expiry_input.is_none())
        {
            let needs_clock = cfg!(any(test, feature = "test-hooks"))
                || self.message_clock_needed
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
                            work_limit: WorkUnits::new(65536)
                        }
                    ),
                    StepOutcome::Applied { .. }
                ) {
                    return Err(Failure::plain("message_clock_not_applied"));
                }
            }
        }
        let mut control_budget = crate::writer::CONTROL_QUEUE;
        // Work per pass, not a capacity: each of these inputs can leave an
        // effect in the core's box, which the dispatch after the step empties.
        let mut closing_budget = self.closing_inputs.len().min(self.limits.pass_facts);
        let mut terminal_budget = self.terminal_inputs.len().min(self.limits.pass_facts);
        let mut expiry_budget = self.expiry_inputs.len().min(4);
        let mut step_budget = (self.inputs.len()
            + self.control_inputs.len()
            + closing_budget
            + terminal_budget
            + expiry_budget
            + usize::from(self.message_expiry_input.is_some())
            + self.deferred_retries)
            .min(self.limits.drain_steps);
        let mut steps_taken = 0usize;
        let mut outbox_len_at_dispatch = self.owner.outbox().map_or(0, |b| b.len());
        let mut dispatch_blocked = false;
        loop {
            let progress_before_pass = self.progress;
            while steps_taken < step_budget {
                steps_taken += 1;
                // A wake serves the oldest packet that waits, ahead of
                // everything else and without moving the others.
                let retried = if self.deferred_retries > 0 {
                    self.deferred_retries -= 1;
                    self.pop_deferred_front()
                } else {
                    None
                };
                let from_retry = retried.is_some();
                let control = if !from_retry && control_budget > 0 {
                    self.control_inputs.pop_front()
                } else {
                    None
                };
                let Some(input) = retried
                    .or(control.inspect(|_| control_budget -= 1))
                    .or_else(|| {
                        if closing_budget > 0 {
                            closing_budget -= 1;
                            self.pop_closing_input()
                        } else {
                            None
                        }
                    })
                    .or_else(|| {
                        if terminal_budget > 0 {
                            terminal_budget -= 1;
                            self.pop_terminal_input()
                        } else {
                            None
                        }
                    })
                    .or_else(|| {
                        if expiry_budget > 0 {
                            expiry_budget -= 1;
                            self.expiry_inputs.pop_front()
                        } else {
                            None
                        }
                    })
                    .or_else(|| self.message_expiry_input.take())
                    .or_else(|| {
                        let popped = self.inputs.pop_front()?;
                        if let Input::WorkAvailable { session } = &popped {
                            self.queued_work_sessions.remove(session);
                        }
                        Some(popped)
                    })
                else {
                    break;
                };
                if !from_retry
                    && !self.deferred_by_conn.is_empty()
                    && let Some(connection) = business_connection(&input)
                    && self.deferred_by_conn.contains_key(&connection)
                {
                    self.push_deferred_back(input);
                    continue;
                }
                let input = match input {
                    Input::ObservedPuback {
                        connection,
                        packet_id,
                        reason,
                        target,
                    } => {
                        if !self.queued_generation_is_current(connection.generation)? {
                            self.events
                                .emit(
                                    "puback_ignored",
                                    puback_fields(
                                        connection,
                                        packet_id,
                                        reason,
                                        target,
                                        None,
                                        "connection_closed",
                                    ),
                                )
                                .map_err(event_failure)?;
                            continue;
                        }
                        #[cfg(feature = "test-hooks")]
                        let target = if MUTANT_REBIND_OBSERVED_PUBACK {
                            self.owner.puback_target(connection, packet_id)
                        } else {
                            target
                        };
                        if target.is_none()
                            || self.owner.puback_target(connection, packet_id) != target
                        {
                            self.events
                                .emit(
                                    "puback_ignored",
                                    puback_fields(
                                        connection,
                                        packet_id,
                                        reason,
                                        target,
                                        None,
                                        "no_current_matching_occurrence",
                                    ),
                                )
                                .map_err(event_failure)?;
                        }
                        Input::ObservedPuback {
                            connection,
                            packet_id,
                            reason,
                            target,
                        }
                    }
                    Input::Packet {
                        packet: InboundPacket::Puback { .. } | InboundPacket::Qos2Ack { .. },
                        ..
                    } => return Err(Failure::plain("uncorrelated_puback_input")),
                    Input::Packet {
                        connection,
                        ingress_order,
                        packet,
                    } => {
                        if !self.queued_generation_is_current(connection)? {
                            continue;
                        }
                        Input::Packet {
                            connection,
                            ingress_order,
                            packet,
                        }
                    }
                    Input::OpenSession {
                        connection,
                        client_id,
                        clean_start,
                        expiry,
                    } => {
                        if !self.peers.contains_key(&connection) {
                            self.session_requests
                                .retain(|_, pending| pending.connection != connection);
                            continue;
                        }
                        Input::OpenSession {
                            connection,
                            client_id,
                            clean_start,
                            expiry,
                        }
                    }
                    Input::OpenSessionWillAt {
                        connection,
                        client_id,
                        clean_start,
                        expiry,
                        will,
                        now: _,
                    } => {
                        if !self.peers.contains_key(&connection) {
                            self.session_requests
                                .retain(|_, pending| pending.connection != connection);
                            continue;
                        }
                        Input::OpenSessionWillAt {
                            connection,
                            client_id,
                            clean_start,
                            expiry,
                            will,
                            now: self.session_now()?,
                        }
                    }
                    Input::OpenSessionAt {
                        connection,
                        client_id,
                        clean_start,
                        expiry,
                        now: _,
                    } => {
                        if !self.peers.contains_key(&connection) {
                            self.session_requests
                                .retain(|_, pending| pending.connection != connection);
                            continue;
                        }
                        let now = self.session_now()?;
                        Input::OpenSessionAt {
                            connection,
                            client_id,
                            clean_start,
                            expiry,
                            now,
                        }
                    }
                    Input::SessionEstablished {
                        connection,
                        session,
                        client_id,
                        revision,
                        session_present,
                    } => Input::SessionEstablished {
                        connection: connection.filter(|g| self.peers.contains_key(g)),
                        session,
                        client_id,
                        revision,
                        session_present,
                    },
                    other => other,
                };
                let finish = match &input {
                    Input::StoreResult {
                        operation,
                        reply:
                            StoreReply::Outcome(StoreOutcome::Committed(
                                CommitFacts::DeliveryFinished { delivery, .. },
                            )),
                        ..
                    } => Some((*operation, *delivery)),
                    _ => None,
                };
                let wakes_business = matches!(
                    input,
                    Input::StoreResult { .. } | Input::ReceiptsCollected { .. }
                );
                let collected = match &input {
                    Input::ReceiptsCollected {
                        effect,
                        collection,
                        reply:
                            ReceiptCollectionReply::Applied {
                                closure,
                                removed,
                                retained,
                                more,
                            },
                    } if self.owner.collection_reply_is_live(*effect, *collection) => {
                        Some((*collection, *closure, *removed, *retained, *more))
                    }
                    _ => None,
                };
                let ready_work = self.work_progress_hint(&input);
                let will_progress = matches!(
                    &input,
                    Input::StoreResult {
                        reply: StoreReply::Outcome(StoreOutcome::Committed(
                            CommitFacts::WillPublished { .. }
                        )),
                        ..
                    }
                ) || matches!(&input, Input::WillPage { next: Some(_), .. })
                    || (matches!(
                        &input,
                        Input::StoreResult {
                            reply: StoreReply::Outcome(StoreOutcome::Committed(
                                CommitFacts::SessionLifecycle(_)
                            )),
                            ..
                        }
                    ) && matches!(
                        self.owner.lifecycle_action(),
                        Some(SessionAction::EndWill { .. })
                    ));

                let opening = match &input {
                    Input::OpenSessionWillAt { connection, .. }
                    | Input::OpenSession { connection, .. }
                    | Input::OpenSessionAt { connection, .. } => Some(*connection),
                    _ => None,
                };
                let lifecycle_result = match &input {
                    Input::StoreResult {
                        operation,
                        reply: StoreReply::Outcome(StoreOutcome::RejectedCertain(_)),
                        ..
                    } if self.owner.lifecycle_operation_is_live(*operation) => Some(*operation),
                    _ => None,
                };
                let control_refusal =
                    lifecycle_result.and_then(|_| match self.owner.lifecycle_action() {
                        Some(SessionAction::Detach { .. }) => Some("session_detach_rejected"),
                        Some(SessionAction::Expire { .. }) => Some("session_expire_rejected"),
                        _ => None,
                    });
                let previous_lifecycle = self.owner.lifecycle_dispatch().map(|(op, _)| op);
                let mut applied = false;
                let business = business_connection(&input);
                // What can end an inbound exchange without an answer of the
                // store: the terminal receipt of a frame, a closed connection.
                let closes = Self::closing_input(&input);
                let returns_credit = closes
                    || matches!(&input, Input::FrameResult { progress, .. }
                    if !matches!(progress, FrameProgress::Started));
                let is_recovery = matches!(&input, Input::Recovery { .. });
                let may_change_bindings = is_recovery
                    || closes
                    || matches!(
                        &input,
                        Input::SessionEstablished { .. }
                            | Input::OpenSession { .. }
                            | Input::OpenSessionAt { .. }
                            | Input::OpenSessionWillAt { .. }
                            | Input::ExpireSession { .. }
                            | Input::Packet {
                                packet: InboundPacket::ConnectResume { .. },
                                ..
                            }
                            | Input::StoreResult {
                                reply: StoreReply::Outcome(StoreOutcome::Committed(
                                    CommitFacts::SessionLifecycle(_)
                                )),
                                ..
                            }
                    );
                let outgoing_binding_prune = if self.outgoing_bindings.is_empty() {
                    OutgoingPrune::None
                } else {
                    match &input {
                        Input::ObservedPuback {
                            connection,
                            packet_id,
                            ..
                        }
                        | Input::ObservedQos2Ack {
                            connection,
                            packet_id,
                            ..
                        } => OutgoingPrune::Key((*connection, *packet_id)),
                        Input::StoreResult {
                            reply:
                                StoreReply::Outcome(StoreOutcome::Committed(
                                    CommitFacts::DeliveryFinished {
                                        target, packet_id, ..
                                    }
                                    | CommitFacts::DeliveryStarted {
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
                        } => match self.owner.connection_of(*target) {
                            Some(generation) => OutgoingPrune::Key((
                                ConnectionKey {
                                    session: *target,
                                    generation,
                                },
                                *packet_id,
                            )),
                            None => OutgoingPrune::Full,
                        },
                        Input::StoreResult {
                            reply:
                                StoreReply::Outcome(StoreOutcome::Committed(
                                    CommitFacts::PublicationAdmitted { .. }
                                    | CommitFacts::RetainedQos0Stored { .. }
                                    | CommitFacts::InboundReleased { .. }
                                    | CommitFacts::SubscriptionsChanged { .. }
                                    | CommitFacts::DeliveryClaimed { .. }
                                    | CommitFacts::DeliveryConsumed { .. }
                                    | CommitFacts::WillPublished { .. },
                                )),
                            ..
                        }
                        | Input::FrameResult {
                            progress: FrameProgress::Written { .. } | FrameProgress::Started,
                            ..
                        }
                        | Input::Packet {
                            packet:
                                InboundPacket::Publish { .. }
                                | InboundPacket::Subscribe { .. }
                                | InboundPacket::Unsubscribe { .. }
                                | InboundPacket::Pingreq,
                            ..
                        }
                        | Input::Continue { .. }
                        | Input::WorkAvailable { .. }
                        | Input::WorkPage { .. }
                        | Input::MessageClock { .. }
                        | Input::MaintainReceipts
                        | Input::ReceiptsCollected { .. }
                        | Input::DiscoverWills
                        | Input::WillPage { .. }
                        | Input::Qos0EmitRefused { .. } => OutgoingPrune::None,
                        _ => OutgoingPrune::Full,
                    }
                };
                let is_receipt_collected = matches!(&input, Input::ReceiptsCollected { .. });
                let may_change_timers = !self.timers.is_empty()
                    && !matches!(
                        &input,
                        Input::Packet {
                            packet: InboundPacket::Publish { .. }
                                | InboundPacket::Subscribe { .. }
                                | InboundPacket::Unsubscribe { .. }
                                | InboundPacket::Pingreq
                                | InboundPacket::Puback { .. }
                                | InboundPacket::Qos2Ack { .. },
                            ..
                        } | Input::ObservedPuback { .. }
                            | Input::ObservedQos2Ack { .. }
                            | Input::FrameResult { .. }
                            | Input::Continue { .. }
                            | Input::WorkAvailable { .. }
                            | Input::WorkPage { .. }
                            | Input::MessageClock { .. }
                            | Input::Qos0EmitRefused { .. }
                    );
                let fast_batch_step = matches!(
                    &input,
                    Input::StoreResult {
                        reply: StoreReply::Outcome(StoreOutcome::Committed(
                            CommitFacts::PublicationAdmitted { .. }
                                | CommitFacts::DeliveryClaimed { .. }
                                | CommitFacts::DeliveryFinished { .. }
                                | CommitFacts::DeliveryStarted { .. }
                                | CommitFacts::DeliveryPubrelReady { .. }
                                | CommitFacts::InboundReleased { .. },
                        )),
                        ..
                    } | Input::WorkAvailable { .. }
                        | Input::WorkPage { .. }
                        | Input::ObservedPuback { .. }
                        | Input::ObservedQos2Ack { .. }
                        | Input::FrameResult {
                            progress: FrameProgress::Written { .. } | FrameProgress::Started,
                            ..
                        }
                        | Input::Packet {
                            packet: InboundPacket::Publish {
                                qos: mqttiumd_contract::packet::Qos::AtLeastOnce
                                    | mqttiumd_contract::packet::Qos::ExactlyOnce,
                                ..
                            } | InboundPacket::Pubrel { .. },
                            ..
                        }
                );
                let work_limit = if is_recovery {
                    WorkUnits::new(u64::MAX)
                } else {
                    WorkUnits::new(65536)
                };
                match self.owner.step(input, TurnGrant { work_limit }) {
                    StepOutcome::Applied { .. } => {
                        applied = true;
                        self.progress = self.progress.wrapping_add(1);
                        if is_receipt_collected {
                            self.receipt_pressure_blocked = false;
                        }
                        if let Some(peer) = business.and_then(|key| self.peers.get_mut(&key))
                            && peer.capacity_wait.take().is_some()
                        {
                            self.capacity_waiters = self.capacity_waiters.saturating_sub(1);
                        }
                        if let Some(connection) = opening
                            && let Some((operation, _)) = self.owner.lifecycle_dispatch()
                            && previous_lifecycle != Some(operation)
                            && let Some(pending) = self
                                .session_requests
                                .values_mut()
                                .find(|pending| pending.connection == connection)
                        {
                            pending.operation = Some(operation);
                        }
                        if wakes_business && self.owner.collection_ready() {
                            self.maintenance_needed = true;
                        }
                        if let Some((collection, closure, removed, retained, more)) = collected {
                            if more
                                || self.owner.receipt_status().is_some_and(|(_, span, holes)| {
                                    span >= mqttiumd_contract::receipts::RECEIPT_COLLECTION_TRIGGER
                                        && span > holes
                                })
                            {
                                self.maintenance_needed = true;
                            }
                            self.events
                                .emit_with("receipt_collection_applied", || {
                                    let mut details = crate::writer::collection_details(collection);
                                    crate::writer::returned_closure_details(&mut details, closure);
                                    details["removed"] = json!(removed);
                                    details["retained"] = json!(retained.to_string());
                                    details["more"] = json!(more);
                                    json!({"command_kind":"ReceiptCollection","details":details})
                                })
                                .map_err(event_failure)?;
                        }
                        if let Some((id, delivery)) = finish {
                            self.events.emit_with("core_finish_observed",||json!({"operation":operation(id),"details":{"delivery":delivery_text(delivery)}})).map_err(event_failure)?;
                        }
                        if wakes_business || returns_credit {
                            if self.capacity_waiters != 0 {
                                self.credit_progress = Instant::now();
                            }
                            if is_receipt_collected || !self.receipt_pressure_blocked {
                                // What the deferred packets wait for may be there
                                // now. They are tried again from the oldest, each
                                // ahead of later bytes of its connection, until one
                                // still cannot be served. Control packets can still
                                // pass while a durable routing mutation remains
                                // unresolved.
                                self.deferred_retries = self.deferred_inputs.len();
                            }
                        }
                    }
                    StepOutcome::NeedCapacity { input, need, .. } => {
                        match input {
                            expiry @ Input::MessagesExpired { .. } => {
                                self.message_expiry_input = Some(expiry);
                            }
                            packet @ Input::Packet { .. }
                                if matches!(
                                    need.resource,
                                    Resource::ResultSlots | Resource::ReceiptSlots
                                ) && need.wake == WakeCondition::ResultDelivered =>
                            {
                                if need.resource == Resource::ReceiptSlots {
                                    let was_blocked = self.receipt_pressure_blocked;
                                    self.receipt_pressure_blocked = true;
                                    self.maintenance_needed = true;
                                    if !was_blocked && !from_retry {
                                        let (span, holes) = self
                                            .owner
                                            .receipt_status()
                                            .map(|(_, s, h)| (s, h))
                                            .unwrap_or((0, 0));
                                        self.events.emit("receipt_window_pressure",json!({"details":{"resource":"ReceiptSlots","code":"receipt_window_business_full","span":span,"unconsumed_holes":holes}})).map_err(event_failure)?;
                                    }
                                }
                                self.defer(packet, from_retry)
                            }
                            // The effect box is emptied by the dispatch that
                            // follows this pass. The packet waits for it in
                            // order, behind nothing of its own connection.
                            //
                            // A deferred packet keeps its place in the lane, and
                            // the lane admits a business packet only above its
                            // control reserve: the packets that wait can never
                            // take the places of those that release what the
                            // box waits for. No count of them closes a
                            // connection (record 0046).
                            packet @ Input::Packet { .. }
                                if need.resource == Resource::EffectSlots
                                    && need.wake == WakeCondition::EffectAccepted =>
                            {
                                self.effects_awaited = true;
                                self.defer(packet, from_retry)
                            }
                            // A credit that an admission or an exchange in
                            // flight holds: the result of the store that ends
                            // it returns the credit and takes the deferred
                            // packets up again. The wait is bounded per
                            // connection (`CAPACITY_WAIT`). Only a business
                            // packet waits: nothing of its connection passes
                            // it, so no order changes.
                            packet @ Input::Packet { .. }
                                if need.wake == WakeCondition::ResourceReleased
                                    && business.is_some() =>
                            {
                                if let Some(key) = business
                                    && let Some(peer) = self.peers.get_mut(&key)
                                {
                                    if peer.capacity_wait.is_none() {
                                        peer.capacity_wait = Some(Instant::now());
                                        self.capacity_waiters += 1;
                                    }
                                    self.pending_peers.insert(key);
                                }
                                self.defer(packet, from_retry)
                            }
                            Input::Packet { connection, .. } => {
                                // No transition applied and no effect was submitted for this packet.
                                // Close its transport rather than reorder its positive acknowledgements.
                                self.events.emit("input_capacity_refused",json!({"connection":connection.get().to_string(),"details":{"accepted_command_rejected":false,"resource":format!("{:?}",need.resource),"wake":format!("{:?}",need.wake)}})).map_err(event_failure)?;
                                self.close(connection, "input_capacity")?;
                            }
                            other if Self::terminal_input(&other) => {
                                if let Some(id) = Self::terminal_effect(&other) {
                                    *self.terminal_effects.get_or_insert_default(id) += 1;
                                }
                                self.terminal_inputs.push_back(other);
                            }
                            other if Self::closing_input(&other) => {
                                self.closing_inputs.push_back(other)
                            }
                            other @ Input::ExpireSession { .. } => {
                                self.expiry_inputs.push_back(other)
                            }
                            other if self.maintenance_input(&other) => {
                                self.control_inputs.push_back(other)
                            }
                            other => {
                                if let Input::WorkAvailable { session } = &other {
                                    self.queued_work_sessions.insert(*session);
                                }
                                self.inputs.push_back(other);
                            }
                        }
                    }
                    StepOutcome::Exhausted { .. } => return Err(Failure::plain("core_exhausted")),
                    StepOutcome::Fatal(fault) => {
                        self.events
                            .emit(
                                "core_fault",
                                json!({"details":{"code":format!("{fault:?}")}}),
                            )
                            .map_err(event_failure)?;
                        return Err(Failure::plain("core_fault"));
                    }
                }
                if applied {
                    self.session_cardinality_dirty = true;
                    if may_change_bindings {
                        self.bindings_dirty = true;
                    }
                    match outgoing_binding_prune {
                        OutgoingPrune::None => {}
                        OutgoingPrune::Key(key) => self.prune_outgoing_binding_key(key),
                        OutgoingPrune::Full => self.prune_outgoing_bindings(),
                    }
                    // A terminal result is consumed only after the exact input was
                    // applied. Defer the full timer sweep to the end of the pass
                    // (or when `ArmDeadline` nears its cap) to avoid O(N) scans per step.
                    if may_change_timers {
                        self.timers_dirty = true;
                    }
                }
                if applied && (returns_credit || wakes_business) {
                    dispatch_blocked = false;
                }
                let inputs_before_dispatch = self.inputs.len();
                let must_dispatch = !applied
                    || !fast_batch_step
                    || cfg!(feature = "test-hooks")
                    || self.owner.outbox().is_some_and(|box_| {
                        box_.free_slots() < 16
                            || (!dispatch_blocked
                                && box_.len().saturating_sub(outbox_len_at_dispatch) >= 32)
                    });
                if must_dispatch {
                    let accepted = self.dispatch_returning_accepted()?;
                    outbox_len_at_dispatch = self.owner.outbox().map_or(0, |b| b.len());
                    dispatch_blocked = accepted == 0 && outbox_len_at_dispatch > 0;
                }
                if applied
                    && let Some(operation) = lifecycle_result
                    && !self.owner.lifecycle_operation_is_live(operation)
                {
                    self.session_requests
                        .retain(|_, pending| pending.operation != Some(operation));
                    // A known no-effect result is not a completed detach/expiry.
                    // Stop observably rather than silently losing the continuation
                    // or manufacturing a second command while its outcome is unknown.
                    if let Some(code) = control_refusal {
                        return Err(Failure::plain(code));
                    }
                }
                if applied
                    && will_progress
                    && !self.owner.will_status().admission_pending
                    && !self
                        .control_inputs
                        .iter()
                        .any(|input| matches!(input, Input::DiscoverWills))
                {
                    self.queue(Input::DiscoverWills)?;
                }
                if applied && let Some((session, initial)) = ready_work {
                    self.rearm_ready_work(session, initial);
                }
                if applied && self.inputs.len() > inputs_before_dispatch {
                    step_budget = step_budget
                        .saturating_add(self.inputs.len() - inputs_before_dispatch)
                        .min(self.limits.drain_steps);
                }
            }
            if self.timers_dirty {
                self.timers_dirty = false;
                self.timers
                    .retain(|token, _| self.owner.deadline_is_live(*token));
            }
            self.dispatch()?;
            if !self.writer.is_direct() || self.progress == progress_before_pass {
                break;
            }
            self.writer_results()?;
            let extra = self.inputs.len()
                + self.control_inputs.len()
                + self.closing_inputs.len().min(self.limits.pass_facts)
                + self.terminal_inputs.len().min(self.limits.pass_facts)
                + self.deferred_retries;
            if extra == 0 || steps_taken >= self.limits.drain_steps {
                break;
            }
            control_budget = crate::writer::CONTROL_QUEUE;
            closing_budget = self.closing_inputs.len().min(self.limits.pass_facts);
            terminal_budget = self.terminal_inputs.len().min(self.limits.pass_facts);
            step_budget = steps_taken
                .saturating_add(extra)
                .min(self.limits.drain_steps);
            outbox_len_at_dispatch = self.owner.outbox().map_or(0, |b| b.len());
            dispatch_blocked = false;
        }
        self.schedule_maintenance();
        let deferred = self.owner.will_status().deferred;
        if deferred != self.last_will_deferred {
            if let Some((id, reason)) = deferred {
                let code = match reason {
                    mqttiumd_core::owner::WillDeferredReason::Fanout => "fanout_capacity",
                    mqttiumd_core::owner::WillDeferredReason::ReceiptCapacity => "receipt_capacity",
                    mqttiumd_core::owner::WillDeferredReason::PayloadLimit => "payload_limit",
                    mqttiumd_core::owner::WillDeferredReason::StoreRefusal => "store_refusal",
                };
                self.events.emit("will_deferred",json!({"operation":operation(id),"details":{"code":code,"durable_intention_preserved":true}})).map_err(event_failure)?;
            }
            self.last_will_deferred = deferred;
        }
        self.emit_session_cardinality()?;
        Ok(())
    }
    fn prune_outgoing_binding_key(&mut self, key: (ConnectionKey, OutboundPacketId)) {
        if let Some(&target) = self.outgoing_bindings.get(&key) {
            let live = self.owner.puback_target(key.0, key.1) == Some(target)
                || self.owner.qos2_ack_target(key.0, key.1, Qos2Ack::Pubrec) == Some(target);
            if !live {
                self.outgoing_bindings.remove(&key);
            }
        }
    }
    fn prune_outgoing_bindings(&mut self) {
        let owner = &self.owner;
        self.outgoing_bindings
            .retain(|(connection, packet_id), target| {
                owner.puback_target(*connection, *packet_id) == Some(*target)
                    || owner.qos2_ack_target(*connection, *packet_id, Qos2Ack::Pubrec)
                        == Some(*target)
            });
    }
    fn schedule_maintenance(&mut self) {
        if !self.maintenance_needed {
            return;
        }
        // Under high-throughput traffic, let receipts accumulate up to one full
        // MAX_COLLECTION_ROWS sweep before issuing a control transaction; under
        // low traffic or idle ticks (20ms), flush promptly at the contract trigger.
        let urgent = cfg!(any(test, feature = "test-hooks"))
            || self.receipt_pressure_blocked
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
    fn due_deadlines(&mut self) -> Result<(), Failure> {
        self.due_deadlines_at(Instant::now())
    }
    fn due_deadlines_at(&mut self, now: Instant) -> Result<(), Failure> {
        if self.timers.is_empty() {
            return Ok(());
        }
        let due: Vec<_> = self
            .timers
            .iter()
            .filter(|(_, due)| **due <= now)
            .map(|(token, _)| *token)
            .collect();
        for token in due {
            let input = Input::Deadline {
                token,
                now: LogicalTime::new(
                    self.started
                        .elapsed()
                        .as_millis()
                        .try_into()
                        .map_err(|_| Failure::plain("clock_exhausted"))?,
                ),
            };
            let room = if self.maintenance_input(&input) {
                self.control_inputs.len() < crate::writer::CONTROL_QUEUE
            } else {
                self.ordinary_deadline_count() < DEADLINE_INPUTS
            };
            if !room {
                continue;
            } // The exact token remains in the bounded timer map.
            self.queue(input)?;
            self.timers.remove(&token);
        }
        Ok(())
    }
    /// The connection of every cached session the owner has attached. One
    /// pass over each side, where a lookup per session was quadratic.
    fn bindings(&self) -> FlatMap<SessionId, ConnectionGeneration> {
        let mut out = FlatMap::new();
        for (session, generation) in self.owner.connections() {
            if self.session_ids.contains(&session) {
                out.insert(session, generation);
            }
        }
        out
    }

    fn dispatch(&mut self) -> Result<(), Failure> {
        let _ = self.dispatch_returning_accepted()?;
        Ok(())
    }

    fn dispatch_returning_accepted(&mut self) -> Result<usize, Failure> {
        #[cfg(not(feature = "test-hooks"))]
        if self.owner.outbox().is_some_and(|box_| box_.is_empty()) {
            return Ok(0);
        }
        if self.timers_dirty && self.timers.len() >= self.limits.timers.saturating_sub(16) {
            self.timers_dirty = false;
            self.timers
                .retain(|token, _| self.owner.deadline_is_live(*token));
        }
        if self.bindings_dirty {
            self.cached_bindings = self.bindings();
            self.bindings_dirty = false;
        }
        let mut sink = Sink {
            writer: &mut self.writer,
            limits: self.limits,
            peers: &mut self.peers,
            events: &self.events,
            inflight: &mut self.inflight,
            lifecycle_inflight: &mut self.lifecycle_inflight,
            lifecycle_operation: self.owner.lifecycle_dispatch().map(|(op, _)| op),
            will_operation: self.owner.will_dispatch().map(|(op, _)| op),
            will_reads: &mut self.will_reads,
            work_reads: &mut self.work_reads,
            bindings: &self.cached_bindings,
            #[cfg(feature = "test-hooks")]
            work_cache: &self.work_cache,
            #[cfg(feature = "test-hooks")]
            hooks: &self.hooks,
            #[cfg(feature = "test-hooks")]
            emission_barriers: &mut self.emission_barriers,
            #[cfg(feature = "test-hooks")]
            releasing_qos0: false,
            timers: &mut self.timers,
            inputs: &mut self.inputs,
            queued_work_sessions: &mut self.queued_work_sessions,
            terminal_inputs: &mut self.terminal_inputs,
            terminal_effects: &mut self.terminal_effects,
            ordinary_reserved: self.deferred_inputs.len() + self.will_connects.len(),
            queued_writer_replies: None,
            frames: &mut self.frames,
            outgoing_bindings: &mut self.outgoing_bindings,
            close: Vec::new(),
            mark_pending_peers: Vec::new(),
            pending_work_targets: Vec::new(),
            direct_skip_next_deadline: false,
            session_change: None,
            error: None,
        };
        #[cfg(feature = "test-hooks")]
        sink.release_qos0();
        let report = self.owner.dispatch(&mut sink);
        let close = std::mem::take(&mut sink.close);
        let mark_pending = std::mem::take(&mut sink.mark_pending_peers);
        let work_targets = std::mem::take(&mut sink.pending_work_targets);
        let changed = sink.session_change.take();
        let error = sink.error.take();
        drop(sink);
        for generation in mark_pending {
            self.pending_peers.insert(generation);
        }
        for target in work_targets {
            self.notify_work(target)?;
        }
        if report.accepted != 0 {
            self.progress = self.progress.wrapping_add(1);
            if std::mem::take(&mut self.effects_awaited) {
                // The packets that found the box full are tried again, the
                // oldest first.
                self.deferred_retries = self.deferred_inputs.len();
            }
        }
        if let Some(error) = error {
            return Err(error);
        }
        let had_close_or_change = changed.is_some() || !close.is_empty();
        for (generation, code) in close {
            // A terminated TLS sender can reject a queued PINGRESP before
            // the reactor reaches the normal DISCONNECT already in its reader
            // lane. The ordered input terminal decides that transport loss.
            if code == "outgoing_queue_full"
                && self.peers.get(&generation).is_some_and(|peer| {
                    peer.io.is_tls()
                        && peer.io.terminal_reason().is_some()
                        && peer.closing.is_none()
                })
            {
                continue;
            }
            self.close(generation, code)?;
        }
        if let Some(changed) = changed {
            self.session_changed(changed)?;
        }
        if had_close_or_change && !self.timers.is_empty() {
            self.timers
                .retain(|token, _| self.owner.deadline_is_live(*token));
        }
        self.session_cardinality_dirty = true;
        self.emit_session_cardinality()?;
        Ok(report.accepted)
    }
    fn apply_control_response(&mut self, response: ControlResponse) -> Result<(), Failure> {
        match response {
            ControlResponse::WillBarrier { connection, result } => {
                let pending = result?;
                let earlier_open = self.will_connects.get(&connection).is_some_and(|waiting| {
                    self.session_requests.values().any(|request| {
                        request.connection < connection
                            && request.client_id == waiting.connect.client_id
                    })
                });
                let attached = self
                    .will_connects
                    .get(&connection)
                    .and_then(|waiting| self.sessions.get(waiting.connect.client_id.as_slice()))
                    .and_then(|session| self.owner.connection_of(session.id))
                    .is_some_and(|old| old != connection && self.peers.contains_key(&old));
                if let Some(waiting) = self.will_connects.get_mut(&connection) {
                    if !waiting.awaiting {
                        return Err(Failure::plain("will_barrier_correlation"));
                    }
                    waiting.awaiting = false;
                    if pending || earlier_open || attached {
                        if !waiting.reported_pending {
                            self.events
                                .emit(
                                    "will_connect_deferred",
                                    json!({
                                        "connection":connection.get().to_string(),
                                        "details":{"authority":"durable_will_barrier"}
                                    }),
                                )
                                .map_err(event_failure)?;
                            waiting.reported_pending = true;
                        }
                        waiting.retry_at = Instant::now() + Duration::from_millis(100);
                    } else {
                        let waiting = self
                            .will_connects
                            .remove(&connection)
                            .expect("checked pending connect");
                        self.begin_session_open(connection, waiting.connect)?;
                    }
                }
            }
            ControlResponse::Collected {
                effect,
                collection,
                result,
            } => {
                let reply = result?;
                if self.owner.collection_reply_is_live(effect, collection) {
                    self.queue(Input::ReceiptsCollected {
                        effect,
                        collection,
                        reply,
                    })?;
                } else {
                    self.events.emit("receipt_collection_result_ignored",json!({"command_kind":"ReceiptCollection","details":crate::writer::collection_details(collection)})).map_err(event_failure)?;
                }
            }
            ControlResponse::Lifecycle {
                effect,
                operation,
                reply,
            } => {
                if self.lifecycle_inflight.get(&effect) == Some(&operation) {
                    self.lifecycle_inflight.remove(&effect);
                    if matches!(
                        reply,
                        StoreReply::Outcome(StoreOutcome::Committed(
                            CommitFacts::WillPublished { .. }
                        ))
                    ) {
                        let targets: Vec<_> =
                            self.sessions.values().map(|session| session.id).collect();
                        for session in targets {
                            self.queue_work(session);
                        }
                    }
                    if self.control_inputs.len() >= crate::writer::CONTROL_QUEUE {
                        return Err(Failure::plain("core_control_input_budget"));
                    }
                    self.control_inputs.push_back(Input::StoreResult {
                        effect,
                        operation,
                        reply,
                    });
                } else {
                    self.events.emit("session_result_ignored", json!({"operation":crate::events::operation(operation),"details":{"effect":effect_text(effect),"code":"stale_lifecycle_result"}})).map_err(event_failure)?;
                }
            }
            ControlResponse::MessageExpiryUnknown { now, after } => {
                if self.message_expiry_request != Some((now, after)) {
                    return self.ignore_message_expiry_result(now, after, "stale_unknown");
                }
                self.message_expiry_retry = true;
            }
            ControlResponse::MessageExpiry { now, after, result } => {
                if self.message_expiry_request != Some((now, after))
                    || self.message_expiry_input.is_some()
                {
                    return self.ignore_message_expiry_result(now, after, "stale_known");
                }
                let sweep = result?;
                // The worker's cloneable reply may share its immutable result with
                // a duplicate. Copy only its bounded absolute removal knowledge.
                if sweep.removed.len() > mqttiumd_store_api::MESSAGE_EXPIRY_SWEEP_DELIVERIES {
                    return Err(Failure::plain("message_expiry_reply_budget"));
                }
                let removed = BoundedVec::from_slice(sweep.removed.as_slice(), sweep.removed.len())
                    .ok_or_else(|| Failure::plain("message_expiry_reply_budget"))?;
                if self.message_expiry_retry_attempts != 0 {
                    let mut details = crate::writer::message_expiry_details(now, after);
                    details["attempt"] = json!(self.message_expiry_retry_attempts);
                    details["removed"] = json!(removed.len());
                    self.events
                        .emit("message_expiry_retry_applied", json!({"details":details}))
                        .map_err(event_failure)?;
                }
                self.message_expiry_after = sweep.next;
                self.message_expiry_scan = Instant::now();
                self.message_expiry_request = None;
                self.message_expiry_retry = false;
                self.message_expiry_retry_attempts = 0;
                if !removed.is_empty() {
                    self.message_expiry_input = Some(Input::MessagesExpired {
                        deliveries: removed,
                    });
                }
            }
            ControlResponse::ClockCheckpoint {
                expected,
                reading,
                result,
            } => {
                if self.clock_checkpoint.map(|(r, _)| r) != Some(reading)
                    || expected != self.clock_anchor
                {
                    return Err(Failure::plain("clock_checkpoint_correlation"));
                }
                self.clock_anchor = result?;
                self.clock_checkpoint = None;
                if self.clock_anchor.reading.domain == reading.domain
                    && self.clock_anchor.reading.tick >= reading.tick
                {
                    self.last_clock_checkpoint = Instant::now();
                    self.checkpoint_degraded = false;
                }
                self.events.emit("session_clock_checkpoint", json!({"details":{"logical":self.clock_anchor.logical.to_string(),"tick":self.clock_anchor.reading.tick.to_string()}})).map_err(event_failure)?;
            }
            ControlResponse::Failed(failure) => return Err(failure),
            ControlResponse::Closed { .. } => {
                return Err(Failure::plain("unexpected_closed_collection"));
            }
        }
        Ok(())
    }

    fn ignore_message_expiry_result(
        &self,
        now: MessageTime,
        after: Option<PublicationId>,
        code: &'static str,
    ) -> Result<(), Failure> {
        let mut details = crate::writer::message_expiry_details(now, after);
        details["code"] = json!(code);
        self.events
            .emit("message_expiry_result_ignored", json!({"details":details}))
            .map_err(event_failure)
    }

    fn writer_results(&mut self) -> Result<(), Failure> {
        // Replies have their own reserved capacity. The owner retains bounded
        // attempt correlation; no adapter history or second retry loop exists.
        for _ in 0..crate::writer::CONTROL_QUEUE.saturating_sub(self.control_inputs.len()) {
            let response = match self.writer.try_recv_control() {
                Ok(reply) => {
                    self.progress = self.progress.wrapping_add(1);
                    reply
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    return Err(Failure::plain("writer_control_disconnected"));
                }
            };
            self.apply_control_response(response)?;
        }
        let writer_budget = if self.inputs.is_empty() {
            WRITER_INPUTS
        } else {
            WRITER_INPUTS.saturating_sub(self.writer_input_count())
        };
        for _ in 0..writer_budget {
            let response = match self.writer.try_recv() {
                Ok(r) => {
                    self.progress = self.progress.wrapping_add(1);
                    r
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    return Err(Failure::plain("writer_disconnected"));
                }
            };
            match response {
                Response::Wills { effect, result } => {
                    if self.will_reads.take() != Some(effect) {
                        return Err(Failure::plain("will_read_correlation"));
                    }
                    let page = result.map_err(|e| Failure::read(e, ReadPhase::Work))?;
                    self.queue(Input::WillPage {
                        effect,
                        items: page.items,
                        next: page.next,
                    })?;
                }
                Response::Store {
                    effect,
                    operation,
                    reply,
                    targets,
                } => {
                    if self.inflight.remove(&effect) != Some(operation) {
                        return Err(Failure::plain("uncorrelated_store_result"));
                    }
                    #[cfg(feature = "test-hooks")]
                    if matches!(
                        reply,
                        StoreReply::Outcome(StoreOutcome::Committed(
                            CommitFacts::DeliveryConsumed { .. }
                        ))
                    ) {
                        self.last_consume_result = Some((effect, operation, reply));
                    }
                    #[cfg(feature = "test-hooks")]
                    if let StoreReply::Outcome(StoreOutcome::Committed(
                        CommitFacts::DeliveryFinished { delivery, .. }
                        | CommitFacts::DeliveryConsumed { delivery, .. },
                    )) = reply
                    {
                        self.work_cache.remove(&delivery);
                    }
                    #[cfg(feature = "test-hooks")]
                    if matches!(
                        reply,
                        StoreReply::Outcome(StoreOutcome::Committed(
                            CommitFacts::DeliveryFinished { .. }
                        ))
                    ) {
                        self.last_finish_result = Some((effect, operation, reply));
                    }
                    self.queue(Input::StoreResult {
                        effect,
                        operation,
                        reply,
                    })?;
                    if matches!(
                        reply,
                        StoreReply::Outcome(StoreOutcome::Committed(
                            CommitFacts::PublicationAdmitted { .. }
                                | CommitFacts::RetainedQos0Stored { .. }
                                | CommitFacts::SubscriptionsChanged { .. }
                        ))
                    ) {
                        for target in targets {
                            self.notify_work(target)?;
                        }
                    }
                }
                Response::Work {
                    effect,
                    session,
                    result,
                } => {
                    #[cfg(feature = "test-hooks")]
                    if result.is_ok()
                        && self
                            .hooks
                            .directory
                            .as_ref()
                            .is_some_and(|d| d.join("hold-work-pages").exists())
                    {
                        if self.deferred_work.len() >= IN_FLIGHT {
                            return Err(Failure::plain("held_work_budget"));
                        }
                        self.deferred_work.push_back(Response::Work {
                            effect,
                            session,
                            result,
                        });
                        self.events.emit("work_page_held",json!({"details":{"session":session.get().to_string(),"effect":effect_text(effect)}})).map_err(event_failure)?;
                        continue;
                    }
                    if self.work_reads.remove(&effect) != Some(session) {
                        return Err(Failure::plain("uncorrelated_work_page"));
                    }
                    let page = result.map_err(|e| Failure::read(e, ReadPhase::Work))?;
                    self.session_cardinality_dirty = true;
                    if !self.session_ids.contains(&session) {
                        self.events.emit("work_page_ignored", json!({"details":{"session":session.get().to_string(),"effect":effect_text(effect),"code":"retired_session"}})).map_err(event_failure)?;
                        continue;
                    }
                    #[cfg(feature = "test-hooks")]
                    for item in page.items.as_slice() {
                        if self.work_cache.len() >= 128
                            && !self.work_cache.contains_key(&item.delivery)
                        {
                            return Err(Failure::plain("work_cache_budget"));
                        }
                        self.work_cache.insert(item.delivery, item.clone());
                    }
                    self.queue(Input::WorkPage {
                        effect,
                        session,
                        items: page.items,
                        next: page.next,
                    })?;
                }
                Response::Session { .. } => {
                    // Product CONNECTs exclusively use the receipted owner path.
                    return Err(Failure::plain("unexpected_admin_session_result"));
                }
                Response::Recovery(_) | Response::Stopped => {
                    return Err(Failure::plain("unexpected_writer_result"));
                }
            }
        }
        Ok(())
    }
    fn notify_work(&mut self, session: SessionId) -> Result<(), Failure> {
        #[cfg(feature = "test-hooks")]
        if self
            .hooks
            .directory
            .as_ref()
            .is_some_and(|d| d.join("drop-work-notifications").exists())
        {
            self.events
                .emit(
                    "work_notification_dropped",
                    json!({"details":{"session":session.get().to_string()}}),
                )
                .map_err(event_failure)?;
            return Ok(());
        }
        self.queue_work(session);
        Ok(())
    }
    /// A hint follows only a relevant input actually applied by the owner.
    /// Eligibility remains a pure-core decision; an applied no-op is not
    /// authority to poll a blocked destination or repeatedly read an empty page.
    fn work_progress_hint(&self, input: &Input) -> Option<(SessionId, bool)> {
        match input {
            Input::WorkPage { session, .. } | Input::WorkAvailable { session } => {
                Some((*session, false))
            }
            Input::SessionEstablished {
                session,
                connection: Some(_),
                ..
            } => Some((*session, true)),
            Input::StoreResult {
                reply:
                    StoreReply::Outcome(StoreOutcome::Committed(
                        CommitFacts::DeliveryClaimed { target, .. }
                        | CommitFacts::DeliveryFinished { target, .. }
                        | CommitFacts::DeliveryConsumed { target, .. }
                        | CommitFacts::DeliveryPubrelReady { target, .. }
                        | CommitFacts::DeliveryStarted { target, .. }
                        | CommitFacts::DeliveryExpired { target, .. },
                    )),
                ..
            } => Some((*target, false)),
            Input::StoreResult {
                operation,
                reply:
                    StoreReply::Outcome(
                        StoreOutcome::Committed(CommitFacts::PublicationAdmitted { .. })
                        | StoreOutcome::RejectedCertain(_),
                    ),
                ..
            } => self
                .owner
                .inline_claim_target(*operation)
                .map(|target| (target, false)),
            Input::StoreResult {
                reply:
                    StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::SubscriptionsChanged {
                        session,
                        ..
                    })),
                ..
            } => Some((*session, false)),
            Input::ObservedPuback { connection, .. }
            | Input::ObservedQos2Ack { connection, .. } => Some((connection.session, false)),
            Input::FrameResult { connection, .. } | Input::Qos0EmitRefused { connection, .. } => {
                Some((connection.session, false))
            }
            Input::Packet {
                packet: InboundPacket::ConnectResume { client_id },
                ..
            } => self
                .sessions
                .get(client_id.as_slice())
                .map(|s| (s.id, true)),
            Input::Packet {
                connection,
                packet: InboundPacket::Puback { .. },
                ..
            } => self
                .owner
                .session_for_connection(*connection)
                .map(|id| (id, false)),
            _ => None,
        }
    }
    fn rearm_ready_work(&mut self, session: SessionId, initial: bool) {
        if self.queued_work_sessions.contains(&session) {
            return;
        }
        let usable = self.owner.connection_of(session).is_some_and(|generation| {
            self.peers
                .get(&generation)
                .is_some_and(|p| p.connected && !p.pending && p.closing.is_none())
        });
        if usable && (initial || self.owner.has_ready_work(session)) {
            self.queue_work_front(session);
        }
    }
    fn queue_work_front(&mut self, session: SessionId) {
        if self.ordinary_room() != 0 {
            if self.queued_work_sessions.insert(session) {
                self.inputs.push_front(Input::WorkAvailable { session });
            }
        } else if self.pending_work.len() < crate::writer::SESSIONS
            && self.pending_work_set.insert(session)
        {
            self.pending_work.push_back(session);
        }
    }
    fn queue_work(&mut self, session: SessionId) {
        if self.ordinary_room() != 0 {
            if self.queued_work_sessions.insert(session) {
                self.inputs.push_back(Input::WorkAvailable { session });
            }
        } else if self.pending_work.len() < crate::writer::SESSIONS
            && self.pending_work_set.insert(session)
        {
            self.pending_work.push_back(session);
        }
    }
    fn drain_pending_work(&mut self) {
        while self.ordinary_room() != 0 {
            let Some(session) = self.pending_work.pop_front() else {
                break;
            };
            self.pending_work_set.remove(&session);
            if self.queued_work_sessions.insert(session) {
                self.inputs.push_back(Input::WorkAvailable { session });
            }
        }
    }
    #[cfg(test)]
    fn discover_work(&mut self) {
        self.discover_work_at(Instant::now());
    }
    fn discover_work_at(&mut self, now: Instant) {
        if now.saturating_duration_since(self.last_will_scan) >= Duration::from_millis(100)
            && self.control_inputs.len() < crate::writer::CONTROL_QUEUE
            && !self
                .control_inputs
                .iter()
                .chain(self.inputs.iter())
                .any(|input| matches!(input, Input::DiscoverWills))
        {
            self.last_will_scan = now;
            self.control_inputs.push_back(Input::DiscoverWills);
        }
        #[cfg(feature = "test-hooks")]
        if MUTANT_NOTIFICATIONS_ONLY {
            return;
        }
        if now.saturating_duration_since(self.last_work_scan) < Duration::from_millis(100) {
            return;
        }
        self.last_work_scan = now;
        let sessions: Vec<_> = self
            .sessions
            .values()
            .filter(|s| self.owner.connection_of(s.id).is_some())
            .map(|s| s.id)
            .collect();
        for session in sessions {
            self.queue_work(session);
        }
    }
    fn connect(
        &mut self,
        generation: ConnectionGeneration,
        connect: codec::Connect,
    ) -> Result<(), Failure> {
        if let Some(peer) = self.peers.get_mut(&generation) {
            if peer.version.is_some() {
                return self.close(generation, "second_connect");
            }
            peer.version = Some(connect.version);
        }
        if let Some(security) = &self.security {
            let principal = self
                .peers
                .get(&generation)
                .and_then(|peer| peer.io.principal());
            if principal.is_none_or(|principal| {
                !security
                    .policy
                    .allows_client(principal, connect.client_id.as_slice())
            }) {
                return self.authorization_refusal(
                    generation,
                    "client_id",
                    Some(connect.max_packet_size as usize),
                );
            }
            if let Some(will) = &connect.will
                && principal.is_none_or(|principal| {
                    !security
                        .policy
                        .allows_publish(principal, will.topic.as_slice())
                })
            {
                return self.authorization_refusal(
                    generation,
                    "will",
                    Some(connect.max_packet_size as usize),
                );
            }
        }
        self.events.emit("connect_validated",json!({"connection":generation.get().to_string(),"details":{"protocol_level":connect.version.level(),"keep_alive":connect.keep_alive,"max_packet_size":connect.max_packet_size,"receive_maximum":connect.receive_maximum,"topic_alias_maximum":connect.topic_alias_maximum,"request_problem_information":connect.request_problem_information,"clean_start":connect.clean_start,"expiry":expiry_json(connect.expiry)}})).map_err(event_failure)?;
        if let Some(p) = self.peers.get_mut(&generation) {
            if p.connected || p.pending {
                return self.close(generation, "second_connect");
            }
            p.keep_alive = connect.keep_alive;
            p.max_packet = connect.max_packet_size as usize;
            p.last_packet = Instant::now();
        }
        #[cfg(feature = "test-hooks")]
        if self.captured.is_none() {
            self.events.emit("input_captured",json!({"connection":generation.get().to_string(),"details":{"packet_kind":"CONNECT"}})).map_err(event_failure)?;
            self.captured = Some((generation, connect.clone()));
        }
        self.queue(Input::ConnectionSettings {
            connection: generation,
            receive_maximum: connect.receive_maximum,
            maximum_packet_size: connect.max_packet_size,
        })?;
        if self.session_requests.len() >= self.limits.connections {
            return self.close(generation, "session_request_limit");
        }
        let request = self
            .next_session_request
            .checked_add(1)
            .ok_or_else(|| Failure::plain("session_request_exhausted"))?;
        self.next_session_request = request;
        self.session_requests.insert(
            request,
            PendingSession {
                client_id: connect.client_id.clone(),
                connection: generation,
                operation: None,
                close_intent: None,
            },
        );
        if let Some(peer) = self.peers.get_mut(&generation) {
            peer.pending = true;
            peer.connect_expiry = connect.expiry;
        }
        if let Some(peer) = self.peers.get_mut(&generation) {
            peer.will_managed = connect.will.is_some();
        }
        let old = self
            .sessions
            .get(connect.client_id.as_slice())
            .and_then(|session| self.owner.connection_of(session.id));
        if let Some(old) = old.filter(|old| *old != generation) {
            self.start_takeover(old)?;
        }
        self.will_connects.insert(
            generation,
            PendingWillConnect {
                connect,
                previous: old.filter(|old| *old != generation),
                awaiting: false,
                reported_pending: false,
                retry_at: Instant::now(),
            },
        );
        self.pump_will_connects()?;
        Ok(())
    }
    fn start_takeover(&mut self, old: ConnectionGeneration) -> Result<(), Failure> {
        // The old transport closes before the new Will/Open can be accepted.
        if let Some(peer) = self.peers.get_mut(&old) {
            if peer.version == Some(Version::V311) {
                return self.close(old, "session_taken_over");
            }
            let bytes = codec::disconnect_reason(0x8e);
            if peer
                .io
                .outgoing
                .try_send(net::Write {
                    expiry: None,
                    correlation: None,
                    bytes: codec::FrameBytes::from(bytes),
                    payload: None,
                    report: net::WriteReport::Complete,
                })
                .is_ok()
            {
                peer.closing = Some(("session_taken_over", Instant::now()));
                self.pending_peers.insert(old);
            } else {
                self.close(old, "session_taken_over")?;
            }
        }
        Ok(())
    }
    fn begin_session_open(
        &mut self,
        generation: ConnectionGeneration,
        connect: codec::Connect,
    ) -> Result<(), Failure> {
        if !self.peers.contains_key(&generation) {
            return Ok(());
        }
        self.queue(Input::OpenSessionWillAt {
            now: self.session_now()?,
            connection: generation,
            client_id: connect.client_id,
            clean_start: connect.clean_start,
            expiry: connect.expiry,
            will: connect.will,
        })
    }
    fn pump_will_connects(&mut self) -> Result<(), Failure> {
        let ready: Vec<_> = self
            .will_connects
            .iter()
            .filter(|(_, pending)| {
                !pending.awaiting
                    && pending.retry_at <= Instant::now()
                    && pending
                        .previous
                        .is_none_or(|old| !self.peers.contains_key(&old))
            })
            .take(4)
            .map(|(id, _)| *id)
            .collect();
        for connection in ready {
            let current = self
                .will_connects
                .get(&connection)
                .and_then(|pending| self.sessions.get(pending.connect.client_id.as_slice()))
                .and_then(|session| self.owner.connection_of(session.id))
                .filter(|old| *old != connection && self.peers.contains_key(old));
            if let Some(old) = current {
                self.will_connects
                    .get_mut(&connection)
                    .expect("pending connect")
                    .previous = Some(old);
                self.start_takeover(old)?;
                continue;
            }
            let pending = self
                .will_connects
                .get_mut(&connection)
                .expect("selected pending connect");
            let request = ControlRequest::WillBarrier {
                connection,
                client: pending.connect.client_id.clone(),
            };
            match self.writer.try_send_control(request) {
                Ok(()) => pending.awaiting = true,
                Err(TrySendError::Full(_)) => break,
                Err(TrySendError::Disconnected(_)) => {
                    return Err(Failure::plain("writer_control_disconnected"));
                }
            }
        }
        Ok(())
    }
    #[cfg(test)]
    fn packet(
        &mut self,
        generation: ConnectionGeneration,
        first: u8,
        body: impl Into<codec::FrameBytes>,
    ) -> Result<(), Failure> {
        let observed = self.clock.read().map(|reading| MessageTime {
            domain: reading.domain,
            tick: reading.tick,
        });
        self.packet_at(generation, first, body.into(), observed)
    }

    fn authorization_refusal(
        &mut self,
        generation: ConnectionGeneration,
        stage: &'static str,
        before_connack: Option<usize>,
    ) -> Result<(), Failure> {
        self.events
            .emit(
                "authorization_refused",
                json!({"connection":generation.get().to_string(),
            "details":{"stage":stage,"reason":0x87}}),
            )
            .map_err(event_failure)?;
        if let Some(peer) = self.peers.get_mut(&generation) {
            let version = peer.version.unwrap_or_default();
            let bytes = if before_connack.is_some() {
                Some(
                    codec::session_refusal_for(version, 0x87)
                        .map_err(|_| Failure::plain("connack_authorization_reason"))?,
                )
            } else if version == Version::V5 {
                Some(codec::disconnect_reason(0x87))
            } else {
                None
            };
            if let Some(bytes) = bytes
                && bytes.len() <= before_connack.unwrap_or(peer.max_packet)
                && peer
                    .io
                    .outgoing
                    .try_send(net::Write {
                        correlation: None,
                        bytes: codec::FrameBytes::from(bytes),
                        payload: None,
                        expiry: None,
                        report: net::WriteReport::Complete,
                    })
                    .is_ok()
            {
                peer.closing = Some(("not_authorized", Instant::now()));
                self.pending_peers.insert(generation);
                return Ok(());
            }
        }
        self.close(generation, "not_authorized")
    }

    /// A fixed header the transport refused: the transport reads no
    /// further packet. An MQTT 5 client that holds its CONNACK is told why
    /// before the close. A refused first header names no protocol version
    /// to answer in, and MQTT 3.1.1 has no packet for it.
    fn header_refused(
        &mut self,
        generation: ConnectionGeneration,
        code: &'static str,
    ) -> Result<(), Failure> {
        let (name, reason) = if code == "packet_too_large" {
            ("PacketTooLarge", 0x95u8)
        } else {
            ("MalformedHeader", 0x81u8)
        };
        self.events
            .emit(
                "packet_refused",
                json!({"connection":generation.get().to_string(),
                    "details":{"code":name,"reason":reason}}),
            )
            .map_err(event_failure)?;
        if let Some(p) = self.peers.get_mut(&generation)
            && p.connected
            && p.version == Some(codec::Version::V5)
            && p.closing.is_none()
        {
            let bytes = codec::disconnect_reason(reason);
            if bytes.len() <= p.max_packet
                && p.io
                    .outgoing
                    .try_send(net::Write {
                        expiry: None,
                        correlation: None,
                        bytes: codec::FrameBytes::from(bytes),
                        payload: None,
                        report: net::WriteReport::Complete,
                    })
                    .is_ok()
            {
                p.closing = Some((code, Instant::now()));
                self.pending_peers.insert(generation);
                return Ok(());
            }
        }
        self.close(generation, code)
    }

    fn packet_at(
        &mut self,
        generation: ConnectionGeneration,
        first: u8,
        body: impl Into<codec::FrameBytes>,
        observed: net::FrameObservation,
    ) -> Result<(), Failure> {
        let body = body.into();
        if !self.admit_generation(generation)? {
            return Ok(());
        }
        if self
            .peers
            .get(&generation)
            .is_some_and(|peer| peer.io.wire_progress.has_pending())
        {
            self.wire_progress(generation)?;
        }
        let version = self.peers.get(&generation).and_then(|peer| peer.version);
        if first >> 4 == 1 && version.is_some() {
            return self.close(generation, "second_connect");
        }
        // The Maximum Packet Size of the CONNACK binds every later packet
        // of an MQTT 5 client, not only a PUBLISH whose payload is too
        // large (MQTT-3.2.2-15). The CONNECT came before any announcement.
        if version == Some(codec::Version::V5)
            && codec::wire_size(body.len()) > codec::ANNOUNCED_PACKET_BYTES as usize
        {
            return self.header_refused(generation, "packet_too_large");
        }
        let mut decoded = match codec::decode_for(version.unwrap_or_default(), first, &body, BOUNDS)
        {
            Ok(v) => v,
            Err(error) => {
                self.events.emit("packet_refused",json!({"connection":generation.get().to_string(),"details":{"code":error.code(),"reason":error.reason_code()}})).map_err(event_failure)?;
                // Refusal output has the same bounded absolute write path as business frames.
                if let Some(p) = self.peers.get_mut(&generation) {
                    let cap = if p.connected {
                        Some(p.max_packet as u32)
                    } else {
                        error.peer_maximum()
                    };
                    let response_version = version
                        .or_else(|| {
                            (first >> 4 == 1)
                                .then(|| codec::connect_response_version(&body))
                                .flatten()
                        })
                        .unwrap_or_default();
                    if let Some(cap) = cap
                        && let Some(bytes) =
                            codec::refusal_for(response_version, error, !p.connected)
                        && bytes.len() <= cap as usize
                        && p.io
                            .outgoing
                            .try_send(net::Write {
                                expiry: None,
                                correlation: None,
                                bytes: codec::FrameBytes::from(bytes),
                                payload: None,
                                report: net::WriteReport::Complete,
                            })
                            .is_ok()
                    {
                        p.closing = Some(("packet_refused", Instant::now()));
                        self.pending_peers.insert(generation);
                        return Ok(());
                    }
                }
                return self.close(generation, "packet_refused");
            }
        };
        if let Some(security) = &self.security {
            let targets: Vec<&mut InboundPacket> = match &mut decoded {
                Packet::Core(packet) | Packet::TimedPublish { packet, .. } => vec![packet],
                // Each unit request is authorized as a request of its own.
                Packet::Requests { parts, .. } => parts.iter_mut().collect(),
                _ => Vec::new(),
            };
            let principal = if targets.is_empty() {
                None
            } else {
                let Some(principal) = self
                    .peers
                    .get(&generation)
                    .and_then(|peer| peer.io.principal())
                else {
                    return self.close(generation, "tls_principal_missing");
                };
                Some(principal)
            };
            let mut filters_denied = false;
            for packet in targets {
                let principal = principal.expect("a principal for every target");
                if let InboundPacket::Subscribe {
                    filters, denied, ..
                } = packet
                {
                    for (index, filter) in filters.as_slice().iter().enumerate() {
                        if !security
                            .policy
                            .allows_filter(principal, filter.filter.as_slice())
                        {
                            *denied |= 1 << index;
                        }
                    }
                    filters_denied |= *denied != 0;
                } else if !security.policy.allows_packet(principal, packet) {
                    let stage = if matches!(packet, InboundPacket::Publish { .. }) {
                        "publish"
                    } else {
                        "unsubscribe"
                    };
                    return self.authorization_refusal(generation, stage, None);
                }
            }
            if filters_denied {
                self.events
                    .emit(
                        "authorization_refused",
                        json!({
                            "connection":generation.get().to_string(),
                            "details":{"stage":"subscribe","reason":0x87}
                        }),
                    )
                    .map_err(event_failure)?;
            }
        }
        let decoded = match decoded {
            Packet::TimedPublish {
                mut packet,
                interval,
            } => {
                // This may be older than the reactor's latest clock reading:
                // channel and pending-CONNECT waits consume message lifetime.
                let observed = observed.map_err(|error| Failure::plain(error.code()))?;
                if observed.domain != self.clock_anchor.reading.domain {
                    return Err(Failure::plain("session_clock_domain_changed"));
                }
                let _ = self.session_now()?;
                let expiry = MessageExpiry { observed, interval };
                expiry
                    .deadline()
                    .map_err(|_| Failure::plain("message_expiry_overflow"))?;
                let InboundPacket::Publish {
                    expiry: saved,
                    topic,
                    qos,
                    retain,
                    ..
                } = &mut packet
                else {
                    return Err(Failure::plain("timed_publish_kind"));
                };
                *saved = Some(expiry);
                self.events.emit("message_expiry_received", json!({"connection":generation.get().to_string(),"details":{
                    "topic":std::str::from_utf8(topic.as_slice()).map_err(|_| Failure::plain("invalid_topic"))?.chars().take(256).collect::<String>(),
                    "qos":qos.to_wire(),"retain":retain,"interval":interval,
                    "authority":"reader_frame_completion",
                    "domain":hex_domain(observed.domain),"tick":observed.tick.to_string()
                }})).map_err(event_failure)?;
                self.message_clock_needed = true;
                Packet::Core(packet)
            }
            other => other,
        };
        let p = self
            .peers
            .get_mut(&generation)
            .ok_or_else(|| Failure::plain("generation_missing"))?;
        if p.keep_alive != 0 && p.io.last_heard().is_none() {
            p.last_packet = Instant::now();
        }
        match decoded {
            Packet::TimedPublish { .. } => Err(Failure::plain("timed_publish_not_anchored")),
            Packet::Connect(connect) => self.connect(generation, connect),
            Packet::Requests {
                packet_id,
                unsubscribe,
                parts,
                answers,
            } => {
                if !p.connected {
                    return self.close(generation, "packet_before_connack");
                }
                if p.requests.is_some() {
                    return Err(Failure::plain("requests_overlap"));
                }
                p.requests = Some(Requests {
                    packet_id,
                    unsubscribe,
                    parts: parts.into(),
                    answers,
                    served: Vec::new(),
                    awaiting: false,
                });
                self.pending_peers.insert(generation);
                self.serve_requests(generation)
            }
            Packet::Disconnect {
                expiry,
                publish_will,
            } => {
                if p.connect_expiry == SessionExpiry::OnDisconnect
                    && expiry.is_some_and(|value| value != SessionExpiry::OnDisconnect)
                {
                    let bytes = codec::disconnect_reason(0x82);
                    self.events.emit("packet_refused", json!({"connection":generation.get().to_string(),"details":{"code":"SessionExpiryIncreaseForbidden","reason":0x82}})).map_err(event_failure)?;
                    if bytes.len() <= p.max_packet
                        && p.io
                            .outgoing
                            .try_send(net::Write {
                                expiry: None,
                                correlation: None,
                                bytes: codec::FrameBytes::from(bytes),
                                payload: None,
                                report: net::WriteReport::Complete,
                            })
                            .is_ok()
                    {
                        p.closing = Some(("packet_refused", Instant::now()));
                        self.pending_peers.insert(generation);
                        return Ok(());
                    }
                    return self.close(generation, "packet_refused");
                }
                let observed_at = self.session_now()?;
                let p = self
                    .peers
                    .get_mut(&generation)
                    .ok_or_else(|| Failure::plain("generation_missing"))?;
                p.will_disconnect = Some(publish_will);
                p.disconnect_expiry = expiry;
                p.close_observed_at = Some(observed_at);
                if let Some(pending) = self
                    .session_requests
                    .values_mut()
                    .find(|pending| pending.connection == generation)
                {
                    pending.close_intent = Some(CloseIntent {
                        expiry: expiry.unwrap_or(p.connect_expiry),
                        observed_at,
                        explicit: expiry.is_some(),
                        will: p.will_managed.then_some(publish_will),
                    });
                }
                // Earlier decoded packets precede this close in the TCP stream.
                // Apply their facts while this generation is authoritative;
                // accepted durable work then outlives its transport.
                self.drain_inputs()?;
                self.close(generation, "client_disconnect")
            }
            Packet::Core(packet) => {
                if !p.connected {
                    return self.close(generation, "packet_before_connack");
                }
                p.ingress = p
                    .ingress
                    .checked_add(1)
                    .ok_or_else(|| Failure::plain("ingress_exhausted"))?;
                let ingress_order = p.ingress;
                #[cfg(feature = "test-hooks")]
                if MUTANT_QOS0_ACCOUNTING
                    && matches!(
                        &packet,
                        InboundPacket::Publish {
                            qos: mqttiumd_contract::packet::Qos::AtMostOnce,
                            ..
                        }
                    )
                {
                    let charged = p
                        .receiving
                        .publish_qos(
                            65535,
                            ingress_order,
                            mqttiumd_contract::packet::Qos::AtLeastOnce,
                        )
                        .is_ok();
                    self.events.emit("qos0_receive_credit",json!({"connection":generation.get().to_string(),"details":{"charged":charged,"wrong_qos":1}})).map_err(event_failure)?;
                }
                if let InboundPacket::Publish {
                    packet_id: Some(id),
                    qos,
                    dup,
                    ..
                } = &packet
                {
                    let pid = id.get();
                    let wire_qos = qos.to_wire();
                    let dup = *dup;
                    self.events.emit_with("publish_validated", || json!({"connection":generation.get().to_string(),"details":{"packet_id":pid,"qos":wire_qos,"dup":dup,"publish_order":ingress_order.to_string()}})).map_err(event_failure)?;
                }
                if let InboundPacket::Publish {
                    packet_id: Some(id),
                    qos,
                    ..
                } = &packet
                    && p.receiving
                        .publish_qos(id.get(), ingress_order, *qos)
                        .is_err()
                {
                    let id_raw = id.get();
                    let qos_val = *qos;
                    if p.io.written.has_pending() {
                        if let Some(code) = self.collect_written(generation)? {
                            return self.close(generation, code);
                        }
                        if self.peers.get_mut(&generation).is_some_and(|peer| {
                            peer.receiving
                                .publish_qos(id_raw, ingress_order, qos_val)
                                .is_ok()
                        }) {
                            return self.queue(Input::Packet {
                                connection: generation,
                                ingress_order,
                                packet,
                            });
                        }
                    }
                    let Some(p) = self.peers.get_mut(&generation) else {
                        return Ok(());
                    };
                    let pending_terminal = self.frames.values().any(|frame| {
                        frame.connection.generation == generation
                            && frame
                                .packet_id
                                .zip(frame.terminal)
                                .is_some_and(|(id, terminal)| {
                                    p.receiving.can_complete_frame(
                                        id,
                                        frame.receive_serial,
                                        terminal,
                                    )
                                })
                    });
                    // MQTT 3.1.1 has no Receive Maximum to announce: a burst
                    // beyond the window is pressure, never a violation.
                    let unannounced = p.version == Some(codec::Version::V311);
                    if pending_terminal || unannounced {
                        if p.deferred_publish.is_some() {
                            return Err(Failure::plain("deferred_publish_overlap"));
                        }
                        // A successful write can become visible to the client
                        // just before the network thread publishes its progress.
                        // Keep this one bounded input instead of declaring a
                        // Receive Maximum violation from a stale window.
                        p.deferred_publish = Some(DeferredPublish {
                            packet,
                            ingress_order,
                            since: Instant::now(),
                            wire: pending_terminal,
                        });
                        self.pending_peers.insert(generation);
                        self.events.emit("publish_waiting_wire",json!({"connection":generation.get().to_string(),"details":{"publish_order":ingress_order.to_string()}})).map_err(event_failure)?;
                        return Ok(());
                    }
                    let bytes = codec::FrameBytes::from_slice(&[0xe0, 2, 0x93, 0]);
                    if p.version != Some(codec::Version::V311)
                        && bytes.len() <= p.max_packet
                        && p.io
                            .outgoing
                            .try_send(net::Write {
                                expiry: None,
                                correlation: None,
                                bytes,
                                payload: None,
                                report: net::WriteReport::Complete,
                            })
                            .is_ok()
                    {
                        p.closing = Some(("receive_maximum", Instant::now()));
                        self.pending_peers.insert(generation);
                        return Ok(());
                    }
                    return self.close(generation, "receive_maximum");
                }
                if let InboundPacket::Puback { packet_id, reason } = packet {
                    // The durable identity belongs to this transport's
                    // confirmed CONNECT. A takeover may already have moved
                    // owner authority while its old Close effect is blocked.
                    // Preserve the original key, never retarget or fail other
                    // connections merely because this generation is stale.
                    let session = self
                        .peers
                        .get(&generation)
                        .and_then(|peer| peer.session)
                        .ok_or_else(|| Failure::plain("puback_session_unconfirmed"))?;
                    let connection = ConnectionKey {
                        session,
                        generation,
                    };
                    // This is the authoritative observation boundary: decoded
                    // bytes have left the reader channel, but this input has
                    // not entered either owner queue. Missing means finalNone.
                    let target = self
                        .outgoing_bindings
                        .get(&(connection, packet_id))
                        .copied();
                    self.puback_observation = self
                        .puback_observation
                        .checked_add(1)
                        .ok_or_else(|| Failure::plain("puback_observation_exhausted"))?;
                    let observation = self.puback_observation;
                    self.events
                        .emit_with("puback_observed", || {
                            puback_fields(
                                connection,
                                packet_id,
                                reason,
                                target,
                                Some(observation),
                                "observed",
                            )
                        })
                        .map_err(event_failure)?;
                    #[cfg(feature = "test-hooks")]
                    if self.hooks.directory.as_ref().is_some_and(|dir| {
                        consume_control(&dir.join("copy-next-puback-observation"))
                    }) {
                        let copied = CopiedPuback {
                            connection,
                            packet_id,
                            reason,
                            target,
                            observation: self.puback_observation,
                        };
                        self.events
                            .emit("puback_observation_copied", copied.fields("copied"))
                            .map_err(event_failure)?;
                        self.copied_puback = Some(copied);
                    }
                    return self.queue(Input::ObservedPuback {
                        connection,
                        packet_id,
                        reason,
                        target,
                    });
                }
                if let InboundPacket::Qos2Ack {
                    kind,
                    packet_id,
                    reason,
                } = packet
                {
                    let session = self
                        .peers
                        .get(&generation)
                        .and_then(|peer| peer.session)
                        .ok_or_else(|| Failure::plain("qos2_ack_session_unconfirmed"))?;
                    let connection = ConnectionKey {
                        session,
                        generation,
                    };
                    let target = self
                        .outgoing_bindings
                        .get(&(connection, packet_id))
                        .copied();
                    return self.queue(Input::ObservedQos2Ack {
                        connection,
                        kind,
                        packet_id,
                        reason,
                        target,
                    });
                }
                self.queue(Input::Packet {
                    connection: generation,
                    ingress_order,
                    packet,
                })
            }
        }
    }
    fn admit_generation(&self, generation: ConnectionGeneration) -> Result<bool, Failure> {
        if self.peers.contains_key(&generation) {
            return Ok(true);
        }
        #[cfg(feature = "test-hooks")]
        if MUTANT_ALLOW_STALE {
            self.events
                .emit(
                    "stale_generation_accepted",
                    json!({"connection":generation.get().to_string()}),
                )
                .map_err(event_failure)?;
            return Ok(true);
        }
        self.events
            .emit(
                "stale_generation_ignored",
                json!({"connection":generation.get().to_string()}),
            )
            .map_err(event_failure)?;
        Ok(false)
    }
    fn connections(&mut self) -> Result<(), Failure> {
        self.connections_at(Instant::now())
    }
    fn connections_at(&mut self, now: Instant) -> Result<(), Failure> {
        self.pump_will_connects()?;
        let mut keys = std::mem::take(&mut self.peer_keys);
        keys.clear();
        let deadline_sweep = now.saturating_duration_since(self.last_peer_deadline_scan)
            >= Duration::from_millis(100);
        self.ready_peers.drain_into(&mut self.ready_raw);
        if deadline_sweep {
            self.last_peer_deadline_scan = now;
            self.ready_raw.clear();
            keys.extend(self.peers.keys().copied());
            if keys.len() > 1 {
                keys.sort_unstable();
            }
        } else {
            keys.extend(self.pending_peers.iter().copied());
            keys.extend(self.ready_raw.drain(..).map(ConnectionGeneration::new));
            #[cfg(test)]
            for (&generation, peer) in self.peers.iter() {
                if peer.io.uses_std_channel() {
                    keys.push(generation);
                }
            }
            if keys.len() > 1 {
                keys.sort_unstable();
                keys.dedup();
            }
        }
        if !keys.is_empty() {
            let start = self.peer_cursor % keys.len();
            keys.rotate_left(start);
            self.peer_cursor = start + 1;
        }
        let result = (|| -> Result<(), Failure> {
            for &generation in &keys {
                let (active, needs_deadline_check, is_retiring) = match self.peers.get(&generation)
                {
                    Some(peer) => {
                        let io_active = peer.io.take_activity();
                        let state_active = io_active
                            || peer.retire_code.is_some()
                            || peer.held_packet.is_some()
                            || peer.deferred_publish.is_some()
                            || !peer.behind.is_empty()
                            || peer.requests.is_some()
                            || peer.blocked_output.is_some()
                            || peer.closing.is_some();
                        let needs_deadline = deadline_sweep
                            || peer.closing.is_some()
                            || peer.pending
                            || !peer.connected
                            || peer.capacity_wait.is_some();
                        (state_active, needs_deadline, peer.retire_code)
                    }
                    None => {
                        if !self.pending_peers.is_empty() {
                            self.pending_peers.remove(&generation);
                        }
                        continue;
                    }
                };
                if !active && !needs_deadline_check {
                    if !self.pending_peers.is_empty() {
                        self.pending_peers.remove(&generation);
                    }
                    continue;
                }
                if let Some(code) = is_retiring {
                    self.wire_progress(generation)?;
                    self.collect_written(generation)?;
                    self.drain_inputs()?;
                    if !self
                        .peers
                        .get(&generation)
                        .is_some_and(|peer| peer.io.is_finished())
                    {
                        continue;
                    }
                    // JoinHandle completion makes the producer's final sends visible.
                    self.collect_written(generation)?;
                    self.drain_inputs()?;
                    if self
                        .terminal_inputs
                        .iter()
                        .any(|input| matches!(input, Input::FrameResult {connection,..} | Input::Qos0EmitRefused {connection,..} if connection.generation == generation))
                    {
                        continue;
                    }
                    self.finish_close(generation, code)?;
                    continue;
                }
                let mut drained_incoming = true;
                if active {
                    let (tls, has_wire_progress, has_written) = match self.peers.get(&generation) {
                        Some(peer) => (
                            peer.io.is_tls(),
                            peer.io.wire_progress.has_pending(),
                            peer.io.written.has_pending(),
                        ),
                        None => continue,
                    };
                    let mut tls_close = None;
                    let mut write_close = None;
                    let mut input_ended = false;
                    if has_wire_progress {
                        self.wire_progress(generation)?;
                    }
                    if has_written && let Some(code) = self.collect_written(generation)? {
                        write_close = Some(code);
                    }
                    let backpressured = self.peers.get_mut(&generation).is_some_and(|peer| {
                        if let Some(effect) = peer.blocked_output
                            && self.owner.outbox().is_some_and(|outbox| {
                                outbox.pending().any(|pending| pending.id() == effect)
                            })
                        {
                            peer.io.terminal_reason().is_none()
                        } else {
                            peer.blocked_output = None;
                            false
                        }
                    });
                    if !backpressured {
                        self.retry_deferred_publish(generation)?;
                    }
                    self.serve_requests(generation)?;
                    drained_incoming = false;
                    for _ in 0..32 {
                        // A CONNECT can use one slot now and reserve one for Open.
                        // Leave the bytes with their socket reader until both fit.
                        if backpressured || self.ordinary_room() < 2 {
                            break;
                        }
                        let business_waits = self.business_waits(generation);
                        // A packet that was kept behind a publication is
                        // older than the held packet and than the lane.
                        let mut from_behind = false;
                        let incoming = match self.peers.get_mut(&generation) {
                            Some(p) if p.closing.is_some() || p.requests.is_some() => {
                                break;
                            }
                            Some(p) if p.pending && p.held_packet.is_some() => break,
                            Some(p) if p.deferred_publish.is_none() && !p.behind.is_empty() => {
                                if business_waits
                                    && p.behind
                                        .front()
                                        .is_some_and(|(first, ..)| business_header(*first))
                                {
                                    break;
                                }
                                let (first, body, observed) =
                                    p.behind.pop_front().expect("checked packet behind");
                                p.behind_bytes -= body.len();
                                from_behind = true;
                                Ok(net::Incoming::Packet {
                                    first,
                                    body,
                                    observed,
                                })
                            }
                            Some(p)
                                if business_waits
                                    && p.deferred_publish.is_none()
                                    && p.held_packet
                                        .as_ref()
                                        .is_some_and(|(first, ..)| business_header(*first)) =>
                            {
                                break;
                            }
                            Some(p) => match p.held_packet.take() {
                                Some((first, body, observed)) => Ok(net::Incoming::Packet {
                                    first,
                                    body,
                                    observed,
                                }),
                                None if !p.io.incoming.has_pending() => {
                                    drained_incoming = true;
                                    break;
                                }
                                None => p.io.incoming.try_recv(),
                            },
                            None => break,
                        };
                        if incoming.is_ok() {
                            self.progress = self.progress.wrapping_add(1);
                        }
                        match incoming {
                            Ok(net::Incoming::Authenticated { elapsed }) => {
                                self.events.emit("tls_authenticated", json!({"connection":generation.get().to_string(),
                                    "details":{"version":"TLS1.3","handshake_us":elapsed.as_micros() as u64}}))
                                    .map_err(event_failure)?;
                            }
                            Ok(net::Incoming::Packet {
                                first,
                                body,
                                observed,
                            }) => {
                                if first >> 4 != 14
                                    && self.peers.get(&generation).is_some_and(|p| p.pending)
                                {
                                    self.peers
                                        .get_mut(&generation)
                                        .expect("checked peer")
                                        .held_packet = Some((first, body, observed));
                                    break;
                                }
                                // A publication waits for a slot of the window.
                                // The PUBREL of another exchange goes ahead: it
                                // frees a slot. Every other packet waits behind
                                // it, in order, up to a bound; past the bound
                                // the reader stops as it did.
                                if let Some(peer) = self.peers.get_mut(&generation)
                                    && peer.deferred_publish.is_some()
                                    && !peer.overtakes_deferred(first, &body)
                                {
                                    match peer.keep_behind((first, body, observed)) {
                                        Ok(()) => continue,
                                        Err(packet) => {
                                            peer.held_packet = Some(packet);
                                            break;
                                        }
                                    }
                                }
                                // It was taken from the lane to learn what it is;
                                // it goes no further for now (record 0046).
                                if business_waits
                                    && business_header(first)
                                    && let Some(peer) = self.peers.get_mut(&generation)
                                {
                                    if from_behind {
                                        peer.behind_bytes += body.len();
                                        peer.behind.push_front((first, body, observed));
                                    } else {
                                        peer.held_packet = Some((first, body, observed));
                                    }
                                    break;
                                }
                                self.packet_at(generation, first, body, observed)?
                            }
                            Ok(net::Incoming::Refused { code }) => {
                                self.header_refused(generation, code)?;
                                break;
                            }
                            Ok(net::Incoming::Closed { code }) => {
                                if tls {
                                    input_ended = true;
                                    tls_close = Some(code);
                                    break;
                                }
                                self.drain_inputs()?;
                                self.close(generation, code)?;
                                break;
                            }
                            Err(TryRecvError::Empty) => {
                                drained_incoming = true;
                                break;
                            }
                            Err(TryRecvError::Disconnected) => {
                                drained_incoming = true;
                                let code = self
                                    .peers
                                    .get(&generation)
                                    .and_then(|p| p.io.terminal_reason())
                                    .unwrap_or("reader_closed");
                                if tls {
                                    input_ended = true;
                                    tls_close = Some(code);
                                    break;
                                }
                                self.drain_inputs()?;
                                self.close(generation, code)?;
                                break;
                            }
                        }
                    }
                    if self
                        .peers
                        .get(&generation)
                        .is_some_and(|peer| peer.io.written.has_pending())
                        && let Some(code) = self.collect_written(generation)?
                    {
                        write_close.get_or_insert(code);
                    }
                    if let Some(code) = write_close {
                        if tls {
                            tls_close.get_or_insert(code);
                        } else {
                            self.close(generation, code)?;
                        }
                    }
                    // A TLS write terminal can precede a decoded normal DISCONNECT.
                    // Defer this close until the ordered input terminal. Keep running
                    // connection deadlines while earlier input is held behind Open.
                    if let Some(code) = tls_close
                        && (input_ended
                            || self
                                .peers
                                .get(&generation)
                                .is_some_and(|peer| peer.closing.is_some()))
                    {
                        // The two queue observations may race even though they come
                        // from one TLS task. Apply the frame facts while the exact
                        // generation still owns its exchange.
                        self.drain_inputs()?;
                        if let Some(peer) = self.peers.get_mut(&generation)
                            && self.terminal_inputs.iter().any(|input| matches!(input,
                                Input::FrameResult { connection, .. } | Input::Qos0EmitRefused { connection, .. }
                                    if connection.generation == generation))
                        {
                            let (_, since) = peer.closing.get_or_insert((code, now));
                            if now.saturating_duration_since(*since) < Duration::from_millis(300) {
                                self.pending_peers.insert(generation);
                                continue;
                            }
                        }
                        self.close(generation, code)?;
                    }
                }
                if needs_deadline_check && let Some(p) = self.peers.get(&generation) {
                    let expired = if let Some((code, since)) = p.closing
                        && now.saturating_duration_since(since) > Duration::from_millis(300)
                    {
                        Some(code)
                    } else if (p.pending || !p.connected)
                        && now.saturating_duration_since(p.created) > Duration::from_secs(3)
                    {
                        Some("connect_deadline")
                    } else if p.capacity_wait.is_some_and(|since| {
                        now.saturating_duration_since(since.max(self.credit_progress))
                            > CAPACITY_WAIT
                    }) {
                        Some("input_capacity")
                    } else if p.keep_alive != 0
                        // The clock runs from the last complete packet the
                        // transport received, taken by the core or not: a peer
                        // the broker keeps hearing is not silent, a packet the
                        // broker holds does not stop the clock, and taking an
                        // old packet does not restart it. The time the core
                        // took a packet stands in only for a transport that
                        // has reported no reception.
                        && now.saturating_duration_since(p.io.last_heard().unwrap_or(p.last_packet))
                            > Duration::from_millis(u64::from(p.keep_alive) * 1500)
                    {
                        Some("keep_alive")
                    } else {
                        None
                    };
                    if let Some(code) = expired {
                        self.close(generation, code)?;
                    }
                }
                match self.peers.get(&generation) {
                    Some(peer)
                        if peer.needs_poll() || !drained_incoming || peer.io.has_pending() =>
                    {
                        self.pending_peers.insert(generation);
                    }
                    _ => {
                        if !self.pending_peers.is_empty() {
                            self.pending_peers.remove(&generation);
                        }
                    }
                }
            }
            Ok(())
        })();
        self.peer_keys = keys;
        result?;
        // Accepted frames whose socket closed still receive terminal failure; commits are separate.
        if self.stale_frames_dirty {
            self.stale_frames_dirty = false;
            let stale: Vec<_> = self
                .frames
                .iter()
                .filter(|(_, v)| !self.peers.contains_key(&v.connection.generation))
                .map(|(id, v)| (id, v.connection))
                .collect();
            for (effect, connection) in stale {
                self.frames.remove(&effect);
                self.queue(Input::FrameResult {
                    effect,
                    connection,
                    progress: FrameProgress::Failed,
                })?;
            }
        }
        for n in (0..self.retiring.len()).rev() {
            if self.retiring[n].is_finished() {
                if !self.retiring[n].join_finished() {
                    return Err(Failure::plain("socket_thread_panicked"));
                }
                self.retiring.swap_remove(n);
            }
        }
        Ok(())
    }
    /// Collect exact frame facts independently of whether transport retirement
    /// was requested. A terminal task does not order the other input channel.
    fn collect_written(
        &mut self,
        generation: ConnectionGeneration,
    ) -> Result<Option<&'static str>, Failure> {
        let tls = self
            .peers
            .get(&generation)
            .is_some_and(|peer| peer.io.is_tls());
        let mut tls_close = None;
        let receipt_limit = net::WRITTEN_SLOTS;
        for _ in 0..receipt_limit {
            let written = match self.peers.get(&generation) {
                Some(p) => p.io.written.try_recv(),
                None => break,
            };
            if written.is_ok() {
                self.progress = self.progress.wrapping_add(1);
            }
            match written {
                Ok(net::Written::Started { correlation }) => {
                    if let Some((effect, connection)) = correlation
                        && self.frames.get(&effect).is_some_and(|frame| {
                            frame.connection == connection
                                && matches!(frame.kind, "PUBLISH" | "PUBLISH0")
                        })
                    {
                        self.events.emit("message_transmission_started",json!({"connection":connection.generation.get().to_string(),"details":{"effect":effect_text(effect),"authority":"positive_socket_write"}})).map_err(event_failure)?;
                        self.queue(Input::FrameResult {
                            effect,
                            connection,
                            progress: FrameProgress::Started,
                        })?;
                    }
                }
                Ok(net::Written::NotStarted {
                    correlation,
                    reason,
                }) => {
                    if let Some((effect, connection)) = correlation
                        && let Some(pending) = self.frames.remove(&effect)
                    {
                        if pending.connection != connection {
                            return Err(Failure::plain("frame_connection_mismatch"));
                        }
                        self.outgoing_bindings
                            .retain(|_, value| value.frame != effect);
                        self.events.emit("message_transmission_not_started",json!({"connection":connection.generation.get().to_string(),"details":{"effect":effect_text(effect),"reason":format!("{reason:?}"),"authority":"zero_socket_bytes"}})).map_err(event_failure)?;
                        if pending.kind == "PUBLISH0" {
                            let reason = match reason {
                                mqttiumd_contract::message_expiry::MessageExpiryBlock::Expired => Qos0DropReason::MessageExpired,
                                mqttiumd_contract::message_expiry::MessageExpiryBlock::TemporalUncertain => Qos0DropReason::TemporalUncertain,
                            };
                            self.queue(Input::Qos0EmitRefused {
                                effect,
                                connection,
                                reason,
                            })?;
                        } else {
                            self.queue(Input::FrameResult {
                                effect,
                                connection,
                                progress: FrameProgress::NotStarted { reason },
                            })?;
                        }
                    }
                }
                Ok(net::Written::Complete { correlation, bytes }) => {
                    if let Some((effect, connection)) = correlation {
                        #[cfg(feature = "test-hooks")]
                        if self
                            .frames
                            .get(&effect)
                            .is_some_and(|frame| frame.kind == "PUBLISH")
                            && self.hooks.directory.as_ref().is_some_and(|dir| {
                                consume_control(&dir.join("hold-next-outgoing-written"))
                            })
                        {
                            if self.deferred_outgoing_written.is_some() {
                                return Err(Failure::plain("outgoing_written_hold_overlap"));
                            }
                            self.copied_outgoing_frame = Some((
                                effect,
                                connection,
                                FrameProgress::Written {
                                    confirmed: bytes as u64,
                                    total: bytes as u64,
                                },
                            ));
                            self.deferred_outgoing_written =
                                Some((effect, connection, bytes, Instant::now()));
                            self.events.emit("outgoing_written_held",json!({"connection":connection.generation.get().to_string(),"details":{"effect":effect_text(effect),"socket_write_complete":true,"bytes":bytes}})).map_err(event_failure)?;
                            continue;
                        }
                        #[cfg(feature = "test-hooks")]
                        if !self.deferred_ack_used
                            && self.hooks.pause.iter().any(|p| p == "ack.receipt")
                            && let Some(pending) = self.frames.get(&effect)
                            && pending.kind == "PUBACK"
                        {
                            self.events.emit("barrier_hit", json!({"point":"ack.receipt","connection":generation.get().to_string(),"details":{"effect":effect_text(effect),"packet_id":pending.packet_id.unwrap_or(0),"publish_order":pending.receive_serial.map(|v|v.to_string()).unwrap_or_default(),"bytes":bytes,"socket_write_complete":true}})).map_err(event_failure)?;
                            self.deferred_ack_used = true;
                            self.deferred_ack = Some((effect, connection, bytes, Instant::now()));
                            continue;
                        }
                        self.written_complete(effect, connection, bytes)?;
                    } else if let Some((code, _)) =
                        self.peers.get(&generation).and_then(|p| p.closing)
                    {
                        tls_close = Some(code);
                    }
                }
                Ok(net::Written::Failed { correlation, code }) => {
                    if let Some((effect, connection)) = correlation {
                        if let Some(frame) = self.frames.remove(&effect) {
                            self.retained_write_failed(effect, &frame, code)?;
                        }
                        self.queue(Input::FrameResult {
                            effect,
                            connection,
                            progress: FrameProgress::Failed,
                        })?;
                    }
                    if tls {
                        tls_close.get_or_insert(code);
                    } else {
                        tls_close = Some(code);
                        break;
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    if tls {
                        let reason = self
                            .peers
                            .get(&generation)
                            .and_then(|peer| peer.io.terminal_reason());
                        tls_close.get_or_insert(reason.unwrap_or("writer_closed"));
                    } else {
                        tls_close = Some("writer_closed");
                    }
                    break;
                }
            }
        }
        Ok(tls_close)
    }

    /// Network completion and core receipt consumption have separate lanes.
    /// No lock is held across I/O. Scope/occurrence/QoS are checked for every
    /// early receive-credit release; stale progress is never inferred by PI.
    fn wire_progress(&mut self, generation: ConnectionGeneration) -> Result<(), Failure> {
        let Some(peer) = self.peers.get_mut(&generation) else {
            return Ok(());
        };
        for _ in 0..net::OUTPUT_SLOTS + 1 + 8 {
            let progress = match peer.io.wire_progress.try_recv() {
                Ok(progress) => {
                    self.progress = self.progress.wrapping_add(1);
                    progress
                }
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
            };
            // Written may already have consumed a completion between two
            // polls of these independent lanes. Its late copy is harmless.
            let Some(frame) = self.frames.get_mut(&progress.effect) else {
                continue;
            };
            if frame.connection != progress.connection
                || progress.connection.generation != generation
            {
                return Err(Failure::plain("wire_progress_connection_mismatch"));
            }
            if let Some((id, terminal)) = frame.packet_id.zip(frame.terminal) {
                let released = peer
                    .receiving
                    .complete_frame(id, frame.receive_serial, terminal);
                frame.wire_released |= released;
                let frame_kind = frame.kind;
                let frame_serial = frame.receive_serial;
                self.events.emit_with("wire_receive_credit",||json!({"connection":generation.get().to_string(),"details":{"effect":effect_text(progress.effect),"packet_kind":frame_kind,"packet_id":id,"publish_order":frame_serial.map(|v|v.to_string()).unwrap_or_default(),"released":released}})).map_err(event_failure)?;
            }
        }
        Ok(())
    }

    fn retry_deferred_publish(&mut self, generation: ConnectionGeneration) -> Result<(), Failure> {
        if self.ordinary_room() == 0 {
            return Ok(());
        }
        let Some(peer) = self.peers.get_mut(&generation) else {
            return Ok(());
        };
        let Some(deferred) = peer.deferred_publish.as_ref() else {
            return Ok(());
        };
        let InboundPacket::Publish {
            packet_id: Some(id),
            qos,
            ..
        } = &deferred.packet
        else {
            return Err(Failure::plain("deferred_publish_kind"));
        };
        if peer
            .receiving
            .publish_qos(id.get(), deferred.ingress_order, *qos)
            .is_ok()
        {
            let deferred = peer
                .deferred_publish
                .take()
                .ok_or_else(|| Failure::plain("deferred_publish_missing"))?;
            self.queue(Input::Packet {
                connection: generation,
                ingress_order: deferred.ingress_order,
                packet: deferred.packet,
            })?;
        } else if deferred.wire && deferred.since.elapsed() > Duration::from_millis(500) {
            self.close(generation, "wire_progress_deadline")?;
        }
        Ok(())
    }
    /// A socket write receipt is local progress, not evidence of peer receipt.
    fn written_complete(
        &mut self,
        effect: EffectId,
        connection: ConnectionKey,
        bytes: usize,
    ) -> Result<(), Failure> {
        let Some(pending) = self.frames.remove(&effect) else {
            if !self.peers.contains_key(&connection.generation) {
                self.events.emit("frame_result_ignored", json!({"connection":connection.generation.get().to_string(),"details":{"effect":effect_text(effect),"code":"retired_transport"}})).map_err(event_failure)?;
                return Ok(());
            }
            return Err(Failure::plain("uncorrelated_frame_result"));
        };
        self.session_cardinality_dirty = true;
        if pending.connection != connection {
            return Err(Failure::plain("frame_connection_mismatch"));
        }
        let generation = connection.generation;
        let (kind, id) = (pending.kind, pending.packet_id);
        if kind == "PUBLISH0" {
            self.events.emit_with("frame_written",||json!({"connection":generation.get().to_string(),"details":{"packet_kind":"PUBLISH","qos":0,"effect":effect_text(effect),"bytes":bytes}})).map_err(event_failure)?;
        } else if kind != "PINGRESP" || pending.was_retained {
            let serial = pending.receive_serial;
            self.events.emit_with("frame_written",||json!({"connection":generation.get().to_string(),"details":{"packet_kind":kind,"effect":effect_text(effect),"packet_id":id.unwrap_or(0),"publish_order":serial.map(|v|v.to_string()).unwrap_or_default(),"bytes":bytes}})).map_err(event_failure)?;
        }
        if let Some(terminal) = pending.terminal
            && let Some(p) = self.peers.get_mut(&generation)
            && let Some(id) = id
        {
            let released_now = p
                .receiving
                .complete_frame(id, pending.receive_serial, terminal);
            // If no newer occurrence occupies the identifier, this receipt
            // confirms its own already published wire completion. A newer
            // occurrence still meets the exact guards and is left untouched.
            let released = released_now || (pending.wire_released && !p.receiving.contains(id));
            #[cfg(feature = "test-hooks")]
            let released = if MUTANT_STALE_RECEIPT && !released {
                p.receiving.complete_current(id)
            } else {
                released
            };
            let serial = pending.receive_serial;
            self.events.emit_with("receive_credit", || json!({"connection":generation.get().to_string(),"details":{"packet_kind":kind,"effect":effect_text(effect),"packet_id":id,"publish_order":serial.map(|v|v.to_string()).unwrap_or_default(),"released":released}})).map_err(event_failure)?;
        }
        #[cfg(feature = "test-hooks")]
        if kind == "PUBLISH"
            && self
                .hooks
                .directory
                .as_ref()
                .is_some_and(|dir| consume_control(&dir.join("copy-next-outgoing-frame-result")))
        {
            self.copied_outgoing_frame = Some((
                effect,
                connection,
                FrameProgress::Written {
                    confirmed: bytes as u64,
                    total: bytes as u64,
                },
            ));
            self.events.emit("outgoing_frame_result_copied",json!({"connection":generation.get().to_string(),"details":{"effect":effect_text(effect),"bytes":bytes}})).map_err(event_failure)?;
        }
        self.queue(Input::FrameResult {
            effect,
            connection,
            progress: FrameProgress::Written {
                confirmed: bytes as u64,
                total: bytes as u64,
            },
        })
    }
    #[cfg(feature = "test-hooks")]
    fn controls(&mut self) -> Result<(), Failure> {
        if let Some(dir) = self.hooks.directory.clone() {
            if consume_control(&dir.join("replay-copied-puback-observation")) {
                let copied = self
                    .copied_puback
                    .ok_or_else(|| Failure::plain("puback_copy_missing"))?;
                self.events
                    .emit("puback_observation_replayed", copied.fields("replayed"))
                    .map_err(event_failure)?;
                self.queue(copied.input())?;
            }
            if let Some((effect, connection, bytes, since)) = self.deferred_outgoing_written {
                if !self.frames.contains_key(&effect) {
                    self.deferred_outgoing_written = None;
                    self.events.emit("outgoing_written_discarded",json!({"connection":connection.generation.get().to_string(),"details":{"effect":effect_text(effect),"code":"connection_closed"}})).map_err(event_failure)?;
                } else if since.elapsed() > Duration::from_secs(30) {
                    return Err(Failure::plain("outgoing_written_deadline"));
                } else if consume_control(&dir.join("release-outgoing-written")) {
                    self.deferred_outgoing_written = None;
                    self.written_complete(effect, connection, bytes)?;
                    self.events.emit("outgoing_written_released",json!({"connection":connection.generation.get().to_string(),"details":{"effect":effect_text(effect)}})).map_err(event_failure)?;
                }
            }
            if consume_control(&dir.join("replay-last-outgoing-frame-result")) {
                let (effect, connection, progress) = self
                    .copied_outgoing_frame
                    .ok_or_else(|| Failure::plain("outgoing_frame_copy_missing"))?;
                self.queue(Input::FrameResult {
                    effect,
                    connection,
                    progress,
                })?;
                self.events.emit("outgoing_frame_result_replayed",json!({"connection":connection.generation.get().to_string(),"details":{"effect":effect_text(effect)}})).map_err(event_failure)?;
            }
            if consume_control(&dir.join("replay-last-finish-result")) {
                let (effect, operation_id, reply) = self
                    .last_finish_result
                    .ok_or_else(|| Failure::plain("finish_result_copy_missing"))?;
                self.queue(Input::StoreResult {
                    effect,
                    operation: operation_id,
                    reply,
                })?;
                self.events.emit("finish_result_replayed",json!({"operation":operation(operation_id),"command_kind":"Finish","details":{"effect":effect_text(effect)}})).map_err(event_failure)?;
            }
        }
        if self
            .hooks
            .directory
            .as_ref()
            .is_none_or(|d| !d.join("hold-work-pages").exists())
        {
            while let Some(response) = self.deferred_work.pop_front() {
                let Response::Work {
                    effect,
                    session,
                    result,
                } = response
                else {
                    return Err(Failure::plain("held_work_kind"));
                };
                if self.work_reads.remove(&effect) != Some(session) {
                    return Err(Failure::plain("uncorrelated_work_page"));
                }
                let page = result.map_err(|e| Failure::read(e, ReadPhase::Work))?;
                self.session_cardinality_dirty = true;
                if !self.session_ids.contains(&session) {
                    self.events.emit("work_page_ignored", json!({"details":{"session":session.get().to_string(),"effect":effect_text(effect),"code":"retired_session"}})).map_err(event_failure)?;
                    continue;
                }
                for item in page.items.as_slice() {
                    if self.work_cache.len() >= 128 && !self.work_cache.contains_key(&item.delivery)
                    {
                        return Err(Failure::plain("work_cache_budget"));
                    }
                    self.work_cache.insert(item.delivery, item.clone());
                }
                self.queue(Input::WorkPage {
                    effect,
                    session,
                    items: page.items,
                    next: page.next,
                })?;
            }
        }
        if let Some(dir) = &self.hooks.directory {
            let replay = dir.join("replay-last-consume-result");
            if replay.exists() {
                std::fs::remove_file(replay).map_err(|_| Failure::plain("hook_release"))?;
                let (effect, operation, reply) = self
                    .last_consume_result
                    .ok_or_else(|| Failure::plain("no_consume_result"))?;
                self.queue(Input::StoreResult {
                    effect,
                    operation,
                    reply,
                })?;
                self.events
                    .emit(
                        "consume_result_replayed",
                        json!({"operation":crate::events::operation(operation)}),
                    )
                    .map_err(event_failure)?;
            }
        }
        if let Some((effect, connection, bytes, since)) = self.deferred_ack {
            if !self.frames.contains_key(&effect) {
                self.deferred_ack = None;
            } else if since.elapsed() > Duration::from_secs(30) {
                return Err(Failure::plain("ack_receipt_deadline"));
            } else if let Some(dir) = &self.hooks.directory {
                let release = dir.join("release-ack.receipt");
                if release
                    .try_exists()
                    .map_err(|_| Failure::plain("hook_release"))?
                {
                    std::fs::remove_file(release).map_err(|_| Failure::plain("hook_release"))?;
                    self.deferred_ack = None;
                    self.written_complete(effect, connection, bytes)?;
                    self.events.emit("barrier_released", json!({"point":"ack.receipt","connection":connection.generation.get().to_string(),"details":{"effect":effect_text(effect)}})).map_err(event_failure)?;
                }
            }
        }
        if let Some(dir) = &self.hooks.directory
            && consume_control(&dir.join("replay-session-close"))
        {
            let copied = self
                .copied_session_close
                .ok_or_else(|| Failure::plain("no_copied_session_close"))?;
            self.events
                .emit("session_close_replayed", copied.fields())
                .map_err(event_failure)?;
            self.queue(Input::ConnectionClosed {
                connection: copied.generation,
            })?;
        }
        if let Some(dir) = &self.hooks.directory
            && consume_control(&dir.join("replay-session-change"))
        {
            let copied = self
                .copied_session_change
                .clone()
                .ok_or_else(|| Failure::plain("no_copied_session_change"))?;
            self.events
                .emit("session_change_replayed", copied.fields())
                .map_err(event_failure)?;
            self.session_changed(copied)?;
        }
        if let Some(dir) = &self.hooks.directory {
            let replay = dir.join("replay-captured");
            if replay.exists() {
                std::fs::remove_file(replay).map_err(|_| Failure::plain("hook_release"))?;
                let (generation, connect) = self
                    .captured
                    .clone()
                    .ok_or_else(|| Failure::plain("no_captured_input"))?;
                if self.admit_generation(generation)? {
                    self.connect(generation, connect)?;
                }
            }
        }
        Ok(())
    }
    fn retained_write_failed(
        &self,
        effect: EffectId,
        frame: &PendingFrame,
        code: &str,
    ) -> Result<(), Failure> {
        if frame.was_retained {
            self.events
                .emit(
                    "frame_failed",
                    json!({"connection":frame.connection.generation.get().to_string(),
                "details":{"effect":effect_text(effect),"packet_kind":frame.kind,"code":code}}),
                )
                .map_err(event_failure)?;
        }
        Ok(())
    }
}
#[cfg(feature = "test-hooks")]
impl AppliedSessionChange {
    fn fields(&self) -> serde_json::Value {
        let (action, session) = match self.change {
            SessionChange::Opened { session, .. } => ("Opened", session),
            SessionChange::Closed { session, .. } => ("Closed", session),
            SessionChange::Detached { session, .. } => ("Detached", session),
        };
        let mut fields = json!({"operation":operation(self.operation),"details":{"action":action,"session":session.get().to_string()}});
        if let Some(connection) = self.connection {
            fields["connection"] = json!(connection.get().to_string());
        }
        fields
    }
}
#[cfg(feature = "test-hooks")]
#[derive(Clone, Copy)]
struct SessionCloseObservation {
    generation: ConnectionGeneration,
    session: SessionId,
    opened_by: Option<OperationId>,
}
#[cfg(feature = "test-hooks")]
impl SessionCloseObservation {
    fn fields(self) -> serde_json::Value {
        let mut fields = json!({"connection":self.generation.get().to_string(),"details":{"session":self.session.get().to_string()}});
        if let Some(token) = self.opened_by {
            fields["operation"] = operation(token);
        }
        fields
    }
}
#[cfg(feature = "test-hooks")]
#[derive(Clone, Copy)]
struct CopiedPuback {
    connection: ConnectionKey,
    packet_id: OutboundPacketId,
    reason: u8,
    target: Option<PubackTarget>,
    observation: u64,
}
#[cfg(feature = "test-hooks")]
impl CopiedPuback {
    fn input(self) -> Input {
        Input::ObservedPuback {
            connection: self.connection,
            packet_id: self.packet_id,
            reason: self.reason,
            target: self.target,
        }
    }
    fn fields(self, code: &str) -> serde_json::Value {
        puback_fields(
            self.connection,
            self.packet_id,
            self.reason,
            self.target,
            Some(self.observation),
            code,
        )
    }
}
#[cfg(feature = "test-hooks")]
fn consume_control(path: &std::path::Path) -> bool {
    path.exists() && std::fs::remove_file(path).is_ok()
}
#[cfg(feature = "test-hooks")]
struct HeldEmission {
    point: &'static str,
    effect: OwnedEffect,
    since: Instant,
    released: bool,
}
enum OutgoingPrune {
    None,
    Key((ConnectionKey, OutboundPacketId)),
    Full,
}

struct Sink<'a, 'b> {
    writer: &'a mut StoreHandle<'b>,
    peers: &'a mut FlatMap<ConnectionGeneration, Peer>,
    events: &'a Events,
    inflight: &'a mut ScopedFlatMap<EffectId, OperationId>,
    lifecycle_inflight: &'a mut BTreeMap<EffectId, OperationId>,
    lifecycle_operation: Option<OperationId>,
    will_operation: Option<OperationId>,
    will_reads: &'a mut Option<EffectId>,
    work_reads: &'a mut ScopedFlatMap<EffectId, SessionId>,
    bindings: &'a FlatMap<SessionId, ConnectionGeneration>,
    #[cfg(feature = "test-hooks")]
    work_cache: &'a BTreeMap<DeliveryId, WorkItem>,
    #[cfg(feature = "test-hooks")]
    hooks: &'a Hooks,
    #[cfg(feature = "test-hooks")]
    emission_barriers: &'a mut VecDeque<HeldEmission>,
    #[cfg(feature = "test-hooks")]
    releasing_qos0: bool,
    timers: &'a mut BTreeMap<DeadlineToken, Instant>,
    inputs: &'a mut VecDeque<Input>,
    queued_work_sessions: &'a mut FlatSet<SessionId>,
    terminal_inputs: &'a mut VecDeque<Input>,
    terminal_effects: &'a mut ScopedFlatMap<EffectId, u32>,
    ordinary_reserved: usize,
    queued_writer_replies: Option<usize>,
    frames: &'a mut ScopedFlatMap<EffectId, PendingFrame>,
    outgoing_bindings: &'a mut FlatMap<(ConnectionKey, OutboundPacketId), PubackTarget>,
    limits: Limits,
    close: Vec<(ConnectionGeneration, &'static str)>,
    mark_pending_peers: Vec<ConnectionGeneration>,
    pending_work_targets: Vec<SessionId>,
    direct_skip_next_deadline: bool,
    session_change: Option<AppliedSessionChange>,
    error: Option<Failure>,
}
impl Sink<'_, '_> {
    fn push_terminal(&mut self, input: Input) {
        if let Some(id) = Runtime::terminal_effect(&input) {
            *self.terminal_effects.get_or_insert_default(id) += 1;
        }
        self.terminal_inputs.push_back(input);
    }

    fn writer_reply_room(&mut self) -> bool {
        let pending =
            self.inflight.len() + self.work_reads.len() + usize::from(self.will_reads.is_some());
        if self.inputs.len() + pending < WRITER_INPUTS {
            return true;
        }
        let queued = *self.queued_writer_replies.get_or_insert_with(|| {
            self.inputs
                .iter()
                .filter(|input| Runtime::writer_input(input))
                .count()
        });
        queued + pending < WRITER_INPUTS
    }
}
impl Sink<'_, '_> {
    fn submit_lifecycle(&mut self, effect: OwnedEffect) -> SubmitResult {
        if self.lifecycle_inflight.len() >= LIFECYCLE_IN_FLIGHT {
            return SubmitResult::NotSubmitted {
                effect,
                reason: SubmitBlock::InFlightLimit,
            };
        }
        let operation_id = match effect.effect() {
            Effect::Persist { command } => command.recipe.operation(),
            Effect::ResolveAndFence { operation } => *operation,
            _ => unreachable!("lifecycle effect classified by caller"),
        };
        let id = effect.id();
        let details = match effect.effect() {
            Effect::Persist { command } => recipe_details(command.recipe.get()),
            _ => json!({"action":"Resolve"}),
        };
        match self
            .writer
            .try_send_control(ControlRequest::Lifecycle(effect))
        {
            Ok(()) => {
                self.lifecycle_inflight.insert(id, operation_id);
                if self.events.emit("command_submitted", json!({"operation":operation(operation_id),"command_kind":"SessionLifecycle","details":details})).is_err() {
                    self.error = Some(Failure::plain("events_unavailable"));
                }
                SubmitResult::Accepted
            }
            Err(TrySendError::Full(ControlRequest::Lifecycle(effect))) => {
                SubmitResult::NotSubmitted {
                    effect,
                    reason: SubmitBlock::QueueFull,
                }
            }
            Err(TrySendError::Disconnected(ControlRequest::Lifecycle(effect))) => {
                self.error = Some(Failure::plain("writer_control_disconnected"));
                SubmitResult::NotSubmitted {
                    effect,
                    reason: SubmitBlock::Closed,
                }
            }
            _ => unreachable!("exact control request variant"),
        }
    }
}
#[cfg(feature = "test-hooks")]
impl Sink<'_, '_> {
    fn release_qos0(&mut self) {
        let Some(front) = self.emission_barriers.front_mut() else {
            return;
        };
        if !front.released {
            let Some(dir) = &self.hooks.directory else {
                self.error = Some(Failure::plain("hook_directory"));
                return;
            };
            let release = dir.join(format!("release-{}", front.point));
            if release.exists() {
                if std::fs::remove_file(release).is_err() {
                    self.error = Some(Failure::plain("hook_release"));
                    return;
                }
                front.released = true;
            } else if front.since.elapsed() > Duration::from_secs(30) {
                self.error = Some(Failure::plain("hook_deadline"));
                return;
            } else {
                return;
            }
        }
        let held = self
            .emission_barriers
            .pop_front()
            .expect("checked bounded queue");
        self.releasing_qos0 = true;
        let result = self.try_submit(held.effect);
        self.releasing_qos0 = false;
        if let SubmitResult::NotSubmitted { effect, .. } = result {
            self.emission_barriers.push_front(HeldEmission {
                point: held.point,
                effect,
                since: held.since,
                released: true,
            });
        }
    }
}
impl EffectSink for Sink<'_, '_> {
    fn try_submit(&mut self, effect: OwnedEffect) -> SubmitResult {
        let id = effect.id();
        let refused = |effect, reason| SubmitResult::NotSubmitted { effect, reason };
        if self.error.is_some() {
            return refused(effect, SubmitBlock::Closed);
        }
        // Applying a cache fact or shutting down a transport belongs between
        // effects: a later Persist/CONNACK must not race this dispatch tail.
        if self.session_change.is_some() || !self.close.is_empty() {
            return refused(effect, SubmitBlock::QueueFull);
        }
        if !matches!(effect.effect(), Effect::ArmDeadline { .. }) {
            self.direct_skip_next_deadline = false;
        }
        match effect.effect() {
            Effect::SessionChanged {
                operation,
                connection,
                client_id,
                change,
            } => {
                self.session_change = Some(AppliedSessionChange {
                    operation: *operation,
                    connection: *connection,
                    client_id: client_id.clone(),
                    change: *change,
                });
                let _ = effect.into_parts();
                SubmitResult::Accepted
            }
            Effect::SessionRefused { connection, reason } => {
                let generation = *connection;
                let reason = *reason;
                if let Some(peer) = self.peers.get_mut(&generation) {
                    let bytes = match codec::session_refusal_for(
                        peer.version.unwrap_or_default(),
                        reason,
                    ) {
                        Ok(bytes) => bytes,
                        Err(_) => {
                            self.error = Some(Failure::plain("invalid_session_refusal"));
                            return refused(effect, SubmitBlock::Closed);
                        }
                    };
                    if bytes.len() <= peer.max_packet
                        && peer
                            .io
                            .outgoing
                            .try_send(net::Write {
                                expiry: None,
                                correlation: None,
                                bytes: codec::FrameBytes::from(bytes),
                                payload: None,
                                report: net::WriteReport::Complete,
                            })
                            .is_ok()
                    {
                        peer.closing = Some(("session_refused", Instant::now()));
                        self.mark_pending_peers.push(generation);
                    } else {
                        self.close.push((generation, "session_refused"));
                    }
                }
                if self.events.emit("session_refused", json!({"connection":generation.get().to_string(),"details":{"reason_code":reason,"code":"lifecycle_rejected_certain"}})).is_err() {
                    self.error = Some(Failure::plain("events_unavailable"));
                }
                let _ = effect.into_parts();
                SubmitResult::Accepted
            }
            Effect::Qos0Dropped {
                origin,
                target,
                reason,
            } => {
                let (origin, target, reason) = (*origin, *target, *reason);
                if self.events.emit_with("qos0_dropped",||json!({"details":{"origin":qos0_origin(origin),"target":target.map(|t|t.get().to_string()).unwrap_or_default(),"reason":reason.code()}})).is_err(){self.error=Some(Failure::plain("events_unavailable"));}
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
                if self
                    .events
                    .emit_with("delivery_discarded", || {
                        json!({
                            "operation": operation(finished),
                            "command_kind": "Finish",
                            "details": {
                                "delivery": delivery_text(delivery),
                                "target": target.get().to_string(),
                                "reason": reason.code(),
                            },
                        })
                    })
                    .is_err()
                {
                    self.error = Some(Failure::plain("events_unavailable"));
                }
                let _ = effect.into_parts();
                SubmitResult::Accepted
            }
            Effect::EmitQos0 {
                expiry,
                properties,
                connection,
                origin,
                cause,
                retain,
                topic,
                payload,
            } => {
                let (connection, origin) = (*connection, *origin);
                if self
                    .terminal_inputs
                    .len()
                    .checked_add(2 * self.frames.len())
                    .is_none_or(|used| used >= self.limits.terminal_inputs)
                    || self.frames.len() >= self.limits.frames
                {
                    return refused(effect, SubmitBlock::QueueFull);
                }
                #[cfg(feature = "test-hooks")]
                if let Qos0Origin::Consumed {
                    operation: id_op, ..
                } = origin
                    && self.hooks.pause.iter().any(|p| p == "consume.emission")
                    && !self.releasing_qos0
                {
                    if self.emission_barriers.len() >= IN_FLIGHT {
                        return refused(effect, SubmitBlock::QueueFull);
                    }
                    if self.events.emit("barrier_hit",json!({"point":"consume.emission","command_kind":"Consume","operation":operation(id_op)})).is_err(){self.error=Some(Failure::plain("events_unavailable"));return refused(effect,SubmitBlock::Closed);}
                    self.emission_barriers.push_back(HeldEmission {
                        point: "consume.emission",
                        effect,
                        since: Instant::now(),
                        released: false,
                    });
                    return SubmitResult::Accepted;
                }
                let generation = connection.generation;
                let result = if let Some(peer) = self.peers.get_mut(&generation).filter(|p| {
                    p.closing.is_none()
                        && self.bindings.get(&connection.session) == Some(&generation)
                }) {
                    let version = peer.version.unwrap_or_default();
                    match codec::encode_qos0_segments(
                        version,
                        topic,
                        payload,
                        *retain
                            && (version == Version::V5 || *cause == DeliveryCause::RetainedReplay),
                        expiry.map(|value| value.interval),
                        properties.as_deref(),
                    ) {
                        Ok(encoded) if encoded.total_len <= peer.max_packet => {
                            let write_expiry = expiry.map(|expiry| net::WriteExpiry {
                                expiry,
                                value_offset: encoded.expiry_value_offset,
                                started: false,
                            });
                            #[cfg(feature = "test-hooks")]
                            let duplicate = if MUTANT_REPLAY_QOS0
                                && matches!(origin, Qos0Origin::Consumed { .. })
                            {
                                Some((encoded.header.clone(), encoded.payload.clone()))
                            } else {
                                None
                            };
                            let result = peer.io.outgoing.try_send(net::Write {
                                expiry: write_expiry,
                                correlation: Some((id, connection)),
                                bytes: encoded.header,
                                payload: encoded.payload,
                                report: net::WriteReport::Complete,
                            });
                            #[cfg(feature = "test-hooks")]
                            if result.is_ok()
                                && let Some((bytes, payload)) = duplicate
                            {
                                let _ = peer.io.outgoing.try_send(net::Write {
                                    expiry: None,
                                    correlation: None,
                                    bytes,
                                    payload,
                                    report: net::WriteReport::None,
                                });
                            }
                            match result {
                                Ok(()) => Ok(()),
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
                            PendingFrame {
                                connection,
                                kind: "PUBLISH0",
                                packet_id: None,
                                receive_serial: None,
                                terminal: None,
                                wire_released: false,
                                was_retained: false,
                            },
                        );
                        if self.events.emit_with("frame_queued",||json!({"connection":generation.get().to_string(),"details":{"packet_kind":"PUBLISH","qos":0,"origin":qos0_origin(origin),"effect":effect_text(id)}})).is_err(){self.error=Some(Failure::plain("events_unavailable"));}
                    }
                    Err(reason) => self.push_terminal(Input::Qos0EmitRefused {
                        effect: id,
                        connection,
                        reason,
                    }),
                }
                let _ = effect.into_parts();
                SubmitResult::Accepted
            }
            Effect::Persist { command }
                if matches!(
                    command.recipe.get(),
                    Recipe::SessionLifecycle(_) | Recipe::WillPublication(_)
                ) =>
            {
                if let Recipe::SessionLifecycle(lifecycle) = command.recipe.get()
                    && let SessionAction::Open {
                        expected: Some(guard),
                        ..
                    }
                    | SessionAction::OpenAt {
                        expected: Some(guard),
                        ..
                    }
                    | SessionAction::OpenWillAt {
                        expected: Some(guard),
                        ..
                    } = &lifecycle.action
                    && self
                        .peers
                        .values()
                        .any(|p| p.session == Some(guard.session) && p.closing.is_some())
                {
                    return refused(effect, SubmitBlock::QueueFull);
                }
                self.submit_lifecycle(effect)
            }
            Effect::Persist { command } => {
                if self.inflight.len() >= self.limits.in_flight || !self.writer_reply_room() {
                    return refused(effect, SubmitBlock::InFlightLimit);
                }
                let operation_id = command.recipe.operation();
                let command_kind = crate::writer::recipe_kind(command.recipe.get());
                let details = self
                    .events
                    .records("command_submitted")
                    .then(|| recipe_details(command.recipe.get()));
                #[cfg(feature = "test-hooks")]
                if MUTANT_EARLY_QOS0
                    && let Recipe::ConsumeDelivery(c) = command.recipe.get()
                    && let Some(item) = self.work_cache.get(&c.delivery)
                    && let Some(g) = self.bindings.get(&c.target)
                    && let Some(peer) = self.peers.get(g)
                    && let Ok(bytes) = codec::encode_qos0(&item.topic, &item.payload)
                {
                    let _ = peer.io.outgoing.try_send(net::Write {
                        expiry: None,
                        correlation: None,
                        bytes: codec::FrameBytes::from(bytes),
                        payload: None,
                        report: net::WriteReport::Complete,
                    });
                }
                #[cfg(feature = "test-hooks")]
                let early_finish = if MUTANT_EARLY_FINISH
                    && let Recipe::FinishDelivery(f) = command.recipe.get()
                {
                    Some(Input::StoreResult {
                        effect: id,
                        operation: operation_id,
                        reply: StoreReply::Outcome(StoreOutcome::Committed(
                            CommitFacts::DeliveryFinished {
                                delivery: f.delivery,
                                target: f.target,
                                packet_id: f.packet_id,
                                claim: f.claim,
                            },
                        )),
                    })
                } else {
                    None
                };
                #[cfg(feature = "test-hooks")]
                if MUTANT_EARLY_OUTGOING
                    && let Recipe::ClaimDelivery(c) = command.recipe.get()
                    && let Some(item) = self.work_cache.get(&c.delivery)
                    && let Some(generation) = self.bindings.get(&c.target)
                    && let Some(peer) = self.peers.get(generation)
                    && let Ok(bytes) =
                        codec::encode_publish(c.packet_id, &item.topic, &item.payload, false)
                {
                    let _ = peer.io.outgoing.try_send(net::Write {
                        expiry: None,
                        correlation: None,
                        bytes: codec::FrameBytes::from(bytes),
                        payload: None,
                        report: net::WriteReport::Complete,
                    });
                }
                #[cfg(feature = "test-hooks")]
                if MUTANT_EARLY_ACK
                    && let mqttiumd_contract::command::Recipe::AdmitPublication(admission) =
                        command.recipe.get()
                    && admission.qos == mqttiumd_contract::packet::Qos::ExactlyOnce
                    && let Some(p) = self.peers.values().find(|p| {
                        p.receiving
                            .serial(admission.source.packet_id.get())
                            .is_some()
                    })
                {
                    let _ = p.io.outgoing.try_send(net::Write {
                        expiry: None,
                        correlation: None,
                        bytes: codec::FrameBytes::from(
                            codec::encode(OutboundFrame::Pubrec {
                                packet_id: admission.source.packet_id,
                                reason: mqttiumd_contract::packet::AckReason::Success,
                            })
                            .unwrap_or_default(),
                        ),
                        payload: None,
                        report: net::WriteReport::Complete,
                    });
                }
                #[cfg(feature = "test-hooks")]
                if MUTANT_EARLY_PUBACK
                    && let mqttiumd_contract::command::Recipe::AdmitPublication(admission) =
                        command.recipe.get()
                    && admission.qos == mqttiumd_contract::packet::Qos::AtLeastOnce
                    && let Some(p) = self.peers.values().find(|p| {
                        p.receiving
                            .serial(admission.source.packet_id.get())
                            .is_some()
                    })
                {
                    let _ = p.io.outgoing.try_send(net::Write {
                        expiry: None,
                        correlation: None,
                        bytes: codec::FrameBytes::from(
                            codec::encode(OutboundFrame::Puback {
                                packet_id: admission.source.packet_id,
                                reason: mqttiumd_contract::packet::PubackReason::Success,
                            })
                            .unwrap_or_default(),
                        ),
                        payload: None,
                        report: net::WriteReport::Complete,
                    });
                }
                if let Some((reply, targets)) = self.writer.apply_persist_direct(command) {
                    self.inputs.push_back(Input::StoreResult {
                        effect: id,
                        operation: operation_id,
                        reply,
                    });
                    if let Some(count) = self.queued_writer_replies.as_mut() {
                        *count += 1;
                    }
                    if !targets.is_empty() {
                        self.pending_work_targets.extend(targets);
                    }
                    self.direct_skip_next_deadline = matches!(
                        reply,
                        StoreReply::Outcome(
                            StoreOutcome::Committed(_) | StoreOutcome::RejectedCertain(_)
                        )
                    );
                    if self
                        .events
                        .emit_with("command_submitted", || {
                            json!({"operation":operation(operation_id),"command_kind":command_kind,"details":details.unwrap_or_else(|| json!({}))})
                        })
                        .is_err()
                    {
                        self.error = Some(Failure::plain("events_unavailable"));
                    }
                    let _ = effect.into_parts();
                    return SubmitResult::Accepted;
                }
                match self.writer.try_send(Request::Effect(effect)) {
                    Ok(()) => {
                        self.inflight.insert(id, operation_id);
                        #[cfg(feature = "test-hooks")]
                        if let Some(input) = early_finish {
                            self.inputs.push_back(input);
                            if let Some(count) = self.queued_writer_replies.as_mut() {
                                *count += 1;
                            }
                        }
                        if self
                            .events
                            .emit_with("command_submitted", || {
                                json!({"operation":operation(operation_id),"command_kind":command_kind,"details":details.unwrap_or_else(|| json!({}))})
                            })
                            .is_err()
                        {
                            self.error = Some(Failure::plain("events_unavailable"));
                        }
                        SubmitResult::Accepted
                    }
                    Err(TrySendError::Full(Request::Effect(effect))) => {
                        refused(effect, SubmitBlock::QueueFull)
                    }
                    Err(TrySendError::Disconnected(Request::Effect(effect))) => {
                        self.error = Some(Failure::plain("writer_disconnected"));
                        refused(effect, SubmitBlock::Closed)
                    }
                    _ => unreachable!(),
                }
            }
            Effect::ResolveAndFence { operation }
                if self.lifecycle_operation == Some(*operation)
                    || self.will_operation == Some(*operation) =>
            {
                self.submit_lifecycle(effect)
            }
            Effect::ResolveAndFence { operation } => {
                if self.inflight.len() >= self.limits.in_flight || !self.writer_reply_room() {
                    return refused(effect, SubmitBlock::InFlightLimit);
                }
                let op = *operation;
                match self.writer.try_send(Request::Effect(effect)) {
                    Ok(()) => {
                        self.inflight.insert(id, op);
                        SubmitResult::Accepted
                    }
                    Err(TrySendError::Full(Request::Effect(e))) => {
                        refused(e, SubmitBlock::QueueFull)
                    }
                    Err(TrySendError::Disconnected(Request::Effect(e))) => {
                        refused(e, SubmitBlock::Closed)
                    }
                    _ => unreachable!(),
                }
            }
            Effect::EmitFrame {
                connection,
                frame,
                publish_order,
                pubrel,
            } => {
                let (connection, frame) = (*connection, *frame);
                let receive_serial = *publish_order;
                let binding = match (*pubrel, frame) {
                    (Some((delivery, claim)), OutboundFrame::Pubrel { packet_id }) => {
                        let key = (connection, packet_id);
                        if self.outgoing_bindings.len() >= self.limits.outgoing_bindings {
                            return refused(effect, SubmitBlock::InFlightLimit);
                        }
                        if self.outgoing_bindings.contains_key(&key) {
                            self.error = Some(Failure::plain("pubrel_binding_conflict"));
                            return refused(effect, SubmitBlock::InFlightLimit);
                        }
                        Some((
                            key,
                            PubackTarget {
                                delivery,
                                claim,
                                frame: id,
                            },
                        ))
                    }
                    (None, _) => None,
                    (Some(_), _) => unreachable!("only PUBREL binds an outgoing occurrence here"),
                };
                let terminal = match frame {
                    OutboundFrame::Puback { .. } => Some(Terminal::Puback),
                    OutboundFrame::Pubcomp { .. } => Some(Terminal::Pubcomp),
                    OutboundFrame::Pubrec { reason, .. }
                        if reason != mqttiumd_contract::packet::AckReason::Success =>
                    {
                        Some(Terminal::RejectedPubrec)
                    }
                    _ => None,
                };
                let generation = connection.generation;
                let (kind, packet_id) = frame_info(frame);
                if self
                    .terminal_inputs
                    .len()
                    .checked_add(2 * self.frames.len())
                    .is_none_or(|used| used >= self.limits.terminal_inputs)
                    || self.frames.len() >= self.limits.frames
                    || (kind == "CONNACK"
                        && ordinary_room(
                            self.inputs,
                            self.ordinary_reserved,
                            self.limits.ordinary_inputs,
                        ) == 0)
                {
                    return refused(effect, SubmitBlock::QueueFull);
                }
                let Some(peer) = self.peers.get_mut(&generation).filter(|peer| {
                    peer.closing.is_none()
                        && self.bindings.get(&connection.session) == Some(&generation)
                }) else {
                    let _ = effect.into_parts();
                    if self.events.emit("frame_failed",json!({"connection":generation.get().to_string(),"details":{"effect":effect_text(id),"packet_kind":kind,"code":"unusable_generation"}})).is_err(){self.error=Some(Failure::plain("events_unavailable"));}
                    self.push_terminal(Input::FrameResult {
                        effect: id,
                        connection,
                        progress: FrameProgress::Failed,
                    });
                    return SubmitResult::Accepted;
                };
                // The answer of one unit request of a larger SUBSCRIBE or
                // UNSUBSCRIBE: it is kept for the one acknowledgement of
                // the whole request and reported to the core as written.
                let part_of = matches!(frame, OutboundFrame::Unsuback { .. });
                if let Some(answered) = packet_id
                    && matches!(kind, "SUBACK" | "UNSUBACK")
                    && let Some(requests) = peer.requests.as_mut()
                    && requests.awaiting
                    && requests.packet_id.get() == answered
                    && requests.unsubscribe == part_of
                {
                    let version = peer.version.unwrap_or_default();
                    let Ok(bytes) = codec::encode_for(version, frame) else {
                        self.close.push((generation, "failure_without_wire_ack"));
                        return refused(effect, SubmitBlock::Closed);
                    };
                    requests
                        .served
                        .extend_from_slice(codec::filter_ack_codes(version, &bytes));
                    requests.awaiting = false;
                    let _ = effect.into_parts();
                    self.push_terminal(Input::FrameResult {
                        effect: id,
                        connection,
                        progress: FrameProgress::Written {
                            confirmed: bytes.len() as u64,
                            total: bytes.len() as u64,
                        },
                    });
                    return SubmitResult::Accepted;
                }
                let bytes = match codec::encode_frame_bytes(peer.version.unwrap_or_default(), frame)
                {
                    Ok(bytes) if bytes.len() <= peer.max_packet => bytes,
                    Err(error) if error.code() == "AckNotEncodableInMqtt311" => {
                        let code = if matches!(
                            frame,
                            OutboundFrame::Puback {
                                reason: mqttiumd_contract::packet::PubackReason::QuotaExceeded,
                                ..
                            } | OutboundFrame::Pubrec {
                                reason: mqttiumd_contract::packet::AckReason::QuotaExceeded,
                                ..
                            }
                        ) {
                            "capacity_exceeded"
                        } else {
                            "failure_without_wire_ack"
                        };
                        self.close.push((generation, code));
                        return refused(effect, SubmitBlock::Closed);
                    }
                    _ => {
                        self.close.push((generation, "outgoing_queue_full"));
                        return refused(effect, SubmitBlock::Closed);
                    }
                };
                let report = if cfg!(any(test, feature = "test-hooks")) && terminal.is_some() {
                    net::WriteReport::CompleteAndWireProgress
                } else {
                    net::WriteReport::Complete
                };
                match peer.io.outgoing.try_send(net::Write {
                    expiry: None,
                    correlation: Some((id, connection)),
                    bytes,
                    payload: None,
                    report,
                }) {
                    Ok(()) => {
                        if let Some((key, target)) = binding {
                            self.outgoing_bindings.insert(key, target);
                        }
                        if kind == "CONNACK" {
                            // Pipeline processing starts only after the core's
                            // CONNACK occupies the ordered socket output queue.
                            peer.pending = false;
                            self.mark_pending_peers.push(generation);
                            // Readiness is effective only after the ordered
                            // CONNACK enters the bounded writer queue. The
                            // earlier cache-install fence deliberately kept
                            // this destination ineligible for discovery.
                            if self.queued_work_sessions.insert(connection.session) {
                                self.inputs.push_back(Input::WorkAvailable {
                                    session: connection.session,
                                });
                            }
                        }
                        self.frames.insert(
                            id,
                            PendingFrame {
                                connection,
                                kind,
                                packet_id,
                                receive_serial,
                                terminal,
                                wire_released: false,
                                was_retained: peer.submitted_output(id),
                            },
                        );
                        let _ = effect.into_parts();
                        if kind!="PINGRESP"&&self.events.emit_with("frame_queued",||json!({"connection":generation.get().to_string(),"details":{"packet_kind":kind,"effect":effect_text(id),"packet_id":packet_id.unwrap_or(0)}})).is_err(){self.error=Some(Failure::plain("events_unavailable"));}
                        SubmitResult::Accepted
                    }
                    Err(TrySendError::Full(_)) => {
                        self.mark_pending_peers.push(generation);
                        if peer.blocked_output.replace(id) != Some(id)
                            && self.events.emit_with("effect_retained",||json!({"connection":generation.get().to_string(),"details":{"code":"output_queue_full","effect":effect_text(id),"packet_kind":kind}})).is_err(){self.error=Some(Failure::plain("events_unavailable"));}
                        refused(effect, SubmitBlock::TransportBackpressure)
                    }
                    Err(TrySendError::Disconnected(_)) => {
                        self.close.push((generation, "outgoing_queue_full"));
                        refused(effect, SubmitBlock::Closed)
                    }
                }
            }
            Effect::CloseConnection { connection, reason } => {
                let code = match reason {
                    CloseReason::ProtocolError => "protocol_error",
                    CloseReason::SessionTakenOver => "session_taken_over",
                    CloseReason::OutcomeUnknown => "outcome_unknown",
                    CloseReason::CapacityExceeded => "capacity_exceeded",
                };
                let mut flushing = false;
                // An MQTT 5 client is told why: its session was taken over,
                // or a limit of the broker was exceeded (Quota exceeded).
                let notice = match reason {
                    CloseReason::SessionTakenOver => Some(0x8e),
                    CloseReason::CapacityExceeded => Some(0x97),
                    CloseReason::ProtocolError | CloseReason::OutcomeUnknown => None,
                };
                if let Some(notice) = notice
                    && let Some(peer) = self.peers.get_mut(connection)
                {
                    if peer.closing.is_some() {
                        flushing = true;
                    } else if peer.version != Some(Version::V311) {
                        let bytes = codec::disconnect_reason(notice);
                        if bytes.len() <= peer.max_packet
                            && peer
                                .io
                                .outgoing
                                .try_send(net::Write {
                                    expiry: None,
                                    correlation: None,
                                    bytes: codec::FrameBytes::from(bytes),
                                    payload: None,
                                    report: net::WriteReport::Complete,
                                })
                                .is_ok()
                        {
                            peer.closing = Some((code, Instant::now()));
                            self.mark_pending_peers.push(*connection);
                            flushing = true;
                        }
                    }
                }
                if !flushing {
                    self.close.push((*connection, code));
                }
                let _ = effect.into_parts();
                SubmitResult::Accepted
            }
            Effect::CollectReceipts { collection } => {
                let collection = *collection;
                match self.writer.try_send_control(ControlRequest::Collect {
                    effect: id,
                    collection,
                }) {
                    Ok(()) => {
                        let mut details = crate::writer::collection_details(collection);
                        details["effect"] = json!(effect_text(id));
                        if self
                            .events
                            .emit(
                                "receipt_collection_submitted",
                                json!({"command_kind":"ReceiptCollection","details":details}),
                            )
                            .is_err()
                        {
                            self.error = Some(Failure::plain("events_unavailable"));
                        }
                        let _ = effect.into_parts();
                        SubmitResult::Accepted
                    }
                    Err(TrySendError::Full(_)) => refused(effect, SubmitBlock::QueueFull),
                    Err(TrySendError::Disconnected(_)) => {
                        self.error = Some(Failure::plain("writer_control_disconnected"));
                        refused(effect, SubmitBlock::Closed)
                    }
                }
            }
            Effect::ArmDeadline { token, after } => {
                if std::mem::take(&mut self.direct_skip_next_deadline) {
                    let _ = effect.into_parts();
                    return SubmitResult::Accepted;
                }
                if self.timers.len() >= self.limits.timers {
                    return refused(effect, SubmitBlock::QueueFull);
                }
                let Some(due) = Instant::now().checked_add(Duration::from_millis(after.get()))
                else {
                    self.error = Some(Failure::plain("timer_overflow"));
                    return refused(effect, SubmitBlock::Closed);
                };
                self.timers.insert(*token, due);
                let _ = effect.into_parts();
                SubmitResult::Accepted
            }
            Effect::Continue { continuation } => {
                if ordinary_room(
                    self.inputs,
                    self.ordinary_reserved,
                    self.limits.ordinary_inputs,
                ) == 0
                {
                    return refused(effect, SubmitBlock::QueueFull);
                }
                self.inputs.push_front(Input::Continue {
                    continuation: *continuation,
                    budget: WorkUnits::new(65536),
                });
                let _ = effect.into_parts();
                SubmitResult::Accepted
            }
            Effect::LoadWills { .. } => {
                if self.will_reads.is_some() || !self.writer_reply_room() {
                    return refused(effect, SubmitBlock::InFlightLimit);
                }
                match self.writer.try_send(Request::Effect(effect)) {
                    Ok(()) => {
                        *self.will_reads = Some(id);
                        SubmitResult::Accepted
                    }
                    Err(TrySendError::Full(Request::Effect(effect))) => {
                        refused(effect, SubmitBlock::QueueFull)
                    }
                    Err(TrySendError::Disconnected(Request::Effect(effect))) => {
                        refused(effect, SubmitBlock::Closed)
                    }
                    _ => unreachable!(),
                }
            }
            Effect::LoadWork { session, .. } => {
                if self.work_reads.len() >= self.limits.in_flight || !self.writer_reply_room() {
                    return refused(effect, SubmitBlock::InFlightLimit);
                }
                let session = *session;
                match self.writer.try_send(Request::Effect(effect)) {
                    Ok(()) => {
                        self.work_reads.insert(id, session);
                        SubmitResult::Accepted
                    }
                    Err(TrySendError::Full(Request::Effect(e))) => {
                        refused(e, SubmitBlock::QueueFull)
                    }
                    Err(TrySendError::Disconnected(Request::Effect(e))) => {
                        self.error = Some(Failure::plain("writer_disconnected"));
                        refused(e, SubmitBlock::Closed)
                    }
                    _ => unreachable!(),
                }
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
                #[cfg(feature = "test-hooks")]
                if self
                    .hooks
                    .pause
                    .iter()
                    .any(|point| point == "publish.emission")
                    && !self.releasing_qos0
                {
                    if self.emission_barriers.len() >= IN_FLIGHT {
                        return refused(effect, SubmitBlock::QueueFull);
                    }
                    if self.events.emit("barrier_hit",json!({"point":"publish.emission","command_kind":"Claim","operation":operation(claim),"details":{"delivery":delivery_text(delivery)}})).is_err() {
                        self.error=Some(Failure::plain("events_unavailable")); return refused(effect, SubmitBlock::Closed);
                    }
                    self.emission_barriers.push_back(HeldEmission {
                        point: "publish.emission",
                        effect,
                        since: Instant::now(),
                        released: false,
                    });
                    return SubmitResult::Accepted;
                }
                if self
                    .terminal_inputs
                    .len()
                    .checked_add(2 * self.frames.len())
                    .is_none_or(|used| used >= self.limits.terminal_inputs)
                    || self.frames.len() >= self.limits.frames
                {
                    return refused(effect, SubmitBlock::QueueFull);
                }
                let generation = connection.generation;
                let Some(peer) = self.peers.get_mut(&generation).filter(|p| {
                    p.closing.is_none()
                        && self.bindings.get(&connection.session) == Some(&generation)
                }) else {
                    let _ = effect.into_parts();
                    if self
                        .events
                        .emit(
                            "frame_failed",
                            json!({
                                "connection": generation.get().to_string(),
                                "details": {"effect": effect_text(id), "packet_kind": "PUBLISH",
                                            "code": "unusable_generation"}
                            }),
                        )
                        .is_err()
                    {
                        self.error = Some(Failure::plain("events_unavailable"));
                    }
                    self.push_terminal(Input::FrameResult {
                        effect: id,
                        connection,
                        progress: FrameProgress::Failed,
                    });
                    return SubmitResult::Accepted;
                };
                let version = peer.version.unwrap_or_default();
                let retain =
                    retain && (version == Version::V5 || *cause == DeliveryCause::RetainedReplay);
                let encoded = match codec::encode_delivery_segments(
                    version,
                    (packet_id, *qos),
                    topic,
                    payload,
                    dup,
                    retain,
                    expiry.map(|value| value.interval),
                    properties.as_deref(),
                ) {
                    Ok(encoded) if encoded.total_len <= peer.max_packet => encoded,
                    _ => {
                        self.close.push((generation, "outgoing_packet_limit"));
                        return refused(effect, SubmitBlock::Closed);
                    }
                };
                let write_expiry = expiry.map(|expiry| net::WriteExpiry {
                    expiry,
                    value_offset: encoded.expiry_value_offset,
                    started: *started,
                });
                let binding_key = (connection, packet_id);
                let binding = PubackTarget {
                    delivery,
                    claim,
                    frame: id,
                };
                if self.outgoing_bindings.contains_key(&binding_key) {
                    self.error = Some(Failure::plain("outgoing_binding_conflict"));
                    return refused(effect, SubmitBlock::InFlightLimit);
                }
                if self.outgoing_bindings.len() >= self.limits.outgoing_bindings {
                    return refused(effect, SubmitBlock::InFlightLimit);
                }
                // Register before the network task can observe this frame.
                // A rejected queue submission rolls this precise entry back.
                self.outgoing_bindings.insert(binding_key, binding);
                let track_written = write_expiry.is_some()
                    || peer.blocked_output == Some(id)
                    || self.events.records("frame_written")
                    || cfg!(any(test, feature = "test-hooks"));
                match peer.io.outgoing.try_send(net::Write {
                    expiry: write_expiry,
                    correlation: track_written.then_some((id, connection)),
                    bytes: encoded.header,
                    payload: encoded.payload,
                    report: if track_written {
                        net::WriteReport::Complete
                    } else {
                        net::WriteReport::None
                    },
                }) {
                    Ok(()) => {
                        let was_retained = peer.submitted_output(id);
                        if track_written {
                            self.frames.insert(
                                id,
                                PendingFrame {
                                    connection,
                                    kind: "PUBLISH",
                                    packet_id: Some(packet_id.get()),
                                    receive_serial: None,
                                    terminal: None,
                                    wire_released: false,
                                    was_retained,
                                },
                            );
                        }
                        let _ = effect.into_parts();
                        if self.events.emit_with("frame_queued",||json!({"connection":generation.get().to_string(),"details":{"packet_kind":"PUBLISH","effect":effect_text(id),"delivery":delivery_text(delivery),"target":connection.session.get().to_string(),"packet_id":packet_id.get(),"claim":operation_text(claim),"dup":dup,"retain":retain}})).is_err(){self.error=Some(Failure::plain("events_unavailable"));}
                        SubmitResult::Accepted
                    }
                    Err(TrySendError::Full(_)) => {
                        self.outgoing_bindings.remove(&binding_key);
                        self.mark_pending_peers.push(generation);
                        if peer.blocked_output.replace(id) != Some(id)
                            && self.events.emit_with("effect_retained",||json!({"connection":generation.get().to_string(),"details":{"code":"output_queue_full","effect":effect_text(id),"packet_kind":"PUBLISH"}})).is_err(){self.error=Some(Failure::plain("events_unavailable"));}
                        refused(effect, SubmitBlock::TransportBackpressure)
                    }
                    Err(TrySendError::Disconnected(_)) => {
                        self.outgoing_bindings.remove(&binding_key);
                        self.close.push((generation, "outgoing_queue_full"));
                        refused(effect, SubmitBlock::Closed)
                    }
                }
            }
        }
    }
}
fn input_connection(input: &Input) -> Option<ConnectionGeneration> {
    match input {
        Input::Packet { connection, .. } => Some(*connection),
        Input::ObservedPuback { connection, .. } | Input::ObservedQos2Ack { connection, .. } => {
            Some(connection.generation)
        }
        _ => None,
    }
}
fn puback_fields(
    connection: ConnectionKey,
    packet_id: OutboundPacketId,
    reason: u8,
    target: Option<PubackTarget>,
    observation: Option<u64>,
    code: &str,
) -> serde_json::Value {
    let target=target.map(|target|json!({"delivery_id":{"publication_id":{"incarnation":target.delivery.publication.incarnation().get().to_string(),"epoch":target.delivery.publication.epoch().get().to_string(),"owner":target.delivery.publication.owner().get().to_string(),"sequence":target.delivery.publication.sequence().get().to_string()},"index":target.delivery.index},"claim_operation_id":operation(target.claim),"frame_effect_id":{"epoch":target.frame.epoch.get().to_string(),"owner":target.frame.owner.get().to_string(),"sequence":target.frame.sequence.get().to_string()}}));
    let mut puback = json!({"connection":{"session":connection.session.get().to_string(),"generation":connection.generation.get().to_string()},"packet_id":packet_id.get(),"reason_code":reason,"target":target});
    if let Some(sequence) = observation {
        puback["source_observation_sequence"] = json!(sequence.to_string());
    }
    json!({"connection":connection.generation.get().to_string(),"puback":puback,"details":{"authority":"reader_dequeue","code":code}})
}
fn frame_info(frame: OutboundFrame) -> (&'static str, Option<u16>) {
    match frame {
        OutboundFrame::Connack { .. } => ("CONNACK", None),
        OutboundFrame::Pingresp => ("PINGRESP", None),
        OutboundFrame::Puback { packet_id, .. } => ("PUBACK", Some(packet_id.get())),
        OutboundFrame::Pubrec { packet_id, .. } => ("PUBREC", Some(packet_id.get())),
        OutboundFrame::Pubcomp { packet_id, .. } => ("PUBCOMP", Some(packet_id.get())),
        OutboundFrame::Pubrel { packet_id } => ("PUBREL", Some(packet_id.get())),
        OutboundFrame::Suback { packet_id, .. } => ("SUBACK", Some(packet_id.get())),
        OutboundFrame::Unsuback { packet_id, .. } => ("UNSUBACK", Some(packet_id.get())),
    }
}
/// PUBLISH, SUBSCRIBE and UNSUBSCRIBE ask the broker for new work. Every
/// other packet completes or ends work it already holds.
fn business_header(first: u8) -> bool {
    matches!(first >> 4, 3 | 8 | 10)
}
fn business_connection(input: &Input) -> Option<ConnectionGeneration> {
    match input {
        Input::Packet {
            connection,
            packet:
                InboundPacket::Publish { .. }
                | InboundPacket::Subscribe { .. }
                | InboundPacket::Unsubscribe { .. },
            ..
        } => Some(*connection),
        _ => None,
    }
}
fn hex_domain(domain: [u8; 32]) -> String {
    domain.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn delivery_text(id: mqttiumd_contract::ids::DeliveryId) -> String {
    format!(
        "{}/{}/{}/{}/{}",
        id.publication.incarnation().get(),
        id.publication.epoch().get(),
        id.publication.owner().get(),
        id.publication.sequence().get(),
        id.index
    )
}
fn expiry_json(expiry: SessionExpiry) -> serde_json::Value {
    match expiry {
        SessionExpiry::OnDisconnect => json!("OnDisconnect"),
        SessionExpiry::Never => json!("Never"),
        SessionExpiry::After(seconds) => json!({"After":seconds}),
    }
}
fn recipe_details(recipe: &Recipe) -> serde_json::Value {
    let recipe = recipe.inner_publication();
    if let Recipe::ExpiringPublication(value) = recipe {
        let (publication, topic) = match &value.admission {
            mqttiumd_contract::message_expiry::ExpiringAdmission::Qos12(v) => {
                (v.admission.publication, &v.admission.topic)
            }
            mqttiumd_contract::message_expiry::ExpiringAdmission::Qos0(v) => {
                (v.publication, &v.topic)
            }
        };
        return json!({"publication":format!("{:?}",publication),"topic":String::from_utf8_lossy(topic.as_slice()).chars().take(256).collect::<String>(),
            "message_expiry_domain":hex_domain(value.expiry.observed.domain),"message_expiry_tick":value.expiry.observed.tick.to_string(),"message_expiry_interval":value.expiry.interval});
    }
    match recipe {
        Recipe::WillPublication(value) => {
            json!({"will":operation_text(value.will),"publication":format!("{:?}",value.publication),"destinations":value.targets.len(),"topic_bytes":value.topic.len(),"payload_bytes":value.payload.length.get()})
        }
        Recipe::SessionLifecycle(lifecycle) => match &lifecycle.action {
            SessionAction::Open {
                expected,
                clean_start,
                expiry,
                ..
            }
            | SessionAction::OpenWillAt {
                expected,
                clean_start,
                expiry,
                ..
            }
            | SessionAction::OpenAt {
                expected,
                clean_start,
                expiry,
                ..
            } => {
                json!({"action":"Open","clean_start":clean_start,"expiry":expiry_json(*expiry),"expected_session":expected.map(|g|g.session.get().to_string())})
            }
            SessionAction::EndWill {
                guard,
                expiry,
                observed_at,
                publish,
            } => {
                json!({"action":"EndWill","session":guard.session.get().to_string(),"opened_by":guard.opened_by.map(operation_text),"expiry":expiry_json(*expiry),"observed_at":observed_at.to_string(),"publish":publish})
            }
            SessionAction::Detach {
                guard,
                expiry,
                observed_at,
            } => {
                json!({"action":"Detach","session":guard.session.get().to_string(),"expiry":expiry_json(*expiry),"observed_at":observed_at.to_string()})
            }
            SessionAction::Expire {
                guard,
                detached,
                now,
            } => {
                json!({"action":"Expire","session":guard.session.get().to_string(),"detached_by":operation(detached.operation),"deadline":detached.deadline.map(|n|n.to_string()),"now":now.to_string()})
            }
            SessionAction::Close { guard, reason } => {
                json!({"action":"Close","session":guard.session.get().to_string(),"opened_by":guard.opened_by.map(operation_text).unwrap_or_default(),"reason":format!("{reason:?}")})
            }
        },
        Recipe::ConsumeDelivery(c) => {
            json!({"delivery":delivery_text(c.delivery),"target":c.target.get().to_string()})
        }
        Recipe::ClaimDelivery(c) => {
            json!({"delivery":delivery_text(c.delivery),"target":c.target.get().to_string(),"packet_id":c.packet_id.get()})
        }
        Recipe::FinishDelivery(c) => {
            json!({"delivery":delivery_text(c.delivery),"target":c.target.get().to_string(),"packet_id":c.packet_id.get(),"claim":operation_text(c.claim)})
        }
        _ => json!({}),
    }
}
fn qos0_origin(origin: Qos0Origin) -> String {
    match origin {
        Qos0Origin::Volatile { publication } => format!(
            "volatile/{}/{}/{}",
            publication.source.session.get(),
            publication.source.generation.get(),
            publication.ingress_order
        ),
        Qos0Origin::Consumed {
            delivery,
            operation: id,
        } => format!(
            "consumed/{}/{}",
            delivery_text(delivery),
            operation_text(id)
        ),
    }
}
fn operation_text(id: OperationId) -> String {
    format!(
        "{}/{}/{}/{}",
        id.incarnation().get(),
        id.epoch().get(),
        id.owner().get(),
        id.sequence().get()
    )
}
fn effect_text(id: EffectId) -> String {
    format!(
        "{}/{}/{}",
        id.epoch.get(),
        id.owner.get(),
        id.sequence.get()
    )
}
fn event_failure(_: std::io::Error) -> Failure {
    Failure::plain("events_unavailable")
}
fn nz(v: u32) -> NonZeroU32 {
    NonZeroU32::new(v).expect("fixed nonzero budget")
}

fn normalize_path_for_collision(path: &std::path::Path) -> PathBuf {
    if let (Some(parent), Some(name)) = (path.parent(), path.file_name()) {
        let dir = if parent.as_os_str().is_empty() {
            std::path::Path::new(".")
        } else {
            parent
        };
        if let Ok(canon) = std::fs::canonicalize(dir) {
            return canon.join(name);
        }
    }
    path.to_path_buf()
}

fn path_with_suffix(path: &std::path::Path, suffix: &str) -> PathBuf {
    let mut os = path.as_os_str().to_os_string();
    os.push(suffix);
    PathBuf::from(os)
}

fn paths_collide_with_db(events: &std::path::Path, db: &std::path::Path) -> bool {
    let ev = normalize_path_for_collision(events);
    let ev_prev = path_with_suffix(&ev, ".previous");
    let db_norm = normalize_path_for_collision(db);
    let db_paths = [
        db_norm.clone(),
        path_with_suffix(&db_norm, "-wal"),
        path_with_suffix(&db_norm, "-shm"),
        path_with_suffix(&db_norm, ".mlog"),
        path_with_suffix(&db_norm, ".writer-lock"),
    ];
    db_paths.iter().any(|p| *p == ev || *p == ev_prev)
}

pub fn run(config: Config) -> Result<(), Failure> {
    let disk_db_active = matches!(
        config.storage_mode,
        crate::writer::StorageMode::Durable
            | crate::writer::StorageMode::Log
            | crate::writer::StorageMode::Volatile {
                snapshot_interval: Some(_),
            }
    ) || config.require_existing_db;
    if disk_db_active && paths_collide_with_db(&config.events, &config.db) {
        return Err(Failure::plain("events_db_path_collision"));
    }
    let events = Events::open_at(&config.events, config.event_level).map_err(event_failure)?;
    let security = match &config.tls {
        Some(files) => Security::load(files).map(Some).map_err(Failure::plain),
        None if config.listen.ip().is_loopback() => Ok(None),
        None => Err(Failure::plain("cleartext_requires_loopback")),
    };
    let security = match security {
        Ok(security) => security,
        Err(failure) => {
            let _ = events.emit("startup_or_runtime_refused", failure.fields());
            return Err(failure);
        }
    };
    let cancel = Arc::new(AtomicBool::new(false));
    let signal = cancel.clone();
    ctrlc::set_handler(move || signal.store(true, Ordering::Relaxed))
        .map_err(|_| Failure::plain("signal_handler"))?;
    events.emit("starting",json!({"details":{"test_hooks":cfg!(feature="test-hooks"),"sqlite_kill_points":mqttiumd_store_sqlite::KILL_POINTS_ENABLED}})).map_err(event_failure)?;
    let policy = security
        .as_ref()
        .map(|security| security.policy.storage().clone());
    let use_direct = matches!(
        config.storage_mode,
        crate::writer::StorageMode::Volatile { .. }
    ) && config.hooks.directory.is_none();
    if use_direct {
        let (mut direct, boot) = DirectWriter::open(
            config.db,
            events.clone(),
            config.hooks.clone(),
            cancel.clone(),
            policy,
            config.require_existing_db,
            config.storage_mode,
        )?;
        let result = run_inner(
            &config.listen,
            config.hooks,
            &events,
            cancel.clone(),
            StoreHandle::Direct(&mut direct),
            Some(boot),
            security,
            config.limits,
        );
        if let Err(failure) = &result {
            let _ = events.emit("startup_or_runtime_refused", failure.fields());
        }
        cancel.store(true, Ordering::Relaxed);
        let writer_result = direct.finish();
        if let Err(failure) = &writer_result {
            let _ = events.emit("startup_or_runtime_refused", failure.fields());
        } else {
            events.emit("stopped",json!({"details":{"writer_released":true,"accepted_commands_certainly_rejected":false}})).map_err(event_failure)?;
        }
        return result.and(writer_result);
    }
    let mut writer = Writer::spawn_configured(
        config.db,
        events.clone(),
        config.hooks.clone(),
        cancel.clone(),
        policy,
        config.require_existing_db,
        config.storage_mode,
    )?;
    let result = run_inner(
        &config.listen,
        config.hooks,
        &events,
        cancel.clone(),
        StoreHandle::Threaded(&mut writer),
        None,
        security,
        config.limits,
    );
    if let Err(failure) = &result {
        let _ = events.emit("startup_or_runtime_refused", failure.fields());
    }
    cancel.store(true, Ordering::Relaxed);
    // Stop is queued after already accepted work. No accepted command is called rejected.
    if !writer.stop_within(Instant::now() + Duration::from_secs(5)) {
        let _ = events.emit(
            "shutdown_incomplete",
            json!({"details":{"code":"writer_deadline","accepted_outcome":"unknown"}}),
        );
        return Err(Failure::plain("writer_shutdown_deadline"));
    }
    let writer_result = writer.join_finished();
    if let Err(failure) = &writer_result {
        let _ = events.emit("startup_or_runtime_refused", failure.fields());
    } else {
        events.emit("stopped",json!({"details":{"writer_released":true,"accepted_commands_certainly_rejected":false}})).map_err(event_failure)?;
    }
    result.and(writer_result)
}
/// Accepts on its own thread so that the reactor never polls the listener.
/// The lane is bounded: a reactor that does not take connections leaves
/// them in the kernel backlog.
struct Acceptor {
    accepted: std::sync::mpsc::Receiver<Result<std::net::TcpStream, ()>>,
    handle: thread::JoinHandle<()>,
}

impl Acceptor {
    fn spawn(
        listener: TcpListener,
        cancel: Arc<AtomicBool>,
        wake: crate::wake::Wake,
    ) -> Result<Self, Failure> {
        listener
            .set_nonblocking(false)
            .map_err(|_| Failure::plain("listen_blocking"))?;
        let (sender, accepted) = std::sync::mpsc::sync_channel(4);
        let handle = thread::Builder::new()
            .name("mqttiumd-acceptor".into())
            .spawn(move || {
                while !cancel.load(Ordering::Relaxed) {
                    let item = match listener.accept() {
                        Ok((stream, _)) => Ok(stream),
                        Err(error)
                            if matches!(
                                error.kind(),
                                std::io::ErrorKind::Interrupted
                                    | std::io::ErrorKind::ConnectionAborted
                                    | std::io::ErrorKind::WouldBlock
                            ) =>
                        {
                            continue;
                        }
                        Err(_) => Err(()),
                    };
                    if cancel.load(Ordering::Relaxed) {
                        break;
                    }
                    let failed = item.is_err();
                    if sender.send(item).is_err() {
                        break;
                    }
                    wake.wake();
                    if failed {
                        break;
                    }
                }
            })
            .map_err(|_| Failure::plain("acceptor_spawn"))?;
        Ok(Self { accepted, handle })
    }

    /// The cancel flag is already set. One local connection ends the
    /// blocking accept; the listener closes with the thread.
    fn stop(self, address: SocketAddr) {
        drop(self.accepted);
        let _ = std::net::TcpStream::connect_timeout(&address, Duration::from_millis(200));
        let deadline = Instant::now() + Duration::from_secs(1);
        while !self.handle.is_finished() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(2));
        }
        if self.handle.is_finished() {
            let _ = self.handle.join();
        }
    }
}

fn check_startup_cancel(cancel: &AtomicBool) -> Result<(), Failure> {
    if cancel.load(Ordering::Relaxed) {
        Err(Failure::plain("startup_cancelled"))
    } else {
        Ok(())
    }
}
#[allow(clippy::too_many_arguments)]
fn run_inner(
    listen: &SocketAddr,
    hooks: Hooks,
    events: &Events,
    cancel: Arc<AtomicBool>,
    mut writer: StoreHandle<'_>,
    direct_boot: Option<Boot>,
    security: Option<Security>,
    limits: Limits,
) -> Result<(), Failure> {
    let _ = &hooks;
    let (boot, wake) = match (&mut writer, direct_boot) {
        (StoreHandle::Direct(_), Some(boot)) => (boot, crate::wake::Wake::new()),
        (StoreHandle::Threaded(w), None) => {
            (w.await_boot(Duration::from_secs(10))?, w.wake.clone())
        }
        _ => return Err(Failure::plain("startup_protocol")),
    };
    let epoch = boot.grant.epoch();
    let incarnation = boot.grant.incarnation();
    let collect_closed = boot.collect_closed;
    let sqlite = boot.sqlite.clone();
    let storage_mode = boot.storage_mode;
    let mut authority = OwnerAuthority::from_grant(boot.grant, NonZeroU32::MIN)
        .map_err(|_| Failure::plain("authority_allocation"))?;
    let outbox = authority
        .create_outbox(
            LogicalOwnerId::new(0),
            NonZeroUsize::new(limits.effect_box).expect("nonzero"),
            nz(65536),
        )
        .map_err(|_| Failure::plain("outbox_creation"))?;
    // Every session can hold a window of exchanges only its client ends:
    // the table is sized for the sessions, not for the connections (record
    // 0061).
    let max_exchanges = crate::writer::SESSIONS * crate::receiving::MAXIMUM;
    let mut owner = Owner::new(
        OwnerConfig {
            max_sessions: nz(crate::writer::SESSIONS as u32),
            max_routes: nz(mqttiumd_contract::pubsub::MAX_SUBSCRIPTIONS as u32),
            max_exchanges: nz(max_exchanges as u32),
            session_qos2_exchanges: nz(crate::receiving::MAXIMUM as u32),
            account: AccountId::new(1),
            payload_bytes: PayloadBytes::new(64 * 1024 * 1024),
            resolution_delay: LogicalDuration::new(10000),
            retained_operations: nz(128),
            remembered_connections: nz(8),
        },
        outbox,
    )
    .map_err(|_| Failure::plain("owner_creation"))?;
    #[cfg(feature = "test-hooks")]
    let enable_inline_claims = hooks.directory.is_none();
    #[cfg(not(feature = "test-hooks"))]
    let enable_inline_claims = true;
    if enable_inline_claims {
        owner.enable_inline_claims();
    }
    // The worker handle stays in outer run for cleanup even if recovery fails.
    let expiry_timers = boot
        .sessions
        .values()
        .filter_map(|s| {
            s.detached.filter(|d| d.deadline.is_some()).map(|detached| {
                (
                    s.id,
                    ExpiryTimer {
                        guard: SessionGuard {
                            session: s.id,
                            opened_by: s.opened_by,
                        },
                        detached,
                        queued: false,
                    },
                )
            })
        })
        .collect();
    let mut runtime = Runtime {
        progress: 0,
        wake,
        limits,
        security,
        message_expiry_request: None,
        message_expiry_retry: false,
        message_expiry_retry_attempts: 0,
        message_expiry_observation: None,
        message_expiry_after: None,
        message_expiry_scan: Instant::now(),
        message_expiry_input: None,
        message_last_time: None,
        last_message_clock_sync: Instant::now(),
        message_clock_needed: true,
        clock: boot.clock,
        clock_anchor: boot.clock_anchor,
        clock_checkpoint: None,
        last_clock_checkpoint: Instant::now(),
        checkpoint_degraded: false,
        expiry_timers,
        expiry_inputs: VecDeque::new(),
        last_expiry_inventory: None,
        owner,
        _authority: authority,
        writer,
        events: events.clone(),
        peers: FlatMap::new(),
        ready_peers: net::ReadyQueue::new(),
        ready_raw: Vec::new(),
        pending_peers: FlatSet::new(),
        capacity_waiters: 0,
        retiring: Vec::new(),
        session_ids: boot.sessions.values().map(|s| s.id).collect(),
        sessions: boot.sessions,
        session_requests: BTreeMap::new(),
        will_connects: BTreeMap::new(),
        will_reads: None,
        last_will_deferred: None,
        next_session_request: 0,
        generation: ConnectionGeneration::new(0),
        peer_cursor: 0,
        peer_keys: Vec::new(),
        last_peer_deadline_scan: Instant::now(),
        inputs: VecDeque::new(),
        control_inputs: VecDeque::new(),
        closing_inputs: VecDeque::new(),
        terminal_inputs: VecDeque::new(),
        terminal_effects: ScopedFlatMap::new(),
        deferred_inputs: VecDeque::new(),
        deferred_by_conn: FlatMap::new(),
        effects_awaited: false,
        deferred_retries: 0,
        credit_progress: Instant::now(),
        timers: BTreeMap::new(),
        inflight: ScopedFlatMap::new(),
        lifecycle_inflight: BTreeMap::new(),
        last_session_change: None,
        session_cardinality_dirty: false,
        last_session_cardinality: None,
        work_reads: ScopedFlatMap::new(),
        pending_work: VecDeque::new(),
        pending_work_set: BTreeSet::new(),
        queued_work_sessions: FlatSet::new(),
        cached_bindings: FlatMap::new(),
        bindings_dirty: true,
        stale_frames_dirty: false,
        timers_dirty: false,
        receipt_pressure_blocked: false,
        last_work_scan: Instant::now(),
        last_will_scan: Instant::now(),
        last_maintenance_flush: Instant::now(),
        maintenance_needed: false,
        #[cfg(feature = "test-hooks")]
        work_cache: BTreeMap::new(),
        #[cfg(feature = "test-hooks")]
        deferred_work: VecDeque::new(),
        #[cfg(feature = "test-hooks")]
        emission_barriers: VecDeque::new(),
        #[cfg(feature = "test-hooks")]
        last_consume_result: None,
        started: Instant::now(),
        #[cfg(feature = "test-hooks")]
        captured: None,
        #[cfg(feature = "test-hooks")]
        hooks,
        #[cfg(feature = "test-hooks")]
        deferred_ack: None,
        #[cfg(feature = "test-hooks")]
        deferred_ack_used: false,
        frames: ScopedFlatMap::new(),
        outgoing_bindings: FlatMap::new(),
        puback_observation: 0,
        #[cfg(feature = "test-hooks")]
        copied_puback: None,
        #[cfg(feature = "test-hooks")]
        deferred_outgoing_written: None,
        #[cfg(feature = "test-hooks")]
        copied_outgoing_frame: None,
        #[cfg(feature = "test-hooks")]
        last_finish_result: None,
        #[cfg(feature = "test-hooks")]
        copied_session_change: None,
        #[cfg(feature = "test-hooks")]
        copied_session_close: None,
    };
    let result = (|| {
        let mut cursor = None;
        let mut rows = 0usize;
        let mut saw_epoch = false;
        loop {
            check_startup_cancel(&cancel)?;
            let response = match runtime.writer.exec_recovery(cursor)? {
                Some(reply) => reply,
                None => runtime
                    .writer
                    .await_recovery_reply(&cancel, Duration::from_secs(10))?,
            };
            let Response::Recovery(page) = response else {
                return Err(Failure::plain("recovery_protocol"));
            };
            let page = page.map_err(|refused| match refused {
                mqttiumd_store_api::RecoveryRefused::Read(e) => {
                    Failure::read(e, ReadPhase::Recovery)
                }
                _ => Failure::plain(refused.code()),
            })?;
            check_startup_cancel(&cancel)?;
            rows = rows
                .checked_add(page.facts.as_slice().len())
                .filter(|v| *v <= 65536)
                .ok_or_else(|| Failure::read(ReadError::LimitExceeded, ReadPhase::Recovery))?;
            for fact in page.facts.as_slice() {
                if let RecoveredFact::Epoch {
                    incarnation: found,
                    epoch: found_epoch,
                } = fact
                {
                    if *found != incarnation || *found_epoch != epoch {
                        return Err(Failure::plain("recovered_epoch_mismatch"));
                    }
                    saw_epoch = true;
                }
            }
            let last = page.next.is_none();
            cursor = page.next;
            runtime.queue(Input::Recovery {
                facts: page.facts,
                last,
            })?;
            runtime.drain_inputs()?;
            if !runtime.inputs.is_empty() {
                return Err(Failure::plain("recovery_incomplete"));
            }
            if last {
                break;
            }
        }
        if !saw_epoch || runtime.owner.is_recovering() {
            return Err(Failure::plain("recovery_incomplete"));
        }
        if collect_closed {
            let collection = ClosedReceiptCollection {
                through: mqttiumd_contract::ids::RunEpoch::new(
                    epoch
                        .get()
                        .checked_sub(1)
                        .ok_or_else(|| Failure::plain("epoch_underflow"))?,
                ),
                limit: 128,
            };
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut completed = false;
            for _ in 0..128 {
                check_startup_cancel(&cancel)?;
                if Instant::now() >= deadline {
                    return Err(Failure::plain("startup_collection_deadline"));
                }
                let reply = match runtime.writer.exec_closed_collection(collection)? {
                    Some(reply) => reply,
                    None => runtime.writer.await_collection_reply(&cancel, deadline)?,
                };
                let ControlResponse::Closed {
                    collection: found,
                    result,
                } = reply
                else {
                    return Err(Failure::plain("startup_collection_protocol"));
                };
                if found != collection {
                    return Err(Failure::plain("startup_collection_protocol"));
                }
                match result? {
                    ClosedReceiptReply::Applied {
                        through,
                        removed,
                        removed_origins,
                        retained,
                        more,
                    } => {
                        events.emit("closed_receipts_collected",json!({"details":{"through":through.get().to_string(),"removed":removed,"removed_origins":removed_origins,"retained":retained.to_string(),"more":more}})).map_err(event_failure)?;
                        if !more {
                            completed = true;
                            break;
                        }
                    }
                    ClosedReceiptReply::PendingOrUnknown => thread::sleep(Duration::from_millis(2)),
                    ClosedReceiptReply::Refused(_) => {
                        return Err(Failure::plain("startup_collection_refused"));
                    }
                }
            }
            if !completed {
                return Err(Failure::plain("startup_collection_budget"));
            }
        }
        // Empty-prefix registration is itself an exact owner transition. No
        // listener or business command exists until its durable reply is applied.
        check_startup_cancel(&cancel)?;
        runtime.queue(Input::MaintainReceipts)?;
        let deadline = Instant::now() + Duration::from_secs(30);
        while !runtime.owner.collection_ready() {
            if Instant::now() >= deadline {
                return Err(Failure::plain("receipt_registration_deadline"));
            }
            runtime.writer_results()?;
            check_startup_cancel(&cancel)?;
            runtime.due_deadlines()?;
            runtime.drain_inputs()?;
            runtime.dispatch()?;
            thread::sleep(Duration::from_millis(1));
        }
        runtime.emit_session_clock_anchor()?;
        let now = runtime.message_now()?;
        events.emit("message_expiry_clock", json!({"details":{
            "domain":hex_domain(now.domain),"tick":now.tick.to_string(),"origin_status":runtime.clock.origin_status(),
            "policy":"quarantine-on-unknown-origin","time_unit":"nanoseconds","scan_ms":100,"batch":4,
            "started_proof":"positive-socket-write-then-durable-marker","terminal_input_slots":limits.terminal_inputs,
            "socket_write_receipt_slots":net::WRITTEN_SLOTS
        }})).map_err(event_failure)?;
        events.emit("receipt_policy",json!({"details":{"receipt_window":mqttiumd_contract::receipts::RECEIPT_WINDOW,"receipt_data_window":mqttiumd_contract::receipts::RECEIPT_DATA_WINDOW,"receipt_control_reserve":mqttiumd_contract::receipts::RECEIPT_CONTROL_RESERVE,"receipt_collection_rows":mqttiumd_contract::receipts::MAX_COLLECTION_ROWS,"receipt_collection_bytes":mqttiumd_store_api::ReadBudget::COLLECTION.max_bytes,"receipt_collection_trigger":mqttiumd_contract::receipts::RECEIPT_COLLECTION_TRIGGER,"max_receipt_origins":mqttiumd_contract::receipts::MAX_RECEIPT_ORIGINS,"startup_origins_trigger":32,"startup_receipts_trigger":4096,"startup_closed_collection":collect_closed,"control_request_slots":crate::writer::CONTROL_QUEUE,"control_reply_slots":crate::writer::CONTROL_QUEUE,"control_input_slots":crate::writer::CONTROL_QUEUE}})).map_err(event_failure)?;
        if let Some(sqlite) = &sqlite {
            events.emit("sqlite_configuration",json!({"details":{"journal_mode":sqlite.journal_mode,"synchronous":sqlite.synchronous,"wal_autocheckpoint":sqlite.wal_autocheckpoint,"checkpoint_policy":sqlite.checkpoint_policy,"sqlite_version":sqlite.sqlite_version,"user_version":sqlite.user_version,"page_size":sqlite.page_size,"kill_points":sqlite.kill_points,"hooks_active":false,"install_order":"sqlite-autocheckpoint-before-optional-business-hook"}})).map_err(event_failure)?;
        }
        let sqlite_schema = sqlite.as_ref().map_or_else(
            || {
                if runtime.security.is_some() {
                    mqttiumd_store_sqlite::SECURE_SCHEMA_VERSION
                } else {
                    mqttiumd_store_sqlite::SCHEMA_VERSION
                }
            },
            |s| s.user_version,
        );
        check_startup_cancel(&cancel)?;
        let listener =
            TcpListener::bind(listen).map_err(|_| Failure::plain("listen_unavailable"))?;
        let address = listener
            .local_addr()
            .map_err(|_| Failure::plain("listen_address"))?;
        check_startup_cancel(&cancel)?;
        let profile = if runtime.security.is_some() {
            "mtls-will-pubsub-v1"
        } else {
            "tcp-will-pubsub-v1"
        };
        let mut readiness = json!({"details":{"address":address.to_string(),"epoch":epoch.get().to_string(),"incarnation":incarnation.get().to_string(),"max_connections":limits.connections,"max_sessions":crate::writer::SESSIONS,"max_packet_bytes":codec::MAX_PACKET_BYTES,"max_client_id_bytes":crate::writer::MAX_CLIENT,"max_topic_bytes":crate::writer::MAX_TOPIC,"max_payload_bytes":crate::writer::MAX_PAYLOAD,"receive_maximum":crate::receiving::MAXIMUM,"max_exchange_slots":max_exchanges,"input_slots":net::INPUT_SLOTS,"output_slots":net::OUTPUT_SLOTS,"writer_slots":crate::writer::QUEUE,"commands_in_flight":limits.in_flight,"max_timers":limits.timers,"receipt_slots":crate::writer::limits().receipt_slots.get(),"partial_packet_ms":5000,"connect_ms":3000,"write_ms":250,"remembered_connections":8,"max_subscriptions":mqttiumd_contract::pubsub::MAX_SUBSCRIPTIONS,"max_targets":mqttiumd_contract::retained::MAX_RETAIN_MASK_TARGETS,"outgoing_per_connection":8,"outgoing_metadata":limits.outgoing_bindings,"outgoing_packet_identifier_max":65535,"outgoing_packet_identifier_probe_limit":129,"outgoing_payload_bytes":64*1024*1024,"work_page_items":8,"work_scan_ms":100,"wildcard_subscriptions":true,"no_local":true,"shared_subscriptions":false,"subscription_identifiers":false,"sqlite_schema":sqlite_schema,"retained_available":true,"retained_topics":mqttiumd_contract::retained::MAX_RETAINED_TOPICS,"retained_bytes":mqttiumd_contract::retained::MAX_RETAINED_BYTES,"max_replay_candidates":mqttiumd_contract::retained::MAX_REPLAYS,"profile":profile}});
        if let Some(details) = readiness["details"].as_object_mut() {
            details.insert("storage_mode".into(), json!(storage_mode.mode_name()));
            details.insert(
                "durability_guarantee".into(),
                json!(storage_mode.durability_guarantee()),
            );
            details.insert(
                "snapshot_interval_secs".into(),
                json!(storage_mode.snapshot_interval_secs()),
            );
        }
        if let Some(security) = &runtime.security {
            events.emit("tls_profile", json!({"details":{"version":"TLS1.3","client_certificate":"required",
                "policy_fingerprint":hex_domain(security.policy.storage().fingerprint),"max_handshakes":security::MAX_UNAUTHENTICATED,
                "handshake_ms":security::HANDSHAKE_TIMEOUT.as_millis() as u64,"handshake_bytes":security::HANDSHAKE_BYTES,
                "certificate_bytes":security::CERT_BYTES,"chain_bytes":security::CHAIN_BYTES,"chain_certificates":security::CHAIN_CERTS,
                "tls_buffer_bytes":security::TLS_BUFFER_BYTES,"resumption":false,"early_data":false}})).map_err(event_failure)?;
        }
        events.emit("will_capabilities",json!({"details":{"will":true,"will_qos":1,"will_retain":true,"will_delay_interval":0,"will_message_expiry":false,"max_will_intents":mqttiumd_contract::will::MAX_WILL_INTENTS,"max_will_bytes":mqttiumd_contract::will::MAX_WILL_BYTES,"max_will_receipt_bytes":8388608}})).map_err(event_failure)?;
        events.emit("ready", readiness).map_err(event_failure)?;
        match storage_mode {
            crate::writer::StorageMode::Durable => {
                println!("mqttiumd ready {address} ({profile})");
            }
            crate::writer::StorageMode::Log => {
                println!(
                    "mqttiumd ready {address} ({profile}, storage=log, durability={})",
                    storage_mode.durability_guarantee()
                );
            }
            crate::writer::StorageMode::Volatile { .. } => {
                println!(
                    "mqttiumd ready {address} ({profile}, storage=volatile, durability={})",
                    storage_mode.durability_guarantee()
                );
            }
        }
        let acceptor = Acceptor::spawn(listener, cancel.clone(), runtime.wake.clone())?;
        let served = (|| {
            let mut counters_due = Instant::now() + COUNTERS_INTERVAL;
            while !cancel.load(Ordering::Relaxed) {
                let before = runtime.progress;
                for _ in 0..32 {
                    if !runtime.admits_handshake() {
                        break;
                    }
                    match acceptor.accepted.try_recv() {
                        Ok(Ok(stream)) => {
                            runtime.progress = runtime.progress.wrapping_add(1);
                            let _ = runtime.accept(stream)?;
                        }
                        Ok(Err(())) => return Err(Failure::plain("accept_failed")),
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => {
                            return Err(Failure::plain("accept_failed"));
                        }
                    }
                }
                runtime.turn()?;
                let idle = runtime.progress == before;
                // The idle wait below is shorter than the interval, so the
                // summary is at most one tick late.
                if Instant::now() >= counters_due {
                    runtime.events.flush_tallies().map_err(event_failure)?;
                    counters_due = Instant::now() + COUNTERS_INTERVAL;
                }
                if idle {
                    // Nothing was runnable. Every producer signals after it
                    // publishes; the timeout only bounds the wall-clock
                    // checks that have no producer.
                    runtime.wake.wait(runtime.idle_wait());
                }
            }
            // What was counted since the last summary is not lost at the end.
            runtime.events.flush_tallies().map_err(event_failure)
        })();
        acceptor.stop(address);
        served
    })();
    cancel.store(true, Ordering::Relaxed);
    // A typed startup/read failure remains authoritative and performs no
    // further durable writes. A normal stop finishes safe zero-expiry ends.
    let session_result = if result.is_ok() {
        runtime.shutdown_sessions(Duration::from_secs(2))
    } else {
        Ok(())
    };
    for (_, p) in std::mem::take(&mut runtime.peers) {
        p.io.close();
        runtime.retiring.push(p.io);
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    while runtime.retiring.iter().any(|p| !p.is_finished()) && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(2));
    }
    let mut socket_result = Ok(());
    for mut p in runtime.retiring {
        if !p.join_finished() {
            socket_result = Err(Failure::plain("socket_shutdown_deadline"));
        }
    }
    result.and(session_result).and(socket_result)
}

#[cfg(test)]
#[path = "runtime_tests.rs"]
mod tests;
