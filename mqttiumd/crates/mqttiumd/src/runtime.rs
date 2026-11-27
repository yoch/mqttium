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
use mqttiumd_contract::flat_map::{FlatMap, FlatSet, ScopedSeqMap};
use mqttiumd_contract::ids::{
    AccountId, ConnectionGeneration, ConnectionKey, DeadlineToken, DeliveryId, EffectId,
    LogicalDuration, LogicalOwnerId, LogicalTime, OperationId, OperationSeq, OutboundPacketId,
    PublicationId, Revision, RunEpoch, SessionId, StoreIncarnation,
};
use mqttiumd_contract::message_expiry::{MessageExpiry, MessageTime};
use mqttiumd_contract::outcome::{CommitFacts, StoreOutcome, StoreReply};
use mqttiumd_contract::packet::{InboundPacket, OutboundFrame, Qos, SharedBytes};
#[cfg(feature = "test-hooks")]
use mqttiumd_contract::pubsub::WorkItem;
use mqttiumd_contract::pubsub::{PubackTarget, Qos0DropReason, Qos0Origin};
use mqttiumd_contract::receipts::{
    ClosedReceiptCollection, ClosedReceiptReply, ReceiptClosure, ReceiptCollection,
    ReceiptCollectionReply,
};
use mqttiumd_contract::session::{
    SessionAction, SessionChange, SessionDetachment, SessionEndReason, SessionExpiry, SessionGuard,
};
use mqttiumd_contract::step::{StepOutcome, TurnGrant};
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
const SESSION_EXPIRY_PER_TURN: usize = 4;
const MESSAGE_EXPIRY_PAGE_SIZE: u32 = 4;
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
    pub(crate) frames: usize,
    /// A close can retire every accepted frame while an outbox-sized set of
    /// synchronously refused volatile effects also awaits its exact owner
    /// fact.
    pub(crate) terminal_inputs: usize,
    /// Packets and facts waiting for the core, the writer's and the
    /// deadlines' reserved shares excluded.
    ordinary_inputs: usize,
    /// Effects the core may hold between two dispatches.
    effect_box: usize,
    /// Outgoing packet identifiers in flight: eight per connection.
    pub(crate) outgoing_bindings: usize,
    pub(crate) timers: usize,
    /// Concurrent business commands/work reads allowed in flight to the writer.
    pub(crate) in_flight: usize,
    /// Closing and terminal facts applied by one pass over the inputs.
    pass_facts: usize,
    /// Maximum step transitions applied by one pass over the inputs.
    pub(crate) drain_steps: usize,
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
    max_packet: net::MAX_PACKET_BYTES,
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
    pub delivery_shards: usize,
}
/// Cold connection state allocated lazily only when a peer holds backpressure
/// buffers, multi-part subscribe requests, deferred publishes, or close state.
/// Idle and steady-state peers keep `cold == None` (zero heap bytes).
#[derive(Default)]
struct PeerCold {
    opened_by_foreign: Option<OperationId>,
    will_disconnect: Option<bool>,
    disconnect_expiry: Option<SessionExpiry>,
    close_observed_at: Option<u64>,
    /// The next packet of this connection, taken from its lane and not
    /// yet given to the core: a packet behind a CONNECT that is still
    /// being opened, or a business packet the lane has no place for.
    held_packet: Option<(u8, net::FrameBytes, net::FrameObservation)>,
    /// Since when a packet of this connection waits for a credit that
    /// other work holds.
    capacity_wait: Option<Instant>,
    requests: Option<Requests>,
    closing: Option<(&'static str, Instant)>,
    deferred_publish: Option<DeferredPublish>,
    /// The packets read past `deferred_publish`, in their order, with their
    /// bytes counted.
    behind: VecDeque<(u8, net::FrameBytes, net::FrameObservation)>,
    behind_bytes: usize,
    blocked_output: Option<EffectId>,
    retire_code: Option<&'static str>,
    shard_detached: Option<Arc<AtomicBool>>,
}

impl PeerCold {
    fn room_behind(&self, bytes: usize) -> bool {
        self.behind.len() < BEHIND_PACKETS && bytes <= BEHIND_BYTES - self.behind_bytes
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.opened_by_foreign.is_none()
            && self.will_disconnect.is_none()
            && self.disconnect_expiry.is_none()
            && self.close_observed_at.is_none()
            && self.held_packet.is_none()
            && self.capacity_wait.is_none()
            && self.requests.is_none()
            && self.closing.is_none()
            && self.deferred_publish.is_none()
            && self.behind.is_empty()
            && self.blocked_output.is_none()
            && self.retire_code.is_none()
            && self.shard_detached.is_none()
    }

    #[inline]
    fn state_active(&self) -> bool {
        self.retire_code.is_some()
            || self.held_packet.is_some()
            || self.deferred_publish.is_some()
            || !self.behind.is_empty()
            || self.requests.is_some()
            || self.blocked_output.is_some()
            || self.closing.is_some()
    }

    #[inline]
    fn needs_poll(&self) -> bool {
        self.state_active() || self.capacity_wait.is_some()
    }
}

/// Compact per-connection state (~96 bytes in release vs 944 bytes previously).
/// Rare fields live in `cold`, and QoS 1/2 receive windows live in `receiving`.
struct Peer {
    io: net::Connection,
    created_ns: u64,
    last_packet_ns: u64,
    ingress: u64,
    opened_by_seq: u64,
    session: Option<SessionId>,
    receiving: Option<Box<crate::receiving::Window>>,
    cold: Option<Box<PeerCold>>,
    connect_expiry: SessionExpiry,
    max_packet: u32,
    receive_maximum: u16,
    keep_alive: u16,
    version: Option<Version>,
    connected: bool,
    pending: bool,
    will_managed: bool,
    has_opened_by: bool,
}
/// The packets kept behind a waiting publication: one more lane of them,
/// and no more bytes than one packet of the largest size.
const BEHIND_PACKETS: usize = net::INPUT_SLOTS;
const BEHIND_BYTES: usize = net::MAX_PACKET_BYTES;

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
    #[inline]
    fn cold_mut(&mut self) -> &mut PeerCold {
        self.cold.get_or_insert_with(Default::default)
    }

    #[inline]
    fn gc_cold(&mut self) {
        if self.cold.as_ref().is_some_and(|c| c.is_empty()) {
            self.cold = None;
        }
    }

    #[inline]
    fn max_packet_bytes(&self) -> usize {
        self.max_packet as usize
    }

    #[inline]
    fn created_instant(&self, started: Instant) -> Instant {
        started + Duration::from_nanos(self.created_ns)
    }

    #[inline]
    fn last_packet_instant(&self, started: Instant) -> Instant {
        started + Duration::from_nanos(self.last_packet_ns)
    }

    #[inline]
    fn opened_by(
        &self,
        origin: (StoreIncarnation, RunEpoch, LogicalOwnerId),
    ) -> Option<OperationId> {
        if !self.has_opened_by {
            return None;
        }
        if let Some(foreign) = self.cold.as_ref().and_then(|c| c.opened_by_foreign) {
            return Some(foreign);
        }
        Some(OperationId::new(
            origin.0,
            origin.1,
            origin.2,
            OperationSeq::new(self.opened_by_seq),
        ))
    }

    #[inline]
    fn set_opened_by(
        &mut self,
        id: OperationId,
        origin: (StoreIncarnation, RunEpoch, LogicalOwnerId),
    ) {
        self.has_opened_by = true;
        if (id.incarnation(), id.epoch(), id.owner()) == origin {
            self.opened_by_seq = id.sequence().get();
            if let Some(c) = self.cold.as_mut() {
                c.opened_by_foreign = None;
            }
        } else {
            self.cold_mut().opened_by_foreign = Some(id);
        }
    }

    #[inline]
    fn closing(&self) -> Option<(&'static str, Instant)> {
        self.cold.as_ref().and_then(|c| c.closing)
    }

    #[inline]
    fn set_closing(&mut self, value: (&'static str, Instant)) {
        self.cold_mut().closing = Some(value);
    }

    #[inline]
    fn retire_code(&self) -> Option<&'static str> {
        self.cold.as_ref().and_then(|c| c.retire_code)
    }

    #[inline]
    fn blocked_output(&self) -> Option<EffectId> {
        self.cold.as_ref().and_then(|c| c.blocked_output)
    }

    #[inline]
    fn deferred_publish(&self) -> Option<&DeferredPublish> {
        self.cold.as_ref().and_then(|c| c.deferred_publish.as_ref())
    }

    #[inline]
    #[cfg(test)]
    fn held_packet(&self) -> Option<&(u8, net::FrameBytes, net::FrameObservation)> {
        self.cold.as_ref().and_then(|c| c.held_packet.as_ref())
    }

    #[inline]
    fn receiving_mut(&mut self) -> &mut crate::receiving::Window {
        self.receiving.get_or_insert_with(Default::default)
    }

    #[inline]
    fn receiving_can_complete(&self, id: u16, serial: Option<u64>, terminal: Terminal) -> bool {
        self.receiving
            .as_ref()
            .is_some_and(|w| w.can_complete_frame(id, serial, terminal))
    }

    #[inline]
    fn receiving_complete(&mut self, id: u16, serial: Option<u64>, terminal: Terminal) -> bool {
        self.receiving
            .as_mut()
            .is_some_and(|w| w.complete_frame(id, serial, terminal))
    }

    #[inline]
    fn receiving_contains(&self, id: u16) -> bool {
        self.receiving.as_ref().is_some_and(|w| w.contains(id))
    }

    #[cfg(any(test, feature = "test-hooks"))]
    #[inline]
    fn receiving_serial(&self, id: u16) -> Option<u64> {
        self.receiving.as_ref().and_then(|w| w.serial(id))
    }

    /// Whether this packet goes ahead of the publication that waits for a
    /// slot: only a PUBREL, which completes another exchange and frees its
    /// slot, and commutes with every packet but the PUBLISH of its own
    /// identifier. Everything else keeps its place in the stream.
    fn overtakes_deferred(&self, first: u8, body: &[u8]) -> bool {
        if first >> 4 != 6 || body.len() < 2 {
            return false;
        }
        let Some(cold) = self.cold.as_deref() else {
            return true;
        };
        let id = u16::from_be_bytes([body[0], body[1]]);
        let waiting = cold
            .deferred_publish
            .as_ref()
            .and_then(|deferred| match &deferred.packet {
                InboundPacket::Publish { packet_id, .. } => packet_id.map(|id| id.get()),
                _ => None,
            });
        waiting != Some(id)
            && cold
                .behind
                .iter()
                .all(|(first, body, _)| raw_publish_id(*first, body) != Some(id))
    }

    /// The retained head cannot be examined again until a receive credit
    /// returns. An independent PUBREL can still pass a completely full prefix.
    fn lookahead_blocked(&self) -> bool {
        self.version == Some(codec::Version::V311)
            && self.cold.as_deref().is_some_and(|cold| {
                cold.deferred_publish.is_some()
                    && cold.held_packet.as_ref().is_some_and(|(first, body, _)| {
                        !cold.room_behind(body.len()) && !self.overtakes_deferred(*first, body)
                    })
            })
    }

    /// Keeps a packet behind the waiting publication, or hands it back
    /// when the bound is reached: it then stays the held packet and the
    /// rest stays with the transport.
    fn keep_behind(
        &mut self,
        packet: (u8, net::FrameBytes, net::FrameObservation),
    ) -> Result<(), (u8, net::FrameBytes, net::FrameObservation)> {
        let bytes = packet.1.len();
        let cold = self.cold_mut();
        if !cold.room_behind(bytes) {
            return Err(packet);
        }
        cold.behind_bytes += bytes;
        cold.behind.push_back(packet);
        Ok(())
    }

    #[inline]
    fn submitted_output(&mut self, effect: EffectId) -> bool {
        if let Some(cold) = self.cold.as_mut()
            && cold.blocked_output == Some(effect)
        {
            cold.blocked_output = None;
            self.gc_cold();
            return true;
        }
        false
    }

    #[inline]
    fn needs_poll(&self) -> bool {
        !self.connected || self.pending || self.cold.as_ref().is_some_and(|c| c.needs_poll())
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
    kind: FrameKind,
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
/// Dense slab storage for live `Peer` records indexed by a compact
/// `FlatMap<ConnectionGeneration, u32>`, so empty hash slots cost 16 B instead
/// of `size_of::<Peer>()`.
#[derive(Default)]
struct PeerTable {
    entries: Vec<Option<(ConnectionGeneration, Peer)>>,
    free: Vec<u32>,
    index: FlatMap<ConnectionGeneration, u32>,
    len: usize,
}

impl PeerTable {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            free: Vec::new(),
            index: FlatMap::new(),
            len: 0,
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.len
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    fn contains_key(&self, key: &ConnectionGeneration) -> bool {
        self.index.contains_key(key)
    }

    #[inline]
    fn get(&self, key: &ConnectionGeneration) -> Option<&Peer> {
        let &slot = self.index.get(key)?;
        self.entries[slot as usize].as_ref().map(|(_, p)| p)
    }

    #[inline]
    fn get_mut(&mut self, key: &ConnectionGeneration) -> Option<&mut Peer> {
        let &slot = self.index.get(key)?;
        self.entries[slot as usize].as_mut().map(|(_, p)| p)
    }

    fn insert(&mut self, key: ConnectionGeneration, peer: Peer) -> Option<Peer> {
        if let Some(&slot) = self.index.get(&key) {
            return self.entries[slot as usize]
                .replace((key, peer))
                .map(|(_, old)| old);
        }
        let slot = if let Some(free) = self.free.pop() {
            self.entries[free as usize] = Some((key, peer));
            free
        } else {
            let slot = self.entries.len() as u32;
            self.entries.push(Some((key, peer)));
            slot
        };
        self.index.insert(key, slot);
        self.len += 1;
        None
    }

    fn remove(&mut self, key: &ConnectionGeneration) -> Option<Peer> {
        let slot = self.index.remove(key)?;
        let (_, removed) = self.entries[slot as usize].take()?;
        self.len -= 1;
        if self.len == 0 {
            self.entries.clear();
            self.free.clear();
        } else {
            self.free.push(slot);
        }
        Some(removed)
    }

    #[inline]
    fn keys(&self) -> impl Iterator<Item = &ConnectionGeneration> + '_ {
        self.entries
            .iter()
            .filter_map(|slot| slot.as_ref().map(|(k, _)| k))
    }

    #[inline]
    fn values(&self) -> impl Iterator<Item = &Peer> + '_ {
        self.entries
            .iter()
            .filter_map(|slot| slot.as_ref().map(|(_, p)| p))
    }

    #[cfg(test)]
    #[inline]
    fn iter(&self) -> impl Iterator<Item = (&ConnectionGeneration, &Peer)> + '_ {
        self.entries
            .iter()
            .filter_map(|slot| slot.as_ref().map(|(k, p)| (k, p)))
    }

    #[allow(dead_code)]
    #[inline]
    fn values_mut(&mut self) -> impl Iterator<Item = &mut Peer> + '_ {
        self.entries
            .iter_mut()
            .filter_map(|slot| slot.as_mut().map(|(_, p)| p))
    }
}

impl std::ops::Index<&ConnectionGeneration> for PeerTable {
    type Output = Peer;

    #[inline]
    fn index(&self, key: &ConnectionGeneration) -> &Self::Output {
        self.get(key).expect("peer generation present")
    }
}

impl std::ops::IndexMut<&ConnectionGeneration> for PeerTable {
    #[inline]
    fn index_mut(&mut self, key: &ConnectionGeneration) -> &mut Self::Output {
        self.get_mut(key).expect("peer generation present")
    }
}

impl IntoIterator for PeerTable {
    type Item = (ConnectionGeneration, Peer);
    type IntoIter = std::iter::Flatten<std::vec::IntoIter<Option<(ConnectionGeneration, Peer)>>>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_iter().flatten()
    }
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
    shards: Option<crate::delivery_shard::ShardPool>,
    shard_notices: Vec<crate::delivery_shard::ShardToCoord>,
    pending_suback_notify: FlatSet<SessionId>,
    incarnation: StoreIncarnation,
    epoch: RunEpoch,
    writer: StoreHandle<'a>,
    events: Events,
    peers: PeerTable,
    topic_cache: codec::TopicCache,
    ready_peers: Arc<net::ReadyQueue>,
    ready_raw: Vec<u64>,
    pending_peers: FlatSet<ConnectionGeneration>,
    capacity_waiters: usize,
    retiring: Vec<net::Connection>,
    sessions: BTreeMap<SharedBytes, Session>,
    session_ids: BTreeSet<SessionId>,
    session_revisions: FlatMap<SessionId, Revision>,
    session_requests: BTreeMap<u64, PendingSession>,
    will_connects: BTreeMap<ConnectionGeneration, PendingWillConnect>,
    will_reads: Option<EffectId>,
    last_will_deferred: Option<(OperationId, mqttiumd_core::owner::WillDeferredReason)>,
    next_session_request: u64,
    generation: ConnectionGeneration,
    peer_cursor: usize,
    peer_keys: Vec<ConnectionGeneration>,
    scratch_incoming: VecDeque<net::Incoming>,
    scratch_written: Vec<net::Written>,
    scratch_progress: Vec<net::WireProgress>,
    scratch_responses: Vec<Response>,
    sink_close: Vec<(ConnectionGeneration, &'static str)>,
    sink_mark_pending: Vec<ConnectionGeneration>,
    sink_work_targets: Vec<SessionId>,
    sink_attach_ready: Vec<ConnectionKey>,
    sink_suback_ready: Vec<SessionId>,
    last_peer_deadline_scan: Instant,
    inputs: VecDeque<Input>,
    /// FIFO lane for credit-releasing subscriber acknowledgements
    /// (`ObservedPuback` / `ObservedQos2Ack`), drained ahead of new inbound
    /// `PUBLISH` packets so fan-in subscribers release Receive Maximum slots
    /// and store references before new admissions overflow the inline window.
    ack_inputs: VecDeque<Input>,
    control_inputs: VecDeque<Input>,
    closing_inputs: VecDeque<Input>,
    terminal_inputs: VecDeque<Input>,
    terminal_effects: ScopedSeqMap<EffectId, u32>,
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
    inflight: ScopedSeqMap<EffectId, OperationId>,
    /// Subset of `inflight` ordinary-lane commands that depend on or mutate
    /// `route_revision` or target session presence; control-lane lifecycle and
    /// Will admissions wait for this set to drain so they never overtake and
    /// invalidate in-flight admissions of unrelated clients.
    inflight_route_ops: ScopedSeqMap<EffectId, ()>,
    lifecycle_inflight: BTreeMap<EffectId, OperationId>,
    last_session_change: Option<OperationId>,
    session_cardinality_dirty: bool,
    last_session_cardinality: Option<[usize; 13]>,
    work_reads: ScopedSeqMap<EffectId, SessionId>,
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
    retained_output: crate::retained_output::RetainedOutput,
    frames: ScopedSeqMap<EffectId, PendingFrame>,
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
    #[inline]
    fn origin(&self) -> (StoreIncarnation, RunEpoch, LogicalOwnerId) {
        (self.incarnation, self.epoch, LogicalOwnerId::new(0))
    }

    #[inline]
    fn elapsed_ns(&self, now: Instant) -> u64 {
        u64::try_from(now.saturating_duration_since(self.started).as_nanos()).unwrap_or(u64::MAX)
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
                    limit: MESSAGE_EXPIRY_PAGE_SIZE,
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
            if !cfg!(any(test, feature = "test-hooks"))
                && now.domain == self.clock_anchor.reading.domain
                && !self.owner.has_expiring_messages()
                && self.writer.skip_empty_message_expiry_sweep()
            {
                self.message_expiry_scan = now_instant;
                self.message_expiry_observation = Some(now);
                return Ok(());
            }
            now
        };
        match self.writer.try_send_control(ControlRequest::MessageExpiry {
            now,
            after,
            limit: MESSAGE_EXPIRY_PAGE_SIZE,
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
            let mut budget = SESSION_EXPIRY_PER_TURN;
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
                self.events.emit("session_clock_degraded", json!({
                    "details": {
                        "code": "checkpoint_late",
                        "age_ms": self.last_clock_checkpoint.elapsed().as_millis().to_string(),
                    },
                })).map_err(event_failure)?;
            }
        }
        let inventory = (self.expiry_timers.len(), self.clock_checkpoint.is_some());
        if self.last_expiry_inventory != Some(inventory) {
            self.events
                .emit(
                    "expiry_inventory",
                    json!({
                        "details": {
                            "finite_timers": inventory.0,
                            "checkpoint_inflight": inventory.1,
                            "queued": self.expiry_inputs.len(),
                            "period_ms": 30000,
                        },
                    }),
                )
                .map_err(event_failure)?;
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
        if now.saturating_duration_since(self.last_message_clock_sync) >= IDLE_TICK {
            self.message_clock_needed = true;
        }
        self.writer_results()?;
        if self.shards.is_some() {
            self.drain_shard_notices()?;
        }
        if !self.owner.outbox_is_empty() {
            self.dispatch()?;
        }
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

    fn drain_shard_notices(&mut self) -> Result<(), Failure> {
        let mut notices = std::mem::take(&mut self.shard_notices);
        notices.clear();
        let drain_res = if let Some(shards) = self.shards.as_mut() {
            shards.drain_notices(&mut notices)
        } else {
            Ok(())
        };
        let apply_res = (|| -> Result<(), Failure> {
            for notice in notices.drain(..) {
                match notice {
                    crate::delivery_shard::ShardToCoord::CloseConnection {
                        generation,
                        code,
                        disconnect_reason,
                    } => {
                        self.progress = self.progress.wrapping_add(1);
                        let mut flushing = false;
                        if let Some(reason) = disconnect_reason
                            && let Some(peer) = self.peers.get_mut(&generation)
                            && peer.connected
                            && peer.version == Some(Version::V5)
                            && peer.closing().is_none()
                        {
                            let bytes = codec::disconnect_reason(reason);
                            if bytes.len() <= peer.max_packet_bytes()
                                && peer
                                    .io
                                    .try_send_outgoing(net::Write {
                                        expiry: None,
                                        correlation: None,
                                        bytes: net::FrameBytes::from(bytes),
                                        payload: None,
                                        report: net::WriteReport::Complete,
                                    })
                                    .is_ok()
                            {
                                peer.set_closing((code, Instant::now()));
                                self.pending_peers.insert(generation);
                                flushing = true;
                            }
                        }
                        if !flushing {
                            self.close(generation, code)?;
                        }
                    }
                    crate::delivery_shard::ShardToCoord::Fatal(err) => return Err(err),
                }
            }
            Ok(())
        })();
        self.shard_notices = notices;
        drain_res.and(apply_res)
    }

    /// Attaches the sessions whose CONNACK is queued to their shards. A
    /// connection that cannot be attached is closed with the reason: left
    /// open, it would get no delivery and nothing would say why.
    fn attach_ready_shards(&mut self, ready: &[ConnectionKey]) -> Result<(), Failure> {
        if ready.is_empty() || self.shards.is_none() {
            return Ok(());
        }
        for &connection in ready {
            let pending_inline = self
                .owner
                .pending_delegated_inline_count(connection.session);
            let Some(peer) = self.peers.get(&connection.generation) else {
                continue;
            };
            if peer.closing().is_some() {
                continue;
            }
            let Some(revision) = self.session_revisions.get(&connection.session).copied() else {
                self.close(connection.generation, "shard_attach_without_revision")?;
                continue;
            };
            let version = peer.version.unwrap_or_default();
            let receive_maximum = peer.receive_maximum;
            let max_packet = peer.max_packet;
            let outgoing = peer.io.outgoing_handle();
            if let Some(shards) = self.shards.as_mut() {
                let route = shards.route_for(connection);
                if !peer.io.install_shard_route(route) {
                    self.close(connection.generation, "shard_attached_twice")?;
                    continue;
                }
                shards.attach_session(
                    connection.session,
                    revision,
                    connection.generation,
                    receive_maximum,
                    max_packet,
                    version,
                    outgoing,
                    pending_inline,
                );
            }
        }
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
        let now_ns = self.elapsed_ns(Instant::now());
        self.pending_peers.insert(self.generation);
        self.peers.insert(
            self.generation,
            Peer {
                io,
                created_ns: now_ns,
                last_packet_ns: now_ns,
                ingress: 0,
                opened_by_seq: 0,
                session: None,
                receiving: None,
                cold: None,
                connect_expiry: SessionExpiry::OnDisconnect,
                max_packet: 268_435_455,
                receive_maximum: crate::receiving::MAXIMUM as u16,
                keep_alive: 0,
                version: None,
                connected: false,
                pending: false,
                will_managed: false,
                has_opened_by: false,
            },
        );
        self.events
            .emit(
                "connection_opened",
                json!({
                    "connection": self.generation.get().to_string(),
                    "details": {"send_buffer_bytes": sendbuf},
                }),
            )
            .map_err(event_failure)?;
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
        if matches!(
            input,
            Input::ObservedPuback { .. } | Input::ObservedQos2Ack { .. }
        ) {
            self.ack_inputs.push_back(input);
        } else {
            self.inputs.push_back(input);
        }
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
        let shards_active = self.shards.is_some();
        let Some(p) = self.peers.get_mut(&generation) else {
            return Ok(());
        };
        let Some(cold) = p.cold.as_mut() else {
            return Ok(());
        };
        if cold.closing.is_some() {
            return Ok(());
        }
        let Some(requests) = cold.requests.as_mut() else {
            return Ok(());
        };
        if requests.awaiting {
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
        if bytes.len() > p.max_packet as usize {
            return self.close(generation, "outgoing_packet_limit");
        }
        let write = net::Write {
            expiry: None,
            correlation: None,
            bytes: net::FrameBytes::from(bytes),
            payload: None,
            report: net::WriteReport::None,
        };
        let sent = if shards_active && !requests.unsubscribe {
            p.io.try_send_suback(write)
        } else {
            p.io.try_send_outgoing(write)
        };
        if sent.is_ok() {
            let notify_session = (!requests.unsubscribe).then_some(p.session).flatten();
            cold.requests = None;
            p.gc_cold();
            self.progress = self.progress.wrapping_add(1);
            if let Some(session) = notify_session
                && self.shards.is_some()
                && self.pending_suback_notify.remove(&session)
            {
                self.notify_work(session)?;
            }
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
            self.deferred_inputs.len() + self.will_connects.len() + self.ack_inputs.len(),
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
        if peer.retire_code().is_some() {
            return Ok(());
        }
        let cold = peer.cold_mut();
        cold.retire_code = Some(code);
        cold.closing.get_or_insert((code, Instant::now()));
        let needs_observed = cold.close_observed_at.is_none();
        self.pending_peers.insert(generation);
        if needs_observed {
            let observed = self.session_now()?;
            self.peers
                .get_mut(&generation)
                .expect("live retiring peer")
                .cold_mut()
                .close_observed_at = Some(observed);
        }
        self.peers[&generation].io.close();
        let must_preserve_write_facts = self.frames.values().any(|frame| {
            frame.connection.generation == generation && frame.kind == FrameKind::Publish
        }) || self.terminal_inputs.iter().any(|input| {
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
        if let Some(shards) = self.shards.as_mut()
            && let Some(peer) = self.peers.get_mut(&generation)
            && let Some(session) = peer.session
        {
            // Both TCP and TLS publish their final I/O observations before
            // marking the task finished. A takeover already waits for this
            // peer to leave the table before it submits the next Open.
            if !peer.io.is_finished() {
                return Ok(());
            }
            let cold = peer.cold_mut();
            let detached = cold.shard_detached.get_or_insert_with(|| {
                shards.detach_session(
                    session,
                    generation,
                    self.owner.pending_delegated_inline_count(session),
                )
            });
            if !detached.load(Ordering::Acquire) {
                return Ok(());
            }
        }
        let origin = self.origin();
        if let Some(peer) = self.peers.remove(&generation) {
            self.pending_peers.remove(&generation);
            let (capacity_wait, close_observed_at, disconnect_expiry, will_disconnect) =
                match peer.cold.as_deref() {
                    Some(c) => (
                        c.capacity_wait,
                        c.close_observed_at,
                        c.disconnect_expiry,
                        c.will_disconnect,
                    ),
                    None => (None, None, None, None),
                };
            let opened_by = peer.opened_by(origin);
            if capacity_wait.is_some() {
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
                    opened_by,
                };
                self.events
                    .emit("session_close_copied", observation.fields())
                    .map_err(event_failure)?;
                self.copied_session_close = Some(observation);
            }
            // Freeze the observation before any delayed Open/COMMIT result.
            let observed_at = match close_observed_at {
                Some(value) => value,
                None => self.session_now()?,
            };
            let intent = CloseIntent {
                expiry: disconnect_expiry.unwrap_or(peer.connect_expiry),
                observed_at,
                explicit: disconnect_expiry.is_some(),
                will: peer.will_managed.then_some(will_disconnect.unwrap_or(true)),
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
                && let Some(input) = Self::closure_input(session, generation, opened_by, intent)
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
                if frame.kind == FrameKind::Publish0 {
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
            if self.shards.is_some() {
                self.drain_shard_notices()?;
            }
            self.connections()?;
            self.due_deadlines()?;
            self.drain_inputs()?;
            self.dispatch()?;
            let status = self.owner.session_status();
            let complete = (self.shards.is_none() || self.peers.is_empty())
                && !status.lifecycle_pending
                && status.retired_flights == 0
                && self.sessions.values().all(|session| {
                    session.expiry == SessionExpiry::Never
                        || (matches!(session.expiry, SessionExpiry::After(_))
                            && session.detached.is_some())
                })
                && self.inputs.is_empty()
                && self.ack_inputs.is_empty()
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
                && self.owner.outbox().is_empty();
            if complete {
                self.events
                    .emit(
                        "session_shutdown_completed",
                        json!({
                            "details": {
                                "persistent_sessions": self.sessions.len(),
                                "owner_bindings": status.bindings,
                            },
                        }),
                    )
                    .map_err(event_failure)?;
                return Ok(());
            }
            if Instant::now() >= deadline {
                let mut fields = json!({
                    "details": {
                        "code": "pending_or_unknown",
                        "phase": "Shutdown",
                        "lifecycle_pending": status.lifecycle_pending,
                        "retired_flights": status.retired_flights,
                        "in_flight": self.inflight.len()+self.lifecycle_inflight.len(),
                        "closing_inputs": self.closing_inputs.len(),
                    },
                });
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
                self.events
                    .emit(
                        "session_change_ignored",
                        json!({
                            "operation": operation(changed_by),
                            "details": {"code": "stale_lifecycle_result"},
                        }),
                    )
                    .map_err(event_failure)?;
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
                    client_id.clone(),
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
                    self.session_revisions.remove(&replaced.id);
                }
                self.session_ids.insert(session);
                self.session_revisions.insert(session, revision);
                let origin = self.origin();
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
                                .opened_by(origin)
                                .is_some_and(|original| original != changed_by)
                        {
                            return Err(Failure::plain("transport_session_rebound"));
                        }
                        peer.session = Some(session);
                        peer.set_opened_by(changed_by, origin);
                        if let Some(intent) = close_intent {
                            let cold = peer.cold_mut();
                            cold.will_disconnect = intent.will;
                            cold.disconnect_expiry = Some(intent.expiry);
                            cold.close_observed_at = Some(intent.observed_at);
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

                let mut fields = json!({
                    "operation": operation(changed_by),
                    "details": {
                        "session": session.get().to_string(),
                        "session_present": session_present,
                        "expiry": expiry_json(expiry),
                        "source": "lifecycle_commit",
                    },
                });
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
                value.detached = Some(Box::new(detached));
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
                "action": "Opened",
                "session": session.get().to_string(),
                "revision": revision.get().to_string(),
                "session_present": session_present,
                "expiry": expiry_json(expiry),
                "retired_session": retired.map(|id|id.get().to_string()),
                "route_revision": route_revision.get().to_string(),
                "cancelled_inbound": cancelled.inbound,
                "cancelled_deliveries": cancelled.deliveries,
                "cancelled_routes": cancelled.routes,
            }),
            SessionChange::Detached {
                session,
                expiry,
                detached,
            } => {
                let mut details = json!({
                    "action": "Detached",
                    "session": session.get().to_string(),
                    "expiry": expiry_json(expiry),
                    "detached_by": operation_text(detached.operation),
                });
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
                "action": "Closed",
                "session": session.get().to_string(),
                "route_revision": route_revision.get().to_string(),
                "cancelled_inbound": cancelled.inbound,
                "cancelled_deliveries": cancelled.deliveries,
                "cancelled_routes": cancelled.routes,
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
        self.events
            .emit(
                "session_cardinality",
                json!({
                    "details": {
                        "session_cache": values[0],
                        "owner_sessions": values[1],
                        "owner_bindings": values[2],
                        "work_requests": values[3],
                        "work_ready": values[4],
                        "emit_registrations": values[5],
                        "lifecycle_requests": values[6],
                        "closed_generations": values[7],
                        "retired_flights": values[8],
                        "owner_inbound": values[9],
                        "owner_outgoing": values[10],
                        "frames": values[11],
                        "timers": values[12],
                    },
                }),
            )
            .map_err(event_failure)
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
            .chain(self.ack_inputs.iter())
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
        self.session_revisions.remove(&retired);
        self.pending_suback_notify.remove(&retired);
        if let Some(shards) = self.shards.as_mut() {
            shards.retire_session(retired);
        }
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
            && self.ack_inputs.is_empty()
            && self.control_inputs.is_empty()
            && self.closing_inputs.is_empty()
            && self.terminal_inputs.is_empty()
            && self.expiry_inputs.is_empty()
            && self.message_expiry_input.is_none())
        {
            let needs_clock = cfg!(any(test, feature = "test-hooks"))
                || self.message_clock_needed
                || self.message_last_time.is_none()
                || self.owner.has_expiring_messages();
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
            + self.ack_inputs.len()
            + self.control_inputs.len()
            + closing_budget
            + terminal_budget
            + expiry_budget
            + usize::from(self.message_expiry_input.is_some())
            + self.deferred_retries)
            .min(self.limits.drain_steps);
        let mut steps_taken = 0usize;
        let mut outbox_len_at_dispatch = self.owner.outbox().len();
        let mut dispatch_blocked = false;
        loop {
            let progress_before_pass = self.progress;
            while steps_taken < step_budget {
                steps_taken += 1;
                let (from_retry, mut input) = if self.deferred_retries == 0
                    && self.control_inputs.is_empty()
                    && self.closing_inputs.is_empty()
                    && self.terminal_inputs.is_empty()
                    && self.expiry_inputs.is_empty()
                    && self.message_expiry_input.is_none()
                {
                    let Some(input) = self.ack_inputs.pop_front().or_else(|| {
                        let popped = self.inputs.pop_front()?;
                        if let Input::WorkAvailable { session } = &popped {
                            self.queued_work_sessions.remove(session);
                        }
                        Some(popped)
                    }) else {
                        break;
                    };
                    (false, input)
                } else {
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
                        .or_else(|| self.ack_inputs.pop_front())
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
                    (from_retry, input)
                };
                if !from_retry
                    && !self.deferred_by_conn.is_empty()
                    && let Some(connection) = business_connection(&input)
                    && self.deferred_by_conn.contains_key(&connection)
                {
                    self.push_deferred_back(input);
                    continue;
                }
                match &mut input {
                    Input::ObservedPuback {
                        connection,
                        packet_id,
                        reason,
                        target,
                    } => {
                        let (conn, pi, rsn) = (*connection, *packet_id, *reason);
                        if !self.queued_generation_is_current(conn.generation)? {
                            self.events
                                .emit(
                                    "puback_ignored",
                                    puback_fields(
                                        conn,
                                        pi,
                                        rsn,
                                        *target,
                                        None,
                                        "connection_closed",
                                    ),
                                )
                                .map_err(event_failure)?;
                            continue;
                        }
                        #[cfg(feature = "test-hooks")]
                        if MUTANT_REBIND_OBSERVED_PUBACK {
                            *target = self.owner.puback_target(conn, pi);
                        }
                        if target.is_none() {
                            self.events
                                .emit(
                                    "puback_ignored",
                                    puback_fields(
                                        conn,
                                        pi,
                                        rsn,
                                        *target,
                                        None,
                                        "no_current_matching_occurrence",
                                    ),
                                )
                                .map_err(event_failure)?;
                        }
                    }
                    Input::Packet {
                        packet: InboundPacket::Puback { .. } | InboundPacket::Qos2Ack { .. },
                        ..
                    } => return Err(Failure::plain("uncorrelated_puback_input")),
                    Input::Packet { connection, .. } => {
                        if !self.queued_generation_is_current(*connection)? {
                            continue;
                        }
                    }
                    Input::OpenSession { connection, .. } => {
                        if !self.peers.contains_key(connection) {
                            let conn = *connection;
                            self.session_requests
                                .retain(|_, pending| pending.connection != conn);
                            continue;
                        }
                    }
                    Input::OpenSessionWillAt {
                        connection, now, ..
                    }
                    | Input::OpenSessionAt {
                        connection, now, ..
                    } => {
                        if !self.peers.contains_key(connection) {
                            let conn = *connection;
                            self.session_requests
                                .retain(|_, pending| pending.connection != conn);
                            continue;
                        }
                        *now = self.session_now()?;
                    }
                    Input::SessionEstablished { connection, .. } => {
                        *connection = connection.filter(|g| self.peers.contains_key(g));
                    }
                    _ => {}
                }
                let mut finish: Option<(OperationId, DeliveryId)> = None;
                let mut wakes_business = false;
                let mut collected: Option<(ReceiptCollection, ReceiptClosure, u16, u64, bool)> =
                    None;
                let mut ready_work: Option<(SessionId, bool)> = None;
                let mut will_progress = false;
                let mut opening: Option<ConnectionGeneration> = None;
                let mut lifecycle_result: Option<OperationId> = None;
                let mut business: Option<ConnectionGeneration> = None;
                let mut returns_credit = false;
                let mut is_recovery = false;
                let mut may_change_bindings = false;
                let mut outgoing_binding_prune = OutgoingPrune::None;
                let mut is_receipt_collected = false;
                let mut may_change_timers = false;
                let mut fast_batch_step = false;
                let mut zero_effect_step = false;
                let has_outgoing_bindings = !self.outgoing_bindings.is_empty();
                let has_timers = !self.timers.is_empty();

                match &input {
                    Input::Packet {
                        connection, packet, ..
                    } => match packet {
                        InboundPacket::Publish { qos, .. } => {
                            business = Some(*connection);
                            fast_batch_step = !matches!(qos, Qos::AtMostOnce);
                        }
                        InboundPacket::Pubrel { .. } => {
                            fast_batch_step = true;
                            may_change_timers = has_timers;
                        }
                        InboundPacket::Subscribe { .. } | InboundPacket::Unsubscribe { .. } => {
                            business = Some(*connection);
                        }
                        InboundPacket::Pingreq => {}
                        InboundPacket::ConnectResume { client_id } => {
                            may_change_bindings = true;
                            may_change_timers = has_timers;
                            if has_outgoing_bindings {
                                outgoing_binding_prune = OutgoingPrune::Full;
                            }
                            ready_work = self
                                .sessions
                                .get(client_id.as_slice())
                                .map(|s| (s.id, true));
                        }
                        InboundPacket::Puback { .. } => {
                            if has_outgoing_bindings {
                                outgoing_binding_prune = OutgoingPrune::Full;
                            }
                            ready_work = self
                                .owner
                                .session_for_connection(*connection)
                                .map(|id| (id, false));
                        }
                        InboundPacket::Qos2Ack { .. } => {
                            if has_outgoing_bindings {
                                outgoing_binding_prune = OutgoingPrune::Full;
                            }
                        }
                    },
                    Input::StoreResult {
                        operation, reply, ..
                    } => {
                        wakes_business = true;
                        may_change_timers = has_timers;
                        match reply {
                            StoreReply::Outcome(StoreOutcome::Committed(facts)) => match facts {
                                CommitFacts::PublicationAdmitted { .. } => {
                                    fast_batch_step = true;
                                }
                                CommitFacts::DeliveryFinished {
                                    delivery,
                                    target,
                                    packet_id,
                                    ..
                                } => {
                                    finish = Some((*operation, *delivery));
                                    fast_batch_step = true;
                                    zero_effect_step = true;
                                    ready_work = Some((*target, false));
                                    if has_outgoing_bindings {
                                        outgoing_binding_prune =
                                            match self.owner.connection_of(*target) {
                                                Some(generation) => OutgoingPrune::Key((
                                                    ConnectionKey {
                                                        session: *target,
                                                        generation,
                                                    },
                                                    *packet_id,
                                                )),
                                                None => OutgoingPrune::Full,
                                            };
                                    }
                                }
                                CommitFacts::DeliveryClaimed { target, .. }
                                | CommitFacts::DeliveryConsumed { target, .. } => {
                                    fast_batch_step =
                                        matches!(facts, CommitFacts::DeliveryClaimed { .. });
                                    ready_work = Some((*target, false));
                                }
                                CommitFacts::DeliveryStarted {
                                    target, packet_id, ..
                                }
                                | CommitFacts::DeliveryPubrelReady {
                                    target, packet_id, ..
                                }
                                | CommitFacts::DeliveryExpired {
                                    target, packet_id, ..
                                } => {
                                    fast_batch_step = matches!(
                                        facts,
                                        CommitFacts::DeliveryStarted { .. }
                                            | CommitFacts::DeliveryPubrelReady { .. }
                                    );
                                    ready_work = Some((*target, false));
                                    if has_outgoing_bindings {
                                        outgoing_binding_prune =
                                            match self.owner.connection_of(*target) {
                                                Some(generation) => OutgoingPrune::Key((
                                                    ConnectionKey {
                                                        session: *target,
                                                        generation,
                                                    },
                                                    *packet_id,
                                                )),
                                                None => OutgoingPrune::Full,
                                            };
                                    }
                                }
                                CommitFacts::InboundReleased { .. } => {
                                    fast_batch_step = true;
                                }
                                CommitFacts::RetainedQos0Stored { .. } => {}
                                CommitFacts::SubscriptionsChanged { session, .. } => {
                                    ready_work = Some((*session, false));
                                }
                                CommitFacts::WillPublished { .. } => {
                                    will_progress = true;
                                }
                                CommitFacts::SessionLifecycle(_) => {
                                    may_change_bindings = true;
                                    if has_outgoing_bindings {
                                        outgoing_binding_prune = OutgoingPrune::Full;
                                    }
                                    will_progress = matches!(
                                        self.owner.lifecycle_action(),
                                        Some(SessionAction::EndWill { .. })
                                    );
                                }
                            },
                            StoreReply::Outcome(StoreOutcome::RejectedCertain(_)) => {
                                if has_outgoing_bindings {
                                    outgoing_binding_prune = OutgoingPrune::Full;
                                }
                                if self.owner.lifecycle_operation_is_live(*operation) {
                                    lifecycle_result = Some(*operation);
                                }
                            }
                            _ => {
                                if has_outgoing_bindings {
                                    outgoing_binding_prune = OutgoingPrune::Full;
                                }
                            }
                        }
                    }
                    Input::ObservedPuback {
                        connection,
                        packet_id,
                        ..
                    }
                    | Input::ObservedQos2Ack {
                        connection,
                        packet_id,
                        ..
                    } => {
                        fast_batch_step = true;
                        ready_work = Some((connection.session, false));
                        if has_outgoing_bindings {
                            outgoing_binding_prune = OutgoingPrune::Key((*connection, *packet_id));
                        }
                    }
                    Input::FrameResult {
                        connection,
                        progress,
                        ..
                    } => {
                        ready_work = Some((connection.session, false));
                        returns_credit = !matches!(progress, FrameProgress::Started);
                        if matches!(
                            progress,
                            FrameProgress::Written { .. } | FrameProgress::Started
                        ) {
                            fast_batch_step = true;
                            zero_effect_step = !self.owner.has_held_answers();
                        } else if has_outgoing_bindings {
                            outgoing_binding_prune = OutgoingPrune::Full;
                        }
                    }
                    Input::WorkAvailable { session } | Input::WorkPage { session, .. } => {
                        fast_batch_step = true;
                        ready_work = Some((*session, false));
                    }
                    Input::Continue { .. } | Input::MessageClock { .. } | Input::DiscoverWills => {}
                    Input::Qos0EmitRefused { connection, .. } => {
                        ready_work = Some((connection.session, false));
                    }
                    Input::WillPage { next, .. } => {
                        will_progress = next.is_some();
                    }
                    Input::MaintainReceipts => {
                        may_change_timers = has_timers;
                    }
                    Input::ReceiptsCollected {
                        effect,
                        collection,
                        reply,
                    } => {
                        wakes_business = true;
                        is_receipt_collected = true;
                        may_change_timers = has_timers;
                        if let ReceiptCollectionReply::Applied {
                            closure,
                            removed,
                            retained,
                            more,
                        } = reply
                            && self.owner.collection_reply_is_live(*effect, *collection)
                        {
                            collected = Some((*collection, *closure, *removed, *retained, *more));
                        }
                    }
                    Input::ConnectionClosed { .. }
                    | Input::EndSession { .. }
                    | Input::EndSessionWill { .. }
                    | Input::DetachSession { .. } => {
                        returns_credit = true;
                        may_change_bindings = true;
                        may_change_timers = has_timers;
                        if has_outgoing_bindings {
                            outgoing_binding_prune = OutgoingPrune::Full;
                        }
                    }
                    Input::OpenSessionWillAt { connection, .. }
                    | Input::OpenSession { connection, .. }
                    | Input::OpenSessionAt { connection, .. } => {
                        opening = Some(*connection);
                        may_change_bindings = true;
                        may_change_timers = has_timers;
                        if has_outgoing_bindings {
                            outgoing_binding_prune = OutgoingPrune::Full;
                        }
                    }
                    Input::SessionEstablished {
                        session,
                        connection,
                        ..
                    } => {
                        may_change_bindings = true;
                        may_change_timers = has_timers;
                        if has_outgoing_bindings {
                            outgoing_binding_prune = OutgoingPrune::Full;
                        }
                        if connection.is_some() {
                            ready_work = Some((*session, true));
                        }
                    }
                    Input::ExpireSession { .. } => {
                        may_change_bindings = true;
                        may_change_timers = has_timers;
                        if has_outgoing_bindings {
                            outgoing_binding_prune = OutgoingPrune::Full;
                        }
                    }
                    Input::Recovery { .. } => {
                        is_recovery = true;
                        may_change_bindings = true;
                        may_change_timers = has_timers;
                        if has_outgoing_bindings {
                            outgoing_binding_prune = OutgoingPrune::Full;
                        }
                    }
                    Input::ConnectionSettings { .. }
                    | Input::Deadline { .. }
                    | Input::MessagesExpired { .. } => {
                        may_change_timers = has_timers;
                        if has_outgoing_bindings {
                            outgoing_binding_prune = OutgoingPrune::Full;
                        }
                    }
                }
                let observed_puback_meta = match &input {
                    Input::ObservedPuback {
                        connection,
                        packet_id,
                        reason,
                        target: Some(target),
                    } => Some((*connection, *packet_id, *reason, *target)),
                    _ => None,
                };
                let was_rejected_certain = matches!(
                    &input,
                    Input::StoreResult {
                        reply: StoreReply::Outcome(StoreOutcome::RejectedCertain(_)),
                        ..
                    }
                );
                let control_refusal =
                    lifecycle_result.and_then(|_| match self.owner.lifecycle_action() {
                        Some(SessionAction::Detach { .. }) => Some("session_detach_rejected"),
                        Some(SessionAction::Expire { .. }) => Some("session_expire_rejected"),
                        _ => None,
                    });
                let previous_lifecycle =
                    opening.and_then(|_| self.owner.lifecycle_dispatch().map(|(op, _)| op));
                let mut applied = false;
                let work_limit = if is_recovery {
                    WorkUnits::new(u64::MAX)
                } else {
                    WorkUnits::new(65536)
                };
                let inputs_before_dispatch = self.inputs.len();
                let mut dispatched_inline = false;
                let mut inline_accepted = 0usize;
                let mut inline_changed: Option<AppliedSessionChange> = None;
                let mut inline_error: Option<Failure> = None;
                let mut input_slot = Some(input);
                let step_outcome = if !cfg!(feature = "test-hooks")
                    && !zero_effect_step
                    && (!fast_batch_step || self.writer.is_direct())
                    && self.owner.outbox_is_empty()
                    && !self.bindings_dirty
                    && !self.timers_dirty
                    && opening.is_none()
                    && lifecycle_result.is_none()
                    && !will_progress
                    && !may_change_bindings
                    && self.owner.lifecycle_dispatch().is_none()
                    && self.owner.will_dispatch().is_none()
                {
                    dispatched_inline = true;
                    let shards_active = self.shards.is_some();
                    self.sink_close.clear();
                    self.sink_mark_pending.clear();
                    self.sink_work_targets.clear();
                    self.sink_attach_ready.clear();
                    self.sink_suback_ready.clear();
                    let mut sink = Sink {
                        writer: &mut self.writer,
                        limits: self.limits,
                        peers: &mut self.peers,
                        events: &self.events,
                        inflight: &mut self.inflight,
                        inflight_route_ops: &mut self.inflight_route_ops,
                        lifecycle_inflight: &mut self.lifecycle_inflight,
                        lifecycle_operation: None,
                        will_operation: None,
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
                        ordinary_reserved: self.deferred_inputs.len()
                            + self.will_connects.len()
                            + self.ack_inputs.len(),
                        queued_writer_replies: None,
                        frames: &mut self.frames,
                        outgoing_bindings: &mut self.outgoing_bindings,
                        shards_active,
                        shard_attach_ready: &mut self.sink_attach_ready,
                        shard_suback_ready: &mut self.sink_suback_ready,
                        close: &mut self.sink_close,
                        mark_pending_peers: &mut self.sink_mark_pending,
                        pending_work_targets: &mut self.sink_work_targets,
                        direct_skip_next_deadline: false,
                        session_change: None,
                        error: None,
                    };
                    let mut replay_sink = self.retained_output.sink(&mut sink);
                    let (outcome, accepted) = if let Some(shards) = self.shards.as_mut() {
                        self.owner.step_and_dispatch_in_place_with_oracle(
                            &mut input_slot,
                            TurnGrant { work_limit },
                            &mut replay_sink,
                            shards,
                        )
                    } else {
                        self.owner.step_and_dispatch_in_place(
                            &mut input_slot,
                            TurnGrant { work_limit },
                            &mut replay_sink,
                        )
                    };
                    inline_accepted = accepted;
                    inline_changed = sink.session_change.take();
                    inline_error = sink.error.take();
                    outcome
                } else if let Some(shards) = self.shards.as_mut() {
                    self.owner.step_in_place_with_oracle(
                        &mut input_slot,
                        TurnGrant { work_limit },
                        shards,
                    )
                } else {
                    self.owner
                        .step_in_place(&mut input_slot, TurnGrant { work_limit })
                };
                match step_outcome {
                    mqttiumd_core::owner::StepResult::Applied { .. } => {
                        applied = true;
                        self.progress = self.progress.wrapping_add(1);
                        if is_receipt_collected {
                            self.receipt_pressure_blocked = false;
                        }
                        if let Some(peer) = business.and_then(|key| self.peers.get_mut(&key))
                            && let Some(cold) = peer.cold.as_mut()
                            && cold.capacity_wait.take().is_some()
                        {
                            self.capacity_waiters = self.capacity_waiters.saturating_sub(1);
                            peer.gc_cold();
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
                            self.events
                                .emit_with("core_finish_observed", || {
                                    json!({
                                        "operation": operation(id),
                                        "details": {"delivery": delivery_text(delivery)},
                                    })
                                })
                                .map_err(event_failure)?;
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
                    mqttiumd_core::owner::StepResult::NeedCapacity { need, .. } => {
                        let input = input_slot.take().expect("input retained on NeedCapacity");
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
                                        self.events
                                            .emit(
                                                "receipt_window_pressure",
                                                json!({
                                                    "details": {
                                                        "resource": "ReceiptSlots",
                                                        "code": "receipt_window_business_full",
                                                        "span": span,
                                                        "unconsumed_holes": holes,
                                                    },
                                                }),
                                            )
                                            .map_err(event_failure)?;
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
                                    let cold = peer.cold_mut();
                                    if cold.capacity_wait.is_none() {
                                        cold.capacity_wait = Some(Instant::now());
                                        self.capacity_waiters += 1;
                                    }
                                    self.pending_peers.insert(key);
                                }
                                self.defer(packet, from_retry)
                            }
                            Input::Packet { connection, .. } => {
                                // No transition applied and no effect was submitted for this packet.
                                // Close its transport rather than reorder its positive acknowledgements.
                                self.events
                                    .emit(
                                        "input_capacity_refused",
                                        json!({
                                            "connection": connection.get().to_string(),
                                            "details": {
                                                "accepted_command_rejected": false,
                                                "resource": format!("{:?}",need.resource),
                                                "wake": format!("{:?}",need.wake),
                                            },
                                        }),
                                    )
                                    .map_err(event_failure)?;
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
                            other @ (Input::ObservedPuback { .. }
                            | Input::ObservedQos2Ack { .. }) => {
                                self.ack_inputs.push_back(other);
                            }
                            other => {
                                if let Input::WorkAvailable { session } = &other {
                                    self.queued_work_sessions.insert(*session);
                                }
                                self.inputs.push_back(other);
                            }
                        }
                    }
                    mqttiumd_core::owner::StepResult::Exhausted { .. } => {
                        return Err(Failure::plain("core_exhausted"));
                    }
                    mqttiumd_core::owner::StepResult::Fatal(fault) => {
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
                    if self.shards.is_some() {
                        if let Some(batch) = self.owner.take_last_delegated_committed() {
                            let mut targets_to_check: Vec<SessionId> =
                                batch.claims.iter().map(|c| c.target).collect();
                            targets_to_check.sort_unstable();
                            targets_to_check.dedup();
                            if let Some(shards) = self.shards.as_mut() {
                                let owner = &self.owner;
                                shards.send_inline_committed(batch, |session| {
                                    owner.delegated_inline_unknown(session)
                                });
                                // The held claims, then the barrier: the shard
                                // reads the session after every claim owed.
                                for session in targets_to_check {
                                    shards.release_held_claims(
                                        session,
                                        owner.delegated_inline_unknown(session),
                                    );
                                    if owner.pending_delegated_inline_count(session) == 0 {
                                        shards.flush_dirty_barrier_if_pending(session);
                                    }
                                }
                            }
                        }
                        if let Some(batch) = self.owner.take_last_delegated_rejected() {
                            let mut targets_to_check: Vec<SessionId> =
                                batch.claims.iter().map(|c| c.target).collect();
                            targets_to_check.sort_unstable();
                            targets_to_check.dedup();
                            if let Some(shards) = self.shards.as_mut() {
                                let owner = &self.owner;
                                for session in targets_to_check {
                                    shards.release_held_claims(
                                        session,
                                        owner.delegated_inline_unknown(session),
                                    );
                                    if owner.pending_delegated_inline_count(session) == 0 {
                                        shards.flush_dirty_barrier_if_pending(session);
                                    }
                                }
                            }
                        }
                        if was_rejected_certain && let Some(shards) = self.shards.as_mut() {
                            let owner = &self.owner;
                            shards.flush_idle_dirty_barriers(|s| {
                                owner.pending_delegated_inline_count(s)
                            });
                        }
                        if let Some(batch) = self.owner.take_last_delegated_qos0()
                            && let Some(shards) = self.shards.as_mut()
                        {
                            shards.send_qos0(batch);
                        }
                    }
                    if let Some((connection, packet_id, reason, target)) = observed_puback_meta
                        && self.owner.last_observed_puback_ignored()
                    {
                        self.events
                            .emit(
                                "puback_ignored",
                                puback_fields(
                                    connection,
                                    packet_id,
                                    reason,
                                    Some(target),
                                    None,
                                    "no_current_matching_occurrence",
                                ),
                            )
                            .map_err(event_failure)?;
                    }
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
                if dispatched_inline {
                    if !self.sink_mark_pending.is_empty() {
                        for generation in self.sink_mark_pending.drain(..) {
                            self.pending_peers.insert(generation);
                        }
                    }
                    if !self.sink_attach_ready.is_empty() {
                        let mut ready = std::mem::take(&mut self.sink_attach_ready);
                        let res = self.attach_ready_shards(&ready);
                        ready.clear();
                        self.sink_attach_ready = ready;
                        res?;
                    }
                    if !self.sink_suback_ready.is_empty() {
                        let mut suback = std::mem::take(&mut self.sink_suback_ready);
                        let mut res = Ok(());
                        for session in suback.drain(..) {
                            if self.pending_suback_notify.remove(&session)
                                && let Err(e) = self.notify_work(session)
                            {
                                res = Err(e);
                                break;
                            }
                        }
                        self.sink_suback_ready = suback;
                        res?;
                    }
                    if !self.sink_work_targets.is_empty() {
                        let mut targets = std::mem::take(&mut self.sink_work_targets);
                        let mut res = Ok(());
                        for target in targets.drain(..) {
                            if let Err(e) = self.notify_work(target) {
                                res = Err(e);
                                break;
                            }
                        }
                        self.sink_work_targets = targets;
                        res?;
                    }
                    if inline_accepted != 0 {
                        self.progress = self.progress.wrapping_add(1);
                        if std::mem::take(&mut self.effects_awaited) {
                            self.deferred_retries = self.deferred_inputs.len();
                        }
                    }
                    if let Some(error) = inline_error {
                        return Err(error);
                    }
                    let had_close_or_change =
                        inline_changed.is_some() || !self.sink_close.is_empty();
                    if !self.sink_close.is_empty() {
                        let mut close = std::mem::take(&mut self.sink_close);
                        let mut res = Ok(());
                        for (generation, code) in close.drain(..) {
                            if code == "outgoing_queue_full"
                                && self.peers.get(&generation).is_some_and(|peer| {
                                    peer.io.is_tls()
                                        && peer.io.terminal_reason().is_some()
                                        && peer.closing().is_none()
                                })
                            {
                                continue;
                            }
                            if let Err(e) = self.close(generation, code) {
                                res = Err(e);
                                break;
                            }
                        }
                        self.sink_close = close;
                        res?;
                    }
                    if let Some(changed) = inline_changed {
                        self.session_changed(changed)?;
                    }
                    if had_close_or_change && !self.timers.is_empty() {
                        self.timers
                            .retain(|token, _| self.owner.deadline_is_live(*token));
                    }
                    if inline_accepted != 0 || had_close_or_change {
                        self.session_cardinality_dirty = true;
                        self.emit_session_cardinality()?;
                    }
                    outbox_len_at_dispatch = self.owner.outbox().len();
                    dispatch_blocked = inline_accepted == 0 && outbox_len_at_dispatch > 0;
                } else {
                    let must_dispatch = !applied
                        || !fast_batch_step
                        || cfg!(feature = "test-hooks")
                        || self.owner.outbox().free_slots() < 16
                        || (!dispatch_blocked
                            && self
                                .owner
                                .outbox()
                                .len()
                                .saturating_sub(outbox_len_at_dispatch)
                                >= 32);
                    if must_dispatch {
                        let accepted = self.dispatch_returning_accepted()?;
                        outbox_len_at_dispatch = self.owner.outbox().len();
                        dispatch_blocked = accepted == 0 && outbox_len_at_dispatch > 0;
                    }
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
                if applied && self.owner.last_step_may_unblock_work() {
                    if self.shards.is_some() {
                        // The stored targets of a commit: their shards read
                        // them after the lease's last mark.
                        if let Some(first) = self.owner.last_inline_ready_target() {
                            self.queue_work(first);
                            for index in 0..self.owner.last_inline_ready_extra().len() {
                                let target = self.owner.last_inline_ready_extra()[index];
                                self.queue_work(target);
                            }
                        }
                    } else if let Some((session, initial)) = ready_work {
                        self.rearm_ready_work(session, initial);
                    } else if let Some(first) = self.owner.last_inline_ready_target() {
                        self.rearm_ready_work(first, false);
                        if !self.owner.last_inline_ready_extra().is_empty() {
                            let extra: Vec<SessionId> =
                                self.owner.last_inline_ready_extra().to_vec();
                            for target in extra {
                                self.rearm_ready_work(target, false);
                            }
                        }
                    }
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
            self.drain_pending_work();
            if !self.writer.is_direct() || self.progress == progress_before_pass {
                break;
            }
            self.writer_results()?;
            let extra = self.inputs.len()
                + self.ack_inputs.len()
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
            outbox_len_at_dispatch = self.owner.outbox().len();
            dispatch_blocked = false;
        }
        if self.inputs.is_empty() {
            self.inputs.clear();
        }
        if self.terminal_inputs.is_empty() {
            self.terminal_inputs.clear();
        }
        if self.closing_inputs.is_empty() {
            self.closing_inputs.clear();
        }
        if self.deferred_inputs.is_empty() {
            self.deferred_inputs.clear();
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
                self.events
                    .emit(
                        "will_deferred",
                        json!({
                            "operation": operation(id),
                            "details": {"code": code, "durable_intention_preserved": true},
                        }),
                    )
                    .map_err(event_failure)?;
            }
            self.last_will_deferred = deferred;
        }
        self.emit_session_cardinality()?;
        Ok(())
    }
    #[inline]
    fn prune_outgoing_binding_key(&mut self, key: (ConnectionKey, OutboundPacketId)) {
        if let Some(&target) = self.outgoing_bindings.get(&key)
            && !self.owner.outgoing_binding_is_live(key.0, key.1, target)
        {
            self.outgoing_bindings.remove(&key);
        }
    }
    fn prune_outgoing_bindings(&mut self) {
        let owner = &self.owner;
        self.outgoing_bindings
            .retain(|(connection, packet_id), target| {
                owner.outgoing_binding_is_live(*connection, *packet_id, *target)
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
    /// pass over each side, reusing the existing map buffer in place.
    fn refresh_bindings(&mut self) {
        self.cached_bindings.clear();
        for (session, generation) in self.owner.connections() {
            if self.session_ids.contains(&session) {
                self.cached_bindings.insert(session, generation);
            }
        }
    }

    fn dispatch(&mut self) -> Result<(), Failure> {
        let _ = self.dispatch_returning_accepted()?;
        Ok(())
    }

    fn dispatch_returning_accepted(&mut self) -> Result<usize, Failure> {
        #[cfg(not(feature = "test-hooks"))]
        if self.owner.outbox_is_empty() && self.retained_output.is_empty() {
            return Ok(0);
        }
        if self.timers_dirty && self.timers.len() >= self.limits.timers.saturating_sub(16) {
            self.timers_dirty = false;
            self.timers
                .retain(|token, _| self.owner.deadline_is_live(*token));
        }
        if self.bindings_dirty {
            self.refresh_bindings();
            self.bindings_dirty = false;
        }
        let shards_active = self.shards.is_some();
        let mut sink = Sink {
            writer: &mut self.writer,
            limits: self.limits,
            peers: &mut self.peers,
            events: &self.events,
            inflight: &mut self.inflight,
            inflight_route_ops: &mut self.inflight_route_ops,
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
            ordinary_reserved: self.deferred_inputs.len()
                + self.will_connects.len()
                + self.ack_inputs.len(),
            queued_writer_replies: None,
            frames: &mut self.frames,
            outgoing_bindings: &mut self.outgoing_bindings,
            shards_active,
            shard_attach_ready: &mut self.sink_attach_ready,
            shard_suback_ready: &mut self.sink_suback_ready,
            close: &mut self.sink_close,
            mark_pending_peers: &mut self.sink_mark_pending,
            pending_work_targets: &mut self.sink_work_targets,
            direct_skip_next_deadline: false,
            session_change: None,
            error: None,
        };
        #[cfg(feature = "test-hooks")]
        sink.release_qos0();
        let retried = self.retained_output.retry(&mut sink);
        let report = self
            .owner
            .dispatch(&mut self.retained_output.sink(&mut sink));
        let changed = sink.session_change.take();
        let error = sink.error.take();
        drop(sink);
        if !self.sink_mark_pending.is_empty() {
            for generation in self.sink_mark_pending.drain(..) {
                self.pending_peers.insert(generation);
            }
        }
        if !self.sink_attach_ready.is_empty() {
            let mut ready = std::mem::take(&mut self.sink_attach_ready);
            let res = self.attach_ready_shards(&ready);
            ready.clear();
            self.sink_attach_ready = ready;
            res?;
        }
        if !self.sink_suback_ready.is_empty() {
            let mut suback = std::mem::take(&mut self.sink_suback_ready);
            let mut res = Ok(());
            for session in suback.drain(..) {
                if self.pending_suback_notify.remove(&session)
                    && let Err(e) = self.notify_work(session)
                {
                    res = Err(e);
                    break;
                }
            }
            self.sink_suback_ready = suback;
            res?;
        }
        if !self.sink_work_targets.is_empty() {
            let mut targets = std::mem::take(&mut self.sink_work_targets);
            let mut res = Ok(());
            for target in targets.drain(..) {
                if let Err(e) = self.notify_work(target) {
                    res = Err(e);
                    break;
                }
            }
            self.sink_work_targets = targets;
            res?;
        }
        if report.accepted + retried != 0 {
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
        let had_close_or_change = changed.is_some() || !self.sink_close.is_empty();
        if !self.sink_close.is_empty() {
            let mut close = std::mem::take(&mut self.sink_close);
            let mut res = Ok(());
            for (generation, code) in close.drain(..) {
                // A terminated TLS sender can reject a queued PINGRESP before
                // the reactor reaches the normal DISCONNECT already in its reader
                // lane. The ordered input terminal decides that transport loss.
                if code == "outgoing_queue_full"
                    && self.peers.get(&generation).is_some_and(|peer| {
                        peer.io.is_tls()
                            && peer.io.terminal_reason().is_some()
                            && peer.closing().is_none()
                    })
                {
                    continue;
                }
                if let Err(e) = self.close(generation, code) {
                    res = Err(e);
                    break;
                }
            }
            self.sink_close = close;
            res?;
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
        Ok(report.accepted + retried)
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
                    self.events
                        .emit(
                            "receipt_collection_result_ignored",
                            json!({
                                "command_kind": "ReceiptCollection",
                                "details": crate::writer::collection_details(collection),
                            }),
                        )
                        .map_err(event_failure)?;
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
                    self.events.emit("session_result_ignored", json!({
                        "operation": crate::events::operation(operation),
                        "details": {"effect": effect_text(effect), "code": "stale_lifecycle_result"},
                    })).map_err(event_failure)?;
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
                    if let Some(shards) = self.shards.as_mut() {
                        shards.send_messages_expired(removed.as_slice());
                    }
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
                self.events
                    .emit(
                        "session_clock_checkpoint",
                        json!({
                            "details": {
                                "logical": self.clock_anchor.logical.to_string(),
                                "tick": self.clock_anchor.reading.tick.to_string(),
                            },
                        }),
                    )
                    .map_err(event_failure)?;
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
        let mut responses = std::mem::take(&mut self.scratch_responses);
        responses.clear();
        let drain_res = self.writer.drain_responses(&mut responses, writer_budget);
        if matches!(drain_res, Err(TryRecvError::Disconnected)) {
            self.scratch_responses = responses;
            return Err(Failure::plain("writer_disconnected"));
        }
        let apply_res = (|| -> Result<(), Failure> {
            for response in responses.drain(..) {
                self.progress = self.progress.wrapping_add(1);
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
                        self.inflight_route_ops.remove(&effect);
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
                        // With shards, the step of an admission's commit names its
                        // stored targets, and they are notified after it: the step
                        // marks their leases dirty, and the shard must see that
                        // mark. A resolution names no target here either.
                        let named_by_step = self.shards.is_some()
                            && self.owner.names_stored_targets_at_commit(operation);
                        self.queue(Input::StoreResult {
                            effect,
                            operation,
                            reply,
                        })?;
                        if !named_by_step
                            && matches!(
                                reply,
                                StoreReply::Outcome(StoreOutcome::Committed(
                                    CommitFacts::PublicationAdmitted { .. }
                                        | CommitFacts::RetainedQos0Stored { .. }
                                ))
                            )
                        {
                            for target in targets {
                                self.notify_work(target)?;
                            }
                        } else if matches!(
                            reply,
                            StoreReply::Outcome(StoreOutcome::Committed(
                                CommitFacts::SubscriptionsChanged { .. }
                            ))
                        ) {
                            if self.shards.is_some() {
                                // Defer shard work notification until SUBACK enters
                                // ConnCore::outgoing so retained replays never
                                // overtake SUBACK on the wire (MQTT-3.8.4-1).
                                for target in targets {
                                    self.pending_suback_notify.insert(target);
                                }
                            } else {
                                for target in targets {
                                    self.notify_work(target)?;
                                }
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
                            self.events
                                .emit(
                                    "work_page_held",
                                    json!({
                                        "details": {
                                            "session": session.get().to_string(),
                                            "effect": effect_text(effect),
                                        },
                                    }),
                                )
                                .map_err(event_failure)?;
                            continue;
                        }
                        if self.work_reads.remove(&effect) != Some(session) {
                            return Err(Failure::plain("uncorrelated_work_page"));
                        }
                        let page = result.map_err(|e| Failure::read(e, ReadPhase::Work))?;
                        self.session_cardinality_dirty = true;
                        if !self.session_ids.contains(&session) {
                            self.events
                                .emit(
                                    "work_page_ignored",
                                    json!({
                                        "details": {
                                            "session": session.get().to_string(),
                                            "effect": effect_text(effect),
                                            "code": "retired_session",
                                        },
                                    }),
                                )
                                .map_err(event_failure)?;
                            continue;
                        }
                        #[cfg(feature = "test-hooks")]
                        for item in page.items.as_slice() {
                            if self.work_cache.len()
                                >= crate::writer::SESSIONS * crate::receiving::MAXIMUM
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
        })();
        self.scratch_responses = responses;
        apply_res
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
    #[inline]
    fn rearm_ready_work(&mut self, session: SessionId, initial: bool) {
        if self.shards.is_some() {
            return;
        }
        if self.queued_work_sessions.contains(&session) {
            return;
        }
        let generation = if initial {
            self.owner.connection_of(session)
        } else {
            self.owner.ready_work_generation(session)
        };
        if let Some(generation) = generation
            && self
                .peers
                .get(&generation)
                .is_some_and(|p| p.connected && !p.pending && p.closing().is_none())
        {
            self.queue_work_front(session);
        }
    }
    fn queue_work_front(&mut self, session: SessionId) {
        if self.shards.is_some() {
            let pending = self.owner.pending_delegated_inline_count(session);
            if let Some(shards) = self.shards.as_mut() {
                shards.notify_dirty(session, true, pending);
            }
            return;
        }
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
        if self.shards.is_some() {
            let pending = self.owner.pending_delegated_inline_count(session);
            if let Some(shards) = self.shards.as_mut() {
                shards.notify_dirty(session, true, pending);
            }
            return;
        }
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
        // Work notifications are lossless: `queue_work` retains sessions in
        // `pending_work` whenever the ordinary input lane is full and
        // `drain_pending_work` re-enqueues them as soon as space opens. Only
        // keep the 100 ms full-session rescan when the test hook deliberately
        // drops work notifications.
        #[cfg(not(feature = "test-hooks"))]
        let rescan_all = false;
        #[cfg(feature = "test-hooks")]
        let rescan_all = self
            .hooks
            .directory
            .as_ref()
            .is_some_and(|d| d.join("drop-work-notifications").exists());
        if !self.pending_work.is_empty() {
            self.drain_pending_work();
        }
        if !rescan_all {
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
        self.events
            .emit(
                "connect_validated",
                json!({
                    "connection": generation.get().to_string(),
                    "details": {
                        "protocol_level": connect.version.level(),
                        "keep_alive": connect.keep_alive,
                        "max_packet_size": connect.max_packet_size,
                        "receive_maximum": connect.receive_maximum,
                        "topic_alias_maximum": connect.topic_alias_maximum,
                        "request_problem_information": connect.request_problem_information,
                        "clean_start": connect.clean_start,
                        "expiry": expiry_json(connect.expiry),
                    },
                }),
            )
            .map_err(event_failure)?;
        let now_ns = self.elapsed_ns(Instant::now());
        if let Some(p) = self.peers.get_mut(&generation) {
            if p.connected || p.pending {
                return self.close(generation, "second_connect");
            }
            p.keep_alive = connect.keep_alive;
            p.max_packet = connect.max_packet_size;
            p.receive_maximum = connect.receive_maximum;
            p.last_packet_ns = now_ns;
        }
        #[cfg(feature = "test-hooks")]
        if self.captured.is_none() {
            self.events
                .emit(
                    "input_captured",
                    json!({
                        "connection": generation.get().to_string(),
                        "details": {"packet_kind": "CONNECT"},
                    }),
                )
                .map_err(event_failure)?;
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
            if peer.version == Some(codec::Version::V311) {
                return self.close(old, "session_taken_over");
            }
            let bytes = codec::disconnect_reason(0x8e);
            if peer
                .io
                .try_send_outgoing(net::Write {
                    expiry: None,
                    correlation: None,
                    bytes: net::FrameBytes::from(bytes),
                    payload: None,
                    report: net::WriteReport::Complete,
                })
                .is_ok()
            {
                peer.set_closing(("session_taken_over", Instant::now()));
                self.pending_peers.insert(old);
                #[cfg(feature = "test-hooks")]
                if self.hooks.directory.is_some() {
                    self.events
                        .emit(
                            "takeover_disconnect_queued",
                            json!({"connection": old.get().to_string()}),
                        )
                        .map_err(event_failure)?;
                }
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
        body: impl Into<net::FrameBytes>,
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
                && bytes.len() <= before_connack.unwrap_or(peer.max_packet_bytes())
                && peer
                    .io
                    .try_send_outgoing(net::Write {
                        correlation: None,
                        bytes: net::FrameBytes::from(bytes),
                        payload: None,
                        expiry: None,
                        report: net::WriteReport::Complete,
                    })
                    .is_ok()
            {
                peer.set_closing(("not_authorized", Instant::now()));
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
            && p.closing().is_none()
        {
            let bytes = codec::disconnect_reason(reason);
            if bytes.len() <= p.max_packet_bytes()
                && p.io
                    .try_send_outgoing(net::Write {
                        expiry: None,
                        correlation: None,
                        bytes: net::FrameBytes::from(bytes),
                        payload: None,
                        report: net::WriteReport::Complete,
                    })
                    .is_ok()
            {
                p.set_closing((code, Instant::now()));
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
        body: impl Into<net::FrameBytes>,
        observed: net::FrameObservation,
    ) -> Result<(), Failure> {
        let body = body.into();
        let (has_wire_progress, version) = match self.peers.get(&generation) {
            Some(peer) => (peer.io.has_pending_wire_progress(), peer.version),
            None => {
                if !self.admit_generation(generation)? {
                    return Ok(());
                }
                (false, None)
            }
        };
        if has_wire_progress {
            self.wire_progress(generation)?;
        }
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
        let mut decoded = match codec::decode_for_with_cache(
            version.unwrap_or_default(),
            first,
            &body,
            BOUNDS,
            Some(&mut self.topic_cache),
        ) {
            Ok(v) => v,
            Err(error) => {
                self.events
                    .emit(
                        "packet_refused",
                        json!({
                            "connection": generation.get().to_string(),
                            "details": {"code": error.code(), "reason": error.reason_code()},
                        }),
                    )
                    .map_err(event_failure)?;
                // Refusal output has the same bounded absolute write path as business frames.
                if let Some(p) = self.peers.get_mut(&generation) {
                    let cap = if p.connected {
                        Some(p.max_packet)
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
                            .try_send_outgoing(net::Write {
                                expiry: None,
                                correlation: None,
                                bytes: net::FrameBytes::from(bytes),
                                payload: None,
                                report: net::WriteReport::Complete,
                            })
                            .is_ok()
                    {
                        p.set_closing(("packet_refused", Instant::now()));
                        self.pending_peers.insert(generation);
                        return Ok(());
                    }
                }
                return self.close(generation, "packet_refused");
            }
        };
        if let Some(security) = &self.security {
            let needs_auth = matches!(
                decoded,
                Packet::Core(_) | Packet::TimedPublish { .. } | Packet::Requests { .. }
            );
            if needs_auth {
                let Some(principal) = self
                    .peers
                    .get(&generation)
                    .and_then(|peer| peer.io.principal())
                else {
                    return self.close(generation, "tls_principal_missing");
                };
                let mut filters_denied = false;
                let mut authorize = |packet: &mut InboundPacket| -> Option<&'static str> {
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
                        None
                    } else if !security.policy.allows_packet(principal, packet) {
                        Some(if matches!(packet, InboundPacket::Publish { .. }) {
                            "publish"
                        } else {
                            "unsubscribe"
                        })
                    } else {
                        None
                    }
                };
                let mut refused_stage: Option<&'static str> = None;
                match &mut decoded {
                    Packet::Core(packet) | Packet::TimedPublish { packet, .. } => {
                        refused_stage = authorize(packet);
                    }
                    // Each unit request is authorized as a request of its own.
                    Packet::Requests { parts, .. } => {
                        for packet in parts.iter_mut() {
                            if let Some(stage) = authorize(packet) {
                                refused_stage = Some(stage);
                                break;
                            }
                        }
                    }
                    _ => {}
                }
                if let Some(stage) = refused_stage {
                    return self.authorization_refusal(generation, stage, None);
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
                self.events.emit("message_expiry_received", json!({
                    "connection": generation.get().to_string(),
                    "details": {
                        "topic": std::str::from_utf8(topic.as_slice()).map_err(|_| Failure::plain("invalid_topic"))?.chars().take(256).collect::<String>(),
                        "qos": qos.to_wire(),
                        "retain": retain,
                        "interval": interval,
                        "authority": "reader_frame_completion",
                        "domain": hex_domain(observed.domain),
                        "tick": observed.tick.to_string(),
                    },
                })).map_err(event_failure)?;
                self.message_clock_needed = true;
                Packet::Core(packet)
            }
            other => other,
        };
        let started = self.started;
        let shards_active = self.shards.is_some();
        let p = self
            .peers
            .get_mut(&generation)
            .ok_or_else(|| Failure::plain("generation_missing"))?;
        if p.keep_alive != 0 && p.io.last_heard().is_none() {
            p.last_packet_ns =
                u64::try_from(Instant::now().saturating_duration_since(started).as_nanos())
                    .unwrap_or(u64::MAX);
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
                if shards_active && !unsubscribe {
                    p.io.owe_suback();
                }
                let cold = p.cold_mut();
                if cold.requests.is_some() {
                    return Err(Failure::plain("requests_overlap"));
                }
                cold.requests = Some(Requests {
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
                    self.events.emit("packet_refused", json!({
                        "connection": generation.get().to_string(),
                        "details": {"code": "SessionExpiryIncreaseForbidden", "reason": 0x82},
                    })).map_err(event_failure)?;
                    if bytes.len() <= p.max_packet_bytes()
                        && p.io
                            .try_send_outgoing(net::Write {
                                expiry: None,
                                correlation: None,
                                bytes: net::FrameBytes::from(bytes),
                                payload: None,
                                report: net::WriteReport::Complete,
                            })
                            .is_ok()
                    {
                        p.set_closing(("packet_refused", Instant::now()));
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
                let connect_expiry = p.connect_expiry;
                let will_managed = p.will_managed;
                {
                    let cold = p.cold_mut();
                    cold.will_disconnect = Some(publish_will);
                    cold.disconnect_expiry = expiry;
                    cold.close_observed_at = Some(observed_at);
                }
                if let Some(pending) = self
                    .session_requests
                    .values_mut()
                    .find(|pending| pending.connection == generation)
                {
                    pending.close_intent = Some(CloseIntent {
                        expiry: expiry.unwrap_or(connect_expiry),
                        observed_at,
                        explicit: expiry.is_some(),
                        will: will_managed.then_some(publish_will),
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
                if shards_active && matches!(packet, InboundPacket::Subscribe { .. }) {
                    p.io.owe_suback();
                }
                p.ingress = p
                    .ingress
                    .checked_add(1)
                    .ok_or_else(|| Failure::plain("ingress_exhausted"))?;
                let ingress_order = p.ingress;
                let peer_session = p.session;
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
                        .receiving_mut()
                        .publish_qos(
                            65535,
                            ingress_order,
                            mqttiumd_contract::packet::Qos::AtLeastOnce,
                        )
                        .is_ok();
                    self.events
                        .emit(
                            "qos0_receive_credit",
                            json!({
                                "connection": generation.get().to_string(),
                                "details": {"charged": charged, "wrong_qos": 1},
                            }),
                        )
                        .map_err(event_failure)?;
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
                    self.events
                        .emit_with("publish_validated", || {
                            json!({
                                "connection": generation.get().to_string(),
                                "details": {
                                    "packet_id": pid,
                                    "qos": wire_qos,
                                    "dup": dup,
                                    "publish_order": ingress_order.to_string(),
                                },
                            })
                        })
                        .map_err(event_failure)?;
                }
                if let InboundPacket::Publish {
                    packet_id: Some(id),
                    qos,
                    ..
                } = &packet
                    && p.receiving_mut()
                        .publish_qos(id.get(), ingress_order, *qos)
                        .is_err()
                {
                    let id_raw = id.get();
                    let qos_val = *qos;
                    if p.io.has_pending_written() {
                        if let Some(code) = self.collect_written(generation)? {
                            return self.close(generation, code);
                        }
                        if self.peers.get_mut(&generation).is_some_and(|peer| {
                            peer.receiving_mut()
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
                                    p.receiving_can_complete(id, frame.receive_serial, terminal)
                                })
                    });
                    // MQTT 3.1.1 has no Receive Maximum to announce: a burst
                    // beyond the window is pressure, never a violation.
                    let unannounced = p.version == Some(codec::Version::V311);
                    if pending_terminal || unannounced {
                        let cold = p.cold_mut();
                        if cold.deferred_publish.is_some() {
                            return Err(Failure::plain("deferred_publish_overlap"));
                        }
                        // A successful write can become visible to the client
                        // just before the network thread publishes its progress.
                        // Keep this one bounded input instead of declaring a
                        // Receive Maximum violation from a stale window.
                        cold.deferred_publish = Some(DeferredPublish {
                            packet,
                            ingress_order,
                            since: Instant::now(),
                            wire: pending_terminal,
                        });
                        self.pending_peers.insert(generation);
                        self.events
                            .emit(
                                "publish_waiting_wire",
                                json!({
                                    "connection": generation.get().to_string(),
                                    "details": {"publish_order": ingress_order.to_string()},
                                }),
                            )
                            .map_err(event_failure)?;
                        return Ok(());
                    }
                    let bytes = net::FrameBytes::from_slice(&[0xe0, 2, 0x93, 0]);
                    if p.version != Some(codec::Version::V311)
                        && bytes.len() <= p.max_packet_bytes()
                        && p.io
                            .try_send_outgoing(net::Write {
                                expiry: None,
                                correlation: None,
                                bytes,
                                payload: None,
                                report: net::WriteReport::Complete,
                            })
                            .is_ok()
                    {
                        p.set_closing(("receive_maximum", Instant::now()));
                        self.pending_peers.insert(generation);
                        return Ok(());
                    }
                    return self.close(generation, "receive_maximum");
                }
                if p.version == Some(codec::Version::V311)
                    && let InboundPacket::Pubrel { packet_id } = &packet
                    && let Some(window) = p.receiving.as_mut()
                {
                    window.pubrel_observed(packet_id.get());
                }
                if let InboundPacket::Puback { packet_id, reason } = packet {
                    // The durable identity belongs to this transport's
                    // confirmed CONNECT. A takeover may already have moved
                    // owner authority while its old Close effect is blocked.
                    // Preserve the original key, never retarget or fail other
                    // connections merely because this generation is stale.
                    let session =
                        peer_session.ok_or_else(|| Failure::plain("puback_session_unconfirmed"))?;
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
                    let session = peer_session
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
        let active_peer_count = keys.len();
        let max_non_ack_per_peer = if active_peer_count > 16 {
            (512 / active_peer_count).clamp(16, 32)
        } else {
            32
        };
        let mut cursor_start = 0usize;
        if !keys.is_empty() {
            cursor_start = self.peer_cursor % keys.len();
            keys.rotate_left(cursor_start);
            self.peer_cursor = cursor_start + 1;
        }
        let mut starved_offset: Option<usize> = None;
        let result = (|| -> Result<(), Failure> {
            for (peer_offset, &generation) in keys.iter().enumerate() {
                let (
                    active,
                    needs_deadline_check,
                    is_retiring,
                    tls,
                    has_wire_progress,
                    has_written,
                    has_cold,
                ) = match self.peers.get(&generation) {
                    Some(peer) => {
                        let io_active = peer.io.take_activity();
                        let cold = peer.cold.as_deref();
                        let state_active = io_active || cold.is_some_and(PeerCold::state_active);
                        let needs_deadline = deadline_sweep
                            || peer.pending
                            || !peer.connected
                            || cold
                                .is_some_and(|c| c.closing.is_some() || c.capacity_wait.is_some());
                        (
                            state_active,
                            needs_deadline,
                            cold.and_then(|c| c.retire_code),
                            peer.io.is_tls(),
                            peer.io.has_pending_wire_progress(),
                            peer.io.has_pending_written(),
                            cold.is_some(),
                        )
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
                    let mut tls_close = None;
                    let mut write_close = None;
                    let mut input_ended = false;
                    if has_wire_progress {
                        self.wire_progress(generation)?;
                    }
                    if has_written && let Some(code) = self.collect_written(generation)? {
                        write_close = Some(code);
                    }
                    let backpressured = if has_cold {
                        self.peers.get_mut(&generation).is_some_and(|peer| {
                            if let Some(effect) = peer.blocked_output()
                                && self
                                    .owner
                                    .outbox()
                                    .pending()
                                    .any(|pending| pending.id() == effect)
                            {
                                peer.io.terminal_reason().is_none()
                            } else {
                                if let Some(c) = peer.cold.as_mut() {
                                    c.blocked_output = None;
                                }
                                peer.gc_cold();
                                false
                            }
                        })
                    } else {
                        false
                    };
                    if has_cold {
                        if !backpressured {
                            self.retry_deferred_publish(generation)?;
                        }
                        self.serve_requests(generation)?;
                    }
                    // A held packet that cannot fit keeps its place. Repeated
                    // polls of the same retained packet are not reader progress.
                    let blocked_prefix = has_cold
                        && self
                            .peers
                            .get(&generation)
                            .is_some_and(Peer::lookahead_blocked);
                    if blocked_prefix
                        && self
                            .peers
                            .get(&generation)
                            .and_then(|peer| peer.receiving.as_ref())
                            .is_some_and(|window| window.all_wait_for_remote_pubrel())
                    {
                        self.close(generation, "receive_lookahead_capacity")?;
                        continue;
                    }
                    let backpressured = backpressured || blocked_prefix;
                    drained_incoming = false;
                    if !backpressured
                        && self.ordinary_room() >= 2
                        && let Some(mut ingress) = self
                            .peers
                            .get(&generation)
                            .filter(|peer| {
                                peer.connected
                                    && !peer.pending
                                    && peer.cold.is_none()
                                    && peer.io.has_pending_incoming()
                            })
                            .and_then(|peer| peer.io.incoming_batch())
                    {
                        // Cap non-ACK ingress packets at 32 per peer per turn so
                        // one publisher cannot monopolize the lane, while allowing
                        // credit-releasing ACKs (PUBACK/PUBREC/PUBREL/PUBCOMP) to
                        // drain up to 256 per turn so a fan-in subscriber keeps up
                        // with many publishers inside one group commit.
                        let mut batch = std::mem::take(&mut self.scratch_incoming);
                        batch.clear();
                        let mut hit_terminal = false;
                        let mut restored = false;
                        let mut status = net::DrainStatus::Empty;
                        let mut total_drained = 0usize;
                        let mut non_ack_drained = 0usize;
                        let mut saw_ack = false;
                        let max_non_ack = max_non_ack_per_peer;
                        const MAX_TOTAL_FAST: usize = 256;
                        loop {
                            let room = self.ordinary_room().saturating_sub(1);
                            if room == 0 {
                                starved_offset.get_or_insert(peer_offset);
                                break;
                            }
                            if total_drained >= MAX_TOTAL_FAST || non_ack_drained >= max_non_ack {
                                break;
                            }
                            let cap = if saw_ack {
                                32
                            } else {
                                (max_non_ack - non_ack_drained).min(32)
                            };
                            let chunk = cap.min(room).min(MAX_TOTAL_FAST - total_drained);
                            status = ingress.take(&mut batch, chunk);
                            if batch.is_empty() {
                                break;
                            }
                            let mut stopped_early = false;
                            while let Some(incoming) = batch.pop_front() {
                                self.progress = self.progress.wrapping_add(1);
                                if self.ordinary_room() < 2 {
                                    starved_offset.get_or_insert(peer_offset);
                                    batch.push_front(incoming);
                                    stopped_early = true;
                                    break;
                                }
                                if let net::Incoming::Packet { first, .. } = &incoming
                                    && !matches!(first >> 4, 4..=7)
                                    && non_ack_drained >= max_non_ack
                                {
                                    batch.push_front(incoming);
                                    stopped_early = true;
                                    break;
                                }
                                let business_waits = self.business_waits(generation);
                                let (stop, peer_pending, has_deferred) =
                                    match self.peers.get(&generation) {
                                        Some(p) => {
                                            let c = p.cold.as_deref();
                                            (
                                                c.is_some_and(|c| {
                                                    c.closing.is_some() || c.requests.is_some()
                                                }),
                                                p.pending,
                                                c.is_some_and(|c| c.deferred_publish.is_some()),
                                            )
                                        }
                                        None => (true, false, false),
                                    };
                                if stop {
                                    batch.push_front(incoming);
                                    stopped_early = true;
                                    break;
                                }
                                total_drained += 1;
                                match incoming {
                                    net::Incoming::Authenticated { elapsed } => {
                                        non_ack_drained += 1;
                                        self.events
                                            .emit(
                                                "tls_authenticated",
                                                json!({
                                                    "connection": generation.get().to_string(),
                                                    "details": {
                                                        "version": "TLS1.3",
                                                        "handshake_us": elapsed.as_micros() as u64,
                                                    },
                                                }),
                                            )
                                            .map_err(event_failure)?;
                                    }
                                    net::Incoming::Packet {
                                        first,
                                        body,
                                        observed,
                                    } => {
                                        if matches!(first >> 4, 4..=7) {
                                            saw_ack = true;
                                        } else {
                                            non_ack_drained += 1;
                                        }
                                        if first >> 4 != 14 && peer_pending {
                                            if let Some(peer) = self.peers.get_mut(&generation) {
                                                peer.cold_mut().held_packet =
                                                    Some((first, body, observed));
                                            }
                                            stopped_early = true;
                                            break;
                                        }
                                        if has_deferred
                                            && let Some(peer) = self.peers.get_mut(&generation)
                                            && !peer.overtakes_deferred(first, &body)
                                        {
                                            match peer.keep_behind((first, body, observed)) {
                                                Ok(()) => continue,
                                                Err(packet) => {
                                                    peer.cold_mut().held_packet = Some(packet);
                                                    stopped_early = true;
                                                    break;
                                                }
                                            }
                                        }
                                        if business_waits
                                            && business_header(first)
                                            && let Some(peer) = self.peers.get_mut(&generation)
                                        {
                                            peer.cold_mut().held_packet =
                                                Some((first, body, observed));
                                            stopped_early = true;
                                            break;
                                        }
                                        self.packet_at(generation, first, body, observed)?;
                                    }
                                    net::Incoming::Refused { code } => {
                                        hit_terminal = true;
                                        self.header_refused(generation, code)?;
                                        stopped_early = true;
                                        break;
                                    }
                                    net::Incoming::Closed { code } => {
                                        hit_terminal = true;
                                        if tls {
                                            input_ended = true;
                                            tls_close = Some(code);
                                            stopped_early = true;
                                            break;
                                        }
                                        self.drain_inputs()?;
                                        self.close(generation, code)?;
                                        stopped_early = true;
                                        break;
                                    }
                                }
                            }
                            restored = !batch.is_empty();
                            ingress.finish(&mut batch);
                            if stopped_early || status != net::DrainStatus::More {
                                break;
                            }
                        }
                        if !restored && !hit_terminal {
                            match status {
                                net::DrainStatus::Empty => {
                                    drained_incoming = true;
                                }
                                net::DrainStatus::Disconnected => {
                                    drained_incoming = true;
                                    let code = self
                                        .peers
                                        .get(&generation)
                                        .and_then(|p| p.io.terminal_reason())
                                        .unwrap_or("reader_closed");
                                    if tls {
                                        input_ended = true;
                                        tls_close = Some(code);
                                    } else {
                                        self.drain_inputs()?;
                                        self.close(generation, code)?;
                                    }
                                }
                                net::DrainStatus::More => {}
                            }
                        }
                        self.scratch_incoming = batch;
                    } else {
                        let mut one = std::mem::take(&mut self.scratch_incoming);
                        one.clear();
                        for _ in 0..max_non_ack_per_peer {
                            // A CONNECT can use one slot now and reserve one for Open.
                            // Leave the bytes with their socket reader until both fit.
                            if backpressured {
                                break;
                            }
                            if self.ordinary_room() < 2 {
                                starved_offset.get_or_insert(peer_offset);
                                break;
                            }
                            let business_waits = self.business_waits(generation);
                            // A packet that was kept behind a publication is
                            // older than the held packet and than the lane.
                            let mut from_behind = false;
                            let incoming = match self.peers.get_mut(&generation) {
                                Some(p)
                                    if p.cold.as_deref().is_some_and(|c| {
                                        c.closing.is_some() || c.requests.is_some()
                                    }) =>
                                {
                                    break;
                                }
                                Some(p)
                                    if p.pending
                                        && p.cold
                                            .as_deref()
                                            .is_some_and(|c| c.held_packet.is_some()) =>
                                {
                                    break;
                                }
                                Some(p)
                                    if p.cold.as_deref().is_some_and(|c| {
                                        c.deferred_publish.is_none() && !c.behind.is_empty()
                                    }) =>
                                {
                                    let cold = p.cold.as_mut().expect("checked cold");
                                    if business_waits
                                        && cold
                                            .behind
                                            .front()
                                            .is_some_and(|(first, ..)| business_header(*first))
                                    {
                                        break;
                                    }
                                    let (first, body, observed) =
                                        cold.behind.pop_front().expect("checked packet behind");
                                    cold.behind_bytes -= body.len();
                                    p.gc_cold();
                                    from_behind = true;
                                    Ok(net::Incoming::Packet {
                                        first,
                                        body,
                                        observed,
                                    })
                                }
                                Some(p)
                                    if business_waits
                                        && p.cold.as_deref().is_some_and(|c| {
                                            c.deferred_publish.is_none()
                                                && c.held_packet.as_ref().is_some_and(
                                                    |(first, ..)| business_header(*first),
                                                )
                                        }) =>
                                {
                                    break;
                                }
                                Some(p) => {
                                    let held = p.cold.as_mut().and_then(|c| c.held_packet.take());
                                    if held.is_some() {
                                        p.gc_cold();
                                    }
                                    match held {
                                        Some((first, body, observed)) => {
                                            Ok(net::Incoming::Packet {
                                                first,
                                                body,
                                                observed,
                                            })
                                        }
                                        None if !p.io.has_pending_incoming() => {
                                            drained_incoming = true;
                                            break;
                                        }
                                        None => {
                                            one.clear();
                                            let status = p.io.drain_incoming(&mut one, 1);
                                            if let Some(pkt) = one.pop_front() {
                                                Ok(pkt)
                                            } else if status == net::DrainStatus::Disconnected {
                                                Err(TryRecvError::Disconnected)
                                            } else {
                                                Err(TryRecvError::Empty)
                                            }
                                        }
                                    }
                                }
                                None => break,
                            };
                            if incoming.is_ok() {
                                self.progress = self.progress.wrapping_add(1);
                            }
                            match incoming {
                                Ok(net::Incoming::Authenticated { elapsed }) => {
                                    self.events
                                        .emit(
                                            "tls_authenticated",
                                            json!({
                                                "connection": generation.get().to_string(),
                                                "details": {
                                                    "version": "TLS1.3",
                                                    "handshake_us": elapsed.as_micros() as u64,
                                                },
                                            }),
                                        )
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
                                            .cold_mut()
                                            .held_packet = Some((first, body, observed));
                                        break;
                                    }
                                    // A publication waits for a slot of the window.
                                    // The PUBREL of another exchange goes ahead: it
                                    // frees a slot. Every other packet waits behind
                                    // it, in order. Retain the first packet that
                                    // does not fit for the local-progress check.
                                    if let Some(peer) = self.peers.get_mut(&generation)
                                        && peer.deferred_publish().is_some()
                                        && !peer.overtakes_deferred(first, &body)
                                    {
                                        match peer.keep_behind((first, body, observed)) {
                                            Ok(()) => continue,
                                            Err(packet) => {
                                                peer.cold_mut().held_packet = Some(packet);
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
                                        let cold = peer.cold_mut();
                                        if from_behind {
                                            cold.behind_bytes += body.len();
                                            cold.behind.push_front((first, body, observed));
                                        } else {
                                            cold.held_packet = Some((first, body, observed));
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
                        self.scratch_incoming = one;
                    }
                    if self
                        .peers
                        .get(&generation)
                        .is_some_and(|peer| peer.io.has_pending_written())
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
                                .is_some_and(|peer| peer.closing().is_some()))
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
                            let (_, since) = peer.cold_mut().closing.get_or_insert((code, now));
                            if now.saturating_duration_since(*since) < Duration::from_millis(300) {
                                self.pending_peers.insert(generation);
                                continue;
                            }
                        }
                        self.close(generation, code)?;
                    }
                }
                if needs_deadline_check && let Some(p) = self.peers.get_mut(&generation) {
                    let closing = p.closing();
                    let capacity_wait = p.cold.as_deref().and_then(|c| c.capacity_wait);
                    let last_heard_or_pkt =
                        p.io.last_heard()
                            .unwrap_or_else(|| p.last_packet_instant(self.started));
                    let expired = if let Some((code, since)) = closing
                        && now.saturating_duration_since(since) > Duration::from_millis(300)
                    {
                        Some(code)
                    } else if (p.pending || !p.connected)
                        && now.saturating_duration_since(p.created_instant(self.started))
                            > Duration::from_secs(3)
                    {
                        Some("connect_deadline")
                    } else if capacity_wait.is_some_and(|since| {
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
                        && now.saturating_duration_since(last_heard_or_pkt)
                            > Duration::from_millis(u64::from(p.keep_alive) * 1500)
                    {
                        Some("keep_alive")
                    } else {
                        if deadline_sweep
                            && !active
                            && p.connected
                            && !p.pending
                            && p.cold.is_none()
                            && now.saturating_duration_since(last_heard_or_pkt)
                                >= Duration::from_millis(200)
                        {
                            p.io.shrink_idle_queues();
                            if p.receiving.as_ref().is_some_and(|w| w.is_empty()) {
                                p.receiving = None;
                            }
                        }
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
        if let Some(offset) = starved_offset {
            self.peer_cursor = cursor_start.wrapping_add(offset);
        }
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
        let mut batch = std::mem::take(&mut self.scratch_written);
        batch.clear();
        let (tls, status) = match self.peers.get(&generation) {
            Some(peer) => (
                peer.io.is_tls(),
                peer.io.drain_written(&mut batch, net::WRITTEN_SLOTS),
            ),
            None => {
                self.scratch_written = batch;
                return Ok(None);
            }
        };
        let mut tls_close = None;
        let mut broke_early = false;
        let res = (|| -> Result<(), Failure> {
            for written in batch.drain(..) {
                self.progress = self.progress.wrapping_add(1);
                match written {
                    net::Written::Started { correlation } => {
                        if let Some((effect, connection)) = correlation
                            && self.frames.get(&effect).is_some_and(|frame| {
                                frame.connection == connection
                                    && matches!(
                                        frame.kind,
                                        FrameKind::Publish | FrameKind::Publish0
                                    )
                            })
                        {
                            self.events
                                .emit(
                                    "message_transmission_started",
                                    json!({
                                        "connection": connection.generation.get().to_string(),
                                        "details": {
                                            "effect": effect_text(effect),
                                            "authority": "positive_socket_write",
                                        },
                                    }),
                                )
                                .map_err(event_failure)?;
                            self.queue(Input::FrameResult {
                                effect,
                                connection,
                                progress: FrameProgress::Started,
                            })?;
                        }
                    }
                    net::Written::NotStarted {
                        correlation,
                        reason,
                    } => {
                        if let Some((effect, connection)) = correlation
                            && let Some(pending) = self.frames.remove(&effect)
                        {
                            if pending.connection != connection {
                                return Err(Failure::plain("frame_connection_mismatch"));
                            }
                            self.outgoing_bindings
                                .retain(|_, value| value.frame != effect);
                            self.events
                                .emit(
                                    "message_transmission_not_started",
                                    json!({
                                        "connection": connection.generation.get().to_string(),
                                        "details": {
                                            "effect": effect_text(effect),
                                            "reason": format!("{reason:?}"),
                                            "authority": "zero_socket_bytes",
                                        },
                                    }),
                                )
                                .map_err(event_failure)?;
                            if pending.kind == FrameKind::Publish0 {
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
                    net::Written::Complete { correlation, bytes } => {
                        if let Some((effect, connection)) = correlation {
                            #[cfg(feature = "test-hooks")]
                            if self
                                .frames
                                .get(&effect)
                                .is_some_and(|frame| frame.kind == FrameKind::Publish)
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
                                self.events
                                    .emit(
                                        "outgoing_written_held",
                                        json!({
                                            "connection": connection.generation.get().to_string(),
                                            "details": {
                                                "effect": effect_text(effect),
                                                "socket_write_complete": true,
                                                "bytes": bytes,
                                            },
                                        }),
                                    )
                                    .map_err(event_failure)?;
                                continue;
                            }
                            #[cfg(feature = "test-hooks")]
                            if !self.deferred_ack_used
                                && self.hooks.pause.iter().any(|p| p == "ack.receipt")
                                && let Some(pending) = self.frames.get(&effect)
                                && pending.kind == FrameKind::Puback
                            {
                                self.events.emit("barrier_hit", json!({
                                    "point": "ack.receipt",
                                    "connection": generation.get().to_string(),
                                    "details": {
                                        "effect": effect_text(effect),
                                        "packet_id": pending.packet_id.unwrap_or(0),
                                        "publish_order": pending.receive_serial.map(|v|v.to_string()).unwrap_or_default(),
                                        "bytes": bytes,
                                        "socket_write_complete": true,
                                    },
                                })).map_err(event_failure)?;
                                self.deferred_ack_used = true;
                                self.deferred_ack =
                                    Some((effect, connection, bytes, Instant::now()));
                                continue;
                            }
                            self.written_complete(effect, connection, bytes)?;
                        } else if let Some((code, _)) =
                            self.peers.get(&generation).and_then(Peer::closing)
                        {
                            tls_close = Some(code);
                        }
                    }
                    net::Written::Failed { correlation, code } => {
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
                            broke_early = true;
                            break;
                        }
                    }
                }
            }
            if !broke_early && status == net::DrainStatus::Disconnected {
                if tls {
                    let reason = self
                        .peers
                        .get(&generation)
                        .and_then(|peer| peer.io.terminal_reason());
                    tls_close.get_or_insert(reason.unwrap_or("writer_closed"));
                } else {
                    tls_close = Some("writer_closed");
                }
            }
            Ok(())
        })();
        batch.clear();
        self.scratch_written = batch;
        res?;
        Ok(tls_close)
    }

    /// Network completion and core receipt consumption have separate lanes.
    /// No lock is held across I/O. Scope/occurrence/QoS are checked for every
    /// early receive-credit release; stale progress is never inferred by PI.
    fn wire_progress(&mut self, generation: ConnectionGeneration) -> Result<(), Failure> {
        let mut batch = std::mem::take(&mut self.scratch_progress);
        batch.clear();
        if let Some(peer) = self.peers.get(&generation) {
            peer.io
                .drain_wire_progress(&mut batch, net::OUTPUT_SLOTS + 1 + 8);
        }
        if batch.is_empty() {
            self.scratch_progress = batch;
            return Ok(());
        }
        let res = (|| -> Result<(), Failure> {
            let Some(peer) = self.peers.get_mut(&generation) else {
                return Ok(());
            };
            for progress in batch.drain(..) {
                self.progress = self.progress.wrapping_add(1);
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
                    let released = peer.receiving_complete(id, frame.receive_serial, terminal);
                    frame.wire_released |= released;
                    let frame_kind = frame.kind;
                    let frame_serial = frame.receive_serial;
                    self.events.emit_with("wire_receive_credit",||json!({
                        "connection": generation.get().to_string(),
                        "details": {
                            "effect": effect_text(progress.effect),
                            "packet_kind": frame_kind.as_str(),
                            "packet_id": id,
                            "publish_order": frame_serial.map(|v|v.to_string()).unwrap_or_default(),
                            "released": released,
                        },
                    })).map_err(event_failure)?;
                }
            }
            Ok(())
        })();
        batch.clear();
        self.scratch_progress = batch;
        res
    }

    fn retry_deferred_publish(&mut self, generation: ConnectionGeneration) -> Result<(), Failure> {
        if self.ordinary_room() == 0 {
            return Ok(());
        }
        let Some(peer) = self.peers.get_mut(&generation) else {
            return Ok(());
        };
        let Some(deferred) = peer.deferred_publish() else {
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
        let (id_raw, qos_val, ingress_order, wire, since) = (
            id.get(),
            *qos,
            deferred.ingress_order,
            deferred.wire,
            deferred.since,
        );
        if peer
            .receiving_mut()
            .publish_qos(id_raw, ingress_order, qos_val)
            .is_ok()
        {
            let deferred = peer
                .cold
                .as_mut()
                .and_then(|c| c.deferred_publish.take())
                .ok_or_else(|| Failure::plain("deferred_publish_missing"))?;
            peer.gc_cold();
            self.queue(Input::Packet {
                connection: generation,
                ingress_order: deferred.ingress_order,
                packet: deferred.packet,
            })?;
        } else if wire && since.elapsed() > Duration::from_millis(500) {
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
                self.events
                    .emit(
                        "frame_result_ignored",
                        json!({
                            "connection": connection.generation.get().to_string(),
                            "details": {"effect": effect_text(effect), "code": "retired_transport"},
                        }),
                    )
                    .map_err(event_failure)?;
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
        if kind == FrameKind::Publish0 {
            self.events
                .emit_with("frame_written", || {
                    json!({
                        "connection": generation.get().to_string(),
                        "details": {
                            "packet_kind": "PUBLISH",
                            "qos": 0,
                            "effect": effect_text(effect),
                            "bytes": bytes,
                        },
                    })
                })
                .map_err(event_failure)?;
        } else if kind != FrameKind::Pingresp || pending.was_retained {
            let serial = pending.receive_serial;
            self.events
                .emit_with("frame_written", || {
                    json!({
                        "connection": generation.get().to_string(),
                        "details": {
                            "packet_kind": kind.as_str(),
                            "effect": effect_text(effect),
                            "packet_id": id.unwrap_or(0),
                            "publish_order": serial.map(|v|v.to_string()).unwrap_or_default(),
                            "bytes": bytes,
                        },
                    })
                })
                .map_err(event_failure)?;
        }
        if kind == FrameKind::Pubrec
            && pending.terminal.is_none()
            && let Some(id) = id
            && let Some(peer) = self.peers.get_mut(&generation)
            && peer.version == Some(codec::Version::V311)
            && let Some(window) = peer.receiving.as_mut()
        {
            window.pubrec_written(id, pending.receive_serial);
        }
        if let Some(terminal) = pending.terminal
            && let Some(p) = self.peers.get_mut(&generation)
            && let Some(id) = id
        {
            let released_now = p.receiving_complete(id, pending.receive_serial, terminal);
            // If no newer occurrence occupies the identifier, this receipt
            // confirms its own already published wire completion. A newer
            // occurrence still meets the exact guards and is left untouched.
            let released = released_now || (pending.wire_released && !p.receiving_contains(id));
            #[cfg(feature = "test-hooks")]
            let released = if MUTANT_STALE_RECEIPT && !released {
                p.receiving.as_mut().is_some_and(|w| w.complete_current(id))
            } else {
                released
            };
            let serial = pending.receive_serial;
            self.events
                .emit_with("receive_credit", || {
                    json!({
                        "connection": generation.get().to_string(),
                        "details": {
                            "packet_kind": kind.as_str(),
                            "effect": effect_text(effect),
                            "packet_id": id,
                            "publish_order": serial.map(|v|v.to_string()).unwrap_or_default(),
                            "released": released,
                        },
                    })
                })
                .map_err(event_failure)?;
        }
        #[cfg(feature = "test-hooks")]
        if kind == FrameKind::Publish
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
            self.events
                .emit(
                    "outgoing_frame_result_copied",
                    json!({
                        "connection": generation.get().to_string(),
                        "details": {"effect": effect_text(effect), "bytes": bytes},
                    }),
                )
                .map_err(event_failure)?;
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
                    self.events.emit("outgoing_written_discarded",json!({
                        "connection": connection.generation.get().to_string(),
                        "details": {"effect": effect_text(effect), "code": "connection_closed"},
                    })).map_err(event_failure)?;
                } else if since.elapsed() > Duration::from_secs(30) {
                    return Err(Failure::plain("outgoing_written_deadline"));
                } else if consume_control(&dir.join("release-outgoing-written")) {
                    self.deferred_outgoing_written = None;
                    self.written_complete(effect, connection, bytes)?;
                    self.events
                        .emit(
                            "outgoing_written_released",
                            json!({
                                "connection": connection.generation.get().to_string(),
                                "details": {"effect": effect_text(effect)},
                            }),
                        )
                        .map_err(event_failure)?;
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
                self.events
                    .emit(
                        "outgoing_frame_result_replayed",
                        json!({
                            "connection": connection.generation.get().to_string(),
                            "details": {"effect": effect_text(effect)},
                        }),
                    )
                    .map_err(event_failure)?;
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
                self.events
                    .emit(
                        "finish_result_replayed",
                        json!({
                            "operation": operation(operation_id),
                            "command_kind": "Finish",
                            "details": {"effect": effect_text(effect)},
                        }),
                    )
                    .map_err(event_failure)?;
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
                    self.events
                        .emit(
                            "work_page_ignored",
                            json!({
                                "details": {
                                    "session": session.get().to_string(),
                                    "effect": effect_text(effect),
                                    "code": "retired_session",
                                },
                            }),
                        )
                        .map_err(event_failure)?;
                    continue;
                }
                for item in page.items.as_slice() {
                    if self.work_cache.len() >= crate::writer::SESSIONS * crate::receiving::MAXIMUM
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
                    self.events
                        .emit(
                            "barrier_released",
                            json!({
                                "point": "ack.receipt",
                                "connection": connection.generation.get().to_string(),
                                "details": {"effect": effect_text(effect)},
                            }),
                        )
                        .map_err(event_failure)?;
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
                "details":{"effect":effect_text(effect),"packet_kind":frame.kind.as_str(),"code":code}}),
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
        let mut fields = json!({
            "operation": operation(self.operation),
            "details": {"action": action, "session": session.get().to_string()},
        });
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
        let mut fields = json!({
            "connection": self.generation.get().to_string(),
            "details": {"session": self.session.get().to_string()},
        });
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
    peers: &'a mut PeerTable,
    events: &'a Events,
    inflight: &'a mut ScopedSeqMap<EffectId, OperationId>,
    inflight_route_ops: &'a mut ScopedSeqMap<EffectId, ()>,
    lifecycle_inflight: &'a mut BTreeMap<EffectId, OperationId>,
    lifecycle_operation: Option<OperationId>,
    will_operation: Option<OperationId>,
    will_reads: &'a mut Option<EffectId>,
    work_reads: &'a mut ScopedSeqMap<EffectId, SessionId>,
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
    terminal_effects: &'a mut ScopedSeqMap<EffectId, u32>,
    ordinary_reserved: usize,
    queued_writer_replies: Option<usize>,
    frames: &'a mut ScopedSeqMap<EffectId, PendingFrame>,
    outgoing_bindings: &'a mut FlatMap<(ConnectionKey, OutboundPacketId), PubackTarget>,
    shards_active: bool,
    shard_attach_ready: &'a mut Vec<ConnectionKey>,
    shard_suback_ready: &'a mut Vec<SessionId>,
    limits: Limits,
    close: &'a mut Vec<(ConnectionGeneration, &'static str)>,
    mark_pending_peers: &'a mut Vec<ConnectionGeneration>,
    pending_work_targets: &'a mut Vec<SessionId>,
    direct_skip_next_deadline: bool,
    session_change: Option<AppliedSessionChange>,
    error: Option<Failure>,
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
    let target = target.map(|target| {
        json!({
            "delivery_id": {
                "publication_id": {
                    "incarnation": target.delivery.publication.incarnation().get().to_string(),
                    "epoch": target.delivery.publication.epoch().get().to_string(),
                    "owner": target.delivery.publication.owner().get().to_string(),
                    "sequence": target.delivery.publication.sequence().get().to_string(),
                },
                "index": target.delivery.index,
            },
            "claim_operation_id": operation(target.claim),
            "frame_effect_id": {
                "epoch": target.frame.epoch.get().to_string(),
                "owner": target.frame.owner.get().to_string(),
                "sequence": target.frame.sequence.get().to_string(),
            },
        })
    });
    let mut puback = json!({
        "connection": {
            "session": connection.session.get().to_string(),
            "generation": connection.generation.get().to_string(),
        },
        "packet_id": packet_id.get(),
        "reason_code": reason,
        "target": target,
    });
    if let Some(sequence) = observation {
        puback["source_observation_sequence"] = json!(sequence.to_string());
    }
    json!({
        "connection": connection.generation.get().to_string(),
        "puback": puback,
        "details": {"authority": "reader_dequeue", "code": code},
    })
}
/// The kind of an outgoing frame the runtime tracks until its write is
/// reported. `Publish0` is a QoS 0 PUBLISH, which has no packet identifier.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum FrameKind {
    Connack,
    Pingresp,
    Puback,
    Pubrec,
    Pubcomp,
    Pubrel,
    Suback,
    Unsuback,
    Publish,
    Publish0,
}

impl FrameKind {
    /// The name the event journal gives the frame.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Connack => "CONNACK",
            Self::Pingresp => "PINGRESP",
            Self::Puback => "PUBACK",
            Self::Pubrec => "PUBREC",
            Self::Pubcomp => "PUBCOMP",
            Self::Pubrel => "PUBREL",
            Self::Suback => "SUBACK",
            Self::Unsuback => "UNSUBACK",
            Self::Publish => "PUBLISH",
            Self::Publish0 => "PUBLISH0",
        }
    }
}

fn frame_info(frame: OutboundFrame) -> (FrameKind, Option<u16>) {
    match frame {
        OutboundFrame::Connack { .. } => (FrameKind::Connack, None),
        OutboundFrame::Pingresp => (FrameKind::Pingresp, None),
        OutboundFrame::Puback { packet_id, .. } => (FrameKind::Puback, Some(packet_id.get())),
        OutboundFrame::Pubrec { packet_id, .. } => (FrameKind::Pubrec, Some(packet_id.get())),
        OutboundFrame::Pubcomp { packet_id, .. } => (FrameKind::Pubcomp, Some(packet_id.get())),
        OutboundFrame::Pubrel { packet_id } => (FrameKind::Pubrel, Some(packet_id.get())),
        OutboundFrame::Suback { packet_id, .. } => (FrameKind::Suback, Some(packet_id.get())),
        OutboundFrame::Unsuback { packet_id, .. } => (FrameKind::Unsuback, Some(packet_id.get())),
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
        return json!({
            "publication": format!("{:?}",publication),
            "topic": String::from_utf8_lossy(topic.as_slice()).chars().take(256).collect::<String>(),
            "message_expiry_domain": hex_domain(value.expiry.observed.domain),
            "message_expiry_tick": value.expiry.observed.tick.to_string(),
            "message_expiry_interval": value.expiry.interval,
        });
    }
    match recipe {
        Recipe::WillPublication(value) => {
            json!({
                "will": operation_text(value.will),
                "publication": format!("{:?}",value.publication),
                "destinations": value.targets.len(),
                "topic_bytes": value.topic.len(),
                "payload_bytes": value.payload.length.get(),
            })
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
                json!({
                    "action": "Open",
                    "clean_start": clean_start,
                    "expiry": expiry_json(*expiry),
                    "expected_session": expected.map(|g|g.session.get().to_string()),
                })
            }
            SessionAction::EndWill {
                guard,
                expiry,
                observed_at,
                publish,
            } => {
                json!({
                    "action": "EndWill",
                    "session": guard.session.get().to_string(),
                    "opened_by": guard.opened_by.map(operation_text),
                    "expiry": expiry_json(*expiry),
                    "observed_at": observed_at.to_string(),
                    "publish": publish,
                })
            }
            SessionAction::Detach {
                guard,
                expiry,
                observed_at,
            } => {
                json!({
                    "action": "Detach",
                    "session": guard.session.get().to_string(),
                    "expiry": expiry_json(*expiry),
                    "observed_at": observed_at.to_string(),
                })
            }
            SessionAction::Expire {
                guard,
                detached,
                now,
            } => {
                json!({
                    "action": "Expire",
                    "session": guard.session.get().to_string(),
                    "detached_by": operation(detached.operation),
                    "deadline": detached.deadline.map(|n|n.to_string()),
                    "now": now.to_string(),
                })
            }
            SessionAction::Close { guard, reason } => {
                json!({
                    "action": "Close",
                    "session": guard.session.get().to_string(),
                    "opened_by": guard.opened_by.map(operation_text).unwrap_or_default(),
                    "reason": format!("{reason:?}"),
                })
            }
        },
        Recipe::ConsumeDelivery(c) => {
            json!({"delivery":delivery_text(c.delivery),"target":c.target.get().to_string()})
        }
        Recipe::ClaimDelivery(c) => {
            json!({
                "delivery": delivery_text(c.delivery),
                "target": c.target.get().to_string(),
                "packet_id": c.packet_id.get(),
            })
        }
        Recipe::FinishDelivery(c) => {
            json!({
                "delivery": delivery_text(c.delivery),
                "target": c.target.get().to_string(),
                "packet_id": c.packet_id.get(),
                "claim": operation_text(c.claim),
            })
        }
        _ => json!({}),
    }
}
/// The origin of a QoS 0 delivery as the journal names it.
pub(crate) fn qos0_origin(origin: Qos0Origin) -> String {
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

mod sink;
mod start;
pub use start::run;

#[cfg(test)]
#[path = "runtime_tests.rs"]
mod tests;
