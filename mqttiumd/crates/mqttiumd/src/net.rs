//! Bounded socket tasks for the product adapter. No durable operation runs here.
//!
//! A connection is one non-blocking task driven by a reactor thread
//! (ADR 0044). The core keeps the same four bounded lanes it always had;
//! their handles tell the reactor when the core published a frame or took
//! a value from a lane the task had found full.

use std::collections::VecDeque;
use std::io::{self, IoSlice, Read, Write as _};
use std::net::{Shutdown, TcpStream};
use std::os::fd::{AsRawFd, RawFd};
#[cfg(test)]
use std::sync::Condvar;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::TrySendError;
#[cfg(test)]
use std::sync::mpsc::{Receiver, RecvTimeoutError, SendError, SyncSender, TryRecvError};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use mqttiumd_contract::ids::{ConnectionGeneration, ConnectionKey, EffectId, LogicalOwnerId};
use mqttiumd_contract::message_expiry::{
    ExpiryState, MessageExpiry, MessageExpiryBlock, MessageTime,
};
use mqttiumd_contract::packet::SharedBytes;

use crate::codec::{self, ErrorKind};
use crate::security::{Policy, Principal};
use crate::session_time::{SessionClock, SystemClock, TimeError};
use crate::wake::Wake;

pub(crate) mod reactor;
mod tls;

use reactor::{Driven, Link, Reactors, Turn};

pub const INPUT_SLOTS: usize = 32;
pub const OUTPUT_SLOTS: usize = 256;
pub const MAX_PACKET_BYTES: usize = 1_048_576 + 65_536;
// A correlated frame can publish Started and one terminal result. This is a
// receipt bound; it grants no additional outgoing-frame admission capacity.
pub const WRITTEN_SLOTS: usize = 2 * (OUTPUT_SLOTS + 1) + 8;
// Queued writes, one active write and eight terminal receipts. Wire
// progress is an independent bounded lane, not the core's receipt lane.
const WIRE_PROGRESS_SLOTS: usize = OUTPUT_SLOTS + 1 + 8;
#[cfg(test)]
const POLL: Duration = Duration::from_millis(20);
const RESULT_WAIT: Duration = Duration::from_millis(100);
const PARTIAL_PACKET_TIMEOUT: Duration = Duration::from_secs(5);
const WRITE_TIMEOUT: Duration = Duration::from_millis(250);
/// Bytes one reactor thread scratch read buffer holds per turn.
pub(crate) const READ_BUFFER: usize = 32_768;
/// Packets or frames one turn may move before other tasks are served.
const BURST: usize = 64;
/// Maximum bytes and frames opportunistically coalesced into one write/TLS record.
const COALESCE_BYTES: usize = 16_384;
const MAX_COALESCE_FRAMES: usize = 32;
/// Payloads above this threshold bypass coalescing and stream zero-copy via `writev` / TLS slices.
const SEGMENTED_PAYLOAD_THRESHOLD: usize = 1_024;

#[derive(Debug)]
pub enum Incoming {
    Authenticated {
        elapsed: Duration,
    },
    Packet {
        first: u8,
        body: FrameBytes,
        /// Frozen when the complete frame leaves the socket reader, before
        /// bounded channel or pending-CONNECT residence can age it.
        observed: FrameObservation,
    },
    /// The peer sent a fixed header the broker refuses. The transport reads
    /// no further packet and drops what follows, but still writes: the
    /// runtime may name the cause before it closes the connection.
    Refused {
        code: &'static str,
    },
    Closed {
        code: &'static str,
    },
}

pub type FrameObservation = Result<MessageTime, TimeError>;

/// Inline byte buffer for small MQTT control frames, incoming bodies, and
/// PUBLISH headers (PUBACK = 4 B, PUBREC/PUBREL/PUBCOMP = 4 B, PINGRESP = 2 B,
/// CONNACK = 24 B, short PUBLISH headers <= 30 B). Choosing 30 keeps
/// `size_of::<FrameBytes>() == 32` (matching `Vec<u8>` + tag) while avoiding
/// heap allocations on control and short PUBLISH frames.
pub const INLINE_FRAME_CAP: usize = 30;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrameBytes {
    Inline {
        buf: [u8; INLINE_FRAME_CAP],
        len: u8,
    },
    Heap(Vec<u8>),
}

impl FrameBytes {
    #[inline]
    pub fn try_zeroed(len: usize) -> Result<Self, std::collections::TryReserveError> {
        if len <= INLINE_FRAME_CAP {
            Ok(Self::Inline {
                buf: [0u8; INLINE_FRAME_CAP],
                len: len as u8,
            })
        } else {
            let mut vec = Vec::new();
            vec.try_reserve_exact(len)?;
            vec.resize(len, 0);
            Ok(Self::Heap(vec))
        }
    }

    #[inline]
    #[must_use]
    pub const fn empty() -> Self {
        Self::Inline {
            buf: [0u8; INLINE_FRAME_CAP],
            len: 0,
        }
    }

    #[inline]
    #[must_use]
    pub fn from_slice(bytes: &[u8]) -> Self {
        if bytes.len() <= INLINE_FRAME_CAP {
            let mut buf = [0u8; INLINE_FRAME_CAP];
            buf[..bytes.len()].copy_from_slice(bytes);
            Self::Inline {
                buf,
                len: bytes.len() as u8,
            }
        } else {
            Self::Heap(bytes.to_vec())
        }
    }

    #[inline]
    pub fn try_with_capacity(cap: usize) -> Result<Self, std::collections::TryReserveError> {
        if cap <= INLINE_FRAME_CAP {
            Ok(Self::empty())
        } else {
            let mut vec = Vec::new();
            vec.try_reserve_exact(cap)?;
            Ok(Self::Heap(vec))
        }
    }

    #[inline]
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Inline { buf, len } => &buf[..usize::from(*len)],
            Self::Heap(vec) => vec.as_slice(),
        }
    }

    #[inline]
    #[must_use]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        match self {
            Self::Inline { buf, len } => &mut buf[..usize::from(*len)],
            Self::Heap(vec) => vec.as_mut_slice(),
        }
    }

    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::Inline { len, .. } => usize::from(*len),
            Self::Heap(vec) => vec.len(),
        }
    }

    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn push(&mut self, byte: u8) {
        match self {
            Self::Inline { buf, len } if usize::from(*len) < INLINE_FRAME_CAP => {
                buf[usize::from(*len)] = byte;
                *len += 1;
            }
            Self::Inline { buf, len } => {
                let mut vec = Vec::with_capacity(usize::from(*len) + 16);
                vec.extend_from_slice(&buf[..usize::from(*len)]);
                vec.push(byte);
                *self = Self::Heap(vec);
            }
            Self::Heap(vec) => vec.push(byte),
        }
    }

    #[inline]
    pub fn extend_from_slice(&mut self, slice: &[u8]) {
        if slice.is_empty() {
            return;
        }
        match self {
            Self::Inline { buf, len } => {
                let cur = usize::from(*len);
                let new_len = cur + slice.len();
                if new_len <= INLINE_FRAME_CAP {
                    buf[cur..new_len].copy_from_slice(slice);
                    *len = new_len as u8;
                } else {
                    let mut vec = Vec::with_capacity(new_len.max(64));
                    vec.extend_from_slice(&buf[..cur]);
                    vec.extend_from_slice(slice);
                    *self = Self::Heap(vec);
                }
            }
            Self::Heap(vec) => vec.extend_from_slice(slice),
        }
    }
}

impl Default for FrameBytes {
    #[inline]
    fn default() -> Self {
        Self::empty()
    }
}

impl From<Vec<u8>> for FrameBytes {
    #[inline]
    fn from(vec: Vec<u8>) -> Self {
        Self::Heap(vec)
    }
}

impl PartialEq<[u8]> for FrameBytes {
    #[inline]
    fn eq(&self, other: &[u8]) -> bool {
        self.as_slice() == other
    }
}

impl<const N: usize> PartialEq<[u8; N]> for FrameBytes {
    #[inline]
    fn eq(&self, other: &[u8; N]) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl std::ops::Deref for FrameBytes {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl std::ops::DerefMut for FrameBytes {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}

/// Which completion signals the socket task emits after writing a frame.
/// Failure receipts (`Written::Failed`) are always emitted on transport error.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WriteReport {
    /// Emit neither `WireProgress` nor `Written::Complete` on success.
    None,
    /// Emit `Written::Complete` on success, but no separate `WireProgress`.
    Complete,
    /// Emit both `WireProgress` (when correlated) and `Written::Complete`.
    #[default]
    CompleteAndWireProgress,
}

#[derive(Debug)]
pub struct Write {
    pub correlation: Option<(EffectId, ConnectionKey)>,
    pub bytes: FrameBytes,
    pub payload: Option<SharedBytes>,
    pub expiry: Option<Box<WriteExpiry>>,
    pub report: WriteReport,
}

impl Write {
    #[inline]
    #[must_use]
    pub fn total_len(&self) -> usize {
        self.bytes.len() + self.payload.as_ref().map_or(0, SharedBytes::len)
    }

    #[inline]
    fn can_coalesce(&self, limit: usize) -> bool {
        self.expiry.is_none()
            && self.total_len() <= limit
            && self
                .payload
                .as_ref()
                .is_none_or(|p| p.len() <= SEGMENTED_PAYLOAD_THRESHOLD)
    }

    #[inline]
    fn append_to_vec(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.bytes);
        if let Some(payload) = &self.payload {
            out.extend_from_slice(payload.as_slice());
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct WriteExpiry {
    pub expiry: MessageExpiry,
    /// MQTT 5 has four bytes to update. Legacy writes still check the deadline.
    pub value_offset: Option<usize>,
    /// An existing exchange may finish after its original message lifetime.
    pub started: bool,
}

#[derive(Debug)]
pub enum Written {
    Started {
        correlation: Option<(EffectId, ConnectionKey)>,
    },
    NotStarted {
        correlation: Option<(EffectId, ConnectionKey)>,
        reason: MessageExpiryBlock,
    },
    Complete {
        correlation: Option<(EffectId, ConnectionKey)>,
        bytes: usize,
    },
    Failed {
        correlation: Option<(EffectId, ConnectionKey)>,
        code: &'static str,
    },
}

#[derive(Clone, Copy, Debug)]
pub struct WireProgress {
    pub effect: EffectId,
    pub connection: ConnectionKey,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DrainStatus {
    Empty,
    More,
    Disconnected,
}

/// Reusable per-reactor-thread buffers shared across all registered connections
/// so idle connections hold zero heap memory for reads, coalescing, or batches.
pub(crate) struct ReactorScratch {
    pub(crate) read_buf: Box<[u8]>,
    pub(crate) incoming_batch: Vec<Incoming>,
    pub(crate) write_batch: Vec<Write>,
    pub(crate) coalesce_bytes: Vec<u8>,
    pub(crate) reports_batch: Vec<Report>,
}

impl ReactorScratch {
    pub(crate) fn new() -> Self {
        Self {
            read_buf: vec![0u8; READ_BUFFER].into_boxed_slice(),
            incoming_batch: Vec::with_capacity(INPUT_SLOTS),
            write_batch: Vec::with_capacity(MAX_COALESCE_FRAMES),
            coalesce_bytes: Vec::with_capacity(COALESCE_BYTES),
            reports_batch: Vec::with_capacity(BURST),
        }
    }
}

const ALIVE_IN_RX: u8 = 1 << 0;
const ALIVE_IN_TX: u8 = 1 << 1;
const ALIVE_WRITTEN_RX: u8 = 1 << 2;
const ALIVE_WRITTEN_TX: u8 = 1 << 3;
const ALIVE_PROG_RX: u8 = 1 << 4;
const ALIVE_PROG_TX: u8 = 1 << 5;
const ALIVE_ALL: u8 =
    ALIVE_IN_RX | ALIVE_IN_TX | ALIVE_WRITTEN_RX | ALIVE_WRITTEN_TX | ALIVE_PROG_RX | ALIVE_PROG_TX;

struct ToCoreState {
    incoming: VecDeque<Incoming>,
    written: VecDeque<Written>,
    #[allow(
        clippy::box_collection,
        reason = "keeps ToCoreState 24 B smaller for idle connections that never emit wire progress"
    )]
    wire_progress: Option<Box<VecDeque<WireProgress>>>,
    terminal: Option<Terminal>,
    alive: u8,
    /// Saturating count of drained incoming packets; while `< 3` (CONNECT
    /// handshake / idle phase) an empty queue frees its backing buffer
    /// immediately so idle connections retain zero queue heap.
    incoming_seen: u8,
    /// Saturating count of drained write receipts; while `< 3` an empty
    /// written queue frees its backing buffer immediately.
    written_seen: u8,
}

struct OutgoingState {
    queue: VecDeque<Write>,
    tx_alive: bool,
    rx_alive: bool,
    /// Saturating count of popped outgoing frames; while `< 3` (CONNACK
    /// handshake / idle phase) an empty queue frees its backing buffer.
    sent_seen: u8,
}

/// Direct I/O event delivered from an I/O reactor task straight to a resident
/// `DeliveryShard` worker, bypassing the Coordinator thread completely.
#[derive(Debug)]
pub enum ShardIoEvent {
    AckPacket {
        connection: ConnectionKey,
        first: u8,
        body: FrameBytes,
    },
    Written {
        generation: ConnectionGeneration,
        written: Written,
    },
}

/// Shared bounded inbox fed directly by I/O reactor threads for one
/// `DeliveryShard` worker.
pub struct ShardInbox {
    pending: AtomicBool,
    queue: Mutex<VecDeque<ShardIoEvent>>,
    wake: Wake,
}

impl ShardInbox {
    pub fn new(wake: Wake) -> Arc<Self> {
        Arc::new(Self {
            pending: AtomicBool::new(false),
            queue: Mutex::new(VecDeque::new()),
            wake,
        })
    }

    #[inline]
    pub fn has_pending(&self) -> bool {
        self.pending.load(Ordering::Acquire)
    }

    pub fn push_batch(&self, items: &mut Vec<ShardIoEvent>) {
        if items.is_empty() {
            return;
        }
        if let Ok(mut queue) = self.queue.lock() {
            queue.extend(items.drain(..));
            self.pending.store(true, Ordering::Release);
            drop(queue);
            self.wake.wake();
        } else {
            items.clear();
        }
    }

    pub fn drain_into(&self, out: &mut VecDeque<ShardIoEvent>, limit: usize) -> usize {
        if !self.pending.load(Ordering::Acquire) {
            return 0;
        }
        let Ok(mut queue) = self.queue.lock() else {
            return 0;
        };
        let mut taken = 0usize;
        while taken < limit
            && let Some(ev) = queue.pop_front()
        {
            out.push_back(ev);
            taken += 1;
        }
        self.pending.store(!queue.is_empty(), Ordering::Release);
        taken
    }
}

/// Per-connection direct routing descriptor installed once after CONNACK is
/// queued, steering subscriber ACKs (`PUBACK`/`PUBREC`/`PUBCOMP`) and
/// shard-owned `Written` receipts straight to the owning `DeliveryShard`.
pub struct ShardIoRoute {
    pub owner_id: LogicalOwnerId,
    pub connection: ConnectionKey,
    pub inbox: Arc<ShardInbox>,
}

/// Cloneable write-only handle allowing a resident `DeliveryShard` thread to
/// enqueue framed MQTT packets directly onto a subscriber's socket queue.
#[derive(Clone)]
pub struct OutgoingHandle {
    core: Arc<ConnCore>,
}

impl OutgoingHandle {
    #[inline]
    pub fn try_send_outgoing(&self, frame: Write) -> Result<(), TrySendError<Write>> {
        self.core.try_send_outgoing(frame)
    }

    #[inline]
    pub fn is_cancelled(&self) -> bool {
        self.core.is_cancelled()
    }
}

/// Single shared allocation holding all cross-thread connection state: socket,
/// reactor link, control flags, optional TLS principal, and both directional
/// bounded queues (replacing 7 separate `Arc` allocations per connection).
pub(super) struct ConnCore {
    pub(super) stream: TcpStream,
    send_buffer: u32,
    pub(super) link: Arc<Link>,
    wake: Wake,
    ready: Option<(Arc<ReadyQueue>, u64)>,
    pub(super) heard: AtomicU64,
    pub(super) cancelled: AtomicBool,
    activity: AtomicBool,
    in_pending: AtomicBool,
    written_pending: AtomicBool,
    progress_pending: AtomicBool,
    shard_route_active: AtomicBool,
    shard_route: OnceLock<ShardIoRoute>,
    pub(super) principal: Option<Box<OnceLock<Principal>>>,
    to_core: Mutex<ToCoreState>,
    outgoing: Mutex<OutgoingState>,
    #[cfg(test)]
    changed: Condvar,
    #[cfg(test)]
    pause_progress: AtomicBool,
    #[cfg(test)]
    progress_waiting: AtomicBool,
}

impl ConnCore {
    fn new(
        stream: TcpStream,
        send_buffer: u32,
        link: Arc<Link>,
        wake: Wake,
        ready: Option<(Arc<ReadyQueue>, u64)>,
        is_tls: bool,
    ) -> Arc<Self> {
        let initial_activity = ready.is_none();
        Arc::new(Self {
            stream,
            send_buffer,
            link,
            wake,
            ready,
            heard: AtomicU64::new(0),
            cancelled: AtomicBool::new(false),
            activity: AtomicBool::new(initial_activity),
            in_pending: AtomicBool::new(false),
            written_pending: AtomicBool::new(false),
            progress_pending: AtomicBool::new(false),
            shard_route_active: AtomicBool::new(false),
            shard_route: OnceLock::new(),
            principal: is_tls.then(|| Box::new(OnceLock::new())),
            to_core: Mutex::new(ToCoreState {
                incoming: VecDeque::new(),
                written: VecDeque::new(),
                wire_progress: None,
                terminal: None,
                alive: ALIVE_ALL,
                incoming_seen: 0,
                written_seen: 0,
            }),
            outgoing: Mutex::new(OutgoingState {
                queue: VecDeque::new(),
                tx_alive: true,
                rx_alive: true,
                sent_seen: 0,
            }),
            #[cfg(test)]
            changed: Condvar::new(),
            #[cfg(test)]
            pause_progress: AtomicBool::new(false),
            #[cfg(test)]
            progress_waiting: AtomicBool::new(false),
        })
    }

    #[inline]
    pub(super) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    #[inline]
    pub(super) fn mark_active(&self) {
        if !self.activity.swap(true, Ordering::AcqRel)
            && let Some((ready, id)) = &self.ready
        {
            ready.push(*id);
        }
        self.wake.wake();
    }

    fn record(&self, code: &'static str) {
        if let Ok(mut state) = self.to_core.lock() {
            state.terminal.get_or_insert_with(|| Terminal {
                code,
                observed_ns: crate::events::monotonic_ns(),
            });
        }
        self.mark_active();
    }

    /// The socket is shut before the reason is recorded: once a reader of
    /// the reason sees it, no byte can leave any more.
    pub(super) fn stop(&self, code: &'static str) {
        self.cancelled.store(true, Ordering::Release);
        let _ = self.stream.shutdown(Shutdown::Both);
        self.record(code);
    }

    #[inline]
    pub(super) fn has_pending_incoming(&self) -> bool {
        self.in_pending.load(Ordering::Acquire)
    }

    #[inline]
    pub(super) fn has_pending_written(&self) -> bool {
        self.written_pending.load(Ordering::Acquire)
    }

    #[inline]
    pub(super) fn has_pending_wire_progress(&self) -> bool {
        self.progress_pending.load(Ordering::Acquire)
    }

    #[inline]
    pub(super) fn incoming_free_slots(&self) -> usize {
        if !self.in_pending.load(Ordering::Acquire) {
            return INPUT_SLOTS;
        }
        self.to_core
            .lock()
            .map_or(0, |s| INPUT_SLOTS.saturating_sub(s.incoming.len()))
    }

    pub(super) fn try_send_outgoing(&self, frame: Write) -> Result<(), TrySendError<Write>> {
        let mut state = match self.outgoing.lock() {
            Ok(guard) => guard,
            Err(_) => return Err(TrySendError::Disconnected(frame)),
        };
        if !state.rx_alive {
            return Err(TrySendError::Disconnected(frame));
        }
        if state.queue.len() >= OUTPUT_SLOTS {
            return Err(TrySendError::Full(frame));
        }
        state.queue.push_back(frame);
        drop(state);
        #[cfg(test)]
        self.changed.notify_one();
        self.link.notify();
        Ok(())
    }

    #[cfg(test)]
    fn send_outgoing(&self, frame: Write) -> Result<(), SendError<Write>> {
        let mut state = match self.outgoing.lock() {
            Ok(guard) => guard,
            Err(_) => return Err(SendError(frame)),
        };
        while state.rx_alive && state.queue.len() >= OUTPUT_SLOTS {
            state = match self.changed.wait(state) {
                Ok(guard) => guard,
                Err(_) => return Err(SendError(frame)),
            };
        }
        if !state.rx_alive {
            return Err(SendError(frame));
        }
        state.queue.push_back(frame);
        drop(state);
        self.changed.notify_one();
        self.link.notify();
        Ok(())
    }

    /// Pops one frame or an opportunistically coalesced batch of up to
    /// `max_frames` non-expiring frames under a single mutex acquisition.
    /// Non-coalescable frames stay in `queue` without needing a peek slot.
    pub(super) fn pop_outgoing_batch(
        &self,
        out: &mut Vec<Write>,
        allow_coalesce: bool,
        max_bytes: usize,
        max_frames: usize,
    ) -> Result<(), ()> {
        out.clear();
        let mut state = match self.outgoing.lock() {
            Ok(guard) => guard,
            Err(_) => return Err(()),
        };
        let Some(first) = state.queue.pop_front() else {
            if !state.tx_alive {
                return Err(());
            }
            return Ok(());
        };
        let coalesce = allow_coalesce && first.can_coalesce(max_bytes);
        let mut total = first.total_len();
        out.push(first);
        if coalesce {
            while out.len() < max_frames {
                let Some(front) = state.queue.front() else {
                    break;
                };
                let next_len = front.total_len();
                if front.can_coalesce(max_bytes) && total + next_len <= max_bytes {
                    let next = state.queue.pop_front().expect("checked front");
                    total += next_len;
                    out.push(next);
                } else {
                    break;
                }
            }
        }
        state.sent_seen = state.sent_seen.saturating_add(out.len().min(8) as u8);
        if state.queue.is_empty() {
            if state.sent_seen < 3 {
                state.queue = VecDeque::new();
            } else {
                state.queue.clear();
            }
        }
        drop(state);
        #[cfg(test)]
        self.changed.notify_one();
        Ok(())
    }

    pub(super) fn push_incoming_one(
        &self,
        value: Incoming,
    ) -> Result<Option<Incoming>, &'static str> {
        for attempt in 0..2 {
            let mut state = self.to_core.lock().map_err(|_| "input_consumer_closed")?;
            if state.alive & ALIVE_IN_RX == 0 {
                return Err("input_consumer_closed");
            }
            if state.incoming.len() < INPUT_SLOTS {
                state.incoming.push_back(value);
                self.in_pending.store(true, Ordering::Release);
                drop(state);
                #[cfg(test)]
                self.changed.notify_one();
                self.mark_active();
                return Ok(None);
            }
            drop(state);
            if attempt == 0 {
                self.link.owe();
            }
        }
        Ok(Some(value))
    }

    /// Pushes a batch of newly framed packets (plus any previously held
    /// packets in strict order) under a single mutex acquisition and wake.
    /// When a `ShardIoRoute` is installed, subscriber ACKs
    /// (`PUBACK`/`PUBREC`/`PUBCOMP`) bypass the Coordinator queue and route
    /// directly to the owning `DeliveryShard` inbox.
    pub(super) fn push_incoming_batch(
        &self,
        batch: &mut Vec<Incoming>,
        stalled: &mut Option<Box<StalledState>>,
    ) -> Result<bool, &'static str> {
        if self.shard_route_active.load(Ordering::Acquire)
            && let Some(route) = self.shard_route.get()
            && batch.iter().any(|item| {
                matches!(item, Incoming::Packet { first, .. } if matches!(first >> 4, 4 | 5 | 7))
            })
        {
            let mut shard_acks = Vec::new();
            for item in std::mem::take(batch) {
                match item {
                    Incoming::Packet { first, body, .. } if matches!(first >> 4, 4 | 5 | 7) => {
                        shard_acks.push(ShardIoEvent::AckPacket {
                            connection: route.connection,
                            first,
                            body,
                        });
                    }
                    other => batch.push(other),
                }
            }
            if !shard_acks.is_empty() {
                route.inbox.push_batch(&mut shard_acks);
            }
        }
        if let Some(st) = stalled.as_mut()
            && !st.held.is_empty()
        {
            st.held.extend(batch.drain(..));
            let ok = self.flush_held_incoming(st)?;
            if st.is_empty() {
                *stalled = None;
            }
            return Ok(ok);
        }
        if batch.is_empty() {
            return Ok(true);
        }
        let mut cursor = 0usize;
        for attempt in 0..2 {
            let mut state = self.to_core.lock().map_err(|_| "input_consumer_closed")?;
            if state.alive & ALIVE_IN_RX == 0 {
                return Err("input_consumer_closed");
            }
            let room = INPUT_SLOTS.saturating_sub(state.incoming.len());
            let take = room.min(batch.len() - cursor);
            if take > 0 {
                for item in batch.drain(cursor..cursor + take) {
                    state.incoming.push_back(item);
                }
                self.in_pending.store(true, Ordering::Release);
                drop(state);
                #[cfg(test)]
                self.changed.notify_one();
                self.mark_active();
                if batch.is_empty() {
                    return Ok(true);
                }
            } else {
                drop(state);
            }
            if attempt == 0 {
                self.link.owe();
            }
            cursor = 0;
        }
        if !batch.is_empty() {
            stalled
                .get_or_insert_with(Default::default)
                .held
                .extend(batch.drain(..));
            return Ok(false);
        }
        Ok(true)
    }

    pub(super) fn flush_held_incoming(&self, st: &mut StalledState) -> Result<bool, &'static str> {
        if st.held.is_empty() {
            return Ok(true);
        }
        for attempt in 0..2 {
            let mut state = self.to_core.lock().map_err(|_| "input_consumer_closed")?;
            if state.alive & ALIVE_IN_RX == 0 {
                return Err("input_consumer_closed");
            }
            let mut pushed = false;
            while state.incoming.len() < INPUT_SLOTS
                && let Some(item) = st.held.pop_front()
            {
                state.incoming.push_back(item);
                pushed = true;
            }
            if pushed {
                self.in_pending.store(true, Ordering::Release);
            }
            drop(state);
            if pushed {
                #[cfg(test)]
                self.changed.notify_one();
                self.mark_active();
            }
            if st.held.is_empty() {
                return Ok(true);
            }
            if attempt == 0 {
                self.link.owe();
            }
        }
        Ok(false)
    }

    /// Publishes pending write receipts and wire-progress notifications in
    /// strict order under a single mutex acquisition and wake.
    pub(super) fn flush_reports(
        &self,
        fresh: &mut Vec<Report>,
        stalled: &mut Option<Box<StalledState>>,
    ) -> Result<bool, &'static str> {
        let mut shard_moved = false;
        if self.shard_route_active.load(Ordering::Acquire)
            && let Some(route) = self.shard_route.get()
            && fresh.iter().any(
                |r| matches!(r, Report::Written(w) if written_owner(w) == Some(route.owner_id)),
            )
        {
            let mut shard_written = Vec::new();
            for report in std::mem::take(fresh) {
                match report {
                    Report::Written(written) if written_owner(&written) == Some(route.owner_id) => {
                        shard_written.push(ShardIoEvent::Written {
                            generation: route.connection.generation,
                            written,
                        });
                    }
                    other => fresh.push(other),
                }
            }
            if !shard_written.is_empty() {
                route.inbox.push_batch(&mut shard_written);
                shard_moved = true;
            }
        }
        let has_stalled = stalled.as_ref().is_some_and(|s| !s.reports.is_empty());
        if !has_stalled && fresh.is_empty() {
            return Ok(shard_moved);
        }
        if has_stalled && !fresh.is_empty() {
            stalled
                .as_mut()
                .expect("checked stalled")
                .reports
                .extend(fresh.drain(..));
        }
        let mut moved = false;
        let mut full_code: Option<&'static str> = None;
        for attempt in 0..2 {
            let mut state = match self.to_core.lock() {
                Ok(guard) => guard,
                Err(_) => return Err("write_result_queue_full"),
            };
            let mut pushed_written = false;
            let mut pushed_progress = false;
            full_code = None;
            if let Some(st) = stalled.as_mut() {
                while let Some(report) = st.reports.pop_front() {
                    match report {
                        Report::Written(w) => {
                            if state.alive & ALIVE_WRITTEN_RX == 0 {
                                return Err("write_result_queue_full");
                            }
                            if state.written.len() >= WRITTEN_SLOTS {
                                st.reports.push_front(Report::Written(w));
                                full_code = Some("write_result_queue_full");
                                break;
                            }
                            state.written.push_back(w);
                            pushed_written = true;
                        }
                        Report::Progress(p) => {
                            if state.alive & ALIVE_PROG_RX == 0 {
                                return Err("wire_progress_unavailable");
                            }
                            let len = state.wire_progress.as_deref().map_or(0, VecDeque::len);
                            if len >= WIRE_PROGRESS_SLOTS {
                                st.reports.push_front(Report::Progress(p));
                                full_code = Some("wire_progress_unavailable");
                                break;
                            }
                            state
                                .wire_progress
                                .get_or_insert_with(Default::default)
                                .push_back(p);
                            pushed_progress = true;
                        }
                    }
                }
            } else {
                let mut idx = 0usize;
                while idx < fresh.len() {
                    match &fresh[idx] {
                        Report::Written(_) => {
                            if state.alive & ALIVE_WRITTEN_RX == 0 {
                                return Err("write_result_queue_full");
                            }
                            if state.written.len() >= WRITTEN_SLOTS {
                                full_code = Some("write_result_queue_full");
                                break;
                            }
                        }
                        Report::Progress(_) => {
                            if state.alive & ALIVE_PROG_RX == 0 {
                                return Err("wire_progress_unavailable");
                            }
                            let len = state.wire_progress.as_deref().map_or(0, VecDeque::len);
                            if len >= WIRE_PROGRESS_SLOTS {
                                full_code = Some("wire_progress_unavailable");
                                break;
                            }
                        }
                    }
                    idx += 1;
                }
                for report in fresh.drain(..idx) {
                    match report {
                        Report::Written(w) => {
                            state.written.push_back(w);
                            pushed_written = true;
                        }
                        Report::Progress(p) => {
                            state
                                .wire_progress
                                .get_or_insert_with(Default::default)
                                .push_back(p);
                            pushed_progress = true;
                        }
                    }
                }
            }
            if pushed_written {
                self.written_pending.store(true, Ordering::Release);
            }
            if pushed_progress {
                self.progress_pending.store(true, Ordering::Release);
            }
            drop(state);
            if pushed_written || pushed_progress {
                moved = true;
                if let Some(st) = stalled.as_mut() {
                    st.stalled_since = None;
                }
                #[cfg(test)]
                self.changed.notify_one();
                self.mark_active();
            }
            if full_code.is_none() {
                if stalled.as_ref().is_some_and(|s| s.is_empty()) {
                    *stalled = None;
                }
                return Ok(moved || shard_moved);
            }
            if attempt == 0 {
                self.link.owe();
            }
        }
        if !fresh.is_empty() {
            stalled
                .get_or_insert_with(Default::default)
                .reports
                .extend(fresh.drain(..));
        }
        if let Some(code) = full_code {
            let st = stalled.get_or_insert_with(Default::default);
            if st.stalled_since.get_or_insert_with(Instant::now).elapsed() >= RESULT_WAIT {
                return Err(code);
            }
        }
        Ok(moved || shard_moved)
    }

    pub(super) fn drain_incoming(&self, out: &mut VecDeque<Incoming>, limit: usize) -> DrainStatus {
        if !self.in_pending.load(Ordering::Acquire) {
            return DrainStatus::Empty;
        }
        let mut state = match self.to_core.lock() {
            Ok(guard) => guard,
            Err(_) => return DrainStatus::Disconnected,
        };
        let mut taken = 0usize;
        while taken < limit
            && let Some(item) = state.incoming.pop_front()
        {
            out.push_back(item);
            taken += 1;
        }
        state.incoming_seen = state.incoming_seen.saturating_add(taken.min(8) as u8);
        if state.incoming.is_empty() {
            if state.incoming_seen < 3 {
                state.incoming = VecDeque::new();
            } else {
                state.incoming.clear();
            }
        }
        let tx_alive = state.alive & ALIVE_IN_TX != 0;
        let more = !state.incoming.is_empty();
        self.in_pending.store(more || !tx_alive, Ordering::Release);
        drop(state);
        if taken > 0 {
            #[cfg(test)]
            self.changed.notify_one();
            self.link.consumed();
            if more {
                DrainStatus::More
            } else {
                DrainStatus::Empty
            }
        } else if !tx_alive {
            DrainStatus::Disconnected
        } else {
            DrainStatus::Empty
        }
    }

    pub(super) fn push_front_incoming(&self, remaining: &mut VecDeque<Incoming>) {
        if remaining.is_empty() {
            return;
        }
        if let Ok(mut state) = self.to_core.lock() {
            while let Some(item) = remaining.pop_back() {
                state.incoming.push_front(item);
            }
            self.in_pending.store(true, Ordering::Release);
        }
    }

    pub(super) fn drain_written(&self, out: &mut Vec<Written>, limit: usize) -> DrainStatus {
        if !self.written_pending.load(Ordering::Acquire) {
            return DrainStatus::Empty;
        }
        let mut state = match self.to_core.lock() {
            Ok(guard) => guard,
            Err(_) => return DrainStatus::Disconnected,
        };
        let mut taken = 0usize;
        while taken < limit
            && let Some(item) = state.written.pop_front()
        {
            out.push(item);
            taken += 1;
        }
        state.written_seen = state.written_seen.saturating_add(taken.min(8) as u8);
        if state.written.is_empty() {
            if state.written_seen < 3 {
                state.written = VecDeque::new();
            } else {
                state.written.clear();
            }
        }
        let tx_alive = state.alive & ALIVE_WRITTEN_TX != 0;
        let more = !state.written.is_empty();
        self.written_pending
            .store(more || !tx_alive, Ordering::Release);
        drop(state);
        if taken > 0 {
            #[cfg(test)]
            self.changed.notify_one();
            self.link.consumed();
            if more {
                DrainStatus::More
            } else {
                DrainStatus::Empty
            }
        } else if !tx_alive {
            DrainStatus::Disconnected
        } else {
            DrainStatus::Empty
        }
    }

    pub(super) fn drain_wire_progress(&self, out: &mut Vec<WireProgress>, limit: usize) {
        if !self.progress_pending.load(Ordering::Acquire) {
            return;
        }
        let mut state = match self.to_core.lock() {
            Ok(guard) => guard,
            Err(_) => return,
        };
        let mut taken = 0usize;
        let mut more = false;
        if let Some(queue) = state.wire_progress.as_mut() {
            while taken < limit
                && let Some(item) = queue.pop_front()
            {
                out.push(item);
                taken += 1;
            }
            more = !queue.is_empty();
            if !more {
                state.wire_progress = None;
            }
        }
        let tx_alive = state.alive & ALIVE_PROG_TX != 0;
        self.progress_pending
            .store(more || !tx_alive, Ordering::Release);
        drop(state);
        if taken > 0 {
            #[cfg(test)]
            self.changed.notify_one();
            self.link.consumed();
        }
    }

    /// Reclaims empty queue backing arrays on idle connections without
    /// allocating or contending with active traffic.
    pub(super) fn shrink_idle_queues(&self) {
        if !self.in_pending.load(Ordering::Acquire)
            && !self.written_pending.load(Ordering::Acquire)
            && !self.progress_pending.load(Ordering::Acquire)
            && let Ok(mut state) = self.to_core.try_lock()
        {
            if state.incoming.is_empty() && state.incoming.capacity() > 0 {
                state.incoming = VecDeque::new();
            }
            if state.written.is_empty() && state.written.capacity() > 0 {
                state.written = VecDeque::new();
            }
            if state
                .wire_progress
                .as_deref()
                .is_some_and(VecDeque::is_empty)
            {
                state.wire_progress = None;
            }
        }
        if let Ok(mut out) = self.outgoing.try_lock()
            && out.queue.is_empty()
            && out.queue.capacity() > 0
        {
            out.queue = VecDeque::new();
        }
    }

    #[cfg(test)]
    fn try_recv_incoming_one(&self) -> Result<Incoming, TryRecvError> {
        let mut state = match self.to_core.lock() {
            Ok(guard) => guard,
            Err(_) => return Err(TryRecvError::Disconnected),
        };
        if let Some(item) = state.incoming.pop_front() {
            let tx_alive = state.alive & ALIVE_IN_TX != 0;
            self.in_pending
                .store(!state.incoming.is_empty() || !tx_alive, Ordering::Release);
            drop(state);
            self.changed.notify_one();
            self.link.consumed();
            return Ok(item);
        }
        if state.alive & ALIVE_IN_TX == 0 {
            self.in_pending.store(true, Ordering::Release);
            Err(TryRecvError::Disconnected)
        } else {
            self.in_pending.store(false, Ordering::Release);
            Err(TryRecvError::Empty)
        }
    }

    #[cfg(test)]
    fn try_recv_written_one(&self) -> Result<Written, TryRecvError> {
        let mut state = match self.to_core.lock() {
            Ok(guard) => guard,
            Err(_) => return Err(TryRecvError::Disconnected),
        };
        if let Some(item) = state.written.pop_front() {
            let tx_alive = state.alive & ALIVE_WRITTEN_TX != 0;
            self.written_pending
                .store(!state.written.is_empty() || !tx_alive, Ordering::Release);
            drop(state);
            self.changed.notify_one();
            self.link.consumed();
            return Ok(item);
        }
        if state.alive & ALIVE_WRITTEN_TX == 0 {
            self.written_pending.store(true, Ordering::Release);
            Err(TryRecvError::Disconnected)
        } else {
            self.written_pending.store(false, Ordering::Release);
            Err(TryRecvError::Empty)
        }
    }

    #[cfg(test)]
    fn try_recv_progress_one(&self) -> Result<WireProgress, TryRecvError> {
        let mut state = match self.to_core.lock() {
            Ok(guard) => guard,
            Err(_) => return Err(TryRecvError::Disconnected),
        };
        if let Some(queue) = state.wire_progress.as_mut()
            && let Some(item) = queue.pop_front()
        {
            let empty = queue.is_empty();
            if empty {
                state.wire_progress = None;
            }
            let tx_alive = state.alive & ALIVE_PROG_TX != 0;
            self.progress_pending
                .store(!empty || !tx_alive, Ordering::Release);
            drop(state);
            self.changed.notify_one();
            self.link.consumed();
            return Ok(item);
        }
        if state.alive & ALIVE_PROG_TX == 0 {
            self.progress_pending.store(true, Ordering::Release);
            Err(TryRecvError::Disconnected)
        } else {
            self.progress_pending.store(false, Ordering::Release);
            Err(TryRecvError::Empty)
        }
    }

    #[cfg(test)]
    fn recv_timeout_incoming(&self, timeout: Duration) -> Result<Incoming, RecvTimeoutError> {
        let deadline = Instant::now().checked_add(timeout);
        let mut state = match self.to_core.lock() {
            Ok(guard) => guard,
            Err(_) => return Err(RecvTimeoutError::Disconnected),
        };
        loop {
            if let Some(item) = state.incoming.pop_front() {
                let tx_alive = state.alive & ALIVE_IN_TX != 0;
                self.in_pending
                    .store(!state.incoming.is_empty() || !tx_alive, Ordering::Release);
                drop(state);
                self.changed.notify_one();
                self.link.consumed();
                return Ok(item);
            }
            if state.alive & ALIVE_IN_TX == 0 {
                self.in_pending.store(true, Ordering::Release);
                return Err(RecvTimeoutError::Disconnected);
            }
            self.in_pending.store(false, Ordering::Release);
            let remaining =
                deadline.map_or(timeout, |d| d.saturating_duration_since(Instant::now()));
            if remaining.is_zero() {
                return Err(RecvTimeoutError::Timeout);
            }
            state = match self.changed.wait_timeout(state, remaining) {
                Ok((guard, _)) => guard,
                Err(_) => return Err(RecvTimeoutError::Disconnected),
            };
        }
    }

    #[cfg(test)]
    fn recv_timeout_written(&self, timeout: Duration) -> Result<Written, RecvTimeoutError> {
        let deadline = Instant::now().checked_add(timeout);
        let mut state = match self.to_core.lock() {
            Ok(guard) => guard,
            Err(_) => return Err(RecvTimeoutError::Disconnected),
        };
        loop {
            if let Some(item) = state.written.pop_front() {
                let tx_alive = state.alive & ALIVE_WRITTEN_TX != 0;
                self.written_pending
                    .store(!state.written.is_empty() || !tx_alive, Ordering::Release);
                drop(state);
                self.changed.notify_one();
                self.link.consumed();
                return Ok(item);
            }
            if state.alive & ALIVE_WRITTEN_TX == 0 {
                self.written_pending.store(true, Ordering::Release);
                return Err(RecvTimeoutError::Disconnected);
            }
            self.written_pending.store(false, Ordering::Release);
            let remaining =
                deadline.map_or(timeout, |d| d.saturating_duration_since(Instant::now()));
            if remaining.is_zero() {
                return Err(RecvTimeoutError::Timeout);
            }
            state = match self.changed.wait_timeout(state, remaining) {
                Ok((guard, _)) => guard,
                Err(_) => return Err(RecvTimeoutError::Disconnected),
            };
        }
    }

    #[cfg(test)]
    fn recv_timeout_progress(&self, timeout: Duration) -> Result<WireProgress, RecvTimeoutError> {
        let deadline = Instant::now().checked_add(timeout);
        let mut state = match self.to_core.lock() {
            Ok(guard) => guard,
            Err(_) => return Err(RecvTimeoutError::Disconnected),
        };
        loop {
            if let Some(queue) = state.wire_progress.as_mut()
                && let Some(item) = queue.pop_front()
            {
                let empty = queue.is_empty();
                if empty {
                    state.wire_progress = None;
                }
                let tx_alive = state.alive & ALIVE_PROG_TX != 0;
                self.progress_pending
                    .store(!empty || !tx_alive, Ordering::Release);
                drop(state);
                self.changed.notify_one();
                self.link.consumed();
                return Ok(item);
            }
            if state.alive & ALIVE_PROG_TX == 0 {
                self.progress_pending.store(true, Ordering::Release);
                return Err(RecvTimeoutError::Disconnected);
            }
            self.progress_pending.store(false, Ordering::Release);
            let remaining =
                deadline.map_or(timeout, |d| d.saturating_duration_since(Instant::now()));
            if remaining.is_zero() {
                return Err(RecvTimeoutError::Timeout);
            }
            state = match self.changed.wait_timeout(state, remaining) {
                Ok((guard, _)) => guard,
                Err(_) => return Err(RecvTimeoutError::Disconnected),
            };
        }
    }

    /// Offers the terminal facts once, without waiting, and closes all task-side lanes.
    pub(super) fn finish_task(
        &self,
        stalled: Option<Box<StalledState>>,
        code: &'static str,
        active: impl IntoIterator<Item = Option<(EffectId, ConnectionKey)>>,
    ) {
        let queued: Vec<Option<(EffectId, ConnectionKey)>> = match self.outgoing.lock() {
            Ok(mut out) => {
                out.rx_alive = false;
                out.queue.drain(..).map(|f| f.correlation).collect()
            }
            Err(_) => Vec::new(),
        };
        if let Ok(mut state) = self.to_core.lock() {
            if let Some(st) = stalled {
                for report in st.reports {
                    match report {
                        Report::Written(w) => {
                            if state.alive & ALIVE_WRITTEN_RX == 0
                                || state.written.len() >= WRITTEN_SLOTS
                            {
                                break;
                            }
                            state.written.push_back(w);
                        }
                        Report::Progress(p) => {
                            let len = state.wire_progress.as_deref().map_or(0, VecDeque::len);
                            if state.alive & ALIVE_PROG_RX == 0 || len >= WIRE_PROGRESS_SLOTS {
                                break;
                            }
                            state
                                .wire_progress
                                .get_or_insert_with(Default::default)
                                .push_back(p);
                        }
                    }
                }
            }
            let route = self
                .shard_route_active
                .load(Ordering::Acquire)
                .then(|| self.shard_route.get())
                .flatten();
            let mut shard_failed = Vec::new();
            for correlation in active.into_iter().chain(queued) {
                if let Some(route) = route
                    && correlation.is_some_and(|(eff, _)| eff.owner == route.owner_id)
                {
                    shard_failed.push(ShardIoEvent::Written {
                        generation: route.connection.generation,
                        written: Written::Failed { correlation, code },
                    });
                    continue;
                }
                if state.alive & ALIVE_WRITTEN_RX == 0 || state.written.len() >= WRITTEN_SLOTS {
                    break;
                }
                state
                    .written
                    .push_back(Written::Failed { correlation, code });
            }
            if let Some(route) = route
                && !shard_failed.is_empty()
            {
                route.inbox.push_batch(&mut shard_failed);
            }
            if state.alive & ALIVE_IN_RX != 0 && state.incoming.len() < INPUT_SLOTS {
                state.incoming.push_back(Incoming::Closed { code });
            }
            state.alive &= !(ALIVE_IN_TX | ALIVE_WRITTEN_TX | ALIVE_PROG_TX);
            self.in_pending.store(true, Ordering::Release);
            self.written_pending.store(true, Ordering::Release);
            self.progress_pending.store(true, Ordering::Release);
        }
        #[cfg(test)]
        self.changed.notify_all();
        self.mark_active();
    }
}

#[cfg(test)]
#[derive(Clone, Copy)]
enum LaneKind {
    Incoming,
    Written,
    WireProgress,
}

/// Test-only lane handle allowing unit tests to swap an individual lane with an
/// `mpsc` channel while non-swapped lanes run the exact production `ConnCore`.
#[cfg(test)]
pub struct Inbound<T> {
    receiver: InboundRx<T>,
}

#[cfg(test)]
enum InboundRx<T> {
    Shared(Arc<ConnCore>, LaneKind),
    Std(Receiver<T>),
}

#[cfg(test)]
impl<T> Drop for Inbound<T> {
    fn drop(&mut self) {
        if let InboundRx::Shared(core, kind) = &self.receiver {
            if let Ok(mut state) = core.to_core.lock() {
                let bit = match kind {
                    LaneKind::Incoming => ALIVE_IN_RX,
                    LaneKind::Written => ALIVE_WRITTEN_RX,
                    LaneKind::WireProgress => ALIVE_PROG_RX,
                };
                state.alive &= !bit;
            }
            core.changed.notify_all();
        }
    }
}

#[cfg(test)]
impl Inbound<Incoming> {
    #[inline]
    pub fn has_pending(&self) -> bool {
        match &self.receiver {
            InboundRx::Shared(core, _) => core.has_pending_incoming(),
            InboundRx::Std(_) => true,
        }
    }

    pub fn try_recv(&self) -> Result<Incoming, TryRecvError> {
        match &self.receiver {
            InboundRx::Shared(core, _) => core.try_recv_incoming_one(),
            InboundRx::Std(rx) => rx.try_recv(),
        }
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Result<Incoming, RecvTimeoutError> {
        match &self.receiver {
            InboundRx::Shared(core, _) => core.recv_timeout_incoming(timeout),
            InboundRx::Std(rx) => rx.recv_timeout(timeout),
        }
    }
}

#[cfg(test)]
impl Inbound<Written> {
    #[inline]
    pub fn has_pending(&self) -> bool {
        match &self.receiver {
            InboundRx::Shared(core, _) => core.has_pending_written(),
            InboundRx::Std(_) => true,
        }
    }

    pub fn try_recv(&self) -> Result<Written, TryRecvError> {
        match &self.receiver {
            InboundRx::Shared(core, _) => core.try_recv_written_one(),
            InboundRx::Std(rx) => rx.try_recv(),
        }
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Result<Written, RecvTimeoutError> {
        match &self.receiver {
            InboundRx::Shared(core, _) => core.recv_timeout_written(timeout),
            InboundRx::Std(rx) => rx.recv_timeout(timeout),
        }
    }

    pub fn try_iter(&self) -> impl Iterator<Item = Written> + '_ {
        std::iter::from_fn(|| self.try_recv().ok())
    }
}

#[cfg(test)]
impl Inbound<WireProgress> {
    #[inline]
    pub fn has_pending(&self) -> bool {
        match &self.receiver {
            InboundRx::Shared(core, _) => core.has_pending_wire_progress(),
            InboundRx::Std(_) => true,
        }
    }

    pub fn try_recv(&self) -> Result<WireProgress, TryRecvError> {
        match &self.receiver {
            InboundRx::Shared(core, _) => core.try_recv_progress_one(),
            InboundRx::Std(rx) => rx.try_recv(),
        }
    }

    #[allow(dead_code)]
    pub fn recv_timeout(&self, timeout: Duration) -> Result<WireProgress, RecvTimeoutError> {
        match &self.receiver {
            InboundRx::Shared(core, _) => core.recv_timeout_progress(timeout),
            InboundRx::Std(rx) => rx.recv_timeout(timeout),
        }
    }
}

#[cfg(test)]
impl<T> From<Receiver<T>> for Inbound<T> {
    fn from(receiver: Receiver<T>) -> Self {
        Self {
            receiver: InboundRx::Std(receiver),
        }
    }
}

#[cfg(test)]
pub struct Outbound {
    sender: OutboundTx,
}

#[cfg(test)]
enum OutboundTx {
    Shared(Arc<ConnCore>),
    Std(SyncSender<Write>),
}

#[cfg(test)]
impl Drop for Outbound {
    fn drop(&mut self) {
        if let OutboundTx::Shared(core) = &self.sender {
            if let Ok(mut out) = core.outgoing.lock() {
                out.tx_alive = false;
            }
            core.changed.notify_all();
            core.link.notify();
        }
    }
}

#[cfg(test)]
impl Outbound {
    pub fn try_send(&self, frame: Write) -> Result<(), TrySendError<Write>> {
        match &self.sender {
            OutboundTx::Shared(core) => core.try_send_outgoing(frame),
            OutboundTx::Std(tx) => tx.try_send(frame),
        }
    }

    pub fn send(&self, frame: Write) -> Result<(), SendError<Write>> {
        match &self.sender {
            OutboundTx::Shared(core) => core.send_outgoing(frame),
            OutboundTx::Std(tx) => tx.send(frame),
        }
    }
}

#[cfg(test)]
impl From<SyncSender<Write>> for Outbound {
    fn from(sender: SyncSender<Write>) -> Self {
        Self {
            sender: OutboundTx::Std(sender),
        }
    }
}

/// Bounded-cardinality active-peer notification list shared between I/O
/// reactors and the core thread. Each peer pushes its generation at most once
/// per `activity: false -> true` transition, turning core polling from
/// O(total connections) into O(active connections).
#[derive(Default)]
pub(crate) struct ReadyQueue {
    has_pending: AtomicBool,
    queue: Mutex<Vec<u64>>,
}

impl ReadyQueue {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            has_pending: AtomicBool::new(false),
            queue: Mutex::new(Vec::new()),
        })
    }

    #[inline]
    fn push(&self, id: u64) {
        if let Ok(mut queue) = self.queue.lock() {
            queue.push(id);
            self.has_pending.store(true, Ordering::Release);
        }
    }

    pub(crate) fn drain_into(&self, out: &mut Vec<u64>) {
        if !self.has_pending.load(Ordering::Acquire) {
            return;
        }
        if let Ok(mut queue) = self.queue.lock() {
            if out.is_empty() {
                std::mem::swap(&mut *queue, out);
            } else if !queue.is_empty() {
                out.append(&mut queue);
            }
            self.has_pending.store(false, Ordering::Release);
        }
    }
}

#[derive(Clone, Copy)]
struct Terminal {
    code: &'static str,
    observed_ns: Option<u64>,
}

/// The owner's handle on a connection's bounded lanes and transport lifecycle.
/// In release builds this is a single 8-byte `Arc<ConnCore>`.
pub struct Connection {
    core: Arc<ConnCore>,
    #[cfg(test)]
    pub incoming: Inbound<Incoming>,
    #[cfg(test)]
    pub outgoing: Outbound,
    #[cfg(test)]
    pub written: Inbound<Written>,
    #[cfg(test)]
    pub wire_progress: Inbound<WireProgress>,
}

/// Configures the I/O reactor thread pool size prior to first socket spawn.
pub fn configure_io_threads(threads: usize) {
    reactor::configure_threads(threads);
}

/// Defers the wake-ups of the reactor threads until the guard is dropped.
/// A caller that serves many lanes in one pass pays one wake-up per thread
/// instead of one per lane. A deferred wake-up is late, never lost.
pub(crate) fn hold_wakes() -> Option<reactor::Held<'static>> {
    Reactors::shared().ok().map(Reactors::hold)
}

pub fn spawn(stream: TcpStream) -> io::Result<Connection> {
    let clock = SystemClock::new().map_err(io::Error::other)?;
    spawn_with_clock(stream, Arc::new(clock))
}

pub fn spawn_with_clock(stream: TcpStream, clock: Arc<dyn SessionClock>) -> io::Result<Connection> {
    spawn_with_wake(stream, clock, Wake::new())
}

/// The shared core of one connection and the socket settings both transports share.
fn open(
    stream: TcpStream,
    wake: Wake,
    ready: Option<(Arc<ReadyQueue>, u64)>,
    is_tls: bool,
) -> io::Result<(Arc<ConnCore>, Connection)> {
    stream.set_nodelay(true)?;
    stream.set_nonblocking(true)?;
    let socket = socket2::SockRef::from(&stream);
    let send_buffer = u32::try_from(socket.send_buffer_size()?).unwrap_or(u32::MAX);
    let link = Reactors::shared()?.link(wake.clone());
    let core = ConnCore::new(stream, send_buffer, link, wake, ready, is_tls);
    let connection = Connection {
        #[cfg(test)]
        incoming: Inbound {
            receiver: InboundRx::Shared(core.clone(), LaneKind::Incoming),
        },
        #[cfg(test)]
        outgoing: Outbound {
            sender: OutboundTx::Shared(core.clone()),
        },
        #[cfg(test)]
        written: Inbound {
            receiver: InboundRx::Shared(core.clone(), LaneKind::Written),
        },
        #[cfg(test)]
        wire_progress: Inbound {
            receiver: InboundRx::Shared(core.clone(), LaneKind::WireProgress),
        },
        core: core.clone(),
    };
    Ok((core, connection))
}

/// `wake` is signalled whenever the task publishes towards the core.
pub fn spawn_with_wake(
    stream: TcpStream,
    clock: Arc<dyn SessionClock>,
    wake: Wake,
) -> io::Result<Connection> {
    spawn_plain(stream, clock, wake, None)
}

pub(crate) fn spawn_with_ready(
    stream: TcpStream,
    clock: Arc<dyn SessionClock>,
    wake: Wake,
    ready: (Arc<ReadyQueue>, u64),
) -> io::Result<Connection> {
    spawn_plain(stream, clock, wake, Some(ready))
}

fn spawn_plain(
    stream: TcpStream,
    clock: Arc<dyn SessionClock>,
    wake: Wake,
    ready: Option<(Arc<ReadyQueue>, u64)>,
) -> io::Result<Connection> {
    let (core, connection) = open(stream, wake, ready, false)?;
    let task = Task {
        core: core.clone(),
        clock,
        leftover: None,
        parser: Parser::default(),
        stalled: None,
        active: None,
        #[cfg(test)]
        paused: None,
    };
    Reactors::shared()?.register(&core.link, Box::new(task));
    Ok(connection)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_tls(
    stream: TcpStream,
    config: Arc<rustls::ServerConfig>,
    policy: Arc<Policy>,
    clock: Arc<dyn SessionClock>,
    wake: Wake,
    ready: Option<(Arc<ReadyQueue>, u64)>,
    #[cfg(feature = "test-hooks")] trace: (crate::writer::Hooks, crate::events::Events),
) -> io::Result<Connection> {
    let (core, connection) = open(stream, wake, ready, true)?;
    let state = rustls::ServerConnection::new(config).map_err(io::Error::other)?;
    let mut task = tls::Task {
        core: core.clone(),
        state,
        clock,
        policy,
        stalled: None,
        machine: tls::Machine::new(),
        #[cfg(feature = "test-hooks")]
        trace,
    };
    task.state
        .set_buffer_limit(Some(crate::security::TLS_BUFFER_BYTES));
    Reactors::shared()?.register(&core.link, Box::new(task));
    Ok(connection)
}

impl Connection {
    #[inline]
    pub fn is_tls(&self) -> bool {
        self.core.principal.is_some()
    }

    #[inline]
    pub fn is_authenticating(&self) -> bool {
        self.core
            .principal
            .as_ref()
            .is_some_and(|slot| slot.get().is_none())
    }

    #[inline]
    pub fn principal(&self) -> Option<Principal> {
        self.core.principal.as_deref()?.get().copied()
    }

    /// Consumes and reports whether the transport task published any new
    /// incoming packet, write receipt, wire progress, or terminal state since
    /// the last check.
    #[inline]
    pub fn take_activity(&self) -> bool {
        let active = self.core.activity.swap(false, Ordering::AcqRel);
        active || self.has_pending()
    }

    #[cfg(test)]
    pub(crate) fn uses_std_channel(&self) -> bool {
        matches!(self.incoming.receiver, InboundRx::Std(_))
            || matches!(self.written.receiver, InboundRx::Std(_))
            || matches!(self.wire_progress.receiver, InboundRx::Std(_))
    }

    #[inline]
    pub fn has_pending_incoming(&self) -> bool {
        #[cfg(test)]
        {
            self.incoming.has_pending()
        }
        #[cfg(not(test))]
        {
            self.core.has_pending_incoming()
        }
    }

    #[inline]
    pub fn has_pending_written(&self) -> bool {
        #[cfg(test)]
        {
            self.written.has_pending()
        }
        #[cfg(not(test))]
        {
            self.core.has_pending_written()
        }
    }

    #[inline]
    pub fn has_pending_wire_progress(&self) -> bool {
        #[cfg(test)]
        {
            self.wire_progress.has_pending()
        }
        #[cfg(not(test))]
        {
            self.core.has_pending_wire_progress()
        }
    }

    #[inline]
    pub fn has_pending(&self) -> bool {
        self.has_pending_incoming()
            || self.has_pending_written()
            || self.has_pending_wire_progress()
    }

    #[inline]
    pub fn try_send_outgoing(&self, frame: Write) -> Result<(), TrySendError<Write>> {
        #[cfg(test)]
        {
            self.outgoing.try_send(frame)
        }
        #[cfg(not(test))]
        {
            self.core.try_send_outgoing(frame)
        }
    }

    #[inline]
    pub fn drain_incoming(&self, out: &mut VecDeque<Incoming>, limit: usize) -> DrainStatus {
        #[cfg(test)]
        {
            match &self.incoming.receiver {
                InboundRx::Shared(core, _) => core.drain_incoming(out, limit),
                InboundRx::Std(rx) => {
                    // For test-injected std channels, drain one item at a time so
                    // backpressure/pending tests never over-read past a barrier.
                    let cap = limit.min(1);
                    if cap == 0 {
                        return DrainStatus::Empty;
                    }
                    match rx.try_recv() {
                        Ok(item) => {
                            out.push_back(item);
                            DrainStatus::More
                        }
                        Err(TryRecvError::Empty) => DrainStatus::Empty,
                        Err(TryRecvError::Disconnected) => DrainStatus::Disconnected,
                    }
                }
            }
        }
        #[cfg(not(test))]
        {
            self.core.drain_incoming(out, limit)
        }
    }

    #[inline]
    pub fn push_front_incoming(&self, remaining: &mut VecDeque<Incoming>) {
        #[cfg(test)]
        {
            if let InboundRx::Shared(core, _) = &self.incoming.receiver {
                core.push_front_incoming(remaining);
            }
        }
        #[cfg(not(test))]
        {
            self.core.push_front_incoming(remaining);
        }
    }

    #[inline]
    pub fn drain_written(&self, out: &mut Vec<Written>, limit: usize) -> DrainStatus {
        #[cfg(test)]
        {
            match &self.written.receiver {
                InboundRx::Shared(core, _) => core.drain_written(out, limit),
                InboundRx::Std(rx) => {
                    let mut taken = 0;
                    while taken < limit {
                        match rx.try_recv() {
                            Ok(item) => {
                                out.push(item);
                                taken += 1;
                            }
                            Err(TryRecvError::Empty) => return DrainStatus::Empty,
                            Err(TryRecvError::Disconnected) => {
                                return if taken > 0 {
                                    DrainStatus::More
                                } else {
                                    DrainStatus::Disconnected
                                };
                            }
                        }
                    }
                    DrainStatus::More
                }
            }
        }
        #[cfg(not(test))]
        {
            self.core.drain_written(out, limit)
        }
    }

    #[inline]
    pub fn drain_wire_progress(&self, out: &mut Vec<WireProgress>, limit: usize) {
        #[cfg(test)]
        {
            match &self.wire_progress.receiver {
                InboundRx::Shared(core, _) => core.drain_wire_progress(out, limit),
                InboundRx::Std(rx) => {
                    for _ in 0..limit {
                        match rx.try_recv() {
                            Ok(item) => out.push(item),
                            Err(_) => break,
                        }
                    }
                }
            }
        }
        #[cfg(not(test))]
        {
            self.core.drain_wire_progress(out, limit);
        }
    }

    #[inline]
    pub fn shrink_idle_queues(&self) {
        self.core.shrink_idle_queues();
    }

    /// Re-arms the activity flag when the core could not drain a lane fully
    /// (for example due to ordinary-input backpressure).
    #[inline]
    pub fn mark_activity(&self) {
        if !self.core.activity.swap(true, Ordering::AcqRel)
            && let Some((ready, id)) = &self.core.ready
        {
            ready.push(*id);
        }
    }

    #[cfg(test)]
    pub fn pause_wire_progress(&self, pause: bool) {
        self.core.pause_progress.store(pause, Ordering::Release);
    }

    #[cfg(test)]
    pub fn wire_progress_is_waiting(&self) -> bool {
        self.core.progress_waiting.load(Ordering::Acquire)
    }

    /// When the transport received its last complete packet, whether or
    /// not the core has taken it. Keep Alive runs from this instant: what
    /// the peer sent counts when the broker hears it, and a packet the
    /// broker holds does not stop the clock.
    #[inline]
    pub fn last_heard(&self) -> Option<Instant> {
        match self.core.heard.load(Ordering::Relaxed) {
            0 => None,
            since => epoch().checked_add(Duration::from_nanos(since - 1)),
        }
    }

    /// Ends the connection. A plain task is stopped here: its socket is
    /// shut on this thread. A TLS task owns the state that seals the
    /// close_notify alert (RFC 8446 §6.1), so it is only cancelled here: it
    /// sends the alert, shuts the socket and records the reason on its own
    /// thread when it ends. The owner of a TLS connection waits for
    /// `is_finished` before it treats the socket as shut.
    pub fn close(&self) {
        if self.is_tls() {
            self.core.cancelled.store(true, Ordering::Release);
        } else {
            self.core.stop("cancelled");
        }
        self.core.link.notify();
    }

    /// The task published its terminal facts and left its reactor.
    #[inline]
    pub fn is_finished(&self) -> bool {
        self.core.link.is_finished()
    }

    /// Returns false while the task runs. Nothing is left to reap after it.
    #[inline]
    pub fn join_finished(&mut self) -> bool {
        self.is_finished()
    }

    pub fn terminal_reason(&self) -> Option<&'static str> {
        self.core
            .to_core
            .lock()
            .ok()
            .and_then(|state| state.terminal)
            .map(|end| end.code)
    }

    pub fn terminal_observed_ns(&self) -> Option<u64> {
        self.core
            .to_core
            .lock()
            .ok()
            .and_then(|state| state.terminal)?
            .observed_ns
    }

    #[inline]
    pub fn send_buffer_bytes(&self) -> usize {
        self.core.send_buffer as usize
    }

    /// Returns a cloneable write-only handle for a resident `DeliveryShard`.
    #[inline]
    pub fn outgoing_handle(&self) -> OutgoingHandle {
        OutgoingHandle {
            core: self.core.clone(),
        }
    }

    /// Installs the direct I/O route for this connection once `CONNACK` has
    /// been queued so subscriber ACKs and shard-owned `Written` receipts bypass
    /// the Coordinator thread.
    pub fn install_shard_route(&self, route: ShardIoRoute) {
        let _ = self.core.shard_route.set(route);
        self.core.shard_route_active.store(true, Ordering::Release);
    }
}

#[inline]
fn written_owner(written: &Written) -> Option<LogicalOwnerId> {
    let corr = match written {
        Written::Started { correlation }
        | Written::NotStarted { correlation, .. }
        | Written::Complete { correlation, .. }
        | Written::Failed { correlation, .. } => *correlation,
    };
    corr.map(|(effect, _)| effect.owner)
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.close();
        #[cfg(not(test))]
        {
            if let Ok(mut state) = self.core.to_core.lock() {
                state.alive &= !(ALIVE_IN_RX | ALIVE_WRITTEN_RX | ALIVE_PROG_RX);
            }
            if let Ok(mut out) = self.core.outgoing.lock() {
                out.tx_alive = false;
            }
            self.core.link.notify();
        }
    }
}

/// One instant all the transports of the process count from.
fn epoch() -> Instant {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    *EPOCH.get_or_init(Instant::now)
}

#[inline]
fn record_heard(core: &ConnCore) {
    let since = u64::try_from(epoch().elapsed().as_nanos()).unwrap_or(u64::MAX - 1);
    core.heard.store(since + 1, Ordering::Relaxed);
}

fn retryable(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut | io::ErrorKind::Interrupted
    )
}

pub(super) enum Report {
    Written(Written),
    Progress(WireProgress),
}

/// Cold backpressure state for a transport task, allocated only when a lane
/// towards the core refuses an item. Idle and steady-state tasks keep `None`.
#[derive(Default)]
pub(super) struct StalledState {
    pub(super) held: VecDeque<Incoming>,
    pub(super) reports: VecDeque<Report>,
    pub(super) stalled_since: Option<Instant>,
}

impl StalledState {
    #[inline]
    pub(super) fn is_empty(&self) -> bool {
        self.held.is_empty() && self.reports.is_empty()
    }
}

/// Where a parser takes its bytes. Zero means none is available now; the
/// end of the stream is an error named by the source.
trait Source {
    fn pull(&mut self, into: &mut [u8], partial: bool) -> Result<usize, &'static str>;
}

/// Unread partial-packet tail bytes stashed between reactor turns.
struct Leftover {
    bytes: Vec<u8>,
    start: usize,
}

struct ScratchSource<'a> {
    stream: &'a TcpStream,
    leftover: &'a mut Option<Box<Leftover>>,
    scratch: &'a mut [u8],
    scratch_start: usize,
    scratch_end: usize,
}

impl ScratchSource<'_> {
    #[inline]
    fn has_leftover(&self) -> bool {
        self.leftover
            .as_ref()
            .is_some_and(|l| l.start < l.bytes.len())
    }

    #[inline]
    fn scratch_slice(&self) -> &[u8] {
        &self.scratch[self.scratch_start..self.scratch_end]
    }

    #[inline]
    fn advance_scratch(&mut self, n: usize) {
        self.scratch_start += n;
    }

    #[inline]
    fn refill_scratch(&mut self, partial: bool) -> Result<usize, &'static str> {
        self.scratch_start = 0;
        self.scratch_end = read_some(self.stream, self.scratch, partial)?;
        Ok(self.scratch_end)
    }
}

impl Drop for ScratchSource<'_> {
    fn drop(&mut self) {
        let rem = self.scratch_end - self.scratch_start;
        if rem > 0 {
            let slice = &self.scratch[self.scratch_start..self.scratch_end];
            if let Some(left) = self.leftover.as_mut() {
                debug_assert_eq!(left.start, left.bytes.len());
                left.bytes.clear();
                left.bytes.extend_from_slice(slice);
                left.start = 0;
            } else {
                *self.leftover = Some(Box::new(Leftover {
                    bytes: slice.to_vec(),
                    start: 0,
                }));
            }
        } else if self
            .leftover
            .as_ref()
            .is_some_and(|l| l.start >= l.bytes.len())
        {
            *self.leftover = None;
        }
    }
}

/// One read of a non-blocking socket. Zero means nothing is available now.
fn read_some(stream: &TcpStream, into: &mut [u8], partial: bool) -> Result<usize, &'static str> {
    loop {
        match (&*stream).read(into) {
            Ok(0) => {
                return Err(if partial { "truncated_packet" } else { "eof" });
            }
            Ok(count) => return Ok(count),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) if retryable(&error) => return Ok(0),
            Err(_) => return Err("read_io"),
        }
    }
}

impl Source for ScratchSource<'_> {
    fn pull(&mut self, into: &mut [u8], partial: bool) -> Result<usize, &'static str> {
        if let Some(left) = self.leftover.as_mut()
            && left.start < left.bytes.len()
        {
            let avail = left.bytes.len() - left.start;
            let count = into.len().min(avail);
            into[..count].copy_from_slice(&left.bytes[left.start..left.start + count]);
            left.start += count;
            if left.start == left.bytes.len() {
                *self.leftover = None;
            }
            return Ok(count);
        }
        if self.scratch_start == self.scratch_end {
            // A large body is read straight into its packet.
            if into.len() >= self.scratch.len() {
                return read_some(self.stream, into, partial);
            }
            self.scratch_start = 0;
            self.scratch_end = read_some(self.stream, self.scratch, partial)?;
            if self.scratch_end == 0 {
                return Ok(0);
            }
        }
        let count = into.len().min(self.scratch_end - self.scratch_start);
        into[..count]
            .copy_from_slice(&self.scratch[self.scratch_start..self.scratch_start + count]);
        self.scratch_start += count;
        Ok(count)
    }
}

struct PartialParse {
    prefix: [u8; 5],
    prefix_len: u8,
    header: Option<codec::Header>,
    body: FrameBytes,
    offset: usize,
    since: Option<Instant>,
}

struct ParserCold {
    previous: Option<MessageTime>,
    refused: Option<&'static str>,
}

/// Incremental MQTT framing within the current packet's absolute budget.
/// Idle connections keep `partial == None` and `cold == None` (zero heap bytes).
#[derive(Default)]
struct Parser {
    partial: Option<Box<PartialParse>>,
    cold: Option<Box<ParserCold>>,
    previous_tick: u64,
    consumed: usize,
    has_previous_tick: bool,
}

impl Parser {
    #[inline]
    fn is_partial(&self) -> bool {
        self.partial.as_ref().is_some_and(|p| p.since.is_some())
    }

    /// Ends framing: no packet has a deadline any more, the runtime owns
    /// the one of the closing connection.
    fn refuse(&mut self, code: &'static str) -> Incoming {
        self.partial = None;
        self.cold
            .get_or_insert_with(|| {
                Box::new(ParserCold {
                    previous: None,
                    refused: None,
                })
            })
            .refused = Some(code);
        Incoming::Refused { code }
    }

    #[inline]
    fn observe(
        &mut self,
        clock: &dyn SessionClock,
        cached_fixed: &mut Option<FrameObservation>,
    ) -> FrameObservation {
        if let Some(domain) = clock.fixed_domain() {
            if let Some(obs) = *cached_fixed {
                return obs;
            }
            let reading = clock.read()?;
            if reading.domain != domain {
                return Err(TimeError::DomainChanged);
            }
            if self.has_previous_tick && reading.tick < self.previous_tick {
                return Err(TimeError::ClockRegressed);
            }
            self.previous_tick = reading.tick;
            self.has_previous_tick = true;
            let obs = Ok(MessageTime {
                domain,
                tick: reading.tick,
            });
            *cached_fixed = Some(obs);
            return obs;
        }
        let cold = self.cold.get_or_insert_with(|| {
            Box::new(ParserCold {
                previous: None,
                refused: None,
            })
        });
        frame_observation(clock, &mut cold.previous)
    }

    /// Zero-copy fast path for complete packets sitting contiguously in the
    /// reactor scratch buffer: avoids byte-by-byte header pulls, zero-filling
    /// `FrameBytes`, and allocating `PartialParse`.
    fn packet_from_scratch(
        &mut self,
        source: &mut ScratchSource<'_>,
        clock: &dyn SessionClock,
        cached_fixed: &mut Option<FrameObservation>,
    ) -> Result<Option<Incoming>, &'static str> {
        if self.partial.is_none()
            && self.cold.as_ref().is_none_or(|c| c.refused.is_none())
            && !source.has_leftover()
        {
            if source.scratch_slice().is_empty() {
                if source.refill_scratch(false)? == 0 {
                    return Ok(None);
                }
                // A fresh socket read invalidates any previous batch timestamp.
                *cached_fixed = None;
            }
            let avail = source.scratch_slice();
            match codec::header(&avail[..avail.len().min(5)], MAX_PACKET_BYTES) {
                Ok(Some(header)) => {
                    let total = header.encoded_len + header.remaining;
                    if avail.len() >= total {
                        let mut body = FrameBytes::try_with_capacity(header.remaining)
                            .map_err(|_| "allocation_failed")?;
                        body.extend_from_slice(&avail[header.encoded_len..total]);
                        source.advance_scratch(total);
                        self.consumed = self.consumed.wrapping_add(total);
                        let observed = self.observe(clock, cached_fixed);
                        return Ok(Some(Incoming::Packet {
                            first: header.first,
                            body,
                            observed,
                        }));
                    }
                }
                Ok(None) => {}
                Err(error) if error.kind() == ErrorKind::Oversized => {
                    let skip = avail.len().min(5);
                    source.advance_scratch(skip);
                    self.consumed = self.consumed.wrapping_add(skip);
                    return Ok(Some(self.refuse("packet_too_large")));
                }
                Err(_) => {
                    let skip = avail.len().min(5);
                    source.advance_scratch(skip);
                    self.consumed = self.consumed.wrapping_add(skip);
                    return Ok(Some(self.refuse("malformed_header")));
                }
            }
        }
        self.packet(source, clock, cached_fixed)
    }

    fn packet(
        &mut self,
        source: &mut impl Source,
        clock: &dyn SessionClock,
        cached_fixed: &mut Option<FrameObservation>,
    ) -> Result<Option<Incoming>, &'static str> {
        if let Some(code) = self.cold.as_ref().and_then(|c| c.refused) {
            let mut dropped = [0u8; 512];
            let count = source.pull(&mut dropped, false).map_err(|_| code)?;
            self.consumed = self.consumed.wrapping_add(count);
            return Ok(None);
        }
        if self
            .partial
            .as_ref()
            .and_then(|p| p.since)
            .is_some_and(|start| start.elapsed() >= PARTIAL_PACKET_TIMEOUT)
        {
            return Err("partial_packet_timeout");
        }
        if self.partial.is_none() {
            let mut first_byte = [0u8; 1];
            if source.pull(&mut first_byte, false)? == 0 {
                return Ok(None);
            }
            self.consumed = self.consumed.wrapping_add(1);
            let mut prefix = [0u8; 5];
            prefix[0] = first_byte[0];
            match codec::header(&prefix[..1], MAX_PACKET_BYTES) {
                Ok(header) => {
                    self.partial = Some(Box::new(PartialParse {
                        prefix,
                        prefix_len: 1,
                        header,
                        body: FrameBytes::empty(),
                        offset: 0,
                        since: None,
                    }));
                }
                Err(error) if error.kind() == ErrorKind::Oversized => {
                    return Ok(Some(self.refuse("packet_too_large")));
                }
                Err(_) => return Ok(Some(self.refuse("malformed_header"))),
            }
        }
        let state = self.partial.as_mut().expect("partial allocated above");
        while state.header.is_none() {
            let plen = usize::from(state.prefix_len);
            if plen == state.prefix.len() {
                return Ok(Some(self.refuse("malformed_header")));
            }
            let next = &mut state.prefix[plen..plen + 1];
            let partial = plen > 0 || state.since.is_some();
            if source.pull(next, partial)? == 0 {
                if plen > 0 {
                    state.since.get_or_insert_with(Instant::now);
                }
                return Ok(None);
            }
            self.consumed = self.consumed.wrapping_add(1);
            state.prefix_len += 1;
            match codec::header(
                &state.prefix[..usize::from(state.prefix_len)],
                MAX_PACKET_BYTES,
            ) {
                Ok(header) => state.header = header,
                Err(error) if error.kind() == ErrorKind::Oversized => {
                    return Ok(Some(self.refuse("packet_too_large")));
                }
                Err(_) => return Ok(Some(self.refuse("malformed_header"))),
            }
        }
        let header = state.header.expect("header checked");
        if state.body.len() != header.remaining {
            state.body =
                FrameBytes::try_zeroed(header.remaining).map_err(|_| "allocation_failed")?;
        }
        while state.offset < state.body.len() {
            let count = source.pull(&mut state.body[state.offset..], true)?;
            if count == 0 {
                state.since.get_or_insert_with(Instant::now);
                return Ok(None);
            }
            self.consumed = self.consumed.wrapping_add(count);
            state.offset += count;
        }
        if state
            .since
            .is_some_and(|start| start.elapsed() >= PARTIAL_PACKET_TIMEOUT)
        {
            return Err("partial_packet_timeout");
        }
        let finished = self.partial.take().expect("partial finished");
        let observed = self.observe(clock, cached_fixed);
        Ok(Some(Incoming::Packet {
            first: header.first,
            body: finished.body,
            observed,
        }))
    }
}

fn frame_observation(
    clock: &dyn SessionClock,
    previous: &mut Option<MessageTime>,
) -> FrameObservation {
    let reading = clock.read()?;
    let observed = MessageTime {
        domain: reading.domain,
        tick: reading.tick,
    };
    if let Some(last) = *previous {
        if last.domain != observed.domain {
            return Err(TimeError::DomainChanged);
        }
        if observed.tick < last.tick {
            return Err(TimeError::ClockRegressed);
        }
    }
    *previous = Some(observed);
    Ok(observed)
}

#[derive(Debug, PartialEq, Eq)]
enum WriteFailure {
    NotStarted(MessageExpiryBlock),
    Failed(&'static str),
}

fn prepare_expiry(
    bytes: &mut [u8],
    expiry: WriteExpiry,
    clock: &dyn SessionClock,
) -> Result<(), WriteFailure> {
    let field = expiry
        .value_offset
        .map(|at| {
            let end = at
                .checked_add(4)
                .ok_or(WriteFailure::Failed("output_expiry_offset"))?;
            bytes
                .get_mut(at..end)
                .ok_or(WriteFailure::Failed("output_expiry_offset"))
        })
        .transpose()?;
    let state = clock
        .read()
        .ok()
        .and_then(|now| {
            expiry
                .expiry
                .state(MessageTime {
                    domain: now.domain,
                    tick: now.tick,
                })
                .ok()
        })
        .unwrap_or(ExpiryState::TemporalUncertain);
    let remaining = match state {
        ExpiryState::Fresh { remaining } => remaining,
        _ if expiry.started => 0,
        ExpiryState::Expired => {
            return Err(WriteFailure::NotStarted(MessageExpiryBlock::Expired));
        }
        ExpiryState::TemporalUncertain => {
            return Err(WriteFailure::NotStarted(
                MessageExpiryBlock::TemporalUncertain,
            ));
        }
    };
    if let Some(field) = field {
        field.copy_from_slice(&remaining.to_be_bytes());
    }
    Ok(())
}

/// What one write attempt on a frame decided.
#[derive(Debug, PartialEq, Eq)]
enum Attempt {
    /// Every byte was accepted within the budget.
    Done,
    /// Bytes were accepted and some remain.
    Advanced,
    /// The transport accepted nothing now.
    Blocked,
}

pub(crate) type CoalescedEntry = (Option<(EffectId, ConnectionKey)>, WriteReport, usize);

/// A frame (or coalesced batch of non-expiring frames) taken from the output
/// lane and its one absolute write budget. Boxed on `Task` only when a socket
/// write cannot complete in one non-blocking syscall or carries message expiry.
struct Active {
    frame: Write,
    batch: Vec<CoalescedEntry>,
    offset: usize,
    deadline: Instant,
    /// Every byte left within the budget; only the receipts remain.
    written: bool,
}

impl Active {
    fn new(frame: Write) -> Self {
        Self {
            frame,
            batch: Vec::new(),
            offset: 0,
            deadline: Instant::now() + WRITE_TIMEOUT,
            written: false,
        }
    }

    fn correlations(self) -> Vec<Option<(EffectId, ConnectionKey)>> {
        if self.batch.is_empty() {
            vec![self.frame.correlation]
        } else {
            self.batch.into_iter().map(|(c, _, _)| c).collect()
        }
    }
}

/// One attempt to write the rest of a frame. Expiry is checked immediately
/// before each attempt made while no byte has left: an unsuccessful attempt
/// may consume the rest of the lifetime. `started` runs once, on the first
/// accepted byte.
fn write_attempt(
    active: &mut Active,
    cancelled: &AtomicBool,
    clock: &dyn SessionClock,
    write: &mut impl FnMut(&[u8], &[u8], Duration) -> io::Result<usize>,
    started: &mut impl FnMut() -> bool,
) -> Result<Attempt, WriteFailure> {
    let total = active.frame.total_len();
    if total > MAX_PACKET_BYTES {
        return Err(WriteFailure::Failed("output_packet_too_large"));
    }
    if active.offset < total {
        if cancelled.load(Ordering::Acquire) {
            return Err(WriteFailure::Failed("cancelled"));
        }
        let remaining = active.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(WriteFailure::Failed("write_timeout"));
        }
        if active.offset == 0
            && let Some(expiry) = active.frame.expiry.as_deref().copied()
        {
            prepare_expiry(&mut active.frame.bytes, expiry, clock)?;
        }
        let hdr_len = active.frame.bytes.len();
        let (first_part, second_part): (&[u8], &[u8]) = if active.offset < hdr_len {
            (
                &active.frame.bytes[active.offset..],
                active
                    .frame
                    .payload
                    .as_ref()
                    .map_or(&[], SharedBytes::as_slice),
            )
        } else {
            let pay = active
                .frame
                .payload
                .as_ref()
                .map_or(&[][..], SharedBytes::as_slice);
            (&pay[active.offset - hdr_len..], &[])
        };
        match write(first_part, second_part, remaining) {
            Ok(0) => return Err(WriteFailure::Failed("write_zero")),
            Ok(count) => {
                if count > total - active.offset {
                    return Err(WriteFailure::Failed("invalid_write_progress"));
                }
                let first = active.offset == 0;
                active.offset += count;
                if first && !started() {
                    return Err(WriteFailure::Failed("write_result_queue_full"));
                }
            }
            Err(error) if retryable(&error) => return Ok(Attempt::Blocked),
            Err(_) => return Err(WriteFailure::Failed("write_io")),
        }
        if active.offset < total {
            return Ok(Attempt::Advanced);
        }
    }
    if Instant::now() >= active.deadline {
        return Err(WriteFailure::Failed("write_timeout"));
    }
    Ok(Attempt::Done)
}

/// Writes one frame with a transport that waits by itself, within one
/// absolute budget. The task makes the same attempts across its turns.
#[cfg(test)]
fn write_attempts(
    bytes: &mut [u8],
    expiry: Option<WriteExpiry>,
    cancelled: &AtomicBool,
    clock: &dyn SessionClock,
    mut write: impl FnMut(&[u8], Duration) -> io::Result<usize>,
    mut started: impl FnMut() -> bool,
) -> Result<(), WriteFailure> {
    let mut active = Active::new(Write {
        correlation: None,
        bytes: FrameBytes::from_slice(bytes),
        payload: None,
        expiry: expiry.map(Box::new),
        report: WriteReport::CompleteAndWireProgress,
    });
    let result = loop {
        let mut bounded =
            |part: &[u8], _tail: &[u8], remaining: Duration| write(part, remaining.min(POLL));
        match write_attempt(&mut active, cancelled, clock, &mut bounded, &mut started) {
            Ok(Attempt::Done) => break Ok(()),
            Ok(_) => {}
            Err(failure) => break Err(failure),
        }
    };
    bytes.copy_from_slice(&active.frame.bytes);
    result
}

/// One plain TCP connection task (~80 bytes when idle, zero heap buffers).
struct Task {
    core: Arc<ConnCore>,
    clock: Arc<dyn SessionClock>,
    leftover: Option<Box<Leftover>>,
    parser: Parser,
    stalled: Option<Box<StalledState>>,
    active: Option<Box<Active>>,
    #[cfg(test)]
    paused: Option<Instant>,
}

impl Task {
    /// Holds the completion of a correlated frame while a test asks for it.
    #[cfg(test)]
    fn held_by_test(&mut self) -> bool {
        if self.core.pause_progress.load(Ordering::Acquire)
            && self.paused.get_or_insert_with(Instant::now).elapsed() <= Duration::from_secs(2)
        {
            self.core.progress_waiting.store(true, Ordering::Release);
            return true;
        }
        self.paused = None;
        self.core.progress_waiting.store(false, Ordering::Release);
        false
    }

    #[inline]
    fn has_stalled_reports(&self) -> bool {
        self.stalled.as_ref().is_some_and(|s| !s.reports.is_empty())
    }

    #[inline]
    fn push_completion(
        reports: &mut Vec<Report>,
        correlation: Option<(EffectId, ConnectionKey)>,
        report: WriteReport,
        bytes: usize,
    ) {
        if report == WriteReport::CompleteAndWireProgress
            && let Some((effect, connection)) = correlation
        {
            reports.push(Report::Progress(WireProgress { effect, connection }));
        }
        if report != WriteReport::None {
            reports.push(Report::Written(Written::Complete { correlation, bytes }));
        }
    }

    fn output_turn(&mut self, scratch: &mut ReactorScratch) -> Result<bool, &'static str> {
        let mut progressed = false;
        scratch.reports_batch.clear();
        for _ in 0..BURST {
            if self.active.is_none() {
                if self.has_stalled_reports() {
                    break;
                }
                #[cfg(test)]
                let allow_coalesce = !self.core.pause_progress.load(Ordering::Acquire);
                #[cfg(not(test))]
                let allow_coalesce = true;
                self.core
                    .pop_outgoing_batch(
                        &mut scratch.write_batch,
                        allow_coalesce,
                        COALESCE_BYTES,
                        MAX_COALESCE_FRAMES,
                    )
                    .map_err(|()| "output_producer_closed")?;
                if scratch.write_batch.is_empty() {
                    break;
                }
                if scratch.write_batch.len() == 1 {
                    let frame = scratch.write_batch.pop().expect("len == 1");
                    // Fast path: non-expiring single frame completing in one non-blocking write.
                    if frame.expiry.is_none() {
                        let total = frame.total_len();
                        if total > MAX_PACKET_BYTES {
                            self.active = Some(Box::new(Active::new(frame)));
                            return Err("output_packet_too_large");
                        }
                        if self.core.is_cancelled() {
                            self.active = Some(Box::new(Active::new(frame)));
                            return Err("cancelled");
                        }
                        let write_res = match &frame.payload {
                            None => (&self.core.stream).write(&frame.bytes),
                            Some(pay) => (&self.core.stream).write_vectored(&[
                                IoSlice::new(&frame.bytes),
                                IoSlice::new(pay.as_slice()),
                            ]),
                        };
                        match write_res {
                            Ok(count) if count == total => {
                                progressed = true;
                                #[cfg(test)]
                                if frame.correlation.is_some()
                                    && frame.report != WriteReport::None
                                    && self.held_by_test()
                                {
                                    let mut act = Active::new(frame);
                                    act.offset = total;
                                    act.written = true;
                                    self.active = Some(Box::new(act));
                                    break;
                                }
                                Self::push_completion(
                                    &mut scratch.reports_batch,
                                    frame.correlation,
                                    frame.report,
                                    total,
                                );
                                if scratch.reports_batch.len() >= BURST {
                                    self.core.flush_reports(
                                        &mut scratch.reports_batch,
                                        &mut self.stalled,
                                    )?;
                                }
                                continue;
                            }
                            Ok(0) => {
                                self.active = Some(Box::new(Active::new(frame)));
                                return Err("write_zero");
                            }
                            Ok(count) if count > total => {
                                self.active = Some(Box::new(Active::new(frame)));
                                return Err("invalid_write_progress");
                            }
                            Ok(count) => {
                                let mut act = Active::new(frame);
                                act.offset = count;
                                self.active = Some(Box::new(act));
                                progressed = true;
                                continue;
                            }
                            Err(err) if retryable(&err) => {
                                self.active = Some(Box::new(Active::new(frame)));
                                break;
                            }
                            Err(_) => {
                                self.active = Some(Box::new(Active::new(frame)));
                                return Err("write_io");
                            }
                        }
                    } else {
                        self.active = Some(Box::new(Active::new(frame)));
                        progressed = true;
                    }
                } else {
                    // Coalesced batch fast path: flatten into reusable scratch bytes
                    // (zero heap allocation when the single write succeeds).
                    scratch.coalesce_bytes.clear();
                    for f in &scratch.write_batch {
                        f.append_to_vec(&mut scratch.coalesce_bytes);
                    }
                    let total = scratch.coalesce_bytes.len();
                    let materialize =
                        |batch: &mut Vec<Write>, bytes: &[u8], offset: usize| -> Box<Active> {
                            let entries: Vec<CoalescedEntry> = batch
                                .drain(..)
                                .map(|f| (f.correlation, f.report, f.total_len()))
                                .collect();
                            let mut act = Active::new(Write {
                                correlation: None,
                                bytes: FrameBytes::from(bytes.to_vec()),
                                payload: None,
                                expiry: None,
                                report: WriteReport::None,
                            });
                            act.batch = entries;
                            act.offset = offset;
                            Box::new(act)
                        };
                    if self.core.is_cancelled() {
                        self.active = Some(materialize(
                            &mut scratch.write_batch,
                            &scratch.coalesce_bytes,
                            0,
                        ));
                        return Err("cancelled");
                    }
                    match (&self.core.stream).write(&scratch.coalesce_bytes) {
                        Ok(count) if count == total => {
                            progressed = true;
                            for f in scratch.write_batch.drain(..) {
                                Self::push_completion(
                                    &mut scratch.reports_batch,
                                    f.correlation,
                                    f.report,
                                    f.total_len(),
                                );
                            }
                            if scratch.reports_batch.len() >= BURST {
                                self.core
                                    .flush_reports(&mut scratch.reports_batch, &mut self.stalled)?;
                            }
                            continue;
                        }
                        Ok(0) => {
                            self.active = Some(materialize(
                                &mut scratch.write_batch,
                                &scratch.coalesce_bytes,
                                0,
                            ));
                            return Err("write_zero");
                        }
                        Ok(count) if count > total => {
                            self.active = Some(materialize(
                                &mut scratch.write_batch,
                                &scratch.coalesce_bytes,
                                0,
                            ));
                            return Err("invalid_write_progress");
                        }
                        Ok(count) => {
                            self.active = Some(materialize(
                                &mut scratch.write_batch,
                                &scratch.coalesce_bytes,
                                count,
                            ));
                            progressed = true;
                            continue;
                        }
                        Err(err) if retryable(&err) => {
                            self.active = Some(materialize(
                                &mut scratch.write_batch,
                                &scratch.coalesce_bytes,
                                0,
                            ));
                            break;
                        }
                        Err(_) => {
                            self.active = Some(materialize(
                                &mut scratch.write_batch,
                                &scratch.coalesce_bytes,
                                0,
                            ));
                            return Err("write_io");
                        }
                    }
                }
            }
            let active = self.active.as_mut().expect("frame taken above");
            if !active.written {
                let correlation = active.frame.correlation;
                let reports_start = active.frame.expiry.is_some() && correlation.is_some();
                let stream = &self.core.stream;
                let reports_out = &mut scratch.reports_batch;
                let attempt = write_attempt(
                    active,
                    &self.core.cancelled,
                    self.clock.as_ref(),
                    &mut |head, tail, _| {
                        if tail.is_empty() {
                            (&*stream).write(head)
                        } else {
                            (&*stream).write_vectored(&[IoSlice::new(head), IoSlice::new(tail)])
                        }
                    },
                    &mut || {
                        if reports_start {
                            reports_out.push(Report::Written(Written::Started { correlation }));
                        }
                        true
                    },
                );
                match attempt {
                    Ok(Attempt::Done) => active.written = true,
                    Ok(Attempt::Advanced) => {
                        progressed = true;
                        if !scratch.reports_batch.is_empty() {
                            self.core
                                .flush_reports(&mut scratch.reports_batch, &mut self.stalled)?;
                        }
                        continue;
                    }
                    Ok(Attempt::Blocked) => break,
                    Err(WriteFailure::NotStarted(reason)) => {
                        scratch
                            .reports_batch
                            .push(Report::Written(Written::NotStarted {
                                correlation,
                                reason,
                            }));
                        self.active = None;
                        progressed = true;
                        self.core
                            .flush_reports(&mut scratch.reports_batch, &mut self.stalled)?;
                        continue;
                    }
                    Err(WriteFailure::Failed(code)) => return Err(code),
                }
            }
            let active = self.active.as_ref().expect("frame written above");
            if active.batch.is_empty() {
                let (correlation, report, bytes) = (
                    active.frame.correlation,
                    active.frame.report,
                    active.frame.total_len(),
                );
                if correlation.is_some() {
                    #[cfg(test)]
                    if report != WriteReport::None && self.held_by_test() {
                        if !scratch.reports_batch.is_empty() {
                            self.core
                                .flush_reports(&mut scratch.reports_batch, &mut self.stalled)?;
                        }
                        break;
                    }
                }
                Self::push_completion(&mut scratch.reports_batch, correlation, report, bytes);
                self.active = None;
            } else {
                let completed = self.active.take().expect("active batch");
                for (correlation, report, bytes) in completed.batch {
                    Self::push_completion(&mut scratch.reports_batch, correlation, report, bytes);
                }
            }
            progressed = true;
            if !scratch.reports_batch.is_empty() {
                self.core
                    .flush_reports(&mut scratch.reports_batch, &mut self.stalled)?;
            }
        }
        if !scratch.reports_batch.is_empty() {
            self.core
                .flush_reports(&mut scratch.reports_batch, &mut self.stalled)?;
        }
        Ok(progressed)
    }

    fn input_turn(&mut self, scratch: &mut ReactorScratch) -> Result<bool, &'static str> {
        let mut progressed = false;
        if let Some(st) = self.stalled.as_mut()
            && !st.held.is_empty()
        {
            let before = st.held.len();
            let drained = self.core.flush_held_incoming(st)?;
            progressed |= st.held.len() < before;
            if st.is_empty() {
                self.stalled = None;
            }
            if !drained {
                return Ok(progressed);
            }
        }
        let avail = self.core.incoming_free_slots();
        let limit = if avail == 0 { 1 } else { avail.min(BURST) };
        scratch.incoming_batch.clear();
        let mut heard_packet = false;
        let mut cached_fixed: Option<FrameObservation> = None;
        let mut source = ScratchSource {
            stream: &self.core.stream,
            leftover: &mut self.leftover,
            scratch: &mut scratch.read_buf,
            scratch_start: 0,
            scratch_end: 0,
        };
        let mut parse_err: Option<&'static str> = None;
        for _ in 0..limit {
            let before = self.parser.consumed;
            match self.parser.packet_from_scratch(
                &mut source,
                self.clock.as_ref(),
                &mut cached_fixed,
            ) {
                Ok(Some(packet)) => {
                    if matches!(packet, Incoming::Packet { .. }) {
                        heard_packet = true;
                    }
                    scratch.incoming_batch.push(packet);
                    progressed = true;
                }
                Ok(None) => {
                    progressed |= self.parser.consumed != before;
                    break;
                }
                Err(code) => {
                    parse_err = Some(code);
                    break;
                }
            }
        }
        drop(source);
        if heard_packet {
            record_heard(&self.core);
        }
        if !scratch.incoming_batch.is_empty() {
            self.core
                .push_incoming_batch(&mut scratch.incoming_batch, &mut self.stalled)?;
        }
        if let Some(code) = parse_err {
            return Err(code);
        }
        Ok(progressed)
    }
}

impl Driven for Task {
    fn descriptor(&self) -> RawFd {
        self.core.stream.as_raw_fd()
    }

    fn turn(&mut self, scratch: &mut ReactorScratch) -> Result<Turn, &'static str> {
        if self.core.is_cancelled() {
            return Err("cancelled");
        }
        scratch.reports_batch.clear();
        let mut progress = self
            .core
            .flush_reports(&mut scratch.reports_batch, &mut self.stalled)?;
        progress |= self.output_turn(scratch)?;
        progress |= self.input_turn(scratch)?;
        Ok(Turn {
            progress,
            waiting: self.active.is_some()
                || self.parser.is_partial()
                || self.stalled.as_ref().is_some_and(|s| !s.is_empty()),
        })
    }

    fn finish(self: Box<Self>, code: &'static str) {
        let task = *self;
        task.core.stop(code);
        let active = task
            .active
            .map(|act| act.correlations())
            .unwrap_or_default();
        task.core.finish_task(task.stalled, code, active);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session_time::{TimeError, testing::FakeClock};
    use mqttiumd_contract::ids::{
        ConnectionGeneration, EffectSeq, LogicalOwnerId, RunEpoch, SessionId,
    };
    use mqttiumd_contract::packet::SharedBytes;
    use mqttiumd_store_api::SessionClockReading;
    use std::cell::Cell;
    use std::net::{Shutdown, TcpListener};
    use std::thread;
    use std::time::{Duration, Instant};

    fn pair() -> (Connection, TcpStream) {
        pair_with_clock(Arc::new(SystemClock::new().unwrap()))
    }

    fn pair_with_clock(clock: Arc<dyn SessionClock>) -> (Connection, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let peer = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        (spawn_with_clock(server, clock).unwrap(), peer)
    }

    fn control() -> AtomicBool {
        AtomicBool::new(false)
    }

    fn reading(tick: u64) -> SessionClockReading {
        SessionClockReading {
            domain: [1; 32],
            tick,
        }
    }

    fn timed_frame() -> (Vec<u8>, WriteExpiry) {
        let encoded = codec::encode_qos0_expiring(
            &SharedBytes::copy_from(b"t", 1).unwrap(),
            &SharedBytes::copy_from(b"payload", 7).unwrap(),
            false,
            Some(2),
        )
        .unwrap();
        (
            encoded.bytes,
            WriteExpiry {
                expiry: MessageExpiry {
                    observed: MessageTime {
                        domain: [1; 32],
                        tick: 100,
                    },
                    interval: 2,
                },
                value_offset: encoded.expiry_value_offset,
                started: false,
            },
        )
    }

    fn correlation(sequence: u64) -> Option<(EffectId, ConnectionKey)> {
        Some((
            EffectId {
                epoch: RunEpoch::new(1),
                owner: LogicalOwnerId::new(1),
                sequence: EffectSeq::new(sequence),
            },
            ConnectionKey {
                session: SessionId::new(1),
                generation: ConnectionGeneration::new(1),
            },
        ))
    }

    #[test]
    fn failed_frame_observations_never_replace_the_last_valid_reader_time() {
        let clock = FakeClock::new(reading(10));
        let mut previous = None;
        assert_eq!(frame_observation(&clock, &mut previous).unwrap().tick, 10);
        for tick in [5, 8] {
            clock.set(reading(tick));
            assert_eq!(
                frame_observation(&clock, &mut previous),
                Err(TimeError::ClockRegressed)
            );
        }
        clock.fail(TimeError::ClockUnavailable);
        assert_eq!(
            frame_observation(&clock, &mut previous),
            Err(TimeError::ClockUnavailable)
        );
        clock.set(SessionClockReading {
            domain: [2; 32],
            tick: 100,
        });
        assert_eq!(
            frame_observation(&clock, &mut previous),
            Err(TimeError::DomainChanged)
        );
        clock.set(reading(9));
        assert_eq!(
            frame_observation(&clock, &mut previous),
            Err(TimeError::ClockRegressed)
        );
        clock.set(reading(12));
        assert_eq!(frame_observation(&clock, &mut previous).unwrap().tick, 12);
    }

    fn incoming(connection: &Connection) -> Incoming {
        connection
            .incoming
            .recv_timeout(Duration::from_millis(600))
            .unwrap()
    }

    fn refused(connection: &Connection, expected: &str) {
        match incoming(connection) {
            Incoming::Refused { code } => assert_eq!(code, expected),
            packet => panic!("expected a refused header, got {packet:?}"),
        }
    }

    fn closed(connection: &Connection, expected: &str) {
        match incoming(connection) {
            Incoming::Closed { code } => assert_eq!(code, expected),
            packet => panic!("expected closure, got {packet:?}"),
        }
    }

    fn finish(connection: &mut Connection) {
        let deadline = Instant::now() + Duration::from_millis(600);
        while !connection.is_finished() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(2));
        }
        assert!(
            connection.join_finished(),
            "socket tasks outlived cancellation budget"
        );
    }

    #[test]
    fn every_zero_progress_retry_rechecks_expiry_before_any_byte_can_leave() {
        for error_kind in [io::ErrorKind::WouldBlock, io::ErrorKind::Interrupted] {
            let clock = FakeClock::new(reading(100));
            let (mut bytes, expiry) = timed_frame();
            let attempts = Cell::new(0);
            let result = write_attempts(
                &mut bytes,
                Some(expiry),
                &control(),
                &clock,
                |_, _| {
                    attempts.set(attempts.get() + 1);
                    assert_eq!(attempts.get(), 1, "expired zero-progress retry reached I/O");
                    clock.set(reading(expiry.expiry.deadline().unwrap()));
                    Err(io::Error::from(error_kind))
                },
                || panic!("no byte was accepted"),
            );
            assert_eq!(
                result,
                Err(WriteFailure::NotStarted(MessageExpiryBlock::Expired))
            );
            assert_eq!(attempts.get(), 1);
        }
    }

    #[test]
    fn expired_retained_publication_never_reaches_the_first_socket_attempt() {
        let (_, expiry) = timed_frame();
        let encoded = codec::encode_qos0_expiring(
            &SharedBytes::copy_from(b"t", 1).unwrap(),
            &SharedBytes::copy_from(b"retained", 8).unwrap(),
            true,
            Some(2),
        )
        .unwrap();
        let mut bytes = encoded.bytes;
        assert_eq!(bytes[0] & 1, 1, "retained fixture must preserve RETAIN");
        let expiry = WriteExpiry {
            value_offset: encoded.expiry_value_offset,
            ..expiry
        };
        let clock = FakeClock::new(reading(expiry.expiry.deadline().unwrap()));
        let result = write_attempts(
            &mut bytes,
            Some(expiry),
            &control(),
            &clock,
            |_, _| panic!("expired_retained_reached_socket"),
            || panic!("expired_retained_reported_started"),
        );
        assert_eq!(
            result,
            Err(WriteFailure::NotStarted(MessageExpiryBlock::Expired)),
            "expired_retained_missing_certain_disposal"
        );
    }

    #[test]
    fn first_positive_progress_freezes_the_value_and_finishes_after_expiry() {
        let clock = FakeClock::new(reading(1_000_000_100));
        let (mut bytes, expiry) = timed_frame();
        let original = bytes.clone();
        let attempts = Cell::new(0);
        let starts = Cell::new(0);
        let mut observed = Vec::new();
        let result = write_attempts(
            &mut bytes,
            Some(expiry),
            &control(),
            &clock,
            |part, _| {
                attempts.set(attempts.get() + 1);
                let count = if attempts.get() == 1 { 1 } else { part.len() };
                observed.extend_from_slice(&part[..count]);
                // The blocking first attempt was authorized while fresh. Its
                // positive result can arrive after the deadline.
                clock.set(reading(expiry.expiry.deadline().unwrap()));
                Ok(count)
            },
            || {
                starts.set(starts.get() + 1);
                clock.fail(TimeError::ClockUnavailable);
                true
            },
        );
        assert_eq!(result, Ok(()));
        assert_eq!(attempts.get(), 2);
        assert_eq!(starts.get(), 1);
        assert_eq!(observed, bytes);
        assert_eq!(
            &bytes[expiry.value_offset.unwrap()..expiry.value_offset.unwrap() + 4],
            &1u32.to_be_bytes()
        );
        assert_eq!(
            &bytes[..expiry.value_offset.unwrap()],
            &original[..expiry.value_offset.unwrap()]
        );
        assert_eq!(
            &bytes[expiry.value_offset.unwrap() + 4..],
            &original[expiry.value_offset.unwrap() + 4..]
        );
    }

    #[test]
    fn uncertain_clock_readings_block_fresh_transmission_with_no_bytes() {
        for case in 0..4 {
            let clock = FakeClock::new(reading(100));
            let (mut bytes, mut expiry) = timed_frame();
            match case {
                0 => clock.fail(TimeError::ClockUnavailable),
                1 => clock.set(SessionClockReading {
                    domain: [2; 32],
                    tick: 100,
                }),
                2 => clock.set(reading(99)),
                _ => expiry.expiry.observed.tick = u64::MAX,
            }
            assert_eq!(
                write_attempts(
                    &mut bytes,
                    Some(expiry),
                    &control(),
                    &clock,
                    |_, _| panic!("uncertain time granted a write"),
                    || panic!("uncertain time cannot start a frame"),
                ),
                Err(WriteFailure::NotStarted(
                    MessageExpiryBlock::TemporalUncertain
                ))
            );
        }
    }

    #[test]
    fn a_known_started_exchange_replays_with_zero_after_expiry_or_clock_uncertainty() {
        for uncertain in [false, true] {
            let (mut bytes, mut expiry) = timed_frame();
            expiry.started = true;
            let clock = FakeClock::new(reading(expiry.expiry.deadline().unwrap()));
            if uncertain {
                clock.fail(TimeError::ClockUnavailable);
            }
            let starts = Cell::new(0);
            assert_eq!(
                write_attempts(
                    &mut bytes,
                    Some(expiry),
                    &control(),
                    &clock,
                    |part, _| {
                        assert_eq!(
                            &part[expiry.value_offset.unwrap()..expiry.value_offset.unwrap() + 4],
                            &[0; 4]
                        );
                        Ok(part.len())
                    },
                    || {
                        starts.set(starts.get() + 1);
                        true
                    },
                ),
                Ok(())
            );
            assert_eq!(starts.get(), 1);
        }
    }

    #[test]
    fn expiry_without_a_property_still_gates_a_legacy_write() {
        let (_, mut expiry) = timed_frame();
        expiry.value_offset = None;
        let clock = FakeClock::new(reading(100));
        let frame = b"\x30\x05\x00\x01t\0\xff";
        let mut bytes = frame.to_vec();
        assert_eq!(prepare_expiry(&mut bytes, expiry, &clock), Ok(()));
        assert_eq!(bytes, frame);
        clock.set(reading(expiry.expiry.deadline().unwrap()));
        assert_eq!(
            prepare_expiry(&mut bytes, expiry, &clock),
            Err(WriteFailure::NotStarted(MessageExpiryBlock::Expired)),
        );
        assert_eq!(bytes, frame);
    }

    #[test]
    fn a_bad_expiry_value_offset_closes_without_touching_socket_bytes() {
        let (mut bytes, mut expiry) = timed_frame();
        expiry.value_offset = Some(usize::MAX);
        assert_eq!(
            write_attempts(
                &mut bytes,
                Some(expiry),
                &control(),
                &FakeClock::new(reading(100)),
                |_, _| panic!("bad patch granted I/O"),
                || panic!("bad patch cannot start"),
            ),
            Err(WriteFailure::Failed("output_expiry_offset"))
        );
    }

    #[test]
    fn expired_queued_publication_is_disposed_without_closing_the_transport() {
        let clock = Arc::new(FakeClock::new(reading(100)));
        let (mut connection, mut peer) = pair_with_clock(clock.clone());
        connection.pause_wire_progress(true);
        connection
            .outgoing
            .send(Write {
                correlation: correlation(1),
                bytes: FrameBytes::from_slice(&[0xd0, 0]),
                payload: None,
                expiry: None,
                report: WriteReport::CompleteAndWireProgress,
            })
            .unwrap();
        let wait = Instant::now() + Duration::from_millis(600);
        while !connection.wire_progress_is_waiting() {
            assert!(
                Instant::now() < wait,
                "first frame never reached the queue barrier"
            );
            thread::sleep(Duration::from_millis(1));
        }
        let (bytes, expiry) = timed_frame();
        connection
            .outgoing
            .send(Write {
                correlation: correlation(2),
                bytes: FrameBytes::from(bytes),
                payload: None,
                expiry: Some(Box::new(expiry)),
                report: WriteReport::CompleteAndWireProgress,
            })
            .unwrap();
        connection
            .outgoing
            .send(Write {
                correlation: None,
                bytes: FrameBytes::from_slice(&[0xd0, 0]),
                payload: None,
                expiry: None,
                report: WriteReport::CompleteAndWireProgress,
            })
            .unwrap();
        clock.set(reading(expiry.expiry.deadline().unwrap()));
        connection.pause_wire_progress(false);
        assert!(
            matches!(connection.written.recv_timeout(Duration::from_millis(600)).unwrap(), Written::Complete { correlation: value, .. } if value==correlation(1))
        );
        assert!(
            matches!(connection.written.recv_timeout(Duration::from_millis(600)).unwrap(), Written::NotStarted { correlation: value, reason: MessageExpiryBlock::Expired } if value==correlation(2))
        );
        assert!(matches!(
            connection
                .written
                .recv_timeout(Duration::from_millis(600))
                .unwrap(),
            Written::Complete {
                correlation: None,
                bytes: 2
            }
        ));
        let mut actual = [0; 4];
        peer.set_read_timeout(Some(Duration::from_millis(600)))
            .unwrap();
        peer.read_exact(&mut actual).unwrap();
        assert_eq!(actual, [0xd0, 0, 0xd0, 0]);
        peer.write_all(&[0xc0, 0]).unwrap();
        assert!(
            matches!(incoming(&connection), Incoming::Packet {first:0xc0, body,..} if body.is_empty())
        );
        assert!(!connection.core.is_cancelled());
        connection.close();
        finish(&mut connection);
    }

    #[test]
    fn a_started_receipt_and_peer_ack_precede_the_local_completion_receipt() {
        let clock = Arc::new(FakeClock::new(reading(100)));
        let (mut connection, mut peer) = pair_with_clock(clock);
        connection.pause_wire_progress(true);
        let (_, mut expiry) = timed_frame();
        let encoded = codec::encode_publish_expiring(
            mqttiumd_contract::ids::OutboundPacketId::new(7).unwrap(),
            &SharedBytes::copy_from(b"t", 1).unwrap(),
            &SharedBytes::copy_from(b"payload", 7).unwrap(),
            false,
            false,
            Some(2),
        )
        .unwrap();
        expiry.value_offset = encoded.expiry_value_offset;
        let bytes = encoded.bytes;
        let expected = bytes.clone();
        connection
            .outgoing
            .send(Write {
                correlation: correlation(1),
                bytes: FrameBytes::from(bytes),
                payload: None,
                expiry: Some(Box::new(expiry)),
                report: WriteReport::CompleteAndWireProgress,
            })
            .unwrap();
        assert!(
            matches!(connection.written.recv_timeout(Duration::from_millis(600)).unwrap(), Written::Started {correlation:value} if value==correlation(1))
        );
        let mut actual = vec![0; expected.len()];
        peer.set_read_timeout(Some(Duration::from_millis(600)))
            .unwrap();
        peer.read_exact(&mut actual).unwrap();
        assert_eq!(actual, expected);
        assert!(connection.written.try_recv().is_err());
        peer.write_all(&[0x40, 2, 0, 7]).unwrap();
        assert!(
            matches!(incoming(&connection), Incoming::Packet {first:0x40,body,..} if body==[0,7])
        );
        connection.pause_wire_progress(false);
        assert!(
            matches!(connection.written.recv_timeout(Duration::from_millis(600)).unwrap(), Written::Complete {correlation:value,..} if value==correlation(1))
        );
        assert!(connection.written.try_recv().is_err());
        connection.close();
        finish(&mut connection);
    }

    #[test]
    fn fragmented_and_concatenated_packets_keep_their_boundaries() {
        let (mut connection, mut peer) = pair();
        peer.write_all(&[0xc0]).unwrap();
        assert!(connection.incoming.try_recv().is_err());
        peer.write_all(&[0, 0xc0, 0, 0xc0, 0]).unwrap();
        for _ in 0..3 {
            assert!(
                matches!(incoming(&connection), Incoming::Packet { first: 0xc0, body,.. } if body.is_empty())
            );
        }
        connection.close();
        finish(&mut connection);
    }

    #[test]
    fn oversized_header_is_refused_without_waiting_for_its_body() {
        let (mut connection, mut peer) = pair();
        peer.write_all(&[0x34, 0xff, 0xff, 0x7f]).unwrap();
        refused(&connection, "packet_too_large");
        // The write side stays: the runtime can name the cause, and what
        // the peer still sends is dropped, not framed.
        peer.write_all(&[0xc0, 0]).unwrap();
        connection
            .outgoing
            .try_send(Write {
                correlation: None,
                bytes: FrameBytes::from_slice(&[0xe0, 2, 0x95, 0]),
                payload: None,
                expiry: None,
                report: WriteReport::Complete,
            })
            .unwrap();
        let mut answer = [0u8; 4];
        peer.read_exact(&mut answer).unwrap();
        assert_eq!(answer, [0xe0, 2, 0x95, 0]);
        assert!(connection.incoming.try_recv().is_err());
        // The peer closing first keeps the cause.
        drop(peer);
        closed(&connection, "packet_too_large");
        finish(&mut connection);
    }

    #[test]
    fn eof_in_body_is_distinct_from_clean_eof() {
        let (mut connection, mut peer) = pair();
        peer.write_all(&[0x34, 3, 0]).unwrap();
        peer.shutdown(Shutdown::Write).unwrap();
        closed(&connection, "truncated_packet");
        finish(&mut connection);
        let (mut connection, peer) = pair();
        peer.shutdown(Shutdown::Write).unwrap();
        closed(&connection, "eof");
        finish(&mut connection);
    }

    #[test]
    fn output_is_confirmed_only_after_all_bytes_are_written() {
        let (mut connection, mut peer) = pair();
        connection
            .outgoing
            .send(Write {
                correlation: None,
                bytes: FrameBytes::from_slice(&[0xd0, 0]),
                payload: None,
                expiry: None,
                report: WriteReport::Complete,
            })
            .unwrap();
        let result = connection
            .written
            .recv_timeout(Duration::from_millis(600))
            .unwrap();
        assert!(matches!(
            result,
            Written::Complete {
                correlation: None,
                bytes: 2
            }
        ));
        let mut bytes = [0; 2];
        peer.set_read_timeout(Some(Duration::from_millis(600)))
            .unwrap();
        peer.read_exact(&mut bytes).unwrap();
        assert_eq!(bytes, [0xd0, 0]);
        connection.close();
        finish(&mut connection);
    }

    #[test]
    fn nonreading_peer_expires_one_absolute_write_budget() {
        let (mut connection, peer) = pair();
        socket2::SockRef::from(&peer)
            .set_recv_buffer_size(4096)
            .unwrap();
        let started = Instant::now();
        for _ in 0..OUTPUT_SLOTS {
            connection
                .outgoing
                .send(Write {
                    correlation: None,
                    bytes: FrameBytes::from(vec![0; 65_536]),
                    payload: None,
                    expiry: None,
                    report: WriteReport::Complete,
                })
                .unwrap();
        }
        let mut failed = false;
        for _ in 0..OUTPUT_SLOTS {
            match connection
                .written
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
            {
                Written::Failed { code, .. } => {
                    assert_eq!(code, "write_timeout");
                    failed = true;
                    break;
                }
                Written::Complete { .. } => {}
                other => panic!("unexpected result: {other:?}"),
            }
        }
        assert!(failed, "test did not reach socket backpressure");
        assert!(started.elapsed() < Duration::from_secs(1));
        finish(&mut connection);
    }

    #[test]
    fn cancellation_ends_idle_and_partial_socket_tasks() {
        for partial in [false, true] {
            let (mut connection, mut peer) = pair();
            if partial {
                peer.write_all(&[0x34, 127, 0]).unwrap();
            }
            connection.close();
            finish(&mut connection);
        }
    }

    #[test]
    fn noncanonical_header_is_a_malformed_packet_not_clean_eof() {
        let (mut connection, mut peer) = pair();
        peer.write_all(&[0xc0, 0x80, 0]).unwrap();
        refused(&connection, "malformed_header");
        connection.close();
        finish(&mut connection);
    }

    /// A core that takes nothing slows the peer through its transport. The
    /// lane holds its packets, one more is held by the task, the rest stay
    /// in the socket, and all of them arrive in order once the core takes
    /// values again (record 0046).
    #[test]
    fn input_saturation_is_backpressure_and_loses_no_packet() {
        let (mut connection, mut peer) = pair();
        let packets = INPUT_SLOTS + 6;
        for index in 0..packets {
            peer.write_all(&[0x30, 4, 0, 1, b't', u8::try_from(index).unwrap()])
                .unwrap();
        }
        // Longer than any wait the task once bounded before closing.
        thread::sleep(Duration::from_millis(400));
        assert!(!connection.is_finished(), "a slow core closed its peer");
        assert_eq!(connection.terminal_reason(), None);
        for index in 0..packets {
            let deadline = Instant::now() + Duration::from_millis(600);
            let packet = loop {
                match connection.incoming.try_recv() {
                    Ok(packet) => break packet,
                    Err(_) => assert!(Instant::now() < deadline, "packet {index} never arrived"),
                }
                thread::sleep(Duration::from_millis(1));
            };
            assert!(
                matches!(packet, Incoming::Packet { first: 0x30, ref body, .. } if body.last() == Some(&u8::try_from(index).unwrap())),
                "packet {index} out of order: {packet:?}"
            );
        }
        connection.close();
        finish(&mut connection);
    }

    #[test]
    fn cancellation_reports_every_accepted_output_frame() {
        let (mut connection, _peer) = pair();
        for _ in 0..OUTPUT_SLOTS {
            connection
                .outgoing
                .send(Write {
                    correlation: None,
                    bytes: FrameBytes::from(vec![0; 65_536]),
                    payload: None,
                    expiry: None,
                    report: WriteReport::Complete,
                })
                .unwrap();
        }
        connection.close();
        finish(&mut connection);
        assert_eq!(connection.written.try_iter().count(), OUTPUT_SLOTS);
    }

    #[test]
    fn partial_packet_deadline_is_not_reset_by_more_bytes() {
        let (mut connection, mut peer) = pair();
        peer.write_all(&[0x34, 127]).unwrap();
        // A later byte does not create a fresh five-second packet budget.
        thread::sleep(Duration::from_secs(3));
        peer.write_all(&[0]).unwrap();
        let start = Instant::now();
        match connection
            .incoming
            .recv_timeout(Duration::from_millis(2800))
            .unwrap()
        {
            Incoming::Closed { code } => assert_eq!(code, "partial_packet_timeout"),
            packet => panic!("incomplete packet was accepted: {packet:?}"),
        }
        assert!(start.elapsed() < Duration::from_millis(2800));
        finish(&mut connection);
    }
}
