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
#[cfg(test)]
use std::sync::mpsc::{Receiver, RecvTimeoutError, SendError, SyncSender};
use std::sync::mpsc::{TryRecvError, TrySendError};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use mqttiumd_contract::ids::{ConnectionKey, EffectId};
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
pub const OUTPUT_SLOTS: usize = 64;
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
/// Empty lazy queues shrink back to zero heap bytes when their capacity exceeded this threshold.
const LANE_SHRINK_THRESHOLD: usize = 8;

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
/// short PUBLISH headers <= 32 B), eliminating per-frame heap allocations while
/// spilling transparently to `Vec<u8>` for larger frames or coalesced batches.
pub const INLINE_FRAME_CAP: usize = 32;

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
    pub expiry: Option<WriteExpiry>,
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
    fn flatten_into_bytes(&mut self) {
        if let Some(payload) = self.payload.take() {
            self.bytes.extend_from_slice(payload.as_slice());
        }
    }

    #[inline]
    fn append_to(&self, out: &mut FrameBytes) {
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

/// Lazy bounded single-producer/single-consumer queue with a Redis-style
/// inline single-item slot.
///
/// Unlike `std::sync::mpsc::sync_channel(N)`, idle and low-rate connections
/// (CONNECT/CONNACK/keep-alive) transfer single items through `inline` with
/// zero heap allocation; `overflow` allocates lazily only when a second item
/// arrives before the first is consumed and shrinks back to zero after bursts.
struct LaneState<T> {
    inline: Option<T>,
    overflow: VecDeque<T>,
    sender_alive: bool,
    receiver_alive: bool,
}

impl<T> LaneState<T> {
    #[inline]
    fn len(&self) -> usize {
        usize::from(self.inline.is_some()) + self.overflow.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.inline.is_none() && self.overflow.is_empty()
    }

    #[inline]
    fn push_back(&mut self, value: T) {
        if self.inline.is_none() && self.overflow.is_empty() {
            self.inline = Some(value);
        } else {
            self.overflow.push_back(value);
        }
    }

    #[inline]
    fn pop_front(&mut self) -> Option<T> {
        let value = self.inline.take()?;
        if !self.overflow.is_empty() {
            self.inline = self.overflow.pop_front();
            if self.overflow.is_empty() && self.overflow.capacity() > LANE_SHRINK_THRESHOLD {
                self.overflow = VecDeque::new();
            }
        }
        Some(value)
    }
}

struct LaneInner<T> {
    cap: usize,
    pending: AtomicBool,
    state: Mutex<LaneState<T>>,
    #[cfg(test)]
    changed: Condvar,
}

struct LaneTx<T> {
    inner: Arc<LaneInner<T>>,
    control: Option<Control>,
}

impl<T> LaneTx<T> {
    fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        let mut state = match self.inner.state.lock() {
            Ok(guard) => guard,
            Err(_) => return Err(TrySendError::Disconnected(value)),
        };
        if !state.receiver_alive {
            return Err(TrySendError::Disconnected(value));
        }
        if state.len() >= self.inner.cap {
            return Err(TrySendError::Full(value));
        }
        state.push_back(value);
        self.inner.pending.store(true, Ordering::Release);
        drop(state);
        #[cfg(test)]
        self.inner.changed.notify_one();
        if let Some(control) = &self.control {
            control.mark_active();
        }
        Ok(())
    }

    #[cfg(test)]
    fn send(&self, value: T) -> Result<(), SendError<T>> {
        let mut state = match self.inner.state.lock() {
            Ok(guard) => guard,
            Err(_) => return Err(SendError(value)),
        };
        while state.receiver_alive && state.len() >= self.inner.cap {
            state = match self.inner.changed.wait(state) {
                Ok(guard) => guard,
                Err(_) => return Err(SendError(value)),
            };
        }
        if !state.receiver_alive {
            return Err(SendError(value));
        }
        state.push_back(value);
        self.inner.pending.store(true, Ordering::Release);
        drop(state);
        self.inner.changed.notify_one();
        if let Some(control) = &self.control {
            control.mark_active();
        }
        Ok(())
    }
}

impl<T> Drop for LaneTx<T> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.inner.state.lock() {
            state.sender_alive = false;
            self.inner.pending.store(true, Ordering::Release);
        }
        #[cfg(test)]
        self.inner.changed.notify_all();
        if let Some(control) = &self.control {
            control.mark_active();
        }
    }
}

struct LaneRx<T> {
    inner: Arc<LaneInner<T>>,
}

impl<T> LaneRx<T> {
    #[inline]
    fn has_pending(&self) -> bool {
        self.inner.pending.load(Ordering::Acquire)
    }

    fn try_recv(&self) -> Result<T, TryRecvError> {
        let mut state = match self.inner.state.lock() {
            Ok(guard) => guard,
            Err(_) => return Err(TryRecvError::Disconnected),
        };
        if let Some(value) = state.pop_front() {
            self.inner
                .pending
                .store(!state.is_empty() || !state.sender_alive, Ordering::Release);
            drop(state);
            #[cfg(test)]
            self.inner.changed.notify_one();
            return Ok(value);
        }
        if !state.sender_alive {
            self.inner.pending.store(true, Ordering::Release);
            Err(TryRecvError::Disconnected)
        } else {
            self.inner.pending.store(false, Ordering::Release);
            Err(TryRecvError::Empty)
        }
    }

    #[cfg(test)]
    fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        let deadline = Instant::now().checked_add(timeout);
        let mut state = match self.inner.state.lock() {
            Ok(guard) => guard,
            Err(_) => return Err(RecvTimeoutError::Disconnected),
        };
        loop {
            if let Some(value) = state.pop_front() {
                self.inner
                    .pending
                    .store(!state.is_empty() || !state.sender_alive, Ordering::Release);
                drop(state);
                self.inner.changed.notify_one();
                return Ok(value);
            }
            if !state.sender_alive {
                self.inner.pending.store(true, Ordering::Release);
                return Err(RecvTimeoutError::Disconnected);
            }
            self.inner.pending.store(false, Ordering::Release);
            let remaining = match deadline {
                Some(deadline) => deadline.saturating_duration_since(Instant::now()),
                None => timeout,
            };
            if remaining.is_zero() {
                return Err(RecvTimeoutError::Timeout);
            }
            state = match self.inner.changed.wait_timeout(state, remaining) {
                Ok((guard, _)) => guard,
                Err(_) => return Err(RecvTimeoutError::Disconnected),
            };
        }
    }

    fn try_iter(&self) -> impl Iterator<Item = T> + '_ {
        std::iter::from_fn(|| self.try_recv().ok())
    }
}

impl<T> Drop for LaneRx<T> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.inner.state.lock() {
            state.receiver_alive = false;
        }
        #[cfg(test)]
        self.inner.changed.notify_all();
    }
}

fn lazy_lane<T>(cap: usize, control: Option<Control>) -> (LaneTx<T>, LaneRx<T>) {
    let inner = Arc::new(LaneInner {
        cap,
        pending: AtomicBool::new(false),
        state: Mutex::new(LaneState {
            inline: None,
            overflow: VecDeque::new(),
            sender_alive: true,
            receiver_alive: true,
        }),
        #[cfg(test)]
        changed: Condvar::new(),
    });
    (
        LaneTx {
            inner: inner.clone(),
            control,
        },
        LaneRx { inner },
    )
}

/// The core's end of a lane a task publishes into. Taking a value tells
/// the reactor when the task had found the lane full.
pub struct Inbound<T> {
    receiver: InboundRx<T>,
    link: Option<Arc<Link>>,
}

enum InboundRx<T> {
    Lazy(LaneRx<T>),
    #[cfg(test)]
    Std(Receiver<T>),
}

impl<T> Inbound<T> {
    #[inline]
    pub fn has_pending(&self) -> bool {
        match &self.receiver {
            InboundRx::Lazy(rx) => rx.has_pending(),
            #[cfg(test)]
            InboundRx::Std(_) => true,
        }
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let value = match &self.receiver {
            InboundRx::Lazy(rx) => rx.try_recv(),
            #[cfg(test)]
            InboundRx::Std(rx) => rx.try_recv(),
        };
        if value.is_ok()
            && let Some(link) = &self.link
        {
            link.consumed();
        }
        value
    }

    #[cfg(test)]
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        let value = match &self.receiver {
            InboundRx::Lazy(rx) => rx.recv_timeout(timeout),
            InboundRx::Std(rx) => rx.recv_timeout(timeout),
        };
        if value.is_ok()
            && let Some(link) = &self.link
        {
            link.consumed();
        }
        value
    }

    #[cfg(test)]
    pub fn try_iter(&self) -> impl Iterator<Item = T> + '_ {
        std::iter::from_fn(|| self.try_recv().ok())
    }
}

#[cfg(test)]
impl<T> From<Receiver<T>> for Inbound<T> {
    fn from(receiver: Receiver<T>) -> Self {
        Self {
            receiver: InboundRx::Std(receiver),
            link: None,
        }
    }
}

/// The core's end of the frame lane. A published frame runs the task.
pub struct Outbound {
    sender: OutboundTx,
    link: Option<Arc<Link>>,
}

enum OutboundTx {
    Lazy(LaneTx<Write>),
    #[cfg(test)]
    Std(SyncSender<Write>),
}

// Both methods keep the contract of the standard sender: a refused frame
// comes back intact inside the error.
#[expect(clippy::result_large_err, reason = "the refused frame is handed back")]
impl Outbound {
    pub fn try_send(&self, frame: Write) -> Result<(), TrySendError<Write>> {
        let result = match &self.sender {
            OutboundTx::Lazy(tx) => tx.try_send(frame),
            #[cfg(test)]
            OutboundTx::Std(tx) => tx.try_send(frame),
        };
        if result.is_ok()
            && let Some(link) = &self.link
        {
            link.notify();
        }
        result
    }

    #[cfg(test)]
    pub fn send(&self, frame: Write) -> Result<(), SendError<Write>> {
        let result = match &self.sender {
            OutboundTx::Lazy(tx) => tx.send(frame),
            OutboundTx::Std(tx) => tx.send(frame),
        };
        if result.is_ok()
            && let Some(link) = &self.link
        {
            link.notify();
        }
        result
    }
}

#[cfg(test)]
impl From<SyncSender<Write>> for Outbound {
    fn from(sender: SyncSender<Write>) -> Self {
        Self {
            sender: OutboundTx::Std(sender),
            link: None,
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

struct ControlInner {
    cancelled: AtomicBool,
    /// Set whenever a reactor task publishes to any core-bound lane or records
    /// a terminal state, letting the core skip idle peers in O(1).
    activity: AtomicBool,
    ready: Option<(Arc<ReadyQueue>, u64)>,
    terminal: Mutex<Option<Terminal>>,
    wake: Wake,
    /// When the task framed its last complete packet: nanoseconds since
    /// the process epoch, plus one; zero while it has framed none.
    heard: AtomicU64,
    #[cfg(test)]
    pause_progress: AtomicBool,
    #[cfg(test)]
    progress_waiting: AtomicBool,
}

#[derive(Clone)]
struct Control(Arc<ControlInner>);

#[derive(Clone, Copy)]
struct Terminal {
    code: &'static str,
    observed_ns: Option<u64>,
}

impl Control {
    fn new(wake: Wake, ready: Option<(Arc<ReadyQueue>, u64)>) -> Self {
        let initial_activity = ready.is_none();
        Self(Arc::new(ControlInner {
            cancelled: AtomicBool::new(false),
            activity: AtomicBool::new(initial_activity),
            ready,
            terminal: Mutex::new(None),
            wake,
            heard: AtomicU64::new(0),
            #[cfg(test)]
            pause_progress: AtomicBool::new(false),
            #[cfg(test)]
            progress_waiting: AtomicBool::new(false),
        }))
    }

    fn is_cancelled(&self) -> bool {
        self.0.cancelled.load(Ordering::Acquire)
    }

    fn mark_active(&self) {
        if !self.0.activity.swap(true, Ordering::AcqRel)
            && let Some((ready, id)) = &self.0.ready
        {
            ready.push(*id);
        }
        self.0.wake.wake();
    }

    fn record(&self, code: &'static str) {
        if let Ok(mut terminal) = self.0.terminal.lock() {
            terminal.get_or_insert_with(|| Terminal {
                code,
                observed_ns: crate::events::monotonic_ns(),
            });
        }
        // The terminal reason is read by the core outside any lane.
        self.mark_active();
    }

    /// The socket is shut before the reason is recorded: once a reader of
    /// the reason sees it, no byte can leave any more. The flag alone
    /// would not stop a write that already passed its check.
    fn stop(&self, stream: &TcpStream, code: &'static str) {
        self.0.cancelled.store(true, Ordering::Release);
        let _ = stream.shutdown(Shutdown::Both);
        self.record(code);
    }
}

/// The owner polls these bounded queues. Closing cancels the task
/// immediately; the owner then observes its end with `join_finished`,
/// without waiting on a socket in its turn. The terminal reason is kept
/// outside the lanes, so it is readable when they are full.
pub struct Connection {
    pub incoming: Inbound<Incoming>,
    pub outgoing: Outbound,
    pub written: Inbound<Written>,
    pub wire_progress: Inbound<WireProgress>,
    stream: Arc<TcpStream>,
    control: Control,
    link: Arc<Link>,
    send_buffer: usize,
    principal: Option<Arc<OnceLock<Principal>>>,
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

/// The lanes of one connection and the socket settings both transports share.
fn open(
    stream: TcpStream,
    wake: Wake,
    ready: Option<(Arc<ReadyQueue>, u64)>,
) -> io::Result<(Arc<TcpStream>, Lanes, Connection)> {
    stream.set_nodelay(true)?;
    stream.set_nonblocking(true)?;
    let socket = socket2::SockRef::from(&stream);
    let send_buffer = socket.send_buffer_size()?;
    let stream = Arc::new(stream);
    let link = Reactors::shared()?.link(wake.clone());
    let control = Control::new(wake, ready);
    let (input, incoming) = lazy_lane(INPUT_SLOTS, Some(control.clone()));
    let (outgoing, output) = lazy_lane(OUTPUT_SLOTS, None);
    let (written_tx, written) = lazy_lane(WRITTEN_SLOTS, Some(control.clone()));
    let (progress, wire_progress) = lazy_lane(WIRE_PROGRESS_SLOTS, Some(control.clone()));
    let connection = Connection {
        incoming: Inbound {
            receiver: InboundRx::Lazy(incoming),
            link: Some(link.clone()),
        },
        outgoing: Outbound {
            sender: OutboundTx::Lazy(outgoing),
            link: Some(link.clone()),
        },
        written: Inbound {
            receiver: InboundRx::Lazy(written),
            link: Some(link.clone()),
        },
        wire_progress: Inbound {
            receiver: InboundRx::Lazy(wire_progress),
            link: Some(link.clone()),
        },
        stream: stream.clone(),
        control: control.clone(),
        link: link.clone(),
        send_buffer,
        principal: None,
    };
    let lanes = Lanes {
        input,
        output,
        peeked: None,
        written: written_tx,
        progress,
        link,
        control,
        held: None,
        reports: VecDeque::new(),
        stalled: None,
    };
    Ok((stream, lanes, connection))
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
    let (stream, lanes, connection) = open(stream, wake, ready)?;
    let task = Task {
        source: Buffered {
            stream,
            leftover: Vec::new(),
            start: 0,
        },
        parser: Parser::default(),
        clock,
        control: connection.control.clone(),
        lanes,
        active: None,
        #[cfg(test)]
        paused: None,
    };
    Reactors::shared()?.register(&connection.link, Box::new(task));
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
    let (stream, lanes, mut connection) = open(stream, wake, ready)?;
    let principal = Arc::new(OnceLock::new());
    let state = rustls::ServerConnection::new(config).map_err(io::Error::other)?;
    let mut task = tls::Task {
        stream,
        state,
        clock,
        control: connection.control.clone(),
        principal: principal.clone(),
        policy,
        lanes,
        machine: tls::Machine::new(),
        #[cfg(feature = "test-hooks")]
        trace,
    };
    task.state
        .set_buffer_limit(Some(crate::security::TLS_BUFFER_BYTES));
    connection.principal = Some(principal);
    Reactors::shared()?.register(&connection.link, Box::new(task));
    Ok(connection)
}

impl Connection {
    pub fn is_tls(&self) -> bool {
        self.principal.is_some()
    }

    pub fn is_authenticating(&self) -> bool {
        self.principal
            .as_ref()
            .is_some_and(|slot| slot.get().is_none())
    }

    pub fn principal(&self) -> Option<Principal> {
        self.principal.as_ref()?.get().copied()
    }

    /// Consumes and reports whether the transport task published any new
    /// incoming packet, write receipt, wire progress, or terminal state since
    /// the last check.
    #[inline]
    pub fn take_activity(&self) -> bool {
        let active = self.control.0.activity.swap(false, Ordering::AcqRel);
        active || self.has_pending()
    }

    #[cfg(test)]
    pub(crate) fn uses_std_channel(&self) -> bool {
        matches!(self.incoming.receiver, InboundRx::Std(_))
            || matches!(self.written.receiver, InboundRx::Std(_))
            || matches!(self.wire_progress.receiver, InboundRx::Std(_))
    }

    #[inline]
    pub fn has_pending(&self) -> bool {
        self.incoming.has_pending()
            || self.written.has_pending()
            || self.wire_progress.has_pending()
    }

    /// Re-arms the activity flag when the core could not drain a lane fully
    /// (for example due to ordinary-input backpressure).
    #[inline]
    pub fn mark_activity(&self) {
        if !self.control.0.activity.swap(true, Ordering::AcqRel)
            && let Some((ready, id)) = &self.control.0.ready
        {
            ready.push(*id);
        }
    }

    #[cfg(test)]
    pub fn pause_wire_progress(&self, pause: bool) {
        self.control
            .0
            .pause_progress
            .store(pause, Ordering::Release);
    }

    #[cfg(test)]
    pub fn wire_progress_is_waiting(&self) -> bool {
        self.control.0.progress_waiting.load(Ordering::Acquire)
    }

    /// When the transport received its last complete packet, whether or
    /// not the core has taken it. Keep Alive runs from this instant: what
    /// the peer sent counts when the broker hears it, and a packet the
    /// broker holds does not stop the clock.
    pub fn last_heard(&self) -> Option<Instant> {
        match self.control.0.heard.load(Ordering::Relaxed) {
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
            self.control.0.cancelled.store(true, Ordering::Release);
        } else {
            self.control.stop(&self.stream, "cancelled");
        }
        self.link.notify();
    }

    /// The task published its terminal facts and left its reactor.
    pub fn is_finished(&self) -> bool {
        self.link.is_finished()
    }

    /// Returns false while the task runs. Nothing is left to reap after it.
    pub fn join_finished(&mut self) -> bool {
        self.is_finished()
    }

    pub fn terminal_reason(&self) -> Option<&'static str> {
        self.control
            .0
            .terminal
            .lock()
            .ok()
            .and_then(|reason| *reason)
            .map(|end| end.code)
    }

    pub fn terminal_observed_ns(&self) -> Option<u64> {
        self.control
            .0
            .terminal
            .lock()
            .ok()
            .and_then(|reason| *reason)?
            .observed_ns
    }

    pub fn send_buffer_bytes(&self) -> usize {
        self.send_buffer
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.close();
    }
}

/// One instant all the transports of the process count from.
fn epoch() -> Instant {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    *EPOCH.get_or_init(Instant::now)
}

fn retryable(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut | io::ErrorKind::Interrupted
    )
}

enum Report {
    Written(Written),
    Progress(WireProgress),
}

/// The task's end of the four lanes. Nothing here blocks: a value a full
/// lane refused is held, and the task is run again when the core takes a
/// value or at the next tick. A held packet has no deadline; a held
/// receipt has one, counted from its first refusal.
struct Lanes {
    input: LaneTx<Incoming>,
    output: LaneRx<Write>,
    /// One frame peeked during opportunistic coalescing that could not join the batch.
    peeked: Option<Write>,
    written: LaneTx<Written>,
    progress: LaneTx<WireProgress>,
    link: Arc<Link>,
    control: Control,
    held: Option<Incoming>,
    /// Receipts in publication order.
    reports: VecDeque<Report>,
    stalled: Option<Instant>,
}

/// Offers a value to a lane without waiting. A refused value comes back;
/// `Err` means the consumer is gone. The consumer that takes a value after
/// the mark runs the task; the second attempt covers one that took it just
/// before.
fn publish<T>(lane: &LaneTx<T>, link: &Link, mut value: T) -> Result<Option<T>, ()> {
    for attempt in 0..2 {
        match lane.try_send(value) {
            Ok(()) => return Ok(None),
            Err(TrySendError::Disconnected(_)) => return Err(()),
            Err(TrySendError::Full(refused)) => {
                value = refused;
                if attempt == 0 {
                    link.owe();
                }
            }
        }
    }
    Ok(Some(value))
}

impl Lanes {
    /// Holds a complete packet for the input lane. The caller delivers the
    /// previous one first.
    fn hold(&mut self, incoming: Incoming) {
        debug_assert!(self.held.is_none());
        // A refused header is not a packet the peer was heard to send.
        if matches!(incoming, Incoming::Packet { .. }) {
            let since = u64::try_from(epoch().elapsed().as_nanos()).unwrap_or(u64::MAX - 1);
            self.control.0.heard.store(since + 1, Ordering::Relaxed);
        }
        self.held = Some(incoming);
    }

    /// Publishes the held packet. Returns whether the lane is free again.
    fn deliver(&mut self) -> Result<bool, &'static str> {
        let Some(incoming) = self.held.take() else {
            return Ok(true);
        };
        let Some(refused) =
            publish(&self.input, &self.link, incoming).map_err(|()| "input_consumer_closed")?
        else {
            return Ok(true);
        };
        // The packet waits for the core, and the bytes behind it wait in the
        // socket: the peer is slowed by its transport, not disconnected
        // (record 0046). The runtime owns every deadline of a connection.
        self.held = Some(refused);
        Ok(false)
    }

    fn report(&mut self, result: Written) {
        self.reports.push_back(Report::Written(result));
    }

    fn wire_progress(&mut self, progress: WireProgress) {
        self.reports.push_back(Report::Progress(progress));
    }

    /// Publishes pending receipts in order. Returns whether one left.
    fn flush(&mut self) -> Result<bool, &'static str> {
        let mut moved = false;
        while let Some(report) = self.reports.pop_front() {
            let (refused, code) = match report {
                Report::Written(result) => (
                    publish(&self.written, &self.link, result)
                        .map_err(|()| "write_result_queue_full")?
                        .map(Report::Written),
                    "write_result_queue_full",
                ),
                Report::Progress(progress) => (
                    publish(&self.progress, &self.link, progress)
                        .map_err(|()| "wire_progress_unavailable")?
                        .map(Report::Progress),
                    "wire_progress_unavailable",
                ),
            };
            let Some(refused) = refused else {
                moved = true;
                self.stalled = None;
                continue;
            };
            self.reports.push_front(refused);
            if self.stalled.get_or_insert_with(Instant::now).elapsed() >= RESULT_WAIT {
                return Err(code);
            }
            break;
        }
        Ok(moved)
    }

    /// The next frame to write, once every earlier receipt was published.
    fn next_frame(&mut self) -> Result<Option<Write>, &'static str> {
        if !self.reports.is_empty() {
            return Ok(None);
        }
        if let Some(frame) = self.peeked.take() {
            return Ok(Some(frame));
        }
        match self.output.try_recv() {
            Ok(frame) => Ok(Some(frame)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err("output_producer_closed"),
        }
    }

    /// Opportunistically pops a queued frame for write coalescing without
    /// treating a disconnected sender as an error before already-taken frames finish.
    fn pop_coalesce_candidate(&mut self) -> Option<Write> {
        if let Some(frame) = self.peeked.take() {
            return Some(frame);
        }
        self.output.try_recv().ok()
    }

    /// A value waits for room in a lane: the task holds a deadline.
    fn pending(&self) -> bool {
        self.held.is_some() || !self.reports.is_empty()
    }

    /// Offers the terminal facts once, without waiting: the pending
    /// receipts, then a failed receipt for the frame(s) in progress and for
    /// every accepted frame left in the bounded queue. A lane that is full
    /// refuses them, and a frame the core publishes after this pass gets
    /// none: the core gives every frame of a retired transport its one
    /// result when it closes the connection. A failed receipt says the
    /// bytes are not known complete, never that they were rejected.
    fn finish(
        mut self,
        code: &'static str,
        active: impl IntoIterator<Item = Option<(EffectId, ConnectionKey)>>,
    ) {
        for report in self.reports.drain(..) {
            let published = match report {
                Report::Written(result) => self.written.try_send(result).is_ok(),
                Report::Progress(progress) => self.progress.try_send(progress).is_ok(),
            };
            if !published {
                break;
            }
        }
        for correlation in active
            .into_iter()
            .chain(self.peeked.into_iter().map(|frame| frame.correlation))
            .chain(self.output.try_iter().map(|frame| frame.correlation))
        {
            if self
                .written
                .try_send(Written::Failed { correlation, code })
                .is_err()
            {
                break;
            }
        }
        let _ = self.input.try_send(Incoming::Closed { code });
    }
}

/// Where a parser takes its bytes. Zero means none is available now; the
/// end of the stream is an error named by the source.
trait Source {
    fn pull(&mut self, into: &mut [u8], partial: bool) -> Result<usize, &'static str>;
}

/// A plain socket with zero-allocation idle buffering: reads go into the
/// reactor thread's reusable scratch buffer, and only unread partial-packet
/// tail bytes are stashed in `leftover`.
struct Buffered {
    stream: Arc<TcpStream>,
    leftover: Vec<u8>,
    start: usize,
}

struct ScratchSource<'a> {
    buffered: &'a mut Buffered,
    scratch: &'a mut [u8],
    scratch_start: usize,
    scratch_end: usize,
}

impl Drop for ScratchSource<'_> {
    fn drop(&mut self) {
        let rem = self.scratch_end - self.scratch_start;
        if rem > 0 {
            debug_assert_eq!(self.buffered.start, self.buffered.leftover.len());
            self.buffered.leftover.clear();
            self.buffered
                .leftover
                .extend_from_slice(&self.scratch[self.scratch_start..self.scratch_end]);
            self.buffered.start = 0;
        } else if !self.buffered.leftover.is_empty()
            && self.buffered.start == self.buffered.leftover.len()
        {
            self.buffered.leftover = Vec::new();
            self.buffered.start = 0;
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
        if self.buffered.start < self.buffered.leftover.len() {
            let avail = self.buffered.leftover.len() - self.buffered.start;
            let count = into.len().min(avail);
            into[..count].copy_from_slice(
                &self.buffered.leftover[self.buffered.start..self.buffered.start + count],
            );
            self.buffered.start += count;
            if self.buffered.start == self.buffered.leftover.len() {
                self.buffered.leftover = Vec::new();
                self.buffered.start = 0;
            }
            return Ok(count);
        }
        if self.scratch_start == self.scratch_end {
            // A large body is read straight into its packet.
            if into.len() >= self.scratch.len() {
                return read_some(&self.buffered.stream, into, partial);
            }
            self.scratch_start = 0;
            self.scratch_end = read_some(&self.buffered.stream, self.scratch, partial)?;
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

/// Incremental MQTT framing within the current packet's absolute budget.
/// An idle connection has no packet deadline: the runtime separately owns
/// CONNECT and Keep Alive.
#[derive(Default)]
struct Parser {
    prefix: [u8; 5],
    prefix_len: usize,
    header: Option<codec::Header>,
    body: FrameBytes,
    offset: usize,
    since: Option<Instant>,
    // One watermark per reader detects a bad sample even if the provider has
    // recovered before the runtime dequeues this frame. A bad sample never
    // replaces the last valid observation or becomes evidence of expiration.
    previous: Option<MessageTime>,
    /// Bytes taken from the source, modulo the counter width.
    consumed: usize,
    /// The cause a refused fixed header gave. From then on the input is
    /// read and dropped: the peer is not blocked in its own writes while
    /// the reason travels to it, and the connection ends with this cause
    /// whichever side closes first.
    refused: Option<&'static str>,
}

impl Parser {
    fn is_partial(&self) -> bool {
        self.since.is_some()
    }

    /// Ends framing: no packet has a deadline any more, the runtime owns
    /// the one of the closing connection.
    fn refuse(&mut self, code: &'static str) -> Incoming {
        self.refused = Some(code);
        self.since = None;
        Incoming::Refused { code }
    }

    fn packet(
        &mut self,
        source: &mut impl Source,
        clock: &dyn SessionClock,
    ) -> Result<Option<Incoming>, &'static str> {
        if let Some(code) = self.refused {
            let mut dropped = [0u8; 512];
            let count = source.pull(&mut dropped, false).map_err(|_| code)?;
            self.consumed = self.consumed.wrapping_add(count);
            return Ok(None);
        }
        if self
            .since
            .is_some_and(|start| start.elapsed() >= PARTIAL_PACKET_TIMEOUT)
        {
            return Err("partial_packet_timeout");
        }
        while self.header.is_none() {
            if self.prefix_len == self.prefix.len() {
                return Ok(Some(self.refuse("malformed_header")));
            }
            let next = &mut self.prefix[self.prefix_len..self.prefix_len + 1];
            let partial = self.prefix_len > 0 || self.since.is_some();
            if source.pull(next, partial)? == 0 {
                if self.prefix_len > 0 {
                    self.since.get_or_insert_with(Instant::now);
                }
                return Ok(None);
            }
            self.consumed = self.consumed.wrapping_add(1);
            self.prefix_len += 1;
            match codec::header(&self.prefix[..self.prefix_len], MAX_PACKET_BYTES) {
                Ok(header) => self.header = header,
                Err(error) if error.kind() == ErrorKind::Oversized => {
                    return Ok(Some(self.refuse("packet_too_large")));
                }
                Err(_) => return Ok(Some(self.refuse("malformed_header"))),
            }
        }
        let header = self.header.expect("header checked");
        if self.body.len() != header.remaining {
            self.body =
                FrameBytes::try_zeroed(header.remaining).map_err(|_| "allocation_failed")?;
        }
        while self.offset < self.body.len() {
            let count = source.pull(&mut self.body[self.offset..], true)?;
            if count == 0 {
                self.since.get_or_insert_with(Instant::now);
                return Ok(None);
            }
            self.consumed = self.consumed.wrapping_add(count);
            self.offset += count;
        }
        if self
            .since
            .is_some_and(|start| start.elapsed() >= PARTIAL_PACKET_TIMEOUT)
        {
            return Err("partial_packet_timeout");
        }
        self.prefix_len = 0;
        self.header = None;
        self.offset = 0;
        self.since = None;
        Ok(Some(Incoming::Packet {
            first: header.first,
            body: std::mem::take(&mut self.body),
            observed: frame_observation(clock, &mut self.previous),
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
/// lane and its one absolute write budget.
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
    control: &Control,
    clock: &dyn SessionClock,
    write: &mut impl FnMut(&[u8], &[u8], Duration) -> io::Result<usize>,
    started: &mut impl FnMut() -> bool,
) -> Result<Attempt, WriteFailure> {
    let total = active.frame.total_len();
    if total > MAX_PACKET_BYTES {
        return Err(WriteFailure::Failed("output_packet_too_large"));
    }
    if active.offset < total {
        if control.is_cancelled() {
            return Err(WriteFailure::Failed("cancelled"));
        }
        let remaining = active.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(WriteFailure::Failed("write_timeout"));
        }
        if active.offset == 0
            && let Some(expiry) = active.frame.expiry
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
    control: &Control,
    clock: &dyn SessionClock,
    mut write: impl FnMut(&[u8], Duration) -> io::Result<usize>,
    mut started: impl FnMut() -> bool,
) -> Result<(), WriteFailure> {
    let mut active = Active::new(Write {
        correlation: None,
        bytes: FrameBytes::from_slice(bytes),
        payload: None,
        expiry,
        report: WriteReport::CompleteAndWireProgress,
    });
    let result = loop {
        let mut bounded =
            |part: &[u8], _tail: &[u8], remaining: Duration| write(part, remaining.min(POLL));
        match write_attempt(&mut active, control, clock, &mut bounded, &mut started) {
            Ok(Attempt::Done) => break Ok(()),
            Ok(_) => {}
            Err(failure) => break Err(failure),
        }
    };
    bytes.copy_from_slice(&active.frame.bytes);
    result
}

/// One plain TCP connection.
struct Task {
    source: Buffered,
    parser: Parser,
    clock: Arc<dyn SessionClock>,
    control: Control,
    lanes: Lanes,
    active: Option<Active>,
    #[cfg(test)]
    paused: Option<Instant>,
}

impl Task {
    /// Holds the completion of a correlated frame while a test asks for it.
    #[cfg(test)]
    fn held_by_test(&mut self) -> bool {
        if self.control.0.pause_progress.load(Ordering::Acquire)
            && self.paused.get_or_insert_with(Instant::now).elapsed() <= Duration::from_secs(2)
        {
            self.control
                .0
                .progress_waiting
                .store(true, Ordering::Release);
            return true;
        }
        self.paused = None;
        self.control
            .0
            .progress_waiting
            .store(false, Ordering::Release);
        false
    }

    fn output_turn(&mut self) -> Result<bool, &'static str> {
        let mut progressed = false;
        for _ in 0..BURST {
            if self.active.is_none() {
                let Some(mut frame) = self.lanes.next_frame()? else {
                    break;
                };
                let mut batch = Vec::new();
                #[cfg(test)]
                let allow_coalesce = !self.control.0.pause_progress.load(Ordering::Acquire);
                #[cfg(not(test))]
                let allow_coalesce = true;
                if allow_coalesce
                    && frame.can_coalesce(COALESCE_BYTES)
                    && let Some(next) = self.lanes.pop_coalesce_candidate()
                {
                    if next.can_coalesce(COALESCE_BYTES)
                        && frame.total_len() + next.total_len() <= COALESCE_BYTES
                    {
                        let first_len = frame.total_len();
                        let next_len = next.total_len();
                        batch.push((frame.correlation, frame.report, first_len));
                        batch.push((next.correlation, next.report, next_len));
                        frame.flatten_into_bytes();
                        next.append_to(&mut frame.bytes);
                        frame.correlation = None;
                        while batch.len() < MAX_COALESCE_FRAMES {
                            let Some(more) = self.lanes.pop_coalesce_candidate() else {
                                break;
                            };
                            if more.can_coalesce(COALESCE_BYTES)
                                && frame.bytes.len() + more.total_len() <= COALESCE_BYTES
                            {
                                batch.push((more.correlation, more.report, more.total_len()));
                                more.append_to(&mut frame.bytes);
                            } else {
                                self.lanes.peeked = Some(more);
                                break;
                            }
                        }
                    } else {
                        self.lanes.peeked = Some(next);
                    }
                }
                let mut active = Active::new(frame);
                active.batch = batch;
                self.active = Some(active);
                progressed = true;
            }
            let active = self.active.as_mut().expect("frame taken above");
            if !active.written {
                let correlation = active.frame.correlation;
                let reports_start = active.frame.expiry.is_some() && correlation.is_some();
                let stream = &*self.source.stream;
                let lanes = &mut self.lanes;
                let attempt = write_attempt(
                    active,
                    &self.control,
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
                            lanes.report(Written::Started { correlation });
                        }
                        true
                    },
                );
                match attempt {
                    Ok(Attempt::Done) => active.written = true,
                    Ok(Attempt::Advanced) => {
                        progressed = true;
                        continue;
                    }
                    Ok(Attempt::Blocked) => break,
                    Err(WriteFailure::NotStarted(reason)) => {
                        self.lanes.report(Written::NotStarted {
                            correlation,
                            reason,
                        });
                        self.active = None;
                        progressed = true;
                        // The receipt leaves before the next frame is taken.
                        self.lanes.flush()?;
                        continue;
                    }
                    // The frame stays in place: its failed receipt is one
                    // of the terminal facts.
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
                if let Some((effect, connection)) = correlation {
                    #[cfg(test)]
                    if report != WriteReport::None && self.held_by_test() {
                        break;
                    }
                    if report == WriteReport::CompleteAndWireProgress {
                        self.lanes
                            .wire_progress(WireProgress { effect, connection });
                    }
                }
                if report != WriteReport::None {
                    self.lanes.report(Written::Complete { correlation, bytes });
                }
                self.active = None;
            } else {
                let completed = self.active.take().expect("active batch");
                for (correlation, report, bytes) in completed.batch {
                    if report == WriteReport::CompleteAndWireProgress
                        && let Some((effect, connection)) = correlation
                    {
                        self.lanes
                            .wire_progress(WireProgress { effect, connection });
                    }
                    if report != WriteReport::None {
                        self.lanes.report(Written::Complete { correlation, bytes });
                    }
                }
            }
            progressed = true;
            if !self.lanes.reports.is_empty() {
                self.lanes.flush()?;
            }
        }
        Ok(progressed)
    }

    fn input_turn(&mut self, scratch: &mut [u8]) -> Result<bool, &'static str> {
        let mut progressed = false;
        let mut source = ScratchSource {
            buffered: &mut self.source,
            scratch,
            scratch_start: 0,
            scratch_end: 0,
        };
        for _ in 0..BURST {
            if !self.lanes.deliver()? {
                break;
            }
            let before = self.parser.consumed;
            match self.parser.packet(&mut source, self.clock.as_ref())? {
                Some(packet) => {
                    self.lanes.hold(packet);
                    progressed = true;
                }
                None => {
                    progressed |= self.parser.consumed != before;
                    break;
                }
            }
        }
        drop(source);
        // A packet parsed by the last pass is offered before the turn ends.
        self.lanes.deliver()?;
        Ok(progressed)
    }
}

impl Driven for Task {
    fn descriptor(&self) -> RawFd {
        self.source.stream.as_raw_fd()
    }

    fn turn(&mut self, scratch: &mut [u8]) -> Result<Turn, &'static str> {
        if self.control.is_cancelled() {
            return Err("cancelled");
        }
        let mut progress = self.lanes.flush()?;
        progress |= self.output_turn()?;
        progress |= self.lanes.flush()?;
        progress |= self.input_turn(scratch)?;
        Ok(Turn {
            progress,
            waiting: self.active.is_some() || self.parser.is_partial() || self.lanes.pending(),
        })
    }

    fn finish(self: Box<Self>, code: &'static str) {
        let task = *self;
        task.control.stop(&task.source.stream, code);
        let active = task.active.map(Active::correlations).unwrap_or_default();
        task.lanes.finish(code, active);
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

    fn control() -> Control {
        Control::new(Wake::new(), None)
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
                expiry: Some(expiry),
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
        assert!(!connection.control.is_cancelled());
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
                expiry: Some(expiry),
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
