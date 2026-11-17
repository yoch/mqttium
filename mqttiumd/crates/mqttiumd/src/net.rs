//! Bounded socket tasks for the product adapter. No durable operation runs here.

use std::io::{self, Read, Write as _};
use std::net::{Shutdown, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use mqttiumd_contract::ids::{ConnectionKey, EffectId};
use mqttiumd_contract::message_expiry::{
    ExpiryState, MessageExpiry, MessageExpiryBlock, MessageTime,
};

use crate::codec::{self, ErrorKind};
use crate::session_time::{SessionClock, SystemClock};

pub const INPUT_SLOTS: usize = 4;
pub const OUTPUT_SLOTS: usize = 4;
pub const MAX_PACKET_BYTES: usize = 65_536;
// A correlated frame can publish Started and one terminal result. This is a
// receipt bound; it grants no additional outgoing-frame admission capacity.
pub const WRITTEN_SLOTS: usize = 2 * (OUTPUT_SLOTS + 1) + 8;
// Four queued writes, one active write and eight terminal receipts. Wire
// progress is an independent bounded lane, not the core's receipt lane.
const WIRE_PROGRESS_SLOTS: usize = OUTPUT_SLOTS + 1 + 8;
const POLL: Duration = Duration::from_millis(20);
const INPUT_WAIT: Duration = Duration::from_millis(100);
const RESULT_WAIT: Duration = Duration::from_millis(100);
const PARTIAL_PACKET_TIMEOUT: Duration = Duration::from_secs(5);
const WRITE_TIMEOUT: Duration = Duration::from_millis(250);

#[derive(Debug)]
pub enum Incoming {
    Packet { first: u8, body: Vec<u8> },
    Closed { code: &'static str },
}

#[derive(Debug)]
pub struct Write {
    pub correlation: Option<(EffectId, ConnectionKey)>,
    pub bytes: Vec<u8>,
    pub expiry: Option<WriteExpiry>,
}

#[derive(Clone, Copy, Debug)]
pub struct WriteExpiry {
    pub expiry: MessageExpiry,
    /// Offset of the four value bytes supplied by the publication encoder.
    pub value_offset: usize,
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

#[derive(Clone)]
struct Control {
    cancelled: Arc<AtomicBool>,
    terminal: Arc<Mutex<Option<&'static str>>>,
    #[cfg(test)]
    pause_progress: Arc<AtomicBool>,
    #[cfg(test)]
    progress_waiting: Arc<AtomicBool>,
}

impl Control {
    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    fn record(&self, code: &'static str) {
        if let Ok(mut terminal) = self.terminal.lock() {
            terminal.get_or_insert(code);
        }
    }

    fn stop(&self, stream: &TcpStream, code: &'static str) {
        self.record(code);
        self.cancelled.store(true, Ordering::Release);
        let _ = stream.shutdown(Shutdown::Both);
    }
}

/// The owner polls these bounded queues. Closing cancels both tasks immediately;
/// it then reaps them with `join_finished`, without waiting on a socket in its turn.
/// A queue-full terminal code is retained independently of the full input queue.
pub struct Connection {
    pub incoming: Receiver<Incoming>,
    pub outgoing: SyncSender<Write>,
    pub written: Receiver<Written>,
    pub wire_progress: Receiver<WireProgress>,
    stream: TcpStream,
    control: Control,
    reader: Option<JoinHandle<()>>,
    writer: Option<JoinHandle<()>>,
    send_buffer: usize,
}

pub fn spawn(stream: TcpStream) -> io::Result<Connection> {
    let clock = SystemClock::new().map_err(io::Error::other)?;
    spawn_with_clock(stream, Arc::new(clock))
}

pub fn spawn_with_clock(stream: TcpStream, clock: Arc<dyn SessionClock>) -> io::Result<Connection> {
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(POLL))?;
    let socket = socket2::SockRef::from(&stream);
    socket.set_send_buffer_size(4096)?;
    let send_buffer = socket.send_buffer_size()?;
    let input_stream = stream.try_clone()?;
    let output_stream = stream.try_clone()?;
    let (input_tx, incoming) = mpsc::sync_channel(INPUT_SLOTS);
    let (outgoing, output_rx) = mpsc::sync_channel(OUTPUT_SLOTS);
    let (written_tx, written) = mpsc::sync_channel(WRITTEN_SLOTS);
    let (progress_tx, wire_progress) = mpsc::sync_channel(WIRE_PROGRESS_SLOTS);
    let control = Control {
        cancelled: Arc::new(AtomicBool::new(false)),
        terminal: Arc::new(Mutex::new(None)),
        #[cfg(test)]
        pause_progress: Arc::new(AtomicBool::new(false)),
        #[cfg(test)]
        progress_waiting: Arc::new(AtomicBool::new(false)),
    };
    let reader_control = control.clone();
    let reader = thread::Builder::new()
        .name("mqttiumd-reader".into())
        .spawn(move || {
            read_packets(input_stream, input_tx, reader_control);
        })?;
    let writer_control = control.clone();
    let writer = match thread::Builder::new()
        .name("mqttiumd-socket-writer".into())
        .spawn(move || {
            write_frames(
                output_stream,
                output_rx,
                written_tx,
                progress_tx,
                writer_control,
                clock.as_ref(),
            );
        }) {
        Ok(writer) => writer,
        Err(error) => {
            control.stop(&stream, "task_spawn_failed");
            let _ = reader.join();
            return Err(error);
        }
    };
    Ok(Connection {
        incoming,
        outgoing,
        written,
        wire_progress,
        stream,
        control,
        reader: Some(reader),
        writer: Some(writer),
        send_buffer,
    })
}

impl Connection {
    #[cfg(test)]
    pub fn pause_wire_progress(&self, pause: bool) {
        self.control.pause_progress.store(pause, Ordering::Release);
    }

    #[cfg(test)]
    pub fn wire_progress_is_waiting(&self) -> bool {
        self.control.progress_waiting.load(Ordering::Acquire)
    }
    pub fn close(&self) {
        self.control.stop(&self.stream, "cancelled");
    }

    pub fn is_finished(&self) -> bool {
        self.reader.as_ref().is_none_or(JoinHandle::is_finished)
            && self.writer.as_ref().is_none_or(JoinHandle::is_finished)
    }

    /// Joins only after both tasks have ended, returning false while they run.
    pub fn join_finished(&mut self) -> bool {
        if !self.is_finished() {
            return false;
        }
        for handle in [&mut self.reader, &mut self.writer] {
            if let Some(handle) = handle.take()
                && handle.join().is_err()
            {
                self.control.record("task_panicked");
            }
        }
        true
    }

    pub fn terminal_reason(&self) -> Option<&'static str> {
        self.control.terminal.lock().ok().and_then(|reason| *reason)
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

fn retryable(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut | io::ErrorKind::Interrupted
    )
}

/// Reads within the current packet's absolute budget. An idle connection has
/// no packet deadline: the runtime separately owns CONNECT and Keep Alive.
fn read_part(
    stream: &mut TcpStream,
    buffer: &mut [u8],
    started: &mut Option<Instant>,
    control: &Control,
) -> Result<(), &'static str> {
    let mut offset = 0;
    while offset < buffer.len() {
        if control.is_cancelled() {
            return Err("cancelled");
        }
        if started.is_some_and(|instant| instant.elapsed() >= PARTIAL_PACKET_TIMEOUT) {
            return Err("partial_packet_timeout");
        }
        match stream.read(&mut buffer[offset..]) {
            Ok(0) => {
                return Err(if started.is_none() {
                    "eof"
                } else {
                    "truncated_packet"
                });
            }
            Ok(count) => {
                started.get_or_insert_with(Instant::now);
                if started.is_some_and(|instant| instant.elapsed() >= PARTIAL_PACKET_TIMEOUT) {
                    return Err("partial_packet_timeout");
                }
                offset += count;
            }
            Err(error) if retryable(&error) => {}
            Err(_) => return Err("read_io"),
        }
    }
    Ok(())
}

fn next_packet(stream: &mut TcpStream, control: &Control) -> Result<Incoming, &'static str> {
    let mut prefix = [0; 5];
    let mut length = 0;
    let mut started = None;
    let header = loop {
        if length == prefix.len() {
            return Err("malformed_header");
        }
        read_part(
            stream,
            &mut prefix[length..length + 1],
            &mut started,
            control,
        )?;
        length += 1;
        match codec::header(&prefix[..length], MAX_PACKET_BYTES) {
            Ok(Some(header)) => break header,
            Ok(None) => {}
            Err(error) => {
                return Err(if error.kind() == ErrorKind::Oversized {
                    "packet_too_large"
                } else {
                    "malformed_header"
                });
            }
        }
    };
    let mut body = Vec::new();
    body.try_reserve_exact(header.remaining)
        .map_err(|_| "allocation_failed")?;
    body.resize(header.remaining, 0);
    read_part(stream, &mut body, &mut started, control)?;
    Ok(Incoming::Packet {
        first: header.first,
        body,
    })
}

fn read_packets(mut stream: TcpStream, sender: SyncSender<Incoming>, control: Control) {
    let code = loop {
        let mut incoming = match next_packet(&mut stream, &control) {
            Ok(packet) => packet,
            Err(code) => break code,
        };
        let deadline = Instant::now() + INPUT_WAIT;
        loop {
            if control.is_cancelled() {
                break;
            }
            match sender.try_send(incoming) {
                Ok(()) => break,
                Err(TrySendError::Disconnected(_)) => {
                    control.stop(&stream, "input_consumer_closed");
                    return;
                }
                Err(TrySendError::Full(value)) => {
                    incoming = value;
                    if Instant::now() >= deadline {
                        control.stop(&stream, "input_queue_full");
                        // The cause remains in Control even when the queue is full.
                        return;
                    }
                    thread::sleep(Duration::from_millis(1));
                }
            }
        }
    };
    control.stop(&stream, code);
    let _ = sender.try_send(Incoming::Closed { code });
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
    let value_end = expiry
        .value_offset
        .checked_add(4)
        .filter(|end| *end <= bytes.len())
        .ok_or(WriteFailure::Failed("output_expiry_offset"))?;
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
    bytes[expiry.value_offset..value_end].copy_from_slice(&remaining.to_be_bytes());
    Ok(())
}

fn write_attempts(
    bytes: &mut [u8],
    expiry: Option<WriteExpiry>,
    control: &Control,
    clock: &dyn SessionClock,
    mut write: impl FnMut(&[u8], Duration) -> io::Result<usize>,
    mut started: impl FnMut() -> bool,
) -> Result<(), WriteFailure> {
    if bytes.len() > MAX_PACKET_BYTES {
        return Err(WriteFailure::Failed("output_packet_too_large"));
    }
    let deadline = Instant::now() + WRITE_TIMEOUT;
    let mut offset = 0;
    while offset < bytes.len() {
        if control.is_cancelled() {
            return Err(WriteFailure::Failed("cancelled"));
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(WriteFailure::Failed("write_timeout"));
        }
        // An unsuccessful attempt may consume the rest of the lifetime. Gate
        // every zero-progress retry, immediately before granting a write.
        if offset == 0
            && let Some(expiry) = expiry
        {
            prepare_expiry(bytes, expiry, clock)?;
        }
        match write(&bytes[offset..], remaining.min(POLL)) {
            Ok(0) => return Err(WriteFailure::Failed("write_zero")),
            Ok(count) => {
                if count > bytes.len() - offset {
                    return Err(WriteFailure::Failed("invalid_write_progress"));
                }
                let first = offset == 0;
                offset += count;
                if first && !started() {
                    return Err(WriteFailure::Failed("write_result_queue_full"));
                }
            }
            Err(error) if retryable(&error) => {}
            Err(_) => return Err(WriteFailure::Failed("write_io")),
        }
    }
    if Instant::now() >= deadline {
        return Err(WriteFailure::Failed("write_timeout"));
    }
    Ok(())
}

fn write_one(
    stream: &mut TcpStream,
    frame: &mut Write,
    control: &Control,
    clock: &dyn SessionClock,
    sender: &SyncSender<Written>,
) -> Result<(), WriteFailure> {
    write_attempts(
        &mut frame.bytes,
        frame.expiry,
        control,
        clock,
        |bytes, timeout| {
            stream.set_write_timeout(Some(timeout))?;
            stream.write(bytes)
        },
        || {
            frame.expiry.is_none()
                || frame.correlation.is_none()
                || report(
                    sender,
                    Written::Started {
                        correlation: frame.correlation,
                    },
                )
        },
    )
}

fn report(sender: &SyncSender<Written>, mut result: Written) -> bool {
    let deadline = Instant::now() + RESULT_WAIT;
    loop {
        match sender.try_send(result) {
            Ok(()) => return true,
            Err(TrySendError::Disconnected(_)) => return false,
            Err(TrySendError::Full(value)) => {
                result = value;
                if Instant::now() >= deadline {
                    return false;
                }
                thread::sleep(Duration::from_millis(1));
            }
        }
    }
}

fn write_frames(
    mut stream: TcpStream,
    receiver: Receiver<Write>,
    sender: SyncSender<Written>,
    progress: SyncSender<WireProgress>,
    control: Control,
    clock: &dyn SessionClock,
) {
    let code = loop {
        if control.is_cancelled() {
            break "cancelled";
        }
        let mut frame = match receiver.recv_timeout(POLL) {
            Ok(frame) => frame,
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => break "output_producer_closed",
        };
        let result = match write_one(&mut stream, &mut frame, &control, clock, &sender) {
            Ok(()) => Written::Complete {
                correlation: frame.correlation,
                bytes: frame.bytes.len(),
            },
            Err(WriteFailure::NotStarted(reason)) => {
                if !report(
                    &sender,
                    Written::NotStarted {
                        correlation: frame.correlation,
                        reason,
                    },
                ) {
                    break "write_result_queue_full";
                }
                continue;
            }
            Err(WriteFailure::Failed(code)) => {
                let _ = report(
                    &sender,
                    Written::Failed {
                        correlation: frame.correlation,
                        code,
                    },
                );
                break code;
            }
        };
        if let Some((effect, connection)) = frame.correlation {
            // This publication is deliberately separate from Written: the
            // reactor may retain a core receipt while MQTT receive credit has
            // already been returned on the wire. A preempted writer between
            // write_one and this point is handled by bounded ingress deferral.
            #[cfg(test)]
            {
                let start = Instant::now();
                while control.pause_progress.load(Ordering::Acquire) && !control.is_cancelled() {
                    control.progress_waiting.store(true, Ordering::Release);
                    if start.elapsed() > Duration::from_secs(2) {
                        break;
                    }
                    thread::sleep(Duration::from_millis(1));
                }
                control.progress_waiting.store(false, Ordering::Release);
            }
            let deadline = Instant::now() + RESULT_WAIT;
            let mut value = WireProgress { effect, connection };
            loop {
                match progress.try_send(value) {
                    Ok(()) => break,
                    Err(TrySendError::Full(returned))
                        if Instant::now() < deadline && !control.is_cancelled() =>
                    {
                        value = returned;
                        thread::sleep(Duration::from_millis(1));
                    }
                    Err(_) => {
                        control.stop(&stream, "wire_progress_unavailable");
                        return;
                    }
                }
            }
        }
        if !report(&sender, result) {
            break "write_result_queue_full";
        }
    };
    control.stop(&stream, code);
    // Accepted frames left in the bounded queue get explicit failed receipts.
    // Their bytes may not have been attempted; none is called certainly rejected.
    for frame in receiver.try_iter() {
        if !report(
            &sender,
            Written::Failed {
                correlation: frame.correlation,
                code,
            },
        ) {
            control.record("write_result_queue_full");
            break;
        }
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
        Control {
            cancelled: Arc::new(AtomicBool::new(false)),
            terminal: Arc::new(Mutex::new(None)),
            pause_progress: Arc::new(AtomicBool::new(false)),
            progress_waiting: Arc::new(AtomicBool::new(false)),
        }
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
                value_offset: encoded.expiry_value_offset.unwrap(),
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

    fn incoming(connection: &Connection) -> Incoming {
        connection
            .incoming
            .recv_timeout(Duration::from_millis(600))
            .unwrap()
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
            &bytes[expiry.value_offset..expiry.value_offset + 4],
            &1u32.to_be_bytes()
        );
        assert_eq!(
            &bytes[..expiry.value_offset],
            &original[..expiry.value_offset]
        );
        assert_eq!(
            &bytes[expiry.value_offset + 4..],
            &original[expiry.value_offset + 4..]
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
                        assert_eq!(&part[expiry.value_offset..expiry.value_offset + 4], &[0; 4]);
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
    fn a_bad_expiry_value_offset_closes_without_touching_socket_bytes() {
        let (mut bytes, mut expiry) = timed_frame();
        expiry.value_offset = usize::MAX;
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
                bytes: vec![0xd0, 0],
                expiry: None,
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
                bytes,
                expiry: Some(expiry),
            })
            .unwrap();
        connection
            .outgoing
            .send(Write {
                correlation: None,
                bytes: vec![0xd0, 0],
                expiry: None,
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
            matches!(incoming(&connection), Incoming::Packet {first:0xc0, body} if body.is_empty())
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
        expiry.value_offset = encoded.expiry_value_offset.unwrap();
        let bytes = encoded.bytes;
        let expected = bytes.clone();
        connection
            .outgoing
            .send(Write {
                correlation: correlation(1),
                bytes,
                expiry: Some(expiry),
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
        assert!(matches!(incoming(&connection), Incoming::Packet {first:0x40,body} if body==[0,7]));
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
                matches!(incoming(&connection), Incoming::Packet { first: 0xc0, body } if body.is_empty())
            );
        }
        connection.close();
        finish(&mut connection);
    }

    #[test]
    fn oversized_header_is_refused_without_waiting_for_its_body() {
        let (mut connection, mut peer) = pair();
        peer.write_all(&[0x34, 0xff, 0xff, 0x7f]).unwrap();
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
                bytes: vec![0xd0, 0],
                expiry: None,
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
                    bytes: vec![0; 65_536],
                    expiry: None,
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
        closed(&connection, "malformed_header");
        finish(&mut connection);
    }

    #[test]
    fn input_saturation_retains_its_cause_outside_the_full_queue() {
        let (mut connection, mut peer) = pair();
        peer.write_all(&[0xc0, 0].repeat(INPUT_SLOTS + 2)).unwrap();
        let deadline = Instant::now() + Duration::from_millis(600);
        while !connection.is_finished() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(connection.terminal_reason(), Some("input_queue_full"));
        assert_eq!(connection.incoming.try_iter().count(), INPUT_SLOTS);
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
                    bytes: vec![0; 65_536],
                    expiry: None,
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
