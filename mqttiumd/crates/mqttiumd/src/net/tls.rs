//! One rustls connection and one bounded nonblocking task per secure transport.

use super::*;
use crate::security::{HANDSHAKE_BYTES, HANDSHAKE_TIMEOUT, TLS_BUFFER_BYTES};
use rustls::{CertificateError, Error, ProtocolVersion, ServerConnection};

const TURN: Duration = Duration::from_millis(1);
const READ_CHUNK: usize = 16 * 1024;
// In the pinned ring TLS 1.3 provider, a record is a five byte header,
// 16-byte AEAD tag and one inner content-type byte beyond its plaintext.
const RECORD_OVERHEAD: usize = 22;
const RECORD_BODY: usize = 16 * 1024;
#[cfg(feature = "test-hooks")]
const MUTANT_TLS_EARLY_STARTED: bool = false;

pub(super) struct Task {
    pub stream: TcpStream,
    pub state: ServerConnection,
    pub clock: Arc<dyn SessionClock>,
    pub control: Control,
    pub principal: Arc<OnceLock<Principal>>,
    pub policy: Arc<Policy>,
    pub input: SyncSender<Incoming>,
    pub output: Receiver<Write>,
    pub written: SyncSender<Written>,
    pub progress: SyncSender<WireProgress>,
    #[cfg(feature = "test-hooks")]
    pub trace: (crate::writer::Hooks, crate::events::Events),
}

#[derive(Default)]
struct Parser {
    prefix: [u8; 5],
    prefix_len: usize,
    header: Option<codec::Header>,
    body: Vec<u8>,
    offset: usize,
    since: Option<Instant>,
    previous: Option<MessageTime>,
}

impl Parser {
    fn packet(
        &mut self,
        tls: &mut ServerConnection,
        clock: &dyn SessionClock,
    ) -> Result<Option<Incoming>, &'static str> {
        if self
            .since
            .is_some_and(|start| start.elapsed() >= PARTIAL_PACKET_TIMEOUT)
        {
            return Err("partial_packet_timeout");
        }
        while self.header.is_none() {
            if self.prefix_len == self.prefix.len() {
                return Err("malformed_header");
            }
            let next = &mut self.prefix[self.prefix_len..self.prefix_len + 1];
            if read_plain(tls, next, self.since.is_some())? == 0 {
                return Ok(None);
            }
            self.since.get_or_insert_with(Instant::now);
            self.prefix_len += 1;
            self.header = codec::header(&self.prefix[..self.prefix_len], MAX_PACKET_BYTES)
                .map_err(|error| {
                    if error.kind() == ErrorKind::Oversized {
                        "packet_too_large"
                    } else {
                        "malformed_header"
                    }
                })?;
        }
        let header = self.header.expect("header checked");
        if self.body.len() != header.remaining {
            self.body
                .try_reserve_exact(header.remaining)
                .map_err(|_| "allocation_failed")?;
            self.body.resize(header.remaining, 0);
        }
        if self.offset < self.body.len() {
            self.offset += read_plain(tls, &mut self.body[self.offset..], true)?;
            if self.offset < self.body.len() {
                return Ok(None);
            }
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

fn read_plain(
    tls: &mut ServerConnection,
    into: &mut [u8],
    partial: bool,
) -> Result<usize, &'static str> {
    match tls.reader().read(into) {
        Ok(0) => Err(if partial {
            "truncated_packet"
        } else {
            "tls_close_notify"
        }),
        Ok(count) => Ok(count),
        Err(error) if retryable(&error) => Ok(0),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => Err(if partial {
            "truncated_packet"
        } else {
            "tls_eof"
        }),
        Err(_) => Err("tls_plaintext_read"),
    }
}

fn tls_failure(error: &Error) -> &'static str {
    match error {
        Error::NoCertificatesPresented => "tls_client_certificate_missing",
        Error::InvalidCertificate(CertificateError::ApplicationVerificationFailure) => {
            "tls_identity_not_allowed"
        }
        Error::InvalidCertificate(
            CertificateError::Expired
            | CertificateError::NotValidYet
            | CertificateError::ExpiredContext { .. }
            | CertificateError::NotValidYetContext { .. },
        ) => "tls_certificate_time",
        Error::InvalidCertificate(_) => "tls_certificate_invalid",
        Error::PeerIncompatible(_) => "tls_protocol_incompatible",
        Error::FailedToGetCurrentTime => "tls_certificate_clock",
        _ => "tls_protocol_error",
    }
}

struct Encoded {
    frame: Write,
    ciphertext: Vec<u8>,
    offset: usize,
    deadline: Instant,
    #[cfg(feature = "test-hooks")]
    fragment: bool,
}

fn frame_ciphertext(tls: &mut ServerConnection, bytes: &[u8]) -> Result<Vec<u8>, &'static str> {
    if bytes.is_empty() || bytes.len() > MAX_PACKET_BYTES || tls.wants_write() {
        return Err("tls_application_boundary");
    }
    let expected = bytes.len() + bytes.len().div_ceil(RECORD_BODY) * RECORD_OVERHEAD;
    let mut ciphertext = Vec::new();
    ciphertext
        .try_reserve_exact(expected)
        .map_err(|_| "allocation_failed")?;
    // Control output was drained. Seal exactly one MQTT frame and verify
    // its ciphertext extent before letting any record near the transport.
    // An unexpected key-update/alert insertion fails closed at this boundary.
    if tls
        .writer()
        .write(bytes)
        .map_err(|_| "tls_application_encode")?
        != bytes.len()
    {
        return Err("tls_application_buffer");
    }
    let mut sink = BoundedCiphertext {
        bytes: &mut ciphertext,
        limit: expected,
    };
    // The default Write::write_vectored accepts only its first nonempty
    // slice. Each pass therefore extracts one record, within this bound.
    for _ in 0..bytes.len().div_ceil(RECORD_BODY) {
        if !tls.wants_write() {
            break;
        }
        if tls
            .write_tls(&mut sink)
            .map_err(|_| "tls_application_encode")?
            == 0
        {
            return Err("tls_application_encode");
        }
    }
    if tls.wants_write() || ciphertext.len() != expected {
        return Err("tls_application_boundary");
    }
    Ok(ciphertext)
}

/// Cap the extraction sink even if the upstream representation changes.
struct BoundedCiphertext<'a> {
    bytes: &'a mut Vec<u8>,
    limit: usize,
}
impl io::Write for BoundedCiphertext<'_> {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        if input.len() > self.limit - self.bytes.len() {
            return Err(io::Error::other(
                "TLS frame ciphertext exceeds expected extent",
            ));
        }
        self.bytes.extend_from_slice(input);
        Ok(input.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Task {
    #[cfg(feature = "test-hooks")]
    fn armed(&self, name: &str) -> bool {
        self.trace
            .0
            .directory
            .as_ref()
            .is_some_and(|dir| std::fs::remove_file(dir.join(name)).is_ok())
    }

    #[cfg(feature = "test-hooks")]
    fn observe_boundary(
        &self,
        point: &'static str,
        frame: &Write,
        sent: usize,
    ) -> Result<(), &'static str> {
        let (effect, connection) = frame.correlation.ok_or("tls_hook_missing_frame")?;
        self.trace.0.barrier_fields(point, serde_json::json!({"point":point,
            "connection":connection.generation.get().to_string(), "details":{
                "effect":format!("{}/{}/{}",effect.epoch.get(),effect.owner.get(),effect.sequence.get()),
                "ciphertext_bytes_sent":sent}}), &self.trace.1, &self.control.cancelled)
            .map_err(|_| "tls_hook_failed")
    }

    pub fn run(mut self) {
        self.state.set_buffer_limit(Some(TLS_BUFFER_BYTES));
        let result = self.handshake().and_then(|()| self.active());
        let code = result.err().unwrap_or("cancelled");
        self.control.stop(&self.stream, code);
        for frame in self.output.try_iter() {
            if !report(
                &self.written,
                Written::Failed {
                    correlation: frame.correlation,
                    code,
                },
            ) {
                break;
            }
        }
        let _ = self.input.try_send(Incoming::Closed { code });
    }

    fn read_ciphertext(&mut self, limit: usize, handshake: bool) -> Result<usize, &'static str> {
        if !self.state.wants_read() {
            return Ok(0);
        }
        match self
            .state
            .read_tls(&mut (&mut self.stream).take(limit.min(READ_CHUNK) as u64))
        {
            Ok(0) if handshake => Err("tls_handshake_eof"),
            Ok(count) => {
                if let Err(error) = self.state.process_new_packets() {
                    // A best-effort fatal alert carries no application data.
                    if handshake {
                        let _ = self.state.write_tls(&mut self.stream);
                    }
                    return Err(tls_failure(&error));
                }
                Ok(count)
            }
            Err(error) if retryable(&error) => Ok(0),
            Err(_) => Err("tls_read_io"),
        }
    }

    fn flush_control(&mut self) -> Result<usize, &'static str> {
        if !self.state.wants_write() {
            return Ok(0);
        }
        match self.state.write_tls(&mut self.stream) {
            Ok(0) => Err("tls_write_zero"),
            Ok(count) => Ok(count),
            Err(error) if retryable(&error) => Ok(0),
            Err(_) => Err("tls_write_io"),
        }
    }

    fn handshake(&mut self) -> Result<(), &'static str> {
        let start = Instant::now();
        let mut received = 0usize;
        while self.state.is_handshaking() || self.state.wants_write() {
            if self.control.is_cancelled() {
                return Err("cancelled");
            }
            if start.elapsed() >= HANDSHAKE_TIMEOUT {
                return Err("tls_handshake_deadline");
            }
            if received >= HANDSHAKE_BYTES {
                return Err("tls_handshake_bytes");
            }
            let sent = self.flush_control()?;
            let read = self.read_ciphertext(HANDSHAKE_BYTES - received, true)?;
            received += read;
            if sent == 0 && read == 0 {
                thread::sleep(TURN);
            }
        }
        if self.state.protocol_version() != Some(ProtocolVersion::TLSv1_3)
            || self.state.handshake_kind() != Some(rustls::HandshakeKind::Full)
        {
            return Err("tls_handshake_profile");
        }
        let principal = self
            .state
            .peer_certificates()
            .and_then(|chain| chain.first())
            .and_then(|leaf| self.policy.certificate(leaf))
            .ok_or("tls_identity_not_allowed")?;
        self.principal
            .set(principal)
            .map_err(|_| "tls_duplicate_authentication")?;
        self.input
            .try_send(Incoming::Authenticated {
                elapsed: start.elapsed(),
            })
            .map_err(|_| "tls_authentication_receipt")?;
        Ok(())
    }

    fn next_write(&mut self) -> Result<Option<Encoded>, &'static str> {
        // Empty plaintext flushes a queued peer KeyUpdate response into the
        // control lane without creating an application record.
        let _ = self
            .state
            .writer()
            .write(&[])
            .map_err(|_| "tls_control_encode")?;
        if self.state.wants_write() {
            self.flush_control()?;
            return Ok(None);
        }
        let mut frame = match self.output.try_recv() {
            Ok(frame) => frame,
            Err(mpsc::TryRecvError::Empty) => return Ok(None),
            Err(mpsc::TryRecvError::Disconnected) => return Err("output_producer_closed"),
        };
        let deadline = Instant::now() + WRITE_TIMEOUT;
        if let Some(expiry) = frame.expiry {
            match prepare_expiry(&mut frame.bytes, expiry, self.clock.as_ref()) {
                Ok(()) => {}
                Err(WriteFailure::NotStarted(reason)) => {
                    if !report(
                        &self.written,
                        Written::NotStarted {
                            correlation: frame.correlation,
                            reason,
                        },
                    ) {
                        return Err("write_result_queue_full");
                    }
                    return Ok(None);
                }
                Err(WriteFailure::Failed(code)) => return Err(code),
            }
        }
        let ciphertext = frame_ciphertext(&mut self.state, &frame.bytes)?;
        #[cfg(feature = "test-hooks")]
        let fragment = frame.expiry.is_some()
            && frame.correlation.is_some()
            && self.armed("fragment-next-tls-publish");
        #[cfg(feature = "test-hooks")]
        if frame.expiry.is_some() && frame.correlation.is_some() {
            if MUTANT_TLS_EARLY_STARTED
                && !report(
                    &self.written,
                    Written::Started {
                        correlation: frame.correlation,
                    },
                )
            {
                return Err("write_result_queue_full");
            }
            if self.armed("hold-next-tls-frame") {
                self.observe_boundary("tls.frame.sealed", &frame, 0)?;
            }
        }
        Ok(Some(Encoded {
            frame,
            ciphertext,
            offset: 0,
            deadline,
            #[cfg(feature = "test-hooks")]
            fragment,
        }))
    }

    fn output_turn(&mut self, output: &mut Option<Encoded>) -> Result<bool, &'static str> {
        if output.is_none() {
            *output = self.next_write()?;
        }
        let Some(encoded) = output.as_mut() else {
            return Ok(false);
        };
        if encoded.offset == 0
            && let Some(expiry) = encoded.frame.expiry
        {
            let position = expiry.value_offset;
            let prior: [u8; 4] = encoded.frame.bytes[position..position + 4]
                .try_into()
                .map_err(|_| "output_expiry_offset")?;
            match prepare_expiry(&mut encoded.frame.bytes, expiry, self.clock.as_ref()) {
                Ok(()) if encoded.frame.bytes[position..position + 4] == prior => {}
                Ok(()) => return Err("tls_expiry_value_aged"),
                Err(WriteFailure::NotStarted(reason)) => {
                    if !report(
                        &self.written,
                        Written::NotStarted {
                            correlation: encoded.frame.correlation,
                            reason,
                        },
                    ) {
                        return Err("write_result_queue_full");
                    }
                    // The sealed records advanced TLS sequence numbers. Close
                    // rather than reusing that connection with a sequence gap.
                    return Err("tls_expired_before_ciphertext");
                }
                Err(WriteFailure::Failed(code)) => return Err(code),
            }
        }
        if self.control.is_cancelled() {
            return Err("cancelled");
        }
        if Instant::now() >= encoded.deadline {
            return Err("tls_write_timeout");
        }
        let ciphertext = &encoded.ciphertext[encoded.offset..];
        #[cfg(feature = "test-hooks")]
        let ciphertext = if encoded.offset == 0 && encoded.fragment {
            &ciphertext[..3]
        } else {
            ciphertext
        };
        let count = match self.stream.write(ciphertext) {
            Ok(0) => return Err("tls_write_zero"),
            Ok(count) => count,
            Err(error) if retryable(&error) => return Ok(false),
            Err(_) => return Err("tls_write_io"),
        };
        let first = encoded.offset == 0;
        if first
            && encoded.frame.expiry.is_some()
            && encoded.frame.correlation.is_some()
            && !report(
                &self.written,
                Written::Started {
                    correlation: encoded.frame.correlation,
                },
            )
        {
            return Err("write_result_queue_full");
        }
        encoded.offset += count;
        #[cfg(feature = "test-hooks")]
        if first && encoded.fragment {
            self.observe_boundary("tls.frame.first_bytes", &encoded.frame, count)?;
        }
        if encoded.offset == encoded.ciphertext.len() {
            if let Some((effect, connection)) = encoded.frame.correlation {
                self.progress
                    .try_send(WireProgress { effect, connection })
                    .map_err(|_| "wire_progress_unavailable")?;
            }
            if !report(
                &self.written,
                Written::Complete {
                    correlation: encoded.frame.correlation,
                    bytes: encoded.frame.bytes.len(),
                },
            ) {
                return Err("write_result_queue_full");
            }
            *output = None;
        }
        Ok(true)
    }

    fn active(&mut self) -> Result<(), &'static str> {
        let mut parser = Parser::default();
        let mut input = None;
        let mut output = None;
        let mut control_pending = None;
        while !self.control.is_cancelled() {
            let progressed = self.output_turn(&mut output)?;
            if output.is_none() && self.state.wants_write() {
                let first = control_pending.get_or_insert_with(Instant::now);
                if first.elapsed() >= WRITE_TIMEOUT {
                    return Err("tls_control_write_timeout");
                }
            } else {
                control_pending = None;
            }
            let mut received = false;
            if input.is_none() {
                if let Some(packet) = parser.packet(&mut self.state, self.clock.as_ref())? {
                    input = Some((packet, Instant::now()));
                } else if self.read_ciphertext(READ_CHUNK, false)? != 0 {
                    received = true;
                }
            }
            if let Some((packet, start)) = input.take() {
                match self.input.try_send(packet) {
                    Ok(()) => received = true,
                    Err(TrySendError::Full(packet)) if start.elapsed() < INPUT_WAIT => {
                        input = Some((packet, start))
                    }
                    Err(TrySendError::Full(_)) => return Err("input_queue_full"),
                    Err(TrySendError::Disconnected(_)) => return Err("input_consumer_closed"),
                }
            }
            if !progressed && !received {
                thread::sleep(TURN);
            }
        }
        Ok(())
    }
}
