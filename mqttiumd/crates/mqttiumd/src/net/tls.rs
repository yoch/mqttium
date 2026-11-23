//! One rustls connection and one bounded non-blocking task per secure transport.

use super::*;
use crate::security::{HANDSHAKE_BYTES, HANDSHAKE_TIMEOUT};
use rustls::{CertificateError, Error, ProtocolVersion, ServerConnection};

const READ_CHUNK: usize = 16 * 1024;
// In the pinned ring TLS 1.3 provider, a record is a five byte header,
// 16-byte AEAD tag and one inner content-type byte beyond its plaintext.
const RECORD_OVERHEAD: usize = 22;
const RECORD_BODY: usize = 16 * 1024;
#[cfg(feature = "test-hooks")]
const MUTANT_TLS_EARLY_STARTED: bool = false;

pub(super) struct Task {
    pub stream: Arc<TcpStream>,
    pub state: ServerConnection,
    pub clock: Arc<dyn SessionClock>,
    pub control: Control,
    pub principal: Arc<OnceLock<Principal>>,
    pub policy: Arc<Policy>,
    pub lanes: Lanes,
    pub machine: Machine,
    #[cfg(feature = "test-hooks")]
    pub trace: (crate::writer::Hooks, crate::events::Events),
}

enum Phase {
    Handshake { start: Instant, received: usize },
    Active,
}

/// What the task carries from one turn to the next.
pub(super) struct Machine {
    phase: Phase,
    parser: Parser,
    output: Option<Encoded>,
    control_pending: Option<Instant>,
    /// Frame correlations taken from the lane that could not be sealed.
    unsealed: Vec<Option<(EffectId, ConnectionKey)>>,
    /// The transport reported its end; the TLS state names the reason.
    eof: bool,
    eof_reported: bool,
}

impl Machine {
    /// The handshake budget starts here.
    pub fn new() -> Self {
        Self {
            phase: Phase::Handshake {
                start: Instant::now(),
                received: 0,
            },
            parser: Parser::default(),
            output: None,
            control_pending: None,
            unsealed: Vec::new(),
            eof: false,
            eof_reported: false,
        }
    }
}

/// The plaintext rustls already decrypted.
struct Plaintext<'a>(&'a mut ServerConnection);

impl Source for Plaintext<'_> {
    fn pull(&mut self, into: &mut [u8], partial: bool) -> Result<usize, &'static str> {
        read_plain(self.0, into, partial)
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
    batch: Vec<CoalescedEntry>,
    ciphertext: Vec<u8>,
    offset: usize,
    deadline: Instant,
    #[cfg(feature = "test-hooks")]
    fragment: bool,
}

impl Encoded {
    fn correlations(self) -> Vec<Option<(EffectId, ConnectionKey)>> {
        if self.batch.is_empty() {
            vec![self.frame.correlation]
        } else {
            self.batch.into_iter().map(|(c, _, _)| c).collect()
        }
    }
}

fn frame_ciphertext(
    tls: &mut ServerConnection,
    header: &[u8],
    payload: Option<&[u8]>,
) -> Result<Vec<u8>, &'static str> {
    let payload = payload.unwrap_or(&[]);
    let total = header
        .len()
        .checked_add(payload.len())
        .ok_or("tls_application_boundary")?;
    if total == 0 || total > MAX_PACKET_BYTES || tls.wants_write() {
        return Err("tls_application_boundary");
    }
    let expected = total + total.div_ceil(RECORD_BODY) * RECORD_OVERHEAD;
    let mut ciphertext = Vec::new();
    ciphertext
        .try_reserve_exact(expected)
        .map_err(|_| "allocation_failed")?;
    // Control output was drained. Seal exactly one MQTT frame and verify
    // its ciphertext extent before letting any record near the transport.
    // An unexpected key-update/alert insertion fails closed at this boundary.
    // Pass the frame as one contiguous plaintext slice so rustls emits
    // ceil(total / RECORD_BODY) records rather than splitting header and payload
    // into separate AEAD records.
    if payload.is_empty() {
        if tls
            .writer()
            .write(header)
            .map_err(|_| "tls_application_encode")?
            != header.len()
        {
            return Err("tls_application_buffer");
        }
    } else if total <= 2048 {
        let mut stack = [0u8; 2048];
        stack[..header.len()].copy_from_slice(header);
        stack[header.len()..total].copy_from_slice(payload);
        if tls
            .writer()
            .write(&stack[..total])
            .map_err(|_| "tls_application_encode")?
            != total
        {
            return Err("tls_application_buffer");
        }
    } else {
        let mut merged = Vec::new();
        merged
            .try_reserve_exact(total)
            .map_err(|_| "allocation_failed")?;
        merged.extend_from_slice(header);
        merged.extend_from_slice(payload);
        if tls
            .writer()
            .write(&merged)
            .map_err(|_| "tls_application_encode")?
            != total
        {
            return Err("tls_application_buffer");
        }
    }
    let mut sink = BoundedCiphertext {
        bytes: &mut ciphertext,
        limit: expected,
    };
    // The default Write::write_vectored accepts only its first nonempty
    // slice. Each pass therefore extracts one record, within this bound.
    for _ in 0..total.div_ceil(RECORD_BODY) {
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
                "ciphertext_bytes_sent":sent}}), &self.trace.1, &self.control.0.cancelled)
            .map_err(|_| "tls_hook_failed")
    }

    fn read_ciphertext(&mut self, limit: usize, handshake: bool) -> Result<usize, &'static str> {
        if !self.state.wants_read() {
            return Ok(0);
        }
        let mut reader = &*self.stream;
        match self
            .state
            .read_tls(&mut (&mut reader).take(limit.min(READ_CHUNK) as u64))
        {
            Ok(0) if handshake => Err("tls_handshake_eof"),
            Ok(count) => {
                self.machine.eof |= count == 0;
                if let Err(error) = self.state.process_new_packets() {
                    // A best-effort fatal alert carries no application data.
                    if handshake {
                        let mut writer = &*self.stream;
                        let _ = self.state.write_tls(&mut writer);
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
        let mut writer = &*self.stream;
        match self.state.write_tls(&mut writer) {
            Ok(0) => Err("tls_write_zero"),
            Ok(count) => Ok(count),
            Err(error) if retryable(&error) => Ok(0),
            Err(_) => Err("tls_write_io"),
        }
    }

    fn handshake_turn(
        &mut self,
        start: Instant,
        mut received: usize,
    ) -> Result<Turn, &'static str> {
        let mut progress = false;
        while self.state.is_handshaking() || self.state.wants_write() {
            if start.elapsed() >= HANDSHAKE_TIMEOUT {
                return Err("tls_handshake_deadline");
            }
            if received >= HANDSHAKE_BYTES {
                return Err("tls_handshake_bytes");
            }
            let sent = self.flush_control()?;
            let read = self.read_ciphertext(HANDSHAKE_BYTES - received, true)?;
            received += read;
            self.machine.phase = Phase::Handshake { start, received };
            if sent == 0 && read == 0 {
                return Ok(Turn {
                    progress,
                    waiting: true,
                });
            }
            progress = true;
        }
        if self.state.protocol_version() != Some(ProtocolVersion::TLSv1_3)
            || self.state.handshake_kind() != Some(rustls::HandshakeKind::Full)
        {
            return Err("tls_handshake_profile");
        }
        let elapsed = start.elapsed();
        if elapsed >= HANDSHAKE_TIMEOUT {
            return Err("tls_handshake_deadline");
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
        self.lanes
            .input
            .try_send(Incoming::Authenticated { elapsed })
            .map_err(|_| "tls_authentication_receipt")?;
        self.machine.phase = Phase::Active;
        // Application data may already be decrypted: the next turn reads it.
        Ok(Turn {
            progress: true,
            waiting: true,
        })
    }

    /// The next frame, sealed. The flag says whether the attempt advanced
    /// anything although no frame came out of it.
    fn next_write(&mut self) -> Result<(Option<Encoded>, bool), &'static str> {
        // Empty plaintext flushes a queued peer KeyUpdate response into the
        // control lane without creating an application record.
        let _ = self
            .state
            .writer()
            .write(&[])
            .map_err(|_| "tls_control_encode")?;
        if self.state.wants_write() {
            return Ok((None, self.flush_control()? != 0));
        }
        let Some(mut frame) = self.lanes.next_frame()? else {
            return Ok((None, false));
        };
        let deadline = Instant::now() + WRITE_TIMEOUT;
        let mut batch = Vec::new();
        if let Some(expiry) = frame.expiry {
            match prepare_expiry(&mut frame.bytes, expiry, self.clock.as_ref()) {
                Ok(()) => {}
                Err(WriteFailure::NotStarted(reason)) => {
                    self.lanes.report(Written::NotStarted {
                        correlation: frame.correlation,
                        reason,
                    });
                    return Ok((None, true));
                }
                Err(WriteFailure::Failed(code)) => {
                    // The frame was taken: it is one of the terminal facts.
                    self.machine.unsealed.push(frame.correlation);
                    return Err(code);
                }
            }
        } else if frame.can_coalesce(RECORD_BODY)
            && let Some(next) = self.lanes.pop_coalesce_candidate()
        {
            if next.can_coalesce(RECORD_BODY) && frame.total_len() + next.total_len() <= RECORD_BODY
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
                    if more.can_coalesce(RECORD_BODY)
                        && frame.bytes.len() + more.total_len() <= RECORD_BODY
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
        let ciphertext = match frame_ciphertext(
            &mut self.state,
            &frame.bytes,
            frame.payload.as_ref().map(SharedBytes::as_slice),
        ) {
            Ok(ciphertext) => ciphertext,
            Err(code) => {
                if batch.is_empty() {
                    self.machine.unsealed.push(frame.correlation);
                } else {
                    self.machine
                        .unsealed
                        .extend(batch.into_iter().map(|(c, _, _)| c));
                }
                return Err(code);
            }
        };
        #[cfg(feature = "test-hooks")]
        let fragment = frame.expiry.is_some()
            && frame.correlation.is_some()
            && self.armed("fragment-next-tls-publish");
        #[cfg(feature = "test-hooks")]
        if frame.expiry.is_some() && frame.correlation.is_some() {
            if MUTANT_TLS_EARLY_STARTED {
                self.lanes.report(Written::Started {
                    correlation: frame.correlation,
                });
            }
            // A held barrier keeps this reactor thread: test builds only.
            if self.armed("hold-next-tls-frame") {
                self.observe_boundary("tls.frame.sealed", &frame, 0)?;
            }
        }
        Ok((
            Some(Encoded {
                frame,
                batch,
                ciphertext,
                offset: 0,
                deadline,
                #[cfg(feature = "test-hooks")]
                fragment,
            }),
            true,
        ))
    }

    fn output_turn(&mut self, output: &mut Option<Encoded>) -> Result<bool, &'static str> {
        if output.is_none() {
            let (next, advanced) = self.next_write()?;
            *output = next;
            if output.is_none() {
                return Ok(advanced);
            }
        }
        let Some(encoded) = output.as_mut() else {
            return Ok(false);
        };
        if encoded.offset == 0
            && let Some(expiry) = encoded.frame.expiry
        {
            let prior = expiry
                .value_offset
                .map(|at| {
                    let end = at.checked_add(4).ok_or("output_expiry_offset")?;
                    let bytes: [u8; 4] = encoded
                        .frame
                        .bytes
                        .get(at..end)
                        .ok_or("output_expiry_offset")?
                        .try_into()
                        .map_err(|_| "output_expiry_offset")?;
                    Ok((at..end, bytes))
                })
                .transpose()?;
            match prepare_expiry(&mut encoded.frame.bytes, expiry, self.clock.as_ref()) {
                Ok(())
                    if prior.is_none_or(|(range, bytes)| encoded.frame.bytes[range] == bytes) => {}
                Ok(()) => return Err("tls_expiry_value_aged"),
                Err(WriteFailure::NotStarted(reason)) => {
                    self.lanes.report(Written::NotStarted {
                        correlation: encoded.frame.correlation,
                        reason,
                    });
                    // The frame has its terminal receipt. The sealed records
                    // advanced TLS sequence numbers: close rather than reuse
                    // that connection with a sequence gap.
                    *output = None;
                    return Err("tls_expired_before_ciphertext");
                }
                Err(WriteFailure::Failed(code)) => return Err(code),
            }
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
        let count = match (&*self.stream).write(ciphertext) {
            Ok(0) => return Err("tls_write_zero"),
            Ok(count) => count,
            Err(error) if retryable(&error) => return Ok(false),
            Err(_) => return Err("tls_write_io"),
        };
        let first = encoded.offset == 0;
        if first && encoded.frame.expiry.is_some() && encoded.frame.correlation.is_some() {
            self.lanes.report(Written::Started {
                correlation: encoded.frame.correlation,
            });
        }
        encoded.offset += count;
        #[cfg(feature = "test-hooks")]
        if first && encoded.fragment {
            self.observe_boundary("tls.frame.first_bytes", &encoded.frame, count)?;
        }
        if encoded.offset == encoded.ciphertext.len() {
            if encoded.batch.is_empty() {
                let report = encoded.frame.report;
                if report == WriteReport::CompleteAndWireProgress
                    && let Some((effect, connection)) = encoded.frame.correlation
                {
                    self.lanes
                        .wire_progress(WireProgress { effect, connection });
                }
                if report != WriteReport::None {
                    self.lanes.report(Written::Complete {
                        correlation: encoded.frame.correlation,
                        bytes: encoded.frame.total_len(),
                    });
                }
                *output = None;
            } else {
                let completed = output.take().expect("encoded batch");
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
        }
        Ok(true)
    }

    fn input_turn(&mut self) -> Result<bool, &'static str> {
        let mut progressed = false;
        for _ in 0..BURST {
            if !self.lanes.deliver()? {
                return Ok(progressed);
            }
            let packet = self
                .machine
                .parser
                .packet(&mut Plaintext(&mut self.state), self.clock.as_ref())?;
            if let Some(packet) = packet {
                self.lanes.hold(packet);
                progressed = true;
                continue;
            }
            if self.read_ciphertext(READ_CHUNK, false)? != 0 {
                progressed = true;
                continue;
            }
            if !self.machine.eof {
                break;
            }
            // After the end of the transport the TLS state refuses the next
            // plaintext read with the reason; one more pass obtains it.
            if self.machine.eof_reported {
                return Err("tls_eof");
            }
            self.machine.eof_reported = true;
            progressed = true;
        }
        // A packet parsed by the last pass is offered before the turn ends.
        self.lanes.deliver()?;
        Ok(progressed)
    }

    fn active_turn(&mut self) -> Result<Turn, &'static str> {
        let mut progress = self.lanes.flush()?;
        let mut output = self.machine.output.take();
        let mut written = Ok(());
        for _ in 0..BURST {
            match self.output_turn(&mut output) {
                Ok(true) => progress = true,
                Ok(false) => break,
                Err(code) => {
                    written = Err(code);
                    break;
                }
            }
            if let Err(code) = self.lanes.flush() {
                written = Err(code);
                break;
            }
        }
        // A frame in progress stays visible to the terminal facts.
        self.machine.output = output;
        written?;
        if self.machine.output.is_none() && self.state.wants_write() {
            let first = self
                .machine
                .control_pending
                .get_or_insert_with(Instant::now);
            if first.elapsed() >= WRITE_TIMEOUT {
                return Err("tls_control_write_timeout");
            }
        } else {
            self.machine.control_pending = None;
        }
        progress |= self.lanes.flush()?;
        progress |= self.input_turn()?;
        Ok(Turn {
            progress,
            waiting: self.machine.output.is_some()
                || self.machine.control_pending.is_some()
                || self.machine.parser.is_partial()
                || self.lanes.pending(),
        })
    }
}

impl Driven for Task {
    fn descriptor(&self) -> RawFd {
        self.stream.as_raw_fd()
    }

    fn turn(&mut self, _scratch: &mut [u8]) -> Result<Turn, &'static str> {
        if self.control.is_cancelled() {
            return Err("cancelled");
        }
        match self.machine.phase {
            Phase::Handshake { start, received } => self.handshake_turn(start, received),
            Phase::Active => self.active_turn(),
        }
    }

    fn finish(self: Box<Self>, code: &'static str) {
        let task = *self;
        task.control.stop(&task.stream, code);
        let active = task
            .machine
            .output
            .map(Encoded::correlations)
            .unwrap_or_default()
            .into_iter()
            .chain(task.machine.unsealed);
        task.lanes.finish(code, active);
    }
}
