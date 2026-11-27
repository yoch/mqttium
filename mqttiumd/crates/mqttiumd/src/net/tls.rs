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
    pub core: Arc<ConnCore>,
    pub state: ServerConnection,
    pub clock: Arc<dyn SessionClock>,
    pub policy: Arc<Policy>,
    pub stalled: Option<Box<StalledState>>,
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
    output: Option<Box<Encoded>>,
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
        self.trace.0.barrier_fields(point, serde_json::json!({
            "point": point,
            "connection": connection.generation.get().to_string(),
            "details": {
                "effect": format!("{}/{}/{}",effect.epoch.get(),effect.owner.get(),effect.sequence.get()),
                "ciphertext_bytes_sent": sent,
            },
        }), &self.trace.1, &self.core.cancelled)
            .map_err(|_| "tls_hook_failed")
    }

    fn read_ciphertext(&mut self, limit: usize, handshake: bool) -> Result<usize, &'static str> {
        if !self.state.wants_read() {
            return Ok(0);
        }
        let mut reader = &self.core.stream;
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
                        let mut writer = &self.core.stream;
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
        let mut writer = &self.core.stream;
        match self.state.write_tls(&mut writer) {
            Ok(0) => Err("tls_write_zero"),
            Ok(count) => Ok(count),
            Err(error) if retryable(&error) => Ok(0),
            Err(_) => Err("tls_write_io"),
        }
    }

    /// RFC 8446 §6.1: close_notify precedes the end of the write side unless
    /// an error alert was sent. Only an orderly end says it: the close the
    /// core decided, and the answer to the peer's own close_notify. A sealed
    /// frame that did not leave whole would make the alert undecryptable,
    /// and the stream is then truncated in fact. The attempt never waits: a
    /// peer that does not read gets the end of the transport alone.
    fn close_notify(&mut self, code: &'static str) {
        if !matches!(self.machine.phase, Phase::Active)
            || !matches!(code, "cancelled" | "tls_close_notify")
            || self.machine.output.is_some()
        {
            return;
        }
        self.state.send_close_notify();
        while self.state.wants_write() {
            let mut writer = &self.core.stream;
            if !matches!(self.state.write_tls(&mut writer), Ok(count) if count != 0) {
                break;
            }
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
        self.core
            .principal
            .as_deref()
            .ok_or("tls_duplicate_authentication")?
            .set(principal)
            .map_err(|_| "tls_duplicate_authentication")?;
        if self
            .core
            .push_incoming_one(Incoming::Authenticated { elapsed })
            .map_err(|_| "tls_authentication_receipt")?
            .is_some()
        {
            return Err("tls_authentication_receipt");
        }
        self.machine.phase = Phase::Active;
        // Application data may already be decrypted: the next turn reads it.
        Ok(Turn {
            progress: true,
            waiting: true,
        })
    }

    /// The next frame, sealed. The flag says whether the attempt advanced
    /// anything although no frame came out of it.
    fn next_write(
        &mut self,
        scratch: &mut ReactorScratch,
    ) -> Result<(Option<Box<Encoded>>, bool), &'static str> {
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
        if self.stalled.as_ref().is_some_and(|s| !s.reports.is_empty()) {
            return Ok((None, false));
        }
        self.core
            .pop_outgoing_batch(
                &mut scratch.write_batch,
                true,
                RECORD_BODY,
                MAX_COALESCE_FRAMES,
            )
            .map_err(|()| "output_producer_closed")?;
        if scratch.write_batch.is_empty() {
            return Ok((None, false));
        }
        let deadline = Instant::now() + WRITE_TIMEOUT;
        let mut batch = Vec::new();
        let (frame, ciphertext) = if scratch.write_batch.len() == 1 {
            let mut frame = scratch.write_batch.pop().expect("len == 1");
            if let Some(expiry) = frame.expiry.as_deref().copied() {
                match prepare_expiry(&mut frame.bytes, expiry, self.clock.as_ref()) {
                    Ok(()) => {}
                    Err(WriteFailure::NotStarted(reason)) => {
                        scratch
                            .reports_batch
                            .push(Report::Written(Written::NotStarted {
                                correlation: frame.correlation,
                                reason,
                            }));
                        return Ok((None, true));
                    }
                    Err(WriteFailure::Failed(code)) => {
                        self.machine.unsealed.push(frame.correlation);
                        return Err(code);
                    }
                }
            }
            let ciphertext = match frame_ciphertext(
                &mut self.state,
                &frame.bytes,
                frame.payload.as_ref().map(SharedBytes::as_slice),
            ) {
                Ok(ciphertext) => ciphertext,
                Err(code) => {
                    self.machine.unsealed.push(frame.correlation);
                    return Err(code);
                }
            };
            (frame, ciphertext)
        } else {
            scratch.coalesce_bytes.clear();
            for f in scratch.write_batch.drain(..) {
                batch.push((f.correlation, f.report, f.total_len()));
                f.append_to_vec(&mut scratch.coalesce_bytes);
            }
            let ciphertext = match frame_ciphertext(&mut self.state, &scratch.coalesce_bytes, None)
            {
                Ok(ciphertext) => ciphertext,
                Err(code) => {
                    self.machine
                        .unsealed
                        .extend(batch.into_iter().map(|(c, _, _)| c));
                    return Err(code);
                }
            };
            let frame = Write {
                correlation: None,
                bytes: FrameBytes::empty(),
                payload: None,
                expiry: None,
                report: WriteReport::None,
            };
            (frame, ciphertext)
        };
        #[cfg(feature = "test-hooks")]
        let fragment = frame.expiry.is_some()
            && frame.correlation.is_some()
            && self.armed("fragment-next-tls-publish");
        #[cfg(feature = "test-hooks")]
        if frame.expiry.is_some() && frame.correlation.is_some() {
            if MUTANT_TLS_EARLY_STARTED {
                scratch
                    .reports_batch
                    .push(Report::Written(Written::Started {
                        correlation: frame.correlation,
                    }));
            }
            // A held barrier keeps this reactor thread: test builds only.
            if self.armed("hold-next-tls-frame") {
                self.observe_boundary("tls.frame.sealed", &frame, 0)?;
            }
        }
        Ok((
            Some(Box::new(Encoded {
                frame,
                batch,
                ciphertext,
                offset: 0,
                deadline,
                #[cfg(feature = "test-hooks")]
                fragment,
            })),
            true,
        ))
    }

    fn output_turn(
        &mut self,
        output: &mut Option<Box<Encoded>>,
        scratch: &mut ReactorScratch,
    ) -> Result<bool, &'static str> {
        if output.is_none() {
            let (next, advanced) = self.next_write(scratch)?;
            *output = next;
            if output.is_none() {
                return Ok(advanced);
            }
        }
        let Some(encoded) = output.as_mut() else {
            return Ok(false);
        };
        if encoded.offset == 0
            && let Some(expiry) = encoded.frame.expiry.as_deref().copied()
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
                    scratch
                        .reports_batch
                        .push(Report::Written(Written::NotStarted {
                            correlation: encoded.frame.correlation,
                            reason,
                        }));
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
        let count = match (&self.core.stream).write(ciphertext) {
            Ok(0) => return Err("tls_write_zero"),
            Ok(count) => count,
            Err(error) if retryable(&error) => return Ok(false),
            Err(_) => return Err("tls_write_io"),
        };
        let first = encoded.offset == 0;
        if first && encoded.frame.expiry.is_some() && encoded.frame.correlation.is_some() {
            scratch
                .reports_batch
                .push(Report::Written(Written::Started {
                    correlation: encoded.frame.correlation,
                }));
        }
        encoded.offset += count;
        #[cfg(feature = "test-hooks")]
        if first && encoded.fragment {
            self.observe_boundary("tls.frame.first_bytes", &encoded.frame, count)?;
        }
        if encoded.offset == encoded.ciphertext.len() {
            if encoded.batch.is_empty() {
                super::Task::push_completion(
                    &mut scratch.reports_batch,
                    encoded.frame.correlation,
                    encoded.frame.report,
                    encoded.frame.total_len(),
                );
                *output = None;
            } else {
                let completed = output.take().expect("encoded batch");
                for (correlation, report, bytes) in completed.batch {
                    super::Task::push_completion(
                        &mut scratch.reports_batch,
                        correlation,
                        report,
                        bytes,
                    );
                }
            }
        }
        Ok(true)
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
        scratch.incoming_batch.clear();
        let Some((credit, parse, without_credit)) = self
            .core
            .plan_read(&mut self.stalled, &mut scratch.incoming_batch)
        else {
            return Ok(progressed);
        };
        let limit = scratch.incoming_batch.len() + parse;
        let mut heard_packet = false;
        let mut cached_fixed: Option<FrameObservation> = None;
        let mut parse_err: Option<&'static str> = None;
        let mut iterations = 0usize;
        while scratch.incoming_batch.len() < limit && iterations < BURST {
            iterations += 1;
            match self.machine.parser.packet(
                &mut Plaintext(&mut self.state),
                self.clock.as_ref(),
                &mut cached_fixed,
            ) {
                Ok(Some(packet)) => {
                    if matches!(packet, Incoming::Packet { .. }) {
                        heard_packet = true;
                    }
                    scratch.incoming_batch.push(packet);
                    progressed = true;
                    continue;
                }
                Ok(None) => {}
                Err(code) => {
                    parse_err = Some(code);
                    break;
                }
            }
            match self.read_ciphertext(READ_CHUNK, false) {
                Ok(0) => {}
                Ok(_) => {
                    cached_fixed = None;
                    progressed = true;
                    continue;
                }
                Err(code) => {
                    parse_err = Some(code);
                    break;
                }
            }
            if !self.machine.eof {
                break;
            }
            // After the end of the transport the TLS state refuses the next
            // plaintext read with the reason; one more pass obtains it.
            if self.machine.eof_reported {
                parse_err = Some("tls_eof");
                break;
            }
            self.machine.eof_reported = true;
            progressed = true;
        }
        if heard_packet {
            record_heard(&self.core);
        }
        super::ConnCore::park_unpaid_ack(
            &mut self.stalled,
            &mut scratch.incoming_batch,
            without_credit,
        );
        if !scratch.incoming_batch.is_empty() {
            self.core.push_incoming_batch(
                &mut scratch.incoming_batch,
                &mut self.stalled,
                credit,
            )?;
        }
        if let Some(code) = parse_err {
            return Err(code);
        }
        Ok(progressed)
    }

    fn active_turn(&mut self, scratch: &mut ReactorScratch) -> Result<Turn, &'static str> {
        scratch.reports_batch.clear();
        let mut progress = self
            .core
            .flush_reports(&mut scratch.reports_batch, &mut self.stalled)?;
        let mut output = self.machine.output.take();
        let mut written = Ok(());
        for _ in 0..BURST {
            match self.output_turn(&mut output, scratch) {
                Ok(true) => progress = true,
                Ok(false) => break,
                Err(code) => {
                    written = Err(code);
                    break;
                }
            }
            if !scratch.reports_batch.is_empty()
                && let Err(code) = self
                    .core
                    .flush_reports(&mut scratch.reports_batch, &mut self.stalled)
            {
                written = Err(code);
                break;
            }
        }
        if !scratch.reports_batch.is_empty()
            && let Err(code) = self
                .core
                .flush_reports(&mut scratch.reports_batch, &mut self.stalled)
        {
            written = written.and(Err(code));
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
        progress |= self.input_turn(scratch)?;
        Ok(Turn {
            progress,
            waiting: self.machine.output.is_some()
                || self.machine.control_pending.is_some()
                || self.machine.parser.is_partial()
                || self.stalled.as_ref().is_some_and(|s| !s.is_empty()),
        })
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
        match self.machine.phase {
            Phase::Handshake { start, received } => self.handshake_turn(start, received),
            Phase::Active => self.active_turn(scratch),
        }
    }

    fn finish(self: Box<Self>, code: &'static str) {
        let mut task = *self;
        task.close_notify(code);
        task.core.stop(code);
        let active = task
            .machine
            .output
            .map(|e| e.correlations())
            .unwrap_or_default()
            .into_iter()
            .chain(task.machine.unsealed);
        task.core.finish_task(task.stalled, code, active);
    }
}
