//! The control protocol between the supervisor and the child process
//! (plan `docs/missions/L4-process-topology.md`).
//!
//! Nothing here starts or stops a process: the protocol is written and
//! tested over any byte stream first. Three parts:
//!
//! - frames: a big-endian `u32` length, then at most [`MAX_FRAME`] bytes;
//!   a length is checked before anything is allocated;
//! - messages: the actions the supervisor sends, the reports the child
//!   writes on its control descriptor, and the evidence frames it writes
//!   on the descriptor of each origin; one binary encoding, without
//!   options, refused when a byte is left over;
//! - order and flow: the supervisor orders the evidence of one action by
//!   the emission number of the child ([`Sequencer`]); the child emits at
//!   most [`WINDOW`] records not yet acknowledged, and goes past a
//!   decisive record only once it is acknowledged ([`Window`]).
//!
//! The emission number is a claim of the child about the order of its own
//! emissions. It never gives the origin of a record (the descriptor does)
//! and it is bounded by the local order of each origin, which the
//! collector checks, and by the causal parents, which the oracle checks.

use std::collections::BTreeMap;
use std::io::{self, Read, Write};

use sha2::{Digest, Sha256};

/// The largest frame, header excluded. A working value: the run manifest
/// records it as an effective parameter.
pub const MAX_FRAME: usize = 64 * 1024;

/// The records the child may have emitted and not yet seen acknowledged.
/// A working value, recorded like [`MAX_FRAME`].
pub const WINDOW: u64 = 64;

/// Why a frame or a message is refused.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Refusal {
    /// The stream ended inside a frame.
    Truncated,
    /// A length above [`MAX_FRAME`], or a frame that would be.
    TooLarge(usize),
    /// A frame of no byte: every message has a tag.
    Empty,
    /// A tag no message has.
    UnknownTag(u8),
    /// The message ends before its fields do.
    Short,
    /// Bytes after the last field.
    Trailing(usize),
    /// A text field that is not UTF-8.
    NotText,
    /// The stream failed.
    Io(String),
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated => write!(formatter, "the stream ended inside a frame"),
            Self::TooLarge(length) => {
                write!(formatter, "a frame of {length} bytes, above {MAX_FRAME}")
            }
            Self::Empty => write!(formatter, "an empty frame"),
            Self::UnknownTag(tag) => write!(formatter, "unknown tag {tag}"),
            Self::Short => write!(formatter, "a message shorter than its fields"),
            Self::Trailing(count) => write!(formatter, "{count} bytes after the message"),
            Self::NotText => write!(formatter, "a text field that is not UTF-8"),
            Self::Io(error) => write!(formatter, "{error}"),
        }
    }
}

fn io_refusal(error: &io::Error) -> Refusal {
    Refusal::Io(error.to_string())
}

/// Writes one frame. A body above [`MAX_FRAME`] is refused, nothing is
/// written.
pub fn write_frame(stream: &mut impl Write, body: &[u8]) -> Result<(), Refusal> {
    if body.is_empty() {
        return Err(Refusal::Empty);
    }
    if body.len() > MAX_FRAME {
        return Err(Refusal::TooLarge(body.len()));
    }
    let length = u32::try_from(body.len()).map_err(|_| Refusal::TooLarge(body.len()))?;
    stream
        .write_all(&length.to_be_bytes())
        .and_then(|()| stream.write_all(body))
        .and_then(|()| stream.flush())
        .map_err(|error| io_refusal(&error))
}

/// Reads one frame: `None` when the stream ends between two frames. The
/// length is checked before the body is allocated.
pub fn read_frame(stream: &mut impl Read) -> Result<Option<Vec<u8>>, Refusal> {
    let mut header = [0_u8; 4];
    let mut filled = 0;
    while filled < header.len() {
        match stream.read(&mut header[filled..]) {
            Ok(0) if filled == 0 => return Ok(None),
            Ok(0) => return Err(Refusal::Truncated),
            Ok(count) => filled += count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(io_refusal(&error)),
        }
    }
    let length = u32::from_be_bytes(header) as usize;
    if length == 0 {
        return Err(Refusal::Empty);
    }
    if length > MAX_FRAME {
        return Err(Refusal::TooLarge(length));
    }
    let mut body = vec![0_u8; length];
    stream.read_exact(&mut body).map_err(|error| {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            Refusal::Truncated
        } else {
            io_refusal(&error)
        }
    })?;
    Ok(Some(body))
}

/// What the supervisor asks of the child. No identity goes down: a choice
/// is an index in the list the child published, with the digest of that
/// list; a fault is an index in the list the child was started with.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Action {
    /// Start with the rank of the start-up.
    Start { rank: u32 },
    /// One of the eligible choices the child published last.
    Choose { index: u32, digest: [u8; 32] },
    /// A connection of a client, by the generation the supervisor
    /// assigned.
    Connect { generation: u64, client: String },
    /// The bytes a peer sent on a connection, and the incarnation and
    /// sequence of the `PeerPacketSent` record that states them, which the
    /// transport names as the cause of its reception.
    Bytes {
        generation: u64,
        bytes: Vec<u8>,
        sent_incarnation: u64,
        sent_sequence: u64,
    },
    /// The armed deadline due first.
    FireDeadline,
    /// The command a fault holds may enter the writer.
    ReleaseHeld { fault: u32 },
    /// The result a fault holds is handed over.
    DeliverHeld { fault: u32 },
    /// The adapter leaves the barrier of a fault.
    LeaveBarrier { fault: u32 },
    /// The simulated crash of the store adapter (sim-store only).
    Crash,
    /// The durable state, read without the writer (sim-store only).
    Inspect,
    /// The end of the run.
    Stop,
    /// Every evidence frame up to `through`, included, is read.
    Acknowledge { through: u64 },
    /// The recovery of the process just started.
    Recover,
    /// The adapters move what waits in the box.
    Settle,
}

/// What the child writes on its control descriptor.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Report {
    /// The eligible choices, each described by the child, and the digest
    /// of that list ([`choices_digest`]).
    Eligible {
        choices: Vec<String>,
        digest: [u8; 32],
    },
    /// The action is over; the last evidence frame it emitted, if any.
    EndOfAction {
        action_sequence: u64,
        last_emission: Option<u64>,
    },
    /// The action is still in flight inside SQLite. Every frame through
    /// last_emission was accepted by the surviving collector and its ack
    /// consumed by the child. Only SIGKILL may follow this report.
    Parked {
        action_sequence: u64,
        last_emission: u64,
        fault: u32,
        point: String,
        settings: String,
    },
    /// The action was refused; nothing of it was done.
    Refused { reason: String },
    /// The run cannot go on in the child.
    Stopped { reason: String },
    /// A stable typed cause, including failures before Started.
    Failed {
        failure: super::failure::Failure,
        message: String,
    },
    /// The process started: its epoch, the elementary choices of its
    /// start-up for the choice log, and the settings the store engine read
    /// back, as JSON, for the supervisor to compare to the manifest.
    Started {
        epoch: u64,
        startup: Vec<String>,
        settings: String,
    },
    /// What the supervisor needs of the process after an action: whether
    /// an effect waits in the box, whether the store adapter stands at a
    /// barrier, the deadlines armed and not fired, the scenario fault of
    /// the barrier, the hits of each scenario fault in this incarnation,
    /// and the scenario faults that hold an item.
    State {
        busy: bool,
        halted: bool,
        armed: u32,
        barrier: Option<u32>,
        hits: Vec<u64>,
        held: Vec<u32>,
    },
    /// The transport wrote bytes to a client: the record that states it,
    /// by origin, incarnation and sequence, for the peer to name.
    Written {
        client: String,
        bytes: Vec<u8>,
        origin: String,
        incarnation: u64,
        sequence: u64,
    },
    /// The simulated crash is done: the clients whose connection was open.
    Crashed { open: Vec<String> },
    /// The process stopped: the events no record carries, counted by kind.
    Finished { unwritten: Vec<(String, u64)> },
}

/// One record of one origin, with the emission number of the child. The
/// origin is the descriptor the frame arrives on, never a field.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Evidence {
    pub emission: u64,
    pub record: Vec<u8>,
}

/// The digest of a list of eligible choices: each description, length
/// first, in the order of the list.
#[must_use]
pub fn choices_digest(choices: &[String]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update((choices.len() as u64).to_be_bytes());
    for choice in choices {
        hasher.update((choice.len() as u64).to_be_bytes());
        hasher.update(choice.as_bytes());
    }
    hasher.finalize().into()
}

/// The child resolves a `Choose` against the list it published last: an
/// index outside it, or a digest of another list, is refused.
pub fn resolve_choice<'a>(
    published: &'a [String],
    index: u32,
    digest: &[u8; 32],
) -> Result<&'a str, String> {
    if &choices_digest(published) != digest {
        return Err("the choice names a list the child did not publish last".to_owned());
    }
    published
        .get(index as usize)
        .map(String::as_str)
        .ok_or_else(|| format!("choice {index} of {} eligible", published.len()))
}

struct Writer(Vec<u8>);

impl Writer {
    fn tag(tag: u8) -> Self {
        Self(vec![tag])
    }
    fn u32(mut self, value: u32) -> Self {
        self.0.extend_from_slice(&value.to_be_bytes());
        self
    }
    fn u64(mut self, value: u64) -> Self {
        self.0.extend_from_slice(&value.to_be_bytes());
        self
    }
    fn digest(mut self, value: &[u8; 32]) -> Self {
        self.0.extend_from_slice(value);
        self
    }
    fn bytes(self, value: &[u8]) -> Self {
        let mut this = self.u32(u32::try_from(value.len()).unwrap_or(u32::MAX));
        this.0.extend_from_slice(value);
        this
    }
    fn flag(mut self, value: bool) -> Self {
        self.0.push(u8::from(value));
        self
    }
    fn count(self, value: usize) -> Self {
        self.u32(u32::try_from(value).unwrap_or(u32::MAX))
    }
    fn texts(self, values: &[String]) -> Self {
        let mut this = self.count(values.len());
        for value in values {
            this = this.bytes(value.as_bytes());
        }
        this
    }
}

struct Reader<'a> {
    rest: &'a [u8],
}

impl<'a> Reader<'a> {
    fn take(&mut self, count: usize) -> Result<&'a [u8], Refusal> {
        if self.rest.len() < count {
            return Err(Refusal::Short);
        }
        let (head, rest) = self.rest.split_at(count);
        self.rest = rest;
        Ok(head)
    }
    fn u32(&mut self) -> Result<u32, Refusal> {
        let bytes = self.take(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }
    fn u64(&mut self) -> Result<u64, Refusal> {
        let mut value = [0_u8; 8];
        value.copy_from_slice(self.take(8)?);
        Ok(u64::from_be_bytes(value))
    }
    fn digest(&mut self) -> Result<[u8; 32], Refusal> {
        let mut value = [0_u8; 32];
        value.copy_from_slice(self.take(32)?);
        Ok(value)
    }
    fn bytes(&mut self) -> Result<Vec<u8>, Refusal> {
        let length = self.u32()? as usize;
        Ok(self.take(length)?.to_vec())
    }
    fn text(&mut self) -> Result<String, Refusal> {
        String::from_utf8(self.bytes()?).map_err(|_| Refusal::NotText)
    }
    fn flag(&mut self) -> Result<bool, Refusal> {
        match self.take(1)?[0] {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Refusal::Short),
        }
    }
    /// A count of items of at least `least` bytes each: one the rest of the
    /// message cannot hold is refused before anything is allocated.
    fn count(&mut self, least: usize) -> Result<usize, Refusal> {
        let count = self.u32()? as usize;
        if count > self.rest.len() / least.max(1) {
            return Err(Refusal::Short);
        }
        Ok(count)
    }
    fn texts(&mut self) -> Result<Vec<String>, Refusal> {
        let count = self.count(4)?;
        let mut texts = Vec::with_capacity(count);
        for _ in 0..count {
            texts.push(self.text()?);
        }
        Ok(texts)
    }
    fn end<T>(self, value: T) -> Result<T, Refusal> {
        if self.rest.is_empty() {
            Ok(value)
        } else {
            Err(Refusal::Trailing(self.rest.len()))
        }
    }
}

fn split(body: &[u8]) -> Result<(u8, Reader<'_>), Refusal> {
    let (&tag, rest) = body.split_first().ok_or(Refusal::Empty)?;
    Ok((tag, Reader { rest }))
}

impl Action {
    /// The body of the frame that carries the action.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Self::Start { rank } => Writer::tag(1).u32(*rank),
            Self::Choose { index, digest } => Writer::tag(2).u32(*index).digest(digest),
            Self::Connect { generation, client } => {
                Writer::tag(3).u64(*generation).bytes(client.as_bytes())
            }
            Self::Bytes {
                generation,
                bytes,
                sent_incarnation,
                sent_sequence,
            } => Writer::tag(4)
                .u64(*generation)
                .bytes(bytes)
                .u64(*sent_incarnation)
                .u64(*sent_sequence),
            Self::FireDeadline => Writer::tag(5),
            Self::ReleaseHeld { fault } => Writer::tag(6).u32(*fault),
            Self::DeliverHeld { fault } => Writer::tag(7).u32(*fault),
            Self::LeaveBarrier { fault } => Writer::tag(8).u32(*fault),
            Self::Crash => Writer::tag(9),
            Self::Inspect => Writer::tag(10),
            Self::Stop => Writer::tag(11),
            Self::Acknowledge { through } => Writer::tag(12).u64(*through),
            Self::Recover => Writer::tag(13),
            Self::Settle => Writer::tag(14),
        }
        .0
    }

    /// The action a frame carries.
    pub fn decode(body: &[u8]) -> Result<Self, Refusal> {
        let (tag, mut reader) = split(body)?;
        let action = match tag {
            1 => Self::Start {
                rank: reader.u32()?,
            },
            2 => Self::Choose {
                index: reader.u32()?,
                digest: reader.digest()?,
            },
            3 => Self::Connect {
                generation: reader.u64()?,
                client: reader.text()?,
            },
            4 => Self::Bytes {
                generation: reader.u64()?,
                bytes: reader.bytes()?,
                sent_incarnation: reader.u64()?,
                sent_sequence: reader.u64()?,
            },
            5 => Self::FireDeadline,
            6 => Self::ReleaseHeld {
                fault: reader.u32()?,
            },
            7 => Self::DeliverHeld {
                fault: reader.u32()?,
            },
            8 => Self::LeaveBarrier {
                fault: reader.u32()?,
            },
            9 => Self::Crash,
            10 => Self::Inspect,
            11 => Self::Stop,
            12 => Self::Acknowledge {
                through: reader.u64()?,
            },
            13 => Self::Recover,
            14 => Self::Settle,
            other => return Err(Refusal::UnknownTag(other)),
        };
        reader.end(action)
    }
}

impl Report {
    /// The body of the frame that carries the report.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Self::Eligible { choices, digest } => {
                let mut writer = Writer::tag(1)
                    .digest(digest)
                    .u32(u32::try_from(choices.len()).unwrap_or(u32::MAX));
                for choice in choices {
                    writer = writer.bytes(choice.as_bytes());
                }
                writer
            }
            Self::EndOfAction {
                action_sequence,
                last_emission,
            } => match last_emission {
                None => Writer::tag(2).u64(*action_sequence),
                Some(emission) => Writer::tag(3).u64(*action_sequence).u64(*emission),
            },
            Self::Refused { reason } => Writer::tag(4).bytes(reason.as_bytes()),
            Self::Stopped { reason } => Writer::tag(5).bytes(reason.as_bytes()),
            Self::Failed { failure, message } => Writer::tag(12)
                .bytes(failure.to_wire().to_string().as_bytes())
                .bytes(message.as_bytes()),
            Self::Started {
                epoch,
                startup,
                settings,
            } => Writer::tag(6)
                .u64(*epoch)
                .texts(startup)
                .bytes(settings.as_bytes()),
            Self::State {
                busy,
                halted,
                armed,
                barrier,
                hits,
                held,
            } => {
                let mut writer = Writer::tag(7).flag(*busy).flag(*halted).u32(*armed);
                writer = match barrier {
                    None => writer.flag(false),
                    Some(fault) => writer.flag(true).u32(*fault),
                };
                writer = writer.count(hits.len());
                for hit in hits {
                    writer = writer.u64(*hit);
                }
                writer = writer.count(held.len());
                for fault in held {
                    writer = writer.u32(*fault);
                }
                writer
            }
            Self::Written {
                client,
                bytes,
                origin,
                incarnation,
                sequence,
            } => Writer::tag(8)
                .bytes(client.as_bytes())
                .bytes(bytes)
                .bytes(origin.as_bytes())
                .u64(*incarnation)
                .u64(*sequence),
            Self::Crashed { open } => Writer::tag(9).texts(open),
            Self::Finished { unwritten } => {
                let mut writer = Writer::tag(10).count(unwritten.len());
                for (kind, count) in unwritten {
                    writer = writer.bytes(kind.as_bytes()).u64(*count);
                }
                writer
            }
            Self::Parked {
                action_sequence,
                last_emission,
                fault,
                point,
                settings,
            } => Writer::tag(11)
                .u64(*action_sequence)
                .u64(*last_emission)
                .u32(*fault)
                .bytes(point.as_bytes())
                .bytes(settings.as_bytes()),
        }
        .0
    }

    /// The report a frame carries.
    pub fn decode(body: &[u8]) -> Result<Self, Refusal> {
        let (tag, mut reader) = split(body)?;
        let report = match tag {
            1 => {
                let digest = reader.digest()?;
                let count = reader.u32()? as usize;
                // Each choice takes at least its length: a count the frame
                // cannot hold is refused before anything is allocated.
                if count > reader.rest.len() / 4 {
                    return Err(Refusal::Short);
                }
                let mut choices = Vec::with_capacity(count);
                for _ in 0..count {
                    choices.push(reader.text()?);
                }
                Self::Eligible { choices, digest }
            }
            2 => Self::EndOfAction {
                action_sequence: reader.u64()?,
                last_emission: None,
            },
            3 => Self::EndOfAction {
                action_sequence: reader.u64()?,
                last_emission: Some(reader.u64()?),
            },
            4 => Self::Refused {
                reason: reader.text()?,
            },
            5 => Self::Stopped {
                reason: reader.text()?,
            },
            6 => Self::Started {
                epoch: reader.u64()?,
                startup: reader.texts()?,
                settings: reader.text()?,
            },
            7 => {
                let busy = reader.flag()?;
                let halted = reader.flag()?;
                let armed = reader.u32()?;
                let barrier = if reader.flag()? {
                    Some(reader.u32()?)
                } else {
                    None
                };
                let count = reader.count(8)?;
                let mut hits = Vec::with_capacity(count);
                for _ in 0..count {
                    hits.push(reader.u64()?);
                }
                let count = reader.count(4)?;
                let mut held = Vec::with_capacity(count);
                for _ in 0..count {
                    held.push(reader.u32()?);
                }
                Self::State {
                    busy,
                    halted,
                    armed,
                    barrier,
                    hits,
                    held,
                }
            }
            8 => Self::Written {
                client: reader.text()?,
                bytes: reader.bytes()?,
                origin: reader.text()?,
                incarnation: reader.u64()?,
                sequence: reader.u64()?,
            },
            9 => Self::Crashed {
                open: reader.texts()?,
            },
            10 => {
                let count = reader.count(12)?;
                let mut unwritten = Vec::with_capacity(count);
                for _ in 0..count {
                    unwritten.push((reader.text()?, reader.u64()?));
                }
                Self::Finished { unwritten }
            }
            11 => Self::Parked {
                action_sequence: reader.u64()?,
                last_emission: reader.u64()?,
                fault: reader.u32()?,
                point: reader.text()?,
                settings: reader.text()?,
            },
            12 => {
                let raw = reader.bytes()?;
                let value = crate::strict::parse(&raw).map_err(|_| Refusal::Short)?;
                let failure = super::failure::Failure::from_wire(&value).ok_or(Refusal::Short)?;
                let message = reader.text()?;
                if message.len() > super::failure::MAX_FAILURE_MESSAGE {
                    return Err(Refusal::TooLarge(message.len()));
                }
                Self::Failed { failure, message }
            }
            other => return Err(Refusal::UnknownTag(other)),
        };
        reader.end(report)
    }
}

impl Evidence {
    /// The body of the frame that carries the record.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut body = self.emission.to_be_bytes().to_vec();
        body.extend_from_slice(&self.record);
        body
    }

    /// The evidence a frame carries: the emission number, then the record
    /// as the child wrote it, which must not be empty.
    pub fn decode(body: &[u8]) -> Result<Self, Refusal> {
        let mut reader = Reader { rest: body };
        let emission = reader.u64()?;
        if reader.rest.is_empty() {
            return Err(Refusal::Short);
        }
        Ok(Self {
            emission,
            record: reader.rest.to_vec(),
        })
    }
}

/// Why the supervisor refuses the evidence of an action.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum OrderRefusal {
    /// Two frames carry the same emission number.
    Duplicate(u64),
    /// A number of an earlier action of this incarnation.
    AlreadySeen(u64),
    /// A number is missing between the first expected and the last.
    Gap { missing: u64 },
    /// The end of the action names another last emission than the frames.
    EndMismatch {
        stated: Option<u64>,
        found: Option<u64>,
    },
}

impl std::fmt::Display for OrderRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Duplicate(number) => write!(formatter, "emission {number} arrived twice"),
            Self::AlreadySeen(number) => {
                write!(formatter, "emission {number} belongs to an earlier action")
            }
            Self::Gap { missing } => write!(formatter, "emission {missing} is missing"),
            Self::EndMismatch { stated, found } => write!(
                formatter,
                "the action ends at emission {stated:?}, its frames at {found:?}"
            ),
        }
    }
}

/// The supervisor's side of the order of the evidence: the frames of one
/// action, from every descriptor, ordered by the emission number when the
/// child ends the action.
#[derive(Debug, Default)]
pub struct Sequencer<O> {
    next: u64,
    batch: BTreeMap<u64, (O, Vec<u8>)>,
    duplicate: Option<u64>,
    /// Last emission already retained during this action, before ack.
    retained: Option<u64>,
}

impl<O: Clone> Sequencer<O> {
    /// A new child incarnation: its emissions start at 0.
    #[must_use]
    pub fn new() -> Self {
        Self {
            next: 0,
            batch: BTreeMap::new(),
            duplicate: None,
            retained: None,
        }
    }

    /// A frame read on the descriptor of `origin`.
    pub fn receive(&mut self, origin: O, evidence: Evidence) {
        if self.batch.contains_key(&evidence.emission) {
            self.duplicate.get_or_insert(evidence.emission);
            return;
        }
        self.batch
            .insert(evidence.emission, (origin, evidence.record));
    }

    /// Take a contiguous prefix while the action is still in flight.
    /// The caller must retain it before acknowledging it to the child.
    pub fn take_contiguous(&mut self) -> Result<Vec<(O, Vec<u8>)>, OrderRefusal> {
        if let Some(number) = self.duplicate.take() {
            return Err(OrderRefusal::Duplicate(number));
        }
        if let Some((&number, _)) = self.batch.first_key_value()
            && number < self.next
        {
            return Err(OrderRefusal::AlreadySeen(number));
        }
        let mut ready = Vec::new();
        while let Some(value) = self.batch.remove(&self.next) {
            self.retained = Some(self.next);
            self.next += 1;
            ready.push(value);
        }
        Ok(ready)
    }

    /// The end of the action: the batch in emission order, or why it is
    /// refused. A refused batch is dropped; the trace is then incomplete.
    pub fn end_action(
        &mut self,
        last_emission: Option<u64>,
    ) -> Result<Vec<(O, Vec<u8>)>, OrderRefusal> {
        let batch = std::mem::take(&mut self.batch);
        if let Some(number) = self.duplicate.take() {
            return Err(OrderRefusal::Duplicate(number));
        }
        if let Some((&first, _)) = batch.first_key_value()
            && first < self.next
        {
            return Err(OrderRefusal::AlreadySeen(first));
        }
        let found = batch
            .last_key_value()
            .map(|(&last, _)| last)
            .or(self.retained.take());
        if found != last_emission {
            return Err(OrderRefusal::EndMismatch {
                stated: last_emission,
                found,
            });
        }
        for (expected, &number) in (self.next..).zip(batch.keys()) {
            if number != expected {
                return Err(OrderRefusal::Gap { missing: expected });
            }
        }
        if let Some(last) = found {
            self.next = last + 1;
        }
        Ok(batch.into_values().collect())
    }

    /// Whether no frame waits to be ordered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.batch.is_empty() && self.duplicate.is_none()
    }

    /// The first emission number the next action may carry.
    #[must_use]
    pub const fn next(&self) -> u64 {
        self.next
    }
}

/// The child's side of the flow: at most [`WINDOW`] records emitted and
/// not yet acknowledged, and no step past a decisive record before it is
/// acknowledged.
#[derive(Debug, Default)]
pub struct Window {
    emitted: u64,
    acknowledged: u64,
}

impl Window {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            emitted: 0,
            acknowledged: 0,
        }
    }

    /// Whether one more record may be emitted now.
    #[must_use]
    pub const fn open(&self) -> bool {
        self.emitted - self.acknowledged < WINDOW
    }

    /// Takes the emission number of the next record, if the window is
    /// open.
    pub fn emit(&mut self) -> Option<u64> {
        if !self.open() {
            return None;
        }
        let number = self.emitted;
        self.emitted += 1;
        Some(number)
    }

    /// The supervisor read every frame up to `through`. An acknowledgement
    /// of a frame not emitted, or older than one already received, is
    /// refused.
    pub fn acknowledge(&mut self, through: u64) -> Result<(), String> {
        let after = through
            .checked_add(1)
            .ok_or("an acknowledgement past every number")?;
        if after > self.emitted {
            return Err(format!(
                "acknowledgement of emission {through}, {} emitted",
                self.emitted
            ));
        }
        if after < self.acknowledged {
            return Err(format!(
                "acknowledgement of emission {through} after {}",
                self.acknowledged - 1
            ));
        }
        self.acknowledged = after;
        Ok(())
    }

    /// Whether the child may go past the point the record `emission`
    /// describes: a decisive record must be acknowledged first.
    #[must_use]
    pub const fn past(&self, emission: u64, decisive: bool) -> bool {
        !decisive || emission < self.acknowledged
    }

    /// Whether every emitted record is acknowledged: the only state, with
    /// a declared point, in which a kill is allowed.
    #[must_use]
    pub const fn drained(&self) -> bool {
        self.emitted == self.acknowledged
    }
}

/// Whether a record of `origin` and `kind` is decisive: a record of store
/// truth or a reached fault.
#[must_use]
pub fn decisive(origin: &str, kind: &str) -> bool {
    origin == "store-evidence" || kind == "FaultHit"
}

#[cfg(test)]
mod tests {
    use super::*;

    fn every_action() -> Vec<Action> {
        vec![
            Action::Start { rank: 2 },
            Action::Choose {
                index: 3,
                digest: [7; 32],
            },
            Action::Connect {
                generation: 9,
                client: "producer".to_owned(),
            },
            Action::Bytes {
                generation: 9,
                bytes: vec![0x10, 0x00],
                sent_incarnation: 1,
                sent_sequence: 4,
            },
            Action::FireDeadline,
            Action::ReleaseHeld { fault: 1 },
            Action::DeliverHeld { fault: 2 },
            Action::LeaveBarrier { fault: 0 },
            Action::Crash,
            Action::Inspect,
            Action::Stop,
            Action::Acknowledge { through: 63 },
            Action::Recover,
            Action::Settle,
        ]
    }

    fn every_report() -> Vec<Report> {
        let choices = vec!["Packet(1)".to_owned(), "Frame".to_owned()];
        vec![
            Report::Eligible {
                digest: choices_digest(&choices),
                choices,
            },
            Report::Eligible {
                choices: Vec::new(),
                digest: choices_digest(&[]),
            },
            Report::EndOfAction {
                action_sequence: 9,
                last_emission: None,
            },
            Report::EndOfAction {
                action_sequence: 9,
                last_emission: Some(41),
            },
            Report::Parked {
                action_sequence: 9,
                last_emission: 41,
                fault: 2,
                point: "store.commit.prepared".to_owned(),
                settings: "{\"kill_points\":true}".to_owned(),
            },
            Report::Refused {
                reason: "no deadline armed".to_owned(),
            },
            Report::Stopped {
                reason: "core fault".to_owned(),
            },
            Report::Failed {
                failure: super::super::failure::Failure::Read {
                    cause: mqttiumd_store_api::ReadError::InvalidData.into(),
                    phase: mqttiumd_store_api::ReadPhase::Recovery,
                    process_incarnation: 2,
                    page: Some(1),
                },
                message: "durable recovery refused".to_owned(),
            },
            Report::Started {
                epoch: 2,
                startup: vec!["EnterWriter(Opening(1))".to_owned()],
                settings: "{\"journal_mode\":\"wal\"}".to_owned(),
            },
            Report::State {
                busy: true,
                halted: false,
                armed: 1,
                barrier: Some(2),
                hits: vec![1, 0, 3],
                held: vec![0, 2],
            },
            Report::State {
                busy: false,
                halted: false,
                armed: 0,
                barrier: None,
                hits: Vec::new(),
                held: Vec::new(),
            },
            Report::Written {
                client: "producer".to_owned(),
                bytes: vec![0x20, 0x03, 0, 0, 0],
                origin: "transport".to_owned(),
                incarnation: 2,
                sequence: 7,
            },
            Report::Crashed {
                open: vec!["producer".to_owned()],
            },
            Report::Finished {
                unwritten: vec![("ResultDelivered".to_owned(), 3)],
            },
        ]
    }

    #[test]
    fn an_in_flight_prefix_is_ordered_before_its_ack_and_the_end_still_checks() {
        let mut sequencer = Sequencer::new();
        let frame = |emission| Evidence {
            emission,
            record: vec![emission as u8],
        };
        sequencer.receive("store", frame(1));
        assert!(sequencer.take_contiguous().unwrap().is_empty());
        sequencer.receive("core", frame(0));
        assert_eq!(
            sequencer.take_contiguous().unwrap(),
            vec![("core", vec![0]), ("store", vec![1])]
        );
        assert_eq!(sequencer.next(), 2);
        assert!(sequencer.end_action(Some(1)).unwrap().is_empty());
        sequencer.receive("store", frame(2));
        assert_eq!(
            sequencer.take_contiguous().unwrap(),
            vec![("store", vec![2])]
        );
        assert!(matches!(
            sequencer.end_action(Some(3)),
            Err(OrderRefusal::EndMismatch { .. })
        ));
    }

    #[test]
    fn duplicate_evidence_is_refused_after_its_prefix_was_retained() {
        let mut sequencer = Sequencer::new();
        let frame = || Evidence {
            emission: 0,
            record: vec![1],
        };
        sequencer.receive("core", frame());
        assert_eq!(sequencer.take_contiguous().unwrap().len(), 1);
        sequencer.receive("core", frame());
        assert!(matches!(
            sequencer.take_contiguous(),
            Err(OrderRefusal::AlreadySeen(0))
        ));
    }

    #[test]
    fn every_message_reads_back_as_written() {
        for action in every_action() {
            assert_eq!(Action::decode(&action.encode()), Ok(action.clone()));
        }
        for report in every_report() {
            assert_eq!(Report::decode(&report.encode()), Ok(report.clone()));
        }
        let evidence = Evidence {
            emission: 5,
            record: b"{\"kind\":\"FaultHit\"}".to_vec(),
        };
        assert_eq!(Evidence::decode(&evidence.encode()), Ok(evidence));
    }

    #[test]
    fn a_message_with_a_byte_more_or_less_is_refused() {
        for action in every_action() {
            let mut longer = action.encode();
            longer.push(0);
            assert_eq!(
                Action::decode(&longer),
                Err(Refusal::Trailing(1)),
                "{action:?}"
            );
            let body = action.encode();
            if body.len() > 1 {
                assert_eq!(
                    Action::decode(&body[..body.len() - 1]),
                    Err(Refusal::Short),
                    "{action:?}"
                );
            }
        }
        for report in every_report() {
            let mut longer = report.encode();
            longer.push(0);
            assert_eq!(
                Report::decode(&longer),
                Err(Refusal::Trailing(1)),
                "{report:?}"
            );
        }
        assert_eq!(Action::decode(&[0]), Err(Refusal::UnknownTag(0)));
        assert_eq!(Action::decode(&[15]), Err(Refusal::UnknownTag(15)));
        assert_eq!(Report::decode(&[13]), Err(Refusal::UnknownTag(13)));
        // A flag is 0 or 1.
        assert_eq!(
            Report::decode(&[7, 2, 0, 0, 0, 0, 0, 0]),
            Err(Refusal::Short)
        );
        assert_eq!(Action::decode(&[]), Err(Refusal::Empty));
        assert_eq!(Evidence::decode(&5_u64.to_be_bytes()), Err(Refusal::Short));
        let mut not_text = vec![4, 0, 0, 0, 1];
        not_text.push(0xff);
        assert_eq!(Report::decode(&not_text), Err(Refusal::NotText));
    }

    #[test]
    fn a_count_of_choices_the_frame_cannot_hold_is_refused_before_allocation() {
        let mut body = vec![1];
        body.extend_from_slice(&[0; 32]);
        body.extend_from_slice(&u32::MAX.to_be_bytes());
        assert_eq!(Report::decode(&body), Err(Refusal::Short));
    }

    #[test]
    fn frames_are_bounded_and_a_cut_frame_is_refused() {
        let mut stream = Vec::new();
        write_frame(&mut stream, &[1; MAX_FRAME]).unwrap();
        write_frame(&mut stream, &[2]).unwrap();
        let mut reading = stream.as_slice();
        assert_eq!(read_frame(&mut reading).unwrap().unwrap().len(), MAX_FRAME);
        assert_eq!(read_frame(&mut reading).unwrap(), Some(vec![2]));
        assert_eq!(read_frame(&mut reading).unwrap(), None);

        let mut written = Vec::new();
        assert_eq!(
            write_frame(&mut written, &vec![0; MAX_FRAME + 1]),
            Err(Refusal::TooLarge(MAX_FRAME + 1))
        );
        assert_eq!(write_frame(&mut written, &[]), Err(Refusal::Empty));
        assert!(written.is_empty(), "nothing is written for a refused frame");

        // A length above the bound is refused from its header alone.
        let header = u32::try_from(MAX_FRAME + 1).unwrap().to_be_bytes();
        assert_eq!(
            read_frame(&mut header.as_slice()),
            Err(Refusal::TooLarge(MAX_FRAME + 1))
        );
        assert_eq!(
            read_frame(&mut [0_u8, 0].as_slice()),
            Err(Refusal::Truncated)
        );
        assert_eq!(
            read_frame(&mut [0_u8, 0, 0, 3, 1].as_slice()),
            Err(Refusal::Truncated)
        );
        assert_eq!(read_frame(&mut [0_u8; 4].as_slice()), Err(Refusal::Empty));
    }

    #[test]
    fn a_choice_names_the_list_the_child_published_last() {
        let published = vec!["Packet(1)".to_owned(), "Frame".to_owned()];
        let digest = choices_digest(&published);
        assert_eq!(resolve_choice(&published, 1, &digest), Ok("Frame"));
        assert!(resolve_choice(&published, 2, &digest).is_err());
        let older = choices_digest(&["Frame".to_owned(), "Packet(1)".to_owned()]);
        assert_ne!(older, digest, "the digest depends on the order");
        assert!(resolve_choice(&published, 0, &older).is_err());
        // The length comes first: two lists with the same bytes differ.
        assert_ne!(
            choices_digest(&["ab".to_owned()]),
            choices_digest(&["a".to_owned(), "b".to_owned()])
        );
    }

    fn evidence(emission: u64) -> Evidence {
        Evidence {
            emission,
            record: emission.to_string().into_bytes(),
        }
    }

    #[test]
    fn the_evidence_of_an_action_is_ordered_by_emission_across_descriptors() {
        let mut sequencer = Sequencer::new();
        sequencer.receive("store-evidence", evidence(2));
        sequencer.receive("broker-core", evidence(0));
        sequencer.receive("store-adapter", evidence(1));
        let batch = sequencer.end_action(Some(2)).unwrap();
        let origins: Vec<&str> = batch.iter().map(|(origin, _)| *origin).collect();
        assert_eq!(origins, ["broker-core", "store-adapter", "store-evidence"]);
        assert_eq!(sequencer.next(), 3);
        // An action that emitted nothing.
        assert_eq!(sequencer.end_action(None).unwrap(), []);
        sequencer.receive("broker-core", evidence(3));
        assert_eq!(sequencer.end_action(Some(3)).unwrap().len(), 1);
    }

    #[test]
    fn a_gap_a_duplicate_an_old_number_or_a_false_end_is_refused() {
        let mut gap = Sequencer::new();
        gap.receive("broker-core", evidence(0));
        gap.receive("broker-core", evidence(2));
        assert_eq!(
            gap.end_action(Some(2)),
            Err(OrderRefusal::Gap { missing: 1 })
        );

        let mut late_start = Sequencer::<&str>::new();
        late_start.receive("broker-core", evidence(1));
        assert_eq!(
            late_start.end_action(Some(1)),
            Err(OrderRefusal::Gap { missing: 0 })
        );

        let mut twice = Sequencer::new();
        twice.receive("broker-core", evidence(0));
        twice.receive("store-adapter", evidence(0));
        assert_eq!(twice.end_action(Some(0)), Err(OrderRefusal::Duplicate(0)));

        let mut old = Sequencer::new();
        old.receive("broker-core", evidence(0));
        old.end_action(Some(0)).unwrap();
        old.receive("broker-core", evidence(0));
        assert_eq!(old.end_action(Some(0)), Err(OrderRefusal::AlreadySeen(0)));

        let mut short = Sequencer::new();
        short.receive("broker-core", evidence(0));
        assert_eq!(
            short.end_action(Some(1)),
            Err(OrderRefusal::EndMismatch {
                stated: Some(1),
                found: Some(0)
            })
        );
        let mut silent = Sequencer::<&str>::new();
        assert_eq!(
            silent.end_action(Some(0)),
            Err(OrderRefusal::EndMismatch {
                stated: Some(0),
                found: None
            })
        );
    }

    #[test]
    fn a_new_incarnation_numbers_its_emissions_from_zero() {
        let mut first = Sequencer::new();
        first.receive("broker-core", evidence(0));
        first.end_action(Some(0)).unwrap();
        let mut restarted = Sequencer::new();
        restarted.receive("broker-core", evidence(0));
        assert!(restarted.end_action(Some(0)).is_ok());
    }

    #[test]
    fn the_window_bounds_what_is_not_acknowledged() {
        let mut window = Window::new();
        for expected in 0..WINDOW {
            assert_eq!(window.emit(), Some(expected));
        }
        assert!(!window.open());
        assert_eq!(window.emit(), None, "a full window makes the child wait");
        window.acknowledge(9).unwrap();
        assert_eq!(window.emit(), Some(WINDOW));
        assert!(window.acknowledge(WINDOW + 1).is_err(), "not emitted");
        assert!(window.acknowledge(3).is_err(), "older than one received");
        assert!(!window.drained());
        window.acknowledge(WINDOW).unwrap();
        assert!(window.drained());
    }

    #[test]
    fn the_child_goes_past_a_decisive_record_only_once_it_is_acknowledged() {
        let mut window = Window::new();
        let routine = window.emit().unwrap();
        let commit = window.emit().unwrap();
        assert!(window.past(routine, decisive("broker-core", "FrameEligible")));
        assert!(!window.past(commit, decisive("store-evidence", "StoreCommitStable")));
        window.acknowledge(commit).unwrap();
        assert!(window.past(commit, true));
        assert!(decisive("fault-injector", "FaultHit"));
        assert!(!decisive("store-adapter", "CommandSubmitted"));
    }
}
