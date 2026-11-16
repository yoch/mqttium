//! One bounded owner reactor, independent socket tasks and a single SQLite writer.
use crate::{
    codec::{self, Bounds, Packet},
    events::{Events, operation},
    net,
    writer::{Failure, Hooks, Request, Response, Session, Writer},
};
use mqttiumd_contract::capacity::{PayloadBytes, WorkUnits};
use mqttiumd_contract::effects::{
    CloseReason, Effect, EffectSink, FrameProgress, Input, OwnedEffect, OwnerAuthority,
    RecoveredFact, SubmitBlock, SubmitResult,
};
use mqttiumd_contract::ids::{
    AccountId, ConnectionGeneration, ConnectionKey, DeadlineToken, EffectId, LogicalDuration,
    LogicalOwnerId, LogicalTime, OperationId,
};
use mqttiumd_contract::packet::{InboundPacket, OutboundFrame};
use mqttiumd_contract::step::{StepOutcome, TurnGrant};
use mqttiumd_core::owner::{Owner, OwnerConfig};
use mqttiumd_store_api::{ReadError, ReadPhase};
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

const CONNECTIONS: usize = 16;
const IN_FLIGHT: usize = 16;
const TIMERS: usize = 512;
const INPUTS: usize = 128;
const BOUNDS: Bounds = Bounds {
    max_packet: 65536,
    max_client_id: 1024,
    max_topic: 1024,
    max_payload: 32768,
};
#[cfg(feature = "test-hooks")]
const MUTANT_EARLY_ACK: bool = false;
#[cfg(feature = "test-hooks")]
const MUTANT_ALLOW_STALE: bool = false;

pub struct Config {
    pub db: PathBuf,
    pub listen: SocketAddr,
    pub events: PathBuf,
    pub hooks: Hooks,
}
struct Peer {
    io: net::Connection,
    created: Instant,
    last_packet: Instant,
    connected: bool,
    pending: bool,
    keep_alive: u16,
    max_packet: usize,
    ingress: u64,
    receiving: BTreeSet<u16>,
}
struct PendingSession {
    connection: ConnectionGeneration,
    client: Vec<u8>,
}
struct Runtime<'a> {
    owner: Owner,
    _authority: OwnerAuthority,
    writer: &'a mut Writer,
    events: Events,
    peers: BTreeMap<ConnectionGeneration, Peer>,
    retiring: Vec<net::Connection>,
    sessions: BTreeMap<Vec<u8>, Session>,
    session_requests: BTreeMap<u64, PendingSession>,
    next_session_request: u64,
    generation: ConnectionGeneration,
    inputs: VecDeque<Input>,
    timers: BTreeMap<DeadlineToken, Instant>,
    inflight: BTreeMap<EffectId, OperationId>,
    started: Instant,
    captured: Option<(ConnectionGeneration, codec::Connect)>,
    #[cfg(feature = "test-hooks")]
    hooks: Hooks,
    frames: BTreeMap<EffectId, (ConnectionKey, &'static str, Option<u16>)>,
}
impl Runtime<'_> {
    fn queue(&mut self, input: Input) -> Result<(), Failure> {
        if self.inputs.len() >= INPUTS {
            return Err(Failure::plain("core_input_budget"));
        }
        self.inputs.push_back(input);
        Ok(())
    }
    fn close(
        &mut self,
        generation: ConnectionGeneration,
        code: &'static str,
    ) -> Result<(), Failure> {
        if let Some(peer) = self.peers.remove(&generation) {
            peer.io.close();
            self.retiring.push(peer.io);
            self.events
                .emit(
                    "connection_closed",
                    json!({"connection":generation.get().to_string(),"details":{"code":code}}),
                )
                .map_err(event_failure)?;
            self.queue(Input::ConnectionClosed {
                connection: generation,
            })?;
        }
        Ok(())
    }
    fn drain_inputs(&mut self) -> Result<(), Failure> {
        for _ in 0..64 {
            let Some(input) = self.inputs.pop_front() else {
                break;
            };
            let input = match input {
                Input::Packet {
                    connection,
                    ingress_order,
                    packet,
                } => {
                    if !self.admit_generation(connection)? {
                        continue;
                    }
                    Input::Packet {
                        connection,
                        ingress_order,
                        packet,
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
            match self.owner.step(
                input,
                TurnGrant {
                    work_limit: WorkUnits::new(65536),
                },
            ) {
                StepOutcome::Applied { .. } => {}
                StepOutcome::NeedCapacity { input, .. } => {
                    self.inputs.push_front(input);
                    break;
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
            self.dispatch()?;
        }
        Ok(())
    }
    fn dispatch(&mut self) -> Result<(), Failure> {
        let mut sink = Sink {
            writer: self.writer,
            peers: &mut self.peers,
            events: &self.events,
            inflight: &mut self.inflight,
            timers: &mut self.timers,
            inputs: &mut self.inputs,
            frames: &mut self.frames,
            close: Vec::new(),
            error: None,
        };
        let _ = self.owner.dispatch(&mut sink);
        let close = std::mem::take(&mut sink.close);
        let error = sink.error.take();
        drop(sink);
        if let Some(error) = error {
            return Err(error);
        }
        for (generation, code) in close {
            self.close(generation, code)?;
        }
        Ok(())
    }
    fn writer_results(&mut self) -> Result<(), Failure> {
        for _ in 0..64 {
            let response = match self.writer.receiver.try_recv() {
                Ok(r) => r,
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    return Err(Failure::plain("writer_disconnected"));
                }
            };
            match response {
                Response::Store {
                    effect,
                    operation,
                    reply,
                } => {
                    if self.inflight.remove(&effect) != Some(operation) {
                        return Err(Failure::plain("uncorrelated_store_result"));
                    }
                    self.queue(Input::StoreResult {
                        effect,
                        operation,
                        reply,
                    })?;
                }
                Response::Session { request, result } => {
                    let pending = self
                        .session_requests
                        .remove(&request)
                        .ok_or_else(|| Failure::plain("uncorrelated_session_result"))?;
                    match result {
                        Ok(session) => {
                            if pending.client != session.client.as_slice() {
                                return Err(Failure::plain("session_identity_mismatch"));
                            }
                            self.events.emit("session_established",json!({"connection":pending.connection.get().to_string(),"details":{"request":request.to_string(),"session":session.id.get().to_string(),"session_present":session.present}})).map_err(event_failure)?;
                            let connection = self.peers.get_mut(&pending.connection).map(|p| {
                                p.pending = false;
                                p.connected = true;
                                pending.connection
                            });
                            self.queue(Input::SessionEstablished {
                                connection,
                                session: session.id,
                                client_id: session.client.clone(),
                                revision: session.revision,
                                session_present: session.present,
                            })?;
                            self.sessions.insert(pending.client, session);
                        }
                        Err(failure) => {
                            let mut fields = failure.fields();
                            fields["connection"] = json!(pending.connection.get().to_string());
                            fields["details"]["request"] = json!(request.to_string());
                            self.events
                                .emit("session_refused", fields)
                                .map_err(event_failure)?;
                            self.close(pending.connection, failure.code)?;
                        }
                    }
                }
                Response::Recovery(_) | Response::Stopped => {
                    return Err(Failure::plain("unexpected_writer_result"));
                }
            }
        }
        Ok(())
    }
    fn connect(
        &mut self,
        generation: ConnectionGeneration,
        connect: codec::Connect,
    ) -> Result<(), Failure> {
        if let Some(p) = self.peers.get_mut(&generation) {
            if p.connected || p.pending {
                return self.close(generation, "second_connect");
            }
            p.keep_alive = connect.keep_alive;
            p.max_packet = connect.max_packet_size as usize;
            p.last_packet = Instant::now();
        }
        if self.captured.is_none() {
            self.events.emit("input_captured",json!({"connection":generation.get().to_string(),"details":{"packet_kind":"CONNECT"}})).map_err(event_failure)?;
            self.captured = Some((generation, connect.clone()));
        }
        if self.sessions.contains_key(connect.client_id.as_slice()) {
            if let Some(p) = self.peers.get_mut(&generation) {
                p.connected = true;
            }
            self.queue(Input::Packet {
                connection: generation,
                ingress_order: 0,
                packet: InboundPacket::ConnectResume {
                    client_id: connect.client_id,
                },
            })?;
        } else {
            if self.session_requests.len() >= IN_FLIGHT {
                return self.close(generation, "session_request_limit");
            }
            let request = self
                .next_session_request
                .checked_add(1)
                .ok_or_else(|| Failure::plain("session_request_exhausted"))?;
            let client = connect.client_id.as_slice().to_vec();
            match self.writer.sender.try_send(Request::Session {
                request,
                client: connect.client_id,
            }) {
                Ok(()) => {
                    self.next_session_request = request;
                    self.session_requests.insert(
                        request,
                        PendingSession {
                            connection: generation,
                            client,
                        },
                    );
                    if let Some(p) = self.peers.get_mut(&generation) {
                        p.pending = true;
                    }
                }
                Err(_) => self.close(generation, "writer_queue_full")?,
            }
        }
        Ok(())
    }
    fn packet(
        &mut self,
        generation: ConnectionGeneration,
        first: u8,
        body: Vec<u8>,
    ) -> Result<(), Failure> {
        if !self.admit_generation(generation)? {
            return Ok(());
        }
        let decoded = match codec::decode(first, &body, BOUNDS) {
            Ok(v) => v,
            Err(error) => {
                self.events.emit("packet_refused",json!({"connection":generation.get().to_string(),"details":{"code":error.code(),"reason":error.reason_code()}})).map_err(event_failure)?;
                // A refusal never blocks the reactor; if the bounded queue cannot accept it, close.
                if let Some(p) = self.peers.get(&generation) {
                    let bytes = codec::refusal(error, !p.connected);
                    if bytes.len() <= p.max_packet {
                        let _ = p.io.outgoing.try_send(net::Write {
                            correlation: None,
                            bytes,
                        });
                    }
                }
                return self.close(generation, "packet_refused");
            }
        };
        let p = self
            .peers
            .get_mut(&generation)
            .ok_or_else(|| Failure::plain("generation_missing"))?;
        p.last_packet = Instant::now();
        match decoded {
            Packet::Connect(connect) => self.connect(generation, connect),
            Packet::Disconnect => self.close(generation, "client_disconnect"),
            Packet::Core(packet) => {
                if !p.connected {
                    return self.close(generation, "packet_before_connack");
                }
                if let InboundPacket::Publish {
                    packet_id: Some(id),
                    ..
                } = &packet
                {
                    if !p.receiving.contains(&id.get()) && p.receiving.len() >= 8 {
                        return self.close(generation, "receive_maximum");
                    }
                    p.receiving.insert(id.get());
                }
                p.ingress = p
                    .ingress
                    .checked_add(1)
                    .ok_or_else(|| Failure::plain("ingress_exhausted"))?;
                let ingress_order = p.ingress;
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
        let keys: Vec<_> = self.peers.keys().copied().collect();
        for generation in keys {
            for _ in 0..4 {
                let incoming = match self.peers.get(&generation) {
                    Some(p) => p.io.incoming.try_recv(),
                    None => break,
                };
                match incoming {
                    Ok(net::Incoming::Packet { first, body }) => {
                        self.packet(generation, first, body)?
                    }
                    Ok(net::Incoming::Closed { code }) => {
                        self.close(generation, code)?;
                        break;
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        let code = self
                            .peers
                            .get(&generation)
                            .and_then(|p| p.io.terminal_reason())
                            .unwrap_or("reader_closed");
                        self.close(generation, code)?;
                        break;
                    }
                }
            }
            for _ in 0..8 {
                let written = match self.peers.get(&generation) {
                    Some(p) => p.io.written.try_recv(),
                    None => break,
                };
                match written {
                    Ok(net::Written::Complete { correlation, bytes }) => {
                        if let Some((effect, connection)) = correlation {
                            let (_, kind, id) = self
                                .frames
                                .remove(&effect)
                                .ok_or_else(|| Failure::plain("uncorrelated_frame_result"))?;
                            if kind != "PINGRESP" {
                                self.events.emit("frame_written",json!({"connection":generation.get().to_string(),"details":{"packet_kind":kind,"effect":effect_text(effect),"packet_id":id.unwrap_or(0),"bytes":bytes}})).map_err(event_failure)?;
                            }
                            if kind == "PUBCOMP"
                                && let Some(p) = self.peers.get_mut(&generation)
                                && let Some(id) = id
                            {
                                p.receiving.remove(&id);
                            }
                            self.queue(Input::FrameResult {
                                effect,
                                connection,
                                progress: FrameProgress::Written {
                                    confirmed: bytes as u64,
                                    total: bytes as u64,
                                },
                            })?;
                        }
                    }
                    Ok(net::Written::Failed { correlation, code }) => {
                        if let Some((effect, connection)) = correlation {
                            self.frames.remove(&effect);
                            self.queue(Input::FrameResult {
                                effect,
                                connection,
                                progress: FrameProgress::Failed,
                            })?;
                        }
                        self.close(generation, code)?;
                        break;
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        self.close(generation, "writer_closed")?;
                        break;
                    }
                }
            }
            if let Some(p) = self.peers.get(&generation) {
                let expired = if !p.connected && p.created.elapsed() > Duration::from_secs(3) {
                    Some("connect_deadline")
                } else if p.keep_alive != 0
                    && p.last_packet.elapsed()
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
        }
        // Accepted frames whose socket closed still receive terminal failure; commits are separate.
        let stale: Vec<_> = self
            .frames
            .iter()
            .filter(|(_, v)| !self.peers.contains_key(&v.0.generation))
            .map(|(id, v)| (*id, v.0))
            .collect();
        for (effect, connection) in stale {
            self.frames.remove(&effect);
            self.queue(Input::FrameResult {
                effect,
                connection,
                progress: FrameProgress::Failed,
            })?;
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
    #[cfg(feature = "test-hooks")]
    fn controls(&mut self) -> Result<(), Failure> {
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
}
struct Sink<'a> {
    writer: &'a Writer,
    peers: &'a mut BTreeMap<ConnectionGeneration, Peer>,
    events: &'a Events,
    inflight: &'a mut BTreeMap<EffectId, OperationId>,
    timers: &'a mut BTreeMap<DeadlineToken, Instant>,
    inputs: &'a mut VecDeque<Input>,
    frames: &'a mut BTreeMap<EffectId, (ConnectionKey, &'static str, Option<u16>)>,
    close: Vec<(ConnectionGeneration, &'static str)>,
    error: Option<Failure>,
}
impl EffectSink for Sink<'_> {
    fn try_submit(&mut self, effect: OwnedEffect) -> SubmitResult {
        let id = effect.id();
        let refused = |effect, reason| SubmitResult::NotSubmitted { effect, reason };
        if self.error.is_some() {
            return refused(effect, SubmitBlock::Closed);
        }
        match effect.effect() {
            Effect::Persist { command } => {
                if self.inflight.len() >= IN_FLIGHT {
                    return refused(effect, SubmitBlock::InFlightLimit);
                }
                let operation_id = command.recipe.operation();
                #[cfg(feature = "test-hooks")]
                if MUTANT_EARLY_ACK
                    && let mqttiumd_contract::command::Recipe::AdmitPublication(admission) =
                        command.recipe.get()
                    && let Some(p) = self
                        .peers
                        .values()
                        .find(|p| p.receiving.contains(&admission.source.packet_id.get()))
                {
                    let _ = p.io.outgoing.try_send(net::Write {
                        correlation: None,
                        bytes: codec::encode(OutboundFrame::Pubrec {
                            packet_id: admission.source.packet_id,
                            reason: mqttiumd_contract::packet::AckReason::Success,
                        })
                        .unwrap_or_default(),
                    });
                }
                match self.writer.sender.try_send(Request::Effect(effect)) {
                    Ok(()) => {
                        self.inflight.insert(id, operation_id);
                        if self
                            .events
                            .emit(
                                "command_submitted",
                                json!({"operation":operation(operation_id)}),
                            )
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
            Effect::ResolveAndFence { operation } => {
                if self.inflight.len() >= IN_FLIGHT {
                    return refused(effect, SubmitBlock::InFlightLimit);
                }
                let op = *operation;
                match self.writer.sender.try_send(Request::Effect(effect)) {
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
            Effect::EmitFrame { connection, frame } => {
                let (connection, frame) = (*connection, *frame);
                let generation = connection.generation;
                let (kind, packet_id) = frame_info(frame);
                if self.inputs.len() >= INPUTS {
                    return refused(effect, SubmitBlock::QueueFull);
                }
                let Some(peer) = self.peers.get(&generation) else {
                    let _ = effect.into_parts();
                    if self.events.emit("frame_failed",json!({"connection":generation.get().to_string(),"details":{"effect":effect_text(id),"packet_kind":kind,"code":"closed_generation"}})).is_err(){self.error=Some(Failure::plain("events_unavailable"));}
                    self.inputs.push_back(Input::FrameResult {
                        effect: id,
                        connection,
                        progress: FrameProgress::Failed,
                    });
                    return SubmitResult::Accepted;
                };
                let bytes = match codec::encode(frame) {
                    Ok(bytes) if bytes.len() <= peer.max_packet => bytes,
                    _ => {
                        self.close.push((generation, "outgoing_queue_full"));
                        return refused(effect, SubmitBlock::Closed);
                    }
                };
                match peer.io.outgoing.try_send(net::Write {
                    correlation: Some((id, connection)),
                    bytes,
                }) {
                    Ok(()) => {
                        self.frames.insert(id, (connection, kind, packet_id));
                        let _ = effect.into_parts();
                        if kind!="PINGRESP"&&self.events.emit("frame_queued",json!({"connection":generation.get().to_string(),"details":{"packet_kind":kind,"effect":effect_text(id),"packet_id":packet_id.unwrap_or(0)}})).is_err(){self.error=Some(Failure::plain("events_unavailable"));}
                        SubmitResult::Accepted
                    }
                    Err(TrySendError::Full(_)) => {
                        if self.events.emit("effect_retained",json!({"connection":generation.get().to_string(),"details":{"code":"output_queue_full","effect":effect_text(id),"packet_kind":kind}})).is_err(){self.error=Some(Failure::plain("events_unavailable"));}
                        self.close.push((generation, "outgoing_queue_full"));
                        refused(effect, SubmitBlock::QueueFull)
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
                self.close.push((*connection, code));
                let _ = effect.into_parts();
                SubmitResult::Accepted
            }
            Effect::ArmDeadline { token, after } => {
                if self.timers.len() >= TIMERS {
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
                if self.inputs.len() >= INPUTS {
                    return refused(effect, SubmitBlock::QueueFull);
                }
                self.inputs.push_back(Input::Continue {
                    continuation: *continuation,
                    budget: WorkUnits::new(65536),
                });
                let _ = effect.into_parts();
                SubmitResult::Accepted
            }
            Effect::LoadWork { .. } => {
                self.error = Some(Failure::plain("outgoing_delivery_out_of_profile"));
                refused(effect, SubmitBlock::Closed)
            }
        }
    }
}
fn frame_info(frame: OutboundFrame) -> (&'static str, Option<u16>) {
    match frame {
        OutboundFrame::Connack { .. } => ("CONNACK", None),
        OutboundFrame::Pingresp => ("PINGRESP", None),
        OutboundFrame::Pubrec { packet_id, .. } => ("PUBREC", Some(packet_id.get())),
        OutboundFrame::Pubcomp { packet_id, .. } => ("PUBCOMP", Some(packet_id.get())),
    }
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

pub fn run(config: Config) -> Result<(), Failure> {
    let events = Events::open(&config.events).map_err(event_failure)?;
    let cancel = Arc::new(AtomicBool::new(false));
    let signal = cancel.clone();
    ctrlc::set_handler(move || signal.store(true, Ordering::Relaxed))
        .map_err(|_| Failure::plain("signal_handler"))?;
    events.emit("starting",json!({"details":{"test_hooks":cfg!(feature="test-hooks"),"sqlite_kill_points":mqttiumd_store_sqlite::KILL_POINTS_ENABLED}})).map_err(event_failure)?;
    let mut writer = Writer::spawn(
        config.db,
        events.clone(),
        config.hooks.clone(),
        cancel.clone(),
    )?;
    let result = run_inner(
        &config.listen,
        config.hooks,
        &events,
        cancel.clone(),
        &mut writer,
    );
    if let Err(failure) = &result {
        let _ = events.emit("startup_or_runtime_refused", failure.fields());
    }
    cancel.store(true, Ordering::Relaxed);
    // Stop is queued after already accepted work. No accepted command is called rejected.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut stop_sent = false;
    while Instant::now() < deadline && !writer.is_finished() {
        if !stop_sent && writer.sender.try_send(Request::Stop).is_ok() {
            stop_sent = true;
        }
        while writer.receiver.try_recv().is_ok() {}
        thread::sleep(Duration::from_millis(2));
    }
    if !writer.is_finished() {
        let _ = events.emit(
            "shutdown_incomplete",
            json!({"details":{"code":"writer_deadline","accepted_outcome":"unknown"}}),
        );
        return Err(Failure::plain("writer_shutdown_deadline"));
    }
    writer.join_finished()?;
    events.emit("stopped",json!({"details":{"writer_released":true,"accepted_commands_certainly_rejected":false}})).map_err(event_failure)?;
    result
}
fn run_inner(
    listen: &SocketAddr,
    hooks: Hooks,
    events: &Events,
    cancel: Arc<AtomicBool>,
    writer: &mut Writer,
) -> Result<(), Failure> {
    let _ = &hooks;
    let boot = writer
        .boot
        .recv_timeout(Duration::from_secs(10))
        .map_err(|_| Failure::plain("startup_deadline"))??;
    let epoch = boot.grant.epoch();
    let incarnation = boot.grant.incarnation();
    let mut authority = OwnerAuthority::from_grant(boot.grant, NonZeroU32::MIN)
        .map_err(|_| Failure::plain("authority_allocation"))?;
    let outbox = authority
        .create_outbox(
            LogicalOwnerId::new(0),
            NonZeroUsize::new(128).expect("nonzero"),
            nz(65536),
        )
        .map_err(|_| Failure::plain("outbox_creation"))?;
    let owner = Owner::new(
        OwnerConfig {
            max_sessions: nz(128),
            max_routes: nz(1),
            max_exchanges: nz(128),
            account: AccountId::new(1),
            payload_bytes: PayloadBytes::new(4 * 1024 * 1024),
            resolution_delay: LogicalDuration::new(10000),
            retained_operations: nz(128),
            remembered_connections: nz(8),
        },
        outbox,
    )
    .map_err(|_| Failure::plain("owner_creation"))?;
    // The worker handle stays in outer run for cleanup even if recovery fails.
    let mut runtime = Runtime {
        owner,
        _authority: authority,
        writer,
        events: events.clone(),
        peers: BTreeMap::new(),
        retiring: Vec::new(),
        sessions: boot.sessions,
        session_requests: BTreeMap::new(),
        next_session_request: 0,
        generation: ConnectionGeneration::new(0),
        inputs: VecDeque::new(),
        timers: BTreeMap::new(),
        inflight: BTreeMap::new(),
        started: Instant::now(),
        captured: None,
        #[cfg(feature = "test-hooks")]
        hooks,
        frames: BTreeMap::new(),
    };
    let result = (|| {
        let mut cursor = None;
        let mut rows = 0usize;
        let mut saw_epoch = false;
        loop {
            runtime
                .writer
                .sender
                .send(Request::Recovery(cursor))
                .map_err(|_| Failure::plain("writer_closed"))?;
            let response = runtime
                .writer
                .receiver
                .recv_timeout(Duration::from_secs(10))
                .map_err(|_| Failure::plain("recovery_deadline"))?;
            let Response::Recovery(page) = response else {
                return Err(Failure::plain("recovery_protocol"));
            };
            let page = page.map_err(|refused| match refused {
                mqttiumd_store_api::RecoveryRefused::Read(e) => {
                    Failure::read(e, ReadPhase::Recovery)
                }
                _ => Failure::plain(refused.code()),
            })?;
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
        let listener =
            TcpListener::bind(listen).map_err(|_| Failure::plain("listen_unavailable"))?;
        listener
            .set_nonblocking(true)
            .map_err(|_| Failure::plain("listen_nonblocking"))?;
        let address = listener
            .local_addr()
            .map_err(|_| Failure::plain("listen_address"))?;
        events.emit("ready",json!({"details":{"address":address.to_string(),"epoch":epoch.get().to_string(),"incarnation":incarnation.get().to_string(),"max_connections":16,"max_sessions":128,"max_packet_bytes":65536,"input_slots":4,"output_slots":4,"writer_slots":32,"commands_in_flight":16,"max_timers":512,"receipt_slots":8192,"partial_packet_ms":5000,"connect_ms":3000,"write_ms":250,"remembered_connections":8}})).map_err(event_failure)?;
        println!("mqttiumd ready {address} (experimental incoming QoS 2)");
        while !cancel.load(Ordering::Relaxed) {
            for _ in 0..4 {
                match listener.accept() {
                    Ok((stream, _)) => {
                        // Retiring threads count against the same hard connection budget.
                        if runtime.peers.len() + runtime.retiring.len() >= CONNECTIONS {
                            let _ = stream.shutdown(std::net::Shutdown::Both);
                            events
                                .emit(
                                    "connection_refused",
                                    json!({"details":{"code":"connection_limit"}}),
                                )
                                .map_err(event_failure)?;
                            continue;
                        }
                        runtime.generation = runtime
                            .generation
                            .checked_next()
                            .map_err(|_| Failure::plain("generation_exhausted"))?;
                        let io = net::spawn(stream).map_err(|_| Failure::plain("socket_spawn"))?;
                        let sendbuf = io.send_buffer_bytes();
                        runtime.peers.insert(
                            runtime.generation,
                            Peer {
                                io,
                                created: Instant::now(),
                                last_packet: Instant::now(),
                                connected: false,
                                pending: false,
                                keep_alive: 0,
                                max_packet: 268435455,
                                ingress: 0,
                                receiving: BTreeSet::new(),
                            },
                        );
                        events.emit("connection_opened",json!({"connection":runtime.generation.get().to_string(),"details":{"send_buffer_bytes":sendbuf}})).map_err(event_failure)?;
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(_) => return Err(Failure::plain("accept_failed")),
                }
            }
            runtime.writer_results()?;
            runtime.connections()?;
            let due: Vec<_> = runtime
                .timers
                .iter()
                .filter(|(_, due)| **due <= Instant::now())
                .map(|(t, _)| *t)
                .collect();
            for token in due {
                runtime.timers.remove(&token);
                runtime.queue(Input::Deadline {
                    token,
                    now: LogicalTime::new(
                        runtime
                            .started
                            .elapsed()
                            .as_millis()
                            .try_into()
                            .map_err(|_| Failure::plain("clock_exhausted"))?,
                    ),
                })?;
            }
            #[cfg(feature = "test-hooks")]
            runtime.controls()?;
            runtime.drain_inputs()?;
            runtime.dispatch()?;
            thread::sleep(Duration::from_millis(1));
        }
        Ok(())
    })();
    cancel.store(true, Ordering::Relaxed);
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
    result.and(socket_result)
}

#[cfg(test)]
#[path = "runtime_tests.rs"]
mod tests;
