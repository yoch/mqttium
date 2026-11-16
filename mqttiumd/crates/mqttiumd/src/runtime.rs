//! One bounded owner reactor, independent socket tasks and a single SQLite writer.
use crate::{
    codec::{self, Bounds, Packet},
    events::{Events, operation},
    net,
    receiving::Terminal,
    writer::{ControlRequest, ControlResponse, Failure, Hooks, Request, Response, Session, Writer},
};
use mqttiumd_contract::capacity::{PayloadBytes, Resource, WakeCondition, WorkUnits};
use mqttiumd_contract::command::Recipe;
use mqttiumd_contract::effects::{
    CloseReason, Effect, EffectSink, FrameProgress, Input, OwnedEffect, OwnerAuthority,
    RecoveredFact, SubmitBlock, SubmitResult,
};
use mqttiumd_contract::ids::{
    AccountId, ConnectionGeneration, ConnectionKey, DeadlineToken, EffectId, LogicalDuration,
    LogicalOwnerId, LogicalTime, OperationId, OutboundPacketId, SessionId,
};
use mqttiumd_contract::outcome::{CommitFacts, StoreOutcome, StoreReply};
use mqttiumd_contract::packet::{InboundPacket, OutboundFrame};
use mqttiumd_contract::pubsub::{PubackTarget, Qos0DropReason, Qos0Origin};
use mqttiumd_contract::receipts::{
    ClosedReceiptCollection, ClosedReceiptReply, ReceiptCollectionReply,
};
use mqttiumd_contract::session::{SessionAction, SessionChange, SessionEndReason, SessionExpiry};
use mqttiumd_contract::step::{StepOutcome, TurnGrant};
#[cfg(feature = "test-hooks")]
use mqttiumd_contract::{ids::DeliveryId, pubsub::WorkItem};
use mqttiumd_core::owner::{Owner, OwnerConfig};
use mqttiumd_store_api::{ReadError, ReadPhase};
use serde_json::json;
use std::collections::{BTreeMap, VecDeque};
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
const OUTGOING_BINDINGS: usize = 128;
const LIFECYCLE_IN_FLIGHT: usize = 1;
const CLOSING_INPUTS: usize = 2 * CONNECTIONS;
// Per transport: four queued frames, one active and eight terminal receipts.
const FRAMES: usize = CONNECTIONS * (net::OUTPUT_SLOTS + 9);
// A close can retire every accepted frame while an outbox-sized set of
// synchronously refused volatile effects also awaits its exact owner fact.
const TERMINAL_INPUTS: usize = FRAMES + 128;
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
    pub listen: SocketAddr,
    pub events: PathBuf,
    pub hooks: Hooks,
}
struct Peer {
    io: net::Connection,
    created: Instant,
    last_packet: Instant,
    connected: bool,
    session: Option<SessionId>,
    opened_by: Option<OperationId>,
    connect_expiry: SessionExpiry,
    handshake_packet: Option<(u8, Vec<u8>)>,
    pending: bool,
    keep_alive: u16,
    max_packet: usize,
    ingress: u64,
    receiving: crate::receiving::Window,
    closing: Option<(&'static str, Instant)>,
    deferred_publish: Option<DeferredPublish>,
}
struct DeferredPublish {
    packet: InboundPacket,
    ingress_order: u64,
    since: Instant,
}
struct PendingFrame {
    connection: ConnectionKey,
    kind: &'static str,
    packet_id: Option<u16>,
    receive_serial: Option<u64>,
    terminal: Option<Terminal>,
    wire_released: bool,
}
struct PendingSession {
    connection: ConnectionGeneration,
    operation: Option<OperationId>,
    zero_end: bool,
}
#[derive(Clone)]
struct AppliedSessionChange {
    operation: OperationId,
    connection: Option<ConnectionGeneration>,
    client_id: mqttiumd_contract::packet::SharedBytes,
    change: SessionChange,
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
    control_inputs: VecDeque<Input>,
    closing_inputs: VecDeque<Input>,
    terminal_inputs: VecDeque<Input>,
    deferred_inputs: VecDeque<Input>,
    timers: BTreeMap<DeadlineToken, Instant>,
    inflight: BTreeMap<EffectId, OperationId>,
    lifecycle_inflight: BTreeMap<EffectId, OperationId>,
    last_session_change: Option<OperationId>,
    session_cardinality_dirty: bool,
    last_session_cardinality: Option<[usize; 13]>,
    work_reads: BTreeMap<EffectId, SessionId>,
    last_work_scan: Instant,
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
    frames: BTreeMap<EffectId, PendingFrame>,
    outgoing_bindings: BTreeMap<(ConnectionKey, OutboundPacketId), PubackTarget>,
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
impl Runtime<'_> {
    fn accept(&mut self, stream: std::net::TcpStream) -> Result<bool, Failure> {
        // Retiring threads count against the same hard connection budget.
        if self.peers.len() + self.retiring.len() >= CONNECTIONS
            || self.closing_inputs.len() >= CONNECTIONS
            || self.session_requests.len() >= CONNECTIONS
        {
            let _ = stream.shutdown(std::net::Shutdown::Both);
            self.events
                .emit(
                    "connection_refused",
                    json!({"details":{"code":"connection_limit"}}),
                )
                .map_err(event_failure)?;
            return Ok(false);
        }
        self.generation = self
            .generation
            .checked_next()
            .map_err(|_| Failure::plain("generation_exhausted"))?;
        let io = net::spawn(stream).map_err(|_| Failure::plain("socket_spawn"))?;
        let sendbuf = io.send_buffer_bytes();
        self.peers.insert(
            self.generation,
            Peer {
                io,
                created: Instant::now(),
                last_packet: Instant::now(),
                connected: false,
                session: None,
                opened_by: None,
                connect_expiry: SessionExpiry::OnDisconnect,
                handshake_packet: None,
                pending: false,
                keep_alive: 0,
                max_packet: 268435455,
                ingress: 0,
                receiving: crate::receiving::Window::default(),
                closing: None,
                deferred_publish: None,
            },
        );
        self.events.emit("connection_opened", json!({"connection":self.generation.get().to_string(),"details":{"send_buffer_bytes":sendbuf}})).map_err(event_failure)?;
        Ok(true)
    }
    fn queue(&mut self, input: Input) -> Result<(), Failure> {
        if Self::terminal_input(&input) {
            if self.terminal_inputs.iter().any(|queued| queued == &input) {
                return Ok(());
            }
            if self.terminal_inputs.len() >= TERMINAL_INPUTS {
                return Err(Failure::plain("terminal_input_budget"));
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
            if self.closing_inputs.len() >= CLOSING_INPUTS {
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
        if self.inputs.len() + self.deferred_inputs.len() >= INPUTS {
            return Err(Failure::plain("core_input_budget"));
        }
        self.inputs.push_back(input);
        Ok(())
    }
    fn terminal_input(input: &Input) -> bool {
        matches!(
            input,
            Input::FrameResult { .. } | Input::Qos0EmitRefused { .. }
        )
    }
    fn closing_input(input: &Input) -> bool {
        matches!(
            input,
            Input::EndSession { .. } | Input::ConnectionClosed { .. }
        )
    }
    fn maintenance_input(&self, input: &Input) -> bool {
        matches!(
            input,
            Input::ReceiptsCollected { .. } | Input::MaintainReceipts
        ) || matches!(input, Input::StoreResult { operation, .. } if self.owner.lifecycle_operation_is_live(*operation))
            || matches!(input,Input::Deadline {token,..} if self.owner.collection_deadline_is_live(*token) || self.owner.lifecycle_dispatch().is_some_and(|(_,deadline)| deadline == *token))
    }
    fn close(
        &mut self,
        generation: ConnectionGeneration,
        code: &'static str,
    ) -> Result<(), Failure> {
        if let Some(peer) = self.peers.remove(&generation) {
            self.session_cardinality_dirty = true;
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
            // Frame failure can unbind the core before ConnectionClosed is
            // applied. Preserve this transport's confirmed zero-expiry guard
            // independently; neither a cache lookup nor its current binding
            // can recover that authority afterwards.
            if peer.connect_expiry == SessionExpiry::OnDisconnect
                && let Some(session) = peer.session
            {
                self.queue(Input::EndSession {
                    connection: ConnectionKey {
                        session,
                        generation,
                    },
                    opened_by: peer.opened_by,
                    reason: if code == "client_disconnect" {
                        SessionEndReason::Disconnect
                    } else {
                        SessionEndReason::TransportLost
                    },
                })?;
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
                .map(|(id, _)| *id)
                .collect();
            for id in pending {
                let frame = self
                    .frames
                    .remove(&id)
                    .ok_or_else(|| Failure::plain("frame_missing_on_close"))?;
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
            self.deferred_inputs
                .retain(|input| input_connection(input) != Some(generation));
            self.outgoing_bindings
                .retain(|(connection, _), _| connection.generation != generation);
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
                if let Some(old) = retired {
                    self.retire_session(old)?;
                }
                if self.sessions.len() >= crate::writer::SESSIONS
                    && !self.sessions.contains_key(client_id.as_slice())
                {
                    return Err(Failure::plain("session_cache_budget"));
                }
                self.sessions.insert(
                    client_id.as_slice().to_vec(),
                    Session {
                        id: session,
                        revision,
                        client: client_id.clone(),
                        present: session_present,
                        expiry,
                        opened_by: Some(changed_by),
                    },
                );
                if let Some(generation) = connection {
                    let zero_end = self
                        .session_requests
                        .values()
                        .any(|pending| pending.connection == generation && pending.zero_end);
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
                        peer.connected = true;
                        // The ordered CONNACK, not cache installation, releases
                        // the bounded pre-CONNACK ingress gate.
                        peer.pending = true;
                    } else if expiry == SessionExpiry::OnDisconnect || zero_end {
                        self.queue(Input::EndSession {
                            connection: ConnectionKey {
                                session,
                                generation,
                            },
                            opened_by: Some(changed_by),
                            reason: if zero_end {
                                SessionEndReason::Disconnect
                            } else {
                                SessionEndReason::TransportLost
                            },
                        })?;
                    } else {
                        self.queue(Input::ConnectionClosed {
                            connection: generation,
                        })?;
                    }
                }
                let mut fields = json!({"operation":operation(changed_by),"details":{"session":session.get().to_string(),"session_present":session_present,"expiry":expiry.to_wire(),"source":"lifecycle_commit"}});
                if let Some(generation) = connection {
                    fields["connection"] = json!(generation.get().to_string());
                }
                self.events
                    .emit("session_established", fields)
                    .map_err(event_failure)?;
                self.rearm_ready_work(session, true);
            }
            SessionChange::Closed { session, .. } => self.retire_session(session)?,
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
                "session_present":session_present,"expiry":format!("{expiry:?}"),
                "retired_session":retired.map(|id|id.get().to_string()),"route_revision":route_revision.get().to_string(),
                "cancelled_inbound":cancelled.inbound,"cancelled_deliveries":cancelled.deliveries,"cancelled_routes":cancelled.routes
            }),
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
            Input::EndSession { connection, .. } => connection.generation,
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
        self.session_cardinality_dirty = true;
        self.sessions.retain(|_, cached| cached.id != retired);
        self.outgoing_bindings
            .retain(|(connection, _), _| connection.session != retired);
        self.inputs.retain(
            |input| !matches!(input, Input::WorkAvailable { session } if *session == retired),
        );
        self.deferred_inputs.retain(
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
        let mut control_budget = crate::writer::CONTROL_QUEUE;
        let mut closing_budget = self.closing_inputs.len().min(CONNECTIONS);
        let mut terminal_budget = self.terminal_inputs.len().min(CONNECTIONS);
        for _ in
            0..(self.inputs.len() + self.control_inputs.len() + closing_budget + terminal_budget)
                .min(64)
        {
            let control = if control_budget > 0 {
                self.control_inputs.pop_front()
            } else {
                None
            };
            let Some(input) = control
                .inspect(|_| control_budget -= 1)
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
                        self.terminal_inputs.pop_front()
                    } else {
                        None
                    }
                })
                .or_else(|| self.inputs.pop_front())
            else {
                break;
            };
            if let Some(connection) = business_connection(&input)
                && self
                    .deferred_inputs
                    .iter()
                    .any(|older| business_connection(older) == Some(connection))
            {
                self.deferred_inputs.push_back(input);
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
                    if target.is_none() || self.owner.puback_target(connection, packet_id) != target
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
                    packet: InboundPacket::Puback { .. },
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
                        StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::DeliveryFinished {
                            delivery,
                            ..
                        })),
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
            let opening = match &input {
                Input::OpenSession { connection, .. } => Some(*connection),
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
            let previous_lifecycle = self.owner.lifecycle_dispatch().map(|(op, _)| op);
            let mut applied = false;
            match self.owner.step(
                input,
                TurnGrant {
                    work_limit: WorkUnits::new(65536),
                },
            ) {
                StepOutcome::Applied { .. } => {
                    applied = true;
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
                        let mut details = crate::writer::collection_details(collection);
                        crate::writer::returned_closure_details(&mut details, closure);
                        details["removed"] = json!(removed);
                        details["retained"] = json!(retained.to_string());
                        details["more"] = json!(more);
                        self.events
                            .emit(
                                "receipt_collection_applied",
                                json!({"command_kind":"ReceiptCollection","details":details}),
                            )
                            .map_err(event_failure)?;
                    }
                    if let Some((id, delivery)) = finish {
                        self.events.emit("core_finish_observed",json!({"operation":operation(id),"details":{"delivery":delivery_text(delivery)}})).map_err(event_failure)?;
                    }
                    if wakes_business {
                        // The earlier blocked packets stay ahead of later bytes
                        // from their connections. Control packets can still pass
                        // while a durable routing mutation remains unresolved.
                        self.deferred_inputs.append(&mut self.inputs);
                        std::mem::swap(&mut self.inputs, &mut self.deferred_inputs);
                    }
                }
                StepOutcome::NeedCapacity { input, need, .. } => {
                    match input {
                        packet @ Input::Packet { .. }
                            if matches!(
                                need.resource,
                                Resource::ResultSlots | Resource::ReceiptSlots
                            ) && need.wake == WakeCondition::ResultDelivered =>
                        {
                            if need.resource == Resource::ReceiptSlots {
                                let (span, holes) = self
                                    .owner
                                    .receipt_status()
                                    .map(|(_, s, h)| (s, h))
                                    .unwrap_or((0, 0));
                                self.events.emit("receipt_window_pressure",json!({"details":{"resource":"ReceiptSlots","code":"receipt_window_business_full","span":span,"unconsumed_holes":holes}})).map_err(event_failure)?;
                            }
                            self.deferred_inputs.push_back(packet)
                        }
                        Input::Packet { connection, .. } => {
                            // No transition applied and no effect was submitted for this packet.
                            // Close its transport rather than reorder its positive acknowledgements.
                            self.events.emit("input_capacity_refused",json!({"connection":connection.get().to_string(),"details":{"accepted_command_rejected":false}})).map_err(event_failure)?;
                            self.close(connection, "input_capacity")?;
                        }
                        other if Self::terminal_input(&other) => {
                            self.terminal_inputs.push_back(other)
                        }
                        other if Self::closing_input(&other) => {
                            self.closing_inputs.push_back(other)
                        }
                        other if self.maintenance_input(&other) => {
                            self.control_inputs.push_back(other)
                        }
                        other => self.inputs.push_back(other),
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
                self.prune_outgoing_bindings();
                // A terminal result is consumed only after the exact input was
                // applied. The pure owner, rather than elapsed wall time or a
                // received reply, decides which deadlines remain meaningful.
                self.timers
                    .retain(|token, _| self.owner.deadline_is_live(*token));
            }
            self.dispatch()?;
            if applied
                && let Some(operation) = lifecycle_result
                && !self.owner.lifecycle_operation_is_live(operation)
            {
                self.session_requests
                    .retain(|_, pending| pending.operation != Some(operation));
            }
            if applied && let Some((session, initial)) = ready_work {
                self.rearm_ready_work(session, initial);
            }
        }
        self.schedule_maintenance();
        self.emit_session_cardinality()?;
        Ok(())
    }
    fn prune_outgoing_bindings(&mut self) {
        let owner = &self.owner;
        self.outgoing_bindings
            .retain(|(connection, packet_id), target| {
                owner.puback_target(*connection, *packet_id) == Some(*target)
            });
    }
    fn schedule_maintenance(&mut self) {
        if self.maintenance_needed
            && self.control_inputs.len() < crate::writer::CONTROL_QUEUE
            && !self
                .control_inputs
                .iter()
                .any(|i| matches!(i, Input::MaintainReceipts))
        {
            self.control_inputs.push_back(Input::MaintainReceipts);
            self.maintenance_needed = false;
        }
    }
    fn due_deadlines(&mut self) -> Result<(), Failure> {
        let now = Instant::now();
        let due: Vec<_> = self
            .timers
            .iter()
            .filter(|(_, due)| **due <= now)
            .map(|(token, _)| *token)
            .collect();
        for token in due {
            let reserved = self.owner.collection_deadline_is_live(token)
                || self
                    .owner
                    .lifecycle_dispatch()
                    .is_some_and(|(_, deadline)| deadline == token);
            let room = if reserved {
                self.control_inputs.len() < crate::writer::CONTROL_QUEUE
            } else {
                self.inputs.len() + self.deferred_inputs.len() < INPUTS
            };
            if !room {
                continue;
            } // The exact token remains in the bounded timer map.
            self.queue(Input::Deadline {
                token,
                now: LogicalTime::new(
                    self.started
                        .elapsed()
                        .as_millis()
                        .try_into()
                        .map_err(|_| Failure::plain("clock_exhausted"))?,
                ),
            })?;
            self.timers.remove(&token);
        }
        Ok(())
    }
    fn dispatch(&mut self) -> Result<(), Failure> {
        let bindings: BTreeMap<_, _> = self
            .sessions
            .values()
            .filter_map(|s| self.owner.connection_of(s.id).map(|c| (s.id, c)))
            .collect();
        let mut sink = Sink {
            writer: self.writer,
            peers: &mut self.peers,
            events: &self.events,
            inflight: &mut self.inflight,
            lifecycle_inflight: &mut self.lifecycle_inflight,
            lifecycle_operation: self.owner.lifecycle_dispatch().map(|(op, _)| op),
            work_reads: &mut self.work_reads,
            bindings: &bindings,
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
            terminal_inputs: &mut self.terminal_inputs,
            deferred_count: self.deferred_inputs.len(),
            frames: &mut self.frames,
            outgoing_bindings: &mut self.outgoing_bindings,
            close: Vec::new(),
            session_change: None,
            error: None,
        };
        #[cfg(feature = "test-hooks")]
        sink.release_qos0();
        let _ = self.owner.dispatch(&mut sink);
        let close = std::mem::take(&mut sink.close);
        let changed = sink.session_change.take();
        let error = sink.error.take();
        drop(sink);
        if let Some(error) = error {
            return Err(error);
        }
        for (generation, code) in close {
            self.close(generation, code)?;
        }
        if let Some(changed) = changed {
            self.session_changed(changed)?;
        }
        self.timers
            .retain(|token, _| self.owner.deadline_is_live(*token));
        self.session_cardinality_dirty = true;
        self.emit_session_cardinality()?;
        Ok(())
    }
    fn writer_results(&mut self) -> Result<(), Failure> {
        // Replies have their own reserved capacity. The owner retains bounded
        // attempt correlation; no adapter history or second retry loop exists.
        for _ in 0..crate::writer::CONTROL_QUEUE.saturating_sub(self.control_inputs.len()) {
            let response = match self.writer.control_receiver.try_recv() {
                Ok(reply) => reply,
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    return Err(Failure::plain("writer_control_disconnected"));
                }
            };
            match response {
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
                ControlResponse::Failed(failure) => return Err(failure),
                ControlResponse::Closed { .. } => {
                    return Err(Failure::plain("unexpected_closed_collection"));
                }
            }
        }
        for _ in 0..64 {
            if self.inputs.len() + self.deferred_inputs.len() >= INPUTS {
                break;
            }
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
                    if !self.sessions.values().any(|cached| cached.id == session) {
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
                        | CommitFacts::DeliveryConsumed { target, .. },
                    )),
                ..
            } => Some((*target, false)),
            Input::StoreResult {
                reply:
                    StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::SubscriptionsChanged {
                        session,
                        ..
                    })),
                ..
            } => Some((*session, false)),
            Input::ObservedPuback { connection, .. } => Some((connection.session, false)),
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
                .sessions
                .values()
                .find(|s| self.owner.connection_of(s.id) == Some(*connection))
                .map(|s| (s.id, false)),
            _ => None,
        }
    }
    fn rearm_ready_work(&mut self, session: SessionId, initial: bool) {
        let usable = self.owner.connection_of(session).is_some_and(|generation| {
            self.peers
                .get(&generation)
                .is_some_and(|p| p.connected && !p.pending && p.closing.is_none())
        });
        if usable && (initial || self.owner.has_ready_work(session)) {
            // queue_work is FIFO, bounded by the existing input budget, and
            // deduplicated by destination. Each turn retains its 64-input cap.
            self.queue_work(session);
        }
    }
    fn queue_work(&mut self, session: SessionId) {
        if self.inputs.len() + self.deferred_inputs.len() < INPUTS
            && !self
                .inputs
                .iter()
                .any(|i| matches!(i,Input::WorkAvailable{session:s} if *s==session))
        {
            self.inputs.push_back(Input::WorkAvailable { session });
        }
    }
    fn discover_work(&mut self) {
        #[cfg(feature = "test-hooks")]
        if MUTANT_NOTIFICATIONS_ONLY {
            return;
        }
        if self.last_work_scan.elapsed() < Duration::from_millis(100) {
            return;
        }
        self.last_work_scan = Instant::now();
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
        self.events.emit("connect_validated",json!({"connection":generation.get().to_string(),"details":{"keep_alive":connect.keep_alive,"max_packet_size":connect.max_packet_size,"receive_maximum":connect.receive_maximum,"topic_alias_maximum":connect.topic_alias_maximum,"request_problem_information":connect.request_problem_information,"clean_start":connect.clean_start,"expiry":format!("{:?}",connect.expiry)}})).map_err(event_failure)?;
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
        if self.session_requests.len() >= CONNECTIONS {
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
                connection: generation,
                operation: None,
                zero_end: false,
            },
        );
        if let Some(peer) = self.peers.get_mut(&generation) {
            peer.pending = true;
            peer.connect_expiry = connect.expiry;
        }
        self.queue(Input::OpenSession {
            connection: generation,
            client_id: connect.client_id,
            clean_start: connect.clean_start,
            expiry: connect.expiry,
        })?;
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
        self.wire_progress(generation)?;
        let decoded = match codec::decode(first, &body, BOUNDS) {
            Ok(v) => v,
            Err(error) => {
                let error = self
                    .peers
                    .get(&generation)
                    .map_or(error, |peer| error.in_session(peer.connect_expiry));
                self.events.emit("packet_refused",json!({"connection":generation.get().to_string(),"details":{"code":error.code(),"reason":error.reason_code()}})).map_err(event_failure)?;
                // Refusal output has the same bounded absolute write path as business frames.
                if let Some(p) = self.peers.get_mut(&generation) {
                    let cap = if p.connected {
                        Some(p.max_packet as u32)
                    } else {
                        error.peer_maximum()
                    };
                    if let Some(cap) = cap {
                        let bytes = codec::refusal(error, !p.connected);
                        if bytes.len() <= cap as usize
                            && p.io
                                .outgoing
                                .try_send(net::Write {
                                    correlation: None,
                                    bytes,
                                })
                                .is_ok()
                        {
                            p.closing = Some(("packet_refused", Instant::now()));
                            return Ok(());
                        }
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
            Packet::Disconnect { expiry } => {
                if p.connect_expiry == SessionExpiry::OnDisconnect
                    && expiry == Some(SessionExpiry::Never)
                {
                    let bytes = codec::disconnect_reason(0x82);
                    self.events.emit("packet_refused", json!({"connection":generation.get().to_string(),"details":{"code":"SessionExpiryIncreaseForbidden","reason":0x82}})).map_err(event_failure)?;
                    if bytes.len() <= p.max_packet
                        && p.io
                            .outgoing
                            .try_send(net::Write {
                                correlation: None,
                                bytes,
                            })
                            .is_ok()
                    {
                        p.closing = Some(("packet_refused", Instant::now()));
                        return Ok(());
                    }
                    return self.close(generation, "packet_refused");
                }
                if expiry == Some(SessionExpiry::OnDisconnect)
                    && let Some(pending) = self
                        .session_requests
                        .values_mut()
                        .find(|pending| pending.connection == generation)
                {
                    pending.zero_end = true;
                }
                let end = if expiry == Some(SessionExpiry::OnDisconnect) {
                    p.session.map(|session| Input::EndSession {
                        connection: ConnectionKey {
                            session,
                            generation,
                        },
                        opened_by: p.opened_by,
                        reason: SessionEndReason::Disconnect,
                    })
                } else {
                    None
                };
                // Earlier decoded packets precede this close in the TCP stream.
                // Apply their facts while this generation is authoritative;
                // accepted durable work then outlives its transport.
                self.drain_inputs()?;
                if let Some(end) = end {
                    self.queue(end)?;
                }
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
                    self.events.emit("publish_validated", json!({"connection":generation.get().to_string(),"details":{"packet_id":id.get(),"qos":qos.to_wire(),"dup":dup,"publish_order":ingress_order.to_string()}})).map_err(event_failure)?;
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
                    if pending_terminal {
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
                        });
                        self.events.emit("publish_waiting_wire",json!({"connection":generation.get().to_string(),"details":{"publish_order":ingress_order.to_string()}})).map_err(event_failure)?;
                        return Ok(());
                    }
                    let bytes = vec![0xe0, 2, 0x93, 0];
                    if bytes.len() <= p.max_packet
                        && p.io
                            .outgoing
                            .try_send(net::Write {
                                correlation: None,
                                bytes,
                            })
                            .is_ok()
                    {
                        p.closing = Some(("receive_maximum", Instant::now()));
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
                    self.events
                        .emit(
                            "puback_observed",
                            puback_fields(
                                connection,
                                packet_id,
                                reason,
                                target,
                                Some(self.puback_observation),
                                "observed",
                            ),
                        )
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
            self.wire_progress(generation)?;
            self.retry_deferred_publish(generation)?;
            for _ in 0..4 {
                let incoming = match self.peers.get_mut(&generation) {
                    Some(p) if p.closing.is_some() || p.deferred_publish.is_some() => break,
                    Some(p) if p.pending && p.handshake_packet.is_some() => break,
                    Some(p) => match p.handshake_packet.take() {
                        Some((first, body)) => Ok(net::Incoming::Packet { first, body }),
                        None => p.io.incoming.try_recv(),
                    },
                    None => break,
                };
                match incoming {
                    Ok(net::Incoming::Packet { first, body }) => {
                        if first >> 4 != 14
                            && self.peers.get(&generation).is_some_and(|p| p.pending)
                        {
                            self.peers
                                .get_mut(&generation)
                                .expect("checked peer")
                                .handshake_packet = Some((first, body));
                            break;
                        }
                        self.packet(generation, first, body)?
                    }
                    Ok(net::Incoming::Closed { code }) => {
                        self.drain_inputs()?;
                        self.close(generation, code)?;
                        break;
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        self.drain_inputs()?;
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
                                self.deferred_ack =
                                    Some((effect, connection, bytes, Instant::now()));
                                continue;
                            }
                            self.written_complete(effect, connection, bytes)?;
                        } else if let Some((code, _)) =
                            self.peers.get(&generation).and_then(|p| p.closing)
                        {
                            self.close(generation, code)?;
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
                let expired = if let Some((code, since)) = p.closing
                    && since.elapsed() > Duration::from_millis(300)
                {
                    Some(code)
                } else if (p.pending || !p.connected)
                    && p.created.elapsed() > Duration::from_secs(3)
                {
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
            .filter(|(_, v)| !self.peers.contains_key(&v.connection.generation))
            .map(|(id, v)| (*id, v.connection))
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
    /// Network completion and core receipt consumption have separate lanes.
    /// No lock is held across I/O. Scope/occurrence/QoS are checked for every
    /// early receive-credit release; stale progress is never inferred by PI.
    fn wire_progress(&mut self, generation: ConnectionGeneration) -> Result<(), Failure> {
        let Some(peer) = self.peers.get_mut(&generation) else {
            return Ok(());
        };
        for _ in 0..net::OUTPUT_SLOTS + 1 + 8 {
            let progress = match peer.io.wire_progress.try_recv() {
                Ok(progress) => progress,
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
                self.events.emit("wire_receive_credit",json!({"connection":generation.get().to_string(),"details":{"effect":effect_text(progress.effect),"packet_kind":frame.kind,"packet_id":id,"publish_order":frame.receive_serial.map(|v|v.to_string()).unwrap_or_default(),"released":released}})).map_err(event_failure)?;
            }
        }
        Ok(())
    }

    fn retry_deferred_publish(&mut self, generation: ConnectionGeneration) -> Result<(), Failure> {
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
        } else if deferred.since.elapsed() > Duration::from_millis(500) {
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
            self.events.emit("frame_written",json!({"connection":generation.get().to_string(),"details":{"packet_kind":"PUBLISH","qos":0,"effect":effect_text(effect),"bytes":bytes}})).map_err(event_failure)?;
        } else if kind != "PINGRESP" {
            self.events.emit("frame_written",json!({"connection":generation.get().to_string(),"details":{"packet_kind":kind,"effect":effect_text(effect),"packet_id":id.unwrap_or(0),"publish_order":pending.receive_serial.map(|v|v.to_string()).unwrap_or_default(),"bytes":bytes}})).map_err(event_failure)?;
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
            self.events.emit("receive_credit", json!({"connection":generation.get().to_string(),"details":{"packet_kind":kind,"effect":effect_text(effect),"packet_id":id,"publish_order":pending.receive_serial.map(|v|v.to_string()).unwrap_or_default(),"released":released}})).map_err(event_failure)?;
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
                for item in page.items.as_slice() {
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
}
#[cfg(feature = "test-hooks")]
impl AppliedSessionChange {
    fn fields(&self) -> serde_json::Value {
        let (action, session) = match self.change {
            SessionChange::Opened { session, .. } => ("Opened", session),
            SessionChange::Closed { session, .. } => ("Closed", session),
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
    effect: OwnedEffect,
    since: Instant,
    released: bool,
}
struct Sink<'a> {
    writer: &'a Writer,
    peers: &'a mut BTreeMap<ConnectionGeneration, Peer>,
    events: &'a Events,
    inflight: &'a mut BTreeMap<EffectId, OperationId>,
    lifecycle_inflight: &'a mut BTreeMap<EffectId, OperationId>,
    lifecycle_operation: Option<OperationId>,
    work_reads: &'a mut BTreeMap<EffectId, SessionId>,
    bindings: &'a BTreeMap<SessionId, ConnectionGeneration>,
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
    terminal_inputs: &'a mut VecDeque<Input>,
    deferred_count: usize,
    frames: &'a mut BTreeMap<EffectId, PendingFrame>,
    outgoing_bindings: &'a mut BTreeMap<(ConnectionKey, OutboundPacketId), PubackTarget>,
    close: Vec<(ConnectionGeneration, &'static str)>,
    session_change: Option<AppliedSessionChange>,
    error: Option<Failure>,
}
impl Sink<'_> {
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
            .control_sender
            .try_send(ControlRequest::Lifecycle(effect))
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
impl Sink<'_> {
    fn release_qos0(&mut self) {
        let Some(front) = self.emission_barriers.front_mut() else {
            return;
        };
        if !front.released {
            let Some(dir) = &self.hooks.directory else {
                self.error = Some(Failure::plain("hook_directory"));
                return;
            };
            let release = dir.join("release-consume.emission");
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
                effect,
                since: held.since,
                released: true,
            });
        }
    }
}
impl EffectSink for Sink<'_> {
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
                    let bytes = match codec::session_refusal(reason) {
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
                                correlation: None,
                                bytes,
                            })
                            .is_ok()
                    {
                        peer.closing = Some(("session_refused", Instant::now()));
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
                if self.events.emit("qos0_dropped",json!({"details":{"origin":qos0_origin(*origin),"target":target.map(|t|t.get().to_string()).unwrap_or_default(),"reason":reason.code()}})).is_err(){self.error=Some(Failure::plain("events_unavailable"));}
                let _ = effect.into_parts();
                SubmitResult::Accepted
            }
            Effect::EmitQos0 {
                connection,
                origin,
                topic,
                payload,
            } => {
                let (connection, origin) = (*connection, *origin);
                if self.terminal_inputs.len() >= TERMINAL_INPUTS || self.frames.len() >= FRAMES {
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
                    match codec::encode_qos0(topic, payload) {
                        Ok(bytes) if bytes.len() <= peer.max_packet => {
                            #[cfg(feature = "test-hooks")]
                            let duplicate = if MUTANT_REPLAY_QOS0
                                && matches!(origin, Qos0Origin::Consumed { .. })
                            {
                                Some(bytes.clone())
                            } else {
                                None
                            };
                            let result = peer.io.outgoing.try_send(net::Write {
                                correlation: Some((id, connection)),
                                bytes,
                            });
                            #[cfg(feature = "test-hooks")]
                            if result.is_ok()
                                && let Some(bytes) = duplicate
                            {
                                let _ = peer.io.outgoing.try_send(net::Write {
                                    correlation: None,
                                    bytes,
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
                            },
                        );
                        if self.events.emit("frame_queued",json!({"connection":generation.get().to_string(),"details":{"packet_kind":"PUBLISH","qos":0,"origin":qos0_origin(origin),"effect":effect_text(id)}})).is_err(){self.error=Some(Failure::plain("events_unavailable"));}
                    }
                    Err(reason) => self.terminal_inputs.push_back(Input::Qos0EmitRefused {
                        effect: id,
                        connection,
                        reason,
                    }),
                }
                let _ = effect.into_parts();
                SubmitResult::Accepted
            }
            Effect::Persist { command }
                if matches!(command.recipe.get(), Recipe::SessionLifecycle(_)) =>
            {
                if let Recipe::SessionLifecycle(lifecycle) = command.recipe.get()
                    && let SessionAction::Open {
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
                if self.inflight.len() >= IN_FLIGHT {
                    return refused(effect, SubmitBlock::InFlightLimit);
                }
                let operation_id = command.recipe.operation();
                let command_kind = crate::writer::recipe_kind(command.recipe.get());
                let details = recipe_details(command.recipe.get());
                #[cfg(feature = "test-hooks")]
                if MUTANT_EARLY_QOS0
                    && let Recipe::ConsumeDelivery(c) = command.recipe.get()
                    && let Some(item) = self.work_cache.get(&c.delivery)
                    && let Some(g) = self.bindings.get(&c.target)
                    && let Some(peer) = self.peers.get(g)
                    && let Ok(bytes) = codec::encode_qos0(&item.topic, &item.payload)
                {
                    let _ = peer.io.outgoing.try_send(net::Write {
                        correlation: None,
                        bytes,
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
                        correlation: None,
                        bytes,
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
                        correlation: None,
                        bytes: codec::encode(OutboundFrame::Pubrec {
                            packet_id: admission.source.packet_id,
                            reason: mqttiumd_contract::packet::AckReason::Success,
                        })
                        .unwrap_or_default(),
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
                        correlation: None,
                        bytes: codec::encode(OutboundFrame::Puback {
                            packet_id: admission.source.packet_id,
                            reason: mqttiumd_contract::packet::PubackReason::Success,
                        })
                        .unwrap_or_default(),
                    });
                }
                match self.writer.sender.try_send(Request::Effect(effect)) {
                    Ok(()) => {
                        self.inflight.insert(id, operation_id);
                        #[cfg(feature = "test-hooks")]
                        if let Some(input) = early_finish {
                            self.inputs.push_back(input);
                        }
                        if self
                            .events
                            .emit(
                                "command_submitted",
                                json!({"operation":operation(operation_id),"command_kind":command_kind,"details":details}),
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
            Effect::ResolveAndFence { operation }
                if self.lifecycle_operation == Some(*operation) =>
            {
                self.submit_lifecycle(effect)
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
            Effect::EmitFrame {
                connection,
                frame,
                publish_order,
            } => {
                let (connection, frame) = (*connection, *frame);
                let receive_serial = *publish_order;
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
                if self.terminal_inputs.len() >= TERMINAL_INPUTS
                    || self.frames.len() >= FRAMES
                    || (kind == "CONNACK" && self.inputs.len() + self.deferred_count >= INPUTS)
                {
                    return refused(effect, SubmitBlock::QueueFull);
                }
                let Some(peer) = self.peers.get_mut(&generation).filter(|peer| {
                    peer.closing.is_none()
                        && self.bindings.get(&connection.session) == Some(&generation)
                }) else {
                    let _ = effect.into_parts();
                    if self.events.emit("frame_failed",json!({"connection":generation.get().to_string(),"details":{"effect":effect_text(id),"packet_kind":kind,"code":"unusable_generation"}})).is_err(){self.error=Some(Failure::plain("events_unavailable"));}
                    self.terminal_inputs.push_back(Input::FrameResult {
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
                        if kind == "CONNACK" {
                            // Pipeline processing starts only after the core's
                            // CONNACK occupies the ordered socket output queue.
                            peer.pending = false;
                            // Readiness is effective only after the ordered
                            // CONNACK enters the bounded writer queue. The
                            // earlier cache-install fence deliberately kept
                            // this destination ineligible for discovery.
                            if !self.inputs.iter().any(|input| matches!(input, Input::WorkAvailable { session } if *session == connection.session)) {
                                self.inputs.push_back(Input::WorkAvailable { session: connection.session });
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
                            },
                        );
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
                let mut flushing = false;
                if *reason == CloseReason::SessionTakenOver
                    && let Some(peer) = self.peers.get_mut(connection)
                {
                    if peer.closing.is_some() {
                        flushing = true;
                    } else {
                        let bytes = codec::disconnect_reason(0x8e);
                        if bytes.len() <= peer.max_packet
                            && peer
                                .io
                                .outgoing
                                .try_send(net::Write {
                                    correlation: None,
                                    bytes,
                                })
                                .is_ok()
                        {
                            peer.closing = Some((code, Instant::now()));
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
                match self
                    .writer
                    .control_sender
                    .try_send(ControlRequest::Collect {
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
                if self.inputs.len() + self.deferred_count >= INPUTS {
                    return refused(effect, SubmitBlock::QueueFull);
                }
                self.inputs.push_back(Input::Continue {
                    continuation: *continuation,
                    budget: WorkUnits::new(65536),
                });
                let _ = effect.into_parts();
                SubmitResult::Accepted
            }
            Effect::LoadWork { session, .. } => {
                if self.work_reads.len() >= IN_FLIGHT {
                    return refused(effect, SubmitBlock::InFlightLimit);
                }
                let session = *session;
                match self.writer.sender.try_send(Request::Effect(effect)) {
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
                connection,
                delivery,
                packet_id,
                claim,
                topic,
                payload,
                dup,
            } => {
                let (connection, delivery, packet_id, claim, dup) =
                    (*connection, *delivery, *packet_id, *claim, *dup);
                if self.terminal_inputs.len() >= TERMINAL_INPUTS || self.frames.len() >= FRAMES {
                    return refused(effect, SubmitBlock::QueueFull);
                }
                let generation = connection.generation;
                let Some(peer) = self.peers.get_mut(&generation).filter(|p| {
                    p.closing.is_none()
                        && self.bindings.get(&connection.session) == Some(&generation)
                }) else {
                    let _ = effect.into_parts();
                    self.terminal_inputs.push_back(Input::FrameResult {
                        effect: id,
                        connection,
                        progress: FrameProgress::Failed,
                    });
                    return SubmitResult::Accepted;
                };
                let bytes = match codec::encode_publish(packet_id, topic, payload, dup) {
                    Ok(bytes) if bytes.len() <= peer.max_packet => bytes,
                    _ => {
                        self.close.push((generation, "outgoing_packet_limit"));
                        return refused(effect, SubmitBlock::Closed);
                    }
                };
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
                if self.outgoing_bindings.len() >= OUTGOING_BINDINGS {
                    return refused(effect, SubmitBlock::InFlightLimit);
                }
                // Register before the network task can observe this frame.
                // A rejected queue submission rolls this precise entry back.
                self.outgoing_bindings.insert(binding_key, binding);
                match peer.io.outgoing.try_send(net::Write {
                    correlation: Some((id, connection)),
                    bytes,
                }) {
                    Ok(()) => {
                        self.frames.insert(
                            id,
                            PendingFrame {
                                connection,
                                kind: "PUBLISH",
                                packet_id: Some(packet_id.get()),
                                receive_serial: None,
                                terminal: None,
                                wire_released: false,
                            },
                        );
                        let _ = effect.into_parts();
                        if self.events.emit("frame_queued",json!({"connection":generation.get().to_string(),"details":{"packet_kind":"PUBLISH","effect":effect_text(id),"delivery":delivery_text(delivery),"target":connection.session.get().to_string(),"packet_id":packet_id.get(),"claim":operation_text(claim),"dup":dup}})).is_err(){self.error=Some(Failure::plain("events_unavailable"));}
                        SubmitResult::Accepted
                    }
                    Err(TrySendError::Full(_)) => {
                        self.outgoing_bindings.remove(&binding_key);
                        self.close.push((generation, "outgoing_queue_full"));
                        refused(effect, SubmitBlock::QueueFull)
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
        Input::ObservedPuback { connection, .. } => Some(connection.generation),
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
        OutboundFrame::Suback { packet_id, .. } => ("SUBACK", Some(packet_id.get())),
        OutboundFrame::Unsuback { packet_id, .. } => ("UNSUBACK", Some(packet_id.get())),
    }
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
fn recipe_details(recipe: &Recipe) -> serde_json::Value {
    match recipe {
        Recipe::SessionLifecycle(lifecycle) => match &lifecycle.action {
            SessionAction::Open {
                expected,
                clean_start,
                expiry,
                ..
            } => {
                json!({"action":"Open","clean_start":clean_start,"expiry":format!("{expiry:?}"),"expected_session":expected.map(|g|g.session.get().to_string())})
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
        while writer.control_receiver.try_recv().is_ok() {}
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
fn check_startup_cancel(cancel: &AtomicBool) -> Result<(), Failure> {
    if cancel.load(Ordering::Relaxed) {
        Err(Failure::plain("startup_cancelled"))
    } else {
        Ok(())
    }
}
fn startup_reply<T>(
    receiver: &std::sync::mpsc::Receiver<T>,
    cancel: &AtomicBool,
    deadline: Instant,
    code: &'static str,
) -> Result<T, Failure> {
    loop {
        // An already produced typed failure is read before observing the stop
        // flag. Cancelling an outstanding request never calls it rejected.
        match receiver.try_recv() {
            Ok(reply) => return Ok(reply),
            Err(TryRecvError::Disconnected) => return Err(Failure::plain("writer_disconnected")),
            Err(TryRecvError::Empty) => {}
        }
        check_startup_cancel(cancel)?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(Failure::plain(code));
        }
        match receiver.recv_timeout(remaining.min(Duration::from_millis(10))) {
            Ok(reply) => return Ok(reply),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                return Err(Failure::plain("writer_disconnected"));
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
        }
    }
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
    let collect_closed = boot.collect_closed;
    let sqlite = boot.sqlite.clone();
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
            max_routes: nz(128),
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
        control_inputs: VecDeque::new(),
        closing_inputs: VecDeque::new(),
        terminal_inputs: VecDeque::new(),
        deferred_inputs: VecDeque::new(),
        timers: BTreeMap::new(),
        inflight: BTreeMap::new(),
        lifecycle_inflight: BTreeMap::new(),
        last_session_change: None,
        session_cardinality_dirty: false,
        last_session_cardinality: None,
        work_reads: BTreeMap::new(),
        last_work_scan: Instant::now(),
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
        frames: BTreeMap::new(),
        outgoing_bindings: BTreeMap::new(),
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
            runtime
                .writer
                .sender
                .send(Request::Recovery(cursor))
                .map_err(|_| Failure::plain("writer_closed"))?;
            let response = startup_reply(
                &runtime.writer.receiver,
                &cancel,
                Instant::now() + Duration::from_secs(10),
                "recovery_deadline",
            )?;
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
                runtime
                    .writer
                    .control_sender
                    .send(ControlRequest::Closed(collection))
                    .map_err(|_| Failure::plain("writer_control_disconnected"))?;
                let ControlResponse::Closed {
                    collection: found,
                    result,
                } = startup_reply(
                    &runtime.writer.control_receiver,
                    &cancel,
                    deadline,
                    "startup_collection_deadline",
                )?
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
        events.emit("receipt_policy",json!({"details":{"receipt_window":1024,"receipt_data_window":640,"receipt_control_reserve":512,"receipt_collection_rows":128,"receipt_collection_bytes":mqttiumd_store_api::ReadBudget::COLLECTION.max_bytes,"receipt_collection_trigger":64,"max_receipt_origins":64,"startup_origins_trigger":32,"startup_receipts_trigger":4096,"startup_closed_collection":collect_closed,"control_request_slots":crate::writer::CONTROL_QUEUE,"control_reply_slots":crate::writer::CONTROL_QUEUE,"control_input_slots":crate::writer::CONTROL_QUEUE}})).map_err(event_failure)?;
        events.emit("sqlite_configuration",json!({"details":{"journal_mode":sqlite.journal_mode,"synchronous":sqlite.synchronous,"wal_autocheckpoint":sqlite.wal_autocheckpoint,"checkpoint_policy":sqlite.checkpoint_policy,"sqlite_version":sqlite.sqlite_version,"user_version":sqlite.user_version,"page_size":sqlite.page_size,"kill_points":sqlite.kill_points,"hooks_active":false,"install_order":"sqlite-autocheckpoint-before-optional-business-hook"}})).map_err(event_failure)?;
        check_startup_cancel(&cancel)?;
        let listener =
            TcpListener::bind(listen).map_err(|_| Failure::plain("listen_unavailable"))?;
        listener
            .set_nonblocking(true)
            .map_err(|_| Failure::plain("listen_nonblocking"))?;
        let address = listener
            .local_addr()
            .map_err(|_| Failure::plain("listen_address"))?;
        check_startup_cancel(&cancel)?;
        events.emit("ready",json!({"details":{"address":address.to_string(),"epoch":epoch.get().to_string(),"incarnation":incarnation.get().to_string(),"max_connections":16,"max_sessions":128,"max_packet_bytes":65536,"max_client_id_bytes":1024,"max_topic_bytes":1024,"max_payload_bytes":32768,"receive_maximum":8,"max_exchange_slots":128,"input_slots":4,"output_slots":4,"writer_slots":32,"commands_in_flight":16,"max_timers":512,"receipt_slots":8192,"partial_packet_ms":5000,"connect_ms":3000,"write_ms":250,"remembered_connections":8,"max_subscriptions":128,"max_targets":16,"outgoing_per_connection":8,"outgoing_metadata":128,"outgoing_packet_identifier_max":65535,"outgoing_packet_identifier_probe_limit":129,"outgoing_payload_bytes":4194304,"work_page_items":8,"work_scan_ms":100}})).map_err(event_failure)?;
        println!("mqttiumd ready {address} (experimental exact pub/sub)");
        while !cancel.load(Ordering::Relaxed) {
            for _ in 0..4 {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let _ = runtime.accept(stream)?;
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(_) => return Err(Failure::plain("accept_failed")),
                }
            }
            runtime.writer_results()?;
            runtime.connections()?;
            runtime.discover_work();
            runtime.due_deadlines()?;
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
