//! The process under test: the real core and the deterministic store,
//! wired through their adapters, started by the real path.
//!
//! Start-up reads the ledger of the data set, opens the next epoch with
//! an `EpochOpening` that the store adapter carries through its writer,
//! takes the one grant of that opening, builds the creation authority
//! with `OwnerAuthority::from_grant`, creates the one box of the one
//! owner with that authority, builds the owner on it, and hands the
//! owner the recovery inventory the store adapter reads (ADR 0009, 0011,
//! 0014). Nothing here builds an identity, a grant or a result.
//!
//! The adapters of the process are the store adapter of lot L2
//! (`SimStore`), and two of the bench: the timer, which counts a
//! deadline from its acceptance of `ArmDeadline`, and the transport,
//! which turns client bytes into validated packets and the frames the
//! core made eligible into bytes. The effect box dispatches to them in
//! order through one sink, and stops at the first refusal.
//!
//! A crash drops the process: the owner, its box, the authority, the
//! timer, the transport and the volatile layer of the store adapter.
//! The durable state of the store survives and is reopened by the next
//! start-up, which opens a new epoch.

use std::collections::{BTreeMap, VecDeque};
use std::num::{NonZeroU32, NonZeroUsize};

use serde_json::{Value, json};

use mqttiumd_contract::capacity::{PayloadBytes, WorkUnits};
use mqttiumd_contract::effects::{
    Effect, EffectSink, Input, Outbox, OwnedEffect, OwnerAuthority, RecoveredFact, SubmitBlock,
    SubmitResult,
};
use mqttiumd_contract::ids::{
    AccountId, ConnectionGeneration, ConnectionKey, DeadlineToken, EffectId, LogicalDuration,
    LogicalOwnerId, LogicalTime, OperationId, RunEpoch,
};
use mqttiumd_contract::opening::OpeningRejection;
use mqttiumd_contract::outcome::{
    EpochOpening, Observed as OpeningObserved, OpeningOutcome, OpeningReply, OpeningStage,
    RecoveredEpochCheck, StoreReply,
};
use mqttiumd_contract::packet::OutboundFrame;
use mqttiumd_contract::step::{StepOutcome, TurnGrant};
use mqttiumd_core::owner::{Owner, OwnerConfig};

use super::collector::{Cause, Collector, Origin};
use super::mqtt::transport;
use crate::store::adapter::{
    AdapterLimits, Delivery, FaultAction, FaultId, FaultSpec, Performed, SimStore, StoreAction,
    StoreEvent, WorkId, startup_nonce,
};
use crate::store::durable::{DurableStore, MemoryStore};
use crate::store::evidence::{Cause as EvidenceCause, TraceWriter, recovered_fact, store_outcome};

/// Why the run cannot go on. The trace is then declared incomplete.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Stop(pub String);

impl Stop {
    pub fn new(reason: impl Into<String>) -> Self {
        Self(reason.into())
    }
}

/// The parameters of the process, the same at every start-up.
#[derive(Clone, Copy, Debug)]
pub struct Settings {
    /// The slug of the run, in every record.
    pub run_id: &'static str,
    pub seed: u64,
    pub resolution_delay: u64,
    pub work_limit: u64,
    pub recovery_page: usize,
    pub effect_slots: NonZeroUsize,
    pub reservations: NonZeroU32,
    pub adapter: AdapterLimits,
    pub owner: OwnerConfig,
    pub bounds: transport::Bounds,
}

/// The owner of S0: one logical owner, owner 0.
pub const OWNER: LogicalOwnerId = LogicalOwnerId::new(0);

/// Builds the configuration of the owner from what the fixture fixes.
pub fn owner_config(
    max_sessions: u32,
    max_routes: u32,
    max_exchanges: u32,
    payload_bytes: u64,
    resolution_delay: u64,
) -> Result<OwnerConfig, Stop> {
    let bound = |value: u32, name: &str| {
        NonZeroU32::new(value).ok_or_else(|| Stop::new(format!("the fixture gives no {name}")))
    };
    Ok(OwnerConfig {
        max_sessions: bound(max_sessions, "session")?,
        max_routes: bound(max_routes, "route target")?,
        max_exchanges: bound(max_exchanges, "exchange slot")?,
        account: AccountId::new(1),
        payload_bytes: PayloadBytes::new(payload_bytes),
        resolution_delay: LogicalDuration::new(resolution_delay),
        retained_operations: bound(16, "retained operation")?,
        remembered_connections: bound(16, "remembered connection")?,
    })
}

fn decimal(value: u64) -> Value {
    Value::String(value.to_string())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn effect_json(effect: EffectId) -> Value {
    json!({
        "epoch": decimal(effect.epoch.get()),
        "owner": decimal(effect.owner.get()),
        "sequence": decimal(effect.sequence.get()),
    })
}

fn operation_json(operation: OperationId) -> Value {
    json!({
        "incarnation": decimal(operation.incarnation().get()),
        "epoch": decimal(operation.epoch().get()),
        "owner": decimal(operation.owner().get()),
        "sequence": decimal(operation.sequence().get()),
    })
}

fn connection_json(connection: ConnectionKey) -> Value {
    json!({
        "session": decimal(connection.session.get()),
        "generation": decimal(connection.generation.get()),
    })
}

/// The payload of `StoreResultDelivered` for the result the core took,
/// or `None` for a violation, which the format has no outcome for.
fn delivered_json(effect: EffectId, operation: OperationId, reply: &StoreReply) -> Option<Value> {
    let (name, rejected) = store_outcome(*reply)?;
    let mut payload = json!({
        "effect_id": effect_json(effect),
        "operation_id": operation_json(operation),
        "outcome": name,
    });
    if let Some(rejected) = rejected {
        payload["reject_reason"] = json!(rejected);
    }
    Some(payload)
}

/// The payload of `OpeningReplyDelivered`: the whole command the reply
/// answers and its outcome.
fn opening_reply_json(reply: &OpeningReply) -> Value {
    let command = reply.command;
    let mut payload = json!({
        "attempt": {
            "startup_hex": hex(&command.attempt.startup.to_bytes()),
            "ticket": decimal(command.attempt.ticket.get()),
        },
        "incarnation": decimal(command.incarnation.get()),
        "after": decimal(command.after.get()),
    });
    let outcome = match reply.outcome {
        OpeningOutcome::Committed(opened) => {
            payload["epoch"] = decimal(opened.epoch.get());
            "Committed"
        }
        OpeningOutcome::RejectedCertain(rejection) => {
            payload["reject_reason"] = json!(match rejection {
                OpeningRejection::EpochsExhausted => "EpochsExhausted",
                OpeningRejection::ForeignIncarnation => "ForeignIncarnation",
                OpeningRejection::PredecessorMoved => "PredecessorMoved",
                OpeningRejection::AttemptReused => "AttemptReused",
            });
            "RejectedCertain"
        }
        OpeningOutcome::Superseded => "Superseded",
        OpeningOutcome::Unknown => "Unknown",
    };
    payload["outcome"] = json!(outcome);
    payload
}

fn ack_reason(frame: OutboundFrame) -> Option<&'static str> {
    use mqttiumd_contract::packet::AckReason;
    let reason = match frame {
        OutboundFrame::Connack { .. } => return None,
        OutboundFrame::Pubrec { reason, .. } | OutboundFrame::Pubcomp { reason, .. } => reason,
    };
    Some(match reason {
        AckReason::Success => "Success",
        AckReason::PacketIdentifierNotFound => "PacketIdentifierNotFound",
        AckReason::QuotaExceeded => "QuotaExceeded",
        AckReason::ImplementationSpecificError => "ImplementationSpecificError",
    })
}

/// An armed deadline of the timer adapter.
#[derive(Clone, Copy, Debug)]
struct Armed {
    token: DeadlineToken,
    due: u64,
    order: u64,
    cause: Cause,
}

/// The timer adapter: logical time, advanced only by `FireDeadline`.
#[derive(Debug, Default)]
struct Timer {
    now: u64,
    armed: Vec<Armed>,
    next_order: u64,
}

/// One connection of the transport.
#[derive(Debug)]
struct Connection {
    client: String,
    generation: ConnectionGeneration,
    ingress: u64,
    inbound: VecDeque<(Vec<u8>, Cause)>,
    open: bool,
}

/// A frame the core made eligible, waiting for the transport to write it.
#[derive(Debug)]
struct Frame {
    effect: EffectId,
    connection: ConnectionKey,
    frame: OutboundFrame,
    bytes: Vec<u8>,
    cause: Cause,
}

/// The connection adapter of the bench.
#[derive(Debug, Default)]
struct Transport {
    connections: Vec<Connection>,
    frames: VecDeque<Frame>,
}

/// What the bench remembers of the evidence, to name causes.
#[derive(Debug, Default)]
pub struct Books {
    /// The core record whose step published each effect.
    published_by: BTreeMap<EffectId, Cause>,
    /// The `StoreResultHandedOver` record of each piece of work whose
    /// result the adapter handed towards the core, the latest.
    handed: BTreeMap<WorkId, Cause>,
    /// The `OpeningSubmitted` record of each opening attempt.
    opening_submitted: BTreeMap<WorkId, Cause>,
    /// For each fault of this incarnation, the scenario fault it is.
    pub scenario_fault: Vec<usize>,
    /// Hits of each scenario fault, over the whole run.
    pub hits: Vec<u64>,
    /// The work each scenario fault holds, while it holds one.
    pub held: BTreeMap<usize, WorkId>,
    /// Events no record of the formats carries, by kind, counted.
    pub unwritten: BTreeMap<String, u64>,
}

/// What a frame written by the transport showed a peer.
#[derive(Debug)]
pub struct Written {
    pub client: String,
    pub bytes: Vec<u8>,
    pub cause: Cause,
}

/// What the scheduler may choose inside the process.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Choice {
    /// Deliver the next packet a connection received.
    Packet(ConnectionGeneration),
    /// One action of the store adapter.
    Store(StoreAction),
    /// Write the next eligible frame.
    Frame,
}

impl Choice {
    /// A stable identity of the choice for the choice log.
    #[must_use]
    pub fn describe(&self) -> String {
        let work = |id: &WorkId| match id {
            WorkId::Effect(effect) => format!(
                "effect:{}/{}/{}",
                effect.epoch.get(),
                effect.owner.get(),
                effect.sequence.get()
            ),
            WorkId::Opening(attempt) => format!(
                "opening:{}/{}",
                hex(&attempt.startup.to_bytes()),
                attempt.ticket.get()
            ),
        };
        match self {
            Self::Packet(generation) => format!("deliver-packet connection:{}", generation.get()),
            Self::Store(StoreAction::EnterWriter(id)) => format!("enter-writer {}", work(id)),
            Self::Store(StoreAction::ReleaseCommand(id)) => format!("release-command {}", work(id)),
            Self::Store(StoreAction::DeliverResult(id)) => format!("deliver-result {}", work(id)),
            Self::Store(StoreAction::ReleaseResult(id)) => format!("release-result {}", work(id)),
            Self::Frame => "write-frame".to_owned(),
        }
    }
}

/// The process under test.
pub struct Process {
    settings: Settings,
    opening: EpochOpening,
    epoch: RunEpoch,
    store: SimStore<MemoryStore>,
    /// Kept for the life of the process: the one creator of boxes.
    authority: OwnerAuthority,
    owner: Owner,
    writer: TraceWriter<'static>,
    timer: Timer,
    transport: Transport,
    /// An input the core handed back for lack of capacity, with the
    /// record to write once it is applied.
    parked: Option<(Input, Option<Pending>)>,
    /// A result the adapter handed over at a barrier: the core has not
    /// finished consuming it, and does not until the barrier is left.
    consuming: Option<(Input, Option<Pending>)>,
}

/// The record of an input, written once the core applied it.
#[derive(Debug)]
pub struct Pending {
    kind: &'static str,
    payload: Value,
    parents: Vec<Cause>,
}

/// Which channel carries a store event: the component the event belongs
/// to, by its variant (record 0016).
fn channel_of(event: &StoreEvent) -> Option<Origin> {
    Some(match event {
        StoreEvent::CommandSubmitted { .. }
        | StoreEvent::RecoveryPageRead { .. }
        | StoreEvent::ResultDelivered { .. } => Origin::StoreAdapter,
        StoreEvent::CommitStable { .. }
        | StoreEvent::FenceStable { .. }
        | StoreEvent::FenceRefused { .. }
        | StoreEvent::ResultReady { .. } => Origin::StoreEvidence,
        StoreEvent::FaultHit { .. } | StoreEvent::HeldItemReleased { .. } => Origin::FaultInjector,
        StoreEvent::UncorrelatedResult { .. } => Origin::Dispatcher,
    })
}

fn event_name(event: &StoreEvent) -> &'static str {
    match event {
        StoreEvent::CommandSubmitted { .. } => "CommandSubmitted",
        StoreEvent::CommitStable { .. } => "CommitStable",
        StoreEvent::FenceStable { .. } => "FenceStable",
        StoreEvent::FenceRefused { .. } => "FenceRefused",
        StoreEvent::ResultReady { .. } => "ResultReady",
        StoreEvent::ResultDelivered { .. } => "ResultDelivered",
        StoreEvent::FaultHit { .. } => "FaultHit",
        StoreEvent::UncorrelatedResult { .. } => "UncorrelatedResult",
        StoreEvent::RecoveryPageRead { .. } => "RecoveryPageRead",
        StoreEvent::HeldItemReleased { .. } => "HeldItemReleased",
    }
}

/// Writes what the store adapter recorded, event by event, each on the
/// channel of its component. Returns the cause of the last
/// `RecoveryPageRead`, when one was written.
fn flush(
    store: &mut SimStore<MemoryStore>,
    writer: &mut TraceWriter<'static>,
    collector: &mut Collector,
    books: &mut Books,
) -> Result<Option<Cause>, Stop> {
    if store.evidence_lost() {
        return Err(Stop::new("the store adapter lost evidence"));
    }
    let mut page = None;
    for event in store.take_events() {
        // A submission names, as its cause, the record of the core input
        // whose step published the effect it carries (record 0016).
        let causes: Vec<EvidenceCause> = match &event {
            StoreEvent::CommandSubmitted {
                id: WorkId::Effect(effect),
                ..
            } => books
                .published_by
                .get(effect)
                .map(|cause| EvidenceCause {
                    component: cause.origin.name(),
                    incarnation: cause.incarnation,
                    sequence: cause.sequence,
                })
                .into_iter()
                .collect(),
            _ => Vec::new(),
        };
        let record = writer.write_caused(&event, &causes).map_err(|refused| {
            Stop::new(format!("the store writer refused causes: {refused:?}"))
        })?;
        let (Some(channel), Some(record)) = (channel_of(&event), record.as_ref()) else {
            *books
                .unwritten
                .entry(event_name(&event).to_owned())
                .or_insert(0) += 1;
            continue;
        };
        let cause = collector
            .accept(channel, record)
            .map_err(|refusal| Stop::new(format!("the collector refused a record: {refusal}")))?;
        match event {
            StoreEvent::ResultDelivered { id, .. } => {
                books.handed.insert(id, cause);
            }
            StoreEvent::CommandSubmitted {
                id: id @ WorkId::Opening(_),
                ..
            } => {
                books.opening_submitted.insert(id, cause);
            }
            StoreEvent::HeldItemReleased { fault } => {
                if let Some(scenario) = books.scenario_fault.get(fault.0).copied() {
                    books.held.remove(&scenario);
                }
            }
            StoreEvent::FaultHit {
                fault, action, id, ..
            } => {
                if let Some(scenario) = books.scenario_fault.get(fault.0).copied() {
                    if let Some(hits) = books.hits.get_mut(scenario) {
                        *hits = hits.saturating_add(1);
                    }
                    if matches!(action, FaultAction::HoldCommand | FaultAction::DelayResult) {
                        books.held.insert(scenario, id);
                    }
                }
            }
            StoreEvent::RecoveryPageRead { .. } => page = Some(cause),
            _ => {}
        }
    }
    Ok(page)
}

/// The sink of the effect box: one entry point for the three adapters.
struct Sink<'a> {
    store: &'a mut SimStore<MemoryStore>,
    writer: &'a mut TraceWriter<'static>,
    timer: &'a mut Timer,
    transport: &'a mut Transport,
    collector: &'a mut Collector,
    books: &'a mut Books,
    stop: Option<Stop>,
}

impl Sink<'_> {
    fn record(
        &mut self,
        channel: Origin,
        kind: &str,
        parents: &[Cause],
        payload: Value,
    ) -> Option<Cause> {
        match self.collector.emit(channel, kind, parents, payload) {
            Ok(cause) => Some(cause),
            Err(refusal) => {
                self.stop.get_or_insert(Stop::new(format!(
                    "the collector refused a record: {refusal}"
                )));
                None
            }
        }
    }
}

impl EffectSink for Sink<'_> {
    fn try_submit(&mut self, effect: OwnedEffect) -> SubmitResult {
        if self.stop.is_some() {
            return SubmitResult::NotSubmitted {
                effect,
                reason: SubmitBlock::Closed,
            };
        }
        let id = effect.id();
        let parent: Vec<Cause> = self
            .books
            .published_by
            .get(&id)
            .copied()
            .into_iter()
            .collect();
        match effect.effect() {
            Effect::Persist { .. } | Effect::ResolveAndFence { .. } => {
                let result = self.store.try_submit(effect);
                if result == SubmitResult::Accepted
                    && let Err(stop) = flush(self.store, self.writer, self.collector, self.books)
                {
                    self.stop = Some(stop);
                }
                result
            }
            Effect::ArmDeadline { token, after } => {
                let Some(due) = self.timer.now.checked_add(after.get()) else {
                    return SubmitResult::NotSubmitted {
                        effect,
                        reason: SubmitBlock::Closed,
                    };
                };
                let (token, after) = (*token, *after);
                let payload = json!({
                    "effect_id": effect_json(id),
                    "token": decimal(token.get()),
                    "after": decimal(after.get()),
                });
                let Some(cause) = self.record(Origin::Timer, "DeadlineArmed", &parent, payload)
                else {
                    return SubmitResult::NotSubmitted {
                        effect,
                        reason: SubmitBlock::Closed,
                    };
                };
                let order = self.timer.next_order;
                self.timer.next_order += 1;
                self.timer.armed.push(Armed {
                    token,
                    due,
                    order,
                    cause,
                });
                let _ = effect.into_parts();
                SubmitResult::Accepted
            }
            Effect::EmitFrame { connection, frame } => {
                let (connection, frame) = (*connection, *frame);
                let bytes = match transport::encode(frame) {
                    Ok(bytes) => bytes,
                    Err(_) => {
                        return SubmitResult::NotSubmitted {
                            effect,
                            reason: SubmitBlock::Closed,
                        };
                    }
                };
                let (kind, packet_id) = transport::describe(frame);
                let mut payload = json!({
                    "effect_id": effect_json(id),
                    "connection": connection_json(connection),
                    "packet_kind": kind.name(),
                });
                if let Some(packet_id) = packet_id {
                    payload["packet_id"] = json!(packet_id);
                }
                if let Some(reason) = ack_reason(frame) {
                    payload["reason"] = json!(reason);
                }
                if let OutboundFrame::Connack { session_present } = frame {
                    payload["session_present"] = json!(session_present);
                }
                let Some(cause) =
                    self.record(Origin::BrokerCore, "FrameEligible", &parent, payload)
                else {
                    return SubmitResult::NotSubmitted {
                        effect,
                        reason: SubmitBlock::Closed,
                    };
                };
                self.transport.frames.push_back(Frame {
                    effect: id,
                    connection,
                    frame,
                    bytes,
                    cause,
                });
                let _ = effect.into_parts();
                SubmitResult::Accepted
            }
            Effect::CloseConnection { connection, .. } => {
                let generation = *connection;
                if let Some(open) = self
                    .transport
                    .connections
                    .iter_mut()
                    .find(|open| open.generation == generation && open.open)
                {
                    open.open = false;
                    open.inbound.clear();
                    let payload = json!({"client": open.client});
                    let _ = self.record(Origin::Peer, "PeerConnectionClosed", &[], payload);
                }
                let _ = effect.into_parts();
                SubmitResult::Accepted
            }
            // No adapter of S0 takes these: the box keeps them.
            Effect::LoadWork { .. } | Effect::Continue { .. } => SubmitResult::NotSubmitted {
                effect,
                reason: SubmitBlock::Closed,
            },
        }
    }
}

impl Books {
    /// Books for a scenario of `faults` faults.
    #[must_use]
    pub fn new(faults: usize) -> Self {
        Self {
            hits: vec![0; faults],
            ..Self::default()
        }
    }
}

impl Process {
    /// Starts the process on `store` by the real path, arms the faults
    /// `faults` (scenario index and specification), and recovers.
    ///
    /// `rank` counts the start-ups before this one: the nonce of the
    /// opening is drawn from the seed and the rank.
    pub fn start(
        store: MemoryStore,
        rank: u64,
        settings: Settings,
        faults: &[(usize, FaultSpec, &'static str)],
        collector: &mut Collector,
        books: &mut Books,
        choices: &mut Vec<String>,
    ) -> Result<Self, Stop> {
        let mut sim = SimStore::new(store, settings.adapter);
        // The writer of the store's evidence lives as long as the
        // process; the faults are named in the order they are armed.
        let names: Vec<&'static str> = faults.iter().map(|(_, _, name)| *name).collect();
        let names: &'static [&'static str] = Box::leak(names.into_boxed_slice());
        let mut writer = TraceWriter::new(
            settings.run_id,
            collector.incarnation(Origin::StoreAdapter),
            0,
            names,
        );
        // The opening: read the ledger, submit, writer, reply, grant.
        let mut opening = sim.begin_opening(startup_nonce(settings.seed, rank));
        let command = opening
            .submit(sim.store().ledger().last_opened())
            .map_err(|refused| Stop::new(format!("the opening cannot start: {refused:?}")))?;
        sim.submit_opening(command).map_err(|blocked| {
            Stop::new(format!("the adapter refused the opening: {blocked:?}"))
        })?;
        flush(&mut sim, &mut writer, collector, books)?;
        let id = WorkId::Opening(command.attempt);
        let mut reply_cause = None;
        for action in [StoreAction::EnterWriter(id), StoreAction::DeliverResult(id)] {
            choices.push(Choice::Store(action).describe());
            let performed = sim.perform(action);
            flush(&mut sim, &mut writer, collector, books)?;
            match performed {
                Ok(Performed::Executed) => {}
                Ok(Performed::Delivered {
                    delivery: Delivery::Opening(reply),
                    barrier: false,
                }) => {
                    let parent: Vec<Cause> = books
                        .opening_submitted
                        .get(&id)
                        .copied()
                        .into_iter()
                        .collect();
                    let cause = collector
                        .emit(
                            Origin::BrokerCore,
                            "OpeningReplyDelivered",
                            &parent,
                            opening_reply_json(&reply),
                        )
                        .map_err(|refusal| {
                            Stop::new(format!("the collector refused a record: {refusal}"))
                        })?;
                    reply_cause = Some(cause);
                    match opening.observe(reply) {
                        Ok(OpeningObserved::Advanced(OpeningStage::Committed(_))) => {}
                        other => {
                            return Err(Stop::new(format!(
                                "the opening did not commit: {other:?}"
                            )));
                        }
                    }
                }
                other => return Err(Stop::new(format!("the opening went wrong: {other:?}"))),
            }
        }
        let grant = opening
            .hand_over()
            .map_err(|refused| Stop::new(format!("no grant: {refused:?}")))?;
        let epoch = grant.epoch();
        collector
            .emit(
                Origin::BrokerCore,
                "EpochGrantHandedOver",
                &reply_cause.into_iter().collect::<Vec<_>>(),
                json!({
                    "incarnation": decimal(grant.incarnation().get()),
                    "epoch": decimal(epoch.get()),
                }),
            )
            .map_err(|refusal| Stop::new(format!("the collector refused a record: {refusal}")))?;
        let mut authority = OwnerAuthority::from_grant(grant, NonZeroU32::MIN)
            .map_err(|_| Stop::new("the authority could not be allocated"))?;
        let outbox = authority
            .create_outbox(OWNER, settings.effect_slots, settings.reservations)
            .map_err(|refused| Stop::new(format!("no effect box: {refused:?}")))?;
        let owner = Owner::new(settings.owner, outbox)
            .map_err(|refused| Stop::new(format!("no owner: {:?}", refused.reason)))?;

        // The faults of the scenario act on the broker's work; they are
        // armed once the epoch is open.
        books.scenario_fault.clear();
        for (scenario, spec, name) in faults {
            let armed = sim.arm(*spec).map_err(|refused| {
                Stop::new(format!("the fault {name} was not armed: {refused:?}"))
            })?;
            debug_assert_eq!(armed, FaultId(books.scenario_fault.len()));
            books.scenario_fault.push(*scenario);
        }
        let mut process = Self {
            settings,
            opening,
            epoch,
            store: sim,
            authority,
            owner,
            writer,
            timer: Timer::default(),
            transport: Transport::default(),
            parked: None,
            consuming: None,
        };
        flush(&mut process.store, &mut process.writer, collector, books)?;
        Ok(process)
    }

    /// The epoch this process opened.
    #[must_use]
    pub fn epoch(&self) -> RunEpoch {
        self.epoch
    }

    /// Creates an owner the way start-up does: a box from the authority
    /// of this process, then the owner on it. The process is the one
    /// creator of owners; this is how a composition test asks it for a
    /// second owner of an identity it already holds.
    pub fn create_owner(&mut self, owner: LogicalOwnerId) -> Result<Owner, String> {
        let outbox = self
            .authority
            .create_outbox(
                owner,
                self.settings.effect_slots,
                self.settings.reservations,
            )
            .map_err(|refused| format!("{refused:?}"))?;
        let mut config = self.settings.owner;
        config.account = AccountId::new(owner.get().saturating_add(1));
        Owner::new(config, outbox).map_err(|refused| format!("{:?}", refused.reason))
    }

    /// The owners the authority of this process keeps active.
    #[must_use]
    pub fn active_owners(&self) -> usize {
        self.authority.active_owners()
    }

    /// Reads the whole recovery inventory and hands it to the core,
    /// page by page.
    pub fn recover(&mut self, collector: &mut Collector, books: &mut Books) -> Result<(), Stop> {
        let mut cursor = None;
        let mut page_number = 0u64;
        loop {
            let page = match self.store.recover(cursor, self.settings.recovery_page) {
                Ok(page) => page,
                Err(crate::store::durable::RecoveryRefused::FrontierMoved) => {
                    cursor = None;
                    page_number = 0;
                    continue;
                }
                Err(refused) => return Err(Stop::new(format!("recovery refused: {refused:?}"))),
            };
            let read = flush(&mut self.store, &mut self.writer, collector, books)?
                .ok_or_else(|| Stop::new("a recovery page was read without its record"))?;
            for fact in page.facts.as_slice() {
                if let RecoveredFact::Epoch { incarnation, epoch } = fact
                    && self.opening.check_recovered(*incarnation, *epoch)
                        != RecoveredEpochCheck::Confirms
                {
                    return Err(Stop::new(
                        "the inventory does not confirm the epoch this process opened",
                    ));
                }
            }
            let last = page.next.is_none();
            let facts: Vec<Value> = page.facts.as_slice().iter().map(recovered_fact).collect();
            let pending = Pending {
                kind: "RecoveryPageDelivered",
                payload: json!({
                    "run_epoch": decimal(self.epoch.get()),
                    "page": decimal(page_number),
                    "last": last,
                    "facts": facts,
                }),
                parents: vec![read],
            };
            let input = Input::Recovery {
                facts: page.facts,
                last,
            };
            self.input(input, Some(pending), collector, books)?;
            if self.parked.is_some() {
                return Err(Stop::new("the core handed a recovery page back"));
            }
            match page.next {
                None => return Ok(()),
                Some(next) => {
                    cursor = Some(next);
                    page_number += 1;
                }
            }
        }
    }

    /// Hands one input to the core. Applied, its record is written and
    /// the effects it published are dispatched; handed back for lack of
    /// capacity, it is parked and tried again after the next choice.
    fn input(
        &mut self,
        input: Input,
        pending: Option<Pending>,
        collector: &mut Collector,
        books: &mut Books,
    ) -> Result<(), Stop> {
        let grant = TurnGrant {
            work_limit: WorkUnits::new(self.settings.work_limit),
        };
        match self.owner.step(input, grant) {
            StepOutcome::Applied { .. } => {
                let cause = match pending {
                    Some(pending) => Some(
                        collector
                            .emit(
                                Origin::BrokerCore,
                                pending.kind,
                                &pending.parents,
                                pending.payload,
                            )
                            .map_err(|refusal| {
                                Stop::new(format!("the collector refused a record: {refusal}"))
                            })?,
                    ),
                    None => None,
                };
                if let Some(cause) = cause {
                    for effect in self.owner.outbox().into_iter().flat_map(Outbox::pending) {
                        books.published_by.entry(effect.id()).or_insert(cause);
                    }
                }
                self.dispatch(collector, books)
            }
            StepOutcome::NeedCapacity { input, .. } => {
                if self.parked.is_some() {
                    return Err(Stop::new("two inputs wait for capacity"));
                }
                self.parked = Some((input, pending));
                Ok(())
            }
            StepOutcome::Exhausted { exhaustion, .. } => {
                Err(Stop::new(format!("the core is exhausted: {exhaustion:?}")))
            }
            StepOutcome::Fatal(fault) => Err(Stop::new(format!("core fault: {fault:?}"))),
        }
    }

    /// Moves the effects of the box to the adapters, in order.
    fn dispatch(&mut self, collector: &mut Collector, books: &mut Books) -> Result<(), Stop> {
        let mut sink = Sink {
            store: &mut self.store,
            writer: &mut self.writer,
            timer: &mut self.timer,
            transport: &mut self.transport,
            collector,
            books,
            stop: None,
        };
        let _report = self.owner.dispatch(&mut sink);
        match sink.stop {
            Some(stop) => Err(stop),
            None => Ok(()),
        }
    }

    /// Whether an effect still waits in the box, or an input for room.
    #[must_use]
    pub fn busy(&self) -> bool {
        self.owner.outbox().is_some_and(|outbox| !outbox.is_empty()) || self.parked.is_some()
    }

    /// After each choice: the parked input again, then the box.
    pub fn settle(&mut self, collector: &mut Collector, books: &mut Books) -> Result<(), Stop> {
        if let Some((input, pending)) = self.parked.take() {
            self.input(input, pending, collector, books)?;
        }
        self.dispatch(collector, books)
    }

    /// The choices the scheduler may make now, in a fixed order.
    #[must_use]
    pub fn choices(&self) -> Vec<Choice> {
        let mut choices: Vec<Choice> = self
            .transport
            .connections
            .iter()
            .filter(|connection| connection.open && !connection.inbound.is_empty())
            .map(|connection| Choice::Packet(connection.generation))
            .collect();
        // Releases belong to the steps that name a fault.
        choices.extend(
            self.store
                .eligible()
                .into_iter()
                .filter(|action| {
                    matches!(
                        action,
                        StoreAction::EnterWriter(_) | StoreAction::DeliverResult(_)
                    )
                })
                .map(Choice::Store),
        );
        if !self.transport.frames.is_empty() {
            choices.push(Choice::Frame);
        }
        choices
    }

    /// Opens a connection of `client`, with the generation the bench
    /// assigned to it.
    pub fn connect(&mut self, client: &str, generation: ConnectionGeneration) {
        self.transport.connections.push(Connection {
            client: client.to_owned(),
            generation,
            ingress: 0,
            inbound: VecDeque::new(),
            open: true,
        });
    }

    /// The transport receives one whole packet on a connection.
    pub fn receive(
        &mut self,
        generation: ConnectionGeneration,
        bytes: &[u8],
        sent: Cause,
        collector: &mut Collector,
    ) -> Result<(), Stop> {
        let Some(connection) = self
            .transport
            .connections
            .iter_mut()
            .find(|connection| connection.generation == generation && connection.open)
        else {
            // A closed connection carries nothing to the broker.
            return Ok(());
        };
        let cause = collector
            .emit(
                Origin::Transport,
                "ClientBytesReceived",
                &[sent],
                json!({
                    "connection_generation": decimal(generation.get()),
                    "bytes_hex": hex(bytes),
                }),
            )
            .map_err(|refusal| Stop::new(format!("the collector refused a record: {refusal}")))?;
        connection.inbound.push_back((bytes.to_vec(), cause));
        Ok(())
    }

    /// Performs one choice. A frame written returns what its peer saw.
    pub fn perform(
        &mut self,
        choice: &Choice,
        collector: &mut Collector,
        books: &mut Books,
    ) -> Result<Option<Written>, Stop> {
        match choice {
            Choice::Packet(generation) => {
                let bounds = self.settings.bounds;
                let connection = self
                    .transport
                    .connections
                    .iter_mut()
                    .find(|connection| connection.generation == *generation && connection.open)
                    .ok_or_else(|| Stop::new("no such connection"))?;
                let (bytes, received) = connection
                    .inbound
                    .pop_front()
                    .ok_or_else(|| Stop::new("nothing received"))?;
                let ingress = connection.ingress;
                connection.ingress += 1;
                let (packet, kind, packet_id) = transport::decode(&bytes, bounds)
                    .map_err(|why| Stop::new(format!("the transport cannot decode: {why}")))?;
                let mut payload = json!({
                    "connection_generation": decimal(generation.get()),
                    "ingress_order": decimal(ingress),
                    "packet_kind": kind.name(),
                });
                if let Some(packet_id) = packet_id {
                    payload["packet_id"] = json!(packet_id);
                }
                let input = Input::Packet {
                    connection: *generation,
                    ingress_order: ingress,
                    packet,
                };
                let pending = Pending {
                    kind: "PacketDelivered",
                    payload,
                    parents: vec![received],
                };
                self.input(input, Some(pending), collector, books)?;
                Ok(None)
            }
            Choice::Store(action) => {
                let performed = self.store.perform(*action).map_err(|refused| {
                    Stop::new(format!("the store refused {action:?}: {refused:?}"))
                })?;
                flush(&mut self.store, &mut self.writer, collector, books)?;
                if let Performed::Delivered { delivery, barrier } = performed {
                    self.consume(delivery, barrier, collector, books)?;
                }
                Ok(None)
            }
            Choice::Frame => {
                let frame = self
                    .transport
                    .frames
                    .pop_front()
                    .ok_or_else(|| Stop::new("no frame"))?;
                let connection = self
                    .transport
                    .connections
                    .iter()
                    .find(|open| open.generation == frame.connection.generation && open.open);
                let Some(connection) = connection else {
                    let input = Input::FrameResult {
                        effect: frame.effect,
                        connection: frame.connection,
                        progress: mqttiumd_contract::effects::FrameProgress::Failed,
                    };
                    self.input(input, None, collector, books)?;
                    return Ok(None);
                };
                let client = connection.client.clone();
                let total = frame.bytes.len() as u64;
                let progress = collector
                    .emit(
                        Origin::Transport,
                        "FrameWriteProgress",
                        &[frame.cause],
                        json!({
                            "effect_id": effect_json(frame.effect),
                            "connection": connection_json(frame.connection),
                            "bytes_confirmed": decimal(total),
                            "bytes_total": decimal(total),
                        }),
                    )
                    .map_err(|refusal| {
                        Stop::new(format!("the collector refused a record: {refusal}"))
                    })?;
                let _ = frame.frame;
                let input = Input::FrameResult {
                    effect: frame.effect,
                    connection: frame.connection,
                    progress: mqttiumd_contract::effects::FrameProgress::Written {
                        confirmed: total,
                        total,
                    },
                };
                self.input(input, None, collector, books)?;
                Ok(Some(Written {
                    client,
                    bytes: frame.bytes,
                    cause: progress,
                }))
            }
        }
    }

    /// The core takes a result the dispatcher handed over, unless the
    /// adapter stopped at a barrier right after.
    fn consume(
        &mut self,
        delivery: Delivery,
        barrier: bool,
        collector: &mut Collector,
        books: &mut Books,
    ) -> Result<(), Stop> {
        let Delivery::Store(input) = delivery else {
            return Err(Stop::new("an opening reply after start-up"));
        };
        let Input::StoreResult {
            effect,
            operation,
            reply,
        } = &input
        else {
            return Err(Stop::new("a store delivery that is not a result"));
        };
        let id = WorkId::Effect(*effect);
        let parent = books.handed.remove(&id);
        let pending = delivered_json(*effect, *operation, reply).map(|payload| Pending {
            kind: "StoreResultDelivered",
            payload,
            parents: parent.into_iter().collect(),
        });
        if barrier {
            self.consuming = Some((input, pending));
            return Ok(());
        }
        self.input(input, pending, collector, books)
    }

    /// Whether the store adapter is stopped at a barrier.
    #[must_use]
    pub fn halted(&self) -> bool {
        self.store.halted_at().is_some()
    }

    /// Leaves the barrier the store adapter is stopped at, if any: the
    /// adapter goes on from that point, and a result handed over at the
    /// barrier is consumed by the core.
    pub fn leave_barrier(
        &mut self,
        collector: &mut Collector,
        books: &mut Books,
    ) -> Result<(), Stop> {
        if self.store.resume().is_none() {
            return Ok(());
        }
        flush(&mut self.store, &mut self.writer, collector, books)?;
        if let Some((input, pending)) = self.consuming.take() {
            self.input(input, pending, collector, books)?;
        }
        self.settle(collector, books)
    }

    /// `ReleaseHeldCommand`: the command the fault holds may enter the
    /// writer.
    pub fn release_command(
        &mut self,
        id: WorkId,
        collector: &mut Collector,
        books: &mut Books,
    ) -> Result<(), Stop> {
        self.store
            .perform(StoreAction::ReleaseCommand(id))
            .map_err(|refused| {
                Stop::new(format!("the held command was not released: {refused:?}"))
            })?;
        flush(&mut self.store, &mut self.writer, collector, books)?;
        Ok(())
    }

    /// `DeliverHeldResult`: the delayed result is released and handed to
    /// the core.
    pub fn deliver_held_result(
        &mut self,
        id: WorkId,
        collector: &mut Collector,
        books: &mut Books,
    ) -> Result<(), Stop> {
        self.store
            .perform(StoreAction::ReleaseResult(id))
            .map_err(|refused| {
                Stop::new(format!("the held result was not released: {refused:?}"))
            })?;
        flush(&mut self.store, &mut self.writer, collector, books)?;
        let performed = self
            .store
            .perform(StoreAction::DeliverResult(id))
            .map_err(|refused| {
                Stop::new(format!("the held result was not delivered: {refused:?}"))
            })?;
        flush(&mut self.store, &mut self.writer, collector, books)?;
        if let Performed::Delivered { delivery, barrier } = performed {
            self.consume(delivery, barrier, collector, books)?;
        }
        Ok(())
    }

    /// `FireDeadline`, once the box is empty: the armed deadline due
    /// first is delivered at its due time. `None` when none is armed.
    pub fn fire_deadline(
        &mut self,
        collector: &mut Collector,
        books: &mut Books,
    ) -> Result<Option<()>, Stop> {
        let Some(index) = self
            .timer
            .armed
            .iter()
            .enumerate()
            .min_by_key(|(_, armed)| (armed.due, armed.order))
            .map(|(index, _)| index)
        else {
            return Ok(None);
        };
        let armed = self.timer.armed.remove(index);
        self.timer.now = self.timer.now.max(armed.due);
        let now = self.timer.now;
        let pending = Pending {
            kind: "DeadlineDelivered",
            payload: json!({"token": decimal(armed.token.get()), "now": decimal(now)}),
            parents: vec![armed.cause],
        };
        let input = Input::Deadline {
            token: armed.token,
            now: LogicalTime::new(now),
        };
        self.input(input, Some(pending), collector, books)?;
        Ok(Some(()))
    }

    /// The durable state, read from the store without the broker.
    #[must_use]
    pub fn durable(&self) -> crate::store::durable::DurableState {
        self.store.store().snapshot()
    }

    /// Crashes the process. What it recorded is written; the durable
    /// state is reopened from what survived, and nothing else.
    pub fn crash(
        self,
        collector: &mut Collector,
        books: &mut Books,
    ) -> Result<(MemoryStore, Vec<String>), Stop> {
        let Self {
            mut store,
            mut writer,
            transport,
            ..
        } = self;
        flush(&mut store, &mut writer, collector, books)?;
        let open: Vec<String> = transport
            .connections
            .into_iter()
            .filter(|connection| connection.open)
            .map(|connection| connection.client)
            .collect();
        let crashed = store.crash();
        Ok((MemoryStore::reopen(crashed.store.snapshot()), open))
    }
}
