//! The runner: plays the steps of a scenario through the process under
//! test, outside it.
//!
//! The runner is the supervisor and the peers of ADR 0002. It never
//! answers in place of a component: a packet reaches the core only as
//! bytes a peer sent through the transport, a result only through the
//! dispatcher of the store adapter, a deadline only as a token the timer
//! armed. Steps are macros; the runner expands them into elementary
//! choices of the scheduler, each with a stable identity, logged in
//! order and counted against `max_steps`.
//!
//! The scheduler is deterministic: version 1 draws each choice among
//! the eligible ones, in their fixed order, with SplitMix64 seeded by
//! the seed of the run. The same seed gives the same choices, the same
//! trace and the same snapshots.
//!
//! The runner computes no verdict. When a step cannot be played (an
//! expectation of the scenario that did not hold, a budget exhausted, a
//! component that stopped), the run stops there and its trace is
//! declared incomplete; the oracle concludes nothing from it.

use std::collections::BTreeMap;

use serde_json::{Value, json};

use mqttiumd_contract::ids::{ConnectionGeneration, InboundPacketId, SessionId};

use super::backend::Backend;
use super::collector::{Cause, Collector, Origin};
use super::mqtt::{Kind, peer};
use super::process::{Books, Choice, Process, Settings, Stop, Written};
use super::scenario::{Event, Fault, Scenario, Step};
use crate::store::adapter::{FaultSpec, WorkKind};
use crate::store::evidence::{SnapshotContext, snapshot};

/// The version of the scheduler. A change of its choices is a new one.
pub const SCHEDULER_VERSION: u64 = 1;

/// SplitMix64: a small generator whose sequence is fixed by its seed.
#[derive(Clone, Copy, Debug)]
pub struct Generator(u64);

impl Generator {
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn draw(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// An index below `count`, which is not zero.
    pub fn pick(&mut self, count: usize) -> usize {
        (self.draw() % count as u64) as usize
    }
}

/// A simulated client of the fixture.
#[derive(Debug)]
struct Client {
    client_id: Vec<u8>,
    session: SessionId,
    connection: Option<ConnectionGeneration>,
    /// Packets observed and not yet awaited by a step, in order.
    observed: Vec<(peer::Observed, Cause)>,
}

/// A PUBLISH a client sent under a binding, to resume it later.
#[derive(Clone, Debug)]
struct Exchange {
    client: String,
    topic: Vec<u8>,
    qos: u8,
    retain: bool,
    packet_id: Option<u16>,
    payload: Vec<u8>,
}

/// What a run leaves for the campaign.
#[derive(Debug)]
pub struct Outcome {
    pub trace: Vec<u8>,
    /// The snapshots written, by path relative to the campaign, with
    /// their bytes.
    pub snapshots: Vec<(String, Vec<u8>)>,
    /// One line per elementary choice.
    pub choices: Vec<String>,
    /// Why the run stopped before its last step, if it did.
    pub stopped: Option<String>,
    /// Hits of each fault of the scenario, in its order.
    pub hits: Vec<u64>,
    /// Faults the scenario declares and the run did not reach as often
    /// as it expects.
    pub not_reached: Vec<String>,
    /// Records the collector refused.
    pub refused: Vec<String>,
    /// Events the formats have no record for, counted by kind.
    pub unwritten: BTreeMap<String, u64>,
    /// Steps played.
    pub steps_played: usize,
    /// Elementary choices made.
    pub elementary: u64,
}

/// The fixture as the runner needs it: clients and their sessions.
#[derive(Clone, Debug)]
pub struct Population {
    /// Client name, ClientID and the session the store created.
    pub clients: Vec<(String, Vec<u8>, SessionId)>,
}

/// Everything a run needs.
pub struct Plan<'a, B: Backend> {
    pub scenario: &'a Scenario,
    /// The durable store the campaign runs on.
    pub backend: B,
    pub settings: Settings,
    pub population: Population,
    /// The data set, installed from the fixture, no epoch open.
    pub store: B::Store,
    pub collector: Collector,
}

struct Runner<'a, B: Backend> {
    scenario: &'a Scenario,
    backend: B,
    run_id: &'static str,
    settings: Settings,
    collector: Collector,
    books: Books,
    process: Option<Process<B::Store>>,
    /// The durable state while no process runs.
    dormant: Option<B::Store>,
    restarts: u64,
    generator: Generator,
    clients: BTreeMap<String, Client>,
    exchanges: BTreeMap<String, Exchange>,
    next_connection: u64,
    choices: Vec<String>,
    elementary: u64,
    snapshots: Vec<(String, Vec<u8>)>,
    /// Hits of each fault already awaited by a `DriveUntil`.
    awaited: Vec<u64>,
    /// The barriers a `LeaveBarrier` step left, by scenario fault.
    left_barriers: std::collections::BTreeSet<usize>,
    step_index: usize,
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// The selection of a fault, in the terms of the store adapter.
///
/// The bench knows the exchange a binding names (the client's session
/// and Packet Identifier), never an operation (ADR 0003). A resolution
/// carries no source in the adapter: at the fence point, and for the
/// result of a `ResolveAndFence`, the fault selects by kind only. Each
/// scenario of S0 has one exchange, so that selection is the same set.
fn specification(
    fault: &Fault,
    exchanges: &BTreeMap<String, (SessionId, u16)>,
) -> Result<FaultSpec, Stop> {
    let source = match &fault.publication_binding {
        Some(binding) => {
            let (session, raw) = exchanges.get(binding).copied().ok_or_else(|| {
                Stop::new(format!(
                    "{}: the binding {binding} names no exchange",
                    fault.id
                ))
            })?;
            let packet_id = InboundPacketId::new(raw)
                .ok_or_else(|| Stop::new(format!("{}: Packet Identifier 0", fault.id)))?;
            Some((session, packet_id))
        }
        None => None,
    };
    use crate::store::adapter::FaultPoint;
    let resolution = fault.point == FaultPoint::FenceTransit
        || fault.result_of.as_deref() == Some("ResolveAndFence");
    let (kind, source) = if resolution {
        (Some(WorkKind::ResolveAndFence), None)
    } else {
        (fault.command_kind, source)
    };
    Ok(FaultSpec {
        point: fault.point,
        action: fault.action,
        kind,
        source,
        occurrence: fault.occurrence,
    })
}

/// The exchanges the scenario binds, by the step that binds them: the
/// session of the client and the Packet Identifier.
fn bound_exchanges(
    scenario: &Scenario,
    population: &Population,
) -> Result<BTreeMap<String, (SessionId, u16)>, Stop> {
    let mut bound = BTreeMap::new();
    for step in &scenario.steps {
        if let Step::ClientPublish {
            client,
            packet_id: Some(packet_id),
            bind_exchange_as: Some(binding),
            ..
        } = step
        {
            let session = population
                .clients
                .iter()
                .find(|(name, _, _)| name == client)
                .map(|(_, _, session)| *session)
                .ok_or_else(|| Stop::new(format!("no client {client} in the fixture")))?;
            bound.insert(binding.clone(), (session, *packet_id));
        }
    }
    Ok(bound)
}

impl<B: Backend> Runner<'_, B> {
    fn process(&mut self) -> Result<&mut Process<B::Store>, Stop> {
        self.process
            .as_mut()
            .ok_or_else(|| Stop::new("the broker is not running"))
    }

    fn emit(
        &mut self,
        channel: Origin,
        kind: &str,
        parents: &[Cause],
        payload: Value,
    ) -> Result<Cause, Stop> {
        self.collector
            .emit(channel, kind, parents, payload)
            .map_err(|refusal| Stop::new(format!("the collector refused a record: {refusal}")))
    }

    /// The faults to arm at a start-up: those not reached yet.
    fn faults(&self) -> Result<Vec<(usize, FaultSpec, &'static str)>, Stop> {
        let population = Population {
            clients: self
                .clients
                .iter()
                .map(|(name, client)| (name.clone(), client.client_id.clone(), client.session))
                .collect(),
        };
        let bound = bound_exchanges(self.scenario, &population)?;
        let mut armed = Vec::new();
        for (index, fault) in self.scenario.faults.iter().enumerate() {
            if self.books.hits.get(index).copied().unwrap_or(0) > 0 {
                continue;
            }
            let name: &'static str = Box::leak(fault.id.clone().into_boxed_str());
            armed.push((index, specification(fault, &bound)?, name));
        }
        Ok(armed)
    }

    fn start(&mut self) -> Result<(), Stop> {
        let store = self
            .dormant
            .take()
            .ok_or_else(|| Stop::new("no data set to start on"))?;
        let faults = self.faults()?;
        let mut startup = Vec::new();
        let started = Process::start(
            store,
            self.restarts,
            self.settings,
            &faults,
            &mut self.collector,
            &mut self.books,
            &mut startup,
        );
        // The choices of a start-up are elementary choices like others.
        for line in startup {
            self.elementary = self.elementary.saturating_add(1);
            self.choices.push(format!(
                "{} start:{} {line}",
                self.elementary, self.restarts
            ));
        }
        let mut process = started?;
        if self.restarts > 0 {
            let epoch = process.epoch().get();
            self.emit(
                Origin::BenchSupervisor,
                "BrokerRecovered",
                &[],
                json!({"run_epoch": epoch.to_string()}),
            )?;
        }
        process.recover(&mut self.collector, &mut self.books)?;
        self.process = Some(process);
        Ok(())
    }

    /// One elementary choice, drawn among the eligible ones. Returns
    /// false when nothing is eligible.
    fn choose(&mut self, budget: &mut u64) -> Result<bool, Stop> {
        let choices = self.process()?.choices();
        if choices.is_empty() {
            return Ok(false);
        }
        if *budget == 0 {
            return Err(Stop::new("the step exhausted its max_steps"));
        }
        if self.elementary >= self.scenario.max_steps {
            return Err(Stop::new("the run exhausted the max_steps of the scenario"));
        }
        *budget -= 1;
        self.elementary += 1;
        let choice = choices[self.generator.pick(choices.len())].clone();
        self.choices.push(format!(
            "{} step:{} {}",
            self.elementary,
            self.step_index,
            choice.describe()
        ));
        self.apply(&choice)?;
        Ok(true)
    }

    fn apply(&mut self, choice: &Choice) -> Result<(), Stop> {
        let process = self
            .process
            .as_mut()
            .ok_or_else(|| Stop::new("the broker is not running"))?;
        let written = process.perform(choice, &mut self.collector, &mut self.books)?;
        process.settle(&mut self.collector, &mut self.books)?;
        if let Some(written) = written {
            self.observe(written)?;
        }
        Ok(())
    }

    /// A peer decodes the bytes the transport wrote to it.
    fn observe(&mut self, written: Written) -> Result<(), Stop> {
        let observed = peer::observe(&written.bytes)
            .map_err(|why| Stop::new(format!("{} cannot decode: {why}", written.client)))?;
        let mut payload = json!({
            "client": written.client,
            "packet_kind": observed.kind.name(),
            "reason_code": observed.reason_code,
            "bytes_hex": hex(&written.bytes),
        });
        if let Some(packet_id) = observed.packet_id {
            payload["packet_id"] = json!(packet_id);
        }
        if let Some(present) = observed.session_present {
            payload["session_present"] = json!(present);
        }
        let cause = self.emit(
            Origin::Peer,
            "PeerPacketObserved",
            &[written.cause],
            payload,
        )?;
        if let Some(client) = self.clients.get_mut(&written.client) {
            client.observed.push((observed, cause));
        }
        Ok(())
    }

    /// A peer sends one packet on its connection.
    fn send(
        &mut self,
        client: &str,
        kind: Kind,
        packet_id: Option<u16>,
        dup: Option<bool>,
        bytes: Vec<u8>,
    ) -> Result<(), Stop> {
        let connection = self
            .clients
            .get(client)
            .and_then(|client| client.connection)
            .ok_or_else(|| Stop::new(format!("{client} has no connection")))?;
        let mut payload = json!({
            "client": client,
            "packet_kind": kind.name(),
            "bytes_hex": hex(&bytes),
        });
        if let Some(packet_id) = packet_id {
            payload["packet_id"] = json!(packet_id);
        }
        if let Some(dup) = dup {
            payload["dup"] = json!(dup);
        }
        let sent = self.emit(Origin::Peer, "PeerPacketSent", &[], payload)?;
        let process = self
            .process
            .as_mut()
            .ok_or_else(|| Stop::new("the broker is not running"))?;
        process.receive(connection, &bytes, sent, &mut self.collector)
    }

    /// Drives until `done` holds, within `max_steps` choices.
    fn drive_until(
        &mut self,
        max_steps: u64,
        mut done: impl FnMut(&mut Self) -> bool,
    ) -> Result<(), Stop> {
        let mut budget = max_steps;
        loop {
            if done(self) {
                return Ok(());
            }
            if !self.choose(&mut budget)? {
                return Err(Stop::new(
                    "nothing is eligible and the awaited event did not happen",
                ));
            }
        }
    }

    /// Takes the first observation of `client` that matches.
    fn take_observed(
        &mut self,
        client: &str,
        kind: Kind,
        packet_id: Option<u16>,
    ) -> Option<peer::Observed> {
        let client = self.clients.get_mut(client)?;
        let index = client.observed.iter().position(|(observed, _)| {
            observed.kind == kind && (packet_id.is_none() || observed.packet_id == packet_id)
        })?;
        Some(client.observed.remove(index).0)
    }

    fn fault_index(&self, name: &str) -> Result<usize, Stop> {
        self.scenario
            .faults
            .iter()
            .position(|fault| fault.id == name)
            .ok_or_else(|| Stop::new(format!("no fault {name}")))
    }

    fn connect(&mut self, name: &str) -> Result<(), Stop> {
        self.next_connection = self
            .next_connection
            .checked_add(1)
            .ok_or_else(|| Stop::new("connection generations exhausted"))?;
        let generation = ConnectionGeneration::new(self.next_connection);
        let client = self
            .clients
            .get_mut(name)
            .ok_or_else(|| Stop::new(format!("no client {name} in the fixture")))?;
        client.connection = Some(generation);
        client.observed.clear();
        let client_id = client.client_id.clone();
        self.emit(
            Origin::Peer,
            "PeerConnectionOpened",
            &[],
            json!({"client": name}),
        )?;
        self.process()?.connect(name, generation);
        let bytes = peer::connect(&client_id).map_err(Stop::new)?;
        self.send(name, Kind::Connect, None, None, bytes)
    }

    fn play(&mut self, step: &Step) -> Result<(), Stop> {
        match step {
            Step::ClientResumeSession {
                client,
                expect_session_present,
                max_steps,
            } => {
                self.connect(client)?;
                let name = client.clone();
                let mut connack = None;
                self.drive_until(*max_steps, |runner| {
                    connack = runner.take_observed(&name, Kind::Connack, None);
                    connack.is_some()
                })?;
                let connack = connack.ok_or_else(|| Stop::new("no CONNACK"))?;
                if connack.reason_code != 0
                    || connack.session_present != Some(*expect_session_present)
                {
                    return Err(Stop::new(format!(
                        "{client} expected session present {expect_session_present}, observed {connack:?}"
                    )));
                }
                Ok(())
            }
            Step::ClientPublish {
                client,
                topic,
                qos,
                retain,
                packet_id,
                payload,
                bind_exchange_as,
            } => {
                let bytes = peer::publish(topic, *qos, *retain, false, *packet_id, payload)
                    .map_err(Stop::new)?;
                if let Some(binding) = bind_exchange_as {
                    self.exchanges.insert(
                        binding.clone(),
                        Exchange {
                            client: client.clone(),
                            topic: topic.clone(),
                            qos: *qos,
                            retain: *retain,
                            packet_id: *packet_id,
                            payload: payload.clone(),
                        },
                    );
                }
                self.send(client, Kind::Publish, *packet_id, Some(false), bytes)
            }
            Step::ClientResumeExchange { client, exchange } => {
                let sent = self
                    .exchanges
                    .get(exchange)
                    .cloned()
                    .ok_or_else(|| Stop::new(format!("no exchange {exchange}")))?;
                if &sent.client != client {
                    return Err(Stop::new(format!(
                        "{exchange} is not an exchange of {client}"
                    )));
                }
                let bytes = peer::publish(
                    &sent.topic,
                    sent.qos,
                    sent.retain,
                    true,
                    sent.packet_id,
                    &sent.payload,
                )
                .map_err(Stop::new)?;
                self.send(client, Kind::Publish, sent.packet_id, Some(true), bytes)
            }
            Step::ClientSendPubrel { client, packet_id } => {
                let bytes = peer::pubrel(*packet_id).map_err(Stop::new)?;
                self.send(client, Kind::Pubrel, Some(*packet_id), None, bytes)
            }
            Step::DriveUntil { event, max_steps } => match event {
                Event::FaultHit { fault } => {
                    let index = self.fault_index(fault)?;
                    let wanted = self.awaited.get(index).copied().unwrap_or(0) + 1;
                    self.drive_until(*max_steps, |runner| {
                        runner.books.hits.get(index).copied().unwrap_or(0) >= wanted
                    })?;
                    if let Some(awaited) = self.awaited.get_mut(index) {
                        *awaited = wanted;
                    }
                    Ok(())
                }
                Event::PeerPacketObserved {
                    client,
                    packet_kind,
                    packet_id,
                } => {
                    let kind = Kind::from_name(packet_kind)
                        .ok_or_else(|| Stop::new(format!("no packet kind {packet_kind}")))?;
                    let name = client.clone();
                    let packet_id = *packet_id;
                    self.drive_until(*max_steps, |runner| {
                        runner.take_observed(&name, kind, packet_id).is_some()
                    })
                }
            },
            Step::FireDeadline { max_steps } => {
                let mut budget = *max_steps;
                while self.process()?.busy() {
                    if !self.choose(&mut budget)? {
                        return Err(Stop::new("an effect stays in the box"));
                    }
                }
                let process = self
                    .process
                    .as_mut()
                    .ok_or_else(|| Stop::new("the broker is not running"))?;
                if process
                    .fire_deadline(&mut self.collector, &mut self.books)?
                    .is_none()
                {
                    return Err(Stop::new("FireDeadline with no deadline armed"));
                }
                // The core consumed it, or parked it for room.
                while self.process()?.busy() {
                    let process = self
                        .process
                        .as_mut()
                        .ok_or_else(|| Stop::new("the broker is not running"))?;
                    process.settle(&mut self.collector, &mut self.books)?;
                    if self.process()?.busy() && !self.choose(&mut budget)? {
                        return Err(Stop::new("the deadline was not consumed"));
                    }
                }
                Ok(())
            }
            Step::ReleaseHeldCommand { fault } => {
                let index = self.fault_index(fault)?;
                let id = self
                    .books
                    .held
                    .get(&index)
                    .copied()
                    .ok_or_else(|| Stop::new(format!("{fault} holds no command")))?;
                let process = self
                    .process
                    .as_mut()
                    .ok_or_else(|| Stop::new("the broker is not running"))?;
                process.release_command(id, &mut self.collector, &mut self.books)
            }
            Step::DeliverHeldResult { fault, max_steps } => {
                let index = self.fault_index(fault)?;
                let id = self
                    .books
                    .held
                    .get(&index)
                    .copied()
                    .ok_or_else(|| Stop::new(format!("{fault} holds no result")))?;
                let process = self
                    .process
                    .as_mut()
                    .ok_or_else(|| Stop::new("the broker is not running"))?;
                process.deliver_held_result(id, &mut self.collector, &mut self.books)?;
                let mut budget = *max_steps;
                while self.process()?.busy() {
                    if !self.choose(&mut budget)? {
                        return Err(Stop::new("the held result was not consumed"));
                    }
                }
                Ok(())
            }
            Step::LeaveBarrier { fault } => {
                let index = self.fault_index(fault)?;
                let declared = &self.scenario.faults[index];
                if declared.action != crate::store::adapter::FaultAction::Barrier {
                    return Err(Stop::new(format!("{fault} is not a barrier")));
                }
                if self.books.hits.get(index).copied().unwrap_or(0) == 0 {
                    return Err(Stop::new(format!("{fault} was not reached")));
                }
                if !self.left_barriers.insert(index) {
                    return Err(Stop::new(format!("{fault} was already left")));
                }
                let process = self
                    .process
                    .as_mut()
                    .ok_or_else(|| Stop::new("the broker is not running"))?;
                if !process.halted() || self.books.barrier != Some(index) {
                    return Err(Stop::new(format!(
                        "the store adapter is not stopped at {fault}"
                    )));
                }
                process.leave_barrier(&mut self.collector, &mut self.books)
            }
            Step::InspectDurableState { checkpoint } => {
                let durable = match (&self.process, &self.dormant) {
                    (Some(process), _) => self.backend.inspect(process.store()),
                    (None, Some(store)) => self.backend.inspect(store),
                    (None, None) => return Err(Stop::new("no data set to inspect")),
                }
                .map_err(Stop::new)?;
                let document = snapshot(
                    &durable,
                    SnapshotContext {
                        run_id: self.run_id,
                        checkpoint,
                        backend: self.backend.name(),
                    },
                );
                let mut bytes = serde_json::to_vec_pretty(&document)
                    .map_err(|error| Stop::new(error.to_string()))?;
                bytes.push(b'\n');
                let path = format!("snapshots/{checkpoint}.snapshot.json");
                let digest = crate::manifest::sha256_hex(&bytes);
                self.emit(
                    Origin::BenchSupervisor,
                    "DurableStateInspected",
                    &[],
                    json!({
                        "checkpoint": checkpoint,
                        "snapshot": {
                            "path": path,
                            "sha256": digest,
                            "bytes": bytes.len().to_string(),
                        },
                    }),
                )?;
                self.snapshots.push((path, bytes));
                Ok(())
            }
            Step::CrashBroker => {
                let process = self
                    .process
                    .take()
                    .ok_or_else(|| Stop::new("the broker is not running"))?;
                let (store, open) = process.crash(&mut self.collector, &mut self.books)?;
                self.books.held.clear();
                self.emit(
                    Origin::BenchSupervisor,
                    "BrokerCrashed",
                    &[],
                    json!({"failure_model": "process"}),
                )?;
                for name in open {
                    self.emit(
                        Origin::Peer,
                        "PeerConnectionClosed",
                        &[],
                        json!({"client": name}),
                    )?;
                    if let Some(client) = self.clients.get_mut(&name) {
                        client.connection = None;
                    }
                }
                self.dormant = Some(self.backend.reopen_after_crash(store).map_err(Stop::new)?);
                Ok(())
            }
            Step::RestartBroker => {
                if self.process.is_some() {
                    return Err(Stop::new("the broker is already running"));
                }
                self.restarts += 1;
                self.collector.restart_process();
                self.start()
            }
        }
    }
}

/// Plays a scenario. The trace, the snapshots and the choice log come
/// back whatever happens; a run that stopped says why.
pub fn run<B: Backend>(plan: Plan<'_, B>) -> Outcome {
    let Plan {
        scenario,
        backend,
        settings,
        population,
        store,
        mut collector,
    } = plan;
    let header = json!({
        "scenario_id": scenario.id,
        "backend": backend.name(),
        "perimeter": "full",
    });
    let header = collector.emit(Origin::BenchSupervisor, "TraceHeader", &[], header);
    let clients = population
        .clients
        .iter()
        .map(|(name, client_id, session)| {
            (
                name.clone(),
                Client {
                    client_id: client_id.clone(),
                    session: *session,
                    connection: None,
                    observed: Vec::new(),
                },
            )
        })
        .collect();
    let mut runner = Runner {
        scenario,
        backend,
        run_id: settings.run_id,
        settings,
        collector,
        books: Books::new(scenario.faults.len()),
        process: None,
        dormant: Some(store),
        restarts: 0,
        generator: Generator::new(settings.seed),
        clients,
        exchanges: BTreeMap::new(),
        next_connection: 0,
        choices: Vec::new(),
        elementary: 0,
        snapshots: Vec::new(),
        awaited: vec![0; scenario.faults.len()],
        left_barriers: std::collections::BTreeSet::new(),
        step_index: 0,
    };
    let mut stopped = header
        .err()
        .map(|refusal| format!("the header was refused: {refusal}"));
    if stopped.is_none()
        && let Err(Stop(reason)) = runner.start()
    {
        stopped = Some(format!("start-up: {reason}"));
    }
    let mut played = 0;
    if stopped.is_none() {
        for (index, step) in scenario.steps.iter().enumerate() {
            runner.step_index = index;
            if let Err(Stop(reason)) = runner.play(step) {
                stopped = Some(format!("step {index} ({}): {reason}", step.kind()));
                break;
            }
            played += 1;
        }
    }
    let not_reached: Vec<String> = scenario
        .faults
        .iter()
        .zip(&runner.books.hits)
        .filter(|(fault, hits)| **hits != fault.expected_hits)
        .map(|(fault, hits)| {
            format!(
                "{}: {hits} hits, {} expected",
                fault.id, fault.expected_hits
            )
        })
        .collect();
    let Runner {
        collector,
        books,
        choices,
        snapshots,
        elementary,
        ..
    } = runner;
    let (trace, refusals) = collector.finish(stopped.is_none());
    Outcome {
        trace,
        snapshots,
        choices,
        stopped,
        hits: books.hits,
        not_reached,
        refused: refusals.iter().map(ToString::to_string).collect(),
        unwritten: books.unwritten,
        steps_played: played,
        elementary,
    }
}
