//! The process under test in a child process (plan
//! `docs/missions/L4-process-topology.md`).
//!
//! The supervisor starts the same binary with the subcommand `child`. The
//! child runs the real path (the core, the store adapter with its
//! injection layer, the SQLite store, the timer and the transport) and
//! takes one action at a time on its standard input. Its evidence leaves
//! it on one Unix socket per origin, in a directory only the supervisor's
//! user can enter; the supervisor reads each socket on a reader of its
//! own, so the origin of a record is the socket it arrived on, never what
//! the record says of itself. Reports go on the child's standard output.
//!
//! The child relays each record its local collector wrote; the collector
//! of the supervisor checks it against the channel and numbers it in the
//! trace. At the end of each action the child waits until every frame is
//! acknowledged before it reads the next action: between two actions it is
//! drained.
//!
//! Every read of the supervisor has a deadline: a child that writes
//! nothing for [`SILENCE`] stops the run, never an endless wait.

use std::collections::BTreeSet;
use std::io::{BufReader, BufWriter, Read};
use std::os::unix::fs::DirBuilderExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{ChildStdin, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

use mqttiumd_contract::ids::{ConnectionGeneration, SessionId};

use super::backend::sqlite_settings;
use super::campaign::settings_of;
use super::collector::{Cause, Collector, KindTable, Origin};
use super::process::{Books, Choice, Process, Stop, Written};
use super::protocol::{
    Action, Evidence, Report, Sequencer, Window, choices_digest, read_frame, resolve_choice,
    write_frame,
};
use super::runner::{Population, bound_exchanges, specification};
use super::scenario::Scenario;

/// How long the supervisor waits for one frame of a child before it stops
/// the run.
pub const SILENCE: Duration = Duration::from_secs(30);

/// The origins of the child, in the order of their sockets.
pub const CHILD_ORIGINS: [Origin; 7] = [
    Origin::BrokerCore,
    Origin::Dispatcher,
    Origin::FaultInjector,
    Origin::StoreAdapter,
    Origin::StoreEvidence,
    Origin::Timer,
    Origin::Transport,
];

/// One bounded receive queue shared by the seven origins and control.
pub const ARRIVAL_CAPACITY: usize = (CHILD_ORIGINS.len() + 1) * super::protocol::WINDOW as usize;
/// Reports buffered while one action is outstanding.
pub const REPORTS_PER_ACTION: usize = super::protocol::WINDOW as usize;

/// What a child needs to know, the same at every start-up of a run.
#[derive(Clone, Debug)]
pub struct Config {
    /// The binary to start: the binary of the supervisor.
    pub executable: PathBuf,
    /// The campaign directory, which holds the contracts, the scenario
    /// and the fixture the run reads.
    pub campaign: PathBuf,
    pub contracts: String,
    pub scenario: String,
    pub seed: u64,
    pub run_id: String,
    /// The SQLite file of the data set.
    pub store: PathBuf,
    /// The clients and the sessions the fixture installed.
    pub sessions: Vec<(String, SessionId)>,
    /// How long the supervisor waits for a frame of the child before it
    /// stops the run ([`SILENCE`] for a campaign).
    pub silence: Duration,
}

/// What the supervisor knows of the child after an action.
#[derive(Clone, Debug, Default)]
pub struct State {
    pub busy: bool,
    pub halted: bool,
    pub armed: usize,
    pub barrier: Option<usize>,
    /// Hits of each scenario fault in this incarnation.
    pub hits: Vec<u64>,
    pub held: BTreeSet<usize>,
}

enum Arrival {
    Evidence(Origin, Evidence),
    Report(Report),
    Broken(String),
}

/// What the supervisor observed of a kill.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Killed {
    /// The process identifier `spawn` gave.
    pub pid: u32,
    /// The rank of the start-up of the child.
    pub rank: u64,
    /// The signal sent: `SIGKILL`, through `std::process::Child::kill`.
    pub signal_sent: &'static str,
    /// The signal the wait status reports: 9.
    pub wait_signal: i32,
    /// The in-flight point and settings actually reported there, if any.
    pub transaction: Option<Value>,
}

/// The private directory of the sockets of one child, removed when it is
/// dropped, whether the child started or not.
struct SocketDirectory(PathBuf);

impl Drop for SocketDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// The supervisor's side of one child process.
pub struct Child {
    process: std::process::Child,
    pid: u32,
    /// The standard input of the child; `None` once closed.
    actions: Option<BufWriter<ChildStdin>>,
    received: mpsc::Receiver<Arrival>,
    sequencer: Sequencer<Origin>,
    eligible: Vec<String>,
    state: State,
    /// Hits of each scenario fault before this child started.
    base_hits: Vec<u64>,
    epoch: u64,
    rank: u64,
    settings: Value,
    /// Kept for the life of the child, removed with it.
    _sockets: SocketDirectory,
    silence: Duration,
    /// Whether the last action ended: every frame read, acknowledged,
    /// ordered and kept in the trace.
    ended: bool,
    /// A command still owns its SQLite stack, after the retained hit's ack.
    parked: Option<Value>,
}

fn stop(reason: impl Into<String>) -> Stop {
    Stop::new(reason)
}

/// Accepts the one connection of `listener` before `deadline`.
fn accept(listener: &UnixListener, deadline: Instant) -> Result<UnixStream, Stop> {
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                stream
                    .set_nonblocking(false)
                    .map_err(|error| stop(format!("a socket of the child: {error}")))?;
                return Ok(stream);
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if Instant::now() >= deadline {
                    return Err(stop("the child did not connect its evidence sockets"));
                }
                thread::sleep(Duration::from_millis(2));
            }
            Err(error) => return Err(stop(format!("a socket of the child: {error}"))),
        }
    }
}

/// One reader per descriptor: a full descriptor never waits for another.
fn reader(
    stream: impl Read + Send + 'static,
    origin: Option<Origin>,
    to: mpsc::SyncSender<Arrival>,
) {
    thread::spawn(move || {
        let mut stream = BufReader::new(stream);
        loop {
            let read = match read_frame(&mut stream) {
                Ok(None) => return,
                Ok(Some(body)) => match origin {
                    Some(origin) => match Evidence::decode(&body) {
                        Ok(evidence) => Arrival::Evidence(origin, evidence),
                        Err(refusal) => Arrival::Broken(format!("{}: {refusal}", origin.name())),
                    },
                    None => match Report::decode(&body) {
                        Ok(report) => Arrival::Report(report),
                        Err(refusal) => Arrival::Broken(format!("a report: {refusal}")),
                    },
                },
                Err(refusal) => Arrival::Broken(format!(
                    "{}: {refusal}",
                    origin.map_or("the control descriptor", Origin::name)
                )),
            };
            let broken = matches!(read, Arrival::Broken(_));
            if to.send(read).is_err() || broken {
                return;
            }
        }
    });
}

impl Child {
    /// Starts a child of rank `rank` with the scenario faults `armed`, and
    /// has it start the process by the real path.
    pub fn spawn(
        config: &Config,
        rank: u64,
        armed: &[usize],
        collector: &mut Collector,
        books: &mut Books,
    ) -> Result<(Self, Vec<String>), Stop> {
        let sockets = std::env::temp_dir().join(format!(
            "mqttiumd-child-{}-{rank}-{}",
            std::process::id(),
            collector.count()
        ));
        let _ = std::fs::remove_dir_all(&sockets);
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&sockets)
            .map_err(|error| stop(format!("{}: {error}", sockets.display())))?;
        let sockets = SocketDirectory(sockets);
        let mut listeners = Vec::new();
        for origin in CHILD_ORIGINS {
            let path = sockets.0.join(format!("{}.sock", origin.name()));
            let listener = UnixListener::bind(&path)
                .map_err(|error| stop(format!("{}: {error}", path.display())))?;
            listener
                .set_nonblocking(true)
                .map_err(|error| stop(format!("{}: {error}", path.display())))?;
            listeners.push((origin, listener));
        }
        let sessions: Vec<String> = config
            .sessions
            .iter()
            .map(|(name, session)| format!("{name}={}", session.get()))
            .collect();
        let armed_list: Vec<String> = armed.iter().map(ToString::to_string).collect();
        let mut process = Command::new(&config.executable)
            .arg("child")
            .arg("--root")
            .arg(&config.campaign)
            .args(["--contracts", &config.contracts])
            .args(["--scenario", &config.scenario])
            .args(["--seed", &config.seed.to_string()])
            .args(["--run-id", &config.run_id])
            .args(["--rank", &rank.to_string()])
            .arg("--store")
            .arg(&config.store)
            .args(["--sessions", &sessions.join(",")])
            .args(["--armed", &armed_list.join(",")])
            .arg("--sockets")
            .arg(&sockets.0)
            .env_remove("CARGO_TARGET_DIR")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|error| stop(format!("{}: {error}", config.executable.display())))?;
        let pid = process.id();
        let actions = process.stdin.take().ok_or_else(|| stop("no stdin"))?;
        let reports = process.stdout.take().ok_or_else(|| stop("no stdout"))?;
        let (sender, received) = mpsc::sync_channel(ARRIVAL_CAPACITY);
        reader(reports, None, sender.clone());
        // From here every way out goes through `Drop for Child`, which makes
        // sure the process does not outlive the run.
        let mut child = Self {
            process,
            pid,
            actions: Some(BufWriter::new(actions)),
            received,
            sequencer: Sequencer::new(),
            eligible: Vec::new(),
            state: State::default(),
            base_hits: books.hits.clone(),
            epoch: 0,
            rank,
            settings: Value::Null,
            _sockets: sockets,
            silence: config.silence,
            ended: true,
            parked: None,
        };
        let deadline = Instant::now() + config.silence;
        for (origin, listener) in &listeners {
            let stream = accept(listener, deadline)?;
            reader(stream, Some(*origin), sender.clone());
        }
        drop(sender);
        drop(listeners);
        let rank = u32::try_from(rank).map_err(|_| stop("too many start-ups"))?;
        let mut startup = Vec::new();
        for report in child.act(&Action::Start { rank }, collector, books)? {
            if let Report::Started {
                epoch,
                startup: lines,
                settings,
            } = report
            {
                child.epoch = epoch;
                startup = lines;
                child.settings = serde_json::from_str(&settings)
                    .map_err(|error| stop(format!("the settings of the child: {error}")))?;
            }
        }
        if child.epoch == 0 {
            return Err(stop("the child reported no start-up"));
        }
        Ok((child, startup))
    }

    /// The process identifier the operating system gave the child.
    #[must_use]
    pub const fn pid(&self) -> u32 {
        self.pid
    }

    /// The settings the store engine of the child read back.
    #[must_use]
    pub const fn settings(&self) -> &Value {
        &self.settings
    }

    #[must_use]
    pub const fn state(&self) -> &State {
        &self.state
    }

    #[must_use]
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    /// The descriptions of the choices the child published last.
    #[must_use]
    pub fn choices(&self) -> &[String] {
        &self.eligible
    }

    /// One action: written, then every frame of every descriptor read and
    /// acknowledged until the child ends the action; the evidence ordered
    /// and handed to the collector; the state mirrored into `books`. The
    /// other reports come back.
    fn act(
        &mut self,
        action: &Action,
        collector: &mut Collector,
        books: &mut Books,
    ) -> Result<Vec<Report>, Stop> {
        if self.parked.is_some() {
            return Err(stop(
                "a transaction is parked in flight: only SIGKILL may follow",
            ));
        }
        let actions = self
            .actions
            .as_mut()
            .ok_or_else(|| stop("the standard input of the child is closed"))?;
        self.ended = false;
        write_frame(actions, &action.encode())
            .map_err(|refusal| stop(format!("the child does not read: {refusal}")))?;
        let mut reports = Vec::new();
        let mut end: Option<Option<u64>> = None;
        let mut acknowledged = self.sequencer.next().checked_sub(1);
        let mut last_fault = None;
        loop {
            // Retain the ordered prefix outside the child BEFORE its ack.
            // In particular FaultHit is durable to the surviving process
            // while the child is still executing the SQLite callback.
            for (origin, line) in self
                .sequencer
                .take_contiguous()
                .map_err(|why| stop(format!("the evidence of the child: {why}")))?
            {
                let record: Value = serde_json::from_slice(&line)
                    .map_err(|why| stop(format!("a record of {}: {why}", origin.name())))?;
                collector
                    .accept(origin, &record)
                    .map_err(|why| stop(format!("the collector refused a record: {why}")))?;
                if record["kind"] == "FaultHit" {
                    last_fault = Some(record["payload"].clone());
                }
            }
            let contiguous = self.sequencer.next().checked_sub(1);
            if contiguous != acknowledged
                && let Some(through) = contiguous
            {
                let actions = self
                    .actions
                    .as_mut()
                    .ok_or_else(|| stop("the standard input of the child is closed"))?;
                write_frame(actions, &Action::Acknowledge { through }.encode())
                    .map_err(|refusal| stop(format!("the child does not read: {refusal}")))?;
                acknowledged = contiguous;
            }
            if let Some(last) = end
                && last.is_none_or(|last| contiguous.is_some_and(|through| through >= last))
            {
                break;
            }
            match self.received.recv_timeout(self.silence) {
                Ok(Arrival::Evidence(origin, evidence)) => {
                    if evidence.emission.saturating_sub(self.sequencer.next())
                        >= super::protocol::WINDOW
                    {
                        return Err(stop("the child emitted beyond the bounded evidence window"));
                    }
                    self.sequencer.receive(origin, evidence);
                }
                Ok(Arrival::Report(Report::EndOfAction { last_emission })) => {
                    end = Some(last_emission);
                }
                Ok(Arrival::Report(Report::Parked {
                    last_emission,
                    fault,
                    point,
                    settings,
                })) => {
                    if Some(last_emission) != acknowledged || !self.sequencer.is_empty() {
                        return Err(stop(
                            "transaction parked before its evidence was retained and acknowledged",
                        ));
                    }
                    let hit = last_fault
                        .as_ref()
                        .ok_or_else(|| stop("transaction parked without a FaultHit"))?;
                    if hit["point"] != point || hit.get("operation_id").is_none() {
                        return Err(stop("transaction park contradicts the retained FaultHit"));
                    }
                    let settings: Value = serde_json::from_str(&settings)
                        .map_err(|why| stop(format!("transaction settings: {why}")))?;
                    if settings["kill_points"] != true || settings["wal_autocheckpoint"] != 0 {
                        return Err(stop("transaction park has incompatible effective settings"));
                    }
                    self.parked = Some(serde_json::json!({
                        "fault": fault, "point": point, "hit": hit, "backend_settings": settings,
                        "evidence_retained_before_ack": true,
                    }));
                    end = Some(Some(last_emission));
                }
                Ok(Arrival::Report(report)) => {
                    if reports.len() >= REPORTS_PER_ACTION {
                        return Err(stop("the child exceeded the bounded action report list"));
                    }
                    reports.push(report);
                }
                Ok(Arrival::Broken(why)) => return Err(stop(format!("the child broke: {why}"))),
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    return Err(stop(format!(
                        "the child {} wrote nothing for {} ms",
                        self.pid,
                        self.silence.as_millis()
                    )));
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(stop(format!(
                        "the child {} closed its descriptors",
                        self.pid
                    )));
                }
            }
        }
        let batch = self
            .sequencer
            .end_action(end.flatten())
            .map_err(|refusal| stop(format!("the evidence of the child: {refusal}")))?;
        for (origin, line) in batch {
            let record: Value = serde_json::from_slice(&line)
                .map_err(|error| stop(format!("a record of {}: {error}", origin.name())))?;
            collector
                .accept(origin, &record)
                .map_err(|refusal| stop(format!("the collector refused a record: {refusal}")))?;
        }
        // Every frame of the action is now in the trace.
        self.ended = self.parked.is_none();
        let mut others = Vec::new();
        let mut failure = None;
        for report in reports {
            match report {
                Report::Eligible { choices, digest } => {
                    if choices_digest(&choices) != digest {
                        return Err(stop("the child published a list with another digest"));
                    }
                    self.eligible = choices;
                }
                Report::State {
                    busy,
                    halted,
                    armed,
                    barrier,
                    hits,
                    held,
                } => {
                    self.state = State {
                        busy,
                        halted,
                        armed: armed as usize,
                        barrier: barrier.map(|fault| fault as usize),
                        hits,
                        held: held.into_iter().map(|fault| fault as usize).collect(),
                    };
                    for (index, total) in books.hits.iter_mut().enumerate() {
                        *total = self.base_hits.get(index).copied().unwrap_or(0)
                            + self.state.hits.get(index).copied().unwrap_or(0);
                    }
                    books.barrier = self.state.barrier;
                }
                Report::Refused { reason } | Report::Stopped { reason } => {
                    failure.get_or_insert(reason);
                }
                other => others.push(other),
            }
        }
        match failure {
            Some(reason) => Err(stop(reason)),
            None => {
                if self.parked.is_some() {
                    self.eligible.clear();
                }
                Ok(others)
            }
        }
    }

    pub fn choose(
        &mut self,
        index: usize,
        collector: &mut Collector,
        books: &mut Books,
    ) -> Result<Option<Written>, Stop> {
        let index = u32::try_from(index).map_err(|_| stop("a choice beyond the list"))?;
        let digest = choices_digest(&self.eligible);
        let mut written = None;
        for report in self.act(&Action::Choose { index, digest }, collector, books)? {
            if let Report::Written {
                client,
                bytes,
                origin,
                incarnation,
                sequence,
            } = report
            {
                let origin = Origin::from_name(&origin)
                    .filter(|origin| CHILD_ORIGINS.contains(origin))
                    .ok_or_else(|| stop(format!("a frame written by {origin}")))?;
                written = Some(Written {
                    client,
                    bytes,
                    cause: Cause {
                        origin,
                        incarnation,
                        sequence,
                    },
                });
            }
        }
        Ok(written)
    }

    pub fn connect(
        &mut self,
        client: &str,
        generation: ConnectionGeneration,
        collector: &mut Collector,
        books: &mut Books,
    ) -> Result<(), Stop> {
        let action = Action::Connect {
            generation: generation.get(),
            client: client.to_owned(),
        };
        self.act(&action, collector, books).map(drop)
    }

    pub fn receive(
        &mut self,
        generation: ConnectionGeneration,
        bytes: &[u8],
        sent: Cause,
        collector: &mut Collector,
        books: &mut Books,
    ) -> Result<(), Stop> {
        if sent.origin != Origin::Peer {
            return Err(stop("bytes not stated by a peer"));
        }
        let action = Action::Bytes {
            generation: generation.get(),
            bytes: bytes.to_vec(),
            sent_incarnation: sent.incarnation,
            sent_sequence: sent.sequence,
        };
        self.act(&action, collector, books).map(drop)
    }

    pub fn settle(&mut self, collector: &mut Collector, books: &mut Books) -> Result<(), Stop> {
        self.act(&Action::Settle, collector, books).map(drop)
    }

    pub fn recover(&mut self, collector: &mut Collector, books: &mut Books) -> Result<(), Stop> {
        self.act(&Action::Recover, collector, books).map(drop)
    }

    pub fn fire_deadline(
        &mut self,
        collector: &mut Collector,
        books: &mut Books,
    ) -> Result<Option<()>, Stop> {
        if self.state.armed == 0 {
            return Ok(None);
        }
        self.act(&Action::FireDeadline, collector, books)
            .map(|_| Some(()))
    }

    fn fault(fault: usize) -> Result<u32, Stop> {
        u32::try_from(fault).map_err(|_| stop("a fault beyond the list"))
    }

    pub fn release_command(
        &mut self,
        fault: usize,
        collector: &mut Collector,
        books: &mut Books,
    ) -> Result<(), Stop> {
        let fault = Self::fault(fault)?;
        self.act(&Action::ReleaseHeld { fault }, collector, books)
            .map(drop)
    }

    pub fn deliver_held_result(
        &mut self,
        fault: usize,
        collector: &mut Collector,
        books: &mut Books,
    ) -> Result<(), Stop> {
        let fault = Self::fault(fault)?;
        self.act(&Action::DeliverHeld { fault }, collector, books)
            .map(drop)
    }

    pub fn leave_barrier(
        &mut self,
        collector: &mut Collector,
        books: &mut Books,
    ) -> Result<(), Stop> {
        let fault = self
            .state
            .barrier
            .ok_or_else(|| stop("the store adapter stands at no barrier"))?;
        let fault = Self::fault(fault)?;
        self.act(&Action::LeaveBarrier { fault }, collector, books)
            .map(drop)
    }

    /// The simulated crash of the store adapter in the child, then a
    /// clean stop: the clients whose connection was open.
    pub fn crash(
        mut self,
        collector: &mut Collector,
        books: &mut Books,
    ) -> Result<Vec<String>, Stop> {
        let mut open = None;
        for report in self.act(&Action::Crash, collector, books)? {
            if let Report::Crashed { open: clients } = report {
                open = Some(clients);
            }
        }
        let open = open.ok_or_else(|| stop("the child reported no crash"))?;
        self.stop(collector, books)?;
        Ok(open)
    }

    /// Asks the child to stop and waits until it exits on its own, with a
    /// status of success.
    pub fn stop(mut self, collector: &mut Collector, books: &mut Books) -> Result<(), Stop> {
        for report in self.act(&Action::Stop, collector, books)? {
            if let Report::Finished { unwritten } = report {
                for (kind, count) in unwritten {
                    *books.unwritten.entry(kind).or_insert(0) += count;
                }
            }
        }
        let status = self.wait(self.silence)?;
        if !status.success() {
            return Err(stop(format!(
                "the child {} stopped with {status}",
                self.pid
            )));
        }
        Ok(())
    }

    /// Kills the child with `SIGKILL` and waits until the operating system
    /// reports its end by that signal.
    ///
    /// A child is killed only between two actions: each action ends with
    /// every frame acknowledged, so the child is drained and stands at the
    /// point its last decisive record describes. Its end is confirmed by
    /// the wait status (signal 9, no exit code) before this returns; a
    /// frame that arrives after the kill can only describe what was not
    /// allowed, and stops the run.
    pub fn kill(mut self) -> Result<Killed, Stop> {
        if self.actions.is_none() {
            return Err(stop("the child is not driven any more"));
        }
        // Drained, checked: the last action ended, nothing waits in the
        // sequencer, and nothing waits unread on a descriptor.
        if (!self.ended && self.parked.is_none()) || !self.sequencer.is_empty() {
            return Err(stop(format!(
                "the child {} is not drained: an action has not ended",
                self.pid
            )));
        }
        if let Ok(arrival) = self.received.try_recv() {
            let what = match arrival {
                Arrival::Evidence(origin, _) => format!("a record of {}", origin.name()),
                Arrival::Report(_) => "a report".to_owned(),
                Arrival::Broken(why) => why,
            };
            return Err(stop(format!(
                "the child {} is not drained: {what} waits unread",
                self.pid
            )));
        }
        self.process
            .kill()
            .map_err(|error| stop(format!("SIGKILL to the child {}: {error}", self.pid)))?;
        let status = self.wait(self.silence)?;
        let killed = Killed {
            pid: self.pid,
            rank: self.rank,
            signal_sent: "SIGKILL",
            wait_signal: status.signal().unwrap_or(0),
            transaction: self.parked.clone(),
        };
        if status.signal() != Some(9) || status.code().is_some() {
            return Err(stop(format!(
                "the child {} ended with {status}, not by SIGKILL",
                self.pid
            )));
        }
        // Its descriptors close with it: what is left is read to the end.
        let mut late = Vec::new();
        loop {
            match self.received.recv_timeout(self.silence) {
                Ok(Arrival::Evidence(origin, _)) => late.push(origin.name()),
                Ok(Arrival::Report(_) | Arrival::Broken(_)) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    return Err(stop(format!(
                        "the descriptors of the killed child {} did not close",
                        self.pid
                    )));
                }
            }
        }
        if !late.is_empty() {
            return Err(stop(format!(
                "{} records arrived after the kill of the child {}: {}",
                late.len(),
                self.pid,
                late.join(", ")
            )));
        }
        Ok(killed)
    }

    /// Waits for the exit of the child before `limit`.
    fn wait(&mut self, limit: Duration) -> Result<ExitStatus, Stop> {
        let deadline = Instant::now() + limit;
        loop {
            match self.process.try_wait() {
                Ok(Some(status)) => return Ok(status),
                Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(2)),
                Ok(None) => {
                    return Err(stop(format!(
                        "the child {} did not exit within {} ms",
                        self.pid,
                        limit.as_millis()
                    )));
                }
                Err(error) => return Err(stop(format!("waiting for the child: {error}"))),
            }
        }
    }
}

impl Drop for Child {
    fn drop(&mut self) {
        // The end of its standard input stops a child that reads an action;
        // its exit is waited for, briefly. One that still lives is killed
        // and waited for, so that no child outlives the run and keeps the
        // writer lock, even a run stopped on an error. That kill is not a
        // kill of the scenario: it enters neither `kills` nor the trace.
        drop(self.actions.take());
        if self.wait(Duration::from_secs(5)).is_err() {
            eprintln!(
                "mqttiumd-lab: the child {} did not exit when its standard input closed; it is killed so that it does not outlive the run",
                self.pid
            );
            let _ = self.process.kill();
            let _ = self.process.wait();
        }
    }
}

// ---------------------------------------------------------------- child

/// The arguments of `mqttiumd-lab child`.
#[derive(Debug, Default)]
pub struct Arguments {
    pub root: PathBuf,
    pub contracts: String,
    pub scenario: String,
    pub seed: u64,
    pub run_id: String,
    pub rank: u64,
    pub store: PathBuf,
    pub sessions: Vec<(String, u64)>,
    pub armed: Vec<usize>,
    pub sockets: PathBuf,
}

impl Arguments {
    pub fn parse(arguments: &[String]) -> Result<Self, String> {
        let mut parsed = Self::default();
        let mut rest = arguments.iter();
        while let Some(flag) = rest.next() {
            let value = rest
                .next()
                .ok_or_else(|| format!("{flag} needs a value"))?
                .clone();
            let number = |value: &str| {
                value
                    .parse::<u64>()
                    .map_err(|_| format!("{flag}: {value} is not a number"))
            };
            match flag.as_str() {
                "--root" => parsed.root = PathBuf::from(value),
                "--contracts" => parsed.contracts = value,
                "--scenario" => parsed.scenario = value,
                "--seed" => parsed.seed = number(&value)?,
                "--run-id" => parsed.run_id = value,
                "--rank" => parsed.rank = number(&value)?,
                "--store" => parsed.store = PathBuf::from(value),
                "--sockets" => parsed.sockets = PathBuf::from(value),
                "--sessions" => {
                    for pair in value.split(',').filter(|pair| !pair.is_empty()) {
                        let (name, id) = pair
                            .split_once('=')
                            .ok_or_else(|| format!("--sessions: {pair}"))?;
                        parsed.sessions.push((name.to_owned(), number(id)?));
                    }
                }
                "--armed" => {
                    for index in value.split(',').filter(|index| !index.is_empty()) {
                        parsed.armed.push(
                            usize::try_from(number(index)?)
                                .map_err(|_| format!("--armed: {index}"))?,
                        );
                    }
                }
                other => return Err(format!("child: unknown argument {other}")),
            }
        }
        Ok(parsed)
    }
}

fn read_json(path: &Path) -> Result<Value, String> {
    let bytes = std::fs::read(path).map_err(|error| format!("{}: {error}", path.display()))?;
    crate::strict::parse(&bytes).map_err(|error| error.to_string())
}

/// The child's side of the evidence: the records the local collector
/// wrote, each on the socket of its origin, within the window.
struct Relay {
    sockets: Vec<(Origin, BufWriter<UnixStream>)>,
    window: Window,
    last: Option<u64>,
}

impl Relay {
    fn send(&mut self, collector: &mut Collector, actions: &mut impl Read) -> Result<(), String> {
        let lines = collector.take_lines();
        self.send_lines(&lines, actions)
    }

    fn send_lines(&mut self, lines: &[u8], actions: &mut impl Read) -> Result<(), String> {
        for line in lines
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
        {
            let record: Value = serde_json::from_slice(line).map_err(|error| error.to_string())?;
            let origin = record
                .get("origin_component")
                .and_then(Value::as_str)
                .and_then(Origin::from_name)
                .ok_or("a record without its origin")?;
            while !self.window.open() {
                self.read_acknowledgement(actions)?;
            }
            let emission = self.window.emit().ok_or("the window is open")?;
            let socket = self
                .sockets
                .iter_mut()
                .find(|(of, _)| *of == origin)
                .map(|(_, socket)| socket)
                .ok_or_else(|| format!("no socket for {}", origin.name()))?;
            let evidence = Evidence {
                emission,
                record: line.to_vec(),
            };
            write_frame(socket, &evidence.encode()).map_err(|refusal| refusal.to_string())?;
            self.last = Some(emission);
            let kind = record.get("kind").and_then(Value::as_str).unwrap_or("");
            while !self
                .window
                .past(emission, super::protocol::decisive(origin.name(), kind))
            {
                self.read_acknowledgement(actions)?;
            }
        }
        Ok(())
    }

    fn read_acknowledgement(&mut self, actions: &mut impl Read) -> Result<(), String> {
        let body = read_frame(actions)
            .map_err(|refusal| refusal.to_string())?
            .ok_or("the supervisor went away")?;
        match Action::decode(&body).map_err(|refusal| refusal.to_string())? {
            Action::Acknowledge { through } => self.window.acknowledge(through),
            other => Err(format!("an action while one is outstanding: {other:?}")),
        }
    }

    /// The end of an action: every frame acknowledged before the next
    /// action is read.
    fn drain(&mut self, actions: &mut impl Read) -> Result<(), String> {
        while !self.window.drained() {
            self.read_acknowledgement(actions)?;
        }
        Ok(())
    }
}

/// Owned IO can be reached from rusqlite's Send callback without unsafe,
/// a second reader, or a lock held by the action that entered SQLite.
struct ChildIo {
    relay: Relay,
    actions: BufReader<std::io::Stdin>,
    reports: BufWriter<std::io::Stdout>,
}

#[cfg(feature = "kill-points")]
fn arm_transaction_barrier(
    current: &mut Process<mqttiumd_store_sqlite::SqliteStore>,
    choice: &Choice,
    books: &Books,
    io: &Arc<Mutex<ChildIo>>,
) -> Result<bool, Stop> {
    use crate::store::adapter::FaultPoint;
    use mqttiumd_store_sqlite::kill_points::KillPoint;

    let Some(prepared) = current.prepare_transaction_barrier(choice, books)? else {
        return Ok(false);
    };
    let wanted = match prepared.point {
        FaultPoint::CommitPrepared => KillPoint::CommitPrepared,
        FaultPoint::CommitLogged => KillPoint::CommitLogged,
        _ => return Err(stop("an unexpected transaction point")),
    };
    let io = Arc::clone(io);
    let effective = Arc::new(Mutex::new(Value::Null));
    let for_hook = Arc::clone(&effective);
    let mut hits = books.hits.clone();
    let hit = hits
        .get_mut(prepared.scenario_fault)
        .ok_or_else(|| stop("transaction fault has no counter"))?;
    *hit = hit
        .checked_add(1)
        .ok_or_else(|| stop("transaction fault counter exhausted"))?;
    let armed = u32::try_from(current.armed_deadlines()).unwrap_or(u32::MAX);
    let held: Vec<u32> = books
        .held
        .keys()
        .filter_map(|id| u32::try_from(*id).ok())
        .collect();
    current
        .store_mut_for_kill_hook()
        .set_kill_hook(Some(Box::new(move |point| {
            if point != wanted {
                return;
            }
            let outcome = (|| -> Result<(), String> {
                let mut io = io.lock().map_err(|_| "the child IO lock is poisoned")?;
                let ChildIo {
                    relay,
                    actions,
                    reports,
                } = &mut *io;
                // The record was prepared earlier, but its bytes leave the
                // process only NOW, from the actual SQLite callback.
                relay.send_lines(&prepared.lines, actions)?;
                relay.drain(actions)?;
                let fault = u32::try_from(prepared.scenario_fault)
                    .map_err(|_| "transaction fault index does not fit")?;
                write_frame(
                    reports,
                    &Report::State {
                        busy: true,
                        halted: true,
                        armed,
                        barrier: Some(fault),
                        hits: hits.clone(),
                        held: held.clone(),
                    }
                    .encode(),
                )
                .map_err(|why| why.to_string())?;
                write_frame(
                    reports,
                    &Report::Parked {
                        last_emission: relay.last.ok_or("transaction hit was not emitted")?,
                        fault,
                        point: prepared.point.name().to_owned(),
                        settings: for_hook
                            .lock()
                            .map_err(|_| "settings lock poisoned")?
                            .to_string(),
                    }
                    .encode(),
                )
                .map_err(|why| why.to_string())?;
                // An outstanding SQLite call has no action reply. This read
                // parks its stack until SIGKILL; EOF or any extra action is
                // an explicit protocol failure, never a completed command.
                match read_frame(actions).map_err(|why| why.to_string())? {
                    None => Err("the supervisor closed a parked transaction".to_owned()),
                    Some(_) => Err("an action followed a parked transaction".to_owned()),
                }
            })();
            eprintln!(
                "mqttiumd-lab child: {}",
                outcome.expect_err("a parked callback cannot finish")
            );
            std::process::exit(70);
        })))
        .map_err(|why| stop(format!("arming the SQLite point: {why:?}")))?;
    *effective
        .lock()
        .map_err(|_| stop("settings lock poisoned"))? = sqlite_settings(current.store());
    Ok(true)
}

/// `mqttiumd-lab child`: runs the process under test until `Stop` or the
/// end of its standard input.
pub fn serve(arguments: &Arguments) -> Result<(), String> {
    let root = &arguments.root;
    let scenario_document = read_json(&root.join(&arguments.scenario))?;
    let scenario = Scenario::read(&scenario_document)?;
    let fixture_path = scenario_document
        .get("initial_fixture")
        .and_then(Value::as_str)
        .ok_or("the scenario names no fixture")?;
    let fixture = read_json(&root.join(fixture_path))?;
    let schema = read_json(&root.join(format!(
        "{}/trace-record.schema.json",
        crate::files::contract_directories(Path::new(&arguments.contracts))?.1
    )))?;
    let kinds = KindTable::from_schema(&schema)?;
    let settings = settings_of(
        &fixture,
        &arguments.run_id,
        arguments.seed,
        scenario.faults.len(),
    )?;
    let population = Population {
        clients: arguments
            .sessions
            .iter()
            .map(|(name, id)| (name.clone(), Vec::new(), SessionId::new(*id)))
            .collect(),
    };
    let bound = bound_exchanges(&scenario, &population).map_err(|Stop(why)| why)?;
    let mut faults = Vec::new();
    for &index in &arguments.armed {
        let fault = scenario
            .faults
            .get(index)
            .ok_or_else(|| format!("no fault {index}"))?;
        let name: &'static str = Box::leak(fault.id.clone().into_boxed_str());
        faults.push((
            index,
            specification(fault, &bound).map_err(|Stop(why)| why)?,
            name,
        ));
    }
    let mut sockets = Vec::new();
    for origin in CHILD_ORIGINS {
        let path = arguments.sockets.join(format!("{}.sock", origin.name()));
        let socket =
            UnixStream::connect(&path).map_err(|error| format!("{}: {error}", path.display()))?;
        sockets.push((origin, BufWriter::new(socket)));
    }
    let relay = Relay {
        sockets,
        window: Window::new(),
        last: None,
    };
    let io = Arc::new(Mutex::new(ChildIo {
        relay,
        actions: BufReader::new(std::io::stdin()),
        reports: BufWriter::new(std::io::stdout()),
    }));
    let report = |reports: &mut BufWriter<_>, report: &Report| {
        write_frame(reports, &report.encode()).map_err(|refusal| refusal.to_string())
    };
    let mut collector = Collector::new(&arguments.run_id, kinds, scenario.max_trace_bytes);
    for _ in 0..arguments.rank {
        collector.restart_process();
    }
    let mut books = Books::new(scenario.faults.len());
    let mut process: Option<Process<mqttiumd_store_sqlite::SqliteStore>> = None;
    let mut offered: Vec<Choice> = Vec::new();
    let mut published: Vec<String> = Vec::new();
    loop {
        let body = {
            let mut io = io.lock().map_err(|_| "the child IO lock is poisoned")?;
            io.relay.last = None;
            read_frame(&mut io.actions).map_err(|refusal| refusal.to_string())?
        };
        let Some(body) = body else {
            return Ok(());
        };
        let action = Action::decode(&body).map_err(|refusal| refusal.to_string())?;
        let mut others: Vec<Report> = Vec::new();
        let mut stopping = false;
        let outcome: Result<(), Stop> = (|| {
            match action {
                Action::Start { rank } => {
                    if process.is_some() || u64::from(rank) != arguments.rank {
                        return Err(stop("a start-up of another rank, or a second one"));
                    }
                    let store = mqttiumd_store_sqlite::SqliteStore::open(&arguments.store)
                        .map_err(|error| {
                            stop(format!("{}: {error:?}", arguments.store.display()))
                        })?;
                    let read_back = sqlite_settings(&store).to_string();
                    let mut startup = Vec::new();
                    let started = Process::start(
                        store,
                        arguments.rank,
                        settings,
                        &faults,
                        &mut collector,
                        &mut books,
                        &mut startup,
                    )?;
                    others.push(Report::Started {
                        epoch: started.epoch().get(),
                        startup,
                        settings: read_back,
                    });
                    process = Some(started);
                }
                Action::Stop => stopping = true,
                Action::Acknowledge { .. } => {
                    return Err(stop("an acknowledgement outside an action"));
                }
                Action::Inspect => {
                    return Err(stop(
                        "the supervisor inspects the SQLite store itself, read-only",
                    ));
                }
                other => {
                    let current = process
                        .as_mut()
                        .ok_or_else(|| stop("the process is not running"))?;
                    match other {
                        Action::Recover => current.recover(&mut collector, &mut books)?,
                        Action::Settle => current.settle(&mut collector, &mut books)?,
                        Action::Choose { index, digest } => {
                            resolve_choice(&published, index, &digest).map_err(stop)?;
                            let choice = offered
                                .get(index as usize)
                                .cloned()
                                .ok_or_else(|| stop("no such choice"))?;
                            #[cfg(feature = "kill-points")]
                            let armed = {
                                {
                                    let mut guard =
                                        io.lock().map_err(|_| stop("child IO lock poisoned"))?;
                                    let ChildIo { relay, actions, .. } = &mut *guard;
                                    relay.send(&mut collector, actions).map_err(stop)?;
                                    relay.drain(actions).map_err(stop)?;
                                }
                                arm_transaction_barrier(current, &choice, &books, &io)?
                            };
                            let written = current.perform(&choice, &mut collector, &mut books)?;
                            #[cfg(feature = "kill-points")]
                            if armed {
                                current
                                    .store_mut_for_kill_hook()
                                    .set_kill_hook(None)
                                    .map_err(|why| {
                                        stop(format!("disarming a missed point: {why:?}"))
                                    })?;
                                return Err(stop(
                                    "the armed transaction kill point was not reached",
                                ));
                            }
                            current.settle(&mut collector, &mut books)?;
                            if let Some(written) = written {
                                others.push(Report::Written {
                                    client: written.client,
                                    bytes: written.bytes,
                                    origin: written.cause.origin.name().to_owned(),
                                    incarnation: written.cause.incarnation,
                                    sequence: written.cause.sequence,
                                });
                            }
                        }
                        Action::Connect { generation, client } => {
                            current.connect(&client, ConnectionGeneration::new(generation));
                        }
                        Action::Bytes {
                            generation,
                            bytes,
                            sent_incarnation,
                            sent_sequence,
                        } => {
                            let sent = Cause {
                                origin: Origin::Peer,
                                incarnation: sent_incarnation,
                                sequence: sent_sequence,
                            };
                            current.receive(
                                ConnectionGeneration::new(generation),
                                &bytes,
                                sent,
                                &mut collector,
                            )?;
                        }
                        Action::FireDeadline => {
                            if current.fire_deadline(&mut collector, &mut books)?.is_none() {
                                return Err(stop("no deadline armed"));
                            }
                        }
                        Action::ReleaseHeld { fault } => {
                            let id = books
                                .held
                                .get(&(fault as usize))
                                .copied()
                                .ok_or_else(|| stop("the fault holds no command"))?;
                            current.release_command(id, &mut collector, &mut books)?;
                        }
                        Action::DeliverHeld { fault } => {
                            let id = books
                                .held
                                .get(&(fault as usize))
                                .copied()
                                .ok_or_else(|| stop("the fault holds no result"))?;
                            current.deliver_held_result(id, &mut collector, &mut books)?;
                        }
                        Action::LeaveBarrier { fault } => {
                            if books.barrier != Some(fault as usize) {
                                return Err(stop("the adapter stands at another barrier"));
                            }
                            current.leave_barrier(&mut collector, &mut books)?;
                        }
                        Action::Crash => {
                            let taken = process.take().ok_or_else(|| stop("not running"))?;
                            let (store, open) = taken.crash(&mut collector, &mut books)?;
                            // The connection of the writer goes with it.
                            drop(store);
                            books.held.clear();
                            others.push(Report::Crashed { open });
                        }
                        Action::Start { .. }
                        | Action::Stop
                        | Action::Acknowledge { .. }
                        | Action::Inspect => unreachable!("handled above"),
                    }
                }
            }
            Ok(())
        })();
        let mut guard = io.lock().map_err(|_| "the child IO lock is poisoned")?;
        let ChildIo {
            relay,
            actions,
            reports,
        } = &mut *guard;
        relay.send(&mut collector, actions)?;
        for other in &others {
            report(reports, other)?;
        }
        if let Err(Stop(reason)) = outcome {
            report(reports, &Report::Stopped { reason })?;
        }
        if let Some(current) = &process {
            offered = current.choices();
            published = offered.iter().map(Choice::describe).collect();
            report(
                reports,
                &Report::Eligible {
                    digest: choices_digest(&published),
                    choices: published.clone(),
                },
            )?;
            report(
                reports,
                &Report::State {
                    busy: current.busy(),
                    halted: current.halted(),
                    armed: u32::try_from(current.armed_deadlines()).unwrap_or(u32::MAX),
                    barrier: books.barrier.and_then(|fault| u32::try_from(fault).ok()),
                    hits: books.hits.clone(),
                    held: books
                        .held
                        .keys()
                        .filter_map(|fault| u32::try_from(*fault).ok())
                        .collect(),
                },
            )?;
        }
        if stopping {
            report(
                reports,
                &Report::Finished {
                    unwritten: books
                        .unwritten
                        .iter()
                        .map(|(kind, count)| (kind.clone(), *count))
                        .collect(),
                },
            )?;
        }
        report(
            reports,
            &Report::EndOfAction {
                last_emission: relay.last,
            },
        )?;
        relay.drain(actions)?;
        if stopping {
            return Ok(());
        }
    }
}
