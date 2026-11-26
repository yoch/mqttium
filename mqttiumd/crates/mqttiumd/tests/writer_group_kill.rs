//! The real store worker, killed between two answers of a group (record
//! 0045).
//!
//! The members of a group commit together and their answers reach the
//! consumer one by one through the bounded reply lane. A process that dies
//! in between has delivered some answers and not the others. This test
//! drives `writer::Writer` itself, on a real SQLite file, in the normal
//! build with grouping active (no test control is armed), in a child
//! process: one group of `MEMBERS` commands is committed, the consumer
//! takes exactly `k` of its answers, and the parent kills the child with
//! `SIGKILL`. Every `k` from none to all but one is played.
//!
//! After each kill the parent opens the file, starts a new worker on it and
//! presents the same commands again, as one group. The store must hold
//! every member, whatever the number of answers that were taken; each
//! command must be recognised with the outcome its receipt holds, without a
//! write; the answers must come back in request order; and nothing may be
//! admitted or released a second time.
//!
//! How a group of a known size is formed without a seam: a group is made of
//! the commands already waiting in the request lane when the worker takes
//! the first. The worker takes no request while a reply of its own is
//! waiting for room in the reply lane, so the consumer leaves that lane
//! full, queues the commands behind, and only then reads. This is the
//! ordinary backpressure of the product, not a test control.

use std::collections::BTreeSet;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command as Process, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mqttiumd::events::Events;
use mqttiumd::writer::{Hooks, MAX_CLIENT, MAX_PAYLOAD, MAX_TOPIC, Request, Response, Writer};
use mqttiumd_contract::capacity::{ExchangeSlots, PayloadBytes, ReceiptSlots, References};
use mqttiumd_contract::command::{
    AdmitPublication, Command, Costs, DeliveryCause, FrozenRecipe, PayloadRef, Recipe,
    ReleaseInbound, Source, Target,
};
use mqttiumd_contract::effects::{Effect, InboundPhase, OwnedEffect};
use mqttiumd_contract::ids::{
    DeliveryId, EffectId, EffectSeq, ExchangeGeneration, ExchangeId, InboundPacketId,
    LogicalOwnerId, OperationId, OperationSeq, PayloadId, PayloadSeq, PublicationId,
    PublicationSeq, RequestId, RequestSeq, Revision, RunEpoch, SessionId, StoreIncarnation,
};
use mqttiumd_contract::outcome::{StoreOutcome, StoreReply};
use mqttiumd_contract::packet::{Qos, SharedBytes};
use mqttiumd_store_api::{Decision, DurableState, SessionState};
use mqttiumd_store_sqlite::SqliteStore;

/// Set in the environment of the child: the directory of its store.
const CHILD: &str = "MQTTIUMD_WRITER_GROUP_KILL_CHILD";
/// Set in the environment of the child: how many answers it takes.
const TAKE: &str = "MQTTIUMD_WRITER_GROUP_KILL_TAKE";
/// The child stands still once it printed this line.
const AT: &str = "writer-group-kill-child: standing after ";
/// One answer the child took, as `ANSWER<index> <reply>`.
const ANSWER: &str = "writer-group-kill-child: answer ";
/// The commands of the group: three admissions towards two sessions, the
/// first two followed by the release of their inbound exchange.
const MEMBERS: usize = 5;
/// The reply lane of the worker holds this many answers (`writer.rs`,
/// `spawn_bound`: twice the request lane). One request more leaves the
/// worker holding a reply it cannot publish. With a smaller number here
/// the worker is never held: the group then forms only if the commands
/// happen to be queued before the worker reaches the first, and the size
/// asserted on the record of the worker is all that would say otherwise.
const REPLY_LANE: usize = 2 * mqttiumd::writer::QUEUE;
const PATIENCE: Duration = Duration::from_secs(30);
const OWNER: LogicalOwnerId = LogicalOwnerId::new(0);
const CLIENTS: [&[u8]; 3] = [b"producer", b"target-b", b"target-c"];
const TOPIC: &[u8] = b"group/kill";

static NEXT: AtomicU64 = AtomicU64::new(0);

fn fresh_directory(name: &str) -> PathBuf {
    let directory = std::env::temp_dir().join(format!(
        "mqttiumd-writer-group-kill-{}-{}-{name}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&directory).expect("a temporary directory");
    directory
}

fn store_path(directory: &Path) -> PathBuf {
    directory.join("store.sqlite")
}

/// What the commands of the group are built from. The child takes it from
/// the worker; the parent reads the same values back from the file.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Fixture {
    incarnation: StoreIncarnation,
    epoch: RunEpoch,
    /// The producer and the two targets, each with its revision.
    sessions: [(SessionId, Revision); 3],
    route_revision: Revision,
}

impl Fixture {
    fn operation(&self, sequence: u64) -> OperationId {
        OperationId::new(
            self.incarnation,
            self.epoch,
            OWNER,
            OperationSeq::new(sequence),
        )
    }

    fn publication(&self, sequence: u64) -> PublicationId {
        PublicationId::new(
            self.incarnation,
            self.epoch,
            OWNER,
            PublicationSeq::new(sequence),
        )
    }

    fn exchange(&self, sequence: u64) -> ExchangeId {
        ExchangeId {
            session: self.sessions[0].0,
            generation: ExchangeGeneration::new(sequence),
        }
    }

    /// A QoS 2 admission of the producer's packet `packet` towards both
    /// targets. `sequence` numbers every identity of the recipe.
    fn admission(&self, sequence: u64, packet: u16) -> Command {
        let payload = format!("payload {sequence}").into_bytes();
        let length = PayloadBytes::new(payload.len() as u64);
        let publication = self.publication(sequence);
        let targets: Vec<Target> = self.sessions[1..]
            .iter()
            .zip(0u32..)
            .map(|((session, revision), index)| Target {
                session: *session,
                session_revision: *revision,
                delivery: DeliveryId { publication, index },
                effective_qos: Qos::AtLeastOnce,
                cause: DeliveryCause::LiveExact,
            })
            .collect();
        let recipe = Recipe::AdmitPublication(AdmitPublication {
            operation: self.operation(sequence),
            request: RequestId::new(
                self.incarnation,
                self.epoch,
                OWNER,
                RequestSeq::new(sequence),
            ),
            publication,
            source: Source {
                session: self.sessions[0].0,
                session_revision: self.sessions[0].1,
                packet_id: InboundPacketId::new(packet).expect("non-zero"),
                exchange: self.exchange(sequence),
            },
            route_revision: self.route_revision,
            qos: Qos::ExactlyOnce,
            retain: false,
            topic: SharedBytes::copy_from(TOPIC, MAX_TOPIC).expect("fits"),
            payload: PayloadRef {
                id: PayloadId::new(
                    self.incarnation,
                    self.epoch,
                    OWNER,
                    PayloadSeq::new(sequence),
                ),
                length,
            },
            costs: Costs {
                payload_bytes: length,
                references: References::new(targets.len() as u64),
                exchange_slots: ExchangeSlots::new(1),
                receipt_slots: ReceiptSlots::new(1),
            },
            targets: mqttiumd_contract::command::TargetVec::from_slice(&targets, targets.len())
                .expect("fits"),
        });
        Command {
            recipe: FrozenRecipe::freeze(recipe).expect("well formed"),
            payload: Some(SharedBytes::copy_from(&payload, MAX_PAYLOAD).expect("fits")),
        }
    }

    /// The release of the exchange admitted as `admitted_as`.
    fn release(&self, sequence: u64, packet: u16, admitted_as: u64) -> Command {
        let recipe = Recipe::ReleaseInbound(ReleaseInbound {
            operation: self.operation(sequence),
            source_session: self.sessions[0].0,
            packet_id: InboundPacketId::new(packet).expect("non-zero"),
            exchange: self.exchange(admitted_as),
        });
        Command {
            recipe: FrozenRecipe::freeze(recipe).expect("well formed"),
            payload: None,
        }
    }

    /// The group, in request order.
    fn commands(&self) -> Vec<Command> {
        let commands = vec![
            self.admission(1, 11),
            self.release(2, 11, 1),
            self.admission(3, 12),
            self.release(4, 12, 3),
            self.admission(5, 13),
        ];
        assert_eq!(commands.len(), MEMBERS);
        commands
    }

    /// The same values, read back from a file the child wrote. The epoch
    /// is the last one opened: the reader calls this before a new worker
    /// opens the next.
    fn of(state: &DurableState) -> Self {
        let session = |client: &[u8]| {
            let found: Vec<_> = state
                .sessions
                .iter()
                .filter(|(_, record)| {
                    record.state == SessionState::Active && record.client_id == client
                })
                .collect();
            assert_eq!(found.len(), 1, "one session of {client:?}");
            (*found[0].0, found[0].1.revision)
        };
        Self {
            incarnation: state.ledger.incarnation(),
            epoch: state.ledger.last_opened(),
            sessions: CLIENTS.map(session),
            route_revision: state.route_revision,
        }
    }
}

fn operation(command: &Command) -> OperationId {
    command.recipe.get().operation()
}

/// The records of a worker, one JSON object for each complete line. A
/// line the worker is still writing has no end yet and is left out.
fn events(path: &Path) -> Vec<serde_json::Value> {
    let text = std::fs::read_to_string(path).expect("the event file");
    let complete = text.rfind('\n').map_or("", |end| &text[..end]);
    complete
        .lines()
        .map(|line| serde_json::from_str(line).expect("one record for each line"))
        .collect()
}

fn of_kind<'a>(events: &'a [serde_json::Value], kind: &str) -> Vec<&'a serde_json::Value> {
    events
        .iter()
        .filter(|event| event["kind"] == kind)
        .collect()
}

/// What the records of a worker must say of the group: it was applied as
/// one group of `MEMBERS` commands, and each member has one result, in
/// request order, with these `committed_now` and `wrote`.
fn assert_one_group(path: &Path, commands: &[Command], fresh: bool) {
    let events = events(path);
    let groups = of_kind(&events, "store_group");
    assert_eq!(groups.len(), 1, "one group was formed: {groups:?}");
    assert_eq!(
        groups[0]["details"]["commands"],
        serde_json::json!(MEMBERS),
        "every command of the test is a member of that group"
    );
    let results = of_kind(&events, "store_result");
    assert_eq!(results.len(), MEMBERS, "one result for each member");
    for (result, command) in results.iter().zip(commands) {
        assert_eq!(
            result["operation"]["sequence"],
            serde_json::json!(operation(command).sequence().get().to_string()),
            "the results follow the requests"
        );
        assert_eq!(
            result["details"],
            serde_json::json!({"committed_now": fresh, "wrote": fresh, "outcome": "Committed"}),
            "{result}"
        );
    }
}

/// A worker and the directory of its records.
struct Worker {
    writer: Writer,
    events: PathBuf,
    cancel: Arc<AtomicBool>,
}

impl Worker {
    /// The real worker on the file of `directory`, with no test control:
    /// groups are formed.
    fn start(directory: &Path, name: &str) -> (Self, mqttiumd::writer::Boot) {
        let events = directory.join(format!("{name}-events.jsonl"));
        let cancel = Arc::new(AtomicBool::new(false));
        let writer = Writer::spawn(
            store_path(directory),
            Events::open(&events).expect("the event file"),
            Hooks::default(),
            Arc::clone(&cancel),
        )
        .unwrap_or_else(|failure| panic!("the worker did not start: {}", failure.code));
        let boot = match writer.boot.recv_timeout(PATIENCE) {
            Ok(Ok(boot)) => boot,
            Ok(Err(failure)) => panic!("the store did not open: {}", failure.code),
            Err(error) => panic!("the worker did not report its opening: {error}"),
        };
        (
            Self {
                writer,
                events,
                cancel,
            },
            boot,
        )
    }

    fn send(&self, request: Request) {
        assert!(
            self.writer.sender.send(request).is_ok(),
            "the worker is gone"
        );
    }

    fn answer(&self) -> Response {
        self.writer
            .receiver
            .recv_timeout(PATIENCE)
            .expect("an answer of the worker")
    }

    /// The session of `client`, created when it does not exist.
    fn session(&self, request: u64, client: &[u8]) -> (SessionId, Revision) {
        self.send(Request::Session {
            request,
            client: SharedBytes::copy_from(client, MAX_CLIENT).expect("fits"),
        });
        match self.answer() {
            Response::Session {
                request: answered,
                result: Ok(session),
            } if answered == request => (session.id, session.revision),
            _ => panic!("the session of {client:?} was not answered"),
        }
    }

    /// Queues `commands` so that the worker finds them all waiting when it
    /// takes the first, and lets it take them. No answer of the group is
    /// read here.
    fn submit_as_one_group(&self, epoch: RunEpoch, commands: &[Command]) {
        // One request more than the reply lane holds: the worker keeps the
        // last reply and takes nothing until this side reads.
        for request in 0..=REPLY_LANE as u64 {
            self.send(Request::Session {
                request: 1_000 + request,
                client: SharedBytes::copy_from(CLIENTS[0], MAX_CLIENT).expect("fits"),
            });
        }
        // The commands are behind those requests in the lane, and the lane
        // has room for them all: each `send` returns with its command
        // queued and none is taken before the lane of replies is read.
        assert!(commands.len() <= mqttiumd::writer::QUEUE);
        for (command, sequence) in commands.iter().zip(1u64..) {
            self.send(Request::Effect(OwnedEffect::new(
                EffectId {
                    epoch,
                    owner: OWNER,
                    sequence: EffectSeq::new(sequence),
                },
                Effect::Persist {
                    command: command.clone(),
                },
            )));
        }
        for request in 0..=REPLY_LANE as u64 {
            match self.answer() {
                Response::Session {
                    request: answered,
                    result: Ok(_),
                } if answered == 1_000 + request => {}
                _ => panic!("request {request} of the full lane was not answered in order"),
            }
        }
    }

    /// The next answer of the group: the reply to `command`, carried by the
    /// effect numbered `sequence`.
    fn store_answer(&self, epoch: RunEpoch, sequence: u64, command: &Command) -> StoreReply {
        match self.answer() {
            Response::Store {
                effect,
                operation: answered,
                reply,
                ..
            } => {
                assert_eq!(
                    (effect.epoch, effect.sequence),
                    (epoch, EffectSeq::new(sequence)),
                    "the answers follow the requests"
                );
                assert_eq!(answered, operation(command));
                reply
            }
            _ => panic!("answer {sequence} is not the result of a command"),
        }
    }

    /// Waits until the worker recorded the result of every member: the
    /// commit of the group has returned.
    fn wait_for_the_results(&self) {
        let start = Instant::now();
        while of_kind(&events(&self.events), "store_result").len() < MEMBERS {
            assert!(
                start.elapsed() < PATIENCE,
                "the group was not committed within {PATIENCE:?}"
            );
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    fn stop(mut self) {
        self.send(Request::Stop);
        assert!(matches!(self.answer(), Response::Stopped));
        let start = Instant::now();
        while !self.writer.is_finished() {
            assert!(start.elapsed() < PATIENCE, "the worker did not stop");
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(self.writer.join_finished().is_ok());
        assert!(!self.cancel.load(Ordering::Relaxed));
    }
}

/// Not a test of its own: the body of the child process, run only when the
/// parent sets `CHILD`. It commits one group through the real worker, takes
/// `TAKE` of its answers, says so and stands still, the worker alive and
/// the other answers unread, until the parent kills it.
#[test]
fn writer_group_kill_child() {
    let Some(directory) = std::env::var_os(CHILD) else {
        return;
    };
    let directory = PathBuf::from(directory);
    let take: usize = std::env::var(TAKE)
        .expect("how many answers to take")
        .parse()
        .expect("a number");
    assert!(take < MEMBERS);
    let (worker, boot) = Worker::start(&directory, "child");
    let mut sessions = [(SessionId::new(0), Revision::new(0)); 3];
    for ((slot, client), request) in sessions.iter_mut().zip(CLIENTS).zip(0u64..) {
        *slot = worker.session(request, client);
    }
    let fixture = Fixture {
        incarnation: boot.grant.incarnation(),
        epoch: boot.grant.epoch(),
        sessions,
        route_revision: Revision::new(0),
    };
    let commands = fixture.commands();
    worker.submit_as_one_group(fixture.epoch, &commands);
    // Also when no answer is taken, the kill comes after the commit.
    worker.wait_for_the_results();
    let mut out = std::io::stdout().lock();
    for (command, sequence) in commands.iter().zip(1u64..).take(take) {
        let reply = worker.store_answer(fixture.epoch, sequence, command);
        writeln!(out, "{ANSWER}{sequence} {reply:?}").expect("said");
    }
    writeln!(out, "{AT}{take} of {MEMBERS}").expect("said");
    out.flush().expect("flushed");
    drop(out);
    // The worker lives and holds the answers nobody took.
    loop {
        std::thread::sleep(Duration::from_secs(60));
    }
}

struct Holder {
    child: Child,
}

impl Holder {
    /// The test binary itself, run again on `writer_group_kill_child`
    /// only. Returns, with the answers the child said it took, once the
    /// child stands still.
    fn spawn(directory: &Path, take: usize) -> (Self, Vec<String>) {
        let mut child = Process::new(std::env::current_exe().expect("the test binary"))
            .args([
                "--exact",
                "writer_group_kill_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD, directory)
            .env(TAKE, take.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("the child process");
        let stdout = child.stdout.take().expect("its output");
        // Own the child before any fallible wait: a timeout or a closed
        // pipe must still kill and reap it.
        let holder = Self { child };
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        let reader = std::thread::spawn(move || {
            let mut answers = Vec::new();
            let mut standing = None;
            for line in BufReader::new(stdout)
                .take(1 << 16)
                .lines()
                .map_while(Result::ok)
            {
                if let Some(at) = line.find(ANSWER) {
                    answers.push(line[at + ANSWER.len()..].to_owned());
                } else if let Some(at) = line.find(AT) {
                    standing = Some(line[at + AT.len()..].to_owned());
                    break;
                }
            }
            let _ = sender.send((standing, answers));
        });
        let (standing, answers) = receiver
            .recv_timeout(Duration::from_secs(60))
            .expect("the child reported within 60 seconds");
        reader.join().expect("the bounded reader ended");
        assert_eq!(
            standing,
            Some(format!("{take} of {MEMBERS}")),
            "the child did not stand after {take} answers"
        );
        (holder, answers)
    }

    /// `SIGKILL`, then the status the kernel reports.
    fn kill(mut self) {
        self.child.kill().expect("killed");
        let status = self.child.wait().expect("reaped");
        assert_eq!(status.signal(), Some(9), "killed by SIGKILL: {status:?}");
    }
}

impl Drop for Holder {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// What the file must hold of the group, whatever number of its answers
/// was taken: a receipt for each member, the three publications with their
/// payloads and an obligation towards each target, the first two exchanges
/// released once and the third still open.
fn assert_every_member_is_held(state: &DurableState, fixture: &Fixture, commands: &[Command]) {
    for command in commands {
        let record = state
            .operations
            .get(&operation(command))
            .unwrap_or_else(|| panic!("no receipt of {:?}", operation(command)));
        let mut canonical = Vec::new();
        command
            .recipe
            .get()
            .encode_canonical(&mut canonical)
            .expect("canonical bytes");
        assert_eq!(record.recipe, canonical);
        assert!(matches!(record.decision, Decision::Committed(_)));
    }
    assert_eq!(state.operations.len(), MEMBERS, "no other receipt");
    assert!(state.fences.is_empty());
    let admitted: BTreeSet<PublicationId> = [1, 3, 5]
        .into_iter()
        .map(|sequence| fixture.publication(sequence))
        .collect();
    assert_eq!(
        state.publications.keys().copied().collect::<BTreeSet<_>>(),
        admitted,
        "each admission once"
    );
    assert_eq!(state.payloads.len(), admitted.len());
    let owed: BTreeSet<(DeliveryId, SessionId)> = admitted
        .iter()
        .flat_map(|publication| {
            fixture.sessions[1..]
                .iter()
                .zip(0u32..)
                .map(|((session, _), index)| {
                    (
                        DeliveryId {
                            publication: *publication,
                            index,
                        },
                        *session,
                    )
                })
        })
        .collect();
    assert_eq!(
        state
            .obligations
            .iter()
            .map(|(delivery, record)| (*delivery, record.target))
            .collect::<BTreeSet<_>>(),
        owed,
        "one obligation towards each target of each admission"
    );
    // A released exchange keeps its marker in another phase or loses it;
    // either way it is no longer waiting for its release, and the third
    // one still is.
    let waiting: Vec<_> = state
        .inbound
        .iter()
        .filter(|(_, record)| record.phase == InboundPhase::AwaitPubrel)
        .map(|((session, packet), record)| (*session, packet.get(), record.exchange))
        .collect();
    assert_eq!(
        waiting,
        [(fixture.sessions[0].0, 13, fixture.exchange(5))],
        "the first two exchanges were released, the third was not"
    );
    assert_eq!(state.accounts.exchange_slots, ExchangeSlots::new(1));
    assert_eq!(
        state.accounts.receipt_slots,
        ReceiptSlots::new(MEMBERS as u64)
    );
    assert_eq!(
        state.accounts.references,
        References::new(owed.len() as u64)
    );
}

/// Kills the real worker after `take` answers of a committed group, then
/// reopens the file and presents the group again to a new worker.
fn killed_after(take: usize) {
    let directory = fresh_directory(&format!("take-{take}"));
    let path = store_path(&directory);
    let (holder, taken) = Holder::spawn(&directory, take);
    assert_eq!(taken.len(), take, "the child took {take} answers");
    holder.kill();

    // No intervention between the kill and the restart. The file opens as
    // the product opens it, integrity check included.
    let store = SqliteStore::open(&path).expect("opened after the kill");
    let recovered = store.try_snapshot().expect("decodes");
    drop(store);
    let fixture = Fixture::of(&recovered);
    let commands = fixture.commands();
    // The group the child committed was one group of that size, and every
    // member said it committed and wrote.
    assert_one_group(&directory.join("child-events.jsonl"), &commands, true);
    assert_every_member_is_held(&recovered, &fixture, &commands);
    let decided: Vec<StoreReply> = commands
        .iter()
        .map(
            |command| match recovered.operations[&operation(command)].decision {
                Decision::Committed(facts) => StoreReply::Outcome(StoreOutcome::Committed(facts)),
                Decision::Rejected(reason) => panic!("a member was rejected: {reason:?}"),
            },
        )
        .collect();
    // The answers that were taken said what the receipts say.
    for ((answer, reply), sequence) in taken.iter().zip(&decided).zip(1u64..) {
        assert_eq!(answer, &format!("{sequence} {reply:?}"));
    }

    // A new worker, a new epoch, and the same commands again.
    let (worker, boot) = Worker::start(&directory, "restart");
    assert!(boot.grant.epoch() > fixture.epoch, "a later epoch");
    let before = SqliteStore::inspect(&path).expect("read beside the worker");
    assert_every_member_is_held(&before, &fixture, &commands);
    worker.submit_as_one_group(boot.grant.epoch(), &commands);
    for ((command, reply), sequence) in commands.iter().zip(&decided).zip(1u64..) {
        assert_eq!(
            worker.store_answer(boot.grant.epoch(), sequence, command),
            *reply,
            "member {sequence} is recognised with the outcome of its receipt"
        );
    }
    // Recognised as one group, each member without a write.
    assert_one_group(&worker.events, &commands, false);
    assert_eq!(
        SqliteStore::inspect(&path).expect("read beside the worker"),
        before,
        "presenting the group again changed nothing, the revision included"
    );
    worker.stop();
    let store = SqliteStore::open(&path).expect("opened after the clean stop");
    assert_eq!(store.try_snapshot().expect("decodes"), before);
    drop(store);
    std::fs::remove_dir_all(directory).expect("cleanup");
}

#[test]
fn a_worker_killed_before_any_answer_of_its_group_left_every_member() {
    killed_after(0);
}

#[test]
fn a_worker_killed_after_one_answer_of_its_group_left_every_member() {
    killed_after(1);
}

#[test]
fn a_worker_killed_after_two_answers_of_its_group_left_every_member() {
    killed_after(2);
}

#[test]
fn a_worker_killed_after_three_answers_of_its_group_left_every_member() {
    killed_after(3);
}

#[test]
fn a_worker_killed_after_all_answers_but_one_of_its_group_left_every_member() {
    killed_after(MEMBERS - 1);
}
