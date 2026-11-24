//! Regression of finding F3 through the components: answers to
//! resolutions that arrive with a steady lag longer than the old window.
//!
//! The process is started by the runner's real path and driven choice by
//! choice: the first `Persist` commits and its result is lost, one
//! resolution runs per deadline, each answers `Committed`, and each answer
//! is handed to the core only after four newer resolutions ran. The
//! exchange must leave `AdmissionPending`: a positive PUBREC becomes
//! eligible. The scenario format cannot express this lag yet (one fault
//! per passage set in the store adapter), so the test drives the
//! scheduler's choices itself; every choice is one the runner offers.

#[path = "bench_support.rs"]
mod bench_support;

use std::collections::VecDeque;
use std::fs;

use mqttiumd_contract::ids::{ConnectionGeneration, InboundPacketId};
use mqttiumd_lab::bench::backend::Memory;
use mqttiumd_lab::bench::collector::{Collector, KindTable, Origin};
use mqttiumd_lab::bench::mqtt::peer;
use mqttiumd_lab::bench::process::{Books, Choice, Process};
use mqttiumd_lab::bench::setup::Setup;
use mqttiumd_lab::store::adapter::{FaultAction, FaultPoint, FaultSpec, StoreAction, WorkKind};
use mqttiumd_lab::store::durable::MemoryStore;
use serde_json::json;

use bench_support::root;

/// How many newer resolutions run before an answer is handed over.
const LAG: usize = 4;
/// The resolutions the audit's counterexample ran without progress.
const RESOLUTIONS: usize = 24;

struct Bench {
    process: Process<MemoryStore>,
    collector: Collector,
    books: Books,
}

impl Bench {
    fn start() -> Self {
        let read = |path: &str| {
            mqttiumd_lab::strict::parse(&fs::read(root().join(path)).expect(path)).expect(path)
        };
        let fixture = read("tests/fixtures/p1/three-persistent-sessions.json");
        let schema = read("contracts/schema/v1/trace-record.schema.json");
        let setup =
            Setup::from_fixture(&mut Memory, &fixture, "bench-f3", 1, 1).expect("the fixture");
        let (_, _, producer) = setup
            .population
            .clients
            .iter()
            .find(|(name, _, _)| name == "producer")
            .cloned()
            .expect("producer");
        let lost = FaultSpec {
            point: FaultPoint::ResultReady,
            action: FaultAction::DropResult,
            kind: Some(WorkKind::AdmitPublication),
            source: Some((producer, InboundPacketId::new(7).unwrap())),
            occurrence: 1,
        };
        let mut collector = Collector::new(
            "bench-f3",
            KindTable::from_schema(&schema).expect("the schema"),
            1 << 24,
        );
        let mut books = Books::new(1);
        let mut choices = Vec::new();
        let process = Process::start(
            setup.store,
            0,
            setup.settings,
            &[(0, lost, "lost_admission_result")],
            &mut collector,
            &mut books,
            &mut choices,
        )
        .expect("the real start-up");
        let mut bench = Self {
            process,
            collector,
            books,
        };
        bench
            .process
            .recover(&mut bench.collector, &mut bench.books)
            .expect("recovery");
        bench
    }

    fn send(&mut self, generation: ConnectionGeneration, bytes: &[u8]) {
        let sent = self
            .collector
            .emit(
                Origin::Peer,
                "PeerPacketSent",
                &[],
                json!({"client": "producer", "packet_kind": "CONNECT", "bytes_hex": ""}),
            )
            .expect("recorded");
        self.process
            .receive(generation, bytes, sent, &mut self.collector)
            .expect("received");
    }

    fn choose(&mut self, choice: &Choice) -> Option<Vec<u8>> {
        assert!(
            self.process.choices().contains(choice),
            "{choice:?} is not offered"
        );
        let written = self
            .process
            .perform(choice, &mut self.collector, &mut self.books)
            .expect("performed");
        self.process
            .settle(&mut self.collector, &mut self.books)
            .expect("settled");
        written.map(|written| written.bytes)
    }

    fn store_choices(&self, matching: fn(&StoreAction) -> bool) -> Vec<Choice> {
        self.process
            .choices()
            .into_iter()
            .filter(|choice| matches!(choice, Choice::Store(action) if matching(action)))
            .collect()
    }
}

#[test]
fn f3_answers_with_a_steady_lag_longer_than_the_old_window_make_progress() {
    let mut bench = Bench::start();
    let connection = ConnectionGeneration::new(1);
    bench.process.connect("producer", connection);
    bench.send(connection, &peer::connect(b"p1-producer").unwrap());
    bench.choose(&Choice::Packet(connection));
    let connack = bench.choose(&Choice::Frame).expect("CONNACK");
    assert_eq!(peer::observe(&connack).unwrap().session_present, Some(true));

    let publish = peer::publish(b"p1/example", 2, false, false, Some(7), b"payload").unwrap();
    bench.send(connection, &publish);
    bench.choose(&Choice::Packet(connection));
    let persist = bench.store_choices(|action| matches!(action, StoreAction::EnterWriter(_)));
    assert_eq!(persist.len(), 1, "the admission is in transit");
    bench.choose(&persist[0]);
    assert_eq!(bench.books.hits, [1], "the result of the commit is lost");

    let mut answers: VecDeque<Choice> = VecDeque::new();
    let mut progress = None;
    for resolution in 1..=RESOLUTIONS {
        bench
            .process
            .fire_deadline(&mut bench.collector, &mut bench.books)
            .expect("fired")
            .expect("a deadline is armed while the admission is pending");
        let entering = bench.store_choices(|action| matches!(action, StoreAction::EnterWriter(_)));
        assert_eq!(entering.len(), 1, "resolution {resolution} is in transit");
        bench.choose(&entering[0]);
        let ready = bench.store_choices(|action| matches!(action, StoreAction::DeliverResult(_)));
        let new = ready
            .into_iter()
            .find(|choice| !answers.contains(choice))
            .expect("its answer is ready");
        answers.push_back(new);
        if answers.len() > LAG {
            let oldest = answers.pop_front().unwrap();
            bench.choose(&oldest);
            if bench.process.choices().contains(&Choice::Frame) {
                let frame = bench.choose(&Choice::Frame).unwrap();
                progress = Some((resolution, peer::observe(&frame).unwrap()));
                break;
            }
        }
    }
    let (after, pubrec) = progress.unwrap_or_else(|| {
        panic!("{RESOLUTIONS} resolutions with a lag of {LAG}: the exchange is still pending")
    });
    assert_eq!(pubrec.kind, mqttiumd_lab::bench::mqtt::Kind::Pubrec);
    assert_eq!((pubrec.packet_id, pubrec.reason_code), (Some(7), 0));
    assert!(after <= RESOLUTIONS);
}

/// The same counterexample as a scenario the oracle judges: the answer
/// to the first resolution is held while six newer ones run, then
/// handed over. The bound is the result slots of the fixture: seven
/// held answers and one command in transit.
#[test]
fn f3_as_a_scenario_the_first_answer_after_six_newer_resolutions_commits_the_exchange() {
    let campaign =
        bench_support::run_scenario("tests/scenarios/audit/F3-steady-lag.json", 1, "f3-scenario");
    assert_eq!(campaign.report.stopped, None);
    assert!(campaign.report.not_reached.is_empty());
    let trace = bench_support::records(&campaign.trace());
    let resolutions = trace
        .iter()
        .filter(|record| record.kind == "ResolveSubmitted")
        .count();
    assert_eq!(resolutions, 7);
    let late = trace
        .iter()
        .position(|record| record.kind == "HeldItemReleased")
        .expect("the held answer is handed over");
    let pubrec = trace
        .iter()
        .position(|record| {
            record.kind == "PeerPacketObserved" && record.payload["packet_kind"] == "PUBREC"
        })
        .expect("PUBREC");
    assert!(pubrec > late, "PUBREC answers the late answer");
    assert_eq!(trace[pubrec].payload["reason_code"], 0);
}
