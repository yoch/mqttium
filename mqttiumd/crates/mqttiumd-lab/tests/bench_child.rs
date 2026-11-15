//! The process under test in a child process (`run --process child`),
//! without a kill: the supervisor starts the child, drives it action by
//! action, collects its evidence by channel, and stops it cleanly.
//!
//! The child runs the same components on the same file as the in-process
//! runner and is driven by the same choices: the trace and the snapshots
//! must be the same bytes.

#[path = "bench_support.rs"]
mod bench_support;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use mqttiumd_lab::bench::campaign::Request;

use bench_support::execute_apart as execute;
use mqttiumd_lab::bench::child::{Child, Config};
use mqttiumd_lab::bench::collector::{Collector, KindTable};
use mqttiumd_lab::bench::process::Books;

const SCENARIOS: [&str; 7] = [
    "tests/scenarios/p1/P1-02.json",
    "tests/scenarios/p1/P1-02b.json",
    "tests/scenarios/qabs/QABS-A.json",
    "tests/scenarios/qabs/QABS-B.json",
    "tests/scenarios/qabs/QABS-C.json",
    "tests/scenarios/audit/F3-steady-lag.json",
    "tests/scenarios/audit/P1-02b-interposed.json",
];

fn request(scenario: &str, out: PathBuf, backend: &str, process: &str) -> Request {
    Request {
        root: bench_support::root(),
        contracts: PathBuf::from("contracts/v1"),
        scenario: PathBuf::from(scenario),
        out,
        seed: 1,
        run_id: Some("bench-test".to_owned()),
        backend: backend.to_owned(),
        built_from: None,
        process: process.to_owned(),
    }
}

#[test]
fn the_child_gives_the_same_evidence_as_the_in_process_runner() {
    for scenario in SCENARIOS {
        let child_out = bench_support::fresh("child");
        let local_out = bench_support::fresh("local");
        let child = execute_with_lab(&request(scenario, child_out.clone(), "sqlite", "child"));
        let local = execute(&request(
            scenario,
            local_out.clone(),
            "sqlite",
            "in-process",
        ))
        .unwrap_or_else(|error| panic!("{scenario}: {error}"));
        assert_eq!(child.stopped, None, "{scenario}");
        assert_eq!(local.stopped, None, "{scenario}");
        let read = |out: &PathBuf, path: &str| std::fs::read(out.join(path)).unwrap();
        // The same records, but for what only a child process states: the
        // broker of the header and one BrokerKilled per kill (decision
        // 0019). Their numbers shift; their order, origins and contents do
        // not.
        let child_trace = records(&read(&child_out, "trace.jsonl"));
        let local_trace = records(&read(&local_out, "trace.jsonl"));
        assert_eq!(
            child_trace[0]["payload"]["broker"], "child-process",
            "{scenario}: the header"
        );
        assert!(
            local_trace[0]["payload"].get("broker").is_none(),
            "{scenario}: an in-process header names no broker"
        );
        assert!(
            local_trace
                .iter()
                .all(|record| record["kind"] != "BrokerKilled")
        );
        assert_eq!(
            projection(&child_trace),
            projection(&local_trace),
            "{scenario}: the trace"
        );
        let killed = child_trace
            .iter()
            .filter(|record| record["kind"] == "BrokerKilled")
            .count();
        assert_eq!(child_trace.len(), local_trace.len() + killed, "{scenario}");
        let manifest = |out: &PathBuf| -> serde_json::Value {
            serde_json::from_slice(&read(out, "run-manifest.json")).unwrap()
        };
        assert_eq!(
            manifest(&child_out)["broker"],
            "child-process",
            "{scenario}"
        );
        assert!(manifest(&local_out).get("broker").is_none(), "{scenario}");
        assert_eq!(
            read(&child_out, "choices.log"),
            read(&local_out, "choices.log"),
            "{scenario}: the choices"
        );
        for entry in std::fs::read_dir(local_out.join("snapshots")).unwrap() {
            let name = entry.unwrap().file_name().into_string().unwrap();
            let path = format!("snapshots/{name}");
            assert_eq!(
                read(&child_out, &path),
                read(&local_out, &path),
                "{scenario}: {path}"
            );
        }
        let campaign: serde_json::Value =
            serde_json::from_slice(&read(&child_out, "campaign.json")).unwrap();
        assert_eq!(campaign["covers"]["process"], "child");
        // A scenario that crashes the broker kills the child; one that does
        // not stops it cleanly.
        let crashes = std::fs::read_to_string(bench_support::root().join(scenario))
            .unwrap()
            .matches("\"CrashBroker\"")
            .count();
        assert_eq!(
            campaign["kills"].as_array().unwrap().len(),
            crashes,
            "{scenario}"
        );
        let _ = std::fs::remove_dir_all(&child_out);
        let _ = std::fs::remove_dir_all(&local_out);
    }
}

fn records(trace: &[u8]) -> Vec<serde_json::Value> {
    trace
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect()
}

/// Each record by its origin, kind and payload, without BrokerKilled, the
/// broker of the header and the record count of the footer.
fn projection(trace: &[serde_json::Value]) -> Vec<serde_json::Value> {
    trace
        .iter()
        .filter(|record| record["kind"] != "BrokerKilled")
        .map(|record| {
            let mut payload = record["payload"].clone();
            if let Some(payload) = payload.as_object_mut() {
                payload.remove("broker");
                if record["kind"] == "TraceFooter" {
                    payload.remove("record_count");
                }
            }
            serde_json::json!([record["origin_component"], record["kind"], payload])
        })
        .collect()
}

/// Runs a campaign in child mode through the binary of the lab, since the
/// test binary cannot be the child.
fn execute_with_lab(request: &Request) -> ChildReport {
    let _turn = bench_support::spawning();
    let status = std::process::Command::new(env!("CARGO_BIN_EXE_mqttiumd-lab"))
        .args(["run", "--contracts", "contracts/v1"])
        .arg("--scenario")
        .arg(&request.scenario)
        .arg("--out")
        .arg(&request.out)
        .args(["--backend", &request.backend])
        .args(["--process", &request.process])
        .args(["--run-id", request.run_id.as_deref().unwrap()])
        .current_dir(&request.root)
        .output()
        .expect("the lab runs");
    assert!(
        status.status.success(),
        "{}",
        String::from_utf8_lossy(&status.stderr)
    );
    let campaign: serde_json::Value =
        serde_json::from_slice(&std::fs::read(request.out.join("campaign.json")).unwrap()).unwrap();
    ChildReport {
        stopped: campaign["stopped"].as_str().map(str::to_owned),
    }
}

struct ChildReport {
    stopped: Option<String>,
}

#[test]
fn the_child_mode_is_refused_on_the_deterministic_store() {
    let out = bench_support::fresh("child-sim");
    let refused =
        execute(&request(SCENARIOS[0], out.clone(), "sim-store", "child")).expect_err("refused");
    assert!(refused.contains("sqlite only"), "{refused}");
    assert!(!out.exists());
}

#[test]
fn a_child_that_never_speaks_stops_the_run_at_its_deadline() {
    let directory = bench_support::fresh("silent-child");
    std::fs::create_dir_all(&directory).unwrap();
    // A child that ignores its arguments and says nothing.
    let silent = directory.join("silent.sh");
    let pid_file = directory.join("pid");
    std::fs::write(
        &silent,
        format!(
            "#!/bin/sh\necho $$ > {}\nexec sleep 30\n",
            pid_file.display()
        ),
    )
    .unwrap();
    let mut permissions = std::fs::metadata(&silent).unwrap().permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, 0o755);
    std::fs::set_permissions(&silent, permissions).unwrap();
    let config = Config {
        executable: silent,
        campaign: directory.clone(),
        contracts: "contracts/v1".to_owned(),
        scenario: "none.json".to_owned(),
        seed: 1,
        run_id: "silent".to_owned(),
        store: directory.join("none.sqlite"),
        sessions: Vec::new(),
        silence: Duration::from_millis(300),
    };
    let schema = mqttiumd_lab::strict::parse(
        &std::fs::read(bench_support::root().join("contracts/schema/v1/trace-record.schema.json"))
            .unwrap(),
    )
    .unwrap();
    let mut collector = Collector::new("silent", KindTable::from_schema(&schema).unwrap(), 1 << 20);
    let mut books = Books::new(0);
    let started = Instant::now();
    let Err(stopped) = ({
        let _turn = bench_support::spawning();
        Child::spawn(&config, 0, &[], &mut collector, &mut books)
    }) else {
        panic!("a silent child started");
    };
    assert!(stopped.0.contains("did not connect"), "{}", stopped.0);
    assert!(
        started.elapsed() < Duration::from_secs(15),
        "no endless wait"
    );
    // No child outlives the run, even one stopped on an error: it was
    // killed and waited for.
    let pid = std::fs::read_to_string(&pid_file).expect("the child wrote its pid");
    let pid = pid.trim();
    assert!(
        !std::path::Path::new(&format!("/proc/{pid}")).exists(),
        "the silent child {pid} survived the run"
    );
    let _ = std::fs::remove_dir_all(&directory);
}

/// The crash of P1-02 in a child: a real `SIGKILL` to the child that runs
/// the core and the store, its end by signal 9 observed by the supervisor
/// before the next child starts on the same file, and the restart by the
/// real path. The supervisor and its collector stay alive: they write the
/// rest of the trace, and the oracle judges it like the in-process one.
#[test]
fn the_crash_of_a_child_is_a_real_sigkill_and_the_next_child_reads_the_same_file() {
    let out = bench_support::fresh("killed");
    let report = execute_with_lab(&request(SCENARIOS[0], out.clone(), "sqlite", "child"));
    assert_eq!(report.stopped, None);
    let campaign: serde_json::Value =
        serde_json::from_slice(&std::fs::read(out.join("campaign.json")).unwrap()).unwrap();
    let kills = campaign["kills"].as_array().unwrap();
    assert_eq!(kills.len(), 1, "{campaign}");
    let killed = &kills[0];
    assert_eq!(killed["signal_sent"], "SIGKILL");
    assert_eq!(killed["wait_status"]["signal"], 9);
    assert!(killed["wait_status"]["exit_code"].is_null());
    assert_eq!(killed["rank"], "0");
    let pid = killed["pid"].as_u64().unwrap();
    assert_ne!(pid, u64::from(std::process::id()));
    // Its end was waited for: no such process remains.
    assert!(
        !std::path::Path::new(&format!("/proc/{pid}")).exists(),
        "the killed child {pid} is still there"
    );
    let crash = campaign["covers"]["crash"].as_str().unwrap();
    assert!(
        crash.starts_with(&format!("process killed: SIGKILL to pid {pid}")),
        "{crash}"
    );
    assert!(crash.contains("wait status signal 9"), "{crash}");
    assert!(crash.contains("writer lock"), "{crash}");
    assert_eq!(
        campaign["failure_model"]["statement"],
        mqttiumd_lab::bench::campaign::FAILURE_MODEL
    );
    assert!(
        campaign["failure_model"]["statement"]
            .as_str()
            .unwrap()
            .contains("after the decisive record is acknowledged")
    );
    // The records after the kill were written by the supervisor that
    // survived it: the crash, the closed connection, the restart.
    let trace = std::fs::read_to_string(out.join("trace.jsonl")).unwrap();
    let kinds: Vec<String> = trace
        .lines()
        .map(|line| {
            serde_json::from_str::<serde_json::Value>(line).unwrap()["kind"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect();
    let crashed = kinds
        .iter()
        .position(|kind| kind == "BrokerCrashed")
        .unwrap();
    assert!(
        kinds[crashed..]
            .iter()
            .any(|kind| kind == "BrokerRecovered")
    );
    assert_eq!(kinds.last().unwrap(), "TraceFooter");
    // The kill is a record of the trace, from the observed wait status,
    // written just before the crash; no process identifier is in it.
    let all = records(trace.as_bytes());
    let killed_records: Vec<&serde_json::Value> = all
        .iter()
        .filter(|record| record["kind"] == "BrokerKilled")
        .collect();
    assert_eq!(killed_records.len(), 1);
    assert_eq!(kinds[crashed - 1], "BrokerKilled");
    assert_eq!(killed_records[0]["origin_component"], "bench-supervisor");
    assert_eq!(
        killed_records[0]["payload"],
        serde_json::json!({"signal_sent": "SIGKILL", "wait_status": {"signal": 9}})
    );
    // Replayed, it runs in a child again: the recorded command says so.
    let replay = campaign["commands"]["replay"].as_str().unwrap();
    assert!(replay.contains(" --process child"), "{replay}");
    assert!(replay.contains(" --backend sqlite"), "{replay}");
    let _ = std::fs::remove_dir_all(&out);
}

/// A second child started while the first lives is refused by the store:
/// the writer lock of the SQLite file is held for the life of the first.
#[test]
fn a_second_child_on_the_same_file_while_the_first_lives_is_refused() {
    // A campaign leaves the data set, the contracts and the scenario.
    let out = bench_support::fresh("two-writers");
    let campaign =
        execute(&request(SCENARIOS[1], out.clone(), "sqlite", "in-process")).expect("a campaign");
    assert_eq!(campaign.stopped, None);
    let schema = mqttiumd_lab::strict::parse(
        &std::fs::read(bench_support::root().join("contracts/schema/v1/trace-record.schema.json"))
            .unwrap(),
    )
    .unwrap();
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(out.join("campaign.json")).unwrap()).unwrap();
    let run_id = manifest["run_id"].as_str().unwrap().to_owned();
    let campaign_dir = out.canonicalize().unwrap();
    let config = Config {
        executable: PathBuf::from(env!("CARGO_BIN_EXE_mqttiumd-lab")),
        store: campaign_dir.join("store/mqttiumd.sqlite"),
        campaign: campaign_dir,
        contracts: "contracts/v1".to_owned(),
        scenario: "tests/scenarios/p1/P1-02b.json".to_owned(),
        seed: 1,
        run_id: run_id.clone(),
        sessions: sessions(),
        silence: Duration::from_secs(30),
    };
    let collector = || Collector::new(&run_id, KindTable::from_schema(&schema).unwrap(), 1 << 24);
    let (mut first_collector, mut second_collector) = (collector(), collector());
    // The incarnations of the process under test follow the rank.
    for _ in 0..7 {
        first_collector.restart_process();
    }
    for _ in 0..8 {
        second_collector.restart_process();
    }
    let mut books = Books::new(4);
    // A start-up of a rank the data set has not seen opens a new epoch.
    let (first, _) = {
        let _turn = bench_support::spawning();
        Child::spawn(&config, 7, &[], &mut first_collector, &mut books)
    }
    .expect("the first child starts");
    let Err(refused) = ({
        let _turn = bench_support::spawning();
        Child::spawn(&config, 8, &[], &mut second_collector, &mut books)
    }) else {
        panic!("a second writer started on the same file");
    };
    assert!(refused.0.contains("WriterAlive"), "{}", refused.0);
    first
        .stop(&mut first_collector, &mut books)
        .expect("the first child stops");
    let _ = std::fs::remove_dir_all(&out);
}

/// The clients of the fixture and their sessions, as an install gives them.
fn sessions() -> Vec<(String, mqttiumd_contract::ids::SessionId)> {
    let fixture = mqttiumd_lab::strict::parse(
        &std::fs::read(
            bench_support::root().join("tests/fixtures/p1/three-persistent-sessions.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let setup = mqttiumd_lab::bench::campaign::Setup::from_fixture(
        &mut mqttiumd_lab::bench::backend::Memory,
        &fixture,
        "sessions",
        1,
        1,
    )
    .unwrap();
    setup
        .population
        .clients
        .into_iter()
        .map(|(name, _, session)| (name, session))
        .collect()
}

/// Between a kill and the restart nothing opens the database: an
/// inspection there is refused. The scenario format refuses it first (only
/// RestartBroker may follow a crash), before anything is written; the
/// supervisor refuses it again if it ever reached it.
#[test]
fn an_inspection_between_the_kill_and_the_restart_is_refused() {
    let root = bench_support::root();
    let mut scenario: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join(SCENARIOS[0])).unwrap()).unwrap();
    let steps = scenario["steps"].as_array_mut().unwrap();
    let crash = steps
        .iter()
        .position(|step| step["kind"] == "CrashBroker")
        .unwrap();
    steps.insert(
        crash + 1,
        serde_json::json!({"kind": "InspectDurableState", "checkpoint": "after_the_kill"}),
    );
    let relative = format!(
        "target/bench-scenarios/inspect-after-kill-{}.json",
        std::process::id()
    );
    std::fs::create_dir_all(root.join("target/bench-scenarios")).unwrap();
    std::fs::write(
        root.join(&relative),
        serde_json::to_vec_pretty(&scenario).unwrap(),
    )
    .unwrap();
    let out = bench_support::fresh("inspect-after-kill");
    let refused =
        execute(&request(&relative, out.clone(), "sqlite", "child")).expect_err("refused");
    let _ = std::fs::remove_file(root.join(&relative));
    assert!(
        refused.contains("only RestartBroker may follow"),
        "{refused}"
    );
    let _ = std::fs::remove_dir_all(&out);
}
