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

use mqttiumd_lab::bench::campaign::{Request, execute};
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
        assert_eq!(
            read(&child_out, "trace.jsonl"),
            read(&local_out, "trace.jsonl"),
            "{scenario}: the trace"
        );
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

/// Runs a campaign in child mode through the binary of the lab, since the
/// test binary cannot be the child.
fn execute_with_lab(request: &Request) -> ChildReport {
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
    std::fs::write(&silent, "#!/bin/sh\nexec sleep 2\n").unwrap();
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
    let Err(stopped) = Child::spawn(&config, 0, &[], &mut collector, &mut books) else {
        panic!("a silent child started");
    };
    assert!(stopped.0.contains("did not connect"), "{}", stopped.0);
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "no endless wait"
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
    let _ = std::fs::remove_dir_all(&out);
}
