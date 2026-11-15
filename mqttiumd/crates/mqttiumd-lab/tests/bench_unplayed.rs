//! What the bench does not play yet is refused, explicitly and before
//! anything is written: the repeated faults of decision 0018
//! (`from_occurrence`, `expected_hits_at_least`, `lag`). A scenario the
//! contracts accept is never played by ignoring a member. The deadlines a
//! `DriveUntil` fires itself (`fire_deadlines`) are played: see
//! `bench_fire_deadlines.rs`.

#[path = "bench_support.rs"]
mod bench_support;

use std::path::PathBuf;

use mqttiumd_lab::bench::campaign::{Request, execute};

fn refused(scenario: &str, name: &str) -> String {
    refused_on(scenario, name, "sim-store")
}

fn refused_on(scenario: &str, name: &str, backend: &str) -> String {
    let out = bench_support::fresh(name);
    let request = Request {
        root: bench_support::root(),
        contracts: PathBuf::from("contracts/v1"),
        scenario: PathBuf::from(scenario),
        out: out.clone(),
        seed: 1,
        run_id: Some("bench-test".to_owned()),
        backend: backend.to_owned(),
        built_from: None,
        process: "in-process".to_owned(),
    };
    let why = execute(&request).expect_err("refused");
    assert!(!out.exists(), "{}: something was written", out.display());
    why
}

#[test]
fn a_repeated_fault_of_decision_0018_is_refused_before_anything_is_written() {
    let why = refused(
        "contracts/examples/v1/valid/scenario-permanent-delay.json",
        "unplayed-0018",
    );
    assert!(why.contains("from_occurrence"), "{why}");
    assert!(why.contains("not played"), "{why}");
}

/// `tests/scenarios/audit/F3-permanent.json` (`F3-PERMANENT`) is the
/// example of the contracts as it stands; it stays refused, on both
/// stores, until the store adapter repeats a fault (lot L2).
#[test]
fn the_permanent_delay_of_the_audit_is_the_example_and_is_refused_until_faults_repeat() {
    const AUDIT: &str = "tests/scenarios/audit/F3-permanent.json";
    let read = |relative: &str| -> serde_json::Value {
        serde_json::from_slice(&std::fs::read(bench_support::root().join(relative)).unwrap())
            .unwrap()
    };
    let audit = read(AUDIT);
    assert_eq!(
        audit,
        read("contracts/examples/v1/valid/scenario-permanent-delay.json")
    );
    assert_eq!(audit["id"], "F3-PERMANENT");
    for backend in ["sim-store", "sqlite"] {
        let why = refused_on(AUDIT, &format!("unplayed-permanent-{backend}"), backend);
        assert!(why.contains("from_occurrence"), "{backend}: {why}");
        assert!(why.contains("not played"), "{backend}: {why}");
    }
}
