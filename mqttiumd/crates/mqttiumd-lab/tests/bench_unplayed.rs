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
    let out = bench_support::fresh(name);
    let request = Request {
        root: bench_support::root(),
        contracts: PathBuf::from("contracts/v1"),
        scenario: PathBuf::from(scenario),
        out: out.clone(),
        seed: 1,
        run_id: Some("bench-test".to_owned()),
        backend: "sim-store".to_owned(),
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
