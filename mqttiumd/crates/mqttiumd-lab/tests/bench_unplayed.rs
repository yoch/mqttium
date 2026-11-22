//! What the bench does not play yet is refused, explicitly and before
//! anything is written: the repeated faults of decision 0018
//! (`from_occurrence`, `expected_hits_at_least`, `lag`) and the deadlines a
//! `DriveUntil` fires itself (`fire_deadlines`). A scenario the contracts
//! accept is never played by ignoring a member.

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

#[test]
fn a_wait_that_fires_its_own_deadlines_is_refused_before_anything_is_written() {
    let root = bench_support::root();
    let mut scenario: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("tests/scenarios/p1/P1-02.json")).unwrap())
            .unwrap();
    let wait = scenario["steps"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|step| step["kind"] == "DriveUntil")
        .unwrap();
    wait["fire_deadlines"] = serde_json::json!(true);
    let relative = format!(
        "target/bench-scenarios/fire-deadlines-{}.json",
        std::process::id()
    );
    std::fs::create_dir_all(root.join("target/bench-scenarios")).unwrap();
    std::fs::write(
        root.join(&relative),
        serde_json::to_vec_pretty(&scenario).unwrap(),
    )
    .unwrap();
    let why = refused(&relative, "unplayed-fire");
    let _ = std::fs::remove_file(root.join(&relative));
    assert!(why.contains("fire_deadlines"), "{why}");
}
