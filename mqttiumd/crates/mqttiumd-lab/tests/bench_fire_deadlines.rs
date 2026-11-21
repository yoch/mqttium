//! A `DriveUntil` that fires the deadlines itself (`fire_deadlines`,
//! decision 0018): while the awaited event has not come and nothing else
//! is eligible, the armed deadline due first is fired, within `max_steps`.
//!
//! The scenario is the example `scenario-permanent-delay` of lot L3 without
//! its repeated fault: the result of the first `Persist` is lost, and one
//! wait for the PUBREC fires the deadlines of the resolution.

#[path = "bench_support.rs"]
mod bench_support;

use bench_support::{records, run_scenario_on};

/// Writes the variant below the root, in the ignored build directory.
fn scenario() -> String {
    let root = bench_support::root();
    let mut scenario: serde_json::Value = serde_json::from_slice(
        &std::fs::read(root.join("contracts/examples/v1/valid/scenario-permanent-delay.json"))
            .unwrap(),
    )
    .unwrap();
    scenario["id"] = serde_json::json!("F3-FIRED");
    let faults = scenario["faults"].as_array_mut().unwrap();
    faults.retain(|fault| fault["id"] != "late_answers");
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
    relative
}

#[test]
fn a_wait_that_fires_its_deadlines_reaches_the_pubrec_on_both_stores() {
    let relative = scenario();
    for backend in ["sim-store", "sqlite"] {
        let campaign = run_scenario_on(&relative, 1, &format!("fired-{backend}"), backend);
        assert_eq!(campaign.report.stopped, None, "{backend}");
        assert!(campaign.report.not_reached.is_empty(), "{backend}");
        fires_before_the_pubrec(&campaign.trace(), backend);
    }
    // In child mode, through the binary of the lab: the deadlines are
    // fired in the child, at the request of the supervisor.
    let out = bench_support::fresh("fired-child");
    let played = {
        let _turn = bench_support::spawning();
        std::process::Command::new(env!("CARGO_BIN_EXE_mqttiumd-lab"))
            .args([
                "run",
                "--contracts",
                "contracts/v1",
                "--scenario",
                &relative,
            ])
            .arg("--out")
            .arg(&out)
            .args(["--backend", "sqlite", "--process", "child"])
            .args(["--run-id", "bench-test"])
            .current_dir(bench_support::root())
            .output()
            .expect("the lab runs")
    };
    assert!(
        played.status.success(),
        "{}",
        String::from_utf8_lossy(&played.stderr)
    );
    let campaign: serde_json::Value =
        serde_json::from_slice(&std::fs::read(out.join("campaign.json")).unwrap()).unwrap();
    assert!(campaign["stopped"].is_null(), "{campaign}");
    assert_eq!(campaign["covers"]["process"], "child", "{campaign}");
    fires_before_the_pubrec(&std::fs::read(out.join("trace.jsonl")).unwrap(), "child");
    let _ = std::fs::remove_dir_all(&out);
    let again = run_scenario_on(&relative, 1, "fired-again", "sim-store");
    let first = run_scenario_on(&relative, 1, "fired-first", "sim-store");
    assert_eq!(
        again.trace(),
        first.trace(),
        "the same seed gives the same bytes"
    );
    let _ = std::fs::remove_file(bench_support::root().join(&relative));
}

/// At least one deadline is delivered, and the first before the PUBREC.
fn fires_before_the_pubrec(trace: &[u8], label: &str) {
    let trace = records(trace);
    let first_fired = trace
        .iter()
        .position(|record| record.kind == "DeadlineDelivered")
        .unwrap_or_else(|| panic!("{label}: no deadline was fired"));
    let pubrec = trace.iter().position(|record| {
        record.kind == "PeerPacketObserved"
            && record.payload["packet_kind"] == "PUBREC"
            && record.payload["reason_code"] == 0
    });
    assert!(
        pubrec.is_some_and(|pubrec| first_fired < pubrec),
        "{label}: the PUBREC follows the fired deadline"
    );
}
