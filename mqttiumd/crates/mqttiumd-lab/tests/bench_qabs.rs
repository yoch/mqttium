//! QABS-A, B and C through the components (DEM-018, record 0015): a
//! commit against a fence, a fence against a held command, and a fence
//! that cannot be written while the command stays held.

#[path = "bench_support.rs"]
mod bench_support;

use bench_support::{Record, records, run_scenario};

fn played(letter: &str, checkpoint: &str) -> Vec<Record> {
    let campaign = run_scenario(
        &format!("tests/scenarios/qabs/QABS-{letter}.json"),
        1,
        &format!("qabs-{letter}"),
    );
    assert_eq!(campaign.report.stopped, None, "QABS-{letter}");
    assert!(campaign.report.not_reached.is_empty(), "QABS-{letter}");
    campaign.cites_snapshot(checkpoint);
    records(&campaign.trace())
}

fn count(trace: &[Record], kind: &str) -> usize {
    trace.iter().filter(|record| record.kind == kind).count()
}

fn outcomes(trace: &[Record], kind: &str) -> Vec<String> {
    trace
        .iter()
        .filter(|record| record.kind == kind)
        .map(|record| record.payload["outcome"].as_str().unwrap().to_owned())
        .collect()
}

#[test]
fn qabs_a_the_command_commits_before_the_held_fence_enters_the_writer() {
    let trace = played("A", "final");
    assert_eq!(count(&trace, "StoreFenceStable"), 0);
    assert_eq!(count(&trace, "StoreCommitStable"), 2, "admission, release");
    // The held fence reads again in the writer and finds the commit.
    assert_eq!(
        outcomes(&trace, "StoreResultDelivered"),
        ["Committed", "Committed", "Committed"]
    );
    assert!(trace.iter().any(|record| {
        record.kind == "PeerPacketObserved"
            && record.payload["packet_kind"] == "PUBREC"
            && record.payload["reason_code"] == 0
    }));
}

#[test]
fn qabs_b_the_fence_commits_while_the_command_is_held_and_the_command_is_refused() {
    let trace = played("B", "final");
    assert_eq!(count(&trace, "StoreFenceStable"), 1);
    assert_eq!(
        count(&trace, "StoreCommitStable"),
        0,
        "the admission never executes"
    );
    let pubrec = trace
        .iter()
        .find(|record| {
            record.kind == "PeerPacketObserved" && record.payload["packet_kind"] == "PUBREC"
        })
        .expect("a refusing PUBREC");
    assert_eq!(pubrec.payload["reason_code"], 0x83);
    // The released command answers a certain rejection, held at the
    // barrier: the core has not consumed it when the run ends.
    assert_eq!(
        outcomes(&trace, "StoreResultReady"),
        ["RejectedCertain", "RejectedCertain"]
    );
    assert_eq!(
        outcomes(&trace, "StoreResultDelivered"),
        ["RejectedCertain"]
    );
}

#[test]
fn qabs_c_the_fence_is_refused_and_the_command_stays_held() {
    let trace = played("C", "unknown");
    assert_eq!(count(&trace, "StoreFenceRefused"), 1);
    assert_eq!(count(&trace, "StoreFenceStable"), 0);
    assert_eq!(count(&trace, "StoreCommitStable"), 0);
    assert_eq!(
        count(&trace, "HeldItemReleased"),
        0,
        "the command stays held"
    );
    assert_eq!(outcomes(&trace, "StoreResultReady"), ["PendingOrUnknown"]);
    assert!(
        trace
            .iter()
            .all(|record| record.payload["packet_kind"] != "PUBREC"),
        "no acknowledgement while the outcome is unknown"
    );
}

/// QABS-A with its steps changed: written below the root, in the
/// ignored build directory, so that the campaign copies it like any
/// scenario of the checkout.
fn variant(name: &str, change: impl FnOnce(&mut Vec<serde_json::Value>)) -> String {
    let root = bench_support::root();
    let mut scenario: serde_json::Value = serde_json::from_slice(
        &std::fs::read(root.join("tests/scenarios/qabs/QABS-A.json")).unwrap(),
    )
    .unwrap();
    change(scenario["steps"].as_array_mut().unwrap());
    let relative = format!("target/bench-scenarios/{name}-{}.json", std::process::id());
    let path = root.join(&relative);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, serde_json::to_vec_pretty(&scenario).unwrap()).unwrap();
    relative
}

/// The run is not played to its end: refused before it starts, or
/// stopped at the step.
fn not_played(scenario: &str, name: &str) -> String {
    let out = bench_support::fresh(name);
    let request = mqttiumd_lab::bench::campaign::Request {
        root: bench_support::root(),
        contracts: std::path::PathBuf::from("contracts/v1"),
        scenario: std::path::PathBuf::from(scenario),
        out: out.clone(),
        seed: 1,
        run_id: Some("bench-test".to_owned()),
        backend: "sim-store".to_owned(),
        built_from: None,
        process: "in-process".to_owned(),
    };
    let outcome = match mqttiumd_lab::bench::campaign::execute(&request) {
        Err(refused) => refused,
        Ok(report) => report
            .stopped
            .expect("the run must not be played to its end"),
    };
    let _ = std::fs::remove_dir_all(&out);
    let _ = std::fs::remove_file(bench_support::root().join(scenario));
    outcome
}

#[test]
fn leaving_a_barrier_twice_is_refused() {
    let scenario = variant("leave-twice", |steps| {
        let index = steps
            .iter()
            .position(|step| step["kind"] == "LeaveBarrier")
            .unwrap();
        let again = steps[index].clone();
        steps.insert(index + 1, again);
    });
    let why = not_played(&scenario, "leave-twice");
    assert!(why.contains("admission_applied"), "{why}");
}

#[test]
fn leaving_a_fault_that_is_not_a_barrier_is_refused() {
    let scenario = variant("leave-hold", |steps| {
        let index = steps
            .iter()
            .position(|step| step["kind"] == "LeaveBarrier")
            .unwrap();
        steps[index]["fault"] = serde_json::json!("held_fence");
    });
    let why = not_played(&scenario, "leave-hold");
    assert!(why.contains("held_fence"), "{why}");
}

#[test]
fn qabs_a_inspects_at_the_barrier_before_any_result_is_ready() {
    let campaign =
        bench_support::run_scenario("tests/scenarios/qabs/QABS-A.json", 1, "qabs-a-barrier");
    let trace = records(&campaign.trace());
    let at = |kind: &str| trace.iter().position(|record| record.kind == kind).unwrap();
    let barrier = trace
        .iter()
        .position(|record| {
            record.kind == "FaultHit" && record.payload["fault_id"] == "admission_applied"
        })
        .unwrap();
    let inspected = at("DurableStateInspected");
    let ready = at("StoreResultReady");
    assert!(
        barrier < inspected && inspected < ready,
        "{barrier} {inspected} {ready}"
    );
}

/// The trace of a variant of QABS-A that stops before its end.
fn stopped_trace(scenario: &str, name: &str) -> (String, Vec<serde_json::Value>) {
    let campaign = bench_support::run_scenario(scenario, 1, name);
    let _ = std::fs::remove_file(bench_support::root().join(scenario));
    let stopped = campaign
        .report
        .stopped
        .clone()
        .expect("the run must stop before its end");
    let lines = campaign
        .trace()
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect();
    (stopped, lines)
}

#[test]
fn a_wait_that_nothing_eligible_can_meet_is_a_step_blocked_and_the_trace_stays_complete() {
    // Without the release of the held admission, nothing reaches the
    // barrier: the wait of step 5 cannot be met.
    let scenario = variant("blocked-quiescent", |steps| {
        let index = steps
            .iter()
            .position(|step| step["kind"] == "ReleaseHeldCommand")
            .unwrap();
        steps.remove(index);
    });
    let (stopped, trace) = stopped_trace(&scenario, "blocked-quiescent");
    assert!(stopped.starts_with("step 5 (DriveUntil)"), "{stopped}");
    let [.., blocked, footer] = trace.as_slice() else {
        panic!("a short trace");
    };
    assert_eq!(blocked["kind"], "StepBlocked");
    assert_eq!(blocked["origin_component"], "bench-supervisor");
    assert_eq!(
        blocked["payload"],
        serde_json::json!({
            "step": "5",
            "step_kind": "DriveUntil",
            "cause": "quiescent",
            // The deadline the resolution armed again, not fired.
            "armed_deadlines": "1",
            "holding_faults": ["held_admission", "held_fence"],
        })
    );
    assert_eq!(footer["kind"], "TraceFooter");
    assert_eq!(footer["payload"]["complete"], true);
}

#[test]
fn a_wait_that_runs_out_of_steps_is_a_step_blocked_by_its_budget() {
    let scenario = variant("blocked-budget", |steps| {
        let last = steps
            .iter()
            .rposition(|step| step["kind"] == "DriveUntil")
            .unwrap();
        steps[last]["max_steps"] = serde_json::json!("1");
    });
    let (stopped, trace) = stopped_trace(&scenario, "blocked-budget");
    assert!(stopped.contains("max_steps"), "{stopped}");
    let [.., blocked, footer] = trace.as_slice() else {
        panic!("a short trace");
    };
    assert_eq!(blocked["kind"], "StepBlocked");
    assert_eq!(blocked["payload"]["cause"], "budget");
    assert_eq!(blocked["payload"]["step_kind"], "DriveUntil");
    assert_eq!(footer["payload"]["complete"], true);
}

#[test]
fn a_stop_that_is_no_wait_writes_no_step_blocked_and_cuts_the_trace() {
    // A deadline fired before anything armed one.
    let scenario = variant("stop-no-wait", |steps| {
        steps.insert(
            1,
            serde_json::json!({"kind": "FireDeadline", "max_steps": "512"}),
        );
    });
    let (stopped, trace) = stopped_trace(&scenario, "stop-no-wait");
    assert!(stopped.contains("no deadline armed"), "{stopped}");
    assert!(trace.iter().all(|record| record["kind"] != "StepBlocked"));
    assert_eq!(trace.last().unwrap()["payload"]["complete"], false);
}
