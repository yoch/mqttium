//! A command parked inside SQLite stays in flight until SIGKILL.
#[path = "bench_support.rs"]
mod bench_support;

use mqttiumd_lab::bench::scenario::Scenario;
use serde_json::{Value, json};

#[test]
fn the_scenario_reader_preserves_the_transaction_points() {
    for point in ["store.commit.prepared", "store.commit.logged"] {
        let mut document: Value = serde_json::from_slice(
            &std::fs::read(bench_support::root().join("tests/scenarios/p1/P1-02.json")).unwrap(),
        )
        .unwrap();
        document["faults"][0]["point"] = json!(point);
        document["faults"][0]["action"] = json!("barrier");
        let scenario = Scenario::read(&document).expect("a declared transaction point");
        assert_eq!(scenario.faults[0].point.name(), point);
    }
}

#[test]
fn transaction_points_are_refused_before_writing_without_the_child_and_feature() {
    use mqttiumd_lab::bench::campaign::{Request, execute};
    for process in ["in-process", "child"] {
        if process == "child" && cfg!(feature = "kill-points") {
            continue;
        }
        let out = bench_support::fresh("transaction-refused");
        let request = Request {
            root: bench_support::root(),
            contracts: "contracts/v1".into(),
            scenario: "tests/scenarios/crash/K1.json".into(),
            out: out.clone(),
            seed: 1,
            run_id: None,
            backend: "sqlite".to_owned(),
            built_from: None,
            process: process.to_owned(),
        };
        let why = execute(&request).expect_err("an interior hook needs its real child build");
        assert!(why.contains("transaction kill points require"), "{why}");
        assert!(!out.exists(), "refusal must precede campaign writes");
    }
}

#[cfg(feature = "kill-points")]
#[test]
fn acknowledged_transaction_points_kill_and_recover_without_finishing_the_command() {
    for (scenario, point) in [
        ("K1", "store.commit.prepared"),
        ("K2b", "store.commit.logged"),
        ("K3", "store.commit.applied"),
    ] {
        let out = bench_support::fresh(&format!("inflight-{scenario}"));
        let status = std::process::Command::new(env!("CARGO_BIN_EXE_mqttiumd-lab"))
            .args([
                "run",
                "--contracts",
                "contracts/v1",
                "--backend",
                "sqlite",
                "--process",
                "child",
            ])
            .arg("--scenario")
            .arg(format!("tests/scenarios/crash/{scenario}.json"))
            .arg("--out")
            .arg(&out)
            .current_dir(bench_support::root())
            .output()
            .unwrap();
        assert!(
            status.status.success(),
            "{scenario}: {}",
            String::from_utf8_lossy(&status.stderr)
        );
        let campaign: Value =
            serde_json::from_slice(&std::fs::read(out.join("campaign.json")).unwrap()).unwrap();
        assert!(
            campaign["stopped"].is_null(),
            "{scenario}: {}",
            campaign["stopped"]
        );
        assert_eq!(
            campaign["kills"][0]["wait_status"]["signal"], 9,
            "{scenario}"
        );
        assert_eq!(campaign["build_features"], json!(["kill-points"]));
        assert_eq!(campaign["backend_settings"]["kill_points"], true);
        assert!(
            campaign["commands"]["replay"]
                .as_str()
                .unwrap()
                .contains("--features kill-points")
        );
        let trace: Vec<Value> = std::fs::read_to_string(out.join("trace.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let hit = trace
            .iter()
            .position(|record| record["kind"] == "FaultHit" && record["payload"]["point"] == point)
            .unwrap();
        let killed = trace
            .iter()
            .position(|record| record["kind"] == "BrokerKilled")
            .unwrap();
        assert!(hit < killed);
        assert!(
            trace.last().unwrap()["payload"]["complete"]
                .as_bool()
                .unwrap()
        );
        if scenario != "K3" {
            let transaction = &campaign["kills"][0]["transaction"];
            assert_eq!(transaction["point"], point);
            assert_eq!(transaction["evidence_retained_before_ack"], true);
            assert_eq!(transaction["backend_settings"]["wal_autocheckpoint"], 0);
            assert_eq!(
                transaction["backend_settings"]["checkpoint_policy"],
                "disabled-while-kill-hook-armed"
            );
            let operation = &trace[hit]["payload"]["operation_id"];
            assert!(
                !trace[..killed]
                    .iter()
                    .any(|record| record["kind"] == "StoreCommitStable"
                        && &record["payload"]["operation_id"] == operation)
            );
        } else {
            // Different origins are unordered unless the writer states
            // the actual dependency. The applied point is reached only
            // after this command's successful COMMIT was recorded.
            let operation = &trace[hit]["payload"]["operation_id"];
            let stable = trace[..hit]
                .iter()
                .find(|record| {
                    record["kind"] == "StoreCommitStable"
                        && &record["payload"]["operation_id"] == operation
                })
                .expect("K3 recorded this command's commit");
            let cause = json!({
                "origin_component": stable["origin_component"],
                "origin_incarnation": stable["origin_incarnation"],
                "origin_sequence": stable["origin_sequence"],
            });
            assert!(
                trace[hit]["causal_parents"]
                    .as_array()
                    .unwrap()
                    .contains(&cause),
                "K3's FaultHit must follow its own StoreCommitStable, not only share CommandSubmitted as a parent"
            );
        }
        let _ = std::fs::remove_dir_all(&out);
    }
}

/// Run explicitly with --features mqttiumd-store-sqlite/kill-points,
/// without the lab's feature, to exercise Cargo's dependency unification.
#[cfg(not(feature = "kill-points"))]
#[test]
fn dependency_feature_unification_cannot_silently_change_the_rebuilt_binary() {
    use mqttiumd_contract::ids::StoreIncarnation;
    use mqttiumd_lab::bench::campaign::{Request, execute};
    use mqttiumd_lab::store::conformance::fixture_limits;
    let probe = bench_support::fresh("unified-probe").with_extension("sqlite");
    let store = mqttiumd_store_sqlite::SqliteStore::create(
        &probe,
        StoreIncarnation::new(1),
        fixture_limits(),
    )
    .unwrap();
    let instrumented = store.effective_settings().kill_points;
    drop(store);
    for suffix in ["", "-wal", "-shm", ".writer-lock"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", probe.display()));
    }
    if !instrumented {
        return;
    }
    let out = bench_support::fresh("unified-refused");
    let request = Request {
        root: bench_support::root(),
        contracts: "contracts/v1".into(),
        scenario: "tests/scenarios/p1/P1-02.json".into(),
        out: out.clone(),
        seed: 1,
        run_id: None,
        backend: "sqlite".to_owned(),
        built_from: None,
        process: "in-process".to_owned(),
    };
    let why = execute(&request).expect_err("the store carries unrecorded test instrumentation");
    assert!(why.contains("Cargo feature unification"), "{why}");
    assert!(!out.exists(), "the mismatch must be refused before writes");
    let work = bench_support::fresh("unified-rebuild-refused");
    let why = mqttiumd_lab::bench::rebuild::rebuild(&bench_support::root(), &work, &[])
        .expect_err("rebuild cannot silently remove instrumentation either");
    assert!(why.contains("Cargo feature unification"), "{why}");
    assert!(!work.exists(), "refusal must precede source assembly");
}
