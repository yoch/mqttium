//! The runner of lot L4 plays P1-02 through the real core and the
//! deterministic store, and leaves a campaign directory for the oracle.
//!
//! These tests read what the runner wrote; they judge nothing. The
//! verdict is the oracle's (`tools/run_campaign.sh`, and the `campaign`
//! step of the workflow).

#[path = "bench_support.rs"]
mod bench_support;

use bench_support::{Campaign, records, run_scenario};

#[test]
fn p1_02_is_played_to_its_last_step_through_the_components() {
    let campaign = run_scenario("tests/scenarios/p1/P1-02.json", 1, "p102-played");
    assert_eq!(campaign.report.stopped, None);
    assert!(campaign.report.not_reached.is_empty());
    let trace = records(&campaign.trace());
    let kinds: Vec<&str> = trace.iter().map(|record| record.kind.as_str()).collect();
    assert_eq!(kinds.first(), Some(&"TraceHeader"));
    assert_eq!(kinds.last(), Some(&"TraceFooter"));
    assert_eq!(
        trace.last().unwrap().payload["complete"],
        serde_json::json!(true)
    );
    // One admission, its result lost, a crash, a recovery, no second
    // admission, and the exchange completed on the new connection.
    let admissions = trace
        .iter()
        .filter(|record| {
            record.kind == "CommandSubmitted"
                && record.payload["command_kind"] == "AdmitPublication"
        })
        .count();
    assert_eq!(admissions, 1);
    let position = |kind: &str| kinds.iter().position(|found| *found == kind).unwrap();
    let crash = position("BrokerCrashed");
    assert!(position("FaultHit") < crash);
    let pubrec = trace
        .iter()
        .position(|record| {
            record.kind == "PeerPacketObserved" && record.payload["packet_kind"] == "PUBREC"
        })
        .expect("a PUBREC observed");
    assert!(pubrec > crash, "no PUBREC before the crash");
    assert!(trace.iter().any(|record| {
        record.kind == "PeerPacketObserved" && record.payload["packet_kind"] == "PUBCOMP"
    }));
    // The second process runs a new epoch, in a new incarnation.
    let recovered = trace
        .iter()
        .find(|record| record.kind == "BrokerRecovered")
        .unwrap();
    assert_eq!(recovered.payload["run_epoch"], "2");
    // Both checkpoints were written and cited.
    for checkpoint in ["after_commit_before_crash", "final"] {
        campaign.cites_snapshot(checkpoint);
    }
}

#[test]
fn the_same_seed_gives_the_same_trace_byte_for_byte() {
    let first = run_scenario("tests/scenarios/p1/P1-02.json", 7, "p102-same");
    let second = run_scenario("tests/scenarios/p1/P1-02.json", 7, "p102-same");
    assert_eq!(first.trace(), second.trace());
    assert_eq!(first.file("choices.log"), second.file("choices.log"));
    for checkpoint in ["after_commit_before_crash", "final"] {
        let path = format!("snapshots/{checkpoint}.snapshot.json");
        assert_eq!(first.file(&path), second.file(&path));
    }
}

#[test]
fn the_runner_writes_no_verdict() {
    let campaign = run_scenario("tests/scenarios/p1/P1-02.json", 1, "p102-verdict");
    let result: serde_json::Value =
        serde_json::from_slice(&campaign.file("run-result.json")).unwrap();
    assert_eq!(result["qualification"], "provisional");
    assert_eq!(result["verdict"], "INCONCLUSIVE");
    for assertion in result["assertions"].as_array().unwrap() {
        assert_eq!(assertion["status"], "INCONCLUSIVE");
    }
}

#[test]
fn the_manifest_names_the_parameters_and_every_artefact_is_inventoried() {
    let campaign = run_scenario("tests/scenarios/p1/P1-02.json", 3, "p102-manifest");
    let manifest: serde_json::Value =
        serde_json::from_slice(&campaign.file("run-manifest.json")).unwrap();
    assert_eq!(manifest["backend"], "sim-store");
    assert_eq!(manifest["failure_model"], "process");
    assert_eq!(manifest["scheduler"]["seed"], "3");
    assert_eq!(
        manifest["scenario"]["path"],
        "tests/scenarios/p1/P1-02.json"
    );
    assert_eq!(
        manifest["fixture"]["path"],
        "tests/fixtures/p1/three-persistent-sessions.json"
    );
    let inventory: serde_json::Value =
        serde_json::from_slice(&campaign.file("campaign.json")).unwrap();
    for entry in inventory["artifacts"].as_array().unwrap() {
        let path = entry["path"].as_str().unwrap();
        let digest = mqttiumd_lab::manifest::sha256_hex(&campaign.file(path));
        assert_eq!(entry["sha256"], digest.as_str(), "{path}");
    }
    // The oracle and the generators are identified apart from the lab
    // binary, in the manifest itself, each file by its digest.
    for part in ["oracle", "generators"] {
        let files = manifest[part]["files"].as_array().unwrap();
        assert!(!files.is_empty(), "{part}");
        let mut lines = String::new();
        for file in files {
            let path = file["path"].as_str().unwrap();
            let bytes = std::fs::read(bench_support::root().join(path)).unwrap();
            let digest = mqttiumd_lab::manifest::sha256_hex(&bytes);
            assert_eq!(file["sha256"], digest.as_str(), "{path}");
            assert_eq!(file["bytes"], bytes.len().to_string().as_str(), "{path}");
            lines.push_str(&format!("{digest}  {path}\n"));
        }
        assert_eq!(
            manifest[part]["sha256"],
            mqttiumd_lab::manifest::sha256_hex(lines.as_bytes()).as_str(),
            "{part}"
        );
        assert_eq!(inventory[part], manifest[part], "{part}");
    }
    assert!(inventory["oracle"]["sha256"].is_string());
    assert!(inventory["generators"]["sha256"].is_string());
    assert!(inventory["lab"]["sha256"].is_string());
    let _: &Campaign = &campaign;
}
