//! The SQLite store of lot L7 behind the runner: the same scenario, the
//! same seed, the same evidence as on the deterministic store, but for the
//! name of the backend; and the settings the engine read back, with the
//! way the inspector reads, stated in the campaign.

#[path = "bench_support.rs"]
mod bench_support;

use bench_support::run_scenario_on;

const SCENARIOS: [&str; 6] = [
    "tests/scenarios/p1/P1-02.json",
    "tests/scenarios/p1/P1-02b.json",
    "tests/scenarios/qabs/QABS-A.json",
    "tests/scenarios/qabs/QABS-B.json",
    "tests/scenarios/qabs/QABS-C.json",
    "tests/scenarios/audit/F3-steady-lag.json",
];

/// The bytes of a file of the campaign with the name of its backend
/// replaced by the other one's.
fn named(bytes: &[u8], from: &str, to: &str) -> String {
    String::from_utf8(bytes.to_vec())
        .unwrap()
        .replace(
            &format!("\"backend\":\"{from}\""),
            &format!("\"backend\":\"{to}\""),
        )
        .replace(
            &format!("\"backend\": \"{from}\""),
            &format!("\"backend\": \"{to}\""),
        )
}

/// The records of a trace, with the name of the backend and the digest
/// of each snapshot left out: the snapshots name their backend, and they
/// are compared one by one.
fn evidence(trace: &[u8]) -> Vec<serde_json::Value> {
    trace
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| {
            let mut record: serde_json::Value = serde_json::from_slice(line).unwrap();
            let payload = &mut record["payload"];
            if let Some(payload) = payload.as_object_mut() {
                payload.remove("backend");
                if let Some(snapshot) = payload.get_mut("snapshot").and_then(|s| s.as_object_mut())
                {
                    snapshot.remove("sha256");
                    snapshot.remove("bytes");
                }
            }
            record
        })
        .collect()
}

#[test]
fn the_six_campaigns_give_the_same_evidence_on_sqlite_but_for_the_backend() {
    for scenario in SCENARIOS {
        let memory = run_scenario_on(scenario, 1, "same-memory", "sim-store");
        let sqlite = run_scenario_on(scenario, 1, "same-sqlite", "sqlite");
        assert_eq!(sqlite.report.stopped, None, "{scenario}");
        assert!(sqlite.report.not_reached.is_empty(), "{scenario}");
        assert_eq!(
            evidence(&sqlite.trace()),
            evidence(&memory.trace()),
            "{scenario}: the trace"
        );
        let snapshots = std::fs::read_dir(memory.out.join("snapshots")).unwrap();
        let mut count = 0;
        for entry in snapshots {
            let name = entry.unwrap().file_name().into_string().unwrap();
            let relative = format!("snapshots/{name}");
            assert_eq!(
                named(&sqlite.file(&relative), "sqlite", "sim-store"),
                named(&memory.file(&relative), "sqlite", "sim-store"),
                "{scenario}: {relative}"
            );
            count += 1;
        }
        assert!(count > 0, "{scenario}: no snapshot");
        assert!(
            sqlite.out.join("store/mqttiumd.sqlite").is_file(),
            "{scenario}"
        );
    }
}

#[test]
fn the_same_seed_gives_the_same_bytes_on_sqlite() {
    let first = run_scenario_on(SCENARIOS[1], 1, "seed-a", "sqlite");
    let second = run_scenario_on(SCENARIOS[1], 1, "seed-b", "sqlite");
    assert_eq!(first.trace(), second.trace());
}

#[test]
fn the_campaign_states_the_settings_read_back_and_the_inspector() {
    let campaign = run_scenario_on(SCENARIOS[0], 1, "settings", "sqlite");
    let description: serde_json::Value =
        serde_json::from_slice(&campaign.file("campaign.json")).unwrap();
    let settings = &description["backend_settings"];
    assert_eq!(settings["engine"], "sqlite");
    assert_eq!(settings["journal_mode"], "wal");
    assert_eq!(settings["synchronous"], 2);
    assert!(settings["sqlite_version"].is_string());
    let inspector = settings["inspector"].as_str().unwrap_or_default();
    assert!(
        inspector.starts_with("SqliteStore::inspect"),
        "the inspector reads without the writer: {settings}"
    );
}
