//! What the tests of the runner share: run a scenario of the checkout
//! into a fresh campaign directory, and read what it wrote.
#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

use mqttiumd_lab::bench::campaign::{Report, Request, execute};
use serde_json::Value;

/// The `mqttiumd` directory of the checkout.
pub fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

static NEXT: AtomicU64 = AtomicU64::new(0);

/// A campaign directory, removed when the test ends.
pub struct Campaign {
    pub out: PathBuf,
    pub report: Report,
}

impl Drop for Campaign {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.out);
    }
}

impl Campaign {
    pub fn file(&self, relative: &str) -> Vec<u8> {
        fs::read(self.out.join(relative)).unwrap_or_else(|error| panic!("{relative}: {error}"))
    }

    pub fn trace(&self) -> Vec<u8> {
        self.file("trace.jsonl")
    }

    /// The trace cites the snapshot of `checkpoint` by its exact digest.
    pub fn cites_snapshot(&self, checkpoint: &str) {
        let path = format!("snapshots/{checkpoint}.snapshot.json");
        let digest = mqttiumd_lab::manifest::sha256_hex(&self.file(&path));
        let cited = records(&self.trace()).into_iter().any(|record| {
            record.kind == "DurableStateInspected"
                && record.payload["checkpoint"] == checkpoint
                && record.payload["snapshot"]["path"] == path.as_str()
                && record.payload["snapshot"]["sha256"] == digest.as_str()
        });
        assert!(cited, "{checkpoint} is not cited by its digest");
    }
}

/// A fresh directory for one campaign.
pub fn fresh(name: &str) -> PathBuf {
    let number = NEXT.fetch_add(1, Ordering::Relaxed);
    let out = std::env::temp_dir().join(format!(
        "mqttiumd-campaign-{name}-{}-{number}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&out);
    out
}

/// Runs a scenario of the checkout with `seed`.
pub fn run_scenario(scenario: &str, seed: u64, name: &str) -> Campaign {
    run_scenario_on(scenario, seed, name, "sim-store")
}

/// Runs a scenario of the checkout with `seed` on `backend`.
pub fn run_scenario_on(scenario: &str, seed: u64, name: &str, backend: &str) -> Campaign {
    let out = fresh(name);
    let request = Request {
        root: root(),
        contracts: PathBuf::from("contracts/v1"),
        scenario: PathBuf::from(scenario),
        out: out.clone(),
        seed,
        run_id: Some("bench-test".to_owned()),
        backend: backend.to_owned(),
        built_from: None,
        process: "in-process".to_owned(),
    };
    let report = execute(&request).unwrap_or_else(|error| panic!("{scenario}: {error}"));
    Campaign { out, report }
}

/// One record of a trace, as the tests read it.
pub struct Record {
    pub kind: String,
    pub origin: String,
    pub payload: Value,
}

pub fn records(trace: &[u8]) -> Vec<Record> {
    trace
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| {
            let value: Value = serde_json::from_slice(line).expect("a JSON line");
            Record {
                kind: value["kind"].as_str().unwrap().to_owned(),
                origin: value["origin_component"].as_str().unwrap().to_owned(),
                payload: value["payload"].clone(),
            }
        })
        .collect()
}

/// The verdict of the oracle on a campaign of `scenario`.
pub fn judge(campaign: &Campaign, scenario: &str) -> Value {
    let root = root().canonicalize().expect("the checkout");
    let result = Command::new("python3")
        .current_dir(&root)
        .env("PYTHONPATH", root.join("tests/oracle"))
        .args(["-m", "mqttiumd_oracle", "--root"])
        .arg(&campaign.out)
        .args([
            "--contracts",
            "contracts/v1",
            "judge",
            "--scenario",
            scenario,
        ])
        .args(["--trace", "trace.jsonl"])
        .output()
        .expect("python3 runs the oracle");
    assert!(
        result.status.code().is_some_and(|code| code < 4),
        "the oracle could not judge {scenario}: {}\n{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    serde_json::from_slice(&result.stdout).expect("a verdict")
}

pub fn not_passing(verdict: &Value) -> Vec<String> {
    verdict["assertions"]
        .as_object()
        .expect("the assertions")
        .iter()
        .filter(|(_, assertion)| assertion["status"] != "PASS")
        .map(|(name, assertion)| {
            format!("{name} {}: {}", assertion["status"], assertion["reasons"])
        })
        .collect()
}
