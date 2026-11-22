//! Extended-recipe campaigns: wide-mask retained admission (recipe kind 20,
//! 17 targets > legacy 16-bit mask) and annotated MQTT 5 PUBLISH admission
//! (recipe kind 23, all five application properties), played across crash and
//! restart on both `sim-store` and `sqlite` and judged by the independent
//! Python oracle.

#[path = "bench_support.rs"]
mod bench_support;

use std::process::Command;

use bench_support::{judge, not_passing, records, root, run_scenario_on};
use serde_json::Value;

fn scope_unmet(campaign: &bench_support::Campaign, scenario: &str) -> Vec<String> {
    let checkout = root().canonicalize().expect("checkout root");
    let script = r#"
import json, sys
from mqttiumd_oracle import documents, results as ores
contracts = documents.Contracts.open(sys.argv[1], "contracts/v1")
print(json.dumps(ores._scope_unmet(contracts, sys.argv[2], "trace.jsonl")))
"#;
    let out = Command::new("python3")
        .current_dir(&checkout)
        .env("PYTHONPATH", checkout.join("tests/oracle"))
        .args(["-c", script])
        .arg(&campaign.out)
        .arg(scenario)
        .output()
        .expect("python3 scope check");
    assert!(
        out.status.success(),
        "scope check failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).expect("json list")
}

fn recipe_tags(campaign: &bench_support::Campaign, checkpoint: &str) -> Vec<u8> {
    let snapshot: Value =
        serde_json::from_slice(&campaign.file(&format!("snapshots/{checkpoint}.snapshot.json")))
            .expect("snapshot JSON");
    snapshot["operations"]
        .as_array()
        .expect("operations array")
        .iter()
        .filter_map(|op| {
            let hex = op["recipe_hex"].as_str()?;
            // Canonical recipe envelope: 4-byte magic ("MQR\0"), 2-byte format version, 1-byte kind tag.
            u8::from_str_radix(hex.get(12..14)?, 16).ok()
        })
        .collect()
}

#[test]
fn wide_mask_retained_campaign_commits_recipe_20_and_passes_oracle_on_both_stores() {
    for backend in ["sim-store", "sqlite"] {
        let scenario = "tests/scenarios/p1/WIDE-A.json";
        let campaign = run_scenario_on(scenario, 1, &format!("wide-a-{backend}"), backend);
        assert_eq!(campaign.report.stopped, None, "{backend}");
        assert!(campaign.report.not_reached.is_empty(), "{backend}");

        let trace = records(&campaign.trace());
        let admissions = trace
            .iter()
            .filter(|r| {
                r.kind == "CommandSubmitted" && r.payload["command_kind"] == "RetainedPublication"
            })
            .count();
        assert_eq!(
            admissions, 1,
            "{backend}: expected one RetainedPublication admission"
        );

        for checkpoint in ["after_commit_before_crash", "final"] {
            campaign.cites_snapshot(checkpoint);
            let tags = recipe_tags(&campaign, checkpoint);
            assert!(
                tags.contains(&20),
                "{backend}/{checkpoint}: expected recipe tag 20 (WideRetainedPublication), got {tags:?}"
            );
            let snapshot: Value = serde_json::from_slice(
                &campaign.file(&format!("snapshots/{checkpoint}.snapshot.json")),
            )
            .unwrap();
            assert_eq!(
                snapshot["obligations"].as_array().unwrap().len(),
                17,
                "{backend}/{checkpoint}: expected 17 wide targets"
            );
        }

        let verdict = judge(&campaign, scenario);
        assert_eq!(
            verdict["verdict"],
            "PASS",
            "{backend}: oracle verdict was not PASS: {:?}",
            not_passing(&verdict)
        );
        let unmet = scope_unmet(&campaign, scenario);
        assert!(
            unmet.iter().any(|s| s.contains("retained")),
            "{backend}: expected retained scope refusal, got {unmet:?}"
        );
    }
}

#[test]
fn annotated_publish_campaign_commits_recipe_23_and_passes_oracle_on_both_stores() {
    for backend in ["sim-store", "sqlite"] {
        let scenario = "tests/scenarios/p1/ANNOTATED-A.json";
        let campaign = run_scenario_on(scenario, 1, &format!("annotated-a-{backend}"), backend);
        assert_eq!(campaign.report.stopped, None, "{backend}");
        assert!(campaign.report.not_reached.is_empty(), "{backend}");

        let trace = records(&campaign.trace());
        let admissions = trace
            .iter()
            .filter(|r| {
                r.kind == "CommandSubmitted" && r.payload["command_kind"] == "AnnotatedPublication"
            })
            .count();
        assert_eq!(
            admissions, 1,
            "{backend}: expected one AnnotatedPublication admission"
        );

        for checkpoint in ["after_commit_before_crash", "final"] {
            campaign.cites_snapshot(checkpoint);
            let tags = recipe_tags(&campaign, checkpoint);
            assert!(
                tags.contains(&23),
                "{backend}/{checkpoint}: expected recipe tag 23 (AnnotatedPublication), got {tags:?}"
            );
        }

        let verdict = judge(&campaign, scenario);
        assert_eq!(
            verdict["verdict"],
            "PASS",
            "{backend}: oracle verdict was not PASS: {:?}",
            not_passing(&verdict)
        );
        let unmet = scope_unmet(&campaign, scenario);
        assert!(
            unmet
                .iter()
                .any(|s| s.contains("PUBLISH application properties")),
            "{backend}: expected properties scope refusal, got {unmet:?}"
        );
    }
}
