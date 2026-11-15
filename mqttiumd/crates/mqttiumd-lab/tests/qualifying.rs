//! A qualifying result is evidence: it names its scenario, manifest, trace
//! and contracts by digest, and covers what its scenario requires.
//!
//! These documents name files by digest and the contracts in use, so they
//! are built here rather than kept as static examples.

use std::fs;
use std::path::Path;

use mqttiumd_lab::manifest::{self, sha256_hex};
use mqttiumd_lab::workspace::Workspace;
use serde_json::{Value, json};

mod support;
use support::{root, scratch, write};

const SCENARIO: &str = "tests/scenarios/p1/P1-02.json";

fn workspace() -> Workspace {
    Workspace::open(&root(), Path::new("contracts/v1")).expect("the contract directory opens")
}

fn cite(path: &str) -> Value {
    let bytes = fs::read(root().join(path)).expect("cited file");
    json!({"path": path, "sha256": sha256_hex(&bytes), "bytes": bytes.len().to_string()})
}

/// A complete qualifying PASS for P1-02 in a fresh directory.
fn passing(name: &str) -> (std::path::PathBuf, String, Value) {
    let (directory, relative) = scratch(name);
    let collected = manifest::collect(&root(), Path::new("contracts/v1")).expect("manifest");
    write(&directory, "manifest.json", &collected);
    fs::write(directory.join("trace.jsonl"), b"{}\n").expect("trace");
    let scenario: Value =
        serde_json::from_slice(&fs::read(root().join(SCENARIO)).expect("P1-02")).expect("JSON");
    // The assertions of the scenario, and the one every run owes that
    // P1-02 does not list.
    let assertions: Vec<Value> = scenario["assertions"]
        .as_array()
        .expect("assertions")
        .iter()
        .cloned()
        .chain([json!("broker_inputs_have_provenance")])
        .map(|name| json!({"name": name, "status": "PASS"}))
        .collect();
    let result = json!({
        "format": "mqttiumd.run-result",
        "schema_version": 1,
        "qualification": "qualifying",
        "run_id": "p1-02-qualifying",
        "verdict": "PASS",
        "exit_code": 0,
        "scope": "P1-02, built by the test suite",
        "reasons": [],
        "assertions": assertions,
        "faults": [{"id": "lost_admission_result", "expected_hits": "1", "hits": "1"}],
        "scenario": cite(SCENARIO),
        "manifest": cite(&format!("{relative}/manifest.json")),
        "trace": cite(&format!("{relative}/trace.jsonl")),
        "contracts_digest": workspace().contracts_digest(),
        "artifacts": [],
    });
    (directory, relative, result)
}

fn codes_of(directory: &Path, relative: &str, result: &Value) -> Vec<&'static str> {
    write(directory, "result.json", result);
    let mut codes: Vec<&'static str> = workspace()
        .check_file(Path::new(&format!("{relative}/result.json")))
        .diagnostics
        .iter()
        .map(|diagnostic| diagnostic.code)
        .collect();
    codes.sort_unstable();
    codes.dedup();
    codes
}

#[test]
fn a_complete_qualifying_pass_is_accepted() {
    let (directory, relative, result) = passing("qualifying-pass");
    assert_eq!(codes_of(&directory, &relative, &result), Vec::<&str>::new());
}

#[test]
fn a_pass_with_empty_lists_is_refused() {
    let (directory, relative, mut result) = passing("qualifying-empty");
    result["assertions"] = json!([]);
    result["faults"] = json!([]);
    assert_eq!(
        codes_of(&directory, &relative, &result),
        vec!["schema-violation"]
    );
}

#[test]
fn a_pass_with_only_out_of_scope_assertions_is_refused() {
    let (directory, relative, mut result) = passing("qualifying-out-of-scope-only");
    for assertion in result["assertions"].as_array_mut().expect("assertions") {
        assertion["status"] = json!("OUT_OF_SCOPE");
    }
    let codes = codes_of(&directory, &relative, &result);
    assert!(codes.contains(&"verdict-inconsistent"), "{codes:?}");
    assert!(codes.contains(&"result-incomplete"), "{codes:?}");
}

/// P1-02 is the text of the specification and lists neither assertion
/// below; every run owes them all the same (ADR 0003, DEM-MU-03).
const EVERY_RUN: [&str; 2] = ["trace_complete", "broker_inputs_have_provenance"];

#[test]
fn an_assertion_every_run_owes_cannot_be_left_out() {
    for name in EVERY_RUN {
        let (directory, relative, mut result) = passing(&format!("every-run-{name}"));
        let assertions = result["assertions"].as_array_mut().expect("assertions");
        let before = assertions.len();
        assertions.retain(|assertion| assertion["name"] != name);
        assert_eq!(
            assertions.len(),
            before - 1,
            "{name} is reported by a passing result"
        );
        assert_eq!(
            codes_of(&directory, &relative, &result),
            vec!["result-incomplete"],
            "{name}"
        );
    }
}

#[test]
fn an_assertion_of_the_scenario_cannot_be_left_out() {
    let (directory, relative, mut result) = passing("qualifying-missing-assertion");
    result["assertions"]
        .as_array_mut()
        .expect("assertions")
        .pop();
    assert_eq!(
        codes_of(&directory, &relative, &result),
        vec!["result-incomplete"]
    );
}

#[test]
fn an_assertion_of_the_scenario_cannot_be_declared_out_of_scope() {
    let (directory, relative, mut result) = passing("qualifying-out-of-scope");
    result["assertions"][0]["status"] = json!("OUT_OF_SCOPE");
    result["verdict"] = json!("OUT_OF_SCOPE");
    result["exit_code"] = json!(3);
    assert_eq!(
        codes_of(&directory, &relative, &result),
        vec!["result-incomplete"]
    );
}

#[test]
fn an_assertion_the_scenario_does_not_list_is_refused() {
    let (directory, relative, mut result) = passing("qualifying-extra-assertion");
    result["assertions"]
        .as_array_mut()
        .expect("assertions")
        .push(json!({"name": "everything_is_fine", "status": "PASS"}));
    assert_eq!(
        codes_of(&directory, &relative, &result),
        vec!["result-incomplete"]
    );
}

#[test]
fn the_faults_are_exactly_those_of_the_scenario() {
    let (directory, relative, mut result) = passing("qualifying-faults");
    result["faults"][0]["expected_hits"] = json!("2");
    result["faults"][0]["hits"] = json!("2");
    assert_eq!(
        codes_of(&directory, &relative, &result),
        vec!["result-incomplete"]
    );
    result["faults"] = json!([]);
    assert_eq!(
        codes_of(&directory, &relative, &result),
        vec!["result-incomplete"]
    );
}

#[test]
fn an_inconclusive_assertion_or_a_missed_injection_blocks_the_pass() {
    let (directory, relative, mut result) = passing("qualifying-inconclusive");
    result["assertions"][0]["status"] = json!("INCONCLUSIVE");
    assert_eq!(
        codes_of(&directory, &relative, &result),
        vec!["verdict-inconsistent"]
    );
    let (directory, relative, mut result) = passing("qualifying-missed-injection");
    result["faults"][0]["hits"] = json!("0");
    assert_eq!(
        codes_of(&directory, &relative, &result),
        vec!["verdict-inconsistent"]
    );
    result["verdict"] = json!("INCONCLUSIVE");
    result["exit_code"] = json!(2);
    assert_eq!(codes_of(&directory, &relative, &result), Vec::<&str>::new());
}

#[test]
fn cited_files_must_have_the_declared_digest_and_size() {
    let (directory, relative, mut result) = passing("qualifying-digests");
    result["scenario"]["sha256"] = json!("0".repeat(64));
    assert_eq!(
        codes_of(&directory, &relative, &result),
        vec!["evidence-mismatch"]
    );

    let (directory, relative, mut result) = passing("qualifying-size");
    result["trace"]["bytes"] = json!("4");
    assert_eq!(
        codes_of(&directory, &relative, &result),
        vec!["evidence-mismatch"]
    );

    let (directory, relative, result) = passing("qualifying-trace-changed");
    fs::write(directory.join("trace.jsonl"), b"{\"changed\": true}\n").expect("trace");
    assert_eq!(
        codes_of(&directory, &relative, &result),
        vec!["evidence-mismatch"]
    );

    let (directory, relative, mut result) = passing("qualifying-missing-file");
    result["trace"]["path"] = json!(format!("{relative}/absent.jsonl"));
    assert_eq!(
        codes_of(&directory, &relative, &result),
        vec!["file-unreadable"]
    );
}

#[test]
fn a_result_is_evaluated_only_against_the_contracts_it_names() {
    let (directory, relative, mut result) = passing("qualifying-contracts");
    result["contracts_digest"] = json!("0".repeat(64));
    assert_eq!(
        codes_of(&directory, &relative, &result),
        vec!["evidence-mismatch"]
    );
}

#[test]
fn the_cited_scenario_must_be_a_valid_scenario() {
    let (directory, relative, mut result) = passing("qualifying-not-a-scenario");
    result["scenario"] = cite("contracts/v1/profiles.json");
    assert_eq!(
        codes_of(&directory, &relative, &result),
        vec!["reference-invalid"]
    );
}

#[test]
fn a_trace_larger_than_a_document_can_still_be_cited() {
    // Evidence is hashed as a stream; only documents have a size bound.
    let (directory, relative, mut result) = passing("qualifying-large-trace");
    let size = usize::try_from(mqttiumd_lab::workspace::MAX_DOCUMENT_BYTES).expect("fits") + 1;
    fs::write(directory.join("trace.jsonl"), vec![b'\n'; size]).expect("large trace");
    result["trace"] = cite(&format!("{relative}/trace.jsonl"));
    assert_eq!(codes_of(&directory, &relative, &result), Vec::<&str>::new());
}

#[test]
fn a_cited_manifest_larger_than_a_document_is_refused() {
    let (directory, relative, mut result) = passing("qualifying-large-manifest");
    let size = usize::try_from(mqttiumd_lab::workspace::MAX_DOCUMENT_BYTES).expect("fits") + 1;
    fs::write(directory.join("manifest.json"), vec![b' '; size]).expect("large manifest");
    result["manifest"]["bytes"] = json!(size.to_string());
    assert_eq!(
        codes_of(&directory, &relative, &result),
        vec!["file-too-large"]
    );
}
