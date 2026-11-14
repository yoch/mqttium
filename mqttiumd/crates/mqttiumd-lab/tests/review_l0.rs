//! Defects found by the review of lot L0 at revision 422a2bc.
//!
//! Each test builds its documents below the target directory, which is
//! below the mqttiumd root, and expects a diagnostic and exit code 2:
//! never a crash, never an unbounded computation.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use mqttiumd_lab::workspace::Workspace;
use serde_json::{Value, json};

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Creates an empty directory for one test and returns it with its path
/// relative to the mqttiumd root.
fn scratch(name: &str) -> (PathBuf, String) {
    let directory = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    let _ = fs::remove_dir_all(&directory);
    fs::create_dir_all(&directory).expect("scratch directory");
    let canonical_root = root().canonicalize().expect("root");
    let relative = directory
        .canonicalize()
        .expect("scratch directory")
        .strip_prefix(&canonical_root)
        .expect("the target directory is below the mqttiumd root")
        .to_string_lossy()
        .into_owned();
    (directory, relative)
}

fn write(directory: &Path, name: &str, document: &Value) {
    let text = serde_json::to_string_pretty(document).expect("JSON");
    fs::write(directory.join(name), text).expect("document written");
}

fn scenario(fixture: &str) -> Value {
    let bytes = fs::read(root().join("tests/scenarios/p1/P1-02.json")).expect("P1-02");
    let mut document: Value = serde_json::from_slice(&bytes).expect("P1-02 is JSON");
    document["initial_fixture"] = json!(fixture);
    document
}

fn validate(document: &str) -> Option<i32> {
    Command::new(env!("CARGO_BIN_EXE_mqttiumd-lab"))
        .args([
            "validate",
            "--contracts",
            "contracts/v1",
            "--scenario",
            document,
        ])
        .current_dir(root())
        .output()
        .expect("the bench runs")
        .status
        .code()
}

fn codes(document: &str) -> Vec<&'static str> {
    Workspace::open(&root(), Path::new("contracts/v1"))
        .expect("the contract directory opens")
        .check_file(Path::new(document))
        .diagnostics
        .iter()
        .map(|diagnostic| diagnostic.code)
        .collect()
}

#[test]
fn a_scenario_that_names_itself_as_fixture_is_refused_without_a_crash() {
    let (directory, relative) = scratch("self-reference");
    let path = format!("{relative}/self.json");
    write(&directory, "self.json", &scenario(&path));
    assert_eq!(
        validate(&path),
        Some(2),
        "the bench crashed instead of refusing"
    );
}

#[test]
fn two_scenarios_that_name_each_other_are_refused_without_a_crash() {
    let (directory, relative) = scratch("two-document-cycle");
    let first = format!("{relative}/first.json");
    let second = format!("{relative}/second.json");
    write(&directory, "first.json", &scenario(&second));
    write(&directory, "second.json", &scenario(&first));
    assert_eq!(
        validate(&first),
        Some(2),
        "the bench crashed instead of refusing"
    );
}

/// A catalogue whose single event class has `dimensions` guard dimensions of
/// `values` values each, and a seed registry with no rule over it.
fn wide_registry(name: &str, dimensions: usize, values: usize) -> String {
    let (directory, relative) = scratch(name);
    let dimension_names: Vec<String> = (0..dimensions).map(|index| format!("d{index}")).collect();
    let catalog = json!({
        "format": "mqttiumd.transition-catalog",
        "schema_version": 1,
        "id": "wide",
        "guard_dimensions": dimension_names.iter().map(|name| json!({
            "name": name,
            "values": (0..values).map(|index| json!({
                "value": format!("v{index}"),
                "meaning": "One value of a wide guard dimension.",
            })).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
        "event_classes": [{
            "name": "Wide",
            "meaning": "An event with many guard dimensions.",
            "guard_dimensions": dimension_names,
        }],
        "effects": ["Persist"],
        "machines": [{"name": "wide", "state_classes": ["Only"], "event_classes": ["Wide"]}],
    });
    write(&directory, "wide.catalog.json", &catalog);
    let registry = json!({
        "format": "mqttiumd.transition-registry",
        "schema_version": 1,
        "id": "wide",
        "catalog": format!("{relative}/wide.catalog.json"),
        "status": "seed",
        "rules": [],
    });
    write(&directory, "wide.transitions.json", &registry);
    format!("{relative}/wide.transitions.json")
}

#[test]
fn a_guard_domain_beyond_the_coverage_budget_is_refused() {
    // 4^8 = 65 536 guard points in one cell.
    let registry = wide_registry("coverage-budget", 8, 4);
    assert_eq!(codes(&registry), vec!["coverage-budget-exceeded"]);
}
