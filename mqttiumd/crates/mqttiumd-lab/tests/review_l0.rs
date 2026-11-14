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
    // The format is checked before the reference is followed.
    assert_eq!(codes(&path), vec!["reference-invalid"]);
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
    assert_eq!(codes(&first), vec!["reference-invalid"]);
    assert_eq!(codes(&second), vec!["reference-invalid"]);
}

#[test]
fn a_reference_to_a_valid_document_of_another_format_is_refused() {
    let (directory, relative) = scratch("wrong-format");
    let path = format!("{relative}/scenario.json");
    write(
        &directory,
        "scenario.json",
        &scenario("contracts/v1/profiles.json"),
    );
    assert_eq!(codes(&path), vec!["reference-invalid"]);
    assert_eq!(validate(&path), Some(2));
}

#[test]
fn a_reference_to_a_directory_is_refused() {
    let (directory, relative) = scratch("directory-reference");
    let path = format!("{relative}/scenario.json");
    write(&directory, "scenario.json", &scenario("tests/fixtures/p1"));
    assert_eq!(codes(&path), vec!["file-unreadable"]);
    assert_eq!(validate(&path), Some(2));
}

#[test]
fn a_document_larger_than_the_reader_loads_is_refused() {
    let (directory, relative) = scratch("too-large");
    let limit = usize::try_from(mqttiumd_lab::workspace::MAX_DOCUMENT_BYTES).expect("fits");
    fs::write(directory.join("big.json"), vec![b' '; limit + 1]).expect("big file");
    let big = format!("{relative}/big.json");
    let path = format!("{relative}/scenario.json");
    write(&directory, "scenario.json", &scenario(&big));
    assert_eq!(codes(&path), vec!["file-too-large"]);
    assert_eq!(codes(&big), vec!["file-too-large"]);
    assert_eq!(validate(&path), Some(2));
}

#[cfg(unix)]
#[test]
fn a_symbolic_link_is_followed_only_while_it_stays_below_the_root() {
    use std::os::unix::fs::symlink;

    let (directory, relative) = scratch("symbolic-links");
    let fixture = root()
        .join("tests/fixtures/p1/three-persistent-sessions.json")
        .canonicalize()
        .expect("fixture");
    symlink(&fixture, directory.join("inside.json")).expect("link below the root");
    let inside = format!("{relative}/inside-scenario.json");
    write(
        &directory,
        "inside-scenario.json",
        &scenario(&format!("{relative}/inside.json")),
    );
    assert_eq!(codes(&inside), Vec::<&str>::new());

    // The same valid fixture, copied outside the root and linked from inside.
    let name = format!("mqttiumd-outside-{}.json", std::process::id());
    let outside = std::env::temp_dir().join(name);
    fs::copy(&fixture, &outside).expect("copy outside the root");
    symlink(&outside, directory.join("outside.json")).expect("link leaving the root");
    let linked = format!("{relative}/outside.json");
    let through = format!("{relative}/outside-scenario.json");
    write(&directory, "outside-scenario.json", &scenario(&linked));
    let refused = codes(&through);
    let direct = codes(&linked);
    let exit = validate(&through);
    fs::remove_file(&outside).expect("cleanup");
    assert_eq!(refused, vec!["path-escape"]);
    assert_eq!(direct, vec!["path-escape"], "a linked top-level document");
    assert_eq!(exit, Some(2));
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
    // The widest catalogue the schema admits: 16^8 = 2^32 points.
    let widest = wide_registry("coverage-budget-widest", 8, 16);
    assert_eq!(codes(&widest), vec!["coverage-budget-exceeded"]);
}

#[test]
fn a_guard_domain_at_the_coverage_budget_is_checked() {
    // 4^6 = 4 096 guard points: exactly the budget of one cell.
    let registry = wide_registry("coverage-at-budget", 6, 4);
    let report = Workspace::open(&root(), Path::new("contracts/v1"))
        .expect("the contract directory opens")
        .check_file(Path::new(&registry));
    assert!(report.is_accepted(), "{:?}", report.diagnostics);
    let expected = "4096 of 4096 guard points uncovered";
    assert!(
        report.notes.iter().any(|note| note.contains(expected)),
        "{:?}",
        report.notes
    );
}

#[test]
fn a_catalogue_wider_than_the_schema_admits_is_refused_by_the_schema() {
    let registry = wide_registry("coverage-schema-bound", 9, 2);
    assert_eq!(codes(&registry), vec!["reference-invalid"]);
}

#[test]
fn a_report_keeps_a_bounded_number_of_diagnostics_and_counts_the_rest() {
    let (directory, relative) = scratch("many-diagnostics");
    let rules: Vec<Value> = (0..200)
        .map(|index| {
            json!({
                "id": format!("toy.unknown.r{index}"),
                "machine": "nowhere",
                "state_class": "Idle",
                "event_class": "Tick",
                "guard_partition": {},
                "disposition": "REJECT",
                "next_state_class": "Idle",
                "required_effects": [],
                "forbidden_effects": [],
                "resource_delta_class": "none",
                "requirements": ["DEM-015"],
                "witnesses": [],
                "mutations": [],
            })
        })
        .collect();
    let registry = json!({
        "format": "mqttiumd.transition-registry",
        "schema_version": 1,
        "id": "many",
        "catalog": "contracts/examples/v1/valid/toy.catalog.json",
        "status": "seed",
        "rules": rules,
    });
    write(&directory, "many.transitions.json", &registry);
    let report = Workspace::open(&root(), Path::new("contracts/v1"))
        .expect("the contract directory opens")
        .check_file(Path::new(&format!("{relative}/many.transitions.json")));
    let kept = mqttiumd_lab::diagnostic::MAX_DIAGNOSTICS;
    assert_eq!(report.diagnostics.len(), kept);
    assert_eq!(report.omitted_diagnostics, 200 - kept);
    assert!(!report.is_accepted(), "a truncated report still refuses");
}
