//! The example corpus is discriminating: valid documents are accepted and
//! each invalid document is refused for its declared reason, not for any
//! reason at all.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use mqttiumd_lab::diagnostic::code;
use mqttiumd_lab::manifest;
use mqttiumd_lab::workspace::Workspace;
use serde_json::Value;

const INDEX: &str = "contracts/examples/v1/index.json";

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn workspace() -> Workspace {
    Workspace::open(&root(), Path::new("contracts/v1")).expect("the contract directory opens")
}

fn index() -> Value {
    let bytes = fs::read(root().join(INDEX)).expect("the example index is readable");
    serde_json::from_slice(&bytes).expect("the example index is JSON")
}

#[test]
fn the_contract_directory_is_accepted() {
    for (path, report) in workspace().check_contracts() {
        assert!(
            report.is_accepted(),
            "{}: {:?}",
            path.display(),
            report.diagnostics
        );
    }
}

/// A PASS requires a complete registry: every guard point of every cell of
/// the P1 machines has exactly one rule. That a seed lists its holes
/// instead of hiding them stays covered by the toy seed of the corpus and
/// by `review_l0.rs`.
#[test]
fn the_p1_registry_is_complete_and_has_no_hole() {
    let path = Path::new("contracts/v1/p1.transitions.json");
    let bytes = fs::read(root().join(path)).expect("the registry is readable");
    let registry: Value = serde_json::from_slice(&bytes).expect("the registry is JSON");
    assert_eq!(registry["status"], "complete");
    let report = workspace().check_file(path);
    assert!(report.is_accepted(), "{:?}", report.diagnostics);
    assert!(report.notes.is_empty(), "{:?}", report.notes);
}

/// The same registry with one rule removed is refused, whichever rule it
/// is: completeness is checked, not declared.
#[test]
fn the_p1_registry_without_any_one_of_its_rules_has_a_hole() {
    let path = "contracts/v1/p1.transitions.json";
    let bytes = fs::read(root().join(path)).expect("the registry is readable");
    let registry: Value = serde_json::from_slice(&bytes).expect("the registry is JSON");
    let rules = registry["rules"].as_array().expect("rules");
    assert!(rules.len() >= 90);
    let workspace = workspace();
    for (index, rule) in rules.iter().enumerate() {
        let mut without = registry.clone();
        without["rules"]
            .as_array_mut()
            .expect("rules")
            .remove(index);
        let report = workspace.check_value(&without);
        assert!(
            report
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == code::REGISTRY_HOLE),
            "without {}: {:?}",
            rule["id"],
            report.diagnostics
        );
    }
}

#[test]
fn the_example_index_is_accepted() {
    let report = workspace().check_file(Path::new(INDEX));
    assert!(report.is_accepted(), "{:?}", report.diagnostics);
}

#[test]
fn every_valid_example_is_accepted() {
    let workspace = workspace();
    let index = index();
    let valid = index["valid"].as_array().expect("valid list");
    assert!(valid.len() >= 10);
    for file in valid {
        let file = file.as_str().expect("path");
        let report = workspace.check_file(Path::new(file));
        assert!(report.is_accepted(), "{file}: {:?}", report.diagnostics);
    }
}

#[test]
fn every_invalid_example_is_refused_for_its_declared_reason() {
    let workspace = workspace();
    let index = index();
    let invalid = index["invalid"].as_array().expect("invalid list");
    assert!(invalid.len() >= 60);
    for case in invalid {
        let file = case["file"].as_str().expect("path");
        let expected = case["code"].as_str().expect("code");
        let pointer = case["pointer"].as_str().expect("pointer");
        let report = workspace.check_file(Path::new(file));
        assert!(!report.is_accepted(), "{file} was accepted");
        for diagnostic in &report.diagnostics {
            assert_eq!(
                diagnostic.code, expected,
                "{file}: refused for another reason: {diagnostic}"
            );
        }
        assert!(
            report
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.pointer == pointer),
            "{file}: no diagnostic at {pointer:?}: {:?}",
            report.diagnostics
        );
    }
}

/// One misplaced member is one diagnostic: a reader that reports the same
/// mistake twice, or lets it cascade, spends the bounded report on noise.
#[test]
fn a_scenario_with_one_misplaced_step_gets_one_diagnostic() {
    let workspace = workspace();
    for name in [
        "scenario-deliver-while-crashed.json",
        "scenario-second-wait-on-a-holding-fault.json",
        "scenario-release-twice.json",
        "scenario-release-before-hold.json",
        "scenario-inspect-while-crashed.json",
    ] {
        let file = format!("contracts/examples/v1/invalid/{name}");
        let report = workspace.check_file(Path::new(&file));
        assert_eq!(
            report.diagnostics.len(),
            1,
            "{file}: {:?}",
            report.diagnostics
        );
    }
}

/// DEM-018: the three orders of QABS are scenarios of their own, each
/// with the injections that produce its order.
#[test]
fn the_qabs_scenarios_are_accepted_and_differ_by_their_injections() {
    let workspace = workspace();
    let mut actions = Vec::new();
    for letter in ["A", "B", "C"] {
        let file = format!("tests/scenarios/qabs/QABS-{letter}.json");
        let report = workspace.check_file(Path::new(&file));
        assert!(report.is_accepted(), "{file}: {:?}", report.diagnostics);
        let bytes = fs::read(root().join(&file)).expect("scenario");
        let scenario: Value = serde_json::from_slice(&bytes).expect("JSON");
        assert_eq!(scenario["id"], format!("QABS-{letter}"));
        let fence: Vec<String> = scenario["faults"]
            .as_array()
            .expect("faults")
            .iter()
            .filter(|fault| fault["point"] == "store.fence.transit")
            .map(|fault| fault["action"].as_str().expect("action").to_owned())
            .collect();
        actions.push(fence);
    }
    assert_eq!(
        actions,
        [
            vec!["hold_command"],
            vec!["hold_command"],
            vec!["refuse_fence"]
        ]
    );
}

#[test]
fn every_example_file_is_indexed_exactly_once() {
    let index = index();
    let mut indexed: Vec<String> = index["valid"]
        .as_array()
        .expect("valid list")
        .iter()
        .map(|file| file.as_str().expect("path").to_owned())
        .chain(
            index["invalid"]
                .as_array()
                .expect("invalid list")
                .iter()
                .map(|case| case["file"].as_str().expect("path").to_owned()),
        )
        .filter(|file| file.starts_with("contracts/examples/"))
        .collect();
    indexed.sort();
    let mut present = Vec::new();
    for directory in ["valid", "invalid"] {
        let relative = format!("contracts/examples/v1/{directory}");
        for entry in fs::read_dir(root().join(&relative)).expect("example directory") {
            let name = entry.expect("entry").file_name();
            present.push(format!("{relative}/{}", name.to_string_lossy()));
        }
    }
    present.sort();
    assert_eq!(indexed, present);
}

#[test]
fn the_corpus_exercises_every_code_it_can() {
    let index = index();
    let exercised: BTreeSet<&str> = index["invalid"]
        .as_array()
        .expect("invalid list")
        .iter()
        .map(|case| case["code"].as_str().expect("code"))
        .collect();
    // Tested where they are raised, because no static example can carry
    // them: a schema file that is not local, a decimal string that a lax
    // schema validator would let through, a reference cycle that the
    // acyclic format table already excludes, a 16 MiB document, and a
    // qualifying result, which names files and contracts by digest and is
    // built by `tests/qualifying.rs`.
    let elsewhere = [
        code::SCHEMA_NOT_LOCAL,
        code::U64_NOT_CANONICAL,
        code::REFERENCE_CYCLE,
        code::FILE_TOO_LARGE,
        code::RESULT_INCOMPLETE,
    ];
    for known in code::ALL {
        assert!(
            exercised.contains(known) || elsewhere.contains(&known),
            "no invalid example is refused with {known}"
        );
    }
    for used in exercised {
        assert!(code::ALL.contains(&used), "{used} is not a known code");
    }
}

#[test]
fn the_scenario_file_is_the_text_of_the_specification() {
    let chapter = fs::read_to_string(root().join("docs/spec/fr/chapitre-4.md")).expect("chapter 4");
    let scenario = fs::read_to_string(root().join("tests/scenarios/p1/P1-02.json")).expect("P1-02");
    assert!(
        chapter.contains(scenario.trim_end()),
        "P1-02.json drifted from DEM-017"
    );
}

#[test]
fn a_collected_manifest_satisfies_its_schema() {
    let manifest =
        manifest::collect(&root(), Path::new("contracts/v1")).expect("the manifest is collected");
    let report = workspace().check_value(&manifest);
    assert!(report.is_accepted(), "{:?}", report.diagnostics);
    let contracts = manifest["contracts"].as_array().expect("contracts");
    assert!(
        contracts
            .iter()
            .any(|file| file["path"] == "contracts/schema/v1/primitives.schema.json")
    );
    assert!(
        contracts
            .iter()
            .any(|file| file["path"] == "contracts/v1/p1.transitions.json")
    );
}

fn lab(arguments: &[&str]) -> Option<i32> {
    Command::new(env!("CARGO_BIN_EXE_mqttiumd-lab"))
        .args(arguments)
        .current_dir(root())
        .output()
        .expect("the bench runs")
        .status
        .code()
}

#[test]
fn exit_codes_follow_the_verdict_table() {
    let contracts = ["validate", "--contracts", "contracts/v1"];
    assert_eq!(
        lab(&[
            &contracts[..],
            &["--scenario", "tests/scenarios/p1/P1-02.json"]
        ]
        .concat()),
        Some(0)
    );
    assert_eq!(
        lab(&[
            &contracts[..],
            &[
                "--scenario",
                "contracts/examples/v1/invalid/scenario-u64-overflow.json"
            ]
        ]
        .concat()),
        Some(2)
    );
    assert_eq!(
        lab(&[
            &contracts[..],
            &["--scenario", "tests/scenarios/p1/absent.json"]
        ]
        .concat()),
        Some(2)
    );
    // The contract directory may be spelled with a trailing separator.
    assert_eq!(
        lab(&[
            "validate",
            "--contracts",
            "contracts/v1/",
            "--scenario",
            "tests/scenarios/p1/P1-02.json"
        ]),
        Some(0)
    );
    assert_eq!(lab(&["validate"]), Some(2));
    assert_eq!(lab(&["validate", "--contracts", "contracts/v9"]), Some(2));
    assert_eq!(lab(&[]), Some(2));
    // A command that does not exist yet reports no verdict at all.
    for command in ["run", "replay", "minimize"] {
        assert_eq!(lab(&[command]), Some(2), "{command}");
    }
}

#[test]
fn the_contract_digest_does_not_depend_on_how_the_directory_is_spelled() {
    let digest = |spelling: &str| {
        Workspace::open(&root(), Path::new(spelling))
            .expect("the contract directory opens")
            .contracts_digest()
            .to_owned()
    };
    let reference = digest("contracts/v1");
    assert_eq!(reference.len(), 64);
    assert_eq!(digest("contracts/v1/"), reference);
    assert_eq!(digest("./contracts/v1"), reference);
    assert!(Workspace::open(&root(), Path::new("contracts/../contracts/v1")).is_err());
}

/// The reason a refusing PUBREC carries, as the transition catalogue
/// states it for `EmitPubrecRefusal`, is the reason of the contract: the
/// oracle judges traces against the first, the core answers with the
/// second.
#[test]
fn the_reasons_of_a_refusal_are_those_of_the_contract() {
    use mqttiumd_contract::capacity::Resource;
    use mqttiumd_contract::outcome::RejectReason;
    use mqttiumd_contract::packet::AckReason;

    let bytes = fs::read(root().join("contracts/v1/p1.catalog.json")).expect("catalogue");
    let catalogue: Value = serde_json::from_slice(&bytes).expect("JSON");
    let table = catalogue
        .pointer("/refusal_reasons")
        .and_then(Value::as_object)
        .expect("the catalogue states the reasons of EmitPubrecRefusal");
    let rejections = [
        (
            "CapacityExceeded",
            RejectReason::CapacityExceeded(Resource::PayloadBytes),
        ),
        ("Fenced", RejectReason::Fenced),
        ("PreconditionFailed", RejectReason::PreconditionFailed),
        ("EpochClosed", RejectReason::EpochClosed),
    ];
    assert_eq!(table.len(), rejections.len(), "{table:?}");
    for (name, rejection) in rejections {
        let stated = table.get(name).and_then(Value::as_str);
        let reason = format!("{:?}", AckReason::refusing(rejection));
        assert_eq!(stated, Some(reason.as_str()), "{name}");
    }
}

/// The cases of the order of a recovery inventory hold for the contract's
/// rule. The oracle runs the same file through its own copy.
#[test]
fn the_recovery_order_cases_hold_for_the_contract() {
    use mqttiumd_contract::effects::{OrderRefused, RecoveryOrder};
    use mqttiumd_lab::semantic::representative_fact;

    let path = root().join("crates/mqttiumd-lab/tests/support/recovery-order-cases.json");
    let cases: Value = serde_json::from_slice(&fs::read(path).expect("cases")).expect("JSON");
    let cases = cases["cases"].as_array().expect("cases");
    assert!(cases.len() >= 10);
    for case in cases {
        let mut order = RecoveryOrder::new();
        let mut found = Vec::new();
        for (index, fact) in case["facts"].as_array().expect("facts").iter().enumerate() {
            let kind = fact[0].as_str().expect("kind");
            let topic = fact.get(1).and_then(Value::as_str).unwrap_or_default();
            let fact = representative_fact(kind, topic).expect("a kind of fact");
            if let Err(refusal) = order.admit(&fact) {
                let name = match refusal {
                    OrderRefused::RankDecreased { .. } => "RankDecreased",
                    OrderRefused::Repeated(_) => "Repeated",
                    OrderRefused::RoutesUnsorted => "RoutesUnsorted",
                };
                found.push(serde_json::json!([index, name]));
            }
        }
        assert_eq!(Value::Array(found), case["refused"], "{}", case["name"]);
    }
}
