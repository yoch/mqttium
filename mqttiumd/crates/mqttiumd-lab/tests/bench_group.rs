//! Group commit judged by the oracle (record 0045).
//!
//! On the backend `sqlite-group` the store adapter of the bench commits
//! the commands that wait together through `SqliteStore::apply_group`, and
//! the oracle judges the trace of such a run as it judges any other, up to
//! the acknowledgements the peers observed.
//!
//! No scenario written before this one has two commands outstanding at
//! once: each has one client and one exchange. The scenarios of
//! `tests/scenarios/group/` hold two admissions in transit and release
//! both before anything enters the writer, so the group of two is not left
//! to the seed. These tests require that group to have been reached, on
//! the count of the adapter and on the records of the trace; a campaign
//! in which every group holds one command proves nothing about a group
//! and is not taken for one.
//!
//! The oracle is the Python package of `tests/oracle/`, run as
//! `tools/run_campaign.sh` runs it.

#[path = "bench_support.rs"]
mod bench_support;

use std::path::PathBuf;
use std::process::Command;

use bench_support::{Campaign, records, root, run_scenario_on};
use mqttiumd_lab::bench::campaign::{Request, execute};
use serde_json::Value;

const GROUP: &str = "sqlite-group";
/// The scenarios with two admissions waiting together.
const GROUPED: [&str; 3] = [
    "tests/scenarios/group/GROUP-A.json",
    "tests/scenarios/group/GROUP-B.json",
    "tests/scenarios/group/GROUP-ACK.json",
];
/// Scenarios that never have two commands outstanding.
const ALONE: [&str; 6] = [
    "tests/scenarios/p1/P1-02.json",
    "tests/scenarios/p1/P1-02b.json",
    "tests/scenarios/qabs/QABS-A.json",
    "tests/scenarios/qabs/QABS-B.json",
    "tests/scenarios/qabs/QABS-C.json",
    "tests/scenarios/audit/F3-steady-lag.json",
];

/// The verdict of the oracle on a campaign of `scenario`.
fn judge(campaign: &Campaign, scenario: &str) -> Value {
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

fn not_passing(verdict: &Value) -> Vec<String> {
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

fn summary(campaign: &Campaign) -> Value {
    serde_json::from_slice(&campaign.file("campaign.json")).expect("the campaign")
}

/// The evidence of the store in a trace, in the order of its origin: the
/// kind of each record and the sequence of its operation.
fn store_evidence(campaign: &Campaign) -> Vec<(String, String, String)> {
    records(&campaign.trace())
        .into_iter()
        .filter(|record| record.origin == "store-evidence")
        .filter(|record| ["StoreCommitStable", "StoreResultReady"].contains(&record.kind.as_str()))
        .map(|record| {
            let operation = &record.payload["operation_id"];
            (
                record.kind,
                operation["epoch"].as_str().expect("an epoch").to_owned(),
                operation["sequence"]
                    .as_str()
                    .expect("a sequence")
                    .to_owned(),
            )
        })
        .collect()
}

/// The largest number of commits the store recorded with no result
/// between them: the members of one group.
fn commits_before_any_result(campaign: &Campaign) -> usize {
    let mut largest = 0;
    let mut run = 0;
    for (kind, ..) in store_evidence(campaign) {
        run = if kind == "StoreCommitStable" {
            run + 1
        } else {
            0
        };
        largest = largest.max(run);
    }
    largest
}

#[test]
fn a_group_of_two_is_committed_and_the_oracle_passes_the_run() {
    for scenario in GROUPED {
        let campaign = run_scenario_on(scenario, 1, "group", GROUP);
        assert_eq!(campaign.report.stopped, None, "{scenario}");
        assert!(campaign.report.not_reached.is_empty(), "{scenario}");

        // The group was reached: on the count of the adapter, and on the
        // records, where both commits come before either result.
        let summary = summary(&campaign);
        let groups = &summary["store_groups"];
        assert_eq!(groups["largest_group"], "2", "{scenario}: {groups}");
        assert_ne!(groups["groups_of_more_than_one"], "0", "{scenario}");
        assert_eq!(groups["requested_as"], GROUP);
        assert_eq!(commits_before_any_result(&campaign), 2, "{scenario}");
        let evidence = store_evidence(&campaign);
        let admitted = |kind: &str, sequence: &str| {
            evidence
                .iter()
                .position(|(k, epoch, s)| k == kind && epoch == "1" && s == sequence)
                .unwrap_or_else(|| panic!("{scenario}: no {kind} of operation {sequence}"))
        };
        assert!(
            admitted("StoreCommitStable", "2") < admitted("StoreResultReady", "1"),
            "{scenario}: the second admission is committed before the first is answered"
        );

        // What the campaign says of itself. The store is SQLite; the
        // writer is named where the run is asked for and replayed.
        assert_eq!(summary["covers"]["backend"], "sqlite");
        assert!(
            summary["covers"]["commit"]
                .as_str()
                .is_some_and(|commit| commit.starts_with("groups (record 0045)")),
            "{scenario}"
        );
        assert!(
            summary["commands"]["replay"]
                .as_str()
                .is_some_and(|replay| replay.contains(" --backend sqlite-group ")),
            "{scenario}: the replay names the writer"
        );
        let header = &records(&campaign.trace())[0];
        assert_eq!(header.kind, "TraceHeader");
        assert_eq!(header.payload["backend"], "sqlite");

        // The oracle judges that trace as any other.
        let verdict = judge(&campaign, scenario);
        assert_eq!(
            verdict["verdict"],
            "PASS",
            "{scenario}: {:?}",
            not_passing(&verdict)
        );
        for owed in [
            "one_admission_per_observed_inbound_exchange",
            "no_positive_ack_before_confirmed_commit",
            "broker_inputs_have_provenance",
            "trace_complete",
        ] {
            assert_eq!(
                verdict["assertions"][owed]["status"], "PASS",
                "{scenario}: {owed}"
            );
        }
    }
}

/// The same scenarios with one transaction for each command, on SQLite
/// and on the deterministic store: the oracle passes them too, and no
/// commit comes before the result of the one before it.
#[test]
fn the_group_scenarios_pass_with_one_transaction_for_each_command() {
    for scenario in GROUPED {
        for backend in ["sqlite", "sim-store"] {
            let campaign = run_scenario_on(scenario, 1, "group-alone", backend);
            assert_eq!(campaign.report.stopped, None, "{scenario} on {backend}");
            assert_eq!(summary(&campaign)["store_groups"], Value::Null);
            assert_eq!(
                commits_before_any_result(&campaign),
                1,
                "{scenario} on {backend}"
            );
            let verdict = judge(&campaign, scenario);
            assert_eq!(
                verdict["verdict"],
                "PASS",
                "{scenario} on {backend}: {:?}",
                not_passing(&verdict)
            );
        }
    }
}

/// A scenario in which no two commands wait together is played by the
/// grouped writer exactly as by the other: a group of one is `apply`. The
/// trace and the snapshots are the same bytes. Such a campaign shows that
/// the mode changes nothing there, and nothing about a group.
#[test]
fn a_scenario_with_one_command_at_a_time_is_unchanged_by_the_grouped_writer() {
    for scenario in ALONE {
        let grouped = run_scenario_on(scenario, 1, "alone-grouped", GROUP);
        let alone = run_scenario_on(scenario, 1, "alone-sqlite", "sqlite");
        assert_eq!(grouped.report.stopped, alone.report.stopped, "{scenario}");
        // QABS-C never lets its one command enter the writer.
        let largest = summary(&grouped)["store_groups"]["largest_group"].clone();
        assert!(
            largest == "1" || largest == "0",
            "{scenario}: no scenario of this list has two commands outstanding, found {largest}"
        );
        assert_eq!(
            summary(&grouped)["store_groups"]["groups_of_more_than_one"],
            "0"
        );
        assert_eq!(grouped.trace(), alone.trace(), "{scenario}: the trace");
        let mut snapshots = 0;
        for entry in std::fs::read_dir(alone.out.join("snapshots")).expect("snapshots") {
            let name = entry.expect("an entry").file_name();
            let relative = format!("snapshots/{}", name.to_string_lossy());
            assert_eq!(
                grouped.file(&relative),
                alone.file(&relative),
                "{scenario}: {relative}"
            );
            snapshots += 1;
        }
        assert!(snapshots > 0, "{scenario}");
    }
}

#[test]
fn the_grouped_writer_does_not_run_in_a_child_process() {
    let out = bench_support::fresh("group-child");
    let request = Request {
        root: root(),
        contracts: PathBuf::from("contracts/v1"),
        scenario: PathBuf::from(GROUPED[0]),
        out: out.clone(),
        seed: 1,
        run_id: Some("bench-test".to_owned()),
        backend: GROUP.to_owned(),
        built_from: None,
        process: "child".to_owned(),
    };
    let refused = execute(&request).expect_err("refused before anything is written");
    assert!(refused.contains("commits in groups"), "{refused}");
    assert!(!out.exists(), "no campaign directory");
}
