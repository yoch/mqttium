//! The command a campaign records plays the same campaign again (finding
//! R3 of the second audit): the same scenario, seed, run and store. Only
//! the campaign directory changes.
//!
//! The recorded command is executed, not read: its arguments are given to
//! the lab binary as they stand, with the placeholder of the directory
//! replaced.

#[path = "bench_support.rs"]
mod bench_support;

use std::fs;
use std::process::Command;

use serde_json::Value;

use bench_support::{fresh, root, run_scenario_on, spawning};

const SCENARIO: &str = "tests/scenarios/p1/P1-02.json";
const PLACEHOLDER: &str = "<empty directory outside the work tree>";

fn json(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes).expect("JSON")
}

fn the_recorded_command_replays_the_campaign_on(backend: &str) {
    let first = run_scenario_on(SCENARIO, 1, &format!("replayed-{backend}"), backend);
    let recorded = json(&first.file("campaign.json"));
    let command = recorded["commands"]["replay"].as_str().expect("a command");
    assert_eq!(command.matches(PLACEHOLDER).count(), 1, "{command}");

    // Nothing but the directory is changed.
    let out = fresh(&format!("replay-of-{backend}"));
    let (program, arguments) = command.split_once(" -- ").expect("cargo run … -- …");
    assert_eq!(
        program,
        if cfg!(feature = "kill-points") {
            "cargo run -p mqttiumd-lab --locked --features kill-points"
        } else {
            "cargo run -p mqttiumd-lab --locked"
        }
    );
    let arguments: Vec<String> = arguments
        .replace(PLACEHOLDER, "\u{1}")
        .split(' ')
        .map(|argument| argument.replace('\u{1}', &out.display().to_string()))
        .collect();
    let turn = spawning();
    let played = Command::new(env!("CARGO_BIN_EXE_mqttiumd-lab"))
        .args(&arguments)
        .current_dir(root())
        .output()
        .expect("the lab binary");
    drop(turn);
    assert!(
        played.status.success(),
        "{command}: {}",
        String::from_utf8_lossy(&played.stderr)
    );

    let replayed = json(&fs::read(out.join("campaign.json")).expect("campaign.json"));
    let manifest = json(&fs::read(out.join("run-manifest.json")).expect("run-manifest.json"));
    let trace = fs::read(out.join("trace.jsonl")).expect("trace.jsonl");
    let _ = fs::remove_dir_all(&out);
    assert_eq!(replayed["covers"]["backend"], backend, "{command}");
    assert_eq!(manifest["backend"], backend, "{command}");
    for member in ["run_id", "seed", "scenario", "scheduler_version"] {
        assert_eq!(replayed[member], recorded[member], "{member}");
    }
    assert_eq!(trace, first.trace(), "the replay wrote another trace");
    // The replay records the command it was played by.
    assert_eq!(
        replayed["commands"]["replay"],
        recorded["commands"]["replay"]
    );
    // An ordinary replay says what it is: no rebuilt binary, a
    // provisional result.
    assert!(!command.contains("--rebuild"), "{command}");
    let scope = recorded["commands"]["replay_scope"]
        .as_str()
        .expect("a scope");
    assert!(scope.contains("ordinary replay"), "{scope}");
    assert!(scope.contains("provisional"), "{scope}");
}

#[test]
fn the_recorded_command_replays_a_campaign_of_the_deterministic_store() {
    the_recorded_command_replays_the_campaign_on("sim-store");
}

#[test]
fn the_recorded_command_replays_a_campaign_of_the_sqlite_store() {
    the_recorded_command_replays_the_campaign_on("sqlite");
}
