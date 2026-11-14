//! The requirement register and the archived chapters agree.
//!
//! The four chapters are the immutable French source of the contract. The
//! register lists every identifier they define, so that a registry rule or
//! an assertion cannot cite a requirement that does not exist.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use mqttiumd_lab::manifest::sha256_hex;
use serde_json::Value;

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn register() -> Value {
    let bytes = fs::read(root().join("contracts/v1/requirements.json")).expect("register");
    serde_json::from_slice(&bytes).expect("register is JSON")
}

/// Identifiers introduced as `**ID — title.**` in a chapter.
fn defined_in(chapter: &str) -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    for (start, _) in chapter.match_indices("**") {
        let rest = &chapter[start + 2..];
        let Some(end) = rest.find(" — ") else {
            continue;
        };
        let candidate = &rest[..end];
        let shaped = candidate.split_once('-').is_some_and(|(prefix, number)| {
            !prefix.is_empty()
                && prefix
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
                && !number.is_empty()
                && number.bytes().all(|byte| byte.is_ascii_digit())
        });
        if shaped {
            found.insert(candidate.to_owned());
        }
    }
    found
}

#[test]
fn archived_chapters_match_their_recorded_digests() {
    let sums = fs::read_to_string(root().join("docs/spec/fr/SHA256SUMS")).expect("SHA256SUMS");
    let register = register();
    let sources = register["chapters"].as_array().expect("chapters");
    assert_eq!(sources.len(), 4);
    for source in sources {
        let path = source["path"].as_str().expect("path");
        let digest = sha256_hex(&fs::read(root().join(path)).expect("chapter"));
        assert_eq!(source["sha256"], digest.as_str(), "{path} changed");
        let name = path.rsplit('/').next().expect("file name");
        assert!(
            sums.contains(&format!("{digest}  {name}")),
            "{name} is not in SHA256SUMS"
        );
    }
}

#[test]
fn the_register_lists_exactly_the_requirements_of_each_chapter() {
    let register = register();
    for chapter in 1..=4u64 {
        let text = fs::read_to_string(root().join(format!("docs/spec/fr/chapitre-{chapter}.md")))
            .expect("chapter");
        let registered: BTreeSet<String> = register["entries"]
            .as_array()
            .expect("entries")
            .iter()
            .filter(|entry| entry["kind"] == "requirement" && entry["source"]["chapter"] == chapter)
            .map(|entry| entry["id"].as_str().expect("id").to_owned())
            .collect();
        assert_eq!(registered, defined_in(&text), "chapter {chapter}");
    }
}

#[test]
fn every_other_identifier_appears_in_its_source() {
    let register = register();
    for entry in register["entries"].as_array().expect("entries") {
        if entry["kind"] == "requirement" {
            continue;
        }
        let id = entry["id"].as_str().expect("id");
        let path = match (
            entry["source"]["chapter"].as_u64(),
            entry["source"]["decision"].as_str(),
        ) {
            (Some(chapter), None) => format!("docs/spec/fr/chapitre-{chapter}.md"),
            (None, Some(decision)) => register["decisions"]
                .as_array()
                .expect("decisions")
                .iter()
                .find(|record| record["decision"] == decision)
                .unwrap_or_else(|| panic!("{id}: decision {decision} is not registered"))["path"]
                .as_str()
                .expect("path")
                .to_owned(),
            source => panic!("{id}: one source expected, found {source:?}"),
        };
        let text = fs::read_to_string(root().join(&path)).expect("source");
        assert!(text.contains(id), "{id} does not appear in {path}");
    }
}

#[test]
fn only_requirements_of_the_chapters_carry_a_chapter_kind_requirement() {
    let register = register();
    for entry in register["entries"].as_array().expect("entries") {
        if entry["kind"] == "requirement" {
            assert!(entry["source"]["chapter"].is_u64(), "{}", entry["id"]);
        }
    }
}

#[test]
fn the_register_lists_exactly_the_accepted_decision_records() {
    let register = register();
    let registered: BTreeSet<(String, String)> = register["decisions"]
        .as_array()
        .expect("decisions")
        .iter()
        .map(|record| {
            (
                record["path"].as_str().expect("path").to_owned(),
                record["sha256"].as_str().expect("sha256").to_owned(),
            )
        })
        .collect();
    let mut accepted = BTreeSet::new();
    for entry in fs::read_dir(root().join("docs/decisions")).expect("decisions") {
        let name = entry
            .expect("entry")
            .file_name()
            .to_string_lossy()
            .into_owned();
        if !name.starts_with(|first: char| first.is_ascii_digit()) {
            continue;
        }
        let path = format!("docs/decisions/{name}");
        let bytes = fs::read(root().join(&path)).expect("decision record");
        let text = String::from_utf8_lossy(&bytes);
        assert!(
            text.contains("Status: **Accepted") || text.contains("Status: **Proposed"),
            "{path} states no status"
        );
        if text.contains("Status: **Accepted") {
            accepted.insert((path, sha256_hex(&bytes)));
        }
    }
    assert_eq!(
        registered, accepted,
        "the register and the decision records drifted apart"
    );
    for record in register["decisions"].as_array().expect("decisions") {
        let path = record["path"].as_str().expect("path");
        let id = record["decision"].as_str().expect("decision");
        assert!(
            path.starts_with(&format!("docs/decisions/{id}-")),
            "{path} is not record {id}"
        );
    }
}
