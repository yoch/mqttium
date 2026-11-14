//! The contract and the core cannot reach the outside world (DEM-003,
//! ADR 0007): no dependency, `no_std + alloc`, no `unsafe`, no `cfg` that
//! could swap a business decision, no async.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// The conditional-compilation forms a pure crate may use.
const ALLOWED_CFG: [&str; 3] = ["cfg(test)", "cfg_attr(not(test), no_std)", "cfg(kani)"];

/// Returns the reasons `source` breaks the purity rules.
fn violations(source: &str) -> Vec<String> {
    let mut found = Vec::new();
    for needle in [
        "extern crate std",
        "async fn",
        "async move",
        ".await",
        "unsafe ",
    ] {
        if source.contains(needle) {
            found.push(format!("contains `{needle}`"));
        }
    }
    for (start, _) in source.match_indices("cfg") {
        let rest = &source[start..];
        if !(rest.starts_with("cfg(") || rest.starts_with("cfg_attr(")) {
            continue;
        }
        let mut depth = 0usize;
        let mut end = rest.len();
        for (offset, character) in rest.char_indices() {
            match character {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        end = offset + 1;
                        break;
                    }
                }
                _ => {}
            }
        }
        let attribute = &rest[..end];
        if !ALLOWED_CFG.contains(&attribute) {
            found.push(format!("uses `{attribute}`"));
        }
    }
    found
}

fn sources(directory: &Path, found: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(directory).expect("source directory") {
        let path = entry.expect("entry").path();
        if path.is_dir() {
            sources(&path, found);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            found.push(path);
        }
    }
}

#[test]
fn the_rules_reject_what_they_are_meant_to_reject() {
    assert!(violations("#![cfg_attr(not(test), no_std)]\n#[cfg(test)]\nmod tests {}").is_empty());
    assert!(violations("#[cfg(kani)]\nmod proofs {}").is_empty());
    for source in [
        "extern crate std;",
        "#[cfg(simulation)]\nfn decide() {}",
        "#[cfg(feature = \"sim\")]\nfn decide() {}",
        "#[cfg(not(test))]\nfn decide() {}",
        "#[cfg_attr(test, allow(dead_code))]\nfn decide() {}",
        "if cfg!(debug_assertions) {}"
            .replace("cfg!", "cfg")
            .as_str(),
        "pub async fn step() {}",
        "fn step() { other().await }",
        "unsafe { core::hint::unreachable_unchecked() }",
    ] {
        assert!(!violations(source).is_empty(), "accepted: {source}");
    }
}

#[test]
fn pure_crates_keep_their_source_rules() {
    for name in ["mqttiumd-contract", "mqttiumd-core"] {
        let directory = root().join("crates").join(name).join("src");
        let lib = fs::read_to_string(directory.join("lib.rs")).expect("lib.rs");
        assert!(
            lib.contains("#![cfg_attr(not(test), no_std)]"),
            "{name} must be no_std"
        );
        assert!(
            lib.contains("#![forbid(unsafe_code)]"),
            "{name} must forbid unsafe"
        );
        let mut files = Vec::new();
        sources(&directory, &mut files);
        assert!(!files.is_empty());
        for file in files {
            let source = fs::read_to_string(&file).expect("source file");
            let found = violations(&source);
            assert!(found.is_empty(), "{}: {found:?}", file.display());
        }
    }
}

#[test]
fn pure_crates_have_no_external_dependency() {
    let output = Command::new(env!("CARGO"))
        .args([
            "metadata",
            "--format-version",
            "1",
            "--no-deps",
            "--offline",
        ])
        .current_dir(root())
        .output()
        .expect("cargo metadata runs");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let metadata: Value = serde_json::from_slice(&output.stdout).expect("metadata is JSON");
    let dependencies = |name: &str| -> Vec<String> {
        metadata["packages"]
            .as_array()
            .expect("packages")
            .iter()
            .find(|package| package["name"] == name)
            .unwrap_or_else(|| panic!("{name} is not in the workspace"))["dependencies"]
            .as_array()
            .expect("dependencies")
            .iter()
            .map(|dependency| dependency["name"].as_str().expect("name").to_owned())
            .collect()
    };
    assert_eq!(dependencies("mqttiumd-contract"), Vec::<String>::new());
    assert_eq!(
        dependencies("mqttiumd-core"),
        vec!["mqttiumd-contract".to_owned()]
    );
}
