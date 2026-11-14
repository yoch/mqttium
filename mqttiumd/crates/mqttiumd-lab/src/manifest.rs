//! Skeleton of a run manifest (DEM-003, V3-002).
//!
//! A result designates an exact content and configuration. Lot L0 collects
//! the content sections: product revision and local modifications,
//! toolchain, build, contract files and platform. The experiment sections
//! (scenario, fixture, backend, failure model, scheduler, commands) are
//! added by the lots that run something.

use std::fs;
use std::path::Path;
use std::process::Command;

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

/// Lower-case hexadecimal SHA-256 of `bytes`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn run(program: &str, arguments: &[&str], directory: &Path) -> Result<String, String> {
    let output = Command::new(program)
        .args(arguments)
        .current_dir(directory)
        .output()
        .map_err(|error| format!("{program}: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "{program} {}: {}",
            arguments.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .trim_end()
        .to_owned())
}

fn artifact(root: &Path, relative: &str) -> Result<Value, String> {
    let bytes = fs::read(root.join(relative)).map_err(|error| format!("{relative}: {error}"))?;
    Ok(json!({
        "path": relative,
        "sha256": sha256_hex(&bytes),
        "bytes": bytes.len().to_string(),
    }))
}

fn contract_files(root: &Path, directory: &str, found: &mut Vec<String>) -> Result<(), String> {
    let entries =
        fs::read_dir(root.join(directory)).map_err(|error| format!("{directory}: {error}"))?;
    for entry in entries {
        let entry = entry.map_err(|error| format!("{directory}: {error}"))?;
        let relative = format!("{directory}/{}", entry.file_name().to_string_lossy());
        if entry.path().is_dir() {
            contract_files(root, &relative, found)?;
        } else if relative.ends_with(".json") {
            found.push(relative);
        }
    }
    Ok(())
}

/// Collects the content sections of a manifest for the workspace at `root`.
///
/// The product revision and the modified paths come from the git work tree
/// that holds `root`; a manifest of a modified tree says so and lists the
/// paths, so its results cannot be mistaken for those of the clean revision.
pub fn collect(root: &Path) -> Result<Value, String> {
    let revision = run("git", &["rev-parse", "HEAD"], root)?;
    let status = run("git", &["status", "--porcelain", "--", "."], root)?;
    let modified: Vec<&str> = status.lines().collect();
    let rustc = run("rustc", &["--version", "--verbose"], root)?;
    let host = rustc
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .ok_or("rustc did not report its host")?
        .to_owned();
    let mut files = Vec::new();
    contract_files(root, "contracts", &mut files)?;
    files.sort();
    let contracts: Vec<Value> = files
        .iter()
        .map(|file| artifact(root, file))
        .collect::<Result<_, _>>()?;
    let lockfile =
        fs::read(root.join("Cargo.lock")).map_err(|error| format!("Cargo.lock: {error}"))?;
    let workspace =
        fs::read(root.join("Cargo.toml")).map_err(|error| format!("Cargo.toml: {error}"))?;
    Ok(json!({
        "format": "mqttiumd.run-manifest",
        "schema_version": 1,
        "product": {
            "revision": revision,
            "tree_state": if modified.is_empty() { "clean" } else { "modified" },
            "modified_paths": modified,
        },
        "toolchain": {
            "rustc": rustc.lines().next().unwrap_or_default(),
            "cargo": run("cargo", &["--version"], root)?,
            "host": host,
        },
        "build": {
            "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "target": host,
            "lockfile_sha256": sha256_hex(&lockfile),
            "workspace_manifest_sha256": sha256_hex(&workspace),
        },
        "contracts": contracts,
        "platform": {
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::sha256_hex;

    #[test]
    fn sha256_matches_the_published_vectors() {
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
