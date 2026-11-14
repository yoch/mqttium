//! Skeleton of a run manifest (DEM-003, V3-002).
//!
//! A result designates an exact content and configuration. The manifest
//! keeps apart two kinds of statement:
//!
//! - `artifact`: what is certain about the binary that runs. Its digest is
//!   computed from the running executable, and its build parameters were
//!   captured by the build script when it was compiled.
//! - `checkout` and `tools_at_execution`: what was observed around the
//!   binary when it ran. They do not prove that the binary was built from
//!   that checkout or with those tools.
//!
//! Proving that correspondence, keeping the modified content in the run
//! directory and recording the effective parameters belong to the lots
//! that run campaigns; see `docs/reception/open-conditions.md`.

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

fn run_bytes(program: &str, arguments: &[&str], directory: &Path) -> Result<Vec<u8>, String> {
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
    Ok(output.stdout)
}

fn run(program: &str, arguments: &[&str], directory: &Path) -> Result<String, String> {
    run_bytes(program, arguments, directory)
        .map(|bytes| String::from_utf8_lossy(&bytes).trim_end().to_owned())
}

/// One contract file: its path relative to the root, digest and size.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ContractFile {
    pub path: String,
    pub sha256: String,
    pub bytes: u64,
}

/// Lists the files that define a contract revision: the JSON files of the
/// contract directory and of its schema directory, in path order. Examples
/// illustrate the contract and are not part of it.
pub fn contract_files(root: &Path, contracts: &str) -> Result<Vec<ContractFile>, String> {
    let (parent, version) = contracts
        .rsplit_once('/')
        .ok_or_else(|| format!("{contracts}: expected <directory>/<version>"))?;
    let mut files = Vec::new();
    for directory in [contracts.to_owned(), format!("{parent}/schema/{version}")] {
        let entries =
            fs::read_dir(root.join(&directory)).map_err(|error| format!("{directory}: {error}"))?;
        for entry in entries {
            let entry = entry.map_err(|error| format!("{directory}: {error}"))?;
            let path = format!("{directory}/{}", entry.file_name().to_string_lossy());
            if !path.ends_with(".json") || !entry.path().is_file() {
                continue;
            }
            let bytes = fs::read(entry.path()).map_err(|error| format!("{path}: {error}"))?;
            files.push(ContractFile {
                sha256: sha256_hex(&bytes),
                bytes: bytes.len() as u64,
                path,
            });
        }
    }
    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(files)
}

/// The identity of a contract revision by exact content: the digest of one
/// `<sha256>  <path>` line per contract file, in path order.
pub fn contracts_digest<'a>(files: impl IntoIterator<Item = (&'a str, &'a str)>) -> String {
    let mut listing = String::new();
    for (sha256, path) in files {
        listing.push_str(sha256);
        listing.push_str("  ");
        listing.push_str(path);
        listing.push('\n');
    }
    sha256_hex(listing.as_bytes())
}

/// Digest of what differs from the revision: the tracked differences,
/// then one `<sha256>  <path>` line per untracked file, in path order.
fn modifications_digest(root: &Path) -> Result<String, String> {
    let mut content = run_bytes("git", &["diff", "HEAD", "--binary", "--", "."], root)?;
    let listing = run_bytes(
        "git",
        &[
            "ls-files",
            "--others",
            "--exclude-standard",
            "-z",
            "--",
            ".",
        ],
        root,
    )?;
    let mut untracked: Vec<String> = listing
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
        .map(|path| String::from_utf8_lossy(path).into_owned())
        .collect();
    untracked.sort();
    for path in untracked {
        let bytes = fs::read(root.join(&path)).map_err(|error| format!("{path}: {error}"))?;
        content.extend_from_slice(format!("{}  {path}\n", sha256_hex(&bytes)).as_bytes());
    }
    Ok(sha256_hex(&content))
}

/// Collects the content sections of a manifest for the workspace at `root`
/// and the contract directory `contracts`, given relative to it.
pub fn collect(root: &Path, contracts: &str) -> Result<Value, String> {
    let executable =
        std::env::current_exe().map_err(|error| format!("current executable: {error}"))?;
    let binary =
        fs::read(&executable).map_err(|error| format!("{}: {error}", executable.display()))?;
    let revision = run("git", &["rev-parse", "HEAD"], root)?;
    let status = run("git", &["status", "--porcelain", "--", "."], root)?;
    let modified: Vec<&str> = status.lines().collect();
    let files = contract_files(root, contracts)?;
    let digest = contracts_digest(
        files
            .iter()
            .map(|file| (file.sha256.as_str(), file.path.as_str())),
    );
    let listed: Vec<Value> = files
        .iter()
        .map(|file| json!({"path": file.path, "sha256": file.sha256, "bytes": file.bytes.to_string()}))
        .collect();
    let lockfile =
        fs::read(root.join("Cargo.lock")).map_err(|error| format!("Cargo.lock: {error}"))?;
    let workspace =
        fs::read(root.join("Cargo.toml")).map_err(|error| format!("Cargo.toml: {error}"))?;
    Ok(json!({
        "format": "mqttiumd.run-manifest",
        "schema_version": 1,
        "artifact": {
            "sha256": sha256_hex(&binary),
            "bytes": binary.len().to_string(),
            "built_with": {
                "rustc": env!("MQTTIUMD_LAB_BUILD_RUSTC"),
                "target": env!("MQTTIUMD_LAB_BUILD_TARGET"),
                "profile": env!("MQTTIUMD_LAB_BUILD_PROFILE"),
                "opt_level": env!("MQTTIUMD_LAB_BUILD_OPT_LEVEL"),
                "debug_assertions": cfg!(debug_assertions),
                "rustflags": env!("MQTTIUMD_LAB_BUILD_RUSTFLAGS"),
            },
        },
        "checkout": {
            "observed": "at-execution",
            "revision": revision,
            "tree_state": if modified.is_empty() { "clean" } else { "modified" },
            "modified_paths": modified,
            "modifications_sha256": modifications_digest(root)?,
            "lockfile_sha256": sha256_hex(&lockfile),
            "workspace_manifest_sha256": sha256_hex(&workspace),
        },
        "tools_at_execution": {
            "rustc": run("rustc", &["--version"], root)?,
            "cargo": run("cargo", &["--version"], root)?,
        },
        "contracts": listed,
        "contracts_digest": digest,
        "platform": {
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::{contracts_digest, sha256_hex};

    #[test]
    fn the_contract_digest_depends_on_content_path_and_order() {
        let base = contracts_digest([("aa", "x.json"), ("bb", "y.json")]);
        assert_eq!(base, contracts_digest([("aa", "x.json"), ("bb", "y.json")]));
        assert_ne!(base, contracts_digest([("ab", "x.json"), ("bb", "y.json")]));
        assert_ne!(base, contracts_digest([("aa", "z.json"), ("bb", "y.json")]));
        assert_ne!(base, contracts_digest([("bb", "y.json"), ("aa", "x.json")]));
        assert_ne!(base, contracts_digest([("aa", "x.json")]));
    }

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
