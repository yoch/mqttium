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

use crate::files;

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
/// illustrate the contract and are not part of it. Each file goes through
/// the bounded, confined reader.
pub fn contract_files(root: &Path, contracts: &Path) -> Result<Vec<ContractFile>, String> {
    let canonical_root = root
        .canonicalize()
        .map_err(|error| format!("{}: {error}", root.display()))?;
    let (contract_dir, schema_dir) = files::contract_directories(contracts)?;
    let mut listed = Vec::new();
    for directory in [contract_dir, schema_dir] {
        let entries =
            fs::read_dir(root.join(&directory)).map_err(|error| format!("{directory}: {error}"))?;
        for entry in entries {
            let entry = entry.map_err(|error| format!("{directory}: {error}"))?;
            let path = format!("{directory}/{}", entry.file_name().to_string_lossy());
            if !path.ends_with(".json") {
                continue;
            }
            let loaded = files::read_document(&canonical_root, &entry.path())
                .map_err(|(code, message)| format!("{code}: {message}"))?;
            listed.push(ContractFile {
                sha256: sha256_hex(&loaded.bytes),
                bytes: loaded.bytes.len() as u64,
                path,
            });
        }
    }
    listed.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(listed)
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

/// What one path of the work tree holds, without loading it.
fn describe(root: &Path, path: &str) -> Result<String, String> {
    let full = root.join(path);
    let Ok(metadata) = fs::symlink_metadata(&full) else {
        return Ok("deleted".to_owned());
    };
    if metadata.is_symlink() {
        let target = fs::read_link(&full).map_err(|error| format!("{path}: {error}"))?;
        return Ok(format!("link:{}", target.to_string_lossy()));
    }
    if !metadata.is_file() {
        return Ok("special".to_owned());
    }
    fs::File::open(&full)
        .and_then(files::hash_stream)
        .map(|(digest, _)| digest)
        .map_err(|error| format!("{path}: {error}"))
}

/// A path that holds no file, to stand in for the user's exclude file.
const NO_FILE: &str = if cfg!(windows) { "NUL" } else { "/dev/null" };

/// The paths below `root` that differ from the revision or are untracked,
/// in path order, each once.
///
/// The list is defined by paths, not by what git would display. Rename
/// detection is off: a renamed file is one path that disappeared and one
/// that appeared, whatever `diff.renames` says. External diff drivers and
/// text conversions are off. The user-level exclude file is ignored: it is
/// configuration of a person, not content of the tree.
///
/// What git itself considers modified still applies: the `.gitignore`
/// files of the tree, the exclude file of the clone, and the file-mode and
/// line-ending settings of the repository.
fn modified_paths(root: &Path) -> Result<Vec<String>, String> {
    let mut listing = run_bytes(
        "git",
        &[
            "diff",
            "--name-only",
            "--no-renames",
            "--no-ext-diff",
            "--no-textconv",
            "--relative",
            "-z",
            "HEAD",
            "--",
            ".",
        ],
        root,
    )?;
    let exclude = format!("core.excludesFile={NO_FILE}");
    listing.extend(run_bytes(
        "git",
        &[
            "-c",
            &exclude,
            "ls-files",
            "--others",
            "--exclude-standard",
            "-z",
            "--",
            ".",
        ],
        root,
    )?);
    let mut paths: Vec<String> = listing
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
        .map(|path| String::from_utf8_lossy(path).into_owned())
        .collect();
    paths.sort();
    paths.dedup();
    Ok(paths)
}

/// Digest of what differs from the revision: one `<content>  <path>` line
/// per path of [`modified_paths`]. The content is the SHA-256 of a regular
/// file, `link:<target>` for a symbolic link, `deleted` for a missing path.
fn modifications_digest(root: &Path) -> Result<String, String> {
    let mut content = String::new();
    for path in modified_paths(root)? {
        content.push_str(&format!("{}  {path}\n", describe(root, &path)?));
    }
    Ok(sha256_hex(content.as_bytes()))
}

/// Collects the content sections of a manifest for the workspace at `root`
/// and the contract directory `contracts`, given relative to it.
pub fn collect(root: &Path, contracts: &Path) -> Result<Value, String> {
    let executable =
        std::env::current_exe().map_err(|error| format!("current executable: {error}"))?;
    let (binary_digest, binary_length) =
        fs::File::open(&executable)
            .and_then(files::hash_stream)
            .map_err(|error| format!("{}: {error}", executable.display()))?;
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
            "sha256": binary_digest,
            "bytes": binary_length.to_string(),
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
    use std::fs;

    use std::path::{Path, PathBuf};
    use std::process::Command;

    use super::{contracts_digest, describe, modifications_digest, modified_paths, sha256_hex};

    fn git(root: &Path, arguments: &[&str]) {
        let status = Command::new("git")
            .args(arguments)
            .current_dir(root)
            .status()
            .expect("git runs");
        assert!(status.success(), "git {arguments:?}");
    }

    /// A repository with one commit holding `old.rs`.
    fn repository(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!("mqttiumd-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("repository directory");
        git(&root, &["init", "-q"]);
        git(&root, &["config", "user.name", "test"]);
        git(&root, &["config", "user.email", "test@example.invalid"]);
        git(&root, &["config", "commit.gpgsign", "false"]);
        fs::write(
            root.join("old.rs"),
            "pub fn kept() -> u8 {\n    7\n}\n".repeat(8),
        )
        .expect("file");
        git(&root, &["add", "old.rs"]);
        git(&root, &["commit", "-q", "-m", "one file"]);
        root
    }

    #[test]
    fn the_modifications_digest_does_not_depend_on_rename_detection() {
        let root = repository("renames");
        git(&root, &["mv", "old.rs", "new.rs"]);
        git(&root, &["config", "diff.renames", "true"]);
        let detected = modifications_digest(&root).expect("digest");
        let paths_detected = modified_paths(&root).expect("paths");
        git(&root, &["config", "diff.renames", "false"]);
        let undetected = modifications_digest(&root).expect("digest");
        let paths_undetected = modified_paths(&root).expect("paths");
        fs::remove_dir_all(&root).expect("cleanup");
        assert_eq!(detected, undetected, "one work tree, two digests");
        assert_ne!(detected, sha256_hex(b""), "the rename is a modification");
        // A rename is one path gone and one path new, under both settings.
        assert_eq!(paths_detected, vec!["new.rs", "old.rs"]);
        assert_eq!(paths_undetected, paths_detected);
    }

    #[test]
    fn the_modifications_digest_does_not_depend_on_a_user_exclude_file() {
        let root = repository("excludes");
        fs::write(root.join("scratch.tmp"), b"untracked").expect("untracked file");
        let listed = modifications_digest(&root).expect("digest");
        // A user-level ignore file is configuration, not content of the tree.
        let ignore = root.join(".git").join("user-ignore");
        fs::write(&ignore, "*.tmp\n").expect("ignore file");
        git(
            &root,
            &["config", "core.excludesFile", &ignore.to_string_lossy()],
        );
        let ignored = modifications_digest(&root).expect("digest");
        let paths = modified_paths(&root).expect("paths");
        fs::remove_dir_all(&root).expect("cleanup");
        assert_eq!(listed, ignored, "one work tree, two digests");
        assert_eq!(paths, vec!["scratch.tmp"]);
    }

    #[cfg(unix)]
    #[test]
    fn every_kind_of_path_is_described_without_being_followed_or_loaded() {
        use std::os::unix::fs::symlink;

        let root = std::env::temp_dir().join(format!("mqttiumd-describe-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("directory")).expect("directory");
        fs::write(root.join("file"), b"abc").expect("file");
        symlink("nowhere", root.join("dangling")).expect("dangling link");
        symlink("directory", root.join("to-directory")).expect("link to a directory");
        let described: Vec<String> = ["file", "dangling", "to-directory", "directory", "missing"]
            .iter()
            .map(|path| describe(&root, path).expect("described"))
            .collect();
        fs::remove_dir_all(&root).expect("cleanup");
        assert_eq!(
            described,
            vec![
                sha256_hex(b"abc").as_str(),
                "link:nowhere",
                "link:directory",
                "special",
                "deleted"
            ]
        );
    }

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
