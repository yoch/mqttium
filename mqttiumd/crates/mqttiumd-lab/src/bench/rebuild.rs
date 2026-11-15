//! A campaign whose binary is built from the content it records.
//!
//! `run --rebuild` does not run the scenario with the binary that was
//! invoked. It assembles the source of the run (the `mqttiumd` tree of
//! the checked-out revision, with the modifications of the work tree
//! applied), packs it as one deterministic tar, builds `mqttiumd-lab`
//! from that tar alone, offline and with the locked dependencies, and
//! has the binary it built run the campaign. That binary copies the tar
//! into the campaign and names it in the inventory: whoever holds the
//! campaign holds the content its binary was built from, and the command
//! that builds it.
//!
//! The build maps the path of the source to a fixed prefix, so that two
//! builds of one tar on one machine and toolchain give the same binary;
//! whether they do is stated by the campaign, not assumed.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

/// Where the source is mapped in the binary it builds.
pub const SOURCE_PREFIX: &str = "/mqttiumd-source";

/// The command that builds the binary of a campaign from its `source.tar`.
#[must_use]
pub fn build_command() -> String {
    format!(
        "tar -x -f source.tar -C <dir> && cd <dir> && RUSTFLAGS='--remap-path-prefix=<dir>={SOURCE_PREFIX}' cargo build --locked --offline -p mqttiumd-lab --bin mqttiumd-lab, where <dir> is $TMPDIR/mqttiumd-source-<first 16 hex digits of the SHA-256 of source.tar>"
    )
}

/// What a rebuild produced.
#[derive(Debug)]
pub struct Rebuilt {
    /// The binary built from the tar.
    pub binary: PathBuf,
    /// The source of the build, packed.
    pub tar: PathBuf,
    /// The revision the source starts from.
    pub revision: String,
    /// The command that built the binary from the unpacked tar.
    pub build: String,
}

fn run(command: &mut Command, what: &str) -> Result<Vec<u8>, String> {
    let output = command
        .output()
        .map_err(|error| format!("{what}: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "{what}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(output.stdout)
}

/// Unpacks the tracked tree of the revision, then applies what differs
/// from it: the patch of tracked files and a copy of each untracked file.
/// `exclude` (the campaign directory, the work directory) is never copied.
fn assemble(root: &Path, source: &Path, exclude: &[PathBuf]) -> Result<String, String> {
    let revision = String::from_utf8_lossy(&run(
        Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(root),
        "git rev-parse",
    )?)
    .trim()
    .to_owned();
    fs::create_dir_all(source).map_err(|error| format!("{}: {error}", source.display()))?;
    let archive = run(
        Command::new("git")
            .args(["archive", "--format=tar", "HEAD", "."])
            .current_dir(root),
        "git archive",
    )?;
    let mut untar = Command::new("tar")
        .args(["-x", "-f", "-", "-C"])
        .arg(source)
        .stdin(Stdio::piped())
        .spawn()
        .map_err(|error| format!("tar: {error}"))?;
    {
        use std::io::Write;
        let mut input = untar.stdin.take().ok_or("tar: no input")?;
        input
            .write_all(&archive)
            .map_err(|error| format!("tar: {error}"))?;
    }
    let status = untar.wait().map_err(|error| format!("tar: {error}"))?;
    if !status.success() {
        return Err("tar: the archive of the revision was not unpacked".to_owned());
    }
    let patch = run(
        Command::new("git")
            .args([
                "diff",
                "--binary",
                "--no-renames",
                "--no-ext-diff",
                "--no-textconv",
                "--relative",
                "HEAD",
                "--",
                ".",
            ])
            .current_dir(root),
        "git diff",
    )?;
    if !patch.is_empty() {
        let file = source.join(".rebuild.patch");
        fs::write(&file, &patch).map_err(|error| format!("{}: {error}", file.display()))?;
        run(
            Command::new("git")
                .args(["apply", "--whitespace=nowarn", ".rebuild.patch"])
                .current_dir(source),
            "git apply",
        )?;
        fs::remove_file(&file).map_err(|error| format!("{}: {error}", file.display()))?;
    }
    let listing = run(
        Command::new("git")
            .args([
                "-c",
                "core.excludesFile=/dev/null",
                "ls-files",
                "--others",
                "--exclude-standard",
                "-z",
                "--",
                ".",
            ])
            .current_dir(root),
        "git ls-files",
    )?;
    let excluded: Vec<PathBuf> = exclude
        .iter()
        .filter_map(|path| path.canonicalize().ok())
        .collect();
    for path in listing
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
    {
        let path = String::from_utf8_lossy(path).into_owned();
        let from = root.join(&path);
        let Ok(canonical) = from.canonicalize() else {
            continue;
        };
        if excluded.iter().any(|out| canonical.starts_with(out)) || !canonical.is_file() {
            continue;
        }
        let to = source.join(&path);
        if let Some(parent) = to.parent() {
            fs::create_dir_all(parent).map_err(|error| format!("{}: {error}", parent.display()))?;
        }
        fs::copy(&from, &to).map_err(|error| format!("{}: {error}", from.display()))?;
    }
    Ok(revision)
}

/// Packs `source` as a tar whose bytes depend on its content only.
fn pack(source: &Path, tar: &Path) -> Result<(), String> {
    run(
        Command::new("tar")
            .args([
                "--sort=name",
                "--mtime=@0",
                "--owner=0",
                "--group=0",
                "--numeric-owner",
                "--format=gnu",
                "-c",
                "-f",
            ])
            .arg(tar)
            .arg("-C")
            .arg(source)
            .arg("."),
        "tar",
    )
    .map(|_| ())
}

/// Builds `mqttiumd-lab` from the unpacked tar `tar`, in `work`.
fn build(tar: &Path, work: &Path) -> Result<(PathBuf, String), String> {
    // The tree is named by the digest of the tar: its path enters the
    // flags of the build, which the binary records, so that one content
    // gives one path and one set of flags.
    let digest = fs::File::open(tar)
        .and_then(crate::files::hash_stream)
        .map_err(|error| format!("{}: {error}", tar.display()))?
        .0;
    let tree = std::env::temp_dir().join(format!("mqttiumd-source-{}", &digest[..16]));
    let _ = fs::remove_dir_all(&tree);
    fs::create_dir_all(&tree).map_err(|error| format!("{}: {error}", tree.display()))?;
    run(
        Command::new("tar")
            .args(["-x", "-f"])
            .arg(tar)
            .arg("-C")
            .arg(&tree),
        "tar",
    )?;
    let target = tree.join("target");
    let flags = format!("--remap-path-prefix={}={SOURCE_PREFIX}", tree.display());
    let command = build_command();
    run(
        Command::new("cargo")
            .args([
                "build",
                "--locked",
                "--offline",
                "-q",
                "-p",
                "mqttiumd-lab",
                "--bin",
                "mqttiumd-lab",
            ])
            .env("CARGO_TARGET_DIR", &target)
            .env("RUSTFLAGS", &flags)
            .env_remove("CARGO_ENCODED_RUSTFLAGS")
            .current_dir(&tree),
        "cargo build",
    )?;
    let built = target.join("debug").join("mqttiumd-lab");
    let binary = work.join("mqttiumd-lab");
    fs::copy(&built, &binary).map_err(|error| format!("{}: {error}", built.display()))?;
    let _ = fs::remove_dir_all(&tree);
    Ok((binary, command))
}

/// Assembles the source of the run, packs it, and builds the binary.
/// `work` is a directory of its own, removed by the caller.
pub fn rebuild(root: &Path, work: &Path, exclude: &[PathBuf]) -> Result<Rebuilt, String> {
    let source = work.join("source");
    let revision = assemble(root, &source, exclude)?;
    let tar = work.join("source.tar");
    pack(&source, &tar)?;
    let (binary, build) = build(&tar, work)?;
    Ok(Rebuilt {
        binary,
        tar,
        revision,
        build,
    })
}
