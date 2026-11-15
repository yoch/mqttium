//! A campaign directory: everything one run used and produced, so that
//! somebody else can judge it and play it again.
//!
//! The run reads its contracts, its scenario and its fixture from copies
//! it makes first in the campaign directory, and from nothing else: the
//! bytes it hashes, the bytes it interprets and the bytes it leaves for
//! the oracle are the same, and nothing changes them under the run. The
//! oracle judges the directory with `--root` set to it.
//!
//! ```text
//! <campaign>/
//!   contracts/v1/*.json, contracts/schema/v1/*.json   the contracts read
//!   <scenario path>, <fixture path>                   as the checkout holds them
//!   trace.jsonl                                       the trace the collector wrote
//!   snapshots/<checkpoint>.snapshot.json              written during the run
//!   choices.log                                       one elementary choice per line
//!   run-manifest.json                                 mqttiumd.run-manifest
//!   run-result.json                                   provisional, never PASS
//!   modified/                                         what differs from the revision
//!   campaign.json                                     inventory, digests, commands
//! ```
//!
//! The runner computes no verdict: the provisional result says that the
//! run was not judged, and the oracle's verdict is the only one.

use std::fs;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::{Value, json};

use super::collector::{Collector, KindTable};
use super::mqtt::transport::Bounds;
use super::process::{Settings, Stop, owner_config};
use super::runner::{Outcome, Plan, Population, SCHEDULER_VERSION, run};
use super::scenario::Scenario;
use crate::files;
use crate::manifest::{self, sha256_hex};
use crate::store::adapter::AdapterLimits;
use crate::store::durable::{MAX_CLIENT_ID_BYTES, MemoryStore};
use crate::store::fixture;
use crate::strict;
use crate::workspace::Workspace;

/// The resolution delay of the owner, in logical time units. The
/// specification fixes none for S0; the corpus uses 30.
pub const RESOLUTION_DELAY: u64 = 30;

/// What a campaign is asked to do.
#[derive(Clone, Debug)]
pub struct Request {
    /// The checkout the contracts and the scenario are read from.
    pub root: PathBuf,
    pub contracts: PathBuf,
    pub scenario: PathBuf,
    pub out: PathBuf,
    pub seed: u64,
    /// The slug the run is named by; derived from the scenario and the
    /// seed when absent.
    pub run_id: Option<String>,
}

/// What a campaign left, for the command line.
#[derive(Debug)]
pub struct Report {
    pub out: PathBuf,
    pub run_id: String,
    pub contracts_digest: String,
    pub stopped: Option<String>,
    pub not_reached: Vec<String>,
    pub judge: String,
}

fn io(path: &Path, error: std::io::Error) -> String {
    format!("{}: {error}", path.display())
}

fn write(out: &Path, relative: &str, bytes: &[u8]) -> Result<(), String> {
    let path = out.join(relative);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| io(parent, error))?;
    }
    fs::write(&path, bytes).map_err(|error| io(&path, error))
}

/// Copies one document of the checkout into the campaign, through the
/// bounded, confined reader, at the same relative path.
fn copy(canonical_root: &Path, root: &Path, relative: &str, out: &Path) -> Result<Vec<u8>, String> {
    if !crate::workspace::is_contained(relative) {
        return Err(format!("{relative:?} is not a path below the root"));
    }
    let loaded = files::read_document(canonical_root, &root.join(relative))
        .map_err(|(code, message)| format!("{code}: {message}"))?;
    write(out, relative, &loaded.bytes)?;
    Ok(loaded.bytes)
}

fn copy_contracts(root: &Path, contracts: &Path, out: &Path) -> Result<(), String> {
    let canonical_root = root.canonicalize().map_err(|error| io(root, error))?;
    let (contract_dir, schema_dir) = files::contract_directories(contracts)?;
    for directory in [contract_dir, schema_dir] {
        let entries =
            fs::read_dir(root.join(&directory)).map_err(|error| format!("{directory}: {error}"))?;
        let mut names: Vec<String> = entries
            .map(|entry| entry.map(|entry| entry.file_name().to_string_lossy().into_owned()))
            .collect::<Result<_, _>>()
            .map_err(|error| format!("{directory}: {error}"))?;
        names.sort();
        for name in names.iter().filter(|name| name.ends_with(".json")) {
            copy(&canonical_root, root, &format!("{directory}/{name}"), out)?;
        }
    }
    Ok(())
}

fn relative(path: &Path) -> Result<String, String> {
    let text = path
        .to_str()
        .ok_or_else(|| format!("{} is not UTF-8", path.display()))?
        .trim_start_matches("./")
        .to_owned();
    if crate::workspace::is_contained(&text) {
        Ok(text)
    } else {
        Err(format!("{text:?} is not a path below the root"))
    }
}

fn artifact(out: &Path, relative: &str) -> Result<Value, String> {
    let path = out.join(relative);
    let (sha256, bytes) = fs::File::open(&path)
        .and_then(files::hash_stream)
        .map_err(|error| io(&path, error))?;
    Ok(json!({"path": relative, "sha256": sha256, "bytes": bytes.to_string()}))
}

fn git(root: &Path, arguments: &[&str]) -> Result<Vec<u8>, String> {
    let output = Command::new("git")
        .args(arguments)
        .current_dir(root)
        .output()
        .map_err(|error| format!("git: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "git {}: {}",
            arguments.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(output.stdout)
}

/// Keeps what differs from the revision in the campaign: the patch of
/// tracked files, and a copy of each untracked file outside the
/// campaign. A run on a modified tree can be rebuilt from its own
/// directory: check out the revision, apply the patch, copy the files.
fn keep_modifications(root: &Path, out: &Path) -> Result<Vec<String>, String> {
    let patch = git(
        root,
        &[
            "diff",
            "--binary",
            "--no-renames",
            "--no-ext-diff",
            "--no-textconv",
            "--relative",
            "HEAD",
            "--",
            ".",
        ],
    )?;
    let mut kept = Vec::new();
    if !patch.is_empty() {
        write(out, "modified/tracked.patch", &patch)?;
        kept.push("modified/tracked.patch".to_owned());
    }
    let listing = git(
        root,
        &[
            "-c",
            "core.excludesFile=/dev/null",
            "ls-files",
            "--others",
            "--exclude-standard",
            "-z",
            "--",
            ".",
        ],
    )?;
    let canonical_out = out.canonicalize().map_err(|error| io(out, error))?;
    for path in listing
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
    {
        let path = String::from_utf8_lossy(path).into_owned();
        let source = root.join(&path);
        let Ok(canonical) = source.canonicalize() else {
            continue;
        };
        if canonical.starts_with(&canonical_out) || !canonical.is_file() {
            continue;
        }
        let bytes = fs::read(&source).map_err(|error| io(&source, error))?;
        let target = format!("modified/untracked/{path}");
        write(out, &target, &bytes)?;
        kept.push(target);
    }
    Ok(kept)
}

/// The digest of a set of files of the checkout: one `<sha256>  <path>`
/// line each, in path order. Missing files are named as missing.
fn identify(root: &Path, paths: &[String]) -> Value {
    let mut listed = Vec::new();
    let mut lines = String::new();
    for path in paths {
        let digest = fs::File::open(root.join(path))
            .and_then(files::hash_stream)
            .map(|(digest, _)| digest)
            .unwrap_or_else(|_| "missing".to_owned());
        lines.push_str(&format!("{digest}  {path}\n"));
        listed.push(json!({"path": path, "sha256": digest}));
    }
    json!({"sha256": sha256_hex(lines.as_bytes()), "files": listed})
}

fn python_files(root: &Path, directory: &str) -> Vec<String> {
    let mut paths: Vec<String> = fs::read_dir(root.join(directory))
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.ends_with(".py"))
        .map(|name| format!("{directory}/{name}"))
        .collect();
    paths.sort();
    paths
}

fn slug(text: &str) -> String {
    let mut slug: String = text
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    slug.truncate(48);
    if !slug.starts_with(|character: char| character.is_ascii_lowercase()) {
        slug.insert(0, 'r');
    }
    slug
}

fn capacity(fixture: &Value, name: &str) -> Result<u64, String> {
    fixture
        .pointer(&format!("/capacities/{name}"))
        .and_then(Value::as_str)
        .ok_or_else(|| format!("the fixture has no capacity {name}"))
        .and_then(|text| {
            mqttiumd_contract::decimal::parse_u64(text)
                .map_err(|error| format!("{name}: {error:?}"))
        })
}

fn narrow<T: TryFrom<u64>>(value: u64, name: &str) -> Result<T, String> {
    T::try_from(value).map_err(|_| format!("{name} does not fit"))
}

/// The process under test as a fixture sets it up: its settings, the
/// data set installed through store commands, and the clients.
pub struct Setup {
    pub settings: Settings,
    pub store: MemoryStore,
    pub population: Population,
}

impl Setup {
    /// Reads a fixture document (already validated) and installs it.
    pub fn from_fixture(
        fixture_document: &Value,
        run_id: &str,
        seed: u64,
        faults: usize,
    ) -> Result<Self, String> {
        let plan = fixture::plan(fixture_document).map_err(|error| format!("{error:?}"))?;
        let mut store = MemoryStore::create(plan.incarnation, plan.limits);
        let installed =
            fixture::install(&mut store, &plan).map_err(|error| format!("{error:?}"))?;
        let population = Population {
            clients: plan
                .sessions
                .iter()
                .map(|session| {
                    installed
                        .sessions
                        .get(&session.client)
                        .map(|id| (session.client.clone(), session.client_id.clone(), *id))
                        .ok_or_else(|| format!("{} has no session", session.client))
                })
                .collect::<Result<_, _>>()?,
        };
        let limits = plan.limits;
        let owner = owner_config(
            narrow(limits.max_sessions as u64, "max_sessions")?,
            narrow(limits.max_route_targets as u64, "max_route_targets")?,
            narrow(limits.exchange_slots.get(), "exchange_slots")?,
            limits.payload_bytes.get(),
            RESOLUTION_DELAY,
        )
        .map_err(|Stop(reason)| reason)?;
        let effect_slots: usize =
            narrow(capacity(fixture_document, "effect_slots")?, "effect_slots")?;
        let effect_slots = NonZeroUsize::new(effect_slots).ok_or("no effect slot")?;
        let reservations = NonZeroU32::new(narrow(effect_slots.get() as u64, "reservations")?)
            .ok_or("no reservation")?;
        let leaked: &'static str = Box::leak(run_id.to_owned().into_boxed_str());
        let settings = Settings {
            run_id: leaked,
            seed,
            resolution_delay: RESOLUTION_DELAY,
            work_limit: 4096,
            recovery_page: 64,
            effect_slots,
            reservations,
            adapter: AdapterLimits {
                operations_in_flight: plan.operations_in_flight,
                result_slots: plan.result_slots,
                faults: faults.max(1),
                events: 4096,
            },
            owner,
            bounds: Bounds {
                max_client_id: MAX_CLIENT_ID_BYTES,
                max_topic: limits.max_topic_bytes,
                max_payload: limits.max_payload_bytes,
            },
        };
        Ok(Self {
            settings,
            store,
            population,
        })
    }
}

/// Runs one campaign. An error is returned when nothing could be run;
/// a run that stopped still leaves its directory.
pub fn execute(request: &Request) -> Result<Report, String> {
    let out = &request.out;
    if out.exists()
        && fs::read_dir(out)
            .map_err(|error| io(out, error))?
            .next()
            .is_some()
    {
        return Err(format!("{} exists and is not empty", out.display()));
    }
    fs::create_dir_all(out).map_err(|error| io(out, error))?;
    let root = &request.root;
    let canonical_root = root.canonicalize().map_err(|error| io(root, error))?;
    let contracts = relative(&request.contracts)?;
    let scenario_path = relative(&request.scenario)?;

    // One read of every input, into the campaign; then the campaign is
    // the root the run reads.
    copy_contracts(root, Path::new(&contracts), out)?;
    let scenario_bytes = copy(&canonical_root, root, &scenario_path, out)?;
    let scenario_document = strict::parse(&scenario_bytes).map_err(|error| error.to_string())?;
    let fixture_path = scenario_document
        .get("initial_fixture")
        .and_then(Value::as_str)
        .ok_or("the scenario names no fixture")?
        .to_owned();
    copy(&canonical_root, root, &fixture_path, out)?;
    let workspace =
        Workspace::open(out, Path::new(&contracts)).map_err(|error| error.to_string())?;
    let report = workspace.check_file(Path::new(&scenario_path));
    if !report.is_accepted() {
        let first = report
            .diagnostics
            .first()
            .map(ToString::to_string)
            .unwrap_or_default();
        return Err(format!("{scenario_path} is refused: {first}"));
    }
    let scenario = Scenario::read(&scenario_document)?;
    if scenario.perimeter != "full" {
        return Err(format!(
            "the runner plays full scenarios; {} is {}",
            scenario.id, scenario.perimeter
        ));
    }
    let fixture_bytes = fs::read(out.join(&fixture_path)).map_err(|error| io(out, error))?;
    let fixture_document = strict::parse(&fixture_bytes).map_err(|error| error.to_string())?;
    let run_id = request
        .run_id
        .clone()
        .unwrap_or_else(|| slug(&format!("{}-seed-{}", scenario.id, request.seed)));
    let Setup {
        settings,
        store,
        population,
    } = Setup::from_fixture(
        &fixture_document,
        &run_id,
        request.seed,
        scenario.faults.len(),
    )?;
    let schema_path = out.join(format!(
        "{}/trace-record.schema.json",
        files::contract_directories(Path::new(&contracts))?.1
    ));
    let schema = strict::parse(&fs::read(&schema_path).map_err(|error| io(&schema_path, error))?)
        .map_err(|error| error.to_string())?;
    let kinds = KindTable::from_schema(&schema)?;
    let collector = Collector::new(&run_id, kinds, scenario.max_trace_bytes);
    let outcome: Outcome = run(Plan {
        scenario: &scenario,
        settings,
        population,
        store,
        collector,
    });

    // The evidence, as it was collected.
    write(out, "trace.jsonl", &outcome.trace)?;
    for (path, bytes) in &outcome.snapshots {
        write(out, path, bytes)?;
    }
    let mut log = outcome.choices.join("\n");
    log.push('\n');
    write(out, "choices.log", log.as_bytes())?;
    let modified = keep_modifications(root, out)?;

    let out_text = out.display().to_string();
    let replay = format!(
        "cargo run -p mqttiumd-lab --locked -- run --contracts {contracts} --scenario {scenario_path} --seed {} --run-id {run_id} --out <empty directory>",
        request.seed
    );
    let judge = format!(
        "PYTHONPATH=tests/oracle python -m mqttiumd_oracle --root {out_text} --contracts {contracts} judge --scenario {scenario_path} --trace trace.jsonl"
    );

    // The manifest: the binary, the checkout, and the parameters.
    let mut run_manifest = manifest::collect(root, Path::new(&contracts))?;
    run_manifest["scenario"] = artifact(out, &scenario_path)?;
    run_manifest["fixture"] = artifact(out, &fixture_path)?;
    run_manifest["backend"] = json!("sim-store");
    run_manifest["failure_model"] = json!(scenario.failure_model());
    run_manifest["scheduler"] = json!({
        "version": SCHEDULER_VERSION.to_string(),
        "seed": request.seed.to_string(),
    });
    run_manifest["commands"] = json!([replay, judge]);
    let mut text = serde_json::to_vec_pretty(&run_manifest).map_err(|error| error.to_string())?;
    text.push(b'\n');
    write(out, "run-manifest.json", &text)?;

    // The provisional result: the bench judged nothing.
    let mut reasons =
        vec!["the bench computes no verdict: judge the campaign with the oracle".to_owned()];
    if let Some(stopped) = &outcome.stopped {
        reasons.push(format!("the run stopped: {stopped}"));
    }
    for fault in &outcome.not_reached {
        reasons.push(format!("not reached: {fault}"));
    }
    reasons.truncate(256);
    let faults: Vec<Value> = scenario
        .faults
        .iter()
        .zip(&outcome.hits)
        .map(|(fault, hits)| {
            json!({"id": fault.id, "expected_hits": fault.expected_hits.to_string(), "hits": hits.to_string()})
        })
        .collect();
    let assertions: Vec<Value> = scenario
        .assertions
        .iter()
        .map(|name| json!({"name": name, "status": "INCONCLUSIVE", "detail": "not judged by the bench"}))
        .collect();
    let mut artifacts = vec![artifact(out, "trace.jsonl")?];
    for (path, _) in &outcome.snapshots {
        artifacts.push(artifact(out, path)?);
    }
    let result = json!({
        "format": "mqttiumd.run-result",
        "schema_version": 1,
        "qualification": "provisional",
        "run_id": run_id,
        "verdict": "INCONCLUSIVE",
        "exit_code": 2,
        "scope": format!("{} on the deterministic store, simulated process crash, seed {}", scenario.id, request.seed),
        "reasons": reasons,
        "assertions": assertions,
        "faults": faults,
        "artifacts": artifacts,
    });
    let mut text = serde_json::to_vec_pretty(&result).map_err(|error| error.to_string())?;
    text.push(b'\n');
    write(out, "run-result.json", &text)?;

    // Both documents are checked against the contracts of the campaign.
    for document in ["run-manifest.json", "run-result.json"] {
        let report = workspace.check_file(Path::new(document));
        if !report.is_accepted() {
            let first = report
                .diagnostics
                .first()
                .map(ToString::to_string)
                .unwrap_or_default();
            return Err(format!(
                "{document} written by the bench is refused: {first}"
            ));
        }
    }

    // The inventory: every artefact with its digest, and what the
    // campaign covers.
    let mut inventory = Vec::new();
    let mut paths = vec![
        "trace.jsonl".to_owned(),
        "choices.log".to_owned(),
        "run-manifest.json".to_owned(),
        "run-result.json".to_owned(),
        scenario_path.clone(),
        fixture_path.clone(),
    ];
    paths.extend(outcome.snapshots.iter().map(|(path, _)| path.clone()));
    paths.extend(modified.iter().cloned());
    let (contract_dir, schema_dir) = files::contract_directories(Path::new(&contracts))?;
    for directory in [&contract_dir, &schema_dir] {
        let mut names: Vec<String> = fs::read_dir(out.join(directory))
            .map_err(|error| format!("{directory}: {error}"))?
            .filter_map(Result::ok)
            .map(|entry| format!("{directory}/{}", entry.file_name().to_string_lossy()))
            .collect();
        names.sort();
        paths.extend(names);
    }
    for path in &paths {
        inventory.push(artifact(out, path)?);
    }
    let mut oracle_files = python_files(root, "tests/oracle/mqttiumd_oracle");
    oracle_files.push("tests/oracle/requirements.txt".to_owned());
    let campaign = json!({
        "format": "mqttiumd-lab.campaign",
        "version": 1,
        "run_id": run_id,
        "scenario": scenario.id,
        "covers": {
            "backend": "sim-store: the deterministic store of the bench, in memory",
            "crash": "simulated: the process under test is dropped and its store reopened from its durable state, in the same operating-system process",
            "perimeter": "full: client bytes through the transport of the bench, decoded by independent peers",
            "start_up": "the real path: EpochOpening through the store adapter, EpochGrant, OwnerAuthority::from_grant, one box for owner 0, Owner::new",
        },
        "lab": run_manifest["artifact"].clone(),
        "contracts_digest": workspace.contracts_digest(),
        "oracle": identify(root, &oracle_files),
        "generators": identify(root, &[
            "tests/oracle/build_traces.py".to_owned(),
            "tools/build_corpus.py".to_owned(),
        ]),
        "seed": request.seed.to_string(),
        "scheduler_version": SCHEDULER_VERSION.to_string(),
        "resolution_delay": RESOLUTION_DELAY.to_string(),
        "elementary_choices": outcome.elementary.to_string(),
        "steps_played": outcome.steps_played,
        "steps": scenario.steps.len(),
        "stopped": outcome.stopped,
        "faults_not_reached": outcome.not_reached,
        "records_refused": outcome.refused,
        "events_without_record": outcome.unwritten,
        "modified": modified,
        "commands": {"replay": replay, "judge": judge},
        "limits": [
            "the binary is identified by its digest; that it was built from the recorded revision and modifications is not established",
            "the opening of an epoch has no trace record in the formats; it is in the choice log only",
            "a fault at the fence point, or on the result of a resolution, selects every resolution: the store adapter gives a resolution no source",
            "the scenario vocabulary has no step that leaves a barrier: the runner keeps the store adapter at its barrier while the next steps only inspect the store or crash the broker, and leaves it before any other step",
            "the authority of the runner keeps one owner active: S0 runs owner 0 only",
        ],
        "artifacts": inventory,
    });
    let mut text = serde_json::to_vec_pretty(&campaign).map_err(|error| error.to_string())?;
    text.push(b'\n');
    write(out, "campaign.json", &text)?;

    Ok(Report {
        out: out.clone(),
        run_id,
        contracts_digest: workspace.contracts_digest().to_owned(),
        stopped: outcome.stopped,
        not_reached: outcome.not_reached,
        judge,
    })
}
