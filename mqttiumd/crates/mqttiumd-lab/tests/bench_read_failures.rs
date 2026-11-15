//! Real child/supervisor reception of semantic SQLite read failures.
//!
//! Each case copies a closed, checkpointed fixture. Faults here are deliberate
//! storage-image mutations, not process crashes or qualifying business runs.

#[path = "bench_support.rs"]
mod bench_support;

use std::cell::Cell;
use std::fs::{self, OpenOptions};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use mqttiumd_contract::ids::StoreIncarnation;
use mqttiumd_lab::bench::backend::{Backend, Sqlite};
use mqttiumd_lab::bench::campaign::Setup;
use mqttiumd_lab::bench::child::Config;
use mqttiumd_lab::bench::collector::{Collector, KindTable};
use mqttiumd_lab::bench::runner::{Plan, run};
use mqttiumd_lab::bench::scenario::Scenario;
use mqttiumd_lab::store::durable::{DurableState, Limits};
use mqttiumd_store_api::ReadError;
use mqttiumd_store_sqlite::{SqliteStore, writer_lock_path};
use rusqlite::{Connection, OpenFlags, types::Value as SqlValue};
use serde_json::{Value, json};

const SCENARIO: &str = "tests/scenarios/p1/P1-02.json";
const FIXTURE: &str = "tests/fixtures/p1/three-persistent-sessions.json";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mutation {
    Route,
    ClientLimit,
    Meta,
    Access,
    InspectRoute,
    InspectAccess,
    Healthy,
}

#[derive(Clone, Copy)]
struct Case {
    name: &'static str,
    mutation: Mutation,
    page: u32,
    code: Option<&'static str>,
    phase: Option<&'static str>,
    valid_pages: usize,
}

const CASES: [Case; 8] = [
    Case {
        name: "route-before-first-page",
        mutation: Mutation::Route,
        page: 64,
        code: Some("InvalidData"),
        phase: Some("Recovery"),
        valid_pages: 0,
    },
    Case {
        name: "route-after-valid-page",
        mutation: Mutation::Route,
        page: 3,
        code: Some("InvalidData"),
        phase: Some("Recovery"),
        valid_pages: 1,
    },
    Case {
        name: "client-id-over-limit",
        mutation: Mutation::ClientLimit,
        page: 64,
        code: Some("LimitExceeded"),
        phase: Some("Recovery"),
        valid_pages: 0,
    },
    Case {
        name: "meta-before-started",
        mutation: Mutation::Meta,
        page: 64,
        code: Some("InvalidData"),
        phase: Some("Open"),
        valid_pages: 0,
    },
    Case {
        name: "access-before-started",
        mutation: Mutation::Access,
        page: 64,
        code: Some("Unavailable"),
        phase: Some("Open"),
        valid_pages: 0,
    },
    Case {
        name: "external-inspector-invalid",
        mutation: Mutation::InspectRoute,
        page: 64,
        code: Some("InvalidData"),
        phase: Some("Inspect"),
        valid_pages: 1,
    },
    Case {
        name: "external-inspector-unavailable",
        mutation: Mutation::InspectAccess,
        page: 64,
        code: Some("Unavailable"),
        phase: Some("Inspect"),
        valid_pages: 1,
    },
    Case {
        name: "healthy-child",
        mutation: Mutation::Healthy,
        page: 64,
        code: None,
        phase: None,
        valid_pages: 0,
    },
];

fn read_json(path: &Path) -> Value {
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
}
fn write_json(path: &Path, value: &Value) {
    fs::write(path, serde_json::to_vec_pretty(value).unwrap()).unwrap();
}

fn copy_document(root: &Path, out: &Path, relative: &str) {
    let target = out.join(relative);
    fs::create_dir_all(target.parent().unwrap()).unwrap();
    fs::copy(root.join(relative), target).unwrap();
}

fn copy_inputs(root: &Path, out: &Path) {
    for directory in ["contracts/v1", "contracts/schema/v1"] {
        for entry in fs::read_dir(root.join(directory)).unwrap() {
            let name = entry.unwrap().file_name();
            if name.to_str().unwrap().ends_with(".json") {
                copy_document(
                    root,
                    out,
                    &format!("{directory}/{}", name.to_str().unwrap()),
                );
            }
        }
    }
    copy_document(root, out, SCENARIO);
    copy_document(root, out, FIXTURE);
}

fn quote(path: &Path) -> String {
    format!("'{}'", path.to_str().unwrap().replace('\'', "'\"'\"'"))
}

fn executable(out: &Path) -> PathBuf {
    let path = out.join("record-pid.sh");
    fs::write(
        &path,
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$$\" >> {}\nexec {} \"$@\"\n",
            quote(&out.join("pids.txt")),
            quote(Path::new(env!("CARGO_BIN_EXE_mqttiumd-lab")))
        ),
    )
    .unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
    path
}

// All fixture tables are compared byte-for-byte. Only the explicitly listed
// epoch-opening columns may differ after a recovery failure. This comparison
// is fixed before the cases run and keeps all admission/slot/account facts.
fn tables(path: &Path) -> Vec<(String, Vec<Vec<SqlValue>>)> {
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    ["meta", "sessions", "routes", "operations", "fences", "publications", "payloads", "obligations", "inbound"].into_iter().map(|table| {
        let columns = if table == "meta" {
            "one,format,incarnation,collected_through,next_session,route_revision,limit_payload_bytes,limit_references,limit_exchange_slots,limit_receipt_slots,max_targets_per_admission,max_topic_bytes,max_payload_bytes,max_sessions,max_route_targets,used_payload_bytes,used_references,used_exchange_slots,used_receipt_slots"
        } else { "*" };
        let mut statement = connection.prepare(&format!("SELECT {columns} FROM {table} ORDER BY 1")).unwrap();
        let count = statement.column_count();
        let rows = statement.query_map([], |row| (0..count).map(|index| row.get(index)).collect::<Result<Vec<SqlValue>, _>>()).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
        (table.to_owned(), rows)
    }).collect()
}

fn meta(path: &Path) -> (u64, u64, Option<Vec<u8>>) {
    Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .unwrap()
        .query_row(
            "SELECT last_opened,revision,opened_by FROM meta",
            [],
            |row| {
                let epoch: Vec<u8> = row.get(0)?;
                let revision: Vec<u8> = row.get(1)?;
                Ok((
                    u64::from_be_bytes(epoch.try_into().unwrap()),
                    u64::from_be_bytes(revision.try_into().unwrap()),
                    row.get(2)?,
                ))
            },
        )
        .unwrap()
}

fn corrupt(path: &Path, mutation: Mutation) {
    let connection = Connection::open(path).unwrap();
    match mutation {
        Mutation::Route | Mutation::InspectRoute => {
            assert_eq!(
                connection.execute("UPDATE routes SET qos=99", []).unwrap(),
                2
            );
        }
        Mutation::ClientLimit => {
            assert_eq!(connection.execute("UPDATE sessions SET client_id=zeroblob(65536) WHERE id=(SELECT min(id) FROM sessions)", []).unwrap(), 1);
        }
        Mutation::Meta => {
            assert_eq!(
                connection
                    .execute("UPDATE meta SET format='invalid-semantic-format'", [])
                    .unwrap(),
                1
            );
        }
        _ => unreachable!(),
    }
    let check: String = connection
        .query_row("PRAGMA quick_check", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        check, "ok",
        "the reception targets semantic errors, not broken SQLite pages"
    );
}

/// The real external reader is called after mutating an isolated image at an
/// idle inspection seam. There is no substitute empty/error-returning reader.
struct ReceptionBackend {
    inner: Sqlite,
    mutation: Mutation,
    inspected: Cell<bool>,
}
impl Backend for ReceptionBackend {
    type Store = SqliteStore;
    fn name(&self) -> &'static str {
        self.inner.name()
    }
    fn create(
        &mut self,
        incarnation: StoreIncarnation,
        limits: Limits,
    ) -> Result<SqliteStore, ReadError> {
        self.inner.create(incarnation, limits)
    }
    fn reopen_after_crash(&mut self, store: SqliteStore) -> Result<SqliteStore, ReadError> {
        self.inner.reopen_after_crash(store)
    }
    fn inspect(&self, store: Option<&SqliteStore>) -> Result<DurableState, ReadError> {
        if !self.inspected.replace(true) {
            match self.mutation {
                Mutation::InspectRoute => corrupt(&self.inner.path, Mutation::InspectRoute),
                Mutation::InspectAccess => {
                    fs::rename(
                        self.inner.path.parent().unwrap(),
                        self.inner
                            .path
                            .parent()
                            .unwrap()
                            .with_file_name("unavailable-store"),
                    )
                    .unwrap();
                }
                _ => {}
            }
        }
        self.inner.inspect(store)
    }
    fn describe(&self) -> Value {
        self.inner.describe()
    }
    fn crash(&self) -> &'static str {
        self.inner.crash()
    }
}

fn judge(root: &Path, out: &Path) -> Value {
    let result = Command::new("python3")
        .env("PYTHONPATH", root.join("tests/oracle"))
        .args(["-m", "mqttiumd_oracle", "--root"])
        .arg(out)
        .args(["judge", "--scenario", SCENARIO, "--trace", "trace.jsonl"])
        .output()
        .unwrap();
    fs::write(out.join("oracle.stderr"), &result.stderr).unwrap();
    fs::write(out.join("verdict.json"), &result.stdout).unwrap();
    assert!(
        [Some(0), Some(2)].contains(&result.status.code()),
        "oracle refused the actual artifacts: {}\n{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    serde_json::from_slice(&result.stdout).unwrap()
}

fn receive(root: &Path, out: &Path, case: Case) {
    fs::create_dir_all(out).unwrap();
    copy_inputs(root, out);
    let mut document = read_json(&out.join(SCENARIO));
    if matches!(
        case.mutation,
        Mutation::InspectRoute | Mutation::InspectAccess
    ) {
        document["faults"] = json!([]);
        document["steps"] = json!([{"kind":"InspectDurableState","checkpoint":"final"}]);
        write_json(&out.join(SCENARIO), &document);
    }
    let scenario = Scenario::read(&document).unwrap();
    let fixture = read_json(&out.join(FIXTURE));
    let source_path = out.join("baseline.sqlite");
    let mut source_backend = Sqlite::new(source_path.clone());
    let Setup {
        mut settings,
        population,
        store,
    } = Setup::from_fixture(
        &mut source_backend,
        &fixture,
        "typed-read-reception",
        1,
        scenario.faults.len(),
    )
    .unwrap();
    drop(store);
    // Explicitly document the reference image as a closed, checkpointed DB.
    let source_connection = Connection::open(&source_path).unwrap();
    source_connection
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    drop(source_connection);
    let database = out.join("store/case.sqlite");
    fs::create_dir_all(database.parent().unwrap()).unwrap();
    fs::copy(&source_path, &database).unwrap();
    let mut sqlite = Sqlite::new(database.clone());
    let store = sqlite
        .reopen_after_crash(SqliteStore::open(&database).unwrap())
        .unwrap();
    let reference = tables(&database);
    let initial_meta = meta(&database);
    let retained_database = if case.mutation == Mutation::Access {
        out.join("retained-inaccessible.sqlite")
    } else if case.mutation == Mutation::InspectAccess {
        out.join("unavailable-store/case.sqlite")
    } else {
        database.clone()
    };
    match case.mutation {
        Mutation::Route | Mutation::ClientLimit | Mutation::Meta => {
            corrupt(&database, case.mutation)
        }
        Mutation::Access => {
            fs::rename(&database, &retained_database).unwrap();
        }
        _ => {}
    }
    let before_path = if case.mutation == Mutation::Access {
        &retained_database
    } else {
        &database
    };
    let expected = tables(before_path);
    let quick_check: String =
        Connection::open_with_flags(before_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap()
            .query_row("PRAGMA quick_check", [], |row| row.get(0))
            .unwrap();
    assert_eq!(quick_check, "ok");
    settings.recovery_page = case.page as usize;
    let schema = read_json(&out.join("contracts/schema/v1/trace-record.schema.json"));
    let collector = Collector::new(
        settings.run_id,
        KindTable::from_schema(&schema).unwrap(),
        scenario.max_trace_bytes,
    );
    let config = Config {
        executable: executable(out),
        campaign: out.to_path_buf(),
        contracts: "contracts/v1".to_owned(),
        scenario: SCENARIO.to_owned(),
        seed: 1,
        run_id: settings.run_id.to_owned(),
        store: database.clone(),
        sessions: population
            .clients
            .iter()
            .map(|(name, _, session)| (name.clone(), *session))
            .collect(),
        silence: Duration::from_secs(2),
        recovery_page: case.page,
    };
    let started = Instant::now();
    let outcome = run(Plan {
        scenario: &scenario,
        backend: ReceptionBackend {
            inner: sqlite,
            mutation: case.mutation,
            inspected: Cell::new(false),
        },
        settings,
        population,
        store,
        collector,
        child: Some(config),
    });
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "no unbounded retry/restart loop"
    );
    fs::write(out.join("trace.jsonl"), &outcome.trace).unwrap();
    for (path, bytes) in &outcome.snapshots {
        let target = out.join(path);
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::write(target, bytes).unwrap();
    }
    let records: Vec<Value> = outcome
        .trace
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect();
    let failures: Vec<&Value> = records
        .iter()
        .filter(|record| record["kind"] == "DurableReadFailed")
        .collect();
    let pids: Vec<u32> = fs::read_to_string(out.join("pids.txt"))
        .unwrap()
        .lines()
        .map(|line| line.parse().unwrap())
        .collect();
    for pid in &pids {
        assert!(
            !Path::new(&format!("/proc/{pid}")).exists(),
            "child {pid} survived the failure"
        );
    }
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(writer_lock_path(
            if case.mutation == Mutation::InspectAccess {
                &retained_database
            } else {
                &database
            },
        ))
        .unwrap();
    lock.try_lock()
        .expect("the child's exclusive writer lock was released");
    drop(lock);
    let pages: Vec<&Value> = records
        .iter()
        .filter(|record| record["kind"] == "RecoveryPageDelivered")
        .collect();
    if let Some(code) = case.code {
        assert!(outcome.stopped.is_some());
        assert_eq!(
            failures.len(),
            1,
            "stopped={:?}; refused={:?}; {records:#?}",
            outcome.stopped,
            outcome.refused
        );
        let failure = &failures[0]["payload"];
        assert_eq!(failure["code"], code);
        assert_eq!(failure["phase"], case.phase.unwrap());
        assert_eq!(failure["process_incarnation"], "1");
        assert!(!failure.to_string().contains("invalid-semantic-format"));
        assert!(failure["message"].as_str().unwrap().len() <= 1024);
        assert_eq!(outcome.failure_count, 1);
        assert_eq!(
            outcome.failures[0]["record_sequence"],
            failures[0]["record_sequence"]
        );
        assert_eq!(records.last().unwrap()["payload"]["complete"], false);
        assert_eq!(pids.len(), 1, "no automatic restart loop");
        if case.phase == Some("Recovery") {
            assert_eq!(pages.len(), case.valid_pages);
            assert!(pages.iter().all(|page| page["payload"]["last"] == false));
            assert_eq!(failure["page"], case.valid_pages.to_string());
        }
        assert!(records.iter().all(|record| !matches!(
            record["kind"].as_str(),
            Some(
                "StoreCommitStable" | "PublicationAdmitted" | "PeerPacketObserved" | "FrameWritten"
            )
        )));
        let now = meta(&retained_database);
        let opened = case.phase != Some("Open");
        assert_eq!(now.0, initial_meta.0 + u64::from(opened));
        assert_eq!(now.1, initial_meta.1 + u64::from(opened));
        assert_eq!(now.2.is_some(), opened);
        let durable = tables(&retained_database);
        if case.mutation == Mutation::InspectRoute {
            let mut expected_inspector = expected.clone();
            for row in &mut expected_inspector
                .iter_mut()
                .find(|(name, _)| name == "routes")
                .unwrap()
                .1
            {
                row[3] = SqlValue::Integer(99);
            }
            assert_eq!(
                durable, expected_inspector,
                "only the documented route mutation and epoch-opening columns changed"
            );
        } else {
            assert_eq!(
                durable, expected,
                "an incomplete read must not reset or admit anything"
            );
        }
        if case.mutation == Mutation::Access {
            assert!(
                !database.exists(),
                "opening must not create a replacement store"
            );
        }
    } else {
        assert_eq!(outcome.stopped, None);
        assert!(failures.is_empty());
        assert_eq!(outcome.failure_count, 0);
        assert_eq!(records.last().unwrap()["payload"]["complete"], true);
        assert!(!reference.is_empty());
    }
    let verdict = judge(root, out);
    assert_eq!(
        verdict["verdict"],
        if case.code.is_some() {
            "INCONCLUSIVE"
        } else {
            "PASS"
        }
    );
    if let Some(code) = case.code {
        assert_eq!(verdict["failure_count"], "1");
        assert_eq!(verdict["failures"][0]["code"], code);
        assert_eq!(
            verdict["assertions"]["durable_reads_complete"]["status"],
            "INCONCLUSIVE"
        );
    }
    write_json(
        &out.join("campaign.json"),
        &json!({
            "format":"mqttiumd.typed-read-reception", "case":case.name,
            "scope":"deliberately mutated copy of a closed checkpointed fixture, driven through the real child/supervisor; no qualifying business PASS is claimed for a read failure",
            "baseline":"baseline.sqlite", "database":"store/case.sqlite", "quick_check":quick_check,
            "reference_checkpoint":"WAL checkpoint TRUNCATE after closing fixture writer, before copying database",
            "mutation":format!("{:?}",case.mutation), "recovery_page":case.page, "durable_read_budgets":mqttiumd_lab::bench::failure::read_budgets(), "valid_pages":pages.len(),
            "expected_code":case.code, "expected_phase":case.phase, "pids":pids, "children_reaped":true, "writer_released":true,
            "stopped":outcome.stopped, "failure_count":outcome.failure_count.to_string(), "failures":outcome.failures,
            "business_verdict":verdict["verdict"], "reception":"PASS"
        }),
    );
    println!(
        "{}: received {:?}/{:?}, {} delivered page(s), business {}",
        case.name,
        case.code,
        case.phase,
        pages.len(),
        verdict["verdict"]
    );
}

#[test]
fn read_failure_reception() {
    let root = bench_support::root().canonicalize().unwrap();
    let export = std::env::var_os("MQTTIUMD_READ_RECEPTION_OUT").map(PathBuf::from);
    let out = export
        .clone()
        .unwrap_or_else(|| bench_support::fresh("typed-read-reception"));
    assert!(
        !out.exists() || fs::read_dir(&out).unwrap().next().is_none(),
        "receipt output must be empty"
    );
    fs::create_dir_all(&out).unwrap();
    mqttiumd_lab::bench::rebuild::check_features().unwrap();
    let revision = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(&root)
        .output()
        .unwrap();
    assert!(revision.status.success());
    let patch = Command::new("git")
        .args(["diff", "--binary", "HEAD", "--", "."])
        .current_dir(&root)
        .output()
        .unwrap();
    assert!(patch.status.success());
    fs::write(out.join("tracked.patch"), &patch.stdout).unwrap();
    let binary = Path::new(env!("CARGO_BIN_EXE_mqttiumd-lab"));
    let (binary_digest, binary_bytes) =
        mqttiumd_lab::files::hash_stream(fs::File::open(binary).unwrap()).unwrap();
    write_json(
        &out.join("provenance.json"),
        &json!({
            "revision":String::from_utf8(revision.stdout).unwrap().trim(),
            "tracked_patch_sha256":mqttiumd_lab::manifest::sha256_hex(&patch.stdout),
            "binary":{"path":binary, "sha256":binary_digest, "bytes":binary_bytes.to_string()},
            "build_features":mqttiumd_lab::bench::rebuild::BUILD_FEATURES,
            "scope":"actual test-run executable identity, not a qualifying rebuild provenance claim"
        }),
    );
    for case in CASES {
        receive(&root, &out.join(case.name), case);
    }
    if export.is_none() {
        fs::remove_dir_all(out).unwrap();
    }
}
