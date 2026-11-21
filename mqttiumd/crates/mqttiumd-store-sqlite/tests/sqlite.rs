//! What only the SQLite store can show: the settings read back (DEM-011),
//! a file opened again, a schema version refused before any write, a
//! failing medium, and the same answers and states as the deterministic
//! store over every short sequence of commands.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use mqttiumd_contract::ids::StoreIncarnation;
use mqttiumd_contract::outcome::StoreOutcome;
use mqttiumd_lab::store::conformance::{
    INCARNATION, World, accounts_match_what_is_held, fixture_limits, opening_command, operation_id,
};
use mqttiumd_lab::store::durable::{AdminCommand, Durability, DurableStore, Limits, MemoryStore};
use mqttiumd_store_sqlite::{OpenError, SCHEMA_VERSION, SqliteStore};

static NEXT: AtomicU64 = AtomicU64::new(0);

fn fresh_path() -> PathBuf {
    let directory = std::env::temp_dir().join("mqttiumd-store-sqlite-tests");
    std::fs::create_dir_all(&directory).expect("a temporary directory");
    let path = directory.join(format!(
        "sqlite-{}-{}.db",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    for suffix in ["", "-wal", "-shm"] {
        let mut file = path.clone().into_os_string();
        file.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(file));
    }
    path
}

fn memory(incarnation: StoreIncarnation, limits: Limits) -> MemoryStore {
    MemoryStore::create(incarnation, limits)
}

#[test]
fn completed_publications_match_the_deterministic_state_after_reopening() {
    let path = fresh_path();
    let mut sqlite = World::new(&|incarnation, limits| {
        SqliteStore::create(&path, incarnation, limits).expect("created")
    });
    let mut memory = World::new(&memory);
    let first = memory.admission(1, 7, b"unrouted", &[]);
    let commands = [
        &first,
        &memory.release(2, 7, 1),
        &memory.admission(3, 7, b"routed", &[memory.b]),
        &memory.release(4, 7, 3),
    ];
    for command in commands {
        assert_eq!(sqlite.store.apply(command), memory.store.apply(command));
        assert_eq!(
            sqlite.store.snapshot().expect("healthy snapshot"),
            memory.store.snapshot().expect("healthy snapshot")
        );
    }
    let deletion = AdminCommand::DeleteSession { session: memory.b };
    assert_eq!(
        sqlite.store.admin(deletion.clone()),
        memory.store.admin(deletion)
    );
    let complete = memory.store.snapshot().expect("healthy snapshot");
    assert!(complete.publications.is_empty());
    assert!(complete.payloads.is_empty());
    assert_eq!(accounts_match_what_is_held(&complete), Ok(()));
    drop(sqlite.store);
    let mut reopened = SqliteStore::open(&path).expect("same file reopened");
    assert_eq!(reopened.snapshot().expect("healthy snapshot"), complete);
    assert_eq!(reopened.apply(&first), memory.store.apply(&first));
    assert_eq!(reopened.snapshot().expect("healthy snapshot"), complete);
}

#[test]
fn the_settings_in_force_are_read_back() {
    let store = SqliteStore::create(&fresh_path(), INCARNATION, fixture_limits()).expect("created");
    let settings = store.effective_settings();
    assert_eq!(settings.journal_mode, "wal");
    assert_eq!(settings.synchronous, 2, "FULL");
    assert_eq!(settings.user_version, SCHEMA_VERSION);
    assert!(settings.sqlite_version.starts_with("3."), "{settings:?}");
    assert!(!settings.compile_options.is_empty());
    assert!(settings.page_size > 0);
    assert!(settings.wal_autocheckpoint > 0);
    // The settings say which build runs: the workspace tests, without the
    // feature, run a store that carries no kill point.
    assert_eq!(settings.kill_points, cfg!(feature = "kill-points"));
}

#[test]
fn a_store_opened_again_holds_what_was_committed() {
    let path = fresh_path();
    let world = World::new(&|incarnation, limits| {
        SqliteStore::create(&path, incarnation, limits).expect("created")
    });
    let mut store = world.store;
    let command = World {
        store: (),
        producer: world.producer,
        b: world.b,
        c: world.c,
        epoch: world.epoch,
        route_revision: world.route_revision,
    }
    .admission(1, 7, b"payload", &[world.b, world.c]);
    assert!(store.apply(&command).committed_now);
    let before = store.snapshot().expect("healthy snapshot");
    drop(store);
    let reopened = SqliteStore::open(&path).expect("opened again");
    assert_eq!(reopened.snapshot().expect("healthy snapshot"), before);
    assert_eq!(accounts_match_what_is_held(&before), Ok(()));
}

#[test]
fn another_schema_version_is_refused_before_any_write() {
    let path = fresh_path();
    drop(SqliteStore::create(&path, INCARNATION, fixture_limits()).expect("created"));
    {
        let conn = rusqlite::Connection::open(&path).expect("raw");
        conn.pragma_update(None, "user_version", SCHEMA_VERSION + 1)
            .expect("set");
    }
    match SqliteStore::open(&path) {
        Err(OpenError::SchemaVersion(version)) => assert_eq!(version, SCHEMA_VERSION + 1),
        other => panic!("opened: {other:?}"),
    }
    // A file that holds a database is never created over.
    assert!(matches!(
        SqliteStore::create(&path, INCARNATION, fixture_limits()),
        Err(OpenError::NotEmpty)
    ));
}

#[test]
fn a_failing_medium_writes_nothing_and_answers_unknown() {
    let path = fresh_path();
    let mut world = World::new(&|incarnation, limits| {
        SqliteStore::create(&path, incarnation, limits).expect("created")
    });
    let command = world.admission(1, 7, b"payload", &[world.b]);
    let before = world.store.snapshot().expect("healthy snapshot");
    world.store.set_durability(Durability::Failing);
    let applied = world.store.apply(&command);
    assert_eq!(
        applied.reply,
        mqttiumd_contract::outcome::StoreReply::Outcome(StoreOutcome::PendingOrUnknown)
    );
    assert!(!applied.wrote);
    let resolved = world.store.resolve_and_fence(operation_id(world.epoch, 9));
    assert_eq!(resolved.outcome, StoreOutcome::PendingOrUnknown);
    assert_eq!(
        world
            .store
            .admin(AdminCommand::DeleteSession { session: world.c }),
        Err(mqttiumd_lab::store::durable::AdminRefused::WriteFailed)
    );
    let command_open = opening_command(&world.store);
    let _ = world.store.open_epoch(&command_open);
    assert_eq!(world.store.snapshot().expect("healthy snapshot"), before);
    world.store.set_durability(Durability::Available);
    assert!(world.store.apply(&command).committed_now);
}

#[derive(Clone, Copy, Debug)]
enum Step {
    Apply(usize),
    Resolve(u64),
    Release,
    OpenEpoch,
    Collect,
    DeleteTarget,
}

/// The same commands on both stores give the same answers and the same
/// durable state after every step, over every sequence of three steps;
/// the accounts of SQLite always equal what it holds.
#[test]
fn sqlite_answers_as_the_deterministic_store() {
    let steps = [
        Step::Apply(0),
        Step::Apply(1),
        Step::Apply(2),
        Step::Resolve(1),
        Step::Resolve(5),
        Step::Release,
        Step::OpenEpoch,
        Step::Collect,
        Step::DeleteTarget,
    ];
    const DEPTH: u32 = 3;
    let base = steps.len();
    for run in 0..base.pow(DEPTH) {
        let path = fresh_path();
        let mut sqlite = World::new(&|incarnation, limits| {
            SqliteStore::create(&path, incarnation, limits).expect("created")
        });
        let mut memory = World::new(&memory);
        let first = memory.epoch;
        let commands = [
            memory.admission(1, 7, b"payload", &[memory.b, memory.c]),
            memory.admission(2, 7, b"payload", &[memory.b]),
            memory.admission(3, 8, b"", &[]),
        ];
        let mut code = run;
        for depth in 0..DEPTH {
            let step = steps[code % base];
            code /= base;
            let context = format!("run {run}, step {depth}: {step:?}");
            match step {
                Step::Apply(index) => assert_eq!(
                    sqlite.store.apply(&commands[index]),
                    memory.store.apply(&commands[index]),
                    "{context}"
                ),
                Step::Resolve(sequence) => {
                    let operation = operation_id(first, sequence);
                    assert_eq!(
                        sqlite.store.established(operation),
                        memory.store.established(operation),
                        "{context}"
                    );
                    assert_eq!(
                        sqlite.store.resolve_and_fence(operation),
                        memory.store.resolve_and_fence(operation),
                        "{context}"
                    );
                }
                Step::Release => {
                    let release = memory.release(4, 7, 1);
                    assert_eq!(
                        sqlite.store.apply(&release),
                        memory.store.apply(&release),
                        "{context}"
                    );
                }
                Step::OpenEpoch => {
                    let command = opening_command(&memory.store);
                    assert_eq!(
                        sqlite.store.open_epoch(&command),
                        memory.store.open_epoch(&command),
                        "{context}"
                    );
                }
                Step::Collect => {
                    let command = AdminCommand::CollectClosedEpochs { through: first };
                    assert_eq!(
                        sqlite.store.admin(command.clone()),
                        memory.store.admin(command),
                        "{context}"
                    );
                }
                Step::DeleteTarget => {
                    let command = AdminCommand::DeleteSession { session: memory.c };
                    assert_eq!(
                        sqlite.store.admin(command.clone()),
                        memory.store.admin(command),
                        "{context}"
                    );
                }
            }
            let state = sqlite.store.snapshot().expect("healthy snapshot");
            assert_eq!(
                state,
                memory.store.snapshot().expect("healthy snapshot"),
                "{context}"
            );
            assert_eq!(accounts_match_what_is_held(&state), Ok(()), "{context}");
            let mut cursor = None;
            loop {
                let page_sqlite = sqlite.store.recover(cursor, 2);
                let page_memory = memory.store.recover(cursor, 2);
                assert_eq!(page_sqlite, page_memory, "{context}");
                match page_memory {
                    Ok(page) if page.next.is_some() => cursor = page.next,
                    _ => break,
                }
            }
        }
    }
}

/// A durable value the store cannot decode is an error of the snapshot,
/// of the same kind as for `inspect`, never a panic.
#[test]
fn a_value_that_does_not_decode_is_an_error_of_the_snapshot() {
    let path = fresh_path();
    let mut world = World::new(&|incarnation, limits| {
        SqliteStore::create(&path, incarnation, limits).expect("created")
    });
    let command = world.admission(1, 7, b"payload", &[world.b]);
    assert!(world.store.apply(&command).committed_now);
    drop(world);
    // A decision code no version of this store writes.
    rusqlite::Connection::open(&path)
        .expect("raw")
        .execute("UPDATE operations SET decision = 99", [])
        .expect("damaged");
    let store = SqliteStore::open(&path).expect("the file is sound, a value is not");
    let snapshot = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| store.try_snapshot()));
    assert!(
        matches!(
            snapshot,
            Ok(Err(mqttiumd_store_api::ReadError::InvalidData))
        ),
        "{snapshot:?}"
    );
    assert!(matches!(
        SqliteStore::inspect(&path),
        Err(mqttiumd_store_api::ReadError::InvalidData)
    ));
}

/// A threshold readback is not proof of an installed autocheckpoint.
/// With no kill hook, a transaction exceeding sixteen pages must put its
/// entire state in the main database file before the writer is dropped.
#[test]
fn a_sixteen_page_autocheckpoint_reaches_the_main_database_while_the_writer_lives() {
    let path = fresh_path();
    let limits = Limits {
        payload_bytes: mqttiumd_contract::capacity::PayloadBytes::new(1 << 20),
        max_payload_bytes: 1 << 20,
        ..fixture_limits()
    };
    let mut world = World::with_limits(
        &|incarnation, limits| SqliteStore::create(&path, incarnation, limits).expect("created"),
        limits,
    );
    let mut reference = World::with_limits(&MemoryStore::create, limits);
    assert_eq!(
        world
            .store
            .set_wal_autocheckpoint_for_measurement(16)
            .expect("set"),
        16
    );
    assert_eq!(
        world.store.effective_settings().checkpoint_policy,
        "sqlite-autocheckpoint"
    );
    let mut command = mqttiumd_lab::store::conformance::altered(
        &reference.admission(1, 7, b"", &[reference.b, reference.c]),
        |admit| {
            let length = mqttiumd_contract::capacity::PayloadBytes::new(65_536);
            admit.payload.length = length;
            admit.costs.payload_bytes = length;
        },
    );
    command.payload = Some(
        mqttiumd_contract::packet::SharedBytes::copy_from(&vec![42; 65_536], 65_536)
            .expect("large payload"),
    );
    assert!(reference.store.apply(&command).committed_now);
    assert!(world.store.apply(&command).committed_now);
    let expected = reference.store.snapshot().expect("healthy snapshot");
    assert_eq!(world.store.try_snapshot().expect("live state"), expected);
    // An isolated image deliberately omits the WAL. Unlike a crash run,
    // this is only an observation of a checkpoint while the writer is idle.
    let copy = fresh_path();
    std::fs::copy(&path, &copy).expect("database image copied while writer lives");
    let image = SqliteStore::open(&copy).expect("main database image opens");
    assert_eq!(
        image.try_snapshot().expect("checkpointed state"),
        expected,
        "actual checkpoint copied the whole transaction into the database"
    );
}
