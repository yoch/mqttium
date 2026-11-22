//! A commit that is durable and whose call returns an error (record 0045).
//!
//! SQLite runs the write-ahead-log callback of a connection after a commit
//! was written to the log and synchronised, and after its write lock was
//! released. The value the callback returns becomes the result of the
//! `COMMIT` statement: a callback that returns an error makes the call
//! fail for a transaction that is durable. The caller of `apply` or
//! `apply_group` is then told `PendingOrUnknown`, the answer for a commit
//! that failed, while every row is in the file.
//!
//! These tests install such a callback on the connection of a store, which
//! only a test inside this crate can reach: no public way to do it exists,
//! in any build. They need the callback interface of the binding, which
//! this crate compiles under its feature `kill-points` only, so they run
//! where that feature is tested. The engine is the one the product links.
//!
//! What they show is the reconciliation the uncertain answer calls for.
//! Each operation is found by its identity, with the outcome a second
//! store and the deterministic store of the lab gave to the same commands
//! applied one after the other, and nothing is fenced. The commands
//! presented again are recognised and write nothing. The same holds once
//! the file was closed and opened again, and in an image of the file taken
//! at the moment the call returned its error.

use std::collections::BTreeMap;
use std::ffi::c_int;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use mqttiumd_contract::command::Command;
use mqttiumd_contract::ids::{OperationId, Revision};
use mqttiumd_contract::outcome::{StoreOutcome, StoreReply};
use mqttiumd_lab::store::conformance::{
    World, accounts_match_what_is_held, altered, fixture_limits,
};
use mqttiumd_lab::store::durable::MemoryStore;
use mqttiumd_store_api::{Applied, DurableState, DurableStore, Resolved};

use crate::{SqliteStore, UNKNOWN};

/// A directory no other test uses.
fn fresh_directory(name: &str) -> PathBuf {
    let directory = std::env::temp_dir().join(format!(
        "mqttiumd-commit-error-{}-{name}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).expect("a temporary directory");
    directory
}

/// Every row of every table, as the engine returns it, read on a
/// connection of its own while the writer lives.
fn tables(path: &Path) -> BTreeMap<String, Vec<String>> {
    let conn = rusqlite::Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .expect("a reader");
    let names: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name")
        .expect("the schema")
        .query_map([], |row| row.get(0))
        .expect("the tables")
        .collect::<Result<_, _>>()
        .expect("their names");
    assert!(names.iter().any(|name| name == "operations"));
    names
        .into_iter()
        .map(|name| {
            let mut statement = conn
                .prepare(&format!("SELECT * FROM \"{name}\""))
                .expect("the table");
            let columns = statement.column_count();
            let rows = statement
                .query_map([], |row| {
                    let mut cells = Vec::with_capacity(columns);
                    for index in 0..columns {
                        cells.push(row.get::<_, rusqlite::types::Value>(index)?);
                    }
                    Ok(format!("{cells:?}"))
                })
                .expect("its rows")
                .collect::<Result<Vec<String>, _>>()
                .expect("decoded");
            (name, rows)
        })
        .collect()
}

/// The store whose commits return an error, a second SQLite store that
/// receives the same commands one transaction each and never fails, and
/// the deterministic store of the lab, which shares no code with either.
struct Trio {
    directory: PathBuf,
    faulted: World<SqliteStore>,
    faulted_path: PathBuf,
    reference: World<SqliteStore>,
    reference_path: PathBuf,
    model: World<MemoryStore>,
}

impl Trio {
    fn new(name: &str) -> Self {
        let directory = fresh_directory(name);
        let faulted_path = directory.join("faulted.sqlite");
        let reference_path = directory.join("reference.sqlite");
        let world = |path: &Path| {
            World::with_limits(
                &|incarnation, limits| {
                    SqliteStore::create(path, incarnation, limits).expect("a new store")
                },
                fixture_limits(),
            )
        };
        let trio = Self {
            faulted: world(&faulted_path),
            reference: world(&reference_path),
            model: World::with_limits(&MemoryStore::create, fixture_limits()),
            faulted_path,
            reference_path,
            directory,
        };
        trio.same_state();
        trio
    }

    /// One admission towards `b` and `c` and its release, numbered `n`.
    fn pair(&self, n: u64) -> [Command; 2] {
        let world = &self.reference;
        let packet = u16::try_from(n).expect("fits");
        [
            world.admission(2 * n, packet, b"payload", &[world.b, world.c]),
            world.release(2 * n + 1, packet, 2 * n),
        ]
    }

    /// An admission the store refuses: one of its targets is named at a
    /// revision it never had.
    fn stale(&self, n: u64) -> Command {
        let world = &self.reference;
        altered(
            &world.admission(n, 50, b"payload", &[world.b, world.c]),
            |admit| admit.targets.as_mut_slice()[1].session_revision = Revision::new(9),
        )
    }

    /// The same commands on the three stores, each call returning.
    fn everywhere(&mut self, commands: &[Command]) {
        for command in commands {
            let applied = self.faulted.store.apply(command);
            assert!(applied.wrote, "{applied:?}");
            assert_eq!(self.reference.store.apply(command), applied);
            assert_eq!(self.model.store.apply(command), applied, "the model");
        }
        self.same_state();
    }

    /// What the two references answer to `commands` applied one after the
    /// other: the answers the faulted store could not give.
    fn sequentially(&mut self, commands: &[Command]) -> Vec<Applied> {
        commands
            .iter()
            .map(|command| {
                let applied = self.reference.store.apply(command);
                assert_eq!(self.model.store.apply(command), applied, "the model");
                applied
            })
            .collect()
    }

    /// The faulted store holds exactly what the references hold: the
    /// bounded snapshot of both, and the raw rows of the second store.
    fn same_state(&self) {
        let faulted = self.faulted.store.snapshot().expect("healthy snapshot");
        assert_eq!(accounts_match_what_is_held(&faulted), Ok(()));
        assert_eq!(
            faulted,
            self.reference.store.snapshot().expect("healthy snapshot"),
            "the durable state"
        );
        assert_eq!(
            faulted,
            self.model.store.snapshot().expect("healthy snapshot"),
            "the state of the model"
        );
        assert_eq!(
            tables(&self.faulted_path),
            tables(&self.reference_path),
            "the rows of every table"
        );
    }

    /// The state an image of the faulted file holds: the database and its
    /// write-ahead log copied as they stand, without the store being
    /// closed, then opened as a store after a crash is.
    fn image(&self, name: &str) -> DurableState {
        let directory = self.directory.join(name);
        std::fs::create_dir(&directory).expect("a directory for the image");
        let image = directory.join("image.sqlite");
        std::fs::copy(&self.faulted_path, &image).expect("the database");
        let log = |path: &Path| PathBuf::from(format!("{}-wal", path.display()));
        std::fs::copy(log(&self.faulted_path), log(&image)).expect("the write-ahead log");
        let store = SqliteStore::open(&image).expect("the image opens");
        store.try_snapshot().expect("decodes")
    }

    /// One resolution on the three stores: the same answer.
    fn resolve(&mut self, operation: OperationId) -> Resolved {
        let resolved = self.faulted.store.resolve_and_fence(operation);
        assert_eq!(self.reference.store.resolve_and_fence(operation), resolved);
        assert_eq!(
            self.model.store.resolve_and_fence(operation),
            resolved,
            "the model"
        );
        resolved
    }

    /// Reconciles the faulted store after uncertain answers: each
    /// operation is found by its identity with the outcome the references
    /// gave, nothing is fenced, and nothing is written.
    fn reconcile(&mut self, commands: &[Command], expected: &[Applied]) {
        let rows = tables(&self.faulted_path);
        for (command, applied) in commands.iter().zip(expected) {
            let outcome = outcome(applied);
            let operation = operation(command);
            assert_eq!(
                self.faulted.store.established(operation),
                Ok(Some(outcome)),
                "found by its identity"
            );
            assert_eq!(
                self.resolve(operation),
                Resolved {
                    outcome,
                    fenced_now: false,
                },
                "a durable decision is returned, not fenced"
            );
        }
        assert_eq!(
            tables(&self.faulted_path),
            rows,
            "a resolution of what is decided writes nothing"
        );
        self.same_state();
    }
}

fn operation(command: &Command) -> OperationId {
    command.recipe.get().operation()
}

fn outcome(applied: &Applied) -> StoreOutcome {
    match applied.reply {
        StoreReply::Outcome(outcome) => outcome,
        StoreReply::Violation(violation) => panic!("a reference violated: {violation:?}"),
    }
}

/// What a store answers to a command it already decided: the outcome of
/// its receipt, and no write.
fn recognised(expected: &[Applied]) -> Vec<Applied> {
    expected
        .iter()
        .map(|applied| Applied {
            reply: applied.reply,
            committed_now: false,
            wrote: false,
        })
        .collect()
}

/// Makes every commit of `store` return an error once the engine logged
/// it, and counts those commits. The callback is the engine's own
/// write-ahead-log callback: it runs for a commit that wrote the log, and
/// for nothing else.
fn fail_logged_commits(store: &mut SqliteStore) -> Arc<AtomicU32> {
    let logged = Arc::new(AtomicU32::new(0));
    let counted = Arc::clone(&logged);
    store
        .conn
        .wal_hook(Some(
            move |_wal: &mut rusqlite::hooks::Wal, frames: c_int| {
                assert!(frames > 0, "a logged commit has frames");
                counted.fetch_add(1, Ordering::SeqCst);
                Err(rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_IOERR),
                    Some("the commit is durable and its call fails".to_owned()),
                ))
            },
        ))
        .expect("the callback is installed");
    logged
}

/// Lets the commits of `store` return what the engine returns again.
fn heal(store: &mut SqliteStore) {
    store
        .conn
        .wal_hook(None::<fn(&mut rusqlite::hooks::Wal, c_int) -> rusqlite::Result<()>>)
        .expect("the callback is removed");
}

/// First the mechanism alone, on the engine linked here and without the
/// store: the call returns an error and the row is in the file.
#[test]
fn the_engine_reports_an_error_for_a_commit_it_made_durable() {
    let directory = fresh_directory("engine");
    let path = directory.join("engine.sqlite");
    let mut conn = rusqlite::Connection::open(&path).expect("a connection");
    conn.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; CREATE TABLE t (n INTEGER)",
    )
    .expect("configured");
    conn.wal_hook(Some(
        |_wal: &mut rusqlite::hooks::Wal, _frames: c_int| -> rusqlite::Result<()> {
            Err(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_IOERR),
                None,
            ))
        },
    ))
    .expect("the callback is installed");
    let tx = conn.transaction().expect("a transaction");
    tx.execute("INSERT INTO t VALUES (1), (2)", [])
        .expect("staged");
    let failed = tx.commit().expect_err("the call returns an error");
    assert_eq!(
        failed.sqlite_error_code(),
        Some(rusqlite::ErrorCode::Unknown),
        "the engine reports its generic error for a failed callback: {failed}"
    );
    assert!(conn.is_autocommit(), "the transaction ended");
    // Read by a connection that shares nothing with the first.
    let count = |path: &Path| -> (i64, i64) {
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("a reader")
            .query_row("SELECT count(*), sum(n) FROM t", [], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .expect("read")
    };
    assert_eq!(count(&path), (2, 3), "both rows are committed");
    drop(conn);
    assert_eq!(count(&path), (2, 3), "and stay after the connection closed");
    println!(
        "SQLite {}: COMMIT returned `{failed}` for a transaction that is durable",
        rusqlite::version()
    );
    std::fs::remove_dir_all(directory).expect("cleanup");
}

/// A group whose `COMMIT` is durable and returns an error: every member
/// is answered `PendingOrUnknown`, and every member is in the file, the
/// refused one with its receipt.
#[test]
fn a_group_whose_durable_commit_returns_an_error_is_reconciled_without_a_second_effect() {
    let mut trio = Trio::new("group");
    let prefix = trio.pair(1);
    trio.everywhere(&prefix);
    let before = trio.faulted.store.snapshot().expect("healthy snapshot");

    let mut commands = trio.pair(2).to_vec();
    commands.push(trio.stale(8));
    commands.extend(trio.pair(3));
    let members: Vec<&Command> = commands.iter().collect();

    let logged = fail_logged_commits(&mut trio.faulted.store);
    assert_eq!(
        trio.faulted.store.apply_group(&members),
        Ok(vec![UNKNOWN; 5]),
        "a commit whose call failed says nothing certain of any member"
    );
    assert_eq!(
        logged.load(Ordering::SeqCst),
        1,
        "the engine logged the one commit of the group before its call failed"
    );

    // What the caller was not told: the answers of the same commands
    // applied one after the other on stores that did not fail.
    let expected = trio.sequentially(&commands);
    assert_eq!(
        expected
            .iter()
            .map(|applied| (applied.committed_now, applied.wrote))
            .collect::<Vec<_>>(),
        [
            (true, true),
            (true, true),
            (false, true),
            (true, true),
            (true, true)
        ],
        "four commits and the receipt of one rejection"
    );
    assert!(matches!(
        expected[2].reply,
        StoreReply::Outcome(StoreOutcome::RejectedCertain(_))
    ));

    // The group is durable: on the connection that failed, on a
    // connection of its own, and in an image of the file as it stands.
    let committed = trio.faulted.store.snapshot().expect("healthy snapshot");
    assert_ne!(committed, before);
    trio.same_state();
    assert_eq!(trio.image("after-the-error"), committed);

    // The reconciliation the uncertain answer calls for.
    trio.reconcile(&commands, &expected);

    // Presented again while commits still fail: every member is
    // recognised, so nothing is staged and no commit is attempted.
    let rows = tables(&trio.faulted_path);
    assert_eq!(
        trio.faulted.store.apply_group(&members),
        Ok(recognised(&expected)),
        "recognised member by member, with the outcome of its receipt"
    );
    assert_eq!(logged.load(Ordering::SeqCst), 1, "no second commit");
    assert_eq!(tables(&trio.faulted_path), rows, "no second effect");
    assert_eq!(trio.sequentially(&commands), recognised(&expected));
    trio.same_state();

    // The same after the file was closed and opened again.
    heal(&mut trio.faulted.store);
    let Trio {
        directory,
        faulted,
        faulted_path,
        reference,
        ..
    } = trio;
    drop(faulted);
    let mut reopened = SqliteStore::open(&faulted_path).expect("opened again");
    assert_eq!(reopened.try_snapshot().expect("decodes"), committed);
    for (command, applied) in commands.iter().zip(&expected) {
        assert_eq!(
            reopened.established(operation(command)),
            Ok(Some(outcome(applied)))
        );
        assert_eq!(
            reopened.resolve_and_fence(operation(command)),
            Resolved {
                outcome: outcome(applied),
                fenced_now: false,
            }
        );
    }
    assert_eq!(
        reopened.apply_group(&members),
        Ok(recognised(&expected)),
        "recognised after the restart too"
    );
    assert_eq!(reopened.try_snapshot().expect("decodes"), committed);
    assert_eq!(
        reopened.try_snapshot().expect("decodes"),
        reference.store.snapshot().expect("healthy snapshot"),
        "one admission and one release each, as on the store that never failed"
    );
    assert_eq!(tables(&faulted_path), rows);
    drop(reopened);
    drop(reference);
    std::fs::remove_dir_all(directory).expect("cleanup");
}

/// The same outcome for `apply`: one command, one transaction, a commit
/// that is durable and returns an error. A command the store refuses has
/// no commit of its own; the commit that fails is the one of its receipt.
#[test]
fn a_command_whose_durable_commit_returns_an_error_is_reconciled_without_a_second_effect() {
    let mut trio = Trio::new("apply");
    let prefix = trio.pair(1);
    trio.everywhere(&prefix);

    let mut commands = trio.pair(2).to_vec();
    commands.push(trio.stale(8));
    let logged = fail_logged_commits(&mut trio.faulted.store);
    let mut expected = Vec::new();
    for (command, commits) in commands.iter().zip(1u32..) {
        let before = trio.faulted.store.snapshot().expect("healthy snapshot");
        assert_eq!(
            trio.faulted.store.apply(command),
            UNKNOWN,
            "a commit whose call failed says nothing certain"
        );
        assert_eq!(
            logged.load(Ordering::SeqCst),
            commits,
            "the engine logged one commit for this command"
        );
        let applied = trio.sequentially(std::slice::from_ref(command));
        assert!(applied[0].wrote, "{applied:?}");
        let committed = trio.faulted.store.snapshot().expect("healthy snapshot");
        assert_ne!(committed, before);
        trio.same_state();
        assert_eq!(trio.image(&format!("after-error-{commits}")), committed);
        trio.reconcile(std::slice::from_ref(command), &applied);

        let rows = tables(&trio.faulted_path);
        assert_eq!(
            trio.faulted.store.apply(command),
            recognised(&applied)[0],
            "recognised with the outcome of its receipt"
        );
        assert_eq!(logged.load(Ordering::SeqCst), commits, "no second commit");
        assert_eq!(tables(&trio.faulted_path), rows, "no second effect");
        assert_eq!(
            trio.sequentially(std::slice::from_ref(command)),
            recognised(&applied)
        );
        expected.extend(applied);
    }
    assert_eq!(
        expected
            .iter()
            .map(|applied| applied.committed_now)
            .collect::<Vec<_>>(),
        [true, true, false],
        "an admission, its release and a rejection"
    );
    assert!(matches!(
        expected[2].reply,
        StoreReply::Outcome(StoreOutcome::RejectedCertain(_))
    ));

    // The same after the file was closed and opened again.
    heal(&mut trio.faulted.store);
    let committed = trio.faulted.store.snapshot().expect("healthy snapshot");
    let rows = tables(&trio.faulted_path);
    let Trio {
        directory,
        faulted,
        faulted_path,
        reference,
        ..
    } = trio;
    drop(faulted);
    let mut reopened = SqliteStore::open(&faulted_path).expect("opened again");
    assert_eq!(reopened.try_snapshot().expect("decodes"), committed);
    for (command, applied) in commands.iter().zip(&expected) {
        assert_eq!(
            reopened.established(operation(command)),
            Ok(Some(outcome(applied)))
        );
        assert_eq!(
            reopened.resolve_and_fence(operation(command)),
            Resolved {
                outcome: outcome(applied),
                fenced_now: false,
            }
        );
        assert_eq!(
            reopened.apply(command),
            recognised(std::slice::from_ref(applied))[0]
        );
    }
    assert_eq!(reopened.try_snapshot().expect("decodes"), committed);
    assert_eq!(tables(&faulted_path), rows);
    drop(reopened);
    drop(reference);
    std::fs::remove_dir_all(directory).expect("cleanup");
}
