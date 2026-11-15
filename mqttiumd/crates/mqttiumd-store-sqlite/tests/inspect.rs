//! What an inspector outside the writing process may do (record 0002):
//! read the durable state without the writer, in one read transaction,
//! write nothing, and leave the WAL free to be checkpointed. And what a
//! restart checks before it opens an epoch: `PRAGMA quick_check`.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use mqttiumd_contract::capacity::{ExchangeSlots, PayloadBytes, ReceiptSlots, References};
use mqttiumd_lab::store::conformance::{World, accounts_match_what_is_held, fixture_limits};
use mqttiumd_lab::store::durable::{DurableStore, Limits};
use mqttiumd_store_sqlite::{OpenError, SqliteStore};

static NEXT: AtomicU64 = AtomicU64::new(0);

fn fresh_path() -> PathBuf {
    let directory = std::env::temp_dir().join("mqttiumd-store-sqlite-tests");
    std::fs::create_dir_all(&directory).expect("a temporary directory");
    let path = directory.join(format!(
        "inspect-{}-{}.db",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(with_suffix(&path, suffix));
    }
    path
}

fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut file = path.to_path_buf().into_os_string();
    file.push(suffix);
    PathBuf::from(file)
}

fn roomy() -> Limits {
    Limits {
        payload_bytes: PayloadBytes::new(1 << 20),
        references: References::new(10_000),
        exchange_slots: ExchangeSlots::new(4),
        receipt_slots: ReceiptSlots::new(10_000),
        ..fixture_limits()
    }
}

fn world(path: &Path) -> World<SqliteStore> {
    World::with_limits(
        &|incarnation, limits| SqliteStore::create(path, incarnation, limits).expect("created"),
        roomy(),
    )
}

/// One admission and its release, numbered `n`.
fn exchange(world: &mut World<SqliteStore>, n: u64) {
    let packet = u16::try_from(n % 60_000 + 1).expect("fits");
    let admission = world.admission(2 * n, packet, b"payload", &[world.b]);
    assert!(world.store.apply(&admission).committed_now);
    let release = world.release(2 * n + 1, packet, 2 * n);
    assert!(world.store.apply(&release).committed_now);
}

/// While the writer commits in another thread, every state the inspector
/// reads is one state: its accounts equal what it holds.
#[test]
fn an_inspection_reads_one_state_while_the_writer_commits() {
    let path = fresh_path();
    let mut writer = world(&path);
    let before = writer.store.snapshot();
    assert_eq!(SqliteStore::inspect(&path).expect("inspected"), before);
    let done = Arc::new(AtomicBool::new(false));
    let finished = Arc::clone(&done);
    let thread = std::thread::spawn(move || {
        for n in 1..=300 {
            exchange(&mut writer, n);
        }
        finished.store(true, Ordering::SeqCst);
        writer
    });
    let mut inspections = 0u32;
    while !done.load(Ordering::SeqCst) {
        let state = SqliteStore::inspect(&path).expect("inspected");
        assert_eq!(
            accounts_match_what_is_held(&state),
            Ok(()),
            "inspection {inspections}"
        );
        inspections += 1;
    }
    let writer = thread.join().expect("the writer");
    assert!(inspections > 0);
    assert_eq!(
        SqliteStore::inspect(&path).expect("inspected"),
        writer.store.snapshot()
    );
}

fn bytes(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap_or_default()
}

/// An inspection writes nothing to the database or to its WAL, and
/// holds nothing after it returns: a full checkpoint then succeeds.
#[test]
fn an_inspection_writes_nothing_and_holds_nothing() {
    let path = fresh_path();
    let mut writer = world(&path);
    for n in 1..=20 {
        exchange(&mut writer, n);
    }
    let wal = with_suffix(&path, "-wal");
    let (db_before, wal_before) = (bytes(&path), bytes(&wal));
    assert!(!wal_before.is_empty(), "the WAL holds the commits");
    let state = SqliteStore::inspect(&path).expect("inspected");
    assert_eq!(state, writer.store.snapshot());
    assert_eq!(bytes(&path), db_before);
    assert_eq!(bytes(&wal), wal_before);
    // Nothing reads any more: the whole WAL can be checkpointed and reset.
    let other = rusqlite::Connection::open(&path).expect("a connection");
    let (busy, _, _): (i64, i64, i64) = other
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .expect("checkpoint");
    assert_eq!(busy, 0);
    assert_eq!(std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0), 0);
    drop(writer);
}

/// An inspection refuses what `open` refuses: no file, another schema.
#[test]
fn an_inspection_refuses_what_is_not_this_store() {
    let path = fresh_path();
    assert!(SqliteStore::inspect(&path).is_err());
    assert!(!path.exists(), "an inspection creates no file");
    drop(
        SqliteStore::create(
            &path,
            mqttiumd_lab::store::conformance::INCARNATION,
            roomy(),
        )
        .expect("created"),
    );
    rusqlite::Connection::open(&path)
        .expect("raw")
        .pragma_update(None, "user_version", 2)
        .expect("set");
    assert!(matches!(
        SqliteStore::inspect(&path),
        Err(OpenError::SchemaVersion(2))
    ));
}

/// A restart refuses a file whose pages are damaged, even where opening
/// reads nothing: `PRAGMA quick_check` runs before an epoch is opened.
#[test]
fn a_damaged_file_is_refused_at_restart() {
    let path = fresh_path();
    drop(world(&path).store);
    let (page, size): (i64, i64) = {
        let conn = rusqlite::Connection::open(&path).expect("raw");
        let page = conn
            .query_row(
                "SELECT pageno FROM dbstat WHERE name = 'sessions' ORDER BY pageno LIMIT 1",
                [],
                |row| row.get(0),
            )
            .expect("the page of the sessions");
        let size = conn
            .query_row("PRAGMA page_size", [], |row| row.get(0))
            .expect("page size");
        (page, size)
    };
    let mut file = std::fs::read(&path).expect("the file");
    let start = usize::try_from((page - 1) * size).expect("offset");
    // The page type byte: no valid b-tree page has type 0xFF.
    file[start] = 0xFF;
    std::fs::write(&path, &file).expect("damaged");
    match SqliteStore::open(&path) {
        Err(OpenError::Integrity(report)) => assert!(!report.is_empty()),
        other => panic!("a damaged file was opened: {other:?}"),
    }
}
