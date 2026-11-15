//! A physically consistent SQLite file is not necessarily a valid durable state.
//! These requirements replace the review's historical-green observation probes.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use mqttiumd_lab::store::conformance::World;
use mqttiumd_store_api::{DurableStore, MAX_CLIENT_ID_BYTES, RecoveryRefused};
use mqttiumd_store_sqlite::SqliteStore;

static NEXT: AtomicU64 = AtomicU64::new(0);

fn fixture(name: &str) -> PathBuf {
    let directory = std::env::temp_dir().join(format!(
        "mqttiumd-reader-{name}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&directory).expect("isolated directory");
    let path = directory.join("store.sqlite");
    let world = World::new(&|incarnation, limits| {
        SqliteStore::create(&path, incarnation, limits).expect("store created")
    });
    drop(world);
    path
}

fn change(path: &std::path::Path, sql: &str) {
    let raw = rusqlite::Connection::open(path).expect("isolated file opened");
    raw.execute_batch(sql).expect("controlled mutation");
    let checked: String = raw
        .query_row("PRAGMA quick_check", [], |row| row.get(0))
        .expect("physical check runs");
    assert_eq!(checked, "ok", "the fixture is physically consistent");
}

#[test]
fn an_invalid_route_never_panics_or_claims_an_allocation_failure() {
    let path = fixture("invalid-route");
    change(&path, "UPDATE routes SET qos = 99");
    let store = SqliteStore::open(&path).expect("physical opening succeeds");
    let snapshot = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| store.snapshot()));
    assert!(snapshot.is_ok(), "an invalid durable route must be a typed read refusal, not a panic");
    assert_ne!(store.recover(None, 64), Err(RecoveryRefused::AllocationFailed),
        "invalid durable data is not an allocation failure");
}

#[test]
fn an_oversized_client_id_is_refused_before_a_snapshot_is_materialised() {
    let path = fixture("oversized-client");
    change(&path, &format!(
        "UPDATE sessions SET client_id = zeroblob({})", MAX_CLIENT_ID_BYTES + 1
    ));
    let store = SqliteStore::open(&path).expect("physical opening succeeds");
    assert!(store.try_snapshot().is_err(),
        "a snapshot must enforce MAX_CLIENT_ID_BYTES before copying the field");
}
