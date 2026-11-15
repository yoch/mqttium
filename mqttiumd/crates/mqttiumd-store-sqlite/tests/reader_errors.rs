//! A physically consistent SQLite file is not necessarily a valid durable state.
//! These requirements replace the review's historical-green observation probes.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use mqttiumd_lab::store::conformance::World;
use mqttiumd_store_api::{DurableStore, MAX_CLIENT_ID_BYTES, ReadError, RecoveryRefused};
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
    assert_eq!(
        snapshot.expect("a read refusal must not panic"),
        Err(ReadError::InvalidData)
    );
    assert_eq!(store.try_snapshot(), Err(ReadError::InvalidData));
    assert_eq!(SqliteStore::inspect(&path), Err(ReadError::InvalidData));
    assert_eq!(
        store.recover(None, 64),
        Err(RecoveryRefused::Read(ReadError::InvalidData)),
        "invalid durable data is not an allocation failure"
    );
}

#[test]
fn an_oversized_client_id_is_refused_before_a_snapshot_is_materialised() {
    let path = fixture("oversized-client");
    change(
        &path,
        &format!(
            "UPDATE sessions SET client_id = zeroblob({})",
            MAX_CLIENT_ID_BYTES + 1
        ),
    );
    let store = SqliteStore::open(&path).expect("physical opening succeeds");
    assert_eq!(store.try_snapshot(), Err(ReadError::LimitExceeded));
    assert_eq!(SqliteStore::inspect(&path), Err(ReadError::LimitExceeded));
    assert_eq!(
        store.recover(None, 64),
        Err(RecoveryRefused::Read(ReadError::LimitExceeded))
    );
}

#[test]
fn an_invalid_next_page_does_not_replace_the_first_valid_page() {
    let path = fixture("later-route-error");
    change(&path, "UPDATE routes SET qos = 99");
    let store = SqliteStore::open(&path).expect("physical opening succeeds");
    // Epoch, limits, route-table header fit in the first page. The route
    // on the following page is invalid; lookahead may not decode it early.
    let first = store.recover(None, 3);
    assert!(
        first.is_ok(),
        "the valid first page must remain observable: {first:?}"
    );
    let first = first.expect("checked");
    assert_eq!(first.facts.len(), 3);
    assert!(
        first.next.is_some(),
        "the failed route belongs to a later page"
    );
    assert_eq!(
        store.recover(first.next, 3),
        Err(RecoveryRefused::Read(ReadError::InvalidData))
    );
}

#[test]
fn an_unreasonable_page_request_is_a_limit_not_an_allocation_failure() {
    let path = fixture("oversized-page");
    let store = SqliteStore::open(&path).expect("healthy store opens");
    assert_eq!(
        store.recover(None, usize::MAX),
        Err(RecoveryRefused::Read(ReadError::LimitExceeded)),
        "an invalid requested bound must be refused before any reservation"
    );
}

#[test]
fn a_missing_file_is_unavailable_and_is_not_created_by_inspection() {
    let path = fixture("missing-access");
    let missing = path.with_file_name("missing.sqlite");
    assert_eq!(SqliteStore::inspect(&missing), Err(ReadError::Unavailable));
    let error = SqliteStore::open(&missing).expect_err("an existing store is required");
    assert_eq!(error.read_error(), ReadError::Unavailable);
    assert!(!missing.exists());
}

#[test]
fn an_invalid_ledger_cannot_fall_back_to_an_earlier_cached_read() {
    let path = fixture("no-cache");
    let mut store = SqliteStore::open(&path).expect("healthy store opens");
    let command = mqttiumd_lab::store::conformance::opening_command(&store);
    let operation = mqttiumd_lab::store::conformance::operation_id(command.after, 1);
    let revision: Vec<u8> = rusqlite::Connection::open(&path)
        .expect("raw reader")
        .query_row("SELECT revision FROM meta", [], |row| row.get(0))
        .expect("original revision");
    // NULL fits the SQL schema, but an opened epoch must retain its opener.
    change(&path, "UPDATE meta SET opened_by = NULL");
    assert_eq!(store.ledger(), Err(ReadError::InvalidData));
    assert_eq!(store.open_epoch(&command), Err(ReadError::InvalidData));
    assert_eq!(store.established(operation), Err(ReadError::InvalidData));
    let after: Vec<u8> = rusqlite::Connection::open(&path)
        .expect("raw reader")
        .query_row("SELECT revision FROM meta", [], |row| row.get(0))
        .expect("same revision");
    assert_eq!(after, revision, "a failed read did not open another epoch");
}

#[test]
fn invalid_session_state_is_not_silently_filtered_out_of_recovery() {
    let path = fixture("invalid-session-state");
    change(&path, "DELETE FROM routes; UPDATE sessions SET state = 99");
    let store = SqliteStore::open(&path).expect("physical opening succeeds");
    let first = store.recover(None, 3).expect("valid metadata page");
    assert!(
        first.next.is_some(),
        "invalid sessions are still pending facts"
    );
    assert_eq!(
        store.recover(first.next, 64),
        Err(RecoveryRefused::Read(ReadError::InvalidData))
    );
}
