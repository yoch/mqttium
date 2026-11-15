//! Characterise the SQLite/rusqlite versions actually linked, rather than
//! infer an installed callback from a PRAGMA. No store writer is involved.
#![cfg(feature = "kill-points")]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn a_wal_callback_runs_after_the_write_lock_is_released_and_a_pragma_replaces_it() {
    let directory = std::env::temp_dir().join(format!("mqttiumd-wal-hook-{}", std::process::id()));
    std::fs::create_dir_all(&directory).expect("directory");
    let path = directory.join("characterisation.db");
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
    let mut connection = rusqlite::Connection::open(&path).expect("connection");
    connection
        .execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; CREATE TABLE t (n INTEGER)",
        )
        .expect("configured");
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&calls);
    let other_path = path.clone();
    connection
        .wal_hook(Some(
            move |_wal: &mut rusqlite::hooks::Wal, _frames: i32| {
                // The hook is after release of the engine's write lock. A second
                // connection can acquire it now; it performs no durable mutation.
                let other = rusqlite::Connection::open(&other_path)?;
                other.busy_timeout(std::time::Duration::ZERO)?;
                other.execute_batch("BEGIN IMMEDIATE; ROLLBACK")?;
                counted.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        ))
        .expect("hook installed");
    connection
        .execute("INSERT INTO t VALUES (1)", [])
        .expect("commit");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "actual hook and second writer lock observed"
    );
    connection
        .pragma_update(None, "wal_autocheckpoint", 1)
        .expect("autocheckpoint installed later");
    connection
        .execute("INSERT INTO t VALUES (2)", [])
        .expect("commit");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the later autocheckpoint removed the callback"
    );
    let counted = Arc::clone(&calls);
    connection
        .wal_hook(Some(
            move |_wal: &mut rusqlite::hooks::Wal, _frames: i32| {
                counted.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        ))
        .expect("hook installed last");
    connection
        .execute("INSERT INTO t VALUES (3)", [])
        .expect("commit");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "installing the callback last restores it"
    );
    println!(
        "SQLite {}; rusqlite 0.43.0: observed WAL callback, released write lock, replacement by autocheckpoint, callback reinstallation",
        rusqlite::version()
    );
}
