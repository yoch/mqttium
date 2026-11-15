//! An emulation of a process killed while `COMMIT` writes the WAL (plan
//! `docs/missions/L7-kill-inside-a-transaction.md`, K2a). It is a copy of
//! files in a test, not a kill: the WAL of one admission is truncated at
//! every frame boundary and inside every frame, put beside a copy of the
//! database taken before the admission, and opened. Every cut must
//! recover to the state before the admission or to the state after it,
//! never a mix, with `quick_check` passing and the accounts matching.
//!
//! The bench's rule that nothing touches the files between a kill and the
//! restart is unchanged: these copies are made in a test, of a store that
//! is alive and idle, and the live files are only read.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use mqttiumd_contract::capacity::{ExchangeSlots, PayloadBytes, ReceiptSlots, References};
use mqttiumd_lab::store::conformance::{World, accounts_match_what_is_held, fixture_limits};
use mqttiumd_lab::store::durable::{DurableStore, Limits, MemoryStore};
use mqttiumd_store_sqlite::SqliteStore;

/// The WAL header and the header of each frame, in bytes (file format of
/// SQLite, section "WAL file format").
const WAL_HEADER: u64 = 32;
const FRAME_HEADER: u64 = 24;

static NEXT: AtomicU64 = AtomicU64::new(0);

fn fresh_path(name: &str) -> PathBuf {
    let directory = std::env::temp_dir().join("mqttiumd-store-sqlite-tests");
    std::fs::create_dir_all(&directory).expect("a temporary directory");
    let path = directory.join(format!(
        "wal-cut-{name}-{}-{}.db",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    for suffix in ["", "-wal", "-shm", ".writer-lock"] {
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

fn exchange<S: DurableStore>(world: &mut World<S>, n: u64) {
    let packet = u16::try_from(n).expect("fits");
    let admission = world.admission(2 * n, packet, b"payload", &[world.b]);
    assert!(world.store.apply(&admission).committed_now);
    let release = world.release(2 * n + 1, packet, 2 * n);
    assert!(world.store.apply(&release).committed_now);
}

/// Every cut of the WAL of one admission recovers to before or after it.
#[test]
fn every_cut_of_the_wal_of_a_commit_recovers_before_or_after_it() {
    let path = fresh_path("live");
    let mut world = World::with_limits(
        &|incarnation, limits| SqliteStore::create(&path, incarnation, limits).expect("created"),
        roomy(),
    );
    let mut reference = World::with_limits(&MemoryStore::create, roomy());
    for n in 1..=3 {
        exchange(&mut world, n);
        exchange(&mut reference, n);
    }
    let before = reference.store.snapshot();
    assert_eq!(world.store.try_snapshot().expect("decodes"), before);
    // Everything so far into the database, and the WAL reset: the WAL
    // will then hold the admission alone.
    let other = rusqlite::Connection::open(&path).expect("a connection");
    let (busy, _, _): (i64, i64, i64) = other
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .expect("checkpoint");
    assert_eq!(busy, 0);
    drop(other);
    let database = std::fs::read(&path).expect("the database");
    let admission = world.admission(8, 4, b"cut payload", &[world.b, world.c]);
    assert!(world.store.apply(&admission).committed_now);
    assert!(reference.store.apply(&admission).committed_now);
    let after = reference.store.snapshot();
    let wal = std::fs::read(with_suffix(&path, "-wal")).expect("the WAL");
    let page = u64::try_from(world.store.effective_settings().page_size).expect("positive");
    let frame = FRAME_HEADER + page;
    let length = u64::try_from(wal.len()).expect("fits");
    assert_eq!((length - WAL_HEADER) % frame, 0, "whole frames");
    let frames = (length - WAL_HEADER) / frame;
    assert!(frames >= 2, "the admission writes several pages");
    // Cuts: inside the header, at each frame boundary, and inside each
    // frame (in its header, and in its page).
    let mut cuts = vec![0, WAL_HEADER / 2];
    for k in 0..=frames {
        let start = WAL_HEADER + k * frame;
        cuts.push(start);
        if k < frames {
            cuts.extend([start + FRAME_HEADER / 2, start + FRAME_HEADER + page / 2]);
        }
    }
    println!("{frames} frames of {frame} bytes, {} cuts", cuts.len());
    let mut recovered_after = Vec::new();
    for cut in cuts {
        let copy = fresh_path("cut");
        std::fs::write(&copy, &database).expect("the database copied");
        let kept = usize::try_from(cut).expect("fits");
        std::fs::write(with_suffix(&copy, "-wal"), &wal[..kept]).expect("the WAL cut");
        let store = SqliteStore::open(&copy)
            .unwrap_or_else(|error| panic!("the cut at {cut} does not open: {error:?}"));
        let state = store.try_snapshot().expect("decodes");
        assert_eq!(accounts_match_what_is_held(&state), Ok(()), "cut at {cut}");
        if state == after {
            recovered_after.push(cut);
        } else {
            assert_eq!(
                state, before,
                "the cut at {cut} is neither before nor after"
            );
        }
    }
    // The commit is its last frame: only the whole WAL holds it.
    assert_eq!(recovered_after, vec![length]);
    drop(world);
}
