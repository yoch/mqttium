//! An exploration of artificial storage images (plan
//! `docs/missions/L7-kill-inside-a-transaction.md`). It is a copy of
//! files in a test, separate from process kills: the WAL of one admission is truncated at
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
use mqttiumd_lab::store::evidence::{SnapshotContext, snapshot};
use mqttiumd_store_sqlite::SqliteStore;
use serde_json::json;

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
    let artifact_directory = std::env::var_os("MQTTIUMD_WAL_PREFIX_OUT").map(PathBuf::from);
    if let Some(directory) = &artifact_directory {
        std::fs::create_dir(directory).expect("new artifact directory");
        std::fs::create_dir(directory.join("prefixes")).expect("prefix directory");
    }
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
    let checkpoint: (i64, i64, i64) = other
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .expect("checkpoint");
    assert_eq!(
        checkpoint,
        (0, 0, 0),
        "TRUNCATE completed and reset the WAL"
    );
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
    assert!(
        frames <= 256,
        "finite experiment budget: at most 256 frames"
    );
    let be32 = |bytes: &[u8]| u32::from_be_bytes(bytes.try_into().expect("four bytes"));
    assert_eq!(
        u64::from(be32(&wal[8..12])),
        page,
        "WAL page size matches engine"
    );
    let frame_metadata: Vec<_> = (0..frames)
        .map(|index| {
            let offset = usize::try_from(WAL_HEADER + index * frame).expect("fits");
            let database_pages = be32(&wal[offset + 4..offset + 8]);
            json!({
                "index": index.to_string(), "offset": offset.to_string(),
                "page_number": be32(&wal[offset..offset + 4]).to_string(),
                "commit_database_pages": database_pages.to_string(),
                "is_commit_frame": database_pages != 0,
            })
        })
        .collect();
    assert_eq!(
        frame_metadata
            .iter()
            .filter(|frame| frame["is_commit_frame"] == true)
            .count(),
        1,
        "after TRUNCATE the WAL contains the single target transaction"
    );
    assert_eq!(
        frame_metadata.last().expect("frames")["is_commit_frame"],
        true
    );
    if let Some(directory) = &artifact_directory {
        std::fs::write(directory.join("reference-before.db"), &database).expect("base image");
        std::fs::write(directory.join("target-transaction.wal"), &wal).expect("associated WAL");
        for (name, state) in [("before", &before), ("after", &after)] {
            std::fs::write(
                directory.join(format!("{name}.durable-state.txt")),
                format!("{state:#?}\n"),
            )
            .expect("complete reference state");
            let document = snapshot(
                state,
                SnapshotContext {
                    run_id: "wal-prefix-experiment",
                    checkpoint: name,
                    backend: "sim-store",
                },
            );
            std::fs::write(
                directory.join(format!("{name}.snapshot.json")),
                serde_json::to_vec_pretty(&document).expect("JSON"),
            )
            .expect("reference snapshot");
        }
    }
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
    let mut observations = Vec::new();
    for cut in cuts {
        let copy = fresh_path("cut");
        std::fs::write(&copy, &database).expect("the database copied");
        let kept = usize::try_from(cut).expect("fits");
        std::fs::write(with_suffix(&copy, "-wal"), &wal[..kept]).expect("the WAL cut");
        if let Some(directory) = &artifact_directory {
            std::fs::write(
                directory
                    .join("prefixes")
                    .join(format!("{cut:08}.wal-prefix")),
                &wal[..kept],
            )
            .expect("immutable input prefix");
        }
        let store = SqliteStore::open(&copy)
            .unwrap_or_else(|error| panic!("the cut at {cut} does not open: {error:?}"));
        let state = store.try_snapshot().expect("decodes");
        assert_eq!(accounts_match_what_is_held(&state), Ok(()), "cut at {cut}");
        let result = if state == after {
            recovered_after.push(cut);
            "after"
        } else {
            assert_eq!(
                state, before,
                "the cut at {cut} is neither before nor after"
            );
            "before"
        };
        observations.push(json!({
            "cut_bytes": cut.to_string(), "recovered": result,
            "quick_check": "ok (required by SqliteStore::open)",
            "accounts_match": true, "full_state_matches_reference": true,
            "frame_boundary": cut >= WAL_HEADER && (cut - WAL_HEADER) % frame == 0,
            "new_epoch_opened": false,
        }));
    }
    if let Some(directory) = &artifact_directory {
        let settings = world.store.effective_settings();
        let report = json!({
            "format": "mqttiumd.wal-prefix-experiment", "version": 1,
            "scope": "artificial storage images; no process kill; no power-loss claim",
            "construction": "reference-before.db is the checkpointed prefix of three admissions and releases plus fixture; target-transaction.wal contains only the following multirow admission to two targets, no subsequent checkpoint",
            "comparison": "whole DurableState equality against MemoryStore before/after target, before any recovery epoch is opened; no projection",
            "checkpoint_before_capture": {"mode": "TRUNCATE", "busy": checkpoint.0, "log_frames": checkpoint.1, "checkpointed_frames": checkpoint.2},
            "sqlite_version": settings.sqlite_version, "compile_options": settings.compile_options,
            "rusqlite_version": "0.43.0", "journal_mode": settings.journal_mode,
            "synchronous": settings.synchronous, "page_size": page.to_string(),
            "wal_autocheckpoint": settings.wal_autocheckpoint.to_string(),
            "checkpoint_policy": settings.checkpoint_policy,
            "wal_header_bytes": WAL_HEADER, "frame_header_bytes": FRAME_HEADER,
            "wal_bytes": length.to_string(), "frame_bytes": frame.to_string(),
            "frames": frame_metadata, "target_operation_sequence": "8",
            "target_packet_id": 4, "before_revision": before.revision.to_string(),
            "after_revision": after.revision.to_string(), "trials": observations,
            "before_count": observations.len() - recovered_after.len(), "after_count": recovered_after.len(),
            "errors": 0, "verdict": "all constructed images match their expected complete prefix",
        });
        std::fs::write(
            directory.join("report.json"),
            serde_json::to_vec_pretty(&report).expect("JSON"),
        )
        .expect("report written");
    }
    // The commit is its last frame: only the whole WAL holds it.
    assert_eq!(recovered_after, vec![length]);
    drop(world);
}
