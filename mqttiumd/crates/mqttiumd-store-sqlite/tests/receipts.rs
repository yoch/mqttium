//! Receipt retirement is the same bounded contract in the two stores.
use mqttiumd_contract::ids::StoreIncarnation;
use mqttiumd_lab::store::conformance;
use mqttiumd_lab::store::durable::MemoryStore;
use mqttiumd_store_api::Limits;
use mqttiumd_store_sqlite::SqliteStore;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);
fn path() -> PathBuf {
    let directory = std::env::temp_dir().join(format!(
        "mqttiumd-gc-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&directory).unwrap();
    directory.join("store.sqlite")
}
fn sqlite(incarnation: StoreIncarnation, limits: Limits) -> SqliteStore {
    SqliteStore::create(&path(), incarnation, limits).unwrap()
}
#[test]
fn live_bounded_prefix() {
    conformance::live_receipt_collection_is_bounded_and_never_reexecutes(&MemoryStore::create);
    conformance::live_receipt_collection_is_bounded_and_never_reexecutes(&sqlite);
}
#[test]
fn holes_and_identity_guards() {
    conformance::live_receipt_collection_refuses_holes_and_preserves_identity_guards(
        &MemoryStore::create,
    );
    conformance::live_receipt_collection_refuses_holes_and_preserves_identity_guards(&sqlite);
}
#[test]
fn control_reserve() {
    conformance::live_receipt_collection_has_a_control_reserve(&MemoryStore::create);
    conformance::live_receipt_collection_has_a_control_reserve(&sqlite);
}
#[test]
fn legacy_zero() {
    conformance::live_receipt_registration_preserves_legacy_zero(&MemoryStore::create);
    conformance::live_receipt_registration_preserves_legacy_zero(&sqlite);
}
#[test]
fn recovered_frontier_supersedes_leftover_receipt_without_ending_claim() {
    conformance::recovery_omits_logically_retired_receipts_but_preserves_live_claims(
        &MemoryStore::create,
    );
    conformance::recovery_omits_logically_retired_receipts_but_preserves_live_claims(&sqlite);
}
#[test]
fn frontier_validation_and_registration_bounds() {
    conformance::live_receipt_frontiers_validate_registration_and_bounds(&MemoryStore::create);
    conformance::live_receipt_frontiers_validate_registration_and_bounds(&sqlite);
}
#[test]
fn full_quota_and_bounded_startup_collection() {
    conformance::collection_at_full_quota_preserves_business_and_startup_collapses_origins(
        &MemoryStore::create,
    );
    conformance::collection_at_full_quota_preserves_business_and_startup_collapses_origins(&sqlite);
}

use mqttiumd_contract::capacity::{ExchangeSlots, ReceiptSlots};
use mqttiumd_contract::command::Command;
use mqttiumd_contract::ids::{OperationSeq, PayloadSeq, PublicationSeq};
use mqttiumd_contract::outcome::{StoreOutcome, StoreReply};
use mqttiumd_contract::packet::Qos;
use mqttiumd_contract::receipts::{
    ReceiptClosure, ReceiptCollection, ReceiptCollectionReply, ReceiptOrigin,
};
use mqttiumd_lab::store::conformance::{World, altered, fixture_limits, operation_id};
use mqttiumd_store_api::{Durability, DurableStore, ReadError};

fn qos1<S>(world: &World<S>, sequence: u64) -> Command {
    altered(&world.admission(sequence, 7, b"payload", &[]), |admit| {
        admit.qos = Qos::AtLeastOnce;
        admit.costs.exchange_slots = ExchangeSlots::ZERO;
    })
}
fn prepared() -> (
    PathBuf,
    World<SqliteStore>,
    World<MemoryStore>,
    ReceiptClosure,
) {
    let path = path();
    let mut limits = fixture_limits();
    limits.receipt_slots = ReceiptSlots::new(2048);
    let mut sql = World::with_limits(
        &|incarnation, limits| SqliteStore::create(&path, incarnation, limits).unwrap(),
        limits,
    );
    let mut memory = World::with_limits(&MemoryStore::create, limits);
    let empty = ReceiptClosure::empty(ReceiptOrigin::of(operation_id(sql.epoch, 1)));
    let request = ReceiptCollection {
        closure: empty,
        limit: 128,
    };
    assert_eq!(
        sql.store.collect_receipts(request),
        memory.store.collect_receipts(request)
    );
    (path, sql, memory, empty)
}

#[test]
fn partial_sweeps_unknown_commit_and_reopen_have_absolute_accounting() {
    let (path, mut sql, mut memory, empty) = prepared();
    for sequence in 1..=4 {
        let command = qos1(&memory, sequence);
        assert_eq!(sql.store.apply(&command), memory.store.apply(&command));
    }
    let closure = ReceiptClosure {
        through: OperationSeq::new(4),
        publication_through: PublicationSeq::new(4),
        payload_through: PayloadSeq::new(4),
        ..empty
    };
    let request = ReceiptCollection { closure, limit: 1 };
    assert_eq!(
        sql.store.collect_receipts(request),
        memory.store.collect_receipts(request)
    );
    let before = memory.store.snapshot().unwrap();
    sql.store.set_durability(Durability::Failing);
    memory.store.set_durability(Durability::Failing);
    assert_eq!(
        sql.store.collect_receipts(request),
        Ok(ReceiptCollectionReply::PendingOrUnknown)
    );
    assert_eq!(
        memory.store.collect_receipts(request),
        Ok(ReceiptCollectionReply::PendingOrUnknown)
    );
    assert_eq!(sql.store.snapshot().unwrap(), before);
    assert_eq!(memory.store.snapshot().unwrap(), before);
    sql.store.set_durability(Durability::Available);
    memory.store.set_durability(Durability::Available);
    let _lost = sql.store.collect_receipts(request).unwrap();
    let _reference = memory.store.collect_receipts(request).unwrap();
    drop(sql.store);
    let mut reopened = SqliteStore::open(&path).unwrap();
    assert_eq!(reopened.receipt_frontier(empty.origin), Ok(Some(closure)));
    assert_eq!(
        reopened.snapshot().unwrap(),
        memory.store.snapshot().unwrap()
    );
    for sequence in 1..=4 {
        let old = qos1(&memory, sequence);
        assert_eq!(
            reopened.apply(&old).reply,
            StoreReply::Outcome(StoreOutcome::RetiredOutcomeUnavailable)
        );
    }
    // An obsolete retry returns current durable knowledge and can finish
    // the same bounded physical sweep; counts are not additive credits.
    let stale = ReceiptCollection {
        closure: empty,
        limit: 1,
    };
    for remaining in [1, 0] {
        assert_eq!(
            reopened.collect_receipts(stale),
            memory.store.collect_receipts(stale)
        );
        assert_eq!(
            reopened.snapshot().unwrap().accounts.receipt_slots.get(),
            remaining
        );
    }
    let stable = reopened.snapshot().unwrap();
    assert_eq!(
        reopened.collect_receipts(stale),
        Ok(ReceiptCollectionReply::Applied {
            closure,
            removed: 0,
            retained: 0,
            more: false
        })
    );
    assert_eq!(
        reopened.snapshot().unwrap(),
        stable,
        "an empty retry makes no revision or busy work"
    );
}

#[test]
fn maintenance_detects_duplicate_evidence_and_oversize_before_copying() {
    for (mutation, cause) in [
        (
            "INSERT INTO fences(id) SELECT id FROM operations",
            ReadError::InvalidData,
        ),
        (
            "UPDATE operations SET recipe=zeroblob(1048577)",
            ReadError::LimitExceeded,
        ),
        ("DROP TABLE receipt_origins", ReadError::Unavailable),
    ] {
        let (path, mut sql, _memory, empty) = prepared();
        assert!(sql.store.apply(&qos1(&sql, 1)).committed_now);
        let raw = rusqlite::Connection::open(&path).unwrap();
        raw.execute_batch(mutation).unwrap();
        assert_eq!(
            raw.query_row("PRAGMA quick_check", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
        let closure = ReceiptClosure {
            through: OperationSeq::new(1),
            publication_through: PublicationSeq::new(1),
            payload_through: PayloadSeq::new(1),
            ..empty
        };
        assert_eq!(
            sql.store.collect_receipts(ReceiptCollection {
                closure,
                limit: 128
            }),
            Err(cause),
            "{mutation}"
        );
        assert_eq!(
            raw.query_row("SELECT count(*) FROM operations", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            1,
            "a read failure cannot delete evidence"
        );
    }
}

#[cfg(feature = "kill-points")]
#[test]
fn real_receipt_barriers_observe_old_then_committed_state_without_command_identity() {
    use mqttiumd_store_sqlite::receipt_points::ReceiptPoint;
    let (path, mut sql, _memory, empty) = prepared();
    assert!(sql.store.apply(&qos1(&sql, 1)).committed_now);
    let closure = ReceiptClosure {
        through: OperationSeq::new(1),
        publication_through: PublicationSeq::new(1),
        payload_through: PayloadSeq::new(1),
        ..empty
    };
    let request = ReceiptCollection {
        closure,
        limit: 128,
    };
    let observations = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let capture = std::sync::Arc::clone(&observations);
    sql.store
        .set_receipt_hook(Some(Box::new(move |point, collection| {
            let read = rusqlite::Connection::open(&path).unwrap();
            let frontier = read
                .query_row("SELECT consumed_through FROM receipt_origins", [], |row| {
                    row.get::<_, Vec<u8>>(0)
                })
                .unwrap();
            let rows = read
                .query_row("SELECT count(*) FROM operations", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap();
            capture
                .lock()
                .unwrap()
                .push((point, collection, frontier, rows));
        })));
    assert!(matches!(
        sql.store.collect_receipts(request),
        Ok(ReceiptCollectionReply::Applied { removed: 1, .. })
    ));
    assert_eq!(
        *observations.lock().unwrap(),
        vec![
            (
                ReceiptPoint::ClosePrepared,
                request,
                0u64.to_be_bytes().to_vec(),
                1
            ),
            (
                ReceiptPoint::CloseCommitted,
                request,
                1u64.to_be_bytes().to_vec(),
                0
            ),
        ]
    );
    sql.store.set_receipt_hook(None);
}
