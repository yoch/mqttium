//! Full contract conformance, differential parity, crash recovery, torn-tail
//! handling, fail-closed corruption detection, and checkpoint compaction tests
//! for `LogStore`.

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use mqttiumd_contract::command::{ClaimDelivery, Command, FinishDelivery, FrozenRecipe, Recipe};
use mqttiumd_contract::ids::{DeliveryId, OutboundPacketId, Revision, StoreIncarnation};
use mqttiumd_contract::outcome::{StoreOutcome, StoreReply};
use mqttiumd_contract::packet::Qos;
use mqttiumd_lab::store::conformance::{self, World, fixture_limits, operation_id, publication_id};
use mqttiumd_lab::store::retained_conformance;
use mqttiumd_store_api::{AdminCommand, Durability, DurableStore, Limits};
use mqttiumd_store_sqlite::{OpenError, SqliteStore, StoragePolicy};
use mqttiumd_store_volatile::{GroupRefused, LOG_MAX_GROUP, LogStore};

static NEXT: AtomicU64 = AtomicU64::new(0);

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "mqttiumd-logstore-{name}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).expect("isolated directory");
    dir
}

/// Wrapper that cleans up its temporary directory when dropped during the
/// conformance suites (which instantiate hundreds of short-lived stores).
struct TempLogStore {
    inner: Option<LogStore>,
    dir: PathBuf,
}

impl TempLogStore {
    fn create(incarnation: StoreIncarnation, limits: Limits) -> Self {
        let dir = temp_dir("conf");
        let path = dir.join("store.sqlite");
        let store =
            LogStore::open_or_create(&path, incarnation, limits, None, false).expect("log store");
        Self {
            inner: Some(store),
            dir,
        }
    }

    fn store(&self) -> &LogStore {
        self.inner.as_ref().expect("live store")
    }

    fn store_mut(&mut self) -> &mut LogStore {
        self.inner.as_mut().expect("live store")
    }
}

impl Drop for TempLogStore {
    fn drop(&mut self) {
        drop(self.inner.take());
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl DurableStore for TempLogStore {
    fn read_wills(
        &self,
        after: Option<mqttiumd_contract::ids::OperationId>,
        limit: usize,
        budget: mqttiumd_store_api::ReadBudget,
    ) -> Result<mqttiumd_store_api::WillPage, mqttiumd_store_api::ReadError> {
        self.store().read_wills(after, limit, budget)
    }

    fn expire_messages(
        &mut self,
        now: mqttiumd_contract::message_expiry::MessageTime,
        after: Option<mqttiumd_contract::ids::PublicationId>,
        limit: u32,
    ) -> Result<mqttiumd_store_api::MessageExpirySweep, mqttiumd_store_api::ReadError> {
        self.store_mut().expire_messages(now, after, limit)
    }

    fn checkpoint_session_clock(
        &mut self,
        expected: Option<mqttiumd_store_api::SessionClockAnchor>,
        reading: mqttiumd_store_api::SessionClockReading,
    ) -> Result<mqttiumd_store_api::SessionClockAnchor, mqttiumd_store_api::ReadError> {
        self.store_mut().checkpoint_session_clock(expected, reading)
    }

    fn collect_receipts(
        &mut self,
        collection: mqttiumd_contract::receipts::ReceiptCollection,
    ) -> Result<mqttiumd_contract::receipts::ReceiptCollectionReply, mqttiumd_store_api::ReadError>
    {
        self.store_mut().collect_receipts(collection)
    }

    fn receipt_frontier(
        &self,
        origin: mqttiumd_contract::receipts::ReceiptOrigin,
    ) -> Result<Option<mqttiumd_contract::receipts::ReceiptClosure>, mqttiumd_store_api::ReadError>
    {
        self.store().receipt_frontier(origin)
    }

    fn collect_closed_receipts(
        &mut self,
        collection: mqttiumd_contract::receipts::ClosedReceiptCollection,
    ) -> Result<mqttiumd_contract::receipts::ClosedReceiptReply, mqttiumd_store_api::ReadError>
    {
        self.store_mut().collect_closed_receipts(collection)
    }

    fn ledger(
        &self,
    ) -> Result<mqttiumd_contract::command::EpochLedger, mqttiumd_store_api::ReadError> {
        self.store().ledger()
    }

    fn open_epoch(
        &mut self,
        command: &mqttiumd_contract::command::OpenEpoch,
    ) -> Result<mqttiumd_contract::outcome::OpeningOutcome, mqttiumd_store_api::ReadError> {
        self.store_mut().open_epoch(command)
    }

    fn apply(&mut self, command: &Command) -> mqttiumd_store_api::Applied {
        self.store_mut().apply(command)
    }

    fn established(
        &self,
        operation: mqttiumd_contract::ids::OperationId,
    ) -> Result<Option<StoreOutcome>, mqttiumd_store_api::ReadError> {
        self.store().established(operation)
    }

    fn resolve_and_fence(
        &mut self,
        operation: mqttiumd_contract::ids::OperationId,
    ) -> mqttiumd_store_api::Resolved {
        self.store_mut().resolve_and_fence(operation)
    }

    fn admin(
        &mut self,
        command: AdminCommand,
    ) -> Result<mqttiumd_store_api::AdminDone, mqttiumd_store_api::AdminRefused> {
        self.store_mut().admin(command)
    }

    fn recover_with_budget(
        &self,
        cursor: Option<mqttiumd_store_api::RecoveryCursor>,
        limit: usize,
        budget: mqttiumd_store_api::ReadBudget,
    ) -> Result<mqttiumd_store_api::RecoveryPage, mqttiumd_store_api::RecoveryRefused> {
        self.store().recover_with_budget(cursor, limit, budget)
    }

    fn read_work_with_budget(
        &self,
        session: mqttiumd_contract::ids::SessionId,
        after: Option<DeliveryId>,
        limit: usize,
        budget: mqttiumd_store_api::ReadBudget,
    ) -> Result<mqttiumd_store_api::WorkPage, mqttiumd_store_api::ReadError> {
        self.store()
            .read_work_with_budget(session, after, limit, budget)
    }

    fn snapshot_with_budget(
        &self,
        budget: mqttiumd_store_api::ReadBudget,
    ) -> Result<mqttiumd_store_api::DurableState, mqttiumd_store_api::ReadError> {
        self.store().snapshot_with_budget(budget)
    }
}

#[test]
fn log_passes_full_store_conformance_suite() {
    conformance::all(&TempLogStore::create);
}

#[test]
fn log_passes_full_retained_conformance_suite() {
    retained_conformance::all(&TempLogStore::create);
}

#[test]
fn log_passes_full_message_expiry_conformance_suite() {
    retained_conformance::expiry::all(&TempLogStore::create);
}

#[test]
fn log_apply_group_matches_sqlite_and_recovers_after_uncompacted_crash() {
    let dir = temp_dir("diff-group");
    let log_path = dir.join("log.sqlite");
    let sql_path = dir.join("sql.sqlite");
    let limits = fixture_limits();

    let mut log_world = World::with_limits(
        &|incarnation, lim| {
            LogStore::open_or_create(&log_path, incarnation, lim, None, false).expect("log")
        },
        limits,
    );
    let mut sql_world = World::with_limits(
        &|incarnation, lim| SqliteStore::create(&sql_path, incarnation, lim).expect("sqlite"),
        limits,
    );

    // Oversized group refused without mutation.
    let c0 = log_world.admission(1, 7, b"one", &[log_world.b]);
    let oversized: Vec<&Command> = vec![&c0; LOG_MAX_GROUP + 1];
    assert_eq!(
        log_world.store.apply_group(&oversized),
        Err(GroupRefused::TooLarge {
            commands: LOG_MAX_GROUP + 1,
            max: LOG_MAX_GROUP,
        })
    );

    let a1 = log_world.admission(1, 7, b"alpha", &[log_world.b, log_world.c]);
    let a2 = log_world.admission(2, 8, b"beta", &[log_world.b]);
    let a_dup = log_world.admission(1, 7, b"alpha", &[log_world.b, log_world.c]);
    let a_bad = log_world.admission(3, 7, b"dup-pi", &[log_world.b]);
    let claim = Command {
        recipe: FrozenRecipe::freeze(Recipe::ClaimDelivery(ClaimDelivery {
            operation: operation_id(log_world.epoch, 4),
            delivery: DeliveryId {
                publication: publication_id(log_world.epoch, 1),
                index: 0,
            },
            target: log_world.b,
            target_revision: Revision::new(1),
            packet_id: OutboundPacketId::new(11).unwrap(),
        }))
        .unwrap(),
        payload: None,
    };
    let finish = Command {
        recipe: FrozenRecipe::freeze(Recipe::FinishDelivery(FinishDelivery {
            operation: operation_id(log_world.epoch, 5),
            delivery: DeliveryId {
                publication: publication_id(log_world.epoch, 1),
                index: 0,
            },
            target: log_world.b,
            target_revision: Revision::new(1),
            packet_id: OutboundPacketId::new(11).unwrap(),
            claim: operation_id(log_world.epoch, 4),
        }))
        .unwrap(),
        payload: None,
    };
    let batch = [&a1, &a2, &a_dup, &a_bad, &claim, &finish];

    let log_replies = log_world.store.apply_group(&batch).expect("log group");
    let sql_replies = sql_world.store.apply_group(&batch).expect("sqlite group");
    assert_eq!(log_replies, sql_replies);
    let expected = sql_world.store.snapshot().expect("sql snapshot");
    assert_eq!(log_world.store.snapshot().expect("log snapshot"), expected);

    // Failing durability on a write group leaves state & WAL intact and answers Unknown.
    let wal_before = log_world.store.wal_len();
    let fresh = log_world.admission(6, 9, b"gamma", &[log_world.c]);
    log_world.store.set_durability(Durability::Failing);
    sql_world.store.set_durability(Durability::Failing);
    let log_fail = log_world.store.apply_group(&[&a1, &fresh]).unwrap();
    let sql_fail = sql_world.store.apply_group(&[&a1, &fresh]).unwrap();
    assert_eq!(log_fail, sql_fail);
    assert!(
        log_fail
            .iter()
            .all(|r| matches!(r.reply, StoreReply::Outcome(StoreOutcome::PendingOrUnknown)))
    );
    assert_eq!(log_world.store.wal_len(), wal_before);
    assert_eq!(log_world.store.snapshot().unwrap(), expected);

    // Read-only group on failing medium still answers established outcomes.
    let log_ro = log_world.store.apply_group(&[&a1, &a2]).unwrap();
    let sql_ro = sql_world.store.apply_group(&[&a1, &a2]).unwrap();
    assert_eq!(log_ro, sql_ro);

    // Simulate sudden crash WITHOUT calling compact(): drop LogStore and reopen!
    drop(log_world);
    let reopened =
        LogStore::open_or_create(&log_path, StoreIncarnation::new(999), limits, None, true)
            .expect("crash recovery reopen");
    assert_eq!(
        reopened.snapshot().expect("recovered snapshot"),
        expected,
        "crash recovery from uncompacted .mlog must match SQLite byte-for-byte"
    );

    drop(reopened);
    drop(sql_world);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn log_recovers_from_torn_tail_and_refuses_midfile_corruption() {
    let dir = temp_dir("torn-tail");
    let path = dir.join("store.sqlite");
    let wal_path = dir.join("store.sqlite.mlog");
    let limits = fixture_limits();

    let mut world = World::with_limits(
        &|incarnation, lim| {
            LogStore::open_or_create(&path, incarnation, lim, None, false).expect("log")
        },
        limits,
    );
    let a1 = world.admission(1, 7, b"first", &[world.b]);
    let a2 = world.admission(2, 8, b"second", &[world.c]);
    assert!(world.store.apply(&a1).committed_now);
    assert!(world.store.apply(&a2).committed_now);
    let valid_len = world.store.wal_len();
    let expected = world.store.snapshot().expect("expected state");
    drop(world);

    // Case 1: Truncated partial frame at EOF (power loss mid-write).
    {
        let mut f = OpenOptions::new().write(true).open(&wal_path).unwrap();
        f.set_len(valid_len).unwrap();
        f.seek(SeekFrom::Start(valid_len)).unwrap();
        f.write_all(&[0, 0, 0, 80, 0, 1, 1, 0, 0, 0, 5]).unwrap();
        f.sync_all().unwrap();
    }
    {
        let recovered =
            LogStore::open_or_create(&path, StoreIncarnation::new(9), limits, None, true)
                .expect("recover truncated tail");
        assert_eq!(recovered.snapshot().unwrap(), expected);
        assert_eq!(recovered.wal_len(), valid_len);
    }

    // Case 2: Complete-length final frame with corrupted CRC inside pre-allocated tail.
    {
        let mut f = OpenOptions::new().write(true).open(&wal_path).unwrap();
        f.seek(SeekFrom::Start(valid_len)).unwrap();
        // body_len = 6, body = [0,1, 9,9,9,9], bad crc = 0xdeadbeef
        f.write_all(&[0, 0, 0, 6, 0, 1, 9, 9, 9, 9, 0xde, 0xad, 0xbe, 0xef])
            .unwrap();
        // Pre-allocated zero bytes follow the torn frame.
        f.write_all(&[0u8; 4096]).unwrap();
        f.sync_all().unwrap();
    }
    {
        let recovered =
            LogStore::open_or_create(&path, StoreIncarnation::new(9), limits, None, true)
                .expect("recover corrupt tail frame");
        assert_eq!(recovered.snapshot().unwrap(), expected);
        assert_eq!(recovered.wal_len(), valid_len);
    }

    // Case 3: Mid-file bit-flip corruption MUST fail closed with OpenError::Corrupt.
    {
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&wal_path)
            .unwrap();
        // Flip a byte inside the very first committed frame body (offset 30).
        f.seek(SeekFrom::Start(30)).unwrap();
        let mut byte = [0u8; 1];
        f.read_exact(&mut byte).unwrap();
        byte[0] ^= 0xff;
        f.seek(SeekFrom::Start(30)).unwrap();
        f.write_all(&byte).unwrap();
        f.sync_all().unwrap();
    }
    let err = LogStore::open_or_create(&path, StoreIncarnation::new(9), limits, None, true)
        .expect_err("mid-file corruption must fail closed");
    assert!(matches!(err, OpenError::Corrupt), "got {err:?}");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn log_compacts_automatically_and_handles_stale_wal_and_schema15_policy() {
    let dir = temp_dir("compact-policy");
    let path = dir.join("secured.sqlite");
    let wal_path = dir.join("secured.sqlite.mlog");
    let limits = fixture_limits();

    let mut owners = BTreeMap::new();
    owners.insert(b"p1-producer".to_vec(), "alice".to_owned());
    owners.insert(b"p1-b".to_vec(), "bob".to_owned());
    owners.insert(b"p1-c".to_vec(), "carol".to_owned());
    let policy = StoragePolicy {
        fingerprint: [0x5a; 32],
        owners,
    };

    let mut world = World::with_limits(
        &|incarnation, lim| {
            let mut s =
                LogStore::open_or_create(&path, incarnation, lim, Some(policy.clone()), false)
                    .expect("secured log store");
            s.set_checkpoint_bytes(256);
            s
        },
        limits,
    );
    let a1 = world.admission(1, 7, &[b'x'; 512], &[world.b]);
    assert!(world.store.apply(&a1).committed_now);
    // Automatic compaction should have reset the WAL to header-only (24 bytes).
    assert_eq!(world.store.wal_len(), 24);
    let expected = world.store.snapshot().expect("snapshot");
    let target_b = world.b;
    drop(world);

    // Plain SQLite open MUST be refused (Schema 15 on disk).
    assert!(matches!(
        SqliteStore::open(&path),
        Err(OpenError::SchemaVersion(
            mqttiumd_store_sqlite::SECURE_SCHEMA_VERSION
        ))
    ));
    // Secured SQLite open sees the exact compacted state.
    let sql = SqliteStore::open_secured(&path, &policy).expect("open_secured");
    assert_eq!(sql.snapshot().unwrap(), expected);
    drop(sql);

    // Simulate crash between checkpoint rename (rev = R > 0) and WAL rotation
    // (stale .mlog header still claiming base_revision = 0 with stale bytes).
    {
        let mut raw = std::fs::read(&wal_path).unwrap();
        raw[16..24].copy_from_slice(&0u64.to_be_bytes());
        raw.extend_from_slice(b"stale-pre-checkpoint-bytes");
        std::fs::write(&wal_path, &raw).unwrap();
    }
    let mut reopened = LogStore::open_or_create(
        &path,
        StoreIncarnation::new(99),
        limits,
        Some(policy.clone()),
        true,
    )
    .expect("reopen discards stale wal");
    assert_eq!(reopened.wal_len(), 24);
    assert_eq!(reopened.snapshot().unwrap(), expected);

    // Add a route after recovery (uncompacted) and verify crash reopen preserves it.
    reopened.set_checkpoint_bytes(64 * 1024 * 1024);
    reopened
        .admin(AdminCommand::AddRoute {
            topic: b"factory/#".to_vec(),
            session: target_b,
            qos: Qos::AtLeastOnce,
        })
        .expect("admin add route");
    let updated = reopened.snapshot().unwrap();
    assert!(reopened.wal_len() > 24);
    drop(reopened);

    let final_reopened =
        LogStore::open_or_create(&path, StoreIncarnation::new(99), limits, Some(policy), true)
            .expect("final reopen");
    assert_eq!(final_reopened.snapshot().unwrap(), updated);
    drop(final_reopened);

    let _ = std::fs::remove_dir_all(&dir);
}
