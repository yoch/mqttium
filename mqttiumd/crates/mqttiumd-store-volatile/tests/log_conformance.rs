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

/// A group the writer may legitimately form must survive a restart whatever
/// its size on disk. Seventy admissions of one mebibyte each are one group
/// of the writer (at most `LOG_MAX_GROUP` commands, each within the payload
/// limit) and more than the 64 MiB a frame may hold when it is read back.
/// Every member is answered committed, so every member must be found again.
#[test]
fn log_reopens_after_a_group_larger_than_one_frame() {
    const MEMBERS: u64 = 70;
    const PAYLOAD: usize = 1024 * 1024;
    let dir = temp_dir("large-group");
    let path = dir.join("store.sqlite");
    let limits = Limits {
        payload_bytes: mqttiumd_contract::capacity::PayloadBytes::new(128 * 1024 * 1024),
        references: mqttiumd_contract::capacity::References::new(256),
        exchange_slots: mqttiumd_contract::capacity::ExchangeSlots::new(128),
        receipt_slots: mqttiumd_contract::capacity::ReceiptSlots::new(256),
        max_payload_bytes: PAYLOAD,
        ..fixture_limits()
    };
    let mut world = World::with_limits(
        &|incarnation, lim| {
            LogStore::open_or_create(&path, incarnation, lim, None, false).expect("log")
        },
        limits,
    );
    let payload =
        mqttiumd_contract::packet::SharedBytes::copy_from(&vec![0x5a_u8; PAYLOAD], PAYLOAD)
            .expect("within the limit");
    let length = mqttiumd_contract::capacity::PayloadBytes::new(PAYLOAD as u64);
    let commands: Vec<Command> = (1..=MEMBERS)
        .map(|sequence| {
            let packet_id = u16::try_from(sequence).expect("small");
            // The builder of the fixture copies at most a kibibyte: the
            // admission is rebuilt around the large payload.
            let small = world.admission(sequence, packet_id, b"x", &[world.b]);
            let mut large = conformance::altered(&small, |admit| {
                admit.payload.length = length;
                admit.costs.payload_bytes = length;
            });
            large.payload = Some(payload.clone());
            large
        })
        .collect();
    let group: Vec<&Command> = commands.iter().collect();
    assert!(group.len() <= LOG_MAX_GROUP);
    let replies = world
        .store
        .apply_group(&group)
        .expect("a group within the bound");
    for (sequence, reply) in (1..=MEMBERS).zip(&replies) {
        assert!(
            matches!(reply.reply, StoreReply::Outcome(StoreOutcome::Committed(_))),
            "member {sequence}: {:?}",
            reply.reply
        );
    }
    let expected = world
        .store
        .snapshot()
        .expect("the state that was acknowledged");
    drop(world);

    let reopened = LogStore::open_or_create(&path, StoreIncarnation::new(9), limits, None, true)
        .expect("a log the store wrote and acknowledged opens again");
    assert_eq!(reopened.snapshot().unwrap(), expected);
    drop(reopened);
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

#[test]
fn claimed_publication_matches_sqlite_and_recovers_after_crash() {
    use mqttiumd_contract::command::{ClaimedPublication, FinishDelivery, InlineClaim};

    let dir = temp_dir("claimed-pub");
    let log_path = dir.join("log.sqlite");
    let sql_path = dir.join("sql.sqlite");
    let limits = fixture_limits();

    let mut log_world = World::with_limits(
        &|incarnation, lim| {
            let mut s = LogStore::open_or_create(&log_path, incarnation, lim, None, false)
                .expect("log store");
            s.set_checkpoint_bytes(64 * 1024 * 1024);
            s
        },
        limits,
    );
    let mut sql_world = World::with_limits(
        &|incarnation, lim| SqliteStore::create(&sql_path, incarnation, lim).expect("sqlite store"),
        limits,
    );

    let make_claimed = |world: &World<LogStore>, seq: u64, in_pi: u16, out_pi: u16, body: &[u8]| {
        let base = world.admission(seq, in_pi, body, &[world.b]);
        let Recipe::AdmitPublication(admit) =
            Recipe::decode_canonical(&base.recipe.to_canonical_bytes(), 16, 64).unwrap()
        else {
            panic!("expected AdmitPublication");
        };
        Command {
            recipe: FrozenRecipe::freeze(Recipe::ClaimedPublication(ClaimedPublication {
                admission: admit,
                claim: InlineClaim {
                    target_index: 0,
                    packet_id: OutboundPacketId::new(out_pi).unwrap(),
                },
            }))
            .unwrap(),
            payload: base.payload,
        }
    };

    let c1 = make_claimed(&log_world, 1, 7, 11, b"inline-one");
    let c_dup_pi = make_claimed(&log_world, 2, 8, 11, b"inline-dup-pi");
    let c2 = make_claimed(&log_world, 3, 9, 12, b"inline-two");
    let fin1 = Command {
        recipe: FrozenRecipe::freeze(Recipe::FinishDelivery(FinishDelivery {
            operation: operation_id(log_world.epoch, 4),
            delivery: DeliveryId {
                publication: publication_id(log_world.epoch, 1),
                index: 0,
            },
            target: log_world.b,
            target_revision: Revision::new(1),
            packet_id: OutboundPacketId::new(11).unwrap(),
            claim: operation_id(log_world.epoch, 1),
        }))
        .unwrap(),
        payload: None,
    };

    let log_replies = log_world
        .store
        .apply_group(&[&c1, &c_dup_pi, &c2, &fin1])
        .expect("log group");
    let sql_replies = sql_world
        .store
        .apply_group(&[&c1, &c_dup_pi, &c2, &fin1])
        .expect("sql group");
    assert_eq!(log_replies, sql_replies);
    assert!(log_replies[0].committed_now);
    assert!(matches!(
        log_replies[1].reply,
        StoreReply::Outcome(StoreOutcome::RejectedCertain(
            mqttiumd_contract::outcome::RejectReason::PreconditionFailed
        ))
    ));
    assert!(log_replies[2].committed_now);
    assert!(log_replies[3].committed_now);

    let expected = sql_world.store.snapshot().expect("sql snapshot");
    assert_eq!(log_world.store.snapshot().expect("log snapshot"), expected);
    // Delivery 1 was finished; delivery 3 remains claimed with packet_id 12 and claim = op 3.
    assert_eq!(expected.outbound.len(), 1);
    let rec = expected
        .outbound
        .get(&DeliveryId {
            publication: publication_id(log_world.epoch, 3),
            index: 0,
        })
        .expect("inline claim row present");
    assert_eq!(rec.packet_id, OutboundPacketId::new(12).unwrap());
    assert_eq!(rec.claim, operation_id(log_world.epoch, 3));

    // Uncompacted .mlog crash recovery must reconstruct exact outbound & account state!
    let target_b = log_world.b;
    let epoch = log_world.epoch;
    drop(log_world);
    let mut reopened =
        LogStore::open_or_create(&log_path, StoreIncarnation::new(999), limits, None, true)
            .expect("reopen after crash");
    assert_eq!(reopened.snapshot().unwrap(), expected);

    // Finishing the recovered inline-claimed delivery after crash releases all slots cleanly.
    let fin2 = Command {
        recipe: FrozenRecipe::freeze(Recipe::FinishDelivery(FinishDelivery {
            operation: operation_id(epoch, 5),
            delivery: DeliveryId {
                publication: publication_id(epoch, 3),
                index: 0,
            },
            target: target_b,
            target_revision: Revision::new(1),
            packet_id: OutboundPacketId::new(12).unwrap(),
            claim: operation_id(epoch, 3),
        }))
        .unwrap(),
        payload: None,
    };
    let r_log = reopened.apply(&fin2);
    let r_sql = sql_world.store.apply(&fin2);
    assert_eq!(r_log, r_sql);
    assert!(r_log.committed_now);
    assert_eq!(
        reopened.snapshot().unwrap(),
        sql_world.store.snapshot().unwrap()
    );
    assert!(reopened.snapshot().unwrap().outbound.is_empty());

    drop(reopened);
    drop(sql_world);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn multi_claimed_publication_matches_sqlite_and_recovers_after_crash() {
    use mqttiumd_contract::capacity::{BoundedVec, ExchangeSlots, References};
    use mqttiumd_contract::command::{
        ClaimDelivery, FinishDelivery, InlineClaim, MultiClaimedPublication,
    };

    let dir = temp_dir("multi-claimed-pub");
    let log_path = dir.join("log.sqlite");
    let sql_path = dir.join("sql.sqlite");
    let mut limits = fixture_limits();
    limits.exchange_slots = ExchangeSlots::new(16);
    limits.references = References::new(16);

    let mut log_world = World::with_limits(
        &|incarnation, lim| {
            let mut s = LogStore::open_or_create(&log_path, incarnation, lim, None, false)
                .expect("log store");
            s.set_checkpoint_bytes(64 * 1024 * 1024);
            s
        },
        limits,
    );
    let mut sql_world = World::with_limits(
        &|incarnation, lim| SqliteStore::create(&sql_path, incarnation, lim).expect("sqlite store"),
        limits,
    );
    // Open two extra active subscriber sessions (c and d) on both stores so we test 1:3 fan-out
    // with partial claiming (claim b and d inline, leave c unclaimed as an ordinary obligation).
    let open_pair = |log_w: &mut World<LogStore>, sql_w: &mut World<SqliteStore>, name: &[u8]| {
        let cmd = AdminCommand::CreateSession {
            client_id: name.to_vec(),
        };
        let mqttiumd_store_api::AdminDone::SessionCreated { session: s_log, .. } =
            log_w.store.admin(cmd.clone()).expect("open log")
        else {
            panic!("expected SessionCreated");
        };
        let mqttiumd_store_api::AdminDone::SessionCreated { session: s_sql, .. } =
            sql_w.store.admin(cmd).expect("open sql")
        else {
            panic!("expected SessionCreated");
        };
        assert_eq!(s_log, s_sql);
        s_log
    };
    let sub_c = open_pair(&mut log_world, &mut sql_world, b"sub-c");
    let sub_d = open_pair(&mut log_world, &mut sql_world, b"sub-d");

    let make_multi =
        |world: &World<LogStore>, seq: u64, in_pi: u16, claims: &[(u16, u16)], body: &[u8]| {
            let base = world.admission(seq, in_pi, body, &[world.b, sub_c, sub_d]);
            let Recipe::AdmitPublication(admit) =
                Recipe::decode_canonical(&base.recipe.to_canonical_bytes(), 16, 64).unwrap()
            else {
                panic!("expected AdmitPublication");
            };
            let mut vec = BoundedVec::with_capacity(claims.len()).unwrap();
            for &(target_index, pi) in claims {
                vec.try_push(InlineClaim {
                    target_index,
                    packet_id: OutboundPacketId::new(pi).unwrap(),
                })
                .unwrap();
            }
            Command {
                recipe: FrozenRecipe::freeze(Recipe::MultiClaimedPublication(
                    MultiClaimedPublication {
                        admission: admit,
                        claims: vec,
                    },
                ))
                .unwrap(),
                payload: base.payload,
            }
        };

    // 1) Partial multi-claim: targets 0 (b, pi=11) and 2 (d, pi=21) claimed inline; target 1 (c) unclaimed.
    let m1 = make_multi(&log_world, 1, 7, &[(0, 11), (2, 21)], b"fanout-partial");
    // 2) Full multi-claim colliding on target 2's packet_id 21 MUST be refused atomically (no partial insert for target 0/1!).
    let m_dup = make_multi(
        &log_world,
        2,
        8,
        &[(0, 12), (1, 31), (2, 21)],
        b"fanout-dup-pi",
    );
    // 3) Full multi-claim on all 3 targets succeeds atomically.
    let m2 = make_multi(
        &log_world,
        3,
        9,
        &[(0, 12), (1, 31), (2, 22)],
        b"fanout-full",
    );
    // 4) Finish target 0 of m1 using claim = op 1.
    let fin_b = Command {
        recipe: FrozenRecipe::freeze(Recipe::FinishDelivery(FinishDelivery {
            operation: operation_id(log_world.epoch, 4),
            delivery: DeliveryId {
                publication: publication_id(log_world.epoch, 1),
                index: 0,
            },
            target: log_world.b,
            target_revision: Revision::new(1),
            packet_id: OutboundPacketId::new(11).unwrap(),
            claim: operation_id(log_world.epoch, 1),
        }))
        .unwrap(),
        payload: None,
    };

    let log_replies = log_world
        .store
        .apply_group(&[&m1, &m_dup, &m2, &fin_b])
        .expect("log group");
    let sql_replies = sql_world
        .store
        .apply_group(&[&m1, &m_dup, &m2, &fin_b])
        .expect("sql group");
    assert_eq!(log_replies, sql_replies);
    assert!(log_replies[0].committed_now);
    assert!(matches!(
        log_replies[1].reply,
        StoreReply::Outcome(StoreOutcome::RejectedCertain(
            mqttiumd_contract::outcome::RejectReason::PreconditionFailed
        ))
    ));
    assert!(log_replies[2].committed_now);
    assert!(log_replies[3].committed_now);

    let expected = sql_world.store.snapshot().expect("sql snapshot");
    assert_eq!(log_world.store.snapshot().expect("log snapshot"), expected);
    // 1 remaining outbound from m1 (sub_d) + 3 from m2 (b, c, d) = 4 outbound rows;
    // m1's sub_c remains an unclaimed obligation.
    assert_eq!(expected.outbound.len(), 4);
    assert_eq!(expected.obligations.len(), 5);
    assert_eq!(
        expected.outbound[&DeliveryId {
            publication: publication_id(log_world.epoch, 1),
            index: 2,
        }]
            .claim,
        operation_id(log_world.epoch, 1)
    );

    // Crash and recover from uncompacted .mlog!
    let epoch = log_world.epoch;
    drop(log_world);
    let mut reopened =
        LogStore::open_or_create(&log_path, StoreIncarnation::new(999), limits, None, true)
            .expect("reopen after crash");
    assert_eq!(reopened.snapshot().unwrap(), expected);

    // Claim the unclaimed m1 target (sub_c, index 1) and finish m1's sub_d (index 2).
    let claim_c = Command {
        recipe: FrozenRecipe::freeze(Recipe::ClaimDelivery(ClaimDelivery {
            operation: operation_id(epoch, 5),
            delivery: DeliveryId {
                publication: publication_id(epoch, 1),
                index: 1,
            },
            target: sub_c,
            target_revision: Revision::new(1),
            packet_id: OutboundPacketId::new(32).unwrap(),
        }))
        .unwrap(),
        payload: None,
    };
    let fin_d = Command {
        recipe: FrozenRecipe::freeze(Recipe::FinishDelivery(FinishDelivery {
            operation: operation_id(epoch, 6),
            delivery: DeliveryId {
                publication: publication_id(epoch, 1),
                index: 2,
            },
            target: sub_d,
            target_revision: Revision::new(1),
            packet_id: OutboundPacketId::new(21).unwrap(),
            claim: operation_id(epoch, 1),
        }))
        .unwrap(),
        payload: None,
    };
    for cmd in [&claim_c, &fin_d] {
        let r_log = reopened.apply(cmd);
        let r_sql = sql_world.store.apply(cmd);
        assert_eq!(r_log, r_sql);
        assert!(r_log.committed_now);
    }
    assert_eq!(
        reopened.snapshot().unwrap(),
        sql_world.store.snapshot().unwrap()
    );
    drop(reopened);
    drop(sql_world);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn failed_wal_replacement_suspends_durable_writes_until_repaired() {
    let dir = temp_dir("wal-replace-fail");
    let path = dir.join("broker.sqlite");
    let wal_path = dir.join("broker.sqlite.mlog");
    let limits = fixture_limits();

    let mut world = World::with_limits(
        &|incarnation, lim| {
            LogStore::open_or_create(&path, incarnation, lim, None, false).expect("log store")
        },
        limits,
    );
    let a1 = world.admission(1, 7, b"before-compact", &[world.b]);
    assert!(world.store.apply(&a1).committed_now);
    let snapshot_before = world.store.snapshot().expect("snapshot before");

    // Replace the WAL path with a directory so `save_state_atomic(&path)`
    // advances the SQLite checkpoint on disk while `write_fresh_wal_atomic`
    // fails at rename (EISDIR).
    std::fs::remove_file(&wal_path).expect("remove old wal");
    std::fs::create_dir(&wal_path).expect("block wal path with directory");
    assert!(
        world.store.compact().is_err(),
        "compact must report WAL replacement failure"
    );

    // Subsequent durable writes MUST fail closed rather than confirming onto
    // the stale/unlinked WAL file descriptor and vanishing on restart.
    let a2 = world.admission(2, 8, b"must-not-confirm-desynced", &[world.b]);
    let blocked = world.store.apply(&a2);
    assert_eq!(
        blocked.reply,
        StoreReply::Outcome(StoreOutcome::PendingOrUnknown),
        "desynced WAL must suspend durable confirmations"
    );
    assert!(!blocked.committed_now && !blocked.wrote);
    assert_eq!(world.store.snapshot().expect("unchanged"), snapshot_before);

    // Once the WAL path is restored, a fresh compaction recovers durability.
    std::fs::remove_dir(&wal_path).expect("remove blocking directory");
    world
        .store
        .compact()
        .expect("compaction succeeds after WAL path restored");
    let resumed = world.store.apply(&a2);
    assert!(resumed.committed_now);
    let expected = world.store.snapshot().expect("snapshot after resume");
    drop(world);

    let reopened = LogStore::open_or_create(&path, StoreIncarnation::new(1), limits, None, true)
        .expect("reopen after repaired compaction");
    assert_eq!(reopened.snapshot().expect("recovered"), expected);

    drop(reopened);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn multi_owner_admission_and_shard_finish_collect_and_recover_identically_in_log_and_sqlite() {
    use mqttiumd_contract::capacity::BoundedVec;
    use mqttiumd_contract::command::{InlineClaim, MultiClaimedPublication};
    use mqttiumd_contract::ids::{
        LogicalOwnerId, OperationId, OperationSeq, PayloadSeq, PublicationSeq, SessionId,
    };
    use mqttiumd_contract::packet::SharedBytes;
    use mqttiumd_contract::receipts::{
        ReceiptClosure, ReceiptCollection, ReceiptCollectionReply, ReceiptOrigin,
    };
    use mqttiumd_contract::session::{SessionAction, SessionExpiry, SessionLifecycle};

    let dir = temp_dir("multi-owner-sharding");
    let log_path = dir.join("log.sqlite");
    let sql_path = dir.join("sql.sqlite");
    let limits = fixture_limits();

    let mut log_world = World::with_limits(
        &|incarnation, lim| {
            LogStore::open_or_create(&log_path, incarnation, lim, None, false).expect("log store")
        },
        limits,
    );
    let mut sql_world = World::with_limits(
        &|incarnation, lim| SqliteStore::create(&sql_path, incarnation, lim).expect("sqlite store"),
        limits,
    );
    let sub_c = SessionId::new(3);
    let sub_d = SessionId::new(4);
    for (seq, name) in [(100u64, b"sub-c".as_slice()), (101u64, b"sub-d".as_slice())] {
        let open = Command {
            recipe: FrozenRecipe::freeze(Recipe::SessionLifecycle(SessionLifecycle {
                operation: operation_id(log_world.epoch, seq),
                action: SessionAction::Open {
                    client_id: SharedBytes::copy_from(name, 64).unwrap(),
                    expected: None,
                    clean_start: true,
                    expiry: SessionExpiry::Never,
                },
            }))
            .unwrap(),
            payload: None,
        };
        assert_eq!(log_world.store.apply(&open), sql_world.store.apply(&open));
    }

    let owner_op = |owner: u64, seq: u64| {
        OperationId::new(
            StoreIncarnation::new(1),
            log_world.epoch,
            LogicalOwnerId::new(owner),
            OperationSeq::new(seq),
        )
    };

    // Owner 0 admits MultiClaimedPublication targeting (b, c, d), claiming b & c inline.
    let base = log_world.admission(1, 1, b"multi-owner-payload", &[log_world.b, sub_c, sub_d]);
    let Recipe::AdmitPublication(admit) =
        Recipe::decode_canonical(&base.recipe.to_canonical_bytes(), 16, 64).unwrap()
    else {
        panic!("expected AdmitPublication");
    };
    let mut claims = BoundedVec::with_capacity(2).unwrap();
    claims
        .try_push(InlineClaim {
            target_index: 0,
            packet_id: OutboundPacketId::new(1).unwrap(),
        })
        .unwrap();
    claims
        .try_push(InlineClaim {
            target_index: 1,
            packet_id: OutboundPacketId::new(1).unwrap(),
        })
        .unwrap();
    let multi = Command {
        recipe: FrozenRecipe::freeze(Recipe::MultiClaimedPublication(MultiClaimedPublication {
            admission: admit,
            claims,
        }))
        .unwrap(),
        payload: base.payload,
    };
    assert_eq!(log_world.store.apply(&multi), sql_world.store.apply(&multi));

    // Delivery Shard 1 (LogicalOwnerId(1)) finishes delivery index 0 (b, claimed by Owner 0 op 1)
    // and claims+finishes unclaimed delivery index 2 (d) using its own OperationIds.
    let fin_b = Command {
        recipe: FrozenRecipe::freeze(Recipe::FinishDelivery(FinishDelivery {
            operation: owner_op(1, 1),
            delivery: DeliveryId {
                publication: publication_id(log_world.epoch, 1),
                index: 0,
            },
            target: log_world.b,
            target_revision: Revision::new(1),
            packet_id: OutboundPacketId::new(1).unwrap(),
            claim: owner_op(0, 1),
        }))
        .unwrap(),
        payload: None,
    };
    // Delivery Shard 2 (LogicalOwnerId(2)) finishes delivery index 1 (c, claimed by Owner 0 op 1)
    // and claims unclaimed delivery index 2 (d) under Owner 2 op 2.
    let fin_c = Command {
        recipe: FrozenRecipe::freeze(Recipe::FinishDelivery(FinishDelivery {
            operation: owner_op(2, 1),
            delivery: DeliveryId {
                publication: publication_id(log_world.epoch, 1),
                index: 1,
            },
            target: sub_c,
            target_revision: Revision::new(1),
            packet_id: OutboundPacketId::new(1).unwrap(),
            claim: owner_op(0, 1),
        }))
        .unwrap(),
        payload: None,
    };
    let claim_d = Command {
        recipe: FrozenRecipe::freeze(Recipe::ClaimDelivery(ClaimDelivery {
            operation: owner_op(2, 2),
            delivery: DeliveryId {
                publication: publication_id(log_world.epoch, 1),
                index: 2,
            },
            target: sub_d,
            target_revision: Revision::new(1),
            packet_id: OutboundPacketId::new(7).unwrap(),
        }))
        .unwrap(),
        payload: None,
    };
    for cmd in [&fin_b, &fin_c, &claim_d] {
        let r_log = log_world.store.apply(cmd);
        let r_sql = sql_world.store.apply(cmd);
        assert_eq!(r_log, r_sql);
        assert!(r_log.committed_now);
    }

    // Independent ReceiptCollection per owner (Owner 1 & Owner 2) while Owner 0 keeps its frontier.
    for (owner_id, through) in [(1u64, 1u64), (2u64, 2u64)] {
        let col = ReceiptCollection {
            closure: ReceiptClosure {
                origin: ReceiptOrigin {
                    incarnation: StoreIncarnation::new(1),
                    epoch: log_world.epoch,
                    owner: LogicalOwnerId::new(owner_id),
                },
                through: OperationSeq::new(through),
                publication_through: PublicationSeq::new(0),
                payload_through: PayloadSeq::new(0),
            },
            limit: 64,
        };
        let r_log = log_world.store.collect_receipts(col).expect("log collect");
        let r_sql = sql_world.store.collect_receipts(col).expect("sql collect");
        assert_eq!(r_log, r_sql);
        assert!(matches!(r_log, ReceiptCollectionReply::Applied { .. }));
    }

    let expected = sql_world.store.snapshot().expect("sql snapshot");
    assert_eq!(log_world.store.snapshot().expect("log snapshot"), expected);
    assert_eq!(expected.outbound.len(), 1);
    assert_eq!(
        expected.outbound[&DeliveryId {
            publication: publication_id(log_world.epoch, 1),
            index: 2,
        }]
            .claim,
        owner_op(2, 2)
    );

    // Crash & recover uncompacted .mlog, then compact and re-verify against SQLite.
    drop(log_world);
    let mut reopened =
        LogStore::open_or_create(&log_path, StoreIncarnation::new(999), limits, None, true)
            .expect("reopen uncompacted");
    assert_eq!(reopened.snapshot().expect("reopened snapshot"), expected);
    reopened.compact().expect("compact multi-owner log");
    drop(reopened);
    let reopened2 =
        LogStore::open_or_create(&log_path, StoreIncarnation::new(999), limits, None, true)
            .expect("reopen compacted");
    assert_eq!(reopened2.snapshot().expect("compacted snapshot"), expected);

    drop(reopened2);
    drop(sql_world);
    let _ = std::fs::remove_dir_all(&dir);
}
