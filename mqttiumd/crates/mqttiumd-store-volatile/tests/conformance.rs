//! Full contract conformance, differential parity, and atomic SQLite snapshot
//! verification for `VolatileStore` (ADR 0052).

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use mqttiumd_contract::command::{ClaimDelivery, Command, FinishDelivery, FrozenRecipe, Recipe};
use mqttiumd_contract::ids::{DeliveryId, OutboundPacketId, Revision, StoreIncarnation};
use mqttiumd_contract::outcome::{StoreOutcome, StoreReply};
use mqttiumd_contract::packet::Qos;
use mqttiumd_lab::store::conformance::{self, World, fixture_limits, operation_id, publication_id};
use mqttiumd_lab::store::durable::MemoryStore;
use mqttiumd_lab::store::retained_conformance;
use mqttiumd_store_api::{AdminCommand, Durability, DurableStore};
use mqttiumd_store_sqlite::{OpenError, SqliteStore, StoragePolicy};
use mqttiumd_store_volatile::{GroupRefused, MAX_GROUP, VolatileHandle, VolatileStore};

static NEXT: AtomicU64 = AtomicU64::new(0);

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "mqttiumd-volatile-{name}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).expect("isolated directory");
    dir
}

#[test]
fn volatile_passes_full_store_conformance_suite() {
    conformance::all(&VolatileStore::create);
}

#[test]
fn volatile_passes_full_retained_conformance_suite() {
    retained_conformance::all(&VolatileStore::create);
}

#[test]
fn volatile_passes_full_message_expiry_conformance_suite() {
    retained_conformance::expiry::all(&VolatileStore::create);
}

#[test]
fn volatile_apply_group_matches_sqlite_and_memory_differentially() {
    let dir = temp_dir("diff-group");
    let path = dir.join("store.sqlite");
    let limits = fixture_limits();
    let mut mem = World::with_limits(&MemoryStore::create, limits);
    let mut vol = World::with_limits(&VolatileStore::create, limits);
    let mut sql = World::with_limits(
        &|incarnation, lim| SqliteStore::create(&path, incarnation, lim).expect("sqlite"),
        limits,
    );

    // Oversized group refused without mutation.
    let c0 = vol.admission(1, 7, b"one", &[vol.b]);
    let oversized: Vec<&Command> = vec![&c0; MAX_GROUP + 1];
    assert_eq!(
        vol.store.apply_group(&oversized),
        Err(GroupRefused::TooLarge {
            commands: MAX_GROUP + 1,
            max: MAX_GROUP,
        })
    );

    // Group with valid admissions, duplicate presentation, invalid admission, and claim/finish.
    let a1 = vol.admission(1, 7, b"alpha", &[vol.b, vol.c]);
    let a2 = vol.admission(2, 8, b"beta", &[vol.b]);
    let a_dup = vol.admission(1, 7, b"alpha", &[vol.b, vol.c]);
    let a_bad = vol.admission(3, 7, b"dup-pi", &[vol.b]); // collides on inbound packet_id 7!
    let claim = Command {
        recipe: FrozenRecipe::freeze(Recipe::ClaimDelivery(ClaimDelivery {
            operation: operation_id(vol.epoch, 4),
            delivery: DeliveryId {
                publication: publication_id(vol.epoch, 1),
                index: 0,
            },
            target: vol.b,
            target_revision: Revision::new(1),
            packet_id: OutboundPacketId::new(11).unwrap(),
        }))
        .unwrap(),
        payload: None,
    };
    let finish = Command {
        recipe: FrozenRecipe::freeze(Recipe::FinishDelivery(FinishDelivery {
            operation: operation_id(vol.epoch, 5),
            delivery: DeliveryId {
                publication: publication_id(vol.epoch, 1),
                index: 0,
            },
            target: vol.b,
            target_revision: Revision::new(1),
            packet_id: OutboundPacketId::new(11).unwrap(),
            claim: operation_id(vol.epoch, 4),
        }))
        .unwrap(),
        payload: None,
    };
    let batch = [&a1, &a2, &a_dup, &a_bad, &claim, &finish];

    let mem_replies: Vec<_> = batch.iter().map(|c| mem.store.apply(c)).collect();
    let vol_replies = vol.store.apply_group(&batch).expect("volatile group");
    let sql_replies = sql.store.apply_group(&batch).expect("sqlite group");
    assert_eq!(vol_replies, mem_replies);
    assert_eq!(vol_replies, sql_replies);
    assert_eq!(vol.store.snapshot(), mem.store.snapshot());
    assert_eq!(vol.store.snapshot(), sql.store.snapshot());

    // Work pages match identically across all three stores.
    for target in [vol.b, vol.c] {
        assert_eq!(
            vol.store.read_work(target, None, 8),
            mem.store.read_work(target, None, 8)
        );
        assert_eq!(
            vol.store.read_work(target, None, 8),
            sql.store.read_work(target, None, 8)
        );
    }

    // Failing medium on a group that needs a write leaves state intact and answers Unknown.
    let before = vol.store.snapshot().unwrap();
    let fresh = vol.admission(6, 9, b"gamma", &[vol.c]);
    vol.store.set_durability(Durability::Failing);
    sql.store.set_durability(Durability::Failing);
    let vol_fail = vol.store.apply_group(&[&a1, &fresh]).unwrap();
    let sql_fail = sql.store.apply_group(&[&a1, &fresh]).unwrap();
    assert_eq!(vol_fail, sql_fail);
    assert!(
        vol_fail
            .iter()
            .all(|r| { matches!(r.reply, StoreReply::Outcome(StoreOutcome::PendingOrUnknown)) })
    );
    assert_eq!(vol.store.snapshot().unwrap(), before);

    // Read-only group on failing medium still answers established outcomes.
    let vol_ro = vol.store.apply_group(&[&a1, &a2]).unwrap();
    let sql_ro = sql.store.apply_group(&[&a1, &a2]).unwrap();
    assert_eq!(vol_ro, sql_ro);

    drop(sql);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn volatile_handle_snapshot_round_trips_through_sqlite_and_enforces_writer_lock() {
    let dir = temp_dir("snapshot-roundtrip");
    let path = dir.join("store.sqlite");
    let limits = fixture_limits();

    let handle =
        VolatileHandle::open_or_create(&path, StoreIncarnation::new(1), limits, None, false)
            .expect("fresh volatile handle");

    // Exclusive writer lock refuses a second SQLite or Volatile opener while alive.
    assert!(matches!(
        VolatileHandle::open_or_create(&path, StoreIncarnation::new(1), limits, None, false),
        Err(OpenError::WriterAlive)
    ));

    let mut world = World::prepare(handle);
    let a1 = world.admission(1, 7, b"snapshot-payload-1", &[world.b, world.c]);
    let a2 = world.admission(2, 8, b"snapshot-payload-2", &[world.c]);
    assert!(world.store.apply(&a1).committed_now);
    assert!(world.store.apply(&a2).committed_now);
    let expected = world.store.snapshot().expect("volatile snapshot");

    // Atomic Schema-14 snapshot on disk.
    world.store.save_snapshot().expect("save atomic snapshot");

    // Inspector reads the live snapshot cleanly while VolatileHandle holds the writer lock.
    let inspected = SqliteStore::inspect(&path).expect("inspect snapshot");
    assert_eq!(inspected, expected);
    // Second writer is still refused while VolatileHandle lives.
    assert!(matches!(
        SqliteStore::open(&path),
        Err(OpenError::WriterAlive)
    ));
    let target_b = world.b;
    drop(world);

    // Reopen as a real durable SqliteStore: passes PRAGMA quick_check and matches byte-for-byte.
    let sqlite = SqliteStore::open(&path).expect("reopen as SqliteStore");
    assert_eq!(
        sqlite.effective_settings().user_version,
        mqttiumd_store_sqlite::SCHEMA_VERSION
    );
    assert_eq!(sqlite.snapshot().expect("sqlite snapshot"), expected);
    drop(sqlite);

    // Reopen again as VolatileHandle (loads SQLite state + transfers lock with no gap).
    let mut reopened =
        VolatileHandle::open_or_create(&path, StoreIncarnation::new(999), limits, None, true)
            .expect("reopen as VolatileHandle");
    assert_eq!(reopened.snapshot().expect("reopened snapshot"), expected);
    // Continue committing after restart, then snapshot again.
    let _ = reopened
        .admin(AdminCommand::AddRoute {
            topic: b"sensors/#".to_vec(),
            session: target_b,
            qos: Qos::AtLeastOnce,
        })
        .expect("add route after reload");
    let updated = reopened.snapshot().expect("updated snapshot");
    reopened.save_snapshot().expect("second snapshot");
    drop(reopened);

    assert_eq!(SqliteStore::inspect(&path).expect("re-inspect"), updated);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn volatile_handle_preserves_schema15_security_policy_in_snapshots() {
    let dir = temp_dir("secured-snapshot");
    let path = dir.join("secured.sqlite");
    let limits = fixture_limits();
    let mut owners = BTreeMap::new();
    owners.insert(b"p1-producer".to_vec(), "alice".to_owned());
    owners.insert(b"p1-b".to_vec(), "bob".to_owned());
    owners.insert(b"p1-c".to_vec(), "carol".to_owned());
    let policy = StoragePolicy {
        fingerprint: [0x5a; 32],
        owners,
    };

    let handle = VolatileHandle::open_or_create(
        &path,
        StoreIncarnation::new(1),
        limits,
        Some(policy.clone()),
        false,
    )
    .expect("secured volatile handle");
    let world = World::prepare(handle);
    let expected = world.store.snapshot().expect("snapshot");
    world
        .store
        .save_snapshot()
        .expect("save Schema 15 snapshot");
    drop(world);

    // Must refuse plain open (Schema 14 expected vs 15 on disk) and accept secured open!
    assert!(matches!(
        SqliteStore::open(&path),
        Err(OpenError::SchemaVersion(
            mqttiumd_store_sqlite::SECURE_SCHEMA_VERSION
        ))
    ));
    let sqlite = SqliteStore::open_secured(&path, &policy).expect("open_secured");
    assert_eq!(
        sqlite.effective_settings().user_version,
        mqttiumd_store_sqlite::SECURE_SCHEMA_VERSION
    );
    assert_eq!(sqlite.snapshot().expect("secured snapshot"), expected);
    drop(sqlite);
    let _ = std::fs::remove_dir_all(&dir);
}
