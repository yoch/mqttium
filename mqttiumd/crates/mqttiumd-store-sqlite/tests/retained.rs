//! Retained operations use exactly the deterministic writer's contract tests.
use mqttiumd_contract::ids::StoreIncarnation;
use mqttiumd_lab::store::retained_conformance;
use mqttiumd_store_api::Limits;
use mqttiumd_store_sqlite::SqliteStore;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);
fn path() -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "mqttiumd-retained-{}-{}.db",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    assert!(!p.exists());
    p
}
fn sqlite(i: StoreIncarnation, l: Limits) -> SqliteStore {
    SqliteStore::create(&path(), i, l).unwrap()
}
#[test]
fn retained_survives_without_targets_and_producer() {
    retained_conformance::retained_survives_without_targets_and_producer(&sqlite);
}
#[test]
fn retained_replacement_clear_and_replay_freeze() {
    retained_conformance::retained_replacement_clear_and_replay_freeze(&sqlite);
}
#[test]
fn thirty_two_topics_and_ordered_options() {
    retained_conformance::thirty_two_topics_and_ordered_options(&sqlite);
}
#[test]
fn quota_full_replacement_and_no_local_client_identity() {
    retained_conformance::quota_full_replacement_and_no_local_client_identity(&sqlite);
}

fn prepared_file() -> (PathBuf, SqliteStore, mqttiumd_contract::ids::SessionId) {
    use mqttiumd_contract::capacity::{BoundedVec, PayloadBytes, ReceiptSlots, References};
    use mqttiumd_contract::command::{Command, FrozenRecipe, Recipe, UpdateSubscriptions};
    use mqttiumd_contract::packet::Qos;
    use mqttiumd_contract::pubsub::{GrantedSubscriptionChange, SubscriptionOptions};
    use mqttiumd_contract::retained::{ReplaySubscriptions, RetainedPublication};
    use mqttiumd_lab::store::conformance::{World, fixture_limits, operation_id, publication_id};
    use mqttiumd_store_api::DurableStore;
    let path = path();
    let mut limits = fixture_limits();
    limits.references = References::new(128);
    limits.receipt_slots = ReceiptSlots::new(8192);
    limits.payload_bytes = PayloadBytes::new(4 * 1024 * 1024);
    let mut w = World::with_limits(&|i, l| SqliteStore::create(&path, i, l).unwrap(), limits);
    let old = w.admission(1, 1, b"frozen", &[]);
    let mut canonical = Vec::new();
    old.recipe.get().encode_canonical(&mut canonical).unwrap();
    let Recipe::AdmitPublication(mut admission) =
        Recipe::decode_canonical(&canonical, 16, 256).unwrap()
    else {
        panic!()
    };
    admission.retain = true;
    admission.costs.references = References::new(1);
    let topic = admission.topic.clone();
    let command = Command {
        recipe: FrozenRecipe::freeze(Recipe::RetainedPublication(RetainedPublication {
            admission,
            retain_targets: mqttiumd_contract::retained::RetainMask::EMPTY,
        }))
        .unwrap(),
        payload: old.payload,
    };
    assert!(w.store.apply(&command).committed_now);
    let sub = Command {
        recipe: FrozenRecipe::freeze(Recipe::ReplaySubscriptions(ReplaySubscriptions {
            update: UpdateSubscriptions {
                operation: operation_id(w.epoch, 2),
                session: w.b,
                session_revision: mqttiumd_contract::ids::Revision::new(1),
                route_revision: w.route_revision,
                changes: BoundedVec::from_slice(
                    &[GrantedSubscriptionChange {
                        topic,
                        granted: Some(Qos::AtMostOnce),
                        options: SubscriptionOptions::DEFAULT,
                    }],
                    8,
                )
                .unwrap(),
            },
            first_publication: publication_id(w.epoch, 1000),
        }))
        .unwrap(),
        payload: None,
    };
    assert!(w.store.apply(&sub).committed_now);
    (path, w.store, w.b)
}
fn corrupt(path: &std::path::Path, sql: &str) {
    let raw = rusqlite::Connection::open(path).unwrap();
    raw.execute_batch(sql).unwrap();
    let quick: String = raw
        .query_row("PRAGMA quick_check", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        quick, "ok",
        "semantic corruption must remain physically consistent"
    );
}
#[test]
fn retained_and_frozen_replay_reopen_on_the_same_file() {
    use mqttiumd_store_api::DurableStore;
    let (path, store, target) = prepared_file();
    let expected = store.snapshot().unwrap();
    drop(store);
    let reopened = SqliteStore::open(&path).unwrap();
    assert_eq!(reopened.snapshot().unwrap(), expected);
    let work = reopened.read_work(target, None, 8).unwrap();
    assert_eq!(work.items.len(), 1);
    assert!(work.items.as_slice()[0].retain);
    assert_eq!(work.items.as_slice()[0].payload.as_slice(), b"frozen");
}
#[test]
fn semantic_retained_corruption_is_a_typed_snapshot_refusal() {
    use mqttiumd_store_api::{DurableStore, ReadError};
    for (sql, expected) in [
        (
            "UPDATE publications SET qos=99 WHERE id IN (SELECT publication FROM retained)",
            ReadError::InvalidData,
        ),
        (
            "UPDATE retained SET producer_client_id=x'ff'",
            ReadError::InvalidData,
        ),
        (
            "UPDATE retained SET producer_client_id=zeroblob(65536)",
            ReadError::LimitExceeded,
        ),
        (
            "UPDATE retained SET publication=zeroblob(32)",
            ReadError::InvalidData,
        ),
    ] {
        let (path, store, _) = prepared_file();
        corrupt(&path, sql);
        assert_eq!(store.snapshot(), Err(expected), "{sql}");
        assert_eq!(SqliteStore::inspect(&path), Err(expected), "{sql}");
    }
}
#[test]
fn work_and_snapshot_reject_contradictory_replay_flags() {
    use mqttiumd_store_api::{DurableStore, ReadError};
    for sql in [
        "UPDATE obligations SET retain=99",
        "UPDATE obligations SET cause=99",
        "UPDATE obligations SET retain=0",
        "UPDATE publications SET retained_version=NULL WHERE retained_version IS NOT NULL",
    ] {
        let (path, store, target) = prepared_file();
        corrupt(&path, sql);
        assert_eq!(
            store.read_work(target, None, 8),
            Err(ReadError::InvalidData),
            "work: {sql}"
        );
        assert_eq!(
            store.snapshot(),
            Err(ReadError::InvalidData),
            "snapshot: {sql}"
        );
    }
}

#[test]
fn retained_topic_names_are_not_subscription_filters() {
    retained_conformance::retained_topic_names_are_not_subscription_filters(&sqlite);
}

#[test]
fn collected_retained_identities_never_execute_again() {
    retained_conformance::collected_retained_identities_never_execute_again(&sqlite);
}

#[test]
fn rejected_replay_ranges_remain_reserved() {
    retained_conformance::rejected_replay_ranges_remain_reserved(&sqlite);
}
