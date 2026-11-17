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
