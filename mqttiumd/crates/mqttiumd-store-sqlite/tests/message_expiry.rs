//! Both writers obey the same message-lifetime ownership contract.
use mqttiumd_contract::ids::StoreIncarnation;
use mqttiumd_lab::store::retained_conformance::expiry;
use mqttiumd_store_api::Limits;
use mqttiumd_store_sqlite::SqliteStore;
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);
fn sqlite(i: StoreIncarnation, l: Limits) -> SqliteStore {
    let path = std::env::temp_dir().join(format!(
        "mqttiumd-message-expiry-{}-{}.db",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    assert!(!path.exists());
    SqliteStore::create(&path, i, l).unwrap()
}
#[test]
fn shared_refs_replay_and_source_end() {
    expiry::shared_refs_replay_and_source_end(&sqlite);
}
#[test]
fn claimed_copies_require_exact_never_started_guard() {
    expiry::claimed_copies_require_exact_never_started_guard(&sqlite);
}
#[test]
fn zero_replacement_at_capacity_and_qos2_continuation() {
    expiry::zero_replacement_at_capacity_and_qos2_continuation(&sqlite);
}
#[test]
fn temporal_quarantine_and_observation_refusals() {
    expiry::temporal_quarantine_and_observation_refusals(&sqlite);
}
#[test]
fn thirty_two_timed_replays_keep_original_ttl() {
    expiry::thirty_two_timed_replays_keep_original_ttl(&sqlite);
}
