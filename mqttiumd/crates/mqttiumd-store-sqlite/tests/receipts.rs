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
