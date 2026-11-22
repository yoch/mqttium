//! The contract tests of the store (`mqttiumd_lab::store::conformance`),
//! the same cases the deterministic store passes, run on SQLite. Each
//! case is a test of its own, on a file of its own. A case that fails is
//! a finding about this store, never a case to remove.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use mqttiumd_contract::ids::StoreIncarnation;
use mqttiumd_lab::store::conformance;
use mqttiumd_lab::store::durable::Limits;
use mqttiumd_store_sqlite::SqliteStore;

static NEXT: AtomicU64 = AtomicU64::new(0);

/// A path no other test uses, with no file behind it.
fn fresh_path() -> PathBuf {
    let directory = std::env::temp_dir().join("mqttiumd-store-sqlite-tests");
    std::fs::create_dir_all(&directory).expect("a temporary directory");
    let name = format!(
        "{}-{}.db",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    );
    let path = directory.join(name);
    for suffix in ["", "-wal", "-shm"] {
        let mut file = path.clone().into_os_string();
        file.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(file));
    }
    path
}

fn sqlite(incarnation: StoreIncarnation, limits: Limits) -> SqliteStore {
    SqliteStore::create(&fresh_path(), incarnation, limits).expect("a new store")
}

#[test]
fn the_suite_runs_every_check() {
    conformance::all(&sqlite);
}

#[test]
fn opening_opens_the_next_epoch_and_closes_the_older_ones() {
    conformance::opening_opens_the_next_epoch_and_closes_the_older_ones(&sqlite);
}

#[test]
fn an_opening_that_expected_another_epoch_opens_nothing() {
    conformance::an_opening_that_expected_another_epoch_opens_nothing(&sqlite);
}

#[test]
fn an_opening_delivered_again_returns_its_commit() {
    conformance::an_opening_delivered_again_returns_its_commit(&sqlite);
}

#[test]
fn the_opener_of_the_last_epoch_cannot_open_the_next_under_its_identity() {
    conformance::the_opener_of_the_last_epoch_cannot_open_the_next_under_its_identity(&sqlite);
}

#[test]
fn the_first_command_of_a_restart_is_not_its_predecessors() {
    conformance::the_first_command_of_a_restart_is_not_its_predecessors(&sqlite);
}

#[test]
fn a_command_of_an_unopened_epoch_never_executes() {
    conformance::a_command_of_an_unopened_epoch_never_executes(&sqlite);
}

#[test]
fn an_admission_commits_everything_together() {
    conformance::an_admission_commits_everything_together(&sqlite);
}

#[test]
fn a_repeated_command_returns_its_result_without_a_second_effect() {
    conformance::a_repeated_command_returns_its_result_without_a_second_effect(&sqlite);
}

#[test]
fn a_failed_guard_leaves_no_admitted_subset() {
    conformance::a_failed_guard_leaves_no_admitted_subset(&sqlite);
}

#[test]
fn each_account_is_checked() {
    conformance::each_account_is_checked(&sqlite);
}

#[test]
fn a_rejection_stays_a_rejection_after_quota_is_released() {
    conformance::a_rejection_stays_a_rejection_after_quota_is_released(&sqlite);
}

#[test]
fn another_recipe_under_the_same_operation_is_a_violation() {
    conformance::another_recipe_under_the_same_operation_is_a_violation(&sqlite);
}

#[test]
fn resolve_returns_a_commit_and_fences_an_absence() {
    conformance::resolve_returns_a_commit_and_fences_an_absence(&sqlite);
}

#[test]
fn a_release_ends_the_inbound_exchange() {
    conformance::a_release_ends_the_inbound_exchange(&sqlite);
}

#[test]
fn a_release_holds_the_slot_of_its_receipt() {
    conformance::a_release_holds_the_slot_of_its_receipt(&sqlite);
}

#[test]
fn a_release_without_a_receipt_slot_changes_nothing() {
    conformance::a_release_without_a_receipt_slot_changes_nothing(&sqlite);
}

#[test]
fn collecting_an_epoch_gives_back_the_slots_of_its_releases() {
    conformance::collecting_an_epoch_gives_back_the_slots_of_its_releases(&sqlite);
}

#[test]
fn deleting_the_producer_deletes_no_obligation_of_another_session() {
    conformance::deleting_the_producer_deletes_no_obligation_of_another_session(&sqlite);
}

#[test]
fn an_older_epoch_is_not_entered_again() {
    conformance::an_older_epoch_is_not_entered_again(&sqlite);
}

#[test]
fn a_collected_identity_is_retired_not_rejected() {
    conformance::a_collected_identity_is_retired_not_rejected(&sqlite);
}

#[test]
fn an_identity_of_another_data_set_is_refused() {
    conformance::an_identity_of_another_data_set_is_refused(&sqlite);
}

#[test]
fn recovery_reads_bounded_pages_of_durable_facts() {
    conformance::recovery_reads_bounded_pages_of_durable_facts(&sqlite);
}

#[test]
fn administration_refusals_change_nothing() {
    conformance::administration_refusals_change_nothing(&sqlite);
}

#[test]
fn an_admission_without_targets_holds_no_payload_bytes() {
    conformance::an_admission_without_targets_holds_no_payload_bytes(&sqlite);
}

#[test]
fn a_completed_unrouted_exchange_releases_its_publication() {
    conformance::a_completed_unrouted_exchange_releases_its_publication(&sqlite);
}

#[test]
fn publication_metadata_lives_until_its_last_relation_ends() {
    conformance::publication_metadata_lives_until_its_last_relation_ends(&sqlite);
}

#[test]
fn collected_publication_identities_never_enter_a_new_epoch() {
    conformance::collected_publication_identities_never_enter_a_new_epoch(&sqlite);
}

#[test]
fn a_deleted_session_frees_its_place() {
    conformance::a_deleted_session_frees_its_place(&sqlite);
}

#[test]
fn a_client_id_longer_than_mqtt_allows_is_refused() {
    conformance::a_client_id_longer_than_mqtt_allows_is_refused(&sqlite);
}

#[test]
fn what_is_established_in_epoch_zero_is_never_collected() {
    conformance::what_is_established_in_epoch_zero_is_never_collected(&sqlite);
}

#[test]
fn every_receipt_and_fence_holds_a_receipt_slot() {
    conformance::every_receipt_and_fence_holds_a_receipt_slot(&sqlite);
}

#[test]
fn recovered_facts_follow_the_order_of_an_inventory() {
    conformance::recovered_facts_follow_the_order_of_an_inventory(&sqlite);
}
