//! The contract tests of `DurableStore`, run on the deterministic store.
//!
//! The checks live in `mqttiumd_lab::store::conformance`; the SQLite
//! store of lot L7 runs the same ones with its own factory.

use mqttiumd_contract::ids::StoreIncarnation;
use mqttiumd_lab::store::conformance::{self, World};
use mqttiumd_lab::store::durable::{
    DurableStore, Limits, MAX_CLIENT_ID_BYTES, MemoryStore, ReadError, RecoveryRefused,
};

fn memory(incarnation: StoreIncarnation, limits: Limits) -> MemoryStore {
    MemoryStore::create(incarnation, limits)
}

#[test]
fn opening_opens_the_next_epoch_and_closes_the_older_ones() {
    conformance::opening_opens_the_next_epoch_and_closes_the_older_ones(&memory);
}

#[test]
fn an_opening_that_expected_another_epoch_opens_nothing() {
    conformance::an_opening_that_expected_another_epoch_opens_nothing(&memory);
}

#[test]
fn an_opening_delivered_again_returns_its_commit() {
    conformance::an_opening_delivered_again_returns_its_commit(&memory);
}

#[test]
fn the_opener_of_the_last_epoch_cannot_open_the_next_under_its_identity() {
    conformance::the_opener_of_the_last_epoch_cannot_open_the_next_under_its_identity(&memory);
}

#[test]
fn the_first_command_of_a_restart_is_not_its_predecessors() {
    conformance::the_first_command_of_a_restart_is_not_its_predecessors(&memory);
}

#[test]
fn a_command_of_an_unopened_epoch_never_executes() {
    conformance::a_command_of_an_unopened_epoch_never_executes(&memory);
}

#[test]
fn an_admission_commits_everything_together() {
    conformance::an_admission_commits_everything_together(&memory);
}

#[test]
fn a_repeated_command_returns_its_result_without_a_second_effect() {
    conformance::a_repeated_command_returns_its_result_without_a_second_effect(&memory);
}

#[test]
fn a_failed_guard_leaves_no_admitted_subset() {
    conformance::a_failed_guard_leaves_no_admitted_subset(&memory);
}

#[test]
fn each_account_is_checked() {
    conformance::each_account_is_checked(&memory);
}

#[test]
fn a_rejection_stays_a_rejection_after_quota_is_released() {
    conformance::a_rejection_stays_a_rejection_after_quota_is_released(&memory);
}

#[test]
fn another_recipe_under_the_same_operation_is_a_violation() {
    conformance::another_recipe_under_the_same_operation_is_a_violation(&memory);
}

#[test]
fn resolve_returns_a_commit_and_fences_an_absence() {
    conformance::resolve_returns_a_commit_and_fences_an_absence(&memory);
}

#[test]
fn a_release_ends_the_inbound_exchange() {
    conformance::a_release_ends_the_inbound_exchange(&memory);
}

#[test]
fn deleting_the_producer_deletes_no_obligation_of_another_session() {
    conformance::deleting_the_producer_deletes_no_obligation_of_another_session(&memory);
}

#[test]
fn an_older_epoch_is_not_entered_again() {
    conformance::an_older_epoch_is_not_entered_again(&memory);
}

#[test]
fn a_collected_identity_is_retired_not_rejected() {
    conformance::a_collected_identity_is_retired_not_rejected(&memory);
}

#[test]
fn an_identity_of_another_data_set_is_refused() {
    conformance::an_identity_of_another_data_set_is_refused(&memory);
}

#[test]
fn recovery_reads_bounded_pages_of_durable_facts() {
    conformance::recovery_reads_bounded_pages_of_durable_facts(&memory);
}

#[test]
fn administration_refusals_change_nothing() {
    conformance::administration_refusals_change_nothing(&memory);
}

#[test]
fn the_suite_runs_every_check() {
    conformance::all(&memory);
}

#[test]
fn an_admission_without_targets_holds_no_payload_bytes() {
    conformance::an_admission_without_targets_holds_no_payload_bytes(&memory);
}

#[test]
fn a_deleted_session_frees_its_place() {
    conformance::a_deleted_session_frees_its_place(&memory);
}

#[test]
fn a_client_id_longer_than_mqtt_allows_is_refused() {
    conformance::a_client_id_longer_than_mqtt_allows_is_refused(&memory);
}

#[test]
fn what_is_established_in_epoch_zero_is_never_collected() {
    conformance::what_is_established_in_epoch_zero_is_never_collected(&memory);
}

#[test]
fn a_release_holds_the_slot_of_its_receipt() {
    conformance::a_release_holds_the_slot_of_its_receipt(&memory);
}

#[test]
fn a_release_without_a_receipt_slot_changes_nothing() {
    conformance::a_release_without_a_receipt_slot_changes_nothing(&memory);
}

#[test]
fn collecting_an_epoch_gives_back_the_slots_of_its_releases() {
    conformance::collecting_an_epoch_gives_back_the_slots_of_its_releases(&memory);
}

#[test]
fn every_receipt_and_fence_holds_a_receipt_slot() {
    conformance::every_receipt_and_fence_holds_a_receipt_slot(&memory);
}

/// A fact that cannot be built within its budget is an error where it
/// stands, never the end of the inventory: the page before it is
/// complete and says that more follows, and the page that reaches it is
/// refused. Durable state that holds a ClientID longer than MQTT allows
/// can only come from outside the store (a corrupt or foreign file);
/// it is read back here as such.
#[test]
fn a_fact_that_cannot_be_built_is_not_the_end_of_the_inventory() {
    let world = World::new(&memory);
    let mut durable = world.store.snapshot().expect("healthy durable snapshot");
    let record = durable.sessions.get_mut(&world.b).expect("b exists");
    record.client_id = vec![b'x'; MAX_CLIENT_ID_BYTES + 1];
    let reopened = MemoryStore::reopen(durable);

    // The epoch, the limits, the route table, its two targets and the
    // session `producer` come before `b`.
    let first = reopened.recover(None, 6).expect("six facts that fit");
    assert_eq!(first.facts.as_slice().len(), 6);
    let next = first.next.expect("the inventory goes on");
    assert_eq!(
        reopened.recover(Some(next), 4),
        Err(RecoveryRefused::Read(ReadError::LimitExceeded))
    );
    assert_eq!(
        reopened.recover(None, 64),
        Err(RecoveryRefused::Read(ReadError::LimitExceeded))
    );
}

#[test]
fn recovered_facts_follow_the_order_of_an_inventory() {
    conformance::recovered_facts_follow_the_order_of_an_inventory(&memory);
}
