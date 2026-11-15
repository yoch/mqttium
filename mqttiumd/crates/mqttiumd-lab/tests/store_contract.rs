//! The contract tests of `DurableStore`, run on the deterministic store.
//!
//! The checks live in `mqttiumd_lab::store::conformance`; the SQLite
//! store of lot L7 runs the same ones with its own factory.

use mqttiumd_contract::ids::StoreIncarnation;
use mqttiumd_lab::store::conformance;
use mqttiumd_lab::store::durable::{Limits, MemoryStore};

fn memory(incarnation: StoreIncarnation, limits: Limits) -> MemoryStore {
    MemoryStore::create(incarnation, limits)
}

#[test]
fn opening_opens_the_next_epoch_and_closes_the_older_ones() {
    conformance::opening_opens_the_next_epoch_and_closes_the_older_ones(&memory);
}

#[test]
fn an_opening_that_expected_another_epoch_is_rejected() {
    conformance::an_opening_that_expected_another_epoch_is_rejected(&memory);
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
