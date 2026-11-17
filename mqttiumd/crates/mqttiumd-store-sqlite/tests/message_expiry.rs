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

#[test]
fn expired_retained_values_are_skipped_without_maintenance() {
    expiry::expired_retained_values_are_skipped_without_maintenance(&sqlite);
}
#[test]
fn offline_expiry_removes_pending_copies() {
    expiry::offline_expiry_removes_pending_copies(&sqlite);
}
#[test]
fn frozen_observations_may_precede_latest_checkpoint() {
    expiry::frozen_observations_may_precede_latest_checkpoint(&sqlite);
}

fn prepared_file() -> (
    std::path::PathBuf,
    SqliteStore,
    mqttiumd_contract::ids::SessionId,
) {
    use mqttiumd_contract::capacity::References;
    use mqttiumd_contract::command::{ClaimDelivery, Command, FrozenRecipe, Recipe};
    use mqttiumd_contract::ids::{DeliveryId, OutboundPacketId, Revision};
    use mqttiumd_contract::message_expiry::{
        ExpiringAdmission, ExpiringPublication, MessageExpiry,
    };
    use mqttiumd_contract::retained::RetainedPublication;
    use mqttiumd_lab::store::conformance::{World, fixture_limits, operation_id, publication_id};
    use mqttiumd_store_api::{DurableStore, SessionClockReading};
    let path = std::env::temp_dir().join(format!(
        "mqttiumd-message-expiry-read-{}-{}.db",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let mut w = World::with_limits(
        &|i, l| SqliteStore::create(&path, i, l).unwrap(),
        fixture_limits(),
    );
    let now = expiry::at(0);
    w.store
        .checkpoint_session_clock(
            None,
            SessionClockReading {
                domain: now.domain,
                tick: now.tick,
            },
        )
        .unwrap();
    let old = w.admission(1, 1, b"payload", &[w.b]);
    let mut canonical = Vec::new();
    old.recipe.get().encode_canonical(&mut canonical).unwrap();
    let Recipe::AdmitPublication(mut admission) =
        Recipe::decode_canonical(&canonical, 16, 256).unwrap()
    else {
        panic!()
    };
    admission.retain = true;
    admission.costs.references = References::new(2);
    let c = Command {
        recipe: FrozenRecipe::freeze(Recipe::ExpiringPublication(ExpiringPublication {
            admission: ExpiringAdmission::Qos12(RetainedPublication {
                admission,
                retain_targets: 0,
            }),
            expiry: MessageExpiry {
                observed: now,
                interval: 1,
            },
        }))
        .unwrap(),
        payload: old.payload,
    };
    assert!(w.store.apply(&c).committed_now);
    let claim = Command {
        recipe: FrozenRecipe::freeze(Recipe::ClaimDelivery(ClaimDelivery {
            operation: operation_id(w.epoch, 2),
            delivery: DeliveryId {
                publication: publication_id(w.epoch, 1),
                index: 0,
            },
            target: w.b,
            target_revision: Revision::new(1),
            packet_id: OutboundPacketId::new(1).unwrap(),
        }))
        .unwrap(),
        payload: None,
    };
    assert!(w.store.apply(&claim).committed_now);
    (path, w.store, w.b)
}

#[test]
fn expiry_and_started_metadata_survive_reopen() {
    use mqttiumd_store_api::DurableStore;
    let (path, store, target) = prepared_file();
    let expected = store.snapshot().unwrap();
    drop(store);
    let reopened = SqliteStore::open(&path).unwrap();
    assert_eq!(reopened.snapshot().unwrap(), expected);
    let work = reopened.read_work(target, None, 8).unwrap();
    assert_eq!(work.items.as_slice()[0].expiry.unwrap().interval, 1);
    assert!(!work.items.as_slice()[0].claim.unwrap().started);
}

#[test]
fn physically_healthy_expiry_corruption_refuses_before_selection() {
    use mqttiumd_store_api::{DurableStore, ReadError, RecoveryRefused};
    for sql in [
        "UPDATE publications SET expiry_tick=x'ffffffffffffffff',expiry_interval=1",
        "UPDATE publications SET expiry_domain=zeroblob(32)",
        "UPDATE outbound SET started=1; UPDATE obligations SET qos=2",
    ] {
        let (path, store, target) = prepared_file();
        let raw = rusqlite::Connection::open(&path).unwrap();
        raw.execute_batch(sql).unwrap();
        let quick: String = raw
            .query_row("PRAGMA quick_check", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            quick, "ok",
            "requires semantic, physically consistent corruption"
        );
        assert_eq!(
            store.snapshot(),
            Err(ReadError::InvalidData),
            "snapshot {sql}"
        );
        assert_eq!(
            SqliteStore::inspect(&path),
            Err(ReadError::InvalidData),
            "inspect {sql}"
        );
        assert_eq!(
            store.recover(None, 1),
            Err(RecoveryRefused::Read(ReadError::InvalidData)),
            "recovery {sql}"
        );
        let after = store.read_work(target, None, 8).err();
        assert_eq!(after, Some(ReadError::InvalidData), "work {sql}");
    }
}

#[test]
fn failed_expiry_transaction_preserves_all_shared_ownership() {
    use mqttiumd_store_api::{Durability, DurableStore, ReadError};
    let (_path, mut store, _target) = prepared_file();
    let before = store.snapshot().unwrap();
    store.set_durability(Durability::Failing);
    assert_eq!(
        store.expire_messages(expiry::at(1), None, 4),
        Err(ReadError::Unavailable)
    );
    assert_eq!(store.snapshot().unwrap(), before);
    store.set_durability(Durability::Available);
    let result = store.expire_messages(expiry::at(1), None, 4).unwrap();
    assert_eq!(result.retained_removed, 1);
    assert!(result.removed.is_empty());
    let state = store.snapshot().unwrap();
    assert_eq!(state.outbound, before.outbound);
    assert_eq!(state.accounts.references.get(), 1);
}

#[test]
fn maintenance_does_not_read_past_its_publication_budget() {
    use mqttiumd_contract::command::{Command, FrozenRecipe, Recipe};
    use mqttiumd_contract::message_expiry::{
        ExpiringAdmission, ExpiringPublication, MessageExpiry,
    };
    use mqttiumd_contract::retained::RetainedPublication;
    use mqttiumd_lab::store::conformance::{World, fixture_limits, publication_id};
    use mqttiumd_store_api::{DurableStore, ReadError, SessionClockReading};
    let path = std::env::temp_dir().join(format!(
        "mqttiumd-message-expiry-bound-{}-{}.db",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let mut l = fixture_limits();
    l.references = mqttiumd_contract::capacity::References::new(128);
    l.exchange_slots = mqttiumd_contract::capacity::ExchangeSlots::new(128);
    let mut w = World::with_limits(&|i, l| SqliteStore::create(&path, i, l).unwrap(), l);
    let now = expiry::at(0);
    w.store
        .checkpoint_session_clock(
            None,
            SessionClockReading {
                domain: now.domain,
                tick: now.tick,
            },
        )
        .unwrap();
    for n in 1..=5 {
        let old = w.admission(n, n as u16, b"state", &[w.b]);
        let mut raw = Vec::new();
        old.recipe.get().encode_canonical(&mut raw).unwrap();
        let Recipe::AdmitPublication(admission) = Recipe::decode_canonical(&raw, 16, 256).unwrap()
        else {
            panic!()
        };
        let c = Command {
            recipe: FrozenRecipe::freeze(Recipe::ExpiringPublication(ExpiringPublication {
                admission: ExpiringAdmission::Qos12(RetainedPublication {
                    admission,
                    retain_targets: 0,
                }),
                expiry: MessageExpiry {
                    observed: now,
                    interval: 1,
                },
            }))
            .unwrap(),
            payload: old.payload,
        };
        assert!(w.store.apply(&c).committed_now);
    }
    let raw = rusqlite::Connection::open(&path).unwrap();
    raw.execute(
        "UPDATE publications SET expiry_tick=x'ffffffffffffffff' WHERE id=?1",
        [publication_id(w.epoch, 5).to_be_bytes().as_slice()],
    )
    .unwrap();
    let page = w
        .store
        .expire_messages(expiry::at(1), None, 4)
        .expect("sweep_read_past_publication_budget");
    assert_eq!(page.removed.len(), 4);
    assert!(page.more);
    assert_eq!(page.next, Some(publication_id(w.epoch, 4)));
    assert_eq!(
        w.store.expire_messages(expiry::at(1), page.next, 4),
        Err(ReadError::InvalidData),
        "selected_corrupt_expiry_was_hidden"
    );
}

#[test]
fn timed_commands_require_the_current_boot_authority() {
    expiry::timed_commands_require_the_current_boot_authority(&sqlite);
}
#[test]
fn timed_replay_rejection_reserves_the_full_identity_range() {
    expiry::timed_replay_rejection_reserves_the_full_identity_range(&sqlite);
}
#[test]
fn empty_retained_clear_still_creates_its_live_publication() {
    expiry::empty_retained_clear_still_creates_its_live_publication(&sqlite);
}

#[test]
fn timed_replay_cannot_observe_before_the_message_origin() {
    expiry::timed_replay_cannot_observe_before_the_message_origin(&sqlite);
}

#[test]
fn maintenance_retry_skips_admissions_after_the_frozen_observation() {
    expiry::maintenance_retry_skips_admissions_after_the_frozen_observation(&sqlite);
}

#[test]
fn started_marker_requires_message_expiry() {
    expiry::started_marker_requires_message_expiry(&sqlite);
}

#[test]
fn delayed_started_result_remains_admissible_after_expiry() {
    expiry::delayed_started_result_remains_admissible_after_expiry(&sqlite);
}
