//! Durable exact routes and directional QoS 1 delivery slots. These are store
//! contract tests, independent of the network composition's system reception.
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use mqttiumd_contract::capacity::{BoundedVec, ExchangeSlots};
use mqttiumd_contract::command::{
    ChangeSubscriptions, ClaimDelivery, Command, ConsumeDelivery, FinishDelivery, FrozenRecipe,
    Recipe,
};
use mqttiumd_contract::effects::RecoveredFact;
use mqttiumd_contract::ids::{DeliveryId, OutboundPacketId, Revision};
use mqttiumd_contract::outcome::{CommitFacts, StoreOutcome, StoreReply};
use mqttiumd_contract::packet::{Qos, SharedBytes};
use mqttiumd_contract::pubsub::SubscriptionChange;
use mqttiumd_lab::store::conformance::{
    TOPIC, World, accounts_match_what_is_held, altered, open, operation_id, publication_id,
};
use mqttiumd_lab::store::durable::MemoryStore;
use mqttiumd_store_api::{Durability, DurableStore, ReadError, RecoveryRefused};
use mqttiumd_store_sqlite::{OpenError, SqliteStore};

static NEXT: AtomicU64 = AtomicU64::new(0);
fn fresh_path(name: &str) -> PathBuf {
    let directory = std::env::temp_dir().join(format!(
        "mqttiumd-pubsub-store-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&directory).unwrap();
    directory.join(format!("{name}.sqlite"))
}
fn command(recipe: Recipe) -> Command {
    Command {
        recipe: FrozenRecipe::freeze(recipe).unwrap(),
        payload: None,
    }
}

#[test]
fn subscriptions_claims_and_last_reference_survive_reopen_without_resurrection() {
    let path = fresh_path("reopen");
    let mut sqlite =
        World::new(&|incarnation, limits| SqliteStore::create(&path, incarnation, limits).unwrap());
    let mut memory = World::new(&MemoryStore::create);
    let old_epoch = memory.epoch;
    let delivery = |index| DeliveryId {
        publication: publication_id(old_epoch, 1),
        index,
    };
    let claim = |seq, target, index| {
        command(Recipe::ClaimDelivery(ClaimDelivery {
            operation: operation_id(old_epoch, seq),
            delivery: delivery(index),
            target,
            target_revision: Revision::new(1),
            packet_id: OutboundPacketId::new(7).unwrap(),
        }))
    };
    let finish = |epoch, seq, target, index, claimed_as| {
        command(Recipe::FinishDelivery(FinishDelivery {
            operation: operation_id(epoch, seq),
            delivery: delivery(index),
            target,
            target_revision: Revision::new(1),
            packet_id: OutboundPacketId::new(7).unwrap(),
            claim: operation_id(old_epoch, claimed_as),
        }))
    };
    let remove = command(Recipe::ChangeSubscriptions(ChangeSubscriptions {
        operation: operation_id(old_epoch, 6),
        session: memory.b,
        session_revision: Revision::new(1),
        route_revision: memory.route_revision,
        changes: BoundedVec::from_slice(
            &[SubscriptionChange {
                topic: SharedBytes::copy_from(TOPIC, 256).unwrap(),
                subscribe: false,
            }],
            8,
        )
        .unwrap(),
    }));
    for command in [
        memory.admission(1, 7, b"payload", &[memory.b, memory.c]),
        memory.release(2, 7, 1),
        claim(3, memory.b, 0),
        claim(4, memory.c, 1),
        finish(old_epoch, 5, memory.b, 0, 3),
        remove,
    ] {
        assert_eq!(sqlite.store.apply(&command), memory.store.apply(&command));
        assert_eq!(
            sqlite.store.snapshot().unwrap(),
            memory.store.snapshot().unwrap()
        );
    }
    let surviving = memory.store.snapshot().unwrap();
    assert_eq!(surviving.obligations.len(), 1);
    assert_eq!(surviving.outbound.len(), 1);
    assert_eq!(accounts_match_what_is_held(&surviving), Ok(()));
    drop(sqlite.store);
    let mut reopened = SqliteStore::open(&path).unwrap();
    assert_eq!(reopened.snapshot().unwrap(), surviving);
    assert_eq!(
        reopened.read_work(memory.b, None, 8).unwrap().items.len(),
        0,
        "finished destination never reappears"
    );
    let pending = reopened.read_work(memory.c, None, 8).unwrap();
    assert_eq!(pending.items.len(), 1);
    assert_eq!(pending.items.as_slice()[0].delivery, delivery(1));
    let recovered_claim = pending.items.as_slice()[0].claim.unwrap();
    assert_eq!(recovered_claim.operation, operation_id(old_epoch, 4));
    assert_eq!(recovered_claim.packet_id.get(), 7);
    let epoch = open(&mut reopened);
    assert_eq!(open(&mut memory.store), epoch);
    let inventory = reopened.recover(None, 64).unwrap();
    assert_eq!(inventory, memory.store.recover(None, 64).unwrap());
    assert!(
        inventory
            .facts
            .as_slice()
            .contains(&RecoveredFact::OutboundDelivery {
                expiry: None,
                started: false,
                delivery: delivery(1),
                target: memory.c,
                target_revision: Revision::new(1),
                qos: Qos::AtLeastOnce,
                claim: Some(recovered_claim)
            })
    );
    let terminal = finish(epoch, 7, memory.c, 1, 4);
    assert_eq!(reopened.apply(&terminal), memory.store.apply(&terminal));
    let final_state = memory.store.snapshot().unwrap();
    assert!(
        final_state.outbound.is_empty()
            && final_state.obligations.is_empty()
            && final_state.payloads.is_empty()
            && final_state.publications.is_empty()
    );
    assert_eq!(accounts_match_what_is_held(&final_state), Ok(()));
    assert_eq!(reopened.snapshot().unwrap(), final_state);
    drop(reopened);
    let mut reopened = SqliteStore::open(&path).unwrap();
    assert!(
        !reopened.apply(&terminal).committed_now,
        "replayed finish cannot free anything again"
    );
    assert_eq!(reopened.snapshot().unwrap(), final_state);
}

#[test]
fn new_commands_preserve_unknown_outcomes_and_reconcile_the_same_operation() {
    let path = fresh_path("unknown");
    let mut sqlite =
        World::new(&|incarnation, limits| SqliteStore::create(&path, incarnation, limits).unwrap());
    let mut memory = World::new(&MemoryStore::create);
    let delivery = DeliveryId {
        publication: publication_id(memory.epoch, 2),
        index: 0,
    };
    let recipes = [
        command(Recipe::ChangeSubscriptions(ChangeSubscriptions {
            operation: operation_id(memory.epoch, 1),
            session: memory.b,
            session_revision: Revision::new(1),
            route_revision: memory.route_revision,
            changes: BoundedVec::from_slice(
                &[SubscriptionChange {
                    topic: SharedBytes::copy_from(TOPIC, 256).unwrap(),
                    subscribe: true,
                }],
                8,
            )
            .unwrap(),
        })),
        altered(&memory.admission(2, 7, b"payload", &[memory.b]), |admit| {
            admit.qos = Qos::AtLeastOnce;
            admit.costs.exchange_slots = ExchangeSlots::ZERO;
            admit.route_revision = memory.route_revision.checked_next().unwrap();
        }),
        command(Recipe::ClaimDelivery(ClaimDelivery {
            operation: operation_id(memory.epoch, 3),
            delivery,
            target: memory.b,
            target_revision: Revision::new(1),
            packet_id: OutboundPacketId::new(7).unwrap(),
        })),
        command(Recipe::FinishDelivery(FinishDelivery {
            operation: operation_id(memory.epoch, 4),
            delivery,
            target: memory.b,
            target_revision: Revision::new(1),
            packet_id: OutboundPacketId::new(7).unwrap(),
            claim: operation_id(memory.epoch, 3),
        })),
    ];
    for command in recipes {
        let operation = command.recipe.get().operation();
        let before = memory.store.snapshot().unwrap();
        sqlite.store.set_durability(Durability::Failing);
        memory.store.set_durability(Durability::Failing);
        assert_eq!(sqlite.store.apply(&command), memory.store.apply(&command));
        assert_eq!(
            sqlite.store.apply(&command).reply,
            StoreReply::Outcome(StoreOutcome::PendingOrUnknown)
        );
        assert_eq!(sqlite.store.snapshot().unwrap(), before);
        assert_eq!(memory.store.snapshot().unwrap(), before);
        assert_eq!(sqlite.store.established(operation), Ok(None));
        assert_eq!(
            sqlite.store.resolve_and_fence(operation),
            memory.store.resolve_and_fence(operation)
        );
        assert_eq!(
            sqlite.store.snapshot().unwrap(),
            before,
            "an unavailable fence proves no absence"
        );
        sqlite.store.set_durability(Durability::Available);
        memory.store.set_durability(Durability::Available);
        let committed = sqlite.store.apply(&command);
        assert_eq!(committed, memory.store.apply(&command));
        assert!(committed.committed_now);
        assert_eq!(
            sqlite.store.established(operation),
            Ok(Some(match committed.reply {
                StoreReply::Outcome(outcome) => outcome,
                _ => panic!("no mismatch"),
            }))
        );
        assert_eq!(
            sqlite.store.snapshot().unwrap(),
            memory.store.snapshot().unwrap()
        );
        let stable = sqlite.store.snapshot().unwrap();
        assert!(!sqlite.store.apply(&command).committed_now);
        assert_eq!(sqlite.store.snapshot().unwrap(), stable);
    }
    assert!(sqlite.store.snapshot().unwrap().payloads.is_empty());
}

fn corrupt(path: &Path, sql: &str) {
    let raw = rusqlite::Connection::open(path).unwrap();
    raw.execute_batch(sql).unwrap();
    assert_eq!(
        raw.query_row("PRAGMA quick_check", [], |row| row.get::<_, String>(0))
            .unwrap(),
        "ok"
    );
}

#[test]
fn work_validates_unclaimed_discriminants_before_selecting_qos_or_cursor() {
    for mutation in [
        "UPDATE obligations SET qos=99",
        "UPDATE obligations SET qos=0,cause=99",
        "UPDATE obligations SET qos=2,phase=99",
    ] {
        let path = fresh_path("invalid-unclaimed-work");
        let mut world = World::new(&|incarnation, limits| {
            SqliteStore::create(&path, incarnation, limits).unwrap()
        });
        assert!(
            world
                .store
                .apply(&world.admission(1, 7, b"payload", &[world.b]))
                .committed_now
        );
        let before = world.store.snapshot().unwrap();
        assert!(before.outbound.is_empty());
        let delivery = *before.obligations.keys().next().unwrap();
        assert_eq!(
            world.store.read_work(world.b, None, 8).unwrap().items.len(),
            1
        );
        // Deliberate corruption of a disposable live store, not a product API.
        // The writer remains open: startup validation cannot detect this for it.
        corrupt(&path, mutation);
        assert_eq!(
            world.store.read_work(world.b, None, 8),
            Err(ReadError::InvalidData),
            "{mutation}"
        );
        assert_eq!(
            world.store.read_work(world.b, Some(delivery), 8),
            Err(ReadError::InvalidData),
            "a cursor must not hide {mutation}"
        );
        assert!(
            world
                .store
                .read_work(world.c, None, 8)
                .unwrap()
                .items
                .is_empty()
        );
        assert_eq!(world.store.snapshot(), Err(ReadError::InvalidData));
        drop(world.store);
        assert_eq!(
            SqliteStore::open(&path).unwrap().snapshot(),
            Err(ReadError::InvalidData),
            "startup still refuses {mutation}"
        );
    }
}

#[test]
fn work_selects_pending_qos0_and_qos2() {
    for (wire, qos) in [(0, Qos::AtMostOnce), (2, Qos::ExactlyOnce)] {
        let path = fresh_path("valid-work-qos");
        let mut world = World::new(&|incarnation, limits| {
            SqliteStore::create(&path, incarnation, limits).unwrap()
        });
        assert!(
            world
                .store
                .apply(&world.admission(1, 7, b"payload", &[world.b]))
                .committed_now
        );
        corrupt(&path, &format!("UPDATE obligations SET qos={wire}"));
        let page = world.store.read_work(world.b, None, 8).unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items.as_slice()[0].qos, qos);
        assert!(page.items.as_slice()[0].claim.is_none());
    }
}

#[test]
fn malformed_claim_metadata_stays_typed_through_all_outgoing_readers() {
    for mutation in [
        "UPDATE outbound SET target_revision=x'0000000000000063'",
        "UPDATE outbound SET target=x'0000000000000003'",
        "UPDATE outbound SET claim=zeroblob(32)",
        "DELETE FROM obligations",
    ] {
        let path = fresh_path("invalid-claim");
        let mut world = World::new(&|incarnation, limits| {
            SqliteStore::create(&path, incarnation, limits).unwrap()
        });
        let admission = world.admission(1, 7, b"payload", &[world.b]);
        assert!(world.store.apply(&admission).committed_now);
        let claim = command(Recipe::ClaimDelivery(ClaimDelivery {
            operation: operation_id(world.epoch, 2),
            delivery: DeliveryId {
                publication: publication_id(world.epoch, 1),
                index: 0,
            },
            target: world.b,
            target_revision: Revision::new(1),
            packet_id: OutboundPacketId::new(7).unwrap(),
        }));
        assert!(world.store.apply(&claim).committed_now);
        let target = world.b;
        drop(world.store);
        corrupt(&path, mutation);
        let store = SqliteStore::open(&path).unwrap();
        assert_eq!(store.snapshot(), Err(ReadError::InvalidData), "{mutation}");
        assert_eq!(
            SqliteStore::inspect(&path),
            Err(ReadError::InvalidData),
            "{mutation}"
        );
        assert_eq!(
            store.recover(None, 64),
            Err(RecoveryRefused::Read(ReadError::InvalidData)),
            "{mutation}"
        );
        assert_eq!(
            store.read_work(target, None, 8),
            Err(ReadError::InvalidData),
            "{mutation}"
        );
    }
}

#[test]
fn earlier_schema_refusal_preserves_main_and_uncheckpointed_wal_bytes() {
    for version in [1, 2, 3] {
        let path = fresh_path("schema1");
        let raw = rusqlite::Connection::open(&path).unwrap();
        raw.execute_batch(&format!("PRAGMA journal_mode=WAL;PRAGMA wal_autocheckpoint=0;PRAGMA user_version={version};CREATE TABLE old_store(one INTEGER PRIMARY KEY, contents BLOB);INSERT INTO old_store VALUES(1,x'01020304');")).unwrap();
        let wal = PathBuf::from(format!("{}-wal", path.display()));
        let main_before = std::fs::read(&path).unwrap();
        let wal_before = std::fs::read(&wal).unwrap();
        assert!(!wal_before.is_empty());
        for _ in 0..2 {
            assert!(
                matches!(SqliteStore::open(&path),Err(OpenError::SchemaVersion(found)) if found==version)
            );
            assert_eq!(std::fs::read(&path).unwrap(), main_before);
            assert_eq!(std::fs::read(&wal).unwrap(), wal_before);
        }
        drop(raw);
    }
}

#[test]
fn consuming_a_qos0_obligation_has_no_claim_and_survives_lost_results_and_restart() {
    let path = fresh_path("consume-qos0");
    let mut sqlite =
        World::new(&|incarnation, limits| SqliteStore::create(&path, incarnation, limits).unwrap());
    let mut memory = World::new(&MemoryStore::create);
    let admission = altered(&memory.admission(1, 7, b"payload", &[memory.b]), |a| {
        a.targets.as_mut_slice()[0].effective_qos = Qos::AtMostOnce
    });
    for command in [admission, memory.release(2, 7, 1)] {
        assert_eq!(sqlite.store.apply(&command), memory.store.apply(&command));
    }
    let delivery = DeliveryId {
        publication: publication_id(memory.epoch, 1),
        index: 0,
    };
    let consume = command(Recipe::ConsumeDelivery(ConsumeDelivery {
        operation: operation_id(memory.epoch, 3),
        delivery,
        target: memory.b,
        target_revision: Revision::new(1),
    }));
    let before = memory.store.snapshot().unwrap();
    assert!(before.outbound.is_empty());
    assert_eq!(before.accounts.exchange_slots, ExchangeSlots::ZERO);
    sqlite.store.set_durability(Durability::Failing);
    memory.store.set_durability(Durability::Failing);
    assert_eq!(sqlite.store.apply(&consume), memory.store.apply(&consume));
    assert_eq!(
        sqlite.store.apply(&consume).reply,
        StoreReply::Outcome(StoreOutcome::PendingOrUnknown)
    );
    assert_eq!(sqlite.store.snapshot().unwrap(), before);
    assert_eq!(memory.store.snapshot().unwrap(), before);
    assert_eq!(
        sqlite.store.resolve_and_fence(consume.recipe.operation()),
        memory.store.resolve_and_fence(consume.recipe.operation())
    );
    assert_eq!(sqlite.store.snapshot().unwrap(), before);
    sqlite.store.set_durability(Durability::Available);
    memory.store.set_durability(Durability::Available);
    // The application may lose this committed result; resolution uses its
    // original operation, never a newly invented delivery or packet id.
    assert_eq!(sqlite.store.apply(&consume), memory.store.apply(&consume));
    let established = StoreOutcome::Committed(CommitFacts::DeliveryConsumed {
        delivery,
        target: memory.b,
    });
    assert_eq!(
        sqlite
            .store
            .resolve_and_fence(consume.recipe.operation())
            .outcome,
        established
    );
    assert_eq!(
        memory
            .store
            .resolve_and_fence(consume.recipe.operation())
            .outcome,
        established
    );
    let after = memory.store.snapshot().unwrap();
    assert_eq!(sqlite.store.snapshot().unwrap(), after);
    assert!(
        after.obligations.is_empty()
            && after.outbound.is_empty()
            && after.publications.is_empty()
            && after.payloads.is_empty()
    );
    assert_eq!(accounts_match_what_is_held(&after), Ok(()));
    let raw = rusqlite::Connection::open(&path).unwrap();
    assert_eq!(
        raw.query_row(
            "SELECT length(facts) FROM operations WHERE decision=7",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        44
    );
    drop(raw);
    drop(sqlite.store);
    let mut reopened = SqliteStore::open(&path).unwrap();
    assert_eq!(reopened.snapshot().unwrap(), after);
    assert_eq!(
        reopened.established(consume.recipe.operation()),
        Ok(Some(established))
    );
    assert!(!reopened.apply(&consume).committed_now);
    assert_eq!(reopened.snapshot().unwrap(), after);
    assert!(
        reopened
            .read_work(memory.b, None, 8)
            .unwrap()
            .items
            .is_empty()
    );
    assert_eq!(open(&mut reopened), open(&mut memory.store));
    let inventory = reopened.recover(None, 64).unwrap();
    assert_eq!(inventory, memory.store.recover(None, 64).unwrap());
    assert!(
        !inventory
            .facts
            .as_slice()
            .iter()
            .any(|f| matches!(f, RecoveredFact::OutboundDelivery { .. })),
        "a consumption receipt never reconstructs outgoing work"
    );
    assert!(inventory.facts.as_slice().iter().any(|f|matches!(f,RecoveredFact::Operation{knowledge:mqttiumd_contract::outcome::Knowledge::Committed(CommitFacts::DeliveryConsumed{delivery:found,..}),..} if *found==delivery)));
}

#[test]
fn operation_fact_payloads_are_bounded_and_decoded_without_guessing() {
    let path = fresh_path("facts");
    let mut world =
        World::new(&|incarnation, limits| SqliteStore::create(&path, incarnation, limits).unwrap());
    let changed = command(Recipe::ChangeSubscriptions(ChangeSubscriptions {
        operation: operation_id(world.epoch, 1),
        session: world.b,
        session_revision: Revision::new(1),
        route_revision: world.route_revision,
        changes: BoundedVec::from_slice(
            &[SubscriptionChange {
                topic: SharedBytes::copy_from(TOPIC, 256).unwrap(),
                subscribe: true,
            }],
            8,
        )
        .unwrap(),
    }));
    assert!(matches!(
        world.store.apply(&changed).reply,
        StoreReply::Outcome(StoreOutcome::Committed(
            CommitFacts::SubscriptionsChanged { .. }
        ))
    ));
    let operation = changed.recipe.get().operation();
    drop(world.store);
    corrupt(
        &path,
        "UPDATE operations SET facts=zeroblob(15) WHERE decision=4",
    );
    let mut store = SqliteStore::open(&path).unwrap();
    assert_eq!(store.established(operation), Err(ReadError::InvalidData));
    assert_eq!(
        store.apply(&changed).reply,
        StoreReply::Outcome(StoreOutcome::PendingOrUnknown)
    );
    assert_eq!(store.snapshot(), Err(ReadError::InvalidData));
}

/// The discard of an oversized delivery (ADR 0025 as amended) on SQLite:
/// each restart of the shared check closes the file and opens it again.
#[test]
fn a_never_started_claim_is_ended_once_by_its_guarded_finish_across_reopens() {
    for (qos, timed) in [
        (Qos::AtLeastOnce, false),
        (Qos::ExactlyOnce, false),
        (Qos::AtLeastOnce, true),
        (Qos::ExactlyOnce, true),
    ] {
        let path = fresh_path("discard");
        mqttiumd_lab::store::conformance::a_never_started_claim_is_ended_once_by_its_guarded_finish(
            &|incarnation, limits| SqliteStore::create(&path, incarnation, limits).unwrap(),
            &|store| {
                drop(store);
                SqliteStore::open(&path).unwrap()
            },
            qos,
            timed,
        );
    }
}
