//! Outgoing Packet Identifiers recycle; durable causal identities do not.
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
        "mqttiumd-pi-store-{}-{}",
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
fn recycled_packet_id_after_collected_finish_preserves_occurrence() {
    conformance::recycled_packet_ids_preserve_occurrences_after_receipt_collection(
        &MemoryStore::create,
    );
    conformance::recycled_packet_ids_preserve_occurrences_after_receipt_collection(&sqlite);
}
#[test]
fn lost_claim_and_finish_reply_resolve_without_new_identity() {
    conformance::lost_claim_results_preserve_packet_reservations_until_guarded_finish(
        &MemoryStore::create,
    );
    conformance::lost_claim_results_preserve_packet_reservations_until_guarded_finish(&sqlite);
}

use mqttiumd_contract::capacity::{ExchangeSlots, ReceiptSlots};
use mqttiumd_contract::command::{ClaimDelivery, Command, FinishDelivery, FrozenRecipe, Recipe};
use mqttiumd_contract::effects::RecoveredFact;
use mqttiumd_contract::ids::{
    DeliveryId, OperationSeq, OutboundPacketId, PayloadSeq, PublicationSeq, Revision,
};
use mqttiumd_contract::outcome::{CommitFacts, RejectReason, StoreOutcome, StoreReply};
use mqttiumd_contract::packet::Qos;
use mqttiumd_contract::receipts::{
    ReceiptClosure, ReceiptCollection, ReceiptCollectionReply, ReceiptOrigin,
};
use mqttiumd_lab::store::conformance::{
    World, altered, fixture_limits, open, operation_id, publication_id,
};
use mqttiumd_store_api::{Durability, DurableStore};

#[test]
fn recovery_reserves_unloaded_claim_and_failed_finish_cannot_free_its_pi() {
    let path = path();
    let mut limits = fixture_limits();
    limits.receipt_slots = ReceiptSlots::new(8192);
    let mut sql = World::with_limits(
        &|incarnation, limits| SqliteStore::create(&path, incarnation, limits).unwrap(),
        limits,
    );
    let mut memory = World::with_limits(&MemoryStore::create, limits);
    let old_epoch = memory.epoch;
    let target = memory.b;
    let delivery = |published_as| DeliveryId {
        publication: publication_id(old_epoch, published_as),
        index: 0,
    };
    let claim = |epoch, sequence, published_as| Command {
        recipe: FrozenRecipe::freeze(Recipe::ClaimDelivery(ClaimDelivery {
            operation: operation_id(epoch, sequence),
            delivery: delivery(published_as),
            target,
            target_revision: Revision::new(1),
            packet_id: OutboundPacketId::new(1).unwrap(),
        }))
        .unwrap(),
        payload: None,
    };
    let finish = |epoch, sequence, published_as, claim| Command {
        recipe: FrozenRecipe::freeze(Recipe::FinishDelivery(FinishDelivery {
            operation: operation_id(epoch, sequence),
            delivery: delivery(published_as),
            target,
            target_revision: Revision::new(1),
            packet_id: OutboundPacketId::new(1).unwrap(),
            claim,
        }))
        .unwrap(),
        payload: None,
    };
    let empty = ReceiptClosure::empty(ReceiptOrigin::of(operation_id(old_epoch, 1)));
    assert_eq!(
        sql.store.collect_receipts(ReceiptCollection {
            closure: empty,
            limit: 128
        }),
        memory.store.collect_receipts(ReceiptCollection {
            closure: empty,
            limit: 128
        })
    );
    for seq in 1..=2 {
        let admission = altered(
            &memory.admission(seq, 1, b"numbered responsibility", &[target]),
            |admit| {
                admit.qos = Qos::AtLeastOnce;
                admit.costs.exchange_slots = ExchangeSlots::ZERO;
            },
        );
        assert_eq!(sql.store.apply(&admission), memory.store.apply(&admission));
    }
    let a_claim = claim(old_epoch, 3, 1);
    assert_eq!(sql.store.apply(&a_claim), memory.store.apply(&a_claim));
    let closure = ReceiptClosure {
        through: OperationSeq::new(3),
        publication_through: PublicationSeq::new(2),
        payload_through: PayloadSeq::new(2),
        ..empty
    };
    assert_eq!(
        sql.store.collect_receipts(ReceiptCollection {
            closure,
            limit: 128
        }),
        Ok(ReceiptCollectionReply::Applied {
            closure,
            removed: 3,
            retained: 0,
            more: false
        })
    );
    memory
        .store
        .collect_receipts(ReceiptCollection {
            closure,
            limit: 128,
        })
        .unwrap();
    assert_eq!(
        sql.store.snapshot().unwrap(),
        memory.store.snapshot().unwrap()
    );
    drop(sql.store);
    let mut sql = SqliteStore::open(&path).unwrap();
    let epoch = open(&mut sql);
    assert_eq!(epoch, open(&mut memory.store));
    let mut cursor = None;
    let mut recovered = Vec::new();
    loop {
        let page = sql.recover(cursor, 1).unwrap();
        assert_eq!(page, memory.store.recover(cursor, 1).unwrap());
        recovered.extend_from_slice(page.facts.as_slice());
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    assert!(recovered.iter().any(|fact|matches!(fact,RecoveredFact::OutboundDelivery {delivery:id,claim:Some(claim),..} if *id==delivery(1)&&claim.packet_id.get()==1&&claim.operation==operation_id(old_epoch,3))));
    assert!(recovered.iter().any(|fact|matches!(fact,RecoveredFact::OutboundDelivery {delivery:id,claim:None,..} if *id==delivery(2))));
    // Load B's payload first. A's metadata alone still reserves target/PI1.
    let b_page = sql.read_work(target, Some(delivery(1)), 1).unwrap();
    assert_eq!(b_page.items.as_slice()[0].delivery, delivery(2));
    assert!(b_page.items.as_slice()[0].claim.is_none());
    for sequence in [1, 3] {
        if sequence == 3 {
            let a_finish = finish(epoch, 2, 1, operation_id(old_epoch, 3));
            let before = sql.snapshot().unwrap();
            sql.set_durability(Durability::Failing);
            memory.store.set_durability(Durability::Failing);
            assert_eq!(sql.apply(&a_finish), memory.store.apply(&a_finish));
            assert_eq!(
                sql.apply(&a_finish).reply,
                StoreReply::Outcome(StoreOutcome::PendingOrUnknown)
            );
            assert_eq!(sql.snapshot().unwrap(), before);
            sql.set_durability(Durability::Available);
            memory.store.set_durability(Durability::Available);
        }
        let blocked = claim(epoch, sequence, 2);
        assert_eq!(sql.apply(&blocked), memory.store.apply(&blocked));
        assert_eq!(
            sql.established(blocked.recipe.operation()),
            Ok(Some(StoreOutcome::RejectedCertain(
                RejectReason::PreconditionFailed
            )))
        );
        assert_eq!(
            sql.snapshot().unwrap().outbound[&delivery(1)].claim,
            operation_id(old_epoch, 3)
        );
    }
    let a_finish = finish(epoch, 2, 1, operation_id(old_epoch, 3));
    let _lost = sql.apply(&a_finish);
    let _reference = memory.store.apply(&a_finish);
    let resolved = sql.resolve_and_fence(a_finish.recipe.operation());
    assert_eq!(
        resolved,
        memory.store.resolve_and_fence(a_finish.recipe.operation())
    );
    assert!(
        matches!(resolved.outcome,StoreOutcome::Committed(CommitFacts::DeliveryFinished {claim,..}) if claim==operation_id(old_epoch,3))
    );
    assert!(!resolved.fenced_now);
    let b_claim = claim(epoch, 4, 2);
    assert_eq!(sql.apply(&b_claim), memory.store.apply(&b_claim));
    let reused = sql.snapshot().unwrap();
    assert_eq!(reused.outbound[&delivery(2)].claim, operation_id(epoch, 4));
    assert!(!sql.apply(&a_finish).committed_now);
    assert_eq!(sql.snapshot().unwrap(), reused);
    let obsolete = finish(epoch, 5, 2, operation_id(old_epoch, 3));
    assert_eq!(sql.apply(&obsolete), memory.store.apply(&obsolete));
    assert_eq!(sql.snapshot().unwrap().outbound, reused.outbound);
    let terminal = finish(epoch, 6, 2, operation_id(epoch, 4));
    assert_eq!(sql.apply(&terminal), memory.store.apply(&terminal));
    let done = sql.snapshot().unwrap();
    assert_eq!(done, memory.store.snapshot().unwrap());
    assert!(
        done.outbound.is_empty()
            && done.obligations.is_empty()
            && done.publications.is_empty()
            && done.payloads.is_empty()
    );
    assert_eq!(done.accounts.exchange_slots, ExchangeSlots::ZERO);
}
