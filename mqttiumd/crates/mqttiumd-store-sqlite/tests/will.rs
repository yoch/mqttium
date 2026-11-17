//! Will intent/admission contract, run identically against both stores.
use mqttiumd_contract::capacity::{
    BoundedVec, ExchangeSlots, PayloadBytes, ReceiptSlots, References,
};
use mqttiumd_contract::command::{
    ClaimDelivery, Command, Costs, DeliveryCause, FinishDelivery, FrozenRecipe, PayloadRef, Recipe,
    Target,
};
use mqttiumd_contract::ids::{
    DeliveryId, OutboundPacketId, PayloadId, PayloadSeq, Revision, StoreIncarnation,
};
use mqttiumd_contract::outcome::{CommitFacts, StoreOutcome, StoreReply};
use mqttiumd_contract::packet::{Qos, SharedBytes};
use mqttiumd_contract::session::{
    SessionAction, SessionChange, SessionExpiry, SessionGuard, SessionLifecycle,
};
use mqttiumd_contract::will::{WillPublication, WillSpec, WillState};
use mqttiumd_lab::store::conformance::{
    World, accounts_match_what_is_held, fixture_limits, open, operation_id, publication_id,
};
use mqttiumd_lab::store::durable::MemoryStore;
use mqttiumd_store_api::{AdminCommand, AdminDone, DurableStore, ReadBudget, ReadError};
use mqttiumd_store_sqlite::SqliteStore;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "mqttiumd-will-store-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&p).unwrap();
        Self(p)
    }
    fn db(&self) -> PathBuf {
        self.0.join("store.sqlite")
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn bytes(b: &[u8]) -> SharedBytes {
    SharedBytes::copy_from(b, 32768).unwrap()
}
fn command(operation: mqttiumd_contract::ids::OperationId, action: SessionAction) -> Command {
    Command {
        recipe: FrozenRecipe::freeze(Recipe::SessionLifecycle(SessionLifecycle {
            operation,
            action,
        }))
        .unwrap(),
        payload: None,
    }
}
fn establish(
    store: &mut impl DurableStore,
    epoch: mqttiumd_contract::ids::RunEpoch,
    seq: u64,
    expiry: SessionExpiry,
) -> SessionGuard {
    let op = operation_id(epoch, seq);
    let result = store.apply(&command(
        op,
        SessionAction::OpenWillAt {
            client_id: bytes(b"will-source"),
            expected: None,
            clean_start: false,
            expiry,
            now: 0,
            will: Some(WillSpec {
                topic: bytes(b"p1/topic"),
                payload: bytes(b"offline"),
            }),
        },
    ));
    let StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::SessionLifecycle(
        SessionChange::Opened { session, .. },
    ))) = result.reply
    else {
        panic!("will_open_not_durable: {result:?}");
    };
    SessionGuard {
        session,
        opened_by: Some(op),
    }
}
fn publication(
    world: &World<impl DurableStore>,
    will: mqttiumd_contract::ids::OperationId,
    seq: u64,
) -> Command {
    let op = operation_id(world.epoch, seq);
    let p = publication_id(world.epoch, seq);
    let mut targets = BoundedVec::with_capacity(2).unwrap();
    for (index, session) in [world.b, world.c].into_iter().enumerate() {
        targets
            .try_push(Target {
                session,
                session_revision: Revision::new(1),
                delivery: DeliveryId {
                    publication: p,
                    index: index as u32,
                },
                effective_qos: Qos::AtLeastOnce,
                cause: DeliveryCause::LiveExact,
            })
            .unwrap();
    }
    Command {
        recipe: FrozenRecipe::freeze(Recipe::WillPublication(WillPublication {
            operation: op,
            will,
            publication: p,
            route_revision: world.store.snapshot().unwrap().route_revision,
            topic: bytes(b"p1/topic"),
            payload: PayloadRef {
                id: PayloadId::new(
                    op.incarnation(),
                    op.epoch(),
                    op.owner(),
                    PayloadSeq::new(seq),
                ),
                length: PayloadBytes::new(7),
            },
            costs: Costs {
                payload_bytes: PayloadBytes::new(7),
                references: References::new(2),
                exchange_slots: ExchangeSlots::ZERO,
                receipt_slots: ReceiptSlots::new(1),
            },
            targets,
        }))
        .unwrap(),
        payload: Some(bytes(b"offline")),
    }
}
fn lifecycle_and_independent_responsibilities(mut world: World<impl DurableStore>) {
    let guard = establish(
        &mut world.store,
        world.epoch,
        10,
        SessionExpiry::OnDisconnect,
    );
    let armed = world.store.snapshot().unwrap();
    assert_eq!(armed.wills.len(), 1);
    assert_eq!(
        armed.wills[&guard.opened_by.unwrap()].state,
        WillState::Armed
    );
    assert!(
        world
            .store
            .read_wills(None, 4, ReadBudget::PAGE)
            .unwrap()
            .items
            .is_empty()
    );
    let end = command(
        operation_id(world.epoch, 11),
        SessionAction::EndWill {
            guard,
            expiry: SessionExpiry::OnDisconnect,
            observed_at: 0,
            publish: true,
        },
    );
    assert!(world.store.apply(&end).committed_now);
    let pending = world.store.snapshot().unwrap();
    assert!(!pending.sessions.contains_key(&guard.session));
    assert_eq!(
        pending.wills[&guard.opened_by.unwrap()].state,
        WillState::Pending,
        "will_disappeared_with_source"
    );
    let publish = publication(&world, guard.opened_by.unwrap(), 12);
    let applied = world.store.apply(&publish);
    assert!(
        applied.committed_now,
        "will_not_admitted_after_source_erased: {applied:?}"
    );
    let admitted = world.store.snapshot().unwrap();
    assert!(admitted.wills.is_empty());
    assert_eq!(admitted.obligations.len(), 2);
    assert_eq!(admitted.payloads.values().next().unwrap().references, 2);
    assert_eq!(accounts_match_what_is_held(&admitted), Ok(()));
    assert_eq!(world.store.apply(&publish).reply, applied.reply);
    assert_eq!(
        world.store.snapshot().unwrap(),
        admitted,
        "lost_result_repeated_will_admission"
    );
    let again = publication(&world, guard.opened_by.unwrap(), 13);
    assert!(
        !world.store.apply(&again).committed_now,
        "consumed_intent_recreated_permission"
    );
    assert_eq!(world.store.snapshot().unwrap().obligations.len(), 2);
    assert_eq!(
        world
            .store
            .admin(AdminCommand::DeleteSession { session: world.b }),
        Ok(AdminDone::SessionDeleted)
    );
    let held = world.store.snapshot().unwrap();
    assert_eq!(
        held.obligations.len(),
        1,
        "other_destination_lost_with_session"
    );
    assert_eq!(held.payloads.values().next().unwrap().references, 1);
    let delivery = *held.obligations.keys().next().unwrap();
    let claim = operation_id(world.epoch, 14);
    let packet_id = OutboundPacketId::new(1).unwrap();
    let operation_command = |recipe| Command {
        recipe: FrozenRecipe::freeze(recipe).unwrap(),
        payload: None,
    };
    assert!(
        world
            .store
            .apply(&operation_command(Recipe::ClaimDelivery(ClaimDelivery {
                operation: claim,
                delivery,
                target: world.c,
                target_revision: Revision::new(1),
                packet_id
            })))
            .committed_now
    );
    let finish = operation_command(Recipe::FinishDelivery(FinishDelivery {
        operation: operation_id(world.epoch, 15),
        delivery,
        target: world.c,
        target_revision: Revision::new(1),
        packet_id,
        claim,
    }));
    assert!(world.store.apply(&finish).committed_now);
    let ended = world.store.snapshot().unwrap();
    assert!(ended.obligations.is_empty() && ended.payloads.is_empty());
    assert_eq!(ended.accounts.payload_bytes.get(), 0);
    assert_eq!(ended.accounts.references.get(), 0);
    let repeated = world.store.apply(&finish);
    assert!(!repeated.committed_now);
    assert_eq!(
        world.store.snapshot().unwrap(),
        ended,
        "finish_double_released_will_payload"
    );
}
#[test]
fn sqlite_and_memory_will_survive_source_deletion_and_lost_result() {
    let d = Directory::new();
    let db = d.db();
    lifecycle_and_independent_responsibilities(World::new(&|i, l| {
        SqliteStore::create(&db, i, l).unwrap()
    }));
    lifecycle_and_independent_responsibilities(World::new(&MemoryStore::create));
}
fn startup_and_normal_cancel(mut world: World<impl DurableStore>) {
    let guard = establish(
        &mut world.store,
        world.epoch,
        10,
        SessionExpiry::OnDisconnect,
    );
    world.epoch = open(&mut world.store);
    let pending = world.store.snapshot().unwrap();
    assert!(!pending.sessions.contains_key(&guard.session));
    assert_eq!(
        pending.wills[&guard.opened_by.unwrap()].state,
        WillState::Pending,
        "will_forgotten_on_restart"
    );
    assert!(
        world
            .store
            .apply(&publication(&world, guard.opened_by.unwrap(), 1))
            .committed_now
    );
    let newer = establish(&mut world.store, world.epoch, 2, SessionExpiry::Never);
    assert!(
        world
            .store
            .apply(&command(
                operation_id(world.epoch, 3),
                SessionAction::EndWill {
                    guard: newer,
                    expiry: SessionExpiry::Never,
                    observed_at: 0,
                    publish: false
                }
            ))
            .committed_now
    );
    assert!(
        world.store.snapshot().unwrap().wills.is_empty(),
        "normal_disconnect_published_will"
    );
    let state = world.store.snapshot().unwrap();
    assert_eq!(accounts_match_what_is_held(&state), Ok(()));
}
#[test]
fn both_stores_publish_after_restart_and_cancel_normal_disconnect() {
    let d = Directory::new();
    let db = d.db();
    startup_and_normal_cancel(World::new(&|i, l| SqliteStore::create(&db, i, l).unwrap()));
    startup_and_normal_cancel(World::new(&MemoryStore::create));
}
#[test]
fn invalid_will_state_is_typed_before_pending_selection() {
    let d = Directory::new();
    let db = d.db();
    let mut world = World::new(&|i, l| SqliteStore::create(&db, i, l).unwrap());
    establish(&mut world.store, world.epoch, 10, SessionExpiry::Never);
    let c = rusqlite::Connection::open(&db).unwrap();
    c.execute("UPDATE wills SET state=99", []).unwrap();
    assert_eq!(
        c.query_row("PRAGMA quick_check", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "ok"
    );
    assert_eq!(world.store.snapshot(), Err(ReadError::InvalidData));
    assert_eq!(
        world.store.read_wills(None, 4, ReadBudget::PAGE),
        Err(ReadError::InvalidData)
    );
    assert_eq!(
        world.store.has_will_for_client(b"will-source"),
        Err(ReadError::InvalidData)
    );
}
#[test]
fn will_page_checks_byte_budget_before_copy() {
    let mut s = MemoryStore::create(StoreIncarnation::new(1), fixture_limits());
    let epoch = open(&mut s);
    let guard = establish(&mut s, epoch, 1, SessionExpiry::Never);
    s.apply(&command(
        operation_id(epoch, 2),
        SessionAction::EndWill {
            guard,
            expiry: SessionExpiry::Never,
            observed_at: 0,
            publish: true,
        },
    ));
    assert_eq!(
        s.read_wills(
            None,
            1,
            ReadBudget {
                max_rows: 1,
                max_bytes: 100,
                max_field_bytes: 32768
            }
        ),
        Err(ReadError::LimitExceeded)
    );
}

fn canonical_pressure(mut store: impl DurableStore) {
    let epoch = open(&mut store);
    let payload = bytes(&vec![b'x'; 32768]);
    let mut expected_bytes = 0usize;
    let mut accepted = 0u64;
    let mut blocked = None;
    for n in 0..300u64 {
        let op = operation_id(epoch, 2 * n + 1);
        let request = command(
            op,
            SessionAction::OpenWillAt {
                client_id: bytes(format!("pressure-{n}").as_bytes()),
                expected: None,
                clean_start: false,
                expiry: SessionExpiry::OnDisconnect,
                now: 0,
                will: Some(WillSpec {
                    topic: bytes(b"p1/example"),
                    payload: payload.clone(),
                }),
            },
        );
        let mut canonical = Vec::new();
        request
            .recipe
            .get()
            .encode_canonical(&mut canonical)
            .unwrap();
        let size = canonical.len();
        let result = store.apply(&request);
        if expected_bytes
            .checked_add(size)
            .is_none_or(|n| n > 8 * 1024 * 1024)
        {
            assert_eq!(
                result.reply,
                StoreReply::Outcome(StoreOutcome::PendingOrUnknown),
                "large_receipt_budget_did_not_suspend"
            );
            assert!(
                !result.wrote,
                "budget_failure_created_large_rejection_receipt"
            );
            blocked = Some(op);
            break;
        }
        let StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::SessionLifecycle(
            SessionChange::Opened { session, .. },
        ))) = result.reply
        else {
            panic!("unexpected early pressure {n}: {result:?}");
        };
        expected_bytes += size;
        accepted += 1;
        let guard = SessionGuard {
            session,
            opened_by: Some(op),
        };
        assert!(
            store
                .apply(&command(
                    operation_id(epoch, 2 * n + 2),
                    SessionAction::EndWill {
                        guard,
                        expiry: SessionExpiry::OnDisconnect,
                        observed_at: 0,
                        publish: false
                    }
                ))
                .committed_now
        );
    }
    let blocked = blocked.expect("real large recipe ceiling reached");
    assert!(accepted >= 200 && accepted < 300);
    assert!(
        matches!(
            store.resolve_and_fence(blocked).outcome,
            StoreOutcome::RejectedCertain(_)
        ),
        "reserved_fence_could_not_progress"
    );
    let state = store
        .snapshot()
        .expect("accepted history remains within snapshot budget");
    assert!(state.wills.is_empty());
    assert!(state.sessions.is_empty());
    assert_eq!(state.accounts.payload_bytes.get(), 0);
    assert_eq!(state.accounts.references.get(), 0);
}
#[test]
fn both_stores_reserve_small_fence_progress_when_large_open_receipts_are_full() {
    let mut limits = fixture_limits();
    limits.max_payload_bytes = 32768;
    limits.receipt_slots = ReceiptSlots::new(8192);
    limits.max_sessions = 128;
    canonical_pressure(MemoryStore::create(StoreIncarnation::new(1), limits));
    let d = Directory::new();
    canonical_pressure(SqliteStore::create(&d.db(), StoreIncarnation::new(1), limits).unwrap());
}
