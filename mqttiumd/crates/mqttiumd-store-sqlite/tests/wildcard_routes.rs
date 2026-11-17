//! Durable wildcard subscription options. These are implementation checks,
//! independent of the network/system reception and its public clients.
use mqttiumd_contract::capacity::{
    BoundedVec, ExchangeSlots, PayloadBytes, ReceiptSlots, References,
};
use mqttiumd_contract::command::{
    Command, FrozenRecipe, OpenEpoch, OpeningAttempt, OpeningTicket, Recipe, StartupNonce,
    UpdateSubscriptions,
};
use mqttiumd_contract::effects::RecoveredFact;
use mqttiumd_contract::ids::{
    LogicalOwnerId, OperationId, OperationSeq, Revision, RunEpoch, SessionId, StoreIncarnation,
};
use mqttiumd_contract::packet::{Qos, SharedBytes};
use mqttiumd_contract::pubsub::{GrantedSubscriptionChange, SubscriptionOptions};
use mqttiumd_store_api::{AdminCommand, AdminDone, DurableStore, Limits};
use mqttiumd_store_sqlite::SqliteStore;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "mqttiumd-wildcard-store-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
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
fn limits() -> Limits {
    Limits {
        payload_bytes: PayloadBytes::new(4096),
        references: References::new(16),
        exchange_slots: ExchangeSlots::new(4),
        receipt_slots: ReceiptSlots::new(8192),
        max_targets_per_admission: 16,
        max_topic_bytes: 256,
        max_payload_bytes: 1024,
        max_sessions: 8,
        max_route_targets: 16,
    }
}
fn setup(path: &Path) -> (SqliteStore, SessionId) {
    let mut store = SqliteStore::create(path, StoreIncarnation::new(1), limits()).unwrap();
    let AdminDone::SessionCreated { session, .. } = store
        .admin(AdminCommand::CreateSession {
            client_id: b"subscriber".to_vec(),
        })
        .unwrap()
    else {
        panic!("session")
    };
    store
        .open_epoch(&OpenEpoch {
            attempt: OpeningAttempt {
                startup: StartupNonce::new([1; 16]),
                ticket: OpeningTicket::new(1),
            },
            incarnation: StoreIncarnation::new(1),
            after: RunEpoch::new(0),
        })
        .unwrap();
    (store, session)
}
fn update(
    session: SessionId,
    sequence: u64,
    route_revision: Revision,
    filter: &[u8],
    granted: Option<Qos>,
    options: u8,
) -> Command {
    Command {
        recipe: FrozenRecipe::freeze(Recipe::UpdateSubscriptions(UpdateSubscriptions {
            operation: OperationId::new(
                StoreIncarnation::new(1),
                RunEpoch::new(1),
                LogicalOwnerId::new(0),
                OperationSeq::new(sequence),
            ),
            session,
            session_revision: Revision::new(1),
            route_revision,
            changes: BoundedVec::from_slice(
                &[GrantedSubscriptionChange {
                    topic: SharedBytes::copy_from(filter, 256).unwrap(),
                    granted,
                    options: SubscriptionOptions::from_bits(options).unwrap(),
                }],
                8,
            )
            .unwrap(),
        }))
        .unwrap(),
        payload: None,
    }
}
#[test]
fn wildcard_options_are_committed_recovered_and_replaced_without_history() {
    let directory = Directory::new();
    let (mut store, session) = setup(&directory.db());
    let command = update(
        session,
        1,
        Revision::new(0),
        b"sensor/+",
        Some(Qos::AtLeastOnce),
        4,
    );
    assert!(store.apply(&command).committed_now);
    let state = store.snapshot().unwrap();
    assert_eq!(
        state.routes[b"sensor/+".as_slice()][0].options.bits(),
        4,
        "subscription_options_were_not_durable"
    );
    let recovered = store.recover(None, 8).unwrap();
    assert!(recovered.facts.as_slice().iter().any(|fact|matches!(fact,RecoveredFact::Route {topic,options,..} if topic.as_slice()==b"sensor/+" && options.bits()==4)));
    drop(store);
    let mut store = SqliteStore::open(&directory.db()).unwrap();
    assert_eq!(
        store.snapshot().unwrap().routes[b"sensor/+".as_slice()][0]
            .options
            .bits(),
        4
    );
    assert!(
        !store.apply(&command).committed_now,
        "same operation remains idempotent after reopen"
    );
    for (sequence, bits, qos) in [
        (2, 44, Qos::AtMostOnce),
        (3, 0, Qos::AtLeastOnce),
        (4, 20, Qos::AtLeastOnce),
    ] {
        let revision = store.snapshot().unwrap().route_revision;
        assert!(
            store
                .apply(&update(
                    session,
                    sequence,
                    revision,
                    b"sensor/+",
                    Some(qos),
                    bits
                ))
                .committed_now
        );
        let state = store.snapshot().unwrap();
        assert_eq!(state.routes.len(), 1);
        assert_eq!(state.routes[b"sensor/+".as_slice()].len(), 1);
        assert_eq!(state.routes[b"sensor/+".as_slice()][0].options.bits(), bits);
        assert_eq!(state.routes[b"sensor/+".as_slice()][0].qos, qos);
    }
}

fn old_obligations_and_uncertain_operations_keep_their_admitted_routing<S: DurableStore>(
    make: &dyn Fn(StoreIncarnation, Limits) -> S,
) {
    use mqttiumd_contract::outcome::{StoreReply, StoreViolation};
    use mqttiumd_lab::store::conformance::{TOPIC, World};
    let mut world = World::with_limits(make, limits());
    let admission = world.admission(1, 7, b"before-replacement", &[world.b, world.c]);
    assert!(world.store.apply(&admission).committed_now);
    let before = world.store.snapshot().unwrap();
    let change = update(
        world.b,
        2,
        before.route_revision,
        TOPIC,
        Some(Qos::AtMostOnce),
        4,
    );
    // Deliberately discard the real terminal reply. Resolve must use this
    // operation and the frozen cut, rather than preparing a new route change.
    assert!(world.store.apply(&change).committed_now);
    let operation = change.recipe.get().operation();
    let known = world.store.resolve_and_fence(operation).outcome;
    assert_eq!(world.store.apply(&change).reply, StoreReply::Outcome(known));
    let after = world.store.snapshot().unwrap();
    assert_eq!(
        after.obligations, before.obligations,
        "subscription replacement cannot rewrite admitted QoS or recipient identity"
    );
    assert_eq!(after.payloads, before.payloads);
    assert_eq!(after.inbound, before.inbound);
    assert_eq!(after.outbound, before.outbound);
    assert_eq!(after.publications, before.publications);
    assert_eq!(
        after.routes[TOPIC]
            .iter()
            .find(|target| target.session == world.b)
            .unwrap()
            .qos,
        Qos::AtMostOnce
    );
    assert_eq!(
        after.routes[TOPIC]
            .iter()
            .find(|target| target.session == world.b)
            .unwrap()
            .options
            .bits(),
        4
    );
    let mismatch = update(
        world.b,
        2,
        before.route_revision,
        TOPIC,
        Some(Qos::AtMostOnce),
        8,
    );
    assert_eq!(
        world.store.apply(&mismatch).reply,
        StoreReply::Violation(StoreViolation::RecipeMismatch)
    );
    assert_eq!(world.store.snapshot().unwrap(), after);
    assert!(!world.store.apply(&admission).committed_now);
    assert_eq!(
        world.store.snapshot().unwrap(),
        after,
        "an old admission result never recalculates No Local or the newer projection"
    );
}

#[test]
fn memory_options_do_not_rewrite_old_obligations_or_unknown_operations() {
    old_obligations_and_uncertain_operations_keep_their_admitted_routing(
        &mqttiumd_lab::store::durable::MemoryStore::create,
    );
}
#[test]
fn sqlite_options_do_not_rewrite_old_obligations_or_unknown_operations() {
    let directory = Directory::new();
    old_obligations_and_uncertain_operations_keep_their_admitted_routing(&|incarnation, limits| {
        SqliteStore::create(&directory.db(), incarnation, limits).unwrap()
    });
}

#[test]
fn invalid_sqlite_filter_or_options_is_typed_before_routing_can_hide_it() {
    use mqttiumd_store_api::{ReadError, RecoveryRefused};
    for options in [-1, 1, 3, 48, 64, 256] {
        let directory = Directory::new();
        let (mut store, session) = setup(&directory.db());
        assert!(
            store
                .apply(&update(
                    session,
                    1,
                    Revision::new(0),
                    b"sensor/+",
                    Some(Qos::AtLeastOnce),
                    4
                ))
                .committed_now
        );
        let raw = rusqlite::Connection::open(directory.db()).unwrap();
        raw.execute("UPDATE routes SET options=?1", [options])
            .unwrap();
        assert_eq!(
            raw.query_row("PRAGMA quick_check", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
        assert!(
            matches!(store.snapshot(), Err(ReadError::InvalidData)),
            "invalid options {options} must not become a default route"
        );
        assert_eq!(
            store.recover(None, 8),
            Err(RecoveryRefused::Read(ReadError::InvalidData))
        );
        assert!(matches!(
            SqliteStore::inspect(&directory.db()),
            Err(ReadError::InvalidData)
        ));
    }
    for filter in [
        b"a#".as_slice(),
        b"a/++",
        b"",
        b"bad\0",
        b"\xff",
        b"$share/group/a",
    ] {
        let directory = Directory::new();
        let (mut store, session) = setup(&directory.db());
        assert!(
            store
                .apply(&update(
                    session,
                    1,
                    Revision::new(0),
                    b"sensor/+",
                    Some(Qos::AtLeastOnce),
                    0
                ))
                .committed_now
        );
        let raw = rusqlite::Connection::open(directory.db()).unwrap();
        raw.execute("UPDATE routes SET topic=?1", [filter]).unwrap();
        assert_eq!(
            raw.query_row("PRAGMA quick_check", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
        assert!(
            matches!(store.snapshot(), Err(ReadError::InvalidData)),
            "invalid filter must not become an unmatched route"
        );
        assert_eq!(
            store.recover(None, 8),
            Err(RecoveryRefused::Read(ReadError::InvalidData))
        );
        assert!(matches!(
            SqliteStore::inspect(&directory.db()),
            Err(ReadError::InvalidData)
        ));
    }
}

#[test]
fn invalid_memory_filter_is_typed_instead_of_becoming_an_unmatched_route() {
    use mqttiumd_lab::store::conformance::World;
    use mqttiumd_lab::store::durable::MemoryStore;
    use mqttiumd_store_api::{ReadError, RecoveryRefused};
    let world = World::with_limits(&MemoryStore::create, limits());
    let mut state = world.store.snapshot().unwrap();
    let targets = state.routes.pop_first().unwrap().1;
    state.routes.insert(b"a#".to_vec(), targets);
    let store = MemoryStore::reopen(state);
    assert!(matches!(store.snapshot(), Err(ReadError::InvalidData)));
    assert_eq!(
        store.recover(None, 8),
        Err(RecoveryRefused::Read(ReadError::InvalidData))
    );
}

#[test]
fn schema_six_is_refused_before_any_existing_content_is_modified() {
    use mqttiumd_store_sqlite::OpenError;
    let directory = Directory::new();
    let (store, _) = setup(&directory.db());
    drop(store);
    let raw = rusqlite::Connection::open(directory.db()).unwrap();
    raw.pragma_update(None, "user_version", 6).unwrap();
    raw.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    drop(raw);
    let before = std::fs::read(directory.db()).unwrap();
    assert!(matches!(
        SqliteStore::open(&directory.db()),
        Err(OpenError::SchemaVersion(6))
    ));
    assert_eq!(std::fs::read(directory.db()).unwrap(), before);
}
