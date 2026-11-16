//! Store-only session lifecycle regressions. The separate product reception
//! exercises public MQTT clients, connections and broker process lifetimes.
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use mqttiumd_contract::capacity::ReceiptSlots;
use mqttiumd_contract::command::{ClaimDelivery, Command, FinishDelivery, FrozenRecipe, Recipe};
use mqttiumd_contract::ids::{
    DeliveryId, OperationId, OperationSeq, OutboundPacketId, Revision, SessionId, StoreIncarnation,
};
use mqttiumd_contract::outcome::{
    CommitFacts, RejectReason, StoreOutcome, StoreReply, StoreViolation,
};
use mqttiumd_contract::packet::SharedBytes;
use mqttiumd_contract::receipts::{
    ReceiptClosure, ReceiptCollection, ReceiptCollectionReply, ReceiptOrigin,
};
use mqttiumd_contract::session::{
    SessionAction, SessionCancellations, SessionChange, SessionEndReason, SessionExpiry,
    SessionGuard, SessionLifecycle,
};
use mqttiumd_lab::store::conformance::{
    World, accounts_match_what_is_held, altered, fixture_limits, open, opening_command,
    operation_id, publication_id,
};
use mqttiumd_lab::store::durable::MemoryStore;
use mqttiumd_store_api::{
    AdminCommand, AdminDone, Applied, Durability, DurableState, DurableStore, ReadBudget,
    ReadError, RecoveryRefused, SessionState,
};
use mqttiumd_store_sqlite::{OpenError, SqliteStore};

const CYCLES: u64 = 512;
static NEXT: AtomicU64 = AtomicU64::new(0);

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "mqttiumd-session-store-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).expect("fresh session test directory");
        Self(path)
    }

    fn database(&self) -> PathBuf {
        self.0.join("store.sqlite")
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn completed_cycles_have_no_historical_sessions(mut store: impl DurableStore) {
    for number in 1..=CYCLES {
        let client_id = format!("ephemeral-{number:04}").into_bytes();
        let Ok(AdminDone::SessionCreated { session, .. }) =
            store.admin(AdminCommand::CreateSession { client_id })
        else {
            panic!("the active-session quota must permit cycle {number}");
        };
        assert_eq!(session.get(), number, "session identities never repeat");
        assert_eq!(
            store.admin(AdminCommand::DeleteSession { session }),
            Ok(AdminDone::SessionDeleted),
            "cycle {number} must reach terminal store knowledge"
        );
    }
    let state = store.snapshot().expect("healthy complete logical snapshot");
    assert_eq!(state.limits.max_sessions, 128, "the quota stays unchanged");
    assert_eq!(state.next_session.get(), CYCLES + 1);
    assert_eq!(accounts_match_what_is_held(&state), Ok(()));
    let terminal = state
        .sessions
        .values()
        .filter(|record| record.state == SessionState::Terminal)
        .count();
    assert!(
        state.sessions.is_empty() && terminal == 0,
        "session_terminal_history_grew: {CYCLES} complete cycles retain {} session rows, including {terminal} terminal rows",
        state.sessions.len()
    );
    let bounded = store
        .snapshot_with_budget(ReadBudget {
            max_rows: 129,
            max_bytes: 8 * 1024,
            max_field_bytes: 1024,
        })
        .expect("historical sessions cannot exhaust a live-sized read budget");
    assert_eq!(bounded, state);
}

fn sqlite(path: &Path) -> SqliteStore {
    let mut limits = fixture_limits();
    limits.max_sessions = 128;
    SqliteStore::create(path, StoreIncarnation::new(1), limits).expect("new SQLite store")
}

#[test]
fn memory_session_cycles_do_not_retain_terminal_history() {
    let mut limits = fixture_limits();
    limits.max_sessions = 128;
    completed_cycles_have_no_historical_sessions(MemoryStore::create(
        StoreIncarnation::new(1),
        limits,
    ));
}

#[test]
fn sqlite_session_cycles_do_not_retain_terminal_history() {
    let directory = Directory::new();
    completed_cycles_have_no_historical_sessions(sqlite(&directory.database()));
}

fn command(recipe: Recipe) -> Command {
    Command {
        recipe: FrozenRecipe::freeze(recipe).expect("valid lifecycle recipe"),
        payload: None,
    }
}

fn lifecycle(operation: OperationId, action: SessionAction) -> Command {
    command(Recipe::SessionLifecycle(SessionLifecycle {
        operation,
        action,
    }))
}

fn opening(
    operation: OperationId,
    name: &[u8],
    expected: Option<SessionGuard>,
    clean_start: bool,
    expiry: SessionExpiry,
) -> Command {
    lifecycle(
        operation,
        SessionAction::Open {
            client_id: SharedBytes::copy_from(name, 1024).unwrap(),
            expected,
            clean_start,
            expiry,
        },
    )
}

fn closing(operation: OperationId, guard: SessionGuard, reason: SessionEndReason) -> Command {
    lifecycle(operation, SessionAction::Close { guard, reason })
}

struct Pair {
    directory: Directory,
    sql: World<SqliteStore>,
    mem: World<MemoryStore>,
}

impl Pair {
    fn new() -> Self {
        let directory = Directory::new();
        let path = directory.database();
        let mut limits = fixture_limits();
        limits.max_sessions = 128;
        limits.receipt_slots = ReceiptSlots::new(8192);
        let sql = World::with_limits(&|i, l| SqliteStore::create(&path, i, l).unwrap(), limits);
        let mem = World::with_limits(&MemoryStore::create, limits);
        Self {
            directory,
            sql,
            mem,
        }
    }

    fn apply(&mut self, command: &Command) -> Applied {
        let actual = self.sql.store.apply(command);
        assert_eq!(
            actual,
            self.mem.store.apply(command),
            "both stores answer identically"
        );
        self.state();
        actual
    }

    fn state(&self) -> DurableState {
        let state = self.sql.store.snapshot().unwrap();
        assert_eq!(
            state,
            self.mem.store.snapshot().unwrap(),
            "complete durable state matches"
        );
        assert_eq!(accounts_match_what_is_held(&state), Ok(()));
        state
    }

    fn open_epoch(&mut self) {
        assert_eq!(open(&mut self.sql.store), open(&mut self.mem.store));
        self.state();
    }

    fn collect(&mut self, closure: ReceiptClosure) {
        let collection = ReceiptCollection {
            closure,
            limit: 128,
        };
        let actual = self.sql.store.collect_receipts(collection).unwrap();
        assert_eq!(actual, self.mem.store.collect_receipts(collection).unwrap());
        assert!(matches!(actual, ReceiptCollectionReply::Applied { .. }));
        self.state();
    }

    fn reopen(self) -> Self {
        let before = self.state();
        let World {
            store,
            producer,
            b,
            c,
            epoch,
            route_revision,
        } = self.sql;
        // Release the unique writer before opening the very same database.
        drop(store);
        let reopened = SqliteStore::open(&self.directory.database()).unwrap();
        assert_eq!(reopened.snapshot().unwrap(), before);
        Self {
            directory: self.directory,
            sql: World {
                store: reopened,
                producer,
                b,
                c,
                epoch,
                route_revision,
            },
            mem: self.mem,
        }
    }
}

fn expect_open(applied: Applied) -> (SessionId, bool) {
    let StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::SessionLifecycle(
        SessionChange::Opened {
            session,
            session_present,
            ..
        },
    ))) = applied.reply
    else {
        panic!("open did not commit: {applied:?}");
    };
    (session, session_present)
}

fn rejected(applied: Applied, reason: RejectReason) {
    assert_eq!(
        applied.reply,
        StoreReply::Outcome(StoreOutcome::RejectedCertain(reason))
    );
    assert!(!applied.committed_now);
}

#[test]
fn lifecycle_replay_and_attachment_cas_preserve_business_state() {
    let mut p = Pair::new();
    let op = |n| operation_id(p.mem.epoch, n);
    let first_op = op(1);
    let first = opening(first_op, b"new", None, false, SessionExpiry::Never);
    let (session, present) = expect_open(p.apply(&first));
    assert!(!present);
    let replay = p.apply(&first);
    assert!(!replay.committed_now && !replay.wrote);
    let mismatch = opening(first_op, b"different", None, false, SessionExpiry::Never);
    assert_eq!(
        p.apply(&mismatch).reply,
        StoreReply::Violation(StoreViolation::RecipeMismatch)
    );
    let old = SessionGuard {
        session,
        opened_by: Some(first_op),
    };
    let resume_op = operation_id(p.mem.epoch, 2);
    let (resumed, present) = expect_open(p.apply(&opening(
        resume_op,
        b"new",
        Some(old),
        false,
        SessionExpiry::OnDisconnect,
    )));
    assert_eq!(resumed, session);
    assert!(
        present,
        "zero expiry changes retention after resuming existing state"
    );
    let state = p.state();
    assert_eq!(state.sessions[&session].revision, Revision::new(1));
    assert_eq!(state.sessions[&session].expiry, SessionExpiry::OnDisconnect);
    assert_eq!(state.sessions[&session].opened_by, Some(resume_op));
    rejected(
        p.apply(&closing(
            operation_id(p.mem.epoch, 3),
            old,
            SessionEndReason::TransportLost,
        )),
        RejectReason::PreconditionFailed,
    );
    assert!(
        p.state().sessions.contains_key(&session),
        "old transport cannot close a newer attachment"
    );
    let current = SessionGuard {
        session,
        opened_by: Some(resume_op),
    };
    assert!(
        p.apply(&closing(
            operation_id(p.mem.epoch, 4),
            current,
            SessionEndReason::TransportLost
        ))
        .committed_now
    );
    assert!(!p.state().sessions.contains_key(&session));
    let (next, present) = expect_open(p.apply(&opening(
        operation_id(p.mem.epoch, 5),
        b"new",
        None,
        false,
        SessionExpiry::Never,
    )));
    assert!(next > session && !present);
    let guard = SessionGuard {
        session: next,
        opened_by: Some(operation_id(p.mem.epoch, 5)),
    };
    rejected(
        p.apply(&closing(
            operation_id(p.mem.epoch, 6),
            guard,
            SessionEndReason::TransportLost,
        )),
        RejectReason::PreconditionFailed,
    );
    assert!(
        p.apply(&closing(
            operation_id(p.mem.epoch, 7),
            guard,
            SessionEndReason::Disconnect
        ))
        .committed_now
    );
    p.reopen();
}

#[test]
fn replacement_at_the_full_session_quota_reuses_only_its_own_slot() {
    let mut p = Pair::new();
    for n in 3..128 {
        let command = AdminCommand::CreateSession {
            client_id: format!("full-{n}").into_bytes(),
        };
        assert_eq!(
            p.sql.store.admin(command.clone()),
            p.mem.store.admin(command)
        );
    }
    assert_eq!(p.state().sessions.len(), 128);
    let denied = opening(
        operation_id(p.mem.epoch, 1),
        b"overflow",
        None,
        false,
        SessionExpiry::Never,
    );
    rejected(p.apply(&denied), RejectReason::SessionCapacityExceeded);
    let old_b = p.mem.b;
    let old = SessionGuard {
        session: old_b,
        opened_by: None,
    };
    let replace_op = operation_id(p.mem.epoch, 2);
    let replacement = opening(replace_op, b"p1-b", Some(old), true, SessionExpiry::Never);
    let (new_b, present) = expect_open(p.apply(&replacement));
    assert!(!present && new_b > old_b);
    assert_eq!(p.state().sessions.len(), 128);
    assert!(!p.state().sessions.contains_key(&old_b));
    // The other offline destination and its exact route are conserved.
    assert!(p.state().sessions.contains_key(&p.mem.c));
    assert!(
        p.state()
            .routes
            .values()
            .any(|targets| targets.iter().any(|target| target.session == p.mem.c))
    );
    assert!(
        p.apply(&closing(
            operation_id(p.mem.epoch, 3),
            SessionGuard {
                session: new_b,
                opened_by: Some(replace_op)
            },
            SessionEndReason::Disconnect
        ))
        .committed_now
    );
    let (admitted, _) = expect_open(p.apply(&opening(
        operation_id(p.mem.epoch, 4),
        b"after-slot-free",
        None,
        false,
        SessionExpiry::Never,
    )));
    assert!(admitted > new_b);
    rejected(p.apply(&denied), RejectReason::SessionCapacityExceeded);
    assert_eq!(p.state().sessions.len(), 128);
    p.reopen();
}

#[test]
fn loaded_termination_cancels_only_its_target_and_keeps_shared_payloads() {
    let mut p = Pair::new();
    let first = p.mem.admission(1, 7, b"shared", &[p.mem.b, p.mem.c]);
    assert!(p.apply(&first).committed_now);
    let delivery = DeliveryId {
        publication: publication_id(p.mem.epoch, 1),
        index: 0,
    };
    let claim = command(Recipe::ClaimDelivery(ClaimDelivery {
        operation: operation_id(p.mem.epoch, 2),
        delivery,
        target: p.mem.b,
        target_revision: Revision::new(1),
        packet_id: OutboundPacketId::new(7).unwrap(),
    }));
    assert!(p.apply(&claim).committed_now);
    let produced_by_b = altered(
        &p.mem.admission(3, 8, b"producer-survives", &[p.mem.c]),
        |admit| {
            admit.source.session = p.mem.b;
            admit.source.exchange.session = p.mem.b;
        },
    );
    assert!(p.apply(&produced_by_b).committed_now);
    let close = closing(
        operation_id(p.mem.epoch, 4),
        SessionGuard {
            session: p.mem.b,
            opened_by: None,
        },
        SessionEndReason::Disconnect,
    );
    let closed = p.apply(&close);
    assert_eq!(
        closed.reply,
        StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::SessionLifecycle(
            SessionChange::Closed {
                session: p.mem.b,
                route_revision: Revision::new(3),
                cancelled: SessionCancellations {
                    inbound: 1,
                    deliveries: 1,
                    routes: 1
                },
            }
        )))
    );
    let state = p.state();
    assert_eq!(state.obligations.len(), 2);
    assert!(
        state
            .obligations
            .values()
            .all(|obligation| obligation.target == p.mem.c)
    );
    assert!(state.outbound.is_empty());
    assert_eq!(
        state.inbound.len(),
        1,
        "the other producer's exchange remains"
    );
    assert_eq!(state.payloads.len(), 2);
    assert!(
        state
            .payloads
            .values()
            .all(|payload| payload.references == 1)
    );
    assert_eq!(
        p.sql.store.read_work(p.mem.c, None, 8).unwrap().items.len(),
        2
    );
    let replay = p.apply(&claim);
    assert!(
        !replay.committed_now,
        "historical claim receipt cannot recreate a cancelled responsibility"
    );
    let stale = altered(
        &p.mem.admission(5, 9, b"must-not-admit", &[p.mem.c]),
        |admit| {
            admit.source.session = p.mem.b;
            admit.source.exchange.session = p.mem.b;
        },
    );
    rejected(p.apply(&stale), RejectReason::PreconditionFailed);
    p = p.reopen();
    for (claim_sequence, finish_sequence, admitted_as, index) in [(6, 7, 1, 1), (8, 9, 3, 0)] {
        let delivery = DeliveryId {
            publication: publication_id(p.mem.epoch, admitted_as),
            index,
        };
        let claim_op = operation_id(p.mem.epoch, claim_sequence);
        assert!(
            p.apply(&command(Recipe::ClaimDelivery(ClaimDelivery {
                operation: claim_op,
                delivery,
                target: p.mem.c,
                target_revision: Revision::new(1),
                packet_id: OutboundPacketId::new(7).unwrap(),
            })))
            .committed_now
        );
        assert!(
            p.apply(&command(Recipe::FinishDelivery(FinishDelivery {
                operation: operation_id(p.mem.epoch, finish_sequence),
                delivery,
                target: p.mem.c,
                target_revision: Revision::new(1),
                packet_id: OutboundPacketId::new(7).unwrap(),
                claim: claim_op,
            })))
            .committed_now
        );
    }
    assert!(p.apply(&p.mem.release(10, 7, 1)).committed_now);
    let drained = p.state();
    assert!(
        drained.obligations.is_empty() && drained.outbound.is_empty() && drained.inbound.is_empty()
    );
    assert!(drained.publications.is_empty() && drained.payloads.is_empty());
    assert_eq!(drained.accounts.payload_bytes.get(), 0);
    assert_eq!(drained.accounts.references.get(), 0);
    assert_eq!(drained.accounts.exchange_slots.get(), 0);
}

#[test]
fn only_an_actual_epoch_open_expires_zero_sessions_and_commit_failure_is_atomic() {
    let mut p = Pair::new();
    let policy = opening(
        operation_id(p.mem.epoch, 1),
        b"p1-b",
        Some(SessionGuard {
            session: p.mem.b,
            opened_by: None,
        }),
        false,
        SessionExpiry::OnDisconnect,
    );
    let (b, present) = expect_open(p.apply(&policy));
    assert!(present && b == p.mem.b);
    assert!(
        p.apply(&p.mem.admission(2, 7, b"shared", &[p.mem.b, p.mem.c]))
            .committed_now
    );
    let previous = p.state();
    p.sql.store.set_durability(Durability::Failing);
    p.mem.store.set_durability(Durability::Failing);
    let epoch_opening = opening_command(&p.sql.store);
    assert_eq!(
        p.sql.store.open_epoch(&epoch_opening),
        p.mem.store.open_epoch(&epoch_opening)
    );
    assert_eq!(
        p.state(),
        previous,
        "failed startup cannot partly erase an ephemeral destination"
    );
    p.sql.store.set_durability(Durability::Available);
    p.mem.store.set_durability(Durability::Available);
    p.open_epoch();
    let state = p.state();
    assert!(!state.sessions.contains_key(&p.mem.b));
    assert_eq!(state.obligations.len(), 1);
    assert_eq!(state.obligations.values().next().unwrap().target, p.mem.c);
    let epoch = state.ledger.last_opened();
    let new = opening(
        operation_id(epoch, 1),
        b"new-zero",
        None,
        false,
        SessionExpiry::OnDisconnect,
    );
    let (new_zero, _) = expect_open(p.apply(&new));
    assert_eq!(
        p.sql.store.open_epoch(&epoch_opening),
        p.mem.store.open_epoch(&epoch_opening)
    );
    assert!(
        p.state().sessions.contains_key(&new_zero),
        "AlreadyOpened is not a second startup cleanup"
    );
    p.reopen();
}

#[test]
fn invalid_policy_is_reported_before_empty_work_or_recovery_can_hide_it() {
    let directory = Directory::new();
    let path = directory.database();
    let mut store = sqlite(&path);
    let Ok(AdminDone::SessionCreated { session, .. }) = store.admin(AdminCommand::CreateSession {
        client_id: b"corrupt".to_vec(),
    }) else {
        panic!("session");
    };
    open(&mut store);
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute("UPDATE sessions SET expiry=99", []).unwrap();
    assert_eq!(
        conn.query_row("PRAGMA quick_check", [], |row| row.get::<_, String>(0))
            .unwrap(),
        "ok"
    );
    assert_eq!(store.snapshot(), Err(ReadError::InvalidData));
    assert_eq!(
        store.recover(None, 8),
        Err(RecoveryRefused::Read(ReadError::InvalidData))
    );
    assert_eq!(
        store.read_work(session, None, 8),
        Err(ReadError::InvalidData)
    );
    let before = store.ledger().unwrap();
    let command = opening_command(&store);
    assert_eq!(store.open_epoch(&command), Err(ReadError::InvalidData));
    assert_eq!(
        store.ledger().unwrap(),
        before,
        "typed cleanup refusal rolls back the epoch opening"
    );
}

#[test]
fn receipted_ephemeral_cycles_collect_without_reusing_session_or_attachment_identities() {
    let mut p = Pair::new();
    let origin = ReceiptOrigin::of(operation_id(p.mem.epoch, 1));
    let mut closure = ReceiptClosure::empty(origin);
    p.collect(closure);
    let first = opening(
        operation_id(p.mem.epoch, 1),
        b"cycle-1",
        None,
        false,
        SessionExpiry::OnDisconnect,
    );
    for number in 1..=512 {
        let open_op = operation_id(p.mem.epoch, number * 2 - 1);
        let open = if number == 1 {
            first.clone()
        } else {
            opening(
                open_op,
                format!("cycle-{number}").as_bytes(),
                None,
                false,
                SessionExpiry::OnDisconnect,
            )
        };
        let (session, present) = expect_open(p.apply(&open));
        assert!(!present);
        assert_eq!(session.get(), number + 3);
        assert!(
            p.apply(&closing(
                operation_id(p.mem.epoch, number * 2),
                SessionGuard {
                    session,
                    opened_by: Some(open_op)
                },
                SessionEndReason::TransportLost
            ))
            .committed_now
        );
        if number % 32 == 0 {
            closure.through = OperationSeq::new(number * 2);
            p.collect(closure);
            let state = p.state();
            assert_eq!(state.sessions.len(), 3);
            assert!(state.operations.is_empty());
            assert_eq!(state.accounts.receipt_slots.get(), 0);
            assert_eq!(state.receipt_frontiers.len(), 1);
        }
    }
    assert_eq!(p.state().next_session.get(), 516);
    p = p.reopen();
    assert_eq!(
        p.apply(&first).reply,
        StoreReply::Outcome(StoreOutcome::RetiredOutcomeUnavailable)
    );
    let altered_identity = opening(
        operation_id(p.mem.epoch, 1),
        b"not-the-old-client",
        None,
        true,
        SessionExpiry::Never,
    );
    assert_eq!(
        p.apply(&altered_identity).reply,
        StoreReply::Outcome(StoreOutcome::RetiredOutcomeUnavailable)
    );
    rejected(
        p.apply(&closing(
            operation_id(p.mem.epoch, 1025),
            SessionGuard {
                session: SessionId::new(4),
                opened_by: Some(operation_id(p.mem.epoch, 1)),
            },
            SessionEndReason::Disconnect,
        )),
        RejectReason::PreconditionFailed,
    );
    assert_eq!(p.state().sessions.len(), 3);
}

#[test]
fn schema_four_is_refused_without_modifying_the_existing_database() {
    let directory = Directory::new();
    let path = directory.database();
    drop(sqlite(&path));
    {
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection.pragma_update(None, "user_version", 4).unwrap();
        connection
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
    }
    let before = std::fs::read(&path).unwrap();
    assert!(matches!(
        SqliteStore::open(&path),
        Err(OpenError::SchemaVersion(4))
    ));
    assert_eq!(
        std::fs::read(&path).unwrap(),
        before,
        "an unsupported format is never reset or silently migrated"
    );
}
