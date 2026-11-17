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
    loaded_termination(false);
}

#[test]
fn finite_expiration_cancels_only_its_target_and_keeps_shared_payloads() {
    loaded_termination(true);
}

fn loaded_termination(finite: bool) {
    let mut p = Pair::new();
    let attachment = operation_id(p.mem.epoch, 20);
    if finite {
        checkpoint(&mut p, 8, 0);
        let previous = SessionGuard {
            session: p.mem.b,
            opened_by: None,
        };
        let (same, present) =
            expect_open(p.apply(&finite_open(attachment, b"p1-b", Some(previous), 1, 0)));
        assert_eq!(same, p.mem.b);
        assert!(present);
    }
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
    let close = if finite {
        let guard = SessionGuard {
            session: p.mem.b,
            opened_by: Some(attachment),
        };
        let detach = lifecycle(
            operation_id(p.mem.epoch, 21),
            SessionAction::Detach {
                guard,
                expiry: SessionExpiry::After(1),
                observed_at: 0,
            },
        );
        assert!(p.apply(&detach).committed_now);
        let detached = p.state().sessions[&p.mem.b].detached.unwrap();
        lifecycle(
            operation_id(p.mem.epoch, 4),
            SessionAction::Expire {
                guard,
                detached,
                now: 1_000_000_000,
            },
        )
    } else {
        closing(
            operation_id(p.mem.epoch, 4),
            SessionGuard {
                session: p.mem.b,
                opened_by: None,
            },
            SessionEndReason::Disconnect,
        )
    };
    let closed = p.sql.store.apply(&close);
    assert_eq!(closed, p.mem.store.apply(&close));
    assert_eq!(
        p.sql
            .store
            .snapshot()
            .unwrap()
            .obligations
            .values()
            .filter(|row| row.target == p.mem.c)
            .count(),
        2,
        "session_other_destination_cancelled: closing B must conserve both responsibilities towards C"
    );
    p.state();
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
    conn.execute("UPDATE sessions SET expiry=-1", []).unwrap();
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
    let first = store.recover(None, 3).expect("metadata prefix is coherent");
    assert!(first.next.is_some());
    assert_eq!(
        store.recover(first.next, 8),
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
fn schemas_four_and_five_are_refused_without_modifying_the_existing_database() {
    for version in [4, 5] {
        let directory = Directory::new();
        let path = directory.database();
        drop(sqlite(&path));
        {
            let connection = rusqlite::Connection::open(&path).unwrap();
            connection
                .pragma_update(None, "user_version", version)
                .unwrap();
            connection
                .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
                .unwrap();
        }
        let before = std::fs::read(&path).unwrap();
        assert!(
            matches!(SqliteStore::open(&path),Err(OpenError::SchemaVersion(actual)) if actual==version)
        );
        assert_eq!(
            std::fs::read(&path).unwrap(),
            before,
            "unsupported format is never reset or silently migrated"
        );
    }
}

// Finite time tests compare the complete deterministic/SQLite state. The
// injected domain and nanosecond observations never alter the host clock.
fn checkpoint(p: &mut Pair, domain: u8, tick: u64) -> mqttiumd_store_api::SessionClockAnchor {
    let expected = p.state().session_clock;
    let reading = mqttiumd_store_api::SessionClockReading {
        domain: [domain; 32],
        tick,
    };
    let actual = p
        .sql
        .store
        .checkpoint_session_clock(expected, reading)
        .unwrap();
    assert_eq!(
        actual,
        p.mem
            .store
            .checkpoint_session_clock(expected, reading)
            .unwrap()
    );
    p.state();
    actual
}

fn finite_open(
    operation: OperationId,
    name: &[u8],
    expected: Option<SessionGuard>,
    seconds: u32,
    now: u64,
) -> Command {
    lifecycle(
        operation,
        SessionAction::OpenAt {
            client_id: SharedBytes::copy_from(name, 1024).unwrap(),
            expected,
            clean_start: false,
            expiry: SessionExpiry::After(seconds),
            now,
        },
    )
}

#[test]
fn finite_detach_keeps_the_captured_deadline_and_expire_checks_the_exact_occurrence() {
    use mqttiumd_contract::session::SessionDetachment;
    let mut p = Pair::new();
    checkpoint(&mut p, 7, 100);
    let epoch = p.mem.epoch;
    let opening = operation_id(epoch, 1);
    let (session, _) = expect_open(p.apply(&finite_open(opening, b"finite", None, 1, 0)));
    let guard = SessionGuard {
        session,
        opened_by: Some(opening),
    };
    let detaching = operation_id(epoch, 2);
    let detach = lifecycle(
        detaching,
        SessionAction::Detach {
            guard,
            expiry: SessionExpiry::After(1),
            observed_at: 10,
        },
    );
    // The command's observation precedes its eventual commit. Advancing the
    // global checkpoint in between must not move this captured deadline.
    checkpoint(&mut p, 7, 800_000_100);
    let applied = p.apply(&detach);
    let detached = SessionDetachment {
        operation: detaching,
        deadline: Some(1_000_000_010),
    };
    assert_eq!(
        applied.reply,
        StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::SessionLifecycle(
            SessionChange::Detached {
                session,
                expiry: SessionExpiry::After(1),
                detached
            }
        )))
    );
    checkpoint(&mut p, 7, 900_000_100);
    assert_eq!(
        p.apply(&detach).reply,
        applied.reply,
        "same-operation resolve cannot rearm at commit or result time"
    );
    rejected(
        p.apply(&lifecycle(
            operation_id(epoch, 3),
            SessionAction::Detach {
                guard,
                expiry: SessionExpiry::After(1),
                observed_at: 900_000_000,
            },
        )),
        RejectReason::PreconditionFailed,
    );
    rejected(
        p.apply(&lifecycle(
            operation_id(epoch, 4),
            SessionAction::Expire {
                guard,
                detached,
                now: 1_000_000_009,
            },
        )),
        RejectReason::PreconditionFailed,
    );
    let expired = p.apply(&lifecycle(
        operation_id(epoch, 5),
        SessionAction::Expire {
            guard,
            detached,
            now: 1_000_000_010,
        },
    ));
    assert!(
        matches!(expired.reply,StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::SessionLifecycle(SessionChange::Closed {session:ended,..}))) if ended==session)
    );
    assert!(!p.state().sessions.contains_key(&session));
}

#[test]
fn open_at_the_deadline_does_not_depend_on_a_timer_pass() {
    let mut p = Pair::new();
    checkpoint(&mut p, 2, 0);
    let epoch = p.mem.epoch;
    let first = operation_id(epoch, 1);
    let (session, _) = expect_open(p.apply(&finite_open(first, b"deadline-open", None, 2, 0)));
    let guard = SessionGuard {
        session,
        opened_by: Some(first),
    };
    p.apply(&lifecycle(
        operation_id(epoch, 2),
        SessionAction::Detach {
            guard,
            expiry: SessionExpiry::After(2),
            observed_at: 1,
        },
    ));
    let resume = operation_id(epoch, 3);
    let (resumed, present) = expect_open(p.apply(&finite_open(
        resume,
        b"deadline-open",
        Some(guard),
        2,
        2_000_000_000,
    )));
    assert!(present);
    assert_eq!(resumed, session);
    let nextguard = SessionGuard {
        session,
        opened_by: Some(resume),
    };
    let detaching = operation_id(epoch, 4);
    p.apply(&lifecycle(
        detaching,
        SessionAction::Detach {
            guard: nextguard,
            expiry: SessionExpiry::After(2),
            observed_at: 2_000_000_000,
        },
    ));
    let old = p.state().sessions[&session].detached.unwrap();
    let (new, present) = expect_open(p.apply(&finite_open(
        operation_id(epoch, 5),
        b"deadline-open",
        Some(nextguard),
        2,
        4_000_000_000,
    )));
    assert!(!present);
    assert_ne!(new, session);
    rejected(
        p.apply(&lifecycle(
            operation_id(epoch, 6),
            SessionAction::Expire {
                guard: nextguard,
                detached: old,
                now: 5_000_000_000,
            },
        )),
        RejectReason::PreconditionFailed,
    );
    assert!(
        p.state().sessions.contains_key(&new),
        "old timer cannot delete a ClientID's new incarnation"
    );
}

#[test]
fn a_single_clock_anchor_preserves_same_domain_progress_and_reanchors_conservatively() {
    let mut p = Pair::new();
    let initial = checkpoint(&mut p, 1, 100);
    assert_eq!(initial.logical, 0);
    let advanced = checkpoint(&mut p, 1, 900);
    assert_eq!(advanced.logical, 800);
    let before = p.state();
    let reading = mqttiumd_store_api::SessionClockReading {
        domain: [1; 32],
        tick: 500,
    };
    assert_eq!(
        p.sql
            .store
            .checkpoint_session_clock(Some(advanced), reading),
        Err(ReadError::InvalidData)
    );
    assert_eq!(
        p.mem
            .store
            .checkpoint_session_clock(Some(advanced), reading),
        Err(ReadError::InvalidData)
    );
    assert_eq!(
        p.state(),
        before,
        "a backward clock cannot write or immediately expire"
    );
    let reading = mqttiumd_store_api::SessionClockReading {
        domain: [1; 32],
        tick: 950,
    };
    assert_eq!(
        p.sql.store.checkpoint_session_clock(Some(initial), reading),
        Ok(advanced)
    );
    assert_eq!(
        p.mem.store.checkpoint_session_clock(Some(initial), reading),
        Ok(advanced)
    );
    assert_eq!(
        p.state(),
        before,
        "stale or lost checkpoint replies cannot advance twice"
    );
    let changed = checkpoint(&mut p, 2, 1);
    assert_eq!(
        changed.logical, 800,
        "unknown downtime is not invented from a new origin"
    );
    let p = p.reopen();
    let mut p = p;
    assert_eq!(checkpoint(&mut p, 2, 101).logical, 900);
    assert_eq!(
        p.state().accounts.receipt_slots,
        before.accounts.receipt_slots,
        "clock maintenance has no receipt chain"
    );
}

#[test]
fn startup_detaches_attached_finite_once_but_never_rearms_an_existing_deadline() {
    let mut p = Pair::new();
    checkpoint(&mut p, 3, 0);
    let epoch = p.mem.epoch;
    let op = operation_id(epoch, 1);
    let (session, _) = expect_open(p.apply(&finite_open(op, b"attached-crash", None, 3, 0)));
    checkpoint(&mut p, 3, 1_000_000_000);
    let mut p = p.reopen();
    p.open_epoch();
    let first = p.state().sessions[&session].detached.unwrap();
    assert_eq!(first.deadline, Some(4_000_000_000));
    assert_eq!(
        first.operation, op,
        "startup detachment retains the unique lost attachment"
    );
    checkpoint(&mut p, 3, 2_000_000_000);
    let mut p = p.reopen();
    p.open_epoch();
    assert_eq!(p.state().sessions[&session].detached, Some(first));
    checkpoint(&mut p, 3, 4_000_000_000);
    let mut p = p.reopen();
    p.open_epoch();
    assert!(
        !p.state().sessions.contains_key(&session),
        "continuous boot time expires across repeated process lifetimes"
    );
}

#[test]
fn old_detachment_results_and_timers_cannot_rebind_a_resumed_attachment() {
    let mut p = Pair::new();
    checkpoint(&mut p, 4, 0);
    let epoch = p.mem.epoch;
    let first = operation_id(epoch, 1);
    let (session, _) = expect_open(p.apply(&finite_open(first, b"old-detachment", None, 1, 0)));
    let original = SessionGuard {
        session,
        opened_by: Some(first),
    };
    let detaching = operation_id(epoch, 2);
    let command = lifecycle(
        detaching,
        SessionAction::Detach {
            guard: original,
            expiry: SessionExpiry::After(1),
            observed_at: 10,
        },
    );
    let old_reply = p.apply(&command).reply;
    let old = p.state().sessions[&session].detached.unwrap();
    let resumed = operation_id(epoch, 3);
    let (same, present) = expect_open(p.apply(&finite_open(
        resumed,
        b"old-detachment",
        Some(original),
        1,
        20,
    )));
    assert!(present);
    assert_eq!(same, session);
    let before = p.state();
    assert_eq!(
        p.apply(&command).reply,
        old_reply,
        "a historical reply keeps its original detachment identity"
    );
    assert_eq!(
        p.state(),
        before,
        "idempotent old commands cannot detach the new attachment"
    );
    rejected(
        p.apply(&lifecycle(
            operation_id(epoch, 4),
            SessionAction::Expire {
                guard: original,
                detached: old,
                now: 2_000_000_000,
            },
        )),
        RejectReason::PreconditionFailed,
    );
    let latest = SessionGuard {
        session,
        opened_by: Some(resumed),
    };
    p.apply(&lifecycle(
        operation_id(epoch, 5),
        SessionAction::Detach {
            guard: latest,
            expiry: SessionExpiry::Never,
            observed_at: 30,
        },
    ));
    let never = p.state().sessions[&session].detached.unwrap();
    assert_eq!(never.deadline, None);
    rejected(
        p.apply(&lifecycle(
            operation_id(epoch, 6),
            SessionAction::Expire {
                guard: latest,
                detached: never,
                now: u64::MAX,
            },
        )),
        RejectReason::PreconditionFailed,
    );
    assert_eq!(p.state().sessions[&session].expiry, SessionExpiry::Never);
    // Legacy fixture sessions have no confirmed transport attachment. They
    // must first pass through Open before a product detachment is meaningful.
    rejected(
        p.apply(&lifecycle(
            operation_id(epoch, 7),
            SessionAction::Detach {
                guard: SessionGuard {
                    session: p.mem.b,
                    opened_by: None,
                },
                expiry: SessionExpiry::After(1),
                observed_at: 0,
            },
        )),
        RejectReason::PreconditionFailed,
    );
}

#[test]
fn inconsistent_durable_detachment_is_typed_even_when_sqlite_is_physically_healthy() {
    let directory = Directory::new();
    let path = directory.database();
    let mut store = sqlite(&path);
    let epoch = open(&mut store);
    let anchor = store
        .checkpoint_session_clock(
            None,
            mqttiumd_store_api::SessionClockReading {
                domain: [9; 32],
                tick: 0,
            },
        )
        .unwrap();
    assert_eq!(anchor.logical, 0);
    let operation = operation_id(epoch, 1);
    let (session, _) =
        expect_open(store.apply(&finite_open(operation, b"bad-detached", None, 1, 0)));
    let guard = SessionGuard {
        session,
        opened_by: Some(operation),
    };
    store.apply(&lifecycle(
        operation_id(epoch, 2),
        SessionAction::Detach {
            guard,
            expiry: SessionExpiry::After(1),
            observed_at: 0,
        },
    ));
    let foreign = rusqlite::Connection::open(&path).unwrap();
    foreign
        .execute("UPDATE sessions SET deadline=NULL", [])
        .unwrap();
    assert_eq!(
        foreign
            .query_row("PRAGMA quick_check", [], |row| row.get::<_, String>(0))
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
}

#[test]
fn a_detachment_commit_preserves_observed_progress_even_before_the_periodic_checkpoint() {
    let mut p = Pair::new();
    checkpoint(&mut p, 5, 100);
    let epoch = p.mem.epoch;
    let op = operation_id(epoch, 1);
    let (session, _) = expect_open(p.apply(&finite_open(op, b"coalesced-time", None, 5, 0)));
    p.apply(&lifecycle(
        operation_id(epoch, 2),
        SessionAction::Detach {
            guard: SessionGuard {
                session,
                opened_by: Some(op),
            },
            expiry: SessionExpiry::After(5),
            observed_at: 2_000_000_000,
        },
    ));
    let before = p.state();
    assert_eq!(
        before.session_clock.unwrap().logical,
        2_000_000_000,
        "engaged_temporal_progress_was_lost"
    );
    assert_eq!(before.session_clock.unwrap().reading.tick, 2_000_000_100);
    let original_deadline = before.sessions[&session].detached.unwrap();
    let changed = checkpoint(&mut p, 6, 1);
    assert_eq!(
        changed.logical, 2_000_000_000,
        "changed domain retains progress already proved by Detach"
    );
    assert_eq!(
        p.state().sessions[&session].detached,
        Some(original_deadline)
    );
    assert_eq!(checkpoint(&mut p, 6, 1_000_000_001).logical, 3_000_000_000);
}

#[test]
fn the_largest_finite_interval_and_clock_overflow_have_checked_boundaries() {
    let mut p = Pair::new();
    checkpoint(&mut p, 10, 0);
    let epoch = p.mem.epoch;
    let operation = operation_id(epoch, 1);
    let (session, _) = expect_open(p.apply(&finite_open(
        operation,
        b"largest-finite",
        None,
        u32::MAX - 1,
        0,
    )));
    let guard = SessionGuard {
        session,
        opened_by: Some(operation),
    };
    p.apply(&lifecycle(
        operation_id(epoch, 2),
        SessionAction::Detach {
            guard,
            expiry: SessionExpiry::After(u32::MAX - 1),
            observed_at: 1,
        },
    ));
    assert_eq!(
        p.state().sessions[&session].detached.unwrap().deadline,
        Some(4_294_967_294_000_000_001)
    );
    let advanced = checkpoint(&mut p, 11, 1);
    assert_eq!(advanced.logical, 1);
    let before = p.state();
    let impossible = mqttiumd_store_api::SessionClockReading {
        domain: [11; 32],
        tick: u64::MAX,
    };
    // logical1 + (MAX - tick1) exactly fits MAX; the next same-domain reading
    // cannot move backwards or invent a wrapped logical deadline.
    let last = p
        .sql
        .store
        .checkpoint_session_clock(Some(advanced), impossible)
        .unwrap();
    assert_eq!(
        last,
        p.mem
            .store
            .checkpoint_session_clock(Some(advanced), impossible)
            .unwrap()
    );
    assert_eq!(last.logical, u64::MAX);
    let changed = checkpoint(&mut p, 12, 0);
    assert_eq!(changed.logical, u64::MAX);
    let impossible = mqttiumd_store_api::SessionClockReading {
        domain: [12; 32],
        tick: 1,
    };
    let stable = p.state();
    assert_eq!(
        p.sql
            .store
            .checkpoint_session_clock(Some(changed), impossible),
        Err(ReadError::InvalidData)
    );
    assert_eq!(
        p.mem
            .store
            .checkpoint_session_clock(Some(changed), impossible),
        Err(ReadError::InvalidData)
    );
    assert_eq!(p.state(), stable);
    assert_eq!(
        p.state().sessions[&session].detached,
        before.sessions[&session].detached
    );
}

#[test]
fn impossible_finite_deadline_is_refused_before_sqlite_startup_can_erase_it() {
    let directory = Directory::new();
    let path = directory.database();
    let mut store = sqlite(&path);
    let epoch = open(&mut store);
    store
        .checkpoint_session_clock(
            None,
            mqttiumd_store_api::SessionClockReading {
                domain: [31; 32],
                tick: 0,
            },
        )
        .unwrap();
    let opening = operation_id(epoch, 1);
    let (session, _) =
        expect_open(store.apply(&finite_open(opening, b"impossible-deadline", None, 1, 0)));
    store.apply(&lifecycle(
        operation_id(epoch, 2),
        SessionAction::Detach {
            guard: SessionGuard {
                session,
                opened_by: Some(opening),
            },
            expiry: SessionExpiry::After(1),
            observed_at: 0,
        },
    ));
    let foreign = rusqlite::Connection::open(&path).unwrap();
    foreign
        .execute("UPDATE sessions SET deadline=zeroblob(8)", [])
        .unwrap();
    assert_eq!(
        foreign
            .query_row("PRAGMA quick_check", [], |row| row.get::<_, String>(0))
            .unwrap(),
        "ok"
    );
    assert!(
        matches!(store.snapshot(), Err(ReadError::InvalidData)),
        "impossible_deadline_was_interpreted_as_expired"
    );
    assert_eq!(
        store.recover(None, 8),
        Err(RecoveryRefused::Read(ReadError::InvalidData))
    );
    assert_eq!(
        store.read_work(session, None, 8),
        Err(ReadError::InvalidData)
    );
    let before = store.ledger().unwrap();
    let attempt = opening_command(&store);
    assert_eq!(store.open_epoch(&attempt), Err(ReadError::InvalidData));
    assert_eq!(
        store.ledger().unwrap(),
        before,
        "invalid temporal metadata cannot advance startup"
    );
    assert_eq!(
        foreign
            .query_row(
                "SELECT count(*) FROM sessions WHERE id=?1",
                [session.to_be_bytes().as_slice()],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        1,
        "startup must leave the invalid row untouched"
    );
}

#[test]
fn impossible_finite_deadline_is_refused_before_memory_startup_can_erase_it() {
    let mut store = MemoryStore::create(StoreIncarnation::new(1), fixture_limits());
    let epoch = open(&mut store);
    store
        .checkpoint_session_clock(
            None,
            mqttiumd_store_api::SessionClockReading {
                domain: [32; 32],
                tick: 0,
            },
        )
        .unwrap();
    let opening = operation_id(epoch, 1);
    let (session, _) =
        expect_open(store.apply(&finite_open(opening, b"impossible-deadline", None, 1, 0)));
    store.apply(&lifecycle(
        operation_id(epoch, 2),
        SessionAction::Detach {
            guard: SessionGuard {
                session,
                opened_by: Some(opening),
            },
            expiry: SessionExpiry::After(1),
            observed_at: 0,
        },
    ));
    let mut corrupt = store.snapshot().unwrap();
    corrupt
        .sessions
        .get_mut(&session)
        .unwrap()
        .detached
        .as_mut()
        .unwrap()
        .deadline = Some(0);
    let mut store = MemoryStore::reopen(corrupt);
    assert!(
        matches!(store.snapshot(), Err(ReadError::InvalidData)),
        "impossible_deadline_was_interpreted_as_expired"
    );
    assert_eq!(
        store.recover(None, 8),
        Err(RecoveryRefused::Read(ReadError::InvalidData))
    );
    assert_eq!(
        store.read_work(session, None, 8),
        Err(ReadError::InvalidData)
    );
    let before = store.ledger().unwrap();
    let attempt = opening_command(&store);
    assert_eq!(store.open_epoch(&attempt), Err(ReadError::InvalidData));
    assert_eq!(
        store.ledger().unwrap(),
        before,
        "invalid temporal metadata cannot advance startup"
    );
}

#[test]
fn continuous_domain_progress_prevents_expired_session_resurrection() {
    let mut p = Pair::new();
    checkpoint(&mut p, 33, 0);
    let epoch = p.mem.epoch;
    let operation = operation_id(epoch, 1);
    let (session, _) =
        expect_open(p.apply(&finite_open(operation, b"continuous-domain", None, 3, 0)));
    p.apply(&lifecycle(
        operation_id(epoch, 2),
        SessionAction::Detach {
            guard: SessionGuard {
                session,
                opened_by: Some(operation),
            },
            expiry: SessionExpiry::After(3),
            observed_at: 0,
        },
    ));
    for tick in [1_000_000_000, 2_000_000_000, 4_000_000_000] {
        checkpoint(&mut p, 33, tick);
        p = p.reopen();
        p.open_epoch();
    }
    assert!(
        !p.state().sessions.contains_key(&session),
        "continuous_domain_progress_lost"
    );
}
