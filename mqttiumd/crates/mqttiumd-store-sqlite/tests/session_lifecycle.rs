//! Store-only session lifecycle regressions. The separate product reception
//! exercises public MQTT clients, connections and broker process lifetimes.
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use mqttiumd_contract::ids::StoreIncarnation;
use mqttiumd_lab::store::conformance::{accounts_match_what_is_held, fixture_limits};
use mqttiumd_lab::store::durable::MemoryStore;
use mqttiumd_store_api::{AdminCommand, AdminDone, DurableStore, ReadBudget, SessionState};
use mqttiumd_store_sqlite::SqliteStore;

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
