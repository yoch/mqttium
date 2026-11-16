//! A process killed inside a transaction of the store (plan
//! `docs/missions/L7-kill-inside-a-transaction.md`):
//!
//! - K1 `store.commit.prepared`: every row of a command written, `COMMIT`
//!   not called. The file recovers to the state before the command and
//!   the command, submitted again, is decided as a first submission.
//! - K2b `store.commit.logged`: after the commit and SQLite write-lock
//!   release, before `COMMIT` returns; no checkpoint while the hook is
//!   armed. The file recovers to the state after
//!   the command and the command, submitted again, is answered from the
//!   durable record without a second effect.
//!
//! Run with `cargo test -p mqttiumd-store-sqlite --features kill-points`.

use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use mqttiumd_contract::capacity::{ExchangeSlots, PayloadBytes, ReceiptSlots, References};
use mqttiumd_contract::command::{ClaimDelivery, Command as StoreCommand, FrozenRecipe, Recipe};
use mqttiumd_contract::ids::{DeliveryId, OutboundPacketId, Revision};
use mqttiumd_contract::packet::SharedBytes;
use mqttiumd_contract::session::{
    SessionAction, SessionEndReason, SessionExpiry, SessionGuard, SessionLifecycle,
};
use mqttiumd_lab::store::conformance::{
    World, accounts_match_what_is_held, altered, fixture_limits, operation_id, publication_id,
};
use mqttiumd_lab::store::durable::{DurableStore, Limits, MemoryStore};
use mqttiumd_store_sqlite::SqliteStore;
use mqttiumd_store_sqlite::kill_points::KillPoint;

/// Set in the environment of the child: the path of its store.
const CHILD: &str = "MQTTIUMD_KILL_POINT_CHILD";
/// Set in the environment of the child: the point it stops at.
const POINT: &str = "MQTTIUMD_KILL_POINT";
/// Optional bounded session-lifecycle transaction, using the same child topology.
const SCENARIO: &str = "MQTTIUMD_KILL_SESSION_SCENARIO";
/// The prefix of the line the child prints when it stands at its point,
/// followed by the index of the command it stands in.
const AT: &str = "kill-point-child: at ";
/// The line the child prints if no command reached its point.
const NO_POINT: &str = "kill-point-child: no command reached the point";
/// Exchanges committed before the commands the child may be killed in.
const BEFORE: u64 = 3;
/// At most this many commands after the prefix, two per exchange: more
/// than enough for the WAL to reach the checkpoint threshold.
const COMMANDS: usize = 1200;

static NEXT: AtomicU64 = AtomicU64::new(0);

fn fresh_path() -> PathBuf {
    let directory = std::env::temp_dir().join("mqttiumd-store-sqlite-tests");
    std::fs::create_dir_all(&directory).expect("a temporary directory");
    let path = directory.join(format!(
        "kill-point-{}-{}.db",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    for suffix in ["", "-wal", "-shm", ".writer-lock"] {
        let mut file = path.clone().into_os_string();
        file.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(file));
    }
    path
}

fn roomy() -> Limits {
    Limits {
        payload_bytes: PayloadBytes::new(1 << 20),
        references: References::new(100_000),
        exchange_slots: ExchangeSlots::new(4),
        receipt_slots: ReceiptSlots::new(100_000),
        ..fixture_limits()
    }
}

/// One admission and its release, numbered `n`: two commits.
fn exchange<S: DurableStore>(world: &mut World<S>, n: u64) {
    for command in pair(world, n) {
        assert!(world.store.apply(&command).committed_now);
    }
}

fn pair<S>(world: &World<S>, n: u64) -> [StoreCommand; 2] {
    let packet = u16::try_from(n).expect("fits");
    [
        world.admission(2 * n, packet, b"payload", &[world.b, world.c]),
        world.release(2 * n + 1, packet, 2 * n),
    ]
}

/// The commands after the prefix, in the order the child submits them.
fn commands<S>(world: &World<S>) -> Vec<StoreCommand> {
    (BEFORE + 1..)
        .flat_map(|n| pair(world, n))
        .take(COMMANDS)
        .collect()
}

fn store_world(path: &Path) -> World<SqliteStore> {
    World::with_limits(
        &|incarnation, limits| SqliteStore::create(path, incarnation, limits).expect("created"),
        roomy(),
    )
}

/// The deterministic store after the same prefix: the reference.
fn reference() -> World<MemoryStore> {
    let mut world = World::with_limits(&MemoryStore::create, roomy());
    for n in 1..=BEFORE {
        exchange(&mut world, n);
    }
    world
}

/// The target is also a producer. Its own marker and claimed destination must
/// end, while obligations of its publication towards C remain the broker's.
fn lifecycle_prefix<S: DurableStore>(world: &mut World<S>) {
    let own = altered(
        &world.admission(8, 19, b"B-produced-for-C", &[world.c]),
        |admit| {
            admit.source.session = world.b;
            admit.source.exchange.session = world.b;
        },
    );
    assert!(world.store.apply(&own).committed_now);
    let claim = StoreCommand {
        recipe: FrozenRecipe::freeze(Recipe::ClaimDelivery(ClaimDelivery {
            operation: operation_id(world.epoch, 9),
            delivery: DeliveryId {
                publication: publication_id(world.epoch, 2),
                index: 0,
            },
            target: world.b,
            target_revision: Revision::new(1),
            packet_id: OutboundPacketId::new(7).unwrap(),
        }))
        .unwrap(),
        payload: None,
    };
    assert!(world.store.apply(&claim).committed_now);
}

fn lifecycle_command<S>(world: &World<S>, scenario: &str) -> StoreCommand {
    let guard = SessionGuard {
        session: world.b,
        opened_by: None,
    };
    let action = match scenario {
        "close" => SessionAction::Close {
            guard,
            reason: SessionEndReason::Disconnect,
        },
        "replace" => SessionAction::Open {
            client_id: SharedBytes::copy_from(b"p1-b", 1024).unwrap(),
            expected: Some(guard),
            clean_start: true,
            expiry: SessionExpiry::Never,
        },
        _ => panic!("unknown lifecycle scenario"),
    };
    StoreCommand {
        recipe: FrozenRecipe::freeze(Recipe::SessionLifecycle(SessionLifecycle {
            operation: operation_id(world.epoch, 10),
            action,
        }))
        .unwrap(),
        payload: None,
    }
}

fn point_name(point: KillPoint) -> &'static str {
    match point {
        KillPoint::CommitPrepared => "store.commit.prepared",
        KillPoint::CommitLogged => "store.commit.logged",
    }
}

fn named(name: &str) -> KillPoint {
    [KillPoint::CommitPrepared, KillPoint::CommitLogged]
        .into_iter()
        .find(|point| point_name(*point) == name)
        .expect("a known point")
}

/// Not a test of its own: the body of the child process, run only when
/// the parent sets `CHILD`. It commits the prefix, then submits commands
/// with a hook that stops at the first passage of its point, says in
/// which command, and waits to be killed there.
#[test]
fn kill_point_child() {
    let Some(path) = std::env::var_os(CHILD) else {
        return;
    };
    let point = std::env::var(POINT).expect("a point");
    let store_point = if point == "store.commit.applied" {
        None
    } else {
        Some(named(&point))
    };
    let mut world = store_world(Path::new(&path));
    assert!(world.store.effective_settings().kill_points);
    for n in 1..=BEFORE {
        exchange(&mut world, n);
    }
    let scenario = std::env::var(SCENARIO)
        .ok()
        .filter(|value| !value.is_empty());
    let commands = if let Some(scenario) = &scenario {
        lifecycle_prefix(&mut world);
        vec![lifecycle_command(&world, scenario)]
    } else {
        commands(&world)
    };
    let index = Arc::new(AtomicU64::new(0));
    let current = Arc::clone(&index);
    if let Some(point) = store_point {
        world
            .store
            .set_kill_hook(Some(Box::new(move |passed| {
                if passed == point {
                    park_at(point_name(point), current.load(Ordering::SeqCst));
                }
            })))
            .expect("armed");
    }
    for (at, command) in commands.iter().enumerate() {
        index.store(u64::try_from(at).expect("fits"), Ordering::SeqCst);
        let applied = world.store.apply(command);
        if point == "store.commit.applied" {
            // The transaction has returned to this adapter, but this test
            // has no core and hands no result over before parking.
            assert!(applied.committed_now);
            park_at(&point, u64::try_from(at).expect("fits"));
        }
    }
    println!("{NO_POINT}");
}

fn park_at(point: &str, index: u64) -> ! {
    let mut out = std::io::stdout().lock();
    writeln!(out, "{AT}{point} {index}").expect("said");
    out.flush().expect("flushed");
    loop {
        std::thread::sleep(std::time::Duration::from_secs(60));
    }
}

struct Holder {
    child: Child,
}

impl Holder {
    /// The test binary itself, run again on `kill_point_child` only; it
    /// returns once the child stands at `point`, with the index of the
    /// command it stands in.
    fn spawn(path: &Path, point: &str) -> (Self, usize) {
        Self::spawn_scenario(path, point, "")
    }

    fn spawn_scenario(path: &Path, point: &str, scenario: &str) -> (Self, usize) {
        let mut child = Command::new(std::env::current_exe().expect("the test binary"))
            .args([
                "--exact",
                "kill_point_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD, path)
            .env(POINT, point)
            .env(SCENARIO, scenario)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("the child process");
        let stdout = child.stdout.take().expect("its output");
        // Own the child before any fallible wait: a timeout or closed pipe
        // must still kill and reap it. Both the output and the wait are bounded.
        let holder = Self { child };
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        let reader = std::thread::spawn(move || {
            let mut lines = BufReader::new(stdout)
                .take(4096)
                .lines()
                .map_while(Result::ok);
            let said = lines.find(|line| line.contains(AT) || line.ends_with(NO_POINT));
            let _ = sender.send(said);
        });
        let said = receiver
            .recv_timeout(std::time::Duration::from_secs(15))
            .expect("kill point reported within 15 seconds");
        reader.join().expect("the bounded reader ended");
        let prefix = format!("{AT}{point} ");
        let index = said
            .as_deref()
            .and_then(|line| line.split_once(&prefix))
            .and_then(|(_, index)| index.parse().ok());
        let Some(index) = index else {
            panic!("the child did not stop at {point}: {said:?}");
        };
        (holder, index)
    }

    /// `SIGKILL`, then the status the kernel reports.
    fn kill(mut self) {
        self.child.kill().expect("killed");
        let status = self.child.wait().expect("reaped");
        assert_eq!(status.signal(), Some(9), "killed by SIGKILL: {status:?}");
    }
}

impl Drop for Holder {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Kills the child at `point` and reopens its file: what is durable
/// equals the reference after the commands before the one cut, and that
/// one too when `durable`. The command cut, submitted again, gets the
/// answer of the reference.
fn killed_at(point: &str, durable: bool) -> usize {
    let path = fresh_path();
    let mut reference = reference();
    let commands = commands(&reference);
    let (holder, cut) = Holder::spawn(&path, point);
    for command in &commands[..cut] {
        let _ = reference.store.apply(command);
    }
    let before = reference.store.snapshot().expect("healthy snapshot");
    let mut after_cut = crate::reference();
    for command in &commands[..=cut] {
        let _ = after_cut.store.apply(command);
    }
    let after = after_cut.store.snapshot().expect("healthy snapshot");
    if durable {
        assert!(reference.store.apply(&commands[cut]).committed_now);
    }
    holder.kill();
    // No intervention between the kill and the restart: `open` takes the
    // writer's lock the kernel released, and runs `PRAGMA quick_check`.
    let mut store = SqliteStore::open(&path).expect("opened after the kill");
    let recovered = store.try_snapshot().expect("decodes");
    assert_eq!(accounts_match_what_is_held(&recovered), Ok(()));
    assert_eq!(
        recovered,
        if durable { after } else { before },
        "the whole durable state is the prefix{}",
        if durable { " and the command cut" } else { "" }
    );
    // Submitted again: decided anew (K1), or answered from the durable
    // record without a second effect (K2b), as the reference answers.
    let expected = reference.store.apply(&commands[cut]);
    assert_eq!(expected.committed_now, !durable);
    assert_eq!(store.apply(&commands[cut]), expected);
    assert_eq!(
        store.try_snapshot().expect("decodes"),
        reference.store.snapshot().expect("healthy snapshot")
    );
    cut
}

#[test]
fn a_kill_before_commit_recovers_the_state_before_the_command() {
    assert_eq!(killed_at("store.commit.prepared", false), 0);
}

#[test]
fn a_kill_once_the_commit_is_logged_recovers_the_command() {
    assert_eq!(killed_at("store.commit.logged", true), 0);
}

/// The installed callback actually runs, with checkpoint disabled while
/// armed, and removing it restores the prior threshold.
#[test]
fn the_wal_hook_runs_with_checkpoint_disabled_then_restores_the_threshold() {
    let path = fresh_path();
    let mut world = store_world(&path);
    let threshold = world
        .store
        .set_wal_autocheckpoint_for_measurement(16)
        .expect("set");
    let hits = Arc::new(AtomicU64::new(0));
    let counted = Arc::clone(&hits);
    world
        .store
        .set_kill_hook(Some(Box::new(move |point| {
            if point == KillPoint::CommitLogged {
                counted.fetch_add(1, Ordering::SeqCst);
            }
        })))
        .expect("armed");
    assert_eq!(world.store.effective_settings().wal_autocheckpoint, 0);
    assert_eq!(
        world.store.effective_settings().checkpoint_policy,
        "disabled-while-kill-hook-armed"
    );
    for command in commands(&world).iter().take(20) {
        assert!(world.store.apply(command).committed_now);
    }
    assert_eq!(
        hits.load(Ordering::SeqCst),
        20,
        "the actual WAL callback ran for every commit"
    );
    world.store.set_kill_hook(None).expect("disarmed");
    assert_eq!(
        world.store.effective_settings().wal_autocheckpoint,
        threshold
    );
    assert_eq!(
        world.store.effective_settings().checkpoint_policy,
        "sqlite-autocheckpoint"
    );
    for command in commands(&world).iter().skip(20).take(2) {
        assert!(world.store.apply(command).committed_now);
    }
    assert_eq!(
        hits.load(Ordering::SeqCst),
        20,
        "no kill callback after removal"
    );
}

/// An autocheckpoint PRAGMA replaces SQLite's single WAL callback. An
/// armed store must refuse that reconfiguration, preserving the kill point.
#[test]
fn changing_the_autocheckpoint_cannot_replace_an_armed_kill_hook() {
    let path = fresh_path();
    let mut world = store_world(&path);
    let hits = Arc::new(AtomicU64::new(0));
    let counted = Arc::clone(&hits);
    world
        .store
        .set_kill_hook(Some(Box::new(move |point| {
            if point == KillPoint::CommitLogged {
                counted.fetch_add(1, Ordering::SeqCst);
            }
        })))
        .expect("armed");
    assert!(
        world
            .store
            .set_wal_autocheckpoint_for_measurement(16)
            .is_err(),
        "changing the autocheckpoint would remove the armed WAL callback"
    );
    assert_eq!(world.store.effective_settings().wal_autocheckpoint, 0);
    assert!(world.store.apply(&commands(&world)[0]).committed_now);
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "the WAL callback still runs"
    );
}

/// A physical SQLite consistency check alone misses an incomplete business
/// transaction. The full comparison used by the kill tests rejects it.
#[test]
fn full_state_equality_detects_a_missing_obligation_in_a_sound_sqlite_file() {
    let path = fresh_path();
    let mut world = store_world(&path);
    let mut expected = reference();
    for n in 1..=BEFORE {
        exchange(&mut world, n);
    }
    let command = commands(&world)[0].clone();
    assert!(world.store.apply(&command).committed_now);
    assert!(expected.store.apply(&command).committed_now);
    let complete = expected.store.snapshot().expect("healthy snapshot");
    assert_eq!(
        world.store.try_snapshot().expect("complete state"),
        complete
    );
    drop(world);
    let connection = rusqlite::Connection::open(&path).expect("isolated mutation connection");
    assert_eq!(connection.execute(
        "DELETE FROM obligations WHERE (publication, idx) IN (SELECT publication, idx FROM obligations LIMIT 1)", [],
    ).expect("one obligation omitted"), 1);
    let checked: String = connection
        .query_row("PRAGMA quick_check", [], |row| row.get(0))
        .expect("checked");
    assert_eq!(
        checked, "ok",
        "the physical check passes the partial business state"
    );
    drop(connection);
    let store = SqliteStore::open(&path).expect("physically sound file opens");
    let partial = store.try_snapshot().expect("partial state decodes");
    assert_ne!(
        partial, complete,
        "whole-state comparison rejects the missing obligation"
    );
    assert!(
        accounts_match_what_is_held(&partial).is_err(),
        "business accounts detect the omitted obligation"
    );
}

/// K3 at the adapter boundary: apply returned, no result was delivered to
/// a core, and the reopened state and replay agree with the full reference.
#[test]
fn a_kill_after_apply_before_result_delivery_recovers_the_whole_command() {
    assert_eq!(killed_at("store.commit.applied", true), 0);
}

fn killed_lifecycle_at(scenario: &str, point: &str, durable: bool) {
    let path = fresh_path();
    let mut expected = reference();
    lifecycle_prefix(&mut expected);
    let command = lifecycle_command(&expected, scenario);
    let before = expected.store.snapshot().unwrap();
    if durable {
        assert!(expected.store.apply(&command).committed_now);
    }
    let (holder, cut) = Holder::spawn_scenario(&path, point, scenario);
    assert_eq!(cut, 0, "the lifecycle command itself reaches the hook");
    let pid = holder.child.id();
    holder.kill();
    let mut reopened = SqliteStore::open(&path).expect("same file after confirmed SIGKILL");
    assert_eq!(reopened.effective_settings().user_version, 5);
    let found = reopened.snapshot().unwrap();
    assert_eq!(
        found,
        expected.store.snapshot().unwrap(),
        "lifecycle_atomicity: complete logical state at {scenario}/{point}"
    );
    assert_eq!(accounts_match_what_is_held(&found), Ok(()));
    if durable {
        assert!(!found.sessions.contains_key(&expected.b));
        assert_eq!(
            found.obligations.len(),
            4,
            "three original C obligations plus the old producer's C obligation survive"
        );
        assert!(
            found
                .obligations
                .values()
                .all(|row| row.target == expected.c)
        );
        assert!(found.outbound.is_empty() && found.inbound.is_empty());
        assert_eq!(found.accounts.references.get(), 4);
        assert_eq!(
            found.sessions.len(),
            if scenario == "replace" { 3 } else { 2 }
        );
    } else {
        assert_eq!(found, before);
    }
    let retried = expected.store.apply(&command);
    assert_eq!(retried.committed_now, !durable);
    assert_eq!(reopened.apply(&command), retried);
    assert_eq!(
        reopened.snapshot().unwrap(),
        expected.store.snapshot().unwrap()
    );
    println!(
        "session-lifecycle-kill scenario={scenario} point={point} pid={pid} signal=9 same_file=true full_state=true command_durable={durable}"
    );
}

#[test]
fn session_close_before_commit_keeps_the_complete_previous_state() {
    killed_lifecycle_at("close", "store.commit.prepared", false);
}
#[test]
fn session_close_logged_commit_cancels_only_its_own_responsibilities() {
    killed_lifecycle_at("close", "store.commit.logged", true);
}
#[test]
fn session_close_applied_commit_replays_without_another_cancellation() {
    killed_lifecycle_at("close", "store.commit.applied", true);
}
#[test]
fn session_replace_before_commit_keeps_the_complete_previous_state() {
    killed_lifecycle_at("replace", "store.commit.prepared", false);
}
#[test]
fn session_replace_logged_commit_has_one_fresh_incarnation() {
    killed_lifecycle_at("replace", "store.commit.logged", true);
}
#[test]
fn session_replace_applied_commit_cannot_repeat_the_replacement() {
    killed_lifecycle_at("replace", "store.commit.applied", true);
}
