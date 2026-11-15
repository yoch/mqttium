//! A process killed inside a transaction of the store (plan
//! `docs/missions/L7-kill-inside-a-transaction.md`):
//!
//! - K1 `store.commit.prepared`: every row of a command written, `COMMIT`
//!   not called. The file recovers to the state before the command and
//!   the command, submitted again, is decided as a first submission.
//! - K2b `store.commit.logged`: inside `COMMIT`, the commit frame durable,
//!   `COMMIT` not returned; before, then after, the checkpoint the store's
//!   WAL hook runs at the threshold. The file recovers to the state after
//!   the command and the command, submitted again, is answered from the
//!   durable record without a second effect.
//!
//! Run with `cargo test -p mqttiumd-store-sqlite --features kill-points`.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use mqttiumd_contract::capacity::{ExchangeSlots, PayloadBytes, ReceiptSlots, References};
use mqttiumd_contract::command::Command as StoreCommand;
use mqttiumd_lab::store::conformance::{World, accounts_match_what_is_held, fixture_limits};
use mqttiumd_lab::store::durable::{DurableStore, Limits, MemoryStore};
use mqttiumd_store_sqlite::SqliteStore;
use mqttiumd_store_sqlite::kill_points::KillPoint;

/// Set in the environment of the child: the path of its store.
const CHILD: &str = "MQTTIUMD_KILL_POINT_CHILD";
/// Set in the environment of the child: the point it stops at.
const POINT: &str = "MQTTIUMD_KILL_POINT";
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
        let _ = std::fs::remove_file(with_suffix(&path, suffix));
    }
    path
}

fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut file = path.to_path_buf().into_os_string();
    file.push(suffix);
    PathBuf::from(file)
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

fn point_name(point: KillPoint) -> &'static str {
    match point {
        KillPoint::CommitPrepared => "store.commit.prepared",
        KillPoint::CommitLogged => "store.commit.logged",
        KillPoint::CommitCheckpointed => "store.commit.logged-after-checkpoint",
    }
}

fn named(name: &str) -> KillPoint {
    [
        KillPoint::CommitPrepared,
        KillPoint::CommitLogged,
        KillPoint::CommitCheckpointed,
    ]
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
    let point = named(&std::env::var(POINT).expect("a point"));
    let mut world = store_world(Path::new(&path));
    assert!(world.store.effective_settings().kill_points);
    for n in 1..=BEFORE {
        exchange(&mut world, n);
    }
    let index = Arc::new(AtomicU64::new(0));
    let current = Arc::clone(&index);
    world
        .store
        .set_kill_hook(Some(Box::new(move |passed| {
            if passed == point {
                let mut out = std::io::stdout().lock();
                let at = current.load(Ordering::SeqCst);
                writeln!(out, "{AT}{} {at}", point_name(point)).expect("said");
                out.flush().expect("flushed");
                loop {
                    std::thread::sleep(std::time::Duration::from_secs(60));
                }
            }
        })))
        .expect("armed");
    for (at, command) in commands(&world).iter().enumerate() {
        index.store(u64::try_from(at).expect("fits"), Ordering::SeqCst);
        let _ = world.store.apply(command);
    }
    println!("{NO_POINT}");
}

struct Holder {
    child: Child,
}

impl Holder {
    /// The test binary itself, run again on `kill_point_child` only; it
    /// returns once the child stands at `point`, with the index of the
    /// command it stands in.
    fn spawn(path: &Path, point: KillPoint) -> (Self, usize) {
        let mut child = Command::new(std::env::current_exe().expect("the test binary"))
            .args([
                "--exact",
                "kill_point_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD, path)
            .env(POINT, point_name(point))
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("the child process");
        let stdout = child.stdout.take().expect("its output");
        let mut lines = BufReader::new(stdout).lines().map_while(Result::ok);
        let said = lines.find(|line| line.contains(AT) || line.ends_with(NO_POINT));
        let holder = Self { child };
        let prefix = format!("{AT}{} ", point_name(point));
        let index = said
            .as_deref()
            .and_then(|line| line.split_once(&prefix))
            .and_then(|(_, index)| index.parse().ok());
        let Some(index) = index else {
            panic!("the child did not stop at {}: {said:?}", point_name(point));
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
fn killed_at(point: KillPoint, durable: bool) -> usize {
    let path = fresh_path();
    let mut reference = reference();
    let commands = commands(&reference);
    let (holder, cut) = Holder::spawn(&path, point);
    for command in &commands[..cut] {
        let _ = reference.store.apply(command);
    }
    if durable {
        assert!(reference.store.apply(&commands[cut]).committed_now);
    }
    holder.kill();
    // No intervention between the kill and the restart: `open` takes the
    // writer's lock the kernel released, and runs `PRAGMA quick_check`.
    let mut store = SqliteStore::open(&path).expect("opened after the kill");
    let after = store.try_snapshot().expect("decodes");
    assert_eq!(accounts_match_what_is_held(&after), Ok(()));
    assert_eq!(
        after,
        reference.store.snapshot(),
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
        reference.store.snapshot()
    );
    cut
}

#[test]
fn a_kill_before_commit_recovers_the_state_before_the_command() {
    assert_eq!(killed_at(KillPoint::CommitPrepared, false), 0);
}

#[test]
fn a_kill_once_the_commit_is_logged_recovers_the_command() {
    assert_eq!(killed_at(KillPoint::CommitLogged, true), 0);
}

#[test]
fn a_kill_after_the_checkpoint_inside_commit_recovers_the_command() {
    // The first command whose commit reaches the threshold of the WAL.
    let cut = killed_at(KillPoint::CommitCheckpointed, true);
    assert!(cut > 0, "the threshold is reached after some commits");
}

/// While the WAL points are armed, the store's WAL hook checkpoints as
/// the engine would have: the WAL stays within the threshold and one
/// commit, and each checkpoint is passed once.
#[test]
fn the_wal_hook_keeps_the_checkpoint_of_the_engine() {
    let path = fresh_path();
    let mut world = store_world(&path);
    let threshold = world.store.effective_settings().wal_autocheckpoint;
    let page = world.store.effective_settings().page_size;
    let checkpoints = Arc::new(AtomicU64::new(0));
    let counted = Arc::clone(&checkpoints);
    world
        .store
        .set_kill_hook(Some(Box::new(move |point| {
            if point == KillPoint::CommitCheckpointed {
                counted.fetch_add(1, Ordering::SeqCst);
            }
        })))
        .expect("armed");
    let wal = with_suffix(&path, "-wal");
    let mut largest = 0;
    for command in commands(&world).iter().take(800) {
        assert!(world.store.apply(command).committed_now);
        largest = largest.max(std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0));
    }
    let frame = u64::try_from(page).expect("positive") + 24;
    let bound = 32 + (u64::try_from(threshold).expect("positive") + 64) * frame;
    assert!(checkpoints.load(Ordering::SeqCst) > 0, "a checkpoint ran");
    assert!(largest <= bound, "WAL {largest} bytes, bound {bound}");
    // Disarmed: the engine's own checkpoint is back, at the same threshold.
    world.store.set_kill_hook(None).expect("disarmed");
    assert_eq!(
        world.store.effective_settings().wal_autocheckpoint,
        threshold
    );
}
