//! A process killed inside a transaction of the store (plan
//! `docs/missions/L7-kill-inside-a-transaction.md`), at the point K1
//! `store.commit.prepared`: every row of an admission written, `COMMIT`
//! not called. The file must recover to the state before the command,
//! exactly, and the command must be decided again as a first submission.
//!
//! Run with `cargo test -p mqttiumd-store-sqlite --features kill-points`.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

use mqttiumd_contract::capacity::{ExchangeSlots, PayloadBytes, ReceiptSlots, References};
use mqttiumd_contract::command::Command as StoreCommand;
use mqttiumd_lab::store::conformance::{World, accounts_match_what_is_held, fixture_limits};
use mqttiumd_lab::store::durable::{DurableState, DurableStore, Limits, MemoryStore};
use mqttiumd_store_sqlite::SqliteStore;
use mqttiumd_store_sqlite::kill_points::KillPoint;

/// Set in the environment of the child: the path of its store.
const CHILD: &str = "MQTTIUMD_KILL_POINT_CHILD";
/// The line the child prints when it stands at K1.
const AT_K1: &str = "kill-point-child: at store.commit.prepared";
/// The line the child prints if its command passed without a point.
const NO_POINT: &str = "kill-point-child: the command passed no kill point";
/// Exchanges committed before the command that is cut.
const BEFORE: u64 = 3;

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
        references: References::new(10_000),
        exchange_slots: ExchangeSlots::new(4),
        receipt_slots: ReceiptSlots::new(10_000),
        ..fixture_limits()
    }
}

/// One admission and its release, numbered `n`: two commits.
fn exchange<S: DurableStore>(world: &mut World<S>, n: u64) {
    let packet = u16::try_from(n).expect("fits");
    let admission = world.admission(2 * n, packet, b"payload", &[world.b]);
    assert!(world.store.apply(&admission).committed_now);
    let release = world.release(2 * n + 1, packet, 2 * n);
    assert!(world.store.apply(&release).committed_now);
}

/// The admission the child is killed in: the next one after the prefix.
fn cut<S>(world: &World<S>) -> StoreCommand {
    let n = BEFORE + 1;
    let packet = u16::try_from(n).expect("fits");
    world.admission(2 * n, packet, b"cut payload", &[world.b, world.c])
}

/// The deterministic store after the same prefix: the reference.
fn reference() -> World<MemoryStore> {
    let mut world = World::with_limits(&MemoryStore::create, roomy());
    for n in 1..=BEFORE {
        exchange(&mut world, n);
    }
    world
}

/// Not a test of its own: the body of the child process, run only when
/// the parent sets `CHILD`. It commits the prefix, then submits the cut
/// admission with a hook that stops at K1 and waits to be killed there.
#[test]
fn kill_point_child() {
    let Some(path) = std::env::var_os(CHILD) else {
        return;
    };
    let path = PathBuf::from(path);
    let mut world = World::with_limits(
        &|incarnation, limits| SqliteStore::create(&path, incarnation, limits).expect("created"),
        roomy(),
    );
    assert!(world.store.effective_settings().kill_points);
    for n in 1..=BEFORE {
        exchange(&mut world, n);
    }
    world.store.set_kill_hook(Some(Box::new(|point| {
        if point == KillPoint::CommitPrepared {
            let mut out = std::io::stdout().lock();
            writeln!(out, "{AT_K1}").expect("said");
            out.flush().expect("flushed");
            loop {
                std::thread::sleep(std::time::Duration::from_secs(60));
            }
        }
    })));
    let command = cut(&world);
    let _ = world.store.apply(&command);
    println!("{NO_POINT}");
}

struct Holder {
    child: Child,
}

impl Holder {
    /// The test binary itself, run again on `kill_point_child` only; it
    /// returns once the child stands at K1.
    fn spawn(path: &Path) -> Self {
        let mut child = Command::new(std::env::current_exe().expect("the test binary"))
            .args([
                "--exact",
                "kill_point_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD, path)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("the child process");
        let stdout = child.stdout.take().expect("its output");
        let mut lines = BufReader::new(stdout).lines().map_while(Result::ok);
        let at = lines.find(|line| line.ends_with(AT_K1) || line.ends_with(NO_POINT));
        let holder = Self { child };
        assert!(
            at.as_deref().is_some_and(|line| line.ends_with(AT_K1)),
            "the child did not stop at K1: {at:?}"
        );
        holder
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

#[test]
fn a_kill_before_commit_recovers_the_state_before_the_command() {
    let path = fresh_path();
    let mut reference = reference();
    let before: DurableState = reference.store.snapshot();
    let holder = Holder::spawn(&path);
    // While the child stands inside its transaction, the file shows the
    // committed prefix and nothing of the open transaction.
    assert_eq!(SqliteStore::inspect(&path).expect("inspected"), before);
    holder.kill();
    // No intervention between the kill and the restart: `open` takes the
    // writer's lock the kernel released, and runs `PRAGMA quick_check`.
    let mut store = SqliteStore::open(&path).expect("opened after the kill");
    assert!(store.effective_settings().kill_points);
    let after = store.try_snapshot().expect("decodes");
    assert_eq!(accounts_match_what_is_held(&after), Ok(()));
    assert_eq!(after, before, "the whole durable state is the prefix");
    // The command, submitted again, is decided anew: as a first submission
    // on the prefix, exactly as the deterministic store decides it.
    let command = cut(&reference);
    let expected = reference.store.apply(&command);
    assert!(expected.committed_now);
    assert_eq!(store.apply(&command), expected);
    assert_eq!(
        store.try_snapshot().expect("decodes"),
        reference.store.snapshot()
    );
}
