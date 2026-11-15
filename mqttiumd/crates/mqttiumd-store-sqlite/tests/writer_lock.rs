//! One process owns the store for the whole life of a `SqliteStore`: a
//! second writer is refused at once, from this process or another, and
//! the ownership ends with the store or with the process, a `SIGKILL`
//! included. An inspector does not take it.

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

use mqttiumd_contract::capacity::{ExchangeSlots, PayloadBytes, ReceiptSlots, References};
use mqttiumd_lab::store::conformance::{INCARNATION, World, fixture_limits};
use mqttiumd_lab::store::durable::{DurableStore, Limits};
use mqttiumd_store_sqlite::{OpenError, SqliteStore};

/// Set in the environment of the child: the path of the store it holds.
const CHILD: &str = "MQTTIUMD_WRITER_LOCK_CHILD";
/// The line the child prints once its commits returned.
const READY: &str = "writer-lock-child: ready";

static NEXT: AtomicU64 = AtomicU64::new(0);

fn fresh_path() -> PathBuf {
    let directory = std::env::temp_dir().join("mqttiumd-store-sqlite-tests");
    std::fs::create_dir_all(&directory).expect("a temporary directory");
    let path = directory.join(format!(
        "writer-lock-{}-{}.db",
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
        references: References::new(10_000),
        exchange_slots: ExchangeSlots::new(4),
        receipt_slots: ReceiptSlots::new(10_000),
        ..fixture_limits()
    }
}

fn world(path: &Path) -> World<SqliteStore> {
    World::with_limits(
        &|incarnation, limits| SqliteStore::create(path, incarnation, limits).expect("created"),
        roomy(),
    )
}

/// One admission and its release, numbered `n`: two commits.
fn exchange(world: &mut World<SqliteStore>, n: u64) {
    let packet = u16::try_from(n % 60_000 + 1).expect("fits");
    let admission = world.admission(2 * n, packet, b"payload", &[world.b]);
    assert!(world.store.apply(&admission).committed_now);
    let release = world.release(2 * n + 1, packet, 2 * n);
    assert!(world.store.apply(&release).committed_now);
}

fn refused_as_alive<T: std::fmt::Debug>(result: Result<T, OpenError>) {
    match result {
        Err(OpenError::WriterAlive) => {}
        other => panic!("expected WriterAlive, got {other:?}"),
    }
}

#[test]
fn a_second_writer_in_the_same_process_is_refused() {
    let path = fresh_path();
    let first = world(&path);
    refused_as_alive(SqliteStore::open(&path));
    refused_as_alive(SqliteStore::create(&path, INCARNATION, roomy()));
    // The refused attempts changed nothing.
    let state = first.store.snapshot().expect("healthy snapshot");
    assert_eq!(SqliteStore::inspect(&path).expect("inspected"), state);
    drop(first);
    // The lock file stays; it alone holds nothing back.
    assert!(with_suffix(&path, ".writer-lock").exists());
    let again = SqliteStore::open(&path).expect("reopened once the first is gone");
    assert_eq!(again.snapshot().expect("healthy snapshot"), state);
    refused_as_alive(SqliteStore::open(&path));
}

#[test]
fn an_inspection_reads_while_the_writer_lives() {
    let path = fresh_path();
    let mut writer = world(&path);
    exchange(&mut writer, 1);
    assert_eq!(
        SqliteStore::inspect(&path).expect("inspected"),
        writer.store.snapshot().expect("healthy snapshot")
    );
    // The writer still commits after the inspection.
    exchange(&mut writer, 2);
    assert_eq!(
        SqliteStore::inspect(&path).expect("inspected"),
        writer.store.snapshot().expect("healthy snapshot")
    );
}

/// Not a test of its own: the body of the child process. It runs only
/// when the parent sets `CHILD`, creates the store, commits, says so and
/// waits to be killed, outside any transaction.
#[test]
fn writer_lock_child() {
    let Some(path) = std::env::var_os(CHILD) else {
        return;
    };
    let path = PathBuf::from(path);
    let mut writer = world(&path);
    for n in 1..=5 {
        exchange(&mut writer, n);
    }
    println!("{READY}");
    loop {
        std::thread::sleep(std::time::Duration::from_secs(60));
    }
}

struct Holder {
    child: Child,
}

impl Holder {
    /// The test binary itself, run again on `writer_lock_child` only.
    fn spawn(path: &Path) -> Self {
        let mut child = Command::new(std::env::current_exe().expect("the test binary"))
            .args([
                "--exact",
                "writer_lock_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD, path)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("the child process");
        let stdout = child.stdout.take().expect("its output");
        let ready = BufReader::new(stdout)
            .lines()
            .map_while(Result::ok)
            .any(|line| line.ends_with(READY));
        assert!(ready, "the child holds the store");
        Self { child }
    }

    /// `Child::kill` is `SIGKILL` on Unix: no destructor runs.
    fn kill(mut self) {
        self.child.kill().expect("killed");
        let status = self.child.wait().expect("reaped");
        assert!(!status.success());
    }
}

impl Drop for Holder {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn a_writer_in_another_process_is_refused_until_it_is_killed() {
    let path = fresh_path();
    let holder = Holder::spawn(&path);
    refused_as_alive(SqliteStore::open(&path));
    refused_as_alive(SqliteStore::create(&path, INCARNATION, roomy()));
    // The inspector reads what the child committed while it lives.
    let committed = SqliteStore::inspect(&path).expect("inspected");
    holder.kill();
    // Nothing is touched between the kill and the restart: no file is
    // removed, the WAL and its index stay as the kernel left them.
    let store = SqliteStore::open(&path).expect("opened after the kill, with no intervention");
    // `open` ran `PRAGMA quick_check`; the state is the committed one.
    assert_eq!(store.try_snapshot().expect("decodes"), committed);
    refused_as_alive(SqliteStore::open(&path));
}
