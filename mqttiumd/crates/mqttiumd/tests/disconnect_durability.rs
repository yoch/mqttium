//! Where the end of a session becomes durable (record 0059).
//!
//! A client DISCONNECT has no acknowledgement. The broker closes the
//! transport first and commits the end of the session afterwards, so what
//! the client observes (the end of its stream) is not a durable fact. These
//! tests drive the real executable over loopback TCP, kill it with SIGKILL
//! at a chosen point, start it again on the same store, and read what a
//! client then sees: Session Present, and the Will a normal DISCONNECT
//! cancelled.
//!
//! In durable mode the point is a barrier of the writer, inside the
//! transaction or right after its COMMIT (executable built with
//! `test-hooks`). The mTLS cases also hold log writes on both sides of
//! fdatasync and distinguish the previous and next volatile snapshots.
//! The cleartext cases for these two stores kill after `session_changed`,
//! which the runtime writes once the store has answered.
//!
//! One store, three runs of the executable: `setup` opens the sessions and
//! stops gracefully, `cut` is killed at the barrier, `restart` is observed.
//! A session is opened with Session Expiry Interval 600 s and ended with a
//! DISCONNECT that lowers it to 0: a session whose durable policy is
//! already zero is erased by the next epoch whatever happened before.
//!
//! A kill of the process says nothing about a power loss or about `fsync`:
//! what was written survives in the page cache.
#![cfg(target_os = "linux")]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use mqttiumd_contract::outcome::CommitFacts;
use mqttiumd_store_api::Decision;
use mqttiumd_store_sqlite::SqliteStore;

const WAIT: Duration = Duration::from_secs(10);
const EXPIRY: u32 = 600;
const TARGET: &str = "durability-target";
const WITNESS: &str = "durability-witness";
const MONITOR: &str = "durability-monitor";
const STATUS: &str = "durability/status";
const WILL: &[u8] = b"offline";

/// The directory of one case: its store and the files of each run.
struct Case(PathBuf);

impl Case {
    fn new(name: &str) -> Self {
        static SERIAL: AtomicU32 = AtomicU32::new(0);
        let directory = std::env::temp_dir().join(format!(
            "mqttiumd-durability-{name}-{}-{}",
            std::process::id(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).unwrap();
        Self(directory)
    }

    fn store(&self) -> PathBuf {
        self.0.join("store.sqlite")
    }

    /// One run of the executable on the store of the case. Each run has an
    /// event file of its own; `pauses` arm barriers of the writer and need
    /// the executable built with `test-hooks`.
    fn run(&self, label: &str, mode: &[&str], pauses: &[&str]) -> Broker {
        let events = self.0.join(format!("{label}.events.jsonl"));
        let control = self.0.join(format!("{label}.control"));
        let mut command = Command::new(env!("CARGO_BIN_EXE_mqttiumd"));
        command
            .arg("--db")
            .arg(self.store())
            .args(["--listen", "127.0.0.1:0"])
            .arg("--events")
            .arg(&events)
            .args(["--event-level", "audit"])
            .args(mode);
        if !pauses.is_empty() {
            std::fs::create_dir_all(&control).unwrap();
            command
                .arg("--test-control")
                .arg(&control)
                .args(["--test-pause", &pauses.join(",")]);
        }
        let mut child = command
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let mut line = String::new();
        BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        let address = line
            .strip_prefix("mqttiumd ready ")
            .and_then(|rest| rest.split_whitespace().next())
            .and_then(|text| text.trim_end_matches(',').parse().ok())
            .unwrap_or_else(|| panic!("unexpected readiness line of {label}: {line:?}"));
        Broker {
            child: Some(child),
            address,
            events,
        }
    }
}

impl Drop for Case {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Broker {
    child: Option<Child>,
    address: SocketAddr,
    events: PathBuf,
}

impl Broker {
    fn events(&self, kind: &str) -> Vec<serde_json::Value> {
        std::fs::read_to_string(&self.events)
            .unwrap_or_default()
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .filter(|event| event["kind"] == kind)
            .collect()
    }

    /// Waits until the writer is held at `point`.
    #[cfg(feature = "test-hooks")]
    fn held_at(&self, point: &str) {
        let deadline = Instant::now() + WAIT;
        while !self
            .events("barrier_hit")
            .iter()
            .any(|event| event["point"] == point)
        {
            assert!(
                Instant::now() < deadline,
                "the writer never reached {point}"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// SIGINT: the executable drains its writer and exits with status 0.
    fn stop(mut self) {
        let mut child = self.child.take().unwrap();
        let pid = nix::unistd::Pid::from_raw(child.id() as i32);
        nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGINT).unwrap();
        let deadline = Instant::now() + WAIT;
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert!(status.success(), "graceful stop failed: {status}");
                return;
            }
            assert!(Instant::now() < deadline, "the broker did not stop");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Waits until the runtime has written that a session was closed: the
    /// store has answered the end of the session.
    fn session_closed(&self) {
        let deadline = Instant::now() + WAIT;
        while !self
            .events("session_changed")
            .iter()
            .any(|event| event["details"]["action"] == "Closed")
        {
            assert!(Instant::now() < deadline, "no session was closed");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// SIGKILL, and the proof that the process ended by that signal.
    fn kill(mut self) {
        let mut child = self.child.take().unwrap();
        child.kill().unwrap();
        let status = child.wait().unwrap();
        assert_eq!(
            status.signal(),
            Some(9),
            "the broker was not killed: {status}"
        );
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn remaining_length(mut length: usize, out: &mut Vec<u8>) {
    loop {
        let mut byte = (length % 128) as u8;
        length /= 128;
        if length > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if length == 0 {
            break;
        }
    }
}

fn packet(first: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![first];
    remaining_length(body.len(), &mut out);
    out.extend_from_slice(body);
    out
}

fn text(value: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(&u16::try_from(value.len()).unwrap().to_be_bytes());
    out.extend_from_slice(value);
}

fn read_frame(stream: &mut TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
    let mut first = [0u8; 1];
    stream.read_exact(&mut first)?;
    let mut length = 0usize;
    let mut shift = 0;
    loop {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte)?;
        length |= usize::from(byte[0] & 0x7f) << shift;
        if byte[0] & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    let mut body = vec![0; length];
    stream.read_exact(&mut body)?;
    Ok((first[0], body))
}

/// An MQTT 5 client that keeps its session for `EXPIRY` seconds (Clean
/// Start 0), with or without a QoS 1 Will on `STATUS`, and the Session
/// Present flag of its CONNACK.
fn connect(address: SocketAddr, client_id: &str, will: bool) -> (TcpStream, bool) {
    let mut stream = TcpStream::connect(address).unwrap();
    stream.set_nodelay(true).unwrap();
    stream.set_read_timeout(Some(WAIT)).unwrap();
    let mut body = Vec::new();
    text(b"MQTT", &mut body);
    // Level 5; Will Flag and Will QoS 1 when asked; Keep Alive 60 s.
    body.extend_from_slice(&[5, if will { 0x0c } else { 0x00 }, 0, 60]);
    body.push(5);
    body.push(0x11);
    body.extend_from_slice(&EXPIRY.to_be_bytes());
    text(client_id.as_bytes(), &mut body);
    if will {
        body.push(0);
        text(STATUS.as_bytes(), &mut body);
        text(WILL, &mut body);
    }
    stream.write_all(&packet(0x10, &body)).unwrap();
    let (first, connack) = read_frame(&mut stream).unwrap();
    assert_eq!(first, 0x20, "CONNACK expected by {client_id}");
    assert_eq!(connack[1], 0, "CONNACK reason of {client_id}");
    (stream, connack[0] & 1 == 1)
}

fn subscribe(stream: &mut TcpStream, filter: &str) {
    let mut body = vec![0, 1, 0];
    text(filter.as_bytes(), &mut body);
    body.push(1);
    stream.write_all(&packet(0x82, &body)).unwrap();
    let (first, suback) = read_frame(stream).unwrap();
    assert_eq!((first, suback.last()), (0x90, Some(&1)), "SUBACK");
}

/// A normal DISCONNECT without a property: the session keeps the interval
/// of its CONNECT. The stream is read to its end.
fn detach(mut stream: TcpStream) {
    stream.write_all(&[0xe0, 0]).unwrap();
    assert_eq!(end_of_stream(&mut stream), [0u8; 0]);
}

/// Everything the broker still sends, until it ends the stream.
fn end_of_stream(stream: &mut TcpStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut chunk = [0u8; 64];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => return bytes,
            Ok(count) => bytes.extend_from_slice(&chunk[..count]),
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => return bytes,
            Err(error) => panic!("the broker neither answered nor closed: {error}"),
        }
    }
}

/// The Will publications the store has committed.
fn published_wills(store: &Path) -> usize {
    SqliteStore::inspect(store)
        .unwrap()
        .operations
        .values()
        .filter(|row| {
            matches!(
                row.decision,
                Decision::Committed(CommitFacts::WillPublished { .. })
            )
        })
        .count()
}

/// What the three runs of one case showed.
#[derive(Debug, PartialEq, Eq)]
struct Observed {
    /// The client read the end of its stream after its DISCONNECT, before
    /// the broker was stopped or killed.
    client_saw_end_of_stream: bool,
    /// Session Present of the session the client ended, after the restart.
    ended_session_present: bool,
    /// Session Present of the session nobody ended, after the restart.
    witness_session_present: bool,
    /// The monitor of the Will topic received the Will after the restart.
    monitor_received_will: bool,
}

const DURABLE: &[&str] = &[];

/// `setup`: the target, the witness and the monitor of the Will topic open
/// their sessions and detach; graceful stop. A Will belongs to a connection:
/// the `cut` run installs it.
fn setup(case: &Case, mode: &[&str]) {
    let broker = case.run("setup", mode, &[]);
    let (mut monitor, present) = connect(broker.address, MONITOR, false);
    assert!(!present);
    subscribe(&mut monitor, STATUS);
    detach(monitor);
    for client_id in [TARGET, WITNESS] {
        let (mut client, present) = connect(broker.address, client_id, false);
        assert!(!present, "{client_id}");
        subscribe(&mut client, &format!("durability/command/{client_id}"));
        detach(client);
    }
    broker.stop();
}

/// `cut`, first half: the target resumes its session, with its Will when
/// asked, and ends it with DISCONNECT 0x00 and Session Expiry Interval 0.
/// Returns whether it read the end of its stream.
fn end_the_session(broker: &Broker, will: bool) -> bool {
    let (mut target, present) = connect(broker.address, TARGET, will);
    assert!(present, "the session of the setup run was not resumed");
    target
        .write_all(&packet(0xe0, &[0, 5, 0x11, 0, 0, 0, 0]))
        .unwrap();
    end_of_stream(&mut target).is_empty()
}

/// `restart`: what the clients see on the same store.
fn restart(case: &Case, mode: &[&str], client_saw_end_of_stream: bool) -> Observed {
    let broker = case.run("restart", mode, &[]);
    let (mut monitor, present) = connect(broker.address, MONITOR, false);
    assert!(present, "the monitor lost its session");
    // A pending Will is admitted after the recovery: give it its time, and
    // read no absence from a shorter wait.
    monitor
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let monitor_received_will = match read_frame(&mut monitor) {
        Ok((first, body)) => {
            assert_eq!(first & 0xf0, 0x30, "a PUBLISH on the Will topic");
            assert_eq!(&body[2..2 + STATUS.len()], STATUS.as_bytes());
            assert!(body.ends_with(WILL));
            true
        }
        Err(error) => {
            assert!(
                matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ),
                "the monitor was closed: {error}"
            );
            false
        }
    };
    let (_target, ended_session_present) = connect(broker.address, TARGET, false);
    let (_witness, witness_session_present) = connect(broker.address, WITNESS, false);
    let observed = Observed {
        client_saw_end_of_stream,
        ended_session_present,
        witness_session_present,
        monitor_received_will,
    };
    drop((_target, _witness, monitor));
    broker.stop();
    observed
}

/// The witness of every case below: nothing is killed. The end of the
/// session and the cancellation of the Will are committed before a graceful
/// stop returns, and stay after the restart.
#[test]
fn a_graceful_stop_keeps_the_session_ended_and_the_will_cancelled() {
    let case = Case::new("graceful");
    setup(&case, DURABLE);
    let broker = case.run("cut", DURABLE, &[]);
    let saw_end = end_the_session(&broker, true);
    broker.stop();
    let store = SqliteStore::inspect(&case.store()).unwrap();
    assert_eq!(
        store.sessions.len(),
        2,
        "the witness and the monitor remain"
    );
    assert!(store.wills.is_empty(), "the Will was not cancelled");
    assert_eq!(published_wills(&case.store()), 0);
    assert_eq!(
        restart(&case, DURABLE, saw_end),
        Observed {
            client_saw_end_of_stream: true,
            ended_session_present: false,
            witness_session_present: true,
            monitor_received_will: false,
        }
    );
}

/// The `cut` run of a mode without barriers: the client ends its session
/// and reads the end of its stream, the runtime reports the session closed,
/// and the process is killed.
fn cut_after_the_answer(case: &Case, mode: &[&str]) -> bool {
    let broker = case.run("cut", mode, &[]);
    let saw_end = end_the_session(&broker, false);
    broker.session_closed();
    broker.kill();
    saw_end
}

/// The append-only log gives the same answer as SQLite past its own commit,
/// the `fdatasync` of the record: the end of the session survives the kill.
#[test]
fn log_mode_keeps_the_session_ended_after_a_kill_past_its_commit() {
    const LOG: &[&str] = &["--storage-mode", "log"];
    let case = Case::new("log-committed");
    setup(&case, LOG);
    let saw_end = cut_after_the_answer(&case, LOG);
    assert_eq!(
        restart(&case, LOG, saw_end),
        Observed {
            client_saw_end_of_stream: true,
            ended_session_present: false,
            witness_session_present: true,
            monitor_received_will: false,
        }
    );
}

/// The volatile store commits in memory. A kill returns it to its last
/// snapshot, here the one the graceful stop of the setup run wrote: the
/// session comes back although the store had answered its end.
#[test]
fn volatile_mode_returns_to_its_last_snapshot_after_a_kill() {
    const VOLATILE: &[&str] = &["--storage-mode", "volatile", "--snapshot-interval", "3600"];
    let case = Case::new("volatile-committed");
    setup(&case, VOLATILE);
    let saw_end = cut_after_the_answer(&case, VOLATILE);
    assert_eq!(
        restart(&case, VOLATILE, saw_end),
        Observed {
            client_saw_end_of_stream: true,
            ended_session_present: true,
            witness_session_present: true,
            monitor_received_will: false,
        }
    );
}

/// Upstream ticket 1165 on this broker. The broker is killed while a client
/// with a Will is connected, and only the broker comes back: the Will the
/// client registered is published, with the payload it registered, and the
/// session it had asked to keep is kept (record 0034: surviving Armed
/// occurrences become Pending on restart).
#[test]
fn a_kill_with_a_client_connected_publishes_its_will_after_the_restart() {
    let case = Case::new("will-connected");
    setup(&case, DURABLE);
    let broker = case.run("cut", DURABLE, &[]);
    let (target, present) = connect(broker.address, TARGET, true);
    assert!(present);
    broker.kill();
    drop(target);
    assert_eq!(
        restart(&case, DURABLE, false),
        Observed {
            client_saw_end_of_stream: false,
            ended_session_present: true,
            witness_session_present: true,
            monitor_received_will: true,
        }
    );
    assert_eq!(published_wills(&case.store()), 1);
}

/// Upstream ticket 2811 on this broker. Three QoS 1 messages wait for an
/// offline subscriber. It comes back, reads the first without
/// acknowledging it, and the broker is killed. After the restart it
/// receives the three, in order, the one it had read marked as a
/// redelivery ([MQTT-4.4.0-1]).
#[test]
fn queued_messages_survive_a_kill_with_one_delivery_in_flight() {
    let delivery = |stream: &mut TcpStream| -> (bool, Vec<u8>) {
        let (first, body) = read_frame(stream).unwrap();
        assert_eq!(first & 0xf6, 0x32, "a QoS 1 PUBLISH, got {first:#x}");
        let topic = usize::from(u16::from_be_bytes([body[0], body[1]]));
        let properties = usize::from(body[4 + topic]);
        (first & 0x08 != 0, body[5 + topic + properties..].to_vec())
    };
    let case = Case::new("queued");
    setup(&case, DURABLE);
    let broker = case.run("cut", DURABLE, &[]);
    let (mut publisher, _) = connect(broker.address, "durability-publisher", false);
    for id in 1..=3u8 {
        let mut body = Vec::new();
        text(STATUS.as_bytes(), &mut body);
        body.extend_from_slice(&[0, id, 0]);
        body.extend_from_slice(format!("queued-{id}").as_bytes());
        publisher.write_all(&packet(0x32, &body)).unwrap();
        let (first, puback) = read_frame(&mut publisher).unwrap();
        assert_eq!((first, &puback[..2]), (0x40, &[0, id][..]));
    }
    let (mut monitor, present) = connect(broker.address, MONITOR, false);
    assert!(present);
    assert_eq!(delivery(&mut monitor), (false, b"queued-1".to_vec()));
    broker.kill();
    drop((monitor, publisher));

    let broker = case.run("restart", DURABLE, &[]);
    let (mut monitor, present) = connect(broker.address, MONITOR, false);
    assert!(present);
    assert_eq!(delivery(&mut monitor), (true, b"queued-1".to_vec()));
    // The broker may have sent the two others before it was killed: they
    // can be marked as redeliveries too, which the client cannot tell.
    assert_eq!(delivery(&mut monitor).1, b"queued-2");
    assert_eq!(delivery(&mut monitor).1, b"queued-3");
    drop(monitor);
    broker.stop();
}

#[cfg(feature = "test-hooks")]
mod killed {
    use super::*;

    /// The `cut` run of a case: the writer is held at `point`, the client
    /// ends its session and reads the end of its stream, the store is read
    /// while the writer is held, and the process is killed there.
    fn cut(case: &Case, will: bool, point: &str) -> (bool, usize) {
        let broker = case.run("cut", DURABLE, &[point]);
        let saw_end = end_the_session(&broker, will);
        broker.held_at(point);
        let sessions = SqliteStore::inspect(&case.store()).unwrap().sessions.len();
        broker.kill();
        (saw_end, sessions)
    }

    /// The boundary the audit asked for. The client has read the end of
    /// its stream while the transaction that ends its session is still
    /// open: the kill rolls it back, and the session is resumed.
    #[test]
    fn a_kill_before_the_commit_returns_a_session_whose_client_saw_the_stream_end() {
        let case = Case::new("close-prepared");
        setup(&case, DURABLE);
        let (saw_end, sessions) = cut(&case, false, "session.close.prepared");
        assert_eq!(sessions, 3, "the close was visible before its commit");
        assert_eq!(
            restart(&case, DURABLE, saw_end),
            Observed {
                client_saw_end_of_stream: true,
                ended_session_present: true,
                witness_session_present: true,
                monitor_received_will: false,
            }
        );
    }

    /// Once COMMIT has returned the end of the session survives the kill,
    /// although the runtime never read the result.
    #[test]
    fn a_kill_after_the_commit_keeps_the_session_ended() {
        let case = Case::new("close-committed");
        setup(&case, DURABLE);
        let (saw_end, sessions) = cut(&case, false, "session.close.committed");
        assert_eq!(sessions, 2, "the close was not committed");
        assert_eq!(
            restart(&case, DURABLE, saw_end),
            Observed {
                client_saw_end_of_stream: true,
                ended_session_present: false,
                witness_session_present: true,
                monitor_received_will: false,
            }
        );
    }

    /// The same boundary for the Will. A normal DISCONNECT cancels it, but
    /// the cancellation is one transaction with the end of the session: a
    /// kill before its commit leaves the Will armed, and the restart
    /// publishes it (record 0034: surviving Armed occurrences become
    /// Pending on restart).
    #[test]
    fn a_kill_before_the_commit_publishes_the_will_a_normal_disconnect_cancelled() {
        let case = Case::new("will-prepared");
        setup(&case, DURABLE);
        let (saw_end, sessions) = cut(&case, true, "will.end.prepared");
        assert_eq!(sessions, 3, "the end was visible before its commit");
        assert_eq!(
            restart(&case, DURABLE, saw_end),
            Observed {
                client_saw_end_of_stream: true,
                ended_session_present: true,
                witness_session_present: true,
                monitor_received_will: true,
            }
        );
        assert_eq!(published_wills(&case.store()), 1);
    }

    /// After the commit the Will is cancelled for good.
    #[test]
    fn a_kill_after_the_commit_keeps_the_will_cancelled() {
        let case = Case::new("will-committed");
        setup(&case, DURABLE);
        let (saw_end, sessions) = cut(&case, true, "will.end.committed");
        assert_eq!(sessions, 2, "the end was not committed");
        assert_eq!(
            restart(&case, DURABLE, saw_end),
            Observed {
                client_saw_end_of_stream: true,
                ended_session_present: false,
                witness_session_present: true,
                monitor_received_will: false,
            }
        );
        let store = SqliteStore::inspect(&case.store()).unwrap();
        assert!(store.wills.is_empty());
        assert_eq!(published_wills(&case.store()), 0);
    }
}

mod secure {
    use super::*;

    fn run(suite: &str) {
        let case = Case::new(suite);
        let result = Command::new("python3")
            .args(["-I", "-B"])
            .arg(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("tests/disconnect_durability/recovery.py"),
            )
            .arg(env!("CARGO_BIN_EXE_mqttiumd"))
            .arg(&case.0)
            .arg(suite)
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{suite} failed: {}\n{}\n{}",
            result.status,
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
    }

    #[cfg(feature = "test-hooks")]
    #[test]
    fn sqlite_will_end_recovery_over_tcp_and_mtls() {
        run("durable");
    }

    #[cfg(feature = "test-hooks")]
    #[test]
    fn log_will_end_recovery_over_tcp_and_mtls() {
        run("log");
    }

    #[test]
    fn volatile_will_end_recovery_over_tcp_and_mtls() {
        run("volatile");
    }

    #[cfg(feature = "test-hooks")]
    #[test]
    fn volatile_will_end_before_memory_commit_over_tcp_and_mtls() {
        run("volatile-pre-apply");
    }

    #[cfg(feature = "test-hooks")]
    #[test]
    fn takeover_with_a_partially_written_tls_record() {
        run("takeover");
    }
}
