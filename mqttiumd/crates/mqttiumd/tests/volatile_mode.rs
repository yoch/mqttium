//! End-to-end integration test for `--storage-mode volatile` with periodic and
//! clean-shutdown atomic SQLite snapshots, verifying explicit `DUR-001`
//! readiness reporting and restart interoperability with durable SQLite mode.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mqttiumd_store_sqlite::SqliteStore;

static NEXT: AtomicU64 = AtomicU64::new(0);

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "mqttiumd-volatile-e2e-{name}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).expect("temp dir");
    dir
}

fn spawn_broker(db: &Path, events: &Path, extra_args: &[&str]) -> (Child, SocketAddr, String) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_mqttiumd"));
    cmd.args([
        "--db",
        db.to_str().unwrap(),
        "--listen",
        "127.0.0.1:0",
        "--events",
        events.to_str().unwrap(),
    ]);
    cmd.args(extra_args);
    let mut child = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn mqttiumd");
    let stdout = child.stdout.take().expect("stdout");
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    let read = reader.read_line(&mut line).expect("read readiness line");
    assert!(read > 0, "mqttiumd exited before ready");
    let rest = line
        .strip_prefix("mqttiumd ready ")
        .unwrap_or_else(|| panic!("unexpected readiness line: {line}"));
    let addr: SocketAddr = rest
        .split_whitespace()
        .next()
        .expect("address")
        .parse()
        .expect("socket addr");
    (child, addr, line)
}

fn stop_broker_gracefully(mut child: Child) {
    let pid = nix::unistd::Pid::from_raw(child.id() as i32);
    let _ = nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGINT);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            assert!(status.success(), "clean shutdown failed: {status}");
            return;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("broker did not exit cleanly after SIGINT");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn encode_remaining_length(mut value: usize, out: &mut Vec<u8>) {
    loop {
        let mut byte = (value % 128) as u8;
        value /= 128;
        if value > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
}

fn frame(first: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![first];
    encode_remaining_length(body.len(), &mut out);
    out.extend_from_slice(body);
    out
}

fn mqtt_str(s: &str, out: &mut Vec<u8>) {
    out.extend_from_slice(&(s.len() as u16).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
}

fn read_frame(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut first = [0u8; 1];
    stream.read_exact(&mut first).expect("read first byte");
    let mut remaining = 0usize;
    let mut shift = 0;
    loop {
        let mut b = [0u8; 1];
        stream.read_exact(&mut b).expect("read varint");
        remaining |= usize::from(b[0] & 0x7f) << shift;
        if b[0] & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    let mut body = vec![0u8; remaining];
    stream.read_exact(&mut body).expect("read body");
    (first[0], body)
}

fn connect_v5(addr: SocketAddr, client_id: &str, clean_start: bool, expiry: u32) -> TcpStream {
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .expect("timeout");
    let mut body = Vec::new();
    mqtt_str("MQTT", &mut body);
    body.push(5); // MQTT 5.0
    body.push(if clean_start { 0x02 } else { 0x00 });
    body.extend_from_slice(&60u16.to_be_bytes());
    // Properties: Session Expiry Interval (0x11)
    body.push(5);
    body.push(0x11);
    body.extend_from_slice(&expiry.to_be_bytes());
    mqtt_str(client_id, &mut body);
    stream.write_all(&frame(0x10, &body)).expect("send CONNECT");
    let (first, _) = read_frame(&mut stream);
    assert_eq!(first, 0x20, "expected CONNACK");
    stream
}

fn publish_qos1(stream: &mut TcpStream, packet_id: u16, topic: &str, payload: &[u8], retain: bool) {
    let mut body = Vec::new();
    mqtt_str(topic, &mut body);
    body.extend_from_slice(&packet_id.to_be_bytes());
    body.push(0); // property length 0
    body.extend_from_slice(payload);
    let first = 0x32 | u8::from(retain);
    stream
        .write_all(&frame(first, &body))
        .expect("send PUBLISH");
    let (ack_first, ack_body) = read_frame(stream);
    assert_eq!(ack_first, 0x40, "expected PUBACK");
    assert_eq!(&ack_body[..2], &packet_id.to_be_bytes());
}

fn subscribe_qos1(stream: &mut TcpStream, packet_id: u16, filter: &str) {
    let mut body = Vec::new();
    body.extend_from_slice(&packet_id.to_be_bytes());
    body.push(0); // property length 0
    mqtt_str(filter, &mut body);
    body.push(0x01); // QoS 1
    stream
        .write_all(&frame(0x82, &body))
        .expect("send SUBSCRIBE");
    let (ack_first, ack_body) = read_frame(stream);
    assert_eq!(ack_first, 0x90, "expected SUBACK");
    assert_eq!(&ack_body[..2], &packet_id.to_be_bytes());
}

#[test]
fn volatile_mode_reports_durability_saves_snapshot_and_reopens_in_durable_mode() {
    let dir = temp_dir("snap-restart");
    let db = dir.join("store.sqlite");
    let ev1 = dir.join("events-1.jsonl");
    let (child, addr, ready_line) = spawn_broker(
        &db,
        &ev1,
        &["--storage-mode", "volatile", "--snapshot-interval", "60"],
    );
    assert!(
        ready_line.contains("storage=volatile") && ready_line.contains("periodic_atomic_snapshot"),
        "DUR-001 readiness line must disclose volatile durability: {ready_line}"
    );

    // Publish a QoS 1 retained message and verify live delivery to a subscriber.
    let mut sub = connect_v5(addr, "sub-persistent", true, u32::MAX);
    subscribe_qos1(&mut sub, 1, "sensors/#");
    let mut pub_client = connect_v5(addr, "pub-1", true, 0);
    publish_qos1(&mut pub_client, 1, "sensors/temp", b"23.5C", true);

    let (pub_first, pub_body) = read_frame(&mut sub);
    assert_eq!(pub_first & 0xf0, 0x30);
    // Extract packet id and send PUBACK.
    let topic_len = usize::from(u16::from_be_bytes([pub_body[0], pub_body[1]]));
    assert_eq!(&pub_body[2..2 + topic_len], b"sensors/temp");
    let pi = &pub_body[2 + topic_len..4 + topic_len];
    sub.write_all(&frame(0x40, pi)).expect("send PUBACK");
    sub.write_all(&frame(0xe0, &[])).expect("disconnect sub");
    pub_client
        .write_all(&frame(0xe0, &[]))
        .expect("disconnect pub");
    drop(sub);
    drop(pub_client);

    // Graceful stop flushes the final atomic Schema-14 SQLite snapshot.
    stop_broker_gracefully(child);

    // Verify readiness event recorded explicit DUR-001 fields and snapshot_saved.
    let events_text = std::fs::read_to_string(&ev1).expect("read events");
    assert!(events_text.contains("\"storage_mode\":\"volatile\""));
    assert!(events_text.contains("\"durability_guarantee\":\"periodic_atomic_snapshot\""));
    assert!(events_text.contains("\"volatile_snapshot_saved\""));

    // Snapshot file is a genuine Schema-14 SQLite DB with our retained message and persistent session.
    let inspected = SqliteStore::inspect(&db).expect("inspect snapshot after stop");
    assert_eq!(inspected.retained.len(), 1);
    assert!(inspected.retained.contains_key(b"sensors/temp".as_slice()));

    // Restart the broker in DURABLE mode against the snapshot written by VOLATILE mode!
    let ev2 = dir.join("events-2.jsonl");
    let (child2, addr2, ready2) = spawn_broker(&db, &ev2, &["--storage-mode", "durable"]);
    assert!(!ready2.contains("storage=volatile"));
    let mut late_sub = connect_v5(addr2, "late-sub", true, 0);
    subscribe_qos1(&mut late_sub, 1, "sensors/#");
    let (ret_first, ret_body) = read_frame(&mut late_sub);
    assert_eq!(ret_first & 0xf1, 0x31, "expected retained PUBLISH replay");
    let tlen = usize::from(u16::from_be_bytes([ret_body[0], ret_body[1]]));
    assert_eq!(&ret_body[2..2 + tlen], b"sensors/temp");
    let prop_len = usize::from(ret_body[4 + tlen]);
    let payload = &ret_body[5 + tlen + prop_len..];
    assert_eq!(payload, b"23.5C");

    drop(late_sub);
    stop_broker_gracefully(child2);
    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(unix)]
#[test]
fn failed_final_volatile_snapshot_exits_nonzero_and_preserves_prior_snapshot() {
    use std::os::unix::fs::PermissionsExt;

    let dir = temp_dir("final-snap-fail");
    let db_dir = dir.join("db");
    std::fs::create_dir_all(&db_dir).unwrap();
    // Skip only if the effective user ignores 0o555 directory permissions (e.g. root).
    std::fs::set_permissions(&db_dir, std::fs::Permissions::from_mode(0o555)).unwrap();
    let probe = db_dir.join("perm-probe");
    let dir_is_enforced = std::fs::File::create(&probe).is_err();
    let _ = std::fs::remove_file(&probe);
    std::fs::set_permissions(&db_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    if !dir_is_enforced {
        let _ = std::fs::remove_dir_all(&dir);
        return;
    }
    let db = db_dir.join("store.sqlite");
    let ev1 = dir.join("events-1.jsonl");

    // Run 1: create an initial valid snapshot with explicit 0o600 permissions.
    let (child1, addr1, _) = spawn_broker(
        &db,
        &ev1,
        &["--storage-mode", "volatile", "--snapshot-interval", "60"],
    );
    let mut pub1 = connect_v5(addr1, "pub-seed", true, 0);
    publish_qos1(&mut pub1, 1, "keep/me", b"v1", true);
    drop(pub1);
    stop_broker_gracefully(child1);
    std::fs::set_permissions(&db, std::fs::Permissions::from_mode(0o600)).unwrap();

    // Run 2: reopen in volatile snapshot mode, mutate state, then make `db_dir`
    // read-only before SIGINT so the final atomic snapshot fails!
    let ev2 = dir.join("events-2.jsonl");
    let (mut child2, addr2, _) = spawn_broker(
        &db,
        &ev2,
        &["--storage-mode", "volatile", "--snapshot-interval", "60"],
    );
    let mut pub2 = connect_v5(addr2, "pub-next", true, 0);
    publish_qos1(&mut pub2, 1, "keep/me", b"v2", true);
    drop(pub2);

    let before_stop_bytes = std::fs::read(&db).unwrap();
    std::fs::set_permissions(&db_dir, std::fs::Permissions::from_mode(0o555)).unwrap();
    let pid = nix::unistd::Pid::from_raw(child2.id() as i32);
    let _ = nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGINT);
    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Some(status) = child2.try_wait().expect("try_wait") {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "broker did not exit after SIGINT"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    std::fs::set_permissions(&db_dir, std::fs::Permissions::from_mode(0o755)).unwrap();

    assert!(
        !status.success(),
        "failed final volatile snapshot must yield a non-zero exit code"
    );
    let events_text = std::fs::read_to_string(&ev2).expect("read events-2");
    assert!(
        events_text.contains("\"volatile_snapshot_failed\""),
        "expected volatile_snapshot_failed event: {events_text}"
    );
    assert!(
        events_text.contains("\"startup_or_runtime_refused\""),
        "expected startup_or_runtime_refused event on shutdown failure: {events_text}"
    );
    assert_eq!(
        std::fs::read(&db).unwrap(),
        before_stop_bytes,
        "prior snapshot must remain byte-for-byte intact after failed replacement"
    );
    let inspected = SqliteStore::inspect(&db).expect("inspect prior snapshot");
    let retained_pub = inspected
        .retained
        .get(b"keep/me".as_slice())
        .expect("v1 retained entry preserved")
        .publication;
    let payload_id = inspected.publications[&retained_pub].payload;
    assert_eq!(inspected.payloads[&payload_id].bytes, b"v1");
    assert_eq!(
        std::fs::metadata(&db).unwrap().permissions().mode() & 0o777,
        0o600
    );

    let _ = std::fs::remove_dir_all(&dir);
}
