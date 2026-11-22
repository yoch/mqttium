//! Real TCP regression test for QoS 2 PUBREL ordering (MQTT-4.6.0-4 /
//! MQTT-4.6.0-6) across protocol versions: an MQTT 5.0 publisher sends a QoS 2
//! message carrying a Message Expiry Interval followed by a second QoS 2
//! message without expiry to an MQTT 3.1.1 subscriber, verifying PUBREL order
//! matches PUBREC order through full PUBCOMP resolution of both exchanges.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

static NEXT: AtomicU64 = AtomicU64::new(0);

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "mqttiumd-qos2-order-{name}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).expect("temp dir");
    dir
}

fn spawn_broker(db: &Path, events: &Path, extra_args: &[&str]) -> (Child, SocketAddr) {
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
    let n = reader.read_line(&mut line).expect("read readiness");
    assert!(n > 0, "broker exited before readiness");
    let addr: SocketAddr = line
        .strip_prefix("mqttiumd ready ")
        .unwrap_or_else(|| panic!("unexpected ready line: {line}"))
        .split_whitespace()
        .next()
        .unwrap()
        .parse()
        .unwrap();
    (child, addr)
}

fn stop_broker(mut child: Child) {
    let pid = nix::unistd::Pid::from_raw(child.id() as i32);
    let _ = nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGINT);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "broker failed to exit cleanly: {status}");
            return;
        }
        assert!(Instant::now() < deadline, "broker shutdown timed out");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn encode_varint(mut n: usize, out: &mut Vec<u8>) {
    loop {
        let mut b = (n % 128) as u8;
        n /= 128;
        if n > 0 {
            b |= 0x80;
        }
        out.push(b);
        if n == 0 {
            break;
        }
    }
}

fn frame(first: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![first];
    encode_varint(body.len(), &mut out);
    out.extend_from_slice(body);
    out
}

fn mqtt_str(s: &str, out: &mut Vec<u8>) {
    out.extend_from_slice(&(s.len() as u16).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
}

fn read_frame(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut first = [0u8; 1];
    stream.read_exact(&mut first).expect("read packet type");
    let mut remaining = 0usize;
    let mut shift = 0;
    loop {
        let mut b = [0u8; 1];
        stream.read_exact(&mut b).expect("read remaining length");
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

fn connect_v5(addr: SocketAddr, client_id: &str) -> TcpStream {
    let mut s = TcpStream::connect(addr).expect("connect v5");
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut body = Vec::new();
    mqtt_str("MQTT", &mut body);
    body.push(5);
    body.push(0x02); // Clean Start
    body.extend_from_slice(&60u16.to_be_bytes());
    body.push(0); // Properties length = 0
    mqtt_str(client_id, &mut body);
    s.write_all(&frame(0x10, &body)).unwrap();
    let (ty, resp) = read_frame(&mut s);
    assert_eq!(ty, 0x20);
    assert_eq!(resp[1], 0);
    s
}

fn connect_v311(addr: SocketAddr, client_id: &str, clean_session: bool) -> TcpStream {
    let mut s = TcpStream::connect(addr).expect("connect v311");
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut body = Vec::new();
    mqtt_str("MQTT", &mut body);
    body.push(4); // MQTT 3.1.1
    body.push(if clean_session { 0x02 } else { 0x00 });
    body.extend_from_slice(&60u16.to_be_bytes());
    mqtt_str(client_id, &mut body);
    s.write_all(&frame(0x10, &body)).unwrap();
    let (ty, resp) = read_frame(&mut s);
    assert_eq!(ty, 0x20);
    assert_eq!(resp[1], 0);
    s
}

fn subscribe_v311(stream: &mut TcpStream, packet_id: u16, filter: &str, qos: u8) {
    let mut body = Vec::new();
    body.extend_from_slice(&packet_id.to_be_bytes());
    mqtt_str(filter, &mut body);
    body.push(qos);
    stream.write_all(&frame(0x82, &body)).unwrap();
    let (ty, resp) = read_frame(stream);
    assert_eq!(ty, 0x90);
    assert_eq!(&resp[..2], &packet_id.to_be_bytes());
    assert_eq!(resp[2], qos);
}

fn publish_qos2_v5(
    stream: &mut TcpStream,
    packet_id: u16,
    topic: &str,
    payload: &[u8],
    expiry_secs: Option<u32>,
) {
    let mut body = Vec::new();
    mqtt_str(topic, &mut body);
    body.extend_from_slice(&packet_id.to_be_bytes());
    if let Some(secs) = expiry_secs {
        body.push(5); // property length: 1 byte id + 4 byte u32
        body.push(0x02); // Message Expiry Interval
        body.extend_from_slice(&secs.to_be_bytes());
    } else {
        body.push(0);
    }
    body.extend_from_slice(payload);
    stream.write_all(&frame(0x34, &body)).unwrap(); // PUBLISH QoS 2

    // Complete publisher-side QoS 2 handshake (PUBREC -> PUBREL -> PUBCOMP).
    let (ty, rec) = read_frame(stream);
    assert_eq!(ty, 0x50, "expected PUBREC for publisher");
    assert_eq!(&rec[..2], &packet_id.to_be_bytes());
    stream
        .write_all(&frame(0x62, &packet_id.to_be_bytes()))
        .unwrap();
    let (ty, comp) = read_frame(stream);
    assert_eq!(ty, 0x70, "expected PUBCOMP for publisher");
    assert_eq!(&comp[..2], &packet_id.to_be_bytes());
}

fn read_v311_qos2_publish(stream: &mut TcpStream) -> (String, u16, Vec<u8>) {
    let (ty, body) = read_frame(stream);
    assert_eq!(ty & 0xf6, 0x34, "expected QoS 2 PUBLISH, got 0x{ty:02x}");
    let tlen = usize::from(u16::from_be_bytes([body[0], body[1]]));
    let topic = std::str::from_utf8(&body[2..2 + tlen]).unwrap().to_owned();
    let pid = u16::from_be_bytes([body[2 + tlen], body[3 + tlen]]);
    let payload = body[4 + tlen..].to_vec();
    (topic, pid, payload)
}

#[test]
fn mqtt5_publisher_with_expiry_to_mqtt311_subscriber_preserves_qos2_pubrel_order_over_tcp() {
    let dir = temp_dir("tcp-v5-to-v311");
    let db = dir.join("store.sqlite");
    let events = dir.join("events.jsonl");

    #[cfg(feature = "test-hooks")]
    let (child, addr) = spawn_broker(
        &db,
        &events,
        &[
            "--test-control",
            dir.to_str().unwrap(),
            "--test-pause",
            "delivery.started.prepared",
        ],
    );
    #[cfg(not(feature = "test-hooks"))]
    let (child, addr) = spawn_broker(&db, &events, &[]);

    // 1) Persistent MQTT 3.1.1 subscriber subscribes at QoS 2, then disconnects
    //    so both MQTT 5.0 publications queue cleanly in the store before delivery.
    {
        let mut sub_setup = connect_v311(addr, "sub-311", false);
        subscribe_v311(&mut sub_setup, 1, "order/qos2", 2);
        sub_setup.write_all(&[0xe0, 0x00]).unwrap();
    }

    // 2) MQTT 5.0 publisher publishes message 1 WITH Message Expiry Interval
    //    and message 2 WITHOUT expiry (both QoS 2).
    let mut pub5 = connect_v5(addr, "pub-v5");
    publish_qos2_v5(&mut pub5, 10, "order/qos2", b"first-expiring", Some(60));
    publish_qos2_v5(&mut pub5, 11, "order/qos2", b"second-plain", None);

    // 3) Subscriber reconnects and receives BOTH QoS 2 PUBLISHes in order.
    let mut sub = connect_v311(addr, "sub-311", false);
    let (t1, pid1, p1) = read_v311_qos2_publish(&mut sub);
    assert_eq!(t1, "order/qos2");
    assert_eq!(p1, b"first-expiring");
    let (t2, pid2, p2) = read_v311_qos2_publish(&mut sub);
    assert_eq!(t2, "order/qos2");
    assert_eq!(p2, b"second-plain");
    assert_ne!(pid1, pid2);

    // Under test-hooks, wait until MarkDeliveryStarted for message 1 hits the
    // barrier so message 1 is deterministically paused in Phase::StartPending.
    #[cfg(feature = "test-hooks")]
    {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let text = std::fs::read_to_string(&events).unwrap_or_default();
            if text.contains("\"point\":\"delivery.started.prepared\"") {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "expected delivery.started.prepared barrier to fire"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    // 4) Subscriber sends PUBREC(pid1) followed immediately by PUBREC(pid2).
    let mut both_pubrecs = frame(0x50, &pid1.to_be_bytes());
    both_pubrecs.extend_from_slice(&frame(0x50, &pid2.to_be_bytes()));
    sub.write_all(&both_pubrecs).unwrap();

    // Under test-hooks, wait briefly while message 1 is STILL paused in
    // StartPending (and message 2's PreparePubrel commits) to prove PUBREL(2)
    // does NOT sneak out ahead of PUBREL(1), then release the barrier.
    #[cfg(feature = "test-hooks")]
    {
        sub.set_read_timeout(Some(Duration::from_millis(150)))
            .unwrap();
        let mut probe = [0u8; 1];
        let premature = sub.read(&mut probe);
        assert!(
            premature.is_err(),
            "PUBREL(pid2) must not be emitted while PUBREC(pid1) is still in StartPending"
        );
        sub.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        std::fs::write(dir.join("release-delivery.started.prepared"), b"go").unwrap();
    }

    // 5) Broker MUST emit PUBREL(pid1) strictly before PUBREL(pid2).
    let (r1_ty, r1_body) = read_frame(&mut sub);
    assert_eq!(r1_ty, 0x62, "expected first PUBREL");
    assert_eq!(
        u16::from_be_bytes([r1_body[0], r1_body[1]]),
        pid1,
        "PUBREL for first PUBREC (pid1={pid1}) must leave before pid2={pid2}"
    );
    let (r2_ty, r2_body) = read_frame(&mut sub);
    assert_eq!(r2_ty, 0x62, "expected second PUBREL");
    assert_eq!(
        u16::from_be_bytes([r2_body[0], r2_body[1]]),
        pid2,
        "PUBREL for second PUBREC (pid2={pid2}) must follow pid1={pid1}"
    );

    // 6) Complete both QoS 2 exchanges via PUBCOMP and verify clean resolution.
    sub.write_all(&frame(0x70, &pid1.to_be_bytes())).unwrap();
    sub.write_all(&frame(0x70, &pid2.to_be_bytes())).unwrap();
    // Ping round-trip ensures the broker has processed both PUBCOMPs.
    sub.write_all(&[0xc0, 0x00]).unwrap();
    let (pt, _) = read_frame(&mut sub);
    assert_eq!(pt, 0xd0, "expected PINGRESP");
    sub.write_all(&[0xe0, 0x00]).unwrap();
    drop(sub);

    // Reconnect the persistent 3.1.1 session (clean_session=false) and verify
    // neither exchange is replayed (both resolved cleanly to completion).
    let mut sub_again = connect_v311(addr, "sub-311", false);
    sub_again.write_all(&[0xc0, 0x00]).unwrap();
    let (pt2, _) = read_frame(&mut sub_again);
    assert_eq!(
        pt2, 0xd0,
        "no unresolved QoS 2 PUBLISH or PUBREL should remain after both PUBCOMPs"
    );
    drop(sub_again);
    drop(pub5);

    stop_broker(child);

    // Inspect SQLite store directly: zero remaining obligations or outbound claims.
    let inspected = mqttiumd_store_sqlite::SqliteStore::inspect(&db).expect("inspect db");
    assert!(
        inspected.obligations.is_empty(),
        "both QoS 2 deliveries must be resolved in store"
    );
    assert!(
        inspected.outbound.is_empty(),
        "no outbound QoS 2 claims should remain"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
