//! End-to-end integration tests for MQTT 5 PUBLISH application properties
//! (ADR 0053): Payload Format Indicator (0x01), Content Type (0x03),
//! Response Topic (0x08), Correlation Data (0x09), and ordered User Properties
//! (0x26), across QoS 0/1/2, MQTT 3.1.1 stripping, PFI=1 UTF-8 validation,
//! retained restart persistence (durable SQLite & volatile snapshot), and
//! Maximum Packet Size accounting.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mqttiumd::codec::{self, Bounds, Packet, Version};
use mqttiumd_contract::packet::{InboundPacket, Qos};

static NEXT: AtomicU64 = AtomicU64::new(0);

const BOUNDS: Bounds = Bounds {
    max_packet: mqttiumd::net::MAX_PACKET_BYTES,
    max_client_id: 1024,
    max_topic: 1024,
    max_payload: 1_048_576,
};

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "mqttiumd-props-e2e-{name}-{}-{}",
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
    (child, addr)
}

fn stop_broker(mut child: Child) {
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
            panic!("broker did not exit cleanly");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn encode_varint(mut value: usize, out: &mut Vec<u8>) {
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
    encode_varint(body.len(), &mut out);
    out.extend_from_slice(body);
    out
}

fn mqtt_str(s: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(&(s.len() as u16).to_be_bytes());
    out.extend_from_slice(s);
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

fn connect_v5(addr: SocketAddr, client_id: &str, max_packet: Option<u32>) -> TcpStream {
    let mut stream = TcpStream::connect(addr).expect("connect v5");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut props = Vec::new();
    props.extend_from_slice(&[0x11, 0xff, 0xff, 0xff, 0xff]);
    if let Some(max) = max_packet {
        props.push(0x27);
        props.extend_from_slice(&max.to_be_bytes());
    }
    let mut body = b"\0\x04MQTT\x05\x02\0\x3c".to_vec();
    encode_varint(props.len(), &mut body);
    body.extend_from_slice(&props);
    mqtt_str(client_id.as_bytes(), &mut body);
    stream.write_all(&frame(0x10, &body)).unwrap();
    let (first, resp) = read_frame(&mut stream);
    assert_eq!(first, 0x20);
    assert_eq!(resp[1], 0);
    stream
}

fn connect_v311(addr: SocketAddr, client_id: &str) -> TcpStream {
    let mut stream = TcpStream::connect(addr).expect("connect v311");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut body = b"\0\x04MQTT\x04\x02\0\x3c".to_vec();
    mqtt_str(client_id.as_bytes(), &mut body);
    stream.write_all(&frame(0x10, &body)).unwrap();
    let (first, resp) = read_frame(&mut stream);
    assert_eq!(first, 0x20);
    assert_eq!(resp, [0, 0]);
    stream
}

fn subscribe_v5(stream: &mut TcpStream, packet_id: u16, filter: &str, qos: u8) {
    let mut body = packet_id.to_be_bytes().to_vec();
    body.push(0); // properties len 0
    mqtt_str(filter.as_bytes(), &mut body);
    body.push(qos);
    stream.write_all(&frame(0x82, &body)).unwrap();
    let (first, resp) = read_frame(stream);
    assert_eq!(first, 0x90);
    assert_eq!(&resp[..2], &packet_id.to_be_bytes());
}

fn subscribe_v311(stream: &mut TcpStream, packet_id: u16, filter: &str, qos: u8) {
    let mut body = packet_id.to_be_bytes().to_vec();
    mqtt_str(filter.as_bytes(), &mut body);
    body.push(qos);
    stream.write_all(&frame(0x82, &body)).unwrap();
    let (first, resp) = read_frame(stream);
    assert_eq!(first, 0x90);
    assert_eq!(&resp[..2], &packet_id.to_be_bytes());
}

fn build_all_props() -> Vec<u8> {
    let mut props = Vec::new();
    // Payload Format Indicator = 1 (UTF-8)
    props.extend_from_slice(&[0x01, 1]);
    // Content Type
    props.push(0x03);
    mqtt_str(b"application/json", &mut props);
    // Response Topic
    props.push(0x08);
    mqtt_str(b"rpc/replies/client-1", &mut props);
    // Correlation Data (raw binary including NUL and 0xff)
    props.push(0x09);
    mqtt_str(&[0x00, 0xde, 0xad, 0xbe, 0xef, 0xff], &mut props);
    // Ordered User Properties with repeated keys (MQTT-3.3.2-17)
    for (k, v) in [
        (&b"trace"[..], &b"hop-1"[..]),
        (&b"trace"[..], &b"hop-2"[..]),
        (&b"arch"[..], &b"aarch64"[..]),
    ] {
        props.push(0x26);
        mqtt_str(k, &mut props);
        mqtt_str(v, &mut props);
    }
    props
}

fn publish_v5(
    stream: &mut TcpStream,
    topic: &str,
    qos: u8,
    retain: bool,
    packet_id: Option<u16>,
    props: &[u8],
    payload: &[u8],
) {
    let first = 0x30 | (qos << 1) | u8::from(retain);
    let mut body = Vec::new();
    mqtt_str(topic.as_bytes(), &mut body);
    if let Some(pi) = packet_id {
        body.extend_from_slice(&pi.to_be_bytes());
    }
    encode_varint(props.len(), &mut body);
    body.extend_from_slice(props);
    body.extend_from_slice(payload);
    stream.write_all(&frame(first, &body)).unwrap();
}

fn assert_all_props(packet: Packet, expected_qos: Qos, expected_payload: &[u8]) {
    let Packet::Core(InboundPacket::Publish {
        qos,
        payload,
        properties: Some(props),
        ..
    }) = packet
    else {
        panic!("expected PUBLISH with properties, got {packet:?}");
    };
    assert_eq!(qos, expected_qos);
    assert_eq!(payload.as_slice(), expected_payload);
    assert_eq!(props.payload_format_indicator, Some(1));
    assert_eq!(
        props.content_type.as_ref().map(|b| b.as_slice()),
        Some(&b"application/json"[..])
    );
    assert_eq!(
        props.response_topic.as_ref().map(|b| b.as_slice()),
        Some(&b"rpc/replies/client-1"[..])
    );
    assert_eq!(
        props.correlation_data.as_ref().map(|b| b.as_slice()),
        Some(&[0x00, 0xde, 0xad, 0xbe, 0xef, 0xff][..])
    );
    let user: Vec<(&[u8], &[u8])> = props
        .user_properties
        .iter()
        .map(|(k, v)| (k.as_slice(), v.as_slice()))
        .collect();
    assert_eq!(
        user,
        vec![
            (&b"trace"[..], &b"hop-1"[..]),
            (&b"trace"[..], &b"hop-2"[..]),
            (&b"arch"[..], &b"aarch64"[..]),
        ]
    );
}

#[test]
fn publish_properties_traverse_qos0_qos1_qos2_and_strip_for_v311() {
    let dir = temp_dir("end-to-end");
    let db = dir.join("broker.sqlite");
    let events = dir.join("events.jsonl");
    let (child, addr) = spawn_broker(&db, &events, &[]);

    let mut sub_v5 = connect_v5(addr, "sub-v5", None);
    let mut sub_v3 = connect_v311(addr, "sub-v311");
    let mut pub_v5 = connect_v5(addr, "pub-v5", None);

    subscribe_v5(&mut sub_v5, 1, "rpc/req/#", 2);
    subscribe_v311(&mut sub_v3, 1, "rpc/req/#", 2);

    let props = build_all_props();

    // 1. QoS 0 with all 5 property types (including newline in UTF-8 JSON payload).
    let payload0 = b"{\"op\":\"ping\"}\n";
    publish_v5(&mut pub_v5, "rpc/req/q0", 0, false, None, &props, payload0);

    let (f5, b5) = read_frame(&mut sub_v5);
    assert_all_props(
        codec::decode(f5, &b5, BOUNDS).unwrap(),
        Qos::AtMostOnce,
        payload0,
    );

    let (f3, b3) = read_frame(&mut sub_v3);
    let Packet::Core(InboundPacket::Publish {
        topic,
        qos,
        payload,
        properties,
        ..
    }) = codec::decode_for(Version::V311, f3, &b3, BOUNDS).unwrap()
    else {
        panic!("v311 publish");
    };
    assert_eq!(topic.as_slice(), b"rpc/req/q0");
    assert_eq!(qos, Qos::AtMostOnce);
    assert_eq!(payload.as_slice(), payload0);
    assert_eq!(properties, None);

    // 2. QoS 1 with all 5 property types.
    let payload1 = b"{\"op\":\"qos1\"}";
    publish_v5(
        &mut pub_v5,
        "rpc/req/q1",
        1,
        false,
        Some(10),
        &props,
        payload1,
    );
    let (ack_f, ack_b) = read_frame(&mut pub_v5);
    assert_eq!(ack_f, 0x40);
    assert_eq!(&ack_b[..2], &10u16.to_be_bytes());

    let (f5, b5) = read_frame(&mut sub_v5);
    let decoded1 = codec::decode(f5, &b5, BOUNDS).unwrap();
    let Packet::Core(InboundPacket::Publish {
        packet_id: Some(pi),
        ..
    }) = &decoded1
    else {
        panic!("qos1 pi");
    };
    let pi_raw = pi.get();
    assert_all_props(decoded1, Qos::AtLeastOnce, payload1);
    sub_v5
        .write_all(&frame(0x40, &pi_raw.to_be_bytes()))
        .unwrap();

    let (f3, b3) = read_frame(&mut sub_v3);
    let Packet::Core(InboundPacket::Publish {
        packet_id: Some(pi3),
        payload,
        properties,
        ..
    }) = codec::decode_for(Version::V311, f3, &b3, BOUNDS).unwrap()
    else {
        panic!("v311 qos1");
    };
    assert_eq!(payload.as_slice(), payload1);
    assert_eq!(properties, None);
    sub_v3
        .write_all(&frame(0x40, &pi3.get().to_be_bytes()))
        .unwrap();

    // 3. QoS 2 with all 5 property types.
    let payload2 = b"{\"op\":\"qos2\"}";
    publish_v5(
        &mut pub_v5,
        "rpc/req/q2",
        2,
        false,
        Some(20),
        &props,
        payload2,
    );
    let (rec_f, rec_b) = read_frame(&mut pub_v5);
    assert_eq!(rec_f, 0x50);
    assert_eq!(&rec_b[..2], &20u16.to_be_bytes());
    pub_v5
        .write_all(&frame(0x62, &20u16.to_be_bytes()))
        .unwrap();
    let (comp_f, comp_b) = read_frame(&mut pub_v5);
    assert_eq!(comp_f, 0x70);
    assert_eq!(&comp_b[..2], &20u16.to_be_bytes());

    let (f5, b5) = read_frame(&mut sub_v5);
    let decoded2 = codec::decode(f5, &b5, BOUNDS).unwrap();
    let Packet::Core(InboundPacket::Publish {
        packet_id: Some(pi2),
        ..
    }) = &decoded2
    else {
        panic!("qos2 pi");
    };
    let pi2_raw = pi2.get();
    assert_all_props(decoded2, Qos::ExactlyOnce, payload2);
    sub_v5
        .write_all(&frame(0x50, &pi2_raw.to_be_bytes()))
        .unwrap();
    let (rel_f, rel_b) = read_frame(&mut sub_v5);
    assert_eq!(rel_f, 0x62);
    assert_eq!(&rel_b[..2], &pi2_raw.to_be_bytes());
    sub_v5
        .write_all(&frame(0x70, &pi2_raw.to_be_bytes()))
        .unwrap();

    // 4. PFI=1 with invalid UTF-8 payload is rejected with DISCONNECT 0x99.
    let mut bad_pub = connect_v5(addr, "bad-utf8", None);
    publish_v5(
        &mut bad_pub,
        "rpc/req/bad",
        1,
        false,
        Some(1),
        &[0x01, 1],
        &[0xff, 0xfe, 0x80],
    );
    let (disc_f, disc_b) = read_frame(&mut bad_pub);
    assert_eq!(disc_f, 0xe0);
    assert_eq!(disc_b[0], 0x99);

    drop(sub_v5);
    drop(sub_v3);
    drop(pub_v5);
    drop(bad_pub);
    stop_broker(child);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn retained_publish_properties_survive_volatile_snapshot_and_durable_restart() {
    let dir = temp_dir("retained-restart");
    let db = dir.join("broker.sqlite");
    let events1 = dir.join("events1.jsonl");
    let events2 = dir.join("events2.jsonl");

    // 1. Start in volatile mode with snapshot interval, publish retained QoS 1 with all properties.
    let (child1, addr1) = spawn_broker(
        &db,
        &events1,
        &["--storage-mode", "volatile", "--snapshot-interval", "60"],
    );
    let mut pub_v5 = connect_v5(addr1, "ret-pub", None);
    let props = build_all_props();
    let payload = b"{\"retained\":true}";
    publish_v5(
        &mut pub_v5,
        "state/device-1",
        1,
        true,
        Some(1),
        &props,
        payload,
    );
    let (ack_f, _) = read_frame(&mut pub_v5);
    assert_eq!(ack_f, 0x40);
    drop(pub_v5);
    // Clean shutdown flushes atomic Schema-14 SQLite snapshot with properties column.
    stop_broker(child1);

    // 2. Restart in durable SQLite mode and verify retained replay delivers exact properties.
    let (child2, addr2) = spawn_broker(&db, &events2, &[]);
    let mut sub_v5 = connect_v5(addr2, "ret-sub", None);
    subscribe_v5(&mut sub_v5, 1, "state/#", 1);
    let (f5, b5) = read_frame(&mut sub_v5);
    assert_eq!(f5 & 1, 1, "retained replay flag must be set");
    let decoded = codec::decode(f5, &b5, BOUNDS).unwrap();
    assert_all_props(decoded, Qos::AtLeastOnce, payload);

    drop(sub_v5);
    stop_broker(child2);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn subscriber_maximum_packet_size_accounts_for_publish_properties() {
    let dir = temp_dir("max-packet-props");
    let db = dir.join("broker.sqlite");
    let events = dir.join("events.jsonl");
    let (child, addr) = spawn_broker(&db, &events, &[]);

    // Small subscriber accepts at most 40 bytes: topic "t" + payload "ok" fits
    // (7 bytes), but adding ~90 bytes of properties exceeds 40 bytes and must
    // be discarded rather than written or closing the connection.
    let mut small = connect_v5(addr, "small-sub", Some(40));
    subscribe_v5(&mut small, 1, "t", 1);

    let mut publisher = connect_v5(addr, "prop-pub", None);
    let props = build_all_props();
    publish_v5(&mut publisher, "t", 1, false, Some(1), &props, b"too-big");
    let (ack1, _) = read_frame(&mut publisher);
    assert_eq!(ack1, 0x40);

    // Second message without properties fits easily and must arrive intact.
    publish_v5(&mut publisher, "t", 1, false, Some(2), &[], b"fits");
    let (ack2, _) = read_frame(&mut publisher);
    assert_eq!(ack2, 0x40);

    let (f, b) = read_frame(&mut small);
    let Packet::Core(InboundPacket::Publish {
        payload,
        properties,
        ..
    }) = codec::decode(f, &b, BOUNDS).unwrap()
    else {
        panic!("expected second message");
    };
    assert_eq!(payload.as_slice(), b"fits");
    assert_eq!(properties, None);

    drop(small);
    drop(publisher);
    stop_broker(child);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn invalid_response_topic_and_content_type_are_rejected_over_tcp() {
    let dir = temp_dir("invalid-props");
    let db = dir.join("broker.sqlite");
    let events = dir.join("events.jsonl");
    let (child, addr) = spawn_broker(&db, &events, &[]);

    // 1. MQTT-3.3.2-13: Response Topic (0x08) MUST be valid UTF-8.
    {
        let mut client = connect_v5(addr, "bad-rt-utf8", None);
        let mut props = Vec::new();
        props.push(0x08);
        mqtt_str(&[0xff, 0xfe], &mut props);
        publish_v5(&mut client, "req/1", 1, false, Some(1), &props, b"hello");
        let (f, b) = read_frame(&mut client);
        assert_eq!(
            f, 0xe0,
            "expected DISCONNECT on invalid UTF-8 response topic"
        );
        assert_eq!(b[0], 0x81, "expected Malformed Packet (0x81)");
    }

    // 2. MQTT-3.3.2-14: Response Topic (0x08) MUST NOT contain '+' or '#'.
    for (idx, bad_rt) in [b"replies/+".as_slice(), b"replies/#".as_slice()]
        .into_iter()
        .enumerate()
    {
        let mut client = connect_v5(addr, &format!("bad-rt-wild-{idx}"), None);
        let mut props = Vec::new();
        props.push(0x08);
        mqtt_str(bad_rt, &mut props);
        publish_v5(&mut client, "req/2", 1, false, Some(1), &props, b"hello");
        let (f, b) = read_frame(&mut client);
        assert_eq!(f, 0xe0, "expected DISCONNECT on wildcard response topic");
        assert!(
            matches!(b[0], 0x82 | 0x90),
            "expected Protocol Error / Topic Name invalid, got 0x{:02x}",
            b[0]
        );
    }

    // 3. MQTT-3.3.2-19: Content Type (0x03) MUST be valid UTF-8.
    {
        let mut client = connect_v5(addr, "bad-ct-utf8", None);
        let mut props = Vec::new();
        props.push(0x03);
        mqtt_str(b"text/\xff", &mut props);
        publish_v5(&mut client, "req/3", 1, false, Some(1), &props, b"hello");
        let (f, b) = read_frame(&mut client);
        assert_eq!(f, 0xe0, "expected DISCONNECT on invalid UTF-8 content type");
        assert_eq!(b[0], 0x81, "expected Malformed Packet (0x81)");
    }

    stop_broker(child);
    let _ = std::fs::remove_dir_all(&dir);
}
