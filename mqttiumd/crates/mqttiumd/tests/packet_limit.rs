//! MQTT-3.1.2-25 (MQTT 5.0) on the product executable: a delivery larger
//! than the subscriber's Maximum Packet Size is discarded as completed.
//!
//! This is the active successor of the frozen system reception
//! `tests/system/pubsub_reception.py::destination_packet_limit` (criterion
//! F-packet-limit), which asserts the rule that record 0025 replaced: the
//! connection was closed and the obligation kept for a larger connection.
//! Since the amendment of 21 November 2026 the delivery is discarded, the
//! connection stays open, the next delivery is sent, and a resumed session
//! is owed nothing for the discarded message.
//!
//! The test drives the real binary over loopback TCP with a minimal MQTT 5
//! client written here, at QoS 1 and at QoS 2, and reads the broker's event
//! file and its store beside the wire.
#![cfg(target_os = "linux")]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

const TOPIC: &str = "limit/t";
/// What the subscriber accepts at first, then after it resumes.
const SMALL_LIMIT: u32 = 64;
const LARGE_LIMIT: u32 = 1024;
const WAIT: Duration = Duration::from_secs(10);

struct Broker {
    child: Child,
    address: SocketAddr,
    directory: PathBuf,
}

impl Broker {
    /// Every event is written: `delivery_discarded` is only counted at the
    /// default level of the executable.
    fn start(name: &str) -> Self {
        static SERIAL: AtomicU32 = AtomicU32::new(0);
        let directory = std::env::temp_dir().join(format!(
            "mqttiumd-{name}-{}-{}",
            std::process::id(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).unwrap();
        // Another executable can be measured with the same test.
        let binary = std::env::var_os("MQTTIUMD_TEST_BINARY")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_mqttiumd")));
        let mut child = Command::new(binary)
            .arg("--db")
            .arg(directory.join("store.sqlite"))
            .arg("--listen")
            .arg("127.0.0.1:0")
            .arg("--events")
            .arg(directory.join("events.jsonl"))
            .args(["--event-level", "audit"])
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
            .and_then(|text| text.parse().ok())
            .unwrap_or_else(|| panic!("unexpected readiness line: {line:?}"));
        Self {
            child,
            address,
            directory,
        }
    }

    /// The events of one kind the broker has written so far.
    fn events(&self, kind: &str) -> Vec<serde_json::Value> {
        std::fs::read_to_string(self.directory.join("events.jsonl"))
            .unwrap_or_default()
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .filter(|event| event["kind"] == kind)
            .collect()
    }

    /// The deliveries the store still owes and the claims it holds on them.
    fn owed(&self) -> (usize, usize) {
        let state =
            mqttiumd_store_sqlite::SqliteStore::inspect(&self.directory.join("store.sqlite"))
                .unwrap();
        (state.obligations.len(), state.outbound.len())
    }

    /// Waits until `reached` holds; panics with `what` at the deadline.
    fn until(&self, what: &str, mut reached: impl FnMut(&Self) -> bool) {
        let deadline = Instant::now() + WAIT;
        while !reached(self) {
            assert!(Instant::now() < deadline, "{what}");
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.directory);
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

fn text(value: &str, out: &mut Vec<u8>) {
    out.extend_from_slice(&u16::try_from(value.len()).unwrap().to_be_bytes());
    out.extend_from_slice(value.as_bytes());
}

/// One packet as it came: its first byte, its body, and the bytes it took
/// on the wire, fixed header included.
struct Frame {
    first: u8,
    body: Vec<u8>,
    wire_bytes: usize,
}

fn read_frame(stream: &mut TcpStream) -> Frame {
    let mut first = [0u8; 1];
    stream.read_exact(&mut first).unwrap();
    let mut length = 0usize;
    let mut shift = 0;
    let mut wire_bytes = 1;
    loop {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).unwrap();
        wire_bytes += 1;
        length |= usize::from(byte[0] & 0x7f) << shift;
        if byte[0] & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    let mut body = vec![0; length];
    stream.read_exact(&mut body).unwrap();
    Frame {
        first: first[0],
        body,
        wire_bytes: wire_bytes + length,
    }
}

/// A connected MQTT 5 client and the Session Present flag of its CONNACK.
/// `session` is the Maximum Packet Size of a client that keeps its session
/// (Clean Start 0, Session Expiry Interval one hour); `None` is a client
/// with a clean session and no limit of its own.
fn connect(address: SocketAddr, client_id: &str, session: Option<u32>) -> (TcpStream, bool) {
    let mut stream = TcpStream::connect(address).unwrap();
    stream.set_nodelay(true).unwrap();
    stream.set_read_timeout(Some(WAIT)).unwrap();
    let mut properties = Vec::new();
    if let Some(maximum_packet_size) = session {
        properties.push(0x11);
        properties.extend_from_slice(&3600u32.to_be_bytes());
        properties.push(0x27);
        properties.extend_from_slice(&maximum_packet_size.to_be_bytes());
    }
    let mut body = Vec::new();
    text("MQTT", &mut body);
    // Level 5, Clean Start unless the session is kept, Keep Alive 60 s.
    body.extend_from_slice(&[5, if session.is_some() { 0x00 } else { 0x02 }, 0, 60]);
    body.push(u8::try_from(properties.len()).unwrap());
    body.extend_from_slice(&properties);
    text(client_id, &mut body);
    stream.write_all(&packet(0x10, &body)).unwrap();
    let connack = read_frame(&mut stream);
    assert_eq!(connack.first, 0x20, "CONNACK expected");
    assert_eq!(connack.body[1], 0, "CONNACK reason");
    (stream, connack.body[0] & 1 == 1)
}

fn subscribe(stream: &mut TcpStream, qos: u8) {
    let mut body = vec![0, 1, 0];
    text(TOPIC, &mut body);
    body.push(qos);
    stream.write_all(&packet(0x82, &body)).unwrap();
    let suback = read_frame(stream);
    assert_eq!(suback.first, 0x90, "SUBACK expected");
    assert_eq!(suback.body, [0, 1, 0, qos], "granted QoS {qos}");
}

/// A PUBLISH at QoS 1 or 2 with no property, as either side builds it.
fn publish_packet(qos: u8, packet_id: u16, payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    text(TOPIC, &mut body);
    body.extend_from_slice(&packet_id.to_be_bytes());
    body.push(0);
    body.extend_from_slice(payload);
    packet(0x30 | (qos << 1), &body)
}

/// Publishes and plays the publisher's side of the exchange to its end:
/// the broker has taken the publication when this returns.
fn publish(stream: &mut TcpStream, qos: u8, packet_id: u16, payload: &[u8]) {
    let id = packet_id.to_be_bytes();
    stream
        .write_all(&publish_packet(qos, packet_id, payload))
        .unwrap();
    let answer = read_frame(stream);
    let expected = if qos == 1 { 0x40 } else { 0x50 };
    assert_eq!(
        (answer.first, &answer.body[..2]),
        (expected, &id[..]),
        "the publication is acknowledged"
    );
    assert_eq!(
        answer.body.get(2).copied().unwrap_or(0),
        0,
        "the publication succeeds"
    );
    if qos == 2 {
        stream.write_all(&[0x62, 2, id[0], id[1]]).unwrap();
        let complete = read_frame(stream);
        assert_eq!((complete.first, &complete.body[..2]), (0x70, &id[..]));
    }
}

/// The next packet the subscriber reads is a first delivery of `payload`
/// at `qos` that fits `limit`; it is acknowledged to the end of its
/// exchange.
fn take(stream: &mut TcpStream, qos: u8, payload: &[u8], limit: u32, what: &str) {
    let delivery = read_frame(stream);
    assert_eq!(
        delivery.first,
        0x30 | (qos << 1),
        "{what}: a PUBLISH at QoS {qos} with DUP 0 and RETAIN 0, got {:#x} {:?}",
        delivery.first,
        delivery.body
    );
    assert!(
        delivery.wire_bytes <= limit as usize,
        "{what}: a packet of {} bytes for a client that accepts {limit}",
        delivery.wire_bytes
    );
    // Topic, Packet Identifier, no property, payload.
    let at = 2 + TOPIC.len();
    assert_eq!(&delivery.body[2..at], TOPIC.as_bytes(), "{what}: topic");
    let id = [delivery.body[at], delivery.body[at + 1]];
    assert_eq!(delivery.body[at + 2], 0, "{what}: no property");
    assert_eq!(&delivery.body[at + 3..], payload, "{what}: payload");
    if qos == 1 {
        stream.write_all(&[0x40, 2, id[0], id[1]]).unwrap();
    } else {
        stream.write_all(&[0x50, 2, id[0], id[1]]).unwrap();
        let release = read_frame(stream);
        assert_eq!(
            (release.first, &release.body[..2]),
            (0x62, &id[..]),
            "{what}: PUBREL"
        );
        stream.write_all(&[0x70, 2, id[0], id[1]]).unwrap();
    }
}

/// A PINGREQ is answered next: nothing else was written to this client,
/// and its connection is open.
fn answers_ping_next(stream: &mut TcpStream, what: &str) {
    stream.write_all(&[0xc0, 0]).unwrap();
    let answer = read_frame(stream);
    assert_eq!(
        (answer.first, answer.body.len()),
        (0xd0, 0),
        "{what}: PINGRESP expected next, got {:#x} {:?}",
        answer.first,
        answer.body
    );
}

fn discarded_story(qos: u8) {
    let broker = Broker::start(&format!("packet-limit-qos{qos}"));
    let subscriber_id = format!("small-subscriber-qos{qos}");
    let (mut subscriber, present) = connect(broker.address, &subscriber_id, Some(SMALL_LIMIT));
    assert!(!present, "a new session");
    subscribe(&mut subscriber, qos);
    let (mut publisher, _) = connect(broker.address, &format!("large-publisher-qos{qos}"), None);

    // 114 bytes on the wire for a client that accepts 64.
    let large = [b'x'; 100];
    assert!(publish_packet(qos, 1, &large).len() > SMALL_LIMIT as usize);
    publish(&mut publisher, qos, 1, &large);
    broker.until("no delivery was discarded", |broker| {
        !broker.events("delivery_discarded").is_empty()
    });
    let discards = broker.events("delivery_discarded");
    assert_eq!(discards.len(), 1, "{discards:?}");
    assert_eq!(discards[0]["details"]["reason"], "packet_too_large");
    assert_eq!(discards[0]["command_kind"], "Finish");
    broker.until(
        "the discarded delivery left an obligation or a claim",
        |broker| broker.owed() == (0, 0),
    );
    answers_ping_next(&mut subscriber, "after the discarded delivery");
    assert!(
        broker.events("connection_closed").is_empty(),
        "a connection was closed: {:?}",
        broker.events("connection_closed")
    );

    // What fits is delivered and acknowledged on the same connection.
    publish(&mut publisher, qos, 2, b"small");
    take(
        &mut subscriber,
        qos,
        b"small",
        SMALL_LIMIT,
        "the next delivery",
    );
    broker.until("the acknowledged delivery is still owed", |broker| {
        broker.owed() == (0, 0)
    });
    answers_ping_next(&mut subscriber, "after the acknowledged delivery");

    // The session resumes under a limit the discarded message would fit,
    // and is owed nothing for it.
    subscriber.write_all(&[0xe0, 0]).unwrap();
    let mut rest = Vec::new();
    subscriber.read_to_end(&mut rest).unwrap();
    assert!(
        rest.is_empty(),
        "nothing follows the DISCONNECT, got {rest:?}"
    );
    let (mut resumed, present) = connect(broker.address, &subscriber_id, Some(LARGE_LIMIT));
    assert!(present, "Session Present 1");
    assert!(publish_packet(qos, 1, &large).len() <= LARGE_LIMIT as usize);
    answers_ping_next(&mut resumed, "after the session resumed");
    publish(&mut publisher, qos, 3, b"next");
    take(
        &mut resumed,
        qos,
        b"next",
        LARGE_LIMIT,
        "the first PUBLISH of the resumed session",
    );
    broker.until("the last delivery is still owed", |broker| {
        broker.owed() == (0, 0)
    });
    answers_ping_next(&mut resumed, "at the end");
    assert_eq!(
        broker.events("delivery_discarded").len(),
        1,
        "one delivery was discarded in all"
    );
}

/// A QoS 1 and a QoS 2 delivery over the subscriber's Maximum Packet Size
/// are never written to it, are recorded as discarded for `packet_too_large`
/// and leave no obligation; the connection stays open, the next delivery
/// is sent within the limit, and the session resumed under a larger limit
/// receives no redelivery: its first PUBLISH is a new message.
#[test]
fn a_delivery_over_the_maximum_packet_size_is_discarded_and_the_session_goes_on() {
    for qos in [1, 2] {
        discarded_story(qos);
    }
}
