//! How the executable classes packets a client must not send, read on the
//! wire. Each test answers a signalled behaviour of another broker that the
//! external audit of `47e1d12` left partly verified on this one (report
//! 0034): the reason code a client reads, whether the connection goes on,
//! and what becomes of the Will.
//!
//! A real executable over loopback TCP, with a minimal client written here.
#![cfg(target_os = "linux")]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

struct Broker {
    child: Child,
    address: SocketAddr,
    directory: PathBuf,
}

impl Broker {
    fn start(name: &str) -> Self {
        static SERIAL: AtomicU32 = AtomicU32::new(0);
        let directory = std::env::temp_dir().join(format!(
            "mqttiumd-classes-{name}-{}-{}",
            std::process::id(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_mqttiumd"))
            .arg("--db")
            .arg(directory.join("store.sqlite"))
            .args(["--listen", "127.0.0.1:0"])
            .arg("--events")
            .arg(directory.join("events.jsonl"))
            .args(["--event-level", "audit", "--max-connections", "64"])
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

    /// The refusals the broker recorded, as (code, reason).
    fn refused(&self) -> Vec<(String, u64)> {
        std::fs::read_to_string(self.directory.join("events.jsonl"))
            .unwrap_or_default()
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .filter(|event| event["kind"] == "packet_refused")
            .map(|event| {
                (
                    event["details"]["code"].as_str().unwrap_or("").to_owned(),
                    event["details"]["reason"].as_u64().unwrap_or(0),
                )
            })
            .collect()
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

fn text(value: &str, out: &mut Vec<u8>) {
    out.extend_from_slice(&u16::try_from(value.len()).unwrap().to_be_bytes());
    out.extend_from_slice(value.as_bytes());
}

fn packet(first: u8, body: &[u8]) -> Vec<u8> {
    assert!(body.len() < 128, "one length byte is enough here");
    let mut out = vec![first, body.len() as u8];
    out.extend_from_slice(body);
    out
}

fn read_packet(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut head = [0u8; 2];
    stream.read_exact(&mut head).unwrap();
    let mut body = vec![0; usize::from(head[1])];
    stream.read_exact(&mut body).unwrap();
    (head[0], body)
}

fn socket(address: SocketAddr) -> TcpStream {
    let stream = TcpStream::connect(address).unwrap();
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream
}

/// An MQTT 5 client with a clean session, the given CONNECT properties and,
/// when asked, a QoS 1 Will on `classes/will`.
fn connect_v5(address: SocketAddr, client_id: &str, properties: &[u8], will: bool) -> TcpStream {
    let mut stream = socket(address);
    let mut body = Vec::new();
    text("MQTT", &mut body);
    body.extend_from_slice(&[5, if will { 0x0e } else { 0x02 }, 0, 60]);
    body.push(u8::try_from(properties.len()).unwrap());
    body.extend_from_slice(properties);
    text(client_id, &mut body);
    if will {
        body.push(0);
        text("classes/will", &mut body);
        text("gone", &mut body);
    }
    stream.write_all(&packet(0x10, &body)).unwrap();
    let (first, connack) = read_packet(&mut stream);
    assert_eq!((first, connack[1]), (0x20, 0), "CONNACK of {client_id}");
    stream
}

fn connect_v311(address: SocketAddr, client_id: &str) -> TcpStream {
    let mut stream = socket(address);
    let mut body = Vec::new();
    text("MQTT", &mut body);
    body.extend_from_slice(&[4, 0x02, 0, 60]);
    text(client_id, &mut body);
    stream.write_all(&packet(0x10, &body)).unwrap();
    assert_eq!(read_packet(&mut stream), (0x20, vec![0, 0]), "CONNACK");
    stream
}

/// Everything the broker sends until it ends the stream.
fn until_close(stream: &mut TcpStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut chunk = [0u8; 256];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => return bytes,
            Ok(count) => bytes.extend_from_slice(&chunk[..count]),
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => return bytes,
            Err(error) => panic!("the broker neither answered nor closed: {error}"),
        }
    }
}

/// Whether nothing reaches the client for a while. The stream stays open.
fn silent(stream: &mut TcpStream) -> bool {
    stream
        .set_read_timeout(Some(Duration::from_millis(400)))
        .unwrap();
    let mut byte = [0u8; 1];
    let outcome = stream.read(&mut byte);
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    matches!(&outcome, Err(error) if matches!(
        error.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
    ))
}

fn subscribe_v5(stream: &mut TcpStream, filter: &str, qos: u8) {
    let mut body = vec![0, 1, 0];
    text(filter, &mut body);
    body.push(qos);
    stream.write_all(&packet(0x82, &body)).unwrap();
    assert_eq!(read_packet(stream), (0x90, vec![0, 1, 0, qos]), "SUBACK");
}

fn publish_body(topic: &str, id: Option<u16>, v5: bool) -> Vec<u8> {
    let mut body = Vec::new();
    text(topic, &mut body);
    if let Some(id) = id {
        body.extend_from_slice(&id.to_be_bytes());
    }
    if v5 {
        body.push(0);
    }
    body.extend_from_slice(b"payload");
    body
}

/// Upstream tickets 3589 and 3613. Reserved bits of the fixed header are a
/// Malformed Packet on every packet type a client sends ([MQTT-2.1.3-1],
/// [MQTT-3.14.1-1]): MQTT 5 reads DISCONNECT 0x81, MQTT 3.1.1 has no reason
/// code and reads the end of the stream.
#[test]
fn reserved_fixed_header_flags_are_a_malformed_packet_on_every_packet_type() {
    let broker = Broker::start("flags");
    let mut subscribe = vec![0, 1];
    text("classes/t", &mut subscribe);
    subscribe.push(1);
    let mut unsubscribe = vec![0, 1];
    text("classes/t", &mut unsubscribe);
    let cases: [(&str, u8, Vec<u8>); 9] = [
        ("SUBSCRIBE with flags 0", 0x80, subscribe),
        ("UNSUBSCRIBE with flags 0", 0xa0, unsubscribe),
        ("PUBREL with flags 0", 0x60, vec![0, 1]),
        ("PUBACK with flags 1", 0x41, vec![0, 1]),
        ("PUBREC with flags 1", 0x51, vec![0, 1]),
        ("PUBCOMP with flags 1", 0x71, vec![0, 1]),
        ("PINGREQ with flags 1", 0xc1, vec![]),
        ("DISCONNECT with flags 1", 0xe1, vec![]),
        (
            "PUBLISH at QoS 3",
            0x36,
            publish_body("classes/t", Some(1), false),
        ),
    ];
    for (index, (name, first, body)) in cases.iter().enumerate() {
        let mut modern = connect_v5(broker.address, &format!("flags-5-{index}"), &[], false);
        // The MQTT 5 body has its property length after the identifier.
        let mut body5 = body.clone();
        match first {
            0x80 | 0xa0 => body5.insert(2, 0),
            0x36 => body5 = publish_body("classes/t", Some(1), true),
            _ => {}
        }
        modern.write_all(&packet(*first, &body5)).unwrap();
        assert_eq!(
            until_close(&mut modern),
            [0xe0, 2, 0x81, 0],
            "MQTT 5: {name}"
        );
        let mut legacy = connect_v311(broker.address, &format!("flags-4-{index}"));
        legacy.write_all(&packet(*first, body)).unwrap();
        assert_eq!(until_close(&mut legacy), [0u8; 0], "MQTT 3.1.1: {name}");
    }
    assert!(
        broker.refused().iter().all(|(_, reason)| *reason == 0x81),
        "{:?}",
        broker.refused()
    );
}

/// Upstream ticket 2681. A Packet Identifier of zero on a QoS 1 PUBLISH is
/// a Protocol Error ([MQTT-2.2.1-3], [MQTT-2.3.1-1] in 3.1.1).
#[test]
fn a_packet_identifier_of_zero_is_a_protocol_error() {
    let broker = Broker::start("identifier-zero");
    let mut modern = connect_v5(broker.address, "zero-5", &[], false);
    modern
        .write_all(&packet(0x32, &publish_body("classes/t", Some(0), true)))
        .unwrap();
    assert_eq!(until_close(&mut modern), [0xe0, 2, 0x82, 0]);
    let mut legacy = connect_v311(broker.address, "zero-4");
    legacy
        .write_all(&packet(0x32, &publish_body("classes/t", Some(0), false)))
        .unwrap();
    assert_eq!(until_close(&mut legacy), [0u8; 0]);
}

/// Upstream ticket 3743. A DISCONNECT whose reason code a client may not
/// send (undefined, or reserved to the server) is a Protocol Error, not a
/// normal disconnection: the client reads DISCONNECT 0x82 and its Will is
/// published. The witness, reason 0x00, publishes none (§3.14.2.1,
/// §3.1.2.5).
#[test]
fn an_illegal_disconnect_reason_is_a_protocol_error_and_publishes_the_will() {
    let broker = Broker::start("illegal-disconnect");
    for (reason, answer, will) in [
        (0x05u8, &[0xe0, 2, 0x82, 0][..], true),
        (0x87, &[0xe0, 2, 0x82, 0][..], true),
        (0x00, &[][..], false),
    ] {
        let mut monitor = connect_v5(broker.address, "will-monitor", &[], false);
        subscribe_v5(&mut monitor, "classes/will", 1);
        let mut client = connect_v5(broker.address, "will-owner", &[], true);
        client.write_all(&packet(0xe0, &[reason])).unwrap();
        assert_eq!(until_close(&mut client), answer, "reason {reason:#04x}");
        if will {
            let (first, body) = read_packet(&mut monitor);
            assert_eq!(first & 0xf0, 0x30, "reason {reason:#04x}");
            assert!(body.ends_with(b"gone"), "reason {reason:#04x}");
        }
        assert!(
            silent(&mut monitor),
            "reason {reason:#04x}: one Will at most"
        );
    }
}

/// Upstream ticket 3614. A CONNECT whose reserved flag is set is a
/// Malformed Packet ([MQTT-3.1.2-3]). It is not read to its end, so it
/// gives no limit for an answer: the broker closes without a CONNACK, which
/// the specification allows, and records the class.
#[test]
fn a_reserved_connect_flag_is_closed_without_a_connack_and_recorded_as_malformed() {
    let broker = Broker::start("connect-flag");
    for (level, properties) in [(5u8, &[0u8][..]), (4, &[][..])] {
        let mut stream = socket(broker.address);
        let mut body = Vec::new();
        text("MQTT", &mut body);
        body.extend_from_slice(&[level, 0x03, 0, 60]);
        body.extend_from_slice(properties);
        text("reserved-flag", &mut body);
        stream.write_all(&packet(0x10, &body)).unwrap();
        assert_eq!(until_close(&mut stream), [0u8; 0], "level {level}");
    }
    assert_eq!(
        broker.refused(),
        [
            ("ReservedConnectFlag".to_owned(), 0x81),
            ("ReservedConnectFlag".to_owned(), 0x81)
        ]
    );
}

/// Upstream ticket 3696. A protocol level the broker does not speak, under
/// the protocol name MQTT, is answered with the 3.1.1 CONNACK return code
/// 0x01 and closed ([MQTT-3.1.2-2]). Another protocol name is closed
/// without an answer ([MQTT-3.1.2-1] allows it).
#[test]
fn an_unknown_protocol_level_is_refused_with_return_code_one() {
    let broker = Broker::start("protocol-level");
    for level in [3u8, 6, 0, 255] {
        let mut stream = socket(broker.address);
        let mut body = Vec::new();
        text("MQTT", &mut body);
        body.extend_from_slice(&[level, 0x02, 0, 60]);
        text("level", &mut body);
        stream.write_all(&packet(0x10, &body)).unwrap();
        assert_eq!(until_close(&mut stream), [0x20, 2, 0, 1], "level {level}");
    }
    let mut stream = socket(broker.address);
    let mut body = Vec::new();
    text("MQTX", &mut body);
    body.extend_from_slice(&[4, 0x02, 0, 60]);
    text("name", &mut body);
    stream.write_all(&packet(0x10, &body)).unwrap();
    assert_eq!(until_close(&mut stream), [0u8; 0], "protocol name");
}

/// Upstream tickets 3588 and 3611. An MQTT 5 client was told Receive
/// Maximum 8: a ninth QoS 2 exchange is answered with DISCONNECT 0x93,
/// Receive Maximum exceeded, and the connection is closed ([MQTT-3.3.4-9]).
#[test]
fn a_ninth_exchange_of_an_mqtt5_client_is_receive_maximum_exceeded() {
    let broker = Broker::start("receive-maximum");
    let mut client = connect_v5(broker.address, "ninth", &[], false);
    let mut burst = Vec::new();
    for id in 1..=9 {
        burst.extend_from_slice(&packet(0x34, &publish_body("classes/q", Some(id), true)));
    }
    client.write_all(&burst).unwrap();
    let answer = until_close(&mut client);
    assert!(
        answer.ends_with(&[0xe0, 2, 0x93, 0]),
        "the stream ends with DISCONNECT 0x93: {answer:?}"
    );
    // What precedes it can only be PUBREC of the exchanges within the window.
    assert_eq!(answer.len() % 4, 0, "{answer:?}");
    for acknowledgement in answer[..answer.len() - 4].chunks(4) {
        assert_eq!(
            (acknowledgement[0], acknowledgement[1]),
            (0x50, 2),
            "{answer:?}"
        );
    }
}

/// Upstream tickets 3599 and 3602. A QoS 2 PUBLISH repeated before its
/// PUBREL is the same exchange: each repetition is acknowledged with
/// PUBREC, the connection goes on, the message is delivered once, and the
/// exchange completes ([MQTT-4.3.3-10], [MQTT-4.3.3-12]).
#[test]
fn repeated_duplicates_of_a_qos2_publish_are_acknowledged_and_delivered_once() {
    let broker = Broker::start("duplicates");
    let mut subscriber = connect_v5(broker.address, "duplicate-subscriber", &[], false);
    subscribe_v5(&mut subscriber, "classes/q", 0);
    let mut client = connect_v5(broker.address, "duplicate-publisher", &[], false);
    for repetition in 0..6 {
        let first = if repetition == 0 { 0x34 } else { 0x3c };
        client
            .write_all(&packet(first, &publish_body("classes/q", Some(7), true)))
            .unwrap();
        assert_eq!(
            read_packet(&mut client),
            (0x50, vec![0, 7]),
            "repetition {repetition}"
        );
    }
    client.write_all(&[0x62, 2, 0, 7]).unwrap();
    assert_eq!(read_packet(&mut client), (0x70, vec![0, 7]));
    client.write_all(&[0xc0, 0]).unwrap();
    assert_eq!(
        read_packet(&mut client),
        (0xd0, vec![]),
        "the connection goes on"
    );
    let (first, body) = read_packet(&mut subscriber);
    assert_eq!(first, 0x30);
    assert!(body.ends_with(b"payload"));
    assert!(silent(&mut subscriber), "delivered once");
}

/// Upstream tickets 3604, 3612, 3633 and 3634. A client that sets Request
/// Problem Information to 0 must receive no Reason String and no User
/// Property except on a PUBLISH, a CONNACK or a DISCONNECT
/// ([MQTT-3.1.2-29]). The broker sends none on any packet: every answer
/// below, on its error paths, has no property at all.
#[test]
fn no_property_is_sent_to_a_client_that_refused_problem_information() {
    const REFUSED: &[u8] = &[0x17, 0];
    let broker = Broker::start("problem-information");
    // PUBACK 0x10 (no matching subscriber): identifier and reason, nothing else.
    let mut client = connect_v5(broker.address, "information-1", REFUSED, false);
    client
        .write_all(&packet(0x32, &publish_body("classes/none", Some(1), true)))
        .unwrap();
    assert_eq!(read_packet(&mut client), (0x40, vec![0, 1, 0x10]));
    // PUBREC (plain for a publication without subscriber), then PUBCOMP.
    client
        .write_all(&packet(0x34, &publish_body("classes/none", Some(2), true)))
        .unwrap();
    assert_eq!(read_packet(&mut client), (0x50, vec![0, 2]));
    client.write_all(&[0x62, 2, 0, 2]).unwrap();
    assert_eq!(read_packet(&mut client), (0x70, vec![0, 2]));
    // UNSUBACK 0x11 (no subscription existed): an empty property list.
    let mut body = vec![0, 3, 0];
    text("classes/never", &mut body);
    client.write_all(&packet(0xa2, &body)).unwrap();
    assert_eq!(read_packet(&mut client), (0xb0, vec![0, 3, 0, 0x11]));
    // SUBACK: an empty property list.
    let mut body = vec![0, 4, 0];
    text("classes/t", &mut body);
    body.push(1);
    client.write_all(&packet(0x82, &body)).unwrap();
    assert_eq!(read_packet(&mut client), (0x90, vec![0, 4, 0, 1]));
    // DISCONNECT of the broker: a reason and an empty property list.
    let mut body = vec![0, 5, 0];
    text("$share/group/t", &mut body);
    body.push(1);
    client.write_all(&packet(0x82, &body)).unwrap();
    assert_eq!(until_close(&mut client), [0xe0, 2, 0x9e, 0]);
    for (name, first, body, reason) in [
        ("a malformed packet", 0xc1u8, vec![], 0x81u8),
        (
            "an invalid filter",
            0x82,
            {
                let mut body = vec![0, 6, 0];
                text("classes/#/x", &mut body);
                body.push(1);
                body
            },
            0x82,
        ),
    ] {
        let mut client = connect_v5(broker.address, "information-2", REFUSED, false);
        client.write_all(&packet(first, &body)).unwrap();
        assert_eq!(until_close(&mut client), [0xe0, 2, reason, 0], "{name}");
    }
    // CONNACK of a refused CONNECT (a Will QoS the profile does not store).
    let mut stream = socket(broker.address);
    let mut body = Vec::new();
    text("MQTT", &mut body);
    body.extend_from_slice(&[5, 0x16, 0, 60, 2]);
    body.extend_from_slice(REFUSED);
    text("information-3", &mut body);
    body.push(0);
    text("classes/will", &mut body);
    text("gone", &mut body);
    stream.write_all(&packet(0x10, &body)).unwrap();
    assert_eq!(until_close(&mut stream), [0x20, 3, 0, 0x83, 0]);
}
