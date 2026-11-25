//! The product executable reacts to work when it arrives and rests when
//! there is none (ADR 0044). Both tests drive the real binary over
//! loopback TCP with a minimal MQTT 5 client written here.
//!
//! They are timing observations with wide margins: the polling reactor
//! they replace needed several one-millisecond sleeps per round trip and
//! woke two threads a thousand times a second while idle.
#![cfg(target_os = "linux")]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct Broker {
    child: Child,
    address: SocketAddr,
    directory: PathBuf,
}

impl Broker {
    fn start(name: &str) -> Self {
        Self::start_with(name, &[])
    }

    fn start_with(name: &str, options: &[&str]) -> Self {
        let directory = std::env::temp_dir().join(format!(
            "mqttiumd-{name}-{}-{}",
            std::process::id(),
            Instant::now().elapsed().as_nanos()
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
            .args(options)
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

    /// The refusals and closures the broker recorded, for a failure message.
    fn refusals(&self) -> String {
        std::fs::read_to_string(self.directory.join("events.jsonl"))
            .unwrap_or_default()
            .lines()
            .filter(|line| {
                line.contains("refused")
                    || (line.contains("\"connection_closed\"")
                        && !line.contains("client_disconnect"))
            })
            .take(8)
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Voluntary context switches of every thread of the broker.
    fn wakeups(&self) -> u64 {
        let tasks = format!("/proc/{}/task", self.child.id());
        std::fs::read_dir(tasks)
            .unwrap()
            .filter_map(|entry| std::fs::read_to_string(entry.ok()?.path().join("status")).ok())
            .filter_map(|status| {
                status
                    .lines()
                    .find_map(|line| line.strip_prefix("voluntary_ctxt_switches:"))
                    .and_then(|count| count.trim().parse::<u64>().ok())
            })
            .sum()
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

fn read_packet(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut first = [0u8; 1];
    stream.read_exact(&mut first).unwrap();
    let mut length = 0usize;
    let mut shift = 0;
    loop {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).unwrap();
        length |= usize::from(byte[0] & 0x7f) << shift;
        if byte[0] & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    let mut body = vec![0; length];
    stream.read_exact(&mut body).unwrap();
    (first[0], body)
}

fn connect(address: SocketAddr, client_id: &str) -> TcpStream {
    open(address, client_id).0
}

/// A connected client and the body of the CONNACK it received.
fn open(address: SocketAddr, client_id: &str) -> (TcpStream, Vec<u8>) {
    let mut stream = TcpStream::connect(address).unwrap();
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut body = Vec::new();
    text("MQTT", &mut body);
    // Level 5, Clean Start, Keep Alive 60 s, no properties.
    body.extend_from_slice(&[5, 0x02, 0, 60, 0]);
    text(client_id, &mut body);
    stream.write_all(&packet(0x10, &body)).unwrap();
    let (first, body) = read_packet(&mut stream);
    assert_eq!(first, 0x20, "CONNACK expected");
    assert_eq!(body[1], 0, "CONNACK reason");
    (stream, body)
}

/// Everything the broker sends until it ends the stream.
fn read_until_close(stream: &mut TcpStream) -> Vec<u8> {
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

/// The Maximum Packet Size a CONNACK announces. The walk knows the
/// properties this broker sends.
fn announced_maximum(connack: &[u8]) -> usize {
    let properties = &connack[3..3 + usize::from(connack[2])];
    let mut at = 0;
    while at < properties.len() {
        let width = match properties[at] {
            0x27 => {
                let value: [u8; 4] = properties[at + 1..at + 5].try_into().unwrap();
                return u32::from_be_bytes(value) as usize;
            }
            0x21 | 0x22 => 2,
            0x25 | 0x28 | 0x29 | 0x2a => 1,
            other => panic!("CONNACK property {other:#x} is not known to this test"),
        };
        at += 1 + width;
    }
    panic!("CONNACK announces no Maximum Packet Size");
}

fn subscribe(stream: &mut TcpStream, filter: &str) {
    let mut body = vec![0, 1, 0];
    text(filter, &mut body);
    body.push(0);
    stream.write_all(&packet(0x82, &body)).unwrap();
    let (first, body) = read_packet(stream);
    assert_eq!(first, 0x90, "SUBACK expected");
    assert_eq!(body.last(), Some(&0), "granted QoS 0");
}

fn publish_qos0(stream: &mut TcpStream, topic: &str, payload: &[u8]) {
    let mut body = Vec::new();
    text(topic, &mut body);
    body.push(0);
    body.extend_from_slice(payload);
    stream.write_all(&packet(0x30, &body)).unwrap();
}

/// A publication reaches its subscriber without waiting for a timer. On
/// the polling reactor every hop slept a millisecond, so no round trip
/// took less than two; here the typical one must take less than 1.5 ms.
/// The median is asserted, not the total: a loaded machine delays some
/// round trips, a timer delays all of them.
#[test]
fn a_publication_is_delivered_without_waiting_for_a_timer() {
    const ROUND_TRIPS: u32 = 500;
    let broker = Broker::start("round-trips");
    let mut subscriber = connect(broker.address, "wake-subscriber");
    subscribe(&mut subscriber, "wake/latency");
    let mut publisher = connect(broker.address, "wake-publisher");
    // One unmeasured exchange absorbs the first-use costs.
    publish_qos0(&mut publisher, "wake/latency", b"warm");
    assert_eq!(read_packet(&mut subscriber).0 >> 4, 3);
    let started = Instant::now();
    let mut delays = Vec::new();
    for sequence in 0..ROUND_TRIPS {
        let payload = sequence.to_be_bytes();
        let sent = Instant::now();
        publish_qos0(&mut publisher, "wake/latency", &payload);
        let (first, body) = read_packet(&mut subscriber);
        delays.push(sent.elapsed());
        assert_eq!(first >> 4, 3, "PUBLISH expected");
        assert!(body.ends_with(&payload), "delivery {sequence} out of order");
    }
    let elapsed = started.elapsed();
    delays.sort();
    let median = delays[delays.len() / 2];
    eprintln!("{ROUND_TRIPS} round trips in {elapsed:?}, median {median:?}");
    assert!(
        median < Duration::from_micros(1500),
        "the median round trip took {median:?} ({ROUND_TRIPS} in {elapsed:?})"
    );
}

/// An idle broker with two attached clients is not woken a thousand times
/// a second: its threads wait for a signal or for a coarse tick.
#[test]
fn an_idle_broker_rests() {
    let broker = Broker::start("idle");
    let mut subscriber = connect(broker.address, "idle-subscriber");
    subscribe(&mut subscriber, "wake/idle");
    let _publisher = connect(broker.address, "idle-publisher");
    std::thread::sleep(Duration::from_millis(300));
    let before = broker.wakeups();
    std::thread::sleep(Duration::from_secs(1));
    let wakeups = broker.wakeups() - before;
    eprintln!("{wakeups} voluntary context switches in one idle second");
    assert!(
        wakeups < 1000,
        "{wakeups} voluntary context switches in one idle second"
    );
}

/// Per-message observations are counted and summarised about once a
/// second, not after every exchange that leaves the broker idle.
#[test]
fn suppressed_observations_are_summarised_on_an_interval() {
    // The instrumented build records everything unless told otherwise.
    let broker = Broker::start_with("counters", &["--event-level", "ops"]);
    let mut subscriber = connect(broker.address, "counters-subscriber");
    subscribe(&mut subscriber, "wake/counters");
    let mut publisher = connect(broker.address, "counters-publisher");
    let started = Instant::now();
    for sequence in 0u32..300 {
        publish_qos0(&mut publisher, "wake/counters", &sequence.to_be_bytes());
        assert_eq!(read_packet(&mut subscriber).0 >> 4, 3);
        // The broker goes idle between two exchanges.
        std::thread::sleep(Duration::from_millis(2));
    }
    std::thread::sleep(Duration::from_millis(1200));
    let seconds = started.elapsed().as_secs();
    let log = std::fs::read_to_string(broker.directory.join("events.jsonl")).unwrap();
    let summaries = log
        .lines()
        .filter(|line| line.contains("\"kind\":\"counters\""))
        .count() as u64;
    assert!(summaries >= 1, "no summary of the suppressed observations");
    assert!(
        summaries <= seconds + 2,
        "{summaries} summaries in {seconds} s"
    );
}

/// The connection cap is the operator's: sixty clients are served at once
/// where the first bounded profile stopped at sixteen, and a broker sized
/// for two closes the third transport without a CONNACK.
#[test]
fn the_connection_limit_is_configurable() {
    const CLIENTS: usize = 60;
    let broker = Broker::start_with("many", &["--max-connections", "64"]);
    let mut subscriber = connect(broker.address, "many-subscriber");
    subscribe(&mut subscriber, "wake/many");
    let mut publishers: Vec<TcpStream> = (1..CLIENTS)
        .map(|index| connect(broker.address, &format!("many-publisher-{index}")))
        .collect();
    for (index, publisher) in publishers.iter_mut().enumerate() {
        let payload = u32::try_from(index).unwrap().to_be_bytes();
        publish_qos0(publisher, "wake/many", &payload);
        let (first, body) = read_packet(&mut subscriber);
        assert_eq!(first >> 4, 3, "PUBLISH expected");
        assert!(body.ends_with(&payload), "publication {index} lost");
    }
    drop(publishers);
    drop(subscriber);
    drop(broker);

    let broker = Broker::start_with("few", &["--max-connections", "2"]);
    let _first = connect(broker.address, "few-1");
    let _second = connect(broker.address, "few-2");
    let mut third = TcpStream::connect(broker.address).unwrap();
    third
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut byte = [0u8; 1];
    assert_eq!(
        third.read(&mut byte).unwrap_or(0),
        0,
        "a transport over the limit was answered"
    );
}

/// A burst from many publishers towards one subscriber is pressure, not a
/// fault: QoS 0 publications may be dropped, but no publisher loses its
/// connection. With a per-pass budget sized by the connection limit, the
/// reports of the drops filled the core's effect box and the next publisher
/// served was closed with `input_capacity` (about one run in three).
#[test]
fn a_fan_in_burst_closes_no_publisher() {
    const PUBLISHERS: usize = 40;
    const BURST: usize = 200;
    const ROUNDS: usize = 8;
    let broker = Broker::start_with("fan-in", &["--max-connections", "64"]);
    let mut subscriber = connect(broker.address, "fan-in-subscriber");
    subscribe(&mut subscriber, "wake/fan-in");
    let mut publishers: Vec<TcpStream> = (0..PUBLISHERS)
        .map(|index| connect(broker.address, &format!("fan-in-publisher-{index}")))
        .collect();
    let mut body = Vec::new();
    text("wake/fan-in", &mut body);
    body.push(0);
    body.extend_from_slice(&[0x5a; 64]);
    let burst = packet(0x30, &body).repeat(BURST);
    // The subscriber is read so that it is never the one that stalls.
    let drained = std::thread::spawn(move || {
        let mut received = 0usize;
        subscriber
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut bytes = [0u8; 65536];
        while let Ok(count) = subscriber.read(&mut bytes) {
            if count == 0 {
                break;
            }
            received += count;
        }
        received
    });
    // Every publisher writes at once: their packets meet in the same turns.
    let start = std::sync::Arc::new(std::sync::Barrier::new(PUBLISHERS));
    let writers: Vec<_> = publishers
        .drain(..)
        .map(|mut publisher| {
            let (start, burst) = (start.clone(), burst.clone());
            std::thread::spawn(move || {
                start.wait();
                let mut reply = [0u8; 2];
                for _ in 0..ROUNDS {
                    publisher.write_all(&burst)?;
                    publisher.write_all(&[0xc0, 0])?;
                    publisher.read_exact(&mut reply)?;
                    if reply != [0xd0, 0] {
                        break;
                    }
                }
                Ok::<_, std::io::Error>(reply)
            })
        })
        .collect();
    for (index, writer) in writers.into_iter().enumerate() {
        let reply = writer.join().unwrap().unwrap_or_else(|error| {
            panic!(
                "publisher {index} was closed: {error}\n{}",
                broker.refusals()
            )
        });
        assert_eq!(reply, [0xd0, 0], "PINGRESP expected from publisher {index}");
    }
    assert!(
        drained.join().unwrap() > 0,
        "nothing reached the subscriber"
    );
}

/// Record 0063. A subscriber that reads receives a QoS 0 burst whole:
/// eight publishers write a hundred and twenty-five messages each at once,
/// a thousand in all, within what the broker keeps waiting for one
/// subscriber. The output queue of a connection holds sixty-four frames and
/// the core used to emit a burst without waiting for it, dropping what
/// found it full; with one I/O thread the subscriber is never drained while
/// the publishers are read.
#[test]
fn a_reading_subscriber_receives_a_qos0_burst_whole() {
    const PUBLISHERS: u8 = 8;
    const BURST: u16 = 125;
    let broker = Broker::start_with("qos0-burst-whole", &["--io-threads", "1"]);
    let mut subscriber = connect(broker.address, "burst-whole-subscriber");
    subscribe(&mut subscriber, "burst/whole");
    let start = std::sync::Arc::new(std::sync::Barrier::new(usize::from(PUBLISHERS)));
    let writers: Vec<_> = (0..PUBLISHERS)
        .map(|publisher| {
            let mut stream = connect(broker.address, &format!("burst-whole-{publisher}"));
            let start = start.clone();
            std::thread::spawn(move || {
                let mut burst = Vec::new();
                for sequence in 0..BURST {
                    let mut body = Vec::new();
                    text("burst/whole", &mut body);
                    body.push(0);
                    body.push(publisher);
                    body.extend_from_slice(&sequence.to_be_bytes());
                    burst.extend_from_slice(&packet(0x30, &body));
                }
                start.wait();
                stream.write_all(&burst).unwrap();
                stream.write_all(&[0xc0, 0]).unwrap();
                assert_eq!(read_packet(&mut stream).0, 0xd0, "PINGRESP");
                stream
            })
        })
        .collect();
    let publishers: Vec<TcpStream> = writers
        .into_iter()
        .map(|writer| writer.join().unwrap())
        .collect();

    let mut received = vec![Vec::new(); usize::from(PUBLISHERS)];
    subscriber
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let total = usize::from(PUBLISHERS) * usize::from(BURST);
    // topic 2 + 11, property length 1, payload 3: every packet is the same.
    let mut frame = [0u8; 2 + 13 + 1 + 3];
    let mut count = 0;
    while count < total && subscriber.read_exact(&mut frame).is_ok() {
        assert_eq!(frame[..2], [0x30, 17], "a QoS 0 PUBLISH");
        received[usize::from(frame[16])].push(u16::from_be_bytes([frame[17], frame[18]]));
        count += 1;
    }
    assert_eq!(
        count,
        total,
        "messages of the burst are missing: {:?} received per publisher",
        received.iter().map(Vec::len).collect::<Vec<_>>()
    );
    for (publisher, sequence) in received.iter().enumerate() {
        assert_eq!(
            sequence,
            &(0..BURST).collect::<Vec<_>>(),
            "the burst of publisher {publisher}, whole and in order"
        );
    }
    drop(publishers);
}

/// The size of the packet is the only bound on the filters of a request:
/// sixty-five are served as nine unit requests and acknowledged once
/// (MQTT-3.8.4-5, MQTT-3.10.4-6), and the connection goes on.
#[test]
fn a_request_of_sixty_five_filters_is_served_and_acknowledged_once() {
    let broker = Broker::start("sixty-five");
    let mut client = Peer::connect(broker.address, "sixty-five", true);
    let filters: Vec<String> = (0..65)
        .map(|index| format!("wake/filter/{index}"))
        .collect();
    assert_eq!(
        client.subscribe(7, &filters),
        [0; 65],
        "every filter granted\n{}",
        broker.refusals()
    );
    assert_eq!(
        client.unsubscribe(8, &filters),
        [0; 65],
        "every filter removed\n{}",
        broker.refusals()
    );
    client.stream.write_all(&[0xc0, 0]).unwrap();
    assert_eq!(
        read_packet(&mut client.stream).0,
        0xd0,
        "the connection goes on"
    );
}

/// A fixed header the broker refuses ends the connection, and in MQTT 5
/// the broker says why first: 0x95 for a packet larger than it accepts,
/// 0x81 for flags the protocol forbids (MQTT-3.3.1-2). Only the header of
/// the large packet is sent: the refusal does not wait for a body.
#[test]
fn a_refused_header_is_named_before_the_close() {
    let broker = Broker::start("refused-header");

    let mut large = connect(broker.address, "too-large");
    let mut header = vec![0x32];
    remaining_length(mqttiumd::net::MAX_PACKET_BYTES + 1, &mut header);
    large.write_all(&header).unwrap();
    assert_eq!(
        read_until_close(&mut large),
        [0xe0, 2, 0x95, 0],
        "Packet too large"
    );

    let mut duplicate = connect(broker.address, "dup-at-qos0");
    let mut body = Vec::new();
    text("wake/dup", &mut body);
    body.push(0);
    duplicate.write_all(&packet(0x38, &body)).unwrap();
    assert_eq!(
        read_until_close(&mut duplicate),
        [0xe0, 2, 0x81, 0],
        "Malformed Packet"
    );
}

/// MQTT 5 §3.3.2.1 and §3.3.4: a PUBLISH whose Topic Name is empty and that
/// carries no Topic Alias is a Protocol Error, 0x82, at every QoS. A Topic
/// Name that is present and holds a wildcard keeps Topic Name Invalid, 0x90.
#[test]
fn an_empty_topic_name_is_a_protocol_error_and_a_wildcard_an_invalid_topic_name() {
    let broker = Broker::start("empty-topic");
    for (first, identifier) in [(0x30, &[][..]), (0x32, &[0, 1][..]), (0x34, &[0, 1][..])] {
        let mut publisher = connect(broker.address, &format!("empty-topic-{first:02x}"));
        let mut body = Vec::new();
        text("", &mut body);
        body.extend_from_slice(identifier);
        body.push(0);
        body.extend_from_slice(b"payload");
        publisher.write_all(&packet(first, &body)).unwrap();
        assert_eq!(
            read_until_close(&mut publisher),
            [0xe0, 2, 0x82, 0],
            "first byte {first:#04x}\n{}",
            broker.refusals()
        );
    }
    let mut publisher = connect(broker.address, "wildcard-topic");
    let mut body = Vec::new();
    text("wake/+", &mut body);
    body.extend_from_slice(&[0, 1, 0]);
    body.extend_from_slice(b"payload");
    publisher.write_all(&packet(0x32, &body)).unwrap();
    assert_eq!(
        read_until_close(&mut publisher),
        [0xe0, 2, 0x90, 0],
        "{}",
        broker.refusals()
    );
}

/// MQTT 5 §3.1.2.6 [MQTT-3.1.2-12]: Will QoS 3 is a Malformed Packet. The
/// CONNECT is not validated to its end, so it supplies no response limit:
/// the broker closes without a CONNACK, and the event names the class.
#[test]
fn a_reserved_will_qos_is_closed_without_a_connack_and_recorded_as_malformed() {
    let broker = Broker::start("reserved-will-qos");
    let mut stream = TcpStream::connect(broker.address).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut body = Vec::new();
    text("MQTT", &mut body);
    // Level 5; Clean Start, Will Flag, Will QoS 3; Keep Alive 60 s.
    body.extend_from_slice(&[5, 0x1e, 0, 60, 0]);
    text("reserved-will-qos", &mut body);
    body.push(0);
    text("wake/will", &mut body);
    text("offline", &mut body);
    stream.write_all(&packet(0x10, &body)).unwrap();
    assert_eq!(read_until_close(&mut stream), [0u8; 0], "no CONNACK");
    let refusals = broker.refusals();
    let refused: serde_json::Value = refusals
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .find(|event| event["kind"] == "packet_refused")
        .unwrap_or_else(|| panic!("no packet_refused event\n{refusals}"));
    assert_eq!(
        (
            refused["details"]["code"].as_str(),
            refused["details"]["reason"].as_u64()
        ),
        (Some("ReservedWillQos"), Some(0x81)),
        "{refusals}"
    );
}

/// The Maximum Packet Size of the CONNACK is a promise: a PUBLISH no
/// larger than it is not refused for its size, whatever its shape. The
/// two largest shapes are a QoS 0 and a QoS 1 PUBLISH with a one-byte
/// topic and no property.
#[test]
fn a_publish_within_the_announced_maximum_packet_size_is_accepted() {
    let broker = Broker::start("announced-maximum");
    let (mut client, connack) = open(broker.address, "announced-maximum");
    let maximum = announced_maximum(&connack);
    assert!(
        maximum > 16_384,
        "the header below assumes three length bytes"
    );

    // Fixed header 1 + 3, topic 2 + 1, property length 1.
    let mut body = Vec::new();
    text("t", &mut body);
    body.push(0);
    body.resize(maximum - 4, b'x');
    let largest = packet(0x30, &body);
    assert_eq!(largest.len(), maximum);
    client.write_all(&largest).unwrap();

    // The same with a Packet Identifier: the PUBACK says it was accepted.
    let mut body = Vec::new();
    text("t", &mut body);
    body.extend_from_slice(&[0, 1, 0]);
    body.resize(maximum - 4, b'x');
    let largest = packet(0x32, &body);
    assert_eq!(largest.len(), maximum);
    client.write_all(&largest).unwrap();
    let (first, answer) = read_packet(&mut client);
    assert_eq!(
        (first, &answer[..2]),
        (0x40, &[0, 1][..]),
        "PUBACK expected, got {first:#x} {answer:?}"
    );
    assert!(
        answer.len() == 2 || answer[2] == 0 || answer[2] == 0x10,
        "the publication is accepted: {answer:?}"
    );
}

/// The announced Maximum Packet Size is also a limit: an MQTT 5 packet one
/// byte larger is not served, whatever it carries. The broker answers
/// DISCONNECT 0x95 and closes. Sizes are the total on the wire, fixed
/// header included.
#[test]
fn a_packet_over_the_announced_maximum_packet_size_is_refused() {
    let broker = Broker::start("over-announced");

    let (mut publisher, connack) = open(broker.address, "over-announced-publish");
    let maximum = announced_maximum(&connack);
    // One byte more than the largest PUBLISH the announcement allows: its
    // payload is still within the payload limit of the profile.
    let mut body = Vec::new();
    text("wake/long-topic-name", &mut body);
    body.extend_from_slice(&[0, 1, 0]);
    body.resize(maximum + 1 - 4, b'x');
    let over = packet(0x32, &body);
    assert_eq!(over.len(), maximum + 1);
    publisher.write_all(&over).unwrap();
    assert_eq!(
        read_until_close(&mut publisher),
        [0xe0, 2, 0x95, 0],
        "a PUBLISH of {} bytes after {maximum} were announced\n{}",
        over.len(),
        broker.refusals()
    );

    // Another kind of packet: a SUBSCRIBE made of long filters.
    let (mut subscriber, _) = open(broker.address, "over-announced-subscribe");
    let mut body = vec![0, 1, 0];
    while body.len() + 4 <= maximum {
        text(&"f".repeat(1_000), &mut body);
        body.push(0);
    }
    let over = packet(0x82, &body);
    assert!(over.len() > maximum && over.len() < mqttiumd::net::MAX_PACKET_BYTES);
    subscriber.write_all(&over).unwrap();
    assert_eq!(
        read_until_close(&mut subscriber),
        [0xe0, 2, 0x95, 0],
        "a SUBSCRIBE of {} bytes\n{}",
        over.len(),
        broker.refusals()
    );
}

/// One client of either protocol version for the request tests below.
struct Peer {
    stream: TcpStream,
    v5: bool,
}

impl Peer {
    fn connect(address: SocketAddr, client_id: &str, v5: bool) -> Self {
        if v5 {
            return Self {
                stream: connect(address, client_id),
                v5,
            };
        }
        let mut stream = TcpStream::connect(address).unwrap();
        stream.set_nodelay(true).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut body = Vec::new();
        text("MQTT", &mut body);
        // Level 4, Clean Session, Keep Alive 60 s.
        body.extend_from_slice(&[4, 0x02, 0, 60]);
        text(client_id, &mut body);
        stream.write_all(&packet(0x10, &body)).unwrap();
        assert_eq!(read_packet(&mut stream), (0x20, vec![0, 0]), "CONNACK");
        Self { stream, v5 }
    }

    /// The Packet Identifier and, in MQTT 5, the empty property list.
    fn head(&self, id: u16) -> Vec<u8> {
        let mut body = id.to_be_bytes().to_vec();
        if self.v5 {
            body.push(0);
        }
        body
    }

    /// Sends a SUBSCRIBE at QoS 0 and returns the codes of its SUBACK.
    fn subscribe(&mut self, id: u16, filters: &[String]) -> Vec<u8> {
        let mut body = self.head(id);
        for filter in filters {
            text(filter, &mut body);
            body.push(0);
        }
        self.stream.write_all(&packet(0x82, &body)).unwrap();
        self.acknowledged(0x90, id)
    }

    /// Sends an UNSUBSCRIBE and returns the codes of its UNSUBACK (none in
    /// MQTT 3.1.1, whose UNSUBACK carries no result).
    fn unsubscribe(&mut self, id: u16, filters: &[String]) -> Vec<u8> {
        let mut body = id.to_be_bytes().to_vec();
        if self.v5 {
            body.push(0);
        }
        for filter in filters {
            text(filter, &mut body);
        }
        self.stream.write_all(&packet(0xa2, &body)).unwrap();
        self.acknowledged(0xb0, id)
    }

    fn acknowledged(&mut self, kind: u8, id: u16) -> Vec<u8> {
        let (first, answer) = read_packet(&mut self.stream);
        assert_eq!(
            first, kind,
            "acknowledgement {kind:#x} expected, got {answer:?}"
        );
        let head = self.head(id);
        assert_eq!(
            &answer[..head.len()],
            &head[..],
            "identifier and properties"
        );
        answer[head.len()..].to_vec()
    }

    fn publish(&mut self, topic: &str, payload: &[u8]) {
        let mut body = Vec::new();
        text(topic, &mut body);
        if self.v5 {
            body.push(0);
        }
        body.extend_from_slice(payload);
        self.stream.write_all(&packet(0x30, &body)).unwrap();
    }

    /// The topic and payload of the next PUBLISH this client receives.
    fn delivered(&mut self) -> (String, Vec<u8>) {
        let (first, body) = read_packet(&mut self.stream);
        assert_eq!(
            first & 0xf0,
            0x30,
            "PUBLISH expected, got {first:#x} {body:?}"
        );
        let length = usize::from(u16::from_be_bytes([body[0], body[1]]));
        let topic = String::from_utf8(body[2..2 + length].to_vec()).unwrap();
        let payload = body[2 + length + usize::from(self.v5)..].to_vec();
        (topic, payload)
    }
}

/// A SUBSCRIBE or an UNSUBSCRIBE with more filters than the core takes in
/// one request is served as the sequence of its unit requests would be,
/// and acknowledged once: every filter of a twelve-filter SUBSCRIBE is
/// granted and effective, and every filter of a nine-filter UNSUBSCRIBE is
/// really removed. What a client reads afterwards proves it: a message on
/// a removed filter does not arrive, the next one on a kept filter does.
/// Both protocol versions (counter-audit of bbe1453).
#[test]
fn a_request_with_many_filters_is_served_like_its_unit_requests() {
    for v5 in [true, false] {
        let name = if v5 { "many-v5" } else { "many-v4" };
        let broker = Broker::start(name);
        let mut subscriber = Peer::connect(broker.address, &format!("{name}-sub"), v5);
        let mut publisher = Peer::connect(broker.address, &format!("{name}-pub"), v5);
        let filters: Vec<String> = (0..12).map(|index| format!("many/{index}")).collect();

        assert_eq!(
            subscriber.subscribe(1, &filters),
            [0; 12],
            "{name}: twelve filters granted at QoS 0\n{}",
            broker.refusals()
        );
        // The last filter is beyond the first unit request: it is effective.
        publisher.publish("many/11", b"granted");
        assert_eq!(
            subscriber.delivered(),
            ("many/11".to_owned(), b"granted".to_vec()),
            "{name}"
        );

        let removed = subscriber.unsubscribe(2, &filters[..9]);
        if v5 {
            assert_eq!(
                removed,
                [0; 9],
                "{name}: nine filters removed\n{}",
                broker.refusals()
            );
        } else {
            assert!(
                removed.is_empty(),
                "{name}: an UNSUBACK of 3.1.1 has no payload"
            );
        }
        // Filter 8 was the ninth of the request: it is really gone. The
        // message on a kept filter is the next one the subscriber reads.
        publisher.publish("many/8", b"removed");
        publisher.publish("many/9", b"kept");
        assert_eq!(
            subscriber.delivered(),
            ("many/9".to_owned(), b"kept".to_vec()),
            "{name}: a removed filter still delivered"
        );

        // A filter longer than the profile stores is refused for itself;
        // its neighbour is granted and effective.
        let mixed = ["f".repeat(2_000), "many/mixed".to_owned()];
        let refusal = if v5 { 0x97 } else { 0x80 };
        assert_eq!(subscriber.subscribe(3, &mixed), [refusal, 0], "{name}");
        publisher.publish("many/mixed", b"mixed");
        assert_eq!(
            subscriber.delivered(),
            ("many/mixed".to_owned(), b"mixed".to_vec()),
            "{name}"
        );
        // An UNSUBSCRIBE of filters that were never subscribed, and of one
        // that cannot be: acknowledged, and it says so in MQTT 5.
        let absent: Vec<String> = (0..9).map(|index| format!("absent/{index}")).collect();
        let answer = subscriber.unsubscribe(4, &[&absent[..], &mixed[..1]].concat());
        if v5 {
            assert_eq!(answer, [0x11; 10], "{name}: no subscription existed");
        }
    }
}

/// The Receive Maximum of the CONNACK is a promise to every client at
/// once. A hundred clients each open the eight QoS 2 exchanges it allows, all
/// together, and answer every PUBREC with its PUBREL as it arrives: they
/// respect the window and give the broker what it needs to finish. None
/// of these clients may be closed, and every exchange completes
/// (counter-audit of bbe1453: a peer that had already sent the PUBREL its
/// own progress needed was closed).
///
/// When this test was written the broker held 128 exchange credits for
/// all its clients and most of these publications waited for one. The
/// table is now sized for the sessions (record 0061), so the eight
/// hundred exchanges each find theirs; the wait for a credit is covered by
/// the tests of the loop that give the broker a table of one or eight.
#[test]
fn clients_that_fill_their_announced_window_together_all_complete() {
    // Every client has its publications and, behind them in its stream,
    // the PUBRELs of the first ones acknowledged.
    const CLIENTS: usize = 100;
    const WINDOW: u16 = 8;
    // The instrumented build defaults to the sixteen connections of the
    // bounded profile.
    let broker = Broker::start_with("duplex-window", &["--max-connections", "128"]);
    let start = std::sync::Arc::new(std::sync::Barrier::new(CLIENTS));
    let clients: Vec<_> = (0..CLIENTS)
        .map(|index| {
            let mut stream = connect(broker.address, &format!("duplex-{index}"));
            let start = start.clone();
            std::thread::spawn(move || -> Result<u16, String> {
                let mut burst = Vec::new();
                for id in 1..=WINDOW {
                    let mut body = Vec::new();
                    text(&format!("duplex/{index}"), &mut body);
                    body.extend_from_slice(&id.to_be_bytes());
                    body.push(0);
                    body.extend_from_slice(b"payload");
                    burst.extend_from_slice(&packet(0x34, &body));
                }
                start.wait();
                stream
                    .write_all(&burst)
                    .map_err(|error| error.to_string())?;
                let mut completed = 0;
                while completed < WINDOW {
                    let mut first = [0u8; 2];
                    stream
                        .read_exact(&mut first)
                        .map_err(|error| format!("after {completed} PUBCOMP: {error}"))?;
                    let mut body = vec![0; usize::from(first[1])];
                    stream
                        .read_exact(&mut body)
                        .map_err(|error| error.to_string())?;
                    match first[0] {
                        // PUBREC: a failure Reason Code would be a refusal.
                        0x50 if body.len() == 2 || body[2] < 0x80 => stream
                            .write_all(&[0x62, 2, body[0], body[1]])
                            .map_err(|error| error.to_string())?,
                        0x70 => completed += 1,
                        other => return Err(format!("packet {other:#x} {body:?}")),
                    }
                }
                Ok(completed)
            })
        })
        .collect();
    for (index, client) in clients.into_iter().enumerate() {
        match client.join().unwrap() {
            Ok(completed) => assert_eq!(completed, WINDOW),
            Err(error) => panic!("client {index}: {error}\n{}", broker.refusals()),
        }
    }
}

/// A windowless 3.1.1 publisher may pipeline QoS 1. Every receive credit
/// here returns through a local PUBACK, even if the reader has staged a full
/// prefix. The publisher can read its acknowledgements after sending the batch.
#[test]
fn mqtt311_qos1_pipeline_waits_for_local_credit() {
    let broker = Broker::start("lookahead-qos1-311");
    let mut publisher = Peer::connect(broker.address, "lookahead-qos1", false);
    let mut batch = Vec::new();
    const COUNT: u16 = 64;
    for id in 1..=COUNT {
        let mut body = Vec::new();
        text("pipeline/qos1", &mut body);
        body.extend_from_slice(&id.to_be_bytes());
        body.extend_from_slice(b"value");
        batch.extend_from_slice(&packet(0x32, &body));
    }
    publisher.stream.write_all(&batch).unwrap();
    for id in 1..=COUNT {
        assert_eq!(
            read_packet(&mut publisher.stream),
            (0x40, id.to_be_bytes().to_vec())
        );
    }
    publisher.stream.write_all(&[0xc0, 0]).unwrap();
    assert_eq!(read_packet(&mut publisher.stream), (0xd0, Vec::new()));
}

/// MQTT 3.1.1 has no Receive Maximum: a client may have more QoS 2
/// exchanges in flight than the broker's window of eight. The broker holds
/// the PUBLISH it has no slot for. A slot is freed by a PUBCOMP, which
/// needs the PUBREL of an earlier exchange, and that PUBREL is behind the
/// held PUBLISH in the client's stream: the broker must go on reading it.
/// Every exchange completes, and the subscriber receives each message once
/// and in the order of publication on their one topic (MQTT 3.1.1 §4.3.3,
/// [MQTT-4.5.0-1], [MQTT-4.6.0-5]).
#[test]
fn a_3_1_1_client_with_more_qos2_exchanges_than_the_window_completes_them_all() {
    // Twelve; the twenty a common client library keeps in flight; and the
    // most the broker reads past the publication that waits: its window of
    // eight, that publication, and one lane of thirty-two packets behind.
    for exchanges in [12, 20, 41] {
        qos2_pipeline_completes(exchanges);
    }
}

/// The first publication outside the bounded prefix gets no PUBREC. The
/// publisher can resume the eight admitted exchanges on a new connection.
#[test]
fn mqtt311_resumes_after_a_42_publication_pipeline_fills_the_prefix() {
    let broker = Broker::start("qos2-pipeline-resume");
    let mut witness = connect(broker.address, "independent");
    let name = "pipeline-persistent";
    let (mut publisher, present) = resume_311(broker.address, name);
    assert!(!present);
    let mut burst = Vec::new();
    for id in 1..=42_u16 {
        let mut body = Vec::new();
        text("pipeline/t", &mut body);
        body.extend_from_slice(&id.to_be_bytes());
        body.push(1);
        burst.extend_from_slice(&packet(0x34, &body));
    }
    publisher.write_all(&burst).unwrap();
    for id in 1..=8_u16 {
        assert_eq!(
            read_packet(&mut publisher),
            (0x50, id.to_be_bytes().to_vec())
        );
    }
    assert!(read_until_close(&mut publisher).is_empty());
    witness.write_all(&[0xc0, 0]).unwrap();
    assert_eq!(read_packet(&mut witness), (0xd0, Vec::new()));

    let (mut resumed, present) = resume_311(broker.address, name);
    assert!(present);
    for id in 1..=8_u16 {
        resumed.write_all(&packet(0x62, &id.to_be_bytes())).unwrap();
        assert_eq!(read_packet(&mut resumed), (0x70, id.to_be_bytes().to_vec()));
    }
    qos2_publish(&mut resumed, false, 9, "after-resume");
    assert_eq!(read_packet(&mut resumed), (0x50, vec![0, 9]));
}

fn qos2_pipeline_completes(exchanges: u16) {
    let broker = Broker::start("qos2-pipeline-311");
    let mut subscriber = Peer::connect(broker.address, "pipeline-subscriber", false);
    let mut body = subscriber.head(1);
    text("pipeline/#", &mut body);
    body.push(2);
    subscriber.stream.write_all(&packet(0x82, &body)).unwrap();
    assert_eq!(subscriber.acknowledged(0x90, 1), [2], "QoS 2 granted");

    let mut publisher = Peer::connect(broker.address, "pipeline-publisher", false);
    let mut burst = Vec::new();
    for id in 1..=exchanges {
        let mut body = Vec::new();
        text("pipeline/t", &mut body);
        body.extend_from_slice(&id.to_be_bytes());
        body.extend_from_slice(format!("message-{id}").as_bytes());
        burst.extend_from_slice(&packet(0x34, &body));
    }
    publisher.stream.write_all(&burst).unwrap();
    let mut completed = Vec::new();
    while completed.len() < usize::from(exchanges) {
        let mut first = [0u8; 2];
        publisher
            .stream
            .read_exact(&mut first)
            .unwrap_or_else(|error| {
                panic!(
                    "after {} PUBCOMP of {exchanges}: {error}\n{}",
                    completed.len(),
                    broker.refusals()
                )
            });
        let mut body = vec![0; usize::from(first[1])];
        publisher.stream.read_exact(&mut body).unwrap();
        match first[0] {
            0x50 => publisher
                .stream
                .write_all(&[0x62, 2, body[0], body[1]])
                .unwrap(),
            0x70 => completed.push(u16::from_be_bytes([body[0], body[1]])),
            other => panic!("packet {other:#x} {body:?}\n{}", broker.refusals()),
        }
    }
    completed.sort_unstable();
    assert_eq!(completed, (1..=exchanges).collect::<Vec<_>>());

    let delivered = receive_qos2(&mut subscriber, usize::from(exchanges));
    let expected: Vec<_> = (1..=exchanges).map(|id| format!("message-{id}")).collect();
    assert_eq!(delivered, expected, "each message once, in order");
}

/// The payloads of the next `count` QoS 2 deliveries of a 3.1.1 subscriber,
/// each exchange completed.
fn receive_qos2(subscriber: &mut Peer, count: usize) -> Vec<String> {
    let mut delivered = Vec::new();
    let mut completed = 0;
    while completed < count {
        let (first, body) = read_packet(&mut subscriber.stream);
        match first & 0xf0 {
            0x30 => {
                assert_eq!(first & 0x06, 0x04, "delivered at QoS 2");
                let length = usize::from(u16::from_be_bytes([body[0], body[1]]));
                delivered.push(String::from_utf8(body[4 + length..].to_vec()).unwrap());
                let id = [body[2 + length], body[3 + length]];
                subscriber
                    .stream
                    .write_all(&[0x50, 2, id[0], id[1]])
                    .unwrap();
            }
            0x60 => {
                subscriber
                    .stream
                    .write_all(&[0x70, 2, body[0], body[1]])
                    .unwrap();
                completed += 1;
            }
            other => panic!("packet {other:#x} {body:?}"),
        }
    }
    delivered
}

/// A 3.1.1 client that keeps its session (Clean Session 0), and the
/// Session Present flag of its CONNACK.
fn resume_311(address: SocketAddr, client_id: &str) -> (TcpStream, bool) {
    let mut stream = TcpStream::connect(address).unwrap();
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut body = Vec::new();
    text("MQTT", &mut body);
    body.extend_from_slice(&[4, 0x00, 0, 60]);
    text(client_id, &mut body);
    stream.write_all(&packet(0x10, &body)).unwrap();
    let (first, connack) = read_packet(&mut stream);
    assert_eq!((first, connack[1]), (0x20, 0), "CONNACK");
    (stream, connack[0] & 1 == 1)
}

fn qos2_publish(stream: &mut TcpStream, duplicate: bool, id: u16, payload: &str) {
    let mut body = Vec::new();
    text("stale/t", &mut body);
    body.extend_from_slice(&id.to_be_bytes());
    body.extend_from_slice(payload.as_bytes());
    stream
        .write_all(&packet(if duplicate { 0x3c } else { 0x34 }, &body))
        .unwrap();
}

/// The case of upstream ticket 3809: QoS 2 exchanges a 3.1.1 session left
/// at PUBREC on an earlier connection. A PUBREC follows the commit that
/// routes the publication, so what is acknowledged is delivered, once: a
/// duplicate of an old PUBLISH is acknowledged again and not delivered
/// again (MQTT 3.1.1 §4.3.3, §4.4).
///
/// A session holds eight such exchanges and no more (record 0061). A new
/// QoS 2 publication of a session that owes eight PUBREL is not
/// acknowledged and its connection is closed: only the client can make room, by the PUBREL
/// it must send again when it reconnects ([MQTT-4.4.0-1]). Once it did,
/// the publication is accepted and delivered once: nothing was lost.
#[test]
fn stale_qos2_exchanges_of_a_3_1_1_session_are_bounded_and_lose_no_publication() {
    const WINDOW: u16 = 8;
    let broker = Broker::start("stale-qos2-311");
    let mut subscriber = Peer::connect(broker.address, "stale-subscriber", false);
    let mut body = subscriber.head(1);
    text("stale/#", &mut body);
    body.push(2);
    subscriber.stream.write_all(&packet(0x82, &body)).unwrap();
    assert_eq!(subscriber.acknowledged(0x90, 1), [2], "QoS 2 granted");

    // A connection leaves a full window of exchanges at PUBREC.
    let (mut publisher, present) = resume_311(broker.address, "stale-publisher");
    assert!(!present);
    for id in 1..=WINDOW {
        qos2_publish(&mut publisher, false, id, &format!("old-{id}"));
    }
    for id in 1..=WINDOW {
        assert_eq!(
            read_packet(&mut publisher),
            (0x50, id.to_be_bytes().to_vec()),
            "{}",
            broker.refusals()
        );
    }
    assert_eq!(
        receive_qos2(&mut subscriber, usize::from(WINDOW)),
        (1..=WINDOW)
            .map(|id| format!("old-{id}"))
            .collect::<Vec<_>>(),
        "what was acknowledged was delivered"
    );
    drop(publisher);

    // The session comes back without its PUBREL. A duplicate of an old
    // PUBLISH is acknowledged, not delivered again; a new publication is
    // not acknowledged and the connection is closed.
    let (mut publisher, present) = resume_311(broker.address, "stale-publisher");
    assert!(present);
    qos2_publish(&mut publisher, true, 1, "old-1");
    assert_eq!(read_packet(&mut publisher), (0x50, vec![0, 1]));
    qos2_publish(&mut publisher, false, 1000, "new");
    assert_eq!(
        packet_or_end(&mut publisher),
        None,
        "a ninth exchange of the session was answered"
    );
    // The event is written by the broker after the stream ended.
    let recorded = || {
        std::fs::read_to_string(broker.directory.join("events.jsonl"))
            .unwrap_or_default()
            .lines()
            .any(|line| {
                line.contains("\"connection_closed\"") && line.contains("capacity_exceeded")
            })
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    while !recorded() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        recorded(),
        "the close is recorded with its reason:\n{}",
        broker.refusals()
    );

    // It reconnects and sends its PUBREL again, as it must: every old
    // exchange completes and the publication is accepted.
    let (mut publisher, present) = resume_311(broker.address, "stale-publisher");
    assert!(present);
    for id in 1..=WINDOW {
        let [high, low] = id.to_be_bytes();
        publisher.write_all(&[0x62, 2, high, low]).unwrap();
        assert_eq!(
            read_packet(&mut publisher),
            (0x70, vec![high, low]),
            "{}",
            broker.refusals()
        );
    }
    qos2_publish(&mut publisher, false, 1000, "new");
    assert_eq!(
        read_packet(&mut publisher),
        (0x50, vec![3, 232]),
        "{}",
        broker.refusals()
    );
    assert_eq!(receive_qos2(&mut subscriber, 1), ["new"]);
    publisher.write_all(&[0x62, 2, 3, 232]).unwrap();
    assert_eq!(read_packet(&mut publisher), (0x70, vec![3, 232]));

    // The connection goes on, and the subscriber was sent nothing more.
    publisher.write_all(&[0xc0, 0]).unwrap();
    assert_eq!(read_packet(&mut publisher).0, 0xd0);
    subscriber
        .stream
        .set_read_timeout(Some(Duration::from_millis(300)))
        .unwrap();
    let mut byte = [0u8; 1];
    let more = subscriber.stream.read(&mut byte);
    assert!(
        matches!(&more, Err(error) if matches!(error.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut)),
        "a second delivery: {more:?} {byte:?}"
    );
}

/// The same bound for an MQTT 5 session: a session that owes eight PUBREL
/// and opens a ninth QoS 2 exchange is told Quota exceeded (0x97) before
/// its connection is closed. A QoS 1 publication is not such an exchange:
/// the broker's own PUBACK ends it, and it is served.
#[test]
fn an_mqtt5_session_that_owes_its_window_of_pubrel_is_told_quota_exceeded() {
    const WINDOW: u16 = 8;
    let publish = |stream: &mut TcpStream, first: u8, id: u16| {
        let mut body = Vec::new();
        text("stale/v5", &mut body);
        body.extend_from_slice(&id.to_be_bytes());
        body.extend_from_slice(&[0, b'x']);
        stream.write_all(&packet(first, &body)).unwrap();
    };
    let broker = Broker::start("stale-qos2-v5");
    let (mut publisher, present) = session_v5(broker.address, "stale-v5");
    assert!(!present);
    for id in 1..=WINDOW {
        publish(&mut publisher, 0x34, id);
    }
    for id in 1..=WINDOW {
        let (first, body) = read_packet(&mut publisher);
        assert_eq!((first, &body[..2]), (0x50, &id.to_be_bytes()[..]));
    }
    drop(publisher);

    let (mut publisher, present) = session_v5(broker.address, "stale-v5");
    assert!(present);
    publish(&mut publisher, 0x32, 100);
    let (first, body) = read_packet(&mut publisher);
    assert_eq!(
        (first, &body[..2]),
        (0x40, &[0, 100][..]),
        "{}",
        broker.refusals()
    );
    publish(&mut publisher, 0x34, 101);
    assert_eq!(
        read_until_close(&mut publisher),
        [0xe0, 2, 0x97, 0],
        "{}",
        broker.refusals()
    );
}

/// One packet, or `None` once the broker ended the stream.
fn packet_or_end(stream: &mut TcpStream) -> Option<(u8, Vec<u8>)> {
    fn fill(stream: &mut TcpStream, bytes: &mut [u8]) -> Option<()> {
        match stream.read_exact(bytes) {
            Ok(()) => Some(()),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::UnexpectedEof
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::BrokenPipe
                ) =>
            {
                None
            }
            Err(error) => panic!("the broker neither answered nor closed: {error}"),
        }
    }
    let mut first = [0u8; 1];
    fill(stream, &mut first)?;
    let mut length = 0usize;
    let mut shift = 0;
    loop {
        let mut byte = [0u8; 1];
        fill(stream, &mut byte)?;
        length |= usize::from(byte[0] & 0x7f) << shift;
        if byte[0] & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    let mut body = vec![0; length];
    fill(stream, &mut body)?;
    Some((first[0], body))
}

/// An MQTT 5 client publishes one QoS 1 and one QoS 2 message and is
/// acknowledged for both: the broker admitted them.
fn another_client_is_served(broker: &Broker) {
    let mut other = connect(broker.address, "another-client");
    for (first, id, answer) in [(0x32u8, 1u16, 0x40u8), (0x34, 2, 0x50)] {
        let mut body = Vec::new();
        text("other/t", &mut body);
        body.extend_from_slice(&id.to_be_bytes());
        body.push(0);
        body.push(b'x');
        other.write_all(&packet(first, &body)).unwrap();
        let received = packet_or_end(&mut other);
        let Some((kind, body)) = received else {
            panic!(
                "the broker closed a client that held nothing:\n{}",
                broker.refusals()
            );
        };
        assert_eq!(
            (kind, &body[..2]),
            (answer, &id.to_be_bytes()[..]),
            "{body:?}\n{}",
            broker.refusals()
        );
        assert!(
            body.len() == 2 || body[2] == 0 || body[2] == 0x10,
            "the publication is accepted: {body:?}"
        );
    }
}

/// A session that never completes its QoS 2 exchanges must not take the
/// place of the other clients. Each connection of the session leaves a
/// window of exchanges at PUBREC and the next one starts with a fresh
/// window; with sixteen connections allowed the broker held 128 exchanges
/// for all its clients, and sixteen such connections took them all (report
/// 0033). Whatever the broker answers that session, another client is
/// still acknowledged.
#[test]
fn one_session_cannot_hold_the_inbound_exchanges_of_every_client() {
    const WINDOW: u16 = 8;
    let broker = Broker::start_with("exchange-hoard", &["--max-connections", "16"]);
    let mut id = 0u16;
    'connections: for _ in 0..16 {
        let (mut holder, _) = resume_311(broker.address, "exchange-holder");
        let mut publications = Vec::new();
        for _ in 0..WINDOW {
            id += 1;
            let mut body = Vec::new();
            text("held/t", &mut body);
            body.extend_from_slice(&id.to_be_bytes());
            body.push(b'x');
            publications.extend_from_slice(&packet(0x34, &body));
        }
        // The broker may have closed this connection already.
        let _ = holder.write_all(&publications);
        for _ in 0..WINDOW {
            match packet_or_end(&mut holder) {
                Some((0x50, _)) => {}
                Some(other) => panic!("PUBREC expected, got {other:?}"),
                None => break 'connections,
            }
        }
    }
    another_client_is_served(&broker);
}

/// The same for the payload bytes the broker lends to the publications it
/// is admitting: an exchange whose publication is committed and
/// acknowledged no longer holds its payload, so sessions that leave their
/// exchanges at PUBREC do not use up the 64 MiB every client shares.
#[test]
fn exchanges_left_at_pubrec_do_not_hold_the_payload_bytes_of_every_client() {
    const SESSIONS: usize = 8;
    const WINDOW: u16 = 8;
    // The largest payload, 1 MiB: eight sessions of eight are 64 MiB.
    let payload = vec![b'x'; 1_048_576];
    let broker = Broker::start_with("payload-hoard", &["--max-connections", "16"]);
    let mut holders = Vec::new();
    for session in 0..SESSIONS {
        let (mut holder, _) = resume_311(broker.address, &format!("payload-holder-{session}"));
        for id in 1..=WINDOW {
            let mut body = Vec::new();
            text("held/t", &mut body);
            body.extend_from_slice(&id.to_be_bytes());
            body.extend_from_slice(&payload);
            holder.write_all(&packet(0x34, &body)).unwrap();
            assert_eq!(
                read_packet(&mut holder),
                (0x50, id.to_be_bytes().to_vec()),
                "{}",
                broker.refusals()
            );
        }
        holders.push(holder);
    }
    another_client_is_served(&broker);
}

/// An MQTT 5 client that keeps its session for an hour (Clean Start 0), and
/// the Session Present flag of its CONNACK.
fn session_v5(address: SocketAddr, client_id: &str) -> (TcpStream, bool) {
    let mut stream = TcpStream::connect(address).unwrap();
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut body = Vec::new();
    text("MQTT", &mut body);
    body.extend_from_slice(&[5, 0x00, 0, 60, 5, 0x11]);
    body.extend_from_slice(&3600u32.to_be_bytes());
    text(client_id, &mut body);
    stream.write_all(&packet(0x10, &body)).unwrap();
    let (first, connack) = read_packet(&mut stream);
    assert_eq!((first, connack[1]), (0x20, 0), "CONNACK");
    (stream, connack[0] & 1 == 1)
}

fn subscribe_qos1_v5(stream: &mut TcpStream, filter: &str) {
    let mut body = vec![0, 1, 0];
    text(filter, &mut body);
    body.push(1);
    stream.write_all(&packet(0x82, &body)).unwrap();
    assert_eq!(read_packet(stream), (0x90, vec![0, 1, 0, 1]), "SUBACK");
}

/// Reads QoS 1 deliveries of an MQTT 5 subscriber and acknowledges each,
/// until `count` different payloads were read. A redelivery (DUP) of one
/// already read is acknowledged and not counted again.
fn receive_qos1_v5(stream: &mut TcpStream, count: usize) -> Vec<String> {
    let mut payloads = Vec::new();
    while payloads.len() < count {
        let (first, body) = read_packet(stream);
        assert_eq!(first & 0xf6, 0x32, "QoS 1 PUBLISH, got {first:#x} {body:?}");
        let length = usize::from(u16::from_be_bytes([body[0], body[1]]));
        let id = [body[2 + length], body[3 + length]];
        let properties = usize::from(body[4 + length]);
        let payload = String::from_utf8(body[5 + length + properties..].to_vec()).unwrap();
        stream.write_all(&[0x40, 2, id[0], id[1]]).unwrap();
        if !payloads.contains(&payload) {
            payloads.push(payload);
        }
    }
    payloads
}

/// Upstream ticket 3687 on this broker: a positive PUBACK and what becomes
/// of the message for each subscriber. Three subscribers at QoS 1: one
/// reads, one is connected and reads nothing, one is offline. A PUBACK
/// 0x00 follows the commit that writes one obligation per subscriber, so
/// every acknowledged message is owed to each of them: the one that reads
/// has it at once, the store owes the two others every message, and each
/// receives all of them when it comes back. Nothing is dropped for one
/// subscriber because another is slow (MQTT 5 [MQTT-4.3.2-4],
/// [MQTT-4.5.0-1]).
#[test]
fn every_acknowledged_qos1_message_is_owed_to_and_reaches_each_subscriber() {
    const MESSAGES: usize = 40;
    let broker = Broker::start("owed-to-each");
    let (mut offline, _) = session_v5(broker.address, "owed-offline");
    subscribe_qos1_v5(&mut offline, "owed/#");
    offline.write_all(&[0xe0, 0]).unwrap();
    assert_eq!(read_until_close(&mut offline), [0u8; 0]);
    let (mut silent, _) = session_v5(broker.address, "owed-silent");
    subscribe_qos1_v5(&mut silent, "owed/#");
    let mut reader = connect(broker.address, "owed-reader");
    subscribe_qos1_v5(&mut reader, "owed/#");

    let mut publisher = connect(broker.address, "owed-publisher");
    let expected: Vec<String> = (1..=MESSAGES).map(|n| format!("message-{n}")).collect();
    for (index, payload) in expected.iter().enumerate() {
        let id = u16::try_from(index + 1).unwrap().to_be_bytes();
        let mut body = Vec::new();
        text("owed/t", &mut body);
        body.extend_from_slice(&id);
        body.push(0);
        body.extend_from_slice(payload.as_bytes());
        publisher.write_all(&packet(0x32, &body)).unwrap();
        let (first, puback) = read_packet(&mut publisher);
        assert_eq!(first, 0x40, "{}", broker.refusals());
        assert_eq!(&puback[..2], &id);
        assert!(
            puback.len() == 2 || puback[2] == 0x00,
            "{payload} was not accepted: {puback:?}"
        );
        // The subscriber that reads has each acknowledged message at once.
        assert_eq!(
            receive_qos1_v5(&mut reader, 1),
            std::slice::from_ref(payload)
        );
    }

    // What the store owes: every message to the two that did not read.
    let store = broker.directory.join("store.sqlite");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let owed = mqttiumd_store_sqlite::SqliteStore::inspect(&store)
            .unwrap()
            .obligations
            .len();
        if owed == 2 * MESSAGES {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "{owed} obligations, {} expected",
            2 * MESSAGES
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    // The silent subscriber leaves without having read; both come back.
    // They stay connected until the store owes nothing: an acknowledgement
    // written just before the end of a transport may not be applied.
    drop(silent);
    let mut resumed = Vec::new();
    for client_id in ["owed-silent", "owed-offline"] {
        let (mut stream, present) = session_v5(broker.address, client_id);
        assert!(present, "{client_id}");
        assert_eq!(
            receive_qos1_v5(&mut stream, MESSAGES),
            expected,
            "{client_id}"
        );
        resumed.push(stream);
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while !mqttiumd_store_sqlite::SqliteStore::inspect(&store)
        .unwrap()
        .obligations
        .is_empty()
    {
        assert!(Instant::now() < deadline, "obligations left after delivery");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Upstream ticket 1460 on this broker. The count of connections stays
/// exact through connections that come and go: after forty of them, a
/// limit of two still admits two and no third, and a place that is freed
/// is given again.
#[test]
fn the_connection_limit_stays_exact_after_many_connections_came_and_went() {
    let broker = Broker::start_with("limit-exact", &["--max-connections", "2"]);
    let refused = |address: SocketAddr| {
        let mut transport = TcpStream::connect(address).unwrap();
        transport
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut byte = [0u8; 1];
        transport.read(&mut byte).unwrap_or(0) == 0
    };
    // A place is free once the broker has seen the end of its transport.
    let admitted = |client_id: &str| {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let mut stream = TcpStream::connect(broker.address).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            let mut body = Vec::new();
            text("MQTT", &mut body);
            body.extend_from_slice(&[5, 0x02, 0, 60, 0]);
            text(client_id, &mut body);
            stream.write_all(&packet(0x10, &body)).unwrap();
            let mut head = [0u8; 2];
            if stream.read_exact(&mut head).is_ok() {
                let mut connack = vec![0; usize::from(head[1])];
                stream.read_exact(&mut connack).unwrap();
                assert_eq!((head[0], connack[1]), (0x20, 0));
                return stream;
            }
            assert!(Instant::now() < deadline, "{client_id} was never admitted");
            std::thread::sleep(Duration::from_millis(10));
        }
    };
    for round in 0..20 {
        let one = admitted(&format!("limit-a-{round}"));
        let two = admitted(&format!("limit-b-{round}"));
        // One leaves with a DISCONNECT, the other without a word.
        let mut one = one;
        one.write_all(&[0xe0, 0]).unwrap();
        drop((one, two));
    }
    let first = admitted("limit-first");
    let _second = admitted("limit-second");
    assert!(refused(broker.address), "a third connection was admitted");
    drop(first);
    let _again = admitted("limit-again");
    assert!(refused(broker.address), "a third connection was admitted");
}
