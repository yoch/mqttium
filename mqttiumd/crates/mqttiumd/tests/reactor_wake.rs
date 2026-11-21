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

/// A request with more filters than the broker serves in one request
/// (sixty-four) is still owed its acknowledgement (MQTT-3.8.4-1,
/// MQTT-3.10.4-4): it changes nothing and is answered with a failure for
/// every filter, and the connection stays open.
#[test]
fn a_request_beyond_the_bound_of_a_request_is_refused_per_filter() {
    let broker = Broker::start("beyond-bound");
    let mut client = connect(broker.address, "beyond-bound");
    const FILTERS: usize = 65;

    let mut body = vec![0, 7, 0];
    for index in 0..FILTERS {
        text(&format!("wake/filter/{index}"), &mut body);
        body.push(0);
    }
    client.write_all(&packet(0x82, &body)).unwrap();
    let (first, answer) = read_packet(&mut client);
    assert_eq!(first, 0x90, "SUBACK expected\n{}", broker.refusals());
    assert_eq!(
        &answer[..3],
        &[0, 7, 0],
        "packet identifier and no property"
    );
    assert_eq!(
        &answer[3..],
        &[0x97; FILTERS],
        "every filter refused for quota"
    );

    let mut body = vec![0, 8, 0];
    for index in 0..FILTERS {
        text(&format!("wake/filter/{index}"), &mut body);
    }
    client.write_all(&packet(0xa2, &body)).unwrap();
    let (first, answer) = read_packet(&mut client);
    assert_eq!(first, 0xb0, "UNSUBACK expected\n{}", broker.refusals());
    assert_eq!(
        &answer[..3],
        &[0, 8, 0],
        "packet identifier and no property"
    );
    assert_eq!(&answer[3..], &[0x83; FILTERS], "one failure per filter");

    client.write_all(&[0xc0, 0]).unwrap();
    assert_eq!(
        read_packet(&mut client).0,
        0xd0,
        "the connection stays open"
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
    remaining_length(70_000, &mut header);
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
    assert!(over.len() > maximum && over.len() < 65_536);
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
/// respect the window and give the broker what it needs to finish. The
/// broker holds fewer exchange credits than the windows add up to, so
/// some publications wait; none of these clients may be closed for it,
/// and every exchange completes (counter-audit of bbe1453: a peer that
/// had already sent the PUBREL its own progress needed was closed).
#[test]
fn clients_that_fill_their_announced_window_together_all_complete() {
    // Far more exchanges than the broker has credits (128): the first
    // packets read take them all, and every client that holds one has
    // later publications waiting and its PUBRELs behind them in its stream.
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
