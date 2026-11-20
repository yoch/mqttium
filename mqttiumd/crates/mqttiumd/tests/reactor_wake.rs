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
    stream
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
        let reply = writer
            .join()
            .unwrap()
            .unwrap_or_else(|error| panic!("publisher {index} was closed: {error}"));
        assert_eq!(reply, [0xd0, 0], "PINGRESP expected from publisher {index}");
    }
    assert!(
        drained.join().unwrap() > 0,
        "nothing reached the subscriber"
    );
}
