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

/// A publication reaches its subscriber without waiting for a timer: 500
/// sequential round trips took more than a second on the polling reactor.
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
    for sequence in 0..ROUND_TRIPS {
        let payload = sequence.to_be_bytes();
        publish_qos0(&mut publisher, "wake/latency", &payload);
        let (first, body) = read_packet(&mut subscriber);
        assert_eq!(first >> 4, 3, "PUBLISH expected");
        assert!(body.ends_with(&payload), "delivery {sequence} out of order");
    }
    let elapsed = started.elapsed();
    eprintln!("{ROUND_TRIPS} round trips in {elapsed:?}");
    assert!(
        elapsed < Duration::from_millis(500),
        "{ROUND_TRIPS} round trips took {elapsed:?}"
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
    let broker = Broker::start("counters");
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
