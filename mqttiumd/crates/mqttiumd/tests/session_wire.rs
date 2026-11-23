//! Sessions, takeovers, Wills and retained messages, read on the wire.
//! Each test replays on the executable a signalled behaviour of another
//! broker that the external audit of `47e1d12` left partly verified on
//! this one (report 0034).
//!
//! A real executable over loopback TCP, with a minimal MQTT 5 client
//! written here.
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
            "mqttiumd-session-{name}-{}-{}",
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
            .args(["--max-connections", "128"])
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

    fn closes(&self) -> String {
        std::fs::read_to_string(self.directory.join("events.jsonl"))
            .unwrap_or_default()
            .lines()
            .filter(|line| line.contains("refused") || line.contains("\"connection_closed\""))
            .take(12)
            .collect::<Vec<_>>()
            .join("\n")
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
    let mut out = vec![first];
    let mut length = body.len();
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
    out.extend_from_slice(body);
    out
}

fn read_packet(stream: &mut TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
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

/// A PUBLISH an MQTT 5 client received.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Delivery {
    topic: String,
    payload: Vec<u8>,
    id: Option<u16>,
    qos: u8,
    duplicate: bool,
    retain: bool,
}

struct Client {
    stream: TcpStream,
    session_present: bool,
}

impl Client {
    /// `session`: keep the session for an hour (Clean Start 0), or a clean
    /// one. `will`: a QoS 1 Will with this payload on `session/will`.
    fn connect(address: SocketAddr, client_id: &str, session: bool, will: Option<&str>) -> Self {
        let mut stream = TcpStream::connect(address).unwrap();
        stream.set_nodelay(true).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut flags = if session { 0x00 } else { 0x02 };
        if will.is_some() {
            flags |= 0x0c;
        }
        let mut body = Vec::new();
        text("MQTT", &mut body);
        body.extend_from_slice(&[5, flags, 0, 60]);
        if session {
            body.extend_from_slice(&[5, 0x11]);
            body.extend_from_slice(&3600u32.to_be_bytes());
        } else {
            body.push(0);
        }
        text(client_id, &mut body);
        if let Some(payload) = will {
            body.push(0);
            text("session/will", &mut body);
            text(payload, &mut body);
        }
        stream.write_all(&packet(0x10, &body)).unwrap();
        let (first, connack) = read_packet(&mut stream).unwrap();
        assert_eq!((first, connack[1]), (0x20, 0), "CONNACK of {client_id}");
        Self {
            stream,
            session_present: connack[0] & 1 == 1,
        }
    }

    /// Subscribes one filter; `options` is the subscription options byte
    /// (QoS, and Retain Handling in bits 4 and 5).
    fn subscribe(&mut self, id: u16, filter: &str, options: u8) {
        let mut body = id.to_be_bytes().to_vec();
        body.push(0);
        text(filter, &mut body);
        body.push(options);
        self.stream.write_all(&packet(0x82, &body)).unwrap();
        let (first, suback) = self.next_control();
        let [high, low] = id.to_be_bytes();
        assert_eq!((first, suback), (0x90, vec![high, low, 0, options & 3]));
    }

    fn unsubscribe(&mut self, id: u16, filter: &str) {
        let mut body = id.to_be_bytes().to_vec();
        body.push(0);
        text(filter, &mut body);
        self.stream.write_all(&packet(0xa2, &body)).unwrap();
        let (first, unsuback) = self.next_control();
        let [high, low] = id.to_be_bytes();
        assert_eq!((first, unsuback), (0xb0, vec![high, low, 0, 0]));
    }

    fn publish(&mut self, topic: &str, payload: &[u8], qos: u8, retain: bool, id: u16) {
        let mut body = Vec::new();
        text(topic, &mut body);
        if qos > 0 {
            body.extend_from_slice(&id.to_be_bytes());
        }
        body.push(0);
        body.extend_from_slice(payload);
        self.stream
            .write_all(&packet(0x30 | (qos << 1) | u8::from(retain), &body))
            .unwrap();
        match qos {
            0 => {}
            1 => {
                let (first, puback) = self.next_control();
                assert_eq!((first, &puback[..2]), (0x40, &id.to_be_bytes()[..]));
                assert!(puback.len() == 2 || puback[2] < 0x80, "refused: {puback:?}");
            }
            _ => {
                let (first, pubrec) = self.next_control();
                assert_eq!((first, &pubrec[..2]), (0x50, &id.to_be_bytes()[..]));
                let [high, low] = id.to_be_bytes();
                self.stream.write_all(&[0x62, 2, high, low]).unwrap();
                assert_eq!(self.next_control(), (0x70, vec![high, low]));
            }
        }
    }

    /// The next packet that is not a PUBLISH. A client of these tests that
    /// awaits an acknowledgement has no delivery pending.
    fn next_control(&mut self) -> (u8, Vec<u8>) {
        let (first, body) = read_packet(&mut self.stream).unwrap();
        assert_ne!(first & 0xf0, 0x30, "an unexpected delivery: {body:?}");
        (first, body)
    }

    /// The next delivery, not acknowledged.
    fn delivery(&mut self) -> Delivery {
        let (first, body) = read_packet(&mut self.stream).unwrap();
        assert_eq!(
            first & 0xf0,
            0x30,
            "PUBLISH expected, got {first:#x} {body:?}"
        );
        let qos = (first >> 1) & 3;
        let length = usize::from(u16::from_be_bytes([body[0], body[1]]));
        let mut at = 2 + length;
        let id = (qos > 0).then(|| {
            at += 2;
            u16::from_be_bytes([body[at - 2], body[at - 1]])
        });
        let properties = usize::from(body[at]);
        Delivery {
            topic: String::from_utf8(body[2..2 + length].to_vec()).unwrap(),
            payload: body[at + 1 + properties..].to_vec(),
            id,
            qos,
            duplicate: first & 0x08 != 0,
            retain: first & 0x01 != 0,
        }
    }

    /// Acknowledges a QoS 1 delivery, or completes a QoS 2 one.
    fn acknowledge(&mut self, delivery: &Delivery) {
        let Some(id) = delivery.id else { return };
        let [high, low] = id.to_be_bytes();
        if delivery.qos == 1 {
            self.stream.write_all(&[0x40, 2, high, low]).unwrap();
        } else {
            self.stream.write_all(&[0x50, 2, high, low]).unwrap();
            let (first, pubrel) = read_packet(&mut self.stream).unwrap();
            assert_eq!((first, pubrel), (0x62, vec![high, low]));
            self.stream.write_all(&[0x70, 2, high, low]).unwrap();
        }
    }

    /// A PINGREQ and its PINGRESP: the broker has taken every packet this
    /// client wrote before, the acknowledgements among them. Returns the
    /// deliveries read meanwhile, not acknowledged.
    fn synchronise(&mut self) -> Vec<Delivery> {
        self.stream.write_all(&[0xc0, 0]).unwrap();
        let mut deliveries = Vec::new();
        loop {
            let mut first = [0u8; 1];
            self.stream.peek(&mut first).unwrap();
            if first[0] & 0xf0 != 0x30 {
                assert_eq!(self.next_control(), (0xd0, vec![]));
                return deliveries;
            }
            deliveries.push(self.delivery());
        }
    }

    /// Whether nothing reaches the client for a while.
    fn silent(&mut self) -> bool {
        self.stream
            .set_read_timeout(Some(Duration::from_millis(400)))
            .unwrap();
        let mut byte = [0u8; 1];
        let outcome = self.stream.read(&mut byte);
        self.stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        matches!(&outcome, Err(error) if matches!(
            error.kind(),
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
        ))
    }
}

/// Upstream tickets 1650, 1760 and 2976. A persistent session is taken
/// over while messages flow: by a second connection of its ClientID while
/// the first is still open, then by a third after the second was abandoned
/// without a word. Each takeover succeeds at once, the old connection is
/// told why (DISCONNECT 0x8E), and no message is lost or repeated: what a
/// connection read and did not acknowledge is delivered again to the next,
/// marked DUP, with what was published meanwhile; what it acknowledged is
/// not ([MQTT-3.1.4-3], [MQTT-4.4.0-1]).
#[test]
fn a_session_taken_over_while_messages_flow_loses_none() {
    /// Reads the deliveries `from..=to` in order, acknowledges those up to
    /// `acknowledged`, and requires the DUP flag exactly up to `redelivered`.
    fn take(client: &mut Client, from: u16, to: u16, acknowledged: u16, redelivered: u16) {
        for n in from..=to {
            let delivery = client.delivery();
            assert_eq!(
                (
                    String::from_utf8_lossy(&delivery.payload).into_owned(),
                    delivery.duplicate
                ),
                (format!("message-{n}"), n <= redelivered)
            );
            if n <= acknowledged {
                client.acknowledge(&delivery);
            }
        }
        // The broker has taken the acknowledgements, and has nothing more.
        assert_eq!(client.synchronise(), []);
    }
    let broker = Broker::start("takeover");
    let mut publisher = Client::connect(broker.address, "takeover-publisher", false, None);
    let mut publish = |range: std::ops::RangeInclusive<u16>| {
        for n in range {
            publisher.publish("takeover/t", format!("message-{n}").as_bytes(), 1, false, n);
        }
    };
    let mut first = Client::connect(broker.address, "takeover-session", true, None);
    assert!(!first.session_present);
    first.subscribe(1, "takeover/t", 1);

    // Ten messages. The first connection reads them all and acknowledges
    // the first five.
    publish(1..=10);
    take(&mut first, 1, 10, 5, 0);

    // The second connection takes the session over while the first is
    // open; five more messages are published. It receives what was not
    // acknowledged and nothing that was: 6 to 10 again, as redeliveries,
    // then 11 to 15. It acknowledges up to 12.
    let mut second = Client::connect(broker.address, "takeover-session", true, None);
    assert!(second.session_present);
    assert_eq!(
        read_packet(&mut first.stream).unwrap(),
        (0xe0, vec![0x8e, 0]),
        "Session taken over\n{}",
        broker.closes()
    );
    publish(11..=15);
    take(&mut second, 6, 15, 12, 10);

    // The second connection is abandoned without a word. The third takes
    // over at once: the broker does not wait for the old connection.
    let taking = std::time::Instant::now();
    let mut third = Client::connect(broker.address, "takeover-session", true, None);
    assert!(third.session_present);
    assert!(
        taking.elapsed() < Duration::from_secs(2),
        "{:?}",
        taking.elapsed()
    );
    publish(16..=20);
    take(&mut third, 13, 20, 20, 15);
    assert!(third.silent(), "nothing is delivered after its PUBACK");
    drop(second);
}

/// Upstream ticket 1767. A QoS 2 delivery the subscriber never releases
/// holds one place of its window; the deliveries of the other topics go on.
#[test]
fn a_delivery_that_is_never_released_does_not_stop_the_other_topics() {
    let broker = Broker::start("unreleased");
    let mut subscriber = Client::connect(broker.address, "unreleased-subscriber", false, None);
    subscriber.subscribe(1, "unreleased/#", 2);
    let mut publisher = Client::connect(broker.address, "unreleased-publisher", false, None);
    publisher.publish("unreleased/held", b"held", 2, false, 1);
    let held = subscriber.delivery();
    assert_eq!((held.topic.as_str(), held.qos), ("unreleased/held", 2));
    // Never acknowledged. Twenty more on two other topics all arrive.
    for n in 0..20u16 {
        let topic = if n % 2 == 0 {
            "unreleased/a"
        } else {
            "unreleased/b"
        };
        publisher.publish(topic, format!("message-{n}").as_bytes(), 2, false, n + 2);
        let delivery = subscriber.delivery();
        assert_eq!(
            (delivery.topic.as_str(), delivery.payload.as_slice()),
            (topic, format!("message-{n}").as_bytes()),
            "{}",
            broker.closes()
        );
        subscriber.acknowledge(&delivery);
    }
}

/// Upstream tickets 2553 and 904. The Will of a connection is published
/// when that connection ends for another reason than a normal DISCONNECT:
/// a second CONNECT packet on the same connection, which is a Protocol
/// Error ([MQTT-3.1.0-2]), and the takeover of its session by another
/// connection ([MQTT-3.1.4-3]). Each publishes the Will once.
#[test]
fn a_second_connect_and_a_takeover_each_publish_the_will_once() {
    let broker = Broker::start("will");
    let mut monitor = Client::connect(broker.address, "will-monitor", false, None);
    monitor.subscribe(1, "session/will", 1);

    let mut twice = Client::connect(broker.address, "will-twice", false, Some("second-connect"));
    let mut body = Vec::new();
    text("MQTT", &mut body);
    body.extend_from_slice(&[5, 0x02, 0, 60, 0]);
    text("will-twice", &mut body);
    twice.stream.write_all(&packet(0x10, &body)).unwrap();
    let delivery = monitor.delivery();
    assert_eq!(delivery.payload, b"second-connect");
    monitor.acknowledge(&delivery);
    assert!(monitor.silent(), "once");

    let _old = Client::connect(broker.address, "will-taken", true, Some("taken-over"));
    let _new = Client::connect(broker.address, "will-taken", true, Some("of-the-new-one"));
    let delivery = monitor.delivery();
    assert_eq!(
        delivery.payload, b"taken-over",
        "the Will of the old connection"
    );
    monitor.acknowledge(&delivery);
    assert!(monitor.silent(), "once, and not the Will of the new one");
}

/// Upstream ticket 2402. With Retain Handling 0 the retained message is
/// sent at every subscription, also after an UNSUBSCRIBE of the same
/// filter ([MQTT-3.3.1-9]). Retain Handling 1 sends it only for a new
/// subscription, Retain Handling 2 never.
#[test]
fn a_retained_message_is_sent_again_after_unsubscribe_and_resubscribe() {
    let broker = Broker::start("resubscribe");
    let mut publisher = Client::connect(broker.address, "resubscribe-publisher", false, None);
    publisher.publish("resubscribe/t", b"kept", 1, true, 1);
    let mut client = Client::connect(broker.address, "resubscribe-client", false, None);
    for round in 0..3u16 {
        client.subscribe(10 + round, "resubscribe/t", 1);
        let delivery = client.delivery();
        assert_eq!(
            (delivery.payload.as_slice(), delivery.retain),
            (&b"kept"[..], true)
        );
        client.acknowledge(&delivery);
        // A second SUBSCRIBE of the existing subscription sends it again.
        client.subscribe(20 + round, "resubscribe/t", 1);
        let delivery = client.delivery();
        assert_eq!(delivery.payload, b"kept", "round {round}");
        client.acknowledge(&delivery);
        client.unsubscribe(30 + round, "resubscribe/t");
        assert!(client.silent(), "round {round}");
    }
    // Retain Handling 1: for a new subscription only.
    client.subscribe(40, "resubscribe/t", 0x11);
    let delivery = client.delivery();
    assert_eq!(delivery.payload, b"kept");
    client.acknowledge(&delivery);
    client.subscribe(41, "resubscribe/t", 0x11);
    assert!(
        client.silent(),
        "Retain Handling 1 on an existing subscription"
    );
    client.unsubscribe(42, "resubscribe/t");
    // Retain Handling 2: never.
    client.subscribe(43, "resubscribe/t", 0x21);
    assert!(client.silent(), "Retain Handling 2");
}

/// Upstream ticket 3220. A retained message on an exact topic is found
/// through every filter that matches it, with a wildcard in first place or
/// elsewhere, and every time: forty rounds of each.
#[test]
fn a_retained_message_is_found_through_every_matching_filter_every_time() {
    let broker = Broker::start("wildcards");
    let mut publisher = Client::connect(broker.address, "wildcard-publisher", false, None);
    publisher.publish("site/room/temperature", b"21", 1, true, 1);
    publisher.publish("site/room/humidity", b"40", 1, true, 2);
    let mut client = Client::connect(broker.address, "wildcard-client", false, None);
    let mut id = 10;
    for round in 0..40 {
        for (filter, expected) in [
            ("+/room/temperature", 1),
            ("+/+/temperature", 1),
            ("site/+/temperature", 1),
            ("site/room/+", 2),
            ("+/room/+", 2),
            ("+/#", 2),
            ("#", 2),
            ("+/+/+/+", 0),
            ("+/room", 0),
        ] {
            client.subscribe(id, filter, 0);
            let mut payloads = Vec::new();
            for _ in 0..expected {
                let delivery = client.delivery();
                assert!(delivery.retain, "{filter}");
                payloads.push(delivery.payload);
            }
            payloads.sort();
            let all = [b"21".to_vec(), b"40".to_vec()];
            assert_eq!(
                payloads,
                if expected == 2 {
                    all.to_vec()
                } else {
                    all[..expected].to_vec()
                },
                "round {round}: {filter}"
            );
            client.unsubscribe(id + 1, filter);
            id += 2;
        }
    }
    assert!(client.silent());
}

/// Upstream ticket 3294. Seventy clients each publish at QoS 2 in three
/// rounds, together. Every message of every round is forwarded to the
/// subscriber once.
#[test]
fn seventy_clients_in_rounds_have_every_qos2_message_forwarded_once() {
    const CLIENTS: usize = 70;
    const ROUNDS: usize = 3;
    const EACH: usize = 2;
    let broker = Broker::start("rounds");
    let mut subscriber = Client::connect(broker.address, "rounds-subscriber", false, None);
    subscriber.subscribe(1, "rounds/#", 2);
    let total = CLIENTS * ROUNDS * EACH;
    // Deliveries interleave with the PUBRELs of earlier ones.
    let reader = std::thread::spawn(move || {
        let mut received = Vec::with_capacity(total);
        let mut completed = 0;
        while completed < total {
            let (first, body) = read_packet(&mut subscriber.stream).unwrap();
            match first & 0xf0 {
                0x30 => {
                    assert_eq!(first & 0x06, 0x04, "delivered at QoS 2");
                    let length = usize::from(u16::from_be_bytes([body[0], body[1]]));
                    let id = [body[2 + length], body[3 + length]];
                    let properties = usize::from(body[4 + length]);
                    received
                        .push(String::from_utf8(body[5 + length + properties..].to_vec()).unwrap());
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
        let quiet = subscriber.silent();
        (received, quiet)
    });
    let gate = std::sync::Arc::new(std::sync::Barrier::new(CLIENTS));
    let address = broker.address;
    let publishers: Vec<_> = (0..CLIENTS)
        .map(|client| {
            let gate = gate.clone();
            std::thread::spawn(move || {
                let mut publisher =
                    Client::connect(address, &format!("rounds-{client}"), false, None);
                gate.wait();
                let mut id = 1;
                for round in 0..ROUNDS {
                    for message in 0..EACH {
                        let payload = format!("{client}/{round}/{message}");
                        publisher.publish(
                            &format!("rounds/{client}"),
                            payload.as_bytes(),
                            2,
                            false,
                            id,
                        );
                        id += 1;
                    }
                }
            })
        })
        .collect();
    for publisher in publishers {
        publisher.join().unwrap();
    }
    let (mut received, quiet) = reader.join().unwrap();
    assert!(quiet, "more deliveries than publications");
    received.sort();
    let mut expected: Vec<_> = (0..CLIENTS)
        .flat_map(|client| {
            (0..ROUNDS).flat_map(move |round| {
                (0..EACH).map(move |message| format!("{client}/{round}/{message}"))
            })
        })
        .collect();
    expected.sort();
    assert_eq!(received, expected, "{}", broker.closes());
}

/// Upstream ticket 3771. A retained message is sent on a subscription
/// while the broker distributes large publications to another client.
#[test]
fn a_retained_message_is_sent_while_large_publications_flow() {
    const LARGE: usize = 256 * 1024;
    const COUNT: usize = 24;
    let broker = Broker::start("busy");
    let mut retained = Client::connect(broker.address, "busy-retained", false, None);
    retained.publish("busy/kept", b"kept", 1, true, 1);
    let mut receiver = Client::connect(broker.address, "busy-receiver", false, None);
    receiver.subscribe(1, "busy/large", 1);
    let address = broker.address;
    let flow = std::thread::spawn(move || {
        let mut publisher = Client::connect(address, "busy-publisher", false, None);
        let payload = vec![b'x'; LARGE];
        for n in 0..COUNT {
            publisher.publish("busy/large", &payload, 1, false, n as u16 + 1);
        }
    });
    let reading = std::thread::spawn(move || {
        for _ in 0..COUNT {
            let delivery = receiver.delivery();
            assert_eq!(delivery.payload.len(), LARGE);
            receiver.acknowledge(&delivery);
        }
    });
    let mut client = Client::connect(broker.address, "busy-client", false, None);
    let mut rounds = 0;
    while !flow.is_finished() || rounds < 5 {
        client.subscribe(10 + 2 * rounds, "busy/kept", 1);
        let delivery = client.delivery();
        assert_eq!(
            (delivery.payload.as_slice(), delivery.retain),
            (&b"kept"[..], true)
        );
        client.acknowledge(&delivery);
        client.unsubscribe(11 + 2 * rounds, "busy/kept");
        rounds += 1;
    }
    flow.join().unwrap();
    reading.join().unwrap();
    assert!(rounds >= 5, "{rounds} subscriptions during the flow");
}
