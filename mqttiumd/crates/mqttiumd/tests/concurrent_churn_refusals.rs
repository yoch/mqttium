//! Wire-level regression tests for spurious publication refusals under
//! concurrent session churn, subscription mutations, and asymmetric subscribers.
#![cfg(target_os = "linux")]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

struct Broker {
    child: Child,
    address: SocketAddr,
    directory: PathBuf,
}

impl Broker {
    fn start(name: &str, mode: &str) -> Self {
        static SERIAL: AtomicU32 = AtomicU32::new(0);
        let directory = std::env::temp_dir().join(format!(
            "mqttiumd-churn-{name}-{}-{}",
            std::process::id(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).unwrap();
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_mqttiumd"));
        cmd.arg("--db")
            .arg(directory.join("store.sqlite"))
            .args(["--listen", "127.0.0.1:0"])
            .arg("--events")
            .arg(directory.join("events.jsonl"))
            .args(["--storage-mode", mode])
            .args(["--max-connections", "128"])
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        let mut child = cmd.spawn().unwrap();
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
}

impl Drop for Broker {
    fn drop(&mut self) {
        if std::thread::panicking() {
            if let Ok(status) = self.child.try_wait() {
                eprintln!("=== broker status on panic: {status:?} ===");
            }
            if let Ok(text) = std::fs::read_to_string(self.directory.join("events.jsonl")) {
                let lines: Vec<_> = text.lines().collect();
                let start = lines.len().saturating_sub(20);
                eprintln!("=== last events.jsonl lines ===");
                for l in &lines[start..] {
                    eprintln!("{l}");
                }
            }
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

fn push_str(value: &str, out: &mut Vec<u8>) {
    out.extend_from_slice(&u16::try_from(value.len()).unwrap().to_be_bytes());
    out.extend_from_slice(value.as_bytes());
}

fn encode_packet(first: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 5);
    out.push(first);
    let mut len = body.len();
    loop {
        let mut b = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            b |= 0x80;
        }
        out.push(b);
        if len == 0 {
            break;
        }
    }
    out.extend_from_slice(body);
    out
}

fn read_packet(stream: &mut TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
    let mut first = [0u8; 1];
    stream.read_exact(&mut first)?;
    let mut len = 0usize;
    let mut shift = 0;
    loop {
        let mut b = [0u8; 1];
        stream.read_exact(&mut b)?;
        len |= usize::from(b[0] & 0x7f) << shift;
        if b[0] & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body)?;
    Ok((first[0], body))
}

fn connect_v5_with_rm(
    addr: SocketAddr,
    client_id: &str,
    session_expiry: u32,
    receive_maximum: Option<u16>,
) -> TcpStream {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut body = Vec::new();
    push_str("MQTT", &mut body);
    body.push(5); // MQTT 5.0
    body.push(0x02); // Clean Start
    body.extend_from_slice(&60u16.to_be_bytes());
    let mut props = Vec::new();
    if session_expiry != 0 {
        props.push(0x11);
        props.extend_from_slice(&session_expiry.to_be_bytes());
    }
    if let Some(rm) = receive_maximum {
        props.push(0x21);
        props.extend_from_slice(&rm.to_be_bytes());
    }
    body.push(u8::try_from(props.len()).unwrap());
    body.extend_from_slice(&props);
    push_str(client_id, &mut body);
    stream.write_all(&encode_packet(0x10, &body)).unwrap();
    let (hdr, resp) = read_packet(&mut stream).unwrap();
    assert_eq!(hdr, 0x20, "expected CONNACK for {client_id}");
    assert_eq!(resp[1], 0x00, "CONNACK refused for {client_id}: {resp:?}");
    stream
}

fn connect_v5(addr: SocketAddr, client_id: &str, session_expiry: u32) -> TcpStream {
    connect_v5_with_rm(addr, client_id, session_expiry, None)
}

fn subscribe_v5(stream: &mut TcpStream, pid: u16, filter: &str, qos: u8) {
    let mut body = Vec::new();
    body.extend_from_slice(&pid.to_be_bytes());
    body.push(0); // properties
    push_str(filter, &mut body);
    body.push(qos);
    stream.write_all(&encode_packet(0x82, &body)).unwrap();
    let (hdr, resp) = read_packet(stream).unwrap();
    assert_eq!(hdr, 0x90, "expected SUBACK");
    assert_eq!(u16::from_be_bytes([resp[0], resp[1]]), pid);
    assert!(
        resp.last().copied().unwrap_or(0x80) <= qos,
        "SUBACK refused: {resp:?}"
    );
}

fn publish_qos1_packet(pid: u16, topic: &str, payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(topic.len() + payload.len() + 6);
    push_str(topic, &mut body);
    body.extend_from_slice(&pid.to_be_bytes());
    body.push(0); // properties
    body.extend_from_slice(payload);
    encode_packet(0x32, &body)
}

fn run_session_close_churn_does_not_refuse_unrelated_qos1(mode: &str, total_messages: usize) {
    let broker = Broker::start(&format!("close-{mode}"), mode);
    let addr = broker.address;

    let mut sub = connect_v5(addr, "steady-sub", 0);
    subscribe_v5(&mut sub, 1, "steady/topic", 1);

    let mut pub_stream = connect_v5(addr, "steady-pub", 0);

    let stop = Arc::new(AtomicBool::new(false));
    let churn_cycles = Arc::new(AtomicUsize::new(0));
    let stop_churn = Arc::clone(&stop);
    let cycles_churn = Arc::clone(&churn_cycles);

    // Concurrent churners repeatedly connect, subscribe to an UNRELATED topic,
    // and close (alternating clean DISCONNECT and abrupt TCP close).
    let mut churn_handles = Vec::new();
    for worker in 0..4usize {
        let stop_w = Arc::clone(&stop_churn);
        let cycles_w = Arc::clone(&cycles_churn);
        churn_handles.push(thread::spawn(move || {
            let mut seq = 0usize;
            while !stop_w.load(Ordering::Relaxed) {
                seq += 1;
                let cid = format!("churn-{worker}-{seq}");
                let filter = format!("unrelated/churn/{worker}/{seq}");
                let mut s = connect_v5(addr, &cid, 0);
                subscribe_v5(&mut s, 1, &filter, 1);
                if seq.is_multiple_of(2) {
                    // Clean DISCONNECT (0x00) -> SessionAction::Close on control lane
                    let _ = s.write_all(&[0xe0, 0x00]);
                }
                drop(s);
                cycles_w.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    // Drain subscriber concurrently and send PUBACKs.
    let sub_received = Arc::new(AtomicUsize::new(0));
    let sub_received_t = Arc::clone(&sub_received);
    let sub_handle = thread::spawn(move || {
        while sub_received_t.load(Ordering::Relaxed) < total_messages {
            let (hdr, body) = match read_packet(&mut sub) {
                Ok(pkt) => pkt,
                Err(_) => break,
            };
            if (hdr & 0xf0) == 0x30 {
                let tlen = usize::from(u16::from_be_bytes([body[0], body[1]]));
                let pid = u16::from_be_bytes([body[2 + tlen], body[3 + tlen]]);
                let mut ack = Vec::with_capacity(4);
                ack.extend_from_slice(&pid.to_be_bytes());
                sub.write_all(&encode_packet(0x40, &ack)).unwrap();
                sub_received_t.fetch_add(1, Ordering::Relaxed);
            }
        }
    });

    // Wait briefly so churners are actively subscribing & closing while we pipeline QoS 1.
    let wait_start = Instant::now();
    while churn_cycles.load(Ordering::Relaxed) < 4 && wait_start.elapsed() < Duration::from_secs(3)
    {
        thread::sleep(Duration::from_millis(5));
    }

    // Pipeline up to 8 QoS 1 publications in flight to maximize overlap with writer queue.
    let window = 8usize;
    let mut sent = 0usize;
    let mut acked_ok = 0usize;
    let mut refused_reasons: Vec<u8> = Vec::new();
    let payload = [b'x'; 128];

    while acked_ok + refused_reasons.len() < total_messages {
        while sent < total_messages && (sent - (acked_ok + refused_reasons.len())) < window {
            let pid = ((sent % 60_000) + 1) as u16;
            pub_stream
                .write_all(&publish_qos1_packet(pid, "steady/topic", &payload))
                .unwrap();
            sent += 1;
        }
        let (hdr, body) = read_packet(&mut pub_stream).unwrap();
        assert_eq!(hdr, 0x40, "expected PUBACK, got {hdr:#x}");
        let reason = if body.len() >= 3 { body[2] } else { 0x00 };
        if reason < 0x80 {
            acked_ok += 1;
        } else {
            refused_reasons.push(reason);
        }
    }

    stop.store(true, Ordering::Relaxed);
    for h in churn_handles {
        h.join().unwrap();
    }
    sub_handle.join().unwrap();

    if !refused_reasons.is_empty() {
        let log =
            std::fs::read_to_string(broker.directory.join("events.jsonl")).unwrap_or_default();
        let lines: Vec<&str> = log.lines().collect();
        if let Some(pos) = lines.iter().position(|l| {
            l.contains("Rejected") || l.contains("Precondition") || l.contains("refused")
        }) {
            let start = pos.saturating_sub(20);
            let end = (pos + 10).min(lines.len());
            eprintln!(
                "=== events.jsonl around first refusal (line {pos}) ===\n{}",
                lines[start..end].join("\n")
            );
        } else {
            eprintln!(
                "=== events.jsonl tail ===\n{}",
                lines[lines.len().saturating_sub(30)..].join("\n")
            );
        }
    }
    assert!(
        refused_reasons.is_empty(),
        "mode={mode}: unrelated session churn refused {}/{} QoS 1 publications (churn_cycles={}, reasons={:?})",
        refused_reasons.len(),
        total_messages,
        churn_cycles.load(Ordering::Relaxed),
        &refused_reasons[..refused_reasons.len().min(16)]
    );
    assert_eq!(
        sub_received.load(Ordering::Relaxed),
        total_messages,
        "mode={mode}: subscriber missed acknowledged QoS 1 publications"
    );
}

#[test]
fn unrelated_session_close_does_not_refuse_qos1_in_log_mode() {
    run_session_close_churn_does_not_refuse_unrelated_qos1("log", 800);
}

#[test]
fn unrelated_session_close_does_not_refuse_qos1_in_durable_mode() {
    run_session_close_churn_does_not_refuse_unrelated_qos1("durable", 300);
}

#[test]
fn unrelated_session_close_does_not_refuse_qos1_in_volatile_mode() {
    run_session_close_churn_does_not_refuse_unrelated_qos1("volatile", 800);
}

fn run_fanin_qos1_drains_promptly_without_refusals(
    mode: &str,
    publishers: usize,
    per_publisher: usize,
    sub_receive_maximum: Option<u16>,
) {
    let broker = Broker::start(&format!("fanin-{mode}"), mode);
    let addr = broker.address;
    let total = publishers * per_publisher;

    let mut sub = connect_v5_with_rm(addr, "fanin-sub", 0, sub_receive_maximum);
    subscribe_v5(&mut sub, 1, "fanin/topic", 1);

    let sub_received = Arc::new(AtomicUsize::new(0));
    let sub_received_t = Arc::clone(&sub_received);
    let sub_handle = thread::spawn(move || {
        while sub_received_t.load(Ordering::Relaxed) < total {
            let (hdr, body) = match read_packet(&mut sub) {
                Ok(pkt) => pkt,
                Err(_) => break,
            };
            if (hdr & 0xf0) == 0x30 {
                let tlen = usize::from(u16::from_be_bytes([body[0], body[1]]));
                let pid = u16::from_be_bytes([body[2 + tlen], body[3 + tlen]]);
                let mut ack = Vec::with_capacity(4);
                ack.extend_from_slice(&pid.to_be_bytes());
                sub.write_all(&encode_packet(0x40, &ack)).unwrap();
                sub_received_t.fetch_add(1, Ordering::Relaxed);
            }
        }
    });

    let refused = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();
    let mut pub_handles = Vec::with_capacity(publishers);
    for idx in 0..publishers {
        let refused_t = Arc::clone(&refused);
        pub_handles.push(thread::spawn(move || {
            let mut stream = connect_v5(addr, &format!("fanin-pub-{idx}"), 0);
            let payload = [b'p'; 128];
            let window = 8usize;
            let mut sent = 0usize;
            let mut acked = 0usize;
            while acked < per_publisher {
                while sent < per_publisher && sent - acked < window {
                    let pid = ((sent % 60_000) + 1) as u16;
                    stream
                        .write_all(&publish_qos1_packet(pid, "fanin/topic", &payload))
                        .unwrap();
                    sent += 1;
                }
                let (hdr, body) = read_packet(&mut stream).unwrap();
                assert_eq!(hdr, 0x40, "expected PUBACK");
                let reason = if body.len() >= 3 { body[2] } else { 0x00 };
                if reason >= 0x80 {
                    refused_t.fetch_add(1, Ordering::Relaxed);
                }
                acked += 1;
            }
        }));
    }

    for h in pub_handles {
        h.join().unwrap();
    }
    sub_handle.join().unwrap();
    let elapsed = started.elapsed();

    assert_eq!(
        refused.load(Ordering::Relaxed),
        0,
        "mode={mode} rm={sub_receive_maximum:?}: fan-in refused QoS 1 publications"
    );
    assert_eq!(
        sub_received.load(Ordering::Relaxed),
        total,
        "mode={mode} rm={sub_receive_maximum:?}: subscriber did not drain all {total} fan-in messages (elapsed={elapsed:?})"
    );
    assert!(
        elapsed < Duration::from_secs(8),
        "mode={mode} rm={sub_receive_maximum:?}: fan-in drain stalled ({elapsed:?} for {total} messages)"
    );
}

#[test]
fn fanin_16x1_qos1_zero_refusals_and_fast_drain_in_volatile_mode() {
    run_fanin_qos1_drains_promptly_without_refusals("volatile", 16, 200, None);
}

#[test]
fn fanin_16x1_qos1_zero_refusals_and_fast_drain_in_log_mode() {
    run_fanin_qos1_drains_promptly_without_refusals("log", 16, 150, None);
}

#[test]
fn fanin_16x1_qos1_constrained_receive_maximum_drains_without_cursor_reset_in_log_mode() {
    // Forces backlog beyond subscriber Receive Maximum (16 < 16*8 = 128),
    // exercising monotonic known_through cursor and non-invalidated LoadWork.
    run_fanin_qos1_drains_promptly_without_refusals("log", 16, 120, Some(16));
}

fn run_fanout_1x4_with_slow_subscriber(mode: &str, count: usize) {
    let broker = Broker::start(&format!("fanout-1x4-{mode}"), mode);
    let addr = broker.address;
    let topic = "fanout/partial";

    // 3 fast subscribers (default RM) + 1 slow subscriber (RM=2, delayed PUBACK).
    let mut sub_streams = Vec::new();
    for i in 0..4 {
        let rm = if i == 3 { Some(2) } else { None };
        let mut s = connect_v5_with_rm(addr, &format!("sub-{i}"), 0, rm);
        subscribe_v5(&mut s, 1, topic, 1);
        sub_streams.push(s);
    }

    let fast_received = Arc::new(AtomicUsize::new(0));
    let slow_received = Arc::new(AtomicUsize::new(0));
    let mut sub_handles = Vec::new();
    for (i, mut s) in sub_streams.into_iter().enumerate() {
        let is_slow = i == 3;
        let counter = if is_slow {
            Arc::clone(&slow_received)
        } else {
            Arc::clone(&fast_received)
        };
        sub_handles.push(thread::spawn(move || {
            let mut got = 0usize;
            while got < count {
                let (hdr, body) = read_packet(&mut s).expect("read publish");
                assert_eq!(
                    hdr & 0xf0,
                    0x30,
                    "expected PUBLISH on sub-{i}, got {hdr:#x}"
                );
                let tlen = usize::from(u16::from_be_bytes([body[0], body[1]]));
                let pid = u16::from_be_bytes([body[2 + tlen], body[3 + tlen]]);
                if is_slow {
                    thread::sleep(Duration::from_millis(2));
                }
                s.write_all(&encode_packet(0x40, &pid.to_be_bytes()))
                    .unwrap();
                got += 1;
                counter.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    thread::sleep(Duration::from_millis(20));

    let mut pub_stream = connect_v5(addr, "pub-fanout", 0);
    let payload = [b'f'; 128];
    let started = Instant::now();
    let mut refused = 0usize;
    let mut sent = 0usize;
    let mut acked = 0usize;
    while acked < count {
        while sent < count && sent - acked < 8 {
            let pid = ((sent % 60_000) + 1) as u16;
            pub_stream
                .write_all(&publish_qos1_packet(pid, topic, &payload))
                .unwrap();
            sent += 1;
        }
        let (hdr, body) = read_packet(&mut pub_stream).expect("read puback");
        assert_eq!(hdr, 0x40, "expected PUBACK, got {hdr:#x}");
        if body.len() >= 3 && body[2] >= 0x80 {
            refused += 1;
        }
        acked += 1;
    }
    let pub_elapsed = started.elapsed();

    for h in sub_handles {
        h.join().unwrap();
    }

    assert_eq!(refused, 0, "mode={mode}: fan-out refused QoS 1 publishes");
    assert_eq!(fast_received.load(Ordering::Relaxed), count * 3);
    assert_eq!(slow_received.load(Ordering::Relaxed), count);
    assert!(
        pub_elapsed < Duration::from_secs(3),
        "mode={mode}: publisher stalled behind slow subscriber ({pub_elapsed:?})"
    );
}

#[test]
fn fanout_1x4_partial_inline_claims_with_slow_subscriber_in_volatile_mode() {
    run_fanout_1x4_with_slow_subscriber("volatile", 120);
}

#[test]
fn fanout_1x4_partial_inline_claims_with_slow_subscriber_in_log_mode() {
    run_fanout_1x4_with_slow_subscriber("log", 120);
}
