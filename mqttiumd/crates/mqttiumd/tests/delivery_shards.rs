//! End-to-end TCP wire and crash-recovery integration tests for Lot 10
//! multi-core resident `DeliveryShard` workers (`--delivery-shards K`).
#![cfg(target_os = "linux")]

use std::collections::BTreeSet;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

struct Broker {
    child: Child,
    address: SocketAddr,
    directory: PathBuf,
    keep_dir: bool,
}

impl Broker {
    fn start(name: &str, mode: &str, shards: usize) -> Self {
        static SERIAL: AtomicU32 = AtomicU32::new(0);
        let directory = std::env::temp_dir().join(format!(
            "mqttiumd-shards-{name}-{}-{}",
            std::process::id(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).unwrap();
        Self::spawn_in(directory, mode, shards, false, false, 0)
    }

    fn spawn_in(
        directory: PathBuf,
        mode: &str,
        shards: usize,
        existing_db: bool,
        keep_dir: bool,
        generation: usize,
    ) -> Self {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_mqttiumd"));
        let db_flag = if existing_db { "--existing-db" } else { "--db" };
        cmd.arg(db_flag)
            .arg(directory.join("store.sqlite"))
            .args(["--listen", "127.0.0.1:0"])
            .arg("--events")
            .arg(directory.join(format!("events-{generation}.jsonl")))
            .args(["--storage-mode", mode])
            .args(["--delivery-shards", &shards.to_string()])
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
            keep_dir,
        }
    }

    fn kill_hard(mut self) -> PathBuf {
        self.keep_dir = true;
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.directory.clone()
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        if std::thread::panicking() {
            if let Ok(status) = self.child.try_wait() {
                eprintln!("=== broker status on panic: {status:?} ===");
            }
            if let Ok(entries) = std::fs::read_dir(&self.directory) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().and_then(|e| e.to_str()) == Some("jsonl")
                        && let Ok(text) = std::fs::read_to_string(&path)
                    {
                        let lines: Vec<_> = text.lines().collect();
                        let start = lines.len().saturating_sub(25);
                        eprintln!("=== tail {} ===", path.display());
                        for l in &lines[start..] {
                            eprintln!("{l}");
                        }
                    }
                }
            }
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
        if !self.keep_dir {
            let _ = std::fs::remove_dir_all(&self.directory);
        }
    }
}

#[test]
fn startup_reports_the_effective_collection_identifier_and_expiry_bounds() {
    let broker = Broker::start("limits", "volatile", 2);
    let text = std::fs::read_to_string(broker.directory.join("events-0.jsonl")).unwrap();
    let events: Vec<serde_json::Value> = text
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let details = |kind: &str| {
        &events.iter().find(|event| event["kind"] == kind).unwrap()["details"]
    };
    let ready = details("ready");
    assert_eq!(ready["outgoing_packet_identifier_probe_limit"], 32_769);
    assert_eq!(ready["outgoing_packet_identifier_bitmap_bits"], 512);
    assert_eq!(ready["outgoing_packet_identifier_fallback_probes"], 32_257);
    assert_eq!(ready["qos0_window"], 224);
    assert_eq!(ready["qos0_output_reserve"], 32);
    assert_eq!(ready["output_slots"], 256);
    assert_eq!(ready["session_expiry_timer_slots"], 4096);
    assert_eq!(ready["session_expiry_per_turn"], 4);
    assert_eq!(details("message_expiry_clock")["batch"], 4);
    let policy = details("receipt_policy");
    assert_eq!(policy["receipt_collection_rows"], 512);
    assert_eq!(policy["receipt_collection_bytes"], 24 * 1024 * 1024);
    assert_eq!(policy["receipt_collection_field_bytes"], 2 * 1024 * 1024);
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

fn connect_v5_opts(
    addr: SocketAddr,
    client_id: &str,
    clean_start: bool,
    session_expiry: u32,
    receive_maximum: Option<u16>,
    max_packet_size: Option<u32>,
) -> (TcpStream, bool) {
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
    body.push(5);
    body.push(if clean_start { 0x02 } else { 0x00 });
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
    if let Some(mps) = max_packet_size {
        props.push(0x27);
        props.extend_from_slice(&mps.to_be_bytes());
    }
    body.push(u8::try_from(props.len()).unwrap());
    body.extend_from_slice(&props);
    push_str(client_id, &mut body);
    stream.write_all(&encode_packet(0x10, &body)).unwrap();
    let (hdr, resp) = read_packet(&mut stream).unwrap();
    assert_eq!(hdr, 0x20, "expected CONNACK for {client_id}");
    assert_eq!(resp[1], 0x00, "CONNACK refused for {client_id}: {resp:?}");
    let session_present = (resp[0] & 0x01) != 0;
    (stream, session_present)
}

fn connect_v5(addr: SocketAddr, client_id: &str) -> TcpStream {
    connect_v5_opts(addr, client_id, true, 0, None, None).0
}

fn subscribe_v5(stream: &mut TcpStream, pid: u16, filter: &str, qos: u8) {
    let mut body = Vec::new();
    body.extend_from_slice(&pid.to_be_bytes());
    body.push(0);
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

fn publish_packet(qos: u8, retain: bool, pid: Option<u16>, topic: &str, payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(topic.len() + payload.len() + 8);
    push_str(topic, &mut body);
    if let Some(pid) = pid {
        body.extend_from_slice(&pid.to_be_bytes());
    }
    body.push(0);
    body.extend_from_slice(payload);
    let flags = (qos << 1) | u8::from(retain);
    encode_packet(0x30 | flags, &body)
}

fn decode_v5_publish(hdr: u8, body: &[u8]) -> (u8, bool, String, Option<u16>, Vec<u8>) {
    let qos = (hdr >> 1) & 0x03;
    let retain = (hdr & 0x01) != 0;
    let tlen = usize::from(u16::from_be_bytes([body[0], body[1]]));
    let topic = std::str::from_utf8(&body[2..2 + tlen]).unwrap().to_owned();
    let mut pos = 2 + tlen;
    let pid = if qos > 0 {
        let p = u16::from_be_bytes([body[pos], body[pos + 1]]);
        pos += 2;
        Some(p)
    } else {
        None
    };
    // Decode property length varint
    let mut prop_len = 0usize;
    let mut shift = 0;
    loop {
        let b = body[pos];
        pos += 1;
        prop_len |= usize::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    pos += prop_len;
    (qos, retain, topic, pid, body[pos..].to_vec())
}

fn send_ack(stream: &mut TcpStream, first: u8, pid: u16) {
    stream
        .write_all(&encode_packet(first, &pid.to_be_bytes()))
        .unwrap();
}

fn run_fanout_1x16(mode: &'static str, total: usize) {
    let broker = Broker::start(&format!("fanout-{mode}"), mode, 2);
    let addr = broker.address;
    let n_subs = 16usize;

    let mut sub_handles = Vec::with_capacity(n_subs);
    let ready = Arc::new(AtomicUsize::new(0));
    for i in 0..n_subs {
        let ready = Arc::clone(&ready);
        sub_handles.push(thread::spawn(move || {
            let mut sub = connect_v5(addr, &format!("fan-sub-{i}"));
            subscribe_v5(&mut sub, 1, "fanout/topic", 1);
            ready.fetch_add(1, Ordering::Release);
            let mut expected = 0usize;
            while expected < total {
                let (hdr, body) = read_packet(&mut sub).unwrap();
                assert_eq!(hdr & 0xf0, 0x30, "sub {i} unexpected packet {hdr:#x}");
                let (qos, _retain, topic, pid, payload) = decode_v5_publish(hdr, &body);
                assert_eq!(qos, 1);
                assert_eq!(topic, "fanout/topic");
                let got = usize::from_be_bytes(payload[..8].try_into().unwrap());
                assert_eq!(
                    got, expected,
                    "sub {i} out of FIFO order in {mode}: expected {expected}, got {got}"
                );
                send_ack(&mut sub, 0x40, pid.unwrap());
                expected += 1;
            }
            let _ = sub.write_all(&[0xe0, 0x00]);
        }));
    }

    let wait = Instant::now();
    while ready.load(Ordering::Acquire) < n_subs {
        assert!(wait.elapsed() < Duration::from_secs(10));
        thread::sleep(Duration::from_millis(2));
    }

    let mut pub_stream = connect_v5(addr, "fan-pub");
    let window = 8usize;
    let mut sent = 0usize;
    let mut acked = 0usize;
    while acked < total {
        while sent < total && sent - acked < window {
            let pid = ((sent % 60_000) + 1) as u16;
            let mut payload = [0u8; 64];
            payload[..8].copy_from_slice(&sent.to_be_bytes());
            pub_stream
                .write_all(&publish_packet(
                    1,
                    false,
                    Some(pid),
                    "fanout/topic",
                    &payload,
                ))
                .unwrap();
            sent += 1;
        }
        let (hdr, body) = read_packet(&mut pub_stream).unwrap();
        assert_eq!(hdr, 0x40, "expected PUBACK");
        let reason = if body.len() >= 3 { body[2] } else { 0x00 };
        assert!(reason < 0x80, "refused PUBACK reason {reason:#x} in {mode}");
        acked += 1;
    }

    for h in sub_handles {
        h.join().unwrap();
    }
}

#[test]
fn sharded_fanout_1x16_qos1_fifo_exact_delivery_across_storage_modes() {
    run_fanout_1x16("volatile", 400);
    run_fanout_1x16("log", 400);
    run_fanout_1x16("durable", 60);
}

#[test]
fn sharded_slow_subscriber_isolation_and_fifo_drain() {
    let broker = Broker::start("slow-iso", "log", 2);
    let addr = broker.address;
    let total = 80usize;

    let ready = Arc::new(AtomicUsize::new(0));
    let fast_done = Arc::new(AtomicUsize::new(0));
    let slow_done = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    // 1 slow subscriber with RM=2
    {
        let ready = Arc::clone(&ready);
        let slow_done = Arc::clone(&slow_done);
        handles.push(thread::spawn(move || {
            let (mut sub, _) = connect_v5_opts(addr, "slow-sub", true, 0, Some(2), None);
            subscribe_v5(&mut sub, 1, "iso/topic", 1);
            ready.fetch_add(1, Ordering::Release);
            let mut expected = 0usize;
            while expected < total {
                let (hdr, body) = read_packet(&mut sub).unwrap();
                assert_eq!(hdr & 0xf0, 0x30);
                let (_qos, _, _, pid, payload) = decode_v5_publish(hdr, &body);
                let got = usize::from_be_bytes(payload[..8].try_into().unwrap());
                assert_eq!(got, expected, "slow-sub FIFO violation");
                thread::sleep(Duration::from_millis(8));
                send_ack(&mut sub, 0x40, pid.unwrap());
                expected += 1;
            }
            slow_done.fetch_add(1, Ordering::Release);
        }));
    }

    // 3 fast subscribers across shards
    for i in 0..3usize {
        let ready = Arc::clone(&ready);
        let fast_done = Arc::clone(&fast_done);
        handles.push(thread::spawn(move || {
            let mut sub = connect_v5(addr, &format!("fast-sub-{i}"));
            subscribe_v5(&mut sub, 1, "iso/topic", 1);
            ready.fetch_add(1, Ordering::Release);
            let mut expected = 0usize;
            while expected < total {
                let (hdr, body) = read_packet(&mut sub).unwrap();
                assert_eq!(hdr & 0xf0, 0x30);
                let (_qos, _, _, pid, payload) = decode_v5_publish(hdr, &body);
                let got = usize::from_be_bytes(payload[..8].try_into().unwrap());
                assert_eq!(got, expected, "fast-sub-{i} FIFO violation");
                send_ack(&mut sub, 0x40, pid.unwrap());
                expected += 1;
            }
            fast_done.fetch_add(1, Ordering::Release);
        }));
    }

    while ready.load(Ordering::Acquire) < 4 {
        thread::sleep(Duration::from_millis(2));
    }

    let start = Instant::now();
    let mut pub_stream = connect_v5(addr, "iso-pub");
    for seq in 0..total {
        let pid = (seq + 1) as u16;
        let mut payload = [0u8; 32];
        payload[..8].copy_from_slice(&seq.to_be_bytes());
        pub_stream
            .write_all(&publish_packet(1, false, Some(pid), "iso/topic", &payload))
            .unwrap();
        let (hdr, body) = read_packet(&mut pub_stream).unwrap();
        assert_eq!(hdr, 0x40);
        let reason = if body.len() >= 3 { body[2] } else { 0x00 };
        assert!(reason < 0x80);
    }

    // Fast subscribers should finish well before the slow subscriber finishes 80 * 8ms = 640ms.
    let wait = Instant::now();
    while fast_done.load(Ordering::Acquire) < 3 && wait.elapsed() < Duration::from_secs(5) {
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(fast_done.load(Ordering::Acquire), 3);
    assert!(
        start.elapsed() < Duration::from_millis(550),
        "fast subscribers appeared blocked by slow subscriber: {:?}",
        start.elapsed()
    );

    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(slow_done.load(Ordering::Acquire), 1);
}

#[test]
fn sharded_bidirectional_fanin_qos2_and_max_packet_discard() {
    let broker = Broker::start("bidi-q2", "log", 2);
    let addr = broker.address;

    // Part A: 8 bidirectional clients on the SAME TCP socket (publish QoS 1 + receive QoS 1 + PUBACK)
    let per_client = 30usize;
    let n_clients = 8usize;
    let ready = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for i in 0..n_clients {
        let ready = Arc::clone(&ready);
        handles.push(thread::spawn(move || {
            let mut stream = connect_v5(addr, &format!("bidi-{i}"));
            let my_sub = format!("bidi/ring/{i}");
            let my_pub = format!("bidi/ring/{}", (i + 1) % n_clients);
            subscribe_v5(&mut stream, 1, &my_sub, 1);
            ready.fetch_add(1, Ordering::Release);
            while ready.load(Ordering::Acquire) < n_clients {
                thread::sleep(Duration::from_millis(2));
            }
            let mut sent = 0usize;
            let mut pubacks = 0usize;
            let mut received = 0usize;
            while pubacks < per_client || received < per_client {
                while sent < per_client && sent - pubacks < 4 {
                    let pid = (sent + 10) as u16;
                    stream
                        .write_all(&publish_packet(
                            1,
                            false,
                            Some(pid),
                            &my_pub,
                            &sent.to_be_bytes(),
                        ))
                        .unwrap();
                    sent += 1;
                }
                let (hdr, body) = read_packet(&mut stream).unwrap();
                match hdr & 0xf0 {
                    0x40 => {
                        let reason = if body.len() >= 3 { body[2] } else { 0x00 };
                        assert!(reason < 0x80, "bidi-{i} refused PUBACK {reason:#x}");
                        pubacks += 1;
                    }
                    0x30 => {
                        let (_qos, _, _, pid, payload) = decode_v5_publish(hdr, &body);
                        let got = usize::from_be_bytes(payload[..8].try_into().unwrap());
                        assert_eq!(got, received, "bidi-{i} FIFO mismatch");
                        send_ack(&mut stream, 0x40, pid.unwrap());
                        received += 1;
                    }
                    other => panic!("bidi-{i} unexpected packet {other:#x}"),
                }
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    // Part B: QoS 2 end-to-end (PUBLISH -> PUBREC -> PUBREL -> PUBCOMP) through DeliveryShard
    let mut sub2 = connect_v5(addr, "q2-sub");
    subscribe_v5(&mut sub2, 1, "q2/topic", 2);
    let sub2_handle = thread::spawn(move || {
        let mut expected_pub = 0usize;
        let mut completed = 0usize;
        let mut awaiting_pubrel = BTreeSet::new();
        while completed < 16 {
            let (hdr, body) = read_packet(&mut sub2).unwrap();
            match hdr & 0xf0 {
                0x30 => {
                    let (qos, _, _, pid, payload) = decode_v5_publish(hdr, &body);
                    assert_eq!(qos, 2);
                    let pid = pid.unwrap();
                    assert_eq!(
                        usize::from_be_bytes(payload[..8].try_into().unwrap()),
                        expected_pub
                    );
                    expected_pub += 1;
                    assert!(awaiting_pubrel.insert(pid));
                    send_ack(&mut sub2, 0x50, pid); // PUBREC
                }
                0x60 => {
                    assert_eq!(hdr, 0x62, "expected PUBREL");
                    let pid = u16::from_be_bytes([body[0], body[1]]);
                    assert!(awaiting_pubrel.remove(&pid), "unexpected PUBREL pid {pid}");
                    send_ack(&mut sub2, 0x70, pid); // PUBCOMP
                    completed += 1;
                }
                other => panic!("unexpected packet in q2-sub: {other:#x}"),
            }
        }
        assert_eq!(expected_pub, 16);
        assert!(awaiting_pubrel.is_empty());
    });
    let mut pub2 = connect_v5(addr, "q2-pub");
    for seq in 0..16usize {
        let pid = (seq + 1) as u16;
        pub2.write_all(&publish_packet(
            2,
            false,
            Some(pid),
            "q2/topic",
            &seq.to_be_bytes(),
        ))
        .unwrap();
        let (hdr, body) = read_packet(&mut pub2).unwrap();
        assert_eq!(hdr, 0x50, "expected PUBREC");
        assert_eq!(u16::from_be_bytes([body[0], body[1]]), pid);
        send_ack(&mut pub2, 0x62, pid); // PUBREL
        let (hdr2, body2) = read_packet(&mut pub2).unwrap();
        assert_eq!(hdr2, 0x70, "expected PUBCOMP");
        assert_eq!(u16::from_be_bytes([body2[0], body2[1]]), pid);
    }
    sub2_handle.join().unwrap();

    // Part C: Maximum Packet Size discard (oversized skipped, small delivered)
    let (mut small_sub, _) = connect_v5_opts(addr, "small-sub", true, 0, None, Some(40));
    subscribe_v5(&mut small_sub, 1, "mps/topic", 1);
    let mut mps_pub = connect_v5(addr, "mps-pub");
    let big_payload = vec![b'B'; 128];
    mps_pub
        .write_all(&publish_packet(
            1,
            false,
            Some(1),
            "mps/topic",
            &big_payload,
        ))
        .unwrap();
    let (h1, _) = read_packet(&mut mps_pub).unwrap();
    assert_eq!(h1, 0x40);
    mps_pub
        .write_all(&publish_packet(1, false, Some(2), "mps/topic", b"ok-small"))
        .unwrap();
    let (h2, _) = read_packet(&mut mps_pub).unwrap();
    assert_eq!(h2, 0x40);

    let (hdr, body) = read_packet(&mut small_sub).unwrap();
    assert_eq!(hdr & 0xf0, 0x30);
    let (_qos, _, _, pid, payload) = decode_v5_publish(hdr, &body);
    assert_eq!(payload, b"ok-small");
    send_ack(&mut small_sub, 0x40, pid.unwrap());
}

#[test]
fn sharded_qos0_live_burst_and_retained_replay() {
    const BURST: usize = 150;
    for mode in ["volatile", "log"] {
        let broker = Broker::start(&format!("qos0-ret-{mode}"), mode, 2);
        let addr = broker.address;

        // Publish retained QoS 1 and retained QoS 0 before subscribers exist.
        let mut pub_stream = connect_v5(addr, "ret-pub");
        pub_stream
            .write_all(&publish_packet(1, true, Some(1), "ret/q1", b"retained-q1"))
            .unwrap();
        let (h, _) = read_packet(&mut pub_stream).unwrap();
        assert_eq!(h, 0x40);
        pub_stream
            .write_all(&publish_packet(0, true, None, "ret/q0", b"retained-q0"))
            .unwrap();
        // Sentinel ping to ensure QoS 0 retained publish is committed.
        pub_stream.write_all(&[0xc0, 0x00]).unwrap();
        let (ping, _) = read_packet(&mut pub_stream).unwrap();
        assert_eq!(ping, 0xd0);

        let mut subs = Vec::new();
        for i in 0..4usize {
            let mut sub = connect_v5(addr, &format!("q0-sub-{i}"));
            subscribe_v5(&mut sub, 1, "ret/#", 1);
            let mut got_ret = BTreeSet::new();
            for _ in 0..2 {
                let (hdr, body) = read_packet(&mut sub).unwrap();
                assert_eq!(hdr & 0xf0, 0x30);
                let (qos, retain, topic, pid, payload) = decode_v5_publish(hdr, &body);
                assert!(retain, "expected retain flag on replay in {mode}");
                if let Some(pid) = pid {
                    send_ack(&mut sub, 0x40, pid);
                }
                got_ret.insert((qos, topic, payload));
            }
            assert!(got_ret.contains(&(1, "ret/q1".to_owned(), b"retained-q1".to_vec())));
            assert!(got_ret.contains(&(0, "ret/q0".to_owned(), b"retained-q0".to_vec())));
            subscribe_v5(&mut sub, 2, "live/q0", 1);
            subs.push(sub);
        }

        for seq in 0..BURST {
            pub_stream
                .write_all(&publish_packet(
                    0,
                    false,
                    None,
                    "live/q0",
                    &seq.to_be_bytes(),
                ))
                .unwrap();
        }
        pub_stream
            .write_all(&publish_packet(1, false, Some(2), "live/q0", b"SENTINEL"))
            .unwrap();
        let (h, _) = read_packet(&mut pub_stream).unwrap();
        assert_eq!(h, 0x40);

        for (i, mut sub) in subs.into_iter().enumerate() {
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut next_seq = 0usize;
            let mut saw_sentinel = false;
            // MQTT orders the QoS 0 stream independently of the QoS 1 sentinel.
            for _ in 0..=BURST {
                let remaining = deadline.saturating_duration_since(Instant::now());
                assert!(!remaining.is_zero(), "sub {i} in {mode}: drain deadline");
                sub.set_read_timeout(Some(remaining)).unwrap();
                let (hdr, body) = read_packet(&mut sub).unwrap_or_else(|error| {
                    panic!("sub {i} in {mode}: {next_seq}/{BURST} QoS 0, sentinel={saw_sentinel}: {error}")
                });
                assert_eq!(hdr & 0xf0, 0x30);
                let (qos, _, topic, pid, payload) = decode_v5_publish(hdr, &body);
                assert_eq!(topic, "live/q0");
                if qos == 1 {
                    assert_eq!(payload, b"SENTINEL");
                    assert!(!saw_sentinel, "sub {i} in {mode}: duplicate sentinel");
                    saw_sentinel = true;
                    send_ack(&mut sub, 0x40, pid.unwrap());
                } else {
                    assert_eq!(qos, 0);
                    assert_eq!(payload, next_seq.to_be_bytes(), "sub {i} in {mode}");
                    next_seq += 1;
                }
            }
            assert!(saw_sentinel, "sub {i} in {mode}: missing sentinel");
            assert_eq!(next_seq, BURST, "sub {i} in {mode}");
        }
    }
}

#[test]
fn sharded_churn_takeover_and_persistent_reconnect_replay() {
    let broker = Broker::start("churn-persist", "log", 2);
    let addr = broker.address;

    let (mut psub, present0) = connect_v5_opts(addr, "persist-sub", true, 300, None, None);
    assert!(!present0);
    subscribe_v5(&mut psub, 1, "persist/topic", 1);

    let mut pub_stream = connect_v5(addr, "persist-pub");
    // Phase 1: 20 live messages while connected
    for seq in 0..20usize {
        pub_stream
            .write_all(&publish_packet(
                1,
                false,
                Some((seq + 1) as u16),
                "persist/topic",
                &seq.to_be_bytes(),
            ))
            .unwrap();
        let (h, _) = read_packet(&mut pub_stream).unwrap();
        assert_eq!(h, 0x40);
        let (hdr, body) = read_packet(&mut psub).unwrap();
        let (_, _, _, pid, payload) = decode_v5_publish(hdr, &body);
        assert_eq!(usize::from_be_bytes(payload[..8].try_into().unwrap()), seq);
        send_ack(&mut psub, 0x40, pid.unwrap());
    }
    // Clean disconnect preserving session (expiry=300)
    psub.write_all(&[0xe0, 0x00]).unwrap();
    drop(psub);

    // Phase 2: Publish 40 messages while offline + concurrent unrelated churn
    let stop = Arc::new(AtomicBool::new(false));
    let mut churners = Vec::new();
    for w in 0..3usize {
        let stop = Arc::clone(&stop);
        churners.push(thread::spawn(move || {
            let mut n = 0usize;
            while !stop.load(Ordering::Relaxed) {
                n += 1;
                let mut s = connect_v5(addr, &format!("sh-churn-{w}-{n}"));
                subscribe_v5(&mut s, 1, &format!("other/{w}/{n}"), 1);
                let _ = s.write_all(&[0xe0, 0x00]);
            }
        }));
    }
    for seq in 20..60usize {
        pub_stream
            .write_all(&publish_packet(
                1,
                false,
                Some((seq + 1) as u16),
                "persist/topic",
                &seq.to_be_bytes(),
            ))
            .unwrap();
        let (h, body) = read_packet(&mut pub_stream).unwrap();
        assert_eq!(h, 0x40);
        let reason = if body.len() >= 3 { body[2] } else { 0x00 };
        assert!(reason < 0x80, "refused during churn: {reason:#x}");
    }
    stop.store(true, Ordering::Relaxed);
    for h in churners {
        h.join().unwrap();
    }

    // Phase 3: Reconnect clean_start=false -> receive 20..60 in FIFO order
    let (mut psub2, present1) = connect_v5_opts(addr, "persist-sub", false, 300, None, None);
    assert!(present1, "expected session_present=true on reconnect");
    for expected in 20..60usize {
        let (hdr, body) = read_packet(&mut psub2).unwrap();
        assert_eq!(hdr & 0xf0, 0x30);
        let (_, _, _, pid, payload) = decode_v5_publish(hdr, &body);
        let got = usize::from_be_bytes(payload[..8].try_into().unwrap());
        assert_eq!(got, expected, "offline replay FIFO mismatch");
        send_ack(&mut psub2, 0x40, pid.unwrap());
    }

    // Phase 4: Takeover while connected -> old gets DISCONNECT 0x8e, new gets next live messages
    let (mut psub3, present2) = connect_v5_opts(addr, "persist-sub", false, 300, None, None);
    assert!(present2);
    let (disc_hdr, disc_body) = read_packet(&mut psub2).unwrap();
    assert_eq!(disc_hdr, 0xe0, "expected DISCONNECT on taken-over peer");
    assert_eq!(disc_body.first().copied(), Some(0x8e));
    drop(psub2);

    for seq in 60..75usize {
        pub_stream
            .write_all(&publish_packet(
                1,
                false,
                Some((seq + 1) as u16),
                "persist/topic",
                &seq.to_be_bytes(),
            ))
            .unwrap();
        let (h, _) = read_packet(&mut pub_stream).unwrap();
        assert_eq!(h, 0x40);
        let (hdr, body) = read_packet(&mut psub3).unwrap();
        let (_, _, _, pid, payload) = decode_v5_publish(hdr, &body);
        assert_eq!(usize::from_be_bytes(payload[..8].try_into().unwrap()), seq);
        send_ack(&mut psub3, 0x40, pid.unwrap());
    }
}

#[test]
fn sharded_malformed_puback_closes_only_offending_connection() {
    let broker = Broker::start("bad-puback", "log", 2);
    let addr = broker.address;

    let mut bad = connect_v5(addr, "bad-ack-sub");
    subscribe_v5(&mut bad, 1, "health/topic", 1);
    let mut good = connect_v5(addr, "good-ack-sub");
    subscribe_v5(&mut good, 1, "health/topic", 1);

    // Send malformed PUBACK with packet_id = 0 (protocol error / malformed packet)
    bad.write_all(&encode_packet(0x40, &[0x00, 0x00])).unwrap();
    // Expect DISCONNECT (0xe0) or immediate EOF
    if let Ok((hdr, _)) = read_packet(&mut bad) {
        assert_eq!(hdr, 0xe0, "expected DISCONNECT for malformed PUBACK");
    }

    // Good subscriber remains completely healthy.
    let mut pub_stream = connect_v5(addr, "health-pub");
    pub_stream
        .write_all(&publish_packet(1, false, Some(1), "health/topic", b"alive"))
        .unwrap();
    let (h, _) = read_packet(&mut pub_stream).unwrap();
    assert_eq!(h, 0x40);
    let (hdr, body) = read_packet(&mut good).unwrap();
    let (_, _, _, pid, payload) = decode_v5_publish(hdr, &body);
    assert_eq!(payload, b"alive");
    send_ack(&mut good, 0x40, pid.unwrap());
}

fn run_sigkill_crash_recovery(mode: &str) {
    let dir = {
        let broker = Broker::start(&format!("kill-{mode}"), mode, 2);
        let addr = broker.address;

        let (mut sub, _) = connect_v5_opts(addr, "crash-sub", true, 300, None, None);
        subscribe_v5(&mut sub, 1, "crash/topic", 1);

        let mut pub_stream = connect_v5(addr, "crash-pub");
        // Phase 1: Deliver and ACK 0..10 (finished by shard owner 1..=2)
        for seq in 0..10usize {
            pub_stream
                .write_all(&publish_packet(
                    1,
                    false,
                    Some((seq + 1) as u16),
                    "crash/topic",
                    &seq.to_be_bytes(),
                ))
                .unwrap();
            let (h, _) = read_packet(&mut pub_stream).unwrap();
            assert_eq!(h, 0x40);
            let (hdr, body) = read_packet(&mut sub).unwrap();
            let (_, _, _, pid, payload) = decode_v5_publish(hdr, &body);
            assert_eq!(usize::from_be_bytes(payload[..8].try_into().unwrap()), seq);
            send_ack(&mut sub, 0x40, pid.unwrap());
        }
        // Let shard finish-delivery commits settle on disk before disconnecting sub.
        thread::sleep(Duration::from_millis(80));
        sub.write_all(&[0xe0, 0x00]).unwrap();
        drop(sub);
        thread::sleep(Duration::from_millis(50));

        // Phase 2: Publish 10..25 while subscriber is detached (committed by Coordinator owner 0)
        for seq in 10..25usize {
            pub_stream
                .write_all(&publish_packet(
                    1,
                    false,
                    Some((seq + 1) as u16),
                    "crash/topic",
                    &seq.to_be_bytes(),
                ))
                .unwrap();
            let (h, _) = read_packet(&mut pub_stream).unwrap();
            assert_eq!(h, 0x40);
        }

        // Hard SIGKILL!
        broker.kill_hard()
    };

    // Restart #1: --delivery-shards 2 -> verify 10..25 delivered, none of 0..10 redelivered!
    let dir2 = {
        let broker = Broker::spawn_in(dir.clone(), mode, 2, true, true, 1);
        let addr = broker.address;
        let (mut sub, present) = connect_v5_opts(addr, "crash-sub", false, 300, None, None);
        assert!(present, "session lost across SIGKILL in {mode}");

        for expected in 10..25usize {
            let (hdr, body) = read_packet(&mut sub).unwrap();
            assert_eq!(hdr & 0xf0, 0x30);
            let (_, _, _, pid, payload) = decode_v5_publish(hdr, &body);
            let got = usize::from_be_bytes(payload[..8].try_into().unwrap());
            assert_eq!(got, expected, "post-SIGKILL replay mismatch in {mode}");
            send_ack(&mut sub, 0x40, pid.unwrap());
        }
        thread::sleep(Duration::from_millis(80));
        sub.write_all(&[0xe0, 0x00]).unwrap();
        drop(sub);
        thread::sleep(Duration::from_millis(50));

        // Publish 25..35, then SIGKILL again to test cross-mode recovery (--delivery-shards 0)!
        let mut pub_stream = connect_v5(addr, "crash-pub-2");
        for seq in 25..35usize {
            pub_stream
                .write_all(&publish_packet(
                    1,
                    false,
                    Some((seq + 1) as u16),
                    "crash/topic",
                    &seq.to_be_bytes(),
                ))
                .unwrap();
            let (h, _) = read_packet(&mut pub_stream).unwrap();
            assert_eq!(h, 0x40);
        }
        broker.kill_hard()
    };

    // Restart #2: --delivery-shards 0 (cross-mode recovery from sharded store!)
    {
        let broker = Broker::spawn_in(dir2.clone(), mode, 0, true, false, 2);
        let addr = broker.address;
        let (mut sub, present) = connect_v5_opts(addr, "crash-sub", false, 300, None, None);
        assert!(present, "session lost across cross-mode SIGKILL in {mode}");

        for expected in 25..35usize {
            let (hdr, body) = read_packet(&mut sub).unwrap();
            assert_eq!(hdr & 0xf0, 0x30);
            let (_, _, _, pid, payload) = decode_v5_publish(hdr, &body);
            let got = usize::from_be_bytes(payload[..8].try_into().unwrap());
            assert_eq!(
                got, expected,
                "cross-mode post-SIGKILL replay mismatch in {mode}"
            );
            send_ack(&mut sub, 0x40, pid.unwrap());
        }
    }
    let _ = Path::new(&dir);
}

#[test]
fn sharded_sigkill_crash_and_cross_mode_recovery_log_and_durable() {
    run_sigkill_crash_recovery("log");
    run_sigkill_crash_recovery("durable");
}

#[test]
fn sharded_receipt_collection_under_sustained_fanout() {
    // Sustained 1:8 fan-out of 600 QoS 1 publications across 3 delivery shards
    // generates >4,800 shard FinishDelivery operations, forcing multiple
    // per-shard and Coordinator receipt-collection cycles.
    let broker = Broker::start("receipt-wrap", "log", 3);
    let addr = broker.address;
    let total = 600usize;
    let n_subs = 8usize;

    let ready = Arc::new(AtomicUsize::new(0));
    let mut sub_handles = Vec::with_capacity(n_subs);
    for i in 0..n_subs {
        let ready = Arc::clone(&ready);
        sub_handles.push(thread::spawn(move || {
            let mut sub = connect_v5(addr, &format!("rc-sub-{i}"));
            subscribe_v5(&mut sub, 1, "rc/topic", 1);
            ready.fetch_add(1, Ordering::Release);
            let mut expected = 0usize;
            while expected < total {
                let (hdr, body) = read_packet(&mut sub).unwrap();
                assert_eq!(hdr & 0xf0, 0x30);
                let (_qos, _, _, pid, payload) = decode_v5_publish(hdr, &body);
                let got = usize::from_be_bytes(payload[..8].try_into().unwrap());
                assert_eq!(got, expected, "rc-sub-{i} FIFO mismatch");
                send_ack(&mut sub, 0x40, pid.unwrap());
                expected += 1;
            }
            let _ = sub.write_all(&[0xe0, 0x00]);
        }));
    }

    while ready.load(Ordering::Acquire) < n_subs {
        thread::sleep(Duration::from_millis(2));
    }

    let mut pub_stream = connect_v5(addr, "rc-pub");
    let window = 8usize;
    let mut sent = 0usize;
    let mut acked = 0usize;
    while acked < total {
        while sent < total && sent - acked < window {
            let pid = ((sent % 60_000) + 1) as u16;
            pub_stream
                .write_all(&publish_packet(
                    1,
                    false,
                    Some(pid),
                    "rc/topic",
                    &sent.to_be_bytes(),
                ))
                .unwrap();
            sent += 1;
        }
        let (hdr, body) = read_packet(&mut pub_stream).unwrap();
        assert_eq!(hdr, 0x40);
        let reason = if body.len() >= 3 { body[2] } else { 0x00 };
        assert!(
            reason < 0x80,
            "refused PUBACK during sustained fanout: {reason:#x}"
        );
        acked += 1;
    }

    for h in sub_handles {
        h.join().unwrap();
    }
}
