//! End-to-end tests for native `mosquitto.conf` subset loading (`-c` / `--config`)
//! and static checking (`mqttiumd check-config PATH`, ADR 0054).

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

static NEXT: AtomicU64 = AtomicU64::new(0);

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "mqttiumd-conf-e2e-{name}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).expect("temp dir");
    dir
}

#[test]
fn check_config_exits_zero_for_supported_subset_and_refuses_blocking_directives() {
    let dir = temp_dir("check");
    let valid = dir.join("valid.conf");
    std::fs::write(
        &valid,
        "\
listener 0 127.0.0.1
protocol mqtt
allow_anonymous true
persistence true
persistence_location ./state
persistence_file mqttiumd.sqlite
log_dest file ./events.jsonl
log_type notice
max_connections 256
",
    )
    .unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_mqttiumd"))
        .args(["check-config", valid.to_str().unwrap()])
        .output()
        .expect("run check-config");
    assert!(
        out.status.success(),
        "valid config failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let parsed: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json output");
    assert_eq!(parsed["ok"], true);
    assert_eq!(parsed["directives"].as_array().unwrap().len(), 9);

    let bad = dir.join("bad.conf");
    std::fs::write(
        &bad,
        "listener 1883 127.0.0.1\nallow_anonymous true\nacl_file /etc/mosquitto/acl\nunknown_knob yes\n",
    )
    .unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_mqttiumd"))
        .args(["check-config", bad.to_str().unwrap()])
        .output()
        .expect("run check-config bad");
    assert!(!out.status.success());
    let parsed: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json output");
    assert_eq!(parsed["ok"], false);
    assert!(
        parsed["directives"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|d| d["status"] == "blocking")
            .count()
            >= 2
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn broker_boots_and_serves_mqtt_directly_from_mosquitto_conf() {
    let dir = temp_dir("boot");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();
    let conf = dir.join("mosquitto.conf");
    std::fs::write(
        &conf,
        "\
listener 0 127.0.0.1
allow_anonymous true
mqttiumd_storage_mode volatile
autosave_interval 60
persistence_location ./data
persistence_file volatile.sqlite
log_dest file ./events.jsonl
log_type notice
mqttiumd_io_threads 2
",
    )
    .unwrap();

    // Run TWO consecutive startups with the exact same mosquitto.conf and event journal.
    for run_idx in 1..=2 {
        let mut child = Command::new(env!("CARGO_BIN_EXE_mqttiumd"))
            .args(["-c", conf.to_str().unwrap()])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn mqttiumd -c");

        let stdout = child.stdout.take().unwrap();
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        let n = reader.read_line(&mut line).expect("read readiness");
        assert!(n > 0, "run {run_idx} exited before readiness");
        assert!(
            line.contains("storage=volatile") && line.contains("periodic_atomic_snapshot"),
            "expected volatile periodic snapshot disclosure on run {run_idx}: {line}"
        );
        let addr: SocketAddr = line
            .strip_prefix("mqttiumd ready ")
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap()
            .parse()
            .unwrap();

        // Verify real MQTT 5 CONNECT + PINGREQ round-trip on the configured listener.
        let mut stream = TcpStream::connect(addr).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let connect = b"\x10\x13\x00\x04MQTT\x05\x02\x00\x3c\x00\x00\x06cfg-ok";
        stream.write_all(connect).unwrap();
        let mut connack = [0u8; 2];
        stream.read_exact(&mut connack).unwrap();
        assert_eq!(connack[0], 0x20);
        let mut rest = vec![0u8; usize::from(connack[1])];
        stream.read_exact(&mut rest).unwrap();
        assert_eq!(rest[1], 0);
        stream.write_all(&[0xc0, 0x00]).unwrap();
        let mut pingresp = [0u8; 2];
        stream.read_exact(&mut pingresp).unwrap();
        assert_eq!(pingresp, [0xd0, 0x00]);
        drop(stream);

        // Graceful shutdown flushes volatile snapshot to `./data/volatile.sqlite`.
        let pid = nix::unistd::Pid::from_raw(child.id() as i32);
        let _ = nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGINT);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert!(
                    status.success(),
                    "run {run_idx} clean shutdown failed: {status}"
                );
                break;
            }
            assert!(
                Instant::now() < deadline,
                "run {run_idx} shutdown timed out"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    assert!(data.join("volatile.sqlite").is_file());
    assert!(dir.join("events.jsonl").is_file());
    assert!(
        dir.join("events.jsonl.previous").is_file(),
        "second startup with the same journal must rotate run 1 to .previous"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn explicit_listener_without_allow_anonymous_is_refused() {
    let dir = temp_dir("implicit-anon");
    let conf = dir.join("mosquitto.conf");
    std::fs::write(
        &conf,
        "\
listener 0 127.0.0.1
persistence false
log_dest file ./events.jsonl
",
    )
    .unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_mqttiumd"))
        .args(["check-config", conf.to_str().unwrap()])
        .output()
        .expect("check-config");
    assert!(!out.status.success());
    let parsed: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json");
    assert_eq!(parsed["ok"], false);

    let run_out = Command::new(env!("CARGO_BIN_EXE_mqttiumd"))
        .args(["-c", conf.to_str().unwrap()])
        .output()
        .expect("run -c");
    assert!(!run_out.status.success());
    assert!(!dir.join("events.jsonl").exists());

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn persistence_false_ignores_existing_db_and_autosave_interval() {
    let dir = temp_dir("pf-existing");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();
    let db_path = data.join("existing.sqlite");

    // Seed an existing database on disk with a retained message via a durable run.
    let seed_conf = dir.join("seed.conf");
    std::fs::write(
        &seed_conf,
        "\
listener 0 127.0.0.1
allow_anonymous true
persistence true
persistence_location ./data
persistence_file existing.sqlite
log_dest file ./seed-events.jsonl
",
    )
    .unwrap();
    {
        let mut child = Command::new(env!("CARGO_BIN_EXE_mqttiumd"))
            .args(["-c", seed_conf.to_str().unwrap()])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn seed broker");
        let mut reader = BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        assert!(reader.read_line(&mut line).unwrap() > 0);
        let addr: SocketAddr = line
            .strip_prefix("mqttiumd ready ")
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap()
            .parse()
            .unwrap();
        let mut stream = TcpStream::connect(addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        // MQTT 5 CONNECT (client_id="seeder")
        stream
            .write_all(b"\x10\x13\x00\x04MQTT\x05\x02\x00\x3c\x00\x00\x06seeder")
            .unwrap();
        let mut hdr = [0u8; 2];
        stream.read_exact(&mut hdr).unwrap();
        let mut rest = vec![0u8; usize::from(hdr[1])];
        stream.read_exact(&mut rest).unwrap();
        // Retained QoS 1 PUBLISH to topic "disk/only", payload "old"
        // topic len=9 + "disk/only" (9) + packet_id (2) + props len (1) + "old" (3) = 17
        stream
            .write_all(b"\x33\x11\x00\x09disk/only\x00\x01\x00old")
            .unwrap();
        let mut puback = [0u8; 2];
        stream.read_exact(&mut puback).unwrap();
        assert_eq!(puback[0], 0x40);
        let mut pb = vec![0u8; usize::from(puback[1])];
        stream.read_exact(&mut pb).unwrap();
        drop(stream);

        let pid = nix::unistd::Pid::from_raw(child.id() as i32);
        let _ = nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGINT);
        assert!(child.wait().unwrap().success());
    }
    let db_bytes_before = std::fs::read(&db_path).expect("seed db bytes");
    let db_mtime_before = std::fs::metadata(&db_path).unwrap().modified().unwrap();

    // Now start with `persistence false` + `autosave_interval 1` pointing at the same
    // persistence_location/persistence_file. It MUST NOT load the retained message,
    // MUST NOT write snapshots, and MUST leave `existing.sqlite` byte-for-byte untouched.
    let volatile_conf = dir.join("volatile.conf");
    std::fs::write(
        &volatile_conf,
        "\
listener 0 127.0.0.1
allow_anonymous true
persistence false
autosave_interval 1
persistence_location ./data
persistence_file existing.sqlite
log_dest file ./events.jsonl
",
    )
    .unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_mqttiumd"))
        .args(["-c", volatile_conf.to_str().unwrap()])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn persistence-false broker");
    let mut reader = BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    assert!(reader.read_line(&mut line).unwrap() > 0);
    assert!(
        line.contains("durability=none_in_memory_only"),
        "expected pure in-memory durability with persistence false + autosave: {line}"
    );
    let addr: SocketAddr = line
        .strip_prefix("mqttiumd ready ")
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .parse()
        .unwrap();

    // Subscribe to "disk/only" and verify NO retained message was loaded from disk.
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_millis(300)))
        .unwrap();
    stream
        .write_all(b"\x10\x13\x00\x04MQTT\x05\x02\x00\x3c\x00\x00\x06reader")
        .unwrap();
    let mut hdr = [0u8; 2];
    stream.read_exact(&mut hdr).unwrap();
    let mut rest = vec![0u8; usize::from(hdr[1])];
    stream.read_exact(&mut rest).unwrap();
    // SUBSCRIBE pkt_id=1, props=0, filter="disk/only", options=1 -> len = 2+1+2+9+1 = 15
    stream
        .write_all(b"\x82\x0f\x00\x01\x00\x00\x09disk/only\x01")
        .unwrap();
    stream.read_exact(&mut hdr).unwrap();
    assert_eq!(hdr[0], 0x90, "expected SUBACK");
    let mut suback = vec![0u8; usize::from(hdr[1])];
    stream.read_exact(&mut suback).unwrap();
    // Send PINGREQ and verify PINGRESP arrives with no intervening retained PUBLISH.
    stream.write_all(&[0xc0, 0x00]).unwrap();
    let mut pingresp = [0u8; 2];
    stream.read_exact(&mut pingresp).unwrap();
    assert_eq!(
        pingresp,
        [0xd0, 0x00],
        "existing DB retained message must not be resurrected under persistence false"
    );
    drop(stream);

    let pid = nix::unistd::Pid::from_raw(child.id() as i32);
    let _ = nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGINT);
    assert!(child.wait().unwrap().success());

    // Existing DB must be completely untouched and no snapshot events emitted.
    assert_eq!(std::fs::read(&db_path).unwrap(), db_bytes_before);
    assert_eq!(
        std::fs::metadata(&db_path).unwrap().modified().unwrap(),
        db_mtime_before
    );
    let events_text = std::fs::read_to_string(dir.join("events.jsonl")).unwrap();
    assert!(!events_text.contains("volatile_snapshot_saved"));
    assert!(!events_text.contains("volatile_snapshot_failed"));

    let _ = std::fs::remove_dir_all(&dir);
}
