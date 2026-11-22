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
persistence false
autosave_interval 60
persistence_location ./data
persistence_file volatile.sqlite
log_dest file ./events.jsonl
log_type notice
mqttiumd_io_threads 2
",
    )
    .unwrap();

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
    assert!(n > 0, "exited before readiness");
    assert!(
        line.contains("storage=volatile"),
        "expected volatile mode disclosure: {line}"
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
            assert!(status.success(), "clean shutdown failed: {status}");
            break;
        }
        assert!(Instant::now() < deadline, "shutdown timed out");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(data.join("volatile.sqlite").is_file());
    assert!(dir.join("events.jsonl").is_file());

    let _ = std::fs::remove_dir_all(&dir);
}
