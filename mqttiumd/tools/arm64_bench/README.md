# ARM64 Load & Idle-Memory Benchmark Harness

Reproducible C/Python load generator and idle-memory harness used for
[report 0029](../../docs/reports/0029-lot5-lot6-arm64-performance-and-durability.md).

## Files

- `arm64_bench.c` — Zero-dependency C + OpenSSL (`epoll` + TLS 1.3 mTLS) MQTT 5
  load driver that accounts separately for `scheduled`, `submitted`,
  `acked_ok`, `acked_refused`, `ack_missing`, `received`,
  `delivered_after_drain`, `ack_no_deliv`, `duplicates`, `invalid`,
  `disconnects`, and publish-to-deliver / publish-to-ack latency percentiles
  (`p50`, `p95`, `p99`).
- `run_arm64_bench.py` — Single-group runner comparing `mqttiumd` and Mosquitto,
  recording the exact backing mount/filesystem (`findmnt`), DB/WAL/`.mlog` file
  sizes, broker command line, and Mosquitto config.
- `run_arm64_suite.py` — Full 59-scenario suite across Plain TCP, TLS 1.3 mTLS,
  256-connection concurrency, 64 KiB payloads (real disk backing), and a
  `tmpfs` (`/dev/shm`) backing control.
- `idle_rss_bench.py` — Idle connection footprint benchmark (`VmRSS`,
  `RssAnon`, `Private_Dirty`, `Pss`, thread count, FD count) for Plain TCP
  (0..768 connections) and TLS 1.3 mTLS (0..400 connections).

## Reproduce

From `mqttiumd/`:

```bash
cargo build -p mqttiumd --profile dist --locked
sh preview/build-mosquitto-reference.sh /tmp/load-reference

# Full 59-scenario throughput & latency suite on real disk (e.g. /var/tmp on ext4)
python3 tools/arm64_bench/run_arm64_suite.py \
  --binary target/dist/mqttiumd \
  --mosquitto /tmp/load-reference/mosquitto-2.0.21 \
  --work-dir /var/tmp \
  --out /tmp/arm64_suite_results.json

# Idle memory footprint per connection (Plain & mTLS)
python3 tools/arm64_bench/idle_rss_bench.py \
  --binary target/dist/mqttiumd \
  --mosquitto /tmp/load-reference/mosquitto-2.0.21 \
  --out /tmp/idle_rss_results.json
```

Generated JSON outputs and compiled binaries must stay outside the repository
(e.g. `/tmp/`) and are never committed.
