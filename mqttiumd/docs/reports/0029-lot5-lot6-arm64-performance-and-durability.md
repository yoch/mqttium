# Lot 5 & Lot 6 ARM64 performance, idle memory, and durability review dossier

This report records the complete empirical before/after evaluation and
verification matrix for **Lot 5** (`4afeff66`) and **Lot 6** (`4c4e4506`) on
branch `perf/lot5-runtime-qos1-logstore`, compared against the received PR #648
head `014e04b4` (`claude/optimistic-feynman-fqh947`, untouched) and pinned
reference broker Mosquitto 2.0.21.

Architectural decisions are documented in Proposed records
[0055](../decisions/0055-logstore-append-only-wal.md) (`LogStore`, `--storage-mode log`)
and [0056](../decisions/0056-claimed-publication-and-core-optimizations.md)
(`Recipe::ClaimedPublication`, `ScopedMap`, Redis/FlashMQ-inspired structures).

## Host, toolchain, and storage backing

- **Hardware**: ARM64 4-core Cortex-A76 (256 KiB L1d, 256 KiB L1i, 2 MiB L2, 2 MiB L3).
- **Kernel**: Linux `6.18.60+rpt-rpi-2712` (`aarch64`).
- **Toolchain**: `rustc 1.97.0 (2d8144b78 2026-07-07)`, `cargo 1.97.0`, built with `--profile dist --locked`.
- **Reference broker**: `mosquitto version 2.0.21` (built from pinned source).
- **CPU pinning**: broker pinned to CPUs `0,1,2` (`--io-threads 2`); load generator pinned to CPU `3`.
- **Real disk backing**: `/dev/sda2` (`ext4`) for all `real ext4 backing` scenarios; `/dev/shm` (`tmpfs`) only for the explicit backing-contrast control.
- **Generator**: [tools/arm64_bench/arm64_bench.c](../../tools/arm64_bench/arm64_bench.c) (`sha256: b0ec96a4cc59037e4b9f2eb33c35cd73762b487a5fc6a5ad4136275ca6103dd7`), MQTT 5, 2-second publish window followed by full drain, reporting separated `submitted`, `acked_ok`, `acked_refused`, `delivered_after_drain`, `ack_no_deliv`, `disconnects`, `invalid`, DB/WAL/`.mlog` sizes, and publish-to-deliver `p50 / p99` latencies.

## Reproduce

```sh
cd mqttiumd
cargo build -p mqttiumd --profile dist --locked
sh preview/build-mosquitto-reference.sh /tmp/load-reference
python3 tools/arm64_bench/idle_rss_bench.py \
  --binary target/dist/mqttiumd \
  --mosquitto /tmp/load-reference/mosquitto-2.0.21 \
  --out /tmp/idle_rss_lot6.json
python3 tools/arm64_bench/run_arm64_suite.py \
  --binary target/dist/mqttiumd \
  --mosquitto /tmp/load-reference/mosquitto-2.0.21 \
  --work-dir /var/tmp \
  --out /tmp/arm64_lot6_results.json
```

## Summary of key ARM64 gains (`014e04b4` -> `4afeff66` -> `4c4e4506`)

Across all **59 / 59 scenarios** on `4c4e4506`, **0 integrity errors** occurred (`acked_refused = 0`, `ack_no_deliv = 0`, `disconnects = 0`, `invalid = 0`; every QoS 1 publication submitted was acknowledged OK and delivered after drain).

| Workload | Mode | `014e04b4` (PR #648) | Lot 5 (`4afeff66`) | **Lot 6 (`4c4e4506`)** | Mosquitto 2.0.21 (RAM) |
| - | - | -: | -: | -: | -: |
| Plain 32p QoS 1 @ 20k/s | `mqttiumd-volatile-ram` | 19,999/s (1040.07 / 1300.15 ms) | 20,000/s (0.21 / 2.13 ms) | **20,000/s (0.06 / 0.79 ms)** | 20,000/s (0.02 / 0.12 ms) |
| Plain 32p QoS 1 @ 60k/s | `mqttiumd-volatile-ram` | 21,956/s (1634.39 / 2161.87 ms) | 25,108/s (19.75 / 30.89 ms) | **33,464/s (7.48 / 22.36 ms)** | 60,000/s (0.02 / 0.14 ms) |
| Plain 32p QoS 1 @ 60k/s | `mqttiumd-log-ext4` | — | 16,828/s (458.15 / 638.25 ms) | **20,488/s (9.76 / 47.45 ms)** | *(RAM only)* |
| Plain 32p QoS 1 @ 60k/s | `mqttiumd-durable-ext4` | 3,395/s (2514.11 / 3486.31 ms) | 2,224/s (843.53 / 1088.42 ms) | **3,379/s (79.30 / 94.60 ms)** | *(RAM only)* |
| mTLS 32p QoS 1 @ 20k/s | `mqttiumd-volatile-ram` | 20,000/s (1196.59 / 1416.72 ms) | 20,000/s (0.50 / 3.03 ms) | **20,000/s (0.08 / 1.15 ms)** | 20,000/s (0.03 / 0.04 ms) |
| mTLS 32p QoS 1 @ 20k/s | `mqttiumd-log-ext4` | — | 17,516/s (29.64 / 186.53 ms) | **17,508/s (7.98 / 40.44 ms)** | *(RAM only)* |
| mTLS 32p QoS 1 @ 20k/s | `mqttiumd-durable-ext4` | 2,877/s (2310.70 / 2849.00 ms) | 2,392/s (879.19 / 1191.85 ms) | **3,258/s (80.49 / 100.92 ms)** | *(RAM only)* |
| 256 conns QoS 0 @ 120k/s | `mqttiumd-log-ext4` | — | 120,000/s (6.28 / 10.95 ms) | **119,985/s (1.73 / 3.90 ms)** | 120,000/s (415.88 / 649.70 ms) |
| 256 conns QoS 1 @ 20k/s | `mqttiumd-volatile-ram` | 5,080/s (352.55 / 651.90 ms) | 18,430/s (50.62 / 117.33 ms) | **20,000/s (0.23 / 1.43 ms)** | 20,000/s (0.02 / 0.04 ms) |
| 256 conns QoS 1 @ 20k/s | `mqttiumd-log-ext4` | — | 11,401/s (109.83 / 300.96 ms) | **20,000/s (8.71 / 46.52 ms)** | *(RAM only)* |
| 256 conns QoS 1 @ 20k/s | `mqttiumd-durable-ext4` | 4,044/s (3022.10 / 3975.81 ms) | 2,045/s (2166.90 / 3231.98 ms) | **2,384/s (372.28 / 938.21 ms)** | *(RAM only)* |
| 64 KiB 4p QoS 1 @ 2k/s | `mqttiumd-log-ext4` | — | 570/s (738.00 / 1246.86 ms) | **807/s (29.90 / 91.40 ms)** | *(RAM only)* |
| 64 KiB 4p QoS 1 @ 2k/s | `mqttiumd-durable-ext4` | 420/s (1550.34 / 1888.07 ms) | 449/s (576.18 / 875.50 ms) | **708/s (40.76 / 300.01 ms)** | *(RAM only)* |
| tmpfs 32p QoS 1 @ 60k/s | `mqttiumd-log-tmpfs` | — | 23,824/s (21.74 / 35.15 ms) | **33,022/s (8.22 / 21.70 ms)** | — |
| tmpfs 32p QoS 1 @ 60k/s | `mqttiumd-durable-tmpfs` | 8,520/s (2492.85 / 3499.20 ms) | 6,804/s (922.83 / 1165.03 ms) | **7,539/s (33.92 / 39.39 ms)** | — |

## Idle memory per connection (ARM64)

| Profile | Conns | `014e04b4` RSS / Dirty | Lot 5 (`4afeff66`) RSS / Dirty | **Lot 6 (`4c4e4506`) RSS / Dirty** | Mosquitto 2.0.21 RSS / Dirty |
| - | -: | -: | -: | -: | -: |
| Plain TCP | 128 | 74,112 / 73,088 B | 14,464 / 14,464 B | **12,416 / 10,368 B** | 768 / 768 B |
| Plain TCP | 256 | 73,152 / 72,640 B | 13,376 / 13,376 B | **11,456 / 10,432 B** | 768 / 768 B |
| Plain TCP | 400 | 71,148 / 70,820 B | 11,223 / 11,223 B | **10,650 / 9,994 B** | 778 / 778 B |
| Plain TCP | 512 | — | 10,720 / 10,720 B | **9,952 / 9,440 B** | 768 / 768 B |
| Plain TCP | 768 | — | 9,408 / 9,408 B | **8,597 / 8,256 B** | 768 / 768 B |
| TLS 1.3 mTLS | 128 | 55,040 / 52,992 B | 28,416 / 28,416 B | **27,520 / 24,960 B** | 40,704 / 39,168 B |
| TLS 1.3 mTLS | 256 | 50,176 / 49,152 B | 22,848 / 10,560 B | **21,760 / 20,480 B** | 39,936 / 38,912 B |
| TLS 1.3 mTLS | 400 | — | 20,726 / 12,861 B | **19,702 / 18,883 B** | 39,526 / 38,871 B |

All `mqttiumd` runs held exactly `1.00 FD/conn` and 6 threads total (1 main/owner, 2 epoll I/O workers, 1 acceptor, 1 store writer, 1 signal thread).

## Complete 59-scenario accounting (`4c4e4506`)

Separated accounting for every scenario in `arm64_lot6_results.json`. In every row `ack_no_deliv = 0`, `disconnects = 0`, `invalid = 0`, and `acked_refused = 0`.

### Plain TCP 256B (real ext4 backing)

| Target | Pairs | QoS | Offered/s | Submitted | Ack OK | Refused | Delivered | Deliv/s | p50 / p99 (ms) | FS | WAL / `.mlog` |
| - | -: | -: | -: | -: | -: | -: | -: | -: | -: | - | -: |
| `mqttiumd-volatile-ram` | 4 | 0 | 60,000 | 120,000 | 0 | 0 | 120,000 | 60,000 | 0.03 / 1.67 | `ext4` | 0 B |
| `mqttiumd-volatile-snap` | 4 | 0 | 60,000 | 120,000 | 0 | 0 | 120,000 | 60,000 | 0.03 / 1.77 | `ext4` | 0 B |
| `mqttiumd-log-ext4` | 4 | 0 | 60,000 | 120,000 | 0 | 0 | 120,000 | 60,000 | 0.03 / 2.02 | `ext4` | 262,168 B |
| `mqttiumd-durable-ext4` | 4 | 0 | 60,000 | 120,000 | 0 | 0 | 120,000 | 60,000 | 0.03 / 1.39 | `ext4` | 304,912 B |
| `mosquitto-2.0.21-ram` | 4 | 0 | 60,000 | 120,000 | 0 | 0 | 120,000 | 60,000 | 0.56 / 1.16 | `ext4` | 0 B |
| `mqttiumd-volatile-ram` | 32 | 0 | 120,000 | 240,000 | 0 | 0 | 240,000 | 120,000 | 0.04 / 3.17 | `ext4` | 0 B |
| `mqttiumd-volatile-snap` | 32 | 0 | 120,000 | 240,000 | 0 | 0 | 240,000 | 120,000 | 0.04 / 5.41 | `ext4` | 0 B |
| `mqttiumd-log-ext4` | 32 | 0 | 120,000 | 240,000 | 0 | 0 | 240,000 | 120,000 | 0.04 / 1.33 | `ext4` | 262,168 B |
| `mqttiumd-durable-ext4` | 32 | 0 | 120,000 | 240,000 | 0 | 0 | 240,000 | 120,000 | 0.04 / 2.84 | `ext4` | 1,755,152 B |
| `mosquitto-2.0.21-ram` | 32 | 0 | 120,000 | 240,000 | 0 | 0 | 240,000 | 120,000 | 2.19 / 4.57 | `ext4` | 0 B |
| `mqttiumd-volatile-ram` | 4 | 1 | 3,000 | 6,000 | 6,000 | 0 | 6,000 | 3,000 | 0.05 / 0.10 | `ext4` | 0 B |
| `mqttiumd-volatile-snap` | 4 | 1 | 3,000 | 6,000 | 6,000 | 0 | 6,000 | 3,000 | 0.04 / 0.10 | `ext4` | 0 B |
| `mqttiumd-log-ext4` | 4 | 1 | 3,000 | 5,950 | 5,950 | 0 | 5,950 | 2,975 | 3.09 / 15.89 | `ext4` | 8,388,992 B |
| `mqttiumd-durable-ext4` | 4 | 1 | 3,000 | 5,602 | 5,602 | 0 | 5,602 | 2,801 | 6.28 / 18.90 | `ext4` | 4,190,072 B |
| `mosquitto-2.0.21-ram` | 4 | 1 | 3,000 | 6,000 | 6,000 | 0 | 6,000 | 3,000 | 0.02 / 0.15 | `ext4` | 0 B |
| `mqttiumd-volatile-ram` | 32 | 1 | 3,000 | 6,000 | 6,000 | 0 | 6,000 | 3,000 | 0.05 / 0.14 | `ext4` | 0 B |
| `mqttiumd-volatile-snap` | 32 | 1 | 3,000 | 6,000 | 6,000 | 0 | 6,000 | 3,000 | 0.05 / 0.14 | `ext4` | 0 B |
| `mqttiumd-log-ext4` | 32 | 1 | 3,000 | 6,000 | 6,000 | 0 | 6,000 | 3,000 | 2.40 / 17.93 | `ext4` | 4,194,688 B |
| `mqttiumd-durable-ext4` | 32 | 1 | 3,000 | 5,702 | 5,702 | 0 | 5,702 | 2,851 | 76.66 / 659.26 | `ext4` | 4,429,032 B |
| `mosquitto-2.0.21-ram` | 32 | 1 | 3,000 | 6,000 | 6,000 | 0 | 6,000 | 3,000 | 0.02 / 0.08 | `ext4` | 0 B |
| `mqttiumd-volatile-ram` | 32 | 1 | 20,000 | 40,000 | 40,000 | 0 | 40,000 | 20,000 | 0.06 / 0.79 | `ext4` | 0 B |
| `mqttiumd-volatile-snap` | 32 | 1 | 20,000 | 40,000 | 40,000 | 0 | 40,000 | 20,000 | 0.06 / 0.81 | `ext4` | 0 B |
| `mqttiumd-log-ext4` | 32 | 1 | 20,000 | 34,371 | 34,371 | 0 | 34,371 | 17,186 | 8.22 / 40.38 | `ext4` | 25,166,208 B |
| `mqttiumd-durable-ext4` | 32 | 1 | 20,000 | 6,728 | 6,728 | 0 | 6,728 | 3,364 | 79.11 / 94.07 | `ext4` | 4,354,872 B |
| `mosquitto-2.0.21-ram` | 32 | 1 | 20,000 | 40,000 | 40,000 | 0 | 40,000 | 20,000 | 0.02 / 0.12 | `ext4` | 0 B |
| `mqttiumd-volatile-ram` | 32 | 1 | 60,000 | 66,929 | 66,929 | 0 | 66,929 | 33,464 | 7.48 / 22.36 | `ext4` | 0 B |
| `mqttiumd-volatile-snap` | 32 | 1 | 60,000 | 64,400 | 64,400 | 0 | 64,400 | 32,200 | 7.14 / 43.39 | `ext4` | 0 B |
| `mqttiumd-log-ext4` | 32 | 1 | 60,000 | 40,976 | 40,976 | 0 | 40,976 | 20,488 | 9.76 / 47.45 | `ext4` | 29,360,512 B |
| `mqttiumd-durable-ext4` | 32 | 1 | 60,000 | 6,758 | 6,758 | 0 | 6,758 | 3,379 | 79.30 / 94.60 | `ext4` | 4,354,872 B |
| `mosquitto-2.0.21-ram` | 32 | 1 | 60,000 | 120,000 | 120,000 | 0 | 120,000 | 60,000 | 0.02 / 0.14 | `ext4` | 0 B |
### TLS 1.3 mTLS 256B (real ext4 backing)

| Target | Pairs | QoS | Offered/s | Submitted | Ack OK | Refused | Delivered | Deliv/s | p50 / p99 (ms) | FS | WAL / `.mlog` |
| - | -: | -: | -: | -: | -: | -: | -: | -: | -: | - | -: |
| `mqttiumd-volatile-ram` | 32 | 0 | 60,000 | 120,000 | 0 | 0 | 120,000 | 60,000 | 0.04 / 1.56 | `ext4` | 0 B |
| `mqttiumd-volatile-snap` | 32 | 0 | 60,000 | 120,000 | 0 | 0 | 120,000 | 60,000 | 0.04 / 1.24 | `ext4` | 0 B |
| `mqttiumd-log-ext4` | 32 | 0 | 60,000 | 120,000 | 0 | 0 | 120,000 | 60,000 | 0.04 / 0.63 | `ext4` | 262,168 B |
| `mqttiumd-durable-ext4` | 32 | 0 | 60,000 | 120,000 | 0 | 0 | 120,000 | 60,000 | 0.04 / 3.92 | `ext4` | 2,022,952 B |
| `mosquitto-2.0.21-ram` | 32 | 0 | 60,000 | 120,000 | 0 | 0 | 120,000 | 60,000 | 0.04 / 0.51 | `ext4` | 0 B |
| `mqttiumd-volatile-ram` | 32 | 1 | 20,000 | 40,000 | 40,000 | 0 | 40,000 | 20,000 | 0.08 / 1.15 | `ext4` | 0 B |
| `mqttiumd-volatile-snap` | 32 | 1 | 20,000 | 40,000 | 40,000 | 0 | 40,000 | 20,000 | 0.08 / 1.20 | `ext4` | 0 B |
| `mqttiumd-log-ext4` | 32 | 1 | 20,000 | 35,015 | 35,015 | 0 | 35,015 | 17,508 | 7.98 / 40.44 | `ext4` | 25,166,208 B |
| `mqttiumd-durable-ext4` | 32 | 1 | 20,000 | 6,517 | 6,517 | 0 | 6,517 | 3,258 | 80.49 / 100.92 | `ext4` | 4,379,592 B |
| `mosquitto-2.0.21-ram` | 32 | 1 | 20,000 | 40,000 | 40,000 | 0 | 40,000 | 20,000 | 0.03 / 0.04 | `ext4` | 0 B |
### High Concurrency 256 conns Plain (real ext4 backing)

| Target | Pairs | QoS | Offered/s | Submitted | Ack OK | Refused | Delivered | Deliv/s | p50 / p99 (ms) | FS | WAL / `.mlog` |
| - | -: | -: | -: | -: | -: | -: | -: | -: | -: | - | -: |
| `mqttiumd-volatile-ram` | 128 | 0 | 120,000 | 239,989 | 0 | 0 | 239,989 | 119,994 | 2.17 / 5.97 | `ext4` | 0 B |
| `mqttiumd-volatile-snap` | 128 | 0 | 120,000 | 239,868 | 0 | 0 | 239,868 | 119,934 | 2.10 / 4.77 | `ext4` | 0 B |
| `mqttiumd-log-ext4` | 128 | 0 | 120,000 | 239,970 | 0 | 0 | 239,970 | 119,985 | 1.73 / 3.90 | `ext4` | 262,168 B |
| `mqttiumd-durable-ext4` | 128 | 0 | 120,000 | 240,000 | 0 | 0 | 240,000 | 120,000 | 2.18 / 15.95 | `ext4` | 4,120,032 B |
| `mosquitto-2.0.21-ram` | 128 | 0 | 120,000 | 240,000 | 0 | 0 | 240,000 | 120,000 | 415.88 / 649.70 | `ext4` | 0 B |
| `mqttiumd-volatile-ram` | 128 | 1 | 20,000 | 40,000 | 40,000 | 0 | 40,000 | 20,000 | 0.23 / 1.43 | `ext4` | 0 B |
| `mqttiumd-volatile-snap` | 128 | 1 | 20,000 | 40,000 | 40,000 | 0 | 40,000 | 20,000 | 0.21 / 1.23 | `ext4` | 0 B |
| `mqttiumd-log-ext4` | 128 | 1 | 20,000 | 40,000 | 40,000 | 0 | 40,000 | 20,000 | 8.71 / 46.52 | `ext4` | 29,360,512 B |
| `mqttiumd-durable-ext4` | 128 | 1 | 20,000 | 4,767 | 4,767 | 0 | 4,767 | 2,384 | 372.28 / 938.21 | `ext4` | 4,416,672 B |
| `mosquitto-2.0.21-ram` | 128 | 1 | 20,000 | 40,000 | 40,000 | 0 | 40,000 | 20,000 | 0.02 / 0.04 | `ext4` | 0 B |
### Large Payload 64 KiB Plain (real ext4 backing)

| Target | Pairs | QoS | Offered/s | Submitted | Ack OK | Refused | Delivered | Deliv/s | p50 / p99 (ms) | FS | WAL / `.mlog` |
| - | -: | -: | -: | -: | -: | -: | -: | -: | -: | - | -: |
| `mqttiumd-volatile-ram` | 4 | 1 | 2,000 | 4,000 | 4,000 | 0 | 4,000 | 2,000 | 0.11 / 0.30 | `ext4` | 0 B |
| `mqttiumd-volatile-snap` | 4 | 1 | 2,000 | 4,000 | 4,000 | 0 | 4,000 | 2,000 | 0.12 / 0.30 | `ext4` | 0 B |
| `mqttiumd-log-ext4` | 4 | 1 | 2,000 | 1,614 | 1,614 | 0 | 1,614 | 807 | 29.90 / 91.40 | `ext4` | 5,273,664 B |
| `mqttiumd-durable-ext4` | 4 | 1 | 2,000 | 1,416 | 1,416 | 0 | 1,416 | 708 | 40.76 / 300.01 | `ext4` | 5,677,392 B |
| `mosquitto-2.0.21-ram` | 4 | 1 | 2,000 | 4,000 | 4,000 | 0 | 4,000 | 2,000 | 0.06 / 0.13 | `ext4` | 0 B |
### Durable QoS 1 Control on tmpfs (/dev/shm) for backing contrast

| Target | Pairs | QoS | Offered/s | Submitted | Ack OK | Refused | Delivered | Deliv/s | p50 / p99 (ms) | FS | WAL / `.mlog` |
| - | -: | -: | -: | -: | -: | -: | -: | -: | -: | - | -: |
| `mqttiumd-log-tmpfs` | 32 | 1 | 20,000 | 40,000 | 40,000 | 0 | 40,000 | 20,000 | 0.06 / 0.94 | `tmpfs` | 29,360,512 B |
| `mqttiumd-durable-tmpfs` | 32 | 1 | 20,000 | 14,860 | 14,860 | 0 | 14,860 | 7,430 | 33.51 / 854.51 | `tmpfs` | 4,387,832 B |
| `mqttiumd-log-tmpfs` | 32 | 1 | 60,000 | 66,044 | 66,044 | 0 | 66,044 | 33,022 | 8.22 / 21.70 | `tmpfs` | 16,777,600 B |
| `mqttiumd-durable-tmpfs` | 32 | 1 | 60,000 | 15,078 | 15,078 | 0 | 15,078 | 7,539 | 33.92 / 39.39 | `tmpfs` | 4,375,472 B |

## Verification matrix (`4c4e4506`)

| Check | Command | Result |
| - | - | - |
| Formatting | `cargo fmt --all -- --check` | PASS |
| Pure-crate boundary (`no_std`, no `unsafe`, closed macros/attrs) | `cargo test -p mqttiumd-lab --test boundary --locked` | PASS (17/17) |
| Full workspace tests (unit, integration, bench, rebuild, replay, SQLite, volatile/log conformance, `ClaimedPublication` crash recovery) | `cargo test --workspace --locked` | PASS (0 failures) |
| Product tests with test-hooks | `cargo test -p mqttiumd --features test-hooks --locked` | PASS (0 failures) |
| Transactional kill-points suite | `cargo test -p mqttiumd-store-sqlite --features mqttiumd-lab/kill-points --locked` | PASS (0 failures) |
| Clippy (workspace, `test-hooks`, `kill-points`) | `cargo clippy ... --all-targets --locked -- -D warnings` (3 passes) | PASS (0 warnings) |
| Dist release build | `cargo build --profile dist -p mqttiumd --locked` | PASS |
| Contract validation & digest | `cargo run -q -p mqttiumd-lab -- validate --contracts contracts/v1 --scenario tests/scenarios/p1/P1-02.json` | PASS (`sha256:305db0c49a43b1d269aeeca59aaf203c99521e19360f5dd93fdc7220fe37c8`, 17 docs) |
| Derived corpus & schemas | `python3 tools/build_corpus.py --check && python3 tools/check_schemas.py` | PASS (624 files fresh; 648 docs agreed, 0 disagreements) |
| Oracle traces & unit tests | `python3 tests/oracle/build_traces.py --check && python3 -m unittest discover -t tests/oracle -s tests/oracle/tests` | PASS (405 traces fresh; 274/274 tests OK) |
| Conformance matrix | `python3 conformance/check.py --check` | PASS (MQTT 5.0: 0 open; MQTT 3.1.1: 0 open) |
| Formal TLA+ models | `python3 models/check_models.py` | PASS (13/13 models consistent) |
| Differential compat vs Mosquitto 2.0.21 (`durable`) | `python3 tests/compat/run.py --broker target/dist/mqttiumd --reference .../mosquitto-2.0.21` | PASS (126/126, 0 defect, 21/21 expectations) |
| Differential compat vs Mosquitto 2.0.21 (`log`) | `python3 tests/compat/run.py --broker <log-wrapper> --reference .../mosquitto-2.0.21` | PASS (126/126, 0 defect, 21/21 expectations) |
| Frozen files unchanged | `git diff --stat -- Cargo.lock contracts/v1/product-profile.json docs/spec/fr proofs credits.rs capacity.rs ids.rs` | PASS (0 bytes changed) |

## What this report does not show

- **Single-host contention**: the broker (3 CPUs) and load generator (1 CPU) share one Raspberry Pi 5 host over loopback TCP.
- **Hardware power-cut testing**: `LogStore` tests verify `fdatasync`, torn-tail truncation, stale-WAL discard, and fail-closed mid-file corruption in software, not physical power removal on storage controllers with volatile write caches.
- **Kani pilot proof re-execution**: `proofs/credits/`, `credits.rs`, `capacity.rs`, and `ids.rs` were not modified and keep their verified `BODY.sha256` digests unchanged; Kani was not re-invoked on ARM64.
- **Mosquitto durability contrast**: Mosquitto 2.0.21 is run with `persistence false` (in-memory only); it does not perform per-commit `fdatasync`, so durable `log` and `durable` modes carry strictly stronger crash guarantees than the reference column.
