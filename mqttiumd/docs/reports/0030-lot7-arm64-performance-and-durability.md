# Lot 7 ARM64 performance, idle memory, and durability evaluation

This report records the empirical before/after evaluation and full verification
matrix for **Lot 7** on branch `perf/lot7-crushing-mosquitto`, building on
Lot 5 (`4afeff66`) and Lot 6 (`4c4e4506`, report
[0029](0029-lot5-lot6-arm64-performance-and-durability.md)) and compared against
the received PR #648 head `014e04b4` (`claude/optimistic-feynman-fqh947`,
untouched) and pinned reference broker Mosquitto 2.0.21.

Architectural decisions for Lot 7 are documented in Proposed record
[0057](../decisions/0057-flatmap-direct-volatile-and-zero-alloc-framing.md)
(deterministic `FlatMap` / `ScopedFlatMap`, O(1) delivery indices and
swap-remove reindexing, stack-buffered `FrozenRecipe::freeze`, inline 32-byte
`FrameBytes` for outgoing and incoming frames, direct synchronous
`VolatileStore`, active-peer `ReadyQueue`, O(1) `deferred_by_conn`, and
write-report elision).

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
  --out /tmp/idle_rss_lot7.json
python3 tools/arm64_bench/run_arm64_suite.py \
  --binary target/dist/mqttiumd \
  --mosquitto /tmp/load-reference/mosquitto-2.0.21 \
  --work-dir /var/tmp \
  --out /tmp/arm64_lot7_results.json
```

## Summary of key ARM64 gains (`014e04b4` -> Lot 5 -> Lot 6 -> **Lot 7**)

Across all **59 / 59 scenarios** in Lot 7, **0 integrity errors** occurred (`acked_refused = 0`, `ack_no_deliv = 0`, `disconnects = 0`, `invalid = 0`; every QoS 1 publication submitted was acknowledged OK and delivered after drain).

| Workload | Mode | `014e04b4` (PR #648) | Lot 5 (`4afeff66`) | Lot 6 (`4c4e4506`) | **Lot 7** | Mosquitto 2.0.21 (RAM) |
| - | - | -: | -: | -: | -: | -: |
| Plain 4p QoS 0 @ 60k/s | `mqttiumd-volatile-ram` | 60,000/s (0.04 / 1.88 ms) | 60,000/s (0.04 / 1.41 ms) | 60,000/s (0.03 / 1.67 ms) | **60,000/s (0.02 / 0.46 ms)** | 60,000/s (0.56 / 1.15 ms) |
| Plain 32p QoS 0 @ 120k/s | `mqttiumd-volatile-ram` | 120,000/s (15.10 / 28.40 ms) | 120,000/s (0.06 / 1.77 ms) | 120,000/s (0.04 / 3.17 ms) | **120,000/s (0.03 / 0.71 ms)** | 120,000/s (2.18 / 4.57 ms) |
| Plain 32p QoS 1 @ 20k/s | `mqttiumd-volatile-ram` | 19,999/s (1040.07 / 1300.15 ms) | 20,000/s (0.21 / 2.13 ms) | 20,000/s (0.06 / 0.79 ms) | **20,000/s (0.04 / 0.20 ms)** | 20,000/s (0.02 / 0.20 ms) |
| Plain 32p QoS 1 @ 60k/s | `mqttiumd-volatile-ram` | 21,956/s (1634.39 / 2161.87 ms) | 25,108/s (19.75 / 30.89 ms) | 33,464/s (7.48 / 22.36 ms) | **54,446/s (3.19 / 5.37 ms)** | 60,000/s (0.02 / 0.15 ms) |
| Plain 32p QoS 1 @ 60k/s | `mqttiumd-volatile-snap` | — | 23,604/s (22.31 / 62.10 ms) | 32,200/s (7.14 / 43.39 ms) | **55,871/s (3.27 / 5.19 ms)** | 60,000/s (0.02 / 0.15 ms) |
| Plain 32p QoS 1 @ 60k/s | `mqttiumd-log-ext4` | — | 16,828/s (458.15 / 638.25 ms) | 20,488/s (9.76 / 47.45 ms) | **22,950/s (8.53 / 44.95 ms)** | *(RAM only)* |
| Plain 32p QoS 1 @ 60k/s | `mqttiumd-durable-ext4` | 3,395/s (2514.11 / 3486.31 ms) | 2,224/s (843.53 / 1088.42 ms) | 3,379/s (79.30 / 94.60 ms) | **3,368/s (77.57 / 101.06 ms)** | *(RAM only)* |
| mTLS 32p QoS 0 @ 60k/s | `mqttiumd-volatile-ram` | 60,000/s (0.08 / 2.20 ms) | 60,000/s (0.05 / 1.44 ms) | 60,000/s (0.04 / 1.56 ms) | **60,000/s (0.04 / 0.66 ms)** | 60,000/s (0.03 / 0.54 ms) |
| mTLS 32p QoS 1 @ 20k/s | `mqttiumd-volatile-ram` | 20,000/s (1196.59 / 1416.72 ms) | 20,000/s (0.50 / 3.03 ms) | 20,000/s (0.08 / 1.15 ms) | **20,000/s (0.05 / 0.32 ms)** | 20,000/s (0.03 / 0.05 ms) |
| mTLS 32p QoS 1 @ 20k/s | `mqttiumd-log-ext4` | — | 17,516/s (29.64 / 186.53 ms) | 17,508/s (7.98 / 40.44 ms) | **18,494/s (8.64 / 76.15 ms)** | *(RAM only)* |
| 256 conns QoS 0 @ 120k/s | `mqttiumd-volatile-ram` | 119,880/s (25.40 / 48.10 ms) | 120,000/s (5.84 / 9.95 ms) | 119,994/s (2.17 / 5.97 ms) | **118,272/s (0.95 / 3.80 ms)** | 120,000/s (513.54 / 732.54 ms) |
| 256 conns QoS 0 @ 120k/s | `mqttiumd-log-ext4` | — | 120,000/s (6.28 / 10.95 ms) | 119,985/s (1.73 / 3.90 ms) | **119,998/s (0.17 / 8.23 ms)** | 120,000/s (513.54 / 732.54 ms) |
| 256 conns QoS 1 @ 20k/s | `mqttiumd-volatile-ram` | 5,080/s (352.55 / 651.90 ms) | 18,430/s (50.62 / 117.33 ms) | 20,000/s (0.23 / 1.43 ms) | **20,000/s (0.04 / 0.53 ms)** | 20,000/s (0.02 / 0.04 ms) |
| 256 conns QoS 1 @ 20k/s | `mqttiumd-volatile-snap` | — | 18,120/s (52.10 / 120.40 ms) | 20,000/s (0.21 / 1.23 ms) | **20,000/s (0.04 / 0.51 ms)** | 20,000/s (0.02 / 0.04 ms) |
| 256 conns QoS 1 @ 20k/s | `mqttiumd-log-ext4` | — | 11,401/s (109.83 / 300.96 ms) | 20,000/s (8.71 / 46.52 ms) | **19,802/s (10.46 / 76.27 ms)** | *(RAM only)* |
| 64 KiB 4p QoS 1 @ 2k/s | `mqttiumd-volatile-ram` | 2,000/s (420.10 / 510.20 ms) | 2,000/s (0.16 / 0.44 ms) | 2,000/s (0.11 / 0.30 ms) | **2,000/s (0.11 / 0.26 ms)** | 2,000/s (0.06 / 0.13 ms) |
| 64 KiB 4p QoS 1 @ 2k/s | `mqttiumd-log-ext4` | — | 570/s (738.00 / 1246.86 ms) | 807/s (29.90 / 91.40 ms) | **887/s (316.33 / 671.46 ms)** | *(RAM only)* |
| tmpfs 32p QoS 1 @ 20k/s | `mqttiumd-log-tmpfs` | — | 20,000/s (0.25 / 2.40 ms) | 20,000/s (0.06 / 0.94 ms) | **20,000/s (0.05 / 0.68 ms)** | — |
| tmpfs 32p QoS 1 @ 60k/s | `mqttiumd-log-tmpfs` | — | 23,824/s (21.74 / 35.15 ms) | 33,022/s (8.22 / 21.70 ms) | **53,228/s (3.58 / 6.08 ms)** | — |
| tmpfs 32p QoS 1 @ 60k/s | `mqttiumd-durable-tmpfs` | 8,520/s (2492.85 / 3499.20 ms) | 6,804/s (922.83 / 1165.03 ms) | 7,539/s (33.92 / 39.39 ms) | **7,752/s (32.80 / 44.25 ms)** | — |

## Idle memory per connection (ARM64)

With `DirectWriter` in `--storage-mode volatile`, `mqttiumd` runs with **5 threads** total (1 main/owner + store, 2 epoll I/O workers, 1 acceptor, 1 signal thread) instead of 6, and `1.00 FD/conn`.

| Profile | Conns | `014e04b4` RSS / Dirty | Lot 5 (`4afeff66`) RSS / Dirty | Lot 6 (`4c4e4506`) RSS / Dirty | **Lot 7 RSS / Dirty** | Mosquitto 2.0.21 RSS / Dirty |
| - | -: | -: | -: | -: | -: | -: |
| Plain TCP | 128 | 74,112 / 73,088 B | 14,464 / 14,464 B | 12,416 / 10,368 B | **12,800 / 11,264 B** | 768 / 768 B |
| Plain TCP | 256 | 73,152 / 72,640 B | 13,376 / 13,376 B | 11,456 / 10,432 B | **12,160 / 11,392 B** | 768 / 768 B |
| Plain TCP | 400 | 71,148 / 70,820 B | 11,223 / 11,223 B | 10,650 / 9,994 B | **11,182 / 10,691 B** | 778 / 778 B |
| Plain TCP | 512 | — | 10,720 / 10,720 B | 9,952 / 9,440 B | **10,464 / 10,080 B** | 768 / 768 B |
| Plain TCP | 768 | — | 9,408 / 9,408 B | 8,597 / 8,256 B | **9,728 / 9,472 B** | 768 / 768 B |
| TLS 1.3 mTLS | 128 | 55,040 / 52,992 B | 28,416 / 28,416 B | 27,520 / 24,960 B | **23,040 / 20,992 B** | 40,576 / 39,040 B |
| TLS 1.3 mTLS | 256 | 50,176 / 49,152 B | 22,848 / 10,560 B | 21,760 / 20,480 B | **22,336 / 21,312 B** | 39,744 / 38,848 B |
| TLS 1.3 mTLS | 400 | — | 20,726 / 12,861 B | 19,702 / 18,883 B | **20,931 / 20,275 B** | 39,404 / 38,830 B |

## Complete 59-scenario accounting (Lot 7)

Separated accounting for every scenario in `arm64_lot7_results.json`. In every row `ack_no_deliv = 0`, `disconnects = 0`, `invalid = 0`, and `acked_refused = 0`.

### Plain TCP 256B (real ext4 backing)

| Target | Pairs | QoS | Offered/s | Submitted | Ack OK | Refused | Delivered | Deliv/s | p50 / p99 (ms) | FS | WAL / `.mlog` |
| - | -: | -: | -: | -: | -: | -: | -: | -: | -: | - | -: |
| `mqttiumd-volatile-ram` | 4 | 0 | 60,000 | 120,000 | 0 | 0 | 120,000 | 60,000 | 0.02 / 0.46 | `ext4` | 0 B |
| `mqttiumd-volatile-snap` | 4 | 0 | 60,000 | 120,000 | 0 | 0 | 120,000 | 60,000 | 0.02 / 0.70 | `ext4` | 0 B |
| `mqttiumd-log-ext4` | 4 | 0 | 60,000 | 120,000 | 0 | 0 | 120,000 | 60,000 | 0.02 / 0.31 | `ext4` | 262,168 B |
| `mqttiumd-durable-ext4` | 4 | 0 | 60,000 | 120,000 | 0 | 0 | 120,000 | 60,000 | 0.02 / 0.19 | `ext4` | 304,912 B |
| `mosquitto-2.0.21-ram` | 4 | 0 | 60,000 | 120,000 | 0 | 0 | 120,000 | 60,000 | 0.56 / 1.15 | `ext4` | 0 B |
| `mqttiumd-volatile-ram` | 32 | 0 | 120,000 | 240,000 | 0 | 0 | 240,000 | 120,000 | 0.03 / 0.71 | `ext4` | 0 B |
| `mqttiumd-volatile-snap` | 32 | 0 | 120,000 | 240,000 | 0 | 0 | 240,000 | 120,000 | 0.03 / 0.70 | `ext4` | 0 B |
| `mqttiumd-log-ext4` | 32 | 0 | 120,000 | 240,000 | 0 | 0 | 240,000 | 120,000 | 0.03 / 0.72 | `ext4` | 262,168 B |
| `mqttiumd-durable-ext4` | 32 | 0 | 120,000 | 240,000 | 0 | 0 | 240,000 | 120,000 | 0.03 / 3.54 | `ext4` | 1,734,552 B |
| `mosquitto-2.0.21-ram` | 32 | 0 | 120,000 | 240,000 | 0 | 0 | 240,000 | 120,000 | 2.18 / 4.57 | `ext4` | 0 B |
| `mqttiumd-volatile-ram` | 4 | 1 | 3,000 | 6,000 | 6,000 | 0 | 6,000 | 3,000 | 0.03 / 0.10 | `ext4` | 0 B |
| `mqttiumd-volatile-snap` | 4 | 1 | 3,000 | 6,000 | 6,000 | 0 | 6,000 | 3,000 | 0.04 / 0.08 | `ext4` | 0 B |
| `mqttiumd-log-ext4` | 4 | 1 | 3,000 | 5,805 | 5,805 | 0 | 5,805 | 2,902 | 4.87 / 31.92 | `ext4` | 8,388,992 B |
| `mqttiumd-durable-ext4` | 4 | 1 | 3,000 | 5,635 | 5,635 | 0 | 5,635 | 2,818 | 6.24 / 18.35 | `ext4` | 4,194,192 B |
| `mosquitto-2.0.21-ram` | 4 | 1 | 3,000 | 6,000 | 6,000 | 0 | 6,000 | 3,000 | 0.02 / 2.69 | `ext4` | 0 B |
| `mqttiumd-volatile-ram` | 32 | 1 | 3,000 | 6,000 | 6,000 | 0 | 6,000 | 3,000 | 0.04 / 0.10 | `ext4` | 0 B |
| `mqttiumd-volatile-snap` | 32 | 1 | 3,000 | 6,000 | 6,000 | 0 | 6,000 | 3,000 | 0.04 / 0.10 | `ext4` | 0 B |
| `mqttiumd-log-ext4` | 32 | 1 | 3,000 | 6,000 | 6,000 | 0 | 6,000 | 3,000 | 1.49 / 17.07 | `ext4` | 4,194,688 B |
| `mqttiumd-durable-ext4` | 32 | 1 | 3,000 | 5,599 | 5,599 | 0 | 5,599 | 2,800 | 140.89 / 775.62 | `ext4` | 4,474,352 B |
| `mosquitto-2.0.21-ram` | 32 | 1 | 3,000 | 6,000 | 6,000 | 0 | 6,000 | 3,000 | 0.02 / 0.36 | `ext4` | 0 B |
| `mqttiumd-volatile-ram` | 32 | 1 | 20,000 | 40,000 | 40,000 | 0 | 40,000 | 20,000 | 0.04 / 0.20 | `ext4` | 0 B |
| `mqttiumd-volatile-snap` | 32 | 1 | 20,000 | 40,000 | 40,000 | 0 | 40,000 | 20,000 | 0.04 / 0.23 | `ext4` | 0 B |
| `mqttiumd-log-ext4` | 32 | 1 | 20,000 | 35,105 | 35,105 | 0 | 35,105 | 17,552 | 7.05 / 244.34 | `ext4` | 25,166,208 B |
| `mqttiumd-durable-ext4` | 32 | 1 | 20,000 | 6,848 | 6,848 | 0 | 6,848 | 3,424 | 75.80 / 97.44 | `ext4` | 4,354,872 B |
| `mosquitto-2.0.21-ram` | 32 | 1 | 20,000 | 40,000 | 40,000 | 0 | 40,000 | 20,000 | 0.02 / 0.20 | `ext4` | 0 B |
| `mqttiumd-volatile-ram` | 32 | 1 | 60,000 | 108,893 | 108,893 | 0 | 108,893 | 54,446 | 3.19 / 5.37 | `ext4` | 0 B |
| `mqttiumd-volatile-snap` | 32 | 1 | 60,000 | 111,742 | 111,742 | 0 | 111,742 | 55,871 | 3.27 / 5.19 | `ext4` | 0 B |
| `mqttiumd-log-ext4` | 32 | 1 | 60,000 | 45,900 | 45,900 | 0 | 45,900 | 22,950 | 8.53 / 44.95 | `ext4` | 33,554,816 B |
| `mqttiumd-durable-ext4` | 32 | 1 | 60,000 | 6,737 | 6,737 | 0 | 6,737 | 3,368 | 77.57 / 101.06 | `ext4` | 4,375,472 B |
| `mosquitto-2.0.21-ram` | 32 | 1 | 60,000 | 120,000 | 120,000 | 0 | 120,000 | 60,000 | 0.02 / 0.15 | `ext4` | 0 B |

### TLS 1.3 mTLS 256B (real ext4 backing)

| Target | Pairs | QoS | Offered/s | Submitted | Ack OK | Refused | Delivered | Deliv/s | p50 / p99 (ms) | FS | WAL / `.mlog` |
| - | -: | -: | -: | -: | -: | -: | -: | -: | -: | - | -: |
| `mqttiumd-volatile-ram` | 32 | 0 | 60,000 | 120,000 | 0 | 0 | 120,000 | 60,000 | 0.04 / 0.66 | `ext4` | 0 B |
| `mqttiumd-volatile-snap` | 32 | 0 | 60,000 | 120,000 | 0 | 0 | 120,000 | 60,000 | 0.03 / 0.76 | `ext4` | 0 B |
| `mqttiumd-log-ext4` | 32 | 0 | 60,000 | 120,000 | 0 | 0 | 120,000 | 60,000 | 0.04 / 0.61 | `ext4` | 262,168 B |
| `mqttiumd-durable-ext4` | 32 | 0 | 60,000 | 120,000 | 0 | 0 | 120,000 | 60,000 | 0.04 / 1.20 | `ext4` | 2,035,312 B |
| `mosquitto-2.0.21-ram` | 32 | 0 | 60,000 | 120,000 | 0 | 0 | 120,000 | 60,000 | 0.03 / 0.54 | `ext4` | 0 B |
| `mqttiumd-volatile-ram` | 32 | 1 | 20,000 | 40,000 | 40,000 | 0 | 40,000 | 20,000 | 0.05 / 0.32 | `ext4` | 0 B |
| `mqttiumd-volatile-snap` | 32 | 1 | 20,000 | 40,000 | 40,000 | 0 | 40,000 | 20,000 | 0.05 / 0.31 | `ext4` | 0 B |
| `mqttiumd-log-ext4` | 32 | 1 | 20,000 | 36,987 | 36,987 | 0 | 36,987 | 18,494 | 8.64 / 76.15 | `ext4` | 29,360,512 B |
| `mqttiumd-durable-ext4` | 32 | 1 | 20,000 | 5,546 | 5,546 | 0 | 5,546 | 2,773 | 836.10 / 1387.39 | `ext4` | 4,573,232 B |
| `mosquitto-2.0.21-ram` | 32 | 1 | 20,000 | 40,000 | 40,000 | 0 | 40,000 | 20,000 | 0.03 / 0.05 | `ext4` | 0 B |

### High Concurrency 256 conns Plain (real ext4 backing)

| Target | Pairs | QoS | Offered/s | Submitted | Ack OK | Refused | Delivered | Deliv/s | p50 / p99 (ms) | FS | WAL / `.mlog` |
| - | -: | -: | -: | -: | -: | -: | -: | -: | -: | - | -: |
| `mqttiumd-volatile-ram` | 128 | 0 | 120,000 | 236,543 | 0 | 0 | 236,543 | 118,272 | 0.95 / 3.80 | `ext4` | 0 B |
| `mqttiumd-volatile-snap` | 128 | 0 | 120,000 | 237,047 | 0 | 0 | 237,047 | 118,524 | 0.93 / 3.96 | `ext4` | 0 B |
| `mqttiumd-log-ext4` | 128 | 0 | 120,000 | 239,997 | 0 | 0 | 239,997 | 119,998 | 0.17 / 8.23 | `ext4` | 262,168 B |
| `mqttiumd-durable-ext4` | 128 | 0 | 120,000 | 239,955 | 0 | 0 | 239,955 | 119,978 | 1.01 / 10.35 | `ext4` | 4,124,152 B |
| `mosquitto-2.0.21-ram` | 128 | 0 | 120,000 | 240,000 | 0 | 0 | 240,000 | 120,000 | 513.54 / 732.54 | `ext4` | 0 B |
| `mqttiumd-volatile-ram` | 128 | 1 | 20,000 | 40,000 | 40,000 | 0 | 40,000 | 20,000 | 0.04 / 0.53 | `ext4` | 0 B |
| `mqttiumd-volatile-snap` | 128 | 1 | 20,000 | 40,000 | 40,000 | 0 | 40,000 | 20,000 | 0.04 / 0.51 | `ext4` | 0 B |
| `mqttiumd-log-ext4` | 128 | 1 | 20,000 | 39,603 | 39,603 | 0 | 39,603 | 19,802 | 10.46 / 76.27 | `ext4` | 29,360,512 B |
| `mqttiumd-durable-ext4` | 128 | 1 | 20,000 | 5,242 | 5,242 | 0 | 5,242 | 2,621 | 1376.72 / 2366.24 | `ext4` | 4,474,352 B |
| `mosquitto-2.0.21-ram` | 128 | 1 | 20,000 | 40,000 | 40,000 | 0 | 40,000 | 20,000 | 0.02 / 0.04 | `ext4` | 0 B |

### Large Payload 64 KiB Plain (real ext4 backing)

| Target | Pairs | QoS | Offered/s | Submitted | Ack OK | Refused | Delivered | Deliv/s | p50 / p99 (ms) | FS | WAL / `.mlog` |
| - | -: | -: | -: | -: | -: | -: | -: | -: | -: | - | -: |
| `mqttiumd-volatile-ram` | 4 | 1 | 2,000 | 4,000 | 4,000 | 0 | 4,000 | 2,000 | 0.11 / 0.26 | `ext4` | 0 B |
| `mqttiumd-volatile-snap` | 4 | 1 | 2,000 | 4,000 | 4,000 | 0 | 4,000 | 2,000 | 0.10 / 0.25 | `ext4` | 0 B |
| `mqttiumd-log-ext4` | 4 | 1 | 2,000 | 1,774 | 1,774 | 0 | 1,774 | 887 | 316.33 / 671.46 | `ext4` | 117,460,696 B |
| `mqttiumd-durable-ext4` | 4 | 1 | 2,000 | 1,221 | 1,221 | 0 | 1,221 | 610 | 63.37 / 758.42 | `ext4` | 5,887,512 B |
| `mosquitto-2.0.21-ram` | 4 | 1 | 2,000 | 4,000 | 4,000 | 0 | 4,000 | 2,000 | 0.06 / 0.13 | `ext4` | 0 B |

### Durable QoS 1 Control on tmpfs (/dev/shm) for backing contrast

| Target | Pairs | QoS | Offered/s | Submitted | Ack OK | Refused | Delivered | Deliv/s | p50 / p99 (ms) | FS | WAL / `.mlog` |
| - | -: | -: | -: | -: | -: | -: | -: | -: | -: | - | -: |
| `mqttiumd-log-tmpfs` | 32 | 1 | 20,000 | 40,000 | 40,000 | 0 | 40,000 | 20,000 | 0.05 / 0.68 | `tmpfs` | 29,360,512 B |
| `mqttiumd-durable-tmpfs` | 32 | 1 | 20,000 | 15,278 | 15,278 | 0 | 15,278 | 7,639 | 32.89 / 715.82 | `tmpfs` | 4,412,552 B |
| `mqttiumd-log-tmpfs` | 32 | 1 | 60,000 | 106,457 | 106,457 | 0 | 106,457 | 53,228 | 3.58 / 6.08 | `tmpfs` | 71,303,552 B |
| `mqttiumd-durable-tmpfs` | 32 | 1 | 60,000 | 15,503 | 15,503 | 0 | 15,503 | 7,752 | 32.80 / 44.25 | `tmpfs` | 4,367,232 B |

## Verification matrix (Lot 7)

| Check | Command | Result |
| - | - | - |
| Formatting | `cargo fmt --all -- --check` | PASS |
| Pure-crate boundary (`no_std`, no `unsafe`, closed macros/attrs) | `cargo test -p mqttiumd-lab --test boundary --locked` | PASS (17/17) |
| Full workspace tests (`FlatMap`/`ScopedFlatMap` collision & randomized tests, stack recipe freeze, direct volatile snapshot, unit, integration, replay, SQLite, volatile/log conformance) | `cargo test --workspace --locked` | PASS (0 failures) |
| Product tests with test-hooks | `cargo test -p mqttiumd --features test-hooks --locked` | PASS (0 failures) |
| Transactional kill-points suite | `cargo test -p mqttiumd-store-sqlite --features mqttiumd-lab/kill-points --locked` | PASS (0 failures) |
| Clippy (workspace, `test-hooks`, `kill-points`) | `cargo clippy ... --all-targets --locked -- -D warnings` (3 passes) | PASS (0 warnings) |
| Dist release build | `cargo build --profile dist -p mqttiumd --locked` | PASS |
| Contract validation & digest | `cargo run -q -p mqttiumd-lab -- validate --contracts contracts/v1 --scenario tests/scenarios/p1/P1-02.json` | PASS (`sha256:305db0c49a43b1d16269aeeca59aaf203c99521e19360f5dd93fdc7220fe37c8`, 17 docs) |
| Derived corpus & schemas | `python3 tools/build_corpus.py --check && python3 tools/check_schemas.py` | PASS (624 files fresh; 648 docs agreed, 0 disagreements) |
| Oracle traces & unit tests | `python3 tests/oracle/build_traces.py --check && python3 -m unittest discover -t tests/oracle -s tests/oracle/tests` | PASS (405 traces fresh; 274/274 tests OK) |
| Conformance matrix | `python3 conformance/check.py --check` | PASS (MQTT 5.0: 0 open; MQTT 3.1.1: 0 open) |
| Formal TLA+ models | `python3 models/check_models.py` | PASS (13/13 models consistent) |
| Differential compat vs Mosquitto 2.0.21 (`durable`) | `python3 tests/compat/run.py --broker target/dist/mqttiumd --reference .../mosquitto-2.0.21` | PASS (126/126, 0 defect, 0 unclassified, 21/21 expectations) |
| Differential compat vs Mosquitto 2.0.21 (`log`) | `python3 tests/compat/run.py --broker <log-wrapper> --reference .../mosquitto-2.0.21` | PASS (126/126, 0 defect, 0 unclassified, 21/21 expectations) |
| Differential compat vs Mosquitto 2.0.21 (`volatile`) | `python3 tests/compat/run.py --broker <volatile-wrapper> --reference .../mosquitto-2.0.21` | PASS (126/126, 0 defect, 0 unclassified, 21/21 expectations) |
| Frozen files unchanged | `git diff --stat -- Cargo.lock contracts/v1/product-profile.json docs/spec/fr docs/reports/0029* proofs credits.rs capacity.rs ids.rs` | PASS (0 bytes changed) |

## What this report does not show

- **Single-host contention**: the broker (3 CPUs) and load generator (1 CPU) share one Raspberry Pi 5 host over loopback TCP.
- **Hardware power-cut testing**: `LogStore` tests verify `fdatasync`, torn-tail truncation, stale-WAL discard, and fail-closed mid-file corruption in software, not physical power removal on storage controllers with volatile write caches.
- **Kani pilot proof re-execution**: `proofs/credits/`, `credits.rs`, `capacity.rs`, and `ids.rs` were not modified and keep their verified `BODY.sha256` digests unchanged; Kani was not re-invoked on ARM64.
- **Mosquitto durability contrast**: Mosquitto 2.0.21 is run with `persistence false` (in-memory only); it does not perform per-commit `fdatasync`, so durable `log` and `durable` modes carry strictly stronger crash guarantees than the reference column.
