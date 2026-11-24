# Lot 8 ARM64 QoS 1 throughput, idle memory, and WAL safety evaluation

This report records the empirical before/after evaluation, high-rate saturation comparison, idle-memory footprint, and full verification matrix for **Lot 8** on branch `perf/lot8-qos1-idle-ram` (branched from Lot 7 commit `290db2be`), compared against Lot 7 ([report 0030](0030-lot7-arm64-performance-and-durability.md)) and system reference broker Mosquitto 2.1.2 (`/usr/sbin/mosquitto`).

Architectural decisions for Lot 8 are documented in Proposed record [0064](../decisions/0064-lot8-direct-dispatch-compact-state-and-wal-safety.md).

## Host, toolchain, and storage backing

- **Hardware**: ARM64 4-core Cortex-A76 (Raspberry Pi 5, 7.9 GiB RAM, 0 B swap).
- **Kernel**: Linux `6.18.60+rpt-rpi-2712` (`aarch64`).
- **Toolchain**: `rustc 1.97.0 (2d8144b78 2026-07-07)`, built with `cargo build -p mqttiumd --profile dist --locked` (`target/dist/mqttiumd`, `sha256: 7e496f3952dd0a298777b18b3797fe338d307a150b1ac4725927665c9985abb1`).
- **Reference broker**: `mosquitto version 2.1.2` (`/usr/sbin/mosquitto`; note that `run_arm64_suite.py` prints the legacy label string `mosquitto-2.0.21-ram`, while the binary executed in every Lot 8 run below is `/usr/sbin/mosquitto` `2.1.2`).
- **CPU pinning**: broker pinned to CPUs `0,1,2` (`--io-threads 2`); load generator pinned to CPU `3`.
- **Real disk backing**: `/dev/sda2` (`ext4`) via a temporary working directory under `mqttiumd/target/bench-ext4-tmp` for all `real ext4 backing` runs; `tmpfs` (`/dev/shm` and `/tmp`) only for explicit `tmpfs` controls and RAM saturation comparisons.
- **Generator**: [tools/arm64_bench/arm64_bench.c](../../tools/arm64_bench/arm64_bench.c) (`sha256: b0ec96a4cc59037e4b9f2eb33c35cd73762b487a5fc6a5ad4136275ca6103dd7`), MQTT 5, 2-second publish window followed by full drain.

## Reproduce

From `mqttiumd/`:

```sh
cargo build -p mqttiumd --profile dist --locked

# Idle memory footprint (Plain TCP 0..768, TLS 1.3 mTLS 0..400)
python3 tools/arm64_bench/idle_rss_bench.py \
  --binary target/dist/mqttiumd \
  --mosquitto /usr/sbin/mosquitto \
  --plain-counts 128,256,512,768 \
  --mtls-counts 64,128,256,400 \
  --out /tmp/idle_rss_lot8_final.json

# Full 59-scenario suite on real ext4 backing
mkdir -p target/bench-ext4-tmp
python3 tools/arm64_bench/run_arm64_suite.py \
  --binary target/dist/mqttiumd \
  --mosquitto /usr/sbin/mosquitto \
  --work-dir "$PWD/target/bench-ext4-tmp" \
  --out /tmp/arm64_lot8_suite_results.json
rm -rf target/bench-ext4-tmp

# High-rate saturation A/B stress suite (Plain TCP volatile, log-tmpfs, TLS 1.3 mTLS)
python3 tools/arm64_bench/run_arm64_bench.py \
  --binary target/dist/mqttiumd --mosquitto /usr/sbin/mosquitto \
  --only both --work-dir /tmp \
  --cases 4:1:100000,4:1:150000,4:1:200000,32:1:100000,32:1:150000,128:1:60000,128:1:100000,32:0:300000 \
  --extra=--storage-mode --extra=volatile --extra=--no-db --extra=--io-threads --extra=2

python3 tools/arm64_bench/run_arm64_bench.py \
  --binary target/dist/mqttiumd --mosquitto /usr/sbin/mosquitto \
  --only mqttiumd --work-dir /tmp \
  --cases 4:1:100000,32:1:100000,32:1:150000 \
  --extra=--storage-mode --extra=log --extra=--io-threads --extra=2

python3 tools/arm64_bench/run_arm64_bench.py \
  --binary target/dist/mqttiumd --mosquitto /usr/sbin/mosquitto \
  --only both --tls --work-dir /tmp \
  --cases 32:0:120000,32:1:60000,32:1:100000 \
  --extra=--storage-mode --extra=volatile --extra=--no-db --extra=--io-threads --extra=2

# QoS 0 paced delivery verification (record 0063)
python3 tools/measure_qos0_delivery.py --binary target/dist/mqttiumd --shape bursts --messages 100000 --burst 100 --subscribers 1
python3 tools/measure_qos0_delivery.py --binary target/dist/mqttiumd --shape bursts --messages 100000 --burst 1000 --subscribers 1
python3 tools/measure_qos0_delivery.py --binary target/dist/mqttiumd --shape blast --messages 100000 --subscribers 8
```

## Summary of key ARM64 gains (Lot 7 -> **Lot 8** vs Mosquitto 2.1.2)

Across all **59 / 59 scenarios** of the `ext4` suite and all **25 / 25** high-rate saturation runs, `mqttiumd` recorded **0 integrity errors** (`acked_refused = 0`, `ack_no_deliv = 0`, `disconnects = 0`, `invalid = 0`, `loss = 0.0 %`).

### 59-scenario suite highlights (`--profile dist`)

| Workload | Mode | Lot 7 (report 0030) | **Lot 8 (`dist`)** | Mosquitto 2.1.2 (RAM) |
| - | - | -: | -: | -: |
| Plain 32p QoS 0 @ 120k/s | `volatile-ram` | 120,000/s (0.03 / 0.71 ms) | **120,000/s (0.03 / 2.97 ms)** | 120,000/s (2.18 / 4.57 ms) |
| Plain 32p QoS 0 @ 120k/s | `volatile-snap` | 120,000/s (0.03 / 0.70 ms) | **120,000/s (0.02 / 0.53 ms)** | 120,000/s (2.18 / 4.57 ms) |
| Plain 32p QoS 1 @ 20k/s | `volatile-ram` | 20,000/s (0.04 / 0.20 ms) | **20,000/s (0.03 / 0.13 ms)** | 20,000/s (0.01 / 1.50 ms) |
| Plain 32p QoS 1 @ 60k/s | `volatile-ram` | 54,446/s (3.19 / 5.37 ms) | **60,000/s (0.04 / 0.49 ms)** | 60,000/s (0.01 / 0.12 ms) |
| Plain 32p QoS 1 @ 60k/s | `volatile-snap` | 55,871/s (3.27 / 5.19 ms) | **58,886/s (0.03 / 0.76 ms)** | 60,000/s (0.01 / 0.12 ms) |
| Plain 32p QoS 1 @ 60k/s | `log-ext4` | 22,950/s (8.53 / 44.95 ms) | **26,483/s (397.70 / 746.67 ms)** | *(RAM only)* |
| Plain 32p QoS 1 @ 20k/s | `log-ext4` | 17,552/s (7.05 / 244.34 ms) | **19,050/s (2.43 / 33.14 ms)** | *(RAM only)* |
| mTLS 32p QoS 0 @ 60k/s | `volatile-ram` | 60,000/s (0.04 / 0.66 ms) | **60,000/s (0.03 / 0.50 ms)** | 60,000/s (0.04 / 0.60 ms) |
| mTLS 32p QoS 1 @ 20k/s | `volatile-ram` | 20,000/s (0.05 / 0.32 ms) | **20,000/s (0.04 / 0.20 ms)** | 20,000/s (0.03 / 0.04 ms) |
| 256 conns QoS 0 @ 120k/s | `log-ext4` | 119,998/s (0.17 / 8.23 ms) | **120,000/s (0.04 / 1.74 ms)** | 120,000/s (0.05 / 18.21 ms) |
| 256 conns QoS 1 @ 20k/s | `volatile-ram` | 20,000/s (0.04 / 0.53 ms) | **20,000/s (0.04 / 0.23 ms)** | 20,000/s (0.02 / 3.49 ms) |
| 256 conns QoS 1 @ 20k/s | `log-ext4` | 19,802/s (10.46 / 76.27 ms) | **20,000/s (2.26 / 20.42 ms)** | *(RAM only)* |
| 64 KiB 4p QoS 1 @ 2k/s | `log-ext4` | 887/s (316.33 / 671.46 ms) | **1,140/s (23.68 / 54.23 ms)** | *(RAM only)* |
| tmpfs 32p QoS 1 @ 60k/s | `log-tmpfs` | 53,228/s (3.58 / 6.08 ms) | **59,992/s (0.16 / 1.93 ms)** | *(RAM only)* |
| tmpfs 32p QoS 1 @ 60k/s | `durable-tmpfs` | 7,752/s (32.80 / 44.25 ms) | **8,034/s (31.62 / 36.82 ms)** | *(RAM only)* |

### High-rate saturation A/B (`tmpfs`, 256 B payload, 2 s window + drain)

| Workload | Mode | **Lot 8 `mqttiumd` (`dist`)** | Mosquitto 2.1.2 (RAM) | Notes |
| - | - | -: | -: | - |
| Plain 4p QoS 1 @ 100k/s | `volatile-ram` | **97,938/s (0.05 / 0.20 ms)** | 99,749/s (0.08 / 0.12 ms) | Lower p50 than Mosquitto |
| Plain 4p QoS 1 @ 150k/s | `volatile-ram` | **129,687/s (0.18 / 0.35 ms)** | 150,000/s (0.14 / 0.28 ms) | **2.07×** vs Lot 7 (`62,515/s`) |
| Plain 4p QoS 1 @ 200k/s | `volatile-ram` | **121,857/s (0.19 / 0.58 ms)** | 174,839/s (1.34 / 1.65 ms) | 7× lower p50 under saturation |
| Plain 32p QoS 1 @ 100k/s | `volatile-ram` | **98,378/s (0.75 / 3.30 ms)** | 99,982/s (0.65 / 1.03 ms) | **+80.7 %** vs Lot 7 (`54,446/s`) |
| Plain 32p QoS 1 @ 150k/s | `volatile-ram` | **89,539/s (2.18 / 5.91 ms)** | 148,830/s (0.80 / 15.60 ms) | 2.6× lower p99 |
| Plain 128p QoS 1 @ 60k/s | `volatile-ram` | **59,608/s (4.03 / 14.00 ms)** | 60,000/s (0.02 / 1.35 ms) | 256 connections |
| Plain 128p QoS 1 @ 100k/s | `volatile-ram` | **62,632/s (14.03 / 22.53 ms)** | 62,900/s (3.47 / 5.11 ms) | Tied throughput at 256 conns |
| Plain 32p QoS 0 @ 300k/s | `volatile-ram` | **299,983/s (0.28 / 1.63 ms)** | 300,000/s (8.64 / 20.57 ms) | **30.9× lower p50, 12.6× lower p99** |
| Plain 4p QoS 1 @ 100k/s | `log-tmpfs` | **92,406/s (0.15 / 0.87 ms)** | *(RAM only)* | CRC-32C + `fdatasync` |
| Plain 32p QoS 1 @ 100k/s | `log-tmpfs` | **69,538/s (3.14 / 12.00 ms)** | *(RAM only)* | CRC-32C + `fdatasync` |
| Plain 32p QoS 1 @ 150k/s | `log-tmpfs` | **75,653/s (2.80 / 9.78 ms)** | *(RAM only)* | CRC-32C + `fdatasync` |
| **mTLS 32p QoS 0 @ 120k/s** | `volatile-ram` | **119,985/s (0.29 / 1.59 ms, 0.0 % loss)** | 93,575/s (672.74 / 1229.88 ms, **22.0 % loss**) | **Zero loss, 2,320× lower p50** |
| **mTLS 32p QoS 1 @ 60k/s** | `volatile-ram` | **59,974/s (0.53 / 1.65 ms)** | 41,832/s (49.50 / 56.43 ms) | **1.43× throughput, 93× lower p50** |
| **mTLS 32p QoS 1 @ 100k/s** | `volatile-ram` | **81,566/s (2.51 / 3.86 ms)** | 43,168/s (51.06 / 53.44 ms) | **1.89× throughput, 20× lower p50** |

### QoS 0 paced delivery verification (`measure_qos0_delivery.py`)

| Shape | Messages | Subscribers | Received (min / max) | Closed subs | Delivered/s |
| - | -: | -: | -: | -: | -: |
| `bursts` (burst 100, 64 B) | 100,000 | 1 | **100,000 / 100,000 (100.0 %)** | 0 | **334,726/s** |
| `bursts` (burst 1,000, 64 B) | 100,000 | 1 | **100,000 / 100,000 (100.0 %)** | 0 | **410,558/s** |
| `blast` (64 B) | 100,000 | 8 | **100,000 / 100,000 (100.0 %)** | 0 | **593,873/s** |

## Idle memory per connection (ARM64, `idle_rss_bench.py`)

`mqttiumd` runs with **5 threads** total (`DirectWriter` in `volatile` mode) and **1.00 FD/conn** across every point.

| Profile | Conns | Lot 7 RSS / Dirty | **Lot 8 RSS / Dirty (`dist`)** | **Lot 8 Total VmRSS** | Mosquitto 2.1.2 RSS / Dirty | Mosquitto 2.1.2 Total VmRSS |
| - | -: | -: | -: | -: | -: | -: |
| Plain TCP | 0 | — | — | **7,312 KiB** | — | 11,360 KiB |
| Plain TCP | 128 | 12,800 / 11,264 B | **7,168 / 6,144 B** | **8,208 KiB** | 4,992 / 4,992 B | 11,984 KiB |
| Plain TCP | 256 | 12,160 / 11,392 B | **6,336 / 5,824 B** | **8,896 KiB** | 5,056 / 5,056 B | 12,624 KiB |
| Plain TCP | 512 | 10,464 / 10,080 B | **5,728 / 5,472 B** | **10,176 KiB** | 5,088 / 5,088 B | 13,904 KiB |
| Plain TCP | 768 | 9,728 / 9,472 B | **5,269 / 5,099 B** | **11,264 KiB** | 5,077 / 5,077 B | 15,168 KiB |
| TLS 1.3 mTLS | 0 | — | — | **8,576 KiB** | — | 14,112 KiB |
| TLS 1.3 mTLS | 64 | — | **20,480 / 16,384 B** | **9,856 KiB** | 46,592 / 43,520 B | 17,024 KiB |
| TLS 1.3 mTLS | 128 | 23,040 / 20,992 B | **17,408 / 15,360 B** | **10,752 KiB** | 45,824 / 43,264 B | 19,840 KiB |
| TLS 1.3 mTLS | 256 | 22,336 / 21,312 B | **15,808 / 14,784 B** | **12,528 KiB** | 44,480 / 43,200 B | 25,232 KiB |
| TLS 1.3 mTLS | 400 | 20,931 / 20,275 B | **14,746 / 14,090 B** | **14,336 KiB** | 43,950 / 43,131 B | 31,280 KiB |

- **Plain TCP 512→768 marginal slope**: `mqttiumd` = **4,352 B/conn** vs Mosquitto 2.1.2 = **4,992 B/conn**; at 768 connections `mqttiumd` uses **25.7 % less total VmRSS** (`11,264 KiB` vs `15,168 KiB`).
- **TLS 1.3 mTLS 256→400 marginal slope**: `mqttiumd` = **12,864 B/conn** vs Mosquitto 2.1.2 = **43,008 B/conn** (**3.34×** smaller marginal cost, **2.18×** smaller total VmRSS at 400 connections).

## Complete 59-scenario accounting (Lot 8, real `ext4` + `tmpfs` control)

In every row below, `acked_refused = 0`, `ack_no_deliv = 0`, `disconnects = 0`, and `invalid = 0`. Reference rows ran `/usr/sbin/mosquitto` (`2.1.2`).

### Plain TCP 256 B (real `ext4` backing)

| Target | Pairs | QoS | Offered/s | Submitted | Ack OK | Delivered | Deliv/s | p50 / p99 (ms) | FS | WAL / `.mlog` |
| - | -: | -: | -: | -: | -: | -: | -: | -: | - | -: |
| `mqttiumd-volatile-ram` | 4 | 0 | 60,000 | 120,000 | 0 | 120,000 | 60,000 | 0.02 / 1.28 | `ext4` | 0 B |
| `mqttiumd-volatile-snap` | 4 | 0 | 60,000 | 120,000 | 0 | 120,000 | 60,000 | 0.02 / 1.55 | `ext4` | 0 B |
| `mqttiumd-log-ext4` | 4 | 0 | 60,000 | 120,000 | 0 | 120,000 | 60,000 | 0.02 / 1.58 | `ext4` | 262,168 B |
| `mqttiumd-durable-ext4` | 4 | 0 | 60,000 | 120,000 | 0 | 120,000 | 60,000 | 0.02 / 4.37 | `ext4` | 321,392 B |
| `mosquitto-2.1.2-ram` | 4 | 0 | 60,000 | 120,000 | 0 | 120,000 | 60,000 | 0.56 / 1.15 | `ext4` | 0 B |
| `mqttiumd-volatile-ram` | 32 | 0 | 120,000 | 240,000 | 0 | 240,000 | 120,000 | 0.03 / 2.97 | `ext4` | 0 B |
| `mqttiumd-volatile-snap` | 32 | 0 | 120,000 | 240,000 | 0 | 240,000 | 120,000 | 0.02 / 0.53 | `ext4` | 0 B |
| `mqttiumd-log-ext4` | 32 | 0 | 120,000 | 240,000 | 0 | 240,000 | 120,000 | 0.03 / 0.78 | `ext4` | 262,168 B |
| `mqttiumd-durable-ext4` | 32 | 0 | 120,000 | 240,000 | 0 | 240,000 | 120,000 | 0.03 / 2.41 | `ext4` | 1,742,792 B |
| `mosquitto-2.1.2-ram` | 32 | 0 | 120,000 | 240,000 | 0 | 240,000 | 120,000 | 2.18 / 4.57 | `ext4` | 0 B |
| `mqttiumd-volatile-ram` | 4 | 1 | 3,000 | 6,000 | 6,000 | 6,000 | 3,000 | 0.03 / 0.08 | `ext4` | 0 B |
| `mqttiumd-volatile-snap` | 4 | 1 | 3,000 | 6,000 | 6,000 | 6,000 | 3,000 | 0.03 / 0.08 | `ext4` | 0 B |
| `mqttiumd-log-ext4` | 4 | 1 | 3,000 | 5,893 | 5,893 | 5,893 | 2,946 | 3.21 / 20.60 | `ext4` | 8,388,992 B |
| `mqttiumd-durable-ext4` | 4 | 1 | 3,000 | 5,654 | 5,654 | 5,654 | 2,827 | 6.35 / 89.01 | `ext4` | 4,206,552 B |
| `mosquitto-2.1.2-ram` | 4 | 1 | 3,000 | 6,000 | 6,000 | 6,000 | 3,000 | 0.01 / 2.69 | `ext4` | 0 B |
| `mqttiumd-volatile-ram` | 32 | 1 | 3,000 | 6,000 | 6,000 | 6,000 | 3,000 | 0.03 / 0.09 | `ext4` | 0 B |
| `mqttiumd-volatile-snap` | 32 | 1 | 3,000 | 6,000 | 6,000 | 6,000 | 3,000 | 0.03 / 0.09 | `ext4` | 0 B |
| `mqttiumd-log-ext4` | 32 | 1 | 3,000 | 6,000 | 6,000 | 6,000 | 3,000 | 1.23 / 8.72 | `ext4` | 4,194,688 B |
| `mqttiumd-durable-ext4` | 32 | 1 | 3,000 | 5,643 | 5,643 | 5,643 | 2,822 | 34.04 / 138.77 | `ext4` | 4,272,472 B |
| `mosquitto-2.1.2-ram` | 32 | 1 | 3,000 | 6,000 | 6,000 | 6,000 | 3,000 | 0.01 / 1.85 | `ext4` | 0 B |
| `mqttiumd-volatile-ram` | 32 | 1 | 20,000 | 40,000 | 40,000 | 40,000 | 20,000 | 0.03 / 0.13 | `ext4` | 0 B |
| `mqttiumd-volatile-snap` | 32 | 1 | 20,000 | 40,000 | 40,000 | 40,000 | 20,000 | 0.03 / 0.12 | `ext4` | 0 B |
| `mqttiumd-log-ext4` | 32 | 1 | 20,000 | 38,101 | 38,101 | 38,101 | 19,050 | 2.43 / 33.14 | `ext4` | 29,360,512 B |
| `mqttiumd-durable-ext4` | 32 | 1 | 20,000 | 6,039 | 6,039 | 6,039 | 3,020 | 79.80 / 134.82 | `ext4` | 4,375,472 B |
| `mosquitto-2.1.2-ram` | 32 | 1 | 20,000 | 40,000 | 40,000 | 40,000 | 20,000 | 0.01 / 1.50 | `ext4` | 0 B |
| `mqttiumd-volatile-ram` | 32 | 1 | 60,000 | 120,000 | 120,000 | 120,000 | 60,000 | 0.04 / 0.49 | `ext4` | 0 B |
| `mqttiumd-volatile-snap` | 32 | 1 | 60,000 | 117,772 | 117,772 | 117,772 | 58,886 | 0.03 / 0.76 | `ext4` | 0 B |
| `mqttiumd-log-ext4` | 32 | 1 | 60,000 | 52,966 | 52,966 | 52,966 | 26,483 | 397.70 / 746.67 | `ext4` | 41,943,424 B |
| `mqttiumd-durable-ext4` | 32 | 1 | 60,000 | 5,651 | 5,651 | 5,651 | 2,826 | 81.86 / 167.44 | `ext4` | 4,371,352 B |
| `mosquitto-2.1.2-ram` | 32 | 1 | 60,000 | 120,000 | 120,000 | 120,000 | 60,000 | 0.01 / 0.12 | `ext4` | 0 B |

### TLS 1.3 mTLS 256 B (real `ext4` backing)

| Target | Pairs | QoS | Offered/s | Submitted | Ack OK | Delivered | Deliv/s | p50 / p99 (ms) | FS | WAL / `.mlog` |
| - | -: | -: | -: | -: | -: | -: | -: | -: | - | -: |
| `mqttiumd-volatile-ram` | 32 | 0 | 60,000 | 120,000 | 0 | 120,000 | 60,000 | 0.03 / 0.50 | `ext4` | 0 B |
| `mqttiumd-volatile-snap` | 32 | 0 | 60,000 | 120,000 | 0 | 120,000 | 60,000 | 0.03 / 1.04 | `ext4` | 0 B |
| `mqttiumd-log-ext4` | 32 | 0 | 60,000 | 120,000 | 0 | 120,000 | 60,000 | 0.03 / 0.82 | `ext4` | 262,168 B |
| `mqttiumd-durable-ext4` | 32 | 0 | 60,000 | 120,000 | 0 | 120,000 | 60,000 | 0.03 / 3.89 | `ext4` | 2,027,072 B |
| `mosquitto-2.1.2-ram` | 32 | 0 | 60,000 | 120,000 | 0 | 120,000 | 60,000 | 0.04 / 0.60 | `ext4` | 0 B |
| `mqttiumd-volatile-ram` | 32 | 1 | 20,000 | 40,000 | 40,000 | 40,000 | 20,000 | 0.04 / 0.20 | `ext4` | 0 B |
| `mqttiumd-volatile-snap` | 32 | 1 | 20,000 | 40,000 | 40,000 | 40,000 | 20,000 | 0.04 / 0.19 | `ext4` | 0 B |
| `mqttiumd-log-ext4` | 32 | 1 | 20,000 | 34,786 | 34,786 | 34,786 | 17,393 | 3.55 / 44.73 | `ext4` | 25,166,208 B |
| `mqttiumd-durable-ext4` | 32 | 1 | 20,000 | 6,636 | 6,636 | 6,636 | 3,318 | 78.48 / 97.83 | `ext4` | 4,367,232 B |
| `mosquitto-2.1.2-ram` | 32 | 1 | 20,000 | 40,000 | 40,000 | 40,000 | 20,000 | 0.03 / 0.04 | `ext4` | 0 B |

### High concurrency 256 connections Plain TCP 256 B (real `ext4` backing)

| Target | Pairs | QoS | Offered/s | Submitted | Ack OK | Delivered | Deliv/s | p50 / p99 (ms) | FS | WAL / `.mlog` |
| - | -: | -: | -: | -: | -: | -: | -: | -: | - | -: |
| `mqttiumd-volatile-ram` | 128 | 0 | 120,000 | 239,994 | 0 | 239,994 | 119,997 | 0.15 / 1.82 | `ext4` | 0 B |
| `mqttiumd-volatile-snap` | 128 | 0 | 120,000 | 239,529 | 0 | 239,529 | 119,764 | 0.70 / 2.16 | `ext4` | 0 B |
| `mqttiumd-log-ext4` | 128 | 0 | 120,000 | 240,000 | 0 | 240,000 | 120,000 | 0.04 / 1.74 | `ext4` | 262,168 B |
| `mqttiumd-durable-ext4` | 128 | 0 | 120,000 | 240,000 | 0 | 240,000 | 120,000 | 0.08 / 5.25 | `ext4` | 4,128,272 B |
| `mosquitto-2.1.2-ram` | 128 | 0 | 120,000 | 240,000 | 0 | 240,000 | 120,000 | 0.05 / 18.21 | `ext4` | 0 B |
| `mqttiumd-volatile-ram` | 128 | 1 | 20,000 | 40,000 | 40,000 | 40,000 | 20,000 | 0.04 / 0.23 | `ext4` | 0 B |
| `mqttiumd-volatile-snap` | 128 | 1 | 20,000 | 40,000 | 40,000 | 40,000 | 20,000 | 0.04 / 0.35 | `ext4` | 0 B |
| `mqttiumd-log-ext4` | 128 | 1 | 20,000 | 40,000 | 40,000 | 40,000 | 20,000 | 2.26 / 20.42 | `ext4` | 29,360,512 B |
| `mqttiumd-durable-ext4` | 128 | 1 | 20,000 | 6,183 | 6,183 | 6,183 | 3,092 | 397.08 / 1781.34 | `ext4` | 4,540,272 B |
| `mosquitto-2.1.2-ram` | 128 | 1 | 20,000 | 40,000 | 40,000 | 40,000 | 20,000 | 0.02 / 3.49 | `ext4` | 0 B |

### Large payload 64 KiB Plain TCP (real `ext4` backing) and `tmpfs` control

| Target | Pairs | QoS | Offered/s | Submitted | Ack OK | Delivered | Deliv/s | p50 / p99 (ms) | FS | WAL / `.mlog` |
| - | -: | -: | -: | -: | -: | -: | -: | -: | - | -: |
| `mqttiumd-volatile-ram` | 4 | 1 | 2,000 | 4,000 | 4,000 | 4,000 | 2,000 | 0.09 / 0.24 | `ext4` | 0 B |
| `mqttiumd-volatile-snap` | 4 | 1 | 2,000 | 4,000 | 4,000 | 4,000 | 2,000 | 0.10 / 0.25 | `ext4` | 0 B |
| `mqttiumd-log-ext4` | 4 | 1 | 2,000 | 2,281 | 2,281 | 2,281 | 1,140 | 23.68 / 54.23 | `ext4` | 18,115,588 B |
| `mqttiumd-durable-ext4` | 4 | 1 | 2,000 | 1,092 | 1,092 | 1,092 | 546 | 61.64 / 291.73 | `ext4` | 5,978,152 B |
| `mosquitto-2.1.2-ram` | 4 | 1 | 2,000 | 4,000 | 4,000 | 4,000 | 2,000 | 0.06 / 0.12 | `ext4` | 0 B |
| `mqttiumd-log-tmpfs` | 32 | 1 | 20,000 | 40,000 | 40,000 | 40,000 | 20,000 | 0.04 / 0.99 | `tmpfs` | 29,360,512 B |
| `mqttiumd-durable-tmpfs` | 32 | 1 | 20,000 | 15,978 | 15,978 | 15,978 | 7,989 | 31.12 / 36.08 | `tmpfs` | 4,371,352 B |
| `mqttiumd-log-tmpfs` | 32 | 1 | 60,000 | 119,985 | 119,985 | 119,985 | 59,992 | 0.16 / 1.93 | `tmpfs` | 79,692,160 B |
| `mqttiumd-durable-tmpfs` | 32 | 1 | 60,000 | 16,069 | 16,069 | 16,069 | 8,034 | 31.62 / 36.82 | `tmpfs` | 4,371,352 B |

## Verification ledger

All commands were executed sequentially on this ARM64 host (`CARGO_BUILD_JOBS=1` for test binaries to stay well within 7.9 GiB RAM with zero swap):

| Check | Command | Outcome |
| - | - | - |
| Formatting | `cargo fmt --all -- --check` | PASS |
| Clippy (default, `kill-points`, `test-hooks`) | `cargo clippy ... -- -D warnings` (all 3 configs) | PASS (0 warnings) |
| Pure-crate boundary (ADR 0007) | `cargo test -p mqttiumd-lab --test boundary --locked` | PASS (17/17) |
| Kani credits proof digest | `sha256sum crates/mqttiumd-contract/src/capacity.rs` | `71378725…` (unchanged) |
| Workspace unit & integration tests (default) | `cargo test -j 1 -p <crate> --locked -- --test-threads=2` (all 7 crates) | **PASS (1,380/1,380)** |
| Instrumented `kill-points` | `cargo test -j 1 -p mqttiumd-lab --features kill-points --locked -- --test-threads=2` | **PASS (334/334)** |
| Instrumented `test-hooks` | `cargo test -j 1 -p mqttiumd --features test-hooks --locked -- --test-threads=2` | **PASS (288/288)** |
| WAL compaction durability regression | `failed_wal_replacement_suspends_durable_writes_until_repaired` (`log_conformance`) | PASS (real `EISDIR` fault) |
| Oracle unit tests | `python3 -m unittest discover -t tests/oracle -s tests/oracle/tests` | **PASS (274/274)** |
| End-to-end oracle campaigns | `tools/run_campaign.sh` on `P1-02` (`sim-store`, `sqlite`) & `QABS-B` (`sim-store`) | **PASS (3/3)** |
| Compat harness unit tests | `python3 -m unittest discover -t tests/compat -s tests/compat` | **PASS (64/64)** |
| Mutant patch applicability | `git apply --check tests/mutants/*/*.mutant` | **PASS (23/23)** |
| Conformance register | `python3 conformance/check.py --check` | PASS (0 open) |
| Formal model consistency | `python3 models/check_models.py` | **PASS (13/13 consistent)** |
| Requirement register & derived corpus | `python3 tools/build_corpus.py --check`, `tests/oracle/build_traces.py --check`, `mqttiumd-lab validate` | **PASS** (`624` derived, `405` traces, `17` docs; contract digest unchanged: `sha256:21e96cc81c1f48d59841d8424fee2290102c6782b025222f2d6d2fe72ccf9bb5`) |

### Not run in this sandbox
- Differential `tests/compat/run.py` against Mosquitto 2.1.4 (offline sandbox has `/usr/sbin/mosquitto` 2.1.2 and cached 2.0.21; `tests/compat/run.py` refuses versions other than pinned 2.1.4).
- `tools/run_loop_mutants.sh` execution and TLC (`tla2tools.jar` not installed locally; `git worktree` requires write access to `.git`). All 23/23 `.mutant` patches were verified to apply cleanly and all 13/13 TLA+ models passed `models/check_models.py`.
- `proofs/credits/run.sh` (Kani not installed; all files hashed in `proofs/credits/BODY.sha256` remain byte-for-byte unchanged).
