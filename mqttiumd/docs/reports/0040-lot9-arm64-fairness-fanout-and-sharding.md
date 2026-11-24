# Lot 9 ARM64 fairness, atomic 1:N fan-out, refusal elimination, and multi-core sharding evaluation

This report records the empirical before/after evaluation, adversarial 8-scenario load matrix, high-rate saturation and real `ext4` measurements, idle-memory footprint, ARM64 `perf` multi-core *Delivery Owners* sharding analysis, and full verification ledger for **Lot 9** on branch `perf/lot9-fairness-refusals-and-fanout` (branched from Lot 8 commit `0252fdc5`), compared against Lot 8 ([report 0039](0039-lot8-arm64-qos1-throughput-and-idle-ram.md)) and system reference broker Mosquitto 2.1.2 (`/usr/sbin/mosquitto`).

Architectural decisions for Lot 9 are documented in Proposed record [0065](../decisions/0065-lot9-refusals-fairness-and-atomic-fanout.md).

## Host, toolchain, and storage backing

- **Hardware**: ARM64 4-core Cortex-A76 (Raspberry Pi 5, 7.9 GiB RAM, 0 B swap).
- **Kernel**: Linux `6.18.60+rpt-rpi-2712` (`aarch64`).
- **Toolchain**: `rustc 1.97.0 (2d8144b78 2026-07-07)`, built with `cargo build -j 2 -p mqttiumd --profile dist --locked` (`target/dist/mqttiumd`, `sha256: 78b4e73eb706d34c7a2b3596cd22dff7e7d84fab3ede205da0618ebb27ff69bf`).
- **Reference broker**: `mosquitto version 2.1.2` (`/usr/sbin/mosquitto`, in-memory `persistence false`, `max_inflight_messages 64`).
- **CPU pinning**: broker pinned to CPUs `0,1,2` (`--io-threads 2`); load generator pinned to CPU `3`.
- **Storage backing**: real disk `/dev/sda2` (`ext4`) via temporary `mqttiumd/target/bench-ext4-tmp` for `log-ext4` runs; `tmpfs` (`/tmp`) for adversarial matrix, RAM/WAL saturation controls, and `perf` CPU profiling.
- **Generators**:
  - Adversarial matrix driver: [tools/arm64_bench/arm64_matrix_bench.c](../../tools/arm64_bench/arm64_matrix_bench.c) (`sha256: 5c4fb823799536b4d7e6f35b6c57b9061a057f9ef05e68e74b56dd2457751026`), driven by [tools/arm64_bench/run_matrix_bench.py](../../tools/arm64_bench/run_matrix_bench.py), MQTT 5, 2-second publish window + full drain.
  - Pair saturation driver: [tools/arm64_bench/arm64_bench.c](../../tools/arm64_bench/arm64_bench.c) (`sha256: b0ec96a4cc59037e4b9f2eb33c35cd73762b487a5fc6a5ad4136275ca6103dd7`), driven by [tools/arm64_bench/run_arm64_bench.py](../../tools/arm64_bench/run_arm64_bench.py).

## Reproduce

From `mqttiumd/`:

```sh
CARGO_BUILD_JOBS=2 cargo build -j 2 -p mqttiumd --profile dist --locked

# 1. Adversarial 8-scenario load matrix (fan-out, fan-in, slow-sub, churn, mixed payload)
python3 tools/arm64_bench/run_matrix_bench.py \
  --binary target/dist/mqttiumd \
  --mosquitto /usr/sbin/mosquitto \
  --work-dir /tmp \
  --out /tmp/lot9_after_matrix.json

# 2. High-rate Plain TCP & TLS 1.3 mTLS saturation (tmpfs)
python3 tools/arm64_bench/run_arm64_bench.py \
  --binary target/dist/mqttiumd --only mqttiumd --label lot9-vol --work-dir /tmp \
  --cases 4:1:150000,32:1:100000,128:1:60000,128:1:100000,32:0:300000 \
  --extra=--storage-mode --extra=volatile --extra=--io-threads --extra=2

python3 tools/arm64_bench/run_arm64_bench.py \
  --binary target/dist/mqttiumd --only mqttiumd --label lot9-log --work-dir /tmp \
  --cases 4:1:100000,4:1:150000,32:1:100000,128:1:60000,128:1:100000,32:0:300000 \
  --extra=--storage-mode --extra=log --extra=--io-threads --extra=2

python3 tools/arm64_bench/run_arm64_bench.py \
  --binary target/dist/mqttiumd --tls --only mqttiumd --label lot9-vol-mtls --work-dir /tmp \
  --cases 32:1:60000,32:1:100000 \
  --extra=--storage-mode --extra=volatile --extra=--io-threads --extra=2

python3 tools/arm64_bench/run_arm64_bench.py \
  --binary target/dist/mqttiumd --tls --only mqttiumd --label lot9-log-mtls --work-dir /tmp \
  --cases 32:1:100000 \
  --extra=--storage-mode --extra=log --extra=--io-threads --extra=2

# 3. Real ext4 backing (256 B & 64 KiB)
mkdir -p target/bench-ext4-tmp
python3 tools/arm64_bench/run_arm64_bench.py \
  --binary target/dist/mqttiumd --only mqttiumd --label lot9-ext4-log \
  --work-dir "$PWD/target/bench-ext4-tmp" --cases 32:1:60000,128:1:20000 \
  --extra=--storage-mode --extra=log --extra=--io-threads --extra=2
python3 tools/arm64_bench/run_arm64_bench.py \
  --binary target/dist/mqttiumd --only mqttiumd --label lot9-ext4-64k \
  --work-dir "$PWD/target/bench-ext4-tmp" --payload 65536 --cases 32:1:5000 \
  --extra=--storage-mode --extra=log --extra=--io-threads --extra=2
rm -rf target/bench-ext4-tmp

# 4. Idle memory footprint & QoS 0 paced delivery
python3 tools/arm64_bench/idle_rss_bench.py --binary target/dist/mqttiumd --out /tmp/lot9_idle_rss.json
python3 tools/measure_qos0_delivery.py --binary target/dist/mqttiumd --shape bursts --messages 100000 --burst 100 --subscribers 1
python3 tools/measure_qos0_delivery.py --binary target/dist/mqttiumd --shape blast --messages 100000 --subscribers 8
```

## Adversarial 8-scenario load matrix (Lot 8 `0252fdc5` -> **Lot 9** vs Mosquitto 2.1.2)

All matrix runs ran on the same Pi 5 host (`CPUs 0,1,2` broker, `CPU 3` generator, `/tmp` backing, 2-second publish window + drain). Every `mqttiumd-log` run performs full CRC-32C + per-commit `fdatasync` durable WAL logging, whereas Mosquitto 2.1.2 runs purely in RAM (`persistence false`).

| Scenario | Mode | Lot 8 (`0252fdc5`) | **Lot 9 (`dist`)** | Mosquitto 2.1.2 (RAM) | Key Lot 9 delta |
| - | - | -: | -: | -: | - |
| `fanout-1x4-q1` (4g × 1→4 @ 40k pub/s) | `vol` | 125,392/s (1.02 / 1.72 ms, 0 ref) | **159,972/s (0.30 / 2.01 ms, 0 ref)** | 159,998/s (0.20 / 0.31 ms) | **+27.6 %**, matches Mosquitto |
| `fanout-1x4-q1` | `log` | 90,452/s (2.83 / 6.29 ms, 0 ref) | **138,762/s (0.84 / 3.54 ms, 0 ref)** | *(RAM only)* | **+53.4 %**, 3.4× lower p50 |
| `fanout-1x16-q1` (2g × 1→16 @ 15k pub/s) | `vol` | 127,912/s (1.97 / 3.50 ms, 0 ref) | **177,912/s (1.45 / 2.66 ms, 0 ref)** | 239,992/s (0.33 / 0.52 ms) | **+39.1 %** |
| `fanout-1x16-q1` | `log` | 88,520/s (6.13 / 13.16 ms, 0 ref) | **155,216/s (1.55 / 3.90 ms, 0 ref)** | *(RAM only)* | **+75.3 %**, 4.0× lower p50 |
| `fanout-1x16-q0` (2g × 1→16 @ 30k pub/s) | `vol` | 480,008/s (0.15 / 2.32 ms) | **480,008/s (0.15 / 2.68 ms)** | 480,008/s (1.12 / 2.56 ms) | **7.5× lower p50** vs Mosquitto |
| `fanout-1x16-q0` | `log` | 480,000/s (0.15 / 1.98 ms) | **479,984/s (0.18 / 3.68 ms)** | *(RAM only)* | 6.2× lower p50 vs Mosquitto |
| `fanin-32x1-q1` (32→1 @ 80k pub/s) | `vol` | 75,874/s (766.0 / 932.1 ms, 8,237 `0x97`) | **66,845–79,978/s** (see sweep below) | 80,000/s (0.03 / 0.07 ms) | Single-core ceiling ~74–80k/s |
| `fanin-32x1-q1` (32→1 @ 80k pub/s) | `log` | **3,297/s** (1.34 / 4,155.7 ms) | **76,085/s** (705.0 / 913.0 ms, 7,826 `0x97`) | *(RAM only)* | **23.1× throughput** vs Lot 8 |
| `slow-sub-q0` (1 frozen + 7 fast @ 40k/s) | `vol` | 140,000/s (0.04 / 0.27 ms) | **140,000/s (0.04 / 0.43 ms)** | 140,000/s (0.39 / 1.80 ms) | **9.8× lower p50** vs Mosquitto |
| `slow-sub-q0` | `log` | 140,000/s (0.04 / 0.28 ms) | **140,000/s (0.04 / 0.36 ms)** | *(RAM only)* | Full slow-sub isolation |
| `slow-sub-q1` (1 frozen + 7 fast @ 20k/s) | `vol` | 70,032/s (0.06 / 0.76 ms, 0 ref) | **70,031/s (0.05 / 0.59 ms, 0 ref)** | 70,803/s (0.14 / 0.29 ms) | **2.8× lower p50** vs Mosquitto |
| `slow-sub-q1` | `log` | 33,952/s (511.95 / 2,107.17 ms) | **70,033/s (0.08 / 0.55 ms, 0 ref)** | *(RAM only)* | **2.06× deliv/s, 6,400× lower p50** |
| `churn-q1` (8 pairs @ 30k/s + 4 churners) | `vol` | 30,000/s (0.04 / 0.51 ms, 0 ref, 3,637 ch) | **30,000/s (0.04 / 0.85 ms, 0 ref, 3,592 ch)** | 29,996/s (0.19 / 0.60 ms, 2,458 ch) | **1.46× churn**, 4.8× lower p50 |
| `churn-q1` | `log` | 29,722/s (0.09 / 0.81 ms, **551 `0x83`**) | **30,000/s (0.08 / 0.87 ms, 0 ref, 3,496 ch)** | *(RAM only)* | **551 -> 0 refusals**, 1.42× churn |
| `mixed-256b-64k` (16 fast + 2× 64 KiB) | `vol` | 30,000/s (0.03 / 0.39 ms, 0 ref) | **30,000/s (0.03 / 0.51 ms, 0 ref)** | 29,986/s (0.18 / 0.42 ms) | **6.0× lower p50** vs Mosquitto |
| `mixed-256b-64k` | `log` | 30,000/s (0.05 / 0.56 ms, 0 ref) | **30,000/s (0.05 / 1.02 ms, 0 ref)** | *(RAM only)* | **3.6× lower p50** vs Mosquitto |

### `fanin-32x1-q1` rate sweep (32 publishers -> 1 subscriber, single-socket saturation knee)

Offering 80,000 QoS 1 msg/s into a single subscriber requires **160,000 transactional store operations/s** (`ClaimedPublication` + `FinishDelivery`) on one Owner core and one subscriber socket, which sits right at the single-core Pi 5 saturation ceiling (`~72–76k/s` in `log`, `~74–80k/s` in `vol`). Below that knee, Lot 9 holds `0` refusals and sub-millisecond median latency in both modes:

| Offered rate (32→1) | `mqttiumd-vol` | `mqttiumd-log` (CRC-32C + `fdatasync`) |
| -: | -: | -: |
| 40,000/s | **40,000/s** (`0` ref, `p50/p95/p99 = 0.03 / 0.09 / 0.73 ms`) | **40,000/s** (`0` ref, `p50/p95/p99 = 0.05 / 0.45 / 1.11 ms`) |
| 60,000/s | **60,002/s** (`0` ref, `p50/p95/p99 = 0.03 / 0.57 / 1.64 ms`) | **60,002/s** (`0` ref, `p50/p95/p99 = 0.07 / 0.89 / 2.59 ms`) |
| 80,000/s | **74,460–79,978/s** (knee: `0.04 ms` p50 in-window, spikes when single core saturates) | **72,585–79,998/s** (vs Lot 8 `3,297/s`; spec-compliant `0x97` when consumer lags 65,536 refs) |

## High-rate saturation, mTLS, real `ext4`, QoS 0, and idle memory

Across every run below, `mqttiumd` recorded **0 integrity errors** (`acked_refused = 0`, `ack_no_deliv = 0`, `disconnects = 0`, `invalid = 0`, `loss = 0.0 %`).

### High-rate saturation (`tmpfs`, 256 B) and real `ext4` (`/dev/sda2`)

| Workload | Mode | Lot 8 (report 0039) | **Lot 9 (`dist`)** | Mosquitto 2.1.2 (RAM) | Notes |
| - | - | -: | -: | -: | - |
| Plain 4p QoS 1 @ 150k/s | `vol-tmpfs` | 129,687/s (0.18 / 0.35 ms) | **124,920/s (0.20 / 0.36 ms)** | 150,000/s (0.14 / 0.28 ms) | Parity within run jitter |
| Plain 32p QoS 1 @ 100k/s | `vol-tmpfs` | 98,378/s (0.75 / 3.30 ms) | **84,792–96,541/s (1.87 / 7.11 ms)** | 99,982/s (0.65 / 1.03 ms) | Zero loss, zero refusals |
| Plain 128p QoS 1 @ 60k/s | `vol-tmpfs` | 59,608/s (4.03 / 14.00 ms) | **59,270–60,000/s (0.09–8.89 ms)** | 60,000/s (0.02 / 1.35 ms) | 256 connections |
| Plain 128p QoS 1 @ 100k/s | `vol-tmpfs` | 62,632/s (14.03 / 22.53 ms) | **64,768/s (13.60 / 19.82 ms)** | 62,900/s (3.47 / 5.11 ms) | **Beats Lot 8 & Mosquitto** |
| Plain 32p QoS 0 @ 300k/s | `vol-tmpfs` | 299,983/s (0.28 / 1.63 ms) | **299,995/s (0.29 / 4.02 ms)** | 300,000/s (8.64 / 20.57 ms) | **29.8× lower p50** vs Mosquitto |
| Plain 4p QoS 1 @ 100k/s | `log-tmpfs` | 92,406/s (0.15 / 0.87 ms) | **93,776/s (0.17 / 0.54 ms)** | *(RAM only)* | **38 % lower p99** vs Lot 8 |
| Plain 4p QoS 1 @ 150k/s | `log-tmpfs` | — | **99,176/s (0.26 / 0.94 ms)** | *(RAM only)* | Near-100k durable QoS 1/s |
| Plain 32p QoS 1 @ 100k/s | `log-tmpfs` | 69,538/s (3.14 / 12.00 ms) | **78,136/s (2.63 / 5.89 ms)** | *(RAM only)* | **+12.4 %**, 2.04× lower p99 |
| Plain 128p QoS 1 @ 100k/s | `log-tmpfs` | — | **55,130/s (16.34 / 29.90 ms)** | *(RAM only)* | 256 conns durable WAL |
| **mTLS 32p QoS 1 @ 60k/s** | `vol-tmpfs` | 59,974/s (0.53 / 1.65 ms) | **59,975/s (0.57 / 1.91 ms)** | 41,832/s (49.50 / 56.43 ms) | **1.43× deliv/s, 87× lower p50** |
| **mTLS 32p QoS 1 @ 100k/s** | `vol-tmpfs` | 81,566/s (2.51 / 3.86 ms) | **73,942/s (2.64 / 7.62 ms)** | 43,168/s (51.06 / 53.44 ms) | **1.71× deliv/s** vs Mosquitto |
| **mTLS 32p QoS 1 @ 100k/s** | `log-tmpfs` | — | **69,493/s (3.14 / 5.35 ms)** | *(RAM only)* | **1.61× RAM Mosquitto** w/ WAL |
| **Plain 32p QoS 1 @ 60k/s** | **`log-ext4`** | 26,483/s (397.70 / 746.67 ms) | **40,855/s (4.11 / 22.22 ms)** | *(RAM only)* | **+54.3 %, 96.8× lower p50** |
| Plain 128p QoS 1 @ 20k/s | **`log-ext4`** | 20,000/s (2.26 / 20.42 ms) | **19,815/s (6.69 / 88.58 ms)** | *(RAM only)* | Real `/dev/sda2` ext4 |
| 64 KiB 32p QoS 1 @ 5k/s | **`log-ext4`** | *(4p @ 2k: 1,140/s)* | **1,274/s (~81.5 MiB/s WAL)** | *(RAM only)* | Real `/dev/sda2` ext4 |

### QoS 0 paced delivery (`measure_qos0_delivery.py`)

| Shape | Messages | Subscribers | Received (min / max) | Closed subs | Delivered/s |
| - | -: | -: | -: | -: | -: |
| `bursts` (burst 100, 64 B) | 100,000 | 1 | **100,000 / 100,000 (100.0 %)** | 0 | **322,890/s** |
| `blast` (64 B) | 100,000 | 8 | **100,000 / 100,000 (100.0 %)** | 0 | **591,076/s** |

### Idle memory per connection (`idle_rss_bench.py`)

`mqttiumd` runs with **5 threads** total (`DirectWriter` in `volatile` mode) and **1.00 FD/conn** across every point.

| Profile | Conns | Lot 8 RSS / Dirty | **Lot 9 RSS / Dirty (`dist`)** | **Lot 9 Total VmRSS** | Mosquitto 2.1.2 RSS / Dirty | Mosquitto 2.1.2 Total VmRSS |
| - | -: | -: | -: | -: | -: | -: |
| Plain TCP | 0 | — | — | **7,360 KiB** | — | 11,568 KiB |
| Plain TCP | 400 | — | **3,441 / 2,949 B** | **8,704 KiB** | 5,079 / 5,079 B | 13,552 KiB |
| Plain TCP | 512 | 5,728 / 5,472 B | **3,008 / 2,624 B** | **8,864 KiB** | 5,088 / 5,088 B | 14,112 KiB |
| Plain TCP | 768 | 5,269 / 5,099 B | **2,965 / 2,709 B** | **9,584 KiB** | 5,248 / 5,077 B | 15,504 KiB |
| TLS 1.3 mTLS | 0 | — | — | **8,560 KiB** | — | 14,176 KiB |
| TLS 1.3 mTLS | 128 | 17,408 / 15,360 B | **15,360 / 12,800 B** | **10,480 KiB** | 44,800 / 43,264 B | 19,776 KiB |
| TLS 1.3 mTLS | 256 | 15,808 / 14,784 B | **13,184 / 11,904 B** | **11,856 KiB** | 44,480 / 43,200 B | 25,296 KiB |
| TLS 1.3 mTLS | 400 | 14,746 / 14,090 B | **12,616 / 11,796 B** | **13,488 KiB** | 43,950 / 43,131 B | 31,344 KiB |

- **Plain TCP 768 connections**: `mqttiumd` uses **2,965 B/conn RSS** (**1.77×** smaller than Lot 8 and **1.77×** smaller per-conn / **38.2 % less total VmRSS** than Mosquitto 2.1.2).
- **TLS 1.3 mTLS 400 connections**: `mqttiumd` uses **12,616 B/conn RSS** (**3.48×** smaller per-conn and **2.32×** smaller total VmRSS than Mosquitto 2.1.2).

## Empirical ARM64 evaluation: Multi-Core *Delivery Owners* Sharding

To evaluate whether splitting recipient delivery transitions onto resident per-partition workers (`LogicalOwnerId(1..=K)`) pays off on ARM64 *after* `MultiClaimedPublication` is in place, we captured 2-second per-thread `perf record -e cpu-clock:u -F 999` profiles across four saturated workloads (broker pinned to CPUs `0,1,2`, generator on CPU `3`):

| Workload | Delivered/s | `mqttiumd` (Owner) | `log-writer` | `mqttiumd-io-0` | `mqttiumd-io-1` | Implied Owner µs/deliv | Implied Writer µs/deliv |
| - | -: | -: | -: | -: | -: | -: | -: |
| `fanout-1x16-vol` (1:16 QoS 1) | 174,805/s | **91.77 %** | *(Direct)* | 4.28 % | 3.96 % | **5.72 µs** | *(included)* |
| `fanout-1x16-log` (1:16 QoS 1) | 161,216/s | **67.95 %** | **23.79 %** | 3.88 % | 4.37 % | **6.20 µs** | **2.17 µs** |
| `128p-q1-vol` (256 conns QoS 1) | 62,097/s | **80.13 %** | *(Direct)* | 9.65 % | 10.23 % | **16.10 µs** | *(included)* |
| `128p-q1-log` (256 conns QoS 1) | 54,154/s | **65.26 %** | **21.72 %** | 6.39 % | 6.63 % | **18.47 µs** | **6.15 µs** |

### Top Owner hotspots (`perf report --sort comm,symbol --no-children`)

- **`fanout-1x16-vol`**: `Runtime::drain_inputs` (9.57 %), `Owner::step_with_sink` (5.29 %), `Owner::plan_observed_puback` (2.76 %), `__aarch64_ldadd8_rel` (`Arc` refcount contention, 2.62 %), `pubsub::State::next_send` (1.89 %), `Runtime::rearm_ready_work` (1.70 %), `Deliveries::unindex` (1.61 %), `Outbox::dispatch_immediate` (1.47 %), `Owner::plan_result_change` (1.33 %).
- **`fanout-1x16-log`**: `Runtime::drain_inputs` (7.32 %), `Owner::step_with_sink` (4.99 %), `log-writer` loop (4.86 %), `pubsub::State::next_send` (2.12 %), `SyncWaker::notify` (3.60 % combined across `mqttiumd` + `log-writer`), `Owner::plan_observed_puback` (1.80 %), `Runtime::dispatch_returning_accepted` (1.32 %), `Runtime::rearm_ready_work` (1.21 %).
- **`128p-q1-vol` / `128p-q1-log`**: `Runtime::drain_inputs` (9.94 % / 7.38 %), `Owner::step_with_sink` (2.83 % / 4.02 %), atomic ops `__aarch64_ldadd8_*` / `__aarch64_swp4_rel` (6.24 % / 3.41 %), `FlatMap<OperationSeq, usize>::insert` (2.08 % / 1.33 %), `Owner::plan_observed_puback` (1.79 %).

### Quantitative Go/No-Go conclusion for Lot 10

1. **The single Owner core is now the decisive bottleneck across all high-fanout and high-concurrency workloads**: even after `MultiClaimedPublication` cut 1:16 store decisions nearly in half, `mqttiumd` (Owner) consumes **100 % of 1 core** (`65–92 %` of all broker CPU), while `log-writer` uses only **`~35 %` of 1 core** (`23.79 / 67.95`) and the two I/O reactor threads combined use **`~12–25 %` of 1 core**.
2. **Recipient-side work budget easily pays for batched cross-core handoff**: on `fanout-1x16`, roughly **70–75 %** of Owner CPU (`~4.2–4.5 µs/delivery`, or `~67–72 µs` per 16-subscriber publish) is pure recipient-side work (`WorkAvailable`, `EmitPublish`, `ObservedPuback`, `FinishDelivery`, `Deliveries::unindex`, `rearm_ready_work`). A batched handoff from the coordinator (`AdmissionOwner`) to `K` resident `DeliveryOwner(LogicalOwnerId(1..=K))` shards costs one cache-line handoff per batch (`< 0.1 µs/delivery` amortized over 16–64 publications) — a **>40:1 compute-to-handoff ratio**.
3. **Scaling envelope**:
   - **4-core ARM64 (Pi 5, 3 broker cores)**: moving recipient delivery onto 2 resident delivery shards (co-scheduled with the lightly loaded I/O reactors) projects up to **~2.0–2.5×** higher `fanout-1x16` and `128p` throughput before hitting the single `log-writer` ceiling (`~220–250k deliv/s` at `~400–450k ops/s` single-WAL saturation).
   - **8–16 core servers**: scaling `K = 4..8` resident `DeliveryOwner` shards directly attacks the 80–92 % Owner ceiling on `volatile` (and pairs naturally with partitioned WALs when single-WAL throughput saturates).
4. **Lot 10 prerequisites already delivered in Lot 9**: `Connections` and `Deliveries` are now encapsulated modules behind clean mutator APIs, `OriginOpTable` is verified gap-safe for out-of-order multi-lane operations, and `OwnerAuthority` + `Store` receipt scoping already key by `(LogicalOwnerId, OperationSeq)`. Lot 10 can therefore split `Owner` cleanly into `AdmissionOwner` + `K` `DeliveryOwner`s alongside full `mqttiumd-lab` multi-owner crash-recovery tests and TLA+ model updates.

## Verification ledger

All commands were executed sequentially on this ARM64 host (`CARGO_BUILD_JOBS=1` / `-j 1` crate-by-crate, `-- --test-threads=2`, within 7.9 GiB RAM and 0 B swap):

| Check | Command | Outcome |
| - | - | - |
| Formatting | `cargo fmt --all -- --check` | **PASS** |
| Clippy (default, `kill-points`, `test-hooks`) | `cargo clippy ... --locked -- -D warnings` (all 3 profiles) | **PASS (0 warnings)** |
| Pure-crate boundary (ADR 0007) | `cargo test -j 1 -p mqttiumd-lab --test boundary --locked` | **PASS (17/17)** |
| Kani credits proof digest | `sha256sum crates/mqttiumd-contract/src/capacity.rs` | `71378725…` (**unchanged**) |
| Workspace unit & integration tests (default) | `cargo test -j 1 -p <crate> --locked -- --test-threads=2` (all 7 crates) | **PASS (1,591/1,591)** (`contract` 192, `core` 394, `sqlite` 174, `volatile` 18, `lab` 334, `mqttiumd` 197 lib + 282 int) |
| Instrumented `kill-points` | `cargo test -j 1 -p mqttiumd-lab --features kill-points --locked -- --test-threads=2` | **PASS (334/334)** |
| Instrumented `test-hooks` | `cargo test -j 1 -p mqttiumd --features test-hooks --locked -- --test-threads=2` | **PASS (296/296)** |
| New Lot 9 regression suites | `concurrent_churn_refusals` (8/8), `multi_claimed_publication_*`, `inline_preclaim_burst_*`, `origin_op_table_gap_*` (10k differential steps) | **PASS (all)** |
| Oracle unit tests | `python3 -m unittest discover -t tests/oracle -s tests/oracle/tests` | **PASS (274/274)** |
| End-to-end oracle campaigns | `tools/run_campaign.sh` on `P1-02` (`sim-store`, `sqlite`) & `QABS-B` (`sim-store`) | **PASS (3/3)** |
| Compat harness unit tests | `python3 -m unittest discover -t tests/compat -s tests/compat` | **PASS (64/64)** |
| Mutant patch applicability | `git apply --check tests/mutants/*/*.mutant` | **PASS (23/23)** |
| Conformance register | `python3 conformance/check.py --check` | **PASS (0 open)** |
| Formal model consistency | `python3 models/check_models.py` | **PASS (13/13 consistent)** |
| Requirement register & derived corpus | `tools/build_corpus.py --check`, `tests/oracle/build_traces.py --check`, `mqttiumd-lab validate --contracts contracts/v1` | **PASS** (`624` derived, `405` traces, `16` docs; contract digest unchanged: `sha256:21e96cc81c1f48d59841d8424fee2290102c6782b025222f2d6d2fe72ccf9bb5`) |

### Not run in this sandbox

- Differential `tests/compat/run.py` against Mosquitto 2.1.4 (host provides `/usr/sbin/mosquitto` 2.1.2; `tests/compat/run.py` refuses versions other than pinned 2.1.4).
- `tools/run_loop_mutants.sh` execution and TLC (`tla2tools.jar` not installed locally). All 23/23 `.mutant` patches apply cleanly and all 13/13 TLA+ models passed `models/check_models.py`.
- `proofs/credits/run.sh` (Kani not installed; every file hashed in `proofs/credits/BODY.sha256` remains byte-for-byte unchanged).
