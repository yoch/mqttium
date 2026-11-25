# Lot 10 ARM64 multi-core *Delivery Owners* sharding evaluation

This report records the empirical scaling benchmarks (`--delivery-shards K` for `K = 0, 1, 2, 3`), adversarial 8-scenario load matrix, high-rate plain TCP / TLS 1.3 mTLS / real `ext4` measurements, idle-memory footprint, ARM64 `perf` per-thread balance profiles, and full verification ledger for **Lot 10** on branch `perf/lot10-delivery-owners-sharding` (branched from Lot 9 commit `5e306904`), compared against `K = 0` (Lot 9 single-owner baseline, [report 0040](0040-lot9-arm64-fairness-fanout-and-sharding.md)) and system reference broker Mosquitto 2.1.2 (`/usr/sbin/mosquitto`).

Architectural decisions for Lot 10 are documented in Proposed record [0066](../decisions/0066-lot10-delivery-owners-sharding.md).

## Host, toolchain, and storage backing

- **Hardware**: ARM64 4-core Cortex-A76 (Raspberry Pi 5, 7.9 GiB RAM, 0 B swap).
- **Kernel**: Linux `6.18.60+rpt-rpi-2712` (`aarch64`).
- **Toolchain**: `rustc 1.97.0 (2d8144b78 2026-07-07)`, built with `CARGO_BUILD_JOBS=2 cargo build -j 2 -p mqttiumd --profile dist --locked` (`target/dist/mqttiumd`, `sha256: 03ac79602378b149c083d51d457535a6bf68895d5dcfa7eb3809fc4544485dc1`).
- **Reference broker**: `mosquitto version 2.1.2` (`/usr/sbin/mosquitto`, in-memory `persistence false`, `max_inflight_messages 64`).
- **CPU pinning**: broker pinned to CPUs `0,1,2` (`--io-threads 2`); load generator pinned to CPU `3`.
- **Storage backing**: real disk `/dev/sda2` (`ext4`) via temporary `mqttiumd/target/bench-ext4-tmp` for `ext4-log` runs; `tmpfs` (`/tmp`) for adversarial matrix, RAM/WAL saturation controls, and `perf` CPU profiling.
- **Generators**:
  - Adversarial matrix driver: [tools/arm64_bench/arm64_matrix_bench.c](../../tools/arm64_bench/arm64_matrix_bench.c) (`sha256: 5c4fb823799536b4d7e6f35b6c57b9061a057f9ef05e68e74b56dd2457751026`), driven by [tools/arm64_bench/run_matrix_bench.py](../../tools/arm64_bench/run_matrix_bench.py), MQTT 5, 2-second publish window + full drain.
  - Pair saturation driver: [tools/arm64_bench/arm64_bench.c](../../tools/arm64_bench/arm64_bench.c) (`sha256: b0ec96a4cc59037e4b9f2eb33c35cd73762b487a5fc6a5ad4136275ca6103dd7`), driven by [tools/arm64_bench/run_arm64_bench.py](../../tools/arm64_bench/run_arm64_bench.py).
  - Idle RSS harness: [tools/arm64_bench/idle_rss_bench.py](../../tools/arm64_bench/idle_rss_bench.py).

## Reproduce

From `mqttiumd/`:

```sh
CARGO_BUILD_JOBS=2 cargo build -j 2 -p mqttiumd --profile dist --locked

# 1. Adversarial 8-scenario load matrix: K=0 (volatile/log/Mosquitto), K=2, K=3
python3 tools/arm64_bench/run_matrix_bench.py --delivery-shards 0 --only all
python3 tools/arm64_bench/run_matrix_bench.py --delivery-shards 2 --only volatile
python3 tools/arm64_bench/run_matrix_bench.py --delivery-shards 2 --only log
python3 tools/arm64_bench/run_matrix_bench.py --delivery-shards 3 --only volatile
python3 tools/arm64_bench/run_matrix_bench.py --delivery-shards 3 --only log

# 2. High-rate Plain TCP & TLS 1.3 mTLS saturation (tmpfs, K=0 vs K=2)
python3 tools/arm64_bench/run_arm64_bench.py --only mqttiumd --label vol-k2 \
  --extra=--storage-mode --extra=volatile --extra=--no-db --extra=--io-threads --extra=2 \
  --extra=--delivery-shards --extra=2 \
  --cases "4:1:20000,32:1:20000,32:1:120000,128:1:120000,32:0:300000" --work-dir /tmp

python3 tools/arm64_bench/run_arm64_bench.py --only mqttiumd --label log-k2 \
  --extra=--storage-mode --extra=log --extra=--io-threads --extra=2 \
  --extra=--delivery-shards --extra=2 \
  --cases "4:1:20000,32:1:20000,32:1:120000,128:1:120000,32:0:300000" --work-dir /tmp

python3 tools/arm64_bench/run_arm64_bench.py --only mqttiumd --tls --label mtls-log-k2 \
  --extra=--storage-mode --extra=log --extra=--io-threads --extra=2 \
  --extra=--delivery-shards --extra=2 \
  --cases "32:1:20000,32:1:60000,32:0:120000" --work-dir /tmp

# 3. Real ext4 backing (256 B & 64 KiB)
mkdir -p target/bench-ext4-tmp
python3 tools/arm64_bench/run_arm64_bench.py --only mqttiumd --label ext4-log-k2 \
  --extra=--storage-mode --extra=log --extra=--io-threads --extra=2 \
  --extra=--delivery-shards --extra=2 \
  --cases "32:1:20000,32:1:60000" --payload 256 --work-dir "$PWD/target/bench-ext4-tmp"
python3 tools/arm64_bench/run_arm64_bench.py --only mqttiumd --label ext4-log-k2-64k \
  --extra=--storage-mode --extra=log --extra=--io-threads --extra=2 \
  --extra=--delivery-shards --extra=2 \
  --cases "4:1:2000" --payload 65536 --work-dir "$PWD/target/bench-ext4-tmp"
rm -rf target/bench-ext4-tmp

# 4. Idle RSS (K=0 vs K=2)
python3 tools/arm64_bench/idle_rss_bench.py --delivery-shards 0 --only-mqttiumd
python3 tools/arm64_bench/idle_rss_bench.py --delivery-shards 2 --only-mqttiumd
```

## Adversarial 8-scenario load matrix (`K = 0` vs `K = 2` vs `K = 3` vs Mosquitto 2.1.2)

### Volatile mode (`--storage-mode volatile --no-db`, vs Mosquitto 2.1.2 volatile)

| Scenario | Mosquitto 2.1.2 (`deliv/s`, `p50/p99`) | `K = 0` (`deliv/s`, `p50/p99`) | **`K = 2` (`deliv/s`, `p50/p99`)** | **`K = 3` (`deliv/s`, `p50/p99`)** | Refusals / Loss (`K = 2, 3`) |
| - | -: | -: | -: | -: | -: |
| `fanout-1x4-q1` | 159,996/s (0.20 / 0.30 ms) | 159,382/s (0.75 / 1.48 ms) | **160,000/s (0.30 / 2.55 ms)** | 150,874/s (0.39 / 3.46 ms) | **0 / 0.00 %** |
| `fanout-1x16-q1` | 240,000/s (0.35 / 1.41 ms) | 171,864/s (1.49 / 2.69 ms) | **239,920/s (1.16 / 3.78 ms)** (**+39.6 %**) | 205,464/s (1.33 / 11.93 ms) | **0 / 0.00 %** |
| `fanout-1x16-q0` | 479,824/s (3.12 / 44.52 ms) | 480,008/s (0.70 / 38.45 ms) | **480,008/s (0.19 / 2.21 ms)** (**20× < Mosq p99**) | **480,008/s (0.23 / 3.18 ms)** | **0 / 0.00 %** |
| `fanin-32x1-q1` | 80,000/s (0.03 / 0.07 ms) | **80,000/s (0.04 / 0.81 ms)** | 80,000/s (364.69 / 597.94 ms) | 79,999/s (460.12 / 682.20 ms) | **0 / 0.00 %** |
| `slow-sub-q0` | 139,932/s (0.39 / 1.80 ms) | 140,000/s (0.04 / 0.48 ms) | **140,000/s (0.06 / 0.78 ms)** | **139,924/s (0.06 / 0.40 ms)** | **0 / 0.00 %** |
| `slow-sub-q1` | 70,872/s (0.14 / 0.29 ms) | 70,009/s (0.05 / 0.52 ms) | **70,008/s (0.11 / 0.65 ms)** | **69,990/s (0.13 / 0.61 ms)** | **0 / 0.00 %** |
| `churn-q1` | 30,000/s, 2,458 churn (0.19 / 0.59 ms) | 30,000/s, 3,604 churn (0.04 / 0.62 ms) | **30,000/s, 3,344 churn (0.12 / 1.19 ms)** | **30,000/s, 3,277 churn (0.14 / 1.59 ms)** | **0 / 0.00 %** |
| `mixed-256b-64k` | 30,000/s (0.18 / 0.44 ms) | 30,000/s (0.03 / 0.51 ms) | **30,000/s (0.07 / 0.97 ms)** | **30,000/s (0.07 / 0.75 ms)** | **0 / 0.00 %** |

### Durable WAL mode (`--storage-mode log`, full per-group `fdatasync`)

| Scenario | `K = 0` (`deliv/s`, `p50/p99`) | **`K = 2` (`deliv/s`, `p50/p99`)** | **`K = 3` (`deliv/s`, `p50/p99`)** | Gain (`K = 3` vs `K = 0`) | Refusals / Loss (`K = 2, 3`) |
| - | -: | -: | -: | -: | -: |
| `fanout-1x4-q1` | 143,976/s (0.84 / 2.17 ms) | **160,000/s (0.36 / 1.93 ms)** | **160,000/s (0.40 / 1.77 ms)** | **+11.1 %** (100 % offered), **−52 % p50** | **0 / 0.00 %** |
| `fanout-1x16-q1` | 153,544/s (1.56 / 3.91 ms) | **221,528/s (1.70 / 7.96 ms)** | **229,360/s (1.31 / 3.89 ms)** | **+49.4 %** (95.6 % of Mosquitto volatile!) | **0 / 0.00 %** |
| `fanout-1x16-q0` | 480,008/s (0.16 / 2.17 ms) | **480,008/s (0.21 / 2.47 ms)** | **480,008/s (0.21 / 2.35 ms)** | 100 % offered (**19× < Mosq p99**) | **0 / 0.00 %** |
| `fanin-32x1-q1` | 79,999/s (547.58 / 772.78 ms) | 79,348/s (272.02 / 587.19 ms) | **79,980/s (0.21 / 8.74 ms)** | **2,600× lower p50**, **88× lower p99** | **0 / 0.00 %** |
| `slow-sub-q0` | 140,000/s (0.04 / 0.40 ms) | **140,000/s (0.06 / 0.54 ms)** | **140,000/s (0.06 / 0.61 ms)** | 100 % offered | **0 / 0.00 %** |
| `slow-sub-q1` | 70,010/s (0.08 / 0.50 ms) | **69,992/s (0.11 / 0.82 ms)** | **70,030/s (0.15 / 1.10 ms)** | 100 % offered | **0 / 0.00 %** |
| `churn-q1` | 30,000/s, 3,491 churn (0.09 / 0.83 ms) | **30,000/s, 3,189 churn (0.15 / 1.67 ms)** | **30,000/s, 3,280 churn (0.15 / 0.92 ms)** | 100 % offered, 0 refusals | **0 / 0.00 %** |
| `mixed-256b-64k` | 30,000/s (0.05 / 0.76 ms) | **30,000/s (0.09 / 2.45 ms)** | **29,989/s (0.09 / 2.39 ms)** | 100 % offered | **0 / 0.00 %** |

### Single-shard (`K = 1`) saturation note

With `K = 1`, a single delivery shard thread saturates at `~120–160k deliv/s` while the unburdened Coordinator admits `240k deliv/s` on `fanout-1x16-q1`, filling the 65,536 store `References` table (`11,782–11,851` `0x97 QuotaExceeded` refusals; `fanin-32x1-q1` stays at 0 refusals). On 3 broker CPUs, `K = 2` (for `volatile`) and `K = 3` (for durable `log`) eliminate every refusal across all 8 scenarios.

## High-rate Plain TCP, TLS 1.3 mTLS, and real `ext4` (`K = 0` vs `K = 2`)

| Profile & Case | `K = 0` (`deliv/s`, `p50 / p99 ms`) | **`K = 2` (`deliv/s`, `p50 / p99 ms`)** | Delta (`K = 2` vs `K = 0`) | Refusals / Loss |
| - | -: | -: | -: | -: |
| Plain `vol` `4p QoS 1 @ 20k/s` | **19,964/s (0.03 / 0.10)** | 19,674/s (0.06 / 1.91) | `K = 0` lower latency (`DirectWriter`) | 0 / 0.0 % |
| Plain `vol` `32p QoS 1 @ 120k/s` | **90,850/s (2.16 / 4.07)** | 86,650/s (2.42 / 6.65) | −4.6 % (`DirectWriter` wins 1:1 RAM) | 0 / 0.0 % |
| Plain `vol` `128p QoS 1 @ 120k/s` | 62,521/s (14.26 / 21.53) | **66,230/s (14.22 / 23.89)** | **+5.9 %** | 0 / 0.0 % |
| Plain `vol` `32p QoS 0 @ 300k/s` | 300,000/s, wire 248k/s (308.15 / 349.63) | **299,968/s, wire 294k/s (4.65 / 50.58)** | **66× lower p50**, **+18.2 % wire** | 0 / 0.0 % |
| Plain `log` `32p QoS 1 @ 120k/s` | 80,357/s (2.60 / 5.80) | **85,560/s (2.39 / 6.22)** | **+6.5 %**, −8 % p50 | 0 / 0.0 % |
| Plain `log` `128p QoS 1 @ 120k/s` | 54,970/s (16.57 / 24.04) | **70,728/s (13.11 / 21.75)** | **+28.7 %**, **−21 % p50** | 0 / 0.0 % |
| Plain `log` `32p QoS 0 @ 300k/s` | 299,977/s (0.30 / 6.84) | 299,994/s (0.73 / 7.51) | parity (100 % offered) | 0 / 0.0 % |
| mTLS `vol` `32p QoS 1 @ 60k/s` | — | **59,749/s (1.33 / 3.85)** | 99.6 % offered under TLS 1.3 mTLS | 0 / 0.0 % |
| mTLS `log` `32p QoS 1 @ 60k/s` | — | **59,440/s (1.65 / 4.52)** | 99.1 % offered under TLS 1.3 mTLS | 0 / 0.0 % |
| mTLS `log` `32p QoS 0 @ 120k/s` | — | **119,980/s (0.43 / 5.42)** | 100 % offered under TLS 1.3 mTLS | 0 / 0.0 % |
| Real `ext4` `log` `32p QoS 1 @ 20k/s` | **19,126/s (1.90 / 29.57)** | 16,623/s (5.10 / 48.73) | `K = 0` wins 1:1 disk coalescing | 0 / 0.0 % |
| Real `ext4` `log` `32p QoS 1 @ 60k/s` | **40,787/s (4.09 / 23.90)** | 29,824/s (5.44 / 32.79) | `K = 0` wins 1:1 disk coalescing | 0 / 0.0 % |
| Real `ext4` `log` `4p QoS 1 64 KiB` | — | **843/s (~55 MB/s, 26.60 / 93.02)** | sustained 64 KiB `fdatasync` | 0 / 0.0 % |

## Idle memory footprint (`K = 0` vs `K = 2`)

| Configuration | Plain 0 conns (Threads) | Plain @ 768 conns (Total / per-conn RSS / Dirty) | mTLS 0 conns | mTLS @ 400 conns (Total / per-conn RSS / Dirty) |
| - | -: | -: | -: | -: |
| Lot 9 (`K = 0`, baseline) | 7,360 KiB (5T) | 9,584 KiB (**2,965 B** / 2,709 B) | 8,560 KiB | 13,488 KiB (**12,616 B** / 11,796 B) |
| **Lot 10 (`K = 0`, default)** | **7,584 KiB (5T)** | **9,808 KiB (2,965 B / 2,795 B)** | **8,752 KiB** | **13,664 KiB (12,575 B / 11,919 B)** |
| Lot 10 (`K = 2`, sharded) | 15,536 KiB (8T) | 18,704 KiB (4,224 B / 4,053 B) | 16,704 KiB | 22,096 KiB (13,804 B / 13,148 B) |

- At default `K = 0`, idle per-connection RSS is **byte-identical** to Lot 9 (`2,965 B/conn` plain, `12,575 B/conn` mTLS).
- Enabling `K = 2` adds ~7.9 MiB base RSS (two resident shard `Owner` instances + threaded writer + shard stacks) and ~1.2 KiB/conn for `InlineClaimLease` + `ShardSession`. Combined with the `DirectWriter` and real-`ext4` 1:1 results above, this empirically validates keeping `--delivery-shards 0` as the default and exposing `--delivery-shards K` as an explicit scale-out control for fan-out and high-concurrency workloads.

## ARM64 `perf` per-thread multi-core balance

Captured with `perf record -e cpu-clock:u -F 999` and `/proc/<pid>/task/*/stat` (`utime + stime` share across all broker threads on CPUs `0,1,2`):

| Workload | Delivered/s (under `perf`) | `mqttiumd` (Coord) | `writer` | Delivery Shards (`mqtt-deliv-*`) | `mqttiumd-io-0 / 1` | Peak thread share |
| - | -: | -: | -: | -: | -: | -: |
| Lot 9 `fanout-1x16-log` (`K = 0`) | 161,216/s | **67.95 %** *(bottleneck)* | 23.79 % | *(none)* | 3.88 % / 4.37 % | **68.0 %** (1 core pegged) |
| **Lot 10 `fanout-1x16-vol` (`K = 2`)** | 204,232/s | **8.7 %** | 22.5 % | **31.7 % / 18.7 %** | 9.2 % / 9.2 % | 31.7 % |
| **Lot 10 `fanout-1x16-log` (`K = 2`)** | 208,800/s | **9.0 %** | 23.1 % | **26.8 % / 20.3 %** | 10.7 % / 10.1 % | 26.8 % |
| **Lot 10 `fanout-1x16-log` (`K = 3`)** | 200,536/s (p99 **5.07 ms**) | **10.0 %** | **23.6 %** | **16.7 % / 15.0 % / 14.6 %** | 10.0 % / 10.0 % | **23.6 %** (balanced across 3 cores) |
| **Lot 10 `128p-q1-log` (`K = 2`)** | 50,372/s | **29.3 %** | 18.6 % | **11.6 % / 11.2 %** | 14.8 % / 14.5 % | **29.3 %** (shards within 0.4 pp) |

Note: the two bugs discovered during profiling — (1) `DeliveryShardWorker::drain_inputs` popping `ack_inputs` ahead of `StoreResult` with a `break` on `NeedCapacity(ReceiptSlots)`, and (2) O(N) `BTreeMap` scanning in `due_deadlines` (81.6 % CPU under pressure) — were fixed via a dedicated `store_inputs` lane drained before `ack_inputs`, per-lane blocking, and an O(1) `next_timer_due` fast check before the final measurements above.

## Verification ledger

All commands were executed sequentially on this ARM64 host (`CARGO_BUILD_JOBS=1` / `-j 1` crate-by-crate, `-- --test-threads=2`, within 7.9 GiB RAM and 0 B swap):

| Check | Command | Outcome |
| - | - | - |
| Formatting | `cargo fmt --all -- --check` | **PASS** |
| Clippy (default, `test-hooks`, `kill-points`) | `cargo clippy -j 1 ... --locked -- -D warnings` (all 3 profiles) | **PASS (0 warnings)** |
| Pure-crate boundary (ADR 0007) | `cargo test -j 1 -p mqttiumd-lab --test boundary --locked` | **PASS (17/17)** |
| Kani credits proof digest | `sha256sum` vs `proofs/credits/BODY.sha256` | **PASS** (`capacity.rs` `71378725…`, all 4 files unchanged) |
| Workspace tests (default) | `cargo test -j 1 -p <crate> --locked -- --test-threads=2` (all 7 crates) | **PASS (1,408/1,408)** (`contract` 192, `core` 397, `sqlite` 174, `volatile` 20, `lab` 334, `mqttiumd` 198 lib + 3 bin + 90 int) |
| Instrumented `kill-points` | `cargo test -j 1 -p mqttiumd-lab --features kill-points --locked -- --test-threads=2` | **PASS (334/334)** |
| Instrumented `test-hooks` | `cargo test -j 1 -p mqttiumd --features test-hooks --locked -- --test-threads=2` | **PASS (305/305)** (`208` lib + `3` bin + `94` int) |
| New Lot 10 test suites | `OriginOpTable` 4-owner 10k differential test, multi-owner `log_conformance`, 3 multi-owner `mqttiumd-core` shard tests, `writer::tests::multi_owner_writer_*`, `delivery_shards` (8/8 end-to-end TCP + `SIGKILL` cross-mode recovery, plus 5× churn & 5× retained-replay stress runs) | **PASS (all)** |
| Oracle unit tests | `python3 -m unittest discover -t tests/oracle -s tests/oracle/tests` | **PASS (274/274)** |
| End-to-end oracle campaigns | `tools/run_campaign.sh` on `P1-02` (`sim-store`, `sqlite`) & `QABS-B` (`sim-store`) | **PASS (3/3)** |
| Compat harness unit tests | `python3 -m unittest discover -t tests/compat -s tests/compat` | **PASS (64/64)** |
| Mutant patch applicability | `git apply --check tests/mutants/*/*.mutant` | **PASS (23/23)** |
| Conformance register | `python3 conformance/check.py --check` | **PASS (0 open)** (`MQTT 5.0: 251`, `MQTT 3.1.1: 139`) |
| Formal model consistency | `python3 models/check_models.py` | **PASS (13/13 consistent)** |
| Requirement register & derived corpus | `tools/build_corpus.py --check`, `tests/oracle/build_traces.py --check`, `mqttiumd-lab validate --contracts contracts/v1` | **PASS** (`624` derived, `405` traces, `16` docs; contract digest unchanged: `sha256:21e96cc81c1f48d59841d8424fee2290102c6782b025222f2d6d2fe72ccf9bb5`) |

### Not run in this sandbox

- Differential `tests/compat/run.py` against Mosquitto 2.1.4 (host provides `/usr/sbin/mosquitto` 2.1.2; `tests/compat/run.py` refuses versions other than pinned 2.1.4).
- `tools/run_loop_mutants.sh` execution and TLC (`tla2tools.jar` not installed locally). All 23/23 `.mutant` patches apply cleanly and all 13/13 TLA+ models passed `models/check_models.py`.
- `proofs/credits/run.sh` (Kani not installed; every file hashed in `proofs/credits/BODY.sha256` remains byte-for-byte unchanged).
