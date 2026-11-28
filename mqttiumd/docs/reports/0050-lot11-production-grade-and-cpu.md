# Lot 11 — Production-Grade Hardening, Formal Models, and ARM64 A/B Measurements

Branch `perf/lot11-production-grade-and-cpu`, cut from `6915fbf3`
(`perf/lot10-measurements`, code equivalent to `40c0e8e1` of report 0049).
This report documents the changes, formal verification, test verification,
and alternated ARM64 A/B measurements executed against `head` (`6915fbf3`)
and Mosquitto 2.1.2 (`persistence false`, volatile mode only).

## Scope and Commits

Nine commits on `perf/lot11-production-grade-and-cpu` above `6915fbf3`:

| Commit | Reserve / Workstream | Summary |
| - | - | - |
| `4be87e63` | Lot 11A (R2 — CPU/delivery) | `SeqMap` / `ScopedSeqMap` O(1) indexed maps, reusable `Owner::scratch_effects`, in-place stepping (`StepResult`), `ClaimSlots`, `InFlightVolatile`, direct inline QoS 0 delivery, `LaneSender`/`LaneReceiver`, `ConnCore::outgoing_pending`, packed `slot_gen`, reusable `Sink` scratch buffers |
| `7bfd3efe` | Lot 11B (R1 — Fan-in scan frontier) | `DirtyDuringLoad` (`DIRTY_SET_CAP = 16 > WORK_PAGE_LIMIT = 8`) preserves pagination cursor under monotonic fan-in while rewinding on lower commits / invalidations / overflow; TLA+ model `MScanFrontier` (`models/scan-frontier/`) |
| `fdd96f36` | Lot 11C-1 (R3 — Retained QoS 0 replay) | Pipelined `ConsumeDelivery` within `QOS0_CONSUMED_WINDOW = 16` via `next_pipelined_action`, grouping retained QoS 0 consume commits while preserving atomic retained snapshots (ADR 0032) |
| `06ab8c87` | Lot 11C-2 (R7 — TLS takeover barrier flake) | Reset `encoded.deadline` (`WRITE_TIMEOUT`) after `test-hooks` barriers and check `is_cancelled()` in `net/tls.rs`; permanent `takeover-k0-delayed-drain` scenario in `disconnect_durability` |
| `e1913ece` | Lot 11C-3 (ADR 0055 — LogStore v2) | `.mlog` v2 16-byte self-authenticating backward footer (`[body_crc][body_len][footer_crc][b"MLGE"]`, `FRAME_OVERHEAD = 20`) and backward recovery verification; Proposed ADR `0068` |
| `8393fe68` | Lot 11C-4 (R4 & R6 — Session open batching & idle RSS) | `Owner::lifecycle: Vec<Flight>` (`LIFECYCLE_IN_FLIGHT = 16`, `CONTROL_QUEUE = 32`), `fast_batch_step` on `OpenSession*`, `Signalled::try_send_quiet` + batched `wake_control()`, `Writer` `lifecycle_group_responses_into` (`apply_group`), TLA+ model `MSessionOpenBatch`, Proposed ADR `0069`; lazy `LeaseTable::slots` growth inside pre-allocated `BoundedVec` without editing any `BODY.sha256` file |
| `3762afb1` | Lot 11D (R5 — Sharded mode D9/D8/D7 + 3 models) | D9 O(1) `terminal_by_conn` in `finish_detaches()`; D8 per-lane writer drain without cross-owner head-of-line blocking; D7 full `ShardSink` parity (`frame_queued`, `frame_failed`, deduplicated `effect_retained`, oversize `SubmitBlock::Closed`, `TrySendError::Disconnected` close notice, `ConnCore::closing` fence, `outgoing_waiter` direct socket-drain wake); TLS read-without-credit unit test + real TCP/TLS bug fix (`progressed |= !scratch.incoming_batch.is_empty()` when promoting parked `unpaid_ack`); 3 TLA+ models (`MShardAckRoute`, `MShardDetach`, `MShardStop`) |
| `9e62e036` | Pure-crate ADR 0007 boundary | Remove unlisted `#[inline]` / `#[cold]` attributes from `mqttiumd-contract` and `mqttiumd-core` |
| `fe85e2ab` | Pure-crate ADR 0007 + Clippy | Drop derivable `impl Default` on `ClaimSlots` so both `boundary` (ADR 0007) and `clippy -D warnings` pass |

## Verification Summary

### Rust Workspace, Feature Matrix, and Static Checks

All executed at `fe85e2ab`:

- `cargo fmt --all --check`: **PASS**
- `cargo clippy --workspace --all-targets -- -D warnings`: **PASS (0 warnings)**
- `cargo clippy -p mqttiumd --features test-hooks --all-targets -- -D warnings`: **PASS (0 warnings)**
- `cargo clippy -p mqttiumd-store-sqlite --features kill-points --all-targets -- -D warnings`: **PASS (0 warnings)**
- `mqttiumd-lab --test boundary` (ADR 0007 pure-crate boundary): **17 / 17 PASS**
- `mqttiumd-contract`: **186 unit + 12 integration = 198 / 198 PASS**
- `mqttiumd-core`: **409 / 409 PASS**
- `mqttiumd-store-sqlite` (default + `kill-points`): **17 / 17 suites PASS**
- `mqttiumd-store-volatile` (unit + `conformance` 6/6 + `log_conformance` v2 18/18): **PASS**
- `mqttiumd-lab` (88 unit + 18 bench integration suites including `bench_rebuild`, `bench_group`, `bench_child`, `bench_order`, `bench_f3`): **PASS**
- `mqttiumd` (`--lib` 250 + `--bin` 4 + 14 integration test binaries including `delivery_shards` 11/11, `reactor_wake` 23/23, `writer_group_kill` 6/6, `concurrent_churn_refusals` 8/8, `disconnect_durability` 7/7): **PASS**
- `mqttiumd --features test-hooks`: `sharded_lease` **4 / 4 PASS**, `disconnect_durability` **15 / 15 PASS** (including `secure::takeover_with_a_partially_written_tls_record` and `takeover-k0-delayed-drain`)

### Proofs, Schemas, Conformance, and Formal Models (TLC)

- `proofs/credits/BODY.sha256` (`sha256sum -c`): **6 / 6 OK** (zero edits to Kani-proved files)
- `tools/build_corpus.py --check`: **637 derived files, 0 not up to date, 0 stale**
- `tools/check_schemas.py`: **661 documents, 0 disagreements**
- `conformance/check.py --check`: **PASS** (MQTT 5.0: 251 statements, 0 open; MQTT 3.1.1: 139 statements, 0 open)
- `models/check_models.py`: **20 model directories / 28 models consistent**
- `models/test_check_models.py`: **18 / 18 OK**
- `tools/test_perf_gate.py`: **12 / 12 OK**

Five new TLA+ models verified with OpenJDK 25.0.5 + `tla2tools-1.7.4.jar` (`tools/formal/run_tlc.py`):

| Model directory | Configuration | Expected | Result | Distinct states |
| - | - | - | - | -: |
| `models/scan-frontier` | `MScanFrontier-safety.cfg` | pass | pass | 50,495 |
| `models/scan-frontier` | `MScanFrontier-monotone-no-rewind.cfg` | pass | pass | 542 |
| `models/scan-frontier` | `MScanFrontier-76af3a5-rewind.cfg` | violation `MonotoneFanInProgress` | violation | 56 |
| `models/scan-frontier` | `MScanFrontier-lot9-lost-wakeup.cfg` | violation `NoInvisibleObligation` | violation | 103 |
| `models/scan-frontier` | `MScanFrontier-mut-no-floor-check.cfg` | violation `NoInvisibleObligation` | violation | 2,973 |
| `models/scan-frontier` | `MScanFrontier-mut-no-rescan-on-empty.cfg` | violation `NoInvisibleObligation` | violation | 103 |
| `models/scan-frontier` | `MScanFrontier-mut-advance-under-pending.cfg` | violation `KnownThroughSound` | violation | 66 |
| `models/session-open-batch` | `MSessionOpenBatch-safety.cfg` | pass | pass | 8,596 |
| `models/session-open-batch` | `MSessionOpenBatch-reach-batch.cfg` | violation `NeverBatched` | violation | 19 |
| `models/session-open-batch` | `MSessionOpenBatch-mut-same-client.cfg` | violation `AtMostOneFlightPerClient` | violation | 5 |
| `models/session-open-batch` | `MSessionOpenBatch-mut-no-capacity.cfg` | violation `NoUnexpectedBatchRefusal` | violation | 256 |
| `models/session-open-batch` | `MSessionOpenBatch-mut-exclusive-overlap.cfg` | violation `NoExclusiveOverlap` | violation | 125 |
| `models/shard-ack-route` | `MShardAckRoute-safety.cfg` | pass | pass | 399 |
| `models/shard-ack-route` | `MShardAckRoute-reach-nonack-zero-credit.cfg` | violation `NeverReadNonAckAtZeroCredit` | violation | 4 |
| `models/shard-ack-route` | `MShardAckRoute-mut-no-park.cfg` | violation `DisconnectNeverOvertakesAck` | violation | 19 |
| `models/shard-ack-route` | `MShardAckRoute-mut-early-credit.cfg` | violation `BudgetBounded` | violation | 5 |
| `models/shard-ack-route` | `MShardAckRoute-mut-double-write-free.cfg` | violation `BudgetBounded` | violation | 7 |
| `models/shard-detach` | `MShardDetach-safety.cfg` | pass | pass | 38 |
| `models/shard-detach` | `MShardDetach-mut-write-while-closing.cfg` | violation `NoWriteAfterClosing` | violation | 6 |
| `models/shard-detach` | `MShardDetach-mut-ignore-terminal.cfg` | violation `NoDetachWithPendingWork` | violation | 37 |
| `models/shard-detach` | `MShardDetach-mut-ignore-finish-inflight.cfg` | violation `NoDetachWithPendingWork` | violation | 37 |
| `models/shard-stop` | `MShardStop-safety.cfg` | pass | pass | 74 |
| `models/shard-stop` | `MShardStop-mut-lane-stop.cfg` | violation `StopNeverLost` | violation | 8 |
| `models/shard-stop` | `MShardStop-mut-drop-inflight.cfg` | violation `CleanExitDrainedAll` | violation | 20 |
| `models/shard-stop` | `MShardStop-mut-silent-panic.cfg` | violation `PanicAlwaysReported` | violation | 4 |

All **25 / 25** TLC configurations matched their declared outcome.

## ARM64 Measurement Method

- **Host.** Raspberry Pi 5 (4-core ARM64 Cortex-A76, 7.9 GiB RAM, 0 swap),
  Linux `6.18.60+rpt-rpi-2712` aarch64, `rustc 1.97.0 (2d8144b78 2026-07-07)`,
  `gcc (Debian 16.2.0-4) 16.2.0`. All benchmarks executed sequentially on an
  otherwise idle machine with tmpfs `/tmp` (`--work-dir /tmp`).
- **Binaries (`--profile dist`).**
  - `head` (`6915fbf3` / `40c0e8e1`): sha256 `8480bb63677fe4e841c573db8176352f8b869d91d2313949c8eb586a1cc4b842`
  - `lot11` (`fe85e2ab`): sha256 `2a3d62da5f2e1266af7cf0c680623683237019ab97c1fa8aebf8be8f161a64b3`
- **Reference broker.** System `/usr/sbin/mosquitto` version `2.1.2` (sha256
  `997040b66e543ad316e5c799ca1abb169b9cd7716723b5662d99c726313f717d`), running
  with `persistence false`. Compared **only** against `--storage-mode volatile`
  and idle RSS; `log` mode is compared solely against `mqttiumd` itself.
- **CPU pinning.** Broker pinned to CPUs `0,1,2` (`--io-threads 2` in pairs
  and matrix suites, default in `run_perf_gate.py`); generator pinned to CPU
  `3`. Every A/B series alternates target execution order across even/odd
  passes; tables report medians and `[min..max]` ranges.

## 1. Performance Gate (`run_perf_gate.py`, 6 Alternated Passes)

Command:

```bash
python3 mqttiumd/tools/run_perf_gate.py /tmp/mqttiumd-head mqttiumd/target/dist/mqttiumd \
  --passes 6 --broker-cpus 0,1,2 --client-cpus 3
```

Gate criterion: median `B / A` of broker CPU per delivery `<= 1.05` on the four
gated shapes (`A = head`, `B = lot11`).

| Shape | `head` rate (/s) | `lot11` rate (/s) | Rate B/A | `head` CPU (µs/deliv) | `lot11` CPU (µs/deliv) | Median B/A CPU | Verdict |
| - | -: | -: | -: | -: | -: | -: | - |
| `qos0 bursts x1` | 349,867 | 483,256 | 1.381× | 3.833 | 3.100 | **0.808** | **pass** |
| `qos0 bursts x8` | 582,432 | 685,382 | 1.177× | 2.292 | 2.175 | **0.966** | **pass** |
| `qos1 pairs x1` | 71,226 | 71,654 | 1.006× | 26.416 | 26.483 | **1.004** | **pass** |
| `qos1 pairs x8` | 71,727 | 72,150 | 1.006× | 25.875 | 25.344 | **0.981** | **pass** |
| `qos0 blast x1` | 377,694 | 592,442 | 1.569× | 3.500 | 2.800 | **0.800** | reported |
| `qos0 blast x8` | 574,586 | 686,406 | 1.195× | 2.046 | 1.892 | **0.926** | reported |

All four gated shapes pass; QoS 0 throughput increases by **+17.7 % to +56.9 %**
while cutting broker CPU/delivery by up to **20.0 %**.

## 2. Pairs — Volatile vs `head` & Mosquitto 2.1.2 (4 Alternated Passes)

Command:

```bash
python3 mqttiumd/tools/arm64_bench/run_ab_campaign.py --driver pairs --mode volatile \
  --work-dir /tmp --passes 4 --io-threads 2 \
  --target head=/tmp/mqttiumd-head \
  --target lot11=mqttiumd/target/dist/mqttiumd \
  --target mosquitto=/usr/sbin/mosquitto \
  --cases 4:1:300000,32:1:200000,128:1:150000,32:0:300000 --out /tmp/lot11_final_pairs_vol.jsonl
```

Zero refusals and 0.0 % loss across all 48 runs.

| Case | Target | Median deliv/s `[min..max]` | p50 (ms) | p99 (ms) | Broker CPU µs/deliv `[min..max]` |
| - | - | -: | -: | -: | -: |
| `4:1:300000` | `head` | 119,811 `[118,266..120,866]` | 0.23 | 0.40 | 16.04 `[15.73..16.08]` |
| `4:1:300000` | **`lot11`** | **131,090** `[123,285..133,922]` (+9.4 %) | **0.21** | **0.37** | **15.22** `[14.90..15.41]` (-5.1 %) |
| `4:1:300000` | `mosquitto 2.1.2` | 170,522 `[170,274..171,067]` | 1.40 | 1.78 | 5.87 `[5.85..5.87]` |
| `32:1:200000` | `head` | 89,020 `[87,488..92,072]` | 2.51 | 3.70 | 19.94 `[19.39..20.30]` |
| `32:1:200000` | **`lot11`** | **99,400** `[94,641..100,960]` (+11.7 %) | **2.20** | **3.34** | **18.77** `[18.56..19.07]` (-5.8 %) |
| `32:1:200000` | `mosquitto 2.1.2` | 158,084 `[156,642..158,995]` | 11.67 | 14.44 | 6.36 `[6.34..6.42]` |
| `128:1:150000` | `head` | 61,395 `[54,522..62,724]` | 14.66 | 23.42 | 28.97 `[28.91..30.17]` |
| `128:1:150000` | **`lot11`** | **70,100** `[69,940..70,700]` (+14.2 %) | **12.92** | **18.43** | **26.76** `[26.24..27.07]` (-7.6 %) |
| `128:1:150000` | `mosquitto 2.1.2` | 137,558 `[135,677..138,860]` | 52.39 | 63.45 | 7.53 `[7.45..7.63]` |
| `32:0:300000` | `head` | 299,984 `[299,980..299,998]` | 0.30 | 2.27 | 8.87 `[8.58..9.07]` |
| `32:0:300000` | **`lot11`** | 299,984 `[299,981..299,998]` | **0.17** | **1.69** | **7.92** `[7.48..8.38]` (-10.7 %) |
| `32:0:300000` | `mosquitto 2.1.2` | 300,000 `[300,000..300,000]` | 9.82 | 23.39 | 3.33 `[3.33..3.33]` |

**Honest R2 gap note:** Lot 11 cuts broker CPU/delivery by 5.1–10.7 % across
all pairs shapes and improves throughput by +9.4 % to +14.2 % while holding
3.4×–58× lower latency than Mosquitto 2.1.2, yet `mqttiumd` still uses
**2.4×–3.6×** Mosquitto's broker CPU per delivery in volatile mode (due to
multi-thread handoffs, canonical recipe/store indirection, and durable
state-machine bookkeeping). Further reductions remain open to callgrind-guided
profiling (R2).

## 3. Pairs — Log Mode on `/tmp` tmpfs (4 Alternated Passes)

Command:

```bash
python3 mqttiumd/tools/arm64_bench/run_ab_campaign.py --driver pairs --mode log \
  --work-dir /tmp --passes 4 --io-threads 2 \
  --target head=/tmp/mqttiumd-head \
  --target lot11=mqttiumd/target/dist/mqttiumd \
  --cases 4:1:300000,32:1:200000,128:1:150000 --out /tmp/lot11_final_pairs_log.jsonl
```

Zero refusals and 0.0 % loss across all 24 runs. Note that `lot11` commits
`.mlog` v2 frames (16-byte backward footer, ADR 0068) while still outperforming
`head`'s v1 frames on every metric:

| Case | Target | Median deliv/s `[min..max]` | p50 (ms) | p99 (ms) `[min..max]` | Broker CPU µs/deliv |
| - | - | -: | -: | -: | -: |
| `4:1:300000` | `head` | 100,936 `[85,713..101,144]` | 0.28 | 0.92 `[0.62..303.89]` | 22.76 |
| `4:1:300000` | **`lot11`** | **107,800** `[106,756..108,985]` (+6.8 %) | **0.26** | **0.58** `[0.56..0.61]` | **21.87** (-3.9 %) |
| `32:1:200000` | `head` | 76,389 `[73,570..76,950]` | 2.87 | 49.23 `[12.45..147.08]` | 27.92 |
| `32:1:200000` | **`lot11`** | **87,162** `[86,410..88,316]` (+14.1 %) | **2.48** | **4.45** `[4.39..4.65]` (÷11.1) | **25.70** (-7.9 %) |
| `128:1:150000` | `head` | 55,116 `[54,652..55,504]` | 16.81 | 26.40 `[24.11..27.99]` | 38.15 |
| `128:1:150000` | **`lot11`** | **59,578** `[56,250..60,172]` (+8.1 %) | **15.40** | **23.96** `[22.34..32.94]` | **36.44** (-4.5 %) |

## 4. Matrix — Log Fan-in (R1) and Sharded Fan-out (R5) (4 Alternated Passes)

Command:

```bash
python3 mqttiumd/tools/arm64_bench/run_ab_campaign.py --driver matrix --mode log \
  --work-dir /tmp --passes 4 --io-threads 2 \
  --target head=/tmp/mqttiumd-head \
  --target head-k2=/tmp/mqttiumd-head,--delivery-shards,2 \
  --target lot11=mqttiumd/target/dist/mqttiumd \
  --target lot11-k2=mqttiumd/target/dist/mqttiumd,--delivery-shards,2 \
  --target lot11-k3=mqttiumd/target/dist/mqttiumd,--delivery-shards,3 \
  --cases fanin-32x1-q1@60000,fanin-32x1-q1@70000,fanin-32x1-q1@80000,fanout-1x16-q1@240000 \
  --out /tmp/lot11_final_matrix_log.jsonl
```

| Case | Target | Median deliv/s `[min..max]` | p50 (ms) | p99 (ms) | Refused `[min..max]` | Loss % | Broker CPU µs/deliv |
| - | - | -: | -: | -: | -: | -: | -: |
| `fanin-32x1-q1@60000` | `head` (`K=0`) | 60,002 `[59,999..60,002]` | 0.07 | 1.26 `[1.19..587.87]` | 0 `[0..0]` | 0.0 | 39.17 |
| `fanin-32x1-q1@60000` | `head-k2` | 60,002 `[60,002..60,002]` | 0.12 | 2.12 | 0 `[0..0]` | 0.0 | 40.71 |
| `fanin-32x1-q1@60000` | **`lot11` (`K=0`)** | 60,002 `[60,002..60,002]` | **0.06** | **1.11** `[1.05..13.39]` | 0 `[0..0]` | 0.0 | **38.88** |
| `fanin-32x1-q1@60000` | `lot11-k2` | 60,002 `[60,002..60,002]` | 0.10 | 1.53 | 0 `[0..0]` | 0.0 | 40.83 |
| `fanin-32x1-q1@60000` | `lot11-k3` | 60,002 `[59,968..60,002]` | 0.10 | 1.13 | 0 `[0..0]` | 0.0 | 40.71 |
| `fanin-32x1-q1@70000` | `head` (`K=0`) | 70,004 `[67,610..70,004]` | 464.48 | 749.64 | 0 `[0..4,786]` | 0.0 `[0..3.42]` | 38.61 |
| `fanin-32x1-q1@70000` | `head-k2` | 70,004 `[70,000..70,004]` | 0.74 | 228.59 | 0 `[0..0]` | 0.0 | 37.53 |
| `fanin-32x1-q1@70000` | **`lot11` (`K=0`)** | 70,003 `[66,428..70,004]` | 264.21 | 453.88 | **0 `[0..0]`** | **0.0** | **35.54** (-8.0 %) |
| `fanin-32x1-q1@70000` | **`lot11-k2`** | 70,004 `[70,002..70,004]` | **0.13** | **1.66** | 0 `[0..0]` | 0.0 | 36.00 |
| `fanin-32x1-q1@70000` | `lot11-k3` | 70,004 `[70,003..70,004]` | 0.14 | 27.22 | 0 `[0..0]` | 0.0 | 36.18 |
| `fanin-32x1-q1@80000` | `head` (`K=0`) | 68,493 `[66,520..80,000]` | 843.72 | 1,102.95 | 23,013 `[0..26,961]` | 14.39 | 39.79 |
| `fanin-32x1-q1@80000` | `head-k2` | 78,203 `[70,964..80,000]` | 142.97 | 690.29 | 0 `[0..18,069]` | 0.0 `[0..11.29]` | 36.75 |
| `fanin-32x1-q1@80000` | **`lot11` (`K=0`)** | **79,999** `[79,661..79,999]` (+16.8 %) | 432.59 | 687.05 | **0** `[0..678]` | **0.0** `[0..0.42]` | **33.75** (-15.2 %) |
| `fanin-32x1-q1@80000` | `lot11-k2` | 79,991 `[77,852..79,998]` | 138.10 | 352.69 | 0 `[0..4,294]` | 0.0 `[0..2.68]` | 34.22 |
| `fanin-32x1-q1@80000` | `lot11-k3` | 79,994 `[70,789..79,999]` | 224.24 | 382.28 | 0 `[0..18,411]` | 0.0 `[0..11.51]` | 34.44 |
| `fanout-1x16-q1@240000` | `head` (`K=0`) | 160,400 `[158,560..161,232]` | 1.57 | 2.81 | 0 `[0..0]` | 0.0 | 11.41 |
| `fanout-1x16-q1@240000` | `head-k2` | 187,296 `[161,864..193,008]` | 2.84 | 1,256.05 | 2,258 `[0..10,007]` | 8.17 | 16.96 |
| `fanout-1x16-q1@240000` | `lot11` (`K=0`) | 172,088 `[168,256..172,536]` (+7.3 %) | 1.46 | 2.63 | 0 `[0..0]` | 0.0 | 10.96 |
| `fanout-1x16-q1@240000` | **`lot11-k2`** | **239,376** `[195,472..246,208]` (+27.8 % vs `head-k2`) | **1.42** | **5.98** (÷210 vs `head-k2`) | **0** `[0..2,443]` | **0.0** `[0..9.09]` | **11.32** (-33.3 % vs `head-k2`) |
| `fanout-1x16-q1@240000` | **`lot11-k3`** | **244,404** `[242,960..248,304]` (+52.4 % vs `head` `K=0`) | **1.19** | **3.17** | **0 `[0..0]`** | **0.0** | **10.84** (-5.0 % vs `head` `K=0`) |

**Key findings for R1 and R5:**
- **R1 closed:** Default `K = 0` recovers full 80,000/s median fan-in throughput
  in `log` mode (`68.5k -> 80.0k`, median refusals `23,013 -> 0`, `-15.2 %` CPU/delivery),
  and eliminates the `588 ms` p99 spike seen on `head` at 60,000/s.
- **R5 hardened & dramatically improved on wide fan-out:** Fixing D8
  (cross-owner writer head-of-line blocking), D9 (quadratic detach scans), D7
  (direct socket-drain shard wake instead of 1 ms polling), and Lot 11A/11B
  turns `K = 2` on `fanout-1x16-q1@240000` from a collapsing 1,256 ms p99 /
  8.17 % loss / 16.96 µs/deliv into 239.4k/s / 5.98 ms p99 / 0 median refused /
  11.32 µs/deliv (1 of 4 passes still saw 2,443 refusals). `K = 3` sustains
  **244.4k/s** across all 4/4 passes with **0 refused**, **1.19 ms p50**,
  **3.17 ms p99**, and **10.84 µs/deliv** (lower per-delivery CPU than `K = 0`).

## 5. Idle Memory / RSS (R6 — `idle_rss_bench.py`)

Measured with `mqttiumd/tools/arm64_bench/idle_rss_bench.py` (`--delivery-shards 0`
and `--delivery-shards 2`).

| Config | Target | Conns | `VmRSS` (kB) | `RssAnon` (kB) | `Private_Dirty` (kB) | Bytes/conn (dirty) |
| - | - | -: | -: | -: | -: | -: |
| Plain TCP (`K=0`) | `head` | 0 | 7,520 | 2,944 | 5,888 | — |
| Plain TCP (`K=0`) | **`lot11`** | 0 | **5,552** (-26.2 %) | **896** (-69.6 %) | **896** (-84.8 %) | — |
| Plain TCP (`K=0`) | `mosquitto 2.1.2` | 0 | 11,392 | 2,704 | 2,704 | — |
| Plain TCP (`K=0`) | `head` | 768 | 9,824 | 5,056 | 8,192 | 3,072 |
| Plain TCP (`K=0`) | **`lot11`** | 768 | **7,920** (-19.4 %) | **3,008** (-40.5 %) | **3,008** (-63.3 %) | **2,816** |
| Plain TCP (`K=0`) | `mosquitto 2.1.2` | 768 | 15,200 | 6,512 | 6,512 | 5,077 |
| mTLS (`K=0`) | `head` | 0 | 8,672 | 3,648 | 7,040 | — |
| mTLS (`K=0`) | **`lot11`** | 0 | **6,784** (-21.8 %) | **1,616** (-55.7 %) | **1,616** (-77.0 %) | — |
| mTLS (`K=0`) | `mosquitto 2.1.2` | 0 | 13,968 | 3,312 | 3,312 | — |
| mTLS (`K=0`) | `head` | 400 | 13,632 | 8,288 | 12,000 | 12,698 |
| mTLS (`K=0`) | **`lot11`** | 400 | **11,776** (-13.6 %) | **6,288** (-24.1 %) | **6,288** (-47.6 %) | **11,960** |
| mTLS (`K=0`) | `mosquitto 2.1.2` | 400 | 31,136 | 20,160 | 20,160 | 43,131 |
| Plain TCP (`K=2`) | `head-k2` | 0 | 15,456 | 10,864 | 13,808 | — |
| Plain TCP (`K=2`) | **`lot11-k2`** | 0 | **6,080** (-60.7 %) | **1,440** (-86.7 %) | **1,440** (-89.6 %) | — |
| Plain TCP (`K=2`) | `head-k2` | 512 | 17,616 | 12,832 | 15,968 | 4,320 |
| Plain TCP (`K=2`) | **`lot11-k2`** | 512 | **8,336** (-52.7 %) | **3,440** (-73.2 %) | **3,440** (-78.5 %) | **4,000** |
| mTLS (`K=2`) | `head-k2` | 128 | 18,720 | 13,360 | 17,072 | 16,768 |
| mTLS (`K=2`) | **`lot11-k2`** | 128 | **9,472** (-49.4 %) | **3,984** (-70.2 %) | **3,984** (-76.7 %) | **14,464** |

Lazily growing `LeaseTable::slots` inside its pre-allocated `BoundedVec` plus
Lot 11A's compact structures removes **~2.0 MiB** of anonymous RSS at `K = 0`
and **~9.4 MiB** at `K = 2` (three `Owner` instances), bringing idle `K = 0`
`VmRSS` to **5.4 MiB** (less than half Mosquitto 2.1.2's 11.1 MiB) and `K = 2`
to **5.9 MiB** (only +528 kB above `K = 0`, compared with +7.9 MiB before).

## 6. Direct Commit-Count A/B for R4 (Session Opens) and R3 (Retained QoS 0)

Six alternated passes each in `--storage-mode log` on `/tmp` (`--io-threads 2`):

| Workload | Target | Median elapsed (ms) `[min..max]` | Rate | Median `.mlog` frames `[min..max]` |
| - | - | -: | -: | -: |
| **R4**: 128 concurrent distinct MQTT 5 opens | `head` | 38.527 `[35.107..39.640]` | 3,322 opens/s | 128.0 `[128..128]` |
| **R4**: 128 concurrent distinct MQTT 5 opens | **`lot11`** | **5.857** `[5.783..6.547]` (÷6.58) | **21,853 opens/s** | **8.0** `[8..8]` (÷16.0) |
| **R3**: 16 retained QoS 0 replay on SUBSCRIBE | `head` | 0.495 `[0.423..0.972]` | — | 17.0 `[17..17]` |
| **R3**: 16 retained QoS 0 replay on SUBSCRIBE | **`lot11`** | **0.391** `[0.327..0.690]` (-21 %) | — | **3.0** `[3..5]` (÷5.67) |

On a disk where each `fdatasync` costs tens of milliseconds, collapsing 128
serial open commits into 8 batched frames (16× reduction) and 17 serial
retained-replay commits into 3 frames (5.7× reduction) directly removes the
serial fsync amplification recorded in R4 and R3.

## Remaining Limits and Open Decisions

1. **Owner decisions required:** Acceptance or revision of Proposed ADRs `0066`
   (sharded mode), `0067` (operational event details), `0068` (`.mlog` v2
   backward footer), and `0069` (concurrent distinct session open batching),
   plus the strategic decision on whether to keep and promote `--delivery-shards`
   out of "experimental" given both x86-64 (report 0049) and ARM64 (this report).
2. **Remaining R2 CPU gap vs Mosquitto:** Volatile `mqttiumd` uses 2.4×–3.6×
   Mosquitto 2.1.2's CPU/delivery on ARM64 pairs; closing more of that gap
   requires callgrind/perf-guided work beyond structural table optimisations.
3. **Unreproduced flake (R7):** `a_shard_that_panics_is_reported_to_the_coordinator`
   was never reproduced; its panic-reporting protocol is now formally checked
   in `MShardStop`, but the historical single local failure on PR #684 has no
   reproduced root cause.
4. **Deferred items (unchanged):** R9 (`drain_inputs` / `Sink::try_submit`
   split with per-arm instruction counts), slow-disk (80 ms `fsync`) and
   non-loopback cross-host reception, oracle coverage of `LogStore` /
   `VolatileStore` / sharded mode, `MTakeoverInFlight` connect-beside-ended-binding,
   and physical power-loss reception.
