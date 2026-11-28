# Lot 11 Hand-off & Lot 12 Start — Herdr 3-Agent Team Guide

Hand-over after Lot 11 (`perf/lot11-production-grade-and-cpu`, report
[`0050`](../reports/0050-lot11-production-grade-and-cpu.md)) for the three-agent
Herdr workflow (`$TEAM-architect`, `$TEAM-neon`, `$TEAM-reviewer`). Read this
file, then [`../../AGENTS.md`](../../AGENTS.md), [`../reports/0050-lot11-production-grade-and-cpu.md`](../reports/0050-lot11-production-grade-and-cpu.md),
and [`../reception/open-conditions.md`](../reception/open-conditions.md).

## 1. Repository & Branch State

| Branch | Head | Remote / PR | State |
| - | - | - | - |
| `refactor/lot10-validated-base` | `40c0e8e` | PR #684 | Lot 10 validation & fixes; awaiting owner review |
| `perf/lot10-measurements` | `6915fbf3` | PR #685 (draft) | Report 0049, measurement tooling, `L11-optimisation.md`; broker binary identical to `40c0e8e` |
| **`perf/lot11-production-grade-and-cpu`** | `5f4ea58d` (+ this hand-off) | Pushed to `origin` | Stacked on `6915fbf3`; 10 atomic verified commits + Report `0050` + updated `open-conditions.md` |

### Lot 11 Commit Chain (above `6915fbf3`)

1. `4be87e63` — **Lot 11A (R2)**: `SeqMap`/`ScopedSeqMap`, reusable `Owner::scratch_effects`, in-place stepping (`StepResult`), `ClaimSlots`, `InFlightVolatile`, direct inline QoS 0 delivery, `LaneSender`/`LaneReceiver`, `ConnCore::outgoing_pending`, packed `slot_gen`, reusable `Sink` scratch buffers.
2. `7bfd3efe` — **Lot 11B (R1)**: `DirtyDuringLoad` (`DIRTY_SET_CAP = 16 > WORK_PAGE_LIMIT = 8`) + TLA+ model [`MScanFrontier`](../../models/scan-frontier/README.md) (7/7 TLC configs).
3. `fdd96f36` — **Lot 11C-1 (R3)**: pipelined `ConsumeDelivery` within `QOS0_CONSUMED_WINDOW = 16`.
4. `06ab8c87` — **Lot 11C-2 (R7)**: `net/tls.rs` `encoded.deadline` reset after `test-hooks` barriers + `is_cancelled()` + permanent `takeover-k0-delayed-drain` test.
5. `e1913ece` — **Lot 11C-3 (ADR 0055)**: `.mlog` v2 16-byte self-authenticating backward footer (`FRAME_OVERHEAD = 20`) + Proposed ADR [`0068`](../decisions/0068-logstore-v2-backward-readable-footer.md).
6. `8393fe68` — **Lot 11C-4 (R4 & R6)**: concurrent distinct-client session open batching (`LIFECYCLE_IN_FLIGHT = 16`, `CONTROL_QUEUE = 32`, `try_send_quiet` + batched `wake_control()`, `apply_group`), TLA+ model [`MSessionOpenBatch`](../../models/session-open-batch/README.md) (5/5 TLC configs), Proposed ADR [`0069`](../decisions/0069-concurrent-distinct-session-open-batching.md); lazy `LeaseTable::slots` growth inside pre-allocated `BoundedVec` without modifying any `BODY.sha256` file.
7. `3762afb1` — **Lot 11D (R5)**: D9 O(1) `terminal_by_conn`, D8 per-lane writer drain without cross-owner head-of-line blocking, D7 full `ShardSink` parity + `ConnCore::closing` fence + `outgoing_waiter` direct socket-drain wake, TLS read-without-credit test + real TCP/TLS `unpaid_ack` promotion `progressed` bug fix (`net.rs`, `net/tls.rs`), and 3 TLA+ models ([`MShardAckRoute`](../../models/shard-ack-route/README.md), [`MShardDetach`](../../models/shard-detach/README.md), [`MShardStop`](../../models/shard-stop/README.md), 13/13 TLC configs).
8. `9e62e036` & `fe85e2ab` — Pure-crate ADR 0007 boundary compliance (`#[inline]`/`#[cold]`/`#[default]` cleanup & `ClaimSlots` `Default` removal so both `mqttiumd-lab --test boundary` and `clippy -D warnings` pass).
9. `5f4ea58d` — Updated [`open-conditions.md`](../reception/open-conditions.md) and added [`0050-lot11-production-grade-and-cpu.md`](../reports/0050-lot11-production-grade-and-cpu.md).

## 2. Status of L11 Reserves (R1–R10)

| Reserve | Status after Lot 11 | Evidence & Remaining Work |
| - | - | - |
| **R1** (fan-in cost of `76af3a5`) | **Closed** | `DirtyDuringLoad` + `MScanFrontier` (7/7 TLC); ARM64 `fanin-32x1-q1@80k` (`log`, `K=0`): `68.5k → 80.0k/s`, median refused `23,013 → 0`, `-15.2 %` CPU/deliv |
| **R2** (CPU/delivery vs Mosquitto) | **Improved, still open** | Perf gate PASS (`qos0 bursts x1: 0.808`, `qos0 blast x1: 0.800`, `qos1 pairs x8: 0.981`); pairs `+9.4–14.2 %` throughput, `-5.1–10.7 %` CPU; volatile `mqttiumd` still uses `2.4×–3.6×` Mosquitto 2.1.2's CPU/deliv → **primary Lot 12 target under callgrind/perf** |
| **R3** (retained QoS 0 serial commits) | **Closed** | 16 retained QoS 0 replay in `log` mode: `17 → 3 [3..5]` `.mlog` frames (÷5.7) |
| **R4** (serial session opens) | **Closed** (ADR `0069` Proposed) | `MSessionOpenBatch` (5/5 TLC); 128 concurrent opens in `log` mode: `38.53 ms / 128 frames → 5.86 ms / 8 frames` (6.58× faster, 16× fewer commits); slow-disk reception remains open |
| **R5** (sharded mode hardening) | **Hardened, owner decision open** | D7, D8, D9, TLS credit test, and all 3 missing shard models closed (13/13 TLC); ARM64 `fanout-1x16-q1@240k` (`log`): `K=2` p99 `1,256 ms → 5.98 ms`, `K=3` sustains `244.4k/s` with `0` refused and `10.84 µs/deliv` (< `K=0`). Still open: owner decision on promoting `--delivery-shards` & accepting ADR `0066`; out-of-order store answers under core capacity exhaustion |
| **R6** (idle memory / RSS) | **Substantially improved** | Plain `0 conns`: `K=0` `7.5 → 5.6 MB` (`RssAnon 2.9 → 0.9 MB`, < ½ Mosquitto 2.1.2); `K=2` `15.5 → 6.1 MB` (`-9.4 MB`, `RssAnon 10.9 → 1.4 MB`) |
| **R7** (two intermittent tests) | **Half closed** | `takeover_with_a_partially_written_tls_record` root-caused and fixed (`06ab8c87`); `a_shard_that_panics_is_reported_to_the_coordinator` still unreproduced (protocol modelled in `MShardStop`) |
| **R8** (measurement limits) | **ARM64 A/B added, limits open** | Alternated ARM64 A/B added in Report `0050`; slow-disk (`80 ms` fsync) and cross-host non-loopback receptions remain open |
| **R9** (split `drain_inputs` / `Sink::try_submit`) | **Deferred (unchanged)** | Must be done arm-by-arm with callgrind instruction counts (pairs naturally with R2 profiling in Lot 12) |
| **R10** (work order) | **Updated** | Reflected in `open-conditions.md` |
| **ADR 0055** (backward-readable `.mlog`) | **Closed** (ADR `0068` Proposed) | `.mlog` v2 16-byte footer + backward verification; physical power-loss reception remains open |

## 3. Recommended First Wave for the Herdr Team

### Phase 1 — Independent Audit & Reception of Lot 11 (Read-Only + Decisions)

1. **`$TEAM-reviewer` (Codex)**:
   - Audit `git diff 6915fbf3..5f4ea58d` focus areas:
     - `7bfd3efe` (`connections.rs` `DirtyDuringLoad` frontier vs `MScanFrontier`),
     - `e1913ece` (`log.rs` v2 backward footer parsing, torn-tail vs mid-file corruption boundary),
     - `8393fe68` (`owner.rs`/`writer.rs`/`leases.rs`: concurrent open batching conflicts/exclusives, `LeaseTable` lazy slots vs `BODY.sha256`),
     - `3762afb1` (`net.rs`/`net/tls.rs` `unpaid_ack` `progressed` fix, `ShardSink` parity, D8 writer drain, D9 `terminal_by_conn`, 3 new TLA+ models).
   - Report ONLY actionable defects / edge cases with concrete reproduction ideas.
2. **`$TEAM-architect` (Claude)**:
   - Synthesize Reviewer's findings with Proposed ADRs `0066`, `0067`, `0068`, `0069` and Reports `0049` (x86-64) + `0050` (ARM64).
   - Prepare concise decision checklist for the repository owner (merge order for #684 / #685 / Lot 11, ADR acceptance, sharded mode promotion).
   - Specify Lot 12's exact scope and performance targets before implementation starts.
3. **`$TEAM-neon` (`agy` / Gemini)**:
   - Address any defects found by Reviewer **test-first** (failing test commit, then fix), re-run targeted checks + `boundary` + perf gate, then execute Lot 12.

### Phase 2 — Lot 12 Engineering Priorities (After Lot 11 Audit)

1. **R2 + R9 (CPU/delivery & instruction-counted refactor)**: Profile `mqttiumd` volatile QoS 1 pairs (`4`/`32` pairs) under `callgrind`/`valgrind` (`tools/count_broker_instructions.sh`) to identify the top remaining instruction sinks vs Mosquitto, optimize measurably under the perf gate, and split `drain_inputs` / `Sink::try_submit` arm-by-arm with instruction evidence.
2. **Medium open conditions**:
   - `MTakeoverInFlight`: model + replay for connect-beside-ended-binding (`OldClosedBeforeNewBound`).
   - Slow-disk (`80 ms` simulated fsync) reception for R3 retained replay and R4 session-open bursts.
   - Oracle coverage for `LogStore` / `VolatileStore` / sharded mode (if prioritized by Architect).
   - Sharded mode: out-of-order store answers under core capacity exhaustion (if sharded mode is kept).
3. **Low open conditions**: `VolatileStore` `OriginOpTable` ring retention, recovery-page full-inventory scan, expiry sweep at session bound.

## 4. Herdr Team Operating Rules on This Host

This machine is a **Raspberry Pi 5 (4 ARM64 Cortex-A76 cores, 7.9 GiB RAM, 0 swap)**.
`~/.local/bin/herdr-team` starts all three agents in the **same working tree** (`$ROOT`), so follow strict turn-taking:

1. **Single writer on the working tree**: Only `$TEAM-neon` edits Rust code and runs `git commit` (unless Architect explicitly edits docs/ADRs/models during a paused hand-off, or you use `herdr worktree` to isolate branches). `$TEAM-reviewer` stays strictly read-only.
2. **Single Cargo / benchmark runner at a time**: Never run two `cargo build`, `cargo test`, or TLC runs concurrently on 7.9 GiB / 0 swap. Always use:
   - `cargo clippy -j 1 ...` / `cargo test -j 1 ... -- --test-threads=2`
   - `cargo build -j 2 --profile dist -p mqttiumd --bin mqttiumd` (alone)
   - Keep the machine **100 % idle** during `run_perf_gate.py`, `run_ab_campaign.py`, and `idle_rss_bench.py` (always pass `--work-dir /tmp` so tmpfs is used).
3. **Hard invariants (never regress)**:
   - Pure-crate ADR 0007 boundary: run `cargo test -j 1 -p mqttiumd-lab --test boundary` after ANY edit to `mqttiumd-contract` or `mqttiumd-core` (no `#[inline]`, `#[cold]`, `#[default]`, `#[allow(...)]`, or `unsafe`).
   - Kani proof integrity: never touch the 6 files pinned by `mqttiumd/proofs/credits/BODY.sha256`.
   - Test-first fixes, perf gate `median B/A <= 1.05`, English-only maintained docs, never push without explicit owner consent.
   - Offline TLC jar is cached at `/home/pi/.cache/mqttium/formal/tla2tools-1.7.4.jar` (OpenJDK 25 installed):
     `python3 tools/formal/run_tlc.py --models-dir mqttiumd/models/<dir>`
