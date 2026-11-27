# Lot 11: optimisation — start here

Hand-over for the agent that takes up the optimisation of mqttiumd after
the validation of lot 10 (reports 0043 to 0049). It says where the work
stands, what the measured baseline is, the rules the owner set, and every
reservation the validation left, each with its evidence, the code it
concerns, what would close it and the trap to avoid. Read it, then
`../../AGENTS.md`, then the rows of `../reception/open-conditions.md` it
names.

## Where things stand (27 November 2026)

| Branch | PR | State |
| - | - | - |
| `perf/lot10-delivery-owners-sharding` (`9dfd568`) | — | Lot 10 as delivered; the base of the next two |
| `refactor/lot10-validated-base` (`40c0e8e`) | yoch/mqttium#684 | The validation of lot 10, reports 0043 to 0048, the owner's decisions of 27 November applied; ready for review, not merged |
| `perf/lot10-measurements` | yoch/mqttium#685 | Report 0049, the measurement tooling, this note; draft, stacked on #684 |

Start from the head of `perf/lot10-measurements`, or from `main` once
these are merged. Nothing in #685 changes the broker: its executable is
the one of `40c0e8e`.

## Rules the owner set

- **No change may cost performance under the gate** of report 0043
  (`tools/run_perf_gate.py`): dist builds, broker on CPUs 0–1 and clients
  on 2–3, A/B alternated, median of B/A of the broker's CPU per delivery
  over 6 passes; above 1.05 replay over 12 and that decides; between 1.03
  and 1.05 the instruction count decides. A group of changes that costs is
  withdrawn, not adjusted.
- **Correctness first.** A defect is fixed test first (a commit whose test
  fails, then the fix); a contract change needs a decision record; the
  owner accepts records, never the agent; a test is never weakened to make
  a change pass. The full list is in `../../AGENTS.md` (load-bearing rules,
  pure crates and their macro list, the files `capacity.rs`, `credits.rs`,
  `ids.rs` and `leases.rs` that a proof pins).
- **Evidence.** Every performance claim gives its commands, its revision,
  its statistic over passes and what was not measured (report 0049 is the
  model). Raw results and binaries stay out of the repository.

## The measured baseline (report 0049, x86-64 VM, head `40c0e8e`)

| Measure | Value |
| - | - |
| Instructions, QoS 0 bursts ×1 / ×8, QoS 1 pairs ×1 / ×8 | 280.9 M / 703.9 M / 800.2 M / 1,254.8 M |
| QoS 1 pairs at saturation, volatile, 4 / 32 / 128 pairs | 137k / 148k / 89k deliveries/s |
| Broker CPU per delivery there | 14.7 / 13.5 / 24.0 µs; Mosquitto 2.1.4: 5.7 / 5.7 / 7.6 µs |
| Fan-out 1→16 QoS 1 ceiling, K = 0 | 280k/s volatile, 211k/s log; Mosquitto 415k/s at 2.3 µs |
| Fan-in 32→1 QoS 1, log mode | keeps up to 60,000–70,000/s (lot 9: 70,000–80,000) |
| Idle memory | 8.6 MB at rest, 2,760 B per plain connection, 11,880 B per mTLS connection |

Commands, from `mqttiumd/`:

```bash
cargo build -p mqttiumd --profile dist --locked
python3 tools/run_perf_gate.py A/mqttiumd B/mqttiumd --passes 6
bash tools/count_broker_instructions.sh --shape qos1-pairs --clients 8 B/mqttiumd 1 4000
python3 tools/arm64_bench/run_ab_campaign.py --driver matrix --mode log --work-dir /dev/shm \
  --target a=A/mqttiumd --target b=B/mqttiumd --cases fanin-32x1-q1@70000 --out runs.jsonl
python3 tools/arm64_bench/run_ab_campaign.py --summary runs.jsonl
```

The binaries and raw JSON of report 0049 were not kept: rebuild the
revisions to compare. The machine has `valgrind` and `callgrind_annotate`
but no `perf`.

## Reservations

### R1. The fan-in pays for a correctness fix

- **What.** In log mode the fan-in of the default path keeps up to
  60,000–70,000 publications per second instead of 70,000–80,000 in lot 9,
  at 18 % more CPU per delivery at 70,000/s.
- **Evidence.** Report 0049, "Lot 10 at K = 0" and "Cause"; `git bisect`
  names `76af3a5`, confirmed over 12 alternated passes against its parent
  `285a4f8`.
- **Code.** `crates/mqttiumd-core/src/owner/connections.rs`:
  `Connections::complete_page` keeps a rescan requested during a work read
  (`rescan_needed |= discard`) and drops the read's cursor;
  `mark_all_dirty` clears the scan frontiers; `clear_known_through`.
  `crates/mqttiumd-core/src/owner/pubsub.rs`, `apply_pubsub`: the inline
  claim is skipped while `rescan_needed`.
- **What would close it.** A rescan that resumes at the first obligation
  made visible during the read instead of the start, or that keeps the
  inline claim when it is provably safe; measured with the fan-in sweep
  above. Which part of the cost comes from the restarts and which from the
  skipped claims was not instrumented: count them first.
- **Trap.** `76af3a5` fixes a lost wake-up; the test in
  `owner/pubsub/tests.rs` that asserts "a lower obligation remains
  invisible" guards it. Do not restore the lot 9 behaviour. A new scan
  frontier is a protocol change: model it (TLA+, as `models/inline-lease`)
  before the code.

### R2. CPU per delivery is 2.4 to 3.5 times Mosquitto's

- **What.** The largest room: every shape spends 2.4 to 3.5 times
  Mosquitto's CPU per delivery (report 0049, "Against Mosquitto"), while
  mqttiumd keeps a far lower latency under load.
- **Where to start.** Profile with callgrind on the gate shapes
  (`valgrind --tool=callgrind --separate-threads=yes`, then
  `callgrind_annotate`); report 0040 (ARM64, older code) put 68–92 % of the
  time in the owner thread and 22–24 % in the writer in log mode: a hypothesis
  to check, not a fact.
- **Trap.** A gain on one shape that costs on another fails the gate; the
  x8 shapes are noisy (up to 9 % between runs), decide with instruction
  counts when the median falls between 1.03 and 1.05.

### R3. A retained replay commits one command at a time

- **What.** In `check_retained_qos0_replay`
  (`crates/mqttiumd/src/runtime_tests.rs`), a SUBSCRIBE that replays 16
  retained QoS 0 messages waited for 17 store commits, one after the
  other (commit `40c0e8e`, which gave that wait 10 s). On a disk that
  synchronises in 100 ms the replay takes about 1.7 s.
- **Status.** Observed, not analysed: which commands they are was not
  checked.
- **Trap.** Record 0032 requires an atomic frozen replay; grouping must
  keep it. Open-conditions row "Retained replay one commit at a time".

### R4. Session openings are serial

- **What.** `LIFECYCLE_IN_FLIGHT = 1` (`crates/mqttiumd/src/runtime.rs`,
  record 0048): a burst of N connections needs about 2·N synchronisations
  in a row within the 3 s connect deadline (record 0022). 128 clients at
  10 ms per synchronisation need about 2.6 s.
- **Decided.** The owner chose to state the limit (migration guide) and to
  study several openings in flight or the openings of a burst committed
  together, with a model (report 0048, open-conditions row "Session
  openings one at a time").
- **Trap.** The core's session lifecycle assumes one operation in flight;
  either option changes records 0048 (and 0045 for grouping).

### R5. The sharded mode: wait for the owner

- **Measured** (report 0049, "The sharded mode"): on three broker cores
  K = 3 raises a wide QoS 1 fan-out by 21 % (volatile) and 59 % (log) and
  K = 2 keeps up with the log fan-in, but K >= 1 loses 16 to 41 % on 4 and
  32 volatile pairs, adds latency, costs up to 65 % more CPU per delivery
  and 7.6 MB; K = 2 is unstable beyond 240,000 deliveries/s; the volatile
  fan-in falls behind at every K >= 1; K = 1 is worse nearly everywhere.
- **Open.** D7, D8, D9 and the models of the routing of acknowledgements,
  the end of a generation and the stop (open-conditions row "The sharded
  mode is experimental"). The owner has not decided whether to keep the
  mode after these figures.
- **Trap.** Do not optimise `crates/mqttiumd/src/delivery_shard.rs` before
  that decision; the K = 0 path is the product.

### R6. Idle memory

- 8.6 MB at rest against 5.5 MB for Mosquitto 2.1.4; 2,760 B per plain
  connection (Mosquitto 5,019 B) and 11,880 B per mTLS connection
  (Mosquitto 28,805 B). At 768 plain connections Mosquitto uses less in
  total (9.3 against 10.7 MB). Report 0049; measured with
  `tools/arm64_bench/idle_rss_bench.py`.

### R7. Two intermittent test failures, never reproduced

- `runtime::tests::sharded_faults::a_shard_that_panics_is_reported_to_the_coordinator`
  (`crates/mqttiumd/src/runtime_tests/sharded_faults.rs`): once, in a
  local workspace run right after a build; not in 248 runs since, nor with
  every `fsync` slowed to 300 ms (PR #684).
- `secure::takeover_with_a_partially_written_tls_record`
  (`crates/mqttiumd/tests/disconnect_durability.rs`, scenario
  `takeover` of `tests/disconnect_durability/recovery.py`): once, in the
  CI of #685 at `be5b361`; the old connection read a TLS end without
  close_notify where it expected the drained PUBLISH; 15 of 15 locally.
- Open-conditions row "Two intermittent test failures". If one recurs,
  reproduce it under the loads of report 0048 (CPU on two busy cores,
  `strace` delays on `fsync`) before changing anything.

### R8. What the measurements do not cover

The x86-64 VM is not the Pi of reports 0039 to 0041 (the ARM64 suite is
the owner's, at reception); one generator core; no QoS 2 driver; the
durable SQLite mode only under the gate; K at most 3 on three broker
cores; noise up to 9 % between runs.

### R9. Two large functions

`Runtime::drain_inputs` (`crates/mqttiumd/src/runtime.rs`) and
`Sink::try_submit` (`crates/mqttiumd/src/runtime/sink.rs`), about 1,100
lines each, keep their arms inline: they are the per-publication paths.
Split them arm by arm only with an instruction count for each arm (report
0047); low priority.

### R10. The order of the work

The owner's priorities of 27 November are at the end of the validation
section of `../reception/open-conditions.md`: D8, D9, the missing models
and the measurements first; the measurements are done on x86-64 (report
0049).
