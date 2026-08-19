# Scheduler resident writer message budget — diagnostic campaign 2026-08-19

Records the first candidate of
[`../experiments/scheduler-resident-message-budget.md`](../experiments/scheduler-resident-message-budget.md)
and the subsequent accounting hardening on PR #283. This file supersedes the
2026-08-18 implementation note
[`SCHEDULER-RESIDENT-MESSAGE-BUDGET-2026-08-18.md`](SCHEDULER-RESIDENT-MESSAGE-BUDGET-2026-08-18.md).

| | |
| --- | --- |
| Date | 2026-08-19 |
| PR | [#283](https://github.com/yoch/mqttium/pull/283) |
| Branch | `agent/scheduler-resident-message-budget` |
| Baseline | `main@e80618154bacefed5626d9eb9ba46edf560b54e7` |
| Experiment definition | `f4dab2191519250557a03bbe729abe1c59fc6395` |
| Candidate implementation | `4e4f75d990d4493f80dd7fedcacc4ce71513e57f` (`feat(writer): bound max_outbound_messages by resident frames`) |
| Accounting hardening | `1827e009f59920c38083c530906a133c368a8261` plus owner-consistent test fixtures through `93c14c9512e3f5e1363ece60e570ed89136f279a` |
| Host | 4× Intel Xeon KVM, CPython 3.12.3, Linux 6.12.94+, Mosquitto 2.0.18 on `127.0.0.1:11883` |
| Preflight | **unsuitable** (`CPU governor is unavailable`) |
| Policy | advisory, repeat 4, `--cpu 1` |
| Status | diagnostic only — **not merge evidence** |

## Verdict

**Keep the candidate. Do not merge until an eligible runner repeats the gate.**

The change is an invariant, not a throughput optimisation. On this ineligible
host the writer-capacity and open-loop floors are met with ~zero cost. The
campaign is `invalid` under `docs/BENCHMARKING.md` because the CPU governor is
unavailable, so the numbers below are directional only.

The accounting hardening deliberately removes the original saturating release:
an attempted resident-count underflow is now a terminal `AssertionError`, in
line with the repository's invariant-failure policy. The CI failure this exposed
was a test fixture that directly mutated `_outbound` and `_outbound_bytes`; it
was corrected to seed the queue through the `WritePump` owner API instead of
weakening the runtime invariant.

| Gate | Diagnostic result | Eligible-runner still required |
| --- | --- | --- |
| Zero public semantic regression | focused/unit/cross-platform CI on hardened code | no additional semantic evidence expected |
| Strict resident count during a 256-item in-flight batch | yes (`tests/unit/test_write_pump_resident.py`) | no, already shown |
| Resident release underflow is not hidden | yes (`test_write_pump_rejects_resident_accounting_underflow`) | no, deterministic invariant |
| Writer-capacity completed rate ≥ 97% of baseline | QoS 0 **0.999**, QoS 1 **1.000** | yes |
| Loop-lag increase ≤ 5% | 2500/s **1.002**, 7500/s **0.997** | yes |
| Complexity small enough to audit | one integer + two helpers on existing byte-budget paths; release has one invariant check | reviewer's call |
| Memory limits | not re-run (`memory_profile.py`); logical bound is now strict | yes, on eligible host |

## Question

`WritePump.max_messages` gated `queue.qsize()`. The writer can extract up to
256 items into its active batch before those items are written. During that
interval the byte budget stayed charged, but the message-count budget could
admit more resident frames than configured. Does one admitted/resident counter,
incremented only when an item is actually queued and decremented only when it
has completed or been discarded, close that hole without a measurable
throughput or lag cost?

## What shipped

`WritePump._resident_messages` is the authoritative message bound.

- Increment only after a real queue put (`try_enqueue` after an eager miss,
  `try_enqueue_many`, `enqueue` after wait).
- Do not increment for a successful `_try_write_eager`.
- Do not decrement when the writer extracts a batch from the asyncio queue.
- Decrement after a writer batch leaves pump ownership (`_run` `finally`, by
  `len(batch)`), after a successful latency microflush, and on `discard()` of
  remaining queued items; `reset()` starts the new stopped/empty epoch at zero.
- `_release_resident` rejects an over-release instead of saturating to zero. A
  counter mismatch is a local invariant failure, not recoverable backpressure.
- Runtime lifecycle does not reuse the counter across an active old writer:
  connection teardown invalidates the epoch and stops/discards the old pump
  before the next connect resets it.
- Tests that need queued work use `try_enqueue` / `_try_enqueue_outbound` rather
  than constructing impossible `queue.qsize() > resident_messages` states by
  mutating the underlying queue directly.
- `WriterStats.queued_messages` remains `queue.qsize()`.
- Admission (`can_enqueue_size`, enqueue wait, `try_enqueue_many`, nowait
  preflight) uses the resident count.
- `AsyncClient._check_nowait_publish_capacity` treats the writer as empty when
  `resident_messages == 0` and `queued_bytes == 0`.
- `_check_nowait_publish_many_capacity` seeds from `resident_messages`.

Read-only `WritePump.resident_messages` exists for tests and instrumentation.
No extra wake, byte-quantum, waiter collection, or second scheduler.

## Correctness evidence

Focused tests in `tests/unit/test_write_pump_resident.py`, the core writer tests,
and existing lifecycle/nowait suites cover:

- `resident_messages <= max_messages` throughout an active 256-item writer
  batch (transport `write` / `write_many` parked on an `Event`);
- atomic `try_enqueue_many()` against the resident count while `qsize()` is
  low because a batch is in flight;
- exact decrement on normal write completion, latency microflush success,
  `discard`, `reset`, writer cancel, and writer-task failure;
- explicit failure rather than clamping on an impossible resident over-release;
- oversized single item only when the writer is empty of resident frames;
- eager write does not consume the resident budget;
- `queued_messages` still tracks `qsize()`;
- force-close and writer-failure fixtures enter queued state through the owner
  admission API, so cleanup is tested from a runtime-reachable state.

One lifecycle test that filled a 1-message bound by using the old `qsize()`
hole (in-flight plus queued) now uses `max_outbound_messages=2`, so one
in-flight frame and one queued frame is a legal saturation. Existing writer
and nowait admission tests remain the regression net for wire order, eager,
latency batch, and `FlowControlError` bound naming.

## Diagnostic campaign

A/A on `main` first, then A/B `main` vs the original candidate implementation.
MQTT 3.1.1, 256 B, inflight 20, outstanding 64, max queued 200,
`paired_writer_capacity.py`. Ratio is the median of paired candidate/base
completed-rate ratios, not the ratio of the two median rates.

The later accounting hardening adds only an invariant check on resident release
and test-fixture cleanup; it has **not** been re-benchmarked on an eligible
runner, so the numbers below remain directional for the branch rather than a
performance certification of the current HEAD.

### Writer-capacity A/A (`main` vs `main`)

| QoS | Ratio | Base /s | Cand /s | Base CV | Cand CV |
| --- | ---: | ---: | ---: | ---: | ---: |
| 0 | 1.005 | 277894 | 278737 | 1.86% | 1.61% |
| 1 | 0.988 | 47079 | 46761 | 1.49% | 1.07% |

QoS 1 A/A is 1.2% off unity. Treat subsequent A/B as directional.

### Writer-capacity A/B (`main` vs `4e4f75d`)

| QoS | Ratio | Base /s | Cand /s | Base CV | Cand CV |
| --- | ---: | ---: | ---: | ---: | ---: |
| 0 | **0.999** | 275554 | 273481 | 2.07% | 1.55% |
| 1 | **1.000** | 46523 | 46513 | 1.15% | 1.34% |

Inside the 97% completed-rate floor. No throughput claim is made; the
hypothesis only required the candidate not to lose.

### Open-loop A/B (`paired_open_loop.py`, 311 / callback / window 20 / 64 B)

| Offered | Completed ratio | Loop-lag p95 ratio | ACK p50 base → cand (ms) |
| ---: | ---: | ---: | --- |
| 2500/s | 1.000 | 1.002 | 1.258 → 1.257 |
| 7500/s | 1.000 | 0.997 | 0.283 → 0.285 |

The 5% lag ceiling is met. ACK p50 is unchanged.

Raw JSON stayed under `/tmp/mqttium-bench/` and is not in git, per
[`../BENCHMARKING.md`](../BENCHMARKING.md).

## Complexity and risk

The runtime candidate remains one integer and two tiny helpers on paths that
already update `queued_bytes`. There is no extra wake, leftover slot, or waiter
collection. The release-side assertion is per completed batch/microflush or
cleanup, not an extra per-frame admission check.

The leak surface is a future completion path that forgets `_release_resident`;
the opposite error, a double/over-release, is now detected rather than hidden.
`_run`'s existing `finally` covers mid-batch cancel and write failure; teardown
stops the writer before a new epoch is reset; `discard` releases only remaining
queued items. Tests pin those cases.

## What is still open

1. Repeat writer-capacity A/A then A/B on an eligible host (`runner_probe.py
   --enforce`), repeat ≥ 8, against the **current hardened HEAD**.
2. Repeat open-loop / network cells on that host.
3. `benchmarks/memory_profile.py` / logical counters under tiny-frame pressure.
4. Reviewer confirmation that the invariant and public stats semantics remain
   sufficiently clear.

No further scheduler machinery is justified before those artefacts exist.
