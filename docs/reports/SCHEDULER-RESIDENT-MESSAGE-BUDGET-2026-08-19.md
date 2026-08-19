# Scheduler resident writer message budget — diagnostic campaign 2026-08-19

Records the first candidate of
[`../experiments/scheduler-resident-message-budget.md`](../experiments/scheduler-resident-message-budget.md)
at the feat commit named below. This file supersedes the 2026-08-18
implementation note
[`SCHEDULER-RESIDENT-MESSAGE-BUDGET-2026-08-18.md`](SCHEDULER-RESIDENT-MESSAGE-BUDGET-2026-08-18.md).

| | |
| --- | --- |
| Date | 2026-08-19 |
| PR | [#283](https://github.com/yoch/mqttium/pull/283) |
| Branch | `agent/scheduler-resident-message-budget` |
| Baseline | `main@e80618154bacefed5626d9eb9ba46edf560b54e7` |
| Experiment definition | `f4dab2191519250557a03bbe729abe1c59fc6395` |
| Commit described | `4e4f75d990d4493f80dd7fedcacc4ce71513e57f` (`feat(writer): bound max_outbound_messages by resident frames`) |
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

| Gate | Diagnostic result | Eligible-runner still required |
| --- | --- | --- |
| Zero public semantic regression | yes (unit tests + `queued_messages` still `qsize()`) | no, already shown |
| Strict resident count during a 256-item in-flight batch | yes (`tests/unit/test_write_pump_resident.py`) | no, already shown |
| Writer-capacity completed rate ≥ 97% of baseline | QoS 0 **0.999**, QoS 1 **1.000** | yes |
| Loop-lag increase ≤ 5% | 2500/s **1.002**, 7500/s **0.997** | yes |
| Complexity small enough to audit | one integer + two helpers on existing byte-budget paths | reviewer's call |
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
- Decrement after a successful write (`_run` `finally`, by `len(batch)`), after
  a successful latency microflush, on `discard()` of remaining queued items, and
  on `reset()`.
- `_release_resident` clamps at 0. It must not assert underflow: tests and the
  writer-failure path can `put_nowait` without a matching admit, and `_run`'s
  `finally` must not mask a transport error.
- `WriterStats.queued_messages` remains `queue.qsize()`.
- Admission (`can_enqueue_size`, enqueue wait, `try_enqueue_many`, nowait
  preflight) uses the resident count.
- `AsyncClient._check_nowait_publish_capacity` treats the writer as empty when
  `resident_messages == 0` and `queued_bytes == 0`.
- `_check_nowait_publish_many_capacity` seeds from `resident_messages`.

Read-only `WritePump.resident_messages` exists for tests and instrumentation.
No extra wake, byte-quantum, or scheduler machinery.

## Correctness evidence

Focused tests in `tests/unit/test_write_pump_resident.py` (59 passed):

- `resident_messages <= max_messages` throughout an active 256-item writer
  batch (transport `write` / `write_many` parked on an `Event`);
- atomic `try_enqueue_many()` against the resident count while `qsize()` is
  low because a batch is in flight;
- exact decrement on normal write completion, latency microflush success,
  `discard`, `reset`, writer cancel, and writer-task failure;
- oversized single item only when the writer is empty of resident frames;
- eager write does not consume the resident budget;
- `queued_messages` still tracks `qsize()`.

One lifecycle test that filled a 1-message bound by using the old `qsize()`
hole (in-flight plus queued) now uses `max_outbound_messages=2`, so one
in-flight frame and one queued frame is a legal saturation. Existing writer
and nowait admission tests remain the regression net for wire order, eager,
latency batch, and `FlowControlError` bound naming. Full `tests/unit` on this
tree at the feat commit: 1250 passed.

## Diagnostic campaign

A/A on `main` first, then A/B `main` vs this candidate. MQTT 3.1.1, 256 B,
inflight 20, outstanding 64, max queued 200, `paired_writer_capacity.py`.
Ratio is the median of paired candidate/base completed-rate ratios, not the
ratio of the two median rates.

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

The production change is one integer and two one-line helpers on paths that
already update `queued_bytes`. There is no extra wake, leftover slot, or
waiter collection.

The leak surface is a new completion path that forgets `_release_resident`.
`_run`'s existing `finally` covers mid-batch cancel and write failure; `reset`
zeros the epoch; `discard` releases remaining queued items and leaves an
in-flight batch to that `finally`. Tests pin those cases.

Do not merge on correctness tests alone. The experiment contract also requires
eligible-runner writer-capacity, open-loop, and a memory-profile check that the
configured count bound is now strict under tiny-frame pressure.

## What is still open

1. Repeat writer-capacity A/A then A/B on an eligible host (`runner_probe.py
   --enforce`), repeat ≥ 8.
2. Repeat open-loop / network cells on that host.
3. `benchmarks/memory_profile.py` / logical counters under tiny-frame pressure.
4. Reviewer confirmation that the invariant stays auditable.

No further code is justified until those artefacts exist.
