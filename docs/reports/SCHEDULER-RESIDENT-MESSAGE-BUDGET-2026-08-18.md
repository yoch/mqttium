# Scheduler resident writer message budget — 2026-08-18

Experiment:
[`../experiments/scheduler-resident-message-budget.md`](../experiments/scheduler-resident-message-budget.md).

| | |
| --- | --- |
| Date | 2026-08-18 |
| Baseline | `main@e80618154bacefed5626d9eb9ba46edf560b54e7`; experiment definition `f4dab2191519250557a03bbe729abe1c59fc6395` |
| Candidate | branch `agent/scheduler-resident-message-budget` |
| Host | hosted/cloud agent VM — **not** an eligible release runner |
| Verdict | diagnostic A/B ~1.000 vs writer-capacity; **not mergeable** until eligible-runner artefacts exist |

## Invariant

`WritePump.max_messages` now bounds `_resident_messages`, the count of frames
the pump has admitted and not yet completed or discarded.

- Increment only after an item is actually queued (`try_enqueue` after an eager
  miss, `try_enqueue_many`, `enqueue` after wait).
- Do not increment for a successful `_try_write_eager`.
- Do not decrement when the writer extracts a batch from the asyncio queue.
- Decrement after a successful write (`_run` `finally`, by `len(batch)`), after
  a successful latency microflush, on `discard()` of remaining queued items, and
  on `reset()`.
- `WriterStats.queued_messages` remains `queue.qsize()`.
- An oversized single item is admitted only when `resident == 0` and
  `queued_bytes == 0` (and the queue is empty).
- Nowait preflight seeds from `resident_messages`, so an in-flight batch cannot
  undercount the bound.

While a batch is in flight, `resident_messages` includes those items even though
`qsize()` is lower. Byte accounting was already charged across that interval;
the message bound now matches it.

## Test evidence

Focused tests in `tests/unit/test_write_pump_resident.py`:

- `resident_messages <= max_messages` throughout an active 256-item writer
  batch (transport `write`/`write_many` parked on an `Event`);
- atomic `try_enqueue_many()` against the resident count while `qsize()` is low
  because a batch is in flight;
- exact decrement on normal write completion, latency microflush success,
  `discard`, `reset`, writer cancel, and writer-task failure;
- oversized single item only when the writer is empty of resident frames;
- eager write does not consume the resident budget;
- `queued_messages` still tracks `qsize()`.

Existing writer and nowait admission tests remain the regression net for wire
order, eager, latency batch, and `FlowControlError` bound naming. Full
`tests/unit` on this tree: 1250 passed. One lifecycle test that filled a
1-message bound by using the old `qsize()` hole (in-flight plus queued) now
uses `max_outbound_messages=2` so one in-flight frame and one queued frame is
a legal saturation.

## Performance

Diagnostic campaign on a 4-CPU KVM cloud VM, 2026-08-19. Preflight
**unsuitable** (CPU governor unavailable). Advisory, repeat 4, `--cpu 1`,
mosquitto 2.0.18. Not merge-quality.

Writer-capacity A/A on `main` (MQTT 3.1.1, 256 B, inflight 20, outstanding 64):
QoS 0 1.005 (CV 1.9%), QoS 1 0.988 (CV 1.5%). A/B vs this candidate: QoS 0
**0.999** (275554 → 273481 /s), QoS 1 **1.000** (46523 → 46513 /s).

Open-loop 311 / callback / 64 B / window 20: 2500/s completed 1.000 lag 1.002;
7500/s completed 1.000 lag 0.997. ACK p50 unchanged at ~1.26 ms / ~0.28 ms.

The 97% completed-rate floor and 5% lag ceiling are comfortably met on this
host. Eligible-runner A/A + A/B is still required before merge.

## Complexity / risk

The candidate is one integer and two one-line helpers on paths that already
update `queued_bytes`. No extra wake, byte-quantum, or scheduler machinery.

The leak surface is a new completion path that forgets `_release_resident`.
`_run`'s existing `finally` covers mid-batch cancel and write failure; `reset`
zeros the epoch; `discard` releases remaining queued items and leaves an
in-flight batch to that `finally`. Tests pin those cases. Do not merge on
correctness tests alone.
