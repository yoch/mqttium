# Scheduler resident writer message budget — 2026-08-18

Experiment:
[`../experiments/scheduler-resident-message-budget.md`](../experiments/scheduler-resident-message-budget.md).

| | |
| --- | --- |
| Date | 2026-08-18 |
| Baseline | `main@e80618154bacefed5626d9eb9ba46edf560b54e7`; experiment definition `f4dab2191519250557a03bbe729abe1c59fc6395` |
| Candidate | branch `agent/scheduler-resident-message-budget` |
| Host | hosted/cloud agent VM — **not** an eligible release runner |
| Verdict | correctness candidate implemented; **not mergeable** until eligible-runner A/A + A/B artefacts exist |

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

Not measured here. The experiment's acceptance gate still requires an eligible
runner, A/A control on `benchmarks/paired_writer_capacity.py`, A/B for QoS 0
and QoS 1, and the memory/open-loop advisories listed in the experiment doc.
Numbers from this host are not merge-quality and were not collected.

## Complexity / risk

The candidate is one integer and two one-line helpers on paths that already
update `queued_bytes`. No extra wake, byte-quantum, or scheduler machinery.

The leak surface is a new completion path that forgets `_release_resident`.
`_run`'s existing `finally` covers mid-batch cancel and write failure; `reset`
zeros the epoch; `discard` releases remaining queued items and leaves an
in-flight batch to that `finally`. Tests pin those cases. Do not merge on
correctness tests alone.
