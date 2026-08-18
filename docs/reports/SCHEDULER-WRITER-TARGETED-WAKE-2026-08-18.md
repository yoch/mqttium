# Targeted writer waiter wakeups — 2026-08-18

Experiment contract:
[`../experiments/scheduler-writer-targeted-wake.md`](../experiments/scheduler-writer-targeted-wake.md).

| | |
| --- | --- |
| Date | 2026-08-18 |
| Baseline | `main@e80618154bacefed5626d9eb9ba46edf560b54e7` |
| Scope | `WritePump` capacity-release wakeup policy only. No resident-counter, byte-quantum, or waiter-deque change. |
| Status | Candidate implemented and unit-tested. **Not mergeable** until eligible-runner harness A/A and A/B artefacts exist. |

This is a dated report. It records the candidate as written on 2026-08-18. It is
not a contract; do not edit it to match later measurements — attach a new report
with the A/A and A/B numbers.

## Question

`WritePump._run` called `Condition.notify_all()` after every completed batch
released queue capacity. With many blocked `enqueue()` producers, one capacity
release made every waiter runnable even though only a subset could be admitted.
Does waking `n = slots just freed` preserve progress, cancellation, and
lifecycle unparking without a FIFO/weighted waiter structure?

## Candidate

**`notify(n)` on normal capacity release, `notify_all()` on lifecycle.**

- After a written batch: `n = min(self.waiters, len(batch))`. That is one
  waiter per message slot the batch just popped. It is not a scheduler: waiters
  remain on `asyncio.Condition`'s FIFO list.
- `notify(1)` is the original smallest patch and is still correct for
  eventually making progress, but after a 256-item batch it would leave 255
  free slots idle until the next write completed. `notify(n)` is the same
  policy with `n` matching the slots actually freed.
- `wake_waiters()` / `advance_epoch()` keep `notify_all()`. Client discard,
  stop, and transport-failure teardown continue to broadcast through those
  methods.
- If a waiter is cancelled after consuming a notify token, `enqueue`'s wait
  loop hands off `notify(1)` when other waiters remain. A waiter that wakes
  and finds the queue full (a `try_enqueue` race) waits again; the success
  path does not notify extra.
- Eager-write exclusion while `waiters > 0` is unchanged.

No explicit waiter deque. Fairness tests only require that N waiters all
complete while capacity keeps being released.

## Complexity

The production change is two sites in `src/mqttium/api/_writer.py`:

1. `_run`'s post-write notify.
2. A `CancelledError` hand-off inside `enqueue`'s wait loop.

That is the whole policy. There is no new owned collection, no extra lock, and
no change to byte/message admission, wire order, or the writer-task / eager
split. The risk is concentrated in lost-wakeup and lifecycle-broadcast
coverage, which is why those have dedicated tests rather than more machinery.

## Tests and harness

- `tests/unit/test_write_pump_targeted_wake.py`: single waiter resume; many
  waiters completing; first capacity notify equals the freed batch size;
  epoch/discard/stop/transport failure; cancel-after-notify and cancel-while-
  parked; no starvation; eager path stays off while waiters exist.
- `benchmarks/paired_writer_waiter_contention.py`: fresh processes, alternating
  order, JSON under `/tmp`. Defaults: producers 1/4/16, `max_messages=8`.
  64/256 are `--producer-values` options. Do not commit raw JSON.

## Risks that remain

- **No performance claim yet.** The hypothesis is fewer redundant wakeups under
  multi-producer backpressure. Eligible-runner A/A of the new harness, then A/B
  against `notify_all()`, plus `paired_writer_capacity.py` and open-loop
  latency cells, are still required. Hosted-runner numbers are not evidence.
- **Heterogeneous item sizes.** `n` is a message-slot count. A waiter blocked
  on the byte bound can still be woken for a small-item slot, fail admission,
  and wait again. A FIFO/weighted deque is the next candidate only if this
  shows up in fairness tests or measured contention.
- **Cancellation over-notify.** The hand-off fires whenever other waiters
  exist, including cancellation that did not consume a useful wakeup. That is
  a spurious wakeup, not a stall.
- **`try_enqueue` still races waiters.** Pre-existing: a non-waiting producer
  can take a freed slot before a woken waiter re-acquires the condition. The
  waiter loops. Targeted wake does not change that.
- **Batch cap 256 vs default `max_outbound_messages=10_000`.** Waiters only
  exist while the queue is full, so a 256-item batch frees 256 slots and
  `notify(256)` matches them. The `notify(1)` serialization bug appears when
  the batch *is* the whole queue (tight windows, the experiment's regime).

## Verdict

The candidate is small enough to keep: it does not add a waiter scheduler, and
the correctness tests pin the failure modes that would make `notify(n)` worse
than `notify_all()`. Merge is blocked on measured evidence, not on further
code.
