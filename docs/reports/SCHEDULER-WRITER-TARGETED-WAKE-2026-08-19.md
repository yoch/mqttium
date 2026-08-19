# Targeted writer waiter wakeups — diagnostic campaign 2026-08-19

Records the first candidate of
[`../experiments/scheduler-writer-targeted-wake.md`](../experiments/scheduler-writer-targeted-wake.md)
at the feat commit named below. This file supersedes the 2026-08-18
implementation note
[`SCHEDULER-WRITER-TARGETED-WAKE-2026-08-18.md`](SCHEDULER-WRITER-TARGETED-WAKE-2026-08-18.md).

| | |
| --- | --- |
| Date | 2026-08-19 |
| PR | [#284](https://github.com/yoch/mqttium/pull/284) |
| Branch | `agent/scheduler-writer-targeted-wake` |
| Baseline | `main@e80618154bacefed5626d9eb9ba46edf560b54e7` |
| Experiment definition | `7e686e8de01228541d9d1d109faff12452541bb7` |
| Commit described | `1b5d871de11c0e3d09742fb9320d2a78a33027e2` (`feat(writer): wake only as many waiters as slots freed`) |
| Host | 4× Intel Xeon KVM, CPython 3.12.3, Linux 6.12.94+, Mosquitto 2.0.18 on `127.0.0.1:11883` |
| Preflight | **unsuitable** (`CPU governor is unavailable`) |
| Policy | advisory, repeat 4, `--cpu 1` |
| Status | diagnostic only — **not merge evidence** |

## Verdict

**Keep `notify(n)` on normal capacity release. Do not merge until an eligible
runner confirms the harness A/A (especially 16 producers) and the A/B.**

On this ineligible host the hypothesis holds where many producers wait on a
tight writer window: +9.8% at 16 producers and +143% at 64, with a large CPU
and suspension cut. Single-producer throughput is −2.2% (inside the 3%
guardrail). Closed-loop writer-capacity does not move. A FIFO/weighted waiter
deque is not justified yet.

The campaign is `invalid` under `docs/BENCHMARKING.md`. The 16-producer
harness A/A on this tree is 2.1% off the 2% band, so that A/B cell is
directional even before the governor failure.

| Gate | Diagnostic result | Eligible-runner still required |
| --- | --- | --- |
| ≥ 5% at two contention points | **+9.8%** at 16, **+143%** at 64 | yes |
| Single/few-producer regression ≤ 3% | 1 producer **−2.2%**, 4 producers **+1.0%** | yes |
| Writer-capacity non-regression | QoS 0 **1.012**, QoS 1 **1.000** | yes |
| Harness A/A | 1/4/64 inside ~2%; **16 producers 0.979** | yes, especially 16 |
| No starvation / lost wakeup | unit tests | no, already shown |
| FIFO/weighted deque | not added | only if this candidate fails fairness |

## Question

`WritePump._run` called `Condition.notify_all()` after every completed batch
released queue capacity. With many blocked `enqueue()` producers, one capacity
release made every waiter runnable even though only a subset could be
admitted. Does waking `n = slots just freed` preserve progress, cancellation,
and lifecycle unparking without a FIFO/weighted waiter structure?

## What shipped

**`notify(n)` on normal capacity release, `notify_all()` on lifecycle.**

- After a written batch: `n = min(self.waiters, len(batch))`. That is one
  waiter per message slot the batch just popped. Waiters remain on
  `asyncio.Condition`'s FIFO list.
- `notify(1)` is still correct for eventual progress, but after a 256-item
  batch it would leave 255 free slots idle until the next write completed.
  `notify(n)` is the same policy with `n` matching the slots actually freed.
- `wake_waiters()` / `advance_epoch()` keep `notify_all()`. Client discard,
  stop, and transport-failure teardown continue to broadcast through those
  methods.
- If a waiter is cancelled after consuming a notify token, `enqueue`'s wait
  loop hands off `notify(1)` when other waiters remain. A waiter that wakes
  and finds the queue full (a `try_enqueue` race) waits again.
- Eager-write exclusion while `waiters > 0` is unchanged.

No explicit waiter deque. No change to byte/message admission, wire order, or
the writer-task / eager split.

## Correctness evidence

`tests/unit/test_write_pump_targeted_wake.py` (16 passed):

- single waiter resume;
- many waiters completing while capacity keeps being released;
- first capacity notify equals the freed batch size;
- epoch / discard / stop / transport failure still broadcast;
- cancel-after-notify and cancel-while-parked;
- no starvation;
- eager path stays off while waiters exist.

Existing writer tests remain the regression net for wire order and admission.

## Harness

`benchmarks/paired_writer_waiter_contention.py`: fresh processes, alternating
order, JSON under `/tmp`. Defaults: producers 1/4/16, `max_messages=8`.
64/256 are `--producer-values` options. It records completed ops/s, process
CPU, `enqueue_suspensions`, and enqueue-wait p50/p95/p99. Indexed in
[`../BENCHMARKING.md`](../BENCHMARKING.md). Do not commit raw JSON.

## Diagnostic campaign

### Writer-capacity guard vs `main` (`paired_writer_capacity.py`, 311, 256 B, inf 20, out 64)

| QoS | Ratio | Base /s | Cand /s | Base CV | Cand CV |
| --- | ---: | ---: | ---: | ---: | ---: |
| 0 | 1.012 | 271379 | 278038 | 1.61% | 1.92% |
| 1 | 1.000 | 46792 | 47048 | 1.51% | 0.60% |

Non-targeted closed-loop is flat.

### Waiter-contention A/A (this tree vs this tree)

`max_messages=8`, 64 B, 20 000 ops, producers 1/4/16/64.

| Producers | Ratio | Base /s | Cand /s | Base CV | Cand CV |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 0.996 | 528672 | 527198 | 0.30% | 1.15% |
| 4 | 0.998 | 445267 | 445215 | 0.59% | 0.93% |
| 16 | **0.979** | 318422 | 312319 | 0.54% | 0.98% |
| 64 | 0.995 | 289337 | 289047 | 0.48% | 2.98% |

The 16-producer A/A is 2.1% off the 2% band. Treat that A/B cell as
directional even on an eligible host until A/A recovers.

### Waiter-contention A/B (`notify_all()` vs `notify(n)`)

| Producers | Rate | Base /s | Cand /s | CPU s | Suspensions | Enqueue p99 (ms) |
| ---: | ---: | ---: | ---: | --- | --- | --- |
| 1 | 0.978 | 533287 | 522217 | 0.038 → 0.038 | 2499 → 2499 | 0.011 → 0.011 |
| 4 | 1.010 | 438716 | 445515 | 0.046 → 0.045 | 6246 → 6246 | 0.016 → 0.016 |
| 16 | **1.098** | 284190 | 311469 | 0.070 → 0.064 | 21240 → 16476 | 0.035 → 0.196 |
| 64 | **2.433** | 119308 | 290626 | 0.168 → 0.069 | 81264 → 19453 | 0.107 → 0.923 |

Two contention points clear the 5% bar. Enqueue p99 **rises** at 16/64:
waiters queue instead of stampeding. Throughput, CPU and suspensions are the
target metrics; a higher wait p99 is the intended shape of a non-herding
wakeup.

Open-loop/network cells were not run for this PR on this host. They remain
part of the contract's eligible-runner gate.

## Complexity and risk

The production change is two sites in `src/mqttium/api/_writer.py`:

1. `_run`'s post-write notify.
2. A `CancelledError` hand-off inside `enqueue`'s wait loop.

There is no new owned collection, no extra lock, and no change to admission
or wire order. Remaining risks:

- **Eligible-runner confirmation.** Re-run harness A/A (especially 16
  producers) then A/B, plus open-loop/network, on an eligible host.
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
  the batch *is* the whole queue (tight windows, this experiment's regime).

## What is still open

1. Eligible-runner harness A/A, then A/B, including 64/256 producers if the
   16-producer A/A recovers.
2. Open-loop / network non-regression on that host.
3. No FIFO/weighted waiter structure unless this candidate fails fairness or
   heterogeneous-size contention.

Keep the candidate. Merge is blocked on an eligible runner, not on further
code.
