# Scheduler experiment: targeted publish admission wake — 2026-08-18

> **Superseded** by
> [`SCHEDULER-PUBLISH-TARGETED-WAKE-2026-08-19.md`](SCHEDULER-PUBLISH-TARGETED-WAKE-2026-08-19.md),
> which names the feat commit and records the 2026-08-19 diagnostic campaign.
> This file is the implementation note as written on 2026-08-18.

Records candidate 1 of
[`../experiments/scheduler-publish-targeted-wake.md`](../experiments/scheduler-publish-targeted-wake.md).
This is an implementation note plus a diagnostic (ineligible-host) campaign.

| | |
| --- | --- |
| Date | 2026-08-18 |
| Tree | `/tmp/mqttium-publish-wake` against `docs/experiments/scheduler-publish-targeted-wake.md` baseline `main@e806181` |
| Candidate | deque of waiter futures; one ACK wakes one producer |
| Performance campaign | diagnostic A/A + A/B on an ineligible cloud VM, 2026-08-19 |

## Verdict

**Diagnostic keep; not merge evidence.** On an ineligible 4-CPU cloud VM
(2026-08-19, advisory, repeat 4) the harness A/A was inside ~2% at every cell.
A/B vs `main` (MQTT 3.1.1 QoS 1, 64 B):

| Publishers | inflight | Rate | base/s | cand/s | CPU s |
| --- | --- | --- | --- | --- | --- |
| 1 | 1 | 1.015 | 19030 | 19205 | 0.105 → 0.104 |
| 1 | 4 | 0.994 | 26292 | 26192 | 0.075 → 0.076 |
| 4 | 1 | **1.291** | 14898 | 19186 | 0.134 → 0.104 |
| 4 | 4 | **1.069** | 22621 | 24174 | 0.088 → 0.083 |
| 16 | 1 | **2.269** | 8529 | 19402 | 0.235 → 0.103 |
| 16 | 4 | **1.574** | 15307 | 24176 | 0.131 → 0.083 |

Four contention cells beat 5%. Low-contention cells stay within 3%. Writer-capacity
guard 1.003 / 1.006. Publish-call p99 rises under contention (queueing vs
stampede); completed rate and CPU are the target. Confirm on an eligible host.

## What changed

`AsyncClient` used one `asyncio.Event` (`_publish_space`) for every producer
blocked in `publish()` / `_admit_publish_many()`. `_settle_publish` and
`_notify_publish_space` both `set()` that event, so one slot release scheduled
every waiter onto `_engine_lock`.

Candidate 1 keeps the wait/retry loop and replaces only the wake hint:

- Register a future while still holding `_engine_lock` (same lost-wakeup window
  as the old `clear()`-under-lock).
- `_wake_publish_waiters(n=1)` from `_settle_publish`: pop pending futures until
  one incomplete future is completed. One ACK → one waiter.
- `_notify_publish_space()` still completes every pending future (disconnect,
  writer failure, reconnect-disabled teardown, `_force_close`).
- Cancellation: a still-pending future is discarded; a future that was already
  completed (token consumed) forwards the wakeup to the next waiter.
- `_publish_waiters` remains an `int` for `ClientStats` and existing tests.
- Internal counters `_publish_wakeups` / `_publish_wait_retries` are test and
  harness hints, not public statistics.

`publish_many()` still parks as one waiter for the whole chunk and is woken once
per ACK until `queue_publish_many` succeeds. WritePump notify policy is
untouched. No FIFO/weighted admission queue was added.

## Complexity and risk

The extra state is a deque of futures plus two integers. That is more than a
single Event, but far less than a second admission scheduler: waiters never
own slots, budgets, or packet ids. The subtle piece is cancellation forwarding.
Getting it wrong either leaks a wakeup (one waiter starved until a later ACK)
or double-wakes (harmless retry, `FlowControlError`, park again). Tests cover
the steal case.

FIFO order of the deque is incidental fairness, not a promised policy. A later
weighted queue would have to beat this candidate on the experiment's own gate
before paying for that state.

Hot-path cost when nobody is waiting is still an integer check in
`_settle_publish`. The wait path allocates one Future per park, which is paid
only under contention.

## Tests

`tests/unit/test_publish_targeted_wake.py` plus the unchanged
`tests/unit/test_async_publish_admission.py`:

- existing wait-until-complete behaviour
- two waiters, one completion → exactly one proceeds
- two completions can release two waiters
- cancelling one parked waiter does not steal the only wakeup
- terminal teardown fails every parked publisher
- `publish_many()` waits, and a chunk that needs two slots is woken once per ACK
- several producers all complete while completions keep arriving
- wakeups ≤ completions on the non-teardown path

## Harness

`benchmarks/paired_publish_admission_contention.py` mirrors
`paired_writer_capacity.py`: `--base-root` / `--candidate-root`, JSON to
`--output`, native `await publish()` with 1/4/16 concurrent tasks by default
and `--publisher-values` / `--inflight-values` for 64/256 or window 1/4/20.

It records completed rate, process CPU, publish-call p50/p95/p99, wakeup/retry
counters when present, and min/max per-producer completions.

## What is still open

- Eligible-runner A/A then A/B (this VM has no CPU governor).
- Open-loop/network as non-targeted guards on that host.
