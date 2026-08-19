# Targeted publish admission wakeups — diagnostic campaign 2026-08-19

Records candidate 1 of
[`../experiments/scheduler-publish-targeted-wake.md`](../experiments/scheduler-publish-targeted-wake.md)
at the feat commit named below. This file supersedes the 2026-08-18
implementation note
[`SCHEDULER-PUBLISH-TARGETED-WAKE-2026-08-18.md`](SCHEDULER-PUBLISH-TARGETED-WAKE-2026-08-18.md).

> **Measurement correction.** The historical contention numbers in this report
> timed the return from `publish()` and therefore measured **admission-return
> rate**, not QoS completion. The benchmark now records both admission and true
> completion and uses true completion as the primary throughput gate. Final
> eligible ARM64 evidence is recorded in the PR discussion; the historical
> table below is retained for traceability.

| | |
| --- | --- |
| Date | 2026-08-19 |
| PR | [#285](https://github.com/yoch/mqttium/pull/285) |
| Branch | `agent/scheduler-publish-targeted-wake` |
| Baseline | `main@e80618154bacefed5626d9eb9ba46edf560b54e7` |
| Experiment definition | `d0b812ef067b2318b73ace4206dc2e65ab9a2f21` |
| Commit described | `acc427ae839c1ed926f74e825a15dae0f85545a0` (`feat(client): wake one publish waiter per admission release`) |
| Host | 4× Intel Xeon KVM, CPython 3.12.3, Linux 6.12.94+, Mosquitto 2.0.18 on `127.0.0.1:11883` |
| Preflight | **unsuitable** (`CPU governor is unavailable`) |
| Policy | advisory, repeat 4, `--cpu 1` |
| Status | diagnostic only — **not merge evidence** |

## Verdict

**Keep candidate 1 (deque of waiter futures, one ACK wakes one producer). Do
not merge until an eligible runner repeats the gate.**

On this ineligible host the hypothesis holds under publish-admission
contention: +29% at 4 publishers / inflight 1, +127% at 16 / 1, +57% at
16 / 4. Single-publisher cells stay within the 3% guardrail. Writer-capacity
is flat. Weighted/FIFO admission state was not added.

The campaign is `invalid` under `docs/BENCHMARKING.md`. Harness A/A on this
tree was inside ~2% at every cell, which is why the A/B is at least
internally consistent on this host — it is still not merge-quality.

| Gate | Diagnostic result | Eligible-runner still required |
| --- | --- | --- |
| ≥ 5% at two contention points | **+29% / +127% / +57%** at 4×inf1, 16×inf1, 16×inf4 | yes |
| Low-contention regression ≤ 3% | 1 pub inf1 **+1.5%**, 1 pub inf4 **−0.6%** | yes |
| Writer-capacity non-regression | QoS 0 **1.003**, QoS 1 **1.006** | yes |
| Harness A/A | all six cells inside ~2% | yes |
| Terminal teardown wakes everyone | unit tests | no, already shown |
| Engine/writer remain admission truth | yes; waiters only retry | no, already shown |

## Question

All `publish()` callers blocked by protocol admission waited on one
`asyncio.Event`. A single ACK set that event and made every blocked publisher
runnable even when only one slot became available. Under high producer
concurrency that is a thundering herd around `_engine_lock`. Does waking only
the number of publishers plausibly serviceable by newly released capacity
preserve progress, `publish_many()` semantics, and terminal teardown without a
weighted admission queue?

## What shipped

Candidate 1 keeps the wait/retry loop and replaces only the wake hint.

- Register a future while still holding `_engine_lock` (same lost-wakeup
  window as the old `clear()`-under-lock).
- `_wake_publish_waiters(n=1)` from `_settle_publish`: pop pending futures
  until one incomplete future is completed. One ACK → one waiter.
- `_notify_publish_space()` still completes every pending future (disconnect,
  writer failure, reconnect-disabled teardown, `_force_close`).
- Cancellation: a still-pending future is discarded; a future that was
  already completed (token consumed) forwards the wakeup to the next waiter.
- `_publish_waiters` remains an `int` for `ClientStats` and existing tests.
- Internal counters `_publish_wakeups` / `_publish_wait_retries` are test and
  harness hints, not public statistics.

`publish_many()` still parks as one waiter for the whole chunk and is woken
once per ACK until `queue_publish_many` succeeds. WritePump notify policy is
untouched. Engine and writer remain the only sources of admission truth: a
woken producer always retries `queue_publish` / `queue_publish_many` under
`_engine_lock`.

## Correctness evidence

`tests/unit/test_publish_targeted_wake.py` (21 passed) plus the unchanged
`tests/unit/test_async_publish_admission.py`:

- existing wait-until-complete behaviour;
- two waiters, one completion → exactly one proceeds;
- two completions can release two waiters;
- cancelling one parked waiter does not steal the only wakeup;
- terminal teardown fails every parked publisher;
- `publish_many()` waits, and a chunk that needs two slots is woken once per
  ACK;
- several producers all complete while completions keep arriving;
- wakeups ≤ completions on the non-teardown path.

## Harness

`benchmarks/paired_publish_admission_contention.py` mirrors
`paired_writer_capacity.py`: `--base-root` / `--candidate-root`, JSON to
`--output`, native `await publish()` with 1/4/16 concurrent tasks by default
and `--publisher-values` / `--inflight-values` for 64/256 or window 1/4/20.
It records completed rate, process CPU, publish-call p50/p95/p99, wakeup/retry
counters when present, and min/max per-producer completions. Do not commit
raw JSON.

## Diagnostic campaign

MQTT 3.1.1 QoS 1, 64 B, `paired_publish_admission_contention.py`.

### Harness A/A (this tree vs this tree)

| Publishers | Inflight | Ratio | Base /s | Cand /s | Base CV | Cand CV |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 0.984 | 19340 | 18920 | 0.99% | 0.85% |
| 1 | 4 | 1.010 | 26260 | 26334 | 1.34% | 2.03% |
| 4 | 1 | 0.996 | 19143 | 18834 | 1.12% | 1.25% |
| 4 | 4 | 0.993 | 24219 | 24089 | 0.49% | 0.73% |
| 16 | 1 | 0.996 | 19357 | 19136 | 0.79% | 1.23% |
| 16 | 4 | 1.004 | 24445 | 24459 | 1.55% | 0.96% |

All cells inside ~2%.

### Harness A/B (`asyncio.Event` vs waiter-future deque)

| Publishers | Inflight | Rate | Base /s | Cand /s | CPU s | Call p50 (ms) | Call p99 (ms) | Wakeups |
| ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| 1 | 1 | 1.015 | 19030 | 19205 | 0.105 → 0.104 | 0.052 → 0.051 | 0.060 → 0.060 | 2192 |
| 1 | 4 | 0.994 | 26292 | 26192 | 0.075 → 0.075 | 0.006 → 0.006 | 0.096 → 0.097 | 872 |
| 4 | 1 | **1.291** | 14898 | 19186 | 0.133 → 0.104 | 0.066 → 0.208 | 0.077 → 0.224 | 2184 |
| 4 | 4 | **1.069** | 22621 | 24174 | 0.088 → 0.082 | 0.006 → 0.006 | 0.121 → 0.525 | 2168 |
| 16 | 1 | **2.269** | 8529 | 19402 | 0.234 → 0.103 | 0.118 → 0.832 | 0.169 → 0.883 | 2136 |
| 16 | 4 | **1.574** | 15307 | 24176 | 0.131 → 0.082 | 0.006 → 0.006 | 0.197 → 1.995 | 2136 |

Four contention cells beat 5%. Low-contention cells stay within 3%. Publish
call p99 **rises** under contention (queueing vs stampede); completed rate and
CPU are the target. Wakeups exist only on the candidate (~2100 per cell) and
match wait-retries: one park, one wake, one retry.

### Writer-capacity guard vs `main`

| QoS | Ratio | Base /s | Cand /s | Base CV | Cand CV |
| --- | ---: | ---: | ---: | ---: | ---: |
| 0 | 1.003 | 277797 | 278164 | 1.74% | 0.75% |
| 1 | 1.006 | 46504 | 46784 | 1.02% | 1.11% |

Open-loop/network cells were not run for this PR on this host. They remain
part of the contract's eligible-runner gate.

## Complexity and risk

The extra state is a deque of futures plus two integers. That is more than a
single Event, but far less than a second admission scheduler: waiters never
own slots, budgets, or packet ids.

The subtle piece is cancellation forwarding. Getting it wrong either leaks a
wakeup (one waiter starved until a later ACK) or double-wakes (harmless retry,
`FlowControlError`, park again). Tests cover the steal case.

FIFO order of the deque is incidental fairness, not a promised policy. A later
weighted queue would have to beat this candidate on the experiment's own gate
before paying for that state.

Hot-path cost when nobody is waiting is still an integer check in
`_settle_publish`. The wait path allocates one Future per park, which is paid
only under contention.

## What is still open

1. Eligible-runner A/A then A/B, including 64/256 publishers if the default
   matrix recovers.
2. Open-loop / `paired_network.py` as non-targeted guards on that host.
3. No weighted/FIFO admission queue unless this candidate fails fairness.

Keep the candidate. Merge is blocked on an eligible runner, not on further
code.
