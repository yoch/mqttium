# Scheduler experiment: targeted writer waiter wakeups

Baseline: `main@e80618154bacefed5626d9eb9ba46edf560b54e7`.

## Hypothesis

`WritePump` currently calls `Condition.notify_all()` whenever a completed batch releases writer capacity. With many blocked producers, one capacity release can make every waiter runnable even though only a subset can be admitted, causing avoidable event-loop wakeups and lock contention.

## Candidate

Start with the smallest possible change: normal capacity release wakes only the waiters that can use the slots just freed (`notify(n)` with `n = min(waiters, items released)`; `notify(1)` is the degenerate case when the batch size is 1). Lifecycle/epoch transitions must continue to wake all waiters so stale waiters cannot remain parked. Do not introduce an explicit FIFO/weighted waiter structure unless this minimal candidate demonstrates a real benefit but exposes fairness or heterogeneous-size limitations.

## Required correctness evidence

- No lost wakeups when capacity is released.
- Every blocked producer eventually progresses while capacity continues to become available.
- Epoch advance, discard/teardown, cancellation, and transport failure cannot strand waiters.
- Wire order, eager-write exclusion while waiters exist, and byte/message admission semantics remain unchanged.
- Cancellation of one waiter does not consume the only useful wakeup permanently.

## Performance evidence

The target is contention, so single-producer throughput is a guardrail rather than the primary metric.

1. Add/run a paired writer-waiter contention microbenchmark with 1, 4, 16, 64, and 256 concurrent producers against deliberately tight writer capacity. Record completed operations/s, CPU time, enqueue suspensions, wake/retry count, and p50/p95/p99 enqueue wait.
2. Validate the new harness A/A before using it for an A/B claim.
3. Run `paired_writer_capacity.py` as a non-regression guard for the established QoS 0/1 writer regime.
4. Run open-loop/network cells to ensure the reduced wake policy does not trade contention wins for latency regressions.

## Acceptance gate

- Target contention benchmark improves by >= 5% at at least two meaningful concurrency points, or removes a demonstrably large number of redundant wakes with >= 2% CPU/latency gain in a directly isolated microbenchmark.
- Candidate wins the majority required by `docs/BENCHMARKING.md` and the harness A/A control passes.
- Single/few-producer throughput does not regress by > 3%.
- Loop lag does not regress by > 5%.
- No fairness/starvation regression in focused tests.
- If `notify(1)` is insufficient, any FIFO/weighted replacement must justify its extra state with stronger measured evidence.

Do not merge until the benchmark artefacts and a complexity/risk assessment are attached to the PR discussion.

## Outcome

**Candidate implemented: `notify(n)` on normal capacity release, `notify_all()` on lifecycle.** This is still the minimal targeted-wake policy, not a FIFO/weighted waiter scheduler.

`notify(1)` is the experiment's original smallest change, but after a writer batch of size `B` it would leave `B-1` free slots while other waiters stay parked until the next write completes. That still progresses, but it serializes producers and is likely a throughput regression. The implemented policy wakes `n = min(self.waiters, len(batch))` — one waiter per message slot the batch just freed.

Lifecycle/epoch paths are unchanged:

- `wake_waiters()` / `advance_epoch()` still `notify_all()`.
- Client discard / stop / transport-failure teardown still broadcasts through those methods so stale waiters cannot remain parked.

Lost-wakeup hand-off: if a waiter is cancelled after consuming a `notify(n)` token, `enqueue`'s wait loop `notify(1)`s another waiter when `waiters > 1`.

**Tests.** `tests/unit/test_write_pump_targeted_wake.py` covers single-waiter resume, many waiters completing, epoch/discard/stop/transport-failure, cancellation not eating the only wakeup, no indefinite starvation, and eager-write exclusion while `waiters > 0`.

**Harness.** `benchmarks/paired_writer_waiter_contention.py` is a fresh-process ABBA microbenchmark of concurrent `WritePump.enqueue()` against a tight `max_messages` window. Default concurrency is 1, 4, 16; 64/256 are CLI options. It records completed ops/s, CPU time, `enqueue_suspensions`, and enqueue-wait p50/p95/p99. Raw JSON stays out of git.

**Still required before merge.** Eligible-runner harness A/A, then A/B against the `notify_all()` baseline; `paired_writer_capacity.py` as the established writer-regime guard; open-loop/network cells so a contention win is not paid for in latency. Complexity/risk: [`docs/reports/SCHEDULER-WRITER-TARGETED-WAKE-2026-08-18.md`](../reports/SCHEDULER-WRITER-TARGETED-WAKE-2026-08-18.md).
