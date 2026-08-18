# Scheduler experiment: targeted writer waiter wakeups

Baseline: `main@e80618154bacefed5626d9eb9ba46edf560b54e7`.

## Hypothesis

`WritePump` currently calls `Condition.notify_all()` whenever a completed batch releases writer capacity. With many blocked producers, one capacity release can make every waiter runnable even though only a subset can be admitted, causing avoidable event-loop wakeups and lock contention.

## Candidate

Start with the smallest possible change: normal capacity release wakes only one writer waiter. Lifecycle/epoch transitions must continue to wake all waiters so stale waiters cannot remain parked. Do not introduce an explicit FIFO/weighted waiter structure unless this minimal candidate demonstrates a real benefit but exposes fairness or heterogeneous-size limitations.

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
