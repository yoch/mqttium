# Scheduler experiment: targeted publish admission wakeups

Baseline: `main@e80618154bacefed5626d9eb9ba46edf560b54e7`.

## Hypothesis

All `publish()` callers blocked by protocol admission currently wait on one `asyncio.Event`. A single ACK can set that event and make every blocked publisher runnable even when only one or a few protocol slots became available. Under high producer concurrency this can create a thundering herd around `_engine_lock` and inflate CPU and tail latency.

## Candidate

Replace wake-all admission with the smallest mechanism that wakes only the number of publishers plausibly serviceable by newly released capacity. Preserve `publish_many()` semantics and terminal teardown wakeups. Do not add weighted/FIFO admission state unless a simpler targeted-wake design demonstrates measurable benefit but cannot provide adequate fairness.

## Required correctness evidence

- No lost wakeups if capacity is released before, during, or immediately after a producer begins waiting.
- Disconnect, writer failure, reconnect failure, and final teardown wake every blocked publisher so terminal errors propagate.
- Multiple capacity releases can wake multiple producers without requiring an unrelated future ACK.
- `publish()` and `publish_many()` remain bounded and cannot starve indefinitely while releasable capacity exists.
- Packet-id/flow limits and writer admission remain authoritative; the wake mechanism must not become a second source of protocol state.

## Performance evidence

1. Add/run a paired native async contention benchmark using 1, 4, 16, 64, and 256 concurrent publisher tasks with deliberately small inflight windows (for example 1, 4, 20) so admission wait is exercised.
2. Record completed rate, process CPU, p50/p95/p99 publish-call latency, scheduler wake/retry counts, loop lag, and fairness across producers.
3. Validate the harness with A/A before A/B claims.
4. Run existing `paired_open_loop.py`, `paired_network.py`, and `paired_writer_capacity.py` as non-targeted guards.

## Acceptance gate

- >= 5% reproducible gain at at least two contention/load points, or >= 2% isolated scheduler gain with a large demonstrated reduction in redundant wakeups, following `docs/BENCHMARKING.md`.
- No throughput regression > 3% in low-contention/non-targeted cells.
- Loop lag regression <= 5% where the metric is in a comparable pacing regime.
- No starvation/fairness regression and no terminal-wakeup regressions.
- Any explicit FIFO/weighted waiter queue must justify its extra state with stronger evidence than a simple targeted wake.

Do not merge until benchmark artefacts and the complexity/risk assessment are attached to the PR discussion.
