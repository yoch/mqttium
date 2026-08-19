# Scheduler experiment: strict resident writer message budget

Baseline: `main@e80618154bacefed5626d9eb9ba46edf560b54e7`.

## Hypothesis

`WritePump.max_messages` currently gates `queue.qsize()`, while the writer can remove up to 256 items into its active batch before those items are written. The byte budget remains charged during that interval, but the message-count budget can temporarily admit more resident frames than configured.

## Candidate

Introduce one authoritative resident/admitted message counter that is incremented only when an item is actually queued and decremented only when the item has completed or is discarded. Eager writes must not consume this budget. Batch extraction must not reduce the counter. Keep the public queue semantics, wire order, byte accounting, eager fast path, and latency microbatch behavior unchanged.

This is primarily an invariant/memory-bound improvement, not a throughput optimisation. It should be rejected if the stricter invariant requires material hot-path complexity or causes measurable performance loss.

## Required correctness evidence

- `resident_messages <= max_messages` throughout an active 256-item writer batch.
- Atomic `try_enqueue_many()` admission against the resident count.
- Exact decrement on normal write completion, latency microflush, discard/reset, cancellation/failure cleanup, and epoch transitions.
- A resident over-release must fail as an invariant violation rather than be silently clamped.
- Existing oversized-single-item semantics remain unchanged.
- Existing writer statistics remain well defined; if `queued_messages` keeps its current meaning, expose no misleading renamed metric.

## Performance evidence

Use an eligible runner and the repository benchmark contract.

1. A/A control on the baseline using `benchmarks/paired_writer_capacity.py`.
2. A/B baseline vs candidate for QoS 0 and QoS 1, 256-byte payloads, inflight 20, outstanding 64, max queued 200, repeat >= 8.
3. `benchmarks/memory_profile.py` / logical counters to demonstrate that the configured count bound is now strict under tiny-frame pressure.
4. Advisory open-loop/network runs to verify no latency or loop-lag regression.

## Acceptance gate

Because the target benefit is a stricter invariant rather than raw speed:

- zero public semantic regression;
- strict resident count demonstrated;
- impossible resident accounting states fail locally instead of being normalized;
- candidate completed rate >= 97% of baseline on non-targeted writer cells;
- loop lag increase <= 5%;
- memory limits unchanged or improved;
- code complexity must stay small enough that the invariant is directly auditable.

Do not merge from this experiment until the benchmark artefacts and a complexity/risk assessment are attached to the PR discussion.

## Outcome

Candidate implemented in this worktree: one `_resident_messages` counter,
incremented only after an item is actually queued and decremented only when
the item has completed or is discarded. Eager writes do not consume it; batch
extraction does not reduce it. `queued_messages` remains `queue.qsize()`.
`can_enqueue_size` / `enqueue` / `try_enqueue_many` / nowait preflight admit
against the resident count.

Accounting hardening on 2026-08-19 removed the original saturating release:
`_release_resident` now raises `AssertionError` if code attempts to release
more frames than the pump owns. CI exposed test fixtures that had bypassed the
owner by mutating `queue` / `_outbound` directly; those fixtures now seed work
through `try_enqueue` / `_try_enqueue_outbound`, preserving the runtime
invariant while testing the same failure/cleanup behavior. The connection
lifecycle already invalidates/stops the old writer before reset of a new epoch,
so cross-epoch underflow is not a legitimate state to normalize.

Correctness coverage is in `tests/unit/test_write_pump_resident.py`, the core
writer tests, eager/failure tests, and memory-cleanup lifecycle tests.

Diagnostic A/B on an ineligible cloud VM (2026-08-19), measured before the
release-assertion hardening: writer-capacity QoS 0/1 ratios 0.999 / 1.000;
open-loop completed/lag ~1.00. Directional: no throughput or lag cost. The
current hardened HEAD still requires eligible-runner A/A+A/B before merge.

Complexity/risk: the change remains one integer plus small helpers on paths
that already mutate `queued_bytes`. The main risk is a leak if a new completion
path forgets `_release_resident`; an over-release is now detected rather than
hidden. `reset`/`discard` and `_run`'s existing `finally` cover epoch transitions,
mid-batch cancel, and transport failure. No extra scheduler machinery. Do not
merge until the eligible-runner artefacts exist.

Published report:
[`docs/reports/SCHEDULER-RESIDENT-MESSAGE-BUDGET-2026-08-19.md`](../reports/SCHEDULER-RESIDENT-MESSAGE-BUDGET-2026-08-19.md).
