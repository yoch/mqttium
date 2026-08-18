# Scheduler experiment: byte-bounded writer quantum

Baseline: `main@e80618154bacefed5626d9eb9ba46edf560b54e7`.

## Hypothesis

`WritePump._run()` currently caps a worker batch at 256 items but not by bytes. The scheduling quantum is therefore similar for 256 tiny frames and 256 much larger frames even though the latter can monopolize the writer for far longer. A second byte cap may improve control-packet and completion tail latency under mixed traffic without materially reducing throughput.

## Candidate

Keep the existing 256-item ceiling and add one byte ceiling while building a writer batch. Always admit the first item even if it exceeds the byte ceiling, then stop before adding a later item that would cross the quantum. Preserve FIFO order by leaving the non-selected item in the queue; do not introduce priorities or packet-type reordering.

The first candidate should use one fixed byte quantum selected by screening rather than an adaptive policy. Adaptive/window-aware scheduling is a separate hypothesis and should not be mixed into this PR.

## Required correctness evidence

- Exact FIFO wire order for plain and segmented `WriteItem` values.
- A first oversized item still progresses.
- No item is lost or duplicated at the byte-boundary cut.
- Queue/task accounting, `queued_bytes`, batch statistics, eager-write exclusion, and waiter notifications remain exact.
- No new transport `drain()` behavior.

## Performance evidence

Screen a small set of byte quanta (for example 32, 64, 128, 256 KiB) without committing multiple runtime policies. Select one candidate only if it shows a stable frontier.

For the selected candidate:

1. Validate harness A/A on an eligible runner.
2. Run `paired_writer_capacity.py` for QoS 0/1 closed-loop throughput.
3. Run `paired_open_loop.py` at fixed rates with callback and receipt completion, including small and 4 KiB payloads and multiple inflight windows.
4. Run `network_release_gate.py` or the calibrated equivalent for authoritative network no-regression evidence.
5. Include a mixed-load diagnostic that keeps outbound traffic saturated while observing PUBACK/control scheduling p95/p99.

## Acceptance gate

- A network optimisation needs >= 5% reproducible gain at at least two relevant load points under `docs/BENCHMARKING.md`, unless the benefit is explicitly tail-latency-only and is comparably strong/stable.
- Non-targeted completed rate must stay within -3%.
- Loop lag must not increase > 5% in comparable pacing regimes.
- Writer-capacity QoS 0/1 floors must remain satisfied.
- Added code should be a small local extension of the existing batching loop; reject if a more complex scheduler is needed for a marginal result.

Do not merge until the selected quantum, rejected screening points, raw benchmark artefacts, and complexity/risk assessment are attached to the PR discussion.
