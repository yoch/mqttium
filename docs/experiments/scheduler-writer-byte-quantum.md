# Scheduler experiment: byte-bounded writer quantum

Baseline: `main@e80618154bacefed5626d9eb9ba46edf560b54e7`.

## Hypothesis

`WritePump._run()` currently caps a worker batch at 256 items but not by bytes. The scheduling quantum is therefore similar for 256 tiny frames and 256 much larger frames even though the latter can monopolize the writer for far longer. A second byte cap may improve control-packet and completion tail latency under mixed traffic without materially reducing throughput.

## Candidate

Keep the existing 256-item ceiling and add one byte ceiling while building a writer batch. Always admit the first item even if it exceeds the byte ceiling, then stop before adding a later item that would cross the quantum. Preserve FIFO order: `asyncio.Queue` has no peek/putleft, so a leftover is stored in `WritePump._held` and becomes the first item of the next batch. Do not introduce priorities or packet-type reordering.

The first candidate uses one fixed byte quantum rather than an adaptive policy. Adaptive/window-aware scheduling is a separate hypothesis and is not mixed into this change.

### Selected quantum

**64 KiB** (`_WRITER_BATCH_MAX_BYTES`). Middle of the screening set, and distinct from the existing 48 KiB latency-microbatch target (`_LATENCY_BATCH_TARGET_BYTES`), which is a different mechanism and is not coupled to this cap.

### Rejected screening points

32 KiB, 128 KiB and 256 KiB are **not yet measured on an eligible runner**. They remain screening alternatives, not code paths. `benchmarks/writer_byte_quantum_screen.py` can vary the constant locally; it is diagnostic and must not commit result JSON.

## Required correctness evidence

- Exact FIFO wire order for plain and segmented `WriteItem` values.
- A first oversized item still progresses.
- No item is lost or duplicated at the byte-boundary cut.
- Queue/task accounting, `queued_bytes`, batch statistics, eager-write exclusion, and waiter notifications remain exact.
- `queued_messages` counts a held leftover (`qsize + 1`), and eager write refuses while `_held` is set.
- No new transport `drain()` behavior.

Covered by `tests/unit/test_write_pump_byte_quantum.py`.

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

## Outcome

Correctness candidate implemented: one named 64 KiB ceiling beside the existing 256-item cap, with a leftover slot so FIFO is preserved. Diagnostic campaign 2026-08-19 (ineligible cloud VM): writer-capacity 0.980 / 1.012, open-loop completed/lag ~1.00, mixed QoS 1 tail p50 9.47 → 7.46 ms under a 32 KiB flood. 32/128/256 KiB remain screen-only. Eligible `network_release_gate.py` still required.

## Complexity and risk

Small local extension of the writer batching loop: one constant, one `_held` slot, and honest `queued_messages` / eager / discard / reset accounting. No packet-type priorities, no `drain()`, no adaptive window. Principal risks are FIFO (never `put_nowait` a leftover) and join-counter leaks on discard of a held item; both have unit tests. Throughput and tail-latency impact under mixed traffic is the open measurement question.
