# Writer-task byte quantum — first candidate 64 KiB — 2026-08-18

Records the first candidate of
[`../experiments/scheduler-writer-byte-quantum.md`](../experiments/scheduler-writer-byte-quantum.md).

| | |
| --- | --- |
| Date | 2026-08-18 |
| Experiment | Writer-task batch byte cap (`WritePump._run`) |
| Selected quantum | **64 KiB** (`mqttium.api._writer._WRITER_BATCH_MAX_BYTES`) |
| Item ceiling | unchanged 256 (`_WRITER_BATCH_MAX_ITEMS`) |
| Host / preflight | ineligible cloud VM (governor unavailable), 2026-08-19 diagnostic |
| Verdict | **Diagnostic keep for 64 KiB.** Eligible-runner `network_release_gate.py` still required. |

## Question

`WritePump._run` already stops a worker batch at 256 items. It did not stop by
bytes, so 256 large frames could monopolize the writer for much longer than 256
tiny frames. Does adding one fixed byte ceiling, while keeping FIFO, improve
control-packet / completion tail latency under mixed traffic without a more
complex scheduler?

## What changed

The writer task still takes at most 256 items. While building that batch it also
stops before adding a later item that would push the batch over 64 KiB. The
first item is always admitted, even if it is larger than the quantum.

`asyncio.Queue` has no peek or put-left. An item that does not fit is stored in
`WritePump._held` and becomes the first item of the next batch. `task_done()`
runs only after that item is actually written. `queued_messages` is
`queue.qsize() + (1 if _held else 0)` so admission and stats stay honest.
Eager write and the latency microbatch refuse while `_held` is set: the writer
is not idle. `discard()` `task_done()`s the leftover; `reset()` abandons it with
the old queue.

The 48 KiB latency-microbatch target is a different mechanism and is not coupled
to this constant.

## Screening points

The contract's set is 32, 64, 128 and 256 KiB. This change implements **one**
runtime value.

| Quantum | Status |
| --- | --- |
| 32 KiB | Diagnostic screen only: splits 10×10 KiB into 4 batches; more cuts than 64 KiB |
| **64 KiB** | Selected: splits 40 KiB pairs and mixed 32 KiB+control; keeps 256×64 B as one batch |
| 128 KiB | Diagnostic screen: all mixes in this set fit in one batch (no mixed-tail cut) |
| 256 KiB | Same as 128 KiB on this mix set |

Local screen (`benchmarks/writer_byte_quantum_screen.py`) plus paired
diagnostics on an ineligible 4-CPU cloud VM, 2026-08-19 (advisory, repeat 4):

- Writer-capacity: QoS 0 **0.980** (inside the −3% floor), QoS 1 **1.012**.
- Open-loop 311/callback/window 20: completed 0.992–1.000, lag 0.998–1.005.
- Null-transport mixed drain (2000 × 32 KiB+64 B): 16 batches / 2.43 ms → 2000
  batches / 5.21 ms. More, smaller turns; expected.
- ABBA mixed QoS 1 tail under a 32 KiB QoS 0 flood (4 pairs, 200 probes):
  main p50/p99 **9.47 / 9.82 ms**, candidate **7.46 / 7.60 ms** (~21% faster).

64 KiB is the interesting screening point: 128/256 KiB do not split this mixed
traffic. Confirm with `network_release_gate.py` on an eligible runner.

## Correctness

`tests/unit/test_write_pump_byte_quantum.py` pins:

- FIFO wire order for plain bytes and segmented tuple `WriteItem`s at a byte cut
- a first oversized item still progresses
- no loss or duplication across the hold / next-batch boundary
- `queued_bytes == 0`, `join()` complete, `_held is None` after a split
- eager write does not overtake a held leftover
- `discard()` / `reset()` clear held without leaking `join()`
- 40 KiB + 40 KiB → two batches of one; ten slices under 64 KiB → one batch;
  257 one-byte items still split on the 256-item ceiling

## Complexity and risk

This is a small local extension of the existing batching loop: one named
constant and one leftover slot. No packet-type priorities, no transport
`drain()`, no adaptive/window-aware scheduling.

Load-bearing risks, all unit-tested:

- putting a leftover back with `put_nowait` would append it at the tail
- `queue.qsize()` undercounts by one while `_held` is set
- eager write while `_held` is set would overtake the leftover
- `discard()` must `task_done()` a held item or `join()` leaks

Open risk: whether 64 KiB is the right quantum on an eligible runner, and
whether the tail-latency gain (if any) stays within the
[`../BENCHMARKING.md`](../BENCHMARKING.md) network-optimisation bar without
hurting writer-capacity floors.

## Outcome

Implement the 64 KiB candidate so the hypothesis can be measured. Diagnostic
A/B on an ineligible cloud VM shows no writer-capacity/open-loop regression and
a ~21% mixed QoS 1 tail cut under a 32 KiB flood. Do not merge until
`network_release_gate.py` (or the calibrated equivalent) runs on an eligible
host.
