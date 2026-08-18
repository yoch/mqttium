# Writer-task byte quantum — first candidate 64 KiB — 2026-08-18

Records the first candidate of
[`../experiments/scheduler-writer-byte-quantum.md`](../experiments/scheduler-writer-byte-quantum.md).

| | |
| --- | --- |
| Date | 2026-08-18 |
| Experiment | Writer-task batch byte cap (`WritePump._run`) |
| Selected quantum | **64 KiB** (`mqttium.api._writer._WRITER_BATCH_MAX_BYTES`) |
| Item ceiling | unchanged 256 (`_WRITER_BATCH_MAX_ITEMS`) |
| Host / preflight | not run; no eligible-runner measurement in this change |
| Verdict | **Correctness candidate only.** Network screening is still required before merge. |

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
| 32 KiB | Not yet measured on an eligible runner; not a code path |
| **64 KiB** | Initial candidate (middle of the screen; distinct from 48 KiB latency microbatch) |
| 128 KiB | Not yet measured on an eligible runner; not a code path |
| 256 KiB | Not yet measured on an eligible runner; not a code path |

`benchmarks/writer_byte_quantum_screen.py` can monkeypatch the constant for a
local diagnostic. Do not commit its JSON.

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

Implement the 64 KiB candidate so the hypothesis can be measured. Do not treat
this report as a merge decision: A/A, `paired_writer_capacity.py`,
`paired_open_loop.py`, mixed-load PUBACK/control tails, and
`network_release_gate.py` (or the calibrated equivalent) are still required.
