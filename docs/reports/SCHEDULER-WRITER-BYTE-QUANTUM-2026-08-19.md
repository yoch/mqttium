# Writer-task byte quantum (64 KiB) — diagnostic campaign 2026-08-19

Records the first candidate of
[`../experiments/scheduler-writer-byte-quantum.md`](../experiments/scheduler-writer-byte-quantum.md)
at the feat commit named below. This file supersedes the 2026-08-18
implementation note
[`SCHEDULER-WRITER-BYTE-QUANTUM-2026-08-18.md`](SCHEDULER-WRITER-BYTE-QUANTUM-2026-08-18.md).

| | |
| --- | --- |
| Date | 2026-08-19 |
| PR | [#286](https://github.com/yoch/mqttium/pull/286) |
| Branch | `agent/scheduler-writer-byte-quantum` |
| Baseline | `main@e80618154bacefed5626d9eb9ba46edf560b54e7` |
| Experiment definition | `bcc1d88cd7a740da93fa43fc57e3cdb757575f16` |
| Commit described | `70f6535a6ff04965a079d1daca07079b620687cc` (`feat(writer): cap writer-task batches at 64 KiB`) |
| Selected quantum | **64 KiB** (`mqttium.api._writer._WRITER_BATCH_MAX_BYTES`) |
| Item ceiling | unchanged 256 (`_WRITER_BATCH_MAX_ITEMS`) |
| Host | 4× Intel Xeon KVM, CPython 3.12.3, Linux 6.12.94+, Mosquitto 2.0.18 on `127.0.0.1:11883` |
| Preflight | **unsuitable** (`CPU governor is unavailable`) |
| Policy | advisory, repeat 4, `--cpu 1` |
| Status | diagnostic only — **not merge evidence** |

## Verdict

**Keep the 64 KiB candidate. Do not merge until `network_release_gate.py` (or
the calibrated equivalent) runs on an eligible runner.**

64 KiB is the interesting screening point: it splits 40 KiB pairs and mixed
32 KiB + control traffic while still packing 256 × 64 B into one batch.
128 KiB and 256 KiB leave those mixes in one batch, so they cannot show a
mixed-tail cut on this set. 32 KiB over-splits (four batches for 10 × 10 KiB).

On this ineligible host, writer-capacity QoS 0 is **0.980** (inside the −3%
floor), QoS 1 is **1.012**, and open-loop completed/lag stay ~1.00. Mixed QoS 1
tail under a 32 KiB QoS 0 flood is directionally faster when arms are
interleaved; sequential candidate runs are bimodal and are not a paired
claim. 32/128/256 KiB remain screen-only constants, not runtime policies.

| Gate | Diagnostic result | Eligible-runner still required |
| --- | --- | --- |
| Network optimisation ≥ 5% at two load points, or a comparably strong tail cut | interleaved mixed tail ~21% p50 (9.47 → 7.46 ms); sequential runs bimodal | **yes, `network_release_gate.py`** |
| Non-targeted completed rate within −3% | writer-capacity QoS 0 **0.980**, QoS 1 **1.012** | yes |
| Loop lag ≤ 5% | 0.998–1.005 across four open-loop cells | yes |
| FIFO / leftover accounting | unit tests | no, already shown |
| One runtime quantum | 64 KiB; 32/128/256 screen-only | confirm 64 KiB on eligible host |

## Question

`WritePump._run` already stops a worker batch at 256 items. It did not stop
by bytes, so 256 large frames could monopolize the writer for much longer
than 256 tiny frames. Does adding one fixed byte ceiling, while keeping FIFO,
improve control-packet / completion tail latency under mixed traffic without
a more complex scheduler?

## What shipped

The writer task still takes at most 256 items. While building that batch it
also stops before adding a later item that would push the batch over 64 KiB.
The first item is always admitted, even if it is larger than the quantum.

`asyncio.Queue` has no peek or put-left. An item that does not fit is stored
in `WritePump._held` and becomes the first item of the next batch.
`task_done()` runs only after that item is actually written.
`queued_messages` is `queue.qsize() + (1 if _held else 0)` so admission and
stats stay honest. Eager write and the 48 KiB latency microbatch refuse while
`_held` is set: the writer is not idle. `discard()` `task_done()`s the
leftover; `reset()` abandons it with the old queue.

The 48 KiB latency-microbatch target is a different mechanism and is not
coupled to this constant. No packet-type priorities, no transport `drain()`,
no adaptive/window-aware scheduling.

## Screening points

The contract's set is 32, 64, 128 and 256 KiB. This change implements **one**
runtime value. Local screen: `benchmarks/writer_byte_quantum_screen.py`.

| Quantum | 40 KiB pair | 10 × 10 KiB | 256 × 64 B | Mixed 32 KiB + control | Status |
| --- | ---: | ---: | ---: | ---: | --- |
| 32 KiB | 2 batches | 4 batches | 1 batch | 4 batches | screen only; more cuts than 64 KiB |
| **64 KiB** | 2 | 2 | 1 | 2 | **selected** |
| 128 KiB | 1 | 1 | 1 | 1 | screen only; no mixed-tail cut |
| 256 KiB | 1 | 1 | 1 | 1 | same as 128 KiB on this mix set |

## Correctness evidence

`tests/unit/test_write_pump_byte_quantum.py` (38 passed):

- FIFO wire order for plain bytes and segmented tuple `WriteItem`s at a byte
  cut;
- a first oversized item still progresses;
- no loss or duplication across the hold / next-batch boundary;
- `queued_bytes == 0`, `join()` complete, `_held is None` after a split;
- eager write does not overtake a held leftover;
- `discard()` / `reset()` clear held without leaking `join()`;
- 40 KiB + 40 KiB → two batches of one; ten slices under 64 KiB → one batch;
- 257 one-byte items still split on the 256-item ceiling.

## Diagnostic campaign

### Writer-capacity A/B vs `main` (`paired_writer_capacity.py`, 311, 256 B, inf 20, out 64)

| QoS | Ratio | Base /s | Cand /s | Base CV | Cand CV |
| --- | ---: | ---: | ---: | ---: | ---: |
| 0 | **0.980** | 271298 | 265819 | 2.41% | 1.19% |
| 1 | 1.012 | 46521 | 46800 | 1.69% | 0.68% |

QoS 0 sits 2.0% below baseline, inside the −3% non-targeted floor. It is the
cell to watch on an eligible host.

### Open-loop A/B (`paired_open_loop.py`, 311 / callback / window 20)

| Payload | Offered | Completed ratio | Loop-lag p95 ratio | ACK p50 base → cand (ms) |
| ---: | ---: | ---: | ---: | --- |
| 64 B | 2500/s | 0.999 | 0.998 | 1.262 → 1.266 |
| 64 B | 7500/s | 0.992 | 0.999 | 0.279 → 0.280 |
| 4 KiB | 2500/s | 0.998 | 1.005 | 1.268 → 1.269 |
| 4 KiB | 7500/s | 1.000 | 1.002 | 0.286 → 0.292 |

Completed-rate and lag floors are met. The 4 KiB / 7500/s ACK p50 move
(+2.2%) is noise-scale on this host, not a tail claim.

### Null-transport mixed drain (2000 × 32 KiB + 64 B)

| Arm | Median batches | Median drain |
| --- | ---: | --- |
| `main` (no byte cap) | 16 | 2.43 ms |
| 64 KiB candidate | 2000 | 5.21 ms |

More, smaller writer turns. Expected: each 32 KiB + 64 B pair is two batches
once 64 KiB is the quantum. This is a scheduling-shape check, not a
throughput win.

### Mixed QoS 1 tail under a 32 KiB QoS 0 flood

Local helper `mixed_qos1_tail.py` (not a repository harness): 4 flooder tasks,
QoS 0 32 KiB saturating the writer, QoS 1 64 B probes.

**Sequential 4 × 300 probes** (not ABBA; files
`/tmp/mqttium-bench/mixed_tail_{main,quantum}.txt`):

| Arm | Run 1 p50/p99 | Run 2 | Run 3 | Run 4 |
| --- | --- | --- | --- | --- |
| `main` | 9.70 / 9.95 | 9.59 / 9.99 | 9.61 / 9.89 | 9.52 / 10.54 |
| 64 KiB | 7.77 / 7.90 | 7.75 / 7.90 | 9.45 / 9.71 | 9.42 / 9.63 |

The candidate is bimodal once the flood and probe are not interleaved across
arms. Sequential medians are not a paired claim.

**Interleaved ABBA, 4 pairs × 200 probes** (campaign pairing, same helper):

| Arm | p50 ms | p99 ms |
| --- | ---: | ---: |
| `main` | 9.47 | 9.82 |
| 64 KiB candidate | 7.46 | 7.60 |

About 21% faster on both p50 and p99 when the two arms share the host in
ABBA order. That is the directional mixed-tail signal. It is **not**
`network_release_gate.py` and it is not merge-quality.

## Complexity and risk

Small local extension of the existing batching loop: one named constant and
one leftover slot. Load-bearing risks, all unit-tested:

- putting a leftover back with `put_nowait` would append it at the tail;
- `queue.qsize()` undercounts by one while `_held` is set;
- eager write while `_held` is set would overtake the leftover;
- `discard()` must `task_done()` a held item or `join()` leaks.

Open measurement risks:

- whether 64 KiB is the right quantum on an eligible runner;
- whether the mixed-tail gain survives `network_release_gate.py` without
  pushing writer-capacity QoS 0 through the −3% floor;
- sequential mixed-tail bimodality: pairing is mandatory, not optional.

## What is still open

1. Eligible-runner A/A, then `paired_writer_capacity.py`,
   `paired_open_loop.py`, and `network_release_gate.py`.
2. A mixed-load cell on that host that keeps outbound traffic saturated while
   observing PUBACK/control p95/p99, with interleaved pairing.
3. Leave 32/128/256 KiB as screen-only unless 64 KiB fails that gate.

Keep 64 KiB. Merge is blocked on an eligible runner, not on a second
runtime policy.
