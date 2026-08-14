# Independent QoS 0 and latency audit — 2026-08-14

## Scope and evidence boundary

This audit covers MQTTium at commit `702fbd6` plus the working-tree audit
changes described below. It does not use measurements, classifications or
adapter behaviour from the external cross-client benchmark. Those observations
are treated only as hypotheses that motivated an independent code and contract
review.

The functional evidence is MQTTium's own source, unit tests and live Mosquitto
integration suite. Performance evidence requires this repository's runner
preflight and direct `AsyncClient` benchmark path; no cross-client adapter is
part of that path.

## Decision

There is no MQTTium correctness defect to fix in `publish_nowait()`.

- `publish_nowait()` is deliberately non-suspending and raises
  `FlowControlError` when the logical or encoded-writer budget is full.
- Refusal occurs before packet-identifier allocation or protocol-store commit;
  the existing atomicity tests remain green.
- QoS 0 has no broker acknowledgement. Its receipt and `on_publish` callback
  mark admission to the writer queue, not `transport.write()`, socket drain or
  broker receipt.
- A QoS 0 callback therefore cannot be used as a socket-level outstanding-byte
  gate. A producer that must wait for capacity uses `await publish()`; a
  non-suspending producer owns its shed, retry or spill policy.
- The 1 MiB default writer-byte bound remains unchanged. It is a Stable memory
  safety default, and an empty writer queue still admits one oversized item to
  preserve forward progress.

No production API, default, callback execution discipline, TCP option or native
MID semantics changed as a result of this audit. A scheduling optimization for
terminal QoS 1/2 completion was retained after the independent investigation
below.

## Changes retained

- `docs/OPERATIONS.md` now states the QoS 0 completion boundary and the
  operational consequence for counters and large-payload queue sizing.
- A native regression test proves that the QoS 0 callback may complete while
  the encoded frame remains queued, and that the next full-queue refusal is
  atomic.
- `paired_open_loop.py` now supports fixed absolute target rates and
  outbound-window sweeps while preserving the existing calibrated-fraction
  default. It records the window and load basis in every scenario.
- Callback samples retain a FIFO of timestamps per packet identifier. The first
  direct run exposed the previous one-timestamp map when Mosquitto acknowledged
  quickly enough to reuse a MID before all queued callback observations were
  consumed; the harness now measures that legal reuse instead of failing with a
  `KeyError`. Worker failures also surface their stderr in the parent process.
- Passing the same source root as base and candidate marks an A/A control and
  invalidates a completed-rate ratio outside 1 +/- 2%. Baseline completed-rate
  and p50-latency CV must each remain at or below 5%.
- Terminal QoS 1/2 completion now admits `on_publish` directly to its bounded
  callback queue when capacity is immediately available and settles the receipt
  inline. Callback code still runs only in the isolated worker. If the queue is
  full, the unchanged EffectPump path retains ordering and backpressure. QoS 0
  deliberately retains its existing writer-admission ordering.

The fixed-rate path calls `AsyncClient` directly and retains the separate
`mosquitto_sub` observer that verifies exact delivery and sequence outside the
publisher process.

## Validation

Environment: CPython 3.12.13, Mosquitto 2.0.18, Linux 6.8 low-latency kernel.

- Ruff formatting and lint: passed.
- mypy over `src/mqttium`: passed, 62 source files.
- Bandit high/medium severity scan: passed.
- Unit suite: **1,019 passed**, **88.95%** source coverage (80% gate).
- Live Mosquitto integration suite on `127.0.0.1:11883`: **15 passed**, no
  skips.
- Fuzz campaigns: codec, engine and WebSocket each completed 20,000 iterations
  with zero crashes or invariant violations; Hypothesis suite: **4 passed**.
- Full versioned memory campaign: all **15 scenarios** remained within their
  tracemalloc and logical-counter limits.

## Independent latency investigation

The direct path used `AsyncClient`, a local Mosquitto 2.0.18 broker and a
`mosquitto_sub` observer in a separate process. It used no compatibility facade,
cross-client adapter or external benchmark data. Initial diagnostics were
correctly rejected while the host load exceeded the runner limit. The retained
evidence below was collected after a strict probe passed with load/core
`0.239`, CPU `8.1%`, a performance governor and temperature below the configured
limit.

The pre-change code path explains the difference:

1. Without `on_publish`, a `PUBLISH_COMPLETE` effect settles its receipt inline
   in the reader task.
2. With `on_publish`, the same effect leaves the inline path, enters
   `EffectPump`, wakes its task, settles the receipt there, enqueues a callback,
   and is finally observed by the isolated callback worker.
3. Under load the reader repeatedly waits for effect-pump progress. In four
   5,000-message/s diagnostic pairs, the current path enqueued roughly
   6,200-6,700 effects and suspended roughly 4,900-5,900 times per sample.

The narrow correction enqueues the callback without suspending when the bounded
queue has room, settles the receipt inline, and retains the existing
asynchronous effect path when the queue is full. The callback still executes
only in the callback worker, so user-code isolation and callback backpressure
remain intact.

The same-tree A/A control at 5,000 messages/s passed: baseline p50 CV `2.42%`,
completed-rate ratio `0.9997`, loop-lag ratio `1.0221`, and complete ordered
delivery. At 10,000 messages/s, completed rate (`1.0003`) and loop lag (`1.0204`)
remained neutral, but callback p50 CV reached `25.93%`; that latency cell is
diagnostic rather than acceptance evidence.

The short alternating A/B then compared the frozen pre-change tree with the
candidate directly:

| Target | Before callback p50 | After callback p50 | Completed-rate ratio | Winning pairs |
| ---: | ---: | ---: | ---: | ---: |
| 5,000 msg/s | 0.532 ms | 0.249 ms | 1.0062 | 6/6 |
| 10,000 msg/s | 28.413 ms | 0.512 ms | 1.0715 | 6/6 |

At 5,000 messages/s the pre-change baseline CV was `1.53%`, so this is valid
release evidence and exceeds the 5% improvement requirement without a
throughput or loop-lag regression. At 10,000 messages/s the old path was
bimodal (`0.736` to `65.229 ms`) and its CV was `71.92%`; the absolute magnitude
is therefore not a valid precise comparison. It is still useful saturation
evidence: the candidate stayed between `0.477` and `0.520 ms`, won all six
pairs, completed all deliveries in order, and did not regress throughput or
loop lag.

A single targeted 7,500-message/s follow-up was used to test whether a second
stable network point existed below saturation. It did not: the old path ranged
from `0.564` to `5.572 ms` (CV `109.58%`), while the candidate stayed between
`0.356` and `0.412 ms` and won all six pairs. No lower target was searched for;
doing so would select a convenient point rather than characterize the observed
phase transition.

An exact isolated profile supplies the neutral and mechanism controls. Receipt
completion was unchanged at `18.81517` calls/op. Callback completion fell from
`56.2855` to `43.40164` calls/op (22.9% fewer), and peak traced allocation fell
from `47,544` to `35,848` bytes. Eleven paired microbenchmark samples all
favoured the candidate, with median throughput ratio `1.247`. The full unit,
integration, fuzz and memory campaigns above cover the semantic and resource
guards.

One apparent Paho saturation timeout was also reproduced against the frozen
tree and traced to the restricted test sandbox blocking the asyncio self-pipe.
The same scenario outside that sandbox completed normally, and the full memory
campaign passed; it is not a product regression.

## Performance decision

The callback scheduling path is the independently reproduced bottleneck; the
protocol core is not implicated. The correction is retained because one fully
valid network point shows a large improvement, the overloaded point shows the
same direction in every pair, and exact profiles independently prove removal of
work without moving the receipt-only path. There is no basis for changing
`publish_nowait()`, QoS 0 completion, public API, resource defaults or protocol
state.

The professional recommendation is **keep this narrow QoS 1/2 callback-handoff
optimization and stop here**. A wider payload/window matrix may characterize
where saturation begins, but it is not required to justify this mechanism-level
fix and should not delay it. Receipt-task latency should not be used as a neutral
network control until that observer passes its own A/A; exact hot-path profiles
already provide the appropriate neutral control for this change.
