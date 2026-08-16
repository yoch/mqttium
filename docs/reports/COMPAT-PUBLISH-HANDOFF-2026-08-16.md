# Compat publish handoff — 2026-08-16

Answers Gap A, Gap C and Gap D of
[`FLOORS-NOT-CEILINGS-2026-08-16.md`](FLOORS-NOT-CEILINGS-2026-08-16.md).

| | |
| --- | --- |
| Date | 2026-08-16 |
| Commit described | `a7a9332` (branch `perf/compat-floors-2026-08-16`), on top of `acde00f` |
| Scope | `src/mqttium/compat/paho.py` and its tests and contracts. No change under `protocol/` or `api/` |
| Host | i7-3770, 8 logical CPUs, `performance` governor, Mosquitto 2.0.18, `runner_probe.py` **eligible** (`load_1m_per_cpu` 0.11, `cpu_percent` 4.9, pkg temp 56 °C) |

This is a dated report. It records what was true of `a7a9332`. It is not a
contract; do not edit it to match later code — write a new report instead
(see [`README.md`](README.md)).

## Verdict

Gap A's mechanism is confirmed by direct measurement and is fixed. Gap C is
implemented and its work removal is proven by exact counts; **no local timing
claim is made for it**, because the harness could not pass its own A/A control.
Gap D is implemented. Gap B was not touched.

The report's proposed mechanism for Gap A was **not** used. See "Why not the
proposed mechanism" below.

## Gap A — the mechanism, measured

`benchmarks/compat_qosn_submit_ab.py` reports the drain batch size, which is the
quantity the report predicted would be stuck at 1. It is. Medians of 5 samples
of 2 000 publishes, 400 warmup, on the eligible host above:

| | mean drain batch | submit msgs/s | p50 | `coalesced / callback` |
| --- | ---: | ---: | ---: | ---: |
| Before, 1 producer | **1.00** | 11 602 | 77.6 µs | **0.69×** |
| After, 1 producer | **203.50** | 52 108 | 7.0 µs | **2.84×** |
| Before, 8 producers | 7.27 | 17 431 | 405.2 µs | 0.96× |
| After, 8 producers | 250.00 | 39 435 | 7.3 µs | 2.00× |

`max_batch_size` before was **1** with one producer: not "small", exactly one.
The `coalesced / callback` column is the within-run control — `callback` and
`coroutine` are unchanged code paths measured in the same process — and it is
the honest comparison, because the absolute rates move with the interpreter and
host. Before this change the coalesced queue ran at **0.69× the plain
one-callback-per-message handoff it exists to beat**.

**What this benchmark is not.** It drives a live compatibility loop with a
`CONNECTED` engine and **no broker I/O**. It measures the cross-thread handoff,
not end-to-end throughput. 52 k submits/s is not 52 k publishes/s. The
cross-client floor (compat QoS 1 ≥ Paho's 9.6 k) is an external-harness claim
and is deliberately **not** asserted here.

## Why not the proposed mechanism

The report proposed a thread-safe `PacketIdPool` plus a `packet_id=` parameter
threaded through `engine.queue_publish` / `OutboundSession.queue_publish`. That
was rejected before implementation, for reasons that are checkable:

1. `PacketIdPool` is mutated from about a dozen loop-side sites — `release` at
   `outbound.py:576/630/644/777/986/1000/1035/1084` and `engine.py:462/525/731`,
   `reserve` at `outbound.py:961`, `clear` at `outbound.py:280/1041` and
   `engine.py:522`. Off-loop `allocate()` is only safe if **every** one of those
   takes the same mutex, including `clear()` on session teardown and `reserve()`
   during replay hydration. That is a lock across the protocol core's hottest
   path, to fix a compat-only defect.
2. The new parameter lands in `OutboundSession.queue_publish`, whose own comment
   (`outbound.py:440-443`) records that each extra Python frame there measured
   about 1.5 %.
3. [`COMPAT.md`](../COMPAT.md) §8 states that "engine, store, **packet
   identifiers**, receipts, connection epochs, and effect ordering are committed
   together on the network loop". The proposal carves an exception into a
   documented invariant.

**What was done instead.** The façade mints its own correlation identifier from
a wrapping `1..65535` namespace and returns immediately, binds it to the real
packet identifier when the loop commits, and translates back when `on_publish`
is dispatched. Nothing under `protocol/` or `api/` changed. The blocking future,
its cancellation handling and `_PUBLISH_HANDOFF_TIMEOUT` were deleted, so the
change removes more machinery than it adds.

The cost is a documented divergence: `MQTTMessageInfo.mid` is a correlation
identifier, not the wire packet identifier. No Paho API uses `mid` for anything
but correlating `publish()` with `on_publish`, and the façade already diverged
on mids (QoS 0 → `None`).

## Gap A — behaviour changes

- A late admission refusal is reported as `rc` on the returned handle rather
  than synchronously. `wait_for_publish()` / `is_published()` re-check `rc`
  **after** the handoff resolves; without that they would have returned silently
  on a publication that was never queued. Cross-thread ingress saturation is
  still refused synchronously with `mid=None`.
- Removing the block removes implicit producer backpressure, so
  `MQTT_ERR_QUEUE_SIZE` becomes reachable for QoS 1/2 under sustained overload.
  Observed immediately: a 20 000-message QoS 0 loop against a live broker now
  saturates the 10 000-request ingress bound and the producer must shed. That is
  Paho's shape and the point of the change; it is documented in
  [`COMPAT.md`](../COMPAT.md) and [`OPERATIONS.md`](../OPERATIONS.md).

## Gap C — work removed, not time saved

QoS 0 is now committed through the same writer-direct path the native client
uses, with the effect path as fallback. Loop-side commit, no broker, 1 000
messages:

| per QoS 0 message | before | after |
| --- | ---: | ---: |
| engine effects created | 2.00 | **0.00** |
| effect-pump deque enqueues | 2.00 | **0.00** |
| items reaching the writer queue | 1 000 | 1 000 |

These are exact counts with no variance.

**No timing claim is made.** Four attempts at a paired end-to-end A/B failed
their own A/A control on this host:

| harness | A/A ratio | baseline CV | verdict |
| --- | ---: | ---: | --- |
| unpaired, live broker | — | ~10 % | invalid |
| paired ABBA ×7, live broker | 1.054× | 7.6 % | invalid |
| + publisher pinned (`taskset -c 2`) | 0.967× | 3.6 % / 7.9 % | invalid |
| single-threaded drain micro, ×8 pairs | **1.005×** | 11.5 % | ratio control passes, CV gate fails |

The 7-pair runs were additionally biased by an order effect — the first process
in each pair is faster, and an odd pair count gives one arm the lead more often.
Balancing to 8 pairs removed the bias (4/8 wins) but not the spread. Per
[`BENCHMARKING.md`](../BENCHMARKING.md), a benchmark that cannot satisfy its own
A/A control is diagnostic only, so the +16 % first seen in the unpaired run is
**not** reported as a result. Capacity remains an external-harness question.

Ordering across QoS levels is preserved for a structural reason, not by luck:
`_direct_qos0_ready()` declines while effects are pending, which is exactly the
state a QoS 1/2 commit earlier in the same batch leaves behind. Measured in an
alternating QoS 0/1 batch, 1 of 10 QoS 0 messages took the direct path and 9
fell back — the guard doing its job. A new test pins the wire order against a
broker stub.

## Gap D

`max_outbound_inflight` is exposed on the façade constructor (attach-time only,
since `EngineConfig` refuses it once attached), so the external adapter no
longer needs to rebuild the private inner `AsyncClient`.
[`OPERATIONS.md`](../OPERATIONS.md) now records the
`local_receive_maximum` 100-vs-65535 split and the shedding obligation for
`publish_nowait` producers with large payloads.

## Verification run

`ruff format --check`, `ruff check`, `mypy`, `bandit` clean. 1 111 unit tests
pass; coverage 88.61 % (gate 80 %). 15 integration tests **executed** against a
live Mosquitto on `127.0.0.1:11883` — confirmed no skips. Seeded fuzz 20 000
iterations across codec, engine and websocket: 0 crashes, 0 invariant
violations. Hypothesis fuzz passes.

Tests re-anchored rather than deleted: the batching, ordering and loop-stop
tests observed `engine.queue_publish`, which QoS 0 no longer reaches, and now
observe the façade commit boundary, which is path-agnostic. One test had been
passing only because a fresh client's first façade mid and first packet
identifier are both `1`; it now advances the counter first so the namespaces are
observably different.

## Not done

Gap B (native PUBACK latency at a fixed 10 k offer) was not started. It is
sequenced after this change and begins with an attribution measurement, not an
experiment.
