# Cross-client benchmark observations — MQTTium 0.2.0b2

## Scope

MQTTium 0.2.0b2 was measured against seven other Python MQTT clients by an
external harness (`mqtt-python-client-bench`), which drives every client through
one adapter protocol and interleaves them within each measurement point rather
than running one client campaign after another. Two client identities are
benchmarked separately and must not be conflated:

- `mqttium` — the native `AsyncClient`, driven from a sync role worker through an
  asyncio bridge (`io_model = asyncio_bridged`);
- `mqttium-compat` — `mqttium.compat.paho` only (`io_model = sync`).

Rankings below are within an I/O-model peer group and a single MQTT protocol
version, which is how that harness compares clients; cross-group numbers are not
comparable and are marked where quoted.

Environment: Intel i7-3770 (4 physical / 8 logical cores), Linux 6.8
lowlatency, `performance` governor, Python 3.12.3, Mosquitto
`eclipse-mosquitto:2.0.20` in Docker with host networking. Each role runs in its
own process pinned to a disjoint physical-core group — SUT `0,4`, broker `1,5`,
load generator `2,6`, orchestrator `3,7`. Peers: paho-mqtt 2.1.0, gmqtt 0.7.0,
aiomqtt 2.5.1, amqtt 0.11.3, awscrt 0.35.0, zmqtt 0.0.5.

Every campaign figure is a median of 3 runs at 12 s measure / 3 s warmup / 6 s
drain. Differences below roughly 10% are not interpretable at that sample size;
only the large factors are discussed.

`pub_qos_sweep_telemetry` and `puback_latency_qos1` — the two scenarios carrying
the conclusions below — were measured twice, in independent campaigns three days
apart from reset result files. Every client reproduced within ±2.6% on capacity
(most within ±1%) and within ±5% on latency; the figures quoted for those two
scenarios are from the second campaign. The remaining scenarios
(`pub_qos1_inflight`, the payload sweep, `application_rtt_qos1`) were measured
once and are marked where quoted.

## Summary

MQTTium is the fastest client measured at QoS 0 — by a wide margin, in both
identities — and merely mid-pack at QoS 1, with a PUBACK latency roughly three
times its closest architectural peer at an equal offered rate. The QoS 0 result
and the QoS 1 result come from genuinely different code paths, and the gap
between them is the single most interesting thing in the data.

An adapter-side explanation for the QoS 1 cost was proposed and **tested, and it
does not hold** (see "What was ruled out"). The remaining candidates are inside
MQTTium.

## QoS 0: fastest client measured

`pub_qos_sweep_telemetry`, 256-byte payload, MQTTv3.1.1, closed loop with a
64-message outstanding window, median msgs/s:

| client | io_model | QoS 0 | QoS 1 | QoS 2 |
| --- | --- | ---: | ---: | ---: |
| **mqttium** | asyncio_bridged | **38 200** | 13 934 | **10 871** |
| gmqtt | asyncio_bridged | 25 710 | 15 216 | — |
| zmqtt | asyncio_bridged | 18 220 | 14 358 | 9 871 |
| amqtt | asyncio_bridged | 10 619 | 6 789 | 4 176 |
| aiomqtt | asyncio_bridged | 8 888 | 7 061 | 5 941 |
| **mqttium-compat** | sync | **28 344** | 4 203 | 3 952 |
| paho | sync | 12 274 | 9 484 | 7 432 |
| awscrt | crt_event_loop | 15 251 | 19 536 | — |

MQTTv5 figures are within noise of the v3.1.1 ones for every client, MQTTium
included (37 909 / 13 495 / 10 605 against 38 200 / 13 934 / 10 871) — protocol
version is not a cost driver here.

At QoS 0 the native client is **49% ahead of gmqtt**, the next fastest in its
group, and the Paho façade is **2.3× paho itself** in the sync group. The payload
sweep — measured once — shows the lead holding across sizes: 39 647 (empty),
38 101 (64 B), 37 991 (256 B), 35 447 (1 KiB), 29 401 (16 KiB), first in the
bridged group at every size that produced a valid point. The 64 KiB and 1 MiB
points are not usable on this host: only amqtt and aiomqtt produced a valid
median there, precisely because they are too slow to saturate the broker.

These medians are corroborated by an independent method. The harness also runs
ABBA comparisons — interleaved A/B/B/A blocks with a bootstrap confidence
interval on the effect — and those agree with the interleaved-matrix medians to
about 1%: gmqtt over paho at QoS 0 gives a ratio of 2.100 against 2.09 from the
medians, and 1.607 against 1.60 at QoS 1. MQTTium was not part of an ABBA pair,
but the agreement establishes that the ranking method itself is sound.

This is very likely the direct QoS 0 transport write (`_direct_qos0_ready` /
`_try_direct_qos0_publish`). Note that the path is conditional on
`on_publish is None`, and the harness adapter deliberately leaves that callback
unset and signals completion itself, so these numbers reflect the fast path. Any
consumer that installs `on_publish` loses it silently — worth a documentation
note, and worth a regression test, because it is the strongest result MQTTium
has here.

## QoS ≥ 1: high per-message completion cost, well amortised

The QoS 0 → QoS 1 ratio separates MQTTium from its peers:

| client | QoS 0 → QoS 1 |
| --- | --- |
| zmqtt | ÷ 1.27 |
| gmqtt | ÷ 1.69 |
| **mqttium** | **÷ 2.74** |
| **mqttium-compat** | **÷ 6.74** |

Latency makes the same point more cleanly, because gmqtt and MQTTium have almost
identical calibrated publish capacities (13 304 and 13 147 msgs/s), so at
`load_fraction = 0.5` they are driven at essentially the **same absolute rate**
— 6 652 vs 6 574 msgs/s. `puback_latency_qos1`, MQTTv3.1.1, p50/p99 ms:

| client | load 0.5 | load 0.9 |
| --- | --- | --- |
| gmqtt | 0.41 / 0.68 | 1.58 / 2.82 |
| zmqtt | 0.48 / 0.83 | 2.04 / 4.25 |
| **mqttium** | **1.21 / 1.80** | **2.99 / 4.02** |

2.95× gmqtt's p50 at a matched offered rate, same I/O model, same protocol —
reproduced across both campaigns (1.17 then 1.21 ms, against 0.40 then 0.41).
The end-to-end `application_rtt_qos1` scenario agrees and is if anything harsher:
MQTTium 1.56 ms p50 against gmqtt 0.63 ms — and there gmqtt is driven *harder*
(its RTT capacity is 8 383 vs MQTTium's 6 734, so 4 192 vs 3 367 msgs/s). That
scenario was measured once.

Do not read amqtt (0.27 ms) or aiomqtt (0.32 ms) as beating MQTTium on latency:
their calibrated capacity is half, so they are offered ~3 000 msgs/s. gmqtt and
zmqtt are the only fair latency comparisons in this data.

The cost amortises well. `pub_qos1_inflight`, measured once, sweeping the
in-flight window:

| client | window 1 | window 20 | window 100 | scaling |
| --- | ---: | ---: | ---: | --- |
| **mqttium** | 4 279 | 10 862 | 14 467 | **× 3.4** |
| aiomqtt | 3 590 | 6 693 | 6 963 | × 1.9 |
| paho *(sync, not comparable)* | 5 571 | 9 800 | 9 047 | × 1.6 |

So the shape is: expensive per message, excellent under pipelining. That is
consistent with a fixed per-completion overhead rather than a throughput
ceiling — which is what makes the latency number the one to chase.

## What was ruled out

The initial hypothesis was that the harness adapter caused the QoS ≥ 1 cost. It
awaits the receipt (`await receipt.wait()`), so each publish holds a coroutine
suspended on an `asyncio.Event` until the PUBACK, whereas the gmqtt adapter
submits and returns immediately, correlating the ack later through a callback.
Two extra scheduler wakeups per message.

Reading the source weakened the hypothesis before it was tested: `_apply_effect`
has an inline fast path for `PUBLISH_COMPLETE` **while `on_publish is None`**
(`api/async_client.py:1450`), settling the receipt without the callback queue.
Installing `on_publish` routes completion through `_enqueue_callback` →
`_callback_queue` → `_callback_worker` → `_invoke` instead, which is strictly
more work.

Both disciplines were implemented in the adapter and A/B'd. p50 ms, 3 runs per
arm, alternating, same machine, otherwise idle:

| point | receipt (await) | callback (on_publish) |
| --- | ---: | ---: |
| v3.1.1, load 0.5 | 1.776 | 1.869 |
| v5, load 0.5 | 2.016 | 1.997 |
| v3.1.1, load 0.9 | 5.133 | **4.186** |
| v5, load 0.9 | 5.064 | **4.663** |

Two conclusions:

1. **The adapter is not the explanation.** The discipline is worth at most
   ~15–18% at high load and nothing at low load. It cannot produce a 2.9× gap.
   The QoS ≥ 1 cost is inside MQTTium.
2. **The `on_publish is None` fast path does not pay off under load.** At
   `load_fraction = 0.9` the callback queue was *faster* than the inline settle,
   consistently and with non-overlapping spreads (5.14 / 5.03 / 5.13 against
   4.35 / 4.17 / 4.19). Waking one suspended per-message coroutine appears to
   cost more than batching completions through the callback worker. The premise
   behind that branch deserves re-measuring.

These A/B runs used the harness's short "smoke" profile, which shares CPU sets
between roles; absolute latency is inflated relative to the campaign figures
(1.78 ms vs 1.17 ms for the same point). Both arms carry that penalty equally,
so the comparison between them holds, but do not compare these numbers to the
campaign table above.

A matching A/B on QoS 1 *capacity* was not run — the machine stopped being idle.
Nothing here says whether the discipline affects throughput as well as latency.

## Points to verify in this repository

1. **Reproduce the QoS 1 latency without the external harness.** ~1.17 ms p50 at
   ~6 600 msgs/s, single publisher, local broker, in-flight window 64. If a
   direct `AsyncClient` script reproduces it, the harness is fully exonerated and
   the target is the completion pipeline. This is the first thing to do; the
   "Native hot-path performance program" (#39) is the natural home for it.
2. **Cost of the effect pipeline per QoS ≥ 1 publish.** Every publication goes
   `queue_publish` → effect emission → `_collect_effects_locked` → `_drain_effects`
   / `_schedule_effect_flush` → `EffectPump`. gmqtt, the client that beats
   MQTTium here, writes to its connection directly and correlates on the ack
   hook. Worth measuring how much of the 1.17 ms is that indirection.
3. **`_engine_lock` contention on the completion path.** `publish_nowait` avoids
   the lock, but the completion side takes `async with self._engine_lock`. Under
   a 64-deep window the reader task processing PUBACKs and the publisher
   admitting new messages contend for it; check whether that serialises.
4. **Re-measure the `on_publish is None` inline settle** (point 2 of the previous
   section). It is currently an optimisation that measured slower than the path
   it is meant to avoid.
5. **`mqttium.compat.paho` QoS ≥ 1 plateau.** The façade returns 3 111 / 4 169 /
   4 206 msgs/s for in-flight windows of 1 / 20 / 100 — it barely benefits from
   pipelining, where the native client scales ×3.4. At QoS 1 it reaches 4 133
   against paho's 9 398, having been 2.3× *faster* than paho at QoS 0. Candidates
   visible from the outside: the `_publish_pending` `SimpleQueue` with
   `_publish_drain_scheduled` batching, and the fact that the façade always
   installs `_async.on_publish`, which permanently disables the direct QoS 0 path
   for façade users.
6. **`max_outbound_bytes` default vs `publish_nowait` semantics.** The write pump
   defaults to 1 MiB, and `publish_nowait` raises `FlowControlError` as soon as
   either bound is full — 16 slots for a 64 KiB payload, one for a 1 MiB one.
   Benchmarking QoS 0 with large payloads refused 76% of 64 KiB publishes and 98%
   of 1 MiB ones until the harness sized the byte bounds explicitly from its
   requested queue depth. The behaviour is correct and documented in the
   docstring, but the default is surprising next to
   `max_pending_outbound_messages = 10 000`, and a caller sizing a queue in
   messages will not discover the byte bound until payloads grow. Consider
   deriving one from the other, or warning when the byte bound is the binding
   constraint.
7. **Protect the QoS 0 direct path with a regression test.** It is the reason
   MQTTium leads its peer group, it is silently conditional on
   `on_publish is None`, and nothing in the test suite appears to pin it.

## Related

- Issue #57 — `ProtocolError` on every inbound packet received while
  `state=DISCONNECTING`, found during the same campaign. Independent of the
  above; it fires after the measurement window and did not affect any figure
  here.

## Provenance

Raw results, harness source and the adapter used are in
`mqtt-python-client-bench` (`results/mqttium-*.json`,
`results/mqttium-compat-*.json`, `src/mqtt_client_bench/adapters/mqttium.py`).
Both campaigns are committed there. The completion-discipline A/B is not: it was
a temporary adapter switch (`MQTT_BENCH_MQTTIUM_ACK=receipt|callback`), removed
afterwards because the mode check cost a per-message comparison on MQTTium alone,
which is the kind of uneven harness cost that benchmark forbids. Reproducing it
means reapplying that switch.
