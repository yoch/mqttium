# Floors, not ceilings — 2026-08-16

A work plan for MQTTium native and `compat.paho` performance, using other
Python MQTT clients as **attainable floors** rather than as a glass ceiling.
No code in this repository changed as a result of this report. Implement the
experiments from MQTTium, with this repository's paired runners as the
acceptance path.

## Scope and evidence boundary

MQTTium at `1.0.0rc5` (PyPI; native `AsyncClient` + `mqttium.compat.paho`).

Cross-client medians are **hypotheses imported from an external harness**
(`mqtt-python-client-bench`, campaign of 2026-08-16, fingerprint
`a0470f7f34e933e5`, Mosquitto 2.0.20 on the same pinned host that produced
the earlier cross-client records). They are not MQTTium release evidence.
They motivate which gaps to close. Closing a gap requires this repository's
preflight, A/A control and `AsyncClient` / façade paths
([`BENCHMARKING.md`](../BENCHMARKING.md)).

This report does **not** treat the 2026-08-09 `2.95×` PUBACK claim as a fact.
That measurement paced each client at a fraction of its own capacity; it was
retracted in [`README.md`](README.md). Equal-offer latency is the comparison
that counts.

Peer groups in that harness are not an excuse to stop: `sync` (paho,
mqttium-compat), `asyncio_bridged` (mqttium, gmqtt, …), `crt_event_loop`
(awscrt). A floor is “a library already did this on this machine”, regardless
of I/O model.

## Decision

Do not spend the next cycle on native QoS 0 capacity, subscribe ingress, or
unbounded queues. Native publish capacity already leads. The two gaps that
fail the floor test are:

1. **`compat.paho` QoS 1/2 throughput is below Paho** on the same sync API
   shape (5.0 k vs 9.6 k QoS 1). That is a façade defect relative to the
   migration promise, not an asyncio tax that must be accepted.
2. **Native QoS 1 PUBACK p50 at a matched ~10 k msgs/s is ~4× awscrt**
   (0.40 ms vs 0.10 ms). Capacity is already similar (~21 k vs ~20 k). The
   remaining work is scheduling hops, not encode/decode.

Treat Paho 9.6 k and awscrt/paho 0.10–0.14 ms as **minimum targets**. Beating
them is allowed. Matching gmqtt’s 34 k QoS 0 is not a native goal; native is
already at 61 k.

## External floors (2026-08-16 campaign)

Valid medians, MQTTv311, telemetry 256 B, `n=3`, unless noted. Primary metric
for capacity is adapter `completed_success` / s; `$SYS` received-publish
reconciled in `[0.90, 1.20]` on publisher-only points.

### Capacity — `pub_qos_sweep_telemetry`

| QoS | Best | MQTTium native | mqttium-compat | Paho | Notes |
| --- | ---: | ---: | ---: | ---: | --- |
| 0 | **mqttium 61 139** | — | 24 882 | 12 239 | `$SYS` ratio ~1.00 on native; not a queue fiction |
| 1 | **mqttium 20 899** | — | **5 041** | **9 592** | compat **below** Paho |
| 2 | **mqttium 15 722** | — | 4 583 | 7 440 | gmqtt/awscrt: no QoS 2 |

Native QoS 1 inflight sweep on the same campaign: window 1 → ~7.1 k, 20 →
~17.8 k, 64 (ranking window) → ~20.9 k, 100 → ~21.7 k. The ranking window is
~4% off the plateau, not the compat collapse.

### Equal-offer latency (reconstructed)

`puback_latency_qos1` at `load_fraction=0.50` is **not** a cross-client
ranking (faster clients are offered more). At the absolute rates that
fraction happened to produce:

| Client | p50 | offered msgs/s |
| --- | ---: | ---: |
| awscrt | **0.10 ms** | ~9 944 |
| paho | 0.14 ms | ~4 066 (cannot hold 10 k) |
| gmqtt | 0.21 ms | ~8 633 |
| mqttium-compat | 0.27 ms | ~2 528 |
| mqttium | **0.40 ms** | ~10 471 |
| zmqtt | 1.22 ms | ~8 739 |

Little’s law at ~10 k QoS 1: awscrt L≈1, mqttium L≈4. Same completions/s,
longer pipeline. Internal rc5 audit at 10 k callback p50 was ~0.48–0.52 ms
([`INDEPENDENT-QOS0-LATENCY-AUDIT-2026-08-14.md`](INDEPENDENT-QOS0-LATENCY-AUDIT-2026-08-14.md)),
same order.

Application RTT at lf=0.50: paho 0.38 ms @ ~3.2 k pairs; mqttium 1.27 ms @
~4.5 k pairs. Same load-bias. Fixed-rate RTT is out of this plan’s first
cut; PUBACK at 10 k fixed is the native experiment.

### What is already won (do not “optimise” first)

- Native QoS 0/1/2 capacity leads the field. gmqtt 34 k QoS 0 is not a
  ceiling.
- Native `protocol_failed=0` after rc5 (`FlowControlError` → `mid is None`).
- Subscribe / payload ≥64 KiB: Mosquitto CPU / headroom gate. A faster client
  is marked inconclusive. Out of scope until a broker-ceiling probe or a
  different broker.
- Native QoS 0 256 B: `sync_rejected=0`, `$SYS` confirms ingest. Raising
  `max_queued` / setting `max_pending_*=None` will not move the 61 k figure
  and would break fairness vs peers.
- rc5 already removed the EffectPump hop on terminal QoS 1/2 when the
  callback queue has room. Do not re-do that work.

## Gap A — compat QoS ≥1 below Paho

### Observation

Single-producer closed-loop QoS 1: compat ~5 k, Paho ~9.6 k, native ~21 k.
QoS 0 compat already beats Paho (25 k vs 12 k). The hole is QoS ≥1 only.

### Mechanism

[`src/mqttium/compat/paho.py`](../../src/mqttium/compat/paho.py) `publish()`
for QoS 1/2 calls `future.result()` until loop-side MID allocation and
receipt registration (around the `_PUBLISH_HANDOFF_TIMEOUT` wait). A single
producer therefore:

1. enqueues one request;
2. schedules one drain;
3. **blocks**, so nothing else joins the batch;
4. drain sees **batch size ≈ 1**;
5. two context switches per message before the engine even runs.

The coalesced ingress queue
([`docs/COMPAT.md`](../COMPAT.md) §8) pays off with **many** producer
threads filling the queue while a drain runs
(`tests/unit/test_compat_publish_perf.py::test_concurrent_qos1_publish_coalesces_loop_drains`).
The bench is one publisher thread, which is also the Paho-shaped application.

Paho allocates the packet identifier on the calling thread and returns
immediately; the network thread runs in parallel.

Native `publish_nowait` never waits for a MID (loop-local admission).

Packet-id allocation itself (`protocol/packet_ids.py`) is not the bottleneck.
`max_outbound_inflight` missing from the façade ctor is **not** the 5 k
cause (default `None` → broker Receive Maximum). The bench already rebuilds
the inner `AsyncClient` to force the window
(`mqtt-python-client-bench` adapter `mqttium_compat.py`).

### Proposed experiment

Keep the engine loop-owned (COMPAT.md rejected mutating `OutboundSession`
from producer threads). Change **only** packet-identifier attribution:

- At `publish()` QoS ≥1, reserve a MID on a **thread-safe** `PacketIdPool`
  wrapper (lock around allocate/release/reserve only).
- Enqueue `{mid, payload}` on the existing coalesced façade queue.
- Return `MQTTMessageInfo(mid=…)` **immediately**.
- On the loop, `queue_publish(..., packet_id=mid)` consumes that id; do not
  allocate again.
- On admission refusal: release the id, set `rc=MQTT_ERR_QUEUE_SIZE`, fail
  `wait_for_publish()`. Ingress saturation (`max_pending_publish_*`) still
  returns `mid=None` before enqueue, as today.
- Forbidden: touching store, flow, effects, or connection state off-loop.

`publish()` must no longer wait on `Future.result`. Timeouts move to
`wait_for_publish()`. Update
`test_publish_timeout_cancels_before_admission` and
`test_loop_stop_fails_queued_qos1_publish` accordingly.

### Acceptance

1. Unit: unique MIDs under concurrent publishers; mixed QoS ingress order
   unchanged; loop-stop fails pending handoff without leaking ids.
2. `PYTHONPATH=src python benchmarks/compat_qosn_submit_ab.py` — submit rate
   with one worker must move **substantially** (batch size ≫ 1). If it does
   not, abandon before any external campaign.
3. Floor: closed-loop QoS 1 compat **≥ Paho 9.6 k** on the external harness
   (same outstanding window). Native 21 k is not required (the extra thread
   remains).

### Follow-up (API, not the 5 k bug)

Expose `max_outbound_inflight` on `compat.paho.Client` at construction
(attach-time; it is not in `_RUNTIME_MUTABLE_ENGINE_CONFIG_FIELDS`). Document
in COMPAT.md. Then the external adapter can drop the private `_async`
rebuild.

## Gap B — native PUBACK latency at 10 k equal offer

### Observation

At ~10 k msgs/s QoS 1, native p50 ~0.40 ms vs awscrt 0.10 ms vs paho 0.14 ms
(paho’s offer was only ~4 k). rc5 already admitted `on_publish` to the
callback queue without an EffectPump wake. Remaining hops:

1. **WritePump task** — `SEND` is queued; `_run` is often blocked on
   `await queue.get()` ([`src/mqttium/api/_writer.py`](../../src/mqttium/api/_writer.py)).
   The write does not start until the current `publish_nowait` callback
   yields. `StreamTransport.write` is `_writer.write` plus optional drain
   (`transport/_stream.py`); the syscall itself can be eager.
2. **Isolated callback worker** — `_apply_effect_inline` for
   `PUBLISH_COMPLETE` still `try_enqueue_callback`; user `on_publish` runs
   on another task ([`async_client.py`](../../src/mqttium/api/async_client.py)
   ~1540–1550, [`_delivery.py`](../../src/mqttium/api/_delivery.py) worker).
   awscrt fires on the IO thread.
3. **asyncio multiplexing** — publisher, reader, writer, callback, keepalive
   share one loop. Queueing under load is Little’s L≈4.

Not the 300 µs: ACK decode (already specialised), `receipt.wait()`,
`local_receive_maximum`, `max_pending_callbacks=1024` (not saturated at this
rate), copies of 256 B telemetry, missing `TCP_NODELAY` (already on).

### Proposed experiments (separate A/Bs)

Both at **10 000 msgs/s fixed**, `paired_open_loop.py`, `on_publish` path,
preflight `--enforce`. Not calibrated fractions.

**B1. Eager write.** If `WritePump` queue is empty and the writer task is
not inside an in-flight `transport.write`/`drain`, call a new
`write_nowait` on `StreamTransport` (`StreamWriter.write` without awaiting
drain unless the kernel buffer exceeds the existing 64 KiB high-water).
Do not merge reader and writer tasks. Skip eager on WebSocket if
`write_nowait` is absent. Concurrent write vs drain on the same
`StreamWriter` is forbidden — gate on an explicit `_writing` flag.

**B2. Opt-in inline `on_publish`.** Constructor
`on_publish_inline: bool = False`. Default **off** (isolation unchanged).
When true, terminal QoS 1/2 `PUBLISH_COMPLETE` invokes a sync callback on
the reader/finalize task; async callbacks still enqueue. Document that
inline user code delays reads, as Paho does.

Do not combine B1 and B2 in the first pair. Attribute each hop.

### Acceptance

- A/A at 10 k: completed-rate ratio within 2%, p50 CV ≤ 5% (if p50 CV fails,
  treat as diagnostic like the rc5 10 k cell, do not ship a latency claim).
- Floor: callback p50 **≤ 0.14 ms** at 10 k (Paho/awscrt band). 0.10 ms is
  the stretch if both hops land.
- Default (`on_publish_inline=False`) must not regress capacity or isolation
  tests.

The external catalogue already has `puback_latency_fixed_rate` (1 k / 2.5 k
/ 5 k / 10 k). It is not in that repo’s overnight `REPR`. After MQTTium
wins, ask the harness to add it; do not block MQTTium work on that.

## Gap C — compat QoS 0 accidental effect path

### Observation

Compat QoS 0 ~25 k vs native ~61 k. Cross-thread handoff is unavoidable for
Paho. The drain currently always calls `_queue_qos0_on_loop` (effect SEND +
`PUBLISH_COMPLETE`) and never `_try_direct_qos0_publish`
(`paho.py` drain, ~649–654). Native QoS 0 writer-direct is
`_try_direct_qos0_publish` (`async_client.py` ~566–597).

`_direct_qos0_ready()` remains true when `on_publish` is set **if** the
callback queue has capacity. Installing the façade dispatcher does not
forbid the direct path; the drain simply never asks for it.

### Proposed experiment

On the loop thread only, drain QoS 0 via `_try_direct_qos0_publish(..., nowait=True)`;
fall back to `_queue_qos0_on_loop` when it returns `None`. Keep coalesced
off-loop enqueue. Cache the bound method next to `_queue_qos0_on_loop` so
adapter inner-client replacement stays consistent.

### Acceptance

Submit/capacity up vs current 25 k on the same façade; no QoS 1 regression
(Gap A is the QoS 1 path). No claim vs native 61 k.

## Gap D — operations / defaults (documentation and ctor)

Not ranking bugs on the current core suite.

- **`local_receive_maximum`**: `AsyncClient` default **100**,
  `EngineConfig` default **65535**. Document in OPERATIONS.md: inbound QoS 1
  auto-ack is capped at 100 unless raised. Current external subscribe points
  are QoS 0, so they do not hit this.
- **Large payloads + `publish_nowait`**: writer byte budget (default 1 MiB,
  sized up by the external adapter from `max_queued × payload`) still
  saturates; the producer busy-spins `FlowControlError`. Native
  `publish_backpressure="wait"` already exists. Document: nowait producers
  must shed; do not set pending bounds to `None` (OOM). No file-unlimited
  experiment in this plan.
- **`max_outbound_inflight` on the façade**: see Gap A follow-up.

## Implementation order

1. Gap A (compat MID) — largest floor miss, same API shape as Paho.
2. Gap B1 then B2 (native hops) — equal-offer latency.
3. Gap C (compat QoS 0 direct drain).
4. Gap D ctor + OPERATIONS notes.

Stop after step 1 if `compat_qosn_submit_ab.py` (one worker) does not move.

## Out of scope

- Inflating native QoS 0 queues to chase a number `$SYS` already confirms.
- Subscribe vs Mosquitto ~30 k ingress (broker is the SUT).
- Forcing QoS 0 completion to the socket to match Paho’s boundary (different
  contract; broker ingest already matches adapter counts at 256 B).
- Using gmqtt/zmqtt capacity as a native ceiling.
- Shipping `on_publish_inline=True` as the default.
- Mutating the protocol engine from producer threads (COMPAT.md §8 still
  holds except the locked packet-id reserve).

## Validation recipe (this repository)

```bash
# After each experiment, on a preflight-eligible host:
python benchmarks/runner_probe.py --enforce

# Gap A
PYTHONPATH=src python benchmarks/compat_qosn_submit_ab.py --workers 1
PYTHONPATH=src python benchmarks/compat_qosn_submit_ab.py --workers 8

# Gap B (fixed 10k, not fractions)
# Use paired_open_loop.py absolute-rate mode; record window and load basis.
python -m pytest -q tests/unit
```

Do not commit benchmark JSON. Record medians, pair counts, CVs and the
probe blob in a **new** dated report once an experiment lands. Do not edit
this file to match later code.
