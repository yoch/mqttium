# Async handoff audit — 2026-08-14

## Scope

This audit follows the independent QoS 1/2 callback-latency correction merged
as `44b9614` in PR #220. It looks for the same structural pattern across
MQTTium: terminal or immediately admissible work paying an EffectPump task,
queue handoff, or event-loop wake-up that is not required for ordering,
backpressure, callback isolation, persistence, or transport ownership.

The review covered every `EffectKind`, all `asyncio.create_task()` and callback
queue sites under `src/mqttium`, the writer and delivery controllers, native
receipts, and the Paho cross-thread boundary. Measurements use repository-local
microbenchmarks with source-isolated workers. No external adapter or benchmark
result is evidence for this report.

## Integrated correction

Terminal QoS 1/2 `PUBLISH_COMPLETE` and `PUBLISH_FAILED` effects now admit
`on_publish` directly to the bounded callback queue when it has capacity and
settle receipts inline. Callback code still executes only in the isolated
worker. A full callback queue retains the ordered EffectPump path. The eligible
host A/A and A/B evidence, semantic tests, integration, fuzz and memory results
are recorded in `INDEPENDENT-QOS0-LATENCY-AUDIT-2026-08-14.md`.

## Findings

| Priority | Site | Finding | Decision |
| --- | --- | --- | --- |
| High | Native direct QoS 0 publication | Installing `on_publish` disabled the direct writer path even when both bounded queues had immediate capacity. Each publish then created SEND and completion effects and required EffectPump finalisation. | Corrected independently in PR #223 with atomic writer/callback admission and whole-batch fallback. |
| High | Single eligible inbound MESSAGE | Inline delivery accepted a consecutive prefix only when at least two effects were pending. A lone small QoS 0 or fresh automatic QoS 1 MESSAGE therefore scheduled EffectPump despite needing no persistence mark or capacity wait. | Corrected independently in PR #221 by removing only the artificial two-effect minimum. |
| Low | Single CONNACK with `on_connect` | The callback's presence moves an otherwise synchronous CONNACK resolution through EffectPump before bounded callback admission. | Leave unchanged: one event per connection, no material hot-path impact. |

### 1. Native QoS 0 callback gate

`AsyncClient._direct_qos0_ready()` required `on_publish is None`. PR #223
retains the pending-effect ordering gates, checks callback capacity, admits the
encoded item to WritePump, and then queues the callback without an `await`.
These operations are loop-confined, so capacity cannot change between the check
and the two admissions. If either capacity is unavailable, the operation uses
the established engine/effect path. A batch preflights every callback slot
before admitting any direct write.

Eleven final alternating source-isolated microbenchmark pairs measured the
whole `publish_nowait()` plus callback-worker completion path in batches of 64
on an eligible host, pinned to one CPU:

- median candidate/base throughput: **1.9474**;
- range: **1.6697–2.0135**;
- baseline CV: **2.41%**; candidate CV: **5.33%**;
- pairs favouring the candidate: **11/11**;
- seven-pair no-callback timing control: **0.9825**, inside the ±2% guard;
- exact no-callback profile: identical calls and primitive calls per operation;
- exact callback profile: **119.4479 to 73.2041 calls per operation**.

The retained design covers callback-after-writer admission, full callback and
writer queues, immediate refusal, pending-effect ordering, batch atomicity,
mixed QoS and MQTT 5 properties. Its complete evidence is recorded in
`NATIVE-QOS0-CALLBACK-DIRECT-2026-08-14.md`.

### 2. Single inbound MESSAGE threshold

`ApplicationDelivery.deliver_batch_inline()` returned immediately when fewer
than two effects were pending. Everything after that guard already validates
the properties needed for safe single-message admission:

- authoritative `requires_delivery_mark is False` classification;
- small/unaccounted delivery eligibility;
- callback and iterator queue capacity before mutation;
- atomic `message_delivery="both"` admission;
- isolated callback execution.

The candidate removed only the two-effect minimum. Eleven alternating
microbenchmark pairs, each exercising one MESSAGE collection, EffectPump drain,
and callback completion at a time, produced:

- median candidate/base throughput: **6.4588**;
- range: **6.2317–6.6072**;
- baseline CV: **0.91%**; candidate CV: **1.92%**;
- pairs favouring the candidate: **11/11**.

The ratio is deliberately interpreted as scheduler-path evidence, not as an
end-to-end broker throughput forecast: the scenario maximizes isolated
single-packet reads. PR #221 retained that one-line correction after focused
ordering/backpressure tests, 1,022 unit tests, 15 live Mosquitto integrations,
the complete memory profile, and the repository quality gates. Its detailed
record is `SINGLE-MESSAGE-INLINE-DELIVERY-2026-08-14.md`.

### 3. CONNACK callback

A lone successful CONNACK could preflight the callback queue, resolve its
future, and then use the non-blocking callback admission primitive. This would
remove at most one scheduled effect task when `on_connect` is installed.

No runtime candidate is retained. Connect is a cold operation, resumed
sessions commonly produce a multi-effect batch anyway, and there is no
user-visible performance problem. Preserving the current code also avoids a
second ordering case around the connection future, callback-worker wake-up,
callback-queue saturation, and connection-epoch disposal. The repository's
performance rules require explicit measured benefit for added complexity; a
synthetic repeated-CONNACK microbenchmark would magnify work that production
performs once per connection and would not establish such a benefit.

## Boundaries that should not be changed

- WritePump's task and queue provide the single-writer invariant, batching and
  byte/count backpressure; they are ownership boundaries, not redundant hops.
- AUTH handlers must be awaited because their result determines the next wire
  packet.
- persisted/manual QoS 1, QoS 2 and replay MESSAGE effects must wait for
  application capacity before marking durable state delivered.
- DISCONNECTED effects may drain and close a transport; inbound replay must
  re-enter the engine under its lock one bounded batch at a time.
- the callback worker is required for exception isolation, async callbacks and
  bounded shutdown. The optimizations above remove work before it, not the
  worker itself.
- Paho's `call_soon_threadsafe` ingress is the thread-confinement boundary and
  is already coalesced into bounded batches.
- `PublishReceipt.wait()` uses a shared shielded future to preserve independent
  waiter cancellation. Removing that scheduling cost would reintroduce a
  correctness regression.

## Recommendation

Keep the integrated QoS 1/2 correction and the two separately reviewed
high-priority changes:

1. PR #221 allows one already-eligible MESSAGE through the existing inline
   delivery admission;
2. PR #223 extends native direct QoS 0 publication to callbacks with atomic
   writer/callback admission, including `publish_many()`.

Do not optimize CONNACK or remove any of the ownership/backpressure tasks
listed above without a separately reproduced user-visible problem. This is a
deliberate no-change decision, not unfinished implementation work.
