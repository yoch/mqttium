# MQTTium-only hot-path reconnaissance — 2026-09-06

Internal cost map of CPU, allocations, loop scheduling, and fast-path hit
rates on the named `main` revision. This report does **not** propose a
runtime change and does **not** interpret retracted or methodologically
invalidated cross-client RTT numbers.

Independent review before merge narrowed several µs attributions that had
been read as isolated costs. Path identities and hit rates were not
retracted. This file is that corrected evidence record, not a second
campaign.

> **Historical snapshot.** All measurements and source-path claims below describe
> `main@4560e44aeb24b8a4f7f6ce903766579fac481cbd`. Later main revisions deliberately
> changed some hot-path behavior, notably allowing an eligible exact pair of
> synchronous message callbacks to run inline. The “Already landed” / “Preserve”
> table is therefore historical guidance, not a current architectural contract.
> The harness itself was revalidated on `main@6f1351e3b60593946c520b7d1915564f76023873`;
> the historical wall-time measurements were not rewritten or presented as new data.

| | |
| --- | --- |
| Date | 2026-09-06 |
| Repository | `yoch/mqttium` |
| Branch studied | `main` at `4560e44aeb24b8a4f7f6ce903766579fac481cbd` (merge of #429, 1.0.0rc13) |
| Measurement branch | `cursor/perf-recon-map-05b4` (harness + this report only; `src/` unchanged) |
| Host | 4-CPU Linux KVM, Python 3.12.3, `cpu_governor` unreadable — **not** a reference host |
| Harness | `benchmarks/hotpath_recon.py`, in-process fake broker shaped after #418 (`paired_qos1_rtt.py`) |
| Payload | 18-byte JSON `{"t":21.5,"h":40}` on exact topics |
| µs/op evidence | Single baseline per cell, transcribed from that agent run; **not** re-measured for this correction |

**Strong claims** are discrete path identities and hit/miss ratios (0/1 or
exact fractions under a named scheduling shape).

**Weak claims** are wall µs/message and ops/s on this host. They are an
internal order of magnitude, exploratory ratios, and a localisation aid.
They are not a portable estimate, not a confidence interval, and not
authoritative.

Bridged application-RTT / ARM three-client grids that used
`C_common = min(mqttium, gmqtt, paho)` or `AsyncioBridge` are **out of
scope**. They must not be used to pick an optimisation.

## How to reproduce

```bash
python -m pip install -e ".[dev]"
PYTHONPATH=src python benchmarks/hotpath_recon.py --mode campaign --output /tmp/hotpath_recon_campaign.json
PYTHONPATH=src python -m pytest -q tests/unit/test_hotpath_recon.py
```

Loads (absolute, fixed, in-process):

| Load | outstanding / burst | measured operations | warmup |
| --- | ---: | ---: | ---: |
| low | 1 | 4 000 | 200 |
| medium | 8 | 12 000 | 400 |
| high | 32 | 24 000 | 800 |

Scenarios: `qos0_publish`, `qos1_publish`, `qos1_inbound_reply` (one packet
per read), `qos1_inbound_reply_coalesced` (TCP-like coalesced reads). Each
cell is MQTT 3.1.1 and MQTT 5. Baseline cells have no monkeypatches;
instrumented cells wrap instance aliases and selected class methods.

Paired micros (`benchmarks/_paired_scenarios.py`) and `cProfile` ranking
runs are separate experiments. The historical `effect_batch_inline`
scenario uses `batch_size = 8` and eight `EffectKind.SEND` values. It is
**not** a measurement of inbound auto-QoS1 `SEND_ACK + MESSAGE`.

Generated campaign JSON is not committed. Numbers below were transcribed
from the original agent-host run; this correction does not re-run that
campaign.

## Historical work map

Already landed on `main`. Constraints to preserve; do not re-open without
new evidence.

| PR | Already done | Falsified / rejected | Preserve |
| --- | --- | --- | --- |
| [#40](https://github.com/yoch/mqttium/pull/40) `agent/nowait-exact-wire-size` | `publish_nowait()` admits on exact wire size; double encode removed | Estimating size then encoding again | One encode on the QoS 0 direct path |
| [#42](https://github.com/yoch/mqttium/pull/42) `agent/v311-qos1-direct-message-decode` | MQTT 3.1.1 QoS 1 field decode into `_on_qos1()`; no `PublishPacket` on that path | Generic packet-model round trip for v311 QoS 1 | Shared `_on_qos1()` for v311 and v5; RM / ACK / manual-ack stay one machine |
| [#254](https://github.com/yoch/mqttium/pull/254) `agent/fix-eager-write-batching-253` | Eager write is **one DATA frame per loop turn** (ACK permit added later in #420) | Budgets 2/4/8; timing cutoffs; delayed re-arm | Do not revive those alternatives without a new A/B on an eligible host |
| [#402](https://github.com/yoch/mqttium/pull/402) `codex/inline-callback-dispatch` | Idle/single-message sync callback may run inline | Always-inline under burst, async, or re-entrant publish | Worker path when `len(messages) > 1` or the callback cannot run inline |
| [#418](https://github.com/yoch/mqttium/pull/418) `codex/qos1-rtt-harness-main` | Source-shaped in-process QoS 1 RTT harness | A new unrelated RTT framework | Reuse `paired_qos1_rtt.py` / this recon harness |
| [#420](https://github.com/yoch/mqttium/pull/420) `codex/qos1-bounded-ack-eager` | Independent DATA and success-ACK eager permits; QoS 1/2 prepare reused | One permit shared by DATA and ACK | Explicit ACK provenance; no byte sniffing on the DATA eager path |
| [#423](https://github.com/yoch/mqttium/pull/423) `perf/qos12-plain-prepared-carrier` | QoS 1/2 prepared carrier is a 6-field tuple | `NamedTuple` carrier | Keep the plain tuple |
| [#427](https://github.com/yoch/mqttium/pull/427) `perf/explicit-ack-writer-provenance` | `EffectKind.SEND_ACK` | Classifying ACK-ness from wire bytes | `try_enqueue_ack` / `_try_write_ack_eager` |

## Path identity (source at `4560e44`)

### Publish QoS 0

`publish_nowait` → `_try_direct_qos0_publish` (requires QoS 0 and
`_direct_qos0_ready()`; skipped when `on_publish` work cannot take the
direct path's conditions) → `OutboundSession.prepare_qos0` →
`encode_publish_item_v311` / `encode_publish_item_v5` → bound
`WritePump.try_enqueue` → `_try_write_data_eager` or `queue.put_nowait`.

- **Encode passes:** one.
- **Effects:** none on this path (`effect_collect_calls = 0`).
- **Receipt:** `PublishReceipt(mid=None)` with no packet-id future.
- **Loop:** producer may `await asyncio.sleep(0)` in the harness between
  bursts; the writer rearms eager with one `call_soon` after a successful
  eager DATA write.

### Publish QoS 1

`publish_nowait` tries the QoS 0 direct path and misses →
`_check_nowait_publish_capacity` (arithmetic wire size when the writer is
not empty) → `_queue_publish_on_loop` → `OutboundSession.queue_publish`
(prepared tuple, packet id, `OutboundMessage`, store row, flow slot) →
one encode at launch → `SEND` then `PUBLISH_COMPLETE` effects → receipt
registered before the frame can leave → writer DATA path → inbound PUBACK
→ `on_puback` → receipt completion.

- **Encode passes:** one (launch), not a second size-probe encode (#40).
- **Effects:** two single-effect inline collects per publish (`SEND` and
  `PUBLISH_COMPLETE`); PUBACK handling is a further collect.
- **QoS 0 direct hit rate on this scenario:** 0 (expected).
- **Same-process broker work on this path:** decode the PUBLISH, extract
  the packet identifier, build a PUBACK, `loop.call_soon` onto the client
  read queue. That work is inside `RUSAGE_SELF`.

### Inbound QoS 1 and application reply

Socket / fake broker → `IncrementalDecoder.next_packet` (owned copy at
the packet boundary) → `ProtocolEngine.handle_raw` →

- MQTT 3.1.1: `decode_qos12_fields_v311` → `_on_qos1_auto`
- MQTT 5: `decode_publish_fields_v5` → same `_on_qos1_auto`

`_on_qos1_auto` always `_send_ack(encode_puback_success)` then `_emit(MESSAGE)`
— **two effects**, SEND_ACK first so the pump should not need to reorder
a well-formed auto-ACK batch. `take_effects()` releases the Receive
Maximum slot.

`EffectPump.collect_from_engine`: `len(effects) == 1` applies inline;
`len == 2` always takes the multi-effect partition (`sends` / `others`
lists) then `pending.extend` + `drain_inline`.

Callback: one MESSAGE in the drained batch may run inline (#402); two or
more MESSAGE effects in one drain go to `_enqueue_message_batch` (worker).

Optional reply: `on_message` → `publish_nowait(..., qos=1)` → the QoS 1
publish path above, including independent ACK eager for the inbound PUBACK
and DATA eager for the reply PUBLISH.

The client decoder and the fake broker each have an `IncrementalDecoder`.
A full-cell `cProfile` `next_packet` ranking is therefore not a
source-isolated client owned-bytes cost.

## Instrumentation quality

| Tool | Overhead on this host | Scheduling change | Allocation change | Use |
| --- | --- | --- | --- | --- |
| No instrumentation (baseline cells) | — | None | None | Exploratory µs/op and ops/s **on this host only**; single baseline per cell |
| Instance/class counter wraps | 9.6–18.0 % throughput loss (median ~13 %) | Wraps `loop.call_soon` / `create_task` with an extra Python frame; does not change who is scheduled | Extra counter ints; instrumented cells show more gen0 | Hit/miss **ratios** (quantitative under the named harness shape). Absolute `call_soon_per_op` is **qualitative**: it also sees harness `sleep(0)` and asyncio queues |
| `cProfile` | ~3–4× slower (QoS 0 medium 135k → 38k ops/s) | Tracing | Tracing | Function **ranking** only; the fake broker shares `next_packet` |
| `tracemalloc` / `sys.getallocatedblocks` on engine-only loop | Separate experiment | None | Tracer itself | Peak live bytes; objects are released each iteration (`current_bytes` ≈ 32 B after 8 000 ops) |
| Paired micros (`_paired_scenarios.py`) | Isolated, no client loop | None | Scenario-specific | Named scenario only. `effect_batch_inline` is eight `SEND`s, not `SEND_ACK + MESSAGE` |

No event-trace was installed on the product hot path.

## 3. QoS 0 publish hot path

Exploratory baseline CPU µs/message (in-process broker included; one run
per cell):

| Protocol | low (burst 1) | medium (8) | high (32) |
| --- | ---: | ---: | ---: |
| MQTT 3.1.1 | 8.75 | 7.43 | 5.66 |
| MQTT 5 | 6.98 | 6.00 | 4.32 |

Isolated encode: v311 841 ns/op, v5 893 ns/op (`encode_qos0` /
`encode_qos0_v5`). Discard-writer `native_publish_nowait_qos0`: 1.92 µs.
`writer_try_enqueue`: 277 ns.

The full-path MQTT 5 vs 3.1.1 spread on this host **disagrees** with
isolated encode (v5 encode is slightly slower; full-path v5 looked
faster). Treat that spread as **NOISY**.

**Allocations:** one encoded `bytes` frame per publish; no `EngineEffect`;
no packet id; no receipt future. Baseline gen0 delta 0.

**Scheduling (this burst/window shape):** the producer submits `burst`
publishes then yields with `asyncio.sleep(0)`. Under that shape:

- Direct QoS 0 path: **100 %** at every load (no `on_publish` in this
  scenario).
- Eager DATA: **100 %** at burst 1; **12.5 %** at burst 8; **3.125 %** at
  burst 32. That is exactly one eager write per harness turn
  (`hits = operations / burst`). It demonstrates that the
  one-eager-per-loop-turn contract is active. It is not a general law of
  every load shape.
- Misses go to `queue.put_nowait`; the writer `_run` then batches
  (`writer_batched_items` ≈ misses).
- Writer enqueue suspensions: 0.
- Effect pump: unused.
- Exploratory CPU/message **falls** as burst rises even while eager hit
  rate collapses. Batching recoups the 1-per-turn eager miss for
  throughput **in this harness**.

**cProfile ranking** (v311 medium, qualitative, client+broker):
`encode_publish_item_v311`, `next_packet` (client and broker decoders),
`WritePump._run`, `_try_direct_qos0_publish`, `try_enqueue`,
`prepare_qos0`. ~77 primitive calls/op including the fake broker.

## 4. QoS 1 publish hot path

Exploratory baseline CPU µs/message (client **and** in-process broker):

| Protocol | low (window 1) | medium (8) | high (32) |
| --- | ---: | ---: | ---: |
| MQTT 3.1.1 | 41.74 | 24.01 | 17.03 |
| MQTT 5 | 41.62 | 24.25 | 16.99 |

Exploratory ratio vs QoS 0 on the same host: about **4.8×** at window 1,
**3.0×** at window 32. Isolated encode QoS 1 is only ~100 ns above QoS 0
(937 vs 841 ns v311). The full-path delta therefore is **not** the codec.

What that delta **does** demonstrate:

> The QoS 1 full transaction path is materially more expensive than the
> QoS 0 direct path in this harness.

What it does **not** isolate: the share belonging to
`queue_publish` / receipt / store / inflight versus the fake broker
(PUBLISH decode, MID extract, PUBACK construct, `call_soon`) versus
PUBACK ingress on the client.

**Allocations / objects (path identity):** prepared 6-tuple (#423),
packet id, outbound store row, `OutboundMessage`, `PublishReceipt` +
waiter, encoded PUBLISH, inbound PUBACK copy, two `EngineEffect` values
per launch (`SEND`, `PUBLISH_COMPLETE`).

**Scheduling (this burst/window shape):**

- QoS 0 direct: **0 %** (expected miss).
- Eager DATA: same 1 / window as QoS 0 (100 / 12.5 / 3.125 %).
- Eager ACK: unused (this client is the publisher; the fake broker emits
  PUBACKs inbound).
- Effect collects: 2 per operation, **all** `len == 1` inline
  (`effect_collect_single_inline` = `effect_collect_calls`;
  `effect_multi_batches` = 0).
- Immediate writer admission: 100 % (`enqueue_suspensions` = 0).

**cProfile ranking** (v311 medium, qualitative): `next_packet` (client
and broker), `_read_loop`, `encode_publish_item_v311`, `queue_publish`,
`_prepare_publish_request`, `collect_from_engine`, `WritePump._run`,
`on_puback`, `_launch`. Those outbound functions remain **visible
candidates**. They are not a quantitative majority split. ~223 primitive
calls/op.

## 5. Inbound QoS 1 / response hot path

Each operation is: inbound QoS 1 request + inline/worker `on_message` +
QoS 1 reply publish + wait for that reply's PUBACK.

Exploratory baseline CPU µs/operation (client+broker+reply):

| Cell | low | medium | high |
| --- | ---: | ---: | ---: |
| 1 packet/read, MQTT 3.1.1 | 77.85 | 51.33 | 43.10 |
| 1 packet/read, MQTT 5 | 80.33 | 53.38 | 45.17 |
| coalesced read, MQTT 3.1.1 | 77.71 | 39.11 | 27.10 |
| coalesced read, MQTT 5 | 80.44 | 40.40 | 27.85 |

Engine-only `handle_raw` + `take_effects` (no runtime, no broker loop):
exploratory **3.08 µs** v311, **3.62 µs** v5. Always exactly two effects
(`SEND_ACK` + `MESSAGE`). Paired micro `ingress_publish_qos1`: 2.96 µs.
Paired `effect_send_inline`: 805 ns (one `SEND`). Paired
`effect_batch_inline`: 4.07 µs for **eight `SEND`s**, not for
`SEND_ACK + MESSAGE`. Do not treat 4.07 µs as the inbound auto-QoS1
collect cost.

Tracemalloc over 8 000 engine iterations: peak **677 B** v311 / **789 B**
v5; `current_bytes` 32 B afterwards (temporaries die each iteration).
MQTT 5 gen0 collections in the engine loop: 94 vs 14.

The runtime+reply cell is an order of magnitude above the engine-only
loop on this host. That comparison is exploratory and still includes
different work on each side. The two-effect collect is a proven path;
its CPU cost is **unquantified**.

**Fast paths:**

- v311 field decode: **100 %** of inbound QoS 1 (never `PublishPacket.decode`).
- v5 field decode: **100 %** of inbound QoS 1 MQTT 5.
- Callback inline, 1 packet/read: **100 %** even at window 32, because each
  drain still sees a single MESSAGE after SEND_ACK is split out.
- Callback inline, coalesced window 8 and 32: **0 %** — all messages go
  through `_enqueue_message_batch` (#402). Coalesced window 1 remains 100 %
  inline.
- Eager DATA and eager ACK under this burst/window shape: 100 / 12.5 /
  3.125 %, independent permits, one-eager-per-loop-turn contract active.
- `SEND_ACK + MESSAGE` never takes the `len(effects) == 1` inline collect.
  On 1-packet/read, `effect_multi_batches` equals the operation count.

Coalesced medium/high is **faster per message** than 1-packet/read in
this harness despite losing inline callbacks: fewer loop turns and more
writer batching. That is an exploratory throughput observation, not a
latency ranking.

## 6. MQTT 3.1.1 vs MQTT 5

Observed on this revision, not assumed from older reports:

| Site | Observation | Class |
| --- | --- | --- |
| Engine QoS 1 ingress | v5 exploratory total +0.54 µs (+17.5 %) and more gen0 | MEASURABLE MINOR as a **total path delta** |
| Isolated encode QoS 0/1 | v5 +50–60 ns | MEASURABLE MINOR |
| Full QoS 0/1 publish cells | v311 ≈ v5 within noise; QoS 0 v5 even looked faster | NOISY |
| Full inbound+reply | v5 +2–3 µs/op at low; +0.7 µs at coalesced high | MEASURABLE MINOR, not isolated |
| Decode path | v311 `decode_qos12_fields_v311`; v5 `decode_publish_fields_v5`; empty `Properties` present | PROVEN PATH IDENTITY; individual `Properties()` cost **not isolated** |
| Topic alias / property wire size | Not exercised (no aliases, empty properties) | UNKNOWN for those features |

MQTT 5 QoS 1 still shares `_on_qos1_auto`. Empty `Properties` is a
**plausible contributor** to the measured v5 ingress delta, not an
isolated 0.54 µs cost. The rest of v5 field decode is in the same delta.

## 7. Top internal costs

Maximum ten. Classes used below:

- **PROVEN MATERIAL** — observed, load-bearing in this harness, still not
  a cross-client claim.
- **PROVEN PATH IDENTITY / COST UNQUANTIFIED** — the code path is shown;
  µs are not.
- **VISIBLE / PLAUSIBLE CONTRIBUTOR, NOT QUANTITATIVELY ISOLATED**
- **MEASURABLE/VISIBLE CANDIDATE, contribution not source-isolated**
- **MEASURABLE MINOR**, **NOISY**, **UNKNOWN**

1. **QoS 1 full transaction path versus QoS 0 direct** — PROVEN MATERIAL
   in this harness (~17–42 µs vs ~4–9 µs exploratory, broker included).
   Encode is not the delta. The outbound object graph / admission
   (`queue_publish`, receipt, store, inflight) is a **VISIBLE /
   PLAUSIBLE CONTRIBUTOR, NOT QUANTITATIVELY ISOLATED**.

2. **Runtime around inbound QoS 1 + reply, versus engine-only** — PROVEN
   MATERIAL as a coarse split (engine-only ~3 µs; full cell ~27–80 µs
   exploratory). The full cell includes the fake broker, callback, reply
   publish, and PUBACK wait. Sub-path shares are not isolated.

3. **Eager DATA/ACK one frame per loop turn** — PROVEN PATH IDENTITY of
   the #254/#420 contract. Hit rate is `1/burst` **under this
   burst/window scheduling shape**. Not proven as wasted CPU: exploratory
   QoS 0 µs/message **improves** as hit rate falls because `_run` batches
   the queue.

4. **Callback worker when a read coalesces multiple QoS 1 MESSAGE effects** —
   PROVEN PATH IDENTITY (0 % inline at coalesced 8 and 32). Exploratory
   throughput still improves; latency of the first message in a batch is
   UNKNOWN on this harness (no per-message timestamps).

5. **`SEND_ACK + MESSAGE` always a 2-effect collect** — PROVEN PATH
   IDENTITY / COST UNQUANTIFIED. `_on_qos1_auto` emits ACK then MESSAGE.
   `collect_from_engine()` sees `len(effects) == 2` and takes partition /
   pending / drain rather than the `len == 1` fast path. Historical
   `effect_batch_inline` (eight `SEND`s, 4.07 µs) must not be used as
   this cost.

6. **Owned-byte `next_packet` copies on the client decoder** — PROVEN
   PATH IDENTITY (owned copy at the packet boundary). Full-cell
   `cProfile` ranking is a **MEASURABLE/VISIBLE CANDIDATE, contribution
   not source-isolated**, because the fake broker uses its own
   `IncrementalDecoder`.

7. **MQTT 5 empty `Properties()` on QoS 1 ingress** — PROVEN PATH
   IDENTITY that an empty `Properties` exists on the v5 path. Cost:
   plausible contributor to the exploratory +0.54 µs engine delta,
   **not isolated**.

8. **UTF-8 topic encode on every publish** — MEASURABLE/VISIBLE
   CANDIDATE (`encode_utf8` in the QoS 0 profile). Not isolated from
   other encode work.

9. **Absolute `call_soon` per operation** — NOISY. The wrap counts
   harness `sleep(0)` and asyncio internals. Use `eager_rearms` (equals
   eager hits) for writer-specific wakeups.

10. **Full-path MQTT 5 vs 3.1.1 publish throughput on this VM** — NOISY
    (sign disagrees with isolated encode).

Nothing here is classified as an independent, correctness-free waste that
should be patched before native cross-client numbers exist.

## 8. Fast-path effectiveness

Hit rate = hits / (hits + misses). MQTT 3.1.1 and MQTT 5 matched on every
rate below. Eager rows are for **this burst/window scheduling shape**
(submit `burst` or fill `outstanding`, then yield).

| Fast path | low (1) | medium (8) | high (32) | Notes |
| --- | ---: | ---: | ---: | --- |
| QoS 0 direct (QoS 0 scenario) | 1.00 | 1.00 | 1.00 | Unused on QoS 1 (0.00) |
| v311 QoS 1 field decode | 1.00 | 1.00 | 1.00 | Inbound only |
| v5 QoS 1 field decode | 1.00 | 1.00 | 1.00 | Inbound MQTT 5 only |
| Callback inline, 1 pkt/read | 1.00 | 1.00 | 1.00 | Single MESSAGE per drain |
| Callback inline, coalesced | 1.00 | **0.00** | **0.00** | Worker batch (#402) |
| Eager DATA | 1.00 | 0.125 | 0.03125 | 1 per loop turn in this harness (#254/#420) |
| Eager ACK (inbound cells) | 1.00 | 0.125 | 0.03125 | Independent permit, same shape |
| Immediate writer admission | 1.00 | 1.00 | 1.00 | No `enqueue_suspensions` |
| Single-effect collect (QoS 1 publish) | 1.00 | 1.00 | 1.00 | SEND / PUBLISH_COMPLETE |
| Single-effect collect (inbound QoS 1) | 0.00 | 0.00 | 0.00 | Always SEND_ACK+MESSAGE |

A fast path that exists is not the dominant path under this harness's
medium/high shape. Eager and coalesced-callback are the two that **flip**
between low and medium here.

## 9. Potential optimisation targets

Maximum five. **None implemented.** Each is a hypothesis for a later A/B
on an eligible host, after native-async cross-client results exist.
Native PR #32 decides whether T1 is worth measuring more precisely; this
correction does not add that micro.

### T1 — Two-effect collect for auto QoS 1 (`SEND_ACK` + `MESSAGE`)

- **Where:** `InboundSession._on_qos1_auto` → `EffectPump.collect_from_engine`
  (`src/mqttium/protocol/inbound.py`, `src/mqttium/api/_effects.py`).
- **Measured:** always two effects; multi-effect partition/pending/drain
  is taken; `len == 1` inline is not. **Cost unquantified.** Do not cite
  `effect_batch_inline` 4.07 µs.
- **Why it exists:** wire ACK before application delivery; RM slot until
  `take_effects()`; epoch-scoped pending deque for mixed batches.
- **Minimal test (later, only if native RTT points here):** a
  source-shaped `[SEND_ACK, MESSAGE]` micro, not eight `SEND`s.
- **Correctness risk:** ACK/application order, RM handoff, stale-epoch
  discard, failing-flush settlement.
- **Performance risk elsewhere:** replay and mixed SEND+MESSAGE batches
  that today share the partition.

### T2 — QoS 1 outbound object graph (receipt / store / launch)

- **Where:** `OutboundSession.queue_publish`, `_prepare_publish_request`,
  `_launch`, receipt wait (`src/mqttium/protocol/outbound.py`,
  `src/mqttium/api/async_client.py`).
- **Measured:** full QoS 1 transaction vs QoS 0 direct is PROVEN MATERIAL
  in this harness. The object graph is a **visible / plausible
  contributor** (cProfile ranking). It is **not quantitatively isolated**
  from fake-broker PUBACK work.
- **Why it exists:** packet-id / flow-slot / store transaction with
  rollback; receipts before wire.
- **Minimal test:** isolate client CPU from the in-process broker before
  considering collapsing a temporary; `tracemalloc` per
  `publish_nowait(qos=1)` on an eligible host.
- **Correctness risk:** outbound rollback (`test_outbound_transaction.py`).
- **Performance risk:** persistence path if a memory-only shortcut diverges.

### T3 — Inline callback for small coalesced MESSAGE batches

- **Where:** `ApplicationDelivery._enqueue_message_batch` vs
  `dispatch_callback_inline` (`src/mqttium/api/_delivery.py`).
- **Measured:** 0 % inline once a read contains more than one QoS 1
  message. Coalesced high is still the fastest inbound cell in this
  harness (exploratory throughput).
- **Why it exists:** #402 — re-entrant `publish_nowait` from callbacks,
  async callbacks, burst isolation.
- **Minimal test:** allow inline for N=2 sync callbacks only, with the
  existing re-entrancy guard; measure RTT p50, not just ops/s.
- **Correctness risk:** deadlock if a callback publishes and the engine
  lock / delivery lock overlap; ordering vs later messages in the same
  read.
- **Performance risk:** a slow callback would block the reader again.

### T4 — MQTT 5 empty `Properties` on QoS 1 ingress

- **Where:** `decode_publish_fields_v5` / `_on_publish_v5`.
- **Measured:** v5 engine ingress exploratory total +0.54 µs and more
  gen0 versus v311. Empty `Properties` is present on the path. That is a
  **plausible contributor** to the delta, **not an isolated 0.54 µs
  cost**.
- **Why it exists:** MQTT 5 property slot is always present on the
  `Message`.
- **Minimal test:** intern a frozen empty `Properties` singleton for the
  no-property case **after** isolating it from the rest of v5 field
  decode.
- **Correctness risk:** callers mutating a shared empty object.
- **Performance risk:** none expected if the object is immutable.

### T5 — Do not retune eager budgets

Listed so it is **not** silently reopened. Hit rate `1/burst` in this
harness is the #254 contract under this scheduling shape. Exploratory
throughput improved under miss. Any change to 2/4/8, timing cutoffs, or
delayed re-arm needs a new eligible-host A/B that beats
`NATIVE-WRITER-HOP-2026-08-16.md` and the #254 rejection record.

## 10. What corrected `mqtt-python-client-bench` PR #32 must tell us

Native-async pairwise grids (`mqttium ↔ gmqtt`, `mqttium ↔ paho`). Do not
decide from bridged RTT.

| If native PR #32 shows… | Then the map says investigate… | Otherwise |
| --- | --- | --- |
| Publisher QoS 0 still far ahead, publisher QoS 1 at parity | Leave T2 alone; both natives may pay a QoS 1 transaction tax | If QoS 1 publish lags while QoS 0 does not, **first isolate client vs broker/peer**, then consider measuring T2 |
| Matched-load application RTT still lags while publisher QoS 1 is at parity | T1 (measure `[SEND_ACK, MESSAGE]` if needed) and T3, not the publisher | If RTT and publisher QoS 1 both lag, start by isolating the publish transaction, not by assuming the object graph |
| RTT gap only at high outstanding / when the broker coalesces | T3 (worker) and the eager 1/turn **latency** question under a comparable scheduling shape; still no budget bump without A/B | If the gap is already there at outstanding=1, T3 is the wrong target (inline already hits) |
| MQTT 5 native ingress/RTT lags 3.1.1 by more than exploratory noise | Measure T4 against the rest of v5 decode; do not assume empty `Properties` is the 0.54 µs | If protocols match, skip T4 |
| Parity on native publisher QoS 1 **and** native RTT | Ship nothing from this list | Keep this report as the baseline for the next regression |

## 11. Recommendation

**WAIT FOR NATIVE CROSS-CLIENT RESULTS BEFORE OPTIMIZING**

No independent, load-bearing waste was demonstrated. Fast paths that
already exist (QoS 0 direct, v311 QoS 1 field decode, idle callback
inline, outstanding=1 eager DATA/ACK) **do** dominate at low load in this
harness. The paths that miss under this harness's medium/high shape
(eager 1/turn, coalesced callback worker, 2-effect collect) are
documented contracts from #254, #402, and the SEND-before-application
rule. Changing them without native pairwise evidence would be hunting an
explanation for numbers this report is forbidden to use.

## Limitations

- In-process fake broker shares the client process and CPU. Reader,
  writer, broker decode, PUBACK construction, and `call_soon` are one
  `RUSAGE_SELF`.
- One baseline per cell on an unqualified host; no variance, no
  confidence interval, no eligible-runner preflight.
- Not an eligible reference host (`cpu_governor` unreadable).
- No TLS, no persistence, no topic alias, no MQTT 5 user properties, no
  QoS 2, no `on_publish`, automatic inbound ACK only.
- Counter wraps add ~13 % overhead; hit **rates** remain 0/1 or exact
  `1/burst` under this scheduling shape and are not sensitive to that
  overhead.
- `gc_count_delta` on some instrumented coalesced cells went negative
  (collections during the interval); treat GC deltas on instrumented
  cells as qualitative.
- µs figures are transcribed from the original agent campaign. This
  review correction did not re-run that campaign and does not commit the
  raw JSON.
