# Independent performance scan — residuals after #99–#106 fixes

Date: 2026-08-11.
Subject: `cursor/perf-audit-fresh-7ac5` @ `61005c1` (fixes from
[`PERFORMANCE-AUDIT-1.0.0rc1-INDEPENDENT.md`](PERFORMANCE-AUDIT-1.0.0rc1-INDEPENDENT.md)
already applied).
Host: CPython 3.12 on the audit VM. Numbers are diagnostic micro-probes — not
paired A/B release claims.

Method: re-read the live hot paths after the fix commit; falsify each suspicion
with a targeted probe. Explicitly **excluded** (already filed/fixed or rejected):

- `SEGMENT_THRESHOLD` / payload-copy cliff (#99)
- MQTT 5 property double-encode admit+launch (#100 / `property_wire`)
- MQTT 5 QoS 0 ingress fast path (#101)
- SQLite one `COMMIT` per single `queue_publish` (#102)
- `TopicMatcher` O(filters) (#103)
- delivery `logical_size` property re-encode (#104 / `Properties` memo)
- contiguous frame drop / `_retain_publish_item` (#105)
- topic validate-then-`encode_utf8` (#106 / `topic_validated`)
- `PacketType.from_byte` nibble table; Paho sync MID `Future` handoff;
  `_engine_lock` without measurement; inline callbacks / fairness yields (#39)

## Ranked new candidates

| Rank | Severity | Finding |
| --- | --- | --- |
| 1 | High | MQTT 5 QoS 1/2 ingress still builds `PublishPacket` then copies into `Message` |
| 2 | High | Inbound QoS 1/2 emits `MESSAGE` then `SEND`, forcing EffectPump SEND-first reorder on **every** ACK’d publish |
| 3 | High (WS) | WebSocket `write_many` masks one RFC 6455 frame per MQTT packet (no coalesce) |
| 4 | High (SQLite + flow queue) | `_launch` always `put_out`s the full payload BLOB — rewrites when the record was already stored as `QUEUED` |
| 5 | Medium | Default delivery `small_message_limit` ≈ 126 B + `4×len(topic)` heuristic + any properties → accounted path |
| 6 | Medium | Inbound QoS 1 ACK builds `PubAckPacket(mid).encode(...)` instead of the fixed 4-byte frame |
| 7 | Medium | Outbound PUBACK settle always runs `PubAckPacket.decode` for a 2-byte body |
| 8 | Low–Medium | MQTT 5 empty property table allocates a fresh `Properties()` per packet |
| 9 | Low–Medium | Paho `MQTTMessage` stores topic as UTF-8 bytes then re-decodes on every `.topic` read |
| 10 | Low | Non-ASCII outbound topics still encode three times (validate + `size_parts` + wire) |
| 11 | Low | WebSocket `os.urandom(4)` per masked frame |
| 12 | Low | Outbound QoS 0 `SEND`+`PUBLISH_COMPLETE` always takes the multi-effect partition (ordered, but two list allocs) |

### 1. MQTT 5 QoS 1/2 still uses `PublishPacket`

`protocol/inbound.py:267-336` — MQTT 5 QoS 0 is short-circuited (`_decode_v5_qos0_fields`);
QoS 1/2 fall through to `PublishPacket.decode` then field-copy into `Message` /
`_on_qos1`.

**Hypothesis:** same shape as the pre-#101 MQTT 5 QoS 0 gap — intermediate frozen
packet + second object.

**Measured (handler, CONNECTED, auto-ack):**

| Path | ns/msg | vs baseline |
| --- | ---: | --- |
| MQTT 5 QoS 1 | 5 581 | — |
| v3.1.1 QoS 1 (direct fields) | 3 890 | MQTT 5 **1.43×** |
| MQTT 5 QoS 0 (fast path) | 2 595 | QoS 1 **2.15×** |
| decode+`Message` only (empty props) | packet 3 050 / direct 1 678 | **1.82×** |

Path trace: MQTT 5 QoS 1 hits `PublishPacket.decode` once; MQTT 5 QoS 0 and
v3.1.1 QoS 1 hit 0.

**How to measure:** extend `tests/unit/test_qos0_v311_decode_fastpath.py`-style
path pins; A/B `_decode_v5_qos1_fields` → `_on_qos1` vs current; engine handler
ns/msg and `benchmarks/hotpath_profile.py` `ingress_engine` / broker-fed QoS 1.

### 2. Inbound QoS 1/2 effect order forces EffectPump reorder

`protocol/inbound.py:416-429` emits `MESSAGE` then `_send(PUBACK/PUBREC)`.
`api/_effects.py:93-118` partitions SEND-first and rebuilds when a SEND follows a
non-SEND.

**Hypothesis:** every auto-ack’d inbound QoS ≥ 1 pays two list allocations plus a
concatenate, and never the single-effect inline collect path.

**Measured:** 1000 QoS 1 (and QoS 2) ingress batches →
`multi_effect_batches=1000`, `reordered_batches=1000`. QoS 0 ingress:
`multi=0`. Emitting `SEND` before `MESSAGE` matches the order the pump already
forces and would drop the rebuild (partition lists may remain; see 0.2.0b4 note
on detect-then-partition — do not revive that trade without new numbers).

**How to measure:** `EffectPump.stats().reordered_batches` under QoS 1 ingress;
paired A/B of emit order with `hotpath_profile` / broker-fed RTT.

### 3. WebSocket: one masked frame per MQTT write item

`transport/websocket.py:101-121` / `_mask_client_frame` — `WritePump` coalesces
contiguous MQTT frames into `write_many(parts)`, but each part becomes its own
masked binary frame (`os.urandom` + full XOR copy).

**Hypothesis:** WS publish throughput is dominated by per-packet framing, not
MQTT encode.

**Measured:** 32 × 50 B MQTT parts → **57.2 µs** masking vs **4.6 µs** for one
coalesced payload (**~12×**). Single 256 B / 4 KiB masks: ~2.1 µs / ~6.6 µs.

**How to measure:** `WritePump` batch → `WebSocketTransport.write_many` A/B
(coalesce parts under `max_write_batch_bytes` into one binary frame); WS publish
throughput paired run.

### 4. SQLite: `_launch` re-`put_out`s full payload after `QUEUED`

`protocol/outbound.py:642` always `store.put_out(msg)`.
`persistence/sqlite.py:500-530` upserts topic/payload/properties on conflict.
Flow-windowed path: `queue_publish` stores `QUEUED`, then `drain` → `_launch`
writes the same BLOB again. Immediate launch still pays one full insert (expected).

**Hypothesis:** beyond #102’s per-call `COMMIT`, large flow-queued publishes pay
a second payload write; `update_out` (state/dup only) is the natural alternative
when the row already exists.

**Measured (batched):** 256 KiB median insert ~172 µs, launch-rewrite `put_out`
~258 µs; 2 MiB `put_out`/`update_out` ≈ **1.9×**. Small payloads: ratio ~1.1×
(lock/commit dominates).

**How to measure:** SQLite trace / payload-byte counters around
`queue_publish` with `Receive Maximum=1` then PUBACK drain; A/B `update_out` when
row present.

### 5. Delivery “small” gate is tight; properties always accounted

`api/_delivery.py:54-76`, `182-216`, `364-376` — default client yields
`small_message_limit≈126`. Fast path rejects when
`len(payload)+4*len(topic) > limit` **or** `bool(message.properties)`.

**Hypothesis:** typical >100 B telemetry and any MQTT 5 property bag skip the
unaccounted / batch-inline path and pay reservation + token plumbing (encode
itself is memoised after #104).

**Measured:** 64 B + topic 10 → small; 100 B + topic 20 → accounted; 20 B + one
property → accounted (`pending_bytes` reserved).

**How to measure:** counter of fast vs `accept()` / `logical_size` under realistic
payload/topic mix; A/B ASCII-aware size estimate and/or small property bags on
the unaccounted path.

### 6. Inbound PUBACK via `PubAckPacket` wrapper

`protocol/inbound.py:429` — `PubAckPacket(mid=mid).encode(config.protocol)`.
`packets/acks.py:82-93` already special-cases success/no-props to a 4-byte frame,
but still constructs the frozen dataclass first.

**Hypothesis:** ~0.5 µs/msg avoidable on the subscriber ACK hot path.

**Measured:** `PubAckPacket(mid).encode` **646 ns** vs `bytes((0x40,2,…))`
**88 ns**.

**How to measure:** micro A/B on `_on_qos1` auto-ack; QoS 1 ingress handler ns.

### 7. Outbound PUBACK always fully decoded

`protocol/outbound.py:525-526` — `PubAckPacket.decode` on every settle, including
MQTT 3.1.1’s 2-byte remaining length.

**Hypothesis:** mid unpack is enough for the success path; full decode is for
reason/properties.

**Measured:** decode **746 ns** vs mid unpack **40 ns**.

**How to measure:** branch v3.1.1 / empty-body fast mid extract; `qos1_cycle_*`
in `hotpath_profile`.

### 8. Empty MQTT 5 properties → new `Properties()` each time

`codec/properties.py:317-319` — `buf[offset]==0` returns `Properties()`.
MQTT 5 QoS 0 fast path and generic decode both take this path.

**Hypothesis:** ~200 ns + GC pressure per MQTT 5 PUBLISH with empty props;
v3.1.1 uses `properties=None`.

**Measured:** empty `decode_properties` ~**220 ns**; new object identity each call.

**How to measure:** return `None` (or a shared empty singleton) A/B; ingress
MQTT 5 QoS 0 allocs via tracemalloc / `hotpath_profile`.

### 9. Paho façade topic encode/decode round-trip

`compat/paho.py:111-122` — `__init__` does `msg.topic.encode("utf-8")`;
`.topic` decodes again. Distinct from TopicMatcher (#103) and MID Future.

**Hypothesis:** every callback that reads `.topic` re-decodes; construction
always encodes.

**Measured:** encode+decode ~**68 ns** for a short topic (lower bound; real
cost scales with topic length and callback fan-out).

**How to measure:** store `str` (Paho also exposes `str`) or cache decoded;
compat ingress callback profile.

### 10. Non-ASCII topic still encoded three times outbound

After #106, ASCII is: validate (len only) → `size_parts` (len only) →
`topic.encode` once in `encode_publish_item`. Non-ASCII:
`validate_utf8` encodes for length (`codec/primitives.py:56-61`),
`size_parts` encodes again (`outbound.py:805`), wire encodes a third time
(`publish.py:59`).

**Hypothesis:** small absolute cost (~70 ns × 2 extra) but pure redundancy for
i18n topics on QoS ≥ 1.

**How to measure:** pass sized `topic_bytes` from validate/`size_parts` into
`encode_publish_item`; non-ASCII publish microbench.

### 11. WebSocket `os.urandom(4)` per frame

`transport/websocket.py:375-376` — mask key from `os.urandom` on every frame
(compounds with finding 3).

**Measured:** ~**287 ns**/call.

**How to measure:** fold into WS coalesce A/B; optional CSPRNG batching only if
coalesce alone is insufficient.

### 12. Outbound QoS 0 multi-effect partition without reorder

`outbound.py:390-393` emits `SEND` then `PUBLISH_COMPLETE`. Already ordered, but
`len==2` skips single-effect inline collect and always builds `sends`/`others`
lists (`_effects.py:93-118`). Direct QoS 0 writer bypasses this when
`on_publish is None`.

**Hypothesis:** low single-digit percent on the non-direct QoS 0 path only.

**How to measure:** `EffectPump` stats with `on_publish` set; compare to direct
path. Do not revive detect-then-partition without beating the 0.2.0b4 A/B.

## Suggested next measurements (priority)

1. MQTT 5 QoS 1 direct field decode (candidate 1) — mirrors proven v3.1.1/#101 work.
2. Emit `SEND` before `MESSAGE` on inbound auto-ack (candidate 2) — tiny code change, counter-falsifiable.
3. WS frame coalesce (candidate 3) if WebSocket is a product path.
4. SQLite launch `update_out` when row exists (candidate 4) under flow-limited QoS 1.
