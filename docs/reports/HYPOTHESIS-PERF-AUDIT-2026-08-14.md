# Hypothesis-driven performance audit — 2026-08-14

Date: 2026-08-14.

Base commit: `702fbd6` (`1.0.0rc4` / `main`). CPython 3.12.3, Linux 6.12.

This audit does not re-run the QoS 0/1 encode/decode micro-campaign already
recorded in the hot-path reports. It picks one realistic production shape,
states how many times named functions should run and what they should cost,
then compares that to `cProfile`, exact wrappers and `sqlite3` traces.

## Scenario

A durable MQTT 3.1.1 subscriber using `SqliteInflightStore` (so outbound
inflight and restart survive a process crash) receiving automatic-ack QoS 1
telemetry. The inbound table is empty: auto-ack QoS 1 does not persist records.
That is the common "SQLite is open for the session, inbound is fire-and-forget
QoS 1" layout.

Control: the same engine loop against `MemoryInflightStore`.

Companion scenarios (to keep the method honest, not to hunt a second defect):

- MQTT 5 QoS 1 publish+PUBACK with a typical IoT property bag on the same
  `Properties` object;
- MQTT 3.1.1 QoS 1 publish+PUBACK, memory store;
- SQLite QoS 1 4 KiB publish+PUBACK (payload-free settle);
- session resume of 256 inflight 4 KiB QoS 1 records;
- `TopicMatcher` with 200 exact filters and 5 wildcards;
- a 64-packet QoS 0 ingress burst.

## Hypotheses

For 5 000 automatic inbound QoS 1 PUBLISH frames against an empty SQLite
inbound table:

| Probe | Expected per message | Why |
| --- | ---: | --- |
| `put_in` | 0 | auto-ack QoS 1 does not persist |
| `get_in` | 0 | transition store must not rebuild payloads |
| `in_meta` | 0 | nothing is stored, so nothing to look up |
| `contains_in` | 0 | QoS 1 uses `in_meta`, not existence-then-meta |
| `mark_in_delivered` | 0 | auto-ack skips delivery marks |
| `SELECT` on `inbound` | 0 | empty table, no collision to detect |

Wall-clock cost should match the memory-store control, because the only extra
work on the SQLite path is durable outbound machinery that this scenario never
touches.

MQTT 5 property companion: `_encode_properties_uncached` is 0 after warmup
(cache hit); `encode_properties` is 2 (size then encode); a cache hit is cheap
relative to a real encode.

## Measurement at `702fbd6`

Isolated loop, no `cProfile` overlay, 5 000 messages:

| Store | µs/msg | `in_meta`/msg | SQL/msg |
| --- | ---: | ---: | ---: |
| Memory | 7.33 | 1.00 | 0 |
| SQLite | 10.77 | 1.00 | 1.00 SELECT |

SQLite/memory = **1.47**. Every automatic QoS 1 PUBLISH executed

```sql
SELECT state, user_acked, logical_size, seq FROM inbound WHERE mid=?
```

and got no row. `cProfile` on the SQLite path put `Connection.execute` next to
`_on_qos1` itself.

The MQTT 5 companion mostly matched: uncached property encodes were 0 after
warmup, `get_out` was 0 on PUBACK, QoS 0 ingress did not re-encode topics.
One note became Finding 2 (below). Session replay of 256 inflight records
materialised each payload once (`get_out` = 256) and re-encoded once, as the
frame policy requires.

## Finding

`InboundSession._on_qos1` always called `in_meta` / `get_in` before the
automatic-ack path, to detect a packet-identifier collision with a persisted
QoS 2 (or leftover manual-ack) record. That is necessary **while such a record
exists**. It is not necessary when the inbound store is empty, which is the
steady state of an auto-ack QoS 1 subscriber.

Receive Maximum (`_inflight`) cannot be used as a proxy: it also counts
auto-ack slots that never touch the store.

## Fix

Track inbound **store occupancy** separately (`_stored_inbound`). Probe
`in_meta` / `get_in` / `contains_in` only while that count is non-zero.
Increment on a successful `put_in`, decrement on a successful complete/pop,
zero on `discard_session`. Hydration seeds the count from recovered identifiers.

Collision behaviour is unchanged: a live QoS 2 record keeps the count at 1, so
a QoS 1 PUBLISH with the same identifier still probes and still disconnects
with `0x82`. Manual-ack QoS 1 still persists and still redelivers duplicates.

## Confirmation after the fix

Same isolated loop:

| Store | µs/msg | `in_meta`/msg | SQL/msg |
| --- | ---: | ---: | ---: |
| Memory | 7.07 | 0.00 | 0 |
| SQLite | 6.97 | 0.00 | 0.00 |

SQLite/memory = **0.99**. The empty-table SELECT is gone. Unit tests in
`tests/unit/test_inbound_empty_store_probe.py` pin the probe skip, the
QoS 2 occupancy restore, the collision, and manual-ack persistence.
`tests/unit` : 1008 passed.

This is not a release-gate throughput claim. The host is a cloud agent VM; the
ratio and the SQL counts are the evidence, not an absolute msgs/s figure.

## Finding 2 — MQTT 5 QoS 1 launch encodes the property table twice

Hypothesis for a typical IoT property bag (`content_type`,
`payload_format_indicator`, `message_expiry_interval`, two user properties)
reused across 5 000 connected QoS 1 publish+PUBACK cycles:

| Probe | Expected per launch | Why |
| --- | ---: | --- |
| `_encode_properties_uncached` | 0 after warmup | encode cache |
| `encode_properties` | 1 | one walk feeds both wire-size and the frame |
| encoder `encode_properties` | 0 | admission hands the encoded table to the encoder |

`queue_publish` already documented that contract: "One property encode and one
topic measurement feed both the wire-size check and the logical budget."

Measured at `702fbd6` / `fe5fb7e` (before this handoff): `encode_properties` =
**2** per launch (`size_parts`, then `encode_publish_item_v5`). Both were cache
hits. `_freeze_property_signature` / `_signature()` was ~88% of a cache hit
(~1.85 µs of ~2.10 µs), so the second call was only ~30% cheaper than encoding
from scratch (~2.93 µs uncached). `cProfile` on that cycle put
`_freeze_property_signature` at the top.

The encode cache must keep detecting in-place `values` mutation (bytearray of
the same length, `user_property.append`). A generation counter on `set()` is
not enough. The fix is therefore not a cheaper fingerprint: it is to stop
asking the encoder to encode at all.

## Fix (Finding 2)

`size_parts` returns the encoded property table as a fourth element. Connected
QoS 1/2 launch passes it to `encode_publish_item_v5` as `_property_bytes`, the
same pattern as QoS 0 `_topic_bytes`. MQTT 3.1.1 returns `None`; an empty MQTT 5
table returns `b"\x00"` without calling `encode_properties`. The queued drain
path still encodes at launch (no stored property bytes on the record); that is
a different, colder path.

In-place mutation tests in `tests/unit/test_properties.py` are unchanged.

## Confirmation after Finding 2

Isolated connected MQTT 5 QoS 1 launch, same IoT property bag reused across
5 000 cycles, no `cProfile` overlay:

| Probe | per launch |
| --- | ---: |
| `outbound.encode_properties` | 1.00 |
| encoder `encode_properties` | 0.00 |
| `_encode_properties_uncached` | 0.00 |
| `_signature` | 1.00 |
| wall | 6.88 µs |

`tests/unit/test_property_bytes_handoff.py` pins the encoder skip and wire
identity. `tests/unit`: 1012 passed, including the launch-failure double that
now accepts `_property_bytes`. Findings 1–3 later landed on `main` as #225,
#226 and #227.

## Finding 3 — connected QoS 1/2 encodes a non-ASCII Topic Name three times

Hypothesis for a connected QoS 1 launch (memory store, MQTT 5, no properties),
matching the documented "non-ASCII publication encodes its topic twice, not
four" leftover from `0.2.0b4`:

| Probe | Expected per launch | Why |
| --- | ---: | --- |
| `str.encode` on the Topic Name | 1 | validation produces the bytes; the encoder reuses them, as QoS 0 already does |
| wall vs ASCII of the same encoded size | ~1× | encoding is the variable cost; everything else is identical |

Measured after Findings 1–2, before this handoff, 5 000 connected QoS 1
publish+PUBACK cycles, `CountingStr.encode`:

| Topic | `str.encode` | µs/msg | vs ASCII |
| --- | ---: | ---: | ---: |
| ASCII 64 B | 1 | 4.00 | 1.00× |
| UTF-8 short (`capteurs/été`) | **3** | 4.35 | 1.09× |
| UTF-8 256 B | 3 | 4.81 vs 4.11 | 1.17× |
| UTF-8 1024 B | 3 | 5.80 vs 4.53 | 1.28× |
| UTF-8 4096 B | 3 | **9.46 vs 4.65** | **2.03×** |

The three encodes were `validate_utf8` (discarded), `size_parts`
`topic.encode("utf-8")` (discarded), and `encode_utf8` in the PUBLISH encoder.
QoS 0 already hands `_topic_bytes` from `encode_validated_publish_topic`.
Connected QoS 1 did not, because PR #178 kept that path validation-only so a
queued record would not retain a discarded copy. The connected launch path
encodes immediately; it is the same situation as QoS 0.

Time, not just the count, is the defect: a 4 KiB UTF-8 Topic Name costs twice
an ASCII name of the same encoded size.

## Fix (Finding 3)

`validate_utf8` / `validate_publish_topic` return the MQTT byte length.
`size_parts` accepts that length so it does not encode again to measure.
When the session is connected, `_validate_publish_request` uses
`encode_validated_publish_topic` for QoS 1/2 as well as QoS 0, and
`_try_launch` passes `_topic_bytes` into the encoder. An offline queue still
calls `validate_publish_topic` only — no discarded Topic Name copy on a
10 000-deep wait.

## Confirmation after Finding 3

Paired isolated loop on this host, 8 000 connected MQTT 5 QoS 1
publish+PUBACK cycles, median of 5 trials, `time.process_time` (equal to wall
here). `CountingStr.encode` is 1 after the fix, 3 before.

| Topic | before µs | after µs | encodes |
| --- | ---: | ---: | ---: |
| ASCII 4096 B | 9.16 | 8.68 | 1 → 1 |
| UTF-8 4096 B | **14.60** | **10.38** | **3 → 1** |
| ASCII 64 B | 8.25 | 8.08 | 1 → 1 |
| UTF-8 short | 8.39 | 7.98 | **3 → 1** |

UTF-8 4096 B vs ASCII of the same encoded size: **1.59× → 1.20×**. The leftover
1.20× is the one necessary encode (~1.64 µs for 4 KiB UTF-8 vs ~80 ns ASCII).
The two discarded encodes were the defect: ~4.2 µs, matching 2 × 1.64 µs.

`tests/unit/test_topic_bytes_handoff.py` pins the encoder skip, the offline
non-retain, MQTT 3.1.1, `CountingStr.encode == 1`, and wire identity.
`tests/unit`: 1019 passed.

This is not a release-gate throughput claim. The host is a cloud agent VM; the
ratio and the encode counts are the evidence.

## Finding 4 — small-delivery vetoes decoded MQTT 5 properties that fit the limit

Hypothesis for iterator delivery of a typical IoT property bag
(`content_type`, `payload_format_indicator`, `message_expiry_interval`, two
user properties) on a 5-byte payload and a short ASCII topic, default
`AsyncClient` budgets (`small_limit` = 127):

| Probe | Expected | Why |
| --- | --- | --- |
| small-path enqueue | yes | payload + 4×topic + encoded table = 104 ≤ 127 |
| `encode_properties` during accept | 0 | size is known from the decoded table |
| CPU vs no-properties | ~1× | same unaccounted put_nowait |

Measured before this change, 5 000 `AsyncClient._apply_effect` cycles:

| Message | footprint | accounted | µs |
| --- | ---: | ---: | ---: |
| `properties=None` | 41 | 0 | 0.64 |
| empty `Properties()` | 41 | 0 | 0.71 |
| IoT bag (application-built) | 104 | **5000** | **3.65** |
| `payload_format_indicator` only | 44 | **5000** | 2.03 |

104 ≤ 127, so the size check would have admitted the bag. `_accept_iterator_fast`
vetoed on `bool(message.properties)` and fell through to `logical_size`, which
re-encoded the table. Earlier this audit listed that skip as documented; the
contract only talks about large **payloads** starving telemetry. The bool veto
is coarser than the limit it sits next to.

The encoded table length is already on the wire at decode. Re-encoding it to
learn a size the decoder just consumed is the same class of leftover as
Findings 2 and 3.

## Fix (Finding 4)

`decode_properties` records `_wire_size` (property-length VBI plus table).
`set()` / `add_user_property` clear it. Small-delivery adds that length to the
existing `len(payload) + 4 * len(topic)` bound. An application-built bag with
`_wire_size == 0` stays on the accounted path, so tests that inject a
`Properties()` without going through the decoder do not silently become
unaccounted.

Oversized bags (200-byte `correlation_data`) remain accounted.

## Confirmation after Finding 4

Same host, 8 000 iterator-delivery cycles, messages produced by decoding a
real MQTT 5 PUBLISH:

| Message | µs | accounted |
| --- | ---: | ---: |
| no properties | 0.68 | 0 |
| empty table | 0.84 | 0 |
| IoT bag (decoded) | **0.77** | **0** |

Delivery of the IoT bag is 3.65 → 0.77 µs (**4.7×**), in line with the
no-properties path. `tests/unit/test_small_delivery_properties.py` pins the
small-path enqueue, the application-built accounted fallback, oversized
decode, and `set()` invalidation.

After rebase onto `4962b8b` (`main` with #221/#223/#225/#226/#227), Finding 4
still holds: decoded IoT bag 0.74 µs, accounted=0, `encode_properties`=0.

## Finding 5 — accounted inbound still re-encodes a decoded property table

Finding 4 recorded `_wire_size` and used it only as the small-path gate.
Messages that do not fit `small_limit` still called `logical_size` →
`encode_properties`.

Hypothesis for a decoded 200-byte `correlation_data` PUBLISH (footprint 246 >
127) after Finding 4:

| Probe | Expected | Why |
| --- | --- | --- |
| small-path | no | 246 > 127 |
| `encode_properties` during accept | 0 | `_wire_size` is the table length already |
| accounted | all | oversized on purpose |

Measured after Finding 4, before this reuse, 2 000 then 8 000 iterator cycles:

| | encode/msg | µs | accounted |
| --- | ---: | ---: | ---: |
| before | 1.00 | 2.47 | all |
| after | **0.00** | **1.37** | all |

The 1.10 µs gap is the discarded encode (`_signature` plus table walk). The
message remains charged against the accounted budget.

## Fix (Finding 5)

`ApplicationDelivery.logical_size` and `InboundSession.logical_size` use
`_wire_size` when it is positive. Application-built bags (`_wire_size == 0`)
still encode. Empty tables still contribute 0 logical bytes.

## Finding 6 — MQTT 5 QoS 0 still uses the generic field decoder

MQTT 3.1.1 QoS 0 already decodes straight into the delivered `Message`
(`decode_qos0_message_v311`). MQTT 5 skipped `PublishPacket` but still ran every
QoS 0 PUBLISH through `QoS(qos_raw)`, `decode_publish_fields_v5` (the QoS 1/2
field parser) and `_resolve_topic_fields`, then constructed a second `Message`.

Hypothesis for engine-only inbound QoS 0 with an empty MQTT 5 property table
and a non-empty Topic Name (no alias):

| Probe | Expected per message | Why |
| --- | ---: | --- |
| `decode_publish_fields_v5` | 0 | QoS 0 has its own decoder, as 3.1.1 already does |
| `QoS()` | 0 | the handler branches on the flag bits; the message uses `AT_MOST_ONCE` |
| `_resolve_topic_fields` | 0 | empty table cannot carry `topic_alias`; Topic Name is present |
| `decode_properties` | 1 | MQTT 5 requires the property-length byte |
| wall vs MQTT 3.1.1 | ~ decode_properties + `Properties()` | protocol tax, not leftover Python |

Measured before this change, 20 000 isolated `handle_raw` + `take_effects`
cycles, `time.process_time`:

| Path | µs | vs 3.1.1 |
| --- | ---: | ---: |
| MQTT 3.1.1 | 2.28 | — |
| MQTT 5 empty table | **3.04** | **+0.76** |
| MQTT 5 IoT bag | 6.60 | +4.31 |

`cProfile` on 8 000 empty MQTT 5 frames: 6 extra Python calls per message vs
3.1.1, including `QoS.__call__`, `_resolve_topic_fields` and `dict.get` for an
alias that cannot exist. Isolated pieces: `QoS(0)` 0.19 µs, `_resolve` 0.12 µs,
`Message()` 0.84 µs (paid again after the field tuple). `decode_properties` of
`0x00` is 0.25 µs and is the one necessary extra.

A monkey-patched specialized handler on the same process: empty table **2.50 µs**
(−0.54 vs current), IoT bag 5.91 µs (−0.69). The leftover is the incomplete
MQTT 5 half of the QoS 0 specialization, not the property byte.

## Fix (Finding 6)

`decode_qos0_message_v5` unpacks the Topic Name, decodes the property table and
builds the `Message` with `QoS.AT_MOST_ONCE`. `_on_publish_v5` takes that path
when the QoS bits are 0. `_resolve_topic_fields` runs only when the Topic Name
is empty or the decoded table carries `topic_alias`, so establishing and
reusing an inbound alias is unchanged. QoS 1/2 still use
`decode_publish_fields_v5`. Empty MQTT 5 tables still produce `Properties()`,
not `None`, so `message.properties is None` continues to distinguish 3.1.1.

## Confirmation after Finding 6

Same host, 20 000 isolated cycles after the change (filled after tests).

## Remaining leads (measured, not treated as defects)

Same host. MQTT 5 inbound uses `client_id="probe"`.

- Engine-only MQTT 5 decode of a property-bearing PUBLISH remains a decode-walk
  cost after Finding 6 (the empty-table gap vs 3.1.1 should shrink to
  `Properties()` plus `decode_properties`). Delivery no longer multiplies that
  (Findings 4–5).
- `_check_outbound_size` still calls `item_size` when `maximum_packet_size is
  None` (78 ns on this host). Threading the size through the encoder is the
  widening that earlier passes refused; an early return is ~1% of a QoS 1
  launch and is left.
- **Inbound topic aliases.** Empty Topic Name + alias 1 still pays one
  `_resolve_topic_fields` per PUBLISH by design.
- **SQLite session resume** of 256 inflight 4 KiB QoS 1 records: 262 `SELECT`
  (ordered mid lists, two `out_summary_pages` scans — hydrate then replay —
  then `SELECT * FROM outbound WHERE mid=?` once per record). The payload reads
  are required to retransmit; the extra summary scan is cheap next to those
  256 BLOBs.
- **`can_ever_admit` wait path:** 1.00 cache-hit `encode_properties` via
  `logical_size`. Success-path launch does not call this.
- SQLite `BEGIN`/`COMMIT` per `put_out` and `complete_out` is unchanged;
  `batch()` remains `queue_publish_many` only.
- `TopicMatcher` linear wildcard scan: unchanged from the first pass
  (~2.7 µs/lookup with 200 exact + 5 wildcards).
