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
now accepts `_property_bytes`.

## Remaining leads (measured, not treated as defects)

Same host, 5 000 iterations unless noted. MQTT 5 inbound uses a valid CONNACK
with `client_id="probe"`.

- **MQTT 5 inbound properties vs `small_delivery`.** Engine-only QoS 0 decode
  is 4.55 µs without properties and 8.29 µs with the IoT bag (0 encodes either
  way). Adding iterator delivery: 4.77 µs on the small path (`small=1`, 0
  encode) vs 18.90 µs when properties are present (`small=0`, 1 uncached
  `delivery.logical_size` encode). Warm `logical_size` after that first encode
  is 2.32 µs (cache hit, still one `_signature`). The batch skip for
  property-bearing messages is documented. Seeding `_encoded` from the wire
  slice at decode would still pay `_signature` on the size path; not done here.
- **Inbound topic aliases.** Empty Topic Name + alias 1: 1.00
  `_resolve_topic_fields` per PUBLISH, 0 property encodes, 5.34 µs. Matches
  "one dict lookup, no re-encode".
- **SQLite session resume** of 256 inflight 4 KiB QoS 1 records: 262 `SELECT`
  (ordered mid lists, two `out_summary_pages` scans — hydrate then replay —
  then `SELECT * FROM outbound WHERE mid=?` once per record). The payload reads
  are required to retransmit; the extra summary scan is cheap next to those
  256 BLOBs. Summaries remain the right design for a large `QUEUED` queue.
- **`can_ever_admit` wait path:** 1.00 cache-hit `encode_properties` via
  `logical_size`, 2.77 µs. Success-path launch does not call this.
- SQLite `BEGIN`/`COMMIT` per `put_out` and `complete_out` is unchanged;
  `batch()` remains `queue_publish_many` only.
- `TopicMatcher` linear wildcard scan: unchanged from the first pass
  (~2.7 µs/lookup with 200 exact + 5 wildcards).
