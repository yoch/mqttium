# Independent performance audit — `1.0.0rc1` @ `6f72296`

Date: 2026-08-11.
Subject: `mqttium 1.0.0rc1`, commit `6f72296c579a26b421bdb793d3ab58a3be75c47f`.
Host: CPython 3.12.3 on the audit VM (not a release-evidence host). Numbers below
are diagnostic micro-probes and `benchmarks/hotpath_profile.py` call counts —
they identify redundant work and cliffs, they are not paired A/B release claims.

Method: read the current hot paths, then falsify each suspicion with a
targeted probe under `/tmp` (not committed). No prior report was used as a
source of candidates.

Companion GitHub issues (one finding each): see the “Issues” column below.

## Hot-path call budget (cProfile)

`PYTHONPATH=benchmarks:src python3 benchmarks/hotpath_profile.py` on this tree:

| Scenario | Calls / op | Notes from top self time |
| --- | ---: | --- |
| `encode_qos0` | 22.6 | encode + UTF-8 |
| `ingress_engine_qos0` | 19.6 | v3.1.1 QoS 0 fast decode |
| `native_publish_nowait_qos0` | 50.6 | `encode_publish_item`, `validate_utf8`, `encode_utf8`, writer |
| `async_publish_nowait_qos0` | 53.7 | same shape as native |
| `qos1_cycle_memory` | 120.0 | encode + launch + PUBACK |
| `qos1_cycle_sqlite` | 132.1 | dominated by `sqlite3.commit` / `execute` |
| `compat_publish_qos1` | 158.9 | lock + `Future.result` + drain |
| `receipt_settle_unawaited` | 2.0 | already cheap |

## Findings

| # | Severity | Finding | Issue |
| --- | --- | --- | --- |
| 1 | High | `SEGMENT_THRESHOLD` (1 MiB) leaves a payload-copy cliff just below the cut | #99 |
| 2 | High | MQTT 5 QoS ≥ 1 encodes the property table twice on admit+launch | #100 |
| 3 | High | MQTT 5 QoS 0 ingress has no v3.1.1-style direct `Message` path | #101 |
| 4 | High | `SqliteInflightStore`: each single `queue_publish(qos=1)` commits once | #102 |
| 5 | Medium | Paho `TopicMatcher.iter_match` is O(filters) per message | #103 |
| 6 | Medium | Delivery byte accounting re-encodes MQTT 5 properties for `len(...)` | #104 |
| 7 | Medium | Contiguous QoS 1/2 frames are dropped from the record → reconnect re-encodes | #105 |
| 8 | Low–Medium | Every outbound publish validates MQTT-UTF-8 then encodes the topic again | #106 |

Index: #108 (complete); #107 is an incomplete duplicate from the same filing batch.

### 1. Payload-copy cliff under `SEGMENT_THRESHOLD`

`packets/publish.py` builds a contiguous `bytes` frame whenever
`len(payload) < SEGMENT_THRESHOLD` (`transport/writes.py`, 1 MiB). Segmented
`(header, payload)` form starts only at the threshold.

Measured encode cost (median ns/op, same topic, QoS 0):

| Payload | Segmented? | ns/op | ns/KiB |
| --- | --- | ---: | ---: |
| 64 B | no | 814 | — |
| 4 KiB | no | 1 011 | 253 |
| 64 KiB | no | 3 936 | 62 |
| 256 KiB | no | 100 306 | 392 |
| 512 KiB | no | 239 320 | 467 |
| 999 KiB | no | 541 671 | 542 |
| 1 MiB | yes | 1 040 | 1.0 |
| 1 MiB + 1 | yes | 1 045 | 1.0 |

Crossing the threshold drops cost by roughly **500×**. Large-but-sub-MiB
publishes pay a full payload copy that segmented writes already know how to
avoid. Probe: identity of the segmented payload object is preserved
(`item[1] is payload`).

### 2. MQTT 5 property table encoded twice on QoS ≥ 1

`OutboundSession.queue_publish` calls `size_parts()` → `encode_properties(...)`
for wire/budget sizing, then `_launch` → `PublishPacket.encode_write_item` →
`_props_or_empty` → `encode_properties` again.

Instrumentation on a CONNECTED engine with non-empty properties: **exactly 2**
`encode_properties` calls per QoS 1 launch, **1** per QoS 0. Micro-probe of
encode-once vs size-then-encode: **1.76×**, about **+3.4 µs**/op for a small
user-property bag on this host.

### 3. MQTT 5 QoS 0 ingress lacks the v3.1.1 fast path

`InboundSession.on_publish` short-circuits MQTT 3.1.1 QoS 0/1 into direct field
decoders. MQTT 5 always runs `PublishPacket.decode` then builds a `Message`.

Path tracing (CONNECTED engine): v3.1.1 hits `_decode_v311_qos0_message`;
MQTT 5 hits `PublishPacket.decode`.

| Path | v3.1.1 | MQTT 5 (no props) | MQTT 5 (props) |
| --- | ---: | ---: | ---: |
| Handler only (ns/msg) | 2 114 | 3 801 (**1.80×**) | 7 727 (**3.65×**) |
| Decoder + handler (ns/msg) | 3 572 | 5 325 (**1.49×**) | 9 249 (**2.59×**) |

### 4. One SQLite commit per single QoS 1 publish

`SqliteInflightStore.put_out` starts a write transaction when not already inside
`batch()`. `queue_publish_many` wraps the loop in `store.batch()` (one commit).
A lone `queue_publish` does not.

With a CONNECTED engine and a SQLite trace callback counting `COMMIT`:

| Workload | COMMITs |
| --- | ---: |
| 1 × `queue_publish(qos=1)` | 1 |
| 50 × `queue_publish(qos=1)` | 50 |
| 50 × via `queue_publish_many` | 1 |

`qos1_cycle_sqlite` hotpath self-time is dominated by `Connection.commit` /
`execute`. Durable QoS is expected to be slower than memory; the avoidable part
is paying a commit boundary per message on the common single-publish API.

### 5. `TopicMatcher` linear scan (Paho façade)

`dispatch/matcher.py`: `iter_match` splits the topic once, then walks every
registered filter. No exact-match index.

| Filters | ns / match (topic `sensors/7/temp`) |
| ---: | ---: |
| 10 | 3 834 |
| 100 | 26 166 |
| 1 000 | 193 780 |

Cost scales ~linearly. Painful for `message_callback_add` with many filters.

### 6. Delivery accounting re-encodes MQTT 5 properties

`api/_delivery.py` `logical_size()` does
`len(encode_properties(message.properties, PUBLISH))` whenever the MQTT 5
byte budget is active and properties are present.

| Case | ns / call |
| --- | ---: |
| With user properties + content type | 3 503 |
| `properties is None` | 88 |

That is a third encode of a bag the codec already structured on decode.

### 7. Contiguous frames discarded → reconnect re-encode

`_retain_publish_item` keeps only segmented `WriteItem`s. After a normal QoS 1
launch of a 1 KiB payload, `store.get_out(mid).encoded_publish is None`. A
1 MiB+1 payload retains the `(header, payload)` tuple.

`_retransmit` therefore rebuilds contiguous publishes with
`PublishPacket.encode_write_item` (and pays finding 1’s copy again). Session
resume with many mid-sized in-flight messages re-pays encode work that was
already done at launch.

### 8. Topic UTF-8 validated then encoded on every publish

`_validate_publish_request` → `validate_publish_topic` → `validate_utf8`, then
`encode_publish_item` → `encode_utf8`. For ASCII topics, validate is a scan and
encode is another. Both appear in the `native_publish_nowait_qos0` profile at
comparable self time.

| Step (ASCII topic) | ns / op |
| --- | ---: |
| `validate_publish_topic` only | 160 |
| `encode_utf8` only | 123 |

Small per call, but on the hottest QoS 0 path every nanosecond shares a budget
of tens of calls.

## Explicitly checked and not filed

- `PacketType.from_byte` already uses a 16-entry nibble table — residual method
  overhead only; not worth a dedicated issue.
- `_engine_lock` serialises publish against ingress batches by construction;
  this audit did not measure contention under bidirectional load, so no claim.
- Paho cross-thread QoS ≥ 1 handoff (`Future` per publish) is visible in
  `compat_publish_qos1` (158 calls/op, lock/`result`/`drain`) but is inherent to
  returning a MID synchronously from another thread — architectural cost, not a
  defect to “fix” without an API change.

## Reproduction

Artefacts were written under `/tmp/mqttium-perf-audit/` on the audit host
(`hotpath.json`, `probes2.json`, `probes_corrected.json`) and are not part of
this repository. Re-run:

```bash
PYTHONPATH=benchmarks:src python3 benchmarks/hotpath_profile.py \
  --output /tmp/mqttium-perf-audit/hotpath.json
# plus the independent probes exercised during this audit
```
