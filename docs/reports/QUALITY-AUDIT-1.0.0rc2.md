# Deep quality audit at `1.0.0rc2`

| | |
| --- | --- |
| Date | 2026-08-13 |
| Commit audited | `c34a949` (Fix Bugbot RC2 lifecycle findings, tag `v1.0.0rc2`) |
| Scope | Full source tree under `src/`, contract docs, test coverage |
| Method | Direct read of `protocol/` and `api/`; delegated verified audits of `codec/`+`packets/`, `transport/`+`persistence/`, `dispatch/`+`compat/`+`helpers/`; every finding below was reproduced by execution or traced in code |

This is a dated report. It records what was true of commit `c34a949` on
2026-08-13. It is not a contract; do not edit it to match later code — write a
new report instead (see [`README.md`](README.md)).

## Baseline

- 890 unit tests pass; 15 integration tests pass against a live Mosquitto on `127.0.0.1:11883` (no skips).
- `ruff format --check`, `ruff check`, `mypy`, `bandit` all clean.
- Fuzz smoke: `tests/fuzz/fuzz.py --seed 1 --iterations 3000` clean on codec/engine/websocket.
- Coverage: **89 %** overall; `api/async_client.py` at **82 %** (191 missed lines, including the reconnect retry path).

## Verdict (summary)

**Not ready for `1.0.0`.** One CRITICAL defect (reconnect gives up after a
single failed attempt), one spec-inversion (U+FEFF rejected), and two Paho
threading holes that contradict documented COMPAT.md promises. The remainder is
medium/low polish. Estimated fix time with tests: ~2–3 focused days, then the
planned post-RC campaign must be run against tests that can actually exercise a
second reconnect attempt (today they cannot).

## Findings index

| ID | Severity | Location | One-line |
| --- | --- | --- | --- |
| C1 | CRITICAL | `api/async_client.py:822,1435` | Reconnect loop exits after one failed attempt; receipts hang |
| H1 | HIGH | `codec/primitives.py:140-141` | U+FEFF rejected — inverts the cited spec statement |
| H2 | HIGH | `compat/paho.py:270-280` | `loop_start()` race spawns multiple leaked event loops |
| H3 | HIGH | `compat/paho.py:57-70` | `wait_for_publish()` deadlocks the network loop from callbacks |
| H4 | HIGH | `packets/publish.py:124-135`, `inbound.py:777-778` | Empty inbound topic accepted under MQTT 3.1.1 |
| M1 | MEDIUM | `compat/paho.py:688-698` | QoS 0 publish reports success for never-sent messages (COMPAT.md:70) |
| M2 | MEDIUM | `compat/paho.py:266-268` | `is_connected` is a property; Paho's is a method (COMPAT.md:47) |
| M3 | MEDIUM | `compat/paho.py:771` | `on_connect` passes v1 dict, not v2 `ConnectFlags` |
| M4 | MEDIUM | `compat/paho.py:98-99` | `publish(payload=None)` raises `TypeError` (retained-clear idiom) |
| M5 | MEDIUM | `protocol/engine.py:859` | Double full PUBLISH decode on hot ingress path |
| M6 | MEDIUM | `transport/websocket.py:171-177` | Cancelled `close()` leaks the TCP connection |
| M7 | MEDIUM | `persistence/sqlite.py:194-198` | No `busy_timeout`; multi-writer file fails instantly |
| M8 | MEDIUM | `packets/publish.py:61` via `codec/vbi.py:34` | Oversized payload leaks bare `ValueError`, not `MQTTError` |
| L1 | LOW | `protocol/flow_control.py:46-54` | Dead parameter `local_max` |
| L2 | LOW | `api/async_client.py:1440-1445` | Reason code recovered by string parsing |
| L3 | LOW | `packets/connect.py:77-78` | `will_payload`/`will_properties` silently dropped without will topic |
| L4 | LOW | `transport/websocket.py:297-298` | IPv6 literal yields malformed `Host:` header |
| L5 | LOW | `transport/websocket.py:318-324` | Handshake timeout is per-chunk, not total |
| L6 | LOW | `persistence/sqlite.py:187-199,216-217,83-126,752-760` | Constructor leaks, missing table check, magic-key JSON fragility, duplicated paging |
| L7 | LOW | `api/_delivery.py:435` | Vestigial `async` on `reset_stream` |
| L8 | LOW | `protocol/outbound.py:858-860` | Redundant re-check of `properties.values` |
| L9 | LOW | `codec/vbi.py:78-79` | Dead branch after 4-byte cap |
| L10 | LOW | `packets/acks.py:58-64` | Same v5 PUBACK decodes to `properties=None` or `Properties()` |
| L11 | LOW | `docs/API-STABILITY.md:3,157` | Doc drift: says `1.0.0rc1`, code is `1.0.0rc2` |
| L12 | LOW | `compat/paho.py:417-422,290-292,256-257`; `matcher.py:20-24`; `helpers/subscribe.py:50-51` | Facade edge cases: silent rc=1, stop-from-loop, unvalidated callback filters, `$share` callbacks never fire, `msg_count=0` |

---

## CRITICAL

### C1 — Reconnect loop abandons after one failed attempt

- **Location:** `api/async_client.py:822` (`self._intentional_disconnect = True` inside `_connect_once_locked`'s `except BaseException`) interacting with the loop condition at `:1435` (`while self._reconnect.enabled and not self._intentional_disconnect`).
- **What happens:** Any failure of a reconnect attempt (transport timeout, connection refused, CONNACK timeout) sets `_intentional_disconnect = True`. The reconnect loop then fails its `while` condition on the next iteration and exits with `_reconnect_task = None`. Nothing restarts it.
- **Evidence:** Reproduced by driving `_reconnect_loop` against an unreachable host with `ReconnectPolicy(enabled=True, max_retries=None)`: loop exited after exactly one attempt (`policy.attempt == 1`).
- **Impact:**
  - Broker unreachable longer than one `connect_timeout` → **no further reconnects ever**, even after the broker returns. This defeats the library's headline feature.
  - QoS 1/2 receipts in flight at the drop are never settled: `_fail_pending` is skipped by the read-loop teardown (`_will_reconnect()` was true) and never called by the abandoning loop → `receipt.wait()` hangs indefinitely.
  - Violates the documented contract (IMPLEMENTATION-GUIDE §Reconnect: *"Backoff is bounded and jittered… Temporary broker-unavailable errors and network failures may retry"*).
- **Present since:** the initial commit (`5355c32`); survived RC1 and RC2.
- **Test gap:** coverage shows lines `1446-1471` (the `should_retry`/`continue` retry path) **never executed** by unit tests.
- **Fix direction:**
  1. In `_reconnect_loop`, under `_lifecycle_lock`, after the `if self._intentional_disconnect: return` guard, reset `self._intentional_disconnect = False` before each `_connect_once_locked` call.
  2. Alternatively, stop `_connect_once_locked` from setting `_intentional_disconnect` when invoked from the reconnect path (thread a parameter).
- **Verification to add:**
  - Unit: reconnect policy with 3 attempts where the first 2 fail then succeed; assert `attempt == 3` and the loop is still running/returns on success.
  - Unit: after the loop gives up, assert pending receipts are failed (or the waiters are woken) — decide and lock the semantics.
  - Integration: broker down for `2 × connect_timeout`, then up; assert the client reconnects on attempt ≥ 2.

---

## HIGH

### H1 — U+FEFF rejected: inverts the cited spec statement

- **Location:** `codec/primitives.py:140-141`.
- **What happens:** `_validate_mqtt_utf8` raises `MalformedPacketError`/`ProtocolError` for any string containing U+FEFF, citing `[MQTT-1.5.4-3]`.
- **Why it is a bug:** The actual statement (verified against `docs/spec/mqtt-v5.0-statements.json` and v3.1.1's `[MQTT-1.5.3-3]`) reads: *"A UTF-8 encoded sequence 0xEF 0xBB 0xBF is always interpreted as U+FEFF … and MUST NOT be skipped over or stripped off by a packet receiver"* — i.e. U+FEFF is a legal character and receivers must not drop it. Nothing in MQTT forbids it. The spec's hard prohibition is only U+0000 (`[MQTT-1.5.4-2]`); U+FFFE/U+FFFF are SHOULD-level.
- **Evidence:** Hand-built legal `user_property`/PUBLISH topic containing `EF BB BF` raises `MalformedPacketError` on decode → connection teardown on spec-legal broker traffic (plausible with BOM-prefixed international text). Encode raises `ProtocolError`, so the library cannot even *produce* such legal strings.
- **Also:** the string literal at line 140 contains a literal invisible U+FEFF character in the source.
- **Fix direction:** remove the U+FEFF branch; keep the U+0000 check; add a decode+encode round-trip test with U+FEFF in topic/property. Keep rejecting ill-formed UTF-8 and U+0000.

### H2 — Paho `loop_start()` race spawns leaked event loops

- **Location:** `compat/paho.py:270-280`.
- **What happens:** The `if self._thread is not None and self._thread.is_alive(): return` guard and the `self._thread = threading.Thread(...)` assignment are not under a lock. Two producer threads racing a first `publish()`/`connect()` both create a thread; each `_run_loop` builds its own loop and both write `self._loop`/`self._thread` (last-writer-wins). The losing loops are never joinable — `loop_stop()` only stops `self._loop`.
- **Evidence:** Live reproduction: 5 trials of a 2-thread racing start left 2–4 `mqttium-paho-loop` threads alive; the async client was driven from more than one loop, breaking the single-loop ownership invariant the facade exists to protect.
- **Fix direction:** guard `loop_start()` with a lock (or `threading.Lock` + double-check), or move thread creation under an existing serialization point.

### H3 — Paho `wait_for_publish()` deadlocks the network loop from callbacks

- **Location:** `compat/paho.py:57-70`.
- **What happens:** `MQTTMessageInfo.wait_for_publish` calls `asyncio.run_coroutine_threadsafe(...).result(timeout)` without the `_on_network_thread()` guard that `_submit` enforces. Called from inside `on_message`/`on_publish` (where `publish()` is explicitly supported via the fast path), it parks the loop thread waiting on itself; with `timeout=None` the network thread deadlocks permanently (no reader, writer, or keepalive ever runs again).
- **Evidence:** Reproduced: from the loop thread it blocked for the full timeout instead of raising the guard's `RuntimeError`.
- **Fix direction:** add the same `_on_network_thread()` check as `_submit` (raise `RuntimeError`), and document that `wait_for_publish` is not callable from the network thread.

### H4 — Empty inbound topic accepted under MQTT 3.1.1

- **Location:** `packets/publish.py:124-135` (topic validated only in the v5 branch); `protocol/inbound.py:777-778` (`_resolve_topic_fields` returns `''` unchanged for non-v5); `topics.py:52-53` (`validate_received_publish_topic` no-ops on empty).
- **What happens:** a v3.1.1 PUBLISH with a zero-length Topic Name decodes successfully and an empty-topic `Message` is delivered to the application instead of a protocol error.
- **Evidence:** `PublishPacket.decode(0, b"\x00\x00payload", MQTTv311)` yields `topic=''`; `inbound.py:334` only catches wildcard topics, not the empty case.
- **Impact:** Spec violations `[MQTT-4.7.3-1]` / `[MQTT-3.3.2-1]` ("Topic Name MUST be at least one character long"); a hostile/buggy broker delivers an empty-topic message.
- **Fix direction:** reject empty topic in the v3.1.1 decode path (mirror the v5 branch). Add a hostile-broker test with an empty-topic v3.1.1 PUBLISH.

---

## MEDIUM

### M1 — Paho QoS 0 publish reports success for never-sent messages

- **Location:** `compat/paho.py:688-698` + `api/models.py:211-212`.
- **What happens:** the facade fabricates its own `PublishReceipt(qos=0)`, which `is_done()` reports `True` immediately, before the request is admitted cross-thread, and nobody ever settles it. Loop-side failure (invalid topic → `ProtocolError`; native backpressure → `FlowControlError`) is routed only to `on_publish`, and nowhere if `on_publish` is `None`.
- **Evidence:** `publish("bad/+topic", b"x", qos=0)` returns `rc=0`, `is_published()==True`, `wait_for_publish()` succeeds — message silently dropped. Directly contradicts `docs/COMPAT.md:70` (*"wait_for_publish()/is_published() after a failed publish → Raise"*).
- **Fix direction:** either settle the QoS 0 receipt from the loop-side result, or propagate the error to `wait_for_publish`/`is_published` as COMPAT.md promises.

### M2 — Paho `is_connected` shape mismatch

- **Location:** `compat/paho.py:266-268`.
- **What happens:** `is_connected` is a `@property`; Paho's `client.is_connected()` is a method. Migrated code calling `client.is_connected()` gets `TypeError: 'bool' object is not callable`. COMPAT.md:47 lists it "Supported" with no caveat.
- **Fix direction:** make `is_connected` a method (breaking the property, matching Paho), or add a `is_connected()` method and keep a private property; update COMPAT.md accordingly.

### M3 — Paho `on_connect` receives the v1 flags dict, not v2 `ConnectFlags`

- **Location:** `compat/paho.py:771`.
- **What happens:** Paho VERSION2 passes a `ConnectFlags` dataclass (`flags.session_present`); the facade passes the v1 dict `{"session present": bool}`. A genuine v2 callback raises `AttributeError`, swallowed by `_safe_callback` into the loop exception handler.
- **Evidence:** verified. Related: `reason_code` is a raw `int`, not a paho `ReasonCode` (harmless for `== 0` checks but not byte-for-byte compatible).
- **Fix direction:** pass a `ConnectFlags`-shaped object with `.session_present`; update the COMPAT.md matrix row.

### M4 — Paho `publish(payload=None)` raises `TypeError`

- **Location:** `compat/paho.py:98-99` (also 532, 565).
- **What happens:** `len(payload)` on `None`. Paho's `payload` defaults to `None` (zero-length), and `publish(topic, None, retain=True)` is the canonical retained-message-clearing idiom.
- **Evidence:** `TypeError: object of type 'NoneType' has no len()` on the off-loop path.
- **Fix direction:** treat `payload=None` as `b""`.

### M5 — Double full PUBLISH decode on the hot ingress path

- **Location:** `protocol/engine.py:859` (`_check_pending_auto_qos1_receive_maximum` decodes the whole packet) then `inbound.on_publish` decodes it again.
- **What happens:** in auto-ack mode, while `_pending_auto_qos1_mids` is non-empty (i.e. during any pipelined batch of QoS 1/2 PUBLISHes between effect handoffs), every inbound QoS>0 PUBLISH after the first is fully decoded twice — including a second payload slice. This is the hot path for a busy subscriber.
- **Fix direction:** the check only needs the MID: parse `u16 topic-length`, skip, read `u16 mid` — do not build the full `PublishPacket`. Re-measure with a paired benchmark.

### M6 — Cancelled WebSocket `close()` leaks the TCP connection

- **Location:** `transport/websocket.py:171-177`.
- **What happens:** `close()` awaits `self._writer.drain()` *before* calling `_close_stream_writer`; a `CancelledError` delivered at that drain (congested peer, teardown under cancellation) skips `writer.close()`. `StreamTransport.close()` (`_stream.py:55-58`) does it in the safe order.
- **Evidence:** after a cancelled `close()`, `writer.is_closing()` is `False` and the socket stays open until GC.
- **Fix direction:** close the writer before/regardless of the close-frame drain (match `_stream.py` ordering).

### M7 — SQLite store has no `busy_timeout`

- **Location:** `persistence/sqlite.py:194-198`.
- **What happens:** any external writer (a second client instance on the same persistence file, or a CLI tool) makes store calls fail instantly with `sqlite3.OperationalError: database is locked` on the event loop → tears down the MQTT connection.
- **Evidence:** with a second connection holding `BEGIN IMMEDIATE`, `put_out` raises immediately (zero wait).
- **Fix direction:** set `PRAGMA busy_timeout` to a few hundred ms at connection setup.

### M8 — Oversized outbound payload leaks a bare `ValueError`

- **Location:** `packets/publish.py:61` via `codec/vbi.py:34-35`.
- **What happens:** an outbound payload > 268 435 455 bytes raises `ValueError: VBI out of range`. `_check_outbound_size` (`engine.py:821`) runs only *after* encoding, and the API layer has no payload-size pre-check — against the project convention that errors derive from `MQTTError` (this is the one escape found across all fuzz/exception-hygiene checks).
- **Fix direction:** pre-check payload size before encoding and raise a typed `MQTTError` (payload > broker's negotiated limit already raises `PacketTooLargeError`; add the hard wire-limit case).

---

## LOW

- **L1** `protocol/flow_control.py:46-54` — `apply_broker_receive_maximum` takes `local_max` but never uses it (three call sites pass `local_receive_maximum`). Dead parameter; drop it or use it.
- **L2** `api/async_client.py:1440-1445` — CONNACK reason code recovered by `"reason_code=" in str(exc)` string parsing. Fail-open (retries) if the message format changes; carry the reason structurally instead.
- **L3** `packets/connect.py:77-78` — `will_payload`/`will_properties` silently discarded when `will_topic is None`. Also `[MQTT-3.1.3-7]` empty-client-id-with-CleanSession-0 is only enforced at the engine level, not at the packet level (Provisional API; acceptable, note it).
- **L4** `transport/websocket.py:297-298` — `Host: {host}:{port}` with an IPv6 literal yields `Host: ::1:8080` instead of `Host: [::1]:8080`; strict servers 400 the upgrade. (v3.1.1 password-without-username was separately checked and is already rejected by `engine.py:261-272`; not a finding at engine level.)
- **L5** `transport/websocket.py:318-324` — handshake timeout restarts per chunk; a dribbling peer keeps the handshake alive. Mitigated for `AsyncClient` by the outer `wait_for`; exposed when the transport is used standalone.
- **L6** `persistence/sqlite.py` — constructor failure paths leak the sqlite connection (`:187-199`); `user_version`-match fast path does not verify tables exist (`:216-217`); JSON magic-key revival (`__mqttium_bytes__`/`__mqttium_tuple__`) can corrupt/refuse records if a value's only key is a magic key (`:83-126`, defense-in-depth only — not reachable from the wire); `_in_messages_for_mids` re-implements `_pages` chunking (`:752-760`).
- **L7** `api/_delivery.py:435` — `reset_stream` is `async` but contains no `await`; vestigial.
- **L8** `protocol/outbound.py:858-860` — `logical_property_bytes` re-checks `properties is not None and properties.values` after the branch already returned; redundant.
- **L9** `codec/vbi.py:78-79` — `value > _MAX_VBI` branch is unreachable once the 4-byte cap rejects longer encodings.
- **L10** `packets/acks.py:58-64` — semantically identical v5 PUBACKs decode to `properties=None` (2-byte body) vs `Properties()` (3-byte body); consumers must handle both shapes.
- **L11** `docs/API-STABILITY.md:3,157` — "MQTTium is at `1.0.0rc1`"; code is `1.0.0rc2`. Update in the same change as any API doc edit. ROADMAP similarly still says "promote from rc1".
- **L12** `compat/paho.py:417-422` (`disconnect()` from the network thread silently returns `rc=1` and emits a coroutine-never-awaited warning), `:290-292` (`loop_stop()` from the loop thread raises `RuntimeError: cannot join current thread`), `:256-257` (`message_callback_add` accepts invalid filters like `sport/#/ranking` that silently never match); `dispatch/matcher.py:20-24` (`$share/{group}/{filter}` per-filter callbacks never fire for delivered shared messages — intentional, Paho-parity, but undocumented in COMPAT.md); `helpers/subscribe.py:50-51` (`simple()` with `msg_count=0` silently behaves like 1).

---

## Verified clean (do not re-open without evidence)

- **Decoder exception hygiene:** every buffer read is bounds-checked; a 20k-case hostile/truncated fuzz across all decoders × both protocol versions raised only `MQTTError` subclasses. `IncrementalDecoder` handles trickled bytes, 5-byte VBIs, non-canonical remaining lengths, and rejects oversized packets before the body arrives.
- **VBI:** >4-byte and non-minimal encodings rejected; canonical-length checks hold.
- **UTF-8 (except U+FEFF):** U+0000 and ill-formed sequences rejected; surrogate non-representability is structurally sound; FFFE/FFFF as SHOULD-NOT is compliant.
- **Properties:** duplicates of non-repeatable properties, unknown IDs, wrong-packet-type, `nonzero`/`zero_one` bounds, response-topic wildcards/empty, property-length straddles all rejected.
- **Matcher:** `#`-must-be-last, `#` matches zero levels, `+` whole-level, `$`-topic wildcard guard; exact filters O(1); mutation-safe iteration.
- **PacketIdPool:** allocation/reserve/release/clear invariants hold; no leaks found.
- **Outbound admission:** single acquisition point, shared `_rollback`, QoS 1/2 transaction fault-injection covered by `tests/unit/test_outbound_transaction.py`.
- **Persistence parity:** differential fuzzing shows memory and sqlite stores agree on ordering, deletion, replay-page boundaries, and conditional transitions; transactions atomic and migration idempotent; no SQL-injection surface.
- **TLS defaults:** `create_default_context()` + hostname verification; `wss://` + `ssl=False` refused.
- **Exports:** root and `mqttium.api` `__all__` match the Stable names in `docs/API-STABILITY.md`; no Internal name re-exported.
- **errors.py:** everything derives from `MQTTError`; no builtin shadowing.

---

## Action plan for the next RC (recommended order)

1. **C1** — fix + unit/integration tests that exercise a *second* failed attempt (the current test suite provably cannot). Highest priority: it is the library's core promise.
2. **H1** — remove the U+FEFF rejection; add a round-trip test.
3. **H2 + H3** — Paho threading guards; then decide M1–M4: fix them or downgrade the COMPAT.md matrix honestly. Do not ship `is_connected` "Supported" as a property.
4. **H4, M8** — spec/API conformance, cheap.
5. **M6, M7** — operational robustness (WebSocket close order, sqlite `busy_timeout`).
6. **M5** — hot-path double decode; benchmark before/after.
7. **L2, L11** — structural reason code, doc drift.
8. Close remaining LOWs opportunistically.
9. Run the planned post-RC campaign (multi-hour fuzz + soak, Python 3.11–3.14 matrix, EMQX/HiveMQ interop) with the new reconnect tests in place, then re-assess the `1.0.0` promotion.
