# Paho compatibility — choices, differences, and rejected features

This document defines the policy of `mqttium.compat.paho` and explains which
behaviors are intentionally **not supported**. The compatibility façade is an
**additive** layer: the core remains `AsyncClient` / `ProtocolEngine`.

## Goal

Provide the best practical compatibility with Paho
`CallbackAPIVersion.VERSION2` for migration purposes, without reproducing the
monolithic design, historical quirks, and technical debt of the original
client.

## Supported surface (VERSION2)

| API | Status | Notes |
| --- | --- | --- |
| `Client(CallbackAPIVersion.VERSION2, …)` | Supported | Only callback API version supported |
| `loop_start` / `loop_stop` | Supported | Dedicated thread and event loop |
| `connect` / `disconnect` / `reconnect` | Supported | Synchronous and blocking |
| `publish` → `MQTTMessageInfo` | Supported | `wait_for_publish` propagates errors |
| `subscribe` / `unsubscribe` | Supported | Simple `(rc, mid)` form |
| `on_connect(client, userdata, flags, reason_code, properties)` | Supported | |
| `on_disconnect(client, userdata, flags, reason_code, properties)` | Supported | Uses `DisconnectFlags` |
| `on_message` / `message_callback_add` | Supported | Topic is a **str**; specific callbacks take precedence as in Paho |
| `on_publish(…, mid, reason_code, properties)` | Partial | Simplified reason code and properties |
| `username_pw_set` / `will_set` / `user_data_set` | Supported | Configure before `connect` |
| `is_connected` | Supported | |

For one-shot helpers, prefer the async-native `mqttium.helpers` API instead of
`paho.mqtt.publish` or `paho.mqtt.subscribe`.

## Intentional differences

| Topic | Paho | mqttium | Rationale |
| --- | --- | --- | --- |
| Callback API VERSION1 | Supported | **Rejected** | Obsolete API with ambiguous MQTT v3/v5 signatures |
| `loop_forever` / `loop(timeout)` | Supported | Not supported | `loop_start` is sufficient; avoid exposing a second event-loop model |
| `connect_async` | Supported | Not supported | Use `AsyncClient` for native asynchronous operation |
| Non-compliant QoS > 0 republish with a clean session | Historically ambiguous | **Strict MQTT behavior** | Correctness takes precedence over bug compatibility |
| MID for QoS 0 | Allocated | `None` | QoS 0 has no protocol packet identifier |
| Blocking calls from a network callback | Often tolerated | **Rejected** with `RuntimeError` | They would deadlock the single-writer architecture |
| Off-network-thread `publish()` | Internal queue and immediate return | QoS 0 uses a coalesced façade queue consumed by the loop; QoS 1/2 use a short MID-allocation handoff | No publish waits for writer progress, and the protocol engine remains owned by the network thread |
| WebSocket / proxy / SOCKS support | Broad surface | WebSocket through `AsyncClient.connect_ws`; not through the sync façade | Keep transport concerns separate from compatibility concerns |
| Paho file persistence formats | Supported | `SqliteInflightStore` through `AsyncClient` | Do not reproduce Paho-specific binary persistence formats |
| `suppress_exceptions` | Supported | Not supported | Errors must remain observable |
| Public `max_inflight_messages` | Coupled to MID handling | `local_receive_maximum` plus `FlowControl` | Receive Maximum is not the packet-identifier space |

## Explicitly rejected designs

### 1. Reproducing the monolithic `client.py` implementation

**Rejected.** It would mix I/O, protocol state, callbacks, and compatibility
behavior in one large component. MQTTium deliberately separates the
synchronous protocol engine from asynchronous adapters.

### 2. Treating Receive Maximum as a packet-identifier limit

**Rejected.** Packet identifiers span 1 through 65535, while the active
outbound window is managed by `FlowControl`. Conflating the two breaks QoS 1/2
pipelining.

### 3. Adding an artificial timer to fill write batches

**Rejected.** Batching must follow writer readiness and acknowledgment flow,
not an arbitrary delay. The writer already coalesces data that is ready to be
sent.

### 4. Allowing synchronous callbacks to re-enter blocking client methods

**Rejected for this architecture.** The single-writer model combined with
`run_coroutine_threadsafe().result()` would deadlock. Schedule the operation on
another thread or use `AsyncClient`.

### 5. Magical QoS > 0 retransmission with `clean_session=True`

**Rejected.** Durable sessions require `clean_start=False` for MQTT v3 or a
positive `session_expiry_interval` for MQTT v5. Reconnect then uses Clean Start
0.

### 6. Hidden topic caches or automatic topic aliases

**Rejected.** Topic aliases must remain explicit and observable.

### 7. Releasing the local Receive Maximum slot at PUBREC under load

**Rejected for now.** MQTT 5 permits early release, but doing so caused
intermittent stalls under load through an accumulation of `WAIT_PUBCOMP`
messages and writer-queue pressure. The local window therefore remains held
until PUBCOMP. Reconsider this only with supporting measurements.

## Recommended migration path

1. New code → `mqttium.api.AsyncClient`
2. Legacy synchronous VERSION2 code → `mqttium.compat.paho.Client`
3. One-shot operations → `mqttium.helpers.publish` / `subscribe`
4. See also `docs/MIGRATION.md`

## Regression tests

- `tests/unit/test_compat_paho.py` — connection, publishing, callbacks, and filters
- `tests/unit/test_compat_lib_subset.py` — behavioral compatibility subset
- `tests/unit/test_compat_publish_perf.py` — effect ordering, MID reuse, and QoS 0 coalescing
- `tests/integration/test_compat_publish_perf.py` — end-to-end QoS 0 callbacks and delivery
