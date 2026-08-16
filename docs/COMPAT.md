# Paho compatibility — choices, differences, and rejected features

This document defines the policy of `mqttium.compat.paho` and explains which
behaviors are intentionally **not supported**. The compatibility façade is an
**additive** layer: the core remains `AsyncClient` / `ProtocolEngine`.

Use this facade when an existing application depends on Paho VERSION2 callback
shape, synchronous lifecycle calls or producers outside the network thread. New
async code should use `mqttium.api.AsyncClient` directly.

## Goal

Provide the best practical compatibility with Paho
`CallbackAPIVersion.VERSION2` for migration purposes, without reproducing the
monolithic design, historical quirks, and technical debt of the original
client.

## Adoption path

1. Replace the client import and construct
   `Client(CallbackAPIVersion.VERSION2, ...)`.
2. Keep existing VERSION2 callbacks, `loop_start()`, publish/subscribe calls and
   producer threads.
3. Configure bounded request, message and byte limits and handle
   `MQTT_ERR_QUEUE_SIZE`.
4. Move individual service boundaries to `AsyncClient`; remove the facade after
   the final synchronous caller is gone.

The runnable [`examples/paho_compat.py`](../examples/paho_compat.py) demonstrates
the complete connect, subscribe, publish, callback and shutdown lifecycle. See
[`MIGRATION.md`](MIGRATION.md) for native equivalents.

## Supported surface (VERSION2)

| API | Status | Notes |
| --- | --- | --- |
| `Client(CallbackAPIVersion.VERSION2, …)` | Supported | Only callback API version supported |
| `loop_start` / `loop_stop` | Supported | Dedicated thread and event loop |
| `connect` / `disconnect` / `reconnect` | Supported | Synchronous and blocking |
| `publish` → `MQTTMessageInfo` | Supported | `wait_for_publish` propagates errors |
| `subscribe` / `unsubscribe` | Supported | Simple `(rc, mid)` form |
| `on_connect(client, userdata, flags, reason_code, properties)` | Partial | Uses `ConnectFlags.session_present`; reason code remains an integer rather than Paho's `ReasonCode` object |
| `on_disconnect(client, userdata, flags, reason_code, properties)` | Supported | Uses `DisconnectFlags` |
| `on_message` / `message_callback_add` | Supported | Topic is a **str**; specific callbacks take precedence as in Paho; callback filters are validated |
| `on_publish(…, mid, reason_code, properties)` | Partial | Simplified reason code and properties |
| `username_pw_set` / `will_set` / `user_data_set` | Supported | Configure before `connect` |
| `is_connected()` | Supported | Method, matching Paho |
| `max_queued_messages_set(n)` | Supported | Bounds unfinished QoS 1/2 publications; `0` means unlimited, as in Paho |
| `max_queued_bytes_set(n)` | **Additive** | No Paho equivalent; bounds unfinished native publications by logical bytes |
| `max_pending_publish_requests` / `max_pending_publish_bytes` | **Additive constructor limits** | Hard bounds for requests retained in the cross-thread handoff before loop-side admission |
| `max_outbound_inflight` | **Additive constructor limit** | Caps unfinished QoS 1/2 publications below the broker's Receive Maximum; attach-time only, so it cannot be changed after construction |

Shared-subscription callback filters are matched literally, as in Paho: a callback registered for `$share/group/filter` does not match a delivered message whose Topic Name is `filter`.

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
| `MQTTMessageInfo.mid` for QoS 1/2 | The wire packet identifier | A façade correlation identifier, wrapping over `1..65535` and reserved through completion delivery | `publish()` returns before loop-side admission, so no packet identifier exists yet. Packet identifiers stay loop-owned (§8); the façade binds its own value to the real one and translates back when `on_publish` is dispatched. If no callback is due, the MID retires when the native receipt settles; if a dispatcher is already due, it stays reserved until that dispatch finishes. A wrap collision with an active façade MID is refused with `MQTT_ERR_QUEUE_SIZE` instead of aliasing two live/completing publications |
| Blocking calls from the network thread | Often tolerated from callbacks | **Rejected** with `RuntimeError` | Waiting on the same loop would deadlock the single-writer architecture |
| Off-network-thread `publish()` | Internal queue and immediate return | QoS 0/1/2 share a coalesced façade queue and **all return immediately** | No publish waits for the network loop or for writer progress, ordering is preserved across QoS levels, and the protocol engine remains loop-owned |
| WebSocket / proxy / SOCKS support | Broad surface | WebSocket through `AsyncClient.connect_ws`; not through the sync façade | Keep transport concerns separate from compatibility concerns |
| Paho file persistence formats | Supported | `SqliteInflightStore` through `AsyncClient` | Do not reproduce Paho-specific binary persistence formats |
| `suppress_exceptions` | Supported | Not supported | Errors must remain observable |
| Queue saturation | Silently drops or grows without bound depending on `max_queued_messages` | `MQTT_ERR_QUEUE_SIZE` (15): synchronously with `mid=None` when the cross-thread handoff is full, or later as `rc` on the returned handle when loop-side admission refuses | Both the cross-thread handoff and native unfinished-publication queue are bounded; a non-blocking API must refuse rather than stall the caller's thread. `wait_for_publish()` / `is_published()` re-check `rc` after admission, so a late refusal can never report success |
| `wait_for_publish()` / `is_published()` after a failed publish | Report success | **Raise** | Reporting a publication that never happened is worse than an exception |
| Public `max_inflight_messages` | Coupled to MID handling | `local_receive_maximum` plus `FlowControl` | Receive Maximum is not the packet-identifier space |
| `on_publish` assignment | Plain attribute | Property that installs and clears the inner `AsyncClient.on_publish` | The native client routes every completion through its callback queue while that callback exists, so installing a dispatcher unconditionally charged a queue hop per message to façade users who never set `on_publish` |

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

### 8. Mutating the protocol engine directly from publisher threads

**Rejected.** A direct cross-thread prototype can produce a much higher raw
submission rate because it skips the loop handoff entirely, but it breaks the
central ownership invariant of the client. Engine, store, packet identifiers,
receipts, connection epochs, and effect ordering are committed together on the
network loop. Protecting only `queue_publish()` with an extra mutex would leave
other engine paths and lifecycle transitions outside the same critical section.

The compatibility façade instead uses one thread-safe ingress queue. Its retained
requests are hard-bounded independently by `max_pending_publish_requests` and
`max_pending_publish_bytes` (10,000 requests and 64 MiB by default). Admission
reserves both limits before retaining the payload; saturation returns
`MQTT_ERR_QUEUE_SIZE` without scheduling or mutating native protocol state. The
reservation is released after loop-side admission, cancellation, scheduling
failure, or shutdown.

Each loop callback still drains at most 256 requests and targets at most 1 MiB
of logical topic plus payload bytes. A single request larger than that *drain*
target is processed alone. This batching target limits work per event-loop turn;
the separate handoff byte limit is the actual retained-memory bound and must be
configured high enough for the largest publication accepted by the façade.

No caller waits for the loop. Packet identifiers remain loop-owned, so the
façade cannot hand back a wire identifier at `publish()` time; it mints a
correlation identifier from its own wrapping `1..65535` namespace instead. The
calling thread reserves that façade MID until completion no longer has a publish
dispatcher to deliver. If the wrapping generator lands on a façade MID that is
still active, the new publication is refused rather than sharing an identifier.
Once the network loop commits the request, the façade binds its MID to the real
packet identifier and translates it back when `on_publish` is dispatched.

Correlation state follows the completion-delivery lifetime, not the
instantaneous callback setting. This matters because `on_publish` may be added
or removed while a QoS 1/2 publication is still in flight, and a dispatcher may
already be queued when the property changes. A private one-shot receipt-settlement
hook retires the façade reservation immediately when no dispatcher can still be
delivered; otherwise the queued dispatcher owns the reservation and releases it
after the publish callback path finishes. Thus an application that never waits
on the handle cannot leak an active MID, while a slow/already-queued callback
cannot race a wrapped MID reuse. The inner `AsyncClient.on_publish` remains
installed only while the user callback is configured, preserving the native
no-callback fast path.

That is what makes the coalesced queue pay for a single producer. While QoS 1/2
`publish()` blocked until loop-side admission, one producer enqueued a request,
scheduled a drain, then blocked, so nothing could join the batch: the measured
mean drain batch size was exactly 1.00 with one worker, and the coalesced path
ran at 0.69x the plain one-callback-per-message handoff. Returning immediately
raised the same measurement to a mean batch of 224.88 (capped by the 256-request
drain target) and 2.67x that handoff.

The consequences are Paho's own. A producer can outrun the loop, so
`MQTT_ERR_QUEUE_SIZE` is reachable for QoS 1/2 under sustained overload and a
producer must shed rather than rely on the façade to throttle it. A refusal that
happens after `publish()` returned is reported as `rc` on the handle, and
`wait_for_publish()` / `is_published()` re-check `rc` once admission has settled
so a refused publication can never report success. Pending requests are still
failed when the compatibility loop stops.

The A/B benchmark in `benchmarks/compat_qosn_submit_ab.py` compares the old
per-message coroutine handoff, a safe one-callback-per-message alternative, and
the coalesced queue, and reports the drain batch size that explains the
difference. It exercises a live compatibility loop with a CONNECTED engine and
no broker I/O, so it measures the handoff, not end-to-end throughput.

QoS 0 is committed on the loop through the same writer-direct path the native
client uses, falling back to the engine effect path whenever that path declines
— including while effects from a QoS 1/2 commit earlier in the same batch are
still pending, which is what preserves ordering across QoS levels.

## Entry-point summary

1. New code → `mqttium.api.AsyncClient`
2. Legacy synchronous VERSION2 code → `mqttium.compat.paho.Client`
3. One-shot operations → `mqttium.helpers.publish` / `subscribe`
4. See also `docs/MIGRATION.md`

## Regression tests

- `tests/unit/test_compat_paho.py` — connection, publishing, callbacks, and filters
- `tests/unit/test_compat_lib_subset.py` — behavioral compatibility subset
- `tests/unit/test_compat_publish_perf.py` — effect ordering, MID reuse, mixed-QoS coalescing, cancellation, loop shutdown, and concurrent QoS 1 admission
- `tests/unit/test_compat_publish_edges.py` — oversized drain handling and complete QoS 2 publish handshake
- `tests/unit/test_compat_facade_mid_lifecycle.py` — façade MID wrap collisions, settlement/callback release, and callback-toggle correlation
- `tests/integration/test_compat_publish_live.py` — end-to-end QoS 0 and concurrent QoS 1 callbacks and delivery
- `benchmarks/compat_qosn_submit_ab.py` — coroutine, callback, and coalesced QoS 1 submit-rate/latency comparison
