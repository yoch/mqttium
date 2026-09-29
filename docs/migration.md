# Migrating to 1.0.0

## Changes in 1.1

- `ConnectionState.DISCONNECTED` now means that no automatic reconnection is
  pending. While the reconnect policy will retry, `state` and
  `stats().state` read the new `ConnectionState.RECONNECTING`. Code comparing
  `state` against `DISCONNECTED` to detect a lost connection must also accept
  `RECONNECTING`; code that exhaustively matches `ConnectionState` gains a case.
- A refused CONNACK raises `ConnectRefusedError`, a `ProtocolError` subclass
  with `reason_code` and `properties`; `except ProtocolError` still catches it.
- Reconnection now retries causes it used to treat as permanent: broker protocol
  violations other than malformed packets, local limit breaches, a slow
  iterator consumer (`MessageDeliveryError`) and TLS certificate verification
  failures. MQTT 5 *Session taken over* (`0x8E`) is terminal instead of retried.
  `ReconnectPolicy(retry_refused=True)` also retries terminal CONNACK refusals.
- With a `ReconnectPolicy`, the first `connect()` retries transient failures
  instead of failing on the first one; remove application retry loops around
  it, or keep them for the no-policy case. A failed `connect()` no longer ends
  the `messages()` stream.
- QoS 1/2 `publish()` and `publish_nowait()` raise `NotConnectedError` once the
  client is stopped (after `disconnect()` or a loss with no reconnection
  pending) instead of queueing a publication that could never be sent. The
  offline queue before the first `connect()` is unchanged.
- Assigning `on_message` or calling `message_callback_add()` with iterator
  delivery raises `ValueError`; it used to be silently ignored.
- `AsyncClient` supports `async with`, which calls `disconnect()` on exit.
- `connect()` raises `ConnectError` for DNS, TCP, TLS and WebSocket upgrade
  failures. It is still an `OSError`, so existing `except OSError` handlers
  keep working; code catching a specific subclass such as
  `ConnectionRefusedError` or `ssl.SSLCertVerificationError` must inspect
  `exc.__cause__` instead.
- `subscribe()` raises `SubscribeError` when any filter is refused; read
  `exc.result` for the per-filter reason codes. Code that checked
  `reason_codes` for values of `0x80` or above can catch the error instead.
- A refused publication fails its receipt with `PublishRejectedError`, a
  `ProtocolError` subclass with `reason_code` and `properties`.
- `keepalive` must be an `int`.
- `max_inbound_inflight`, `maximum_packet_size` and `max_inbound_inflight_bytes`
  default to `None`. MQTT 5 still advertises and enforces 100 exchanges and
  16 MiB packets; MQTT 3.1.1 enforces inbound limits only when set explicitly,
  and no client bounds inbound bytes by default. Pass the 1.0 values
  (`100`, `16 * 1024 * 1024`, `64 * 1024 * 1024`) to keep the old local bounds.
  An explicit `maximum_packet_size=None` used to mean 16 MiB on both protocols.
- On MQTT 3.1.1, `max_outbound_inflight` defaults to 20 in flight instead of no
  local limit. Set it explicitly for a broker configured with a larger window.
- A publication larger than `max_unacknowledged_bytes` is admitted alone
  instead of raising `FlowControlError`; set the bound above your largest
  message if other publications must not wait behind it.
- After a reconnect without a broker session, unacknowledged QoS 1
  publications are sent again as new ones and their receipts complete normally;
  only QoS 2 receipts fail with `SessionDiscardedError`. Remove application
  code that republished QoS 1 messages on that error. Consumers may see the
  message twice, as QoS 1 always allowed.
- An MQTT 5 client connecting with `clean_start=False` accepts Session Present
  even when it holds no incomplete QoS exchange, so a restarted durable
  subscriber resumes its broker session instead of failing with
  `ProtocolError`. Remove any workaround that connected once with
  `clean_start=True` after a restart; it discarded the session.

## Changes since 1.0.0rc17

The stable 1.0.0 release preserves RC17's client behavior, public API and SQLite
schema 5. RC17 applications need no API or database migration for this release.
The support tiers in the [API contract](api-stability.md) remain distinct:
Stable interfaces follow SemVer, while statistics and the supplied stores stay
Provisional.

## Upgrading from earlier candidates

RC17 revised the RC16 API before 1.0. If you are upgrading from **1.0.0rc16**,
start with [Changes since 1.0.0rc16](#changes-since-100rc16). If you are
upgrading from **1.0.0rc14** (`c194597`, 2026-09-11), review the broader
native-API changes throughout this guide as well.

MQTT 3.1.1/5, all QoS levels, TCP/TLS, WebSocket, Unix, manual acknowledgement
and the memory/SQLite backends remain supported. Applications must adapt to
the changes below; historical database formats are not upgraded automatically.

## Removed surfaces and replacements

| 1.0.0rc14 contract | Current replacement |
| --- | --- |
| `mqttium.compat` / Paho façade | A native `AsyncClient` on the application's event loop |
| `mqttium.helpers` | Explicit connect, operation and disconnect on `AsyncClient` |
| `mqttium.PacketType` | Internal protocol tests can import `mqttium.enums.PacketType`; applications use native models |
| `message_delivery="auto"` or `"both"` | Explicit `"iterator"` (default) or `"callback"` |
| Async `on_message` or topic callbacks | Short synchronous callbacks, or asynchronous processing through `messages()` |
| `on_publish` | `PublishReceipt` / `PublishBatchReceipt` |
| Connect/publish notifications sharing the message worker | Separate lifecycle hooks; publication uses receipts |
| Bounded callback worker: `max_pending_callbacks`, `callback_shutdown_timeout`, `DeliveryStats.callback_queued`/`callback_limit`, `TaskStats.callback_worker` | Callbacks run inline on the reader; count invocations in your own callback if needed |
| `manual_ack=True` with `message_delivery="callback"` | `ValueError`; use `messages()` with `await client.ack(message)` |
| Callback route changes during/after connection | Configure before first attempt; a new client is required for different routes |
| `publish(..., nowait=True)` | Synchronous `publish_nowait(...)`, without `await` |
| `publish_backpressure` / `PublishBackpressure` | Choose `publish()` or `publish_nowait()` per operation |
| Atomic chunks in `publish_many()` | Progressive ordered admission with a receipt for the committed prefix |
| Batch `chunk_size`, `nowait`, `failure_sink` | Removed; `max_failure_details` is a finite integer, default 128 |
| Mutable `Properties` | Construct a new immutable `Properties(mapping)` |
| `set_auth_handler()` / assignment to `auth_handler` | Supply `auth_handler` at construction |
| CONNECT property keys duplicating limit arguments | Use the dedicated constructor arguments |
| Shared mutable reconnect policy | Immutable policy with private state per client |
| `ReconnectPolicy.follow_server_reference` | Removed; inspect `BrokerDisconnectError` and explicitly choose a replacement endpoint |
| Delivery small-message diagnostic fields | Exact `stats().delivery.iterator_bytes` with a finite byte limit; zero byte occupancy/high-water values when `max_iterator_bytes=None` |
| Custom engine/store/transport integration guarantees | Internal implementation interfaces |
| `SubscribeResult.from_packet()` / `UnsubscribeResult.from_packet()` | Use the results returned by `subscribe()` / `unsubscribe()`, or construct `SubscribeResult(mid=..., reason_codes=...)`; decoded SUBACK/UNSUBACK packets remain Internal |

## Changes since 1.0.0rc16

The [pre-1.0 surface review](reports/API-SURFACE-REVIEW-2026-09-24.md) led to
removing unused and duplicate contracts and making internal state private.

| 1.0.0rc16 contract | Replacement |
| --- | --- |
| `MQTTProtocolVersion.MQTTv31` (always refused) | None; MQTT 3.1 is unsupported. Use `MQTTv311` or `MQTTv5` |
| `ConnectionState.RECONNECTING` (never reported) | None; automatic retry reports `CONNECTING` then `CONNECTED` or `DISCONNECTED` |
| `NegotiatedSettings.effective_keepalive` | `NegotiatedSettings.server_keep_alive` (`None` means the requested keepalive applies) |
| `NegotiatedSettings.effective_client_id(local)` | `AsyncClient.effective_client_id` |
| `NegotiatedSettings.from_connack()` | Internal; read `AsyncClient.negotiated` |
| Encoding and decoding methods of `SubscribeOptions`, `ConnAckPacket`, `AuthPacket` | Internal; construct the models and read their fields |
| `PublishBatchError.failures`, `.failure_count`, `.failure_counts` | The same fields on `PublishBatchError.receipt` |
| `PublishBatchError.cause` | `PublishBatchError.__cause__` (the error is raised `from` its cause) |
| `PublishBatchError.receipt` could be `None` | Always the batch receipt; no narrowing needed |
| `PublishBatchReceipt.completed` | `receipt.submitted - receipt.pending_count` |
| Mutable `PublishReceipt.mid` / `.qos`; value equality; constructor fields `_waiters`, `_error`, `_settled` | Read-only `mid` and `qos`; identity equality; `PublishReceipt(mid, qos)` |
| Mutable `SubscribeResult` / `UnsubscribeResult` | Frozen; construct a new value instead of assigning fields |
| Nested statistics types imported from `mqttium.api.stats` or other modules | Import them from `mqttium.api` |
| `will=Message(...)` plus `will_properties=Properties(...)` | `will=PublishMessage(topic, payload, qos=..., retain=..., properties=...)`; a `Message` is refused with `TypeError` |
| Store methods (`put_out`, `get_out`, `complete_out`, `in_replay_pages`, `batch`, ...) called by applications | Internal; use `client.stats()` for a running client. Construction, `store=`, `close()` and `with` stay supported |
| `ClientStats.connection_epoch` (an internal epoch, advanced about twice per connection) | `ClientStats.connections`: connections established since construction |
| `WriterStats.max_messages`, `max_bytes` | `WriterStats.message_limit`, `byte_limit` |
| `DecoderStats.max_packet_size` | The `maximum_packet_size` you configured |
| `TransportStats.kind` | None; the application knows which transport it opened |
| `ProtocolError` for a `Properties` value or name of the wrong type | `TypeError` |
| `ProtocolError` for a `SubscribeOptions` QoS or `retain_handling` out of range | `ValueError`, like an invalid QoS given to `publish()` |
| `__all__` lists of Internal packages (`mqttium.packets`, `codec`, `transport`, `dispatch`, `protocol`, `api.models`, `api.stats`) | None; these packages are Internal |

`MQTTTimeoutError` now also derives from `TimeoutError`; existing
`except MQTTTimeoutError` handlers are unchanged.

## Frozen constructor and snapshot vocabulary

The constructor names every bound after the thing it bounds and refuses
configuration that would have no effect. The signature and defaults are
recorded in `tests/project/test_public_api_surface.py`.

| Previous name | Frozen name | Note |
| --- | --- | --- |
| `local_receive_maximum` | `max_inbound_inflight` | Advertised as Receive Maximum on MQTT 5; enforced locally on both protocols |
| `max_pending_inbound_bytes` | `max_inbound_inflight_bytes` | Exceeding either inbound bound ends the connection (DISCONNECT `0x93` / `0x97`) |
| `max_pending_outbound_messages` / `_bytes` | `max_unacknowledged_messages` / `_bytes` | Admitted QoS 1/2 publications not yet completed, including those awaiting an inflight slot |
| `max_outbound_messages` / `_bytes` | `max_write_queue_messages` / `_bytes` | Encoded frames resident in the writer |
| `max_pending_messages`, `max_pending_delivery_bytes`, `delivery_timeout` | `max_iterator_messages`, `max_iterator_bytes`, `iterator_admission_timeout` | Iterator-only; a non-default value with `message_delivery="callback"` raises `ValueError` |
| `ack_timeout` | `subscribe_timeout` | Default SUBACK/UNSUBACK deadline |
| `ReconnectPolicy.connect_timeout` | `AsyncClient(connect_timeout=...)` | One deadline for explicit `connect*()` calls without `timeout` and for automatic attempts |
| `ReconnectPolicy(enabled=False)` | `reconnect=None` | Passing a policy enables reconnection |
| `max_ingress_batch_bytes` | removed | The 1 MiB / 256-packet decode quantum is a fairness constant |
| MQTT 5 options accepted by an MQTT 3.1.1 client until `connect()` | `ProtocolError` from the constructor | `connect_properties`, properties on `will=PublishMessage(...)`, `topic_alias_maximum`, `auth_handler` |
| `MandatoryResponseTooLargeError` importable from `mqttium.errors` only | Exported from `mqttium` | Local terminal failure; never retried |

`ClientStats` reports connection state, the lifetime `connections` count,
the current reconnect attempt, and one section per queue or window. It no
longer exposes the internal connection epoch or runtime scheduling details:

| Previous field | Frozen field |
| --- | --- |
| `stats().tasks` (`TaskStats`) | removed; `state` and `reconnect_attempt` describe recovery |
| `stats().effects` (`EffectStats`) | removed |
| `writer.batches`, `batched_items`, `batched_bytes`, `segmented_writes`, `enqueue_suspensions`, `eager_writes`, `eager_bytes` | removed; `queued_*`, `high_water_*`, `message_limit`, `byte_limit`, `waiters`, `last_outbound` remain |
| `decoder.ingress_batch_limit_bytes` | removed |
| `outbound.pending_messages` / `pending_bytes` / `pending_high_water_*` | `outbound.unacknowledged_messages` / `unacknowledged_bytes` / `unacknowledged_high_water_*` |
| `outbound.queued_messages`, `flow_inflight`, `flow_limit` | `outbound.awaiting_slot`, `inflight`, `inflight_limit` |
| `inbound.receive_maximum`, `pending_bytes`, `pending_high_water_bytes`, `pending_byte_limit` | `inbound.inflight_limit`, `inflight_bytes`, `inflight_high_water_bytes`, `inflight_byte_limit` |
| `delivery.pending_bytes`, `pending_high_water_bytes`, `max_bytes` | `delivery.iterator_bytes`, `iterator_high_water_bytes`, `iterator_byte_limit` |
| `transport.fragmented_read_bytes`, `pending_control_frames`, `pending_control_bytes` | removed; `buffered_read_bytes` is `None` when total transport backlog is unavailable |

## Delivery handles and disconnect diagnostics

Manual acknowledgement requires a delivered handle from the active logical
exchange. Reconstructing a `Message` from its fields no longer authorizes an
acknowledgement; stale and foreign handles raise `ProtocolError`. Handles survive
a transport reconnect that resumes the same session, including an explicit one.

Nonzero broker DISCONNECT information is available as `BrokerDisconnectError`
through the existing `on_disconnect(error)` signature. It carries `reason_code`
and immutable `properties`, and replaces only an otherwise generic closure.

## Properties

```python
from mqttium.api import Properties

properties = Properties({
    "content_type": "application/json",
    "user_property": [("source", "sensor-1")],
})
updated = Properties({**properties.values, "content_type": "text/plain"})
```

Input dictionaries, lists and binary buffers are copied into an immutable
representation. Repeated properties are tuples, including nested user-property
pairs. Mutation of the original input cannot change stored records, encoded
packets, received messages or reserved byte counts.

## Publication and cancellation

```python
receipt = await client.publish("telemetry", b"sample", qos=1)
await receipt.wait()

# In a context that must never suspend:
receipt = client.publish_nowait("telemetry", b"sample", qos=1)
```

The nonblocking method can raise `FlowControlError` for protocol/writer pressure
or a pending protocol-effect transfer. A full application-delivery queue alone
is not a publication refusal.
Choose an application retry, rejection or spill policy; never busy-spin.

A cancelled `publish()` may already be committed. Cancellation stops the Python
wait; it does not undo MQTT admission. Its effect transfer remains owned by the
client. Receipt waits are independent of each other.

For batches, each successful admission remains committed if a later element or
the input iterator fails. Catch `PublishBatchError` as `exc` and inspect
`exc.receipt.submitted`, `exc.receipt.failure_count` and
`exc.receipt.failure_counts`. The original admission failure, if any, is
`exc.__cause__`; `exc.receipt.failures` contains the bounded failure details.
A cancellation propagates unchanged;
the internally registered aggregate is sealed and its admitted exchanges remain
owned by the client. There is no rollback of a committed prefix.

## Message delivery and lifecycle hooks

Set `message_delivery="callback"` and register short `def` handlers before the
first connection attempt. Async message functions and async callable objects
are rejected before changing registration. Returning an awaitable from a sync
handler is an error, not an implicit task handoff. A handler may explicitly
create an application-owned task, but the application must retain it and bound
the amount of pending work.

For asynchronous message processing, move the body to the iterator:

```python
async for message in client.messages():
    result = await process(message)
    await client.publish("result", result)
```

This pattern can use outgoing capacity while an unrelated delivery is queued.
It is not an unlimited-pressure guarantee: an ACK not yet read can be behind
incoming messages whose queue is full. If a sole consumer must await outgoing
capacity or receipts during sustained bidirectional traffic, use an independently
draining consumer and a bounded application producer with a nonblocking
overflow policy, or separate receiving and publishing connections. A bounded
queue whose consumer stops draining while waiting for publication can recreate
the same dependency. See [bidirectional pressure](operations.md#bidirectional-pressure).

Callback messages are no longer queued: matching synchronous callbacks run on
the delivering reader, and the reader decodes no further packet until they
return. `max_pending_callbacks` and `callback_shutdown_timeout` are removed from
the constructor; `DeliveryStats.callback_queued` and `callback_limit` are
removed without a public replacement, since the snapshot describes retained
state and callback delivery retains nothing. All routes matching one message
run contiguously; the reader yields at a message boundary once its invocation
budget is reached and carries the excess over. `manual_ack=True` now requires
iterator delivery: synchronous callbacks cannot await `ack()`, so acknowledge
from the `messages()` consumer. Iterator delivery charges each message once and
releases the charge when the iterator yields it. A positive
`iterator_admission_timeout` covers iterator byte reservation and queue
admission; `None` has no deadline.
`MessageDeliveryError` reports timeout or an impossible message size.

Replace `on_publish` with receipt observation. Keep lifecycle setup asynchronous:

```python
async def on_connect(connack):
    if not connack.session_present:
        await client.subscribe("commands/#", qos=1)
    await client.publish("status", b"online")

client.on_connect = on_connect
```

Lifecycle hooks execute after the triggering effect and connection locks are
released. `connect()` and `disconnect()` return after the network operation,
not after the hook; incoming messages do not wait for `on_connect` to finish.
Use an application readiness signal when necessary. Hooks describe the latest
state: obsolete pending notifications are coalesced, and external lifecycle
operations cancel obsolete active hooks. A lifecycle operation awaited directly
by the hook itself preserves that caller. Automatic retry waits until the current
`on_disconnect` hook starts, then rechecks user intent and may reconnect while
the hook is still running. The replacement connection's `on_connect` waits for
that hook to finish. Use application-owned synchronization if reconnect must
depend on asynchronous cleanup. See the full
[hook contract](reference/async-client.md#lifecycle-hooks).

`auth_handler` retains its timeout and protocol-specific async behavior.

## SQLite format

The current implementation writes **schema 5** and can reopen a valid schema-5
database. A version upgrade alone does not require a new database path.
Historical schemas 0–4 containing data, future versions and inconsistent
schemas are explicitly refused; use a new database path for an incompatible
format and plan recovery of outstanding work separately. Validation precedes
write-affecting pragmas. Refusal preserves committed schema and data; SQLite may still recover,
checkpoint, or coordinate its main database and journal files. File-byte identity
is not promised. There is no migration or silent reset.

The measured payload-last layout, lazy transactions, metadata transitions and
paged replay remain. Logical record sizes are persisted at initial admission;
there is no historical size backfill. The JSON property representation is
canonical, with explicit binary encoding and no tuple compatibility markers.

The store protocol is internal. `batch()` groups writes; SQLite supplies a
transaction and memory uses a no-op context. The engine still compensates failed
individual publication admissions. Application-wide cross-backend transactions
are not promised.

## Qualification

The tree must pass protocol, persistence, lifecycle, backpressure, fuzz and
real-broker tests. Performance comparisons use exact baseline/candidate commits
and the same delivered work and resource bounds. The lean-native diagnostic
comparison has no performance acceptance threshold; it does not replace the
strict release controls in the [benchmarking contract](benchmarking.md) and
[release procedure](release-process.md). Historical release reports do not
qualify this implementation.

## Decoder storage and ingress contract

The decoder owns packet-boundary bytes; no reusable-buffer view escapes into
protocol state or the application. Ingress remains bounded and connection-scoped.
The current native API removes the direct QoS 0 adapter path and uses the
common engine/effect pipeline for every message.

### Transport receive backlog availability

The Provisional `transport.buffered_read_bytes` field is now `int | None`.
Handle `None` as unavailable in displays and calculations, rather than coercing
it to zero. Pull streams and WebSocket do not expose complete occupancy through
public asyncio APIs; push transports retain their measured byte count and
disconnected clients report zero. No replacement statistics field is added.

### Finite timeout values

Timeout defaults and per-call overrides must be finite and positive. Replace
NaN/infinite values with a finite deadline, or use `None` where the specific
setting documents a default or disabled bound. Zero remains supported for
keepalive and nonnegative reconnect durations, not timeout overrides. Invalid
overrides now raise `ValueError` before changing the client. A `ReconnectPolicy`
with a NaN or infinite `initial_delay`, `multiplier`, `max_delay` or
`stable_after` now raises `ValueError` when constructed; use `max_retries=None`
rather than an infinite delay to retry indefinitely.
