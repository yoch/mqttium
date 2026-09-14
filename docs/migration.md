# Migrating to the lean native experiment

The `codex/lean-native-experiment` branch deliberately breaks the pre-v1 API at
`9ad1f018`. It keeps MQTT 3.1.1/5, all QoS levels, TCP/TLS, WebSocket, Unix,
manual acknowledgement and the memory/SQLite backends. There is no automatic
upgrade of applications or historical databases.

## Removed surfaces and replacements

| Previous contract | Experimental replacement |
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
| Delivery small-message diagnostic fields | Uniform `stats().delivery.iterator_bytes` / `iterator_byte_limit` |
| Custom engine/store/transport integration guarantees | Internal implementation interfaces |

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
| MQTT 5 options accepted by an MQTT 3.1.1 client until `connect()` | `ProtocolError` from the constructor | `connect_properties`, `will_properties`, `topic_alias_maximum`, `auth_handler` |
| `MandatoryResponseTooLargeError` importable from `mqttium.errors` only | Exported from `mqttium` | Local terminal failure; never retried |

`ClientStats` keeps the same shape (state, epoch, reconnect attempt, one
section per queue or window) with renamed fields and without runtime
scheduling detail:

| Previous field | Frozen field |
| --- | --- |
| `stats().tasks` (`TaskStats`) | removed; `state` and `reconnect_attempt` describe recovery |
| `stats().effects` (`EffectStats`) | removed |
| `writer.batches`, `batched_items`, `batched_bytes`, `segmented_writes`, `enqueue_suspensions`, `eager_writes`, `eager_bytes` | removed; `queued_*`, `high_water_*`, `max_*`, `waiters`, `last_outbound` remain |
| `decoder.ingress_batch_limit_bytes` | removed |
| `outbound.pending_messages` / `pending_bytes` / `pending_high_water_*` | `outbound.unacknowledged_messages` / `unacknowledged_bytes` / `unacknowledged_high_water_*` |
| `outbound.queued_messages`, `flow_inflight`, `flow_limit` | `outbound.awaiting_slot`, `inflight`, `inflight_limit` |
| `inbound.receive_maximum`, `pending_bytes`, `pending_high_water_bytes`, `pending_byte_limit` | `inbound.inflight_limit`, `inflight_bytes`, `inflight_high_water_bytes`, `inflight_byte_limit` |
| `delivery.pending_bytes`, `pending_high_water_bytes`, `max_bytes` | `delivery.iterator_bytes`, `iterator_high_water_bytes`, `iterator_byte_limit` |
| `transport.fragmented_read_bytes`, `pending_control_frames`, `pending_control_bytes` | removed; `buffered_read_bytes` includes a fragment under reassembly |

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

The nonblocking method can raise `FlowControlError` for protocol/writer pressure,
a pending protocol-effect transfer. A full application-delivery queue alone is
not a publication refusal.
Choose an application retry, rejection or spill policy; never busy-spin.

A cancelled `publish()` may already be committed. Cancellation stops the Python
wait; it does not undo MQTT admission. Its effect transfer remains owned by the
client. Receipt waits are independent of each other.

For batches, each successful admission remains committed if a later element or
the input iterator fails. Catch `PublishBatchError` and inspect `receipt`,
`cause`, `submitted` and failure counts. A cancellation propagates unchanged;
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
by the hook itself preserves that caller. Automatic retry waits for the current
`on_disconnect` hook, then rechecks user intent. See the full
[hook contract](reference/async-client.md#lifecycle-hooks).

`auth_handler` retains its timeout and protocol-specific async behavior.

## SQLite format

Use a new database path. This experiment writes **schema 5** and can reopen its
own databases. Historical schemas 0–4 containing data, future versions and
inconsistent schemas are explicitly refused. Validation precedes write-affecting
pragmas. Refusal preserves committed schema and data; SQLite may still recover,
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

The branch must pass protocol, persistence, lifecycle, backpressure, fuzz and
real-broker tests. Performance comparisons use exact baseline/candidate commits
and the same delivered work and resource bounds. Performance regressions are
reported rather than used as a gate for this experiment. Historical release
reports do not qualify this implementation.

## Decoder storage and ingress contract

The decoder owns packet-boundary bytes; no reusable-buffer view escapes into
protocol state or the application. Ingress remains bounded and connection-scoped.
The native experiment removes the direct QoS 0 adapter path and uses the common
engine/effect pipeline for every message.
