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
| Callback route changes during/after connection | Configure before first attempt; a new client is required for different routes |
| `publish(..., nowait=True)` | Synchronous `publish_nowait(...)`, without `await` |
| `publish_backpressure` / `PublishBackpressure` | Choose `publish()` or `publish_nowait()` per operation |
| Atomic chunks in `publish_many()` | Progressive ordered admission with a receipt for the committed prefix |
| Batch `chunk_size`, `nowait`, `failure_sink` | Removed; `max_failure_details` is a finite integer, default 128 |
| Mutable `Properties` | Construct a new immutable `Properties(mapping)` |
| `set_auth_handler()` / assignment to `auth_handler` | Supply `auth_handler` at construction |
| CONNECT property keys duplicating limit arguments | Use the dedicated constructor arguments |
| Shared mutable reconnect policy | Immutable policy with private state per client |
| Delivery small-message diagnostic fields | Uniform `stats().delivery.pending_bytes` / `max_bytes` |
| Custom engine/store/transport integration guarantees | Internal implementation interfaces |

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
a pending effect transfer, or an unavailable immediate completion notification.
Choose an application retry, rejection or spill policy; never busy-spin.

A cancelled `publish()` may already be committed. Cancellation stops the Python
wait; it does not undo MQTT admission. Its effect transfer remains owned by the
client. Receipt waits are independent of each other.

For batches, each successful admission remains committed if a later element or
the input iterator fails. Catch `PublishBatchError` and inspect `receipt`,
`cause`, `submitted` and failure counts. A cancellation propagates unchanged;
the internally registered aggregate is sealed and its admitted exchanges remain
owned by the client. There is no rollback of a committed prefix.

## Callback delivery and pressure

Set `message_delivery="callback"` and register routes before calling any
`connect*` method. All message callbacks run in the serial worker, including
synchronous handlers. Tests and applications must not depend on an inline call
or a particular event-loop turn. Await application signals to observe delivery.

A message is charged once until all matching handlers finish. The callback
queue holds at most `max_pending_callbacks` waiting jobs plus one active job.
For iterator delivery, the charge ends when the iterator yields the message.

`delivery_timeout` now defaults to `None`. A positive timeout covers byte
reservation and queue admission together. `MessageDeliveryError` reports an
expired deadline or a message too large for its delivery budget.

With an indefinitely backpressured receiver, awaiting outgoing capacity or an
ACK inside the serial callback can create a circular wait: the needed ACK may
follow an incoming message that cannot yet be delivered. Keep a handler's
publication nonblocking, or hand work to a separate bounded application producer.
See the [cookbook](cookbook.md) for that pattern.

## SQLite format

Use a new database path. This experiment writes **schema 5** and can reopen its
own databases. Historical schemas 0–4 containing data, future versions and
inconsistent schemas are explicitly refused. Validation precedes write-affecting
pragmas, so refusal leaves historical databases unchanged. There is no migration
or silent reset.

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
