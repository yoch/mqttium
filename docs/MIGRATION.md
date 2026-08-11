# Migrating to MQTTium

New async code should use `mqttium.api.AsyncClient`. Existing Paho VERSION2
applications can start with `mqttium.compat.paho.Client` and move to the native
API later. One-shot scripts can use `mqttium.helpers.publish` and
`mqttium.helpers.subscribe`.

## From Paho

The compatibility layer keeps the familiar loop and callback shape:

```python
from mqttium.compat.paho import CallbackAPIVersion, Client


client = Client(CallbackAPIVersion.VERSION2, "client-id")
client.connect("localhost")
client.loop_start()
info = client.publish("events", b"ready", qos=1)
info.wait_for_publish()
```

The native API removes the background thread and makes completion explicit:

```python
from mqttium.api import AsyncClient


client = AsyncClient("client-id")
await client.connect("localhost")
receipt = await client.publish("events", b"ready", qos=1)
await receipt.wait()
await client.disconnect()
```

Important compatibility differences:

- only `CallbackAPIVersion.VERSION2` is supported;
- MQTTium does not reproduce non-compliant QoS republishing after a clean
  session;
- use native `await client.connect()` instead of Paho's historical
  `connect_async` behaviour;
- WebSocket connections are native: `await client.connect_ws(url)`;
- MQTT 5 enhanced authentication uses `auth_handler` or `await client.auth()`;
- durable inflight state is configured with `SqliteInflightStore`.

See [`COMPAT.md`](COMPAT.md) for the complete supported surface.

## From gmqtt

```python
from mqttium.api import AsyncClient


client = AsyncClient("client-id", username=user, password=password)
await client.connect("localhost")
receipt = await client.publish("events", b"ready", qos=1)
await receipt.wait()
await client.disconnect()
```

MQTTium deliberately keeps a packet identifier until PUBCOMP for QoS 2,
separates Receive Maximum from the packet-identifier space, and uses a bounded
incremental decoder.

## Bounded queues are the default

Queues that grow with application load have finite defaults. A publisher that
outruns its broker waits for capacity instead of consuming memory indefinitely.

```python
client = AsyncClient(
    max_pending_outbound_messages=10_000,
    max_pending_outbound_bytes=64 * 1024**2,
    max_pending_inbound_bytes=64 * 1024**2,
    max_pending_delivery_bytes=64 * 1024**2,
    publish_backpressure="wait",
)
```

Existing applications should account for these rules:

- `publish()` waits by default. With `publish_backpressure="error"` or
  `nowait=True`, saturation raises `FlowControlError` without allocating a
  packet identifier or writing store state.
- A publisher waiting for capacity survives a reconnect attempt, but fails when
  the connection becomes terminal.
- Passing `None` disables an individual limit.
- `publish_many()` retains at most `max_failure_details` individual errors while
  keeping exact aggregate counts. Use `failure_sink` when every detail matters.
- The Paho façade maps saturation to `MQTT_ERR_QUEUE_SIZE`; its message and byte
  limits can be adjusted independently.

The writer has its own byte and message limits. Applications sending large
payloads should size the byte budget explicitly rather than relying only on a
message count.

## Durable sessions

```python
from mqttium.api import AsyncClient
from mqttium.persistence.sqlite import SqliteInflightStore


store = SqliteInflightStore("session.sqlite")
client = AsyncClient("durable-client", store=store)
```

Historical SQLite rows are accounted for when they are first reopened. A store
already above a new limit may drain existing work but cannot admit more until it
falls below that limit.

Third-party `InflightStore` implementations remain supported. Implementing the
optional `PagedInflightStore` protocol enables incremental replay without
materialising every payload at once. The fallback eager path is correct but can
use substantially more memory for large sessions.

## Updating `EngineConfig`

`EngineConfig.update()` validates a copy before changing the live object, so an
invalid value cannot leave a partial update. Once attached to a protocol engine,
only settings without derived connection state can change in place, including
credentials, keepalive, authentication, wills, and admission limits.

Changing the MQTT version, `local_receive_maximum`, or `maximum_packet_size`
requires a new engine or client. The removed `max_queued` field should be
replaced with `max_pending_outbound_messages` and
`max_pending_outbound_bytes`; `None`, rather than zero, means unlimited.

## One-shot helpers

```python
from mqttium.helpers import publish, subscribe


await publish.single("events", b"ready", qos=1, hostname="127.0.0.1")
message = await subscribe.simple("events/#", hostname="127.0.0.1")
```

MQTTium is original Apache-2.0 code. Paho and gmqtt are referenced for API and
behavioural comparison; their protocol engines are not copied.
