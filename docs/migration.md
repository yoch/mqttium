# Migrating to MQTTium

New async code should use `mqttium.api.AsyncClient`. Existing Paho VERSION2
applications can start with `mqttium.compat.paho.Client` and move to the native
API later. One-shot scripts can use `mqttium.helpers.publish` and
`mqttium.helpers.subscribe`.

## Choose the adoption path

| Existing code | Start with | Move toward |
| --- | --- | --- |
| Paho VERSION2 callbacks and synchronous callers | `mqttium.compat.paho.Client` | Replace one service boundary at a time with `AsyncClient` |
| Native asyncio code | `mqttium.api.AsyncClient` | Keep protocol completion and backpressure explicit |
| `paho.mqtt.publish` / `subscribe` scripts | `mqttium.helpers` | Use a long-lived `AsyncClient` if operations become frequent |
| gmqtt application | `mqttium.api.AsyncClient` | Review completion, QoS 2 and bounded-queue differences |

The compatibility facade is useful when changing event-loop ownership and API
shape at the same time would make a migration too risky. It is not required for
new async code.

## MQTT 3.1 configuration

`MQTTProtocolVersion.MQTTv31` remains importable with numeric value `3` for
backwards compatibility, but selecting it is no longer executable protocol
support. `EngineConfig(protocol=MQTTProtocolVersion.MQTTv31)` and therefore
`AsyncClient(protocol=MQTTProtocolVersion.MQTTv31)` now fail immediately with
`ValueError` before creating protocol/runtime state. Migrate such configuration
to `MQTTProtocolVersion.MQTTv311` when the broker supports MQTT 3.1.1, or to
`MQTTProtocolVersion.MQTTv5` when MQTT 5 features are required.

## From Paho

The compatibility layer keeps the familiar loop and callback shape:

```python
from mqttium.compat.paho import CallbackAPIVersion, Client


client = Client(CallbackAPIVersion.VERSION2, "client-id")
client.loop_start()
try:
    client.connect("localhost")
    info = client.publish("events", b"ready", qos=1)
    info.wait_for_publish(timeout=5)
finally:
    client.disconnect()
    client.loop_stop()
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
- call `client.is_connected()` as a method, matching Paho;
- MQTTium does not reproduce non-compliant QoS republishing after a clean
  session;
- use native `await client.connect()` instead of Paho's historical
  `connect_async` behaviour;
- WebSocket connections are native: `await client.connect_ws(url)`;
- MQTT 5 enhanced authentication uses `auth_handler` or `await client.auth()`;
- durable inflight state is configured with `SqliteInflightStore`.

A practical staged migration is:

1. change the import and require `CallbackAPIVersion.VERSION2` while preserving
   existing callbacks and caller threads;
2. configure queue and byte limits, then handle `MQTT_ERR_QUEUE_SIZE` as an
   explicit overload result;
3. move new publishing or consuming paths to `AsyncClient`, replacing
   `wait_for_publish()` with `await receipt.wait()`;
4. remove the compatibility loop after the final synchronous boundary is gone.

Native `AsyncClient` also has `message_callback_add` / `message_callback_remove`.
The names match Paho; the callback receives a native `Message` rather than a
façade `MQTTMessage`, and there is no `client` / `userdata` prefix. Matching
filters take precedence over `on_message`, as in Paho.

Do not call blocking `Client` methods from its network-thread callbacks. Move
that operation to another thread or convert the callback path to the native
client.

See [`paho-compatibility.md`](paho-compatibility.md) for the complete supported surface.

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

## Callback callable form

Use `def` for synchronous callbacks and `async def` for callbacks that await.
A synchronous callback that returns a coroutine, `Future`, or other awaitable is
no longer implicitly handed to the callback worker; it is reported as a callback
`TypeError`. Convert such callbacks to `async def`. This removes hidden scheduling
state and makes callback execution mode explicit from the callable itself.

## Durable sessions

```python
from mqttium import MQTTProtocolVersion
from mqttium.api import AsyncClient, Properties, ReconnectPolicy
from mqttium.persistence import SqliteInflightStore


store = SqliteInflightStore("session.sqlite")
client = AsyncClient(
    "durable-client",
    protocol=MQTTProtocolVersion.MQTTv5,
    clean_start=False,
    connect_properties=Properties({"session_expiry_interval": 86_400}),
    reconnect=ReconnectPolicy(),
    store=store,
)
```

SQLite and broker-session retention are separate requirements. See
[`sessions-and-persistence.md`](sessions-and-persistence.md) before relying on
restart recovery.

Historical SQLite rows are accounted for when they are first reopened. A store
already above a new limit may drain existing work but cannot admit more until it
falls below that limit.

Third-party `InflightStore` implementations remain supported, but the Provisional
contract is now complete rather than capability-discovered. Custom stores must
implement the bounded replay, metadata lookup, conditional transition, and
conditional completion methods declared by `InflightStore`; MQTTium no longer
falls back to eager whole-store hydration or read/mutate/write state transitions.

The former `PagedInflightStore`, `BoundedInboundReplayStore`, and
`TransitionInflightStore` capability protocols are removed. The shipped stores
also no longer expose the retired whole-object helpers `update_out`, `out_items`,
`out_pages`, `pop_in`, `update_in`, `in_items`, `in_pages`, or `contains_in`. Use
`out_summary_pages()` / `in_index_pages()` for ordered metadata inspection,
`get_out()` / `get_in()` when a payload is actually needed, and the
`transition_*()` / `complete_*()` methods for state changes.

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

Subscriber process:

```python
from mqttium.helpers import subscribe


message = await subscribe.simple("events/#", hostname="127.0.0.1")
```

Publisher process:

```python
from mqttium.helpers import publish


await publish.single("events/ready", b"ready", qos=1, hostname="127.0.0.1")
```

Start the subscriber first. For repeated operations, keep one `AsyncClient`
connected instead of reconnecting per call.

MQTTium is original Apache-2.0 code. Paho and gmqtt are referenced for API and
behavioural comparison; their protocol engines are not copied.

## Transport receive capabilities

`AsyncTransport` no longer declares `read()`. Receiving is a capability, and a
transport implements exactly one:

- `PullTransport` — `async def read(self, n: int = 65536) -> bytes`
- `DecoderPushTransport` — `def attach_decoder(self, decoder)` plus
  `async def receive(self) -> bool`

Both are exported from `mqttium.transport` and are runtime-checkable, so a
consumer resolves the capability with `isinstance` and never has to guess. A
push-capable transport has no `read()` at all, so the two checks cannot both
succeed.

Custom transports that previously satisfied `AsyncTransport` by providing
`read()` now satisfy `AsyncTransport` *and* `PullTransport`, and need no change.
Code that annotated `AsyncTransport` and called `.read()` should annotate
`PullTransport` instead.

