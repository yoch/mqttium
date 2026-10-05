# Coming from Paho or aiomqtt

MQTTium is not a drop-in replacement for either client. This page lists the
differences that surprise people in their first real deployment, with the
MQTTium way of doing each thing. The [migration guide](migration.md) covers
changes between MQTTium versions.

## Mapping the everyday calls

| Task | Paho (`paho-mqtt` 2.x) | aiomqtt | MQTTium |
| --- | --- | --- | --- |
| Create and connect | `Client(...)`, `connect()`, `loop_start()` | `async with Client(host) as client:` | `async with AsyncClient(client_id) as client:` then `await client.connect(host, port)` |
| Receive | `on_message(client, userdata, msg)` on the network thread | `async for message in client.messages:` | `async for message in client.messages():` (default), or `message_delivery="callback"` with a synchronous `on_message(message)` |
| Per-topic callbacks | `message_callback_add(filter, cb)` | filter inside the loop | `message_callback_add(filter, cb)`, callback delivery only, registered before the first `connect()` |
| Publish and confirm | `publish(...).wait_for_publish()` | `await client.publish(...)` | `receipt = await client.publish(...)`, then `await receipt.wait()` |
| Per-publication completion callback | `on_publish(client, userdata, mid, ...)` | none | `receipt.add_done_callback(fn)`; `fn(receipt)` runs on the loop, `receipt.exception()` gives the outcome |
| Reconnect | `reconnect_delay_set()` + `loop_forever()` | application loop around `async with` | `reconnect=ReconnectPolicy(...)` |
| Shut down | `loop_stop()`, `disconnect()` | leave `async with` | `await client.disconnect()` or leave `async with` |

`async with AsyncClient(...)` only guarantees `disconnect()` on exit; it does
not connect, because the endpoint and TLS settings belong to `connect()`.

## Publishing is two steps

`await client.publish(...)` returns once the publication is admitted: queued
for the wire, with its packet identifier and persisted state if any. The broker
has not answered yet. `await receipt.wait()` waits for PUBACK (QoS 1) or
PUBCOMP (QoS 2) and raises `PublishRejectedError` with the broker's
`reason_code` when it refuses the message. Awaiting every receipt serially
limits throughput to one round trip per message; keep receipts and wait on
them later, or use `publish_many()`.

To act on each completion without a coroutine per message, the way Paho's
`on_publish` is used, register `receipt.add_done_callback(fn)`. `fn(receipt)`
runs later on the event loop, never inside the client, and
`receipt.exception()` is `None` on success or the terminal error. A task per
`receipt.wait()` costs about twice as much per message.

## Subscriptions are the application's, not the client's

MQTTium does not remember subscriptions. When the broker starts a new session
(`session_present` is false: first connection, `clean_start=True`, or an
expired session), subscribe again from `on_connect`:

```python
from mqttium import SubscribeError


async def on_connect(connack) -> None:
    if connack.session_present:
        return  # the broker kept the subscriptions
    try:
        await client.subscribe([("plant/+/telemetry", 1), ("plant/+/alarms", 2)])
    except SubscribeError as exc:
        # The broker refused a filter (ACL, quota); exc.result has every code.
        stop_service(exc)


client.on_connect = on_connect
```

`subscribe()` raises `SubscribeError` when any filter is refused. Paho and
aiomqtt return the SUBACK codes and leave the check to you, and an ACL refusal
then looks like a subscription that receives nothing.

## Knowing when the client has given up

Paho calls `on_disconnect` for every loss and keeps retrying inside
`loop_forever()`. MQTTium's `on_disconnect(cause)` runs once per loss too, but
`client.state` already says what happens next:

```python
from mqttium import ConnectionState


def on_disconnect(cause) -> None:
    if client.state is ConnectionState.DISCONNECTED:
        stopped.set()  # terminal: refused credentials, session taken over, ...
    # RECONNECTING: the ReconnectPolicy will try again by itself.
```

A refused CONNACK is a `ConnectRefusedError` with `reason_code`; set
`ReconnectPolicy(retry_refused=True)` to keep retrying refusals the way Paho
does. With a policy, the first `connect()` also retries, so a service may start
before its broker. Network failures raise `ConnectError`, which is both an
`MQTTError` and an `OSError`.

## Lifecycle hooks describe state, not every transition

`on_connect` and `on_disconnect` may be `async def` and may await the client.
They run on their own task, after the connection work is done, and one pending
notification replaces an older one that has not started: a fast
disconnect/reconnect sequence can produce one `on_connect` for the latest
connection instead of a callback per transition. Read `client.state` or
`client.stats()` when you need the current state rather than counting hooks.

## No internal logging

MQTTium logs nothing. Failures reach you as exceptions, receipts, the
`on_disconnect` cause and `client.stats()`; message callback failures go to the
event loop's exception handler. If you relied on Paho's `enable_logger()`, log
from those points (see [observability](observability.md)).

## Routes and callbacks are frozen at the first connection

`on_message` and `message_callback_add()` routes must be set before the first
`connect()` and cannot change afterwards; subscriptions stay mutable. Message
callbacks are synchronous and run on the reader: a slow callback delays every
later packet, including acknowledgements. Do asynchronous work from
`messages()`, or hand it to your own bounded queue. A shared subscription route
`$share/<group>/<filter>` matches by `<filter>`, the topic the broker delivers.

Assigning `on_message` with the default iterator delivery raises `ValueError`:
nothing would read the queue. Choose `message_delivery="callback"` or consume
`messages()`. Consume `messages()` from a single iterator; a second iterator
waiting at the same time raises `MQTTError`, since each message is delivered
only once.

## Threads

`AsyncClient` belongs to one event loop and is not thread-safe. Code on another
thread (a Paho-style `loop_start()` design, a GUI, a synchronous framework)
hands work to the loop:

```python
future = asyncio.run_coroutine_threadsafe(client.publish("t", b"x", qos=1), loop)
receipt = future.result(timeout=5)
```

## Client identifiers and sessions

With `client_id=""`, an MQTT 5 broker assigns an identifier and MQTTium reuses
it for reconnections of the same client instance; MQTT 3.1.1 requires
`clean_start=True` in that case. A durable session across process restarts
(`clean_start=False`, MQTT 5 `session_expiry_interval`) needs a stable, explicit
`client_id`. See [sessions and persistence](sessions-and-persistence.md).

## Flow control defaults

On MQTT 3.1.1 the broker cannot announce how many QoS 1/2 messages it accepts
in flight, so MQTTium keeps 20 in flight, like Paho's default and Mosquitto's
window. Raise `max_outbound_inflight` only for a broker configured with a larger
window. MQTT 5 follows the broker's Receive Maximum. Inbound limits apply on
MQTT 3.1.1 only when you set them. See
[configuration and sizing](configuration-and-sizing.md).
