# Getting started

This guide takes a new MQTTium user from installation to a complete
publish/subscribe exchange. It then explains the choices that affect completion,
delivery and shutdown.

## Requirements

MQTTium supports Python 3.11 through 3.14 and has no runtime dependencies. You
also need an MQTT 3.1.1 or MQTT 5 broker that the application can reach.

Install the package:

```bash
python -m pip install mqttium
```

The examples assume a broker on `127.0.0.1:1883`. If Mosquitto is already
installed, a development listener can be started with an explicit configuration
rather than relying on system defaults:

```text
listener 1883 127.0.0.1
allow_anonymous true
persistence false
```

Do not use an anonymous development listener on an untrusted network.

## A complete native client

`AsyncClient` belongs to one asyncio event loop. Create, connect, use and close
it from that loop:

```python
import asyncio

from mqttium.api import AsyncClient, Message


async def main() -> None:
    received: asyncio.Future[Message] = asyncio.get_running_loop().create_future()
    client = AsyncClient("getting-started")

    async def on_message(message: Message) -> None:
        if not received.done():
            received.set_result(message)

    client.on_message = on_message
    try:
        await client.connect("127.0.0.1", 1883, timeout=5)
        result = await client.subscribe("mqttium/demo", qos=1)
        if any(code >= 0x80 for code in result.reason_codes):
            raise RuntimeError(f"subscription rejected: {result.reason_codes}")

        receipt = await client.publish("mqttium/demo", b"hello", qos=1)
        await receipt.wait()

        message = await asyncio.wait_for(received, timeout=5)
        print(message.topic, message.payload)
    finally:
        await client.disconnect()


asyncio.run(main())
```

The callback may be synchronous or asynchronous. MQTTium isolates callback
exceptions from protocol processing and reports them to the event loop's
exception handler. A callback should still avoid blocking the loop; move
blocking work to an executor or another service boundary.

## What a publish receipt means

`await client.publish(...)` admits a publication under the configured protocol,
memory and writer limits. It returns a `PublishReceipt`:

- QoS 0 is complete after local writer admission because MQTT defines no broker
  acknowledgement;
- QoS 1 completes on PUBACK;
- QoS 2 completes after the PUBREC/PUBREL/PUBCOMP exchange.

Waiting for `publish()` and waiting for `receipt.wait()` therefore answer
different questions. Applications that need confirmed QoS 1/2 completion should
do both.

For a stream of work, use the bounded batch API:

```python
from mqttium.api import PublishMessage

receipt = await client.publish_many(
    PublishMessage("telemetry", payload, qos=1) for payload in payloads
)
await receipt.wait()
```

The iterable is consumed in bounded chunks. The aggregate receipt retains exact
completion and failure counts without creating one task per publication.

## Choosing inbound delivery

The default `message_delivery="auto"` chooses callback delivery when
`on_message` is assigned and iterator delivery otherwise.

Iterator delivery keeps control flow in the consuming task:

```python
async for message in client.messages():
    await process(message)
```

Callback delivery is useful for event-oriented applications:

```python
async def on_message(message) -> None:
    await process(message)


client.on_message = on_message
```

Use `message_delivery="both"` only when two independent application consumers
really need the same message. Both queues retain a reference and participate in
delivery backpressure.

With `manual_ack=True`, inbound QoS 1 and the final QoS 2 acknowledgement wait
for the application:

```python
client = AsyncClient("worker", manual_ack=True)

async for message in client.messages():
    await persist_business_result(message)
    await client.ack(message)
```

Manual MQTT acknowledgement does not replace application-level idempotency. A
message can be delivered again after a connection or process failure.

## Backpressure and immediate refusal

Queues that grow with load are bounded by count, bytes, or both. The default
native policy waits for capacity. Use an explicit refusal policy when waiting is
not acceptable:

```python
from mqttium import FlowControlError

client = AsyncClient(
    "bounded-producer",
    publish_backpressure="error",
    max_pending_outbound_messages=2_000,
    max_pending_outbound_bytes=16 * 1024**2,
)

try:
    receipt = await client.publish("telemetry", payload, qos=1)
except FlowControlError:
    await shed_or_retry(payload)
```

`publish_nowait()` provides the same immediate-refusal behaviour without a
coroutine suspension, but it must run on the client's owning event-loop thread.
It is not a thread-safe producer API. Threaded applications migrating from Paho
should use the [VERSION2 compatibility facade](paho-compatibility.md).

## MQTT versions and transports

MQTT 3.1.1 is the default. Select MQTT 5 explicitly:

```python
from mqttium import MQTTProtocolVersion

client = AsyncClient("mqtt5-client", protocol=MQTTProtocolVersion.MQTTv5)
```

Connect over TCP or TLS with `connect()`, over WebSocket with `connect_ws()`, or
over a Unix-domain socket with `connect_unix()`:

```python
await client.connect("broker.example", 8883, ssl=tls_context)
await client.connect_ws("wss://broker.example/mqtt", ssl=tls_context)
await client.connect_unix("/run/mosquitto/mosquitto.sock")
```

MQTT 5 properties use the typed `Properties` bag. The broker's CONNACK limits
are available through `client.negotiated`; MQTTium applies them rather than
silently sending unsupported QoS, retain or packet sizes.

## One-shot operations

Small async programs do not need to manage a client directly:

```python
from mqttium.helpers import publish, subscribe

try:
    await publish.single(
        "events/ready",
        b"ready",
        qos=1,
        retain=True,
        hostname="127.0.0.1",
    )
    message = await subscribe.simple("events/ready", hostname="127.0.0.1")
finally:
    # A retained publication with an empty payload clears the broker entry.
    await publish.single(
        "events/ready",
        b"",
        qos=1,
        retain=True,
        hostname="127.0.0.1",
    )
```

The helpers connect, complete the requested operation and disconnect. A
retained message makes this sequential demonstration deterministic; normal
subscriber processes usually start before their publishers. A long-lived
client is more efficient when an application sends or receives repeatedly.

## Errors and shutdown

MQTTium uses typed exceptions for protocol, connection, timeout, packet-size,
flow-control, session and batch failures. Do not catch every exception and
continue blindly: a terminal connection rejection needs a different response
from temporary backpressure.

Keep shutdown in `finally`. `disconnect()` drains queued writes when possible,
cancels reconnect work and closes background tasks owned by the native client.
The client is loop-confined and should not be reused from a different event
loop.

Next steps:

- [Sessions and Persistence](sessions-and-persistence.md) for reconnect and
  restart recovery;
- [Operations](operations.md) for sizing and diagnostics;
- [Migrating to MQTTium](migration.md) for Paho and gmqtt applications;
- [API Stability](api-stability.md) for the supported public contract.
