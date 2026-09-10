# Cookbook

These patterns show ownership and shutdown boundaries. Replace example topics,
timeouts, persistence, and error policy with deployment-specific choices.

## Long-lived service client

```python
import asyncio

from mqttium.api import AsyncClient, ReconnectPolicy


async def run_service() -> None:
    client = AsyncClient(
        "service-a",
        reconnect=ReconnectPolicy(max_retries=None),
    )
    try:
        await client.connect("broker.example", 8883, ssl=True)
        await client.subscribe("commands/service-a", qos=1)

        async for message in client.messages():
            await handle_command(message)
    finally:
        await client.disconnect()


asyncio.run(run_service())
```

Keep blocking application work out of the event loop. If processing must be
durable before MQTT acknowledgement, enable `manual_ack` and acknowledge only
after the durable step.

## Confirmed publisher with a deadline

```python
import asyncio


async def publish_confirmed(client, topic: str, payload: bytes) -> None:
    receipt = await client.publish(topic, payload, qos=1)
    async with asyncio.timeout(10):
        await receipt.wait()
```

The deadline limits this caller's wait. It does not undo an MQTT publication
already admitted to protocol state.

## Bounded telemetry stream

```python
from mqttium import FlowControlError


async def offer_sample(client, sample: bytes) -> bool:
    try:
        client.publish_nowait("telemetry", sample, qos=1)
    except FlowControlError:
        return False
    return True
```

Returning `False` is useful only if the caller actually sheds, aggregates,
retries later, or persists the sample elsewhere.

## Publishing from a message callback

With saturated delivery and `delivery_timeout=None`, awaiting publication
capacity or an ACK inside the serial worker can create a circular wait. The
ACK may sit behind an incoming message waiting for that same worker. Use a
nonblocking offer with an explicit refusal policy:

```python
from mqttium import FlowControlError
from mqttium.api import AsyncClient

client = AsyncClient("responder", message_delivery="callback")
refused_replies = 0


def on_command(message) -> None:
    global refused_replies
    try:
        client.publish_nowait("replies/service-a", message.payload, qos=1)
    except FlowControlError:
        refused_replies += 1


client.message_callback_add("commands/service-a", on_command)
# Configure routes before the first connection attempt.
```

This example explicitly sheds a refused reply and counts it. It never waits
for an ACK inside the handler. If every reply must be retained, hand work to a
separate application producer with its own queue/byte bounds and an explicit
overflow policy; the callback must not wait on a full application queue either.

## Bounded batch publication

```python
from mqttium.api import PublishMessage


async def publish_batch(client, samples) -> None:
    receipt = await client.publish_many(
        (
            PublishMessage("telemetry", sample, qos=1)
            for sample in samples
        ),
    )
    await receipt.wait()
```

`max_failure_details` bounds retained error objects (default 128, optionally
zero). Failure counts remain exact. An ordinary admission or generator error
raises `PublishBatchError` carrying the committed prefix receipt. Cancellation
propagates and seals the aggregate; committed publications remain active.

## Manual acknowledgement after durable work

```python
from mqttium.api import AsyncClient

client = AsyncClient("worker", manual_ack=True)

async for message in client.messages():
    await save_idempotently(message)
    await client.ack(message)
```

Application storage and MQTT acknowledgement are not one atomic transaction.
Use an idempotency key or other duplicate-safe design.

## Runtime health sample

```python
from dataclasses import asdict


def health_fields(client) -> dict[str, object]:
    snapshot = client.stats()
    return {
        "state": snapshot.state.name,
        "reconnect_attempt": snapshot.reconnect_attempt,
        "outbound": asdict(snapshot.outbound),
        "writer": asdict(snapshot.writer),
        "delivery": asdict(snapshot.delivery),
    }
```

Sample at an application-controlled interval and redact identifiers, topics,
properties, and payloads from any surrounding telemetry.

## Durable MQTT 5 gateway

```python
from mqttium import MQTTProtocolVersion
from mqttium.api import AsyncClient, Properties, ReconnectPolicy
from mqttium.persistence import SqliteInflightStore

store = SqliteInflightStore("gateway.sqlite")
client = AsyncClient(
    "gateway-a",
    protocol=MQTTProtocolVersion.MQTTv5,
    clean_start=False,
    connect_properties=Properties(
        {"session_expiry_interval": 86_400}
    ),
    reconnect=ReconnectPolicy(max_retries=None),
    store=store,
)

try:
    await client.connect("broker.example", 8883, ssl=True)
    await serve_gateway(client)
finally:
    await client.disconnect()
    store.close()
```

Use a stable client identifier and configure the broker to retain the session.
SQLite does not persist arbitrary application work or subscription intent.

## Graceful shutdown sequence

1. Stop accepting new application work.
2. Stop or cancel producer tasks according to the application's policy.
3. Wait for required receipts within the service shutdown deadline.
4. Call `await client.disconnect()` in `finally`.
5. Close the application-owned inflight store.
6. Inspect the final snapshot when shutdown behaviour is under investigation.

See [Operations and Observability](operations.md) for the expected idle state.
