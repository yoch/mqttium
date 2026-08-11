# MQTTium

**A dependable, async-native MQTT client for Python.**

MQTT looks simple until a connection drops halfway through a QoS exchange, a
consumer slows down, or a process restarts with work still in flight. MQTTium
is built for those moments. It keeps protocol state explicit, places hard
limits around memory growth, and tells the application when a publication has
actually completed.

MQTTium supports MQTT 3.1.1 and MQTT 5, QoS 0/1/2, TCP, TLS, WebSocket and Unix
sockets. Its primary API is native `asyncio`; a Paho-compatible VERSION2 facade
is available for gradual migrations.

> **Status:** release candidate (`1.0.0rc1`). The native Stable API is frozen
> for 1.0. Provisional extension points may still evolve under their documented
> compatibility policy.

## Design philosophy

MQTTium treats reliability as an API property, not an implementation detail:

- **One owner for protocol state.** Packet identifiers, QoS transitions,
  reconnects and session replay are managed by explicit state machines. This
  makes failures predictable and the implementation easier to audit.
- **Bounded work by default.** Producer, writer and consumer queues have message
  and byte limits. Overload becomes waiting or an explicit error, not
  unbounded memory growth.
- **Observable completion.** A publish receipt distinguishes local admission
  from broker acknowledgement. Runtime snapshots expose pressure and inflight
  state without requiring per-message logging.

Those choices are most useful in services, gateways and devices where
reconnects, sustained traffic or delivery confirmation matter. For a script
that occasionally sends a QoS 0 message, a smaller client may be all you need.

## Install

```bash
python -m pip install mqttium
```

Python 3.11 through 3.14 is supported. MQTTium is licensed under Apache-2.0.

To evaluate the current release candidate directly from source:

```bash
git clone https://github.com/yoch/mqttium.git
cd mqttium
python -m pip install -e .
```

## Quick start

This client subscribes, publishes with QoS 1, waits for the broker's PUBACK and
then consumes the message:

```python
import asyncio

from mqttium.api import AsyncClient


async def main() -> None:
    client = AsyncClient("example-client")
    try:
        await client.connect("127.0.0.1", 1883)
        await client.subscribe("devices/+/status", qos=1)

        receipt = await client.publish(
            "devices/demo/status",
            b"online",
            qos=1,
        )
        await receipt.wait()

        async for message in client.messages():
            print(message.topic, message.payload)
            break
    finally:
        await client.disconnect()


asyncio.run(main())
```

For QoS 0, a receipt is complete once the message has been admitted for
writing. For QoS 1 and 2, `receipt.wait()` follows the MQTT acknowledgement
exchange.

## Receiving messages

Use the async iterator when message processing naturally belongs in a task:

```python
async for message in client.messages():
    await handle(message.topic, message.payload)
```

Or assign a callback when an existing application is callback-oriented:

```python
def on_message(message) -> None:
    print(message.topic, message.payload)


client.on_message = on_message
```

Delivery mode, callback concurrency, queue depth and retained payload bytes are
bounded independently. Slow application code therefore applies visible
backpressure instead of quietly consuming the process's memory. Applications
that need control of acknowledgement timing can enable `manual_ack` and call
`await client.ack(message)` after processing.

## Publishing and overload

`publish()` waits for capacity by default. Applications that must shed load
instead can request immediate refusal:

```python
from mqttium import FlowControlError
from mqttium.api import AsyncClient

client = AsyncClient(publish_backpressure="error")

try:
    receipt = await client.publish("telemetry", payload, qos=1)
except FlowControlError:
    # Retry later, shed load, or apply an application-specific policy.
    ...
```

For high-volume producers, `publish_many()` consumes an iterable in bounded
chunks and uses one aggregate receipt:

```python
from mqttium.api import PublishMessage

batch = await client.publish_many(
    PublishMessage("telemetry", sample, qos=1) for sample in samples
)
await batch.wait()
```

`publish_nowait()` is the synchronous, non-suspending option for code already
running on the client's event-loop thread. It raises `FlowControlError` as soon
as either the protocol or writer budget is full. These entry points share the
same admission and completion rules; choosing one changes waiting and batching,
not delivery semantics.

## Reconnection and durable inflight state

Automatic reconnect is opt-in on the native client:

```python
from mqttium.api import AsyncClient, ReconnectPolicy

client = AsyncClient(
    "gateway",
    clean_start=False,
    reconnect=ReconnectPolicy(
        initial_delay=0.5,
        max_delay=30.0,
        max_retries=None,
    ),
)
```

Reconnect delays use jittered exponential backoff. Terminal broker rejections
are not retried indefinitely, and interrupted QoS exchanges keep their protocol
phase.

When inflight work must survive a process restart, provide a SQLite store:

```python
from mqttium.api import AsyncClient
from mqttium.persistence import SqliteInflightStore

store = SqliteInflightStore("mqtt-session.sqlite")
client = AsyncClient("gateway", clean_start=False, store=store)
```

The store is synchronous and intentionally owned by the application; close it
during application shutdown. Replay is incremental, so restoring a large
session does not require loading every payload into memory at once.

## Transports and MQTT versions

TCP is the default. TLS uses a normal Python `SSLContext`, while WebSocket and
Unix-domain connections have explicit entry points:

```python
await client.connect("broker.example", 8883, ssl=tls_context)
await client.connect_ws("wss://broker.example/mqtt", ssl=tls_context)
await client.connect_unix("/run/mosquitto/mosquitto.sock")
```

Select MQTT 5 when constructing the client:

```python
from mqttium import MQTTProtocolVersion
from mqttium.api import AsyncClient

client = AsyncClient("mqtt5-client", protocol=MQTTProtocolVersion.MQTTv5)
```

The same client API is used for MQTT 3.1.1 and MQTT 5; version-specific
properties remain explicit rather than being silently emulated.

## Moving from Paho

Projects can start with the Paho-compatible facade and migrate one boundary at
a time:

```python
from mqttium.compat.paho import CallbackAPIVersion, Client

client = Client(CallbackAPIVersion.VERSION2)
```

The facade covers the commonly used VERSION2 surface, including filtered
callbacks and threaded producers. It does not reproduce Paho behaviours that
conflict with MQTT correctness or bounded resource use. The exact differences
and native equivalents are documented in
[`docs/COMPAT.md`](docs/COMPAT.md) and [`docs/MIGRATION.md`](docs/MIGRATION.md).

## Stability and evidence

The public contract is divided into Stable, Provisional and Internal surfaces.
The complete boundary and default-value commitments are in
[`docs/API-STABILITY.md`](docs/API-STABILITY.md).

Protocol behaviour is checked with unit, integration, property, fuzz, memory
and broker tests. Performance claims require paired local runs on an eligible
machine; hosted-runner measurements are advisory. Generated measurements stay
outside the source tree so a checkout remains reproducible and clean.

Continue with:

- [`docs/README.md`](docs/README.md) for the documentation map;
- [`docs/DESIGN.md`](docs/DESIGN.md) for architecture and ownership rules;
- [`docs/IMPLEMENTATION-GUIDE.md`](docs/IMPLEMENTATION-GUIDE.md) for operational
  details and configuration;
- [`docs/BENCHMARKING.md`](docs/BENCHMARKING.md) for measurement validity;
- [`docs/STABILITY.md`](docs/STABILITY.md) for release evidence.

Contributions and reproducible bug reports are welcome. See
[`CONTRIBUTING.md`](CONTRIBUTING.md), and report security issues through the
private process in [`SECURITY.md`](SECURITY.md).
