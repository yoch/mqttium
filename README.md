<p align="center">
  <img src="https://raw.githubusercontent.com/yoch/mqttium/main/docs/assets/mqttium-logo-900.png" alt="MQTTium logo" width="180">
</p>

<h1 align="center">MQTTium</h1>

<p align="center"><strong>Async-native MQTT 3.1.1 and MQTT 5 for Python.</strong></p>

<p align="center">
  <a href="https://pypi.org/project/mqttium/"><img alt="PyPI" src="https://img.shields.io/pypi/v/mqttium.svg"></a>
  <a href="https://pypi.org/project/mqttium/"><img alt="Python versions" src="https://img.shields.io/pypi/pyversions/mqttium.svg"></a>
  <a href="https://github.com/yoch/mqttium/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/yoch/mqttium/actions/workflows/ci.yml/badge.svg"></a>
  <a href="https://codecov.io/gh/yoch/mqttium"><img alt="Coverage" src="https://codecov.io/gh/yoch/mqttium/branch/main/graph/badge.svg"></a>
  <a href="https://mqttium.readthedocs.io/en/stable/"><img alt="Documentation" src="https://readthedocs.org/projects/mqttium/badge/?version=stable"></a>
  <a href="https://github.com/yoch/mqttium/blob/main/LICENSE"><img alt="Apache-2.0 license" src="https://img.shields.io/pypi/l/mqttium.svg"></a>
</p>

MQTTium is a fully typed MQTT client for **Python 3.11–3.14**, with no runtime
dependencies. It runs in the application's asyncio event loop and supports
MQTT 3.1.1 and MQTT 5, QoS 0/1/2, and TCP, TLS, WebSocket, and Unix-domain sockets.
Typical uses include services, gateways, and telemetry collectors.

The API provides publish receipts, configurable message and byte limits,
automatic reconnect, and in-memory or SQLite-backed inflight state. MQTT 5
support includes typed properties, Last Will, and enhanced authentication.

[Documentation](https://mqttium.readthedocs.io/en/stable/) ·
[PyPI](https://pypi.org/project/mqttium/) ·
[Release notes](https://github.com/yoch/mqttium/releases)

## Installation

```bash
python -m pip install mqttium==1.1.0
```

When upgrading from 1.0, review the
[1.1 migration notes](https://mqttium.readthedocs.io/en/v1.1.0/migration/#changes-in-11).
The client API and SQLite schema stay on the 1.0 contract, but 1.1 corrects
connection, subscription, limit, and session behaviours that may require an
application adjustment.

## Quickstart

The example uses `127.0.0.1:1883` for a QoS 1 publish/subscribe exchange.

```python
import asyncio

from mqttium.api import AsyncClient


async def main() -> None:
    client = AsyncClient("mqttium-demo")
    try:
        async with asyncio.timeout(10):
            await client.connect("127.0.0.1", 1883)
            await client.subscribe("mqttium/demo", qos=1)

            receipt = await client.publish("mqttium/demo", b"hello", qos=1)
            await receipt.wait()

            async for message in client.messages():
                print(message.topic, message.payload)
                break
    finally:
        await client.disconnect()


asyncio.run(main())
```

Expected output: `mqttium/demo b'hello'`.

## Publication completion

`await client.publish(...)` waits until the publication is admitted and its
bounded effect transfer is complete; it does not wait for the MQTT exchange to
finish. The returned receipt observes that later completion:

| QoS | Receipt completes when |
| --- | --- |
| 0 | The message is handed to the writer; MQTT defines no acknowledgement |
| 1 | The broker sends PUBACK |
| 2 | The PUBREC/PUBREL/PUBCOMP exchange completes |

For QoS 1 and 2, use `await receipt.wait()` when you need to observe that
completion. A completed receipt does not mean that a subscriber has processed
the message; that requires an application-level response.

## Receiving messages

Iterator delivery is the default and the asynchronous processing path. It is
also the only mode that supports `manual_ack=True`:

```python
client = AsyncClient(manual_ack=True)
# Connect and subscribe before entering the receive loop.

async for message in client.messages():
    await process(message)
    await client.ack(message)
```

For synchronous notification, construct the client with
`message_delivery="callback"` and register `on_message` or topic-specific
callbacks before the first connection attempt. Message callbacks are synchronous
by contract and run inline on the delivering reader, outside protocol locks.
They retain no MQTTium callback queue, so callback execution time is natural
receive-side backpressure. Use `messages()` instead when handling needs to
`await`, may take significant time, or needs manual acknowledgement.

Message routes are frozen after the first connection attempt. Create a new
client when a later connection needs a different routing table.
Only one `messages()` iterator may wait at a time; fan out from that consumer
when several application workers need the stream.

## Backpressure and batch publishing

`publish()` waits for capacity by default. Applications with a defined shed,
retry, or spill policy can request immediate refusal instead:

```python
from mqttium import FlowControlError

# Within the event loop, using an already connected client.

try:
    receipt = client.publish_nowait("telemetry", payload, qos=1)
except FlowControlError:
    await shed_or_retry(payload)
```

Outbound protocol state, encoded writes, inbound protocol state, and iterator
delivery have independent bounds because they have different lifetimes. Passing

For a sustained producer, `publish_many()` walks its input progressively instead
of materialising chunks or creating one task per publication. Admissions remain
ordered and the returned aggregate receipt tracks the committed prefix:

```python
from mqttium.api import PublishMessage

batch = await client.publish_many(
    PublishMessage("telemetry", sample, qos=1) for sample in samples
)
await batch.wait()
```

If iteration or admission fails after earlier elements committed,
`PublishBatchError` exposes the aggregate receipt for that committed prefix;
MQTTium does not roll it back.

## Reconnect and durable sessions

Automatic reconnect is opt-in through `ReconnectPolicy`, with jittered retries.
Durable recovery also requires a durable broker session; storing client-side
inflight state alone is not sufficient.

```python
from mqttium import MQTTProtocolVersion
from mqttium.api import AsyncClient, Properties, ReconnectPolicy
from mqttium.persistence import SqliteInflightStore

store = SqliteInflightStore("mqtt-session.sqlite")
client = AsyncClient(
    "gateway",
    protocol=MQTTProtocolVersion.MQTTv5,
    clean_start=False,
    connect_properties=Properties({"session_expiry_interval": 86_400}),
    reconnect=ReconnectPolicy(max_retries=None),
    store=store,
)
```

`SqliteInflightStore` persists unfinished outbound QoS 1/2 exchanges, inbound
QoS 1 still awaiting a manual `ack()`, inbound QoS 2 protocol state, and the
accounting metadata needed to replay them. It does not persist arbitrary
application work, already-acknowledged messages, or subscription intent. The
application owns the store and must close it after the client has shut down.

## Monitoring and architecture

`client.stats()` returns immutable snapshots of resource usage, counters, queue
high-water marks, connection age, the last disconnect, callback failures, and
unrouted messages. `client.negotiated` exposes broker-negotiated settings and
`client.effective_client_id` includes a broker-assigned identifier. The
[operations guide](https://mqttium.readthedocs.io/en/stable/operations/)
explains pressure diagnosis and graceful shutdown. The library does not emit
logs; applications choose how to expose this information.

MQTTium keeps protocol state in a synchronous state machine and leaves sockets,
timers, callbacks, and task ownership to the asyncio adapter. That separation
makes QoS transitions and rollback independently testable while keeping the
native client free of background threads. See the
[architecture](https://mqttium.readthedocs.io/en/stable/architecture/)
for the implementation design.

## Performance

The separate
[mqtt-python-client-bench](https://github.com/yoch/mqtt-python-client-bench)
repository contains cross-client campaigns with source revisions, environment
details, scenario definitions, validity labels, and raw results. Comparisons
require matching completion semantics; unsupported capabilities are marked
`N/A`. It includes gmqtt as an asyncio peer and Eclipse Paho as a synchronous
reference.

The [benchmarking contract](https://mqttium.readthedocs.io/en/stable/benchmarking/)
covers MQTTium regression measurements, including same-code controls and paired
A/B runs. Measurements retain protocol guarantees, resource bounds, and
backpressure. Throughput and latency depend on the machine, broker, workload,
and completion semantics; consult the validity labels and limitations when
using a result.

## Compatibility and API stability

The native client operations, models, receipts, and results are Stable in the
1.x line. Statistics and the supplied memory/SQLite stores are supported but
Provisional. Internal engine, codec, and extension interfaces have no
compatibility guarantee; see the
[stability policy](https://mqttium.readthedocs.io/en/stable/api-stability/).
MQTTium has its own API and is not a drop-in replacement for Paho.

Version 1.1 adds typed failures for connection setup, refused CONNACKs,
subscriptions, and publication acknowledgements. It also reports
`ConnectionState.RECONNECTING` while an automatic retry is pending. The
[migration guide](https://mqttium.readthedocs.io/en/v1.1.0/migration/#changes-in-11)
lists every behavioural change from 1.0.

The project tests interoperability with Mosquitto, EMQX, and HiveMQ Community
Edition. The [compatibility matrix](https://mqttium.readthedocs.io/en/stable/compatibility/)
details the scope of broker, platform, and transport validation.

## Documentation

The links below use the documentation for the current stable release.
For a source checkout, use [latest](https://mqttium.readthedocs.io/en/latest/);
for an older installation, select its version in Read the Docs. The
[migration guide](https://mqttium.readthedocs.io/en/stable/migration/)
records API and persistence changes, including database compatibility.

| What you want to do | Guide |
| --- | --- |
| Connect, publish, and receive messages | [Getting started](https://mqttium.readthedocs.io/en/stable/getting-started/) |
| Move from Paho or aiomqtt | [Paho and aiomqtt guide](https://mqttium.readthedocs.io/en/stable/paho-compatibility/) |
| Process messages and handle acknowledgements | [Cookbook](https://mqttium.readthedocs.io/en/stable/cookbook/) |
| Set queue limits and timeouts | [Configuration and sizing](https://mqttium.readthedocs.io/en/stable/configuration-and-sizing/) |
| Recover sessions after a disconnect or restart | [Sessions and persistence](https://mqttium.readthedocs.io/en/stable/sessions-and-persistence/) |
| Connect securely or use another transport | [Transports and security](https://mqttium.readthedocs.io/en/stable/transports-and-tls/) |
| Inspect runtime state and shut down a service | [Operations](https://mqttium.readthedocs.io/en/stable/operations/) |
| Interpret performance measurements | [Benchmarking](https://mqttium.readthedocs.io/en/stable/benchmarking/) |
| Use MQTT 5 features | [MQTT 5](https://mqttium.readthedocs.io/en/stable/mqtt-5/) |
| Look up signatures, defaults, and exceptions | [API reference](https://mqttium.readthedocs.io/en/stable/reference/) |
| Upgrade an existing application | [Migration guide](https://mqttium.readthedocs.io/en/stable/migration/) |
| Diagnose a connection or delivery problem | [Troubleshooting](https://mqttium.readthedocs.io/en/stable/troubleshooting/) |

## Support and contributing

For usage questions, start with the
[support guide](https://github.com/yoch/mqttium/blob/main/SUPPORT.md).
Report bugs through the
[issue forms](https://github.com/yoch/mqttium/issues/new/choose), including a
small reproducer and your Python and broker versions. Report security issues
privately using the [security policy](https://github.com/yoch/mqttium/blob/main/SECURITY.md).

Code, documentation, and interoperability reports are welcome. The
[contribution guide](https://github.com/yoch/mqttium/blob/main/CONTRIBUTING.md)
explains development setup and checks.

To install a source checkout:

```bash
git clone https://github.com/yoch/mqttium.git
cd mqttium
python -m pip install .
```

Include the Git commit when reporting an issue with a source checkout.

Licensed under [Apache-2.0](https://github.com/yoch/mqttium/blob/main/LICENSE).
