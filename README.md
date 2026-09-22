<p align="center">
  <img src="https://raw.githubusercontent.com/yoch/mqttium/main/docs/assets/mqttium-logo-900.png" alt="MQTTium logo" width="180">
</p>

<h1 align="center">MQTTium</h1>

<p align="center"><strong>A dependable, dependency-free asyncio MQTT client for Python.</strong></p>

<p align="center">
  <a href="https://pypi.org/project/mqttium/"><img alt="PyPI" src="https://img.shields.io/pypi/v/mqttium.svg"></a>
  <a href="https://pypi.org/project/mqttium/"><img alt="Python versions" src="https://img.shields.io/pypi/pyversions/mqttium.svg"></a>
  <a href="https://github.com/yoch/mqttium/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/yoch/mqttium/actions/workflows/ci.yml/badge.svg"></a>
  <a href="https://codecov.io/gh/yoch/mqttium"><img alt="Coverage" src="https://codecov.io/gh/yoch/mqttium/branch/main/graph/badge.svg"></a>
  <a href="https://mqttium.readthedocs.io/en/latest/"><img alt="Documentation" src="https://readthedocs.org/projects/mqttium/badge/?version=latest"></a>
  <a href="https://github.com/yoch/mqttium/blob/main/LICENSE"><img alt="Apache-2.0 license" src="https://img.shields.io/pypi/l/mqttium.svg"></a>
</p>

MQTTium is an async-native MQTT 3.1.1 and MQTT 5 client for Python 3.11–3.14.
It is designed for services, gateways, and connected devices that need explicit
completion semantics, bounded resource use, and predictable recovery when a
connection or process fails.

The package has no runtime dependencies and is fully typed.

The current pre-v1 native API deliberately differs from `1.0.0rc14`, including
message delivery, publication completion, configuration names, and the SQLite
format. If you are upgrading an existing application or database, read the
[migration guide](https://github.com/yoch/mqttium/blob/main/docs/migration.md)
first.

## Why MQTTium?

| Need | MQTTium provides |
| --- | --- |
| Protocol coverage | MQTT 3.1.1 and MQTT 5, QoS 0/1/2, typed properties, Last Will, and enhanced authentication |
| Explicit completion | Publish receipts that separate local admission from the relevant MQTT acknowledgement exchange |
| Controlled load | Independent message and byte budgets, wait-or-refuse backpressure, bounded ingress, writes, and application delivery |
| Session continuity | Jittered reconnect plus in-memory or SQLite-backed inflight state with incremental replay |
| Delivery choices | Async iteration with optional manual acknowledgement, or synchronous auto-ack callbacks |
| Transports | TCP, TLS, WebSocket, and Unix-domain sockets |
| Operations | Immutable runtime snapshots, queue high-water marks, and broker-negotiated limits |
| Efficient native path | Progressive `publish_many()`, loop-bound `publish_nowait()`, and hot paths measured under the same semantics |

MQTTium keeps protocol state in a synchronous state machine and leaves sockets,
timers, callbacks, and task ownership to the asyncio adapter. That separation
makes QoS transitions and rollback independently testable while keeping the
native client free of background threads.

## Install

The examples below describe the current source API, which has not yet been
published. Install a checkout of this revision to use them:

```bash
git clone https://github.com/yoch/mqttium.git
cd mqttium
python -m pip install .
```

For the last published candidate, use `python -m pip install mqttium==1.0.0rc14`
and its [RC14 documentation](https://mqttium.readthedocs.io/en/v1.0.0rc14/).
The source version string remains RC14 until the separate release cut; record
the Git commit when reporting a source-build issue.

## First round trip

This example subscribes, publishes at QoS 1, waits for PUBACK, and consumes the
message:

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

`await client.publish(...)` waits until the publication is admitted and its
bounded effect transfer is complete; it does not wait for the MQTT exchange to
finish. The returned receipt observes that later completion. QoS 0 completes on
writer handoff because MQTT defines no acknowledgement, QoS 1 on PUBACK, and
QoS 2 on PUBCOMP.

## Choose the delivery model deliberately

Iterator delivery is the default and the asynchronous processing path. It is
also the only mode that supports `manual_ack=True`:

```python
client = AsyncClient(manual_ack=True)

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

## Backpressure is part of the API

`publish()` waits for capacity by default. Applications with a defined shed,
retry, or spill policy can request immediate refusal instead:

```python
from mqttium import FlowControlError
from mqttium.api import AsyncClient

client = AsyncClient()

try:
    receipt = client.publish_nowait("telemetry", payload, qos=1)
except FlowControlError:
    await shed_or_retry(payload)
```

Outbound protocol state, encoded writes, inbound protocol state, and iterator
delivery have independent bounds because they have different lifetimes. Passing
`None` disables an optional bound and should be a deliberate capacity decision.

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

## Performance

Performance is a design constraint, not a separate fast mode. MQTTium measures
the native asyncio path together with MQTT semantics, bounded resource use,
backpressure, and event-loop fairness rather than relaxing those contracts for
a benchmark configuration.

The separate
[`mqtt-python-client-bench`](https://github.com/yoch/mqtt-python-client-bench)
project carries cross-client campaigns with exact source revisions, environment
fingerprints, scenario semantics, validity labels, and raw evidence. MQTTium only
treats cross-client points as comparable when the completion contract matches;
unsupported capabilities remain `N/A` rather than being approximated with a
different operation. `gmqtt` is the closest established asyncio peer for many
native scenarios, while Eclipse Paho is retained as a widely known synchronous
reference rather than presented as a direct asyncio peer.

For MQTTium-to-MQTTium regression work, the
[benchmarking contract](https://github.com/yoch/mqttium/blob/main/docs/benchmarking.md)
requires exact source identity and controlled paired measurements. Small
suspected regressions are checked with same-code controls and interleaved A/B
runs before they justify runtime complexity. Absolute throughput still depends
on the machine, broker, workload, and completion semantics.

## Reconnect and durable sessions

Automatic reconnect is opt-in through `ReconnectPolicy`. Durable recovery also
requires a durable broker session; storing client-side inflight state alone is
not sufficient.

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

## Migration from 1.0.0rc14

The current native API is the only supported client surface. The migration guide
covers removed compatibility interfaces and helpers, the frozen constructor and
statistics vocabulary, progressive batch publication, synchronous message
callbacks, receipt-based publication completion, and the new SQLite schema.
Historical databases are not upgraded automatically.

Until the next release is cut, use versioned RC14 documentation for the PyPI
package and `latest` for the current source API. Read the Docs exposes the
published tag under `v1.0.0rc14`; the legacy `stable` URL redirects there until
a final release provides Read the Docs' automatic stable version.

## Documentation

The current source documentation is available on
[Read the Docs latest](https://mqttium.readthedocs.io/en/latest/).
For a released package, select its version instead.

| Start here | Use it for |
| --- | --- |
| [Getting started](https://mqttium.readthedocs.io/en/latest/getting-started/) | Installation, lifecycle, publishing, subscribing, and delivery |
| [Configuration and sizing](https://mqttium.readthedocs.io/en/latest/configuration-and-sizing/) | Choosing queue, byte, inflight, timeout, and reconnect settings |
| [Sessions and persistence](https://mqttium.readthedocs.io/en/latest/sessions-and-persistence/) | Broker sessions, reconnect, SQLite, and restart recovery |
| [Transports and security](https://mqttium.readthedocs.io/en/latest/transports-and-tls/) | TCP, TLS, WebSocket, Unix sockets, and credential handling |
| [MQTT 5](https://mqttium.readthedocs.io/en/latest/mqtt-5/) | Properties, authentication, topic aliases, and negotiated limits |
| [Operations](https://mqttium.readthedocs.io/en/latest/operations/) | Runtime snapshots, pressure diagnosis, and graceful shutdown |
| [Benchmarking](https://mqttium.readthedocs.io/en/latest/benchmarking/) | Performance methodology, regression controls, and measurement semantics |
| [Native API reference](https://mqttium.readthedocs.io/en/latest/reference/) | Supported imports, signatures, defaults, and exceptions |
| [Compatibility matrix](https://mqttium.readthedocs.io/en/latest/compatibility/) | Python, platform, broker, protocol, and transport validation |

Architecture, conformance, stability tiers, benchmarking methodology, and
release evidence are documented separately so current contracts are not mixed
with historical reports.

## Support and contributing

- Read the [support policy](https://github.com/yoch/mqttium/blob/main/SUPPORT.md) before requesting usage help.
- Use the structured issue form for reproducible bugs.
- Report vulnerabilities privately as described in the [security policy](https://github.com/yoch/mqttium/blob/main/SECURITY.md).
- See the [contribution guide](https://github.com/yoch/mqttium/blob/main/CONTRIBUTING.md) for development and validation commands.

MQTTium is original software licensed under
[Apache-2.0](https://github.com/yoch/mqttium/blob/main/LICENSE). Paho and gmqtt
are referenced only for migration, interoperability, and cross-client comparison.
