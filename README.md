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
  <a href="https://mqttium.readthedocs.io/en/stable/"><img alt="Documentation" src="https://readthedocs.org/projects/mqttium/badge/?version=stable"></a>
  <a href="https://github.com/yoch/mqttium/blob/main/LICENSE"><img alt="Apache-2.0 license" src="https://img.shields.io/pypi/l/mqttium.svg"></a>
</p>

MQTTium is an async-native MQTT 3.1.1 and MQTT 5 client for Python 3.11–3.14.
It is designed for services, gateways, and connected devices that need explicit
delivery semantics, bounded resource use, and predictable recovery when a
connection or process fails.

The package has no runtime dependencies and is fully typed.

## Why MQTTium?

| Need | MQTTium provides |
| --- | --- |
| Protocol coverage | MQTT 3.1.1 and MQTT 5, QoS 0/1/2, typed properties, Last Will, and enhanced authentication |
| Explicit completion | Publish receipts that separate local admission from the relevant MQTT acknowledgement exchange |
| Controlled load | Message and byte budgets, wait-or-refuse backpressure, bounded ingress, writes, and application delivery |
| Session continuity | Jittered reconnect plus in-memory or SQLite-backed inflight state with incremental replay |
| Delivery choices | Async iteration, sync or async callbacks, optional dual delivery, and manual acknowledgement |
| Transports | TCP, TLS, WebSocket, and Unix-domain sockets |
| Operations | Immutable runtime snapshots, queue high-water marks, and broker-negotiated limits |
| Efficient native path | Bounded `publish_many()`, loop-bound `publish_nowait()`, and measured hot-path optimisation without changing delivery semantics |

MQTTium keeps protocol state in a synchronous state machine and leaves sockets,
timers, callbacks, and task ownership to the asyncio adapter. That separation
makes QoS transitions and rollback independently testable while keeping the
native client free of background threads.

## Install

```bash
python -m pip install mqttium
```

## First round trip

The example subscribes, publishes at QoS 1, waits for PUBACK, and consumes the
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

For QoS 0, a receipt completes after writer admission because MQTT provides no
broker acknowledgement. QoS 1 completes on PUBACK; QoS 2 completes on PUBCOMP.
Waiting for `publish()` and waiting for `receipt.wait()` therefore answer
different questions.

## Backpressure is part of the API

`publish()` waits for capacity by default. Applications that have a defined
shed, retry, or spill policy can request immediate refusal:

```python
from mqttium import FlowControlError
from mqttium.api import AsyncClient

client = AsyncClient(publish_backpressure="error")

try:
    receipt = await client.publish("telemetry", payload, qos=1)
except FlowControlError:
    await shed_or_retry(payload)
```

Outbound protocol state, encoded writes, inbound protocol state, and delivery
queues have independent bounds because they have different lifetimes. Passing
`None` disables an optional bound and should be a deliberate capacity decision.

For a sustained producer, `publish_many()` consumes an iterable in bounded
chunks and returns one aggregate receipt:

```python
from mqttium.api import PublishMessage

batch = await client.publish_many(
    PublishMessage("telemetry", sample, qos=1) for sample in samples
)
await batch.wait()
```

## Performance

Performance is a design constraint, not a separate fast mode. MQTTium's native
asyncio path is measured together with MQTT semantics, bounded resource use,
backpressure, and event-loop fairness.

Current same-host standard-profile validation of `1.0.0rc13` against the
post-#422 baseline shows the intended publish-path improvements without a
material regression in the QoS 0 or RTT guardrails:

| Workload | `1.0.0rc13` vs post-#422 baseline |
| --- | ---: |
| Publish QoS 0/1/2 sweep | **+9.4%** |
| QoS 1 inflight capacity | **+4.5%** |
| QoS 0 payload throughput | ≈ flat |
| Application RTT capacity | ≈ flat |

These are same-host version-regression results, not cross-client ranking claims.
Absolute throughput depends on the machine, broker, workload, and completion
contract. Cross-client comparisons live in the separate
[`mqtt-python-client-bench`](https://github.com/yoch/mqtt-python-client-bench)
project; its [live report](https://yoch.github.io/mqtt-python-client-bench/)
publishes exact versions, environment details, scenario semantics, and stated
limitations. Release comparisons use interleaved runs so client order does not
silently become part of the result.

MQTTium's own
[benchmarking contract](https://mqttium.readthedocs.io/en/stable/benchmarking/)
defines the paired regression gates used during development.

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

`SqliteInflightStore` persists unfinished outbound QoS 1/2 exchanges and
inbound QoS 2 protocol state. It does not persist arbitrary application work,
delivered callback/iterator queues, or subscription intent. The application
owns the store and must close it after the client has shut down.

## Paho migration

New async applications should use `AsyncClient`. MQTTium also ships a
**Provisional**, Paho-shaped `CallbackAPIVersion.VERSION2` facade for existing
synchronous applications that need an incremental migration path. It is tested
and bounded, but it is not a drop-in promise, a performance-parity promise, or
a second native API. See [Migrating from Paho](https://mqttium.readthedocs.io/en/stable/migration/)
and the [exact compatibility matrix](https://mqttium.readthedocs.io/en/stable/paho-compatibility/).

## Documentation

The complete documentation is available on
[Read the Docs](https://mqttium.readthedocs.io/en/stable/).

| Start here | Use it for |
| --- | --- |
| [Getting started](https://mqttium.readthedocs.io/en/stable/getting-started/) | Installation, lifecycle, publishing, subscribing, and delivery |
| [Configuration and sizing](https://mqttium.readthedocs.io/en/stable/configuration-and-sizing/) | Choosing queue, byte, inflight, timeout, and reconnect settings |
| [Sessions and persistence](https://mqttium.readthedocs.io/en/stable/sessions-and-persistence/) | Broker sessions, reconnect, SQLite, and restart recovery |
| [Transports and security](https://mqttium.readthedocs.io/en/stable/transports-and-tls/) | TCP, TLS, WebSocket, Unix sockets, and credential handling |
| [MQTT 5](https://mqttium.readthedocs.io/en/stable/mqtt-5/) | Properties, authentication, topic aliases, and negotiated limits |
| [Operations](https://mqttium.readthedocs.io/en/stable/operations/) | Runtime snapshots, pressure diagnosis, and graceful shutdown |
| [Benchmarking](https://mqttium.readthedocs.io/en/stable/benchmarking/) | Performance methodology, regression gates, and measurement semantics |
| [Stable API reference](https://mqttium.readthedocs.io/en/stable/reference/) | Supported imports, signatures, defaults, and exceptions |
| [Compatibility matrix](https://mqttium.readthedocs.io/en/stable/compatibility/) | Python, platform, broker, protocol, and transport validation |

Architecture, conformance, stability tiers, benchmarking methodology, and
release evidence are documented separately so current contracts are not mixed
with historical reports.

## Support and contributing

- Read the [support policy](https://github.com/yoch/mqttium/blob/main/SUPPORT.md) before requesting usage help.
- Use the structured issue form for reproducible bugs.
- Report vulnerabilities privately as described in the [security policy](https://github.com/yoch/mqttium/blob/main/SECURITY.md).
- See the [contribution guide](https://github.com/yoch/mqttium/blob/main/CONTRIBUTING.md) for development and validation commands.

MQTTium is original software licensed under [Apache-2.0](https://github.com/yoch/mqttium/blob/main/LICENSE). Paho and
gmqtt are referenced only for migration, interoperability, and independent
comparison.
