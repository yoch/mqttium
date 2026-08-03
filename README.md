# MQTTium

**MQTTium is a reliable, async-native MQTT client for modern Python.**

It combines a synchronous protocol engine with an `asyncio` API, bounded
backpressure, durable inflight persistence, explicit delivery receipts, and
complete MQTT QoS state machines.

> **Status:** alpha (`0.1.0a1`). The implementation is extensively tested, but
> the public API may still change before the first stable release.

## Features

- MQTT 3.1.1 and MQTT 5;
- QoS 0, 1 and 2 with one authoritative protocol state machine;
- TCP, TLS, WebSocket and Unix transports;
- reconnect and session replay;
- bounded callback and async-iterator delivery;
- manual acknowledgement;
- in-memory and SQLite inflight persistence;
- aggregate `publish_many()` with bounded memory and measured throughput gains;
- an additive Paho VERSION2 compatibility façade, isolated from the native API;
- inline type information for type checkers.

Python **3.11–3.14** is supported. MQTTium is licensed under **Apache-2.0**.

## Installation

```bash
python -m pip install mqttium
```

To work on the unreleased development version:

```bash
git clone https://github.com/yoch/mqttium.git
cd mqttium
python -m pip install -e ".[dev,fuzz,release]"
```

## Quick start

```python
import asyncio

from mqttium.api import AsyncClient


async def main() -> None:
    client = AsyncClient("demo-client")
    await client.connect("127.0.0.1", 1883)
    await client.subscribe("demo/#")

    receipt = await client.publish("demo/hello", b"world", qos=1)
    await receipt.wait()

    await client.disconnect()


asyncio.run(main())
```

## Batched publishing

`publish_many()` consumes iterables in bounded chunks and returns one aggregate
receipt rather than creating one task or event per message:

```python
from mqttium.api import PublishMessage

batch = await client.publish_many(
    PublishMessage("telemetry/device-1", payload, qos=1)
    for payload in payloads
)
await batch.wait()
```

The retained paired A/B benchmark measured publisher-throughput geomean
improvements of **36.9% for QoS 0**, **15.9% for QoS 1**, and **7.0% for QoS 2**
against equivalent individual publishing pipelines on the validated source
tree. Benchmark methodology and limitations are documented in
[`docs/BENCHMARKING.md`](docs/BENCHMARKING.md).

## Validation

The release gates include:

- more than 240 unit tests;
- Mosquitto integration tests on Python 3.11, 3.12, 3.13 and 3.14;
- deterministic and Hypothesis-based fuzzing;
- Ruff formatting and linting;
- mypy validation and a PEP 561 `py.typed` marker;
- an 80% coverage gate;
- wheel, source-distribution and isolated-install validation;
- delivery, persistence, TCP, TLS and WAN-profile benchmarks.

## Documentation

- [`docs/DESIGN.md`](docs/DESIGN.md) — architecture and invariants
- [`docs/IMPLEMENTATION-GUIDE.md`](docs/IMPLEMENTATION-GUIDE.md) — protocol contracts
- [`docs/COMPAT.md`](docs/COMPAT.md) — Paho compatibility surface
- [`docs/MIGRATION.md`](docs/MIGRATION.md) — migration guidance
- [`docs/BENCHMARKING.md`](docs/BENCHMARKING.md) — benchmark validity contract
- [`docs/FUZZING.md`](docs/FUZZING.md) — fuzzing strategy
- [`docs/ROADMAP.md`](docs/ROADMAP.md) — remaining stable-release work
- [`PROVENANCE.md`](PROVENANCE.md) — source history and licensing review

## Contributing and security

See [`CONTRIBUTING.md`](CONTRIBUTING.md). Report vulnerabilities through the
private process described in [`SECURITY.md`](SECURITY.md).
