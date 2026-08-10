# MQTTium

**MQTTium is a reliable, async-native MQTT client for modern Python.**

It combines a synchronous protocol engine with an `asyncio` API, bounded
backpressure, durable inflight persistence, explicit delivery receipts, and
complete MQTT QoS state machines.

> **Status:** beta (`0.2.0b3`). The native Stable API tier follows the documented
> compatibility policy; Provisional extension surfaces may still evolve before
> the first stable release.

## Features

- MQTT 3.1.1 and MQTT 5;
- QoS 0, 1 and 2 with one authoritative protocol state machine;
- TCP, TLS, WebSocket and Unix transports;
- reconnect and session replay;
- bounded callback and async-iterator delivery;
- immutable runtime statistics for queues, budgets, receipts and transports;
- manual acknowledgement;
- in-memory and SQLite inflight persistence;
- aggregate `publish_many()` with bounded memory and measured throughput gains;
- synchronous loop-bound `publish_nowait()` for non-suspending native producers;
- an additive Paho VERSION2 compatibility façade, isolated from the native API;
- inline type information for type checkers.

Python **3.11–3.14** is supported. MQTTium is licensed under **Apache-2.0**.

## API stability

The native client is imported from `mqttium.api`; protocol enums and operational
exceptions are imported from `mqttium`. Advanced protocol, persistence,
transport, packet and Paho surfaces are classified separately as Provisional.
See [`docs/API-STABILITY.md`](docs/API-STABILITY.md) for the exact Stable,
Provisional and Internal boundaries.

## Installation

Install the current beta from PyPI:

```bash
python -m pip install --pre mqttium
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

## Non-suspending publishing

A producer already executing on the client's event loop can submit without
creating or awaiting a coroutine:

```python
receipt = client.publish_nowait("telemetry/device-1", payload, qos=1)
# Continue synchronously, then observe completion later if needed.
await receipt.wait()
```

`publish_nowait()` either admits the publication immediately or raises
`FlowControlError`; it never waits for engine or writer capacity. It returns the
normal `PublishReceipt`, so QoS 1/2 completion is observed in the same way as
with `publish()`.

The method follows the same ownership rule as `asyncio.Queue.put_nowait()`: it
is intended for the client's owning event-loop thread, not as a generic
thread-safe API. Cross-thread synchronous callers should use an adapter such as
`mqttium.compat.paho.Client`, which coalesces submissions before handing a
bounded batch to the loop.

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

## Bounded memory

Every queue that can grow with application load is bounded by default, so a
producer that outruns its broker is slowed down rather than allowed to exhaust
the process:

```python
client = AsyncClient(
    max_pending_outbound_messages=10_000,   # unfinished QoS 1/2 publications
    max_pending_outbound_bytes=64 * 1024**2,  # their logical topic+payload+properties
    max_pending_inbound_bytes=64 * 1024**2,   # persisted inbound QoS handshakes
    max_pending_delivery_bytes=64 * 1024**2,  # inbound messages awaiting a consumer
    max_ingress_batch_bytes=1 * 1024**2,      # decoded work before delivery is drained
    publish_backpressure="wait",             # or "error" to refuse immediately
)
```

`publish()` waits for capacity by default and raises `FlowControlError` under
`publish_backpressure="error"` or with `nowait=True`. A refusal is atomic: no
packet identifier is allocated and no store record is written. Pass `None` for
any limit to restore unbounded queueing.

The two inbound byte limits cover different lifetimes:
`max_pending_inbound_bytes` bounds QoS 2 and manually acknowledged QoS 1 data
retained by the session store, while `max_pending_delivery_bytes` bounds data
waiting for callback or iterator consumers. MQTT 5 reports a persisted-session
quota violation with reason `0x97`; MQTT 3.1.1 closes the connection.

The writer also has an independent `max_outbound_bytes=1 MiB` default.
`publish_nowait()` refuses immediately when that wire queue is full, so callers
publishing large payloads should size this byte limit explicitly rather than
assuming `max_outbound_messages` is the only binding constraint. The two
defaults imply about 105 bytes per queued message, so the byte bound is the one
that binds first as payloads grow: 1 MiB admits roughly 16 outstanding 64 KiB
publications, not 10 000. `FlowControlError` names whichever bound refused.

Native QoS 0 uses its direct writer fast path only while `on_publish is None`.
Installing that callback deliberately routes publications through the standard
engine and `EffectPump` so completion callbacks retain their ordering semantics.

These defaults are new in `0.1.0a2`; before them a QoS 1/2 producer could queue
until the 65 535 packet-identifier space was exhausted. See
[`docs/MIGRATION.md`](docs/MIGRATION.md).

## Runtime statistics

`stats()` returns a frozen snapshot without starting a sampler or emitting logs:

```python
snapshot = client.stats()
print(snapshot.outbound.pending_bytes)
print(snapshot.inbound.inflight)
print(snapshot.writer.queued_bytes)
print(snapshot.delivery.pending_bytes)
```

Each section is produced by the component that owns the state — the two protocol
sessions, the effect and write pumps, the transport — and `stats()` only
assembles them. The snapshot also includes lifetime high-water marks, batching
decision counters, task state, receipt counts, decoder buffering and
WebSocket/stream transport buffers. It is intended to be called on the client's
owning event loop. See [`docs/API-STABILITY.md`](docs/API-STABILITY.md).

## Validation

The release gates include:

- more than 500 unit tests;
- Mosquitto integration tests on Python 3.11, 3.12, 3.13 and 3.14;
- deterministic and Hypothesis-based fuzzing;
- Ruff formatting and linting;
- mypy validation and a PEP 561 `py.typed` marker;
- an 80% coverage gate;
- wheel, source-distribution and isolated-install validation;
- delivery, persistence, TCP, TLS and WAN-profile benchmarks.

A separate finalisation workflow runs short reconnect/backpressure soaks on
Linux for relevant pull requests. Extended Linux/macOS soaks and interoperability
campaigns against multiple brokers are available by manual dispatch. Their
acceptance criteria are documented in [`docs/STABILITY.md`](docs/STABILITY.md).

## Documentation

[`docs/README.md`](docs/README.md) indexes everything. The documents most
readers want first:

- [`docs/DESIGN.md`](docs/DESIGN.md) — architecture and invariants
- [`docs/API-STABILITY.md`](docs/API-STABILITY.md) — public API policy and deprecations
- [`docs/IMPLEMENTATION-GUIDE.md`](docs/IMPLEMENTATION-GUIDE.md) — protocol contracts
- [`docs/COMPAT.md`](docs/COMPAT.md) — Paho compatibility surface
- [`docs/MIGRATION.md`](docs/MIGRATION.md) — migration guidance
- [`docs/BETA-REPORTING.md`](docs/BETA-REPORTING.md) — reporting a beta issue
- [`docs/STABILITY.md`](docs/STABILITY.md) — soak and interoperability campaign
- [`docs/BENCHMARKING.md`](docs/BENCHMARKING.md) — benchmark validity contract
- [`docs/FUZZING.md`](docs/FUZZING.md) — fuzzing strategy
- [`docs/RELEASING.md`](docs/RELEASING.md) — rehearsal, publication and failure handling
- [`docs/ROADMAP.md`](docs/ROADMAP.md) — remaining stable-release work
- [`PROVENANCE.md`](PROVENANCE.md) — source history and licensing review

[`docs/reports/`](docs/reports/README.md) holds the dated measurements and
audits behind those choices. They are historical records, not descriptions of
current behaviour.

## Contributing and security

See [`CONTRIBUTING.md`](CONTRIBUTING.md). Report vulnerabilities through the
private process described in [`SECURITY.md`](SECURITY.md).
