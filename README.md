<h1 align="center">
  <img src="https://raw.githubusercontent.com/yoch/mqttium/main/docs/assets/mqttium-logo-900.png" alt="MQTTium" width="220">
</h1>

<p align="center"><strong>A dependable, efficient, async-native MQTT client for Python.</strong></p>

MQTT looks simple until a connection drops halfway through a QoS exchange, a
consumer slows down, or a process restarts with work still in flight. MQTTium
is built for those moments. It keeps protocol state explicit, bounds the work
that can accumulate in memory, and tells the application when a publication
has actually completed.

MQTTium is a good fit for services, gateways and devices where reconnects,
sustained traffic, delivery confirmation or controlled resource use matter. If
an application only sends an occasional QoS 0 message, a smaller client may be
all it needs.

> **Status:** release candidate (`1.0.0rc2`). The native Stable API is frozen
> for 1.0. Provisional extension points may still evolve under their documented
> compatibility policy.

## Why MQTTium?

Reliability is an API property here, not an implementation detail:

- **One owner for protocol state.** Packet identifiers, QoS transitions,
  reconnects and session replay are managed by explicit state machines.
- **Bounded work by default.** Producer, writer and delivery queues have message
  and byte limits. Overload becomes waiting or an explicit error, not runaway
  memory use.
- **Observable completion.** A publish receipt distinguishes local admission
  from broker acknowledgement. Runtime snapshots expose pressure without
  requiring per-message logging.
- **Performance with guardrails.** Optimisations must preserve MQTT semantics,
  backpressure, memory bounds and event-loop fairness, and must be supported by
  reproducible measurements.

## At a glance

| Need | MQTTium provides |
| --- | --- |
| Protocol coverage | MQTT 3.1.1 and MQTT 5, QoS 0/1/2, typed properties, Last Will and enhanced authentication |
| Completion | Per-message and aggregate publish receipts that follow the relevant MQTT acknowledgement exchange |
| Controlled load | Message and byte budgets, wait-or-refuse backpressure, bounded ingress and delivery |
| Session continuity | Jittered reconnect policy plus in-memory or SQLite-backed inflight state with incremental replay |
| Application delivery | Async iterator, sync or async callbacks, optional dual delivery and manual acknowledgement |
| Transports | TCP, TLS, WebSocket and Unix-domain sockets |
| Operations | Immutable statistics, queue high-water marks and broker-negotiated limits without a background sampler |
| Throughput-oriented APIs | Bounded `publish_many()`, loop-bound `publish_nowait()` and one-shot helpers |
| Migration | A Paho-shaped `CallbackAPIVersion.VERSION2` facade with bounded cross-thread producers |
| Packaging | A typed Python 3.11–3.14 package with no runtime dependencies |

## Install

```bash
python -m pip install mqttium
```

MQTTium is licensed under Apache-2.0. To evaluate the current release-candidate
source tree directly:

```bash
git clone https://github.com/yoch/mqttium.git
cd mqttium
python -m pip install -e .
```

## First round trip

The example below subscribes, publishes with QoS 1, waits for the broker's
PUBACK and consumes the message:

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
exchange. See the [Getting Started guide](docs/GETTING-STARTED.md) for callback
delivery, errors and lifecycle details, or run
[`examples/pubsub.py`](examples/pubsub.py) against a local broker.

## Publishing under load

`publish()` waits for capacity by default. Applications that need to shed load
can request immediate refusal:

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

For sustained producers, `publish_many()` consumes an iterable in bounded
chunks and uses one aggregate receipt:

```python
from mqttium.api import PublishMessage

batch = await client.publish_many(
    PublishMessage("telemetry", sample, qos=1) for sample in samples
)
await batch.wait()
```

`publish_nowait()` is the synchronous, non-suspending option for code already
running on the client's event-loop thread. It raises `FlowControlError` when a
protocol or writer budget is full. All three entry points share the same
admission and completion rules; choosing one changes waiting and batching, not
delivery semantics.

## Receiving and acknowledging messages

Use the async iterator when processing naturally belongs in a task:

```python
async for message in client.messages():
    await handle(message.topic, message.payload)
```

Or assign a synchronous or asynchronous callback:

```python
async def on_message(message) -> None:
    await handle(message.topic, message.payload)


client.on_message = on_message
```

Delivery mode, callback queue, iterator queue and retained payload bytes are
bounded independently. Slow application code therefore creates visible
backpressure instead of silently consuming the process's memory. Applications
that need control of inbound QoS acknowledgement timing can set
`manual_ack=True` and call `await client.ack(message)` after processing.

## Surviving disconnects and process restarts

Automatic reconnect is opt-in. `ReconnectPolicy` uses jittered exponential
backoff and stops retrying terminal authentication, authorisation and protocol
errors.

For MQTT 5, a durable broker session and durable client inflight state are two
separate pieces. Configure both explicitly:

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
    reconnect=ReconnectPolicy(
        initial_delay=0.5,
        max_delay=30.0,
        max_retries=None,
    ),
    store=store,
)

try:
    await client.connect("broker.example", 1883)
    # Publish and subscribe normally.
finally:
    await client.disconnect()
    store.close()
```

With MQTT 3.1.1, `clean_start=False` requests the corresponding persistent
broker session; there is no MQTT 5 session-expiry property.

`SqliteInflightStore` persists unfinished outbound QoS 1/2 exchanges and
inbound QoS 2 protocol state. It does not persist arbitrary application work,
delivered iterator/callback queues or subscription intent on its own. The
broker must also retain the MQTT session for an interrupted exchange to resume.
The application owns and closes the synchronous store. Its schema is versioned,
and replay is paged so reopening a large session does not require loading every
payload at once.

The [Sessions and Persistence guide](docs/SESSIONS-AND-PERSISTENCE.md) covers
restart behaviour, clean-session rules, sizing and recovery boundaries. A
runnable configuration is available in
[`examples/durable_session.py`](examples/durable_session.py).

## Moving from Paho without a rewrite

Existing synchronous Paho VERSION2 applications can adopt MQTTium one boundary
at a time. Start with the compatibility facade and move new or performance-
sensitive code to `AsyncClient` when convenient:

```python
from mqttium.compat.paho import CallbackAPIVersion, Client

client = Client(CallbackAPIVersion.VERSION2, client_id="legacy-service")
client.loop_start()
try:
    client.connect("127.0.0.1", 1883)
    info = client.publish("events", b"ready", qos=1)
    info.wait_for_publish(timeout=5)
finally:
    client.disconnect()
    client.loop_stop()
```

The facade supports the common VERSION2 lifecycle, publish/subscribe methods,
filtered message callbacks, credentials, wills, queue controls and producers
running outside the network thread. Its cross-thread handoff is bounded by
message and byte limits.

It is intentionally not a bug-for-bug clone. VERSION1 callbacks, blocking
re-entry from the network callback thread and behaviours that conflict with
MQTT correctness or bounded resources are rejected explicitly. Consult the
[migration guide](docs/MIGRATION.md) for the staged path and the
[compatibility matrix](docs/COMPAT.md) for the exact supported surface. The
complete callback lifecycle is runnable as
[`examples/paho_compat.py`](examples/paho_compat.py).

## Transports and MQTT 5

TCP is the default. TLS uses a normal Python `SSLContext`; WebSocket and
Unix-domain connections have explicit entry points:

```python
await client.connect("broker.example", 8883, ssl=tls_context)
await client.connect_ws("wss://broker.example/mqtt", ssl=tls_context)
await client.connect_unix("/run/mosquitto/mosquitto.sock")
```

MQTT 5 properties remain explicit. MQTTium supports CONNECT, publish,
subscription, Will and AUTH properties, enhanced-authentication handlers and
client-initiated re-authentication. It exposes the broker's negotiated Receive
Maximum, maximum packet size, maximum QoS, feature availability, keepalive and
assigned client identifier through `client.negotiated`. Unsupported broker
capabilities are rejected rather than silently downgraded. Topic aliases are
deliberately explicit and reset with each network connection.

## Observing a running client

`client.stats()` returns an immutable point-in-time tree maintained by the
components that own each queue or protocol state:

```python
snapshot = client.stats()

print(snapshot.state)
print(snapshot.outbound.pending_messages, snapshot.outbound.pending_bytes)
print(snapshot.writer.queued_bytes, snapshot.delivery.pending_bytes)
print(client.negotiated.receive_maximum, client.effective_client_id)
```

The snapshot includes tasks, protocol flow, packet identifiers, ingress,
effects, writer pressure, delivery queues, receipts and transport buffers.
Calling it starts no sampler and emits no logs. MQTTium leaves logging, metrics,
sampling and redaction policy to the application. See the
[Operations guide](docs/OPERATIONS.md) for sizing and diagnosis, and
[`examples/runtime_stats.py`](examples/runtime_stats.py) for a JSON snapshot.

## Efficiency under sustained load

Performance is a design constraint, not a promise to bypass protocol work.
MQTTium batches work that is already ready, bounds each event-loop turn and
avoids background threads in the native client. Batch publishing avoids a task
or completion object per message where possible, while optional features stay
off inactive hot paths. The package itself has no runtime dependencies.

Performance changes are accepted only with reproducible local evidence and
guardrails for memory, latency, fairness and non-targeted workloads. The
[benchmarking contract](docs/BENCHMARKING.md) explains how measurements are
made; the [RC performance report](docs/reports/PERFORMANCE-1.0.0rc1.md) records
current decisions. A user-facing benchmark section will follow when the
cross-client results are stable enough to publish without overclaiming.

## Stability, documentation and trust

The public contract is divided into Stable, Provisional and Internal surfaces.
The exact boundary and default-value commitments are documented in the
[API stability policy](docs/API-STABILITY.md).

Protocol behaviour is checked with unit, integration, property, fuzz, memory
and multi-broker tests. Wheel and source distributions are installed in
isolated environments and exercised over TCP, TLS, WebSocket and Unix sockets,
including SQLite restart, Paho VERSION2 and clean shutdown paths.

Continue with the [documentation home](docs/README.md), which separates current
user guides, technical contracts, maintainer evidence and planned material.
Contributions and reproducible bug reports are welcome; see
[CONTRIBUTING.md](CONTRIBUTING.md). Report security issues through the private
process in [SECURITY.md](SECURITY.md).
