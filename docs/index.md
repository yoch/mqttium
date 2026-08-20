# MQTTium documentation

<p align="center">
  <img src="assets/mqttium-logo.svg" alt="MQTTium logo" width="180">
</p>

MQTTium is a dependency-free, async-native MQTT 3.1.1 and MQTT 5 client for
Python 3.11–3.14. It is built for production services, gateways, and connected
devices that need explicit completion, bounded resource use, and reliable
recovery.

## Choose a path

<div class="grid cards" markdown>

-   :material-rocket-launch-outline: **Build a native client**

    ---

    Install MQTTium and complete a publish/subscribe round trip with
    [`AsyncClient`](getting-started.md).

-   :material-tune-variant: **Prepare for production load**

    ---

    Choose explicit limits in [Configuration and Sizing](configuration-and-sizing.md), then
    use [Operations and Observability](operations.md) to diagnose pressure.

-   :material-database-sync-outline: **Recover across failures**

    ---

    Combine broker session retention, reconnect, and optional SQLite inflight
    state in [Sessions and Persistence](sessions-and-persistence.md).

-   :material-swap-horizontal: **Migrate an existing client**

    ---

    Move from Paho or gmqtt with the [migration guide](migration.md). The Paho
    VERSION2 facade is a Provisional transition surface, not the native API.

</div>

## What the native client provides

| Area | Behaviour |
| --- | --- |
| Protocol | MQTT 3.1.1 and MQTT 5; QoS 0, 1, and 2 |
| Completion | Per-message and aggregate receipts tied to MQTT semantics |
| Flow control | Independent message and byte bounds for protocol, writer, ingress, and delivery state |
| Recovery | Jittered reconnect, broker-session resumption, and optional persistent inflight state |
| Delivery | Async iterator, synchronous or asynchronous callbacks, and manual acknowledgement |
| Transports | TCP, TLS, WebSocket, and Unix-domain sockets |
| Operations | Immutable snapshots and broker-negotiated limits without a background sampler |
| Packaging | Typed Python package with no runtime dependencies |

Start with [Core Concepts](core-concepts.md) for the completion, ownership, and
backpressure model that distinguishes MQTTium from a minimal MQTT wrapper.

## Documentation map

- **Guides** solve application tasks: configuration, sessions, transports,
  MQTT 5, operations, troubleshooting, and complete recipes.
- **Reference** records the Stable public imports, signatures, defaults,
  exceptions, compatibility tiers, and validated environments.
- **Concepts** explain architecture, protocol ownership, conformance, and the
  no-library-logging decision.
- **Maintainer material** defines benchmarking, fuzzing, stability, and release
  evidence requirements.
- **Historical evidence** preserves dated reports without presenting their old
  measurements or conclusions as current behaviour.

## API support tiers

The native entry points in `mqttium`, `mqttium.api`, and `mqttium.helpers` are
the Stable public contract. Persistence, transports, protocol integrations,
diagnostic snapshots, and Paho compatibility are Provisional. Implementation
modules and underscore-prefixed objects are Internal.

See the [API Stability Policy](api-stability.md) before depending on an advanced
integration surface.

## Trust and evidence

Protocol behaviour is checked with unit, integration, property, fuzz, memory,
and broker-interoperability tests. Packaging checks install wheel and source
artifacts in isolation. Performance claims require the reproducible controls in
the [Benchmarking Contract](benchmarking.md).

Current contracts live in the main documentation. Dated audits and campaign
records live in the [Historical Evidence index](reports/README.md); report
bodies are immutable records of the commit they name.
