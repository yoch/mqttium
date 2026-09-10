# Compatibility and validation matrix

This page distinguishes the supported public contract from environments that
the project exercises directly. A listed broker is not a promise about every
edition, plugin, authentication provider, or deployment topology.

## Python and platforms

| Environment | Project status |
| --- | --- |
| CPython 3.11, 3.12, 3.13, 3.14 | Supported |
| Linux | Full unit, Mosquitto integration, packaging, fuzz, and release-gate coverage |
| macOS | Selected version endpoints and lifecycle coverage |
| Windows | Selected version endpoints and lifecycle coverage |
| Linux ARM64 | Dedicated validation and release-gate workflows |
| Free-threaded Python | Not a production guarantee |

The wheel is platform-independent Python. Transport availability still depends
on the operating system; Unix-domain sockets are not a portable Windows API.

## MQTT protocol

Only MQTT 3.1.1 and MQTT 5 are supported and tested.

| Capability | MQTT 3.1.1 | MQTT 5 |
| --- | :---: | :---: |
| CONNECT and clean/new sessions | Yes | Yes |
| QoS 0, 1, and 2 | Yes | Yes |
| Last Will | Yes | Yes |
| Persistent broker session | `clean_start=False` | Clean Start false plus session expiry |
| Typed properties | Not applicable | Yes |
| Enhanced authentication and re-authentication | Not applicable | Yes |
| Negotiated feature and size limits | Limited by protocol | Yes |
| Topic aliases | Not applicable | Explicit, connection-scoped |

Protocol conformance evidence is indexed in [Protocol Conformance](conformance.md).

`MQTTProtocolVersion.MQTTv31` remains a Stable enum member with numeric value
`3` for backwards-compatible imports and persisted configuration parsing. It is
not executable protocol support: selecting it when constructing `AsyncClient`
or `EngineConfig` fails immediately with a clear unsupported-protocol error,
before any transport, store, codec, or protocol session is created. Low-level
decode helpers may still understand v3-shaped packet data; that carries no
support or conformance claim for MQTT 3.1.

## Transports

| Transport | Native `AsyncClient` |
| --- | :---: |
| TCP | Yes |
| TLS | Yes |
| WebSocket | Yes |
| Unix-domain socket | Yes |

## Brokers used by project gates

| Broker | Role in validation |
| --- | --- |
| Eclipse Mosquitto | Routine integration, transport, packaging, and soak tests |
| EMQX | Release interoperability matrix |
| HiveMQ Community Edition | Release interoperability matrix |

MQTT interoperability is defined by protocol behaviour, not a broker brand.
When reporting a broker-specific failure, include the exact product version,
listener configuration, protocol version, transport, authentication method,
and a minimal reproducer.

## API stability

The experiment supports native client operations, models, receipts, results,
statistics and the supplied memory/SQLite stores. The engine, codecs,
transports, records and extension protocols are internal. See
[API Stability](api-stability.md) for exact import paths and tiers.

## What is not claimed

- certification for every broker feature or managed-service configuration;
- a Paho compatibility API;
- process-wide thread safety for `AsyncClient`;
- durable storage of arbitrary application work;
- performance parity across clients with different completion semantics;
- support for invalid MQTT packets or broker quirks that conflict with the
  protocol.
