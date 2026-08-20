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

## Transports

| Transport | Native `AsyncClient` | Paho facade |
| --- | :---: | :---: |
| TCP | Yes | Yes |
| TLS | Yes | Yes |
| WebSocket | Yes | No |
| Unix-domain socket | Yes | No |

The Paho column describes only the documented Provisional VERSION2 subset. It
does not imply general Paho API parity.

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

- **Stable:** canonical native imports in `mqttium`, `mqttium.api`, and
  `mqttium.helpers`.
- **Provisional:** diagnostics, persistence, transports, advanced protocol
  integrations, packet/codec helpers, and Paho compatibility.
- **Internal:** underscore modules, directional sessions, pumps, records, and
  effects.

Only the canonical path receives the stated tier. For example,
`mqttium.api.ReconnectPolicy` is Stable while direct advanced protocol imports
remain Provisional. See [API Stability](api-stability.md).

## What is not claimed

- certification for every broker feature or managed-service configuration;
- compatibility with Paho VERSION1 callbacks or every Paho attribute;
- process-wide thread safety for `AsyncClient`;
- durable storage of arbitrary application work;
- performance parity across clients with different completion semantics;
- support for invalid MQTT packets or broker quirks that conflict with the
  protocol.
