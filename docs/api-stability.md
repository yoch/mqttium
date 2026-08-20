# Public API stability policy

This document defines MQTTium's Stable public API and separates that contract
from Provisional and Internal objects that remain importable in Python.

## Support tiers

Python importability and `__all__` are not stability promises. `__all__` controls
wildcard-import ergonomics only. Support is determined by the tables below.

### Stable

Stable names are the native user-facing contract. Incompatible changes to these
names follow SemVer and the deprecation policy below.

| Entry point | Supported names |
| --- | --- |
| `mqttium` | `MQTTError`, `MalformedPacketError`, `ProtocolError`, `PacketTooLargeError`, `FlowControlError`, `MessageDeliveryError`, `NotConnectedError`, `MQTTTimeoutError`, `SessionDiscardedError`, `PublishBatchError`, `MQTTProtocolVersion`, `QoS`, `ConnectionState`, `__version__` |
| `mqttium.api` | `AsyncClient`, `Message`, `Properties`, `PublishMessage`, `PublishReceipt`, `PublishBatchReceipt`, `SubscribeResult`, `UnsubscribeResult`, `SubscribeOptions`, `ConnAckPacket`, `AuthPacket`, `NegotiatedSettings`, `ReconnectPolicy`, `MessageDelivery`, `PublishBackpressure` |
| `mqttium.helpers` | `publish`, `subscribe` |

`PacketType` remains available from `mqttium` for compatibility with the alpha
series, but it is a low-level provisional enum rather than part of the stable
native-client contract.

### Provisional

Provisional APIs are supported and tested, but may gain fields or be revised in
a future minor release with a changelog entry and migration guidance:

- `ClientStats` and its nested immutable snapshot dataclasses;
- `mqttium.compat` and the documented Paho VERSION2 subset;
- `mqttium.persistence` store protocols and implementations;
- `mqttium.transport` transport protocols and concrete transports;
- `mqttium.protocol.ProtocolEngine`, `EngineConfig`, `NegotiatedSettings`,
  `ReconnectPolicy`, `FlowControl`, `PublishHandle`, `PublishFailure` and
  `DisconnectInfo` for advanced integrations;
- `mqttium.packets` typed packet views;
- `mqttium.codec` framing and codec helpers.

A Provisional designation is not permission for silent breakage. An incompatible
change still requires a changelog entry and migration guidance.

### Internal

Internal objects have no compatibility guarantee even when an implementation
module makes them importable:

- `InboundSession` and `OutboundSession`;
- `EffectPump` and `WritePump`;
- delivery reservations, callback jobs and queue item wrappers;
- persistence records and transition helpers not exported by
  `mqttium.persistence`;
- `EngineEffect`, `EffectKind`, `PacketIdPool`, `WriteItem`, `item_size` and
  batching/segmentation constants;
- any name beginning with `_`;
- direct imports from implementation modules such as `mqttium.api._writer` or
  `mqttium.transport.writes`.

These names may change when correctness, memory bounds or measured performance
requires it. Their presence in a module namespace or historical `__all__` does
not make them supported.

## Native async client contract

The stable `AsyncClient` surface is:

- lifecycle: `connect`, `connect_unix`, `connect_ws`, `disconnect`;
- publication: `publish`, `publish_nowait`, `publish_many`;
- subscriptions: `subscribe`, `unsubscribe`;
- inbound delivery: `messages`, `ack`;
- MQTT 5 authentication: `auth`, `set_auth_handler`;
- state: `state`, `is_connected`, `negotiated`, `effective_client_id`;
- diagnostics: `stats` and the immutable `ClientStats` tree;
- callbacks: `on_connect`, `on_disconnect`, `on_message`, `on_publish` and
  `auth_handler`.

`publish_nowait()` and `stats()` are synchronous but loop-confined. They are not
cross-thread APIs. Threaded migration code should use
`mqttium.compat.paho.Client`.

Constructor keyword arguments are part of the native contract. New optional
keywords may be added compatibly. Existing Stable defaults will not change
without the SemVer and deprecation process below.

`EngineConfig.local_receive_maximum` intentionally defaults to `65535`, while
`AsyncClient.local_receive_maximum` defaults to `100`. The engine default is the
protocol maximum for advanced direct-engine consumers; the client default is a
bounded application-facing inbound concurrency window. Aligning them would
silently change memory and backpressure behaviour, so both defaults are part of
the supported contract.

`tests/project/test_public_api_surface.py` is the executable contract for the
canonical Stable exports, the retained alpha `PacketType` root import, all
supported constructor keywords and defaults, and the parameter lists of Stable
`AsyncClient` methods. Update the policy, changelog and migration guide before
intentionally changing that snapshot.

## Canonical imports

Use the supported entry points rather than implementation-module paths:

```python
from mqttium import FlowControlError, MQTTProtocolVersion, QoS
from mqttium.api import (
    AsyncClient,
    Message,
    Properties,
    ReconnectPolicy,
    SubscribeOptions,
)
```

Existing alpha import paths remain importable. This policy does not remove or
deprecate them; it defines which paths new external code should depend on.

## Statistics compatibility

`ClientStats` and its nested frozen dataclasses are immutable point-in-time
snapshots. Existing fields retain their meaning within the Provisional tier. New
fields may be added in minor releases. High-water fields cover the client or
engine lifetime; calling `stats()` does not start sampling, reset counters, or
emit logs.

The snapshot is diagnostic rather than transactional: related counters are read
consecutively on the owning loop and represent one practically consistent view,
not a lock-free cross-thread atomic transaction.

Each section is produced by the component that owns the state, and `stats()`
only assembles them. Use `ClientStats.outbound` and `ClientStats.inbound` for
directional protocol state; historical field migrations are recorded in the
changelog.

## Compatibility façade

`mqttium.compat` follows the narrower Provisional policy in
[`paho-compatibility.md`](paho-compatibility.md). Only Paho callback API VERSION2 is targeted.
Unsupported Paho behaviour is not promised merely because Paho exposes a
similarly named attribute. The facade is a migration tool, not a performance-
parity promise or a second native API.

## Deprecation policy

For Stable names:

1. a replacement is documented first;
2. the old surface remains available for at least one minor release when
   technically possible;
3. removal occurs only in a major release;
4. correctness or security fixes may tighten invalid behaviour immediately,
   with the behavioural change documented.

Internal names have no deprecation guarantee.

## Change-control gate

The executable public-surface test protects canonical exports, constructor
keywords and defaults, and Stable method parameters. A deliberate change must
update that test, this policy, the changelog, and migration guidance in the same
change. Release evidence must also satisfy [`stability.md`](stability.md).
