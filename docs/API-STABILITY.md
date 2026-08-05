# Public API stability policy

MQTTium is currently an alpha release. This document defines the public API
candidate that is being stabilised before the first non-alpha release and the
rules that separate supported interfaces from implementation details.

## What is public

A name is public when it is exported through `__all__` by one of these modules:

- `mqttium` — protocol enums and base errors;
- `mqttium.api` — `AsyncClient`, receipts, batch models, result models and
  `ClientStats`;
- `mqttium.protocol` — the synchronous protocol engine, configuration, effects,
  flow control, packet identifiers, negotiated settings and reconnect policy;
- `mqttium.persistence` — store protocols and the memory/SQLite implementations;
- `mqttium.transport` — transport protocols and concrete transports;
- `mqttium.compat` — the documented Paho VERSION2 compatibility façade.

Documented public methods and attributes on those exported classes are part of
the same surface. Names beginning with `_`, direct imports from implementation
modules such as `mqttium.api._writer`, and objects exposed only for tests or
instrumentation are not public.

## Native async client

The candidate native surface is:

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
cross-thread APIs. Threaded migration code should use `mqttium.compat.Client`.

Constructor keyword arguments are public configuration. New optional keywords
may be added compatibly. Existing defaults will not be changed after the first
stable release without a documented migration path.

## Statistics compatibility

`ClientStats` and its nested frozen dataclasses are immutable point-in-time
snapshots. Existing fields will retain their meaning. New fields may be added in
minor releases. High-water fields cover the client or engine lifetime; calling
`stats()` does not start sampling, reset counters, or emit logs.

The snapshot is diagnostic rather than transactional: related counters are read
consecutively on the owning loop and represent one practically consistent view,
not a lock-free cross-thread atomic transaction.

Each section is produced by the component that owns the state, and `stats()`
only assembles them: `OutboundStats` and `InboundStats` come from the two
protocol sessions (`mqttium.protocol.stats`), `EffectStats` and `WriterStats`
from the effect and write pumps, and `TransportStats` from the transport itself
(`mqttium.transport.stats`). A transport may implement `stats()` to report its
own buffer occupancy; one that does not is reported through
`TransportStats.unavailable()`, so the method stays optional for third-party
transports.

## Compatibility façade

`mqttium.compat` follows the narrower policy in [`COMPAT.md`](COMPAT.md). Only
Paho callback API VERSION2 is targeted. Unsupported Paho behaviour is not
implicitly promised merely because a similarly named attribute exists in Paho.

## Deprecation policy

Before `1.0`, an incompatible public change requires a changelog entry and a
migration note. After `1.0`:

1. a replacement is documented first;
2. the old surface remains available for at least one minor release when
   technically possible;
3. removal occurs only in a major release;
4. correctness or security fixes may tighten invalid behaviour immediately,
   with the behavioural change documented.

Private names have no deprecation guarantee.

## Stable-release gate

This API candidate becomes the stable contract only after the reconnect,
backpressure and broker-interoperability campaigns described in
[`STABILITY.md`](STABILITY.md) have produced retained successful artefacts.
