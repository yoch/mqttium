# Native API reference

This reference covers the canonical experimental MQTTium imports. Importability from
another module does not grant the same stability tier.

## Canonical entry points

| Entry point | Native names |
| --- | --- |
| `mqttium` | `MQTTError`, `MalformedPacketError`, `ProtocolError`, `PacketTooLargeError`, `FlowControlError`, `MessageDeliveryError`, `NotConnectedError`, `MQTTTimeoutError`, `SessionDiscardedError`, `PublishBatchError`, `MQTTProtocolVersion`, `QoS`, `ConnectionState`, `__version__` |
| `mqttium.api` | `AsyncClient`, `Message`, `Properties`, `PublishMessage`, `PublishReceipt`, `PublishBatchReceipt`, `SubscribeResult`, `UnsubscribeResult`, `SubscribeOptions`, `ConnAckPacket`, `AuthPacket`, `NegotiatedSettings`, `ReconnectPolicy`, `MessageDelivery`, `PublishBackpressure` |

`ClientStats` is available from `mqttium.api` but remains Provisional because
new diagnostic fields may be added. `PacketType` remains importable from
`mqttium` for alpha-series compatibility but is a Provisional low-level enum.

## Reference pages

- [AsyncClient](async-client.md) — constructor, lifecycle, publication,
  subscriptions, delivery, MQTT 5 authentication, state, and callbacks.
- [Models and Settings](models.md) — messages, properties, receipts, results,
  negotiated settings, reconnect, and API mode literals.
- [Errors and Enums](errors-and-enums.md) — the Stable exception hierarchy and
  common protocol/state enums.

## Completion conventions

All client coroutines may propagate a typed `MQTTError` subclass for an MQTT,
connection, flow-control, delivery, timeout, or session failure. Invalid Python
arguments may raise `TypeError` or `ValueError`. Cancellation remains normal
`asyncio.CancelledError` behaviour unless a method documents a cleanup-specific
exception.

Publication has two boundaries:

1. `await client.publish(...)` admits work under the configured bounds;
2. `await receipt.wait()` follows the completion semantics for that QoS.

See [Core Concepts](../core-concepts.md) before choosing timeouts or retry logic.

## Stability rules

This branch intentionally revises the pre-v1 API. See the
[experimental contract](../api-stability.md) and [migration guide](../migration.md)
for supported interfaces and incompatible changes.
