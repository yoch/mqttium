# Models and settings

## Messages and MQTT 5 properties

::: mqttium.api.Message
    options:
      heading_level: 3

::: mqttium.api.Properties
    options:
      heading_level: 3

`Message` is immutable. `Properties` is mutable; avoid mutating the same bag
concurrently while another operation may encode it.

## Publication input and receipts

::: mqttium.api.PublishMessage
    options:
      heading_level: 3

::: mqttium.api.PublishReceipt
    options:
      heading_level: 3

::: mqttium.api.PublishBatchReceipt
    options:
      heading_level: 3

A batch receipt keeps exact aggregate counts while retaining at most the
configured number of individual failures. Use `failure_sink` when every detail
must be copied to application-owned storage.

## Subscription results and options

::: mqttium.api.SubscribeOptions
    options:
      heading_level: 3

::: mqttium.api.SubscribeResult
    options:
      heading_level: 3

::: mqttium.api.UnsubscribeResult
    options:
      heading_level: 3

Reason codes at or above `0x80` represent failure for the corresponding topic
filter. Inspect every returned code for multi-topic operations.

## Connection and authentication packets

::: mqttium.api.ConnAckPacket
    options:
      heading_level: 3

::: mqttium.api.AuthPacket
    options:
      heading_level: 3

## Negotiated settings

::: mqttium.api.NegotiatedSettings
    options:
      heading_level: 3

The negotiated snapshot is reset for a new connection. Topic aliases and other
connection-scoped settings must not be carried across reconnect manually.

## Reconnect policy

::: mqttium.api.ReconnectPolicy
    options:
      heading_level: 3

Automatic reconnect is opt-in by passing a policy to `AsyncClient`. Terminal
authentication, authorization, and protocol responses stop retrying.

## Delivery and backpressure modes

`MessageDelivery` accepts:

- `"auto"` — use the callback when assigned, otherwise the iterator;
- `"callback"` — callback delivery only;
- `"iterator"` — async iterator delivery only;
- `"both"` — independently bounded callback and iterator delivery.

`PublishBackpressure` accepts:

- `"wait"` — suspend the producer until capacity is available;
- `"error"` — raise `FlowControlError` without partially admitting the publish.

These aliases are Stable when imported from `mqttium.api`.
