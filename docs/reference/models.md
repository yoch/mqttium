# Models and settings

## Messages and MQTT 5 properties

::: mqttium.api.Message
    options:
      heading_level: 3

::: mqttium.api.Properties
    options:
      heading_level: 3

`Message` and `Properties` are immutable and own their data. Construct a new
`Properties` from a mapping to change values; repeated properties are tuples.

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
configured finite number of individual failures. Admissions are progressive;
a submission error carries a receipt for the committed prefix.

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

## Delivery mode

`MessageDelivery` accepts `"iterator"` (default) or `"callback"`. The two modes
are exclusive. Callback routes are configured before the first connection
attempt and then permanently frozen for that client.

Use `publish()` to wait for admission or `publish_nowait()` to refuse immediately.
`ReconnectPolicy` is immutable; sharing it never shares retry progression.
