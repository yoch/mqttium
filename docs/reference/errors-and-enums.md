# Errors and enums

## Error hierarchy

All MQTTium-specific public errors derive from `MQTTError`.

::: mqttium.MQTTError
    options:
      heading_level: 3

::: mqttium.MalformedPacketError
    options:
      heading_level: 3

::: mqttium.ProtocolError
    options:
      heading_level: 3

::: mqttium.PacketTooLargeError
    options:
      heading_level: 3

::: mqttium.MandatoryResponseTooLargeError
    options:
      heading_level: 3

::: mqttium.FlowControlError
    options:
      heading_level: 3

::: mqttium.MessageDeliveryError
    options:
      heading_level: 3

::: mqttium.NotConnectedError
    options:
      heading_level: 3

::: mqttium.MQTTTimeoutError
    options:
      heading_level: 3

::: mqttium.SessionDiscardedError
    options:
      heading_level: 3

::: mqttium.SessionReplayError
    options:
      heading_level: 3

::: mqttium.PublishBatchError
    options:
      heading_level: 3

::: mqttium.BrokerDisconnectError
    options:
      heading_level: 3

Invalid arguments raise builtin exceptions, not `MQTTError`: a value of the
wrong Python type raises `TypeError` (for example a `Properties` value of an
unsupported type), and a value out of range raises `ValueError` (for example a
QoS outside 0–2 or `retain_handling` outside 0–2). `ProtocolError` is kept for
MQTT rules: wildcards where a topic name is required, properties a packet cannot
carry, MQTT 5 options on an MQTT 3.1.1 client, and peer violations.

Catch the narrowest useful error. In particular, backpressure, a terminal
connection rejection, and a discarded durable session require different
application responses.

## Protocol and connection enums

::: mqttium.MQTTProtocolVersion
    options:
      heading_level: 3

::: mqttium.QoS
    options:
      heading_level: 3

::: mqttium.ConnectionState
    options:
      heading_level: 3

`PacketType` is internal and is no longer re-exported from `mqttium`.

## Version

`mqttium.__version__` is the installed package version and the source used for
release tagging. Prefer `importlib.metadata.version("mqttium")` when inspecting
distribution metadata without importing the package.
