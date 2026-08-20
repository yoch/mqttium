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

::: mqttium.PublishBatchError
    options:
      heading_level: 3

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

`PacketType` remains importable from `mqttium` but is Provisional and is not part
of this Stable reference.

## Version

`mqttium.__version__` is the installed package version and the source used for
release tagging. Prefer `importlib.metadata.version("mqttium")` when inspecting
distribution metadata without importing the package.
