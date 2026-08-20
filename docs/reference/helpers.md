# One-shot helpers

`mqttium.helpers.publish` and `mqttium.helpers.subscribe` are Stable convenience
modules for small async programs. A long-lived `AsyncClient` is more efficient
when an application performs operations repeatedly.

## Publish one message

::: mqttium.helpers.publish.single
    options:
      heading_level: 3

## Publish several messages

::: mqttium.helpers.publish.multiple
    options:
      heading_level: 3

## Subscribe and collect

::: mqttium.helpers.subscribe.simple
    options:
      heading_level: 3

## Subscribe with a callback

::: mqttium.helpers.subscribe.callback
    options:
      heading_level: 3

The helpers connect, complete their requested work, and disconnect. They use the
same receipt, QoS, transport, and error semantics as `AsyncClient`.

For sequential publish/subscribe demonstrations, start the subscriber first or
use a retained message. Production consumers normally keep a connection open.
