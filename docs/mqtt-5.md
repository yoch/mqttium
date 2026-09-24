# MQTT 5 guide

Select MQTT 5 explicitly:

```python
from mqttium import MQTTProtocolVersion
from mqttium.api import AsyncClient

client = AsyncClient(
    "mqtt5-client",
    protocol=MQTTProtocolVersion.MQTTv5,
)
```

## Properties

MQTTium represents MQTT 5 properties with `Properties`. Packet encoders and
decoders validate which names are legal for each packet type.

```python
from mqttium.api import Properties

properties = Properties(
    {
        "content_type": "application/json",
        "payload_format_indicator": 1,
        "user_property": (("schema", "telemetry-v1"),),
    }
)

receipt = await client.publish(
    "telemetry/device-1",
    b'{"online":true}',
    qos=1,
    properties=properties,
)
await receipt.wait()
```

Properties deeply own their input. Repeated values become tuples and binary
values become owned bytes. Reusing a `Properties` instance is safe; create a
new instance when different values are needed.

Incoming property tables have a local resource limit of 1024 values for
repeatable property identifiers, counting User Property and Subscription
Identifier together. This is an MQTTium decoding limit, not an MQTT 5 protocol
limit. It applies in the shared decoder, including CONNACK before connection
establishment and later PUBLISH/control packets. Values within the budget
retain their order. An excess value raises `ProtocolError` before it is
decoded, and the partial repeated-property collection is discarded. The
configured `maximum_packet_size` byte limit still applies independently;
this count limit does not enlarge it. Outgoing property encoding is unchanged.

## Session expiry

`clean_start=False` asks the broker to resume a session. A positive session
expiry interval tells an MQTT 5 broker how long to retain it:

```python
client = AsyncClient(
    "durable-client",
    protocol=MQTTProtocolVersion.MQTTv5,
    clean_start=False,
    connect_properties=Properties(
        {"session_expiry_interval": 86_400}
    ),
)
```

Broker retention and client inflight persistence solve different halves of
restart recovery. See [Sessions and Persistence](sessions-and-persistence.md).

## Negotiated settings

After CONNACK, `client.negotiated` reports:

- Receive Maximum and maximum packet size;
- maximum QoS and retain availability;
- wildcard, shared-subscription, and subscription-identifier availability;
- inbound topic-alias maximum;
- server keepalive and assigned client identifier;
- session expiry, server reference, and response information.

MQTTium validates later operations against these values. A QoS 2 publish to a
broker advertising maximum QoS 1 raises `ProtocolError`; it is never silently
downgraded.

## Topic aliases

Topic aliases are explicit and connection-scoped. Their mappings reset on every
new network connection, including reconnect. MQTTium does not assign aliases
automatically. Establish or replace one by publishing a non-empty topic, then
reuse it with an empty Topic Name on the same connection:

```python
alias = Properties({"topic_alias": 1})
await client.publish("telemetry/device-1", b"first", properties=alias)
await client.publish("", b"next", properties=alias)
```

Alias zero, an alias above the broker's negotiated Topic Alias Maximum, and an
empty Topic Name with an unknown mapping are rejected before publication state
changes. QoS 1/2 persistence retains the canonical Topic Name, not the
connection-specific omission. Replay therefore sends the full topic and does
not carry the previous connection's alias onto the replacement connection.

## Last Will

Pass a `Message` as `will` and a separate `Properties` bag as
`will_properties`. Broker publication of a Will is controlled by MQTT session
and disconnect semantics; an orderly DISCONNECT normally suppresses it.

## Enhanced authentication

Register a synchronous or asynchronous handler when the broker uses an MQTT 5
challenge exchange. Configure the authentication method in CONNECT properties
and return an `AuthPacket` containing the response:

```python
from mqttium import MQTTProtocolVersion
from mqttium.api import AsyncClient, AuthPacket, Properties


AUTH_METHOD = "your-method"


async def on_auth(packet):
    response_data = await answer_challenge(packet)
    return AuthPacket(
        reason_code=0x18,
        properties=Properties({
            "authentication_method": AUTH_METHOD,
            "authentication_data": response_data,
        }),
    )


client = AsyncClient(
    "authenticated",
    protocol=MQTTProtocolVersion.MQTTv5,
    connect_properties=Properties({"authentication_method": AUTH_METHOD}),
    auth_handler=on_auth,
)
```

Replace `your-method` with the method agreed with the broker. The
application-defined `answer_challenge()` verifies that method, processes the
challenge and returns response bytes; it must protect authentication data.
A synchronous handler can return `AuthPacket` directly. Declare `async def`
when the handler needs to await application work. A synchronous handler
returning an awaitable is a handler error.

The handler's returned response is part of the active AUTH exchange. Return it
instead of awaiting `client.auth()` from inside the handler. The handler runs in
its own task, one call at a time, so it may await other client operations
(`publish()`, `subscribe()`, `disconnect()`), existing receipts or messages
without blocking protocol processing. A response is sent only when it answers
the broker's current Continue authentication (`0x18`) challenge: the return
value for AUTH Success (`0x00`) is ignored, and a response that arrives after
the broker ended the exchange or the connection closed is dropped. Application code
outside the handler can initiate re-authentication with `await client.auth(...)`
after connection; a handler is required because the broker can continue the
exchange.

Each handler call uses `auth_timeout` (10 seconds by default). Timeout, handler
failure, and handler self-cancellation enter the configured connection lifecycle;
cancellation requested on MQTTium's owning task still propagates normally.
Synchronous application code must remain short because an event-loop timeout
cannot preempt it.

## Server references

MQTT 5 can ask a client to use another server. `Use another server` (`0x9C`)
and `Server moved` (`0x9D`) are terminal; MQTTium does not automatically select
a new endpoint. `ReconnectPolicy.follow_server_reference` has been removed:
the old flag retried the original endpoint rather than following the reference.

A nonzero broker DISCONNECT produces `BrokerDisconnectError` through
`on_disconnect(error)` when no more specific failure is already known. Read
`error.reason_code` and `error.properties.get("server_reference")` (if properties
are present), then explicitly choose the destination, credentials and TLS
configuration for any new connection. The property mapping is immutable.
Earlier protocol, transport and local failures remain authoritative. Normal
disconnect and refused-CONNACK exception behavior is unchanged.
