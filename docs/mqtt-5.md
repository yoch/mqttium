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
    }
)
properties.add_user_property("schema", "telemetry-v1")

receipt = await client.publish(
    "telemetry/device-1",
    b'{"online":true}',
    qos=1,
    properties=properties,
)
await receipt.wait()
```

Repeated properties such as user properties are stored as lists. Do not reuse a
mutable property bag concurrently while another operation may encode it.

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
new network connection, including reconnect. Do not persist an alias or assume
that an earlier connection's mapping remains valid.

## Last Will

Pass a `Message` as `will` and a separate `Properties` bag as
`will_properties`. Broker publication of a Will is controlled by MQTT session
and disconnect semantics; an orderly DISCONNECT normally suppresses it.

## Enhanced authentication

Register an authentication handler when the broker uses an MQTT 5 challenge
exchange:

```python
async def on_auth(packet):
    response = await answer_challenge(packet)
    await client.auth(
        reason_code=0x18,
        properties=response,
    )

client.set_auth_handler(on_auth)
```

The application must verify the authentication method and protect challenge
data. Re-authentication can be initiated with `await client.auth(...)` after
connection.

## Server references

MQTT 5 can ask a client to use another server. `ReconnectPolicy` does not follow
that reference by default. Enabling `follow_server_reference` is a deployment
trust decision; validate the target and its TLS identity.
