# `AsyncClient`

`AsyncClient` is the experimental async-native MQTTium API. One instance belongs to
one asyncio event loop. It owns transport lifecycle, reader/writer work,
keepalive, reconnect, receipts, and application delivery; it does not create a
background thread.

::: mqttium.api.AsyncClient
    options:
      members:
        - connect
        - connect_unix
        - connect_ws
        - disconnect
        - publish
        - publish_nowait
        - publish_many
        - subscribe
        - unsubscribe
        - messages
        - ack
        - auth
        - message_callback_add
        - message_callback_remove
        - stats
      inherited_members: false
      heading_level: 2

## State and callbacks

| Member | Meaning |
| --- | --- |
| `state` | Current `ConnectionState` |
| `is_connected` | Whether the client is currently connected |
| `negotiated` | Broker-negotiated MQTT settings after CONNACK |
| `effective_client_id` | Requested or broker-assigned client identifier |
| `on_connect` | Sync or async callback after successful connection |
| `on_disconnect` | Sync or async callback for disconnection |
| `on_message` | Sync or async message callback |
| `message_callback_add` / `message_callback_remove` | Topic-filtered message callbacks; matching filters run instead of `on_message` |
| `on_publish` | Sync or async publish-completion callback |
| `auth_handler` | Read-only MQTT 5 enhanced-authentication handler supplied at construction |

Callbacks execute outside protocol-engine critical sections. Declare synchronous
callbacks with `def` and asynchronous callbacks with `async def`; a synchronous
callable that returns an awaitable violates the callback contract and is reported
as a callback `TypeError` rather than being scheduled implicitly. Synchronous
callbacks must not block the event loop.

`on_connect`, `on_publish` and messages always use one bounded worker. Each
message occupies one job and holds its delivery bytes until its routes finish.
Callback failures go to the event loop exception handler. `on_disconnect` and
authentication are directly awaited outside critical sections.

Matching `message_callback_add` filters run instead of `on_message`, in
registration order. Shared-subscription filters match the filter string
literally. Iterator-only delivery ignores callbacks, including
topic filters. `on_message` and routes are frozen permanently on the first
connection attempt; subscriptions can still change.

## Loop confinement

`publish_nowait()` and `stats()` are synchronous but must run on the owning
event-loop thread. Cross-thread handoff belongs to the application and requires
its own bound.

## Constructor settings

Constructor keywords and defaults are recorded in the experimental contract. The
generated signature above is authoritative for spelling and defaults;
[Configuration and Sizing](../configuration-and-sizing.md) groups them by responsibility
and explains how to choose values.

## Lifecycle pattern

```python
client = AsyncClient("service")
try:
    await client.connect("broker.example", 8883, ssl=True)
    # Subscribe, publish, and consume.
finally:
    await client.disconnect()
```

Keep `disconnect()` in `finally`. An application-owned persistence store is
closed after the client.

## Message iterator lifecycle

A `messages()` iterator belongs to one application-delivery generation. An
unexpected connection loss followed by automatic reconnect keeps that generation
alive, so an `async for` loop or suspended `anext()` continues on the replacement
transport.

A terminal disconnect ends the current generation. A later explicit `connect()`,
`connect_unix()`, or `connect_ws()` starts a new generation. Iterators created for
the previous generation stay terminal and cannot consume messages delivered by
the new connection; call `messages()` again after the explicit connect to consume
the replacement generation.
