# `AsyncClient`

`AsyncClient` is the Stable, async-native MQTTium API. One instance belongs to
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
        - set_auth_handler
        - stats
      inherited_members: false
      heading_level: 2

## Stable state and callbacks

| Member | Meaning |
| --- | --- |
| `state` | Current `ConnectionState` |
| `is_connected` | Whether the client is currently connected |
| `negotiated` | Broker-negotiated MQTT settings after CONNACK |
| `effective_client_id` | Requested or broker-assigned client identifier |
| `on_connect` | Sync or async callback after successful connection |
| `on_disconnect` | Sync or async callback for disconnection |
| `on_message` | Sync or async message callback |
| `on_publish` | Sync or async publish-completion callback |
| `auth_handler` | MQTT 5 enhanced-authentication handler |

Callbacks execute outside protocol-engine critical sections. Synchronous
callbacks must not block the event loop. Callback failures go to the event
loop's exception handler without silently changing protocol state.

`on_connect`, `on_message`, and `on_publish` run on the callback worker, so they
may call `disconnect()` without deadlocking that worker. `on_disconnect` runs on
the reader during teardown. It may call `connect()` or `disconnect()`; an
`on_disconnect` that reconnects should return immediately when
`disconnect()` already requested a terminal shutdown, otherwise it fights the
application's own stop path. A user callback that raises `CancelledError` is a
callback failure and does not stop later queued callbacks.

## Loop confinement

`publish_nowait()` and `stats()` are synchronous but must run on the owning
event-loop thread. They are not cross-thread methods. Use the Provisional Paho
facade only when an existing synchronous application needs a transition path.

## Constructor settings

Constructor keywords and Stable defaults are part of the public contract. The
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
`connect_unix()`, or `connect_ws()` starts a new generation, including when that
explicit connect happens during an automatic reconnect gap. Iterators created for
the previous generation stay terminal and cannot consume messages delivered by
the new connection; call `messages()` again after the explicit connect to consume
the replacement generation.

Cancelling a task blocked in `anext()` closes that async generator, which is
Python's iterator contract rather than a new mqttium generation. Automatic
reconnect keeps the generation; call `messages()` again to bind a fresh iterator
to it.
