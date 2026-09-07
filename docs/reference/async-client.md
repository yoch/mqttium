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
        - message_callback_add
        - message_callback_remove
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
| `message_callback_add` / `message_callback_remove` | Topic-filtered message callbacks; matching filters run instead of `on_message` |
| `on_publish` | Sync or async publish-completion callback |
| `auth_handler` | MQTT 5 enhanced-authentication handler |

Callbacks execute outside protocol-engine critical sections. Synchronous
`on_publish` and eligible `on_message` / topic-filtered callbacks may execute
inline when callback delivery is idle; async, reentrant and queued callbacks
use the bounded worker. Synchronous callbacks must not block the event loop.
Callback failures go to the event loop's exception handler without silently
changing protocol state.

### Optional two-message synchronous burst

`inline_callback_burst=1` is the Stable default and preserves the existing
policy: an isolated eligible message callback may run inline, while a callback
burst is handed to the bounded worker. `inline_callback_burst=2` is an explicit
latency/throughput opt-in for callback-only consumers whose message callback is
strictly synchronous and short. When the reader has exactly two adjacent small
message effects and callback delivery is idle, both callbacks run in the same
reader/effect-drain turn before it yields.

The opt-in does not apply to declared `async def` callbacks, iterator or `both`
delivery, larger bursts, or an already active/queued callback path. It does not
relax `max_pending_callbacks`: the second callback consumes the same logical
reservation that the worker batch would have consumed, so reentrant callback
work queues behind the two-message burst.

A callable that is declared synchronous but returns an awaitable remains
supported by the default `inline_callback_burst=1` path. Under the explicit
`inline_callback_burst=2` contract, however, that return value is invalid:
MQTTium reports a callback `TypeError`; a coroutine result is closed rather than
scheduled. Use the default or declare the callback `async def` when it needs to
await. Because the opt-in executes two user calls in the reader turn, do not use
it for blocking I/O, long CPU work, or callbacks with unbounded service time.

Matching `message_callback_add` filters run instead of `on_message`, in
registration order. Shared-subscription filters match the filter string
literally, as in Paho. Iterator-only delivery ignores callbacks, including
topic filters.

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
`connect_unix()`, or `connect_ws()` starts a new generation. Iterators created for
the previous generation stay terminal and cannot consume messages delivered by
the new connection; call `messages()` again after the explicit connect to consume
the replacement generation.
