# `AsyncClient`

`AsyncClient` is the async-native MQTTium API. One instance belongs to one
asyncio event loop. It owns transport lifecycle, reader/writer work,
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

## State, message callbacks and lifecycle hooks

| Member | Meaning |
| --- | --- |
| `state` | Current `ConnectionState` |
| `is_connected` | Whether the client is currently connected |
| `negotiated` | Broker-negotiated MQTT settings after CONNACK |
| `effective_client_id` | Requested or broker-assigned client identifier |
| `on_connect` | Sync or async lifecycle hook after successful connection setup |
| `on_disconnect` | Sync or async lifecycle hook after connection teardown |
| `on_message` | Short synchronous message callback |
| `message_callback_add` / `message_callback_remove` | Synchronous topic callbacks; matching filters run instead of `on_message` |
| `auth_handler` | Read-only MQTT 5 protocol handler supplied at construction; sync or async |

`on_publish` is removed. Observe publication through `PublishReceipt` or
`PublishBatchReceipt`; QoS 0 completion means writer admission, QoS 1 PUBACK,
and QoS 2 PUBCOMP.

Declare message callbacks with `def`. Async functions and async callable objects
are rejected before registration changes. A synchronous callback returning an
awaitable violates the contract and is reported as a `TypeError`; MQTTium does
not await or implicitly schedule that result. Message callbacks run
synchronously on the reader that delivered the message, outside protocol
critical sections and without any intermediate queue: the reader decodes no
further packet until the current lot has been handed to the application, so
callback cost is the natural backpressure. A callback exception is reported to
the loop's exception handler and delivery continues with the next callback or
message. All routes matching one message run contiguously; the reader counts
every invocation and yields to the event loop at the next message boundary once
its private budget is reached, carrying the excess over. That budget is not a
time limit: synchronous user code cannot be preempted.

Callback mode always uses automatic acknowledgement. Protocol acknowledgement
ordering is independent of callback completion: the QoS 1 PUBACK and the QoS 2
PUBREC are produced before application delivery. Because callbacks execute on
the reader, later packets are not processed until the callback returns.
`manual_ack=True` requires iterator delivery and raises `ValueError` with
`message_delivery="callback"`.

Matching topic filters run in registration order instead of `on_message`.
Shared-subscription filters match the filter string literally. Iterator mode
ignores message callbacks and routes. `on_message` and the routes freeze
permanently on the first connection attempt; subscriptions remain mutable.
Use `messages()` for asynchronous processing or explicitly manage application
work with its own bounds and overflow policy.

### Lifecycle hooks

`on_connect` and `on_disconnect` remain assignable sync-or-async hooks.
Declare `async def` for a hook that awaits application or MQTT work. A
synchronous hook returning an awaitable is reported as a `TypeError`; MQTTium
does not await or implicitly schedule that result.

The callback reference is captured when the lifecycle notification is recorded.
They execute on their own serialized task, after the triggering protocol
effect and connection locks have been released. A successful `connect()` and
`disconnect()` complete their network operation without waiting for hook
completion. `on_connect` may subscribe or publish normally; it is not a barrier
that delays already-available incoming messages until initialization finishes.
Use an application signal if processing depends on that initialization.

Lifecycle notifications describe the latest state, not a lossless transition
log. MQTTium retains one active hook and at most one pending notification;
newer state replaces obsolete pending state. An external connection replacement
or disconnect cancels an obsolete active hook. A lifecycle operation awaited
directly by that same hook preserves its caller, so `on_connect` can await
`disconnect()` and `on_disconnect` can await a replacement `connect()`. An
application-created task is a separate caller. Hooks must cooperate with
cancellation; the next hook starts only after the previous hook has ended.

Overlapping explicit connection attempts are rejected before changing the
endpoint or lifecycle ownership. Cancelling a takeover while it waits for the
connection lock preserves future notifications from the surviving connection.
It does not restore an obsolete hook already cancelled or a pending notification
already replaced when the takeover began. A hook awaiting its own connection
attempt receives that operation's failure normally; this caller preservation
ends when the connection call exits.

`on_disconnect` runs after the old connection resources are retired, with the
original cause or `None` for clean closure. Automatic retry waits until that
hook has started, not until it returns, then rechecks explicit user intent, so
a hook may await work that only the replacement connection can complete. The
replacement's `on_connect` still runs after the hook returns. Hook exceptions and a
manually raised `CancelledError` are reported to the event loop's exception
handler; actual task cancellation retires the hook.

Lifecycle hooks have no implicit execution deadline. An unfinished `on_disconnect`
does not delay automatic or explicit reconnection, but it delays the replacement's
`on_connect`. Use an application deadline when a hook must finish within a
fixed interval, and allow cancellation to propagate.

`auth_handler` accepts synchronous or asynchronous functions. Its response
participates in AUTH, so MQTTium invokes it with `auth_timeout` and the existing
protocol rules. Return an `AuthPacket` for a challenge response; use `async def`
if producing it requires asynchronous work. Lifecycle reentrancy does not
promise arbitrary reentrant operations from AUTH. See
[enhanced authentication](../mqtt-5.md#enhanced-authentication).

## Loop confinement

`publish_nowait()` and `stats()` are synchronous but must run on the owning
event-loop thread. Cross-thread handoff belongs to the application and requires
its own bound.

## Constructor settings

Constructor keywords and defaults are recorded in the API contract. The
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
