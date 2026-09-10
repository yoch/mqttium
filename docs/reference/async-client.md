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

Callbacks execute outside protocol-engine critical sections. Declare synchronous
callbacks with `def` and asynchronous callbacks with `async def`; a synchronous
callable that returns an awaitable violates the callback contract and is reported
as a callback `TypeError` rather than being scheduled implicitly. Synchronous
callbacks must not block the event loop.

Eligible idle `on_publish` and message callbacks may execute inline. For
callback-only message delivery (including `auto` while a callback is installed),
at most the first delivery of an eligible small-message burst runs inline. Its
entire tail is first admitted to the existing bounded callback queue, preserving
FIFO ahead of reentrant admissions and the hard `max_pending_callbacks` bound.
If that tail cannot all fit, the burst keeps bounded worker admission. A single
effect-drain invocation permits only its first eligible message prefix to use
inline delivery. Declared-async and queued/reentrant deliveries use the worker;
`iterator`, `both`, and publish-completion scheduling retain their existing rules.

An ordinary callback exception or a callback-raised `CancelledError` without a
pending task cancellation is reported to the event loop and does not discard the
tail. A propagated task cancellation abandons only the unstarted tail of the same
admission and releases its capacity. Calling `Task.cancel()` then returning
normally is not an interruption of the synchronous callback. Since the first
callback now runs on the reader/effect task rather than the worker for larger
bursts, cancelling the current task there can terminate reception. Use `disconnect()` to request connection shutdown; use an `async def` callback
when callback work needs its own worker context. MQTTium does not call
`Task.uncancel()` or suppress real task cancellation to simulate another owner.
The selected task is an implementation detail, not a callback-local cancellation
scope.

Cancelling the private callback worker retires its active and queued callback
ownership, including queued jobs whose worker never started. It does not cancel
the reader. Queue admission after retirement starts a new worker when needed.
This is cleanup of an internal task, not a new public shutdown API.

The inline budget counts message dispatches: all matching synchronous topic
callbacks for one message remain a single dispatch in registration order, not a
promise of at most one matching filter invocation. The per-message match snapshot
is retained; later routed messages still resolve the then-current configuration.
A directly captured `on_message` callable is retained for the whole admitted burst.

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
