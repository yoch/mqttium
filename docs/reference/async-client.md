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

Message callbacks use one bounded worker for the general case. An eligible
small, non-persisted MESSAGE effect may execute an idle synchronous callback
inline only when it is the sole eligible MESSAGE at the head of a callback-only
run and `AsyncClient` has already released the engine lock. A second eligible
MESSAGE, an async callback, reentrant/queued delivery, direct-decode QoS 0 or
`both` delivery uses the worker. One ordinary queue entry represents each
worker-owned notification; `callback_queued` is the actual queue length,
excluding the active notification. The queue's configured maximum never changes.
A worker turn processes only the notifications already present when it starts;
later arrivals wait for a subsequent turn. This is a count bound, not a time
bound on blocking user code or on individual matching topic filters.

A direct `on_message` is captured at admission. A topic notification snapshots its
ordered live matches when execution begins; changes affect later notifications,
not the current match chain. The topic dispatcher has a stable async form even
when all matches are synchronous. The user-facing `def`/`async def` contract and
rejection of dynamically returned awaitables are unchanged.

Ordinary callback errors, including self-raised `CancelledError` without task
cancellation, are reported and isolated. A real cancellation interrupts the active
notification (including any remaining topic matches), never the reader. Unstarted
notifications remain owned by the delivery controller and are resumed by its
replacement worker. Explicit shutdown, not cancellation of a private task,
chooses whether to drain or discard queued work. Reopen discards the retired
generation's queued work; an active reconnecting callback remains the single
consumer. Admissions already waiting for the retired generation are rejected.

An idle synchronous `on_publish` may still execute inline after receipt
settlement, outside the engine lock. This publish-completion policy is separate
from the narrow singleton MESSAGE-effect policy above. Iterator byte accounting
and `both` destination ordering are retained; the callback leg of `both` and the
direct-decode QoS 0 path are worker-owned. Synchronous callbacks must not block
the event loop. A callback cannot wait to admit more work into its own full
queue. Also avoid application dependency cycles where a callback waits for an
operation whose network progress requires that same saturated delivery queue to
drain; worker isolation is not an unbounded read-ahead guarantee.

Matching `message_callback_add` filters run instead of `on_message`, in
registration order. Shared-subscription filters match the filter string
literally, as in Paho. Each routed message resolves the live configuration when
its dispatcher starts and keeps that message's matching callbacks across awaits.
Later routed messages, including already queued messages, see updated filters
and fallback. Eligible synchronous routes remain inline. If an inline burst's
route becomes asynchronous, only its unstarted tail transfers to the bounded
worker, ahead of work admitted reentrantly by the earlier callback. No callback
prefix is replayed. Direct callbacks captured without a router keep their
existing batch semantics. Iterator-only delivery ignores callbacks, including
topic filters.

When a callback disconnects and reconnects the client before returning, the
current worker job finishes normally. Jobs still queued for the terminally
closed connection are discarded before the replacement connection is reopened;
its newly admitted callbacks use the existing worker and are not discarded by
the previous shutdown request. Already-active batch semantics are unchanged.

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
