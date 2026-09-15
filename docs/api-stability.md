# Native API contract

This is the current pre-v1 native contract. It intentionally revises the
earlier pre-v1 Stable contract and is breaking, not a deprecation bridge. The
migration guide records the differences from `1.0.0rc14` (`c194597`); there are
no compatibility wrappers for removed APIs.

## Supported surface

| Entry point | Supported names |
| --- | --- |
| `mqttium` | Operational `MQTTError` subclasses, `MQTTProtocolVersion`, `QoS`, `ConnectionState`, `__version__` |
| `mqttium.api` | `AsyncClient`, `Message`, `Properties`, `PublishMessage`, `PublishReceipt`, `PublishBatchReceipt`, `SubscribeResult`, `UnsubscribeResult`, `SubscribeOptions`, `ConnAckPacket`, `AuthPacket`, `NegotiatedSettings`, `ReconnectPolicy`, `MessageDelivery`, `ClientStats` |
| `mqttium.persistence` | `MemoryInflightStore`, `SqliteInflightStore` |

The engine, codecs, packet plumbing, directional sessions, transport extension
protocols, store implementation protocol and persistence records are Internal.
Importability and `__all__` are not support promises. Packet models needed by
the native API have their canonical imports in `mqttium.api`.

Paho, one-shot helpers and the root `PacketType` import are removed. The native
client belongs to one event loop. Synchronous methods are loop-confined, not
cross-thread entry points.

## Client operations

- Lifecycle: `connect`, `connect_unix`, `connect_ws`, `disconnect`.
- Publication: `publish`, `publish_nowait`, `publish_many`.
- Subscriptions: `subscribe`, `unsubscribe`.
- Delivery: `messages`, `ack`, `message_callback_add`, `message_callback_remove`.
- Authentication: `auth`, with `auth_handler` fixed at construction.
- State and diagnostics: `state`, `is_connected`, `negotiated`,
  `effective_client_id`, `stats`.
- Lifecycle hooks: `on_connect`, `on_disconnect`.
- Synchronous message callback: `on_message`; completion uses receipts.

`message_delivery` is explicitly `"iterator"` (default) or `"callback"`.
`on_message` and the topic route registry are permanently frozen at the first
connection attempt, including an unsuccessful attempt. Later changes raise
`MQTTError`; use another client instance to install another route configuration.
MQTT subscriptions remain independent and can change while connected.

Matched callbacks run in registration order instead of the `on_message`
fallback. Replacing a filter before connection keeps its position. Every
matching callback runs for one message before the next message is delivered.
Each callback failure is isolated so subsequent matches can still run.

Message callbacks are synchronous-only. Async functions and async callable
objects are rejected before registration changes; a synchronous function that
returns an awaitable is reported as a callback `TypeError`. Message callbacks
run synchronously on the delivering reader outside protocol locks. All routes
matching one message run contiguously; the reader yields to the loop at a
message boundary once its private invocation budget is reached, counting every
route invocation and carrying any excess over to the next yield.
A synchronous callback that blocks the event loop cannot be preempted.

Callback mode always uses automatic acknowledgement, and protocol
acknowledgement ordering is independent of callback completion: the QoS 1
PUBACK and the QoS 2 PUBREC are produced before application delivery. Because
callbacks execute on the reader, later packets are not processed until the
callback returns.
`manual_ack=True` requires iterator delivery, because `ack()` is awaited and
`messages()` is the asynchronous processing mode; the combination with
`message_delivery="callback"` raises `ValueError`.

`on_publish` is removed; individual and aggregate receipts are the publication
completion contract. `on_connect` and `on_disconnect` remain sync-or-async
lifecycle hooks with separate bounded ownership. Network operations do not wait
for hook completion, and incoming delivery does not wait for `on_connect`.
Obsolete pending lifecycle states are coalesced. External lifecycle operations
cancel obsolete active hooks; direct self-reentry preserves the invoking hook.
Automatic retry waits for the current disconnect hook, then rechecks intent.
See the [hook contract](reference/async-client.md#lifecycle-hooks) for ordering,
cancellation and error behavior. Authentication remains protocol-specific and
is awaited with `auth_timeout`.


## Admission and ownership

`publish()` waits for admission and bounded effect transfer. `publish_nowait()`
refuses before mutation when immediate protocol/writer transfer is unavailable.
Unrelated inbound delivery does not block already-decoded protocol completions
or outgoing admission. Bounded ingress can still leave an ACK unread behind
incoming traffic. Cancelling a
publication call before commitment admits nothing; after commitment the
publication may remain active. Cancelling `receipt.wait()` affects only that
waiter, not the MQTT exchange or other waiters.

`publish_many()` admits in input order, one publication at a time. A failed
submission exposes its committed prefix through `PublishBatchError.receipt`.
Cancellation leaves that prefix active and seals its aggregate receipt. Failure
details have a finite configured limit, default 128, while totals stay exact.
Ready QoS 0 batch elements use the same writer handoff as unit publication.
Their aggregate registration precedes wire exposure. An explicit clean refusal
can fall back to ordinary admission; an exception after handoff is never retried.

`Properties` owns an immutable copy of its input mapping, repeated values and
binary data. `ReconnectPolicy` is immutable retry-progression configuration;
passing one enables reconnection, `None` disables it, and each client owns its
retry state. Every default deadline (`connect_timeout`, `ping_timeout`,
`subscribe_timeout`, `auth_timeout`) is a constructor argument; `connect*()`,
`subscribe()` and `unsubscribe()` take a per-call override. CONNECT limits use
dedicated constructor arguments, never precedence between duplicate property
keys and arguments.

The constructor refuses configuration that would have no effect instead of
accepting it: MQTT 5 options (`connect_properties`, `will_properties`,
`topic_alias_maximum`, `auth_handler`) with MQTT 3.1.1 raise `ProtocolError`;
iterator bounds or `manual_ack` with callback delivery raise `ValueError`.

Manual `ack(message)` validates the delivered handle's active logical exchange.
Foreign, reconstructed and completed handles raise `ProtocolError`. A reconnect
that resumes the same session preserves active handles. `messages()` binds an
iterator to its generation when called, before its first advancement.

Nonzero broker DISCONNECT details use `BrokerDisconnectError` with `reason_code`
and immutable `properties` through `on_disconnect`, when no more specific cause
exists. Server-reference reason codes are terminal; there is no automatic
redirection option.

## Resource and documentation contracts

Protocol, writer and delivery budgets represent different lifetimes. Their
bounds remain independent and are named after what they bound:
`max_unacknowledged_*` for retained outbound QoS 1/2 publications,
`max_outbound_inflight` and `max_inbound_inflight(_bytes)` for the Receive
Maximum windows, `max_write_queue_*` for encoded frames, `max_iterator_*` for
the application queue. Outbound bounds refuse or park the producer; inbound
bounds end the connection. The reader's decode quantum is a fixed constant.
`iterator_admission_timeout=None` waits for application capacity without a
deadline; a positive value covers the entire byte-and-queue admission with one
deadline. A message that cannot ever fit fails immediately.

`ClientStats` is an immutable snapshot of what the client is doing for the
application without a logger: connection state and epoch, and for each
sizeable queue or window its occupancy, high-water mark, limit and parked
waiters, in the constructor's vocabulary. Runtime scheduling (task liveness,
effect and writer batching decisions) is not part of the snapshot.
`tests/project/test_public_api_surface.py` records the supported names,
signatures, defaults and snapshot fields. Intentional changes update that
test, maintained documentation, changelog and migration guidance. Historical
reports remain evidence of the commits they describe.
