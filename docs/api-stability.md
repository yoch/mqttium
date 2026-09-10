# Native API contract for the lean experiment

This branch intentionally revises the pre-v1 Stable contract. It is an
incompatible experiment, not a release or a deprecation bridge. The migration
guide records the differences from `main@9ad1f018`; there are no compatibility
wrappers for removed APIs.

## Supported experimental surface

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
- Notifications: `on_connect`, `on_disconnect`, `on_message`, `on_publish`.

`message_delivery` is explicitly `"iterator"` (default) or `"callback"`.
`on_message` and the topic route registry are permanently frozen at the first
connection attempt, including an unsuccessful attempt. Later changes raise
`MQTTError`; use another client instance to install another route configuration.
MQTT subscriptions remain independent and can change while connected.

Matched callbacks run in registration order instead of the `on_message`
fallback. Replacing a filter before connection keeps its position. Every
message is one worker job even when several filters match. Each callback failure
is isolated so subsequent matches can still run.

Declare synchronous callbacks with `def` and asynchronous callbacks with
`async def`; a synchronous function returning an awaitable is reported as a
callback `TypeError`. Message callbacks and `on_connect`/`on_publish`
notifications run in the bounded worker. `on_disconnect` remains awaited by
teardown outside locks. Authentication is awaited with `auth_timeout` because
its result participates in the protocol exchange.

## Admission and ownership

`publish()` waits for admission and bounded effect transfer. `publish_nowait()`
refuses before mutation when immediate transfer is unavailable. Cancelling a
publication call before commitment admits nothing; after commitment the
publication may remain active. Cancelling `receipt.wait()` affects only that
waiter, not the MQTT exchange or other waiters.

`publish_many()` admits in input order, one publication at a time. A failed
submission exposes its committed prefix through `PublishBatchError.receipt`.
Cancellation leaves that prefix active and seals its aggregate receipt. Failure
details have a finite configured limit, default 128, while totals stay exact.

`Properties` owns an immutable copy of its input mapping, repeated values and
binary data. `ReconnectPolicy` is immutable configuration; each client owns its
retry progression. CONNECT limits use dedicated constructor arguments, never
precedence between duplicate property keys and arguments.

Manual `ack(message)` validates the delivered handle's active logical exchange.
Foreign, reconstructed and completed handles raise `ProtocolError`. A reconnect
that resumes the same session preserves active handles. `messages()` binds an
iterator to its generation when called, before its first advancement.

Nonzero broker DISCONNECT details use `BrokerDisconnectError` with `reason_code`
and immutable `properties` through `on_disconnect`, when no more specific cause
exists. Server-reference reason codes are terminal; there is no automatic
redirection option.

## Resource and documentation contracts

Protocol, writer, ingress and delivery budgets represent different lifetimes.
Their bounds remain independent. `delivery_timeout=None` waits for application
capacity without a deadline; a positive value covers the entire byte-and-queue
admission with one deadline. A message that cannot ever fit fails immediately.

Statistics are immutable diagnostic snapshots. Counters for removed internal
optimizations are removed with those mechanisms. `tests/project/test_public_api_surface.py`
records the experimental names, signatures and defaults. Intentional changes
update that test, maintained documentation, changelog and migration guidance.
Historical reports remain evidence of the commits they describe.
