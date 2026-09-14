# Implementation guide

This guide records the contracts that are easiest to break while changing the
engine or runtime. The MQTT 3.1.1 and MQTT 5 specifications remain authoritative;
this guide takes precedence over the higher-level description in `architecture.md`.

## Global invariants

1. **One writer.** Only `WritePump` writes to a transport, and at most one write
   is in flight at a time. FIFO effect order is wire order. The write need not
   happen on the writer *task*: when nothing is queued, no write is in flight
   and no producer is waiting for queue space, `WritePump` may buffer a
   non-segmented frame straight through the transport's optional
   `write_nowait`, which saves the event-loop turn the writer task would
   otherwise cost. A segmented frame is never written that way, because it is
   two consecutive writes and nothing may land between them.
2. **One effect stream.** Engine sessions emit through `ProtocolEngine`; no
   component keeps a second effect list.
3. **Register completion before sending.** A receipt or SUBACK/UNSUBACK future
   must exist before its packet can reach the writer.
4. **Stable byte ownership.** The engine never retains a view into a reusable
   decoder buffer.
5. **Packet IDs are not flow control.** The pool always covers 1..65535;
   Receive Maximum limits unfinished QoS 1/2 publications.
6. **One owner per resource.** The component that reserves a counter, packet ID,
   store row, queue item, or delivery reference also releases it.
7. **No callbacks under protocol locks.** User code may call back into the
   client without deadlocking or observing half-applied state.
8. **No live-connection retransmission timer.** PUBLISH and PUBREL replay only
   after reconnect when the broker retained the session.

## Packet decoding

The decoder accepts arbitrary fragmentation and multiple packets per read. A
malformed or overlong Variable Byte Integer is rejected before allocation.
Negotiated and local maximum packet sizes are enforced as soon as the complete
remaining length is known.

The contiguous fast path may decode directly from the current input chunk. A
packet spanning chunks falls back to the incremental buffer. Either path must
produce the same owned packet values and errors.

Codec primitives are direct MQTT-version implementations. ``ProtocolEngine``
binds its encode/decode functions once through ``packets._bindings.bind_codec``;
the inbound session likewise binds one PUBLISH handler when it is constructed.
Hot handlers therefore contain neither a per-packet protocol branch nor a
generic codec helper call. Acknowledgement bodies treat the two-byte success
form and the MQTT 5 three-byte explicit-reason form as primary paths; absent
properties are ``None``. Internal ``mqttium.packets`` dataclasses remain
thin factories over the same primitives.

MQTT UTF-8 validation applies on both encode and decode. Topics reject wildcards
and U+0000. Filters validate `+` and `#` placement, shared-subscription prefixes,
and empty group names before mutating engine state.

## MQTT 5 properties

Property metadata lives in the codec table, which is the source of truth for
identifier, wire type, legal packet contexts, and repeatability. Do not duplicate
that table in runtime code.

Encoding and decoding must enforce:

- the property is legal for the packet type;
- non-repeatable properties appear at most once;
- `user_property` preserves order;
- PUBLISH subscription identifiers may repeat, but SUBSCRIBE's may not;
- zero is rejected for Receive Maximum, Maximum Packet Size, Topic Alias, and
  Subscription Identifier where the specification forbids it;
- the declared property length exactly matches consumed bytes;
- unknown property identifiers produce `MalformedPacketError`.

The no-properties path encodes to one zero byte without constructing temporary
containers.

## CONNACK negotiation

A successful CONNACK produces an immutable `NegotiatedSettings` separate from
the requested configuration.

| Setting | Default when absent | Required behaviour |
| --- | --- | --- |
| `receive_maximum` | 65535 | outbound limit is the minimum of local and broker limits |
| `maximum_packet_size` | unlimited | reject oversized publish before admission |
| `maximum_qos` | 2 | reject unsupported QoS; never silently downgrade |
| `retain_available` | true | reject retained publish when false |
| `topic_alias_maximum` | 0 | bound explicit outbound aliases |
| `server_keep_alive` | requested keepalive | replace the active keepalive period |
| `assigned_client_identifier` | local client ID | expose as the effective ID |
| subscription capabilities | available | reject unsupported filters before sending |

A broker-assigned MQTT 5 ClientID is retained by the engine as the Session
identity for same-instance durable reconnects. It is not part of the inflight
store contract; process-restart recovery requires a stable configured ClientID.

`EngineConfig.local_receive_maximum` defaults to 65535 because the standalone
engine follows the protocol maximum. `AsyncClient` intentionally defaults to
100 to provide an operationally bounded application client. The native default remains supported; the standalone
engine configuration is internal.

Inbound and outbound topic aliases reset on every network connection. Alias
zero, an inbound alias above the advertised maximum, or an unknown inbound
alias with an empty topic produces MQTT 5 reason `0x94`. Outbound aliases are
explicit: a non-empty Topic Name establishes or replaces the mapping, and a
later empty Topic Name may reuse it only on that same connection. Durable
outbound records retain the canonical Topic Name so replay never depends on an
old connection. MQTTium does not assign outbound aliases automatically.

## Keepalive

The active interval is the broker's Server Keep Alive when supplied, otherwise
the CONNECT value. Zero disables keepalive.

The writer updates `last_outbound` after successful writes. When the interval
expires with no outbound traffic, the client queues PINGREQ through the normal
writer and starts a PINGRESP deadline. Any ordinary incoming packet does not
stand in for PINGRESP. Missing the deadline closes the transport with
`MQTTTimeoutError` and enters reconnect policy.

The keepalive task is connection-scoped. Reader teardown cancels and joins it
before automatic or explicit reconnect may install the next connection's task.
If a negotiated Maximum Packet Size makes the two-byte PINGREQ impossible, the
keepalive records the terminal error and closes only the transport; the reader
still owns connection-visible teardown.

## Reconnect

Each attempt creates a new transport and clears decoder and connection-local
alias state. Backoff is bounded and jittered, and resets after a sufficiently
stable connection.

An explicit `connect()` is a user takeover of a live automatic reconnect task.
It replaces that automatic generation even when it had already connected while
the explicit caller waited for the lifecycle lock, and cancels any successor
retry created while the automatic reader is joined.

Permanent authentication, authorisation, and protocol errors stop retrying.
Temporary broker-unavailable errors and network failures may retry. Pending
receipts survive only while the broker session can still settle them; a clean
CONNACK fails them with `SessionDiscardedError`.

Every effect and deferred replay continuation carries the connection epoch.
Work from an older epoch is discarded rather than applied to the new transport.

## QoS transitions

### Outbound QoS 1

Admission reserves logical capacity, allocates a packet ID, persists the PUBLISH,
registers its receipt, then emits the frame. PUBACK is the terminal broker
boundary: its receipt completes on observation even if durable cleanup fails;
cleanup never vetoes it.

### Outbound QoS 2

PUBLISH remains persisted until PUBREC. A successful PUBREC atomically replaces
the durable record with PUBREL. PUBCOMP is the terminal boundary under the same
rule as PUBACK above. A terminal negative PUBREC fails the receipt and releases
the transaction.

### Inbound QoS 1 and 2

QoS 1 is delivered once and acknowledged immediately unless manual
acknowledgement is enabled. QoS 2 is delivered on the initial PUBLISH and
deduplicated by packet ID until PUBREL. PUBREC remains immediate; manual mode
defers PUBCOMP.

Automatic QoS 1 acknowledgements hold the local Receive Maximum slot until
`take_effects()` hands the PUBACK to the runtime. A retransmission of that
identifier in the same batch reuses the slot; a new identifier is admitted
through the ordinary acquire path. Packet-identifier reuse across unfinished
QoS 1 and QoS 2 exchanges is a protocol error, including when the QoS 1
PUBACK has been emitted but not yet handed off.

Duplicate PUBLISH and PUBREL packets repeat the required protocol response but
never redeliver application data. Orphan PUBREL is answered idempotently.

## Backpressure and rollback

Logical outbound size is payload bytes plus encoded topic and properties. The
admission sequence is validation, size calculation, reservation, packet-ID
allocation, store mutation, and effect emission.

Every unit failure reverses acquisitions through outbound rollback. Batch
publication commits elements progressively and preserves its committed prefix
on errors or cancellation; there are no chunk snapshots.

Writer, outbound inflight, inbound persistence, ingress, and application
delivery budgets are independent. Do not reuse one counter as a proxy for
another lifetime. `max_outbound_messages` bounds writer-resident admitted
frames, including the writer's active batch, not only `queue.qsize()`.

`outbound.can_ever_admit()` considers configured limits, not current occupancy.
It distinguishes work that should wait from work that can never fit.

## Application delivery

`ApplicationDelivery` owns the bounded iterator queue and its byte
reservations, and runs synchronous callbacks inline. Iterator (default) and
callback are exclusive. In iterator mode each message has one byte charge and
one queue item, released when the iterator yields the message; the reader
waits for byte and queue capacity under one `delivery_timeout` deadline.

Message callbacks are synchronous-only and execute on the reader that delivered
the message, after the protocol lock is released and before the reader decodes
further packets. There is no callback queue, worker task or byte reservation.
Registration rejects async functions and async callable objects before
mutation. Returning an awaitable is reported as a callback `TypeError`; MQTTium
does not await it or create a detached task. Ordinary failures are isolated per
invocation; a `CancelledError` raised by user code is reported unless the reader
itself is being cancelled. The private fairness quantum counts actual callback
invocations, including routes inside one message, and yields at the following
message boundary. Synchronous user code cannot be preempted.

`on_publish` is removed: receipts settle without message-queue admission.
`on_connect` and `on_disconnect` use separate bounded lifecycle ownership, after
the triggering effect and connection locks. There is one active hook and one
latest pending state; obsolete states may be coalesced. External lifecycle
operations cancel obsolete hooks, while an operation directly awaited by the
current hook preserves its caller. Network operation completion does not await
hook completion. `on_connect` is not an incoming-data readiness barrier.
Automatic retry awaits the current disconnect hook and rechecks user intent.
Authentication alone remains awaited as protocol work with `auth_timeout`.


`delivery_timeout=None` has no deadline. A positive timeout covers both byte
reservation and queue insertion with one deadline. Timeout or an impossible
message raises `MessageDeliveryError`, releases acquired credits and leaves
persisted delivery state unmarked. A delivered mark denotes queue acceptance
in iterator mode and completed callback invocation in callback mode.

`accept()` returns `None` after an immediate handoff and an awaitable only for
the waiting path or the fairness yield, so the common case creates no
coroutine. Persisted marks follow the handoff and retain fail-stop semantics;
they run synchronously when the engine lock is free and otherwise acquire it.
No user callback executes under that lock.

`EffectPump` owns only protocol work. A reader-owned `DeliveryLane` holds MESSAGE,
DECODED_MESSAGE and CONTINUE_INBOUND_REPLAY. Each bounded lot records an epoch
and its protocol completion target. The reader applies that target before
message delivery; later unrelated protocol work does not extend it. Reader
input pauses until the current delivery lot progresses. Replay continuation
stays behind its prior MESSAGE batch and requests only the next bounded page.
This does not change direct `ProtocolEngine` consumers' continuation obligation.

Publication and already-decoded completions therefore progress independently
of application delivery. The pre-admission protocol drain still settles old
receipt ownership before MID reuse. An ACK unread behind incoming traffic can
still be delayed by bounded ingress; no speculative scanning or unbounded
side queue is introduced. On epoch retirement, cancel the reader's delivery
wait, release untransferred reservations and discard unaccepted effects while
preserving accepted-queue and durable-session semantics. A protocol/writer
failure must wake that reader without joining it from the failing task.

Topic routing belongs to `AsyncClient`. The fallback and routes freeze
permanently on the first connection attempt; MQTT subscriptions remain mutable.
For sustained bidirectional workloads, see the explicit application overflow
policies in [Operations](operations.md#bidirectional-pressure).

## Persistence

Store transitions accept both expected and new states. A mismatch is a protocol
or concurrency error, not a request to overwrite newer state.

SQLite schema 5 accepts only fresh databases and that exact format. Historical,
future and inconsistent schemas are refused without changing committed schema or
data. Validation uses one SQLite read transaction, ends it before journal setup,
and revalidates a fresh database under its creation write lock. Normal recovery
and checkpointing may change physical files. Metadata-only acknowledgement must
not read payload BLOBs.

Paged replay preserves insertion order without duplicates or resurrection. A
page may be shorter when records were acknowledged after the ordered snapshot;
callers must continue until the iterator ends rather than assuming fixed page
length.

Paged iterators in both stores snapshot ordered identifiers when iteration
starts and look up each page as it is consumed. Records deleted before that
lookup are omitted. Runtime replay uses this internal interface.

## Failure semantics

An ingress lot passes through three events that cannot be merged: OBSERVE (a
packet is decoded), COMMIT (local state, durable or in memory, is updated),
EXPOSE (an effect becomes application- or wire-visible). Packets are not
rollbackable once observed; only local state is. The read loop opens one
outer `store.batch()` around the whole ingress lot and collects effects after
it closes; per-packet atomicity is explicitly not specified.

A propagated failure reaches the read-loop `finally`, which advances the
epoch, calls `notify_transport_closed()`, and collects whatever sits in
`engine._effects` under the new epoch before draining it: rollback alone
does not retract effects. Latch, filter, and transport-closed retire
therefore run synchronously under the engine lock with no await
between them. The read loop groups an ingress lot inside `store.batch()`.
For a transactional store, an exception rolls back the durable mutations
covered by that batch. Fail-stop does not assume transactional rollback of
in-memory protocol state. On a local-terminal lot failure, only
already-established terminal publish outcomes (`PUBLISH_COMPLETE` /
`PUBLISH_FAILED`) are intentionally preserved; other unexposed effects from
the failed lot are discarded. This is not a generic effect-dependency or
provenance mechanism.

Four guarantees, all normative:

1. An ingress lot that fails locally exposes none of its ordinary unexposed
   effects. The normal hot path stays `_settle()` then emit; only a cleanup
   failure emits the already-observed outcome first.
2. An observed terminal publish broker outcome determines its receipt
   despite cleanup failure. Durable cleanup never vetoes the terminal effect.
3. A local-terminal failure fail-stops the `AsyncClient`: the original
   exception is preserved end to end (no `str()`, no peer-blaming DISCONNECT
   — `PROTOCOL_ERROR` stays reserved for wire violations); the connection is
   retired immediately; no automatic reconnect or replay; no new publish
   admission; no silent reuse (`connect()` refuses — create a new client);
   remaining receipts fail with the local cause. A replacement client built
   on the same ambiguous or known-stale store is not automatically repaired:
   before supplying persistence state to it, the application must explicitly
   decide and reconcile the store and broker-session state. Direct
   `ProtocolEngine` consumers receive the original exception and decide
   retirement themselves; the engine carries no fail-stop latch.
4. Broker and local store share no distributed transaction: perfect crash
   recovery and absence of duplication are not guaranteed.

Covered by `tests/unit/test_ingress_failure_semantics.py`.

## API completion and errors

- QoS 0 receipts complete at writer admission. `publish_nowait()` preflights
  immediate protocol/writer capacity; awaited publication waits for bounded
  transfer independently of application-delivery capacity.
- QoS 1 receipts complete at PUBACK.
- QoS 2 receipts complete at PUBCOMP.
- SUBACK and UNSUBACK return all per-filter reason codes; a reason code at or
  above `0x80` remains data in the result rather than becoming a blanket
  exception.
- Each transport loss fails pending SUBSCRIBE/UNSUBSCRIBE futures; those
  requests are not replayed. Replayable QoS 1/2 publication receipts survive
  retry attempts but fail when retry becomes terminal or the broker session
  cannot be resumed.
- Public exceptions must not shadow Python built-ins.

QoS 0 publication may bypass general effect creation only with the current
writer epoch, no terminal failure, no pending protocol effects or active
protocol-effect/engine lock. It reuses outbound preparation and registers
the unit receipt or aggregate batch element before handing bytes to `WritePump`.
Batch admission does not allocate a unit receipt. Topic Aliases commit after
acceptance. A clean writer refusal rolls back only that batch registration and
may fall back for awaited publication; writer exceptions retain the registered
prefix and propagate without retry because handoff may already have occurred.
`publish_many()` amortizes ready QoS 0 orchestration with a bounded private
driver while retaining progressive admission and its existing fairness points.
It transfers each item before advancing the source, without holding an engine
lock across source iteration. QoS 1/2 retain ordinary per-item admission.
The protocol-effect drain before awaited admission remains necessary for
receipt settlement before MID reuse; it never drains application delivery.

## Required validation

Before changing one of these contracts, add focused tests for the failure point
as well as the successful path. The minimum relevant matrix includes:

- MQTT 3.1.1 and MQTT 5;
- fragmented and coalesced input;
- QoS duplicates and negative acknowledgements;
- reconnect with `session_present` both true and false;
- memory and SQLite stores, including injected rollback failures;
- bounded and immediate-refusal admission;
- exclusive callback and iterator delivery;
- cancellation and shutdown with blocked producers;
- malformed properties, topics, filters, and aliases.

Run the local `quick` profile for cross-cutting changes. Performance-sensitive
changes also follow [`benchmarking.md`](benchmarking.md); do not trade an
ownership invariant for a favourable isolated number.
