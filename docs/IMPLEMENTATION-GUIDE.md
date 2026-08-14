# Implementation guide

This guide records the contracts that are easiest to break while changing the
engine or runtime. The MQTT 3.1.1 and MQTT 5 specifications remain authoritative;
this guide takes precedence over the higher-level description in `DESIGN.md`.

## Global invariants

1. **One writer.** Only `WritePump` writes to a transport. FIFO effect order is
   wire order.
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
properties are ``None``. Provisional ``mqttium.packets`` dataclasses remain
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

`EngineConfig.local_receive_maximum` defaults to 65535 because the standalone
engine follows the protocol maximum. `AsyncClient` intentionally defaults to
100 to provide an operationally bounded application client. This difference is
part of the Stable API contract.

Inbound topic aliases reset on every network connection. Alias zero, an alias
above the advertised maximum, or an unknown alias with an empty topic produces
MQTT 5 reason `0x94`. MQTTium does not assign outbound aliases automatically.

## Keepalive

The active interval is the broker's Server Keep Alive when supplied, otherwise
the CONNECT value. Zero disables keepalive.

The writer updates `last_outbound` after successful writes. When the interval
expires with no outbound traffic, the client queues PINGREQ through the normal
writer and starts a PINGRESP deadline. Any ordinary incoming packet does not
stand in for PINGRESP. Missing the deadline closes the transport with
`MQTTTimeoutError` and enters reconnect policy.

## Reconnect

Each attempt creates a new transport and clears decoder and connection-local
alias state. Backoff is bounded and jittered, and resets after a sufficiently
stable connection.

Permanent authentication, authorisation, and protocol errors stop retrying.
Temporary broker-unavailable errors and network failures may retry. Pending
receipts survive only while the broker session can still settle them; a clean
CONNACK fails them with `SessionDiscardedError`.

Every effect and deferred replay continuation carries the connection epoch.
Work from an older epoch is discarded rather than applied to the new transport.

## QoS transitions

### Outbound QoS 1

Admission reserves logical capacity, allocates a packet ID, persists the PUBLISH,
registers its receipt, then emits the frame. PUBACK emits completion before
deleting state and releasing the ID and capacity.

### Outbound QoS 2

PUBLISH remains persisted until PUBREC. A successful PUBREC atomically replaces
the durable record with PUBREL. PUBCOMP emits completion and then releases the
remaining resources. A terminal negative PUBREC fails the receipt and releases
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

Every failure path reverses acquisitions in the opposite order. A transactional
batch restores the counter snapshot as one unit because the store may already
have rolled back individual rows.

Writer, outbound inflight, inbound persistence, ingress, and application
delivery budgets are independent. Do not reuse one counter as a proxy for
another lifetime.

`can_ever_admit_publish()` considers configured limits, not current occupancy.
It distinguishes work that should wait from work that can never fit.

## Application delivery

`ApplicationDelivery` is the only owner of callback and iterator queues,
delivery bytes, user-worker state, and delivery counters. A message routed to
multiple consumers is charged once and released after the final reference.

The small-message reserve prevents one large payload from starving telemetry.
It is disabled when it would make a single otherwise valid packet impossible to
admit.

User callbacks run outside engine critical sections. Exceptions are isolated and
reported through the established callback policy; they do not stop the protocol
reader or leak delivery capacity. Consecutive small MESSAGE effects that require
no persisted delivery mark (QoS 0 and fresh automatic QoS 1) may transfer to the
bounded callback/iterator queues during the inline effect drain. Persisted QoS 1,
QoS 2 and replay deliveries keep the established awaited path and are marked only
after application delivery accepts them.

## Persistence

Store transitions accept both expected and new states. A mismatch is a protocol
or concurrency error, not a request to overwrite newer state.

SQLite migrations are atomic and schema-versioned. Unknown newer schemas are
rejected. Metadata-only acknowledgement must not read payload BLOBs.

Paged replay preserves insertion order without duplicates or resurrection. A
page may be shorter when records were acknowledged after the ordered snapshot;
callers must continue until the iterator ends rather than assuming fixed page
length.

## API completion and errors

- QoS 0 receipts complete at writer admission. When callback capacity is
  immediately available, `on_publish` is admitted to the isolated callback
  worker in the same loop turn; otherwise the whole operation retains the
  ordered EffectPump path. A batch must preflight every callback before any
  direct writer admission.
- QoS 1 receipts complete at PUBACK.
- QoS 2 receipts complete at PUBCOMP.
- SUBACK and UNSUBACK return all per-filter reason codes; a reason code at or
  above `0x80` remains data in the result rather than becoming a blanket
  exception.
- Transport loss fails work only after reconnect policy becomes terminal.
- Public exceptions must not shadow Python built-ins.

## Required validation

Before changing one of these contracts, add focused tests for the failure point
as well as the successful path. The minimum relevant matrix includes:

- MQTT 3.1.1 and MQTT 5;
- fragmented and coalesced input;
- QoS duplicates and negative acknowledgements;
- reconnect with `session_present` both true and false;
- memory and SQLite stores, including injected rollback failures;
- bounded and immediate-refusal admission;
- callback, iterator, and combined delivery;
- cancellation and shutdown with blocked producers;
- malformed properties, topics, filters, and aliases.

Run the local `quick` profile for cross-cutting changes. Performance-sensitive
changes also follow [`BENCHMARKING.md`](BENCHMARKING.md); do not trade an
ownership invariant for a favourable isolated number.
