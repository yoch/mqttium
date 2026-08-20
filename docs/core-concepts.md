# Core concepts

MQTTium exposes the decisions that determine whether work is accepted,
acknowledged, retained, or replayed. Understanding these boundaries is more
important than memorising individual methods.

## One client, one event loop

`AsyncClient` belongs to the event loop on which it is used. Create, connect,
publish, consume, and disconnect it from that loop. The native client does not
start a background thread.

Existing synchronous Paho applications can use the Provisional compatibility
facade, which deliberately owns a separate thread and loop. New async code
should not use that extra boundary.

## Admission and completion are different

`await client.publish(...)` waits until MQTTium can admit the publication under
its configured protocol and writer budgets. The returned `PublishReceipt`
tracks protocol completion:

| QoS | `receipt.wait()` completes when |
| --- | --- |
| 0 | the encoded packet is admitted to the writer |
| 1 | PUBACK is received |
| 2 | PUBCOMP is received |

QoS 0 has no broker acknowledgement. Its completion does not prove that the
socket buffer drained or the broker received the packet.

Cancellation of a task waiting on a receipt does not cancel the MQTT exchange.
Apply an application deadline when the business operation has a stricter time
limit than the session and reconnect policy.

## Every growing resource has an owner

MQTTium separates protocol state, encoded writes, input decoding, and
application delivery. Each has a count or byte bound appropriate to its
lifetime. This avoids one misleading “queue size” setting and makes pressure
visible in `client.stats()`.

The default publish policy waits for capacity. Immediate modes raise
`FlowControlError` before a packet identifier or persistent record is committed.
Disabling a bound with `None` transfers responsibility for that resource to the
application.

## Protocol state is deterministic

The synchronous `ProtocolEngine` accepts packets and commands and emits ordered
effects. It owns no socket, timer, callback, or event loop. `AsyncClient` applies
those effects to the runtime.

Outbound and inbound QoS state have separate owners. Packet identifiers span
the MQTT-defined range; Receive Maximum limits unfinished QoS work and is not a
packet-identifier allocator.

Read [Architecture](architecture.md) for the component boundaries and
[Implementation Contracts](implementation-guide.md) for the invariants.

## Broker sessions and client persistence are separate

A broker session retains broker-owned state such as subscriptions and queued
messages. An MQTTium inflight store retains client-side protocol exchanges.
Reliable restart recovery requires compatible settings on both sides.

If the broker reports that no previous session exists, MQTTium cannot resume
old inflight exchanges safely and fails them explicitly. See
[Sessions and Persistence](sessions-and-persistence.md).

## Application delivery is bounded too

Messages can be delivered through an async iterator, a callback, or both. Slow
application code consumes delivery capacity and eventually propagates pressure
instead of growing memory without limit.

`manual_ack=True` delays inbound MQTT acknowledgement until
`await client.ack(message)`. Manual MQTT acknowledgement is not an application
transaction and does not remove the need for idempotent processing.

## Negotiated limits are authoritative

After CONNACK, use `client.negotiated` to inspect Receive Maximum, maximum packet
size, maximum QoS, feature availability, keepalive, topic-alias capacity, and
the assigned client identifier. MQTTium rejects unsupported operations instead
of silently lowering QoS or removing requested semantics.

Next, choose explicit settings in [Configuration and Sizing](configuration-and-sizing.md).
