# Operations

MQTTium exposes protocol and resource state without running a metrics sampler or
emitting library logs. This guide shows how to size a client, observe it and
separate temporary pressure from a stalled connection.

## Start with explicit service objectives

Choose limits from the workload rather than copying a single large queue size.
Record at least:

- maximum accepted payload and topic size;
- expected and burst publication rates;
- broker Receive Maximum and acknowledgement latency;
- maximum time producers may wait;
- maximum callback or iterator processing latency;
- reconnect duration the service is expected to absorb;
- process memory budget.

Message limits control object count. Byte limits control retained payload and
topic data. Most production clients need both.

## Independent pressure boundaries

MQTTium keeps separate budgets because each resource has a different lifetime:

| Boundary | Relevant configuration |
| --- | --- |
| Unfinished outbound QoS state | `max_unacknowledged_messages`, `max_unacknowledged_bytes` |
| Inbound persisted protocol state | `max_inbound_inflight_bytes` |
| Encoded writer queue | `max_write_queue_messages`, `max_write_queue_bytes` |
| Iterator queue (iterator delivery only) | `max_iterator_messages`, `max_iterator_bytes` |
| Broker-facing QoS concurrency | `max_inbound_inflight`, `max_outbound_inflight` and negotiated limits |

Outbound bounds refuse or park the local producer. Inbound bounds cannot
refuse what the broker already sent: exceeding `max_inbound_inflight` or
`max_inbound_inflight_bytes` ends the connection with DISCONNECT `0x93` or
`0x97`, reported through `on_disconnect`. The reader's decode quantum (256
packets or 1 MiB per lot) is a fixed fairness constant, not a bound.

`max_write_queue_messages` bounds writer-resident admitted frames: items still on
the asyncio queue **and** the writer's active batch (up to 256 frames extracted
for one write). `client.stats().writer.queued_messages` remains `queue.qsize()`
and can be lower than the admission count while a batch is in flight. Eager
writes do not consume the message bound.

Do not use a message count as a proxy for bytes when payload sizes vary. Passing
`None` disables an optional bound and should be an explicit capacity decision,
not a first response to saturation.

## Wait or refuse

The default native publish policy waits for protocol and writer capacity. This
propagates backpressure to an async producer without blocking the event loop.

Use `publish_nowait()` when the application has a defined shed, retry or spill
policy. Saturation raises `FlowControlError` before allocating a packet
identifier or committing store state.



### QoS 0 completion is writer admission

MQTT has no broker acknowledgement for QoS 0. MQTTium therefore completes the
`PublishReceipt` after the encoded packet is admitted to the writer. This does
not mean the transport has written the bytes, the socket send buffer has
drained, or the broker has received the publication.

Receipt completion is consequently not a socket-level outstanding-byte limit.
Use `await client.publish(...)` when a producer should wait for writer capacity.
A `publish_nowait()` producer must catch `FlowControlError` and apply its own
shed, retry or spill policy.

A `publish_nowait()` producer sending large payloads will saturate the writer
byte budget (`max_write_queue_bytes`, 1 MiB by default) long before it exhausts the
message count, and a producer that merely retries on `FlowControlError` will
busy-spin against it. Shed, slow down, or spill instead — or use
`await client.publish(...)` and
let the client apply the backpressure for you. Do **not** disable the optional
`max_unacknowledged_*` bounds merely to mask sustained pressure: the writer
bounds stay finite either way, and removing the optional ones moves the failure
from a catchable exception towards memory exhaustion. Size the writer bounds
explicitly instead.

Size `max_write_queue_bytes` from the encoded bytes that may accumulate during the
largest supported burst, not only from the message count. This matters most for
64 KiB and 1 MiB payloads. To preserve forward progress, an empty writer (no
resident frames and no charged bytes) admits one item larger than its byte
limit; no second item is admitted until enough capacity is released. A writer
batch that has left the asyncio queue but is not yet written still occupies
both bounds. Inspect `client.stats().writer` to distinguish local queue
pressure from protocol-level unfinished QoS state.

## Runtime snapshots

Call `client.stats()` on the client's owning event-loop thread:

```python
snapshot = client.stats()

print("state", snapshot.state)
print("reconnect attempt", snapshot.reconnect_attempt)
print(
    "outbound",
    snapshot.outbound.unacknowledged_messages,
    snapshot.outbound.unacknowledged_bytes,
    snapshot.outbound.inflight,
    snapshot.outbound.inflight_limit,
)
print("writer", snapshot.writer.queued_messages, snapshot.writer.queued_bytes)
print("delivery", snapshot.delivery.iterator_queued, snapshot.delivery.iterator_bytes)
print("receipts", snapshot.receipts.publish, snapshot.receipts.publish_batches)
```

The snapshot exists so an application can see what its client is doing without
a logger. Every section describes a queue or window the application can size,
in the same vocabulary as the constructor bound it is measured against:

| Section | Fields | Constructor bound |
| --- | --- | --- |
| `state`, `connection_epoch`, `reconnect_attempt` | connection state, connection counter, retries issued since the last stable connection | `reconnect` |
| `outbound` | `unacknowledged_messages`, `unacknowledged_bytes`, their `*_high_water_*`, `awaiting_slot`, `inflight`, `inflight_limit`, `packet_ids_in_use` | `max_unacknowledged_*`, `max_outbound_inflight` |
| `inbound` | `inflight`, `inflight_limit`, `inflight_bytes`, `inflight_high_water_bytes`, `inflight_byte_limit`, `topic_aliases`, `replay_pending` | `max_inbound_inflight`, `max_inbound_inflight_bytes` |
| `writer` | `queued_messages`, `queued_bytes`, `high_water_*`, `max_messages`, `max_bytes`, `waiters`, `last_outbound` | `max_write_queue_*` |
| `decoder` | `buffered_bytes`, `high_water_bytes`, `max_packet_size` | `maximum_packet_size` |
| `delivery` | `iterator_queued`, `iterator_limit`, `iterator_bytes`, `iterator_high_water_bytes`, `iterator_byte_limit`, `waiters` | `max_iterator_*` |
| `receipts` | `publish`, `publish_batches`, `subscribe`, `unsubscribe`, `publish_waiters` | — |
| `transport` | `kind`, `closing`, `pending_write_bytes`, `buffered_read_bytes` | — |

`waiters` fields count producers currently parked on that bound; a non-zero
value with occupancy at the limit is sustained pressure, a high-water mark at
the limit with zero waiters is a burst that has drained. How the runtime
schedules its own work (background tasks, effect batching, writer batching
decisions) is not part of the snapshot; those counters are maintainer
diagnostics on the private pumps and may change without notice.

High-water values cover the lifetime of the component. Calling `stats()` does
not reset them. The snapshot is practically consistent for diagnostics, not a
cross-thread transactional view.

## Negotiated broker limits

After CONNACK, inspect `client.negotiated` rather than assuming the broker
accepted every requested capability:

```python
limits = client.negotiated
print("receive maximum", limits.receive_maximum)
print("maximum packet size", limits.maximum_packet_size)
print("maximum QoS", limits.maximum_qos)
print("effective keepalive", limits.server_keep_alive)
print("client id", client.effective_client_id)
```

The snapshot also reports retain, wildcard, shared-subscription and
subscription-identifier availability, topic alias maximum, session expiry and
server references. MQTTium validates operations against these settings and
raises rather than silently downgrading unsupported work.

### Inbound concurrency is capped below the protocol maximum

`AsyncClient(max_inbound_inflight=...)` defaults to **100**, not to the
protocol maximum of 65,535 that `EngineConfig` uses for direct-engine consumers.
It is the Receive Maximum MQTTium advertises to the broker, so it bounds how
many inbound QoS 1/2 publications the broker may have unacknowledged at once —
including automatic acknowledgement. A subscriber that needs more inbound
concurrency must raise it explicitly:

```python
client = AsyncClient(max_inbound_inflight=1000)
```

The supported client default is a bounded application-facing window. The
engine is internal. Raising it increases the memory the inbound path may
hold. It is unrelated to `max_outbound_inflight`, which bounds *outbound*
unfinished publications and is capped by the broker's own Receive Maximum.

## Timeouts

Timeouts protect different boundaries:

- `connect_timeout` limits every connection attempt, explicit or automatic;
  `connect(..., timeout=...)` overrides it for one explicit call;
- `ping_timeout` limits the wait for PINGRESP;
- `subscribe_timeout` is the default SUBACK/UNSUBACK deadline; `subscribe()`
  and `unsubscribe()` accept a per-call override. Only the acknowledgement
  wait raises `MQTTTimeoutError`; a failure while handing the request to the
  writer propagates unchanged. Timeout or cancellation abandons only the
  caller's result: a request already sent stays in flight, and its packet
  identifier is released by the late acknowledgement or connection teardown;
- `iterator_admission_timeout=None` waits indefinitely; a positive value covers iterator
  byte reservation and queue admission with one deadline. Callback delivery has
  no queue: synchronous callbacks run on the reader and are never timed out or
  preempted.

Lifecycle hooks have no implicit deadline. Automatic retry waits for the current
`on_disconnect` hook; give the hook an application deadline when needed and
cooperate with cancellation. See the
[lifecycle contract](reference/async-client.md#lifecycle-hooks).

Publication receipts intentionally follow reconnect policy and session outcome
rather than a fixed acknowledgement timer. Add an application deadline with
`asyncio.timeout()` or `asyncio.wait_for()` when a business operation has a
shorter service-level objective. Cancelling one receipt waiter does not cancel
the underlying MQTT publication.

## Distinguishing pressure from a stall

Use several fields together:

- growing outbound pending state with a full negotiated flow window usually
  means the broker acknowledgement path is the bottleneck;
- a growing writer queue points to transport or socket progress;
- delivery queues or delivery bytes at their limit point to slow application
  consumers;
- pending effects with an active effect task may be transient batching, while
  a stable non-zero count after disconnect needs investigation;
- a reconnect task and increasing attempt count show active recovery rather
  than a silent stop;
- packet IDs in use without corresponding pending QoS state indicate an
  invariant failure and should be reported.

Capture two or more snapshots over time. One high-water mark proves that a burst
happened; it does not prove that the queue is still stuck.

## Application-owned instrumentation

MQTTium deliberately does not configure Python logging. Useful message-path
logging is expensive when enabled, can leak topic or payload data and imposes a
global policy on applications.

Instrument the boundaries the service owns:

- time `publish()` and `receipt.wait()` separately;
- count typed exceptions by class and terminal reason;
- sample `stats()` at an interval appropriate for the service;
- log connection transitions, not every payload;
- redact credentials, topics, properties and payloads according to application
  policy.

See [Logging and Observability](observability.md) for an application wrapper example.

## Callback failures

Message callbacks must be short synchronous functions. They run on the
delivering reader outside protocol-engine critical sections. Exceptions and
invalid awaitable returns are sent to the event loop's exception handler and
delivery continues with the next callback or message. Lifecycle hooks may be asynchronous and have separate
ownership; see their [ordering and cancellation rules](reference/async-client.md#lifecycle-hooks).

## Bidirectional pressure

Publication admission and already-decoded protocol completions do not wait for
unrelated inbound application delivery. Incoming messages still obey bounded
queue, byte and ingress limits. The reader stops taking more input when its
current delivery lot cannot progress. An ACK later in the network stream can
therefore remain unread behind incoming traffic.

An iterator consumer that awaits outgoing capacity or an ACK while its own
input queue is saturated can still prevent the needed read. Use an independently
draining consumer plus an application producer with explicit queue/byte bounds
and a nonblocking overflow policy, or separate receiving and publishing
connections. Simply inserting another bounded queue and waiting when it is full
does not remove that dependency. A finite `iterator_admission_timeout` provides bounded
failure, not a promise to sustain an arbitrary offered rate.

Synchronous message callbacks can use `publish_nowait()` with an explicit
refusal policy. Avoid unbounded task creation or busy retry loops. The
[cookbook](cookbook.md#publishing-from-a-message-callback) shows a short handler.

## Graceful shutdown

Keep disconnect and store closure in `finally` blocks. A normal disconnect:

1. stops reconnect attempts;
2. sends DISCONNECT when the transport is connected;
3. allows the writer to drain within its shutdown boundary;
4. closes transport, reader, writer, keepalive and effect work; the reader
   finishes any callback it is already running.

Lifecycle-hook completion is separate: `disconnect()` can return before
`on_disconnect` finishes. Hook cancellation is cooperative; hooks must release
application resources in `finally` blocks.

After shutdown, a public `stats()` snapshot should show `DISCONNECTED`, no
pending subscribe/unsubscribe receipts, no publish waiters, and coherent queues
and windows. Task residency is not part of that contract: `ClientStats` carries
no task section, and `_running_tasks()` is a private maintainer diagnostic
rather than an operational guarantee. Durable inflight records may remain only
when protocol work is intentionally preserved for a broker session; close the
application-owned store after the client.

## Reporting a problem

Retain the MQTTium version, Python version, protocol, broker/version, transport,
client configuration and a minimal reproducer. Include snapshots before and
after the failure, CONNACK or disconnect information, and SQLite/session details
when relevant.

Use the complete checklist in [Reporting Issues](reporting-issues.md).
Performance reports must also include same-machine comparable evidence described
by the [Benchmarking Contract](benchmarking.md).
