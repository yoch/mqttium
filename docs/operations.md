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
| Unfinished outbound QoS state | `max_pending_outbound_messages`, `max_pending_outbound_bytes` |
| Inbound persisted protocol state | `max_pending_inbound_bytes` |
| Encoded writer queue | `max_outbound_messages`, `max_outbound_bytes` |
| Reader processing batch | `max_ingress_batch_bytes` |
| Iterator and callback queues | `max_pending_messages`, `max_pending_callbacks` |
| Retained application-delivery data | `max_pending_delivery_bytes` |
| Broker-facing QoS concurrency | `local_receive_maximum`, `max_outbound_inflight` and negotiated limits |

`max_outbound_messages` bounds writer-resident admitted frames: items still on
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

Use `publish_backpressure="error"`, `publish(..., nowait=True)` or
`publish_nowait()` when the application has a defined shed, retry or spill
policy. Saturation raises `FlowControlError` before allocating a packet
identifier or committing store state.

The Paho facade cannot suspend a synchronous caller for writer progress. It
returns `MQTT_ERR_QUEUE_SIZE` when its cross-thread handoff or native unfinished
publication budget is full. Configure its request and byte limits separately.

### QoS 0 completion is writer admission

MQTT has no broker acknowledgement for QoS 0. MQTTium therefore completes the
`PublishReceipt` and dispatches `on_publish` after the encoded packet has been
admitted to the writer queue. An idle synchronous callback may run inline;
otherwise dispatch uses the bounded callback worker. This boundary does **not**
mean that the transport has written the bytes, that the socket send buffer has
drained, or that the broker has received the publication.

Consequently, an `on_publish` counter is not a socket-level outstanding-byte
limit for QoS 0: incrementing before `publish_nowait()` and decrementing in the
callback can happen within the same event-loop turn while the writer queue keeps
growing. Use `await client.publish(...)` when the producer should wait for
writer capacity. A `publish_nowait()` producer must catch `FlowControlError` and
apply its own shed, retry or spill policy.

A `publish_nowait()` producer sending large payloads will saturate the writer
byte budget (`max_outbound_bytes`, 1 MiB by default) long before it exhausts the
message count, and a producer that merely retries on `FlowControlError` will
busy-spin against it. Shed, slow down, or spill instead — or use
`publish_backpressure="wait"` (the default) with `await client.publish(...)` and
let the client apply the backpressure for you. Do **not** set the pending bounds
to `None` to make the error go away: unbounded queues move the failure from a
catchable exception to memory exhaustion.

Size `max_outbound_bytes` from the encoded bytes that may accumulate during the
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
    snapshot.outbound.pending_messages,
    snapshot.outbound.pending_bytes,
    snapshot.outbound.flow_inflight,
    snapshot.outbound.flow_limit,
)
print("writer", snapshot.writer.queued_messages, snapshot.writer.queued_bytes)
print("delivery", snapshot.delivery.pending_bytes)
print("receipts", snapshot.receipts.publish, snapshot.receipts.publish_batches)
```

The immutable snapshot contains:

- connection state, epoch and reconnect attempt;
- reader, writer, keepalive, reconnect, effect and callback-worker tasks;
- outbound and inbound protocol state and packet identifiers;
- effect-pump and writer queue usage, waiters and high-water marks;
- decoder buffering and ingress limit;
- iterator/callback queue and delivery byte usage;
- pending publish, batch, subscribe and unsubscribe receipts;
- transport buffers and counters where the transport can report them.

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

`AsyncClient(local_receive_maximum=...)` defaults to **100**, not to the
protocol maximum of 65,535 that `EngineConfig` uses for direct-engine consumers.
It is the Receive Maximum MQTTium advertises to the broker, so it bounds how
many inbound QoS 1/2 publications the broker may have unacknowledged at once —
including automatic acknowledgement. A subscriber that needs more inbound
concurrency must raise it explicitly:

```python
client = AsyncClient(local_receive_maximum=1000)
```

The two defaults differ deliberately and both are part of the public contract: the engine
default is the protocol maximum, and the client default is a bounded
application-facing window. Raising it increases the memory the inbound path may
hold. It is unrelated to `max_outbound_inflight`, which bounds *outbound*
unfinished publications and is capped by the broker's own Receive Maximum.

## Timeouts

Timeouts protect different boundaries:

- `connect(..., timeout=...)` limits one connection attempt;
- `ReconnectPolicy.connect_timeout` applies to automatic attempts;
- `ping_timeout` limits the wait for PINGRESP;
- `ack_timeout` is the default SUBACK/UNSUBACK deadline;
- `delivery_timeout` limits waiting for application delivery capacity;
- `callback_shutdown_timeout` limits callback draining during shutdown.

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

Synchronous and asynchronous callbacks run outside protocol-engine critical
sections. An exception is sent to the event loop's exception handler and does
not terminate the reader or leak a delivery reservation.

Install an application exception handler if callback failures need structured
reporting. Do not call blocking compatibility methods from a Paho network-thread
callback; that would wait on the same loop. Schedule work onto another thread or
migrate the callback path to `AsyncClient`.

## Graceful shutdown

Keep disconnect and store closure in `finally` blocks. A normal disconnect:

1. stops reconnect attempts;
2. sends DISCONNECT when the transport is connected;
3. allows the writer to drain within its shutdown boundary;
4. closes transport, reader, writer, keepalive, effects and callback work.

After shutdown, a diagnostic snapshot should show `DISCONNECTED`, no active
tasks, no pending subscribe/unsubscribe receipts and no publish waiters. Durable
inflight records may remain only when protocol work is intentionally preserved
for a broker session; close the application-owned store after the client.

## Reporting a problem

Retain the MQTTium version, Python version, protocol, broker/version, transport,
client configuration and a minimal reproducer. Include snapshots before and
after the failure, CONNACK or disconnect information, and SQLite/session details
when relevant.

Use the complete checklist in [Reporting Issues](reporting-issues.md).
Performance reports must also include same-machine comparable evidence described
by the [Benchmarking Contract](benchmarking.md).
