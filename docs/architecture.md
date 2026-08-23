# MQTTium design

MQTTium is built around one idea: protocol correctness and resource ownership
should remain understandable when the network is unreliable and the application
is under load.

## Design goals

| Area | Goal |
| --- | --- |
| Runtime | Native `asyncio`; a dedicated thread exists only in the Paho adapter |
| Protocols | MQTT 3.1.1 and MQTT 5 with complete QoS 0/1/2 transitions |
| Memory | Bounded admission, writer, ingress, persistence, and delivery queues |
| Recovery | Reconnect and incremental durable-session replay |
| API | A small native client, explicit receipts, immutable diagnostics, optional Paho bridge |
| Performance | Fast common paths without weakening ownership or fairness |

## Architecture

```text
Application
    │
    ▼
AsyncClient ─────────────── ApplicationDelivery
    │                         callbacks, iterator queues, delivery budget
    ├── EffectPump
    │     ordered application of protocol effects
    ├── WritePump
    │     bounded, single-owner transport writer
    └── ProtocolEngine
          ├── OutboundSession
          ├── InboundSession
          ├── PacketIdPool / FlowControl
          └── InflightStore
                    │
                    ├── MemoryInflightStore
                    └── SqliteInflightStore
```

The protocol engine is synchronous. It knows nothing about `asyncio`, sockets,
user callbacks, or reconnect sleeps. Input packets and API commands produce an
ordered stream of `EngineEffect` values. `AsyncClient` owns the runtime objects
that apply those effects.

This boundary keeps protocol transitions deterministic and lets the engine be
tested without a broker.

## Runtime ownership

### Effects

`EffectPump` owns the effect deque, connection epoch, progress counters, and
flush worker. A single immediately applicable effect is handled inline; it does
not allocate a task or enter the deque. Suspended work is tagged with the
connection epoch so effects from an old transport cannot modify a new session.
For QoS 0 writer admission and terminal QoS 1/2 publishes, inline application
may admit `on_publish` to the bounded callback queue and settle the receipt
immediately. User callback code still runs only in the callback worker; a full
queue falls back to the ordered effect path and its existing backpressure. QoS
0 batches preflight capacity for every callback before admitting any write, so
the direct path cannot split a batch across the two paths.

### Network writes

`WritePump` is the only component that writes to the transport. It owns its byte
and message budgets, wake-up condition, batching, and writer task. Wire order is
therefore the same as engine effect order. `max_outbound_messages` counts
writer-resident admitted frames, including the writer's active batch, not only
`queue.qsize()`. Eager writes do not consume that count.

Large payloads may be written in segments, but only the writer controls the
stream. Capacity is returned after the corresponding queued data is drained.

### Application delivery

`ApplicationDelivery` owns callback and iterator queues, byte reservations, the
user-callback worker, shutdown/reset, and delivery statistics. It deliberately
does not own MQTT state, transport state, or reconnect policy.

The delivery mode is selected when the client is constructed. Specialised
admission functions avoid repeated mode branches on every incoming message
while preserving one authoritative owner for reservations and lifecycle.

Callback exceptions are isolated from protocol state. A message delivered to
both a callback and an iterator releases its byte reservation only after both
references are gone.

### Ingress

The incremental decoder owns its reusable input buffer. It never exposes a
`memoryview` backed by that buffer to user code. Complete packet bytes cross the
engine boundary with stable ownership.

Ingress work is drained in bounded batches so a large read cannot starve
outbound acknowledgements or application delivery.

### Connection lifecycle

The reader task is the single owner of teardown for a live connection. A writer,
keepalive, or deferred-effect failure records its primary cause and breaks the
transport; the reader then performs the ordered cleanup and decides whether the
outcome is terminal or enters reconnect policy. Secondary errors while closing
the transport never replace that primary cause. Connection epochs prevent a
late task from an older transport from closing or poisoning its replacement.

## Protocol ownership

`OutboundSession` is the sole owner of outbound QoS publication state:

- packet identifiers used by publications;
- the negotiated inflight window;
- logical message and byte admission;
- queued and persisted outbound records;
- replay and terminal release.

`InboundSession` symmetrically owns incoming PUBLISH state:

- inbound topic aliases;
- local Receive Maximum accounting, including auto-acknowledgements still
  inside the current effect batch;
- inbound QoS 1/2 persistence;
- duplicate suppression, manual acknowledgement, and replay.

Both sessions emit into the engine's one effect stream. Neither owns connection
state or a second effect list.

The packet-identifier pool spans 1 through 65535. Receive Maximum limits only
unfinished QoS 1/2 publications; it does not shrink the identifier space.

## Admission and backpressure

Outbound QoS 1/2 work is admitted in this order:

1. validate the operation and negotiated limits;
2. compute logical size;
3. reserve message and byte capacity;
4. allocate a packet identifier;
5. write the inflight record;
6. emit effects.

Failure before commit unwinds every acquired resource. Batched operations take a
counter snapshot and restore it as a unit if their transaction rolls back.

Applications wait for capacity by default. Immediate mode raises
`FlowControlError`. A terminal disconnect wakes blocked publishers with an
error; an active reconnect keeps them waiting because replay may release the
budget.

Inbound persistence and application delivery have separate byte limits because
they cover different lifetimes. MQTT 5 reports an inbound quota violation with
reason `0x97`; MQTT 3.1.1 closes the connection because it has no equivalent
reason code.

## Receipts and completion

`PublishReceipt.wait()` means:

- QoS 0: admitted by the writer — queued, or buffered straight to the transport
  when that costs no ordering (see `implementation-guide.md` invariant 1).
  Neither means the bytes have reached the network;
- QoS 1: PUBACK received;
- QoS 2: PUBCOMP received.

A receipt is registered before effects can reach the writer. Completion is
emitted before a packet identifier is returned to the pool, preventing a late
acknowledgement from completing a later publication that reused the same ID.

## Persistence

`InflightStore` exposes the correctness-oriented object interface. Optional
paged and transition protocols provide efficient replay and acknowledgement
without changing protocol ownership.

Conditional transitions include the expected state. The store guarantees
atomicity; the session decides which transition is legal. Third-party stores
without the optional protocols use a correct eager fallback.

`SqliteInflightStore` versions its schema with `PRAGMA user_version`. Migrations
run in one transaction, newer unknown schemas are rejected, and write batches
start lazily only when the first mutation occurs.

Metadata needed for acknowledgement appears before payload BLOBs in the schema.
This lets a PUBACK settle a large publication without reading its payload.
Replay snapshots ordered identifiers once and loads bounded pages by primary
key, avoiding repeated full-table sorts.

Incoming replay first restores accounting from metadata and then emits bounded
batches. A continuation effect carries the connection epoch, so disconnecting
mid-replay safely abandons the old cursor.

## Reconnect and sessions

A reconnect creates a new transport and decoder state. Topic aliases are reset
for every network connection. Protocol inflight state survives only when the
broker confirms the session.

If `session_present` is false, stale persisted work is failed and released. If
it is true, outbound and inbound state is replayed in order and under the same
flow-control limits as new traffic.

MQTT 3.1.1 and later do not require timer-based retransmission on a healthy
connection. MQTTium replays PUBLISH and PUBREL only after reconnect, setting DUP
where required.

## Native and compatibility APIs

`AsyncClient.publish_nowait()` is synchronous but event-loop-bound, like
`asyncio.Queue.put_nowait()`. It shares native admission and receipt creation
without pretending to be thread-safe.

The Paho façade owns a bounded cross-thread ingress queue and commits work on the
client loop. It never mutates the protocol engine, receipt registry, or effect
pump directly. Compatibility stops where historical behaviour would violate
MQTT correctness, ordering, or bounded-resource guarantees.

## Observability

`AsyncClient.stats()` assembles immutable snapshots from the components that own
the underlying state. It does not maintain a parallel registry or start a
sampler. High-water marks and decision counters are updated at natural batch or
state-transition boundaries rather than by adding logging to every hot path.

See [`observability.md`](observability.md) for the no-library-logging decision.

## Performance rules

Optimisation is allowed only after correctness and ownership are preserved.

- Count calls and allocations before changing code.
- Keep one owner for every queue, budget, and state machine.
- Prefer construction-time specialisation to repeated hot-path branching.
- Do not inline user callbacks into protocol critical sections.
- Do not remove fairness yields or bounded queues to improve a benchmark.
- Confirm retained changes with paired controls under
  [`benchmarking.md`](benchmarking.md).

## Required tests

Changes to the engine or client must cover malformed incremental input, all QoS
transitions and duplicates, packet-identifier exhaustion and reuse, reconnect
with and without a broker session, persistence rollback, bounded admission,
callback reentrancy, transport failure, and clean shutdown.

Protocol fuzzing, memory thresholds, broker integration, and installed-artifact
smokes complement the unit suite. See [`stability.md`](stability.md).

## Deliberate non-goals

- reproducing non-compliant quirks of another client;
- unbounded queues as defaults;
- global logging or metrics registries;
- a generic internal command bus without a clear owner or invariant;
- production guarantees for free-threaded Python before the runtime and
  dependencies can support them.
