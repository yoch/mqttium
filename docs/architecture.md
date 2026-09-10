# MQTTium design

MQTTium is built around one idea: protocol correctness and resource ownership
should remain understandable when the network is unreliable and the application
is under load.

## Design goals

| Area | Goal |
| --- | --- |
| Runtime | Native `asyncio` on one application event loop |
| Protocols | MQTT 3.1.1 and MQTT 5 with complete QoS 0/1/2 transitions |
| Memory | Bounded admission, writer, ingress, persistence, and delivery queues |
| Recovery | Reconnect and incremental durable-session replay |
| API | A native client, explicit receipts, immutable diagnostics and two stores |
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
Ready message effects transfer directly to their bounded destination when no
durable delivery mark is required. Full destinations and persisted delivery
marks retain asynchronous transfer. All message and publish notifications run
on the serial callback worker, outside engine critical sections.

Ready unit QoS 0 publications can use outbound validation and writer admission
without creating general effects. This requires a current connection and empty
effect pipeline, and reserves callback capacity before handoff. The receipt
exists before the writer can send bytes; topic aliases commit only after writer
acceptance. An asynchronous clean refusal falls back to ordinary admission;
writer exceptions propagate without retry. Batch publication retains its
progressive unit admission path.

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

Topic-filtered callbacks live on `AsyncClient`. `TopicMatcher` chooses which
application callable receives a delivered message; the protocol engine still
emits undifferentiated MESSAGE effects and never imports dispatch code.
Routes and the fallback freeze at the first connection attempt. Their callable
forms are classified once; matching routes execute in registration order within
one worker job, with the fallback used when no route matches.

The construction-time delivery mode selects either iterator or callback
delivery. Every message has one byte charge and one queue item. Immediate
admission checks the same byte/count bounds as waiting admission and avoids a
timeout context when capacity is already available. Waiting admission uses one
deadline across byte reservation and queue insertion.

All message, connect and publish callbacks use one serial worker, including
synchronous callables. Callback exceptions are isolated from protocol state;
message bytes remain reserved until all matching callbacks finish. An active
callback may disconnect and reconnect: reopening discards old queued work and
lets the same worker serve the replacement connection.

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

The lifecycle preserves these generation invariants:

- only the reader performs application-visible teardown for a live generation;
- the reader retires that generation's keepalive task before reconnect can
  install a replacement task reference;
- every deferred effect and writer admission is tagged with that generation's
  epoch and is discarded or rejected after an epoch advance;
- terminal teardown settles each receipt at most once and wakes every producer
  blocked on protocol or writer capacity;
- reconnectable loss keeps the application message stream open, while terminal
  reconnect exhaustion closes delivery and callback resources;
- intentional disconnect remains distinct from the primary failure cause and
  cannot be reversed by an in-progress reconnect attempt;
- an explicit connect already waiting behind an automatic attempt replaces
  that generation even if the automatic attempt reaches CONNECTED first.

## Protocol ownership

`OutboundSession` is the sole owner of outbound QoS publication state:

- packet identifiers used by publications;
- the negotiated inflight window;
- logical message and byte admission;
- queued and persisted outbound records;
- replay and terminal release;
- connection-scoped outbound Topic Alias mappings.

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

Failure before commit unwinds every acquired resource. `publish_many()` shares
the unit admission path and commits a progressive prefix. It drains effects
between elements and limits pending aggregate QoS 1/2 work to the flow window.

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

A receipt is registered before effects can reach the writer. An ACK can free
a packet identifier while its completion effect still waits behind delivery.
The adapter drains old effects before another awaited publication can commit;
`publish_nowait()` refuses while effects remain pending. This settles the old
receipt before the identifier is registered for a later publication, including
within one aggregate batch receipt.

## Persistence

The internal store interface includes bounded pages and conditional metadata
transitions. The two shipped stores own atomic mutations; the directional
sessions own legal transitions and compensation. Extension protocols and
records are internal, with no supported third-party implementation contract.

SQLite schema 5 accepts new databases and this exact format. Older, future and
inconsistent formats are refused after validation in one WAL-aware read transaction,
before configuring journal mode. Refusal preserves committed schema and data;
normal SQLite recovery and checkpointing may change physical files.
Write batches start lazily on the first mutation. Memory batches only group
internal operations; they do not promise application-level rollback.

Metadata columns precede payload BLOBs. Replay snapshots ordered identifiers and
loads bounded pages by primary key. Inbound replay restores accounting from
metadata before emitting bounded batches. Connection epochs prevent an abandoned
replay from affecting a replacement transport.

## Reconnect and sessions

A reconnect creates a new transport and decoder state. Inbound and outbound
Topic Alias mappings are reset for every network connection. Protocol inflight
state survives only when the broker confirms the session. Durable outbound
records always retain their canonical Topic Name, so replay never depends on a
mapping from the dead connection.

If `session_present` is false, stale persisted work is failed and released. If
it is true, outbound and inbound state is replayed in order and under the same
flow-control limits as new traffic.

MQTT 3.1.1 and later do not require timer-based retransmission on a healthy
connection. MQTTium replays PUBLISH and PUBREL only after reconnect, setting DUP
where required.

## Native API

`AsyncClient.publish_nowait()` is synchronous but event-loop-bound, like
`asyncio.Queue.put_nowait()`. It shares native admission and receipt creation
without pretending to be thread-safe.

`publish_nowait()` refuses before mutation if pending effects or full bounded
queues prevent immediate transfer. `publish()` waits for bounded transfer;
cancellation after commitment can leave a publication active.

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
