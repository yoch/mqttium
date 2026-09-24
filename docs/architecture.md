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
    │     ordered protocol work and completion fences
    ├── DeliveryLane
    │     reader-owned bounded message/replay lot
    ├── LifecycleHooks
    │     one active hook and one latest pending notification
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

`EffectPump` owns protocol-effect ordering, connection epochs, completion
counters and its flush task. SEND/SEND_ACK keep wire order among themselves and
ahead of the remaining ordered work (a local DISCONNECT's close, a protocol
error). A ready single protocol effect can apply inline.

Facts the engine has already observed never wait in that lane. CONNACK,
PUBLISH_COMPLETE/FAILED, SUBACK, UNSUBACK and PINGRESP are applied when their
batch is collected, even while earlier SENDs wait for writer capacity. Their
application depends on no earlier output, and publish admission already
collects pending effects before it reuses an identifier. A broker DISCONNECT
latches its cause and seals the writer at collection, so output parked for
capacity fails at once. A peer protocol error fails a pending CONNACK wait at
collection.
A Server AUTH is handed at collection to one `AuthExchange` task per client,
which runs `auth_handler` calls in arrival order outside the effect lane and the
reader. The handler may therefore await client operations, messages from the
same read, existing receipts or `disconnect()`. Each AUTH Continue carries a
challenge token; `ProtocolEngine.respond_auth()` sends the handler's answer only
while the exchange still waits for that challenge, so an answer to AUTH Success
or after the connection ended is dropped. Connection retirement cancels the
task without joining it.
MESSAGE, DECODED_MESSAGE and CONTINUE_INBOUND_REPLAY belong to a separate
reader-owned `DeliveryLane`, which creates no task of its own.

Each delivery lot carries its epoch and the protocol completion target captured
at collection. The reader waits for that target before accepting the lot;
unrelated later protocol work does not extend the barrier. Delivery then runs
outside engine and effect locks. The reader completes its bounded lot before
reading or decoding another, so queue and byte pressure still reaches the
transport. A replay continuation follows the preceding MESSAGE batch and may
request only the next bounded page after those messages are accepted and marked.
Direct engine consumers retain their existing continuation-pumping contract.

Outgoing publication, manual acknowledgements and subscription/authentication
operations wait for protocol work only. A full delivery queue cannot block
already-decoded terminal results or queue outgoing SENDs behind MESSAGE. An ACK
not yet read remains subject to bounded ingress pressure; the reader does not
scan ahead or retain an unbounded message backlog to find it.

Ready QoS 0 publications use outbound validation and writer admission without
general effects when the current protocol/writer state permits it. A full
application-delivery queue alone does not disable this path. The receipt or
aggregate entry exists before bytes reach the writer; topic aliases commit only
after writer acceptance. A clean refusal can fall back to ordinary admission.
An ambiguous write exception retains the registered prefix and is never retried.
Batch publication does not allocate per-item receipts.

### Network writes

`WritePump` is the only component that writes to the transport. It owns its byte
and message budgets, wake-up condition, batching, and writer task. Wire order is
therefore the same as engine effect order. `max_write_queue_messages` counts
writer-resident admitted frames, including the writer's active batch, not only
`queue.qsize()`. Eager writes do not consume that count.

Large payloads may be written in segments, but only the writer controls the
stream. Capacity is returned after the corresponding queued data is drained.

### Application delivery

`ApplicationDelivery` owns the bounded iterator queue, its byte reservations,
inline synchronous callback invocation, stream close/reset, and delivery
statistics. It deliberately does not own MQTT state, transport state, or
reconnect policy.

Handing a message to the application is the commit point of a persisted
inbound exchange. The reader marks the delivery synchronously at that commit,
with the exchange identity the engine attached to the MESSAGE effect, and only
then may the engine send the PUBCOMP (or the PUBACK of a recovered QoS 1 row)
that ends the exchange.

Topic-filtered callbacks live on `AsyncClient`. `TopicMatcher` chooses which
application callable receives a delivered message; the protocol engine still
emits undifferentiated MESSAGE effects and never imports dispatch code.
Routes and the fallback freeze at the first connection attempt. Registration
rejects async message callbacks before mutation. Matching synchronous routes
execute in registration order for one message, with the fallback used when no
route matches.

The construction-time delivery mode selects either iterator or callback
delivery. In iterator mode every message has one byte charge and one queue
item. Immediate admission checks the same byte/count bounds as waiting
admission and creates neither a coroutine nor a timeout context when capacity
is already available. Waiting admission uses one deadline across byte
reservation and queue insertion.

Message callbacks run synchronously on the delivering reader, with no queue,
worker task or byte reservation in between: the reader hands the current lot
to the application before it decodes the next batch, so callback cost is the
backpressure. Exceptions and invalid awaitable returns are reported to the
loop's exception handler and delivery continues. All routes matching one
message run contiguously; the reader charges every invocation against a private
budget and yields to the loop at the next message boundary once it is reached,
carrying any excess over. The budget cannot preempt synchronous user code.
Lifecycle hooks and publication receipts never touch delivery.

Lifecycle hooks have one retained supervisor, one active child and at most one
pending latest-state notification. Setup/teardown holds and a released lifecycle
lock prevent hook start inside the triggering connection operation. New states
replace obsolete pending states. External lifecycle operations cancel obsolete
active hooks; an operation awaited directly by the current hook preserves its
caller. The supervisor reaps the old child before invoking its successor.
Network operations do not wait for hook completion, and `on_connect` does not
hold incoming delivery. Automatic reconnect waits until `on_disconnect` has
started, not until it returns: the hook may await work that only the
replacement connection completes, and that connection's `on_connect` still runs
after it. Reconnect then rechecks explicit intent. A replacement that drops
before `stable_after` is retried at once; the window only decides whether retry
progression resets. AUTH retains its separate protocol timeout and
task (see Effects).

`messages()` captures the delivery generation when called, even if its returned
iterator is never advanced. Closing and reopening delivery leaves old iterators
terminal. Manual acknowledgements carry a private exchange identity, validated
under the engine lock. That identity survives a genuinely resumed active session
and is retired on exchange completion or session discard; automatic delivery
does not allocate identities or an identity index.

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
- every deferred protocol/delivery effect and writer admission is tagged with
  its epoch and is discarded or rejected after an epoch advance;
- teardown cancels the reader if it is waiting for delivery capacity, releases
  its untransferred reservation and discards unaccepted delivery effects;
- protocol/writer failure preserves its first cause and wakes delivery-bound
  readers; closing only the transport cannot release a queue-capacity wait;
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

Failure before commit unwinds every acquired resource. `publish_many()` commits
each element independently. QoS 1/2 use the unit admission path, draining
protocol effects before packet-identifier reuse and limiting pending aggregate
work to the flow window. Ready QoS 0 items share a bounded private driver: each
item transfers to the existing writer before the source iterator advances.
Pressure or a change of QoS returns to ordinary admission; no retained payload
prefix or input chunk sits outside resource accounting.
SUBSCRIBE and UNSUBSCRIBE are validated under the engine lock first, so a
terminal or invalid request is refused without waiting. Otherwise they drain
earlier protocol effects outside the lock and allocate an identifier only
once none remain, so a deferred SUBACK/UNSUBACK never completes a later
request that reuses its identifier.

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
a packet identifier before the adapter applies its terminal result. The
adapter drains prior protocol effects before another awaited publication can
commit; `publish_nowait()` refuses pending protocol transfer. This preserves
settlement before MID reuse, including within one aggregate. Individual and
batch MID receipt registries remain separate. Unrelated application delivery
cannot hold these terminal results behind a MESSAGE effect.

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

`publish_nowait()` refuses before mutation if pending protocol effects or
protocol/writer capacity prevent immediate transfer. `publish()` waits for bounded transfer;
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
sync callback error/fan-out isolation, lifecycle-hook reentrancy and
coalescing, blocked delivery with independent protocol progress, transport
failure, and clean shutdown.

Protocol fuzzing, memory thresholds, broker integration, and installed-artifact
smokes complement the unit suite. See [`stability.md`](stability.md).

## Deliberate non-goals

- reproducing non-compliant quirks of another client;
- unbounded queues as defaults;
- global logging or metrics registries;
- a generic internal command bus without a clear owner or invariant;
- production guarantees for free-threaded Python before the runtime and
  dependencies can support them.
