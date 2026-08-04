# Memory Profile Follow-up

## Status

This document records the remaining memory work identified by the static audit of
MQTTium `main` and separates low-risk cleanup from changes that require an API,
correctness, or performance decision.

The accompanying pull request intentionally implements only changes that do not
alter public behavior or backpressure semantics:

- release peak-sized `PacketIdPool` containers when the pool becomes idle or is
  explicitly cleared;
- release peak-sized dictionaries in `MemoryInflightStore` when the last inbound
  or outbound record is removed, and replace them on explicit clear;
- release WebSocket receive, fragmentation, and pending-control buffers when the
  transport is closed;
- add targeted regression tests for those cleanup paths.

The pull request does **not** claim to solve the dominant under-load memory
problem. The primary remaining issue is that outbound QoS admission is not
bounded by a payload-aware budget before protocol state is committed.

## Audit model

The current process footprint is the sum of several independently retained
layers:

1. application payloads and public receipts;
2. protocol queue and inflight store records;
3. encoded MQTT write items and pending engine effects;
4. writer queue and asyncio transport buffers;
5. inbound decoder, delivery, and callback queues;
6. WebSocket framing and fragmentation buffers;
7. SQLite rows, page cache, reconstructed Python objects, and replay effects;
8. Python allocator arenas and peak-sized container capacity.

A useful fix must therefore state which layer it bounds or releases. A small
writer queue alone cannot guarantee bounded process memory when a larger
protocol queue exists upstream.

# Priority 0: outbound QoS admission

## Problem

For QoS 1 and QoS 2, `ProtocolEngine.queue_publish()` allocates a packet
identifier, creates an `OutboundMessage`, stores it, and appends it to the
protocol queue when the negotiated inflight window is full.

`EngineConfig.max_queued` defaults to zero, meaning unlimited, and the native
client does not currently expose it. The practical ceiling is therefore the
65,535 packet identifier space rather than a memory budget.

The writer limits (`max_outbound_bytes` and `max_outbound_messages`) apply only
after messages have been encoded into writer items. They do not bound payloads
retained by the protocol queue or inflight store.

## Required properties

A replacement admission mechanism should provide all of the following:

- a finite default;
- limits in both message count and estimated retained bytes;
- reservation before packet-id allocation and store mutation;
- exact release on completion, failure, rollback, or session discard;
- consistent behavior for `publish()`, `publish_many()`, and the Paho facade;
- an explicit non-blocking mode;
- metrics for current and high-water usage;
- no dependence on the broker's Receive Maximum for local memory safety.

## Option A: expose the existing count-only queue limit

Add `max_queued_messages` to `AsyncClient` and map it to
`EngineConfig.max_queued`.

### Advantages

- small implementation;
- immediately prevents use of all packet identifiers;
- easy to explain and test.

### Limitations

- a count-only limit is not a memory limit;
- 1,000 one-byte payloads and 1,000 sixteen-megabyte payloads receive the same
  treatment;
- encoded frames, properties, topics, and receipts are not represented in the
  budget.

### Assessment

Useful as an emergency guardrail, but insufficient as the final design.

## Option B: independent count and byte budgets

Add explicit limits such as:

```python
AsyncClient(
    max_queued_messages=...,
    max_queued_bytes=...,
    max_pending_delivery_bytes=...,
)
```

Reserve an estimated retained cost before mutating protocol state. Reconcile the
estimate after encoding where necessary.

### Advantages

- directly addresses the observed failure mode;
- understandable configuration;
- permits different publisher and subscriber budgets;
- can be added without coupling all internal layers.

### Limitations

- requires a defined accounting model;
- shared immutable payload references can make exact physical-byte accounting
  impossible;
- protocol, writer, and transport copies still need separate observation.

### Recommendation

This is the recommended first complete solution. The budget should represent
logical retained bytes rather than attempting to predict exact RSS. It should
include at least payload length, topic bytes, encoded properties, and a fixed
per-record overhead estimate.

For inbound delivery, the implementation may split the byte budget into a
small count-derived pool and an exact-accounted large-message pool. The small
pool is safe only when its per-message threshold is derived from the maximum
number of retained iterator/callback entries, and it must be disabled when the
partition would reduce single-packet capacity. This preserves the hard bound
without imposing per-message accounting on common telemetry payloads.

The default values should be selected after the new benchmark harness is
available. Defaults must be finite even if conservative.

## Option C: one global weighted memory budget

Create a single client-level budget shared by protocol state, effects, writer
items, delivery queues, and callback jobs.

### Advantages

- strongest global bound;
- avoids one layer consuming memory reserved for another;
- enables one coherent high-water metric.

### Limitations

- substantially more invasive;
- ownership transfers between layers become complex;
- shared references must not be charged repeatedly without a clear policy;
- easy to introduce deadlocks or reservation leaks.

### Assessment

A possible later architecture, but too complex for the first corrective step.
Independent budgets with good observability should come first.

# Priority 0: cancellation and non-blocking publication semantics

## Problem

Protocol state and receipts are currently created before the generated effects
are admitted to the writer queue. If effect draining is cancelled or
`nowait=True` encounters writer backpressure, committed state can remain without
a receipt being returned to the caller.

This is both a memory-retention risk and a delivery-semantics problem.

## Decision 1: what does cancellation mean after protocol commit?

### Option A: cancellation rolls back publication

Cancellation before writer admission would remove the store record, release the
packet identifier, remove the receipt, and delete the pending effect.

#### Advantages

- intuitive coroutine semantics;
- no hidden publication after the caller sees cancellation.

#### Risks

- rollback is unsafe once any bytes may have reached the transport;
- determining that boundary across writer and transport buffers is difficult;
- reconnect/session state can make rollback protocol-sensitive.

### Option B: cancellation stops waiting but does not cancel committed delivery

Once admission succeeds and protocol state is committed, the internal delivery
continues independently. Cancelling the API coroutine only stops the caller's
wait. The receipt must be created and made available before the cancellable
stage, or the API must expose a separate submission object.

#### Advantages

- aligns with durable QoS semantics;
- avoids unsafe rollback after possible wire emission;
- simpler internal ownership.

#### Risks

- callers must understand that cancellation is not message cancellation;
- the current API shape may need adjustment to guarantee receipt access.

### Recommendation

Use Option B after a clearly defined admission/commit point. Before that point,
cancellation should leave no state. After that point, delivery should be owned
by an internal task and survive caller cancellation.

## Decision 2: effect flushing architecture

### Option A: producer-owned draining

Each API call continues to drain effects directly, with additional rollback and
cancellation handling.

#### Advantages

- smaller change;
- fewer background tasks.

#### Risks

- effect progress remains coupled to arbitrary caller lifetimes;
- several producers can contend on the flush lock;
- cancellation paths remain difficult to reason about.

### Option B: one dedicated effect flusher

API calls transfer effects to a connection-epoch queue and signal one persistent
flusher task. The flusher owns writer admission and is not cancelled with an
individual caller.

#### Advantages

- clear ownership;
- automatic progress after producer cancellation;
- easier queue accounting and instrumentation;
- natural place to reject stale connection-epoch effects.

#### Risks

- lifecycle and shutdown logic become more important;
- failures must be routed back to receipts reliably;
- tests must cover wakeup coalescing and reconnect races.

### Recommendation

Use a dedicated flusher. This is the cleanest foundation for bounded admission,
epoch safety, and cancellation-independent progress.

# Priority 0: connection-epoch cleanup

## Problem

Closing a client cancels the writer and discards pending engine effects, but old
writer-queue items and their byte accounting need a formally safe cleanup path.
Simply clearing the queue is not enough because another coroutine may already be
waiting inside writer admission and can wake after the clear.

The decoder can also retain connection-scoped input until the next connection
reset if the client remains closed.

## Required design

Introduce a monotonically increasing connection epoch and associate it with:

- pending send effects;
- writer queue items;
- admission reservations;
- the active flusher and writer tasks.

On close:

1. invalidate the epoch;
2. cancel the old writer/flusher;
3. reject or release old-epoch reservations;
4. drain old writer items and reset byte accounting;
5. clear decoder and transport-scoped buffers;
6. wake blocked producers, which must re-check the epoch before committing.

This should be implemented together with the dedicated flusher rather than as an
isolated queue clear.

# Priority 1: inbound delivery budgets

## Problem

The iterator and callback queues are bounded by item count, not payload bytes.
The default iterator limit of 65,536 messages can therefore represent a very
large amount of retained data.

The reader also drains every complete MQTT frame already present in the decoder
before applying delivery backpressure. One large socket read containing many
small frames can produce a large engine-effect burst.

## Recommended changes

- add byte accounting to iterator delivery;
- add byte accounting to callback delivery;
- process ingress with both packet and byte budgets;
- transfer and apply effects between ingress batches;
- yield to the event loop between sufficiently large batches;
- expose current and high-water delivery queue metrics.

## Packet-count versus byte-count policy

Use both. Packet count controls Python object overhead and pathological tiny
messages; byte count controls payload retention.

A batch should stop when either limit is reached.

# Priority 1: QoS state compaction

## QoS 2 after PUBREC

After a successful PUBREC, retransmission requires PUBREL rather than the
original PUBLISH. The current in-memory record can still retain the encoded
PUBLISH alongside the original payload.

### Minimal option

Set `encoded_publish = None` when transitioning to `WAIT_PUBCOMP`.

This appears low risk and should be implemented with a targeted reconnect test.
It was kept out of the cleanup pull request because it changes the retained
protocol record rather than only releasing an idle container.

### Full compaction option

Replace the record with a compact phase-two representation containing only:

- packet identifier;
- `WAIT_PUBCOMP` state;
- encoded PUBREL or enough data to regenerate it.

This would also release topic, payload, retain flag, and PUBLISH properties.

The full option has a larger persistence/schema impact and should be designed
explicitly.

## QoS 1 and QoS 2 before PUBREC

MQTTium retains both the source payload and an encoded PUBLISH for active
messages below the segmentation threshold.

### Option A: keep encoded frames

Optimizes retransmission throughput at the cost of approximately one additional
payload-sized representation.

### Option B: re-encode on retransmission

Reduces retained memory but increases reconnect/retry CPU.

### Option C: size-based policy

Cache encoded frames only below or above a configurable threshold, depending on
benchmark results.

### Recommendation

Benchmark Option B and Option C. The best policy may differ for small telemetry
messages and large binary payloads.

# Priority 1: WebSocket framing

## Current issue

Client-side WebSocket masking creates a complete masked payload and then a
complete frame containing another copy. `write_many()` also materializes every
frame in the current MQTT writer batch before calling `writelines()`.

A direct one-buffer Python implementation was prototyped during this work. It
reduced one full-size temporary allocation but was approximately 9–19% slower in
a small isolated benchmark for medium and large payloads. It was therefore not
included as an obvious fix.

## Options

### Option A: accept the Python one-buffer implementation

Best peak-memory behavior, but with a measured CPU/throughput regression that
must be validated in the real benchmark.

### Option B: bound WebSocket frame batching by bytes

Keep the current masking implementation but stop materializing up to 256 large
frames at once. Flush after a configurable byte budget.

This is likely the best first WebSocket change because it limits aggregate peak
memory without changing masking code.

### Option C: chunk and fragment large WebSocket messages

Mask and send bounded fragments rather than one complete frame.

This can provide strong peak bounds but increases frame overhead and may change
write/latency characteristics.

### Option D: optimized native masking path

Use a C-accelerated implementation or optional extension for large frames.

This offers the best potential CPU and memory result but adds packaging and
maintenance cost.

## Recommendation

Implement byte-bounded `write_many()` first, then compare one-buffer masking and
fragmented large-message sending under TCP, TLS, WS, and WSS benchmark profiles.

# Priority 1: decoder copies

## Current issue

The incremental decoder copies a complete MQTT packet body out of its reusable
`bytearray`. PUBLISH decoding then slices the payload into another `bytes`
object. Large inbound messages can therefore create several simultaneous
payload-sized allocations.

## Options

### Option A: keep owned packet bodies

Preserves the simple lifetime guarantee that no public object aliases a reusable
buffer.

### Option B: specialized PUBLISH decode from the reusable buffer

Parse topic, packet identifier, and properties by index, then copy only the
final payload that must survive delivery.

This removes the full intermediate `remaining` copy for PUBLISH while retaining
owned public payload bytes.

### Option C: public zero-copy views

Expose `memoryview` or a leased buffer to the application.

This changes API and lifetime semantics substantially and is not recommended for
the default API.

### Recommendation

Use a specialized internal PUBLISH path as an optimization after the admission
and queue bounds are fixed.

# Priority 1: SQLite replay and hydration

## Current issue

`SqliteInflightStore.out_items()` and `in_items()` use `fetchall()`. Engine paths
then commonly wrap the returned iterator in `list()`. Startup and reconnect can
therefore hold all SQLite rows, reconstructed message objects, protocol queues,
and replay effects simultaneously.

## Recommended architecture

- add paged store iteration using `fetchmany()`;
- avoid wrapping store iterators in `list()`;
- replay a bounded number of records and bytes per event-loop turn;
- keep only lightweight record identifiers queued when the durable store is the
  source of truth;
- load the full payload only when an inflight slot becomes available;
- record replay batch count, bytes, and latency.

## Concurrency note

A cursor must not remain exposed while arbitrary store mutations occur. The
store API should return explicit pages or snapshots rather than a generator that
holds a database lock across caller yields.

# Priority 2: batch failure retention

## Problem

`PublishBatchReceipt` keeps every failure in a dictionary. A very large input
where every publication fails is therefore not memory-bounded even though the
successful path keeps only a bounded pending window.

## Options

- retain all failures for exact diagnostics;
- retain only the first and last N failures plus a total count;
- aggregate by exception type/reason code;
- stream failures to an application-provided sink.

## Recommendation

Use bounded samples plus aggregate counts by default, with an explicit opt-in for
full failure retention.

This is a public diagnostics-contract decision and should not be changed
silently.

# Priority 2: observability

A memory-safe implementation still needs internal evidence showing where bytes
are retained. Add a stable statistics snapshot with at least:

```text
protocol.queued_messages
protocol.queued_payload_bytes
protocol.inflight_messages
protocol.inflight_payload_bytes
protocol.inbound_inflight
protocol.store_out_records
protocol.store_in_records

effects.pending_count
effects.pending_bytes

writer.queue_count
writer.queue_bytes
writer.transport_buffer_bytes

delivery.iterator_count
delivery.iterator_bytes
delivery.callback_count
delivery.callback_bytes

receipts.single_count
receipts.batch_pending_count

decoder.buffered_bytes
websocket.recv_buffer_bytes
websocket.fragment_bytes
```

Metrics should include current values and high-water values. They should be
cheap enough to leave enabled in benchmark and production diagnostics.

# Benchmark redesign

## Problems with the current application stress benchmark

The current script uses `resource.getrusage(...).ru_maxrss`, which is a process
lifetime maximum. Scenarios run sequentially in one process, so later results
inherit earlier peaks. The benchmark also retains its own `seen` and `records`
lists, which can dominate or contaminate subsequent samples.

## Required harness properties

Each memory scenario should run in a fresh child process and record:

- baseline RSS after imports;
- current RSS throughout the run;
- USS and PSS where available;
- process peak RSS;
- Python traced current and peak allocations;
- MQTTium internal current and high-water counters;
- queue depths and logical retained bytes;
- throughput, ACK latency, and delivery latency;
- memory after producer stop, complete drain, disconnect, and quiescence.

The parent process should collect structured JSON from each child.

## Scenario matrix

At minimum, cover:

- QoS 0, 1, and 2;
- `publish()`, `publish_many()`, and Paho-compatible `publish()`;
- TCP, TLS, WebSocket, and secure WebSocket;
- payload sizes 0, 64 B, 4 KiB, 1 MiB, and the configured maximum;
- regulated and open-loop producers;
- normal, delayed, and absent ACKs;
- fast, slow, and absent consumers;
- iterator, callback, and dual delivery;
- memory and SQLite stores;
- stable connections, reconnect, and large session replay;
- small and large Receive Maximum values.

## Profiling tools

Use allocation tracing for two separate questions:

1. Python object retention and ownership;
2. native allocations and temporary peaks from SSL, SQLite, socket, and
   interpreter internals.

Memray or an equivalent allocator profiler should be run in both Python-focused
and native modes. Tracemalloc remains useful for cheap regression assertions but
cannot explain all RSS.

# Proposed implementation sequence

## Phase 1: establish trustworthy evidence

1. Add current RSS/USS/PSS child-process benchmark isolation.
2. Add internal queue/store/receipt counters.
3. Reproduce the reported benchmark shape and classify retained bytes by layer.

## Phase 2: prevent unbounded outbound growth

1. Expose a finite emergency `max_queued_messages` limit.
2. Add logical byte-budget admission before protocol mutation.
3. Define native async and Paho non-blocking behavior.
4. Add cancellation and `nowait` atomicity tests.

## Phase 3: make effect ownership and close safe

1. Introduce a dedicated effect flusher.
2. Add connection epochs to effects, writer items, and reservations.
3. Drain invalidated writer queues and decoder state safely.
4. Verify reconnect and durable receipt behavior under cancellation.

## Phase 4: reduce large-payload amplification

1. Compact QoS 2 phase-two state.
2. Bound ingress and WebSocket batches by bytes.
3. Page SQLite hydration and replay.
4. Evaluate PUBLISH re-encoding versus encoded-frame caching.
5. Evaluate specialized inbound PUBLISH parsing.

## Phase 5: tighten secondary bounds

1. Bound batch failure details.
2. Add byte budgets for iterator and callback delivery.
3. Add long-running reconnect/backpressure soak tests.

# Acceptance criteria

The memory program should not be considered complete until all of the following
hold:

- a stalled broker cannot make publisher memory grow beyond configured logical
  budgets;
- `nowait` failure leaves no packet identifier, store record, receipt, or effect;
- cancelling a caller cannot strand committed protocol work;
- closing a connection releases or invalidates all transport-epoch buffers;
- a slow or absent consumer is bounded by both message and byte limits;
- SQLite replay memory is proportional to page size, not total session size;
- QoS 2 phase-two state does not retain the original encoded PUBLISH;
- WebSocket peak memory is proportional to a configured frame/batch budget;
- current internal counters return to zero after drain where protocol semantics
  permit it;
- benchmark samples are isolated and distinguish live retention from historical
  RSS peaks;
- throughput and latency regressions are reported alongside memory improvements.

# Recommended immediate next decision

The first decision should be the public admission contract:

1. finite default count and byte limits;
2. native async blocking behavior;
3. Paho-compatible non-blocking failure behavior;
4. cancellation semantics after the admission/commit point.

Once those rules are fixed, the dedicated flusher and connection-epoch cleanup
can be implemented coherently rather than as a series of local queue patches.
