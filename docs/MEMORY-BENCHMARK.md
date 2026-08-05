# Memory Benchmark Methodology

## Purpose

`benchmarks/memory_profile.py` measures memory retained by MQTTium paths that are
not adequately described by throughput benchmarks. It is designed to answer:

- which layer owns the retained bytes;
- whether memory growth is proportional to configured logical capacity;
- how much memory remains after queues and stores are drained;
- whether a change improves memory without relying on a historical process peak;
- how memory changes compare with throughput and latency changes.

The benchmark is diagnostic. It is not a universal prediction of production RSS.
Allocator, Python, libc, SQLite, TLS, and kernel versions affect absolute values.
Comparisons should use the same runner image and Python version whenever possible.

## Process isolation

Every scenario runs in a fresh child process. This is required because
`resource.getrusage(...).ru_maxrss` is a process-lifetime maximum and Python's
allocator can retain arenas after objects are released.

The parent process only launches children and aggregates their JSON output. A
scenario therefore cannot inherit another scenario's peak RSS, Python arenas,
SQLite cache, or queue contents.

## Memory sources

Each phase records:

- **RSS**: resident memory currently mapped into the process;
- **USS**: memory unique to the process;
- **PSS**: shared pages divided proportionally between processes;
- **max RSS**: process-lifetime peak for this isolated child;
- **traced current**: current allocations visible to `tracemalloc`;
- **traced peak**: peak Python-traced allocations since the scenario reset;
- **logical counters**: MQTTium messages, packet identifiers, records, and
  payload/topic bytes owned by the scenario.

RSS includes memory that `tracemalloc` cannot see, including parts of SQLite,
OpenSSL, libc, socket buffers, extension modules, and interpreter internals.

## Logical byte accounting

The agreed logical admission model counts:

```text
payload bytes + UTF-8 topic bytes + encoded property bytes
```

It intentionally does not add a guessed fixed Python-object overhead. The
benchmark still exposes the real RSS and USS amplification caused by objects and
containers, especially through the 64-byte-payload protocol scenario.

Most baseline scenarios do not attach MQTT properties, so their logical total is
payload plus topic. `property_heavy_outbound_4k` attaches independently owned
MQTT 5 property bags and verifies that encoded property bytes participate in
outbound admission accounting.

## Phases

Most scenarios emit these snapshots:

1. `baseline`: imports are complete and garbage collection has run;
2. `loaded`: the target queue or store owns all requested messages;
3. `released`: MQTTium state has been drained or cleared and garbage collection
   has run;
4. `released_after_malloc_trim`: on glibc Linux, `malloc_trim(0)` was attempted
   to distinguish live retention from reclaimable allocator arenas.

The SQLite scenario uses `baseline_before_hydration`. Database creation happens
before that phase, then garbage collection and allocator trimming are requested.
The measured operation is opening and hydrating the durable session.

`malloc_trim` is diagnostic only. MQTTium does not call it in normal operation.

## Baseline scenarios

### `protocol_qos_queue_64b`

Queues 30,000 unique QoS 1 messages while disconnected. It emphasizes per-message
Python object, dictionary, deque, receipt-adjacent, and packet-id overhead rather
than payload size.

### `protocol_qos_queue_4k`

Queues 12,000 unique 4 KiB QoS 1 messages while disconnected. It exposes the
unbounded protocol queue and its payload retention independently of the writer
queue.

### `iterator_delivery_queue_4k`

Fills the public iterator delivery queue with 6,000 unique 4 KiB messages and no
consumer. It establishes the baseline for count-only inbound backpressure before
a shared byte budget is introduced.

### `protocol_bounded_queue_4k`

Attempts 6,000 unique 4 KiB QoS 1 messages with no count limit and an 8 MiB
logical byte limit. It verifies that admission rejects excess messages without
retaining packet identifiers or store records.

### `iterator_delivery_budget_4k`

Fills an iterator delivery budget to approximately 8 MiB and verifies that the
next message remains blocked until the consumer releases a reference.

### `memory_store_4k`

Stores 12,000 unique 4 KiB outbound records in `MemoryInflightStore`, then clears
the store. It measures active retention and idle-container cleanup.

### `sqlite_hydration_4k`

Creates 6,000 queued 4 KiB records without retaining a Python list, then opens a
new store and constructs `ProtocolEngine`. It measures paged payload-free
hydration and lazy payload materialization. Full payloads are loaded only when a
protocol inflight slot becomes available.

## Extended audit scenarios

### `property_heavy_outbound_4k`

Queues 2,000 disconnected MQTT 5 QoS 1 publications with independent expiry,
content, response, correlation and user-property values. It measures the payload,
property bag and encoded-property contribution without sharing one synthetic bag.

### `immediate_refusal_4k`

Attempts 20,000 QoS 1 publications against a one-message admission limit. Exactly
one record and packet identifier must remain; every refusal is checked explicitly.

### `cancelled_admission_4k`

Keeps one publication admitted, parks 511 callers before the commit point and
cancels them. The scenario verifies that no cancelled caller leaves a packet id,
store record, receipt or admission waiter behind.

### `paho_saturation_4k`

Submits 5,000 synchronous Paho-compatible QoS 1 publications across the network
thread boundary with a 512-message protocol limit. Accepted and rejected return
codes, store records and packet identifiers must agree exactly.

### `shared_delivery_both_4k`

Fills iterator and callback delivery with 1,500 messages. Each message has two
references but its logical bytes are charged once, then released after both
consumers finish.

### `websocket_batching_4k`

Masks 300 frames through `write_many()`, enough to cross the 1 MiB batch boundary
once. A counting writer deliberately retains no frame, so the measured peak is
the transport's current batch rather than an artificial history of every write.

### `reconnect_epoch_cleanup_4k`

Saturates the writer queue, stale-effect deque and decoder, advances the
connection epoch and force-closes the client. Exact counters require all three
owners to be empty afterwards; the harness releases its own payload references
before taking the post-cleanup snapshot.

## Running locally

Install benchmark dependencies and run:

```bash
python -m pip install -e ".[dev]" "psutil>=6"
python benchmarks/memory_profile.py \
  --label local-baseline \
  --output /tmp/mqttium-memory-profile.json
```

For a faster smoke run:

```bash
python benchmarks/memory_profile.py \
  --scale 0.1 \
  --label local-smoke \
  --output /tmp/mqttium-memory-profile-smoke.json
```

`--scale` changes message counts but not payload sizes.

## Comparing commits

For each scenario, compare at least:

- loaded RSS, USS, and traced-current deltas from baseline;
- traced peak and isolated max RSS;
- released RSS/USS before and after optional allocator trimming;
- logical message and byte counts;
- operation duration;
- existing publisher, delivery, TLS, WAN, and persistence throughput results.

A memory correction is not complete if it lowers RSS by silently reducing the
workload, dropping messages, changing QoS guarantees, or introducing an
unreported throughput/latency regression.

## Interpretation rules

- A large `traced_current` delta indicates live Python-owned objects.
- A large RSS delta with a much smaller traced delta suggests native allocations,
  allocator fragmentation, SQLite, TLS, or other non-traced memory.
- A large `released` delta that falls after `malloc_trim` indicates reclaimable
  allocator retention rather than live MQTTium references.
- A large delta remaining after drain and trim suggests a live reference or a
  native subsystem that retains memory intentionally.
- A logical byte limit should be evaluated against logical counters; RSS is used
  to measure amplification, not as the admission counter itself.

## Coverage policy

All memory paths identified by the audit now have isolated scenarios and
versioned thresholds. New scenarios should be added only for a new owner,
backpressure policy or measured regression risk; benchmark breadth is not a goal
by itself.
