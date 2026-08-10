# Memory Profile Follow-up — status

This document records what the memory audit of MQTTium `main` produced and how
its recommendations were closed. It supersedes the original decision document:
every option that document presented as open has been decided, implemented and
given a numeric regression guard where measurement is meaningful.

Measurements live in [`MEMORY-BASELINE.md`](MEMORY-BASELINE.md) (before) and
[`MEMORY-RESULTS.md`](MEMORY-RESULTS.md) (after); the harness contract is in
[`MEMORY-BENCHMARK.md`](../MEMORY-BENCHMARK.md).

## Audit model

The process footprint is the sum of several independently retained layers:

1. application payloads and public receipts;
2. protocol queue and inflight store records;
3. encoded wire frames awaiting the writer;
4. transport-level receive, fragmentation and masking buffers;
5. delivery queues awaiting an iterator or callback consumer;
6. allocator behaviour (peak-sized containers that are never returned).

The dominant problem was layer 2: outbound QoS admission was unbounded, so a
producer outrunning its broker grew the store records, the queue and the encoded
frames together until the 65 535 packet-identifier space ran out.

## Shipped

| Audit item | Decision taken | Where |
| --- | --- | --- |
| Outbound QoS admission | Independent **count and byte** budgets, reserved before the packet id and the store write. Rejected: reusing the count-only `max_queued` (payload-blind, since removed) and one global weighted budget (couples unrelated subsystems, hard to reason about). | `protocol/outbound.py` — `_reserve`, `queue_publish` |
| Cancellation semantics | Cancellation stops *waiting*; it never rolls back a publication already committed to the protocol, which would make delivery non-deterministic. | `api/async_client.py` — `_admit_publish` |
| Effect flushing | One dedicated flusher task, with an inline fast path for the common single-effect case so throughput is unaffected. | `api/_effects.py` — `EffectPump` |
| Connection-epoch cleanup | Every effect carries the epoch of the connection that produced it; the epoch is bumped on disconnect and stale effects are dropped. | `api/_effects.py`, `api/_writer.py`, `api/async_client.py` |
| Inbound delivery budgets | One byte budget shared by iterator and callback delivery, charged once per message and released on the last reference. A fraction is reserved for small messages so a large payload cannot starve telemetry. | `api/async_client.py`, `types.Message._delivery_references` |
| QoS 2 state compaction | Full phase-two compaction: topic, payload, PUBLISH properties and encoded frame are released after PUBREC while logical admission size remains until PUBCOMP. | `protocol/outbound.py` — `on_pubrec` |
| WebSocket framing | Byte-bounded `write_many()` batching (1 MiB); an oversized item is written alone. Not pursued: one-buffer masking, forced fragmentation, native masking — none measured. | `transport/websocket.py` — `_write_frame_batch` |
| SQLite replay and hydration | Keyset-paginated reads plus a payload-free `out_summary_pages()` projection; payloads are materialised only when a message is actually replayed. | `persistence/sqlite.py`, `protocol/outbound.py` — `materialize` |
| Batch failure retention | Bounded detail retention (`max_failure_details`, default 128) with exact totals preserved and an optional `failure_sink`. | `api/models.py` — `PublishBatchReceipt._record_failure` |
| Peak-container release | `PacketIdPool` and `MemoryInflightStore` rebind their containers when they empty; WebSocket buffers are released on close. | `protocol/packet_ids.py`, `persistence/memory.py`, `transport/websocket.py` |

The measured effect is concentrated on the paths that were unbounded: SQLite
session hydration of 6 000 × 4 KiB dropped from 60.50 MiB to 11.12 MiB RSS and
from 50.62 MiB to 4.51 MiB traced peak. Deliberately unbounded scenarios are
essentially unchanged, and publish throughput was preserved.

Two defects in this work were found and fixed afterwards, both in paths no test
exercised — see the `Fixed` section of [`CHANGELOG.md`](../../CHANGELOG.md):
the `publish_many` rollback leaked the byte budget on a transactional store, and
a producer parked on admission capacity was never woken when the connection was
lost for good.

## Acceptance criteria

Met:

- a stalled broker cannot make publisher memory grow beyond configured logical
  budgets;
- `nowait` failure leaves no packet identifier, store record, receipt or effect;
- cancelling a caller cannot strand committed protocol work;
- closing a connection releases or invalidates all transport-epoch buffers;
- a slow or absent consumer is bounded by both message and byte limits;
- SQLite replay memory is proportional to page size, not total session size;
- QoS 2 phase-two state retains neither topic, payload, properties nor the encoded PUBLISH;
- WebSocket peak memory is proportional to a configured frame/batch budget;
- benchmark samples are isolated and distinguish live retention from historical
  RSS peaks;
- throughput and latency are reported alongside the memory improvements;
- current outbound admission counters return to zero after sustained QoS 1 load
  drains.

## Finalisation update

The finalisation pass resolved three additional audit items:

- ingress decode work is now bounded by both packet count and
  `max_ingress_batch_bytes`;
- `AsyncClient.stats()` exposes one immutable snapshot for protocol admission,
  effects, writer, decoder, transports, delivery and receipts, including
  lifetime high-water marks;
- a sustained QoS 1 regression test and the real-broker soak harness assert that
  outbound counters, flow slots, writer entries, effects and receipts return to
  zero after drain.

## Memory scenario closure

The remaining coverage item is closed by seven isolated scenarios covering
property-heavy outbound state, immediate refusal, cancellation before commit,
Paho saturation, shared delivery, WebSocket batching and reconnect/epoch cleanup.
Each scenario has exact logical-work assertions plus a `tracemalloc` peak limit.

Two repeated CPython 3.12 / Ubuntu 24.04 probes produced identical traced peaks
for all seven scenarios. Representative peaks were 15.00 MiB for 2,000
property-heavy records, 6.44 MiB for 1,500 shared-delivery messages and 8.08 MiB
for simultaneous writer/effect/decoder saturation. Immediate refusal peaked at
0.05 MiB. Versioned limits are rounded roughly 25–30% above those observations;
benchmark outputs remain workflow artefacts rather than committed baselines.

No runtime change was justified by these measurements. After harness-owned
references were removed, post-cleanup traced allocations fell to tens of
kilobytes. Remaining RSS returned after diagnostic `malloc_trim`, identifying
allocator arenas rather than live MQTTium ownership.
