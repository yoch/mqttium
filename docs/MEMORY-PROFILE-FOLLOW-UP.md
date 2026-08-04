# Memory Profile Follow-up — status

This document records what the memory audit of MQTTium `main` produced, which of
its recommendations shipped, and which remain open. It supersedes the original
decision document: every option that document presented as open has since been
decided and implemented.

Measurements live in [`MEMORY-BASELINE.md`](MEMORY-BASELINE.md) (before) and
[`MEMORY-RESULTS.md`](MEMORY-RESULTS.md) (after); the harness contract is in
[`MEMORY-BENCHMARK.md`](MEMORY-BENCHMARK.md).

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
| Outbound QoS admission | Independent **count and byte** budgets, reserved before the packet id and the store write. Rejected: reusing the count-only `max_queued` (payload-blind) and one global weighted budget (couples unrelated subsystems, hard to reason about). | `protocol/engine.py` — `_reserve_outbound`, `queue_publish` |
| Cancellation semantics | Cancellation stops *waiting*; it never rolls back a publication already committed to the protocol, which would make delivery non-deterministic. | `api/async_client.py` — `_admit_publish` |
| Effect flushing | One dedicated flusher task, with an inline fast path for the common single-effect case so throughput is unaffected. | `api/async_client.py` — `_apply_effect_inline`, `_run_scheduled_effect_flush` |
| Connection-epoch cleanup | Every effect carries the epoch of the connection that produced it; the epoch is bumped on disconnect and stale effects are dropped. | `api/async_client.py` — `_invalidate_connection_epoch` |
| Inbound delivery budgets | One byte budget shared by iterator and callback delivery, charged once per message and released on the last reference. A fraction is reserved for small messages so a large payload cannot starve telemetry. | `api/async_client.py`, `types.Message._delivery_references` |
| QoS 2 state compaction | Minimal option only: the encoded PUBLISH frame is dropped at `WAIT_PUBCOMP`, where it can no longer be retransmitted. | `protocol/engine.py` — `_on_pubrec` |
| WebSocket framing | Byte-bounded `write_many()` batching (1 MiB); an oversized item is written alone. Not pursued: one-buffer masking, forced fragmentation, native masking — none measured. | `transport/websocket.py` — `_write_frame_batch` |
| SQLite replay and hydration | Keyset-paginated reads plus a payload-free `out_summary_pages()` projection; payloads are materialised only when a message is actually replayed. | `persistence/sqlite.py`, `protocol/engine.py` — `_materialize_outbound` |
| Batch failure retention | Bounded detail retention (`max_failure_details`, default 128) with exact totals preserved and an optional `failure_sink`. | `api/models.py` — `PublishBatchReceipt._record_failure` |
| Peak-container release | `PacketIdPool` and `MemoryInflightStore` rebind their containers when they empty; WebSocket buffers are released on close. | `protocol/packet_ids.py`, `persistence/memory.py`, `transport/websocket.py` |

The measured effect is concentrated on the paths that were unbounded: SQLite
session hydration of 6 000 × 4 KiB dropped from 60.50 MiB to 11.12 MiB RSS and
from 50.62 MiB to 4.51 MiB traced peak. Deliberately unbounded scenarios are
essentially unchanged, and publish throughput was preserved.

Two defects in this work were found and fixed afterwards, both in paths no test
exercised — see the `Fixed` section of [`../CHANGELOG.md`](../CHANGELOG.md):
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
- QoS 2 phase-two state does not retain the original encoded PUBLISH;
- WebSocket peak memory is proportional to a configured frame/batch budget;
- benchmark samples are isolated and distinguish live retention from historical
  RSS peaks;
- throughput and latency are reported alongside the memory improvements.

Not met: "current internal counters return to zero after drain" is asserted for
the inbound delivery budget but not for the outbound admission budget under a
sustained load.

## Open

Identified by the audit and **not** implemented. Tracked in
[`ROADMAP.md`](ROADMAP.md).

- **Ingress byte budget.** The read loop bounds each batch at 256 *packets*
  (`api/async_client.py`, `_read_loop`), not at a byte count. A batch of large
  PUBLISHes is still admitted whole.
- **Decoder copies for large payloads.** A large inbound PUBLISH still costs
  several payload-sized allocations. A specialised in-buffer PUBLISH parse was
  designed but not built. Public zero-copy views were rejected outright: they
  would break the owned-bytes invariant.
- **Full QoS 2 phase-two compaction.** Only the encoded frame is released at
  `WAIT_PUBCOMP`; topic, payload and properties are still retained.
- **QoS 1 / pre-PUBREC frame policy.** Only "keep encoded frames" ships.
  Re-encoding on retransmission, and a size-based policy, were never benchmarked.
- **Observability.** Six counters exist (`pending_outbound_messages`,
  `pending_outbound_bytes`, `pending_delivery_bytes`,
  `pending_delivery_high_water_bytes`, `delivery_small_budget_bytes`,
  `delivery_small_message_limit`). The audit specified roughly twenty, including
  effect-queue, writer, decoder, WebSocket-buffer and receipt counters, a
  high-water mark for the outbound budget, and one unified stats snapshot.
  Constraint from [`LOGGING.md`](LOGGING.md): this may not be added through
  `logging`.
- **Benchmark scenario coverage.** `benchmarks/memory_profile.py` covers seven
  scenarios. Property-heavy outbound, immediate-refusal admission, cancellation
  around the commit point, Paho queue saturation, shared iterator/callback
  accounting, WebSocket byte-bounded batches and reconnect/epoch cleanup are
  covered by unit tests but by no memory measurement, so none of them is
  regression-guarded by numbers.
- **Soak tests.** No sustained reconnect/session/backpressure soak exists.
