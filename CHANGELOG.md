# Changelog

All notable changes to MQTTium are documented here.

The format follows Keep a Changelog and versions follow Semantic Versioning.

## [Unreleased]

## [0.1.0a4] - 2026-08-06

### Added

- `TransitionInflightStore`, an optional store extension for atomic, conditional
  and payload-free record transitions (`complete_out`, `transition_out`,
  `contains_in`, `in_meta`, `mark_in_delivered`, `transition_in`, `complete_in`,
  `in_index_pages`, `set_out_logical_size`). `MemoryInflightStore` and
  `SqliteInflightStore` implement it; a store that does not keeps working
  through the existing whole-object path. Acknowledgement handling, the inbound
  duplicate check and delivery marking no longer read a payload back, so a
  PUBACK for a multi-megabyte publication settles on metadata alone.
- Durable schema versioning through `PRAGMA user_version`
  (`SQLITE_SCHEMA_VERSION`). Schema 2 adds a persisted `outbound.logical_size`
  and declares `payload` last, so metadata reads never traverse BLOB overflow
  pages. Databases written by schema 1 are rebuilt in a single transaction on
  open — an interrupted migration reopens as schema 1 rather than as half of
  two — and a database written by a newer MQTTium is refused instead of
  reinterpreted.
- `benchmarks/persistence_index_ab.py`, a rotated A/B of the durable `seq`
  indices over a realistic mixed profile (batched acknowledgement lots,
  reconnect replay, event-loop lag percentiles).
- Decision counters on the two runtime pumps, so their strategies can be
  changed against evidence rather than intuition. `WriterStats` gains `batches`,
  `batched_items`, `batched_bytes`, `segmented_writes` and
  `enqueue_suspensions`; `EffectStats` gains `batches`, `multi_effect_batches`,
  `reordered_batches`, `inline_effects` and `apply_suspensions`.
- `AsyncTransport.stats()`, an optional method a transport may implement to
  report its own buffer occupancy. A transport that does not is reported
  through `TransportStats.unavailable()` instead of being probed attribute by
  attribute from the client.
- `benchmarks/qos1_frame_policy.py` and `docs/QOS1-FRAME-POLICY.md`, retaining
  the allocation/replay A/B that selected the outbound PUBLISH frame policy.
- Seven isolated memory scenarios for the audit's remaining risk paths, with
  exact workload assertions and versioned `tracemalloc` peak thresholds.

### Changed

- `publish_nowait()` and `publish_many_nowait()` now compute the exact MQTT wire size for bounded-writer admission instead of encoding a disposable preview frame. QoS 1/2 now encode only the real publication after packet-ID allocation.
- QoS 1 and pre-PUBREC QoS 2 records no longer retain contiguous encoded
  PUBLISH frames after the initial SEND; those frames duplicated the payload
  and replay already re-encoded them. Segmented `(header, payload)` items remain
  cached because they share the payload, and replay sets DUP by replacing only
  the small header before reusing the tuple on later reconnects.
- Outbound QoS 2 records are fully compacted after PUBREC: topic, payload and
  PUBLISH properties are removed atomically while the original logical size is
  retained until PUBCOMP releases admission accounting. SQLite schema 3 also
  compacts pre-existing WAIT_PUBCOMP rows during migration.

- Inbound restart redelivery is now incremental and backpressured. Replay
  restores the Receive Maximum window from a payload-free index, then emits
  bounded batches driven by an internal `CONTINUE_INBOUND_REPLAY` effect, so
  delivery backpressure applies *between* batches. Peak allocation during a
  4,000 x 1 KiB session replay dropped from 5.9 MiB to 0.76 MiB. Stores without
  the paging and metadata extensions keep the previous eager behaviour.
- `SqliteInflightStore.batch()` is lazy: `BEGIN IMMEDIATE` is deferred to the
  first mutation, so a read-only ingress lot takes no write lock and pays no
  commit.
- `ClientStats` gains directional `outbound` and `inbound` snapshots,
  each produced by the session that owns the state. The pre-stable `protocol`
  field remains as a deprecated aggregate built from those snapshots so existing
  diagnostic consumers keep working. `AsyncClient.stats()` now only assembles
  owner-produced sections instead of reaching through private session or
  transport attributes.
- `EffectPump` partitions a multi-effect batch SEND-first in a single pass, and
  leaves the list alone when it was already ordered. The two-generator form it
  replaces walked the batch twice and always rebuilt the list. This is what pays
  for the new counters: a batch of eight effects is ~11% faster than before, and
  a QoS 0 publish — which emits SEND plus PUBLISH_COMPLETE, so it takes this
  path — is ~4-5% faster end to end.
- Ordered pagination no longer re-issues `WHERE seq>? ORDER BY seq LIMIT ?` per
  page, which re-scanned and re-sorted the table each time and made a full
  replay quadratic. One sorted metadata pass produces the ordered identifiers,
  then each page is read back by primary key. On 10,000 x 4 KiB records this is
  faster than the indexed page-per-query form while adding nothing to every
  publish: no `seq` index is created, and any left by an earlier build is
  dropped on open. `SqliteInflightStore` pages now match `MemoryInflightStore`
  exactly when records are deleted mid-iteration — the page comes back shorter,
  with insertion order preserved and no duplicate or resurrected record.

### Removed

- `InflightStore.pop_out()`. The library never called it — `get_out()` plus
  `delete_out()` cover every path — so it was surface a third-party store had
  to implement for nothing. `pop_in()` stays; the inbound acknowledgement
  fallback still uses it.
- The `outbound.extra` column, written on every insert and never read.
- The base64-text payload reader. Payloads are always stored as BLOBs, so the
  fallback only existed for a storage format no writer produces.

### Fixed

- MQTT 5 PUBLISH packets without properties decode to an empty `Properties`
  rather than `None`, so the small-message delivery fast path tested identity
  and pushed every property-less v5 message into exact byte accounting.

## [0.1.0a3] - 2026-08-05

### Added

- `AsyncClient.stats()` and immutable `ClientStats` snapshots covering protocol
  admission, effect and writer queues, decoder and transport buffers, delivery
  budgets, receipts, task state and lifetime high-water marks. Statistics are
  maintained without logging or background sampling.
- `max_ingress_batch_bytes` on `AsyncClient`. The reader now drains packets in
  batches bounded by both 256 packets and a byte target, while still allowing
  one individually oversized packet to make progress.
- A reconnect/backpressure soak harness plus pull-request Linux checks and
  manually triggered Linux, macOS, EMQX and HiveMQ campaigns with retained JSON
  measurements.
- A documented public API candidate and stable-release acceptance policy.
- `AsyncClient.publish_nowait()`, a synchronous, non-suspending publication
  method for producers already executing on the client's event loop. It performs
  immediate engine and writer-capacity checks, returns the normal
  `PublishReceipt`, and coalesces asynchronous effect completion without
  creating a publication coroutine.
- `ProtocolEngine.reconfigure()`, the validated configuration boundary used by
  runtime adapters instead of mutating the attached configuration directly.

### Fixed

- `benchmarks/paired_regression.py --scenario` is now honoured in parent mode
  instead of silently running the full scenario matrix.

### Changed

- `OutboundSession` now handles `PUBACK`, `PUBREC` and `PUBCOMP` directly, so
  the component that acquires publication budget, packet identifiers, store
  records and flow slots also releases them through terminal acknowledgement.
- The bounded transport writer has been extracted into `WritePump`, which now
  solely owns queue ordering, byte/count backpressure, batching, the writer task
  and `last_outbound`. `AsyncClient` retains transport lifecycle and writer
  failure policy, while direct-bound enqueue methods preserve the SEND hot path.
- The Paho façade now uses a narrow loop-bound `AsyncClient` adapter boundary
  rather than accessing the protocol engine, receipt registries and effect pump
  directly. Its cross-thread batching is preserved: QoS 0 uses receiptless
  batched admission, while QoS 1/2 receives the authoritative MID and registered
  receipt before releasing the calling thread.
- The native `await publish()` hot path remains inline rather than routing
  through adapter wrappers; paired measurements found the wrapper version
  2.36% slower, while the retained path is performance-neutral against `main`.

## [0.1.0a2] - 2026-08-04

### Added

- Configurable outbound admission limits on `AsyncClient`:
  `max_pending_outbound_messages`, `max_pending_outbound_bytes` and
  `publish_backpressure` (`"wait"` or `"error"`). Admission is checked before a
  packet identifier is allocated or a store record is written, so a refusal
  leaves no state behind.
- A shared inbound delivery byte budget (`max_pending_delivery_bytes`) charged
  once per message and released only when the last consumer drops it, so
  iterator and callback delivery cannot double-count the same payload.
- Bounded failure retention for `publish_many()`: `max_failure_details`
  (default 128) and `failure_sink`, plus `PublishBatchError.failure_count` and
  `PublishBatchError.failure_counts`.
- `PagedInflightStore`, an opt-in protocol extending `InflightStore` with
  `out_pages()`, `out_summary_pages()` and `in_pages()`. The engine uses it to
  hydrate a persistent session without materialising every payload at once, and
  falls back to the eager path for a store that does not implement it. Both
  shipped stores implement it.
- `EngineConfig.update()`, which validates a candidate atomically before
  changing fields and restricts derived-state changes after engine attachment.
- Removed the legacy public `EngineConfig.max_queued` field. Use
  `max_pending_outbound_messages` and `max_pending_outbound_bytes`; `None`
  disables either limit while zero rejects new QoS 1/2 publications.
- Paho façade: `max_queued_messages_set()`, `max_queued_bytes_set()` (no Paho
  equivalent) and `MQTT_ERR_QUEUE_SIZE` (15) for admission refusals.

### Changed

- **Behaviour change.** Outbound admission is bounded by default
  (`max_pending_outbound_messages=10_000`, `max_pending_outbound_bytes=64 MiB`,
  `max_pending_delivery_bytes=64 MiB`). Previously a QoS 1/2 producer could
  queue until the 65 535 packet-identifier space was exhausted. Pass `None` for
  either limit to restore unbounded queueing.
- Connection epochs are attached to every engine effect, so work in flight from
  a dead connection can no longer touch its successor.
- SQLite session hydration reads keyset-paginated pages and a payload-free
  summary projection instead of loading every row eagerly.
- WebSocket `write_many()` flushes in batches bounded by
  `max_write_batch_bytes` (1 MiB); an oversized item is written alone.
- Coalesced Paho-compatible QoS 0/1/2 cross-thread publishing onto bounded
  network-loop batches, with atomic queue-size refusal and cancellation-safe
  MID handoff semantics.
  Drains are capped at 256 requests or 1 MiB of logical topic-plus-payload bytes.
- Paho façade: `wait_for_publish()` and `is_published()` now raise on a
  non-zero return code instead of reporting a publication that never happened.

### Fixed

- `publish_many()` no longer leaks the outbound byte budget when a chunk is
  rolled back on a transactional store. `SqliteInflightStore.batch()` rolls back
  before the engine's recovery path runs, so the per-record sizes were already
  gone and the reserved bytes were never returned. Repeated failed chunks —
  from a rejected admission *or* from any mid-batch validation error — would
  eventually exhaust the 64 MiB default and refuse every QoS 1/2 publish.
- A `publish()` parked on outbound admission capacity is now failed when the
  connection is lost for good, instead of waiting forever. Capacity is only
  released by an acknowledgement, and a parked producer holds no receipt, so
  neither the receipt failure path nor the writer-capacity wake-up could reach
  it. A producer parked while a reconnect is pending still waits, as before.
- Publish/QoS completion effects are emitted before the packet identifier is
  released, and receipts are tracked FIFO per identifier, so an acknowledgement
  for a reused MID can no longer settle a stale receipt.
- Restored Paho-compatible publish throughput, which had regressed to roughly a
  third of Paho's by serialising every submission behind the network loop.

## [0.1.0a1] - 2026-08-03

### Added

- Initial async-native MQTT 3.1.1 and MQTT 5 client spin-out.
- QoS 0/1/2, reconnect, session replay and multiple transports.
- In-memory and SQLite inflight persistence.
- Aggregate `publish_many()` pipeline with bounded memory.
- Additive Paho VERSION2 compatibility façade.
- Standalone Python 3.11–3.14 CI, fuzzing and Mosquitto integration tests.
- Wheel, source-distribution and isolated-install release validation.
- PEP 561 inline typing marker.
- PyPI Trusted Publishing workflow using GitHub OIDC.

### Changed

- Replaced the original Paho-shaped topic matcher with an independent
  flat-filter implementation before publication.
- Adopted PEP 639 license metadata and explicit Apache-2.0 package files.

### Removed

- Pre-spin-out comparative analysis and generated coverage data from the
  published source tree.

[Unreleased]: https://github.com/yoch/mqttium/compare/v0.1.0a4...HEAD
[0.1.0a4]: https://github.com/yoch/mqttium/compare/v0.1.0a3...v0.1.0a4
[0.1.0a3]: https://github.com/yoch/mqttium/compare/v0.1.0a2...v0.1.0a3
[0.1.0a2]: https://github.com/yoch/mqttium/compare/v0.1.0a1...v0.1.0a2
[0.1.0a1]: https://github.com/yoch/mqttium/releases/tag/v0.1.0a1
