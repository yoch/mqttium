# Changelog

All notable changes to MQTTium are documented here.

The format follows Keep a Changelog and versions follow Semantic Versioning.

## [Unreleased]

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

[Unreleased]: https://github.com/yoch/mqttium/compare/v0.1.0a2...HEAD
[0.1.0a2]: https://github.com/yoch/mqttium/compare/v0.1.0a1...v0.1.0a2
[0.1.0a1]: https://github.com/yoch/mqttium/releases/tag/v0.1.0a1
