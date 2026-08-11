# Changelog

All notable changes to MQTTium are documented here.

The format follows Keep a Changelog and versions follow Semantic Versioning.

## [Unreleased]

### Fixed

- An inbound PUBLISH whose packet identifier is still owned by an unfinished
  exchange of the other QoS no longer overwrites the stored record. The inbound
  store is keyed by identifier alone, so a QoS 1 PUBLISH reusing the identifier
  of a live QoS 2 exchange (or the reverse) replaced it and stranded its Receive
  Maximum slot and byte reservation permanently. The leak was cumulative and
  ended in the client disconnecting itself with `0x93` (Receive Maximum
  exceeded) while holding no message at all. Such a PUBLISH violates
  [MQTT-2.2.1-4] — the identifier is still in use until the broker has processed
  our acknowledgement — and is now refused with `0x82` (Protocol Error) before
  anything is stored. Only `manual_ack=True` was ever affected: without it a
  QoS 1 PUBLISH stores no record, so neither direction can collide. Genuine
  QoS 2 duplicates and QoS 1 manual-ack redeliveries are unaffected.
- `EffectPump` no longer retains the exception raised while the background
  effect-flush task applies an effect once nothing is left pending. The stored
  error was handed to whichever call next suspended in `drain()`, so a protocol
  error caused by the broker surfaced against an unrelated later `publish()` on
  a healthy connection. A failure that genuinely blocks pending effects is still
  reported to whoever waits on it.
- `PacketIdPool.reserve()` is now idempotent for an identifier the allocation
  frontier has just reached. The redundant reservation was left behind and
  counted twice, permanently inflating `len()`/`available` and blocking the
  `release()` fast path that resets the pool once every identifier is free.
- Connecting over MQTT 3.1.1 with a password but no username is refused instead
  of sending a CONNECT whose password flag is set while the username flag is
  clear, which [MQTT-3.1.2-22] forbids and a broker may reject or misparse.
  MQTT 5 lifted the restriction, so the check is version-specific and the same
  configuration still works there.
- `publish()` rejects `subscription_identifier` in MQTT 5 properties instead of
  putting it on the wire. [MQTT-3.3.4-6] forbids a Subscription Identifier on a
  PUBLISH sent from a Client to a Server; the property table validates per
  packet type and cannot express a restriction that applies to one direction
  only, since the same property is legal on the PUBLISH a broker sends. The
  inbound direction is unchanged.

### Added

- `docs/spec/` vendors every normative statement of MQTT 3.1.1 and 5.0,
  extracted verbatim from the OASIS documents with provenance, checksum and
  regeneration tooling (`tools/extract_spec_statements.py`), and
  `docs/CONFORMANCE.md` records what is verified against them and what is not.
  `tests/unit/test_conformance_statements.py` turns a first set of statements
  into executable checks and verifies its own quotations against the index, so
  a test cannot claim to enforce a statement it misquotes.

### Documentation

- `InflightStore.update_out()` / `update_in()` now state what they actually
  guarantee: the mutable state fields only (`state`/`dup`, and
  `state`/`delivered`/`user_acked`), plus retransmission order.
  `SqliteInflightStore` deliberately narrows the write to those columns so a
  retransmission does not rewrite the payload BLOB, while `MemoryInflightStore`
  replaces the record — the protocol was silent on which was correct. The
  corollary is now spelled out: the phase-two compaction `on_pubrec` applies
  through `update_out()` is best-effort for a store without
  `TransitionInflightStore`. Both built-in stores implement that extension and
  compact through `transition_out(compact=True)` instead.

## [1.0.0rc1] - 2026-08-11

### Added

- A single local release runner with `quick`, `performance`, and `rc` profiles,
  durable manifests under `/tmp`, mandatory broker integration, local quality,
  performance and resource gates, short reconnect soaks, and isolated-artifact
  transport smokes. Cross-version and independent-broker validation remains a
  final GitHub matrix; multi-hour campaigns are available separately after the
  first RC.
- An open-loop A/B harness covering calibrated 50/75/90/100% load, MQTT 3.1.1
  and 5, 64-byte and 4 KiB payloads, receipt and callback completion, ABBA
  ordering, latency, CPU, loop-lag, completeness, and EffectPump counters.
- Exact call-count/allocation profiling for the retained micro-scenario
  registry and resource-aware soak snapshots.

### Changed

- Application delivery is owned by the internal `ApplicationDelivery`
  controller. `AsyncClient` delegates iterator/callback queues, byte
  reservations, callback workers, reset/shutdown, and delivery statistics while
  preserving public signatures, defaults, ordering, backpressure, and callback
  exception isolation. Mode-specialised admission removes repeated hot-path
  branches.
- Paired network evaluation now always persists eligibility, policy, thresholds,
  status, failures, and Markdown before exiting. Advisory runs remain visible
  and non-blocking; strict runs return 1 for a measured regression and 2 for an
  invalid runner or worker sample. Subscriber timestamps are collected in a
  separate process, the observer no longer adds a second PUBACK stream, and
  every cell records calibrated and actual sample duration. A final A/A control
  exceeded the noise budget, so closed-loop network results are advisory rather
  than release gates.
- Paired micro workers use a scenario registry instead of one complex dispatch
  function. Benchmark-only dependencies are available through the `benchmark`
  extra, and accidental tracked `.patch`/`.diff` files are rejected.

### Removed

- The obsolete tracked `network-hotpaths-remaining.patch` review artefact.

## [0.2.0b4] - 2026-08-11

### Changed

- `PublishReceipt` no longer builds an `asyncio.Event` per QoS 1/2
  publication. Completion is a flag, and the shared future behind `wait()` is
  created only if something actually waits, so a publication that is never
  awaited allocates no completion primitive at all. Waiters attach through
  `asyncio.shield`, so cancelling one `wait()` cancels only that waiter and
  leaves the receipt and any other waiter intact — the isolation a per-waiter
  `Event` gave implicitly. `wait()` and `is_done()` are unchanged; the private
  `_event` field is replaced by `_future`/`_settled`.
- The Paho façade now installs its inner `on_publish` dispatcher only while
  the user has set `Client.on_publish`, instead of unconditionally at
  construction. Every façade user previously paid a callback-queue hop per
  acknowledged publication to reach a dispatcher that returned immediately,
  and could never satisfy the native client's direct QoS 0 precondition.
  `Client.on_publish` is now a property; reading and assigning it are
  unchanged, including from a non-loop thread.
- Acknowledgement frames without a reason code or properties are emitted
  directly instead of being assembled through the generic encoder, the publish
  encoder no longer re-converts a `QoS` its caller has already validated, topic
  validation no longer builds an encoded form it discards, and the MQTT UTF-8
  rules are checked with string scans rather than a loop over code points. No
  behaviour changes: the same inputs are accepted and rejected, and the only
  visible difference is that a string breaking several UTF-8 rules at once may
  now cite a different one of them.
- `FlowControlError` from the bounded writer now names the bound that refused
  and its configured value, instead of reporting only that a limit was
  reached. `max_outbound_bytes` (1 MiB) and `max_outbound_messages` (10 000)
  imply about 105 bytes per queued message, so the byte bound is the one that
  binds as payloads grow; the defaults are unchanged.
- Documentation is now split by kind and indexed by `docs/README.md`: maintained
  contracts stay directly under `docs/`, while dated measurements, audits and
  campaign records moved to `docs/reports/`. Entries published before this
  reorganisation refer to those reports by their former top-level `docs/` path.

## [0.2.0b3] - 2026-08-09

### Added

- The release workflow now builds wheel and sdist once, smoke-tests those exact
  artifacts across Python 3.11–3.14 plus TCP, TLS, WebSocket, Unix, SQLite,
  Paho migration and clean shutdown, then publishes the same files.
- Installed-distribution smoke coverage now exercises MQTT 3.1.1 and MQTT 5
  over WebSocket and Unix transports, the documented Paho VERSION2 migration
  subset, cancellation, and clean process shutdown. Stable exports and the
  `AsyncClient` constructor/method signatures are locked by regression tests.
- `max_pending_inbound_bytes` now bounds persisted inbound QoS 2 and
  manually-acknowledged QoS 1 application data at 64 MiB by default. Runtime
  statistics expose current, high-water and configured byte values, and SQLite
  schema 4 preserves exact accounting across restarts.

### Fixed

- Packets already in flight while a graceful disconnect is underway no longer
  produce a spurious `ProtocolError`; terminal disconnect handling remains
  authoritative.
- MQTT 5 property encoding now rejects out-of-range Variable Byte Integers and
  oversized binary values with the public `ProtocolError` contract instead of
  leaking low-level `ValueError` exceptions.

## [0.2.0b2] - 2026-08-07

### Changed

- Native QoS 0 publishing now prepares MQTT 3.1.1 and MQTT 5 PUBLISH frames
  once and admits safe single or batched writes directly into the bounded writer.
  Callback and effect-ordering cases keep the established protocol-engine path.
- WebSocket client masking now uses lazy byte-translation tables instead of a
  Python loop per payload byte, while retaining a fresh RFC 6455 mask per frame.

## [0.2.0b1] - 2026-08-06

### Added

- The supported native entry points now expose every type needed by public
  `AsyncClient` signatures: messages, MQTT 5 properties, connection packets,
  subscribe options, negotiated settings, reconnect policy and configuration
  literals. The root package also exposes the operational exception hierarchy
  and `ConnectionState`. `docs/API-STABILITY.md` classifies Stable, Provisional
  and Internal surfaces independently of Python importability or `__all__`.

### Changed

- Inbound delivery byte accounting is now kept outside public `Message`
  instances. One consumer carries the byte count directly; simultaneous
  callback and iterator delivery share a compact two-reference reservation.
  This removes private mutable state from the frozen model, makes each
  `Message` 16 bytes smaller and preserves exact backpressure and queue bounds.

### Fixed

- The Paho compatibility façade now hard-bounds its cross-thread publish
  handoff by request count and logical bytes. Saturation returns
  `MQTT_ERR_QUEUE_SIZE`; reservations are released on admission, cancellation,
  scheduling failure and shutdown, so publisher threads cannot bypass the
  native client's memory guarantees with an unbounded ingress queue.

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

- Consecutive small QoS 0 `MESSAGE` effects can now be transferred to the bounded iterator/callback queues in one `EffectPump` pass. Single messages, acknowledged QoS, exact byte accounting and full destinations retain the established path; callback execution remains isolated.
- Inbound MQTT 3.1.1 QoS 1 PUBLISH packets now decode their delivery fields directly before entering the shared acknowledgement state machine, avoiding a short-lived intermediate `PublishPacket`; MQTT 5 and QoS 2 retain the generic decoder.
- Inbound MQTT 3.1.1 QoS 0 PUBLISH packets now decode directly into the delivered `Message`, avoiding a short-lived intermediate `PublishPacket`; MQTT 5 and acknowledged QoS paths keep the generic decoder.
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

[Unreleased]: https://github.com/yoch/mqttium/compare/v1.0.0rc1...HEAD
[1.0.0rc1]: https://github.com/yoch/mqttium/compare/v0.2.0b4...v1.0.0rc1
[0.2.0b4]: https://github.com/yoch/mqttium/compare/v0.2.0b3...v0.2.0b4
[0.2.0b3]: https://github.com/yoch/mqttium/compare/v0.2.0b2...v0.2.0b3
[0.2.0b2]: https://github.com/yoch/mqttium/compare/v0.2.0b1...v0.2.0b2
[0.2.0b1]: https://github.com/yoch/mqttium/compare/v0.1.0a4...v0.2.0b1
[0.1.0a4]: https://github.com/yoch/mqttium/compare/v0.1.0a3...v0.1.0a4
[0.1.0a3]: https://github.com/yoch/mqttium/compare/v0.1.0a2...v0.1.0a3
[0.1.0a2]: https://github.com/yoch/mqttium/compare/v0.1.0a1...v0.1.0a2
[0.1.0a1]: https://github.com/yoch/mqttium/releases/tag/v0.1.0a1
