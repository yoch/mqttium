# 0057 — Deterministic `FlatMap`, direct synchronous `VolatileStore`, and zero-allocation framing (Lot 7)

Status: **Proposed** (23 November 2026). Applied to `mqttiumd-contract`, `mqttiumd-core`, `mqttiumd-store-volatile`, and `mqttiumd`.

## Context

Following Lot 5 (`4afeff66`) and Lot 6 (`4c4e4506`, records [0055](0055-logstore-append-only-wal.md) and [0056](0056-claimed-publication-and-core-optimizations.md)), ARM64 `perf` profiling of high-throughput QoS 0 and QoS 1 workloads (32–128 pairs, 64–256 connections) identified five remaining structural costs across the pure core, the runtime, the network framing layer, and the in-memory store:

1. **Point-lookup `BTreeMap` pointer chasing and node churn (`~25 %` of main-thread CPU)**:
   Even after prefix-stripping scoped keys (`ScopedMap`), every QoS 1 publication and acknowledgement traversed 20+ `BTreeMap` indices across `Owner` (`slot_by_key`, `slot_by_op`, `slot_by_deadline`, `slot_by_effect`, `slots_by_session`, `finished_by_op`, `session_by_id`, `session_by_conn`), `pubsub::State` (`delivery_by_id`, `delivery_by_op`, `delivery_by_deadline`, `delivery_by_frame`, `delivery_by_claim_op`, `delivery_by_claim_result_op`, `delivery_by_target_pi`, `deliveries_by_session`, `deliveries_by_window`, `active_by_gen`, `conn_by_gen`), `RouteIndex::exact`, `Runtime` (`peers`, `frames`, `outgoing_bindings`, `inflight`, `work_reads`, `terminal_effects`, `cached_bindings`), and `VolatileStore` (`payloads`, `outbound`, `publications_by_payload`, `obligations_by_pub`, `inbound_by_pub`, `outbound_by_target_pi`, `retained_by_pub`). In addition, `remove_slot_at` and `remove_delivery_at` performed full `unindex(last)` + `index(index)` round-trips on every swap-removal, and `Owner:: outgoing_ack_target` scanned `deliveries_by_session` linearly.
2. **Per-frame and per-command heap allocations (`~4–5 %` of CPU in `libc` `malloc`/`free`)**:
   - Every outgoing control frame (`PUBACK`, `PUBREC`, `PUBREL`, `PUBCOMP`, `PINGRESP`, `CONNACK`, `SUBACK`) and short `PUBLISH` header allocated a fresh `Vec<u8>` on the heap.
   - Every incoming control packet (`PUBACK`, `PUBREC`, `PUBREL`, `PUBCOMP`, `PINGREQ`, `DISCONNECT`) allocated a 0–4 byte `Vec<u8>` in `net::Parser`.
   - `FrozenRecipe::freeze` allocated an intermediate `Vec<u8>` before copying into `Arc<[u8]>`.
3. **Channel, timer, and wake round-trip overhead in `--storage-mode volatile`**:
   Routing `VolatileStore` commands through the background writer channel required cross-thread channel sends, `inflight` map insertions/removals, `ArmDeadline` timer registrations, and an extra reactor turn before `StoreResult` could emit `PUBACK` and `EmitPublish`.
4. **Core-to-transport polling and synchronization waste (`connections_at = 8.42 %` at 256 connections)**:
   - `Runtime::connections` iterated all `peers` on every turn even when only a subset had activity.
   - `Runtime::packet_at` unconditionally called `self.wire_progress(generation)` on every incoming packet, acquiring the `wire_progress` lane mutex even when empty, while outgoing control frames emitted redundant `WireProgress` messages in addition to `Written::Complete`, and non-expiring `EmitPublish` frames tracked `Written::Complete` even when `frame_written` audit logging and transport backpressure were inactive.
   - `Runtime::business_waits` and `Runtime::drain_inputs` linearly scanned `self.deferred_inputs` on every packet and input step, and `Events::records("session_cardinality")` performed a binary search over `OPS_KINDS` after every dispatch.
   - `Owner::has_ready_work` and `pubsub::State::next_send` linearly scanned all in-flight `AwaitPuback` deliveries of a session on every progress hint even when every delivery was already emitted on the active generation.
   - `Input::MessageClock` and `Instant::now()` were sampled repeatedly per turn and per complete packet even when no expiring messages or partial frames were present.

## Decision

1. **Deterministic open-addressed `FlatMap<K, V>`, `FlatSet<K>`, and `ScopedFlatMap<K, V>` (`crates/mqttiumd-contract/src/flat_map.rs`)**:
   - Implemented in pure `#![no_std]` + `alloc` under `#![forbid(unsafe_code)]` and all `mqttiumd-lab --test boundary` token rules, with zero external crates.
   - Uses a deterministic 64-bit Fibonacci multiply-shift mixer (`FlatKey::flat_hash`, `mix64`) with zero ambient randomness.
   - Uses power-of-two linear probing with **backward-shift deletion** (zero tombstones) so high-churn steady-state tables never accumulate deleted slots or reallocate after warm-up.
   - `ScopedFlatMap<K: ScopedKey, V>` combines prefix-stripping (active run-epoch keys stored by their 8-byte `K::Local` sequence in `FlatMap`) with a cold `BTreeMap<K, V>` fallback for prior-epoch keys.
     The active scope is that of the first key a map receives, until a key of a later run arrives; that key's scope then becomes the active one and what the compact map held moves to the fallback (`ScopedKey::later_run`, added on 26 November 2026). Before that rule the first key decided for the whole run: a table rebuilt after a restart received first what earlier runs had left, a retained message or a message queued for an absent session, and every key of the new run went to the ordered fallback.
   - Applied strictly to point-lookup tables across `Owner`, `pubsub::State`, `RouteIndex::exact`, `Runtime`, and `VolatileStore`. Tables whose iteration order or range scans (`range(..)`) are semantically significant (`routes`, `retained`, `wills`, `fences`, `inbound`, `timers`) remain in `BTreeMap` / `ScopedMap`. In `VolatileStore`, `publications`, `payloads`, `obligations` and `outbound` are `ScopedFlatMap`s: the readers that need their order sort what they read, and `operations` and `sessions` have tables of their own (`OriginOpTable`, `SessionTable`).
2. **O(1) delivery indices and swap-remove reindexing (`crates/mqttiumd-core/src/owner{,.rs,/pubsub.rs}`)**:
   - Added `delivery_by_target_pi: FlatMap<(SessionId, OutboundPacketId), usize>` so `Owner::puback_target` and `Owner::qos2_ack_target` resolve in O(1).
   - Added `sent_on_gen: FlatMap<(SessionId, ConnectionGeneration), usize>` and `busy_by_session: FlatMap<SessionId, usize>` on `pubsub::State`, proving in O(1) when `sent + busy == deliveries_by_session[session].len()` that no delivery can satisfy `waiting_to_send(generation, now)` and short-circuiting `next_send` and `has_ready_work` without scanning.
   - Added `reindex_moved_slot` and `reindex_moved_delivery` so swap-removal updates the moved element's index in place instead of full `unindex` + `index` cycles.
3. **Stack-buffered canonical recipe freezing and inline `FrameBytes` (`crates/mqttiumd-contract/src/command.rs`, `crates/mqttiumd/src/{net.rs,codec.rs}`)**:
   - `FrozenRecipe::freeze` encodes fixed-size recipes (`FinishDelivery`, `PreparePubrel`, `ClaimDelivery`, `ConsumeDelivery`, `ReleaseInbound`) and small admissions (`<= 384` B) into a stack `StackBuf<384>` and constructs `Arc<[u8]>` directly in a single allocation.
   - `net::FrameBytes` stores frames and packet bodies `<= 32` B (`INLINE_FRAME_CAP = 32`) inline without heap allocation, used for both outgoing `net::Write::bytes` (`codec::encode_frame_bytes`, `EncodedSegments::header`) and incoming `net::Incoming::Packet::body` (`Parser::body`).
4. **Direct synchronous `VolatileStore` execution (`crates/mqttiumd/src/{writer.rs,runtime.rs}`)**:
   - When `--storage-mode volatile` runs without file test-hooks (`hooks.directory.is_none()`), `Runtime` holds `StoreHandle::Direct(DirectWriter)` on the main thread.
   - `Sink::try_submit` executes `Effect::Persist` synchronously against `VolatileStore::apply`, pushes `Input::StoreResult` directly to `self.inputs`, skips `ArmDeadline` timer registration for immediately settled outcomes (`direct_skip_next_deadline`), and runs an inline follow-up pass in `Runtime::drain_inputs` so `PUBLISH -> Persist -> StoreResult -> PUBACK + EmitPublish` completes within a single reactor turn.
   - Periodic (`100 ms` check) and shutdown atomic Schema-14 / Schema-15 SQLite snapshots run via `DirectWriter::maybe_periodic_snapshot` and `DirectWriter::finish`, preserving non-zero exit propagation on final snapshot failure.
5. **Active-peer `ReadyQueue`, O(1) `deferred_by_conn`, and write-report elision (`crates/mqttiumd/src/{net.rs,runtime.rs,events.rs}`)**:
   - `net::ReadyQueue` records `activity: false -> true` transitions with an `AtomicBool` pending guard and O(1) `std::mem::swap` drain, so `Runtime::connections_at` polls only active and pending peers between `100 ms` deadline sweeps.
   - `net::WriteReport` elides redundant `WireProgress` in release builds (collecting `Written::Complete` prior to draining `incoming` and on `Receive Maximum` pressure) and elides `Written::Complete` + `PendingFrame` registration for non-expiring `EmitPublish` when `!events.records("frame_written")` and no backpressure or test hooks are active.
   - `Runtime` maintains `deferred_by_conn: FlatMap<ConnectionGeneration, usize>` for O(1) `business_waits` and deferred-ordering checks, guards `wire_progress` with `has_pending()`, batches non-retained QoS 0 `PUBLISH` steps in `fast_batch_step`, and short-circuits `Events::records` for `session_cardinality` and `FAST_TALLIES`.
   - `LogStore` raises `DEFAULT_CHECKPOINT_BYTES` from `32 MiB` to `128 MiB` to avoid mid-burst synchronous SQLite compaction stalls while preserving bounded `.mlog` recovery and explicit `set_checkpoint_bytes` overrides.

## Consequences

- **Safety, purity, and contracts**: `#![forbid(unsafe_code)]` is preserved workspace-wide; `Cargo.lock` and `contracts/v1/product-profile.json` are untouched; `mqttiumd-lab --test boundary` (17/17), all 13 TLA+ models (`models/check_models.py`), and 126/126 differential compatibility scenarios against Mosquitto 2.0.21 pass across `durable`, `log`, and `volatile` modes.
- **Durability**: `SqliteStore` (`durable`) and `LogStore` (`log`) retain strict pre-commit `fdatasync` (`DUR-001`, `DUR-002`). `VolatileStore` (`volatile`) remains an explicit opt-in RAM mode (record [0052](0052-volatile-store-and-periodic-snapshot.md)) with atomic SQLite snapshots.
