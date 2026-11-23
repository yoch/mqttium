# 0056 — Atomic `ClaimedPublication` (recipe kind 24) and Lot 5/6 core-runtime optimizations

Status: **Proposed** (23 November 2026). Applied to `mqttiumd-contract`, `mqttiumd-core`, `mqttiumd-store-sqlite`, `mqttiumd-store-volatile`, `mqttiumd-lab`, and `mqttiumd`.

## Context

In the received exact-delivery pipeline, a single online 1-to-1 QoS 1 publication required three separate durable commands (`AdmitPublication`, `ClaimDelivery`, `FinishDelivery`), one durable store read (`LoadWork` → `WorkPage`), two synchronous disk `fdatasync` hops on the publish-to-deliver critical path, and roughly nine `Owner::step()` transitions. Under high concurrency (64–256 connections) on ARM64, profiling (`perf`) additionally identified structural overhead in:

1. Quadratic control-lane rotation in `Outbox::dispatch` / `Runtime::drain_inputs` when effects queued behind writer capacity.
2. Full 32-byte scoped ID keys (`OperationId`, `PublicationId`, `PayloadId`, `DeliveryId`, `EffectId`) dominating B-tree comparisons and cache footprint across `Owner` and `VolatileStore`, plus a 280-byte `Answers` retry table and per-command canonical recipe copies.
3. Heap allocations per connection (`Receiving`, `LaneState`, eager TLS/read/write buffers, `Condvar`s) and string-keyed exact-route `BTreeMap` lookups.

## Decision

1. **Canonical recipe kind 24: `Recipe::ClaimedPublication` (`crates/mqttiumd-contract/src/command.rs`)**:
   - Introduces `InlineClaim { target_index: u16, packet_id: OutboundPacketId }` and `ClaimedPublication { admission: AdmitPublication, claim: InlineClaim }` under `KIND_CLAIMED_PUBLICATION = 24`.
   - Canonical layout (format 1, big-endian):
     `"MQDR" | format:u16(1) | kind:u8(24) | <AdmitPublication body> | target_index:u16 | packet_id:u16`.
   - `ClaimedPublication::check` enforces `admission.check_retained()`, `!admission.retain`, `target_index < admission.targets.len()`, `targets[target_index].effective_qos != Qos::AtMostOnce`, and `packet_id != 0`.
   - `AnnotatedPublication` (kind 23) may wrap `ClaimedPublication` (kind 24) so MQTT 5 PUBLISH application properties use the same atomic path.
   - `ReceiptClass::of` classifies `ClaimedPublication` as `ReceiptClass::Business`; `Recipe::admission()` returns `Some(&claimed.admission)` so receipt identity witness bounds (`retained::bounds` / `witness::bounds`) cover it uniformly.
   - **Zero SQLite schema change**: All three stores (`SqliteStore`, `VolatileStore`/`LogStore`, `MemoryStore`) execute `ClaimedPublication` atomically within one transaction/check-then-mutate step by validating target active session, obligation uniqueness, and `(target, packet_id)` uniqueness in `outbound`, charging `admission.costs.exchange_slots + 1` `ExchangeSlots` (matching `ClaimDelivery` and released identically by `FinishDelivery` / `cancel_session`), inserting the standard `publications`, `payloads`, `obligations`, `outbound` (with `claim = admission.operation`), and `operations` rows, and committing `CommitFacts::PublicationAdmitted { publication, exchange }`. Recovery (`RecoveredFact::OutboundDelivery`) and `FinishDelivery` / `PreparePubrel` operate identically without schema changes.
2. **Opt-in core activation and strict ordering invariant (`crates/mqttiumd-core/src/owner{,.rs,/pubsub.rs}`)**:
   - `Owner::new` defaults `inline_claim_enabled = false` so existing deterministic unit and fault-injection fixtures observing `AdmitPublication` + `LoadWork` + `ClaimDelivery` stay unchanged; the product runtime calls `owner.enable_inline_claims()` whenever test-hook file injection is inactive.
   - Each online `pubsub::Connection` maintains `caught_up: bool` and `dirty_during_load: bool`, while `Owner` tracks `non_inline_targets_by_op` / `non_inline_by_session`. A connection becomes `caught_up` only after a non-discarded `WorkPage(next = None)` with `dirty_during_load == false` and zero in-flight non-inline admissions, subscription replays, retained QoS 0 admissions, or Wills.
   - `plan_admission` selects `ClaimedPublication` only for a single QoS 1/2 target whose connection is online, `caught_up`, has no active scan, has `Receive Maximum` window capacity, delivery table and loaded-payload budget room, a free `OutboundPacketId`, and no earlier `Queued` / `ConsumePending` / `Indeterminate` delivery.
   - A provisional delivery enters `Phase::InlineClaimPending(op)`, reserving its `OutboundPacketId` (`MPacketReuse`) and window slot while `d.in_flight()` remains `None` (routing `StoreResult` and `Deadline` through `Owner::slots`) and `waiting_to_send()` remains `true` (preserving strict MQTT-4.6.0-1 wire order across pipelined claims).
   - On `StoreResult(Committed(PublicationAdmitted))`, `Owner` transitions the inline delivery to `AwaitPuback` and emits `EmitPublish` in the **same** transition when `draft.effects.count + 1 < MAX_TRANSITION_EFFECTS` and `plan_delivery_emit` returns `Frame` (leaving `Held` and `TooLarge` to `WorkAvailable`). On `RejectedCertain`, the provisional delivery is removed and its `PacketId` and window slot are released. On subscriber session retirement while in flight, the delivery is kept retired (`retired_flights` counted) until the owning `Slot` resolves.
3. **Pure-Rust `bbtree` / Redis / FlashMQ-inspired data-structure and runtime optimizations**:
   - `ScopedMap<K, V>` (`crates/mqttiumd-contract/src/scoped_map.rs`, `#![forbid(unsafe_code)]`, `no_std + alloc`) prefix-strips active-scope 32-byte IDs to `u64` local keys with canonical `Ord` merge iteration across hot and cold scopes.
   - Prefix-stripped 88-byte `Answers` in `Owner`, `Arc<[u8]>` zero-copy `FrozenRecipe` bytes, single-pass `AdmitPublication::encode_body`, and zero-copy `VolatileStore` operation receipts.
   - Redis-inspired compact structures: inline 8-slot `Receiving` table, `SmallIndexSet` sorted `Vec<usize>`, `u64` bitmask `free_packet_id()`, `pubrec_pending_count` fast skip, 1-slot inline `LaneState` without `Condvar` outside tests, and demand-allocated TLS/read/write buffers (reducing idle footprint from 71.1 KiB → 8.6 KiB plain and 50.2 KiB → 19.7 KiB mTLS per connection).
   - O(1) `Outbox` `ControlCounts` (`u32`) and `Runtime::drain_inputs` dispatch-block gating eliminating the 54 % CPU quadratic spin, plus deterministic 64-bit FNV-1a hash-indexed exact `RouteIndex` (`crates/mqttiumd-core/src/owner/routing.rs`) with exact byte verification.

## Consequences

- **Contract digest**: Adding `Recipe::ClaimedPublication` is a contract encoding addition (`mqttiumd-contract`), covered by canonical round-trip tests (`claimed_publication_canonical_roundtrip_and_guards`), core transition tests (`inline_claim_commits_emits_rejects_and_handles_toolarge_and_retire`), and three-store + `.mlog` crash-recovery parity tests (`claimed_publication_matches_sqlite_and_recovers_after_crash`). Existing `contracts/v1/` JSON schemas and digest (`sha256:305db0c49a43b1d269aeeca59aaf203c99521e19360f5dd93fdc7220fe37c8`) remain unchanged.
- **Purity & safety**: `mqttiumd-lab --test boundary` (17/17) confirms `mqttiumd-contract` and `mqttiumd-core` retain strict `no_std + alloc`, `#![forbid(unsafe_code)]`, and closed macro/attribute rules. All 13 TLA+ models (`models/check_models.py`) and 126/126 differential compatibility scenarios against Mosquitto 2.0.21 pass in both `durable` and `log` modes.
