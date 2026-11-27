# 0051 — Core scaling, indexed routing, aligned exchange credits, and transport batching

Status: **Accepted** (27 November 2026, by the repository owner after the validation of `9dfd568`, report 0043; proposed on 22 November 2026). Applied to contract, core, SQLite store, and runtime.

Amended by 0061 (accepted): `OwnerConfig.max_exchanges` is `SESSIONS × receiving::MAXIMUM`
(32,768) whatever the connection limit, and `OwnerConfig.session_qos2_exchanges` bounds the
QoS 2 exchanges of a session.

## Context

The initial bounded product profile capped the pure core at 128 sessions and
subscriptions, 16 fanout targets per publication, 32 retained topics, and 32 KiB
payloads, with linear scans across routing, leases, receipts, deliveries, and
SQLite witness checks. In addition, the broker advertised `Receive Maximum = 8`
per connection while sizing the global exchange pool at 128 slots, causing late
capacity refusals under multi-client overload (report 0028).

## Decision

1. **Raised core and store bounds** while keeping the core strictly `no_std +
   alloc`, `#![forbid(unsafe_code)]`, plan-then-apply, and leaving the Kani
   credits proof boundary (`credits.rs`, `capacity.rs`, `ids.rs`) untouched:
   - `MAX_SUBSCRIPTIONS = 16_384`, `MAX_TARGETS = 1_024`,
     `MAX_RETAINED_TOPICS = 4_096`, `MAX_RETAINED_BYTES = 64 MiB`,
     `MAX_WILL_INTENTS = 1_024`, `MAX_WILL_BYTES = 32 MiB`,
     `RECEIPT_WINDOW = 4_096` (`RECEIPT_DATA_WINDOW = 3_584`,
     `RECEIPT_CONTROL_RESERVE = 512`, `MAX_COLLECTION_ROWS = 256`),
     `SESSIONS = 4_096`, `MAX_PAYLOAD = 1 MiB` (`ANNOUNCED_PACKET_BYTES = 1 MiB + 8`).
   - Canonical `RetainMask` preserves legacy recipe kinds `10`/`11`/`17`/`18`
     for `<= 16` targets and uses wide kinds `20`/`21`/`22` for `> 16` targets.
2. **Indexed pure core and recalculated work model**:
   - Exact + wildcard trie `RouteIndex` with `$`-topic isolation, No Local
     before max-QoS aggregation, RAP OR, and canonical `SessionId` ordering.
   - `O(log N)` / `O(1)` indices across `leases`, `receipts`, `sessions`,
     `owner`, and `pubsub` (`VolatileQueue` backed by `VecDeque` + `BTreeMap`).
3. **Aligned exchange credits**:
   - `OwnerConfig.max_exchanges = (limits.connections * receiving::MAXIMUM).max(128)`
     so every admitted connection's full advertised `Receive Maximum` window is
     backed 1:1 by exchange capacity.
4. **SQLite schema 14 / secure schema 15 and sound caches**:
   - Adds `obligations_by_target_order (target, publication, idx)` and cached
     row-value seek queries for `pubsub::work`.
   - Adds connection-local `OriginWitness`, open-will receipt totals, and
     per-session work integrity cache strictly invalidated by `PRAGMA data_version`
     (evaluated fresh per sync, never via `prepare_cached`) and on receipt
     collection or transaction rollback.
5. **Runtime and transport scaling**:
   - Configurable multi-core I/O reactors (`--io-threads N`, default
     `available_parallelism().clamp(2, 4)`), removal of the 4 KiB `SO_SNDBUF`
     clamp, opportunistic non-expiring write coalescing on plain TCP and TLS 1.3
     (up to 16 KiB / 32 frames per write or single TLS 1.3 AEAD record),
     non-shattering `LoadWork` deferral in the writer loop, lock-free atomic
     fast tallies + lazy `emit_with`, cached session bindings, and guarded
     per-turn maintenance.

## Consequences

- `Limits::BOUNDED` preserves the 16-connection test profile while
  `Limits::DEFAULT` scales up to 4,096 connections with aligned exchange slots.
- Legacy schema versions (`<= 13`) are refused cleanly before any mutation.
- Frozen v1 contracts (`contracts/v1/product-profile.json`) and Kani proof
  sources remain unmodified.
