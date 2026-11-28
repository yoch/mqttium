# 0069 — Concurrent distinct-client session open batching

- **Status**: Proposed
- **Date**: 2026-11-27
- **Amends**: [0048](0048-runtime-limits.md) (`LIFECYCLE_IN_FLIGHT`, `CONTROL_QUEUE`)

## Context

Under [ADR 0048](0048-runtime-limits.md), session lifecycle operations ran one
at a time: the core owner held `Option<Flight>` and the runtime enforced
`LIFECYCLE_IN_FLIGHT = 1` over a four-slot `CONTROL_QUEUE`. Every incoming
`CONNECT` therefore incurred a full serial durable commit (`fsync` in `log`
and `durable` storage modes) before the next distinct client could even enter
the store. Under connection storms (hundreds of distinct clients connecting
simultaneously, reserve R4 in
[`../missions/L11-optimisation.md`](../missions/L11-optimisation.md)), session
opening throughput was capped at one client per `fsync` round-trip.

## Decision

1. **Bounded concurrent distinct-client opens in the core owner**:
   `Owner::lifecycle` holds up to `MAX_CONCURRENT_SESSION_OPENS = 16`
   simultaneous flights instead of `Option<Flight>`.
   - A `SessionAction::Open*` flight is marked `batch_safe` only when it cannot
     retire another session, cannot take over an active connection, and cannot
     trigger an inline close follow-up:
     - `will.is_some() || expiry != SessionExpiry::OnDisconnect`, and
     - either `known.is_none()` (brand-new `client_id`) or a non-destructive
       resume of a detached, non-expired, non-`OnDisconnect` session with
       `clean_start == false`.
   - When `lifecycle` is non-empty, a new open may join the batch only if:
     - the new open and every currently in-flight lifecycle entry are
       `batch_safe`,
     - `lifecycle.len() < MAX_CONCURRENT_SESSION_OPENS`,
     - neither `client_id` nor `connection` collides with any in-flight
       lifecycle entry, and
     - for fresh creations (`known.is_none()`),
       `sessions.len() + pending_fresh < sessions.capacity()` so the batch
       cannot overcommit session capacity.
   - All exclusive transitions (`Close`, `Detach`, `Expire`, `EndWill`, live
     connection takeovers, destructive `clean_start`, expired replacements,
     and non-Will `OnDisconnect` opens) wait until `lifecycle.is_empty()` and
     block any concurrent open until they finish.
   - `Change::Submit`, `Change::TransportClosed`, `Change::Await`,
     `Change::Applied`, and `Change::Refused` address their exact
     `OperationId`, so batch replies may complete in any order and each flight
     retains its own resolution deadline and idempotent `ResolveAndFence` path.
2. **Runtime and store-writer batching (amending ADR 0048)**:
   - `LIFECYCLE_IN_FLIGHT` is raised from `1` to `16`.
   - `CONTROL_QUEUE` is raised from `4` to `32` so all 16 concurrent lifecycle
     commands plus reserved receipt collection and message-expiry controls fit
     without stalling the control lane.
   - In `writer.rs`, consecutive `ControlRequest::Lifecycle(Effect::Persist)`
     items on the coordinator lane (when `group > 1`, i.e. outside single-step
     test-hook mode) coalesce into one atomic `store.apply_group(&commands)`
     transaction, preserving per-command `record_applied`, `record_result`, and
     FIFO `ControlResponse::Lifecycle` ordering.
   - `Runtime` tracks out-of-order completions within an active batch in a
     bounded `batch_session_changes` vector alongside `last_session_change`,
     preserving stale-replay fencing across epochs and past batches without
     unbounded memory growth.

## Verification

- Formal model: [`../../models/session-open-batch/MSessionOpenBatch.tla`](../../models/session-open-batch/MSessionOpenBatch.tla)
  proves `CapacityRespected`, `AtMostOneFlightPerClient`,
  `NoExclusiveOverlap`, `OwnerStoreConsistentWhenIdle`, and
  `NoUnexpectedBatchRefusal`, witnesses multi-open group commits
  (`NeverBatched`), and rejects three targeted mutants removing each guard.
- Deterministic core replay test:
  `concurrent_distinct_opens_pipeline_and_serialize_conflicts_and_exclusives`
  in `mqttiumd-core/src/owner/sessions.rs`.
- Runtime log-mode batching test:
  `burst_of_distinct_session_opens_batches_into_fewer_log_commits` in
  `mqttiumd/src/runtime_tests.rs`.

## Consequences

- Distinct-client connection bursts coalesce up to 16 opens per durable store
  group commit (`apply_group`), turning serial per-client `fsync` stalls into
  batched commits.
- Takeovers, clean starts, closures, detachments, expiries, Will handling,
  per-client ordering, and route-mutation serialization (`lifecycle_mutates_routes`
  remains `false` for non-destructive batch-safe opens and `true` for exclusive
  transitions) preserve their exact prior semantics.
- Existing single-step test hooks (`hooks.directory.is_some()`) continue to set
  `group = 1` and observe individual lifecycle barriers unchanged.
