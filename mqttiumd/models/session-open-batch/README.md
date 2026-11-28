# Concurrent distinct-client session opening batch

`MSessionOpenBatch` models bounded concurrent `SessionLifecycle` `Open` flights
for distinct clients committed together via `Writer::apply_group` (reserve R4 of
`docs/missions/L11-optimisation.md`, amending `LIFECYCLE_IN_FLIGHT = 1` of
ADR 0048).

## Questions

- Does concurrent opening ever exceed `Capacity` or cause an unexpected
  `SessionCapacityExceeded` refusal inside a batch? (`CapacityRespected`,
  `NoUnexpectedBatchRefusal`)
- Can two flights for the same `client_id` ever overlap?
  (`AtMostOneFlightPerClient`)
- Can an exclusive lifecycle transition (`Close`, `Detach`, `Expire`, takeover,
  or destructive `CleanStart`) ever overlap another flight?
  (`NoExclusiveOverlap`)
- Once all flights settle, does the owner's session set always equal the store's?
  (`OwnerStoreConsistentWhenIdle`)
- Are multiple distinct opens actually committed together in one `apply_group`?
  (`NeverBatched` violated in `reach-batch`)

## Configurations

| Configuration | Mutation | Expected |
| - | - | - |
| `safety` | `none`, 3 clients, capacity 2, max 2 concurrent | all safety invariants hold |
| `reach-batch` | `none` | `NeverBatched` violated (batch >= 2 reachable) |
| `mut-same-client` | `AllowSameClient` | `AtMostOneFlightPerClient` violated |
| `mut-no-capacity` | `NoCapacityGuard` | `NoUnexpectedBatchRefusal` violated |
| `mut-exclusive-overlap` | `AllowExclusiveOverlap` | `NoExclusiveOverlap` violated |

## Running

From `mqttiumd/`:

```bash
python models/check_models.py
python ../tools/formal/run_tlc.py --models-dir models/session-open-batch
```

## Correspondence

| Model action | Code | Replay |
| - | - | - |
| `SubmitBatchOpen` | `Owner::plan_session_open` (batchable fresh/non-destructive open) | `concurrent_distinct_opens_pipeline_and_serialize_conflicts_and_exclusives` |
| `SubmitExclusiveOpen` | `Owner::plan_session_open` (exclusive takeover / destructive clean-start / at-capacity open) | `concurrent_distinct_opens_pipeline_and_serialize_conflicts_and_exclusives` |
| `SubmitExclusiveClose` | `Owner::plan_session_end`, `Owner::plan_session_detach`, `Owner::plan_session_expire` | `concurrent_distinct_opens_pipeline_and_serialize_conflicts_and_exclusives` |
| `GroupCommitBatch` | `writer.rs` batching consecutive `ControlRequest::Lifecycle(Persist)` via `StoreBackend::apply_group` | `burst_of_distinct_session_opens_batches_into_fewer_log_commits` |
| `CommitExclusive` | `writer.rs` `control_response` single exclusive lifecycle commit | `concurrent_distinct_opens_pipeline_and_serialize_conflicts_and_exclusives` |
| `TimeoutAndResolve` | `Owner::plan_session_deadline` emitting `ResolveAndFence` | `concurrent_distinct_opens_pipeline_and_serialize_conflicts_and_exclusives` |
| `ResolveIndeterminate` | store `resolve_and_fence` settling timed-out flight | `concurrent_distinct_opens_pipeline_and_serialize_conflicts_and_exclusives` |
| `ApplyResult` | `Owner::plan_session_result`, `Owner::apply_session` | `concurrent_distinct_opens_pipeline_and_serialize_conflicts_and_exclusives` |
| `Done` | quiescent state (`flights = {}`) | `burst_of_distinct_session_opens_batches_into_fewer_log_commits` |
