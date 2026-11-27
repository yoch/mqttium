# The inline-claim lease of a sharded session

`MInlineLease` models one subscriber session with `--delivery-shards K >= 1`
(record 0066): the coordinator, which admits publications and may carry an
inline claim for the subscriber in the admission, and the delivery shard
that owns the session, which delivers. The two share an
`InlineClaimLease`; the coordinator reserves packet identifiers from it,
the shard publishes it again once it has read the session's work.

It is runtime-tier evidence: a TLA+ model of the message protocol between
two threads, with configurations for the behaviour of `9dfd568`, the
behaviour after report 0045 (`1ccddfb`), the repair of report 0046 and
mutants of that repair. It is not a proof of the Rust code and replaces
none of its tests.

## Questions

- Can two deliveries the shard holds for the session share a packet
  identifier? (`UniqueIds`)
- Does the subscriber receive the publications of one publisher in the
  order of their admission? (`InOrder`)
- Is anything sent twice on one connection? (`NoDuplicate`)
- When nothing more happens, has every committed publication been
  delivered, and may the coordinator claim inline again?
  (`TerminalDelivered`, `TerminalEligible`)

Every behaviour of the model is finite and ends in a state where no action
but `Done` is enabled (`Terminal`); deadlock checking is on, so no other
state is one where nothing can happen. The last two invariants are
therefore what the liveness properties would say under weak fairness of
every action, checked without fairness.

## The protocol

The coordinator admits publications `1..N` in order (`Admit`). When the
lease allows it (`InlineClaimLease::try_reserve`: the shard has
acknowledged the last dirty mark, the lease is eligible, there is room and
a free identifier) and no stored publication to the session is pending,
the admission carries an inline claim with a reserved identifier;
otherwise the publication is stored and the lease is marked dirty
(`InlineClaimOracle::mark_ineligible`), which records a barrier and sends
nothing.

The store applies admissions in order (`StoreApply`); its answer may be
unknown. The coordinator steps the answers in order (`Reply`) and learns
an unknown outcome later from a resolution (`Resolve`,
`Effect::ResolveAndFence`), which names no target. A committed inline
claim is sent as `CoordToShard::InlineCommitted`; a rejected one gives its
identifier back (`InlineClaimOracle::rollback`). A stored target is
notified (`ShardPool::notify_dirty`): the dirty mark is sent when no inline
claim of the session is pending, and kept as a barrier otherwise, sent by
`ShardPool::flush_dirty_barrier_if_pending` once none is.

A subscription replay commits a row of the session by a command of its own,
at any time, and the session is notified once the SUBACK is queued
(`ReplayCommit`). It is row 0, before every publication, and is not a
publication of the publisher: `InOrder` does not cover it.

The shard reads the coordinator's lane in order (`Recv`), applies the
committed inline claims it received (`ApplyInline`,
`Owner::step_shard_inline_committed`: a delivery it holds is left as it
is, any other is inserted), reads the session's committed rows when a
dirty message asked for it (`Scan`: a stored row is claimed with an
identifier beside those it holds, `Deliveries::free_packet_id`; a row the
coordinator claimed is taken with its identifier), sends its oldest unsent
delivery (`Emit`), finishes a delivery the subscriber acknowledged and
gives its identifier back to a published lease (`Ack`,
`InlineClaimLease::release_pi`), and publishes the lease once its reads
are done (`TryPublish`, `DeliveryShardWorker::try_publish_lease`,
`InlineClaimLease::publish_eligible`).

## Behaviours

| Flag | Code |
| - | - |
| `ForeignIds` | Report 0045: a claim of the shard avoids the identifiers the lease holds at the last dirty message. |
| `PublishWaits` | Report 0045: the shard does not publish the lease while a committed inline claim of the session waits to be applied. |
| `HoldBehind` | A committed inline claim is not sent while an inline admission of the session has an unknown outcome. The held claims are sent in the order of their admission once no outcome of the session is unknown, before any barrier. |
| `ScanGate` | The shard does not read the session while a dirty message with sequence 0 (claims pending) is its last, nor while a committed inline claim of the session waits to be applied. |
| `NotifyAfterStep` | A stored target is notified after the coordinator has stepped the commit, on the answer of the store and on a resolution alike, instead of when the answer arrives. |

`9dfd568` has no flag; `1ccddfb` has `ForeignIds` and `PublishWaits`.
The repair adds `HoldBehind`, `ScanGate` and `NotifyAfterStep`; with the
gate `ForeignIds` is not needed (`safety`), and does no harm
(`safety-with-foreign-ids`). The code keeps `PublishWaits`: it costs one
look at the waiting claims, and a deterministic test plays the state it
guards, which the gate makes unreachable.

## Configurations

| Configuration | Behaviour | Expected |
| - | - | - |
| `safety` | the repair, N = 2, unknown answers, a replay | every invariant holds (234,595 states) |
| `safety-with-foreign-ids` | the repair and `ForeignIds` | every invariant holds |
| `safety-three` | the repair, N = 3, answers known, no replay | every invariant holds (75,604 states) |
| `released-ids` | `9dfd568` | `UniqueIds` violated: the replay is claimed with the identifier of a pending inline claim (suspect S1) |
| `part2-order` | `1ccddfb` | `InOrder` violated: the second publication's claim commits while the first has an unknown outcome, and is sent first (suspect B1) |
| `part2-duplicate` | `1ccddfb` | `NoDuplicate` violated: a read made while the first claim's outcome is unknown takes its committed row; the subscriber acknowledges it; at the resolution the claim arrives and is inserted again |
| `part2-eligible` | `1ccddfb` | `TerminalEligible` violated: the step of a stored publication's commit marks the lease dirty after the notification was sent, and nothing sends that mark |
| `part2-delivered` | `1ccddfb` | `TerminalDelivered` violated: a stored publication whose answer was unknown is committed by a resolution, which notifies nobody |
| `mut-no-hold` | the repair without `HoldBehind` | `InOrder` violated |
| `mut-no-gate-ids` | the repair without `ScanGate` | `UniqueIds` violated |
| `mut-no-gate-duplicate` | the repair without `ScanGate` | `NoDuplicate` violated |
| `mut-gate-ignores-waiting-claims` | the gate opens at the barrier even while committed claims wait to be applied | `NoDuplicate` violated |
| `mut-barrier-before-release` | the barrier is flushed before the held claims are sent | `NoDuplicate` violated |
| `mut-no-notify-eligible` | the repair without `NotifyAfterStep` | `TerminalEligible` violated |
| `mut-no-notify-delivered` | the repair without `NotifyAfterStep` | `TerminalDelivered` violated |

## Scope

- One session and one connection: the attach of the first generation is
  the initial message; detach, a second attach, retire and restart are
  outside the model. The gate covers a later `Attach` with sequence 0 in
  the code as it covers `NotifyDirty`; that is not checked here.
- A read of the store is one atomic step that loads every committed row
  the shard does not hold and claims the stored ones. The pages, the
  cursors (`known_through`, `after`), the asynchronous claim commands and
  rows waiting for room are not modelled; nor is Receive Maximum: the
  shard sends whenever it holds an unsent row.
- The coordinator steps the answers of the store in their order. In the
  runtime an answer the core cannot apply for want of capacity goes back
  to the end of its lane; a later answer may then be stepped first. That
  case is not modelled.
- The lease's atomics are modelled as one step each, and so are the
  coordinator's step and what the runtime does after it.
- QoS 2, message expiry, QoS 0 and identifiers above the lease's bitmap
  are outside the model.

## Running

From `mqttiumd/`:

```bash
python models/check_models.py
python ../tools/formal/run_tlc.py --models-dir models/inline-lease
```

## Correspondence

Each counterexample of `1ccddfb` has a test that fails there, against a
sharded runtime (`runtime_tests/sharded_lease.rs`), with the
`hold-admission-result` hook for the unknown answer; the fence of the
shard is also played by hand on a shard worker
(`delivery_shard/fence_tests.rs`). The repair is
`Owner::names_stored_targets_at_commit` with the notification after the
step in `Runtime::drain_inputs`, `Owner::delegated_inline_unknown` with
`ShardPool::send_inline_committed` and `ShardPool::release_held_claims`,
and `Owner::shard_fence_scans` with
`DeliveryShardWorker::fence_after_message`.

| Model action | Code | Replay |
| - | - | - |
| `Admit` | `Owner::plan_admission`, delegated branch; `InlineClaimLease::try_reserve`; `InlineClaimOracle::mark_ineligible`; `Owner::apply_change` | `a_lease_is_eligible_again_after_a_publication_admitted_without_claim` |
| `StoreApply` | the store worker, `writer.rs`; the `hold-admission-result` hook gives an unknown answer | `an_inline_claim_does_not_overtake_one_whose_outcome_is_unknown` |
| `ReplayCommit` | the subscription's commit and its deferred notification, `Runtime::writer_results` | `a_shard_claim_takes_no_identifier_the_coordinator_reserved` sends its dirty message by hand |
| `Reply` | `Runtime::writer_results`, `Owner::apply_draft`, the shard calls after the step in `Runtime::drain_inputs` | `a_lease_is_eligible_again_after_a_publication_admitted_without_claim` |
| `Resolve` | `Effect::ResolveAndFence`, `Owner::apply_draft` | `a_stored_publication_committed_by_a_resolution_is_delivered` |
| `Recv` | `DeliveryShardWorker::drain_coord_messages`, `DeliveryShardWorker::fence_after_message` | `a_shard_claim_takes_no_identifier_the_coordinator_reserved` |
| `ApplyInline` | `DeliveryShardWorker::drain_pending_inline`, `Owner::step_shard_inline_committed` | `a_session_is_read_only_after_the_claims_that_came_before_its_barrier` |
| `Scan` | `Owner::plan_work`, `Deliveries::free_packet_id` | `a_read_of_the_store_does_not_deliver_a_claim_the_coordinator_still_owes` |
| `Emit` | `Owner::plan_delivery_emit` on the shard | `an_inline_claim_does_not_overtake_one_whose_outcome_is_unknown` |
| `Ack` | the shard's finish of a delivery, `InlineClaimLease::release_pi` | `a_read_of_the_store_does_not_deliver_a_claim_the_coordinator_still_owes` |
| `TryPublish` | `DeliveryShardWorker::try_publish_lease`, `InlineClaimLease::publish_eligible` | `a_published_lease_keeps_the_identifier_of_a_waiting_committed_claim` |
| `Done` | none: the run is over | none |
