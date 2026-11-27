# Group commit of the store

`MGroupCommit` models `SqliteStore::apply_group`
(`crates/mqttiumd-store-sqlite/src/group.rs`, decision record 0045): K
commands in one transaction of the engine, one savepoint per command, one
`COMMIT`, and the answers only after it. The store worker forms the
groups and delivers the answers (`group_responses_into` and the worker
loop in `crates/mqttiumd/src/writer.rs`). A caller that was told nothing
certain about a member reconciles it by its identity.

Its actions are the steps of `apply_group`, `member` and `savepoint`, then
`resolve_and_fence` and the recognition reads of a command presented
again. Its sidecar lists the tests that replay them, in the store crate
and, for the worker killed between two answers, in the product crate. The
store is in the core tier (ADR 0044): this model adds to its evidence and
replaces none of it. It is not a proof of the code.

## Scope

The model covers one store and one caller:

- **The SQLite store**, storage mode `durable`. The worker calls the same
  entry point on the two other stores, and neither is this model.
  `LogStore::apply_group` (mode `log`) writes the commands of a group as
  one frame of its log, synchronises it, and only then applies them in
  memory: there is no transaction, no savepoint and no `COMMIT` to fail
  after it was logged. `VolatileStore::apply_group` (mode `volatile`)
  applies them in memory one after the other. Their comments claim the
  answers of `SqliteStore::apply_group`; that claim rests on their own
  tests, not on this model, and nothing here was checked against them.
- **One owner**, the default runtime (`--delivery-shards 0`): the groups
  are formed from one request lane and the answers leave on one reply
  lane, in the order of the requests. With `--delivery-shards K >= 1`
  (record 0066) the worker takes its requests from 1 + K lanes, the
  shards' first up to a budget, then the Coordinator's, then the shards'
  again, and puts each answer on the lane of the owner that sent the
  command. One group then holds commands of several owners, and the
  answers of different owners leave in no fixed relative order. Neither
  the choice among lanes nor that delivery is modelled: `Release` hands
  the answers over in the order of the members. A scratch run of the four
  passing safety configurations and of the liveness one with that order
  removed from `Release` passes (49,942 states for the largest): no
  property here depends on it. That is a statement about the model, not a
  check of the worker with several lanes.

## Running

From `mqttiumd/`:

```bash
python models/check_models.py
python ../tools/formal/run_tlc.py --models-dir models/group-commit --list
python ../tools/formal/run_tlc.py --models-dir models/group-commit
```

20 configurations, the whole directory in about half a minute; the
largest explores 30,118 states.

## What is modelled

A group of two or three members. Each member runs the steps of `member`
in `group.rs`:

- the recognition reads, inside the transaction, see the members before
  it: an operation presented twice (`Repeats`) or already committed
  before the group (`Settled`) is answered from what is there, and stages
  nothing;
- otherwise a savepoint is opened and the command is accepted and
  released into the group, refused by a guard, or stopped by a fault
  after some of its rows were written;
- a refused member is rolled back to its savepoint, and its rejection is
  recorded on a receipt in a second savepoint; without a receipt slot it
  is answered `PendingOrUnknown`;
- a faulted member is rolled back to its savepoint and answered
  `PendingOrUnknown`; the group goes on.

Then: a group that staged nothing returns its answers without a commit; a
group that staged something writes its meta row once (`write_meta`: each
member advanced the revision in memory), passes K1
`store.commit.prepared` and calls `COMMIT`, which has three results:

- it returns, and every staged member is durable;
- it fails, nothing is durable and every member is `PendingOrUnknown`. A
  meta row that could not be written before K1 is this result as well;
- every staged member is durable and the call returns an error. SQLite
  reports the result of its write-ahead-log callback as the result of
  `COMMIT`, after the transaction was logged. The store sees a commit
  that failed: every member is `PendingOrUnknown`, and every member is on
  disk.

The engine may drop the transaction during a member: the group is lost
and every member is unknown. The process may be killed at any point from
`BEGIN` to the last answer.

Once the call is over, by its last answer or by a kill, the caller
reconciles each member whose answer was unknown or never arrived:

- `Resolve` is `resolve_and_fence` on its identity. A durable decision is
  returned as it is and nothing is written; an absence is fenced, so the
  operation never executes;
- `Represent` presents the same command again. The recognition reads
  answer a decided or fenced operation from what is on disk and write
  nothing; an operation that is absent is decided as a first submission.

`effects` counts, for each operation, how many times its effect was made
durable. No guard reads it.

## What is abstracted away

- The engine. The model takes from SQLite that `COMMIT` is atomic (K1
  leaves no member, K2b `store.commit.logged` leaves every member), that
  `ROLLBACK TO` restores the state at its `SAVEPOINT`, and that a
  transaction which is not committed leaves nothing. It shows nothing
  about `fsync`, the WAL or a power loss.
- The data. An operation is `absent`, `partial`, `applied` or `rejected`;
  rows, guards, quotas, the meta revision and the reason of a rejection
  are folded into that. That a group equals its sequential application
  row for row is the claim of the differential tests, not of this model.
- The caches of the connection (`StoreCaches`, `TxDelta`). They hold
  capacities of receipts, never a decision: `recognise_with_meta` reads
  the operation and the fence from the tables, inside the transaction.
  What a group adds to them is merged only after its `COMMIT` returned
  (`TxDelta::commit_into`), and they are cleared whenever a group is not
  committed or its commit returns an error (`clear_receipt_caches`).
- `MAX_GROUP`, the empty group and the group of one, which is `apply`.
- How the worker forms a group: the `Persist` commands waiting in its
  request lane, at most `GROUP` (which is `MAX_GROUP` for this store),
  none while test controls exist. A `LoadWork` read found among them
  does not end the group: it is answered after the commit. The store
  returns the answers together; `group_responses_into` turns them into
  replies, queued for their owner, and the worker loop publishes them one
  at a time (`rtx.try_send`), in the order of the requests, which is what
  `Release` is. A kill between two of them is
  `reach-crash-after-partial-release`.
- Reconciliation. `Resolve` and `Represent` take one member at a time,
  in any order and in any mix. A command presented again and found absent
  is accepted; that it could be refused, and the read-only `established`,
  add nothing to the question of a second effect. The fence is one durable
  state; its receipt slot and the reason `Fenced` are folded into it: a
  resolution that finds no receipt slot writes no fence and says nothing
  certain, which is the absence of a `Resolve` step here. How the core
  chooses between resolving and presenting again is not here, and
  neither is recovery by pages (model M1).
- Why the call of a durable commit fails. The model has the outcome, not
  the write-ahead-log callback that produces it in the tests.

## Properties

| Invariant | Meaning |
| - | - |
| `AckAfterCommit` | An answer that says committed or rejected is handed over only for a fact that is on disk. |
| `NothingReleasedInTransaction` | While the transaction of the group is open no answer has left. |
| `GroupAtomic` | The disk holds what it held before the group or, once the group committed, that and every staged member. |
| `CrashBeforeCommitLosesGroup` | A kill before the `COMMIT` loses the whole group and no answer was handed over for it. |
| `CommitFailureSaysNothingCertain` | A commit that failed leaves nothing and every answer handed over is `PendingOrUnknown`, also for a member that was only recognised. |
| `UncertainCommitSaysNothingCertain` | A commit that is durable and whose call returned an error hands over `PendingOrUnknown` only, for members that are all on disk: no answer is certain before the caller reconciles. |
| `ReconciledFromDisk` | What a reconciliation tells the caller is what the disk holds, and only a member whose answer said nothing certain is reconciled. |
| `NoSecondEffect` | No operation takes effect twice, through the group or through any reconciliation, and a fence closes only an operation that never took effect. |
| `NoPartialMemberCommitted` | A refused or faulted member leaves none of its rows in the group. |
| `RefusalIsolated` | In a committed group every accepted member is applied, every recorded rejection is on disk and every answer agrees with the disk. |
| `EveryMemberAnswered` | Under weak fairness of the worker, every member is answered unless the process is killed. |

## Mutants

| Configuration | Change | Violated |
| - | - | - |
| `mut-release-before-commit`, `mut-release-before-commit-ack`, `mut-release-before-commit-uncertain` | The answers leave with the group staged and `COMMIT` not called. | `CommitFailureSaysNothingCertain` when the commit then fails; `AckAfterCommit` at once; `UncertainCommitSaysNothingCertain` when the commit is then durable and its call returns an error |
| `mut-refusal-aborts-group`, `mut-refusal-aborts-group-ack` | A refused member rolls the whole transaction back and the group goes on. | `RefusalIsolated`: an accepted member before it is not applied; `AckAfterCommit`: it was answered committed |
| `mut-release-at-savepoint`, `mut-release-at-savepoint-open` | A member's answer leaves when its savepoint is released. | `CrashBeforeCommitLosesGroup`: the kill loses the group after part of it was acknowledged; `NothingReleasedInTransaction` |
| `mut-uncertain-is-not-committed` | The error of the commit call is believed: what the group left is taken for absent and the command presented again is applied. | `NoSecondEffect`: the operation takes effect twice |

The `reach-` configurations show a committed group with a rejection
recorded between applied members, a kill after the commit with only some
answers handed over (every member is on disk and the caller resolves the
rest), a member recognised from an earlier member of its group, a group
that returns without a commit, a commit that fails, a commit that is
durable and returns an error after which the caller finds every member
committed, and a member fenced after a commit that failed.

## Correspondence

The tests are those of `crates/mqttiumd-store-sqlite/tests/group_commit.rs`
unless another file is named. The two kill tests of that file and the
tests of `src/commit_error_tests.rs` run in a build with the feature
`kill-points`; those of `crates/mqttiumd/tests/writer_group_kill.rs` run
in the normal build of the product and in its instrumented one. They form
a group of a known size by leaving the reply lane of the worker full, so
that the worker takes no request while the commands are queued. That lane
holds twice `QUEUE` answers; the tests filled 64 of them, which holds the
worker no longer, and formed their group only because the commands
happened to be queued in time. They fill the lane again.

| Model action | Code | Replay |
| - | - | - |
| `Begin` | `transaction_with_behavior`, a `BEGIN IMMEDIATE`, in `apply_group` | `admissions_with_fan_out_and_their_releases_commit_as_one_group` |
| `MemberStage` | `recognise_with_meta`, then `savepoint` around `stage_command` in `member` | `an_operation_presented_twice_in_one_group_is_decided_once`, `claims_finishes_and_subscription_changes_see_the_earlier_members` |
| `MemberUndo` | `ROLLBACK TO member`, `RELEASE member` in `savepoint` | `a_refused_member_is_recorded_and_its_neighbours_still_apply` |
| `MemberReject` | The second `savepoint`, around `stage_rejection` | `a_refused_member_is_recorded_and_its_neighbours_still_apply`, `a_member_without_a_receipt_slot_is_unknown_inside_a_group_too` |
| `MemberUndoFault` | `Txn::ReadFailed` and `Txn::NotWritten` in `member` | `a_member_whose_read_fails_is_unknown_and_the_group_still_commits` (`src/lib.rs` of the store crate) |
| `EngineDrops` | `tx.is_autocommit()` and `Lost` in `apply_group` | `a_transaction_the_engine_drops_under_a_member_leaves_nothing_of_the_group` |
| `Decide` | `any_wrote` on the answers of the members, before `write_meta` and K1 | A group of recognised members only, which returns without a commit: `a_committed_group_whose_answers_were_lost_is_found_again_by_identity` (the group presented again, no member writes, the state is the same), `a_group_on_a_failing_medium_leaves_nothing_and_says_nothing_certain` (two committed members answered on a failing medium) and, through the real worker, the five tests of `crates/mqttiumd/tests/writer_group_kill.rs`, which present the committed group again and compare the file, its revision included |
| `Commit` | `write_meta`, K1, then `tx.commit()`, not called on a failing medium | `a_group_on_a_failing_medium_leaves_nothing_and_says_nothing_certain`, `a_group_passes_each_kill_point_once`; a commit that fails on a deferred constraint: the second half of `a_transaction_the_engine_drops_under_a_member_leaves_nothing_of_the_group`; the result "durable, and the call returns an error": `a_group_whose_durable_commit_returns_an_error_is_reconciled_without_a_second_effect` and, on the engine alone, `the_engine_reports_an_error_for_a_commit_it_made_durable` (`src/commit_error_tests.rs`). No test makes `write_meta` fail |
| `Release`, `Finish` | The return of `apply_group`; `group_responses_into`, which queues each answer for its owner, and `rtx.try_send` in the worker loop (`writer.rs`) | `varied_groups_equal_their_sequential_application` compares the answers; the five tests of `crates/mqttiumd/tests/writer_group_kill.rs` take none to four of the five answers of a group from the real worker, on its one lane. No test of the product crate observes the delivery around a failed commit, and none the delivery of one group to several owners |
| `Crash` | The kill points K1 and K2b; `SIGKILL` of the process of the worker | `a_kill_before_the_commit_of_a_group_leaves_no_member`, `a_kill_once_the_commit_of_a_group_is_logged_keeps_every_member`; after the commit, between two answers: `a_worker_killed_after_one_answer_of_its_group_left_every_member` and its four siblings (`writer_group_kill.rs`) |
| `Resolve` | `resolve_and_fence` (`src/lib.rs` of the store crate) | `a_committed_group_whose_answers_were_lost_is_found_again_by_identity`, the two reconciliation tests of `src/commit_error_tests.rs`; the fence of an absence: `a_group_on_a_failing_medium_leaves_nothing_and_says_nothing_certain` |
| `Represent` | `recognise`, called by `apply`, and `recognise_with_meta`, which it ends in and which `member` calls | The same tests, and the five tests of `writer_group_kill.rs`, which present the group again to a new worker |
| `Quiescent` | The end of a run of the model | Not an action of the code |

The sidecar lists under `replays` the tests that drive the store through
a trace of the model: a refusal recorded between applied members, a
member without a receipt slot, an operation presented twice, a member
whose read fails, a failing medium, a transaction the engine drops, a
kill on each side of the commit, the answers of a committed group lost,
a commit that is durable and returns an error, for a group and for one
command, and the real worker killed after each number of answers of a
group. No test replays a mutant that lets an answer leave early:
`apply_group` returns its answers only after the commit, so that defect
cannot be driven without changing the function. No test replays
`mut-uncertain-is-not-committed` either: the store recognises an
operation by its receipt whatever its caller believes, so the replays
show that recognition and the second effect cannot be driven without
removing it.
