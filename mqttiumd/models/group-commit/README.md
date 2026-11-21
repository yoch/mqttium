# Group commit of the store

`MGroupCommit` models `SqliteStore::apply_group`
(`crates/mqttiumd-store-sqlite/src/group.rs`, decision record 0045): K
commands in one transaction of the engine, one savepoint per command, one
`COMMIT`, and the answers only after it. The store worker forms the
groups and delivers the answers (`group_responses` and the worker loop in
`crates/mqttiumd/src/writer.rs`).

Its actions are the steps of `apply_group`, `member` and `savepoint`, and
its sidecar lists the tests of the store crate that replay them. The
store is in the core tier (ADR 0044): this model adds to its evidence and
replaces none of it. It is not a proof of the code.

## Running

From `mqttiumd/`:

```bash
python models/check_models.py
python ../tools/formal/run_tlc.py --models-dir models/group-commit --list
python ../tools/formal/run_tlc.py --models-dir models/group-commit
```

16 configurations, the whole directory in about fifteen seconds; the
largest explores 2,056 states.

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
group that staged something passes K1 `store.commit.prepared` and calls
`COMMIT`, which makes every staged member durable or fails and makes every
member `PendingOrUnknown`. The engine may drop the transaction during a
member: the group is lost and every member is unknown. The process may be
killed at any point from `BEGIN` to the last answer.

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
- `MAX_GROUP`, the empty group and the group of one, which is `apply`.
- How the worker forms a group: consecutive `Persist` commands waiting in
  its request lane, at most `GROUP`, none while test controls exist. The
  store returns the answers together; `group_responses` turns them into
  replies and the worker loop publishes them one at a time, in the order
  of the requests, which is what `Release` is. A kill between two of them
  is `reach-crash-after-partial-release`.
- Recovery after the kill: `established` and `resolve_and_fence` on the
  identities of the group are those of `apply` and of model M1.

## Properties

| Invariant | Meaning |
| - | - |
| `AckAfterCommit` | An answer that says committed or rejected is handed over only for a fact that is on disk. |
| `NothingReleasedInTransaction` | While the transaction of the group is open no answer has left. |
| `GroupAtomic` | The disk holds what it held before the group or, once the group committed, that and every staged member. |
| `CrashBeforeCommitLosesGroup` | A kill before the `COMMIT` loses the whole group and no answer was handed over for it. |
| `CommitFailureSaysNothingCertain` | A commit that failed leaves nothing and every answer handed over is `PendingOrUnknown`, also for a member that was only recognised. |
| `NoPartialMemberCommitted` | A refused or faulted member leaves none of its rows in the group. |
| `RefusalIsolated` | In a committed group every accepted member is applied, every recorded rejection is on disk and every answer agrees with the disk. |
| `EveryMemberAnswered` | Under weak fairness of the worker, every member is answered unless the process is killed. |

## Mutants

| Configuration | Change | Violated |
| - | - | - |
| `mut-release-before-commit`, `mut-release-before-commit-ack` | The answers leave with the group staged and `COMMIT` not called. | `CommitFailureSaysNothingCertain` when the commit then fails; `AckAfterCommit` at once |
| `mut-refusal-aborts-group`, `mut-refusal-aborts-group-ack` | A refused member rolls the whole transaction back and the group goes on. | `RefusalIsolated`: an accepted member before it is not applied; `AckAfterCommit`: it was answered committed |
| `mut-release-at-savepoint`, `mut-release-at-savepoint-open` | A member's answer leaves when its savepoint is released. | `CrashBeforeCommitLosesGroup`: the kill loses the group after part of it was acknowledged; `NothingReleasedInTransaction` |

The `reach-` configurations show a committed group with a rejection
recorded between applied members, a kill after the commit with only some
answers handed over (every member is on disk and the caller resolves the
rest), a member recognised from an earlier member of its group, a group
that returns without a commit, and a commit that fails.

## Correspondence

The tests are those of `crates/mqttiumd-store-sqlite/tests/group_commit.rs`
unless another file is named. The two kill tests run in a build with the
feature `kill-points`.

| Model action | Code | Replay |
| - | - | - |
| `Begin` | `transaction_with_behavior(TransactionBehavior::Immediate)` in `apply_group` | `admissions_with_fan_out_and_their_releases_commit_as_one_group` |
| `MemberStage` | `recognise`, then `savepoint` around `stage_command` in `member` | `an_operation_presented_twice_in_one_group_is_decided_once`, `claims_finishes_and_subscription_changes_see_the_earlier_members` |
| `MemberUndo` | `ROLLBACK TO member`, `RELEASE member` in `savepoint` | `a_refused_member_is_recorded_and_its_neighbours_still_apply` |
| `MemberReject` | The second `savepoint`, around `stage_rejection` | `a_refused_member_is_recorded_and_its_neighbours_still_apply`, `a_member_without_a_receipt_slot_is_unknown_inside_a_group_too` |
| `MemberUndoFault` | `Txn::ReadFailed` and `Txn::NotWritten` in `member` | `a_member_whose_read_fails_is_unknown_and_the_group_still_commits` (`src/lib.rs` of the store crate) |
| `EngineDrops` | `tx.is_autocommit()` and `Lost` in `apply_group` | No test drops the transaction of the engine inside a group |
| `Decide` | The test `any member wrote` on the answers, before K1 | `an_empty_group_commits_nothing` is the nearest; no test names a group of recognised members only |
| `Commit` | K1, then `tx.commit()`, not called on a failing medium | `a_group_on_a_failing_medium_leaves_nothing_and_says_nothing_certain`, `a_group_passes_each_kill_point_once` |
| `Release`, `Finish` | The return of `apply_group`; `group_responses` and `rtx.try_send` in the worker loop (`writer.rs`) | `varied_groups_equal_their_sequential_application` compares the answers; no test of the product crate observes their delivery around a kill or a failed commit |
| `Crash` | The kill points K1 and K2b | `a_kill_before_the_commit_of_a_group_leaves_no_member`, `a_kill_once_the_commit_of_a_group_is_logged_keeps_every_member` |
| `Quiescent` | The end of a run of the model | Not an action of the code |

The sidecar lists under `replays` the tests that drive the store through
a trace of the model: a refusal recorded between applied members, a
member without a receipt slot, an operation presented twice, a member
whose read fails, a failing medium, and a kill on each side of the
commit. No test replays a mutant that lets an answer leave early:
`apply_group` returns its answers only after the commit, so that defect
cannot be driven without changing the function.
