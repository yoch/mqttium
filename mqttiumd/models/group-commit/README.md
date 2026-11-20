# Group commit of the store

`MGroupCommit` models `SqliteStore::apply_group` as decision record 0045
(proposed) describes it: K commands in one transaction of the engine, one
savepoint per command, one `COMMIT`, and the answers only after it.

It is a specification model. The record, `group.rs` and the tests of
`apply_group` belong to the store lot and are not on this branch; the
model was written from them, read in that lot's worktree. Its sidecar
therefore lists no test. Once both are merged the tests named in the
correspondence table can be listed and the status reviewed. The store is
in the core tier (ADR 0044): this model adds to its evidence and replaces
none of it.

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
- The delivery of the answers. The store returns them together. The model
  hands them over one member at a time, in order, because the writer that
  will deliver them (record 0045, left open) is another lot; a kill
  between two of them is `reach-crash-after-partial-release`.
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

The tests named are those of
`crates/mqttiumd-store-sqlite/tests/group_commit.rs` in the store lot.
They are not on this branch and the sidecar does not list them.

| Model action | Code (`group.rs`, store lot) | Replay |
| - | - | - |
| `Begin` | `transaction_with_behavior(TransactionBehavior::Immediate)` in `apply_group` | Not on this branch: `admissions_with_fan_out_and_their_releases_commit_as_one_group` |
| `MemberStage` | `recognise`, then `savepoint` around `stage_command` in `member` | Not on this branch: `an_operation_presented_twice_in_one_group_is_decided_once`, `claims_finishes_and_subscription_changes_see_the_earlier_members` |
| `MemberUndo` | `ROLLBACK TO member`, `RELEASE member` in `savepoint` | Not on this branch: `a_refused_member_is_recorded_and_its_neighbours_still_apply` |
| `MemberReject` | The second `savepoint`, around `stage_rejection` | Not on this branch: `a_refused_member_is_recorded_and_its_neighbours_still_apply`, `a_member_without_a_receipt_slot_is_unknown_inside_a_group_too` |
| `MemberUndoFault` | `Txn::ReadFailed` and `Txn::NotWritten` in `member` | Not on this branch: the unit test of the crate that faults each transactional read of a group in turn |
| `EngineDrops` | `tx.is_autocommit()` and `Lost` in `apply_group` | Not on this branch; no test named for it in the record |
| `Decide` | The test `any member wrote` on the answers, before K1 | Not on this branch: `an_empty_group_commits_nothing` is the nearest; a group of recognised members only has no test named in the record |
| `Commit` | K1, then `tx.commit()`, not called on a failing medium | Not on this branch: `a_group_on_a_failing_medium_leaves_nothing_and_says_nothing_certain`, `a_group_passes_each_kill_point_once` |
| `Release`, `Finish` | The return of `apply_group` | Not on this branch: `varied_groups_equal_their_sequential_application` compares the answers; their delivery by the writer does not exist yet |
| `Crash` | The kill points K1 and K2b | Not on this branch: `a_kill_before_the_commit_of_a_group_leaves_no_member`, `a_kill_once_the_commit_of_a_group_is_logged_keeps_every_member` |
| `Quiescent` | The end of a run of the model | Not an action of the code |

No test replays a mutant: `apply_group` has no writer around it yet, so
"an answer left before the commit" cannot be driven on the code.
