# 0020 — A kill inside a transaction of the store: two injection points, and what a run claims

Status: **Accepted** (21 November 2026, by the repository owner;
proposed 15 November 2026, lot L3, second wave). Written by
lot L3 at the request of the integration owner, for the plan of lot L7
(`docs/missions/L7-kill-inside-a-transaction.md`), to be recorded by them.
It completes records 0015 (the injection points) and 0019 (a real kill).

## Decision

- **Two points** in `contracts/v1/fault-points.json`, each `carries:
  command` and `actions: [barrier]`:
  - `store.commit.prepared` (K1): inside the transaction, the guards and
    writes of the command and the meta row written, the commit not; a
    kill leaves the state before the command;
  - `store.commit.logged` (K2b): SQLite's WAL hook runs after the commit
    and after SQLite releases its write lock, before the outcome is
    available to the adapter. No `StoreCommitStable` is written; a kill
    leaves the state after the command, which the recovery shows.
  A barrier there is where the child parks and the supervisor kills it;
  holding a command there (`fault-action-unknown`) or selecting a result
  (`fault-match-invalid`) is refused by both readers. No record kind is
  added: `FaultHit`, `CrashBroker` and `DriveUntil FaultHit` exist.
- **What a run claims.** No manifest member is added. A run kills inside
  a transaction exactly when its scenario has a fault at one of the two
  points: the scenario, which the manifest and the result cite by digest,
  already says it, and the oracle reads it. Such a kill exists only with a
  broker run as a child process: the trace header must claim
  `broker: child-process` (record 0019). The assertion
  `transactional_kill_matches_recovery`, declared by scenarios
  `tests/scenarios/crash/{K1,K2b,K3}.json`, checks the point and operation
  named by `FaultHit`, its ordering before a signal-9 kill, the lack of a
  result before that kill, and the first restarted incarnation's complete
  recovery inventory corroborated by `RecoveryPageRead`. K1's interrupted
  operation is absent and has no `StoreCommitStable`; K2b's is Committed
  without `StoreCommitStable`; K3's is Committed and its stable record
  precedes its point. Missing inventory is inconclusive, a contradicted
  expectation fails. Commits of a new epoch or retransmission are separate
  operations and are not forbidden by the interrupted command's absence.
  The sentence of the bench's failure model (`campaign.json`, `covers.crash`) loses "never inside a
  transaction" for such a run: that sentence is lot L4's.
- **What the oracle concludes.** At K2b a commit is durable without its
  `StoreCommitStable`: on a run whose broker is a child process,
  `one_admission_per_observed_inbound_exchange` counts the `Operation`
  fact `Committed` of the recovery that follows a `BrokerKilled` as the
  admission (oracle only, `fde0c038`). At K1 the operation is absent from
  the state after the restart and the replay is admitted in the next
  incarnation; at K2b and K3 the effect is durable and no second admission
  is made.

## Recovery lifecycle observation

`BrokerRecovered.run_epoch` identifies the startup rank observed by the
surviving supervisor. The matching child origin incarnation and the
`RecoveryPageRead` / `RecoveryPageDelivered` `run_epoch` identify pages of
that startup. This rank is a process lifecycle identity, not the epoch
component of a durable operation id. For SQLite the oracle links a kill
to the first recovery through exactly one matching `BrokerRecovered`,
ordered after the kill, whose observed core parent belongs to that same
child incarnation. A mismatched or reused rank, a mismatched incarnation,
or absent startup evidence is inconclusive. Read-before-delivery and exact
page content remain required. The oracle neither treats collector arrival
as causality nor adds an edge from the supervisor into product startup;
`Trace.before` is unchanged. The simulated backend retains its declared
total observation order.

For an admission observed only through SQLite recovery, the Committed
operation is joined by `OperationId` to its acknowledged original
`CommandSubmitted`. The causally observed peer PUBLISH of that submission
and the peer's own local order identify its exchange. Recovery establishes
the durable outcome; its collector position is not used to place the
admission in the client's history. Missing or ambiguous source evidence
leaves that attribution inconclusive.

## Test instrumentation and checkpoint policy

The lab's `kill-points` feature forwards to the store feature and is off
by default. A feature-enabled run records `build_features` in
`campaign.json` and the store's `kill_points` in `backend_settings`; the
source reconstruction command includes the
same feature in `artifact.built_from.build`, and the second build follows
that command. Cargo feature unification can instrument a dependency of
another workspace member; `tools/check_kill_points.sh` checks a separate
normal product build rather than assuming package isolation.

Arming the store hook disables autocheckpoint and reads back zero before
installing the callback last. Disarming removes that callback before
restoring the saved threshold. A threshold change while armed is refused.
The effective `wal_autocheckpoint` and `checkpoint_policy` are recorded,
including `disabled-while-kill-hook-armed`. PRAGMA readback alone is not
proof of installation: versioned tests observe actual callback invocation,
show that a later autocheckpoint setting replaces it, and demonstrate that
another SQLite connection can acquire `BEGIN IMMEDIATE` inside the hook.
This is characterised on the linked rusqlite 0.43.0 / SQLite 3.54.1.

The child streams the decisive record to the surviving collector, waits
for its acknowledgement, then reports `Parked(last_emission)` while the
command remains in flight. The supervisor accepts a kill only when this
acknowledged frontier is retained, or when an ordinary action has ended
and drained. A missing point, a closed channel or an expired deadline
stops the run explicitly; it does not establish a crash at that point.

## Out of the oracle

- A partial recipe, or a durable state that mixes two states (a
  publication without its obligations, accounts that do not match): the
  oracle checks references (record 0016, F2) but not that the recovered
  state is a prefix of the committed sequence. That equality, on the whole
  `DurableState`, is the store's test (lot L7), against the deterministic
  store.
- K2a, random kills during `COMMIT`, is not a scenario and not a verdict
  of the oracle: its report says so at its head.
