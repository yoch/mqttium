# 0020 — A kill inside a transaction of the store: two injection points, and what a run claims

Status: **Proposed** (15 November 2026, lot L3, second wave). Written by
lot L3 at the request of the integration owner, for the plan of lot L7
(`docs/missions/L7-kill-inside-a-transaction.md`), to be recorded by them.
It completes records 0015 (the injection points) and 0019 (a real kill).
It becomes a requirement only when the owner of the repository accepts it.

## Decision

- **Two points** in `contracts/v1/fault-points.json`, each `carries:
  command` and `actions: [barrier]`:
  - `store.commit.prepared` (K1): inside the transaction, the guards and
    writes of the command and the meta row written, the commit not; a
    kill leaves the state before the command;
  - `store.commit.logged` (K2b): the commit frame written and synced,
    `COMMIT` not returned, no `StoreCommitStable` written; a kill leaves
    the state after the command, which only the recovery shows.
  A barrier there is where the child parks and the supervisor kills it;
  holding a command there (`fault-action-unknown`) or selecting a result
  (`fault-match-invalid`) is refused by both readers. No record kind is
  added: `FaultHit`, `CrashBroker` and `DriveUntil FaultHit` exist.
- **What a run claims.** No manifest member is added. A run kills inside
  a transaction exactly when its scenario has a fault at one of the two
  points: the scenario, which the manifest and the result cite by digest,
  already says it, and the oracle reads it. Such a kill exists only with a
  broker run as a child process: the trace header must claim
  `broker: child-process` (record 0019). The assertion that checks it
  comes with the scenarios K1, K2b and K3. The sentence of the bench's
  failure model (`campaign.json`, `covers.crash`) loses "never inside a
  transaction" for such a run: that sentence is lot L4's.
- **What the oracle concludes.** At K2b a commit is durable without its
  `StoreCommitStable`: on a run whose broker is a child process,
  `one_admission_per_observed_inbound_exchange` counts the `Operation`
  fact `Committed` of the recovery that follows a `BrokerKilled` as the
  admission (oracle only, `fde0c038`). At K1 the operation is absent from
  the state after the restart and the replay is admitted in the next
  incarnation; at K2b and K3 the effect is durable and no second admission
  is made.

## Out of the oracle

- A partial recipe, or a durable state that mixes two states (a
  publication without its obligations, accounts that do not match): the
  oracle checks references (record 0016, F2) but not that the recovered
  state is a prefix of the committed sequence. That equality, on the whole
  `DurableState`, is the store's test (lot L7), against the deterministic
  store.
- K2a, random kills during `COMMIT`, is not a scenario and not a verdict
  of the oracle: its report says so at its head.
