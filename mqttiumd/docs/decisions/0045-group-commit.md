# ADR 0045 — Group commit in the SQLite store

Status: **Proposed** (20 November 2026, lot P4-store). It binds nobody
until the repository owner decides it. It adds an inherent method to
`SqliteStore`; the `DurableStore` trait, the contracts and every durable
encoding are unchanged, so the contract digest that `validate` prints does
not move.

## Context

Every command of the SQLite store is one `BEGIN IMMEDIATE` … `COMMIT`
transaction with `synchronous=FULL`, so every command pays one
synchronisation of the WAL. Report 0001 and the audit behind record 0044
measured a few thousand such transactions per second, which caps the
durable QoS 1 throughput of the broker far below the target profile.

ARC-020 provides for bounded batches: "Les lots sont bornés en nombre
d'opérations, octets et délai d'attente. Chaque réponse positive attend le
commit externe du lot. Un lot abandonné ne doit laisser ni quotas débités
ni projection spéculative déclarée engagée." It lets a first reference
cancel a whole batch on a conflict and requires of a later use of
savepoints "les mêmes garanties pour les opérations indépendantes".

The store contract and its evidence are built on one command per
transaction: the kill points K1 `store.commit.prepared` and K2b
`store.commit.logged` (record 0020), the deterministic twin of the lab,
the loop mutants and the atomic `Commit` of model M1.

## Decision

**A group is K commands in one transaction.**
`SqliteStore::apply_group(&mut self, commands: &[&Command]) ->
Result<Vec<Applied>, GroupRefused>` opens one `BEGIN IMMEDIATE`
transaction, runs each command in order, and ends with one `COMMIT`.
`MAX_GROUP` is 64 commands. A larger group is refused with
`GroupRefused::TooLarge` before anything is read; the commands are
borrowed and stay with the caller. An empty group does nothing. A group of
one is `apply`.

**A member runs the code of `apply`.** The reads that recognise an
operation already decided, the function that stages the guards and rows of
a command, and the function that stages the receipt of a rejection are
each one function, called by `apply` and by a member. They differ only in
what surrounds them:

| Step | `apply` | Member of a group |
| - | - | - |
| Recognition reads | Outside any transaction | Inside the transaction of the group, before the member's savepoint: they see the members before it |
| Guards, rows, meta revision | A transaction of its own | `SAVEPOINT`, released on success |
| Refusal by a guard | The transaction is rolled back | `ROLLBACK TO` the savepoint, then `RELEASE` |
| Rejection receipt | A second transaction; without a receipt slot, nothing certain is said | A second savepoint in the same group; the same rule without a slot |
| Read or write fault | Rolled back, `PendingOrUnknown` | Rolled back to the savepoint, `PendingOrUnknown`; the group goes on |
| Commit | One per command or receipt | One for the group |

The meta revision advances once for each member that stages something, as
it does for each transaction of `apply`. After a committed group the
answers, the bounded snapshot and the rows of every table are those of the
same commands applied one after the other.

**Ack after commit.** The answers are returned only after `COMMIT`
returned. Nothing a member computed leaves the store before that.

**A group that is not committed says nothing certain.** When the group
staged at least one write and was not committed — the medium is failing
(`Durability::Failing`), `COMMIT` fails, the engine dropped the
transaction on an error, or a savepoint could not be closed — the
transaction is rolled back and every member is answered
`StoreOutcome::PendingOrUnknown` with `committed_now` and `wrote` false:
the answer `apply` gives for a commit that failed. This holds for the
member that would have been rejected and for the member that was only
recognised: an answer computed inside a transaction that was rolled back
is not a durable fact, and absence is not rejection. No quota stays
charged: the accounts are in the meta row, rolled back with the rest.
`established` and `resolve_and_fence` then behave on these identities as
after a failed `apply`.

A group in which no member staged anything has nothing to commit. Its
answers were read from the state that was durable before it, as `apply`
reads them, and are returned as they are, also while the medium fails.

**Crash granularity changes from one command to one group.** The two
injection points are unchanged and no point is added.
`store.commit.prepared` is passed once, when every member is staged and
before the `COMMIT` of the group: a kill there leaves no member.
`store.commit.logged` is the WAL hook of that one commit: a kill there
leaves every member. No state between two members of a group is ever
durable. `apply` keeps one command per transaction and both points keep
their meaning for it.

**Savepoints, not cancel-and-rebuild.** ARC-020 lets a first reference
cancel a whole batch on a conflict. A member that is refused here is
undone alone. The guarantee ARC-020 asks of savepoints is the one the
differential tests state: the members around a refused or faulted member
are exactly as after sequential application.

## Consequences

- `crates/mqttiumd-store-sqlite/tests/group_commit.rs` compares a group
  with the same commands applied one transaction each on a second SQLite
  store: replies, snapshot and raw rows, for admissions with fan-out,
  releases, claims and finishes, subscription changes, retained
  publications and their replay, session close and replacement, a
  repeated operation, a recipe mismatch, refusals of each kind and a
  missing receipt slot. It covers the failing medium, the bound, and a
  process killed at each of the two points while it commits a group. A
  unit test faults each transactional read of a group in turn.
- `apply`, `commit_rejection` and `transact` keep their behaviour; the
  conformance suite, K1/K2b/K3, the lab twin and the loop mutants run
  unchanged.
- One transaction now carries up to 64 commands: the WAL grows by the
  whole group before a checkpoint can run, and the engine's write lock is
  held for the whole group.
- The measurement `examples/measure_writer.rs` takes `--group N`. Its
  numbers are exploratory (record 0008) and qualify nothing.

## Left open

- **Group formation.** The store worker of `crates/mqttiumd/` forms a
  group from the consecutive commands already waiting in its request lane,
  up to the size of that lane (32), and publishes their answers in request
  order after the commit. It waits for no further command and has no byte
  bound: the waiting delay and the byte bound of ARC-020 are not
  implemented. No group is formed while test controls are armed.
- **Model.** M1 models the atomic `Commit` of one `ApplyCommand`. A group
  is K `ApplyCommand` steps with no crash between them; the TLA+ model
  `MGroupCommit` (`models/group-commit`) states it and names the tests of
  `tests/group_commit.rs`. It has not been reviewed independently.
- **The other controls.** `resolve_and_fence`, the receipt collections,
  `admin`, `expire_messages`, `checkpoint_session_clock` and `open_epoch`
  stay one transaction each and are not members of a group.
- **The deterministic store** has no group. A campaign of the oracle does
  not exercise `apply_group`; the differential tests above are its
  evidence.
- **Volatile backend** of ARC-020: not addressed.
