# ADR 0045 — Group commit in the SQLite store

Status: **Accepted with amendments** (21 November 2026, decided under the
repository owner's delegation on the advice of the independent audit of
`8e4303d`; proposed on 20 November 2026, lot P4-store). It adds an
inherent method to `SqliteStore`; the `DurableStore` trait, the contracts
and every durable encoding are unchanged. The contract digest that
`validate` prints moves only because the register lists the accepted
records.

Amendment of acceptance (21 November 2026). The decision is accepted for
the SQLite adapter and the store worker, as qualified under
"Consequences": by two references, one of them the deterministic store of
the lab, and by the cases of the lost answers, of the transaction the
engine drops, of the commit that fails and of the largest group. It is
not qualified by an oracle campaign and says nothing of a volatile
backend. What "Left open" lists stays owed, first of all a permanent test
that kills the real worker between two answers of a group.

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
  with the same commands applied one after the other on two references:
  a second SQLite store, one transaction each (replies, snapshot and raw
  rows), and the deterministic store of the lab, which shares no code
  with the adapter (replies and snapshot, on a healthy medium, on a
  failing one and through a resolution). The comparison covers admissions
  with fan-out,
  releases, claims and finishes, subscription changes, retained
  publications and their replay, session close and replacement, a
  repeated operation, a recipe mismatch, refusals of each kind and a
  missing receipt slot. It covers the failing medium, the bound, and a
  process killed at each of the two points while it commits a group. A
  unit test faults each transactional read of a group in turn.
- Four further cases answer the independent audit of `8e4303d`. The
  answers of a committed group are lost: every member is found again by
  its identity with the outcome the lost answer carried, the refused one
  included, nothing is fenced or written, and the group presented again
  admits and releases nothing twice. The engine drops the whole
  transaction under a member (a trigger raising ROLLBACK): no member is
  answered or durable, those staged before the fault included. Every
  member is staged and the COMMIT itself fails (a deferred constraint):
  the same. The largest group of the largest admissions is bounded by the
  accounts of the store, and the member that does not fit is refused
  alone.
- The bytes of a group need no limit of their own. The worker forms a
  group of at most 32 commands, the store accepts at most 64, and each
  command is bounded by the limits of the store (one payload of at most
  32,768 bytes and one topic of at most 1,024 in the product profile):
  one transaction of the product carries at most about 1.1 MiB of
  payload and topic, and never more than the payload account allows.
- A group waits for nothing: it is made of the commands already in the
  request lane when the worker takes the first, so it adds no voluntary
  latency. A command waits only for the commit of the group before its
  own, as it waited for the commits before it when each was alone.
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
  order after the commit. It waits for no further command: the waiting
  delay of ARC-020 is not implemented, and its byte bound follows from
  the bounds of the members (see above) instead of a limit of the group.
  No group is formed while test controls are armed, so the instrumented
  receptions exercise groups of one.
- **Answers delivered in part.** The members of a group are committed
  together and their answers reach the core one by one. A process that
  dies in between has delivered some and not the others; the store then
  holds every member, which is the case of the lost answers above. The
  audit observed it on the real worker (one answer of two published,
  then a kill, then both recognised without a write). No permanent test
  drives the worker itself through that kill.
- **A lost diagnostic.** A recipe presented under an identity that
  already holds another recipe is a violation (`RecipeMismatch`) when it
  is applied alone. Inside a group that is not committed it is answered
  `PendingOrUnknown` like every other member, and a resolution by that
  identity returns the decision of the earlier recipe. The core takes
  each operation identity from a counter that only advances and freezes
  the recipe with it, so the case is a defect of the core, not a state it
  reaches; a group that failed would hide such a defect.
- **Model.** M1 models the atomic `Commit` of one `ApplyCommand`. A group
  is K `ApplyCommand` steps with no crash between them; the TLA+ model
  `MGroupCommit` (`models/group-commit`) states it and names the tests of
  `tests/group_commit.rs`. It has not been reviewed independently.
- **The other controls.** `resolve_and_fence`, the receipt collections,
  `admin`, `expire_messages`, `checkpoint_session_clock` and `open_epoch`
  stay one transaction each and are not members of a group.
- **The deterministic store** has no group: it is the sequential
  reference the differential tests judge the adapter by. A campaign of
  the oracle does not exercise `apply_group`.
- **The model** `MGroupCommit` has no outcome "the commit is durable and
  an error is returned"; the lost answers above are its executable
  counterpart.
- **Volatile backend** of ARC-020: not addressed.
