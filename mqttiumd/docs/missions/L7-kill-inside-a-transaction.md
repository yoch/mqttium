# Transaction crash validation of the SQLite store

Lot L7 with L4 and L3, the increment after the first real crash. The store
points, multiprocess comparisons, command-in-flight protocol and scenarios
are implemented. Reception is recorded in
`../reports/0004-transactional-crash-reception.md`; the finite K2a exploration
has its own `../reports/0005-k2a-sampled-process-kills.md`. Read
`L7-sqlite-store.md` ("Later"), `L4-process-topology.md`
(the allowed states of a kill) and the open condition of model M1 in
`docs/reception/open-conditions.md`.

## Why

The first real crash kills the child after a commit returned, outside any
transaction (`tools/run_killed_campaign.sh`). Model M1 assumes that
`ApplyCommit` is atomic: publication, obligations, inbound slot and
accounts are committed together or not at all. That assumption is the
store's to discharge, by killing the writer inside the transaction and
showing that the file always recovers to the state before the command or
to the state after it, never to a mix.

## The points

The store has one write path, `SqliteStore::transact`: `BEGIN IMMEDIATE`,
the guards and writes of the command, the meta row one revision later,
`COMMIT`. Every command passes there, so every point below is one place.

| Point | Where | Owner | Deterministic |
| - | - | - | - |
| K1 `store.commit.prepared` | in `transact`, after the meta row is written, before `tx.commit()` | store (L7) | yes |
| K2a variable-delay process kills | command released; its position within the store/engine is generally unknown | exploratory tool | no: sampled delays do not locate a kill inside `COMMIT` |
| K2b `store.commit.logged` | the WAL hook after commit and release of SQLite's write lock; `COMMIT` has not returned to the adapter | store (L7) | yes, with a cost below |
| K3 `store.commit.applied` | the existing barrier of the adapter: `COMMIT` returned, no result exists | adapter (L2), exists | yes |

Out of the production binary. K1 and K2b live behind a cargo feature
`kill-points` of `mqttiumd-store-sqlite`, off by default, enabled only by
the child build of the bench. The hook is a plain callback set on the
store (`Box<dyn FnMut(KillPoint) + Send>`), compiled out without the feature; no `unsafe`:
K2b uses rusqlite's safe `Connection::wal_hook` (feature `hooks`). The
store records `kill_points: bool` in its effective settings. The campaign
records `build_features` and `backend_settings.kill_points`; the manifest's
reconstruction command includes the same feature option. The lab rejects
dependency feature unification that instruments the store without enabling
its own forwarding feature, before any campaign write. A check fails a build of the
product in which the feature is on (`cargo tree -e features`).

Cost of K2b. A WAL hook replaces the engine's autocheckpoint
(`sqlite3_wal_autocheckpoint` is itself a WAL hook). A run armed at K1 or
K2b disables autocheckpoint, reads back zero, then installs its hook last.
Removing the hook restores the saved threshold. The settings at the actual
point are recorded. Tests observe callbacks and their replacement by a later
autocheckpoint setting. K2a uses the normal callback with threshold 16; its
position inside `COMMIT` is not established by the sampled delays.

## What the child writes before the kill

A kill is allowed only at a declared point after the decisive record is
acknowledged (L4). At K1 and K2b the callback emits `FaultHit` with the
point, the command, its operation and the occurrence, waits for the
surviving collector's acknowledgement, then reports `Parked` and blocks
inside the hook. K3 is the adapter's existing halted barrier after
`StoreCommitStable`, with no result yet; that action can finish and drain.
The supervisor kills at each boundary. At K1 the child holds the open transaction; at K2b the
engine has already released its write lock (the WAL hook runs after
it), and the child holds only the writer's lock of the store. At K2a
there is no point: before each command the child writes
`CommandSubmitted` and has it acknowledged; the supervisor records the
delay at which it killed and the last acknowledged command. No inspector
is open at a kill; the reference states are read before the command.

## What the oracle must conclude

| Point | Trace before the kill | Durable after restart | Resubmission |
| - | - | - | - |
| K1 | `FaultHit`, no `StoreCommitStable`, no `StoreResultReady` | the state before the command | decided anew, as a first submission |
| K2a | the last `CommandSubmitted`; maybe nothing after | the state before or after the command in flight, and after every command whose `COMMIT` returned | decided anew if absent, answered from the durable record if present |
| K2b | `FaultHit`, no `StoreCommitStable` | the state after the command | answered from the durable record, no second effect |
| K3 | `StoreCommitStable`, no `StoreResultReady` | the state after the command | answered from the durable record, no second effect |

In every row the outcome is unknown to the core at the kill: no result
reached it. The client's retransmission after recovery reaches exactly
one publication.

## Reception

- After every restart: `open` succeeds without intervention (the writer's
  lock is released by the kernel), `PRAGMA quick_check` is `ok`, and
  `accounts_match_what_is_held` holds on `try_snapshot`.
- The recovered state equals a prefix of the committed sequence. The
  reference prefixes are computed by applying the same commands to the
  deterministic store (as the differential test does); equality is on the
  whole `DurableState`, so a mix of two states fails.
- K1, K2b, K3: one scenario each, judged PASS by the oracle, `recompute`
  standing, the kill witnessed as in the first crash.
- K2a: 64 bounded calibration trials followed by one frozen campaign of
  1000 kills, alternating an admission and a release after committed
  prefixes; `wal_autocheckpoint` is 16 pages. Its normal callback is
  separately observed to checkpoint a multirow admission. The report gives
  every requested delay, seed, signal/wait, last observation and full-state
  outcome. At least 100 found and 100 absent results measure diversity;
  neither quota establishes interruption inside `COMMIT`. Trials with an
  already observed return are counted separately.
- What N does not prove: a cut that was not drawn. 1000 draws without a
  mixed state is evidence, not a proof of atomicity; the report says so.
  It is complemented, inside the store, by a WAL-cut emulation: the WAL
  of a transaction truncated at every frame boundary and inside a frame,
  then opened: each cut must recover to the state before or after. That
  emulation is a copy of files in a test, labelled as such; it is not a
  kill, and the bench's rule that nothing touches `-wal` between a kill
  and the restart is unchanged.

## Changes to the contracts (to ask of L3, smallest form)

- Two points in `fault-points.json`: `store.commit.prepared` and
  `store.commit.logged`, each `carries: command`, `actions: [barrier]`.
  A kill at a barrier is already allowed; `CrashBroker` and `DriveUntil
  FaultHit` already exist. No new record kind.
- The scenario vocabulary does not change for K1, K2b and K3. K2a is not
  a scenario: it is a campaign of its own, written by a tool, whose
  report is not an oracle verdict.

## What it supports, and what stays out

The tests support the concrete implementation of M1's atomic `Commit`
(called `ApplyCommit` in the M1a comment): deterministic boundaries at
K1, K2b and K3, sampled execution timings at K2a. A finite campaign is not
a proof for every instant and every failure. Outside its perimeter: fsync
and a power loss (the page cache survives a process kill), torn writes on
the medium, a crash of the kernel, a network file system, and another
program writing the file while ignoring the writer's lock.

## Order of delivery

1. **L7, the smallest executable step.** K1 in the store behind
   `kill-points`, and a test of the crate like `tests/writer_lock.rs`: a
   child process parks at K1 inside an admission, the test kills it, the
   reopened store equals the state before the command, `quick_check`
   and the accounts hold. No contract and no bench change.
2. **L7.** K2b the same way, and the WAL-cut emulation.
3. **L3.** The two points of the catalogue; three scenarios (K1, K2b, K3)
   and what the oracle expects of each.
4. **L4.** The child sets the callback, writes `FaultHit`, waits for the
   acknowledgement and parks; the supervisor kills at the barrier;
   campaigns played and witnessed as the first crash.
5. **L4 and L7.** The K2a sweep: the supervisor's random kills, the
   reference prefixes from the deterministic store, a report under
   `docs/reports/`.
6. **Integrator.** The open condition of M1 updated with the reports.
