# L2 — Deterministic store

Depends on: L0. Runs with: L1, L3, L5, L6. Followed by: L4, and L7 for the
contract it stabilises.

## Goal

A deterministic store for the bench that realises the store contract of
S0, and a suite of contract tests that the SQLite store of lot L7 will
have to pass unchanged.

## Requirements

DEM-009, DEM-010, DEM-011 for the logical frontiers, DEM-012, DEM-013,
DEM-018; ARC-008, ARC-009, ARC-019; V3-006; IO-001 to IO-004. Decision
records 0002, 0003, 0009.

## Deliverables

The first deliverable is item 2, the contract of opening an epoch: lot L1
issues its creation authority from it.

1. **The store**, in `crates/mqttiumd-lab/src/store/`. It keeps apart the
   durable state, a transaction in preparation, a command in transit and a
   result that is ready.
2. **Opening an epoch** (record 0009), as a contract: its durable states,
   its command and its results. The opening is committed once. A reply
   delivered twice, a replayed fact or a further recovery page describes
   the same opening and nothing more; an unknown outcome is a result of
   its own. The hand-over that lets the holder of the committed result
   obtain the one authority of that opening is defined here with L1, with
   the state that controls it.
3. **Commands**: `AdmitPublication` commits publication, payload,
   obligations, inbound slot, accounts and receipt together or nothing;
   `ReleaseInbound`; closing the older epochs.
4. **The four outcomes**, and `ResolveAndFence` ordered with the
   application of commands: a committed operation returns its result; an
   absent one in an open space is fenced durably before a certain rejection
   is announced; without the fence the outcome stays unknown.
5. **Recipes** kept in their exact canonical bytes and compared byte for
   byte when an operation is presented again.
6. **Recovery** in bounded pages from durable state only.
7. **Injection points** of `contracts/v1/fault-points.json`, each one
   recording that it was reached.
8. **Fixture set-up** through valid commands, never by writing state.
9. **Result correlation** (record 0003): a result that answers no effect in
   flight is refused and recorded.
10. **Contract tests** written against an interface that the SQLite store
   can implement.

## Reception

- QABS-A, QABS-B and QABS-C each take a different order of fence and
  command, and each gives the outcome DEM-018 requires. In QABS-B the held
  command is really delivered after the fence and is seen to be refused
  with no effect.
- A rejection stays a rejection after quota is released (P1-11, MU-08).
- A guard that fails leaves no admitted subset (ADM-002, MU-24 on this
  path).
- The same operation with another recipe is a violation with no second
  effect (MU-03).
- A late unknown answer never lowers a terminal result (DEM-MU-05); two
  contradictory terminal results are a fault.
- Deleting the producing session deletes no obligation of another session
  (MU-07).
- Control mutations seen to fail: an observed absence turned into a
  rejection without a fence (DEM-MU-01), an unknown outcome treated as a
  rejection (MU-02).
- Every counter is checked; none wraps.
- Opening an epoch: a duplicated reply gives no second authority; an
  unknown outcome gives no usable one; a replayed fact and a further
  recovery page give none; after the volatile state is lost, recovery
  opens a new epoch and the older one is not entered again (DEM-012).

## Out of scope

SQLite, a real crash of a process, the scheduler and the scenario runner
(L4), the oracle (L3).

## Asks of other lots

From L3: the store snapshot format and the trace records it must emit.
With L1: routes and capacities among the recovered facts, and the
interface of the creation authority.

## Closes

In `../reception/open-conditions.md`: its part of "Creation authority".
