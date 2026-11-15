# L1 — Inbound core

Depends on: L0. Runs with: L2, L3, L5, L6. Followed by: L4.

## Goal

The synchronous core of the S0 perimeter: a persistent session resumed,
one inbound QoS 2 exchange admitted and terminated, with every transition
applied entirely or handed back intact.

## Requirements

DEM-004 to DEM-009, DEM-012; ARC-004, ARC-009, ARC-011; ADM-001 to ADM-006;
ACK-001; ID-001 to ID-004; IO-001 to IO-004. Decision records 0001, 0006,
0007, 0009.

## Deliverables

1. **Creation authority** of record 0009: the bounded authority, the
   serial, uniqueness, atomic creation with its four refusals, retirement
   with its conditions. Its interface comes first, as a contract change,
   so that L2 and L4 build on it; then creation, retirement and their
   tests. Issuing the authority from an opened epoch is defined with L2.
2. **Transitions in the shape of record 0006.** A private plan, consumed
   once; owner and state revision checked before application; resources
   reserved before any mutation; an application that cannot fail for lack
   of room.
3. **Inbound QoS 2 machine**: `Absent`, `AdmissionPending`, `AwaitPubrel`,
   `ReleasePending`, as the registry rules written by L3 state them.
   - A new PUBLISH prepares the admission: identities, exact-topic routing
     from recovered routes, a frozen recipe, `Persist`.
   - PUBREC becomes eligible only after the core knows of the commit.
   - A replayed PUBLISH creates no second admission.
   - PUBREL requests the durable termination; PUBCOMP follows its commit.
4. **Unknown outcome on a live process** (record 0001): the deadline armed
   after `Persist`, `ResolveAndFence`, the guards on token, operation and
   exchange generation, a late unknown result ignored, contradictory
   terminal results as a fault.
5. **Recovery** from bounded pages: sessions, inbound slots, operations of
   an older epoch. Every identity comes from those pages.
6. **Session resume** for a session prepared by the fixture: CONNECT, then
   CONNACK with Session Present.
7. **Exhaustion**: a counter or a reservation table that is definitively
   exhausted is told apart from a temporary lack, and the choice between
   stopping and renewing is explicit.

## Reception

- For every transition, a test at each boundary of capacity (effect slots,
  reservation table, work limit): the same input comes back; state,
  identity counters, accounts, held reservations and the effect box are
  unchanged (P1-19, MU-25). An explanation in a report does not relax
  this: if a piece of metadata may change despite the refusal, that needs
  an explicit amendment of the contract, by a decision record that gives
  the equivalence relation it retains and its consequences.
- The tests of the creation authority named in record 0009: the five of
  its context, the refusals of creation at the last serial and after it,
  no change after a refused creation, no serial used again after
  retirement, each refusal of retirement giving the same box back, a box
  dropped without retirement leaving its owner active.
- Replacing the box of an owner resets none of its counters: the
  sequences of `OperationId` and `EffectId` continue.
- A test for a reservation table whose entries are all spent.
- A plan cannot be applied twice, to another owner, or after the state
  moved.
- One named test per registry rule of the two machines, by rule identifier.
- The credit reservations that `release_if_current` of L6 covers are
  released by that function and by no second variant (DEM-024). The
  reservation entries of a box and the operations of the store have their
  own mechanisms.
- Control mutations seen to fail: acknowledgement before commit (MU-01),
  recipe changed under an operation (MU-03), mutation despite insufficient
  capacity (MU-25), second admission behind a new exchange (DEM-MU-04),
  late unknown result replacing a terminal one (DEM-MU-05).
- The boundary test passes unchanged, or with a list extended by decision.

## Out of scope

The store, the scheduler, traces, bytes on the wire, outbound delivery,
expiry, Will, retained messages, wildcards, shared subscriptions.

## Closes

In `../reception/open-conditions.md`: its part of "Creation authority",
and the two other conditions of "Effect boxes and transitions". The parts
of the authority that belong to L2, L4 and L7 stay open: this lot is
received on its invariant, shown under a start-up precondition that it
states exactly, without waiting for SQLite.
