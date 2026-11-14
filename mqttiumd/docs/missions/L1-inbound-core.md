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

1. **Creation authority** of record 0009, with its tests. First, as a
   contract change.
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
  unchanged, or the mission states exactly which provisional metadata may
  differ and why (P1-19, MU-25).
- The five tests of the creation authority listed in record 0009.
- A test for a reservation table whose entries are all spent.
- A plan cannot be applied twice, to another owner, or after the state
  moved.
- One named test per registry rule of the two machines, by rule identifier.
- `release_if_current` of L6 is the only code that releases a reservation.
- Control mutations seen to fail: acknowledgement before commit (MU-01),
  recipe changed under an operation (MU-03), mutation despite insufficient
  capacity (MU-25), second admission behind a new exchange (DEM-MU-04),
  late unknown result replacing a terminal one (DEM-MU-05).
- The boundary test passes unchanged, or with a list extended by decision.

## Out of scope

The store, the scheduler, traces, bytes on the wire, outbound delivery,
expiry, Will, retained messages, wildcards, shared subscriptions.

## Closes

In `../reception/open-conditions.md`: the three conditions of "Effect boxes
and transitions".
