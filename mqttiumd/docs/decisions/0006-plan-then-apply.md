# 0006 — Shape of a core transition: a single-use plan, then its application

Status: **Accepted with amendments** (14 November 2026, review of PR yoch/mqttium#647).
Applied in lot L0 for the contract types (`PlanStamp`,
`EffectReservation`); by lot L1 for the transitions and L6 for the pilot
proof. Completes DEM-006.

Amended by 0012 (record 0047): a step can end in `StepOutcome::Exhausted`;
the shape of a transition stated below is otherwise unchanged.

## Context

DEM-006 requires that an input is applied or handed back intact, and lists
five phases. It does not say how to obtain this with a `&mut OwnerState`: a
transition that discovers a missing slot after its first mutation cannot
return `NeedCapacity` honestly.

## Decision

Every transition has two functions.

```rust
fn plan(state: &OwnerState, input: &Input, grant: TurnGrant) -> Result<Plan, Refusal>;
fn apply(state: &mut OwnerState, plan: Plan, outbox: &mut Outbox) -> Result<(), CoreFault>;
```

- `plan` reads only. It computes the next state, the effects and every
  resource needed, and returns `NeedCapacity` or a fault otherwise.
- **A plan is used once.** It is not copyable and `apply` consumes it.
- **A plan names what it was computed against.** It carries a `PlanStamp`:
  the owner and the revision of the owner's state. `apply` checks the stamp
  first and refuses a plan for another owner or for a state that moved
  (`CoreFault::StalePlan`). Within one owner turn nothing runs between
  `plan` and `apply`; the stamp is the guard of that rule, not a licence to
  interleave.
- **Resources are bound and bounded.** The caller reserves what the plan
  needs before `apply`. An effect reservation is a key that names its box,
  its table entry and its generation; the box checks it before any
  mutation, and the table is bounded. A refused reservation releases the
  provisional ones and returns `NeedCapacity` with the untouched input.
- `apply` does not allocate fallibly and fails only on a broken stamp or
  reservation, which are core faults.
- A preparation that keeps a cursor is an applied transition with a counted
  continuation, never a `NeedCapacity`.

## Pilot proof

DEM-021 is unchanged: the pilot proof is on `release_if_current`, a
function the product really executes. Proving the plan-and-apply
scaffolding does not replace it.

## Consequences

MU-25 becomes a mutation of a visible boundary: mutate in `plan`.
`StepOutcome` in `mqttiumd-contract` is unchanged.
