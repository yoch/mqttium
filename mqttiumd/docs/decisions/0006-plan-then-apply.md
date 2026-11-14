# 0006 — Shape of a core transition: a pure plan, then an infallible application

Status: **Proposed**. Completes DEM-006. Binds lot L1 and the pilot proof L6.

## Context

DEM-006 requires that an input is applied or handed back intact, and lists
five phases. It does not say how to obtain this with a `&mut OwnerState`:
a transition that discovers a missing slot after its first mutation cannot
return `NeedCapacity` honestly.

## Decision

Every transition has two functions.

```rust
fn plan(state: &OwnerState, input: &Input, grant: TurnGrant) -> Result<Plan, Refusal>;
fn apply(state: &mut OwnerState, plan: Plan, outbox: &mut Outbox, slots: EffectReservation);
```

- `plan` reads only. It computes the next state, the effects and every
  resource needed. It returns `Refusal::NeedCapacity` or a fault.
- The caller reserves what the plan needs, including the effect slots
  (`Outbox::reserve`). A refused reservation releases the provisional ones
  and returns `NeedCapacity` with the untouched input.
- `apply` cannot fail and does not allocate fallibly. It publishes into the
  reserved slots.
- A preparation that keeps a cursor is an applied transition with a counted
  continuation, never a `NeedCapacity`.

## Consequences

MU-25 becomes a mutation of a visible boundary: mutate in `plan`. The Kani
harness of lot L6 targets functions of this shape. `StepOutcome` in
`mqttiumd-contract` is unchanged. If the proposal is refused, lot L1 must
state another mechanism that makes "handed back intact" checkable.
