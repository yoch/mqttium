# 0012 — Refusals of a reservation, their wake-ups, and a definitive refusal at the boundary of a step

Status: **Proposed** (15 November 2026, lot L1, first wave). Written by
lot L1 after two reviews of its contract changes, recorded by the
integration owner. It amends record 0006 (shape of a core transition) and
adds to record 0009 (refusals of creation). It becomes a requirement only
when the owner of the repository accepts it; until then the requirement
register does not list it.

## Context

Record 0006 says that a refused reservation returns `NeedCapacity` and
leaves `StepOutcome` unchanged. The contracts of lot L0 answered a spent
reservation table with a wait that nothing could end, and record 0009
listed four refusals of a creation. The reviews of lot L1 showed three
kinds of refusal that the wait hid.

## Decision

### A refusal names every event that can end it, or none

A refusal names a wake-up only when a retry after it is useful, and it
names every event that can end the lack. A refusal that no event can end
is definitive and names none.

A transition makes one reservation, for all the slots its plan needs. The
box cannot tell whose reservation holds a slot: a plan reserved in several
calls whose sum exceeds the box would be answered with a wait that its own
release ends, and the retry would repeat.

`Outbox::reserve` returns `ReserveRefused`:

| Refusal | Kind | Meaning |
| - | - | - |
| `LargerThanBox { requested, capacity }` | Definitive | The request exceeds the whole capacity of the box |
| `TableExhausted` | Definitive | No entry of the reservation table can ever be taken again: each is spent, or is held at its last generation and is spent when its reservation ends |
| `Need(CapacityNeed)` | Temporary | A lack of effect slots, or else of table entries |

When several hold, the first of this order is reported: `LargerThanBox`,
`TableExhausted`, lack of effect slots, lack of table entries. A
definitive refusal is never hidden behind a temporary one.

Wake-ups:

- Lack of effect slots: `EffectAccepted` when no live reservation holds a
  slot; `EffectAcceptedOrResourceReleased` as soon as one does.
- Lack of table entries: `ResourceReleased`. A reservation ends by
  `release` or by the publication of its last slot; both release its
  entry. `Outbox::publish` returns the slots the reservation still holds,
  and zero is that event.

### At the boundary of a step

`StepOutcome` gains a variant:

```rust
/// Nothing was applied; the same input comes back. No wake-up ends the
/// refusal: the owner is renewed as stated, or it stops.
Exhausted { input: OwnedInput, exhaustion: Exhaustion, work_used: WorkUnits },

pub enum Exhaustion {
    /// The reservation table of the owner's box is exhausted.
    ReservationTable,
    /// An identity counter of the owner reached its end.
    IdentityCounter,
}
```

`Exhausted` keeps everything `NeedCapacity` keeps. It is never turned into
a `CapacityNeed`. `StepOutcome::reservation_refused` is the one place
where a refusal of the box crosses the boundary of a step.

- `ReservationTable` may be renewed by the holder of the creation
  authority: publish or release every reservation the box still holds,
  dispatch the box until it is empty, `retire` it, `create_outbox` for the
  same owner, present the input again. `retire` refuses while a
  reservation lives or an effect waits. The new box has a new serial; no
  counter of the owner is reset. If the authority refuses, or the holder
  chooses not to renew, the owner stops.
- `IdentityCounter` is not renewable within a run epoch. The owner stops;
  the next epoch is a new identity space.
- **S0 chooses to stop in both cases.** Renewal stays a stated procedure,
  covered by the contract tests of retirement and creation, and is wired
  by a later lot if a profile needs it.
- The constructor of an owner refuses a box smaller than the largest
  reservation a transition makes. `LargerThanBox` seen by a transition is
  therefore `Fatal(CoreFault::BoxTooSmall { requested, capacity })`, a
  core fault, not a wait. Like every fault it hands back neither the input
  nor `work_used`.

### Refusals of a creation (addition to record 0009)

`create_outbox(owner, capacity: NonZeroUsize, max_reservations:
NonZeroU32)`: a box of no slot or of no reservation entry cannot be asked
for. The caller converts the effect slots of its profile once, when it
creates the owner. `OwnerAuthority::from_grant(grant, max_owners:
NonZeroU32)`: an authority has at least one table entry, so `TableFull`
always has a box whose retirement lifts it. `from_grant` consumes the
grant of the opening (record 0011) and hands it back when the allocation
of its table fails.

A fifth refusal is added to the four of record 0009: `TooLarge`, for
dimensions no allocation can hold on the platform whatever memory is free.
It is definitive. `AllocationFailed` keeps the recoverable case only.
Order when several hold: `TooLarge`, `SerialsExhausted`, then
`OwnerAlreadyActive`, `TableFull`, `AllocationFailed`.

## Consequences

- `effects.rs`, `capacity.rs` (`WakeCondition::EffectAcceptedOrResourceReleased`)
  and `step.rs` change their public interface. The contract digest is
  unchanged: no file under `contracts/` is touched.
- Still owed by lot L1 with the state of an owner in `mqttiumd-core`: the
  guard of the constructor of an owner, the emission of
  `Exhaustion::IdentityCounter`, and the test that replacing a box resets
  no counter of its owner.
- The transition registry (lot L3) has no guard for an exhausted identity
  counter yet.

## Limits stated by the lot

- The failed allocation of `from_grant` is tested through the private
  function it delegates to after the conversion of `max_owners`: on a
  64-bit platform the public bound does not deterministically exceed what
  an allocator gives.
- `TooLarge` for the reservation table cannot be reached on a 64-bit
  platform and has no test.
