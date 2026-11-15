# 0009 — Creation authority for owners and effect boxes

Status: **Accepted with amendments** (15 November 2026, third review of
PR yoch/mqttium#647). Applied by lots L1, L2, L4 and L7; see "Obligations
by lot".

## Context

An `Outbox` is a primitive with a precondition: one box per owner and run
epoch. Its identity is supplied by whoever creates it, so two boxes created
with the same identity accept each other's reservation keys
(`two_boxes_with_one_identity_are_not_told_apart`). The second review of
lot L0 accepts that precondition for the primitive and requires lot L1 to
make it an invariant that is established and tested, with:

- two creations for one owner in one incarnation;
- destruction then recreation, with an old key kept;
- no silent reuse of an identity;
- contexts that could exchange keys by accident kept apart;
- explicit exhaustion of the identity domain.

Three shapes were named: an owner factory, an arena, a construction token
that cannot be duplicated.

## Decision

A construction token, `OwnerAuthority`, held by the one component that
creates owners.

- `mqttiumd-contract` gains `OwnerAuthority`. It is neither `Clone` nor
  `Copy`.
- `Outbox::with_capacity` stops being public. A box is created by the
  authority and by nothing else.
- The identity of a box is (incarnation, epoch, serial). The serial comes
  from a checked counter of the authority and is never reused.
- The authority keeps, in a bounded table, which owner holds which live
  box, serial included.

### A durable fact is not an authorisation to construct

That a run epoch was opened is a durable fact. A fact can be read any
number of times: in a recovery page, in a reply that is replayed, in a
reply that is delivered twice. Reading it authorises nothing.

- There is at most one authority per live opening of an epoch. It is
  issued once, to the component that submitted the opening and received
  its committed result in this process.
- A replayed fact, a duplicated reply to the opening, or a further
  recovery page gives no second authority. Issuance is a transition of a
  controlling state that can be taken once.
- An opening whose outcome is unknown gives no usable authority. Start-up
  does not proceed on an opening it assumes.
- The hand-over from the code that opens the epoch to the component that
  holds the authority is an explicit procedure, with a state that controls
  it: the authority is moved, never lent to two creators.
- When the volatile start-up state is lost, the authority is lost with it.
  Recovery opens a new epoch (DEM-012); the older epoch is never entered
  again, so nothing can ask for its authority a second time.

A type that is neither `Clone` nor `Copy` cannot be duplicated once it
exists. That says nothing about how many times it is constructed. An
affine permit is therefore accepted only where its issuance is controlled:
the record of a lot states the trusted entry point, the one function that
may construct an authority, and a test exercises the real path that
reaches it, not a helper that builds an authority directly.

### Creation

`create_outbox(owner, capacity, max_reservations)` is atomic: it returns a
box and records it, or it changes nothing. Its refusals are explicit and
distinct:

| Refusal | Meaning |
| - | - |
| Owner already active | The owner holds a live box |
| Table full | The bounded table of the authority has no free entry |
| Allocation failed | Memory could not be obtained; recoverable, the caller may try again |
| Serials exhausted | The serial counter reached its end; definitive, nothing wraps |

Tests: the last usable serial creates a box; the next creation is refused
as exhausted; a refused creation leaves the table, the counter and the
caller's arguments unchanged; a serial is not used again after its box was
retired.

### Retirement

```rust
fn retire(&mut self, outbox: Outbox) -> Result<(), RetireRefused>
```

`RetireRefused` owns the box that was refused and gives the reason. A box
is retired only when all of these hold:

- it was created by this authority;
- it is the exact active entry of its owner, serial included;
- it holds no pending effect;
- it holds no live reservation.

On a refusal the same box comes back and the authority is unchanged.

Dropping a box without retiring it does not free the entry of its owner:
the owner stays active and a later creation for it is refused. An empty
box does not mean that its owner is at rest; retirement states it.

A box created afterwards for the same owner has a new serial, so every key
of the retired box is foreign to it.

### What replacing a box does not reset

Replacing the `Outbox` of an owner resets no counter of that owner. The
sequences behind `OperationId` and `EffectId` continue where they were.

## Obligations by lot

| Lot | Obligation |
| - | - |
| L1 | The bounded authority, the serial, uniqueness, atomic creation with its refusals, retirement with its conditions, and the tests named in this record |
| L2 | The contract of opening an epoch and the hand-over of the authority, with a duplicated reply and an unknown outcome among its cases |
| L4 | The actual wiring of start-up, and composition tests through the real path |
| L7 | The realisation on SQLite |

Lots L3, L5 and L6 create no owner. No lot constructs a box another way.

## Alternatives

- **An owner factory in `mqttiumd-core`.** It would give the same
  guarantee with the constructor and its guard in one crate. It is set
  aside as a choice of placement: `Outbox` and its keys are contract
  types, and the authority is kept beside the type whose precondition it
  establishes.
- **An arena.** Owners live in the arena and keys are arena handles. It
  gives the same guarantee and changes how every owner is addressed; it is
  a larger redesign than S0 needs.

## What this does not settle

Two authorities built for the same incarnation and epoch are two contexts
that this type cannot tell apart. The rules above on issuance are what
prevents it in the product; they are obligations of L2 and L4, not
properties of the type. In the bench, two simulated stores are kept apart
by giving them distinct incarnations; the test for separated contexts
states that rule.

## Consequences

`effects.rs` changes its public interface: this is a contract change, made
first in the wave so that no other lot builds on `Outbox::with_capacity`.
The test `two_boxes_with_one_identity_are_not_told_apart` is replaced by
the tests listed above. The open condition "Creation authority for effect
boxes" is split by lot in `../reception/open-conditions.md`; no part of it
is closed by another lot's part.
