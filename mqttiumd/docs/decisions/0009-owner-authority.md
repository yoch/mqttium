# 0009 — Creation authority for owners and effect boxes

Status: **Accepted with amendments** (15 November 2026, third review of
PR yoch/mqttium#647). Applied by lots L1, L2, L4 and L7; see "Obligations
by lot".

Amended by 0011 and 0012 (record 0047): the opening of an epoch is one
conditional, idempotent command with an identity, and a fifth refusal of
creation is added to the four below.

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
reply that is delivered twice. Reading it authorises nothing. In the
contracts of lot L0, `RecoveredFact` is `Clone` and its variant `Epoch`
holds the incarnation and the epoch: a function that builds an authority
from those values can be called twice, whatever the authority it returns
can or cannot do afterwards.

The rule is:

> Start-up hands over at most one authority for one live opening of an
> epoch. Reading a durable fact again, a duplicated reply or a further
> recovery page does not allow a second one.

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
  Recovery opens the new epoch that DEM-012 requires before any new
  admission. It does not build, in the older epoch, an authority whose
  counter starts again at zero.

A type that is neither `Clone` nor `Copy` cannot be duplicated once it
exists. That says nothing about how many times it is constructed. An
affine permit is therefore accepted only where its issuance is itself
controlled; otherwise the precondition has only moved from one type to
another.

The entry point between the store adapter and the contract crate may be
trusted code. It is then named as such: the lot that writes it states the
one function that may construct an authority, and a test exercises the
path the product really takes to reach it, not a helper that builds an
authority directly. The type system is not credited with a guarantee it
does not give.

### Creation

`create_outbox(owner, capacity, max_reservations)` is atomic: it returns a
box and records it, or it changes nothing. A refused creation leaves
unchanged the table of active owners, the boxes that exist and the next
serial. Every check and every allocation comes before the new entry is
published. Its refusals are explicit and distinct:

| Refusal | Meaning |
| - | - |
| Owner already active | The owner holds a live box |
| Table full | The bounded table of the authority has no free entry |
| Allocation failed | Memory could not be obtained; recoverable, the caller may try again |
| Serials exhausted | The serial counter reached its end; definitive, nothing wraps, and it is never reported as a wait that something could end |

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
the owner stays active and a later creation for it is refused.

An empty box does not mean that its owner is at rest. Adapters may have
accepted effects whose replies arrive later; correlating those replies
still belongs to the life cycle of the owner, and retiring the box does
not end it.

A box created afterwards for the same owner has a new serial, so every key
of the retired box is foreign to it.

### What replacing a box does not reset

The triple (incarnation, epoch, serial) protects the reservation keys of a
box. It renews no other identity of the owner: `OperationId` and
`EffectId` have their own sequences, which do not contain the serial. A
box replaced with those sequences started again could collide with an
older operation or an older reply.

> Replacing an `Outbox` resets no counter of an owner that stays in the
> same identity space. Recreating a whole owner keeps its counters or
> establishes a new identity space explicitly.

For S0 it is enough to keep the counters of the owner when its box is
replaced, and to open a new epoch after a restart. No identifier changes
its layout for this.

### Replacement, at once (amendment, review of the second wave, R2)

```rust
fn replace_outbox(
    &mut self,
    outbox: &mut Outbox,
    capacity: NonZeroUsize,
    max_reservations: NonZeroU32,
) -> Result<(), ReplaceRefusal>
```

The box of an owner is replaced in one step. The box is checked as
`retire` checks it, the new box as `create_outbox` checks it, and the new
box is allocated, before anything changes; then the active entry of the
owner takes the new serial and the old box is dropped. The owner stays
active throughout, and no other box can be created for its identity in
between. `ReplaceRefusal` is `Retire(RetireRefusal)` or
`Create(CreateRefused)`; on any refusal, a failed allocation included,
the box and the authority are unchanged. `OwnerAlreadyActive` and
`TableFull` are never answered: the new box takes the entry of the old
one.

`retire` then `create_outbox` does not renew a box. Between the two calls
the identity is free: when the creation fails, for the size of the box or
for lack of memory, the authority accepts another box for the same owner,
and a second owner of that identity issues the identities the first one
already issued (`review_failed_renewal_must_not_reopen_the_owner_identity`,
lot L1). `retire` stays for the end of an owner; a renewal goes through
`replace_outbox`.

`Owner::renew_box` in `mqttiumd-core` uses it, so an owner never lives
without a box: a refused renewal keeps its box, its effects, its counters
and its state (`a_failed_renewal_keeps_the_box_the_state_and_the_authority`,
`a_valid_renewal_after_refused_ones_keeps_the_counters`).

## Obligations by lot

| Lot | Obligation |
| - | - |
| L1 | The bounded authority, the serial, uniqueness, atomic creation with its refusals, retirement with its conditions, and the tests named in this record |
| L2 | The contract of opening an epoch and the hand-over of the authority, with a duplicated reply and an unknown outcome among its cases |
| L4 | The actual wiring of start-up, and composition tests through the real path |
| L7 | The realisation on SQLite |

Lots L3, L5 and L6 create no owner. No lot constructs a box another way.

Receiving L1 does not wait for SQLite. It requires L1 to show its
invariant under a start-up precondition that is stated exactly, and the
condition of composition stays open until L4 verifies it.

## Alternatives

- **An owner factory in `mqttiumd-core`.** Possible in principle, and not
  different in kind: `OwnerAuthority` is itself an owned factory, with a
  bounded state and an authorisation to create. It is set aside as a
  choice of placement in this architecture: `Outbox` and its keys are
  contract types, and the authority is kept beside the type whose
  precondition it establishes.
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
