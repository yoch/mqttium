# 0009 — Creation authority for owners and effect boxes

Status: **Proposed** (15 November 2026). To be decided before lot L1 starts:
several lots build owners, and they must share one authority.

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

## Decision proposed

A construction token, held by the one component that creates owners.

- `mqttiumd-contract` gains `OwnerAuthority`. It is neither `Clone` nor
  `Copy`. It is built from the durable fact that a run epoch was opened
  (store incarnation and run epoch), which only the store adapter produces.
  The store opens an epoch once, so there is one authority per epoch.
- `Outbox::with_capacity` stops being public. A box is created by
  `authority.create_outbox(owner, capacity, max_reservations)`.
- The identity of a box becomes (incarnation, epoch, serial). The serial
  comes from a checked counter of the authority and is never reused. The
  authority also keeps, in a bounded table, which owners hold a live box.
- A second creation for an owner that holds a live box is refused.
- A box is given back with `authority.retire(outbox)`, which consumes it. A
  box created afterwards for the same owner has a new serial, so every key
  of the retired box is foreign to it.
- When the serial counter is exhausted, creation is refused with an
  explicit exhaustion; nothing wraps.

Who holds the authority: in the product, the start-up code that runs after
the epoch is durably open; in the bench, the scheduler. Lot L1 delivers the
authority and its tests. Lots L2, L3, L5 and L6 create no owner. Lot L4
uses the authority and never constructs a box another way.

## Alternatives

- **An owner factory in `mqttiumd-core`.** The constructor of `Outbox`
  lives in another crate and cannot be made private to the factory; the
  precondition would stay a convention.
- **An arena.** Owners live in the arena and keys are arena handles. It
  gives the same guarantee and changes how every owner is addressed; it is
  a larger redesign than S0 needs.

## What this does not settle

Two authorities built for the same incarnation and epoch are two contexts
that this type cannot tell apart. In the product the store prevents it,
because opening an epoch is a durable, exclusive act. In the bench, two
simulated stores are kept apart by giving them distinct incarnations; the
test for separated contexts states that rule.

## Consequences

`effects.rs` changes its public interface: this is a contract change, made
first in the wave so that no other lot builds on `Outbox::with_capacity`.
The test `two_boxes_with_one_identity_are_not_told_apart` is replaced by
the tests listed above. The open condition "Creation authority for effect
boxes" is closed by lot L1 with this record.
