# 0064 — The owner answers for its box

Status: **Proposed** (24 November 2026). Applied to `mqttiumd-core`,
`mqttiumd` and `mqttiumd-lab` by lot 7 of `refactor/deslop-layers`
([report 0048](../reports/0048-deslop-lot-7-the-owner-answers-for-its-box.md)).
No input, effect, drop reason, event, durable encoding or byte on the wire
changes.

## Context

`Owner::outbox()` lent the owner's `Outbox` as `Option<&Outbox>`. Since
the correction of finding R2 an owner always holds its box
(`Owner::renew_box` replaces it in place). The `Option` was kept so that
earlier callers went on compiling, and
[`open-conditions.md`](../reception/open-conditions.md) recorded that the
signature no longer said what holds.

A lent box also hands its callers the whole read surface of the primitive:
its identity, its capacity, its live reservations. Its callers ask four
things: whether an effect waits, how many, how many slots are free, and
whether a given effect still waits. `deslop-please` reports the accessor
as a field leak.

## Decision

`Owner::outbox()` is removed. The owner answers:

| Question | Asked by |
| - | - |
| `has_pending_effects` | the runtime before a dispatch and at shutdown; the bench, to know whether it is busy |
| `pending_effect_count` | the runtime's drain loop, to decide when to dispatch |
| `free_effect_slots` | the same decision |
| `holds_effect(id)` | the runtime, to know whether a write it holds back still waits in the box |

The owner also lends one read-only view, `pending_effects`: the effects it
will hand out, in order. The bench reads their identities from it; the
tests of the runtime read their content. An `OwnedEffect` is moved, never
copied, so the view lends rather than copies, as the delivery table lends
its rows ([report 0046](../reports/0046-performance-gate-tools-and-decisions.md)).

No caller outside the owner receives the `Outbox`. Creation, renewal,
reservation, publication and dispatch stay with the owner and with the
authority of [record 0009](0009-owner-authority.md).

## Consequences

- The finding of `open-conditions.md` on `Owner::outbox()` is closed.
- `holds_effect` scans the box, as the runtime did; its cost is unchanged.
- The linter does not report `pending_effects`, whose body is a call. The
  header of `.deslop-please.allow` names it as kept read lending.
- `tools/bounded-batch.mutant` names `.outbox()`. Its anchors were already
  broken before lot 7 (report 0042), and it is left as it is.
