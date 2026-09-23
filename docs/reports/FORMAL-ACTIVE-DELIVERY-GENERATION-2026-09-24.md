# Formal active-delivery generation audit — 2026-09-24

## Scope

This report records an independent formal/implementation pass over the ownership
boundary formed by `EffectPump`, `DeliveryLane`, connection epochs and
`ApplicationDelivery`.

Exact baseline:

`main@5774db38b94615417f2c3c6106254429eb52b6f8`

The investigation independently reproduced the already-open issue #500. No
duplicate issue was created.

## Ownership boundary

Three delivery states must not be conflated:

1. **pending lane lot** — still stored in `DeliveryLane` and carrying an epoch;
2. **active but uncommitted admission** — the reader has started applying the
   lot, but iterator capacity has not yet admitted the message;
3. **committed application message** — already in `messages_queue`, after
   which the documented stream-generation/reconnect contract applies.

The pre-existing epoch protection covered state 1. The defect is state 2.

Once `ApplicationDelivery.accept()` returned a slow-path coroutine,
`_accept_waiting_unaccounted()` / `_accept_waiting_accounted()` waited only
for capacity. They did not retain the connection/stream generation that owned
the handoff. If either generation changed while they were suspended, the old
coroutine could wake and commit into the replacement generation.

## Deterministic concrete reproductions

Two independent boundaries reproduce the same ownership bug.

### Stream-generation replacement

1. iterator capacity is full;
2. an old message becomes an active slow admission;
3. `close(); reset_stream()` replaces the iterator queue/generation;
4. the old waiter wakes;
5. rc15 inserts the old message into the replacement queue.

### Connection-epoch replacement

1. iterator capacity is full;
2. an old-epoch message becomes an active slow admission;
3. connection invalidation increments `_connection_epoch`;
4. invalidation is deliberately suspended in `WritePump.advance_epoch()`;
5. capacity becomes available before reader cancellation;
6. rc15 commits the old message after the epoch change.

The second reproduction can be composed with public `disconnect()`; the
focused regression in this PR isolates the exact invalidation boundary.

Both byte-accounted and count-only iterator modes are covered.

## Candidate

The candidate adds one private `ApplicationDelivery` counter:

`_admission_generation`

Only slow, not-yet-committed admissions capture it.

The counter is invalidated when:

- the Network Connection epoch changes; or
- the application stream generation is reset.

A suspended admission re-checks its captured generation before committing. A
stale waiter returns without enqueueing.

This is deliberately narrower than cancelling/dropping the reader or clearing
the iterator queue. Messages already committed to `messages_queue` preserve
the existing automatic-reconnect stream contract.

The synchronous fast path is unchanged except for passing the current integer
generation when it must create the slow coroutine.

## Formal model

`formal/mqtt/ActiveDeliveryGeneration.tla` models:

- connection epoch;
- stream generation;
- admission generation;
- active slow waiter and its captured generation;
- application capacity;
- commit vs stale-drop.

Safety invariant:

`an admission invalidated before commit must never commit`

The original broader executable ownership model also included EffectPump
protocol accounting and DeliveryLane fences.

Independent bounded exploration of that broader model found the rc15 witness
after 1,816 states / 5,522 transitions. The guarded candidate was explored
without a violation through:

- depth 8: 38,656 states / 172,733 transitions;
- depth 9: 88,315 states / 425,216 transitions;
- depth 10: 189,124 states / 971,465 transitions.

The shortest relevant witness is the same ownership shape as #500:

`start_wait -> replace_generation -> capacity_available -> resume_wait`

The TLA+ specification is included as an auditable artifact. TLC was not run in
the original environment because the binary TLA+ tools could not be
materialized there; do not interpret the file alone as a TLC proof.

## Regression coverage

`tests/unit/test_active_delivery_generation.py` covers:

- stream-generation replacement;
- connection-epoch replacement;
- count-only iterator mode;
- byte-accounted iterator mode;
- waiter and DeliveryLane active-count cleanup.

The tests are deterministic: the connection-epoch test holds the writer
condition so invalidation suspends in the exact window under review.

## Remaining qualification

Before merge, require the ordinary repository gates, with particular attention
to:

- existing EffectPump atomicity/failure-routing tests;
- reconnect stale-effect tests;
- terminal writer epoch tests;
- delivery/protocol separation and iterator accounting tests;
- full fuzz/resilience/soak;
- performance qualification confirming no measurable steady-state cost on the
  immediate iterator fast path.

No public API is changed.
