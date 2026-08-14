# Single MESSAGE inline delivery — 2026-08-14

## Finding

`ApplicationDelivery.deliver_batch_inline()` already validates every condition
needed to transfer small non-persisted inbound MESSAGE effects directly to the
bounded callback and iterator queues. It nevertheless returned early for a
single effect, forcing EffectPump scheduling when a transport read contained
only one eligible QoS 0 or fresh automatic QoS 1 publication.

The two-effect minimum was a conservative boundary from the original batching
change, not an ordering, persistence or capacity requirement.

## Change

The delivery function now accepts a one-effect prefix. All existing guards stay
in force:

- stale connection epochs do not enter the delivery function;
- only effects classified with `requires_delivery_mark is False` are eligible;
- large or property-bearing messages retain exact byte accounting;
- callback and iterator queues are checked before either is mutated;
- `message_delivery="both"` remains atomic;
- full queues keep the awaited EffectPump path;
- callback code still runs only in the isolated callback worker.

Persisted/manual QoS 1, QoS 2 and replay delivery are unchanged.

## Independent measurement

The baseline was frozen from merged `main` at `44b9614`. Eleven alternating
source-isolated pairs each collected and drained one eligible MESSAGE at a time,
including callback-worker completion:

| Metric | Result |
| --- | ---: |
| Median candidate/base throughput | **6.4588** |
| Pair range | 6.2317–6.6072 |
| Baseline CV | 0.91% |
| Candidate CV | 1.92% |
| Pairs favouring candidate | 11/11 |

The scenario intentionally isolates the scheduler boundary and is not presented
as an end-to-end broker throughput forecast. Existing burst batching is the
same function with the same loop and therefore does not gain a new per-message
branch; the removed operation is only the constant one-item rejection.

## Decision

Retain the change. It removes an avoidable task wake-up from sparse inbound
traffic through an already-proven admission path without widening eligibility
or weakening bounded delivery.
