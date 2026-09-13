# Uniform callback delivery — QoS0 follow-up and stopping rule

Date: 2026-09-13.

This note records the final follow-up on PR #458 after the RC14 qualification. It does **not** change the retained runtime. The retained measured runtime remains `fb619a27b866c5db869508e6a226db0080e668c7` on RC14 main `c194597bcf5af4951fbec2b560600eef3cb84b3c`.

## Why this follow-up was needed

The retained scheduler is materially simpler than RC14 main and improves event-loop fairness under larger synchronous bursts, but the dedicated direct-QoS0 screen found one localized cost for synchronous singleton delivery:

- throughput about **-5.9%**;
- CPU/completion about **+7.0%**;
- low-window throughput about **-6.0%**;
- first-callback latency about **+10%**;
- loop-lag p95 about **-13%**.

The question was whether this singleton cost could be recovered **without** restoring callback batching, pair-inline scheduling, hidden reservations, private `asyncio.Queue` mutation, or a new burst/timing state machine.

## Benchmark correction

The first post-lock QoS0 A/B attempt (`34592683137`) did not expose a runtime failure. The benchmark probe intentionally rejects dirty Git sources, while that workflow had applied the candidate patch to `slot-b` without committing it. A/A used clean baseline trees and therefore completed; the first candidate process exited at the source-identity check.

The experiment was rerun after materializing the exact patched source as a local immutable commit before measurement. No result from the failed dirty-tree attempt is used as product evidence.

## Broad post-lock ablation — rejected

The broad ablation changed the generic direct-message delivery helper so an idle synchronous singleton could execute inline. Exact materialized candidate: `1d66e574a9f10c94b45b7a4dc60c805cc06a9c11`; Actions run `34777598305`.

It recovered much of the QoS0 singleton cost, but widened inline semantics beyond the actual reader singleton seam. Six existing tests failed exactly because they encoded the former direct-singleton worker ownership; the other 2323 tests passed with 17 skips.

Paired A/B effects versus RC14 main, six 3-second pairs per cell:

| QoS0 sync batch | Throughput | p05 throughput | CPU/completion | First p50 | Loop-lag p95 |
|---:|---:|---:|---:|---:|---:|
| 1 | -1.26% | -1.39% | +1.84% | +2.08% | +4.49% |
| 2 | -2.46% | -2.36% | +2.51% | +1.96% | +5.04% |
| 8 | -1.52% | -1.93% | +1.49% | +2.15% | +3.39% |
| 32 | -1.66% | -1.56% | +1.68% | +1.46% | +13.92% |

This is the wrong trade: the singleton improves, but the generic policy change gives back the loop-lag advantage that motivated the simpler worker. It is **not promoted**.

## Narrow reader-only ablation — functionally clean, still rejected

A second ablation kept `deliver_callback_messages_inline()` unchanged. It only detected a single eligible direct-decoded QoS0 message under the engine lock, then invoked that one synchronous callback immediately after releasing the lock, with no `await` in between. Multi-message and async direct delivery remained worker-owned.

Exact materialized candidate: `b41d7d137fbbac0e4fbb5e67a4524ba89f8e5e2e`; Actions run `34777729652`.

This variant is much cleaner functionally:

- targeted reader tests pass;
- the **entire existing test suite stays green without a whitelist**;
- there is no new queue, task, timer, threshold or lifecycle state;
- the generic callback admission contract is unchanged.

It almost completely removes the batch-1 hotspot:

| QoS0 sync batch | Retained runtime throughput | Narrow throughput | Retained loop-lag p95 | Narrow loop-lag p95 |
|---:|---:|---:|---:|---:|
| 1 | -5.91% | **+0.00%** | -13.16% | -0.98% |
| 2 | -2.61% | **+0.69%** | -3.37% | -0.24% |
| 8 | **-0.12%** | -0.83% | **-1.43%** | +3.48% |
| 32 | -1.94% | -1.95% | **-1.84%** | +4.62% |

For batch1 the narrow variant is essentially neutral against main: CPU/completion is about -0.08%, p05 throughput about -0.88%, and loop-lag about -0.98%. That is a successful local optimization.

However, the larger-burst result explains why it should still **not** be promoted. A network burst is not necessarily delivered to the decoder as one large batch. TCP/transport chunking can expose individual PUBLISH packets in successive reads. A stateless rule that says "one captured message => singleton" therefore opportunistically inlines fragments of a larger burst. The result is visible in batch8/32: the retained scheduler's loop-lag improvement disappears and becomes a regression.

## Why no third heuristic is added

There is no simple source-local fact that distinguishes a genuine isolated QoS0 message from one packet of a fragmented burst.

Possible discriminators were considered and rejected:

- a time-since-last-message threshold;
- a burst/streak counter;
- a dedicated hysteresis bit for message callbacks;
- peeking into transport/socket buffering;
- using the shared callback-worker task as a hysteresis signal.

The first four add timing/transport state to a scheduler whose purpose is simplification. The last one couples message performance to unrelated lifecycle/on_publish callbacks because the worker is shared. All would make behavior harder to predict and test for a gain confined to one QoS0 singleton workload.

This is the **no-overengineering boundary** for the experiment.

## Retained trade-off

Keep the current strict sole-pending MESSAGE-effect fast path and ordinary worker for direct QoS0 delivery.

The retained model has a coherent property:

- serial QoS1 can avoid the queue hop after protocol effects have been ordered and the engine lock released;
- direct QoS0 remains worker-owned, so fragmented bursts do not oscillate between reader and worker ownership;
- larger synchronous bursts keep the event-loop fairness benefit measured in the RC14 stability campaign;
- no extra burst detector or scheduler state exists.

The QoS0 singleton ~6% throughput cost is therefore an explicit, localized price for the simpler and steadier burst model. It is preferable to recovering that number with an ownership heuristic that measurably worsens burst fairness.

Likewise, the fixed-rate QoS1 p95/p99 cost should not be chased by enlarging worker rounds or suppressing the end-of-round yield without new evidence: that yield is the mechanism responsible for the measured loop-lag improvements under synchronous bursts.

## Recommendation

No further scheduler heuristic should be added to PR #458.

The next useful work, if any, should be outside the ownership model itself: ordinary implementation cleanup, documentation, or broader workload validation. A future QoS0 optimization should be accepted only if it recovers singleton performance **without** materially reducing the retained batch8/32 fairness and without adding a new timing/burst state machine.
