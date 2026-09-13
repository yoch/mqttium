# Uniform callback delivery — final RC14 qualification

## Decision

The retained runtime is `fb619a27b866c5db869508e6a226db0080e668c7`, composed on RC14 main `c194597bcf5af4951fbec2b560600eef3cb84b3c`. Later branch commits are documentation and regression-test follow-up only; the scheduler/runtime bytes remain the measured candidate.

The selected architecture is **strict sole-pending synchronous inline + one ordinary callback worker for everything else**:

- a sole pending eligible callback-only `MESSAGE` may execute an idle declared-sync callback inline;
- multi-message bursts, async callbacks, iterator/both delivery, persisted delivery, reentrant work and routed async work remain worker/slow-path owned;
- direct-decoded QoS0 delivery remains worker-owned;
- no physical callback batches;
- no mutation of asyncio queue private `_maxsize` / `_putters` state;
- no pair-inline special case;
- no started-tail handoff between reader/effect ownership and worker ownership;
- one ordinary queue entry represents one queued message notification.

This is the final scheduler policy for PR #458. The measured QoS0 singleton and QoS1 tail-latency costs are accepted as explicit trade-offs rather than hidden behind additional ownership heuristics.

## Structural result vs RC14 main

Against `main@c194597bcf5af4951fbec2b560600eef3cb84b3c`:

- `src/mqttium/api/_delivery.py`: **-25 net lines**;
- `src/mqttium/api/async_client.py`: **-46 net lines**;
- runtime total: **-71 net lines**.

The more important simplification is qualitative: callback-batch reservation, private queue-capacity mutation, pair-inline scheduling and route-tail handoff disappear from the runtime state model.

## Correctness / lifecycle qualification

The composed RC14 candidate passed the normal CI matrix, cross-platform jobs, fuzz, resilience, distribution smoke and soak. Dedicated tests cover normal/eager task factories, cancellation, worker replacement, generation reset, reconnect/reopen, live routing changes, iterator wake-up semantics and the strict singleton fast path.

The explicit stream-generation regression verifies that an old `anext()` waiter terminates on reset without requiring a fresh message, while the replacement generation still accepts fresh delivery. A post-measurement regression test also locks the direct-QoS0 ownership boundary: successive singleton captures from a transport-fragmented burst remain worker-owned.

Codecov reports patch coverage around **88.7%** and project coverage roughly **0.1 percentage point lower** than main. The missing lines are not concentrated in the strict singleton fast path; lifecycle invariants found during the adverse audit have dedicated regression tests.

## Raspberry Pi RC14 fixed-rate RTT qualification

Final exact-source run: Actions run `34589940179` on self-hosted `rpi5`.

Comparison:

- A: `main@c194597bcf5af4951fbec2b560600eef3cb84b3c`
- B: `fb619a27b866c5db869508e6a226db0080e668c7`
- MQTT 3.1.1 / QoS1 / `application_rtt_fixed_rate`
- frozen external pacer at 3942 msg/s
- standard profile, A/A control then four balanced A/B blocks

Observed A/B result:

- p50: approximately **-0.16%**;
- paired p50 interval: approximately **[-0.67%, +0.36%]**;
- initiator CPU per completed RTT: approximately **+0.22%**;
- completed rate: unchanged at the imposed target;
- p95: approximately **+4.9%**;
- p99: approximately **+6.6%**.

The A/A control itself moved roughly +0.74% at p50, so central latency is inside runner noise. The p95/p99 increase is nevertheless directional across the A/B blocks and is retained as a real tail-latency cost rather than hidden behind the neutral p50.

## Hosted QoS1 stability screening

Final RC14 stability run: `34590246335`, six balanced 5-second pairs per cell, with raw 100 ms and 1 s windows retained.

The result is a trade-off, not a universal win:

- sync batch1: throughput/p05 essentially neutral; loop-lag p95 about **-0.4%**;
- sync batch2: throughput/p05 about **+0.9%**; loop-lag p95 about **-3.3%**;
- sync batch8: throughput/p05 about **-1.5%**; loop-lag p95 about **-14.5%**;
- sync batch32: throughput/p05 about **-0.9%**; loop-lag p95 about **-12.6%**;
- filtered sync batch8: throughput/p05 about **-3.4%**; loop-lag p95 about **-14.4%**;
- async batch8: throughput about **+0.8%**;
- iterator/publish controls: approximately neutral.

Interpretation: the simplified worker materially shortens long event-loop occupations under larger synchronous bursts, at the cost of a few percent of peak closed-loop capacity on some sync/filtered cells. This is a fairness / maintainability trade-off, not a blanket performance improvement.

## QoS0 follow-up

A dedicated hosted QoS0 screen on the retained runtime found a localized regression for direct-decode synchronous singleton callback delivery:

- batch1 throughput about **-5.9%**;
- CPU per completion about **+7%**;
- low-window throughput about **-6%**;
- first-callback latency about **+10%**;
- loop-lag p95 about **-13%**.

The first post-lock A/B attempt (`34592683137`) was invalid because the benchmark identity check correctly rejected a dirty candidate tree; it was a workflow problem, not a runtime failure.

Two corrected ablations were then measured:

1. A broad generic-inline ablation (`34777598305`) recovered most singleton throughput but widened inline ownership and worsened loop lag across measured QoS0 cells. Rejected.
2. A narrow reader-only ablation (`34777729652`) was functionally clean and made batch1 essentially neutral, but transport fragmentation caused larger bursts to regain reader ownership. Batch8 loop-lag moved from retained **-1.43%** to **+3.48%**, and batch32 from **-1.84%** to **+4.62%**. Rejected.

There is no stateless local fact that reliably distinguishes a genuinely isolated QoS0 PUBLISH from one packet of a fragmented network burst. Adding a timer, streak counter, hysteresis bit or transport peek would recreate scheduler state for a localized benchmark gain. The detailed stopping rule and measurements are recorded in `UNIFORM-CALLBACK-FOLLOWUP-2026-09-13.md`.

## Final assessment

The strict sole-pending architecture succeeds at the simplification goal:

- **-71 runtime lines** vs RC14 main;
- substantially fewer scheduler/ownership states;
- no private asyncio queue mutation;
- no pair/tail transfer protocol;
- fixed-rate QoS1 p50 and CPU essentially neutral on the RC14 Raspberry Pi reference workload;
- measurable event-loop fairness improvement under larger synchronous bursts;
- explicit bounded worker ownership for fragmented direct-QoS0 bursts.

It is not performance-free:

- fixed-rate QoS1 p95/p99 are modestly worse;
- large synchronous hosted cells lose up to a few percent of closed-loop capacity;
- direct QoS0 singleton callback delivery loses about 6% throughput.

These costs are now **accepted and documented** for this candidate. Chasing either with broader inline dispatch, longer worker rounds or a burst detector would directly weaken the fairness/simplicity objective and add scheduling policy that the evidence does not justify.

## Final disposition

PR #458 is **implementation-complete and ready for review**. No further scheduler heuristic is planned. The branch should remain unmerged until the explicit merge decision is made, but there is no remaining technical blocker inside the scope of this scheduler simplification.
