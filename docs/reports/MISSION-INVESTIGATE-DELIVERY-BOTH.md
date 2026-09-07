# Investigation — `message_delivery="both"`

Base: `main@86560fb0b34be88ac5ab84102c382033e6b3691d`

## Decision boundary

`MessageDelivery` and `AsyncClient.message_delivery` are Stable. The stability policy requires Stable surface removal only in a major release, after deprecation/migration guidance. Therefore this sprint must not remove `"both"` from the 1.x contract merely because a single-owner prototype is shorter.

## Evidence

- Directly named `both` machinery in `ApplicationDelivery`: 68 lines in the pre-investigation implementation.
- A full single-owner prototype removed 124 runtime lines and added 22 (net -102), but it also removed Stable dual-delivery semantics.
- Dual admission microbench on hosted x86 showed about 864 ns/message versus about 242 ns for iterator and 560 ns for callback: the cost is approximately the work of both pipelines, not an anomalous hidden tax.
- Shared byte-reservation lifecycle measured about 657 ns and 88 B/token versus about 292 ns and 40 B for a single reference. That extra state is the cost of keeping one logical byte reservation alive until both consumers release it.
- The documented use case is narrow but real: two independent application consumers receiving the same message with MQTTium-owned backpressure/accounting.

## Behavior-preserving simplification experiment

The no-byte-budget `_accept_both_unaccounted` method was experimentally removed and its calls routed through `_accept_both_fast`.

- focused delivery suite: 76/76 passed;
- implementation delta: 1 insertion, 19 deletions;
- no-byte `both` admission median: about 1203 ns/message baseline versus 1514 ns/message candidate on the same hosted x86 runner, roughly a 26% regression.

The candidate was therefore rejected and is not part of this branch. Saving 18 lines does not justify a material regression in the exact path those lines optimize.

## Recommendation

Keep `message_delivery="both"` and its current optimized implementation for 1.x. Reconsider removing the mode only through the Stable deprecation/major-release process. The single-owner prototype is useful evidence for that future decision, but there is no justified 1.x runtime change from this investigation.
