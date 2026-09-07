# Investigation — `message_delivery="both"`

Base: `main@86560fb0b34be88ac5ab84102c382033e6b3691d`

## Decision boundary

`MessageDelivery` and `AsyncClient.message_delivery` are Stable. The stability policy requires Stable surface removal only in a major release, after deprecation/migration guidance. Therefore this sprint must not remove `"both"` from the 1.x contract merely because a single-owner prototype is shorter.

## Evidence

- Directly named `both` machinery in `ApplicationDelivery`: 68 lines in the pre-investigation implementation.
- A full single-owner prototype removed 124 runtime lines and added 22, but it also removed Stable dual-delivery semantics.
- Dual admission microbench on hosted x86 showed about 864 ns/message versus about 242 ns for iterator and 560 ns for callback: the cost is approximately the work of both pipelines, not an anomalous hidden tax.
- Shared byte-reservation lifecycle measured about 657 ns and 88 B/token versus about 292 ns and 40 B for a single reference. That extra state is the cost of keeping one logical byte reservation alive until both consumers release it.
- The documented use case is narrow but real: two independent application consumers receiving the same message with MQTTium-owned backpressure/accounting.

## Recommendation

Keep `message_delivery="both"` for 1.x. Reconsider removal only through the Stable deprecation/major-release process.

The implementation may still delete behavior-preserving duplication. In particular, the no-byte-budget `_accept_both_unaccounted` path duplicates `_accept_both_fast`; when `small_message_limit is None`, the latter naturally skips size checks while preflighting both queue capacities. Qualify that simplification independently and keep it only if tests and performance remain acceptable.
