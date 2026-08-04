# Packet identifier pool performance audit

## Scope

This audit starts from `main` after PR #18 and isolates the outbound MQTT
packet-identifier allocator used by PUBLISH QoS 1/2, SUBSCRIBE and UNSUBSCRIBE.
The protocol range remains `1..65535`; Receive Maximum is still enforced by
`FlowControl`, not by the identifier pool.

The previous allocator retained every live identifier in a Python `set[int]`
and every released identifier in a `list[int]`. That gives average constant-time
membership, but its memory grows with the number of identifiers that have
crossed the allocator, and persistent-session hydration calls `list.remove()`
for every reserved MID.

## Alternatives evaluated

The audit compared:

- a fixed byte bitmap;
- a Python-integer bitset;
- a two-level bitset;
- arrays and linked free lists;
- interval/range tracking;
- adaptive set-to-bitmap representations;
- a frontier plus sparse holes.

Fixed bitmaps are compact but slower on the release/reallocate hot path in pure
Python. Range tracking is exceptionally compact for sequential identifiers, but
random releases make updates linear in the number of ranges. Array-based free
lists add a permanent allocation and did not beat the existing hot path.

## Selected representation

The selected allocator tracks:

- `_next`: the first identifier that has not crossed the allocation frontier;
- `_free_one`: one scalar hole for the common ACK then immediate reuse path;
- `_free_many`: an optional set created only when several holes coexist;
- `_reserved`: an optional set for out-of-order persistent-session hydration.

Sequential allocation and sequential hydration retain no object per live MID.
When the pool becomes empty, it still resets to MID 1. The existing correctness
requirements therefore remain unchanged: terminal publication effects must be
emitted before MID release, and receipts must be settled FIFO per MID.

## Local A/B evidence

The committed benchmark runs both implementations in the same process, rotates
through the same operations and writes JSON as a build artefact. On CPython
3.13.5 with 30,000 operations and seven repetitions, the median local result was:

| Operation | Previous | Frontier | Change |
| --- | ---: | ---: | ---: |
| Sequential allocate | 271 ns/op | 134 ns/op | 2.0× faster |
| Sequential reserve | 738 ns/op | 178 ns/op | 4.2× faster |
| Reuse, window 1 | 485 ns/op | 437 ns/op | 10% faster |
| Reuse, window 16 | 348 ns/op | 340 ns/op | 2% faster |
| Reuse, window 1000 | 399 ns/op | 345 ns/op | 14% faster |
| Sequential drain | 403 ns/op | 502 ns/op | 25% slower |

Deep retained-size estimates for the pool object and its containers:

| State | Previous | Frontier |
| --- | ---: | ---: |
| Idle | 392 B | 136 B |
| 30,000 live sequential MIDs | 2,937,544 B | 136 B |
| 15,000 of 30,000 released | 3,058,920 B | 944,612 B |

The mass-drain regression is explicit and accepted provisionally because normal
broker traffic releases identifiers interleaved with new allocations, while the
new representation materially improves that steady path and startup hydration.
CI uploads fresh benchmark JSON so the trade-off remains reviewable across
supported Python versions and runners.

## Correctness coverage

The dedicated tests cover full-range exhaustion, out-of-order reservations,
hole reuse, duplicate reserve/release calls, invalid identifiers, empty-pool
reset, the exhaustion edge case where the final MID is reserved, and a
deterministic randomized comparison against a reference `set[int]` model.
