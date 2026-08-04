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

- fixed and lazy byte bitmaps;
- Python-integer and two-level bitsets;
- arrays and linked free lists;
- interval/range tracking;
- adaptive set-to-bitmap representations;
- sparse dictionaries of 64-bit blocks;
- a frontier plus sparse holes.

Fixed bitmaps are compact but slower on the release/reallocate hot path in pure
Python and retain a fixed allocation per client. Range tracking is exceptionally
compact for sequential identifiers, but random releases make updates linear in
the number of ranges. Sparse 64-bit blocks reduce fragmented memory further but
slow the ACK/reallocation path materially.

## Selected representation

The selected allocator tracks:

- `_next`: the first identifier that has not crossed the allocation frontier;
- `_free_one`: one scalar hole for the common ACK then immediate reuse path;
- `_free_many`: an optional set created only when several holes coexist;
- `_free_count`: a scalar count used for constant-time length and empty checks;
- `_reserved`: an optional set for out-of-order persistent-session hydration.

Sequential allocation and sequential hydration retain no object per live MID.
When the pool becomes empty, it still resets to MID 1. The existing correctness
requirements therefore remain unchanged: terminal publication effects must be
emitted before MID release, and receipts must be settled FIFO per MID.

## CI A/B evidence

The committed benchmark runs both implementations in the same process and
writes JSON as a build artefact. The `sequential_drain` case times the complete
burst lifecycle — allocate 30,000 identifiers, then release them sequentially —
so it is not a release-method-only microbenchmark.

On the GitHub Actions reference runner, CPython 3.12.13, 30,000 operations and
seven repetitions, the median result is:

| Operation | Previous | Frontier | Change |
| --- | ---: | ---: | ---: |
| Sequential allocate | 233.3 ns/op | 154.9 ns/op | 33.6% faster |
| Sequential reserve/hydrate | 901.6 ns/op | 158.2 ns/op | 82.5% faster |
| Reuse, window 1 | 411.9 ns/op | 379.8 ns/op | 7.8% faster |
| Reuse, window 16 | 310.7 ns/op | 293.8 ns/op | 5.4% faster |
| Reuse, window 1000 | 360.5 ns/op | 328.2 ns/op | 8.9% faster |
| Allocate + sequential drain | 330.4 ns/op | 410.5 ns/op | 24.2% slower |

Deep retained-size estimates for the pool object and its containers:

| State | Previous | Frontier |
| --- | ---: | ---: |
| Idle | 392 B | 144 B |
| 30,000 live sequential MIDs | 2,937,544 B | 144 B |
| 15,000 of 30,000 released | 3,058,920 B | 944,648 B |

The burst-drain regression is explicit and bounded to the requested range. It is
the price of recording holes rather than every live identifier. Normal broker
traffic interleaves acknowledgements with new allocations, where all measured
windows are faster, while persistent-session hydration and large active sets
benefit materially in both CPU and memory.

## Correctness coverage

The dedicated tests cover full-range exhaustion, out-of-order reservations,
hole reuse, duplicate reserve/release calls, invalid identifiers, empty-pool
reset, scalar hole-count accounting, the exhaustion edge case where the final
MID is reserved, and a deterministic randomized comparison against a reference
`set[int]` model. The complete CI matrix, fuzzing and broker integrations pass
on Python 3.11 through 3.14.
