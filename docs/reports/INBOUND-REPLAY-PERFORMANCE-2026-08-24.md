# Inbound replay performance — 2026-08-24

## Decision

Keep stale-record protection, but align built-in store hydration pages with the
64-message effect batch. This removes the measured SQLite point-lookup cost
without adding a store API, capability adapter, or lifecycle abstraction.

No further replay optimization is justified by this local evidence. The final
candidate has no demonstrated SQLite regression. A small Memory relative delta
at 60,000 records is only 13.4 ms absolute on a reconnect cold path and does not
justify more machinery.

## Compared revisions

- base: `c3f594d73626f0e952392e82d9f3aca5cda508d0`;
- initial stale-replay implementation: `8bd021e95eb6617b51e9b99759c47b36b92253ec`;
- optimized candidate: `50db3f9b3795c9acaedb48710894f81e4b1b9e43`.

The remote refs were fetched immediately before the work. At that point
`origin/main` and the merge base were both
`a336f834c66c4b4cb1a612e7c064ae29bb51cb7b`; the consolidation branch contained
only `c3f594d` and `8bd021e`. No upstream commit invalidated the comparison.

## Method

Each timing sample ran in a fresh Python 3.12 process. Base and candidate order
alternated within pairs. The final timing campaign was pinned to CPU 7 and used
seven pairs for the required 1 KiB cases and five pairs for the additional
16 KiB cases. Replay wall time, process CPU time, message rate, batch count, and
emitted message count were recorded. Every sample emitted exactly the requested
number of messages.

SQLite scenarios reused one read-only seeded database shape across sequential
samples; replay itself did not mutate it. Memory scenarios seeded the store in
each fresh process before the timed interval. Engine construction was measured
separately from replay. Tracemalloc ran in a separate three-pair campaign so its
instrumentation did not contaminate timing results.

The host was under material concurrent load. Raw-arm CV is therefore reported,
and noisy cells are diagnostic rather than evidence of a small delta. Raw JSON
was written to `/tmp/mqttium-replay-ab-50db3f9.json` and
`/tmp/mqttium-replay-alloc-results.json`, in accordance with the benchmarking
contract; those build artifacts are not committed.

## Finding and minimal correction

At `8bd021e`, built-in replay refreshed metadata with `in_meta(mid)` before each
message emission. The cost was real on SQLite:

| Scenario | Candidate/base wall median | Base CV | Candidate CV |
| --- | ---: | ---: | ---: |
| SQLite, 4k × 1 KiB | 2.110 | 9.54% | 7.43% |
| SQLite, 60k × 1 KiB | 1.729 | 7.34% | 5.57% |
| SQLite, 4k × 16 KiB | 1.435 | 3.84% | 3.84% |

The correction uses the existing `BoundedInboundReplayStore` contract. Built-in
stores now hydrate at most one effect batch per page, and the synchronous engine
consumes that fresh page before application code can mutate it. Memory snapshots
the same ordered identifiers as SQLite so deletions do not shift store-specific
batch boundaries. The existing point revalidation remains for legacy paged
stores whose larger pages can span continuations.

## Final exact-candidate timings

| Store | Records | Payload | Base wall | Candidate wall | Ratio | Base CV | Candidate CV | Interpretation |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Memory | 4k | 1 KiB | 12.65 ms | 14.20 ms | 1.122 | 12.51% | 10.64% | noisy |
| Memory | 60k | 1 KiB | 205.90 ms | 219.31 ms | 1.065 | 7.32% | 4.89% | small cold-path cost |
| SQLite | 4k | 1 KiB | 74.32 ms | 69.78 ms | 0.939 | 10.82% | 4.46% | baseline noisy; no regression shown |
| SQLite | 60k | 1 KiB | 1.116 s | 1.078 s | 0.966 | 7.06% | 4.31% | valid; no regression |
| Memory | 4k | 16 KiB | 14.93 ms | 14.29 ms | 0.957 | 63.34% | 2.99% | invalid/noisy |
| SQLite | 4k | 16 KiB | 139.90 ms | 143.87 ms | 1.028 | 20.19% | 13.30% | invalid/noisy |

CPU-time ratios tracked wall-time ratios. The valid SQLite 60k candidate replayed
55,665 messages/s versus 53,777 messages/s for base. This is local diagnostic
evidence, not an eligible-host release-performance claim.

## Peak Python allocation

| Store | Records | Base replay peak | Candidate replay peak | Ratio |
| --- | ---: | ---: | ---: | ---: |
| Memory | 4k | 197,240 B | 195,232 B | 0.990 |
| Memory | 60k | 2,611,352 B | 2,609,344 B | 0.999 |
| SQLite | 4k | 1,109,417 B | 557,815 B | 0.503 |
| SQLite | 60k | 5,819,449 B | 5,263,383 B | 0.904 |

Startup allocation changed by at most 1.6% in these cells. The smaller SQLite
replay peaks are consistent with hydrating 64 rather than 256 records at an
effect boundary.

## Correctness guard

Memory and SQLite stale-record, completion, delivered-state, byte-bound, close,
reconnect, and differential schedule tests pass with the new boundary. A focused
regression also asserts that a 500-record built-in replay performs zero
per-message `in_meta()` probes for both stores. Legacy paged stores retain their
payload-free point validation.
