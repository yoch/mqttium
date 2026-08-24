# Inbound replay performance — final candidate — 2026-08-24

## Scope and decision

This report supersedes the earlier same-day measurement after two correctness
follow-ups changed the replay cursor. Keep the final implementation at
`83c58e22696c9049af55165c672b6b6be390d95b`: built-in stores hydrate one fresh
64-message effect page, while legacy paged stores retain point revalidation.

The required SQLite 60,000-record cell is within 3% of base. Memory at 60,000
records costs 11.8 ms, and the additional 64 MiB SQLite cell costs 13.5 ms.
Those reconnect cold-path costs do not justify a new store API, capability, or
larger replay abstraction.

## Revisions and method

- base: `c3f594d73626f0e952392e82d9f3aca5cda508d0`;
- initial stale-replay implementation: `8bd021e95eb6617b51e9b99759c47b36b92253ec`;
- exact timed candidate: `83c58e22696c9049af55165c672b6b6be390d95b`.

The remote refs were fetched before the work. `origin/main` and the merge base
were `a336f834c66c4b4cb1a612e7c064ae29bb51cb7b`; no upstream change invalidated
the comparison.

Each timing sample ran in a fresh Python 3.12 process pinned to CPU 7. Base and
candidate alternated within pairs: seven pairs for the required 1 KiB cases and
five for the additional 16 KiB cases. SQLite samples reused one seeded database
shape; replay did not mutate it. Memory was seeded before each timed interval.
Every sample emitted exactly the requested number of messages. Wall time,
process CPU time, rate, and CV were recorded; CPU ratios tracked wall ratios.

The host was under concurrent load. Cells with high raw-arm CV are diagnostic,
not proof of a small delta. Raw output is in
`/tmp/mqttium-replay-ab-83c58e2.json`; it remains an uncommitted build artifact.

## Exact-candidate timings

| Store | Records | Payload | Base wall | Candidate wall | Ratio | Base CV | Candidate CV | Interpretation |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Memory | 4k | 1 KiB | 14.18 ms | 15.04 ms | 1.061 | 8.64% | 17.93% | candidate noisy |
| Memory | 60k | 1 KiB | 208.05 ms | 219.88 ms | 1.057 | 4.66% | 3.37% | valid; 11.8 ms cold-path cost |
| SQLite | 4k | 1 KiB | 81.24 ms | 65.78 ms | 0.810 | 12.57% | 10.03% | noisy; no regression shown |
| SQLite | 60k | 1 KiB | 1.057 s | 1.082 s | 1.024 | 3.68% | 6.12% | valid; within 3% |
| Memory | 4k | 16 KiB | 13.86 ms | 14.81 ms | 1.069 | 16.36% | 12.98% | invalid/noisy |
| SQLite | 4k | 16 KiB | 130.68 ms | 144.21 ms | 1.104 | 6.41% | 4.16% | valid; 13.5 ms for 64 MiB |

The valid SQLite 60k candidate replayed 55,436 messages/s versus 56,749 for
base. This is local diagnostic evidence, not an eligible-host release claim.

## Peak Python allocation

Tracemalloc ran separately for three alternating pairs so instrumentation did
not contaminate timings. Every sample again replayed the exact requested count.
Raw output is `/tmp/mqttium-replay-alloc-results.json`.

| Store | Records | Base replay peak | Candidate replay peak | Ratio |
| --- | ---: | ---: | ---: | ---: |
| Memory | 4k | 197,240 B | 195,272 B | 0.990 |
| Memory | 60k | 2,611,352 B | 2,609,384 B | 0.999 |
| SQLite | 4k | 1,109,417 B | 557,823 B | 0.503 |
| SQLite | 60k | 5,819,449 B | 5,263,423 B | 0.904 |

Startup allocation changed by at most 1.6%. The smaller SQLite replay peaks are
consistent with hydrating 64 rather than 256 records at an effect boundary.

## Correctness closure

The full validation initially exposed one cursor-termination difference: a
single-record replay retained the recovered-MID view until an empty continuation.
The cursor now tracks the initial remaining count and settles immediately after
the last hydrated page, accounting once per page rather than per message.

Memory/SQLite stale completion, delivered-state, byte-bound, close, reconnect,
and differential schedule tests pass. A focused regression asserts zero
per-message `in_meta()` probes across a 500-record replay for both built-in
stores. The old report remains unchanged as historical evidence for its named
candidate.
