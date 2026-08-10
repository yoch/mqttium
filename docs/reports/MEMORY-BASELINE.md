# Memory Baseline

This baseline was produced by `benchmarks/memory_profile.py` on GitHub Actions
benchmark run 22 from commit `1a9fe49bd6b2a3018f26171668cb5b18f44a0066`.
The runner used CPython 3.12.13 on Ubuntu 24.04 with glibc 2.39 and psutil 7.2.2.

The commit contains the new benchmark but no protocol admission, delivery-byte,
SQLite paging, or WebSocket batching corrections. It is the before-state for the
remaining commits in this pull request.

## Loaded-state results

| Scenario | Logical data | RSS delta | USS delta | Traced current delta | Traced peak |
|---|---:|---:|---:|---:|---:|
| QoS queue, 30,000 × 64 B | 2.17 MiB | 19.69 MiB | 19.70 MiB | 10.38 MiB | 10.41 MiB |
| QoS queue, 12,000 × 4 KiB | 47.01 MiB | 53.89 MiB | 53.89 MiB | 50.05 MiB | 50.09 MiB |
| Iterator queue, 6,000 × 4 KiB | 23.51 MiB | 25.44 MiB | 25.44 MiB | 24.20 MiB | 24.23 MiB |
| Memory store, 12,000 × 4 KiB | 47.01 MiB | 53.28 MiB | 53.29 MiB | 49.46 MiB | 49.49 MiB |
| SQLite hydration, 6,000 × 4 KiB | 23.51 MiB | 60.50 MiB | 60.50 MiB | 25.30 MiB | 50.62 MiB |

## Interpretation

### Outbound QoS queue

The 64-byte scenario retains only 2.17 MiB of logical payload/topic data but adds
19.69 MiB of RSS. Small-message memory is therefore dominated by Python records,
packet identifiers, dictionary/deque capacity, and other per-message state.

The 4 KiB scenario adds 53.89 MiB RSS for 47.01 MiB of logical data. The dominant
risk is not unusual amplification per message; it is that the protocol queue is
allowed to retain up to the packet-identifier-space limit without a finite
logical byte budget.

### Inbound iterator queue

The loaded RSS delta closely follows the 23.51 MiB logical total. The main issue
is the count-only default capacity, which permits much larger totals when payloads
are large. A shared delivery byte budget is expected to solve this without a
copying optimization.

### In-memory store

Active retention is similar to the outbound queue because both own the same
message objects. After logical release, `tracemalloc` returns to baseline. RSS
returns close to baseline only after diagnostic `malloc_trim(0)`, showing that
most post-drain RSS is allocator retention rather than a live MQTTium reference.

### SQLite hydration

Hydrating 23.51 MiB of logical message data adds 60.50 MiB RSS and reaches a
50.62 MiB Python-traced peak while only 25.30 MiB remains traced after startup.
This is direct evidence of temporary duplication during eager `fetchall()` plus
full object/list/queue hydration. SQLite paging is therefore justified even
before lazy payload loading is considered.

## Release-state results

After queues and stores were cleared, `tracemalloc` current allocation returned
to within approximately 0.002 MiB of baseline in every scenario. Before
`malloc_trim`, glibc retained most large-payload arenas in RSS. After the
diagnostic trim, residual RSS deltas ranged from approximately 0.50 to 1.64 MiB.

MQTTium must not call `malloc_trim` in normal operation. These measurements are
used only to distinguish live references from allocator-managed free memory.

## Comparison requirements

Later commits must preserve scenario count and payload size. Each material
correction should report:

- loaded RSS/USS and traced-current deltas;
- isolated max RSS and traced peak;
- logical counters proving equivalent work;
- released-state behavior;
- existing throughput, TLS, WAN, and persistence benchmark results.
