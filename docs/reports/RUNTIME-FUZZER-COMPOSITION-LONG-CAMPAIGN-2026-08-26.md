# Runtime fuzzer V2 long campaign — 2026-08-26

## Decision

The V2 lifecycle-composition target completed its required long x86-64
campaign without a failure. The campaign exercised 1,000,000 deterministic
schedules at 48 exact steps, covered every composition pair and window depth,
and retained near-total operation and scheduling-trace diversity. This closes
the pending local V2 million-seed gate.

This is a dated evidence report for the exact code and environment below. Do
not rewrite it to describe later code or campaigns.

## Identity and environment

- tested code: `0be97c8df28aa13cf05fc94d8114f7ce2bea8a8f`;
- branch: `codex/runtime-fuzz-timeout-profile` (PR #400);
- target/schema: `runtime-composition` / `mqttium-runtime-fuzz-v2`;
- seed range: `[6,000,000, 7,000,000)`;
- budget: 1,000,000 schedules × 48 exact operations;
- partition: 100 contiguous 10,000-seed process shards;
- concurrency: exactly three worker processes, each at `nice -n 19`;
- timing profile: 30-second whole-schedule watchdog and 10-second connection
  timeout;
- host: Ubuntu Linux 6.8.0-137-lowlatency, x86-64;
- CPU: Intel Core i7-3770, four physical/eight logical CPUs;
- RAM: 33,524,031,488 bytes, with 15,152,603,136 bytes available at start;
- Python: CPython 3.12.13, default Unix selector event loop.

The worktree was clean at the tested SHA when the locked orchestrator started.
Logs, summaries, the orchestrator, the aggregate manifest, and failure
directories were retained outside the repository under
`/tmp/mqttium-v2-long-0be97c8-p3-nice19-20260826/`.

## Method

The external orchestrator launched the existing V2 CLI in 100 bounded shards:

```text
nice -n 19 python -m tests.fuzz.runtime_composition_fuzzer \
  --seed <shard-start> --seeds 10000 --steps 48 \
  --watchdog-seconds 30 --connect-timeout-seconds 10 \
  --artifacts-dir <shard-failure-directory>
```

The manifest records exact Git identity, environment, commands, timing profile,
half-open seed ranges, exit status, stdout/stderr, CPU, wall time, peak RSS,
coverage, pair coverage, and window-depth counts for every shard. Whole-corpus
BLAKE2b-128 sets measure uniqueness; ordered SHA-256 corpus hashes make later
generation comparisons reproducible.

## Result

- schedules: 1,000,000/1,000,000;
- total generated operations: 48,000,000;
- failures and failure artifacts: 0;
- shard exit statuses: 100/100 successful;
- unique complete operation traces: 999,963 (99.9963%);
- unique scheduling traces: 999,598 (99.9598%);
- unique release traces: 64/64;
- ordered operation corpus SHA-256:
  `1a4a346f6c1bac6190cc9dc09f2b3132f8b34d9065ae1e72a6f4c0ede1786556`;
- ordered scheduling corpus SHA-256:
  `cb1f56bc5083d9bc467b9a557e1dff7a646626fa41dfc070785e72f003780614`;
- wall time: 4,255.72 seconds;
- aggregate throughput: 234.98 schedules/second;
- aggregate child CPU time: 10,336.43 seconds;
- shard wall time: median 118.76 seconds, p95 203.93 seconds, maximum
  217.32 seconds;
- maximum per-shard RSS: 88,312 KiB (86.24 MiB).

Pair coverage was balanced within one schedule:

| Pair | Schedules |
| --- | ---: |
| callback × reader teardown | 166,667 |
| callback × reconnect | 166,667 |
| callback × writer | 166,666 |
| effect × reconnect | 166,667 |
| effect × writer | 166,666 |
| writer × reconnect | 166,667 |

Every generated window depth was exercised:

| Open windows | Observations |
| ---: | ---: |
| 0 | 41,834,105 |
| 1 | 4,332,560 |
| 2 | 1,833,335 |

Important ownership boundaries included 500,000 active callback checkpoints,
499,999 active writer checkpoints, 333,333 active effect checkpoints, 500,001
blocked reconnect factories, 166,667 blocked transport closes, 333,334
takeover-generation checkpoints, 83,515 real due-keepalive timeouts, and
1,000,000 terminal settlements.

The four-mutant qualification recorded in
`RUNTIME-FUZZER-COMPOSITION-QUALIFICATION-2026-08-24.md` already demonstrated
the target's sensitivity to composition-specific defects. This healthy long
campaign extends schedule and seed evidence; it does not substitute raw volume
for mutation sensitivity.

## Timeout-methodology context

An earlier V2 attempt on the strict CI defaults was stopped after 910,000 valid
schedules because active host contention repeatedly expired wall-clock
deadlines. Its eight artifacts, replay evidence, and classification are recorded
in `RUNTIME-FUZZER-SHARED-RUNNER-TIMEOUTS-2026-08-26.md`. This campaign used a
fresh disjoint range and the explicit shared-runner profile added by PR #400;
the two attempts were not combined.

The longer deadlines affect only when the harness classifies a wall-clock
timeout. Ownership, wire, epoch, accounting, task-settlement, and terminal
oracles remained unchanged, and every failure would still have stopped the
campaign and produced an artifact.

## Remaining scope

- This is x86-64 evidence. The earlier qualification report recommends a
  controlled ARM64 campaign for cross-architecture comparison; this local host
  cannot provide it.
- V2 and V3 remain explicit/manual targets. The permanent ARM64 nightly remains
  V1-only until a rotation policy is chosen.

Subject to the repository's existing CI and review gates, V2 adds no blocker to
the next release candidate.
