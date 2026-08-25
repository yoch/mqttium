# Runtime fuzzer V3 long campaign — 2026-08-26

## Decision

The V3 runtime-pressure target completed its first long campaign without a
failure. The campaign exercised 1,000,000 deterministic schedules at 48 exact
steps, kept every mandatory pressure surface hot, and produced near-total
operation and scheduling-trace diversity. This result closes the x86-64 long
campaign gate for PR #399. It does not replace the separate ARM64 check or the
still-pending V2 million-seed campaign.

This is a dated evidence report for the exact code and environment below. Do
not rewrite it to describe later code or campaigns.

## Identity and environment

- tested code: `0bae517e413fd5986f3060c609c92599bb2779c9`;
- branch: `codex/pr399-audit-fixes` (PR #399 source was updated to the same SHA);
- target/schema: `runtime-pressure` / `mqttium-runtime-fuzz-v3`;
- seed range: `[4,000,000, 5,000,000)`;
- budget: 1,000,000 schedules × 48 exact operations;
- partition: 100 contiguous 10,000-seed process shards;
- concurrency: exactly three worker processes, each at `nice -n 19`;
- host: Ubuntu Linux 6.8.0-137-lowlatency, x86-64;
- CPU: Intel Core i7-3770, four physical/eight logical CPUs;
- RAM: 33,524,031,488 bytes, with 13,022,572,544 bytes available at start;
- Python: CPython 3.12.3, default Unix selector event loop.

The worktree was clean at the tested SHA. Logs, summaries, the orchestrator,
the aggregate manifest, and failure directories were retained outside the
repository under
`/tmp/mqttium-v3-long-0bae517-p3-nice19-20260825/`.

## Method

The external orchestrator launched the existing V3 CLI with
`--require-coverage` for every shard. Each command used the same form:

```text
nice -n 19 python -m tests.fuzz.runtime_pressure_fuzzer \
  --seed <shard-start> --seeds 10000 --steps 48 --require-coverage \
  --artifacts-dir <shard-failure-directory>
```

The campaign manifest records the exact Git identity, environment, commands,
half-open seed ranges, exit status, stdout/stderr, CPU, wall time, peak RSS,
coverage, and pressure counters for all 100 shards. Whole-corpus BLAKE2b-128
sets measure uniqueness; ordered SHA-256 corpus hashes make later generation
comparisons reproducible.

## Result

- schedules: 1,000,000/1,000,000;
- total generated operations: 48,000,000;
- failures and failure artifacts: 0;
- shard exit statuses: 100/100 successful;
- unique complete operation traces: 999,976 (99.9976%);
- unique scheduling traces: 999,736 (99.9736%);
- ordered operation corpus SHA-256:
  `f487048c672ebf9e1b798d4b56f8324a7d78c3be3ffd90b8e5e370f21189bb4a`;
- ordered scheduling corpus SHA-256:
  `796616e7500591e37b05fe965b228949bd4089ca48aac7c9026d6a916db72cf7`;
- wall time: 3,374.78 seconds;
- aggregate throughput: 296.32 schedules/second;
- aggregate child CPU time: 9,280.36 seconds;
- shard wall time: median 93.33 seconds, p95 105.54 seconds, maximum
  132.42 seconds;
- maximum per-shard RSS: 89,604 KiB (87.50 MiB).

Every family received either 90,909 or 90,910 schedules:

| Family | Schedules |
| --- | ---: |
| eager paced | 90,909 |
| latency batch burst | 90,909 |
| `write_many` burst | 90,909 |
| segmented write | 90,909 |
| parked publisher release | 90,910 |
| parked publisher cancellation | 90,909 |
| parked publisher teardown | 90,909 |
| pressure × reader teardown | 90,909 |
| pressure × reconnect | 90,909 |
| pressure × callback | 90,909 |
| pressure × effect | 90,909 |

## Mandatory pressure coverage

All coverage-gated V3 surfaces were observed:

| Counter | Observations |
| --- | ---: |
| eager accepted | 5,462,120 |
| eager refused | 181,818 |
| latency batches | 90,909 |
| qualifying `write_many` calls | 136,088 |
| segmented writes | 90,909 |
| parked publisher observed | 363,637 |
| writer waiters observed | 90,909 |
| four resident writer frames | 90,909 |
| sixteen resident writer frames | 90,909 |
| all pressure/lifecycle overlaps | 363,636 |
| reader-teardown overlaps | 90,909 |
| reconnect overlaps | 90,909 |
| callback overlaps | 90,909 |
| EffectPump overlaps | 90,909 |

The eight-mutant qualification recorded in
`RUNTIME-FUZZER-PRESSURE-QUALIFICATION-2026-08-25.md` had already passed on
the same tested code. The long healthy campaign therefore extends the seed and
schedule evidence; it does not substitute raw volume for mutation sensitivity.

## Invalid calibration attempt

Before the recorded campaign, terminal interruption of a temporary
ThreadPool-based orchestrator left child workers alive. Repeated restarts
briefly produced three orchestrators and fifteen workers, which violated the
intended local-runner limit. That attempt was stopped, all matching processes
were verified absent, and its output was excluded from this report.

The overloaded attempt wrote one artifact for seed 3,161,661 after the
harness's 0.5-second reconnect deadline expired. The exact seed passed once
through the CLI and 500/500 times in-process on the same SHA. It was classified
as an environmental false positive from the invalid orchestration, not as a
reproducible MQTTium failure. The final campaign used a new disjoint range, one
locked orchestrator, three workers, and `nice -n 19` throughout.

## Remaining scope

- V2 has a clean 50,000-seed calibration and previously found the real
  closing-transport takeover defect documented in its qualification report.
  Its recommended 1,000,000-seed, two-window campaign remains pending.
- This run is x86-64 evidence. A shorter controlled ARM64 replay remains useful
  for cross-architecture confirmation, but is not provided by this local host.
- V2 and V3 are still explicit/manual targets; the permanent 50,000-seed ARM64
  nightly remains V1-only until a rotation policy is chosen.

Subject to the existing CI and review gates, the V3 long-campaign result adds
no new blocker to merging PR #399.
