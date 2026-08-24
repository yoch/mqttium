# Runtime fuzzer composition qualification — 2026-08-24

## Decision

The bounded V2 lifecycle-composition target passes its decision gate. It creates
new search space, detects four interleaving-dependent behavioral mutations, and
completed a healthy 50,000-seed calibration with no failures. The next action is
a sharded long V2 campaign, not more grammar or scheduler architecture.

This report is evidence for the exact code below. It is not a maintained runtime
contract.

## Identity and environment

- V1/main baseline: `807f5e7081613842ad6d3c0cc8390204c4b3c9de`
- qualified V2 code: `b25045ed4c9e0dddec2123937500a1a773b892f3`
- branch: `codex/runtime-concurrency-fuzzer-composition`
- date: 2026-08-24 UTC
- host: Ubuntu Linux 6.8.0-137-lowlatency, x86_64
- CPU: Intel Core i7-3770, four cores/eight logical CPUs
- RAM: 33,524,031,488 bytes
- Python: CPython 3.12.13, default asyncio event loop
- local campaign priority: `nice -n 10`

The worktree was clean at the qualified code SHA. Failure artifacts were directed
outside the repository under `/tmp/mqttium-v2-final/`.

## Architecture delta from V1

V1 remains unchanged. V2 is a separate, small target that subclasses the V1
real-`AsyncClient` harness and reuses its writer, effect, delivery, lifecycle,
liveness, task, receipt, and epoch oracles. It adds only test-side gates at the
existing transport, callback, EffectPump, reconnect-factory, and transport-close
seams. It does not add production scheduling hooks or simulate asyncio.

Every schedule contains legal connected history followed by one of these bounded
ownership pairs:

- callback × reconnect factory;
- old writer × replacement generation;
- deferred effect × reconnect;
- callback × reader/transport teardown, including real due-keepalive teardown;
- callback × writer;
- effect × writer.

The generator records the identity and count of open windows, rejects releases
without an owner, permits exactly two simultaneous windows at the adversarial
boundary, and settles all gates. Release order and bounded intervening yields are
first-class generated operations. Failure artifacts use schema
`mqttium-runtime-fuzz-v2` and add pair, release trace, window depths, checkpoints,
and composition owner state to the inherited wire/epoch evidence.

## Production finding

V2 seed 9 exposed a real lifecycle ownership bug near, but distinct from, the
earlier keepalive cycle:

1. a callback was blocked;
2. a due keepalive timeout entered a blocked transport close;
3. the callback was released and requested explicit `connect()`;
4. the reader had not yet observed EOF;
5. explicit takeover treated the closing transport as a healthy connected
   generation and failed with `ProtocolError`.

The minimal regression
`test_callback_connect_takes_over_keepalive_close_before_reader_finally` was
added red first. The production correction is limited to the existing explicit
connect ownership boundary: `_prepare_explicit_connect()` no longer returns
early for a transport already reporting `is_closing()`. Reader-owned visible
teardown remains unchanged, and no packet hot path or new lifecycle abstraction
was introduced. Commit `badd8ca` contains the regression and fix. The original
seed replays successfully after the correction.

An apparent writer/EOF failure was separately classified as an expected invalid
checkpoint timing: the reader was inside the intentional fatal-disconnect writer
drain timeout, not deadlocked. The writer × reconnect schedule now uses broker
DISCONNECT to reach the intended ownership boundary without weakening an oracle.

## Deterministic generation

The serialized generated schedules for seeds `[100000, 102000)` at 48 steps were
hashed in two independent processes. Both produced:

`e32018705ad5d8537fb56cf36ecfa9b476f3b32828a8e4a8200dac94cd02e6df`

The serialization includes seed, pair, every rendered operation, release trace,
and per-step window depth.

## Behavioral mutation qualification

Each mutation was run over the same 600 seeds `[20000, 20600)` at 48 steps. A
matching composition must occur before the mutation can be observed; tests also
verify that an irrelevant pair remains healthy.

| Mutation | Failures | Detection rate | Required ownership overlap |
| --- | ---: | ---: | --- |
| old writer survives reconnect | 48/600 | 8.00% | writer × replacement generation |
| callback takeover loses | 153/600 | 25.50% | callback × reconnect |
| effect crosses generation | 100/600 | 16.67% | effect × reconnect |
| closing-transport takeover loses | 24/600 | 4.00% | callback × reader/keepalive teardown |

Rates are deliberately neither zero nor universal. All four qualifications had
600 unique complete traces, 600 unique scheduling traces, 64 release traces, and
100 schedules per ownership pair.

## Healthy reference and calibration

The final reference campaign used seeds `[100000, 150000)`, 48 steps, and the
qualified code SHA under `nice -n 10`:

- schedules: 50,000/50,000;
- failures: 0;
- total operations: 2,400,000;
- unique complete operation traces: 50,000;
- unique scheduling traces: 50,000;
- unique release-order traces: 64;
- wall time: 534.951 seconds reported by the target, 536.13 seconds externally;
- CPU time: 521.57 seconds;
- throughput: 93.47 schedules/second;
- peak RSS: 328,868 KiB;
- window-depth observations: depth 0 = 2,100,071; depth 1 = 208,264;
  depth 2 = 91,665.

Pair coverage was balanced to within one schedule:

| Pair | Schedules |
| --- | ---: |
| callback × reader teardown | 8,333 |
| callback × reconnect | 8,333 |
| callback × writer | 8,334 |
| effect × reconnect | 8,333 |
| effect × writer | 8,334 |
| writer × reconnect | 8,333 |

Important boundary counts included 25,001 active writer checkpoints, 25,000
blocked callbacks, 24,999 blocked reconnect factories, 16,667 blocked effects,
8,333 blocked transport closes, 16,666 takeover checkpoints, 4,179 real due
keepalive timeouts, and 50,000 terminal settlements. Every terminal schedule had
zero open composition windows and passed the unchanged V1 accounting, task,
receipt, effect, completed-write, epoch, loop-error, and watchdog oracles.

A smaller final-code reference over 2,000 seeds also completed with zero
failures, 2,000 unique complete and scheduling traces, all 64 release traces,
balanced pair coverage, and all six pair families exercised.

The uniqueness sets account for the approximately linear campaign memory use.
This does not invalidate the target, but a long run must aggregate bounded shard
summaries rather than execute one million seeds in one process.

## V1 permanent nightly evidence

PR #380 merged as `807f5e7081613842ad6d3c0cc8390204c4b3c9de`. The permanent V1 ARM64 nightly
runs 50,000 seeds at 32 steps in ten recorded 5,000-seed shards. Workflow run
number `n` selects the reproducible range starting at
`2,000,000 + (n - 1) * 50,000`; a rerun intentionally reuses its range. Manifests
record exact SHA, environment, ranges, diversity, coverage, performance/RSS, and
failure contexts. The manifest and JSON failures are retained for 30 days.

The first merged-main run executed `[2000000, 2050000)` on ARM64 with zero
failures, 1,600,000 operations, 48,010 operation traces, 47,680 scheduling
traces, 670.15 schedules/second, and 55,072 KiB peak RSS.

## Decision and next campaign

V2 meets every stated decision condition:

- all six intended lifecycle pairs receive balanced coverage;
- all 50,000 calibration schedules have distinct complete and scheduling traces;
- all 64 generated release traces occur;
- four behavioral mutations require composition and are detected at useful
  nontrivial rates;
- healthy execution is deterministic and false-positive-free.

Therefore run a long V2 campaign now. Do not expand the grammar first. A suitable
starting budget is 1,000,000 schedules at 48 steps, partitioned into at least 100
recorded 10,000-seed process shards to bound the measured trace-set memory. Use
the ARM64 self-hosted runner for the main campaign after the V2 commit is merged,
and replay a controlled identical range on x86_64 for cross-architecture
comparison. Preserve per-shard manifests and every failure artifact. Stop and
reduce any production finding before resuming expansion.

Do not add a third ownership window, protocol breadth, a generic scheduler, or a
shrinker until the long campaign results justify one of those changes.
