# Lean-native experiment — 2026-09-10

This incompatible experiment removes 3,369 Python source lines (19.6%) while
retaining the native MQTT features and bounded protocol/application pipelines.
The functional qualification passes; the measured performance trade-offs appear
below. These are diagnostic measurements from an ineligible, shared host, not
release qualification or a public cross-client comparison.

## Exact revisions and scope

- Baseline A: `9ad1f01857306ac5079ffb1d073a59fdb60e1931`.
- Candidate B: `6fd09d8c62c68cc58e408af83df95e8aeeba2044`.
- Branch: `codex/lean-native-experiment`.
- Isolated checkout: `/tmp/mqttium-lean-native-experiment`.
- The primary checkout and its pre-existing untracked audit were preserved.
  No merge, push, tag or publication was performed.
- A later documentation and issue-form commit adds this report; it does not
  change the measured source revision.

MQTT 3.1.1/5, QoS 0/1/2, TCP/TLS, WebSocket, Unix sockets, memory/SQLite,
reconnection, MQTT 5 authentication and manual acknowledgements remain. Runtime
dependencies remain empty. Python 3.11–3.14 remains the target; this local
qualification ran on CPython 3.12.13, not a four-version CI matrix.

## Structural reduction

Counts below include all physical lines in tracked `src/**/*.py`, including
comments and blank lines. Methods are AST function definitions directly on
`AsyncClient`, including property accessors; private names start with `_`.

| Measure | Baseline | Candidate | Change |
| --- | ---: | ---: | ---: |
| Python source files | 64 | 58 | −6 |
| Python source lines | 17,186 | 13,817 | −3,369 (−19.6%) |
| `AsyncClient` methods | 119 | 73 | −46 |
| `AsyncClient` private methods | 93 | 51 | −42 |
| Constructor keyword parameters | 32 | 31 | −1 |
| Client adapter lines | 2,982 | 2,197 | −785 |
| Application delivery lines | 1,036 | 272 | −764 |
| Effect pump lines | 387 | 351 | −36 |
| Outbound session lines | 1,360 | 1,193 | −167 |
| SQLite store lines | 1,041 | 954 | −87 |
| Property codec lines | 430 | 415 | −15 |

The implementation commit changes 192 files: 3,476 inserted and 12,968 deleted
lines across source, tests, documentation and infrastructure (net −9,492).
These repository-wide counts include newly added regression coverage and the
comparison harness; they are not a runtime-complexity metric.

Paho and helpers are removed with their tests, examples and dedicated commands.
The supported experimental surface is the native client, its models/results,
receipts/statistics, operational errors/enums and the two supplied stores.
Engine, codec, transport/store extension protocols and persistence records are
internal without compatibility wrappers. Existing internal importability is
not a support promise. See the [API contract](../api-stability.md) and
[migration guide](../migration.md) for exact names, signatures and differences.

## Contract changes and retained guarantees

| Area | Experimental contract |
| --- | --- |
| Delivery selection | `iterator` by default or `callback`; no `auto`/`both` |
| Routing | Registration-order multi-match and `on_message` fallback; permanently frozen at the first connection attempt |
| Notifications | One bounded serial worker for message, connect and publish callbacks; one job and one byte charge per message |
| Delivery capacity | One accounting rule, no small-message partition/shared reservations; optional single deadline spans byte and queue waits |
| Callback lifecycle | Errors isolated between matches; bytes retained through completion; disconnect/auth directly awaited outside locks; reentrant shutdown does not join itself |
| Admission | Shared unit transaction for awaited and batch publication; strict `publish_nowait()` capacity refusal before commitment |
| Batches | Progressive input, at most one read-ahead item; pending window bounded by flow limit; finite failure details and sealed committed-prefix receipt on every exit |
| Cancellation | Before commitment, no publication; after commitment, active work can survive cancellation; receipt wait cancellation does not cancel MQTT |
| Configuration | Deeply immutable properties with owned binary data, frozen authentication handler and reconnect policy, per-client retry progression |
| CONNECT limits | Dedicated arguments; duplicate properties rejected before mutation |
| Persistence | New schema 5 only, canonical JSON, no historical migrations/backfills; payload-last layout, paged replay, targeted transitions and lazy transactions retained |
| Store grouping | Internal `batch()` groups atomic mutations and engine compensation; memory batches do not promise universal application rollback |

A callback that waits for capacity or an ACK can form a circular wait when its
own delivery pipeline is saturated and no timeout is configured. The migration
and sizing guides document synchronous refusal via `publish_nowait()` and a
separate application producer with its own bound.

Only the writer writes transport bytes. Receipt registration precedes SEND;
segmented writes remain consecutive. Decoder bytes are owned, packet identifiers
remain distinct from flow slots, effects retain connection epochs, callbacks
run outside engine critical sections, and replay remains bounded with no
in-session retransmission timer.

Qualification exposed a batch MID-reuse defect during development: an ACK could
free an identifier while its completion effect remained behind blocked delivery.
A later batch admission could replace the same aggregate receipt entry. The
candidate drains old effects before commitment and rechecks under the engine
lock. A regression test reproduces the blocked-delivery/ACK ordering and proves
that a new publication waits for its own ACK.

SQLite refusal also covers live WAL files. Validation probes an immutable
read-only database when no nonempty WAL exists, or a private database/WAL copy
when one does. It validates the exact schema before opening the source for
normal operation. Regression tests compare database and journal bytes before
and after refusal for historical, future and inconsistent formats. Concurrent
external schema modification is unsupported.

## Functional qualification

| Check | Result |
| --- | --- |
| Ruff formatting and lint, `src tests benchmarks` | Passed, 286 Python files formatted |
| mypy, `src/mqttium` | Passed, 58 source files |
| Bandit, `-q -ll -r src` | Passed |
| Unit + project + mandatory Mosquitto integration + fuzz suites | **1,725 passed**, no skips, 221.20 seconds |
| Coverage, same combined suite | **92.49%**, above the configured 89% threshold |
| Hypothesis and stateful invariant suites | Passed within the combined suite |
| Deterministic fuzz, seed 1 | Codec, engine and WebSocket: 20,000 iterations each; **60,000 total, zero crashes/invariant violations** |
| Strict MkDocs build | Passed |
| Modified workflow/issue YAML and shell syntax | 3 YAML files parsed; 27 shell blocks passed `bash -n` |

The combined run used `MQTTIUM_REQUIRE_BROKER=1`, the local Mosquitto listener
on `127.0.0.1:11883`, and a 30-second per-test timeout. The final command was:

```bash
MQTTIUM_REQUIRE_BROKER=1 python -m pytest -q \
  tests/unit tests/project tests/integration tests/fuzz \
  --cov=mqttium --timeout=30 --tb=short
```

The final run includes the corrected fuzzer-contract assertion text; an earlier
run had 1,724 passes and one assertion-message mismatch, with no missed engine
invariant. That expectation was fixed before the measured candidate commit.


Explicit tests cover immutable input ownership and durable restart, permanent
route freeze through failed connection/reconnect/disconnect, callback
multi-match/error/cancellation/reentrant shutdown, byte/count saturation and
one deadline, credit release, capacity-one progressive batches, generator
failure, cancellation around commitment, blocked writers, immediate ACKs and
MID reuse. Existing transaction, store-commit, paged replay, QoS 2, manual ACK,
connection-epoch, TLS/Unix/WebSocket and reconnect tests remain covered.

Retired tests protected removed Paho/helpers, delivery duplication, inline
callbacks, physical message batches, chunk atomicity, mutable configuration or
historical SQLite migration. Their replacement tests target the new contracts
and state owners instead of restoring historical private relays.

## Comparison method and limitations

The committed `benchmarks/lean_native_compare.py` comparison harness runs
96 cells: two protocol versions × three QoS levels × two stores × two delivery
modes × bursts 1/2/8/long. Each cell first calibrates on A, then runs a same-code
A/A ABBA cycle and two A/B ABBA cycles. Every arm uses a fresh interpreter;
there are four A and four B observations per comparison cell. The A/A arms both
load the exact baseline source. Imports are checked against the selected root.

A self-subscribed native client publishes and consumes verified 256-byte
sequence/timestamp payloads through Mosquitto on localhost. The subscription QoS
matches publication QoS. Both arms use a five-second delivery deadline for this
benchmark. Flow is 20 on both arms; all other configured queue,
byte and ingress limits match. The same application burst pattern drives both
revisions; a long batch uses each revision's `publish_many()` implementation,
including the baseline's chunk behavior. This exercises combined publishing and
receiving, not isolated publisher throughput.

Each phase warms up with 32 messages. Calibration targets 0.2 seconds, with a
minimum of 128 messages (2,048 for long batches) and maximum of 8,192. Actual
sample durations vary and are retained in raw results. The timed phase ends
only after every ordered payload, publication receipt and final inbound QoS 2
handshake completes. Throughput is actual delivered messages divided by elapsed
time. CPU time belongs to the client process and excludes the broker.

Latency starts when the producer constructs an element immediately before
admission. It includes prefetch and local queue residence through application
observation; it is not broker-only latency. The wall-clock throughput measure
also includes waits before the iterable produces an element. Timing runs have
no tracemalloc overhead. A separate phase with up to 2,048 messages measures
peak traced Python allocations after warmup; it excludes pre-existing objects,
SQLite native allocations and kernel buffers. RSS is Linux `VmHWM` sampled in
the performance phase, a process high-water mark, not retained payload memory.

Per cell, each ABBA cycle ratio is the geometric mean of B divided by that of A;
the reported ratio is the geometric mean across the two cycles. Absolute values
are medians of the four raw observations per arm. A ratio therefore need not
equal the quotient of the displayed medians. Group ratios below are unweighted
geometric means across cells, descriptive summaries rather than estimates of an
application workload or statistical significance.

The host is Linux x86-64, Intel Core i7-3770 (8 logical CPUs), CPython 3.12.13,
Mosquitto 2.0.18, governor `performance`. Each client worker is pinned to logical
CPU 4; the existing broker is unpinned. Other host activity was not controlled.
No tests or other heavy work from this task ran concurrently with the comparison.
The immediate preflight failed: 45.7% CPU, load/CPU 0.562, temperature 87°C,
versus usual limits of 20%, 0.25 and 80°C. Consequently all numerical conclusions
are diagnostic. A/A variation below quantifies some, not all, of that noise.
There is no performance acceptance threshold for this experiment.

This matrix does not measure large payloads, rich properties, topic aliases,
reconnect/replay throughput or other transport variants. Their functional
coverage is separate; these throughput ratios should not be extrapolated to
those workloads.

## Observed performance and noise

Across all 96 equally weighted cells, delivered-rate B/A is **0.849** (-15.1%).
CPU/message B/A is **1.282**; p95 latency B/A is **0.682**.
Peak traced Python allocations B/A are **1.350** and peak RSS B/A is **0.990**.
These summaries describe this run; the shared-host noise prevents treating
them as precise portable estimates. No measured slowdown was used to reject
the experiment.

The same-code A/A rate ratios range from 0.277 to 2.831;
the median absolute departure from 1 is 6.3%.
67/96 controls depart by more than 2%.
Baseline raw-arm rate CV has median 10.4% and maximum 130.2%;
70/96 cells exceed the usual 5% diagnostic criterion.
Small apparent improvements or regressions within that variation should
not be interpreted as effects of the code change. There are only two A/B
cycles per cell and no confidence-bound claim.

The largest apparent rate improvement is `5/Q0/memory/iterator/2`: B/A 5.232,
with cycle ratios 1.246, 21.972
and an A/A control of 1.694. This outlier is retained
rather than filtered; it is not presented as an established performance gain.
The median cell rate ratio is 0.868.

63/96 cells have observed delivered-rate ratios below 0.95.
The five lowest ratios, including their neutral controls, are:

| Protocol/QoS/store/mode/burst | A delivered/s | B delivered/s | B/A | A/A |
| --- | ---: | ---: | ---: | ---: |
| 5/Q0/sqlite/callback/long | 45,205 | 15,558 | 0.338 | 0.838 |
| 5/Q0/memory/callback/long | 49,026 | 17,278 | 0.355 | 0.981 |
| 311/Q0/memory/callback/long | 32,416 | 13,338 | 0.418 | 0.986 |
| 311/Q0/memory/callback/1 | 4,984 | 2,316 | 0.458 | 0.975 |
| 5/Q0/memory/iterator/long | 34,561 | 15,265 | 0.461 | 0.990 |

### Group summaries

Every ratio is B/A. Higher is better for rate; lower is better for latency,
CPU and memory. Each following row combines both protocols and all four
burst patterns (eight cells). Traced allocations and RSS measure different
lifetimes and must not be added together.

| QoS | Store | Mode | Rate | p50 | p95 | p99 | CPU/message | Python peak | RSS peak |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | memory | callback | 0.607 | 1.119 | 1.587 | 1.043 | 1.964 | 1.939 | 0.984 |
| 0 | memory | iterator | 0.867 | 0.893 | 0.912 | 0.851 | 1.439 | 1.961 | 0.984 |
| 0 | sqlite | callback | 0.766 | 1.163 | 0.962 | 1.068 | 1.816 | 1.855 | 0.990 |
| 0 | sqlite | iterator | 0.919 | 0.889 | 0.779 | 0.786 | 1.396 | 1.974 | 0.985 |
| 1 | memory | callback | 0.783 | 0.596 | 0.697 | 0.672 | 1.295 | 1.451 | 0.992 |
| 1 | memory | iterator | 0.779 | 0.555 | 0.712 | 0.636 | 1.255 | 1.362 | 0.993 |
| 1 | sqlite | callback | 0.818 | 0.547 | 0.626 | 0.591 | 1.213 | 1.013 | 0.990 |
| 1 | sqlite | iterator | 0.856 | 0.514 | 0.564 | 0.600 | 1.191 | 0.993 | 0.991 |
| 2 | memory | callback | 0.973 | 0.455 | 0.465 | 0.471 | 1.052 | 1.235 | 0.992 |
| 2 | memory | iterator | 0.946 | 0.466 | 0.491 | 0.549 | 1.058 | 1.235 | 0.993 |
| 2 | sqlite | callback | 0.974 | 0.447 | 0.473 | 0.548 | 1.037 | 0.922 | 0.990 |
| 2 | sqlite | iterator | 0.991 | 0.440 | 0.491 | 0.547 | 1.020 | 0.942 | 0.990 |

Burst-only summaries pool all protocol/QoS/store/mode combinations:

| Burst | Rate | p95 | CPU/message | Python peak | RSS peak |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 0.793 | 1.460 | 1.254 | 2.600 | 0.994 |
| 2 | 1.032 | 1.050 | 1.168 | 2.261 | 0.994 |
| 8 | 0.957 | 1.026 | 1.199 | 1.780 | 0.994 |
| long | 0.664 | 0.137 | 1.541 | 0.318 | 0.976 |

Long batches show lower measured residence latency and traced allocation
peaks alongside lower delivered throughput. Small bursts instead show
higher traced allocation peaks. A long-batch element has no timestamp
until the iterable produces it: waiting before that point is excluded
from its latency but included in total elapsed time. Lower per-element
latency here therefore does not mean that the whole batch finishes sooner.
The smaller progressive window is consistent with that trade-off, but
this combined experiment does not isolate the cost of each removed path.

### Every measured cell

A/s and B/s are median delivered messages/s. Other columns are paired
B/A ratios; the last column is the same-code rate control. Sample count
is identical for both revisions within a cell. Raw artifacts include
all absolute latency/CPU/memory measurements and high-water counters.

| Protocol/QoS/store/mode/burst | N | A/s | B/s | Rate | p95 | CPU/msg | Python peak | RSS peak | A/A rate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 311/Q0/memory/iterator/1 | 1248 | 4,661 | 2,739 | 0.583 | 2.665 | 1.584 | 4.472 | 0.994 | 1.504 |
| 311/Q0/memory/iterator/2 | 128 | 178 | 134 | 0.654 | 1.953 | 1.654 | 3.259 | 0.995 | 1.463 |
| 311/Q0/memory/iterator/8 | 128 | 190 | 187 | 0.993 | 1.006 | 1.238 | 2.243 | 0.995 | 1.023 |
| 311/Q0/memory/iterator/long | 6744 | 31,722 | 16,612 | 0.539 | 0.368 | 1.762 | 0.671 | 0.955 | 1.004 |
| 311/Q0/memory/callback/1 | 1192 | 4,984 | 2,316 | 0.458 | 4.302 | 1.980 | 4.623 | 0.994 | 0.975 |
| 311/Q0/memory/callback/2 | 128 | 365 | 262 | 0.766 | 4.529 | 1.609 | 3.026 | 0.995 | 2.831 |
| 311/Q0/memory/callback/8 | 128 | 187 | 186 | 0.995 | 0.962 | 1.578 | 1.968 | 0.995 | 0.977 |
| 311/Q0/memory/callback/long | 5224 | 32,416 | 13,338 | 0.418 | 0.696 | 3.134 | 0.758 | 0.950 | 0.986 |
| 311/Q0/sqlite/iterator/1 | 808 | 4,197 | 3,182 | 0.756 | 1.391 | 1.384 | 4.325 | 0.995 | 1.050 |
| 311/Q0/sqlite/iterator/2 | 128 | 192 | 148 | 0.838 | 1.010 | 1.331 | 3.110 | 0.996 | 0.764 |
| 311/Q0/sqlite/iterator/8 | 128 | 189 | 188 | 0.994 | 1.013 | 1.326 | 2.175 | 0.994 | 0.997 |
| 311/Q0/sqlite/iterator/long | 6224 | 30,165 | 14,747 | 0.477 | 0.436 | 1.929 | 0.815 | 0.955 | 1.003 |
| 311/Q0/sqlite/callback/1 | 768 | 5,069 | 2,565 | 0.561 | 2.727 | 1.884 | 4.532 | 0.995 | 1.088 |
| 311/Q0/sqlite/callback/2 | 128 | 254 | 262 | 1.218 | 0.532 | 1.333 | 2.923 | 0.995 | 0.648 |
| 311/Q0/sqlite/callback/8 | 128 | 190 | 188 | 0.984 | 1.032 | 1.660 | 1.933 | 0.995 | 0.994 |
| 311/Q0/sqlite/callback/long | 2048 | 23,069 | 13,893 | 0.605 | 1.309 | 2.861 | 0.699 | 0.991 | 0.994 |
| 311/Q1/memory/iterator/1 | 424 | 2,498 | 2,052 | 0.736 | 1.865 | 1.291 | 3.983 | 0.996 | 0.902 |
| 311/Q1/memory/iterator/2 | 584 | 3,182 | 2,647 | 0.836 | 1.323 | 1.189 | 3.336 | 0.994 | 0.936 |
| 311/Q1/memory/iterator/8 | 904 | 5,943 | 5,120 | 0.864 | 1.217 | 1.201 | 2.264 | 0.995 | 1.212 |
| 311/Q1/memory/iterator/long | 2136 | 10,066 | 6,148 | 0.623 | 0.108 | 1.528 | 0.162 | 0.986 | 1.119 |
| 311/Q1/memory/callback/1 | 432 | 2,382 | 1,870 | 0.851 | 1.557 | 1.225 | 4.548 | 0.994 | 0.912 |
| 311/Q1/memory/callback/2 | 688 | 3,959 | 2,467 | 0.643 | 2.419 | 1.480 | 3.679 | 0.995 | 1.041 |
| 311/Q1/memory/callback/8 | 848 | 6,181 | 4,498 | 0.731 | 1.361 | 1.386 | 2.358 | 0.995 | 0.926 |
| 311/Q1/memory/callback/long | 2048 | 7,931 | 6,461 | 0.839 | 0.062 | 1.217 | 0.167 | 0.986 | 0.906 |
| 311/Q1/sqlite/iterator/1 | 528 | 1,693 | 1,389 | 0.886 | 1.298 | 1.121 | 1.782 | 0.996 | 1.009 |
| 311/Q1/sqlite/iterator/2 | 544 | 2,041 | 1,927 | 0.898 | 1.170 | 1.154 | 1.709 | 0.992 | 0.566 |
| 311/Q1/sqlite/iterator/8 | 800 | 3,845 | 3,123 | 0.804 | 1.205 | 1.271 | 1.565 | 0.994 | 1.050 |
| 311/Q1/sqlite/iterator/long | 2048 | 5,740 | 4,190 | 0.737 | 0.060 | 1.323 | 0.215 | 0.982 | 1.010 |
| 311/Q1/sqlite/callback/1 | 352 | 1,889 | 1,653 | 0.944 | 1.250 | 1.111 | 1.835 | 0.994 | 1.157 |
| 311/Q1/sqlite/callback/2 | 376 | 1,907 | 1,807 | 0.875 | 1.098 | 1.189 | 1.757 | 0.992 | 1.200 |
| 311/Q1/sqlite/callback/8 | 656 | 4,439 | 3,619 | 0.810 | 1.490 | 1.233 | 1.577 | 0.994 | 0.786 |
| 311/Q1/sqlite/callback/long | 2048 | 5,672 | 4,400 | 0.803 | 0.053 | 1.239 | 0.215 | 0.982 | 0.989 |
| 311/Q2/memory/iterator/1 | 168 | 1,243 | 1,272 | 0.990 | 1.009 | 1.012 | 3.238 | 0.994 | 1.034 |
| 311/Q2/memory/iterator/2 | 456 | 2,366 | 2,282 | 0.957 | 1.039 | 1.016 | 2.789 | 0.995 | 1.190 |
| 311/Q2/memory/iterator/8 | 776 | 4,048 | 4,203 | 1.037 | 0.831 | 0.981 | 1.914 | 0.995 | 1.075 |
| 311/Q2/memory/iterator/long | 2048 | 6,367 | 5,493 | 0.848 | 0.064 | 1.175 | 0.194 | 0.987 | 1.007 |
| 311/Q2/memory/callback/1 | 168 | 1,009 | 1,323 | 1.386 | 0.436 | 0.873 | 3.191 | 0.995 | 1.050 |
| 311/Q2/memory/callback/2 | 528 | 1,664 | 1,677 | 0.911 | 0.923 | 1.083 | 2.681 | 0.995 | 0.996 |
| 311/Q2/memory/callback/8 | 568 | 2,327 | 2,983 | 1.223 | 0.737 | 0.931 | 1.840 | 0.995 | 1.042 |
| 311/Q2/memory/callback/long | 2048 | 5,833 | 4,770 | 0.802 | 0.072 | 1.177 | 0.197 | 0.986 | 1.166 |
| 311/Q2/sqlite/iterator/1 | 128 | 666 | 618 | 0.939 | 1.184 | 1.070 | 1.539 | 0.993 | 1.020 |
| 311/Q2/sqlite/iterator/2 | 216 | 1,108 | 1,132 | 0.970 | 1.106 | 1.037 | 1.650 | 0.993 | 1.209 |
| 311/Q2/sqlite/iterator/8 | 416 | 1,842 | 2,004 | 1.098 | 0.788 | 0.933 | 1.525 | 0.993 | 1.052 |
| 311/Q2/sqlite/iterator/long | 2048 | 2,647 | 2,413 | 0.906 | 0.071 | 1.076 | 0.216 | 0.983 | 1.062 |
| 311/Q2/sqlite/callback/1 | 128 | 583 | 725 | 1.147 | 0.730 | 0.914 | 1.524 | 0.993 | 0.986 |
| 311/Q2/sqlite/callback/2 | 136 | 1,256 | 1,252 | 0.983 | 0.840 | 1.051 | 1.520 | 0.992 | 1.034 |
| 311/Q2/sqlite/callback/8 | 464 | 1,986 | 1,747 | 0.874 | 1.200 | 1.116 | 1.495 | 0.994 | 1.023 |
| 311/Q2/sqlite/callback/long | 2048 | 2,627 | 2,147 | 0.811 | 0.075 | 1.188 | 0.217 | 0.983 | 0.998 |
| 5/Q0/memory/iterator/1 | 776 | 5,328 | 3,478 | 0.647 | 1.631 | 1.469 | 2.884 | 0.994 | 0.932 |
| 5/Q0/memory/iterator/2 | 128 | 92 | 548 | 5.232 | 0.323 | 0.784 | 2.295 | 0.994 | 1.694 |
| 5/Q0/memory/iterator/8 | 128 | 189 | 188 | 1.001 | 0.987 | 1.324 | 1.905 | 0.995 | 0.996 |
| 5/Q0/memory/iterator/long | 6120 | 34,561 | 15,265 | 0.461 | 0.478 | 2.110 | 0.790 | 0.953 | 0.990 |
| 5/Q0/memory/callback/1 | 1208 | 6,053 | 3,793 | 0.574 | 1.935 | 1.736 | 2.917 | 0.994 | 0.821 |
| 5/Q0/memory/callback/2 | 1472 | 295 | 190 | 0.624 | 2.696 | 1.708 | 2.434 | 0.994 | 0.664 |
| 5/Q0/memory/callback/8 | 128 | 192 | 191 | 0.992 | 1.003 | 1.546 | 1.668 | 0.995 | 0.988 |
| 5/Q0/memory/callback/long | 8192 | 49,026 | 17,278 | 0.355 | 0.591 | 3.062 | 0.809 | 0.954 | 0.981 |
| 5/Q0/sqlite/iterator/1 | 560 | 6,120 | 4,241 | 0.715 | 1.451 | 1.400 | 2.741 | 0.995 | 1.068 |
| 5/Q0/sqlite/iterator/2 | 128 | 89 | 222 | 4.768 | 0.312 | 0.831 | 2.235 | 0.995 | 1.600 |
| 5/Q0/sqlite/iterator/8 | 128 | 191 | 190 | 0.994 | 0.920 | 1.366 | 1.823 | 0.995 | 0.999 |
| 5/Q0/sqlite/iterator/long | 6072 | 33,216 | 16,957 | 0.499 | 0.522 | 1.925 | 0.867 | 0.954 | 1.012 |
| 5/Q0/sqlite/callback/1 | 1312 | 5,963 | 3,232 | 0.527 | 2.273 | 1.820 | 2.943 | 0.995 | 0.844 |
| 5/Q0/sqlite/callback/2 | 128 | 201 | 310 | 1.653 | 0.249 | 1.192 | 2.097 | 0.995 | 0.277 |
| 5/Q0/sqlite/callback/8 | 128 | 191 | 190 | 0.992 | 0.993 | 1.411 | 1.719 | 0.994 | 0.997 |
| 5/Q0/sqlite/callback/long | 7448 | 45,205 | 15,558 | 0.338 | 0.667 | 3.236 | 0.737 | 0.960 | 0.838 |
| 5/Q1/memory/iterator/1 | 608 | 3,011 | 2,800 | 0.914 | 1.092 | 1.091 | 2.761 | 0.996 | 1.020 |
| 5/Q1/memory/iterator/2 | 648 | 3,638 | 3,020 | 0.712 | 1.907 | 1.278 | 2.486 | 0.994 | 0.734 |
| 5/Q1/memory/iterator/8 | 1120 | 6,830 | 5,863 | 0.839 | 1.272 | 1.189 | 1.957 | 0.995 | 0.985 |
| 5/Q1/memory/iterator/long | 2200 | 11,467 | 8,553 | 0.747 | 0.076 | 1.319 | 0.180 | 0.987 | 0.950 |
| 5/Q1/memory/callback/1 | 528 | 3,118 | 2,302 | 0.753 | 1.906 | 1.263 | 2.976 | 0.994 | 0.899 |
| 5/Q1/memory/callback/2 | 704 | 4,094 | 3,060 | 0.754 | 1.502 | 1.318 | 2.642 | 0.996 | 0.994 |
| 5/Q1/memory/callback/8 | 1528 | 7,006 | 5,605 | 0.883 | 1.121 | 1.205 | 2.015 | 0.995 | 1.117 |
| 5/Q1/memory/callback/long | 2224 | 10,112 | 7,920 | 0.837 | 0.054 | 1.291 | 0.189 | 0.985 | 1.026 |
| 5/Q1/sqlite/iterator/1 | 176 | 2,021 | 1,667 | 0.793 | 1.664 | 1.225 | 1.681 | 0.994 | 1.065 |
| 5/Q1/sqlite/iterator/2 | 488 | 2,061 | 2,023 | 0.961 | 1.075 | 1.092 | 1.594 | 0.993 | 1.066 |
| 5/Q1/sqlite/iterator/8 | 664 | 4,341 | 3,862 | 1.068 | 0.903 | 1.071 | 1.498 | 0.993 | 0.969 |
| 5/Q1/sqlite/iterator/long | 2048 | 6,283 | 4,750 | 0.753 | 0.058 | 1.300 | 0.231 | 0.982 | 0.812 |
| 5/Q1/sqlite/callback/1 | 344 | 1,960 | 1,347 | 0.631 | 2.670 | 1.365 | 1.702 | 0.992 | 0.934 |
| 5/Q1/sqlite/callback/2 | 264 | 2,852 | 2,466 | 0.872 | 1.228 | 1.135 | 1.674 | 0.992 | 1.187 |
| 5/Q1/sqlite/callback/8 | 976 | 3,842 | 3,708 | 0.953 | 0.991 | 1.109 | 1.514 | 0.993 | 0.928 |
| 5/Q1/sqlite/callback/long | 2048 | 6,946 | 4,684 | 0.715 | 0.067 | 1.349 | 0.236 | 0.983 | 1.157 |
| 5/Q2/memory/iterator/1 | 128 | 1,103 | 1,043 | 0.923 | 1.362 | 1.047 | 2.063 | 0.996 | 0.896 |
| 5/Q2/memory/iterator/2 | 392 | 1,800 | 2,024 | 1.006 | 0.699 | 1.040 | 2.213 | 0.995 | 1.006 |
| 5/Q2/memory/iterator/8 | 856 | 4,564 | 4,171 | 0.931 | 1.121 | 1.084 | 1.733 | 0.996 | 1.042 |
| 5/Q2/memory/iterator/long | 2048 | 6,701 | 5,900 | 0.889 | 0.056 | 1.122 | 0.204 | 0.986 | 0.787 |
| 5/Q2/memory/callback/1 | 152 | 1,215 | 1,186 | 0.951 | 1.047 | 1.038 | 2.260 | 0.994 | 1.393 |
| 5/Q2/memory/callback/2 | 408 | 2,357 | 2,208 | 0.914 | 1.201 | 1.085 | 2.159 | 0.995 | 1.008 |
| 5/Q2/memory/callback/8 | 688 | 4,358 | 3,704 | 0.780 | 1.666 | 1.177 | 1.700 | 0.995 | 0.985 |
| 5/Q2/memory/callback/long | 2048 | 6,175 | 5,894 | 0.955 | 0.049 | 1.090 | 0.210 | 0.985 | 0.979 |
| 5/Q2/sqlite/iterator/1 | 128 | 721 | 730 | 0.970 | 1.059 | 1.034 | 1.451 | 0.993 | 1.084 |
| 5/Q2/sqlite/iterator/2 | 224 | 1,196 | 1,108 | 1.083 | 0.933 | 0.978 | 1.554 | 0.992 | 0.989 |
| 5/Q2/sqlite/iterator/8 | 384 | 1,438 | 1,883 | 1.236 | 0.568 | 0.889 | 1.428 | 0.993 | 1.144 |
| 5/Q2/sqlite/iterator/long | 2048 | 2,770 | 2,170 | 0.789 | 0.082 | 1.172 | 0.230 | 0.984 | 0.847 |
| 5/Q2/sqlite/callback/1 | 136 | 716 | 754 | 1.054 | 0.775 | 0.976 | 1.446 | 0.992 | 1.001 |
| 5/Q2/sqlite/callback/2 | 304 | 1,216 | 1,300 | 1.025 | 0.932 | 0.998 | 1.516 | 0.992 | 1.064 |
| 5/Q2/sqlite/callback/8 | 560 | 2,214 | 2,031 | 1.087 | 0.875 | 0.953 | 1.385 | 0.994 | 0.650 |
| 5/Q2/sqlite/callback/long | 2048 | 2,909 | 2,502 | 0.863 | 0.072 | 1.131 | 0.229 | 0.983 | 0.977 |

All 384 candidate comparison processes completed both phases with exact delivery.
The maximum recorded candidate outbound pending high-water count was 20
(configured flow 20); writer and delivery byte high-water marks were
74,205 and 72,960 bytes.
These measured peaks are scenario observations, not proofs of all resource
bounds; the saturation and transaction regression tests provide separate
functional evidence.


## Reproduction and artifacts

With the two exact revisions checked out and a local anonymous Mosquitto
listener on port 11883, run from the candidate checkout:

```bash
python benchmarks/lean_native_compare.py \
  --base-root /home/yoch/mqttium \
  --candidate-root /tmp/mqttium-lean-native-experiment \
  --output /tmp/mqttium-lean-comparison.json --cpu 4
```

Select an available CPU on another host and record that difference. Results are
machine-specific. Use `benchmarks/runner_probe.py` before comparison; an eligible
idle host and longer repeated samples are needed for precise performance claims.
No optimization should be attributed to one removed mechanism from this combined
change alone.

Run interval: `2026-09-10T01:28:58.206517+00:00` to
`2026-09-10T01:58:40.849987+00:00` (UTC).

- Raw results: `/tmp/mqttium-lean-comparison.json`.
- Raw results SHA-256: `879c1a0077238c9604552c8d554a6d21cac547ca39b716cd2e563deb056f39ea`.
- Harness SHA-256: `873010acc6bbe88524fd708683cd125cbcafca2a65ef180dc1d01c1169378f7f`.
- Preflight: `/tmp/mqttium-lean-runner-final.json`.
- Functional log: `/tmp/lean-all-tests-verified.txt`.
- Deterministic fuzz log: `/tmp/lean-fuzz-final.txt`.
- Documentation log: `/tmp/lean-docs-report.txt`.


Raw benchmark and coverage/build outputs remain outside version control.
Historical report bodies were not changed. The archive index links this report
as the evidence for this incompatible branch, not a replacement release policy.
