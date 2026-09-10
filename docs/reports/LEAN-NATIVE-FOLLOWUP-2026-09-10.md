# Lean-native follow-up — 2026-09-10

The final native-only candidate passes 1,860 local tests and multi-version CI,
and removes 3,236 Python source lines (18.8%) relative to main. The complete
96-cell controlled-TCP comparison measures a descriptive geometric mean of
**10.2% less delivered throughput**, **11.3% more CPU per message**, and
**17.3% lower peak traced Python allocations**. Most of the throughput cost is
in long lots; QoS 0 callback long-lot throughput remains 56–61% below main.

This follow-up fixes callback lifecycle and SQLite validation, then measures
four local optimizations separately. They recover overhead without restoring
the removed scheduling machinery, but do not make the simplified architecture
a general performance improvement. This is an incompatible experiment, not a
merge or release decision.

## Exact source and retained scope

- Original main A: `9ad1f01857306ac5079ffb1d073a59fdb60e1931`.
- Final measured B: `3899139051e40c8e30162e00e9fecd69f00517a3`.
- B runtime tree (`src/mqttium`): `ef9aa2ac1e47448b4ebe3519ba49909b2eddd3e4`.
- PR: [#457](https://github.com/yoch/mqttium/pull/457), branch
  `codex/lean-native-experiment`. The final report commit changes documentation
  only; all measurements identify their actual source commits.
- Runtime dependencies remain empty. Main, release tags and version remain
  unchanged. The primary checkout's pre-existing untracked audit is preserved.

The two exclusive delivery modes, permanently frozen message routes, immutable
properties/configuration, progressive batch receipts and schema-5-only SQLite
remain. This follow-up does not restore Paho, compatibility views, callback
fan-out, inline user callbacks, small-message accounting, or a second scheduler.
The existing pre-admission effect drain remains: old receipts must settle before
an acknowledged packet identifier can be reused, including inside a batch.

## Functional corrections and complexity

1. SQLite fixtures close every created connection, including failure paths.
   This addresses the Python 3.13/3.14 resource warnings seen in earlier CI.
2. Reopening from the active callback retires old queued jobs, clears its pending
   stop, and lets the same worker serve the replacement connection. An active
   job retains its byte credit until completion. Automatic reconnect continues
   to preserve its existing queued deliveries.
3. An `on_disconnect` that raises `CancelledError` without actual task
   cancellation is reported and isolated. Cancellation of the owner still
   propagates; disconnect failure cannot silently skip terminal cleanup or
   automatic reconnect.
4. SQLite validates the actual connection in one coherent read transaction,
   then ends that transaction before journal setup. A fresh database is checked
   again under its initialization write lock. Manual database/WAL copying and
   the final-close race are removed. A refused format preserves committed schema
   and data; normal SQLite recovery/checkpointing may change physical files.
   No stronger byte-for-byte file promise is imposed.

The SQLite tests cover a large database without copies, live and crash-left WAL,
final-writer close at several validation boundaries, historical/future/partial
schemas, partial-DDL rollback, and two real competing initializers. Callback
regressions cover both protocols, all QoS levels, normal/eager task factories,
terminal/failed reconnect, cancellation and connection takeover.

Physical lines below count tracked Python source, including comments and blanks.
They are a structural indicator, not a maintenance-cost proof.

| Measure | Original main | Final candidate | Change |
| --- | ---: | ---: | ---: |
| Python source files | 64 | 58 | −6 |
| Python source lines | 17,186 | 13,950 | −3,236 (−18.8%) |
| `AsyncClient` methods | 119 | 75 | −44 |
| Private client methods | 93 | 53 | −40 |
| Client adapter lines | 2,982 | 2,285 | −697 |
| Application delivery lines | 1,036 | 338 | −698 |
| SQLite store lines | 1,041 | 928 | −113 |

### Alternatives inspected

[#454](https://github.com/yoch/mqttium/pull/454), inspected at
`636de564dc2c92822a8361050cc24e369fe027d4`, contains the same narrow reopen fix.
Its live sync-to-async route handoff and front-of-queue transfer are unnecessary
with this experiment's permanently frozen routes and worker-only callbacks.

[#456](https://github.com/yoch/mqttium/pull/456), inspected at
`b8a70b09a4202278d3c5811be2e7f6b375e716e2`, evaluates a first-inline callback with
its tail pre-admitted to the worker. Its description also identifies the earlier
qualified `b0440f5a8570665da3d919a9e263eee416beae90`. This design adds cancellation
ownership and changes the task in which the first callback runs. Those costs are
not needed to recover the local optimizations here. Its historical Pi timings
do not qualify this candidate or its corrected source. No code from that
scheduler experiment was imported.

## Four isolated optimizations

Each stage changes only the named path. All stages use the identical harness,
eight cells (MQTT 5 × QoS 0/1 × iterator/callback × unit/long, memory), two A/A
ABBA cycles and three A/B ABBA cycles, fresh processes and a 0.5-second timing
calibration target. Each process also runs a separate allocation phase. All
samples in each completed stage are retained; no B/A-based retries or A/A
subtraction.

| Stage | Baseline → candidate | Mechanism |
| --- | --- | --- |
| 1 | `03eb8b1` → `23c6372` | Reserve bytes and enqueue directly when both bounds have space; retain one deadline for the waiting path |
| 2 | `23c6372` → `32483bc` | Apply ready MESSAGE effects directly when no durable mark is required; callbacks stay on the worker |
| 3 | `32483bc` → `b0bc010` | Classify the frozen message callback/each route once; preserve original callable identity in error reporting |
| 4 | `b0bc010` → `3899139` | Hand ready unit QoS 0 publications to the existing writer after validation, receipt creation and capacity checks |

The fourth stage's runtime change is `c14ea0c`; `3899139` adds tests/docs only.
A clean asynchronous writer refusal uses ordinary admission. A writer exception
is never retried because bytes may already have been handed off. Topic Alias
state changes only after acceptance. Batches and QoS 1/2 retain the general path.

Ratios below are B/A geometric means of complete ABBA-cycle ratios, then across
cells with equal cell weights. Higher delivered throughput is better; lower CPU, latency and memory is
better. Cross-cell summaries are descriptive and do not prove every cell wins.

| Stage | Cells | Rate B/A | CPU/msg B/A | p95 B/A | Python peak B/A |
| --- | --- | --- | --- | --- | --- |
| 1 | All 8 | 1.105 | 0.906 | 0.890 | 0.584 |
| 2 | All 8 | 1.120 | 0.925 | 1.162 | 0.994 |
| 3 | All 8 | 1.032 | 0.975 | 0.955 | 0.987 |
| 3 | Callback 4 | 1.056 | 0.952 | 0.940 | 0.986 |
| 4 | All 8 | 1.032 | 0.973 | 0.991 | 1.002 |
| 4 | Unit QoS 0 (2) | 1.157 | 0.884 | 0.891 | 0.981 |


Stage 2 has a real latency trade-off in these samples. For long QoS 0 lots,
iterator p95 rises from 38.38 to 80.27 ms (ratio 2.083) and callback p95 from
42.56 to 99.39 ms (2.302), while delivered throughput improves by 18.2% and
12.4%. Long QoS 1 p95 rises by about 14–15%. The same-source controls do not
explain the repeated QoS 0 shift. The immediate path changes scheduling and
queue residence in this saturated combined producer/consumer workload; it is
not a universal latency improvement. Existing explicit fairness yields and
resource bounds remain. The final original-main comparison below must be read
separately from this incremental-stage comparison.

### Complete staged measurements

All rows use MQTT 5 and memory. `C` is callback, `I` iterator; `L` is long.

| Stage | QoS/mode/burst | Rate | CPU/msg | p95 | Python peak | AA rate | AA A CV |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 0/I/1 | 1.060 | 0.941 | 0.963 | 0.424 | 1.053 | 3.1% |
| 1 | 0/I/L | 1.118 | 0.866 | 0.876 | 0.942 | 0.974 | 3.8% |
| 1 | 0/C/1 | 1.085 | 0.926 | 0.903 | 0.411 | 0.987 | 1.9% |
| 1 | 0/C/L | 1.181 | 0.828 | 0.849 | 0.885 | 0.979 | 1.2% |
| 1 | 1/I/1 | 1.066 | 0.954 | 0.920 | 0.426 | 0.984 | 2.4% |
| 1 | 1/I/L | 1.127 | 0.896 | 0.850 | 0.714 | 1.009 | 6.3% |
| 1 | 1/C/1 | 1.114 | 0.933 | 0.858 | 0.431 | 1.026 | 4.6% |
| 1 | 1/C/L | 1.095 | 0.913 | 0.905 | 0.713 | 1.025 | 3.5% |
| 2 | 0/I/1 | 1.228 | 0.821 | 0.855 | 0.847 | 0.946 | 5.0% |
| 2 | 0/I/L | 1.182 | 0.959 | 2.083 | 1.071 | 1.009 | 2.1% |
| 2 | 0/C/1 | 1.230 | 0.823 | 0.856 | 0.849 | 0.980 | 2.1% |
| 2 | 0/C/L | 1.124 | 1.005 | 2.302 | 1.138 | 0.961 | 2.5% |
| 2 | 1/I/1 | 1.107 | 0.912 | 0.834 | 0.907 | 0.985 | 5.5% |
| 2 | 1/I/L | 1.026 | 0.975 | 1.150 | 1.215 | 1.001 | 1.4% |
| 2 | 1/C/1 | 1.074 | 0.930 | 0.866 | 0.909 | 1.007 | 3.6% |
| 2 | 1/C/L | 1.012 | 0.992 | 1.143 | 1.083 | 0.976 | 4.2% |
| 3 | 0/I/1 | 0.985 | 1.020 | 1.024 | 1.000 | 1.035 | 4.8% |
| 3 | 0/I/L | 0.988 | 1.012 | 1.016 | 1.000 | 0.996 | 2.5% |
| 3 | 0/C/1 | 1.066 | 0.959 | 0.943 | 0.990 | 0.985 | 4.1% |
| 3 | 0/C/L | 1.047 | 0.942 | 0.935 | 0.981 | 1.000 | 0.9% |
| 3 | 1/I/1 | 1.011 | 0.991 | 0.997 | 1.000 | 0.946 | 2.8% |
| 3 | 1/I/L | 1.050 | 0.970 | 0.859 | 0.957 | 1.011 | 1.8% |
| 3 | 1/C/1 | 1.060 | 0.955 | 0.933 | 1.000 | 0.973 | 2.3% |
| 3 | 1/C/L | 1.052 | 0.952 | 0.948 | 0.972 | 1.010 | 0.7% |
| 4 | 0/I/1 | 1.134 | 0.887 | 0.887 | 0.981 | 1.070 | 14.0% |
| 4 | 0/I/L | 1.002 | 0.997 | 0.994 | 1.020 | 1.020 | 4.0% |
| 4 | 0/C/1 | 1.180 | 0.880 | 0.896 | 0.981 | 1.025 | 2.4% |
| 4 | 0/C/L | 1.015 | 0.984 | 1.007 | 1.019 | 0.979 | 3.4% |
| 4 | 1/I/1 | 0.996 | 1.003 | 0.994 | 1.001 | 0.948 | 1.3% |
| 4 | 1/I/L | 0.988 | 1.014 | 1.054 | 1.076 | 1.015 | 5.1% |
| 4 | 1/C/1 | 0.981 | 1.009 | 1.025 | 1.001 | 1.087 | 13.7% |
| 4 | 1/C/L | 0.978 | 1.023 | 1.092 | 0.946 | 1.003 | 1.6% |

## Functional qualification

Local CPython 3.12.13 checks use warnings as errors, strict pytest configuration
and the ordinary 89% coverage threshold.

| Check | Result |
| --- | --- |
| Unit + project tests | **1,697 passed**, 43.23 s |
| Line + branch coverage for that selection | **91.60%**, threshold unchanged |
| Mandatory real-Mosquitto integration + resilience + all fuzz test modules | **163 passed**, no skips, 77.91 s |
| Deterministic codec/engine/WebSocket fuzz, seeds 1/2/3 | **180,000 total cases**, zero crashes or invariant violations |
| Ruff formatting/lint | Passed, 290 Python files |
| mypy | Passed, 58 source files |
| Bandit | Passed |
| Strict documentation build | Passed |

The first complete local optimized-suite run had 1,695 passes and two failures:
a stats fixture expected deferred work from a now-immediate message, and a wakeup
fixture requested a durable mark on a message with no packet identifier. Both
fixtures now use actual QoS 1 identifiers and require durable marks, preserving
the original deferred-path assertions. No runtime change was made to hide these
failures. The full suite above was rerun after the corrections. Original logs
are retained alongside the passing run.

Exact-source GitHub qualification for `3899139` completed successfully:

- [CI 34454613656](https://github.com/yoch/mqttium/actions/runs/34454613656):
  Python 3.11–3.14, macOS/Windows, quality, resilience, fuzz, package, coverage
  and the required aggregate check all pass.
- [Soak 34454613715](https://github.com/yoch/mqttium/actions/runs/34454613715):
  both Linux protocol jobs pass. Scheduled/macOS/interoperability jobs that do
  not run on this event are skipped by workflow policy.
- [Distributions 34454613760](https://github.com/yoch/mqttium/actions/runs/34454613760):
  the single build and all installed-artifact smoke jobs pass. Publishing and
  PyPI verification are skipped; no release was performed.

## Method and limits of the complete comparison

The final matrix compares original main with the optimized experiment, not the
fourth stage alone: 2 protocols × 3 QoS × 2 stores × 2 delivery modes × 4 bursts
= 96 cells. Payload is 256 bytes, flow window 20, with the same client queue and
byte limits in both arms. A self-subscribed client combines native publication
and reception through a temporary Mosquitto on loopback, with
`set_tcp_nodelay true` (port 11884). This differs from the earlier stage runs and
the original diagnostic report, which used the existing broker on port 11883.
It removes broker-side Nagle buffering from the final stimulus, rather than
changing the client or hiding a source regression. This is not a cross-client product
comparison, a pure receive benchmark, or an external fixed-rate workload.

Each cell has a baseline pilot, 2 A/A ABBA cycles and 3 A/B ABBA cycles. The pilot
calibrates a 0.5-second target with the unchanged 128-message floor (2,048 for a
long lot) and 8,192-message ceiling. Actual durations can exceed the target,
particularly small QoS 0 bursts. Timing excludes allocation tracing; every fresh
worker runs a separate traced phase. All complete observations are retained.

Latency starts when each element is constructed immediately before admission;
it includes residence in the client, transport and broker queues. Long-lot
throughput covers waiting before an element is pulled from the iterable as well.
Consequently a progressive batch can show lower per-message latency and lower
whole-lot throughput at the same time: work not yet pulled from the iterable has
not started its per-message clock. This is not latency from a fixed external
arrival schedule. The final inbound acknowledgement tail is included. Every phase verifies
payload, sequence, delivery count, publication receipts and empty protocol state.
CPU is the combined client process only; broker CPU is not included. Traced
Python peak is measured after warmup and excludes pre-existing allocations,
SQLite native allocations and kernel buffers. It is not total application
memory. Peak RSS includes interpreter/module overhead and is diagnostic.

The same-source A/A ratios and raw-arm coefficients of variation (CV) expose
host noise. They are reported rather than subtracted from A/B. Only three A/B
cycle units are available per cell; these descriptive ratios do not establish
simultaneous equivalence of 96 throughput/latency distributions or a strict tail
latency non-regression guarantee. Candidate-side A/A is not a separate phase.
No production performance gate or universal gain is claimed. After completion,
a separate checker verified all 96 unique cells, all ABBA orders, 1,920 complete
workers and their 3,840 measurement phases, then independently reproduced every
reported aggregate ratio from raw observations. The harness and both runtime
source trees were unchanged throughout the completed campaign.

The matrix does not measure large payloads, rich MQTT 5 properties, topic-filter
fan-out, aliases, reconnect/replay throughput, manual acknowledgement or TLS/
WebSocket/Unix transport performance. Those features have separate functional
coverage. The QoS 0 store selection mostly controls for construction/runtime
configuration: it does not turn QoS 0 into a persistent message workload.

## Complete matrix results

Run interval: `2026-09-10T08:35:35.718309+00:00` to `2026-09-10T09:30:05.658269+00:00` (UTC).

| Cells | n | Rate | CPU/msg | p50 | p95 | p99 | Python peak | RSS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| All 96 | 96 | 0.898 | 1.113 | 0.628 | 0.661 | 0.733 | 0.827 | 0.990 |
| QoS 0 | 32 | 0.785 | 1.272 | 1.118 | 1.155 | 1.163 | 0.982 | 0.985 |
| QoS 1 | 32 | 0.930 | 1.073 | 0.509 | 0.512 | 0.639 | 0.634 | 0.992 |
| QoS 2 | 32 | 0.992 | 1.010 | 0.436 | 0.488 | 0.529 | 0.910 | 0.992 |
| Burst 1 | 24 | 0.964 | 1.041 | 1.071 | 1.078 | 1.084 | 1.068 | 0.995 |
| Burst 2 | 24 | 0.977 | 1.019 | 1.060 | 1.048 | 1.058 | 1.154 | 0.995 |
| Burst 8 | 24 | 0.973 | 1.032 | 1.054 | 1.051 | 1.029 | 1.128 | 0.995 |
| Burst long | 24 | 0.709 | 1.400 | 0.130 | 0.161 | 0.244 | 0.337 | 0.975 |
| iterator | 48 | 0.929 | 1.073 | 0.594 | 0.624 | 0.710 | 0.825 | 0.990 |
| callback | 48 | 0.868 | 1.154 | 0.665 | 0.700 | 0.756 | 0.830 | 0.990 |
| memory | 48 | 0.902 | 1.108 | 0.633 | 0.655 | 0.670 | 0.835 | 0.990 |
| sqlite | 48 | 0.894 | 1.118 | 0.624 | 0.666 | 0.801 | 0.820 | 0.989 |


A/A rate ratios range from **0.761 to 1.082**; median absolute
departure from 1 is **1.63%**.
Raw baseline A/A rate CV has median **2.99%** and maximum
**11.00%**; **18/96** cells exceed 5% CV.
The earlier report's diagnostic median CV was 10.4%; these are independently
calibrated runs with different broker TCP settings, not matched observations
for a cross-run speedup estimate.


A/A does not capture every later disturbance: baseline A-arm CV during A/B
has median **3.42%**, maximum **35.84%**,
and exceeds 5% in **23/96** cells. Both CVs are shown
per cell. A cycle-ratio geometric mean can differ from the quotient of the
raw-arm medians; inspect the three stored cycle ratios for volatile cells.


The complete matrix contains 1,920 measured fresh workers, plus 96 pilots.
Their timing phases verify **6,539,360 messages** and their separate
allocation phases verify **3,461,120 messages**, excluding warmup and pilots.
No completed worker reports a payload, sequence, receipt or protocol-state failure.


Across the 72 short-burst cells, rate B/A is **0.972**;
the 24 long-lot cells average **0.709**.
Lower long-lot latency largely reflects progressive admission and less read-ahead;
it does not mean an entire lot finishes sooner. QoS 0 long callbacks retain a
large throughput cost. These results support an explicit simplicity/performance
trade-off, not replacing main on a claim of universal speed improvement.


### All 96 cells

Each numerical metric is B/A. `I` = iterator, `C` = callback; `M` = memory,
`S` = SQLite; `L` = long. The two protocol values are MQTT 3.1.1 and MQTT 5.

| Protocol/QoS/store/mode/burst | Count | Rate | CPU/msg | p50 | p95 | p99 | Python peak | RSS | AA rate | AA A CV | AB A CV |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 311/0/M/I/1 | 3240 | 1.040 | 0.954 | 0.978 | 0.956 | 0.990 | 1.056 | 0.995 | 0.975 | 2.1% | 2.1% |
| 311/0/M/I/2 | 4336 | 1.049 | 0.954 | 0.962 | 0.958 | 0.952 | 1.052 | 0.995 | 1.046 | 7.7% | 3.3% |
| 311/0/M/I/8 | 5216 | 1.012 | 0.990 | 0.999 | 1.009 | 0.994 | 1.044 | 0.996 | 1.007 | 2.6% | 4.9% |
| 311/0/M/I/L | 8192 | 0.576 | 1.737 | 0.864 | 1.046 | 1.060 | 0.875 | 0.953 | 0.954 | 6.8% | 2.1% |
| 311/0/M/C/1 | 3424 | 0.830 | 1.201 | 1.357 | 1.181 | 1.150 | 1.049 | 0.996 | 0.964 | 1.3% | 7.1% |
| 311/0/M/C/2 | 3816 | 0.877 | 1.140 | 1.224 | 1.169 | 1.148 | 0.955 | 0.994 | 1.010 | 2.0% | 1.9% |
| 311/0/M/C/8 | 5384 | 0.879 | 1.137 | 1.176 | 1.212 | 1.226 | 0.895 | 0.995 | 1.018 | 1.4% | 2.9% |
| 311/0/M/C/L | 8192 | 0.438 | 2.285 | 1.217 | 1.637 | 1.675 | 0.892 | 0.956 | 0.997 | 5.4% | 2.0% |
| 311/0/S/I/1 | 2504 | 1.018 | 0.976 | 0.968 | 1.002 | 0.997 | 1.056 | 0.995 | 0.979 | 2.9% | 3.2% |
| 311/0/S/I/2 | 2976 | 1.034 | 0.967 | 0.975 | 0.939 | 0.939 | 1.050 | 0.995 | 0.964 | 1.9% | 2.4% |
| 311/0/S/I/8 | 7120 | 1.044 | 0.970 | 0.984 | 0.894 | 0.894 | 1.050 | 0.996 | 1.016 | 3.4% | 6.2% |
| 311/0/S/I/L | 8192 | 0.577 | 1.733 | 0.933 | 1.121 | 1.151 | 0.875 | 0.957 | 1.082 | 11.0% | 4.1% |
| 311/0/S/C/1 | 3880 | 0.790 | 1.249 | 1.408 | 1.273 | 1.260 | 1.064 | 0.997 | 1.007 | 1.1% | 1.4% |
| 311/0/S/C/2 | 4424 | 0.887 | 1.128 | 1.198 | 1.170 | 1.165 | 0.956 | 0.996 | 1.077 | 9.2% | 2.6% |
| 311/0/S/C/8 | 6088 | 0.872 | 1.147 | 1.193 | 1.158 | 1.149 | 0.937 | 0.996 | 1.032 | 5.4% | 0.9% |
| 311/0/S/C/L | 8192 | 0.408 | 2.450 | 1.300 | 1.761 | 1.807 | 0.892 | 0.950 | 1.063 | 6.8% | 2.7% |
| 311/1/M/I/1 | 2328 | 1.056 | 0.948 | 0.909 | 0.972 | 0.986 | 1.067 | 0.995 | 0.983 | 2.4% | 5.7% |
| 311/1/M/I/2 | 2128 | 1.034 | 0.970 | 0.971 | 0.991 | 1.003 | 1.049 | 0.995 | 0.964 | 1.7% | 4.3% |
| 311/1/M/I/8 | 3880 | 1.001 | 0.999 | 0.985 | 1.055 | 1.056 | 1.079 | 0.995 | 0.992 | 5.4% | 4.1% |
| 311/1/M/I/L | 6400 | 0.833 | 1.200 | 0.054 | 0.060 | 0.069 | 0.110 | 0.989 | 0.940 | 3.3% | 4.6% |
| 311/1/M/C/1 | 2304 | 0.949 | 1.052 | 1.126 | 1.148 | 1.134 | 1.166 | 0.996 | 0.968 | 2.1% | 2.0% |
| 311/1/M/C/2 | 2256 | 0.966 | 1.034 | 1.240 | 1.251 | 1.225 | 1.119 | 0.995 | 0.991 | 1.8% | 1.8% |
| 311/1/M/C/8 | 3784 | 1.000 | 1.013 | 1.126 | 1.017 | 1.000 | 1.087 | 0.996 | 1.002 | 0.7% | 7.9% |
| 311/1/M/C/L | 6576 | 0.821 | 1.217 | 0.055 | 0.063 | 0.069 | 0.114 | 0.988 | 0.954 | 1.5% | 4.4% |
| 311/1/S/I/1 | 1392 | 0.991 | 1.011 | 1.009 | 1.053 | 1.026 | 1.008 | 0.993 | 0.976 | 2.0% | 2.3% |
| 311/1/S/I/2 | 1544 | 1.027 | 0.995 | 0.994 | 0.963 | 0.910 | 1.015 | 0.993 | 1.038 | 4.0% | 4.3% |
| 311/1/S/I/8 | 2408 | 0.987 | 1.014 | 1.003 | 1.049 | 1.053 | 1.032 | 0.994 | 1.006 | 1.9% | 3.1% |
| 311/1/S/I/L | 3848 | 0.754 | 1.262 | 0.054 | 0.057 | 0.236 | 0.144 | 0.984 | 0.996 | 3.4% | 2.4% |
| 311/1/S/C/1 | 1576 | 0.972 | 1.033 | 1.084 | 1.058 | 1.066 | 1.038 | 0.993 | 0.851 | 3.9% | 1.4% |
| 311/1/S/C/2 | 1472 | 0.994 | 1.018 | 1.106 | 1.102 | 1.194 | 1.044 | 0.994 | 1.031 | 7.0% | 1.6% |
| 311/1/S/C/8 | 2704 | 0.957 | 1.049 | 1.122 | 1.276 | 1.353 | 1.054 | 0.993 | 0.997 | 0.8% | 2.8% |
| 311/1/S/C/L | 4096 | 0.775 | 1.243 | 0.055 | 0.048 | 0.251 | 0.148 | 0.985 | 0.761 | 5.2% | 4.0% |
| 311/2/M/I/1 | 1216 | 1.006 | 0.992 | 0.999 | 1.001 | 1.011 | 1.493 | 0.996 | 0.940 | 2.9% | 3.8% |
| 311/2/M/I/2 | 1456 | 0.993 | 0.999 | 0.994 | 1.065 | 1.108 | 1.196 | 0.995 | 1.009 | 1.9% | 3.9% |
| 311/2/M/I/8 | 2072 | 0.987 | 1.012 | 0.991 | 1.041 | 1.056 | 1.009 | 0.995 | 1.000 | 6.0% | 1.3% |
| 311/2/M/I/L | 3928 | 0.925 | 1.082 | 0.040 | 0.051 | 0.061 | 0.244 | 0.986 | 0.992 | 4.2% | 4.0% |
| 311/2/M/C/1 | 1040 | 1.040 | 0.964 | 0.946 | 0.990 | 0.983 | 1.012 | 0.995 | 1.012 | 2.5% | 6.7% |
| 311/2/M/C/2 | 1456 | 1.046 | 0.966 | 0.990 | 0.893 | 0.838 | 1.177 | 0.995 | 1.035 | 6.0% | 7.9% |
| 311/2/M/C/8 | 2416 | 1.011 | 0.988 | 0.969 | 0.970 | 0.953 | 2.667 | 0.996 | 0.987 | 1.5% | 2.3% |
| 311/2/M/C/L | 2936 | 0.935 | 1.071 | 0.041 | 0.050 | 0.059 | 0.320 | 0.983 | 0.993 | 2.2% | 3.5% |
| 311/2/S/I/1 | 720 | 1.013 | 1.007 | 1.028 | 1.082 | 1.063 | 0.674 | 0.994 | 1.006 | 1.2% | 5.2% |
| 311/2/S/I/2 | 848 | 1.000 | 0.993 | 0.996 | 1.033 | 1.492 | 1.260 | 0.994 | 1.010 | 2.9% | 6.5% |
| 311/2/S/I/8 | 1400 | 1.006 | 1.014 | 1.010 | 1.023 | 1.054 | 0.704 | 0.994 | 1.015 | 2.2% | 3.4% |
| 311/2/S/I/L | 2048 | 0.870 | 1.112 | 0.035 | 0.075 | 0.138 | 0.253 | 0.984 | 1.005 | 4.8% | 3.9% |
| 311/2/S/C/1 | 672 | 1.035 | 0.992 | 1.001 | 1.061 | 1.006 | 1.228 | 0.992 | 1.028 | 4.4% | 8.7% |
| 311/2/S/C/2 | 768 | 0.958 | 0.979 | 0.995 | 0.971 | 0.693 | 1.838 | 0.994 | 0.990 | 4.6% | 35.8% |
| 311/2/S/C/8 | 1288 | 1.022 | 0.978 | 0.974 | 0.963 | 0.760 | 1.429 | 0.993 | 0.909 | 3.4% | 4.8% |
| 311/2/S/C/L | 2048 | 1.050 | 1.044 | 0.032 | 0.066 | 0.113 | 0.307 | 0.982 | 1.004 | 3.7% | 25.1% |
| 5/0/M/I/1 | 3152 | 1.006 | 0.987 | 0.995 | 0.988 | 0.969 | 1.040 | 0.994 | 0.990 | 2.2% | 3.0% |
| 5/0/M/I/2 | 3704 | 1.005 | 0.996 | 1.001 | 0.989 | 0.994 | 1.037 | 0.995 | 1.053 | 7.6% | 5.8% |
| 5/0/M/I/8 | 6400 | 0.960 | 1.043 | 1.060 | 1.063 | 1.035 | 1.058 | 0.995 | 0.948 | 2.4% | 2.9% |
| 5/0/M/I/L | 8192 | 0.558 | 1.793 | 0.979 | 1.082 | 1.092 | 0.927 | 0.957 | 0.983 | 3.2% | 5.1% |
| 5/0/M/C/1 | 4152 | 0.788 | 1.246 | 1.419 | 1.298 | 1.273 | 1.028 | 0.997 | 0.957 | 1.7% | 2.0% |
| 5/0/M/C/2 | 4056 | 0.890 | 1.124 | 1.225 | 1.097 | 1.129 | 0.966 | 0.995 | 1.013 | 0.8% | 1.9% |
| 5/0/M/C/8 | 6912 | 0.851 | 1.176 | 1.229 | 1.273 | 1.314 | 0.912 | 0.994 | 0.990 | 3.7% | 2.9% |
| 5/0/M/C/L | 8192 | 0.394 | 2.535 | 1.270 | 1.632 | 1.649 | 0.944 | 0.952 | 1.024 | 4.1% | 6.1% |
| 5/0/S/I/1 | 3344 | 0.988 | 1.012 | 1.043 | 1.029 | 1.041 | 1.036 | 0.996 | 1.003 | 3.2% | 6.7% |
| 5/0/S/I/2 | 3504 | 0.992 | 1.008 | 1.008 | 1.063 | 1.033 | 1.037 | 0.995 | 1.013 | 2.5% | 1.3% |
| 5/0/S/I/8 | 6880 | 0.917 | 1.090 | 1.091 | 1.147 | 1.183 | 1.050 | 0.996 | 1.011 | 3.1% | 2.4% |
| 5/0/S/I/L | 8192 | 0.538 | 1.860 | 0.964 | 1.063 | 1.076 | 0.927 | 0.952 | 0.919 | 1.9% | 2.3% |
| 5/0/S/C/1 | 3032 | 0.825 | 1.193 | 1.287 | 1.293 | 1.366 | 1.049 | 0.996 | 0.990 | 1.4% | 5.4% |
| 5/0/S/C/2 | 4104 | 0.864 | 1.157 | 1.240 | 1.196 | 1.178 | 0.965 | 0.995 | 1.002 | 0.9% | 2.0% |
| 5/0/S/C/8 | 7464 | 0.874 | 1.145 | 1.191 | 1.125 | 1.199 | 0.888 | 0.995 | 0.925 | 1.9% | 4.6% |
| 5/0/S/C/L | 8192 | 0.396 | 2.521 | 1.378 | 1.715 | 1.751 | 0.944 | 0.953 | 0.995 | 1.9% | 5.1% |
| 5/1/M/I/1 | 2144 | 0.971 | 1.024 | 1.011 | 1.058 | 1.071 | 1.043 | 0.996 | 1.009 | 2.1% | 3.4% |
| 5/1/M/I/2 | 2504 | 0.981 | 1.019 | 1.016 | 1.057 | 1.013 | 1.040 | 0.995 | 1.030 | 7.1% | 2.4% |
| 5/1/M/I/8 | 3416 | 0.978 | 1.023 | 1.021 | 1.044 | 1.044 | 1.051 | 0.996 | 1.021 | 2.2% | 4.5% |
| 5/1/M/I/L | 7080 | 0.807 | 1.240 | 0.058 | 0.056 | 0.066 | 0.129 | 0.991 | 0.948 | 1.3% | 2.0% |
| 5/1/M/C/1 | 2296 | 0.934 | 1.068 | 1.146 | 1.152 | 1.158 | 1.096 | 0.996 | 1.046 | 4.6% | 1.7% |
| 5/1/M/C/2 | 2136 | 0.958 | 1.045 | 1.268 | 1.163 | 1.148 | 1.086 | 0.996 | 0.991 | 3.8% | 2.9% |
| 5/1/M/C/8 | 4104 | 0.939 | 1.066 | 1.161 | 1.236 | 1.180 | 1.071 | 0.994 | 0.987 | 3.7% | 4.2% |
| 5/1/M/C/L | 6392 | 0.810 | 1.234 | 0.058 | 0.058 | 0.060 | 0.130 | 0.987 | 0.965 | 3.8% | 3.2% |
| 5/1/S/I/1 | 1384 | 0.939 | 1.049 | 1.042 | 1.061 | 1.228 | 1.018 | 0.993 | 1.025 | 5.9% | 1.8% |
| 5/1/S/I/2 | 1624 | 0.982 | 1.022 | 1.018 | 0.999 | 1.143 | 1.020 | 0.993 | 1.012 | 3.8% | 2.8% |
| 5/1/S/I/8 | 2656 | 0.957 | 1.027 | 1.033 | 1.021 | 1.078 | 1.045 | 0.993 | 1.008 | 1.9% | 3.0% |
| 5/1/S/I/L | 4184 | 0.789 | 1.242 | 0.054 | 0.043 | 0.216 | 0.163 | 0.985 | 0.995 | 4.0% | 4.7% |
| 5/1/S/C/1 | 1528 | 0.968 | 1.036 | 1.083 | 1.097 | 1.137 | 1.039 | 0.993 | 1.014 | 3.2% | 3.5% |
| 5/1/S/C/2 | 1672 | 0.928 | 1.048 | 1.104 | 1.211 | 1.244 | 1.037 | 0.993 | 1.013 | 4.0% | 1.1% |
| 5/1/S/C/8 | 2536 | 1.095 | 1.001 | 1.099 | 1.076 | 1.078 | 1.045 | 0.994 | 0.984 | 3.7% | 19.4% |
| 5/1/S/C/L | 4336 | 0.755 | 1.262 | 0.055 | 0.049 | 0.247 | 0.171 | 0.985 | 1.020 | 0.9% | 1.7% |
| 5/2/M/I/1 | 1272 | 0.980 | 1.018 | 1.044 | 0.993 | 0.993 | 1.037 | 0.995 | 1.023 | 1.5% | 1.1% |
| 5/2/M/I/2 | 1152 | 1.065 | 0.939 | 0.971 | 0.917 | 0.898 | 1.254 | 0.995 | 1.038 | 1.9% | 7.7% |
| 5/2/M/I/8 | 2480 | 1.034 | 0.978 | 0.985 | 0.880 | 0.919 | 2.174 | 0.996 | 1.006 | 4.0% | 8.6% |
| 5/2/M/I/L | 3936 | 0.954 | 1.048 | 0.039 | 0.049 | 0.057 | 0.318 | 0.986 | 0.969 | 0.5% | 5.0% |
| 5/2/M/C/1 | 1224 | 1.006 | 0.993 | 0.985 | 0.992 | 0.974 | 1.343 | 0.996 | 0.977 | 2.7% | 4.3% |
| 5/2/M/C/2 | 1464 | 0.985 | 1.005 | 1.010 | 1.067 | 1.069 | 1.981 | 0.995 | 0.969 | 0.9% | 3.2% |
| 5/2/M/C/8 | 2368 | 1.008 | 0.994 | 0.979 | 0.995 | 1.025 | 1.336 | 0.996 | 1.003 | 1.1% | 5.9% |
| 5/2/M/C/L | 3704 | 0.945 | 1.058 | 0.039 | 0.049 | 0.060 | 0.327 | 0.985 | 1.018 | 7.7% | 5.1% |
| 5/2/S/I/1 | 656 | 0.980 | 1.026 | 1.013 | 1.124 | 1.033 | 1.016 | 0.993 | 1.008 | 1.1% | 2.2% |
| 5/2/S/I/2 | 832 | 0.996 | 0.980 | 1.000 | 0.958 | 1.138 | 2.553 | 0.994 | 1.035 | 5.0% | 3.8% |
| 5/2/S/I/8 | 1344 | 0.999 | 0.977 | 0.994 | 0.915 | 1.071 | 2.117 | 0.993 | 0.995 | 1.8% | 3.5% |
| 5/2/S/I/L | 2048 | 0.928 | 1.032 | 0.033 | 0.052 | 0.127 | 0.496 | 0.984 | 0.998 | 4.3% | 2.7% |
| 5/2/S/C/1 | 704 | 1.095 | 1.033 | 1.015 | 1.111 | 1.219 | 1.207 | 0.994 | 1.022 | 4.5% | 23.3% |
| 5/2/S/C/2 | 752 | 0.980 | 1.018 | 1.043 | 1.045 | 1.022 | 0.997 | 0.993 | 1.011 | 10.0% | 2.5% |
| 5/2/S/C/8 | 1336 | 1.013 | 0.983 | 0.986 | 0.918 | 0.474 | 0.829 | 0.993 | 0.893 | 3.1% | 2.8% |
| 5/2/S/C/L | 2048 | 0.920 | 1.050 | 0.033 | 0.071 | 0.110 | 0.261 | 0.984 | 0.996 | 4.0% | 4.1% |

## Runner, reproducibility and retained evidence

The host is Linux x86-64, Intel Core i7-3770 (8 logical CPUs), CPython 3.12.13,
Mosquitto 2.0.18, with the existing `performance` governor. Each client worker is
pinned to logical CPU 4; the broker is unpinned. No heavy tests or other workloads
from this task ran concurrently with measurements. The desktop and unrelated
host activity remain possible sources of noise.

The first attempt at the complete matrix used the original broker, whose small
QoS 0 bursts switch between sub-millisecond delivery and roughly 40-ms pauses.
In its completed MQTT 3.1.1/memory/iterator/burst-2 cell, identical-source A/A
rate ratios are 1.500 and 0.252, with raw A-arm CV 90.6%. The A/B rate ratio
0.279 cannot be interpreted as a precise source effect under that variation.
A short pilot then selected 4,472 messages for the callback burst-2 cell, making
later stalled workers disproportionately long. The campaign was interrupted
there because the calibration/control behavior was unsuitable. Its five fully
saved cells and interruption log remain in `lean-followup-default-tcp-attempt.*`;
the incomplete sixth cell did not produce a persisted cell record and is not
included in any aggregate. This is an explicit incomplete attempt, not a
completed 96-cell default-broker result.

The accepted final preflight records three consecutive eligible samples: its
last sample is 10.9% CPU, load/CPU 0.091 and 60°C, within the unchanged 20%,
0.25 and 80°C limits. An immediate post-campaign probe records 19.1% CPU and
67°C, but load/CPU 0.258, just above the 0.25 limit; that moving average still
includes recently completed benchmark work. Both snapshots are retained. This
is not a claim that the entire session stayed inside every eligibility limit.

The entire 96-cell campaign was then restarted with an owned temporary broker
using `set_tcp_nodelay true`, leaving the existing broker and both client sources
unchanged. Both broker configurations and the temporary broker log are retained. The
original `/tmp/mosq.conf` contains only its loopback listener, anonymous access
and disabled persistence; it does not override the default TCP setting. Mosquitto's installed
2.0.18 configuration manual documents this setting as disabling Nagle on client
sockets, with a possible increase in packet count. The observed pause pattern is
consistent with TCP buffering/delayed acknowledgements; it was not established
by a packet capture. The reported A/A controls characterize how much variation
remains under the changed stimulus. Neither attempt is silently pooled with the
other, and no B/A-based retry is used within the final campaign.

Earlier stage-4 preflights failed the one-minute-load limit; their observations
are retained. A stage-4 launch was stopped before a complete cell during
requalification, then the full stage ran after an eligible preflight. The
accepted stage-4 initial sample was 14.1% CPU, load/CPU 0.220 and 69°C; its A/A
table still contains noisy cells. A passing preflight is not a guarantee of
noise-free measurements throughout a run.

Compared with the initial experiment, this campaign doubles the A/A cycle count,
increases A/B cycles from two to three, and targets 0.5 seconds instead of 0.2.
The unchanged harness SHA256 is
`873010acc6bbe88524fd708683cd125cbcafca2a65ef180dc1d01c1169378f7f`.
Absolute timings across the two campaigns are not paired and must not be used to
attribute a cross-run speedup solely to code.

Reproduce from clean source checkouts at the exact main and candidate commits,
with a separate broker configured below and the documented pip development tools:

```text
listener 11884 127.0.0.1
allow_anonymous true
persistence false
set_tcp_nodelay true
```

```bash
python benchmarks/runner_probe.py \
  --output /tmp/followup-runner.json --wait-seconds 45 \
  --poll-seconds 3 --consecutive-eligible 3 --enforce
python benchmarks/lean_native_compare.py \
  --base-root /path/to/exact-main --candidate-root /path/to/exact-candidate \
  --output /tmp/followup-matrix.json --port 11884 \
  --protocols 311,5 --qos-values 0,1,2 --stores memory,sqlite \
  --modes iterator,callback --bursts 1,2,8,long \
  --cycles 3 --aa-cycles 2 --target-seconds 0.5 --cpu 4
```

The CPU choice is specific to this host; record any replacement. Reproduce a
stage using its table's two source commits and restrict protocols to `5`, stores
to `memory`, QoS to `0,1` and bursts to `1,long` with the other options unchanged.
The stages use the original broker on port 11883 without the TCP_NODELAY override.
Do not alter either source checkout or run competing qualification tests while
the comparison is active.

Raw JSON, logs, runner probes, exact-source CI status and checksums are retained
outside Git in `/tmp/lean-followup-evidence/`, with the archive
`/tmp/lean-followup-evidence.tar.gz` supplied alongside this report. They are local
artifacts, not a claim of a publicly hosted CI benchmark artifact. The manifest
identifies each file; generated raw data is not committed to the repository.

### Raw-data fingerprints

| File | SHA256 |
| --- | --- |
| `lean-followup-step1.json` | `428f169c681c29786a6cdd3b0b390ac64bd8d2fa8abc580640e5cded6210d944` |
| `lean-followup-step2.json` | `6fe05840e3351a0558e4d7945c1e06759cc28338f6ee729b56660d224ce8482e` |
| `lean-followup-step3.json` | `b62c7f8cfdaa08d9df315339e3c3beafe742b92d1bdd96d3e493421ff15735df` |
| `lean-followup-step4.json` | `26944196b3c155cefdffbf56baabf1cc4f420bfa43d4607cd44731ff8a78bcaa` |
| `lean-followup-final-matrix.json` | `34b991b80b2ae39c659f2361f3b81cf8846568ad8d6224133dbdecf8e6781f13` |
| `lean-followup-default-tcp-attempt.json` | `071d4814ac6a4b6701f8318ab28eea9e0825b802d49c0c963abaeef809bebd5d` |
