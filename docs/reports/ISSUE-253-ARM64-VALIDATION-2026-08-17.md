# Issue #253 — ARM64 writer-regime validation — 2026-08-17

## Verdict

The fix in PR #254 is accepted on the repository's strict paired-performance
criteria without adding a rate-dependent heuristic to the writer.

The exact candidate tested was `5008d69a889d9100f7f3fb2623e6b08a72126524`.
The closed-loop capacity comparison used `v1.0.0rc5` as the historical floor;
the latency comparison used the exact pre-eager commit
`3962f328331b8414a755332aefc3b3d7c261dc6f` so it measures whether the rc6
zero-hop latency benefit is retained rather than comparing against an unrelated
release delta.

All authoritative runs used the self-hosted `rpi5` ARM64 runner, CPython 3.13.5,
Mosquitto 2.0.21, `performance` CPU governor, an enforced eligible
`runner_probe.py` preflight, an isolated broker pinned to CPU 0 and the publisher
worker pinned to CPU 2.

## Closed-loop capacity — strict pass

Workflow run: `32007634918`  
Artifact: `arm64-writer-capacity-32007634918`

Harness: `benchmarks/paired_writer_capacity.py`, MQTT 3.1.1, 256-byte payload,
protocol inflight 20, application outstanding 64, writer queue 200, eight ABBA
pairs, 100,000 QoS 0 and 40,000 QoS 1 operations per sample.

The A/A control compared `v1.0.0rc5` with itself:

| QoS | A/A candidate/base | baseline CV | verdict |
| ---: | ---: | ---: | --- |
| 0 | 99.9846% | 0.3940% | pass |
| 1 | 99.8583% | 0.3172% | pass |

The A/B comparison then measured rc5 against PR #254:

| QoS | rc5 median completed | #254 median completed | candidate/base | baseline CV | gate |
| ---: | ---: | ---: | ---: | ---: | --- |
| 0 | 142,456/s | 136,321/s | **95.5973%** | 1.2209% | pass (>=95%) |
| 1 | 28,330/s | 27,394/s | **96.7034%** | 1.0488% | pass (>=95%) |

This is the missing evidence from issue #253: a synchronous `publish_nowait()`
producer again reaches the rc5 capacity class while still exercising the public
single-publish API. The result does not rely on `publish_many()` or on disabling
eager writes.

## Paced latency — strict pass on comparable regimes

Workflow run: `32009481302`  
Artifact: `arm64-exact254-latency-32009481302`

Harness: `benchmarks/paired_open_loop.py`, MQTT 3.1.1, 256-byte payload, outbound
window 64, callback completion, eight ABBA pairs. The baseline is the exact
pre-eager commit `3962f328...`; the candidate is the exact PR #254 commit.

The independent A/A control passed both retained cells:

| target | base p50 | candidate p50 | completed ratio | loop-lag ratio | base p50 CV | verdict |
| ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 2,500/s | 1.424670 ms | 1.424587 ms | 0.9979 | 1.0011 | 0.0722% | pass |
| 10,000/s | 0.353565 ms | 0.349565 ms | 0.9998 | 1.0458 | 0.7561% | pass |

The A/B result retained the eager-write benefit at both load points, with every
pair favouring the candidate:

| target | pre-eager p50 | #254 p50 | p50 reduction | pairs | completed ratio | loop-lag ratio | verdict |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 2,500/s | 1.425082 ms | 0.333671 ms | **76.59%** | 8/8 | 1.0075 | 0.9947 | pass |
| 10,000/s | 0.354426 ms | 0.286657 ms | **19.12%** | 8/8 | 1.0130 | 0.6084 | pass |

The network-optimisation bar requires a reproducible >=5% gain at two load
points, baseline CV <=5%, neutral control within tolerance, no throughput loss
and no loop-lag regression. These two cells satisfy that contract.

## Why 7,500/s is not used as an ARM64 acceptance cell

The original eager-write report used 2,500 and 7,500 msgs/s on an i7-3770. The
same report documents that `paired_open_loop.py` has a bimodal paced-publisher
loop-lag metric: while the publisher has slack it sleeps and sits on the kernel
wake-up plateau; once per-message work exceeds the pacing interval it stops
sleeping and the measured lag collapses. A change in producer cost moves that
transition point, so a rate inside the transition band compares two different
scheduling regimes.

That exact artifact occurs at 7,500/s on the Pi 5:

- two same-tree A/A attempts at 7,500/s produced loop-lag ratios of 1.3182 and
  1.2183 despite identical code;
- the second A/A also had 5.14% baseline p50 CV and was therefore invalid by the
  repository contract;
- in the exact A/B run, the slower pre-eager arm was already off the sleep
  plateau (roughly 0.08-0.11 ms absolute loop lag) while the faster candidate
  remained on the timer plateau (roughly 1.055 ms), producing a meaningless
  7.57x loop-lag ratio.

The apparent p50 reversal at that rate therefore cannot be treated as a clean
same-regime latency comparison. The correct response is not to tune production
code to one runner's timer-transition band: choose nearby load points where both
arms are on the same side of the transition. On this host, 2,500 and 10,000/s
both pass A/A and A/B strictly.

## Rejected alternatives investigated on the same runner

Two families of experimental changes were screened in temporary benchmark
checkouts only; neither was committed to PR #254.

### Multiple eager writes per loop turn

Budgets of 2, 4 and 8 eager writes were tested. Increasing the budget monotonically
hurt the closed-loop QoS 0 recovery (approximately 94.88%, 91.37% and 87.09% of
rc5 respectively) and did not make the 7,500/s transition cell valid. This is
strictly worse than the one-eager design.

### Inter-arrival-time cutoff

Cutoffs of 60, 75 and 90 microseconds were tested as a way to classify paced
versus closed-loop traffic. The 60 us variant could keep the short capacity gate
near threshold and still improved clean latency cells, but 7,500/s remained in
the same cross-regime artifact. The larger cutoffs reduced capacity further.
The heuristic therefore adds permanent timing policy without solving the actual
measurement problem and is rejected.

## Decision

Keep the simple PR #254 design:

1. one eligible eager frame before the event loop regains control;
2. back-to-back submissions queue and wake the existing coalescing writer;
3. paced traffic can re-arm on the next loop turn while the writer is idle;
4. transport-generation invalidation prevents a delayed re-arm callback from
   crossing reconnect/teardown boundaries.

The functional exact-head CI and soak suite had already passed before these
performance runs. The remaining external `mqtt-python-client-bench` campaign is
useful corroboration before the next release candidate, but it is not a
substitute for — nor a blocker to interpreting — the strict paired A/A + A/B
release evidence above.
