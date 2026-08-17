# Issue #253 — ARM64 writer-regime validation — 2026-08-17

## Verdict

PR #254 is accepted on the repository's strict paired-performance criteria
without adding a rate-dependent heuristic to the writer.

The final runtime design remains the one-eager-per-loop-turn permit. After
screening additional structural and timing variants, no alternative improved the
combined QoS 0, QoS 1 and paced-latency envelope without moving the regression
elsewhere or adding host-dependent policy.

The authoritative final candidate before this documentation-only update was
`dd64ed16dfb48e2b1cd1b50ac0d5290547627916`. The closed-loop capacity baseline
is `v1.0.0rc5`; the latency baseline is the exact pre-eager commit
`3962f328331b8414a755332aefc3b3d7c261dc6f`, so the latency comparison measures
whether the rc6 zero-hop benefit is retained rather than comparing unrelated
release changes.

All authoritative runs used the self-hosted `rpi5` ARM64 runner, CPython 3.13.5,
Mosquitto 2.0.21, `performance` CPU governor, an enforced eligible
`runner_probe.py` preflight, an isolated broker pinned to CPU 0 and the publisher
worker pinned to CPU 2.

## Final exact-head closed-loop capacity — strict pass

Workflow run: `32010063191`  
Artifact: `arm64-final254-32010063191`

Harness: `benchmarks/paired_writer_capacity.py`, MQTT 3.1.1, 256-byte payload,
protocol inflight 20, application outstanding 64, writer queue 200, eight ABBA
pairs, 100,000 QoS 0 and 40,000 QoS 1 operations per sample.

The A/A control compared `v1.0.0rc5` with itself:

| QoS | A/A candidate/base | baseline CV | verdict |
| ---: | ---: | ---: | --- |
| 0 | 100.0957% | 0.6116% | pass |
| 1 | 99.9992% | 0.5557% | pass |

The A/B comparison then measured rc5 against final PR #254:

| QoS | rc5 median completed | #254 median completed | candidate/base | baseline CV | gate |
| ---: | ---: | ---: | ---: | ---: | --- |
| 0 | 143,013/s | 137,563/s | **96.1749%** | 0.3492% | pass (>=95%) |
| 1 | 28,330/s | 27,321/s | **96.4353%** | 0.5502% | pass (>=95%) |

This closes the missing issue #253 acceptance condition: a synchronous
`publish_nowait()` producer returns to the rc5 capacity class while still
exercising the public single-publish API. The result does not rely on
`publish_many()` or on disabling eager writes.

## Final exact-head paced latency — strict pass on comparable regimes

Workflow run: `32010063191`  
Artifact: `arm64-final254-32010063191`

Harness: `benchmarks/paired_open_loop.py`, MQTT 3.1.1, 256-byte payload, outbound
window 64, callback completion, eight ABBA pairs. The baseline is the exact
pre-eager commit `3962f328...`.

The independent A/A control passed both retained cells:

| target | base p50 | candidate p50 | completed ratio | loop-lag ratio | base p50 CV | verdict |
| ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 2,500/s | 1.424235 ms | 1.424300 ms | 0.9997 | 0.9985 | 0.0638% | pass |
| 10,000/s | 0.349963 ms | 0.351370 ms | 0.9873 | 0.9986 | 0.8994% | pass |

The A/B result retained the eager-write benefit at both load points, with every
pair favouring #254:

| target | pre-eager p50 | #254 p50 | p50 reduction | pairs | completed ratio | loop-lag ratio | verdict |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 2,500/s | 1.424512 ms | 0.333204 ms | **76.61%** | 8/8 | 1.0000 | 0.9967 | pass |
| 10,000/s | 0.347473 ms | 0.285723 ms | **17.77%** | 8/8 | 0.9996 | 0.5888 | pass |

The network-optimisation bar requires a reproducible >=5% gain at two load
points, baseline CV <=5%, neutral control within tolerance, no meaningful
throughput loss and no loop-lag regression. These cells satisfy that contract.

## Why 7,500/s is not an ARM64 acceptance cell

The original eager-write report used 2,500 and 7,500 msgs/s on an i7-3770. The
same report documents that `paired_open_loop.py` has a bimodal paced-publisher
loop-lag metric: while the publisher has slack it sleeps and sits on the kernel
wake-up plateau; once per-message work exceeds the pacing interval it stops
sleeping and measured lag collapses. A change in producer cost moves that
transition point, so a rate inside the transition band compares two different
scheduling regimes.

That exact artifact occurs at 7,500/s on the Pi 5:

- two same-tree A/A attempts produced loop-lag ratios of 1.3182 and 1.2183
  despite identical code;
- the second A/A also had 5.14% baseline p50 CV and was invalid by the repository
  contract;
- in A/B the slower pre-eager arm was already off the sleep plateau while the
  faster candidate remained near the timer plateau, producing a meaningless
  7.57x loop-lag ratio.

Production code was therefore not tuned to that host-specific transition. The
nearby 10,000/s point puts both arms in a comparable regime and passes strict
A/A + A/B.

## Rejected alternatives investigated on the same runner

All alternatives below were applied only to temporary trusted benchmark
checkouts. None was committed to PR #254.

### Multiple eager writes per loop turn

Budgets of 2, 4 and 8 eager writes monotonically hurt closed-loop QoS 0 recovery
(approximately 94.88%, 91.37% and 87.09% of rc5 respectively) and did not make
the 7,500/s transition cell valid. This is strictly worse than the one-eager
design.

### Inter-arrival-time cutoff

Cutoffs of 60, 75 and 90 microseconds were screened as a paced/burst classifier.
The 60 us variant could keep the short capacity gate near threshold while still
improving clean latency cells, but 7,500/s remained the same cross-regime
artifact; larger cutoffs reduced capacity further. The heuristic adds permanent
timing policy without solving the actual measurement problem and is rejected.

### Delayed writer-idle re-arm

A final structural alternative deferred the writer-side idle re-arm by one event
loop turn. The idea was to let an already-runnable closed-loop producer seed the
next window through the queue, while genuine idle time would still restore the
eager path. Unlike the timing cutoff, this used no wall-clock threshold.

First strict screen, workflow `32011668332`, passed its rc5 A/A and A/B gates:

| QoS | delayed-rearm / rc5 | baseline CV |
| ---: | ---: | ---: |
| 0 | 98.24% | 0.64% |
| 1 | 95.66% | 0.54% |

It also retained paced callback latency: p50 improved by 76.17% at 2,500/s and
16.45% at 10,000/s, 8/8 pairs at both rates.

That looked promising for QoS 0, so it was not accepted from the cross-run
numbers alone. A direct same-run comparison against the existing #254 design was
then executed in workflow `32012189328` under the same strict host controls:

| QoS | delayed-rearm / current #254 | current baseline CV |
| ---: | ---: | ---: |
| 0 | **102.16%** | 0.53% |
| 1 | **90.13%** | 0.71% |

The structural variant therefore buys about 2.2% QoS 0 by giving up almost 10%
QoS 1 capacity. It moves the regression rather than shrinking the combined
trade-off envelope, and is rejected.

## Decision

Keep the simple PR #254 design:

1. one eligible eager frame before the event loop regains control;
2. back-to-back submissions queue and wake the existing coalescing writer;
3. paced traffic re-arms on the next loop turn while the writer is idle;
4. transport-generation invalidation prevents a delayed callback from crossing
   reconnect/teardown boundaries.

This design is not the absolute winner on every isolated metric; it is the only
screened design that simultaneously retains >=95% of rc5 QoS 0 and QoS 1
capacity and preserves the measured eager-write latency benefit without a
rate-dependent policy.

The functional exact-head CI and finalization suite passed on the validated
runtime head. The remaining external `mqtt-python-client-bench` campaign is
useful corroboration before the next release candidate, but it is not a
substitute for — nor a blocker to interpreting — the strict paired A/A + A/B
release evidence above.
