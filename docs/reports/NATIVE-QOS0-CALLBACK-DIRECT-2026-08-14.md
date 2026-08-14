# Native QoS 0 direct callback admission — 2026-08-14

## Finding

Native QoS 0 publishing already encoded and admitted a packet directly to the
bounded writer when no `on_publish` callback was installed. Installing the
callback forced the same successful writer admission through SEND and
PUBLISH_COMPLETE effects, an EffectPump task wake-up, and then the callback
queue even though MQTT QoS 0 completion means that local admission itself.

This was scheduler overhead, not network or protocol latency. User callback
execution was already isolated in `ApplicationDelivery` and remains so.

## Change

The direct path now snapshots `on_publish`, preflights bounded callback capacity,
admits the encoded packet to `WritePump`, and enqueues the completion job without
yielding. The callback worker remains the only place that invokes user code.

The ordering and failure boundaries are explicit:

- writer refusal raises before a callback is enqueued;
- insufficient callback capacity sends the entire operation through the
  existing ordered EffectPump path;
- a QoS 0 batch preflights capacity for every completion before admitting any
  direct write, so fallback cannot split the batch;
- mixed-QoS batches and pending effects keep the established path;
- callback completion still means writer admission, not socket drain or broker
  receipt.

## Independent measurement

The baseline was frozen from merged `main` at `44b9614`. Eleven alternating,
source-isolated target pairs were pinned to CPU 3 after
`runner_probe --enforce` accepted the host. Each measured 128,000 native
`publish_nowait()` calls and drained the callback worker in bounded batches.

| Target metric | Callback path |
| --- | ---: |
| Median candidate/base throughput | **1.9474** |
| Pair range | 1.6697–2.0135 |
| Baseline CV | 2.41% |
| Candidate CV | 5.33% |
| Pairs favouring candidate | 11/11 |

The complete range and the candidate dispersion are retained rather than
discarding the slow observation. The contract invalidates a microbenchmark when
the baseline CV exceeds 5%; here it is 2.41%, every pair favours the candidate,
and the 8-of-11 acceptance requirement is exceeded.

A seven-pair no-callback timing control had a 0.9825 median ratio, within the
±2% guard, with baseline/candidate CVs of 1.35%/3.48%. Later repetitions were
affected by periodic host interference, so the exact call/allocation profile —
the benchmarking contract's preferred neutral control for callback handoff —
was also compared:

| Exact profile | Baseline | Candidate |
| --- | ---: | ---: |
| Calls per operation, no callback | 42.0994 | 42.0994 |
| Primitive calls per operation, no callback | 42.0858 | 42.0858 |
| Tracemalloc peak, no callback | 30,120 B | 30,248 B |
| Calls per operation, callback | 119.4479 | 73.2041 |
| Tracemalloc peak, callback | 45,608 B | 32,104 B |

The no-callback control therefore adds neither calls nor per-operation
allocation work, while the callback path removes about 46 Python calls per
publication and lowers the isolated allocation peak.

The scenario isolates library admission and callback handoff. It uses no broker,
external adapter, or external benchmark result and is not an end-to-end latency
claim.

## Validation

- 1,021 unit tests passed with 88.96% statement coverage;
- 15 Mosquitto integration tests executed and passed, with no skips;
- all 15 isolated memory scenarios remained within their checked thresholds;
- Ruff formatting and lint, mypy, Bandit, and `git diff --check` passed;
- focused saturation, refusal, callback-isolation, MQTT 5, compatibility and
  mixed/batched publish tests passed.

## Decision

Retain the change. It nearly doubles the isolated callback-admission rate while
preserving callback isolation, queue bounds, ordering, batch atomicity, and the
existing QoS 0 completion contract. The no-callback path remains neutral within
the benchmarking guard.
