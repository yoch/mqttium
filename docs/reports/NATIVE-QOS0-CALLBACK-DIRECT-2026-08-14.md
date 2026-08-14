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

The baseline was frozen from merged `main` at `44b9614`. Seven alternating,
source-isolated pairs were pinned to CPU 3 after `runner_probe --enforce`
accepted the host. Each measured 128,000 native `publish_nowait()` calls and
drained the callback worker in bounded batches.

| Metric | Callback path | No-callback control |
| --- | ---: | ---: |
| Median candidate/base throughput | **1.9218** | 0.9825 |
| Pair range | 1.7517–1.9625 | 0.8911–1.0262 |
| Baseline CV | 1.13% | 1.35% |
| Candidate CV | 3.63% | 3.48% |
| Pairs favouring candidate | 7/7 | 2/7 |

One candidate observation in each scenario was disturbed in the same position;
it is retained in the reported range and CV rather than discarded. Both CVs
remain below 5%, the target result stays positive in every pair, and the neutral
median remains within the ±2% guard.

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
