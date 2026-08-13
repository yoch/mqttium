# Batch fresh automatic QoS 1 MESSAGE delivery with QoS 0

Date: 2026-08-13

Base commit: `a7f18a7` (current main after #204).

## Question

Can the existing small QoS 0 MESSAGE queue-transfer batch also cover fresh
automatic QoS 1 delivery, and can that path avoid the delivery-mark store write,
without weakening restart or persisted QoS semantics?

## Review finding

The original #205 inferred persistence from `message.qos` plus the current
`manual_ack` setting. That is not sufficient. A durable QoS 1 row created while
manual acknowledgement was enabled can be replayed later by a client currently
using automatic acknowledgement. Such a MESSAGE still needs `delivered=True`.
Treating every current auto-QoS1 message as non-persisted would make that replay
eligible for the fast batch and skip the delivery mark, allowing repeat delivery
on a later restart.

The original patch also moved manual-QoS1 and QoS2 delivery marks into the
synchronous batch path without measured end-to-end value for those persisted
cases. SQLite stores are synchronous, so preserving the established per-effect
path avoids widening event-loop critical work unnecessarily.

## Reviewed change

`InboundSession` now tags each MESSAGE effect with whether application acceptance
must mark a persisted inbound row. The runtime consumes that fact directly:

- QoS 0 and fresh automatic QoS 1 have no delivery mark and may batch;
- manual QoS 1, QoS 2, and persisted replay MESSAGEs require a mark and retain
  the established per-effect path;
- automatic QoS 1 single-message fallback also skips the old absent-row mark;
- `EffectPump.drain_inline` can consume an eligible MESSAGE prefix immediately,
  so the common SEND + auto-QoS1 MESSAGE burst need not spawn
  `mqttium-effect-flush`;
- large/property-bearing messages and queue-full cases keep the awaited path;
- `message_delivery="both"` still checks both capacities before either queue
  mutates, and callbacks remain isolated on the worker.

This makes persistence ownership explicit at the protocol/runtime boundary
instead of reconstructing it from current configuration.

For safety and compatibility, an `EngineEffect(MESSAGE, ...)` constructed without
classification keeps `requires_delivery_mark=None`. Such an unclassified effect
is never batch-eligible; if it has a packet identifier, the runtime preserves the
legacy delivery-mark behavior. Only an authoritative producer setting explicit
`False` opts a MESSAGE into the non-persisted fast path.

## Correctness validation

Focused tests pin batching for QoS 0 and fresh auto-QoS1, persisted-message
fallback, mixed-prefix stopping, atomic `both` delivery, stale epochs, inline
drain without scheduling, the auto-QoS1 absent-row skip, and—critically—a
persisted QoS1 replay under a current auto-ack configuration that still marks
the durable row delivered.

The reviewed rebuild also passed ruff and mypy together with the Receive Maximum
handoff regressions introduced by #204. A final conservative-default correction
was then checked against the complete Python 3.11 unit suite after CI exposed a
legacy direct `EngineEffect(MESSAGE, ...)` construction; the full suite passed
with the tri-state fallback in place.

## Broker-backed performance validation

The acceptance measurement used fresh base/candidate Python processes against a
local Mosquitto broker, a Paho QoS 1 publisher, and MQTTium callback delivery.
Both MQTT connections and the subscription were established before timing. Pair
order alternated to reduce runner drift.

Initial matrix against base `a7f18a7`:

| Scenario | Work per variant | candidate/base median | Positive pairs | Range |
| --- | ---: | ---: | ---: | ---: |
| MQTT 5, QoS 1, 64 B | 18,000 messages | **1.0601 (+6.01%)** | 6/7 | 0.9971–1.1202 |
| MQTT 3.1.1, QoS 1, 64 B | 18,000 messages | 0.9861 (-1.39%) | 0/5 | 0.9049–0.9945 |
| MQTT 5, QoS 1, 4 KiB | 6,000 messages | 1.0177 (+1.77%) | 3/5 | 0.8932–1.0890 |

Because the short MQTT 3.1.1 result was consistently slightly negative despite
being below the material-regression threshold, it was not accepted at face
value. A longer focused run used 40,000 messages per variant and nine rotated
pairs:

- MQTT 3.1.1 / QoS 1 / 64 B: **1.0962 (+9.62%) median**;
- **9/9 pairs** favoured the candidate;
- range **1.0674–1.1365** (+6.74% to +13.65%).

The longer run resolves the short-run anomaly in favour of a clear improvement.
The MQTT 5 small-payload hot path independently shows a material gain, while the
4 KiB point is neutral-to-positive at the median rather than a material
regression. Effect counters also show that the candidate is actually consuming
more MESSAGE effects inline, so the observed gain corresponds to the intended
path change rather than a benchmark no-op.

## Decision

Accept the narrowed change. It removes a no-op delivery mark and scheduler hop
from the fresh automatic QoS 1 hot path, demonstrates material broker-backed
improvement on MQTT 5 and MQTT 3.1.1 small-message traffic, and leaves persisted
QoS 1/QoS 2/replay semantics on their established path.
