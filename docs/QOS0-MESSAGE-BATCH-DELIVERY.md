# Batched small QoS 0 delivery

## Problem

The reader already decodes inbound packets and schedules their effects in bounded
lots. With the default isolated callback contract, each eligible QoS 0 `MESSAGE`
effect was nevertheless transferred to the iterator/callback queues through a
separate async dispatch call inside that scheduled flush.

The cost is downstream of MQTT decoding and distinct from the direct MQTT 3.1.1
QoS 0/QoS 1 decoders. It only applies when messages are delivered through the
bounded isolated queues.

## Change

`EffectPump` may consume a consecutive prefix of at least two eligible QoS 0
`MESSAGE` effects in one synchronous pump pass. The optimization is deliberately
narrow:

- a single message keeps the established per-effect path;
- QoS 1 and QoS 2 are never batched here;
- large, property-bearing or exact-byte-accounted messages keep the existing
  awaited path;
- callback and iterator capacity are checked before either destination mutates;
- `message_delivery="both"` remains atomic;
- the batch stops at the first ineligible effect;
- callback execution remains isolated in the callback worker;
- the scheduled flush boundary and reader backpressure remain unchanged.

## Evidence

Seven rotated cycles were run on the final native hot-path stack containing the
exact `publish_nowait()` admission calculation and the MQTT 3.1.1 QoS 0/QoS 1
direct decoders.

| Delivery contract | Direct ingress | Broker-fed ingress |
| --- | ---: | ---: |
| Default isolated callback | **+11.45%** | **+9.34%** |
| Experimental synchronous inline callback | +0.52% | +1.93% |

Every isolated-callback cycle was positive: direct ratios ranged from 1.079 to
1.125 and broker-fed ratios from 1.060 to 1.151. Inline-callback results crossed
both sides of neutral, confirming that this patch specifically amortizes the
isolated queue transfer rather than decoding or callback execution itself.

Final interaction run:
<https://github.com/yoch/mqttium/actions/runs/31062402168>

## Risks

The principal risks are effect ordering, partial `both` delivery and bypassing
backpressure. Tests cover the consecutive-prefix rule, stale epochs, single-item
fallback, acknowledged-message fallback and atomic destination-capacity checks.
The full unit and fuzz suites are required on the final branch.
