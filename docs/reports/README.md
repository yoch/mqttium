# Reports

Dated records of a measurement, an audit or a design decision. Each was true of
the commit it names, on the day it was written.

Read them for rationale — why a hot path is shaped the way it is, what was
measured before a policy was chosen, what a release campaign actually ran.
Never cite one as current behaviour, and do not edit one to match new code: a
superseded report is replaced by a newer report, not rewritten. The maintained
descriptions of current behaviour are the contracts in
[`../`](../README.md).

## Release and quality campaigns

| Date | Report | Records |
| --- | --- | --- |
| 2026-08-10 | [`PERFORMANCE-AUDIT-0.2.0b4.md`](PERFORMANCE-AUDIT-0.2.0b4.md) | Re-reads the cross-client record against `4bdcdb3`, and records the per-message work removed before the `0.2.0b4` tag — with the reasons no throughput figure is attached to it. |
| 2026-08-10 | [`QUALITY-AUDIT-0.2.0b4.md`](QUALITY-AUDIT-0.2.0b4.md) | Frozen quality inventory for the `0.2.0b4` work — module sizes, complexity signals, coverage — at commit `dc13866`. |
| 2026-08-09 | [`CROSS-CLIENT-BENCHMARK-2026-08-09.md`](CROSS-CLIENT-BENCHMARK-2026-08-09.md) | MQTTium `0.2.0b2` against seven other Python MQTT clients, measured by an external harness on a pinned host: fastest at QoS 0 in both identities, mid-pack at QoS 1. |
| 2026-08-05 | [`STABLE-RELEASE-EVIDENCE-2026-08-05.md`](STABLE-RELEASE-EVIDENCE-2026-08-05.md) | Retained CI, finalisation-campaign and benchmark runs backing the stable-release assessment at commit `0006198`. |

The 2026-08-09 cross-client record describes `0.2.0b2`. On the points internal to MQTTium it is
superseded by the 2026-08-10 performance audit, which gives a verdict on each of its claims. Its
cross-client rankings are not restated anywhere as current: they were produced by an external
harness on a pinned host and have not been reproduced in this repository.

## Memory campaign

Four documents from one audit: methodology lives in the
[`MEMORY-BENCHMARK.md`](../MEMORY-BENCHMARK.md) contract, the numbers and the
outcome live here.

| Date | Report | Records |
| --- | --- | --- |
| 2026-08-04 | [`MEMORY-BASELINE.md`](MEMORY-BASELINE.md) | Before-state, from benchmark run 22 at commit `1a9fe49`. |
| 2026-08-04 | [`MEMORY-RESULTS.md`](MEMORY-RESULTS.md) | After-state for the same scenarios, once admission, delivery-budget, pagination and lazy-hydration corrections landed. |
| 2026-08-04 | [`MEMORY-PROFILE-FOLLOW-UP.md`](MEMORY-PROFILE-FOLLOW-UP.md) | How each audit recommendation was closed, and which ones got a numeric regression guard. |

## Hot-path decisions

Each answers one question with a measurement, and the answer is already
implemented — the report exists so the choice stays falsifiable.

| Date | Report | Question answered |
| --- | --- | --- |
| 2026-08-06 | [`QOS0-V311-DECODE.md`](QOS0-V311-DECODE.md) | Should inbound MQTT 3.1.1 QoS 0 PUBLISH decode straight into the delivered `Message`? |
| 2026-08-06 | [`QOS1-V311-DECODE.md`](QOS1-V311-DECODE.md) | Same question for MQTT 3.1.1 QoS 1, ahead of the acknowledgement state machine. |
| 2026-08-06 | [`QOS0-MESSAGE-BATCH-DELIVERY.md`](QOS0-MESSAGE-BATCH-DELIVERY.md) | Can consecutive small QoS 0 `MESSAGE` effects be transferred to the bounded queues in one pass? |
| 2026-08-06 | [`NOWAIT-WIRE-SIZE.md`](NOWAIT-WIRE-SIZE.md) | Can `publish_nowait()` admission compute the exact wire size instead of encoding a disposable preview frame? |
| 2026-08-05 | [`QOS1-FRAME-POLICY.md`](QOS1-FRAME-POLICY.md) | Should a QoS 1 / pre-PUBREC record retain its encoded PUBLISH frame, re-encode on replay, or choose by size? |
| 2026-08-05 | [`PUBLISH-DECODE-PROFILE.md`](PUBLISH-DECODE-PROFILE.md) | Would parsing PUBLISH directly off the decoder's reusable buffer justify a second ingress path? |
| 2026-08-04 | [`PACKET-ID-POOL-PERFORMANCE.md`](PACKET-ID-POOL-PERFORMANCE.md) | What allocator should back the `1..65535` outbound packet-identifier pool? |

Dates are the day each report was added to the repository. Changelog entries
published before 2026-08-10 refer to these files by their former top-level
`docs/` path.
