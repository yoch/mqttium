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

Read the audit's verdict table before quoting the record. One of its conclusions — that the
`on_publish is None` inline settle "does not pay off under load" — **is known to be wrong**: the
experiment behind it varied the benchmark adapter's completion discipline at the same time, so it
cannot attribute anything to that branch, and its author agrees. The record is left as written
because a report states what was believed on its date; the correction lives in the audit. Its QoS 1
latency measurement, by contrast, is **retracted by its author**, and the retraction is the more
useful of the two corrections.

The record framed MQTTium's PUBACK latency as a fixed floor, 2.95x gmqtt at a matched offered rate.
Its author re-tested that on 2026-08-12 and it does not hold. Driving both clients at the *same
absolute* rate rather than at the same fraction of each one's own capacity:

| offered | MQTTium | gmqtt | ratio |
| --- | --- | --- | --- |
| 2,105 msgs/s | 0.318 ms | 0.256 ms | 1.24x |
| 6,246 msgs/s | 1.32 ms | 0.45 ms | 2.94x |

Latency falls fourfold when the offered rate falls threefold, so it is load-dependent queueing and
not a constant delay — the opposite of what the record claimed.

The root cause is a method error, not a defect on either side. That harness paced each client at a
fraction of its *own* calibrated capacity, so a faster client was offered a higher absolute rate and
sat further along its own latency-versus-load curve. Reading the resulting table across clients
penalises whichever client has the most headroom, which was MQTTium. Its author has since added a
fixed-rate scenario for cross-client comparison and marked the fraction-paced one as unsuitable for
it.

One contributing cause is quantified: that harness suspends a coroutine for the whole round trip on
MQTTium while returning immediately and correlating the ack in a callback on gmqtt. Driving MQTTium
the second way is worth 11-34%, growing with load. It has been reverted there rather than kept,
because it would trade one asymmetry for another.

Measured in this repository against the same containerised broker, publisher pinned to one physical
core as that harness pins it, MQTTium is 1.18x gmqtt at 6,246 msgs/s (1.13x unpinned), with a
four-point breakdown putting 71 us of library — 45 us to the wire, 26 us to settle — inside a 207 us
round trip. The same figures hold on the `v1.0.0rc1` tag that was measured externally, so nothing
here was fixed after the fact.

Two hypotheses were tested here and did **not** hold, and are recorded so they are not re-run:
single-physical-core pinning does not amplify MQTTium's scheduling hops (1.18x pinned against 1.13x
unpinned, breakdown unchanged), and the knee that harness later reported between 2,500 and 5,000
msgs/s does not reproduce (0.270-0.284 ms against 0.251-0.301 ms over six alternated samples per
point, on both the awaited and `publish_nowait` paths).

One question is open rather than answered. That harness calibrates MQTTium's ceiling at 12,492
msgs/s; a tight in-process loop here reaches about 17,960 under that harness's own broker and
publisher pinning, so the environment accounts for none of the difference. That does not establish
how much of it is adapter overhead, because a ceiling measured without a pacer or per-message
bookkeeping is not like-for-like with a calibrator's discipline. It matters because that calibration
sets the offered rate for every open-loop point.

Do not treat the cross-client latency column as a statement about MQTTium until that harness can
defend it.

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
| 2026-08-13 | [`AUTO-QOS1-MESSAGE-BATCH-DELIVERY.md`](AUTO-QOS1-MESSAGE-BATCH-DELIVERY.md) | Can fresh automatic QoS 1 MESSAGE effects share the small-message inline batch without weakening persisted replay semantics? |
| 2026-08-13 | [`RM-SLOT-UNTIL-HANDOFF.md`](RM-SLOT-UNTIL-HANDOFF.md) | Should auto-ACK QoS 1 keep the Receive Maximum slot until `take_effects()` instead of reconstructing occupancy with a second decode? |
| 2026-08-12 | [`QOS2-V311-DECODE.md`](QOS2-V311-DECODE.md) | Should inbound MQTT 3.1.1 QoS 2 PUBLISH use the same field decoder as QoS 1? |
| 2026-08-12 | [`ACK-SUCCESS-FASTPATH.md`](ACK-SUCCESS-FASTPATH.md) | Can common success/no-properties acknowledgement frames bypass transient packet objects without weakening long-form MQTT 5 validation? |
| 2026-08-12 | [`QOS12-LAUNCH-ENCODE.md`](QOS12-LAUNCH-ENCODE.md) | Should stored outbound QoS 1/2 launches call the functional PUBLISH encoder directly instead of constructing a transient packet object? |
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
