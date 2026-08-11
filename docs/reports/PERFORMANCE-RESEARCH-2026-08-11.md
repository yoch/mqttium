# Profile-first performance research — 2026-08-11

## Decision

No production optimisation is retained. The campaign found no dominant macro
network cost, and the one profile-backed micro prototype failed the short
screen. The research harness, scenarios and profiler integration are retained
because their A/A controls exposed two harness defects and a three-pair order
bias before either could become a performance claim.

Baseline: `6f72296c579a26b421bdb793d3ab58a3be75c47f`. Reference host: CPython
3.12.13, Intel i7-3770, Linux 6.8 low-latency, `performance` governor and
Mosquitto 2.0.18. The final runner preflight was eligible: 7.2% sampled CPU,
load/CPU 0.082 and maximum reported temperature 59°C.

Raw artefacts are external build outputs:

- clean usage/stress/memory/reconnect run:
  `/tmp/mqttium-research-campaign-clean-final`;
- diagnostic cProfile, py-spy, perf and strace run:
  `/tmp/mqttium-research-campaign-final3`;
- local and TLS A/A controls:
  `/tmp/mqttium-research-aa-local-abba` and
  `/tmp/mqttium-research-aa-tls-abba`;
- rejected VBI quick screen: `/tmp/mqttium-vbi-quick.json`.

## Clean usage profile

Every row verified exact delivery and ordering. QoS 1 ACK latency is timestamped
by a task attached immediately after `publish()`; subscriber latency comes from
the independent observer process. Rates are diagnostic single-run values, not
cross-version promises.

| Workload | Rate | ACK p50 / p95 | Delivery p50 / p95 | Result |
| --- | ---: | ---: | ---: | --- |
| QoS 0 telemetry, 128 B, paced 1k/s | 985 msg/s | N/A | 0.19 / 0.25 ms | complete |
| QoS 0 telemetry burst, 128 B | 20.3k msg/s | N/A | 36.5 / 47.9 ms | complete; queue residence visible |
| QoS 1, 256 B, window 1 | 3.40k msg/s | 0.27 / 0.39 ms | 0.14 / 0.22 ms | complete |
| QoS 1, 256 B, window 64 closed loop | 9.92k msg/s | 4.36 / 6.90 ms | 1.58 / 2.61 ms | complete |
| QoS 1, window 64, 50% calibrated load | 4.10k msg/s | 0.59 / 1.05 ms | 0.18 / 0.38 ms | complete |
| QoS 1, window 64, 90% calibrated load | 5.93k msg/s | 0.86 / 3.43 ms | 0.24 / 1.93 ms | complete |
| MQTT 5 duplex gateway, 1 KiB | 8.12k msg/s | aggregate receipt | 16.7 / 23.5 ms | complete |
| QoS 1 `publish_many`, 256 B | 8.24k msg/s | aggregate receipt | 9.78 / 15.7 ms | complete |
| QoS 1 segmented payload, 1 MiB | 142 msg/s | 30.7 / 51.3 ms | 22.7 / 31.4 ms | complete; 35.3 MiB max RSS |
| MQTT 5 property-heavy QoS 2 | 1.37k msg/s | 7.98 / 45.8 ms | 6.51 / 45.5 ms | complete |
| 10 ms slow callback | 93.9 msg/s | N/A | 555 / 1013 ms | complete; expected backpressure |

The short reconnect soak delivered 150/150 messages across two forced
reconnects and reported stable resources. The scaled memory smoke completed the
property-heavy, cancellation, Paho saturation, shared-delivery, WebSocket and
epoch-cleanup scenarios.

## Profile attribution

The clean worker is CPU-distributed rather than dominated by one function.
Across 5,000 QoS 1 publications, cProfile recorded 853,708 calls in 1.364 s.
The largest MQTTium self-time entries were `encode_publish_item` 34 ms,
`OutboundSession.queue_publish` 31 ms, `AsyncClient.publish` 29 ms,
`_launch` 18 ms and `on_puback` 14 ms. `item_size` was called 15,004 times but
accounted for only 7 ms self / 11 ms cumulative, matching the earlier decision
not to widen the hot signature for about 1%.

The paced py-spy capture collected 498 samples over 2.49 sampled seconds. Its
leaf samples were spread across event-loop callbacks, selector writes,
admission, ingress decode and effect collection; no new library frame dominated.
`py-spy==0.4.2` emitted a CPython-symbol warning on this uv-provided interpreter
but completed and produced a valid Speedscope artefact.

`perf stat --no-inherit` recorded 1.42 billion cycles, 1.45 billion instructions
and 10 context switches for the 5,000-message diagnostic worker. Publisher-only
`strace -c` recorded 181 `sendmsg` calls for 5,000 publications — about 27.6
messages per socket submission — costing 0.56 ms total. `recvfrom` cost another
0.53 ms. Reducing socket calls or adding another writer-batching layer is not a
supported target from this profile.

## Prototype and controls

The only new prototype unrolled `append_vbi`, replacing division/modulo loops
with the known 1–4 byte cases. `encode_qos1` measured candidate/base 1.049, but
only 2/3 pairs favoured the candidate and baseline CV was 5.86%. Per the short
funnel it was reverted without an 11-pair confirmation.

The new network harness first made the publisher and subscriber share one
CPU-pinned process. At 2,000 QoS 1 messages it stalled at 1,913 deliveries. The
retained harness starts `mosquitto_sub` and its timestamping observer before
pinning only the publisher. It also explicitly joins/drains the writer before
blocking on a QoS 0 observer; otherwise admission-complete QoS 0 frames remained
owned by the event loop and appeared lost.

A three-pair TLS A/A initially produced a false +9.9% “candidate” signal on
identical code because the second arm was favoured. Compare mode now requires
at least four repetitions in complete ABBA cycles. Even then the same-source
controls were unsuitable for claims: local TCP reported median 1.066 with 4.64%
baseline CV, outside the ±2% neutral budget, and TLS reported 0.961 with 15.2%
baseline CV. Compare mode now marks such A/A results invalid.

The controlled WAN profile could not run: direct `tc` lacked `CAP_NET_ADMIN`
and passwordless `sudo` was unavailable. No qdisc was changed. No remote broker
was available, so this report makes no WAN or Internet claim.

## Validation

The retained research tooling passes Ruff formatting/linting and mypy. The full
unit suite passed (808 tests), the focused Hypothesis codec/protocol/state
fuzzing passed (40 tests), and the live Mosquitto integration suite passed (15
tests). The broker used for the live check was stopped afterwards.

No production candidate reached confirmation, so no candidate-specific API
equivalence test or long soak was warranted. The discovery campaign nevertheless
ran its reconnect/persistence soak and the bounded memory guardrails. There is
no production-code diff from this campaign.
