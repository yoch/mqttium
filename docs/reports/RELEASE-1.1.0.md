# Release 1.1.0 evidence

Date: 2026-09-29. Candidate runtime: `main` at
`cd2c4289f5bd46bc880ab6ac9442a18eee486150`. The release commit adds only
documentation, the version string, the frozen changelog section, release-facing
installation and support text, and this report. Baseline for comparisons:
`1.0.0` (`61aaa1cb7201e4b9e88dd3d984f007e80d8814e3`).

## Why 1.1.0

An integration of `mqttium==1.0.0` into a production listener (callback
delivery, MQTT 3.1.1, unbounded reconnect policy) reported nine problems. Three
outside audits then read the code and documentation as a production user, a
publish/subscribe user and an adopter would, and found about thirty more of the
same kind: silent losses and clients that stall. 1.1.0 fixes them under one
rule: the client never stalls or loses silently.

Some fixes change Stable behaviour. The maintainer chose a minor release with a
documented exception to SemVer, recorded in the
[API contract](../api-stability.md#the-11-exception); every change and the code
to adapt are in the [1.1 migration notes](../migration.md#changes-in-11).

## Content

| Area | Pull requests |
| --- | --- |
| Losses: writes into a lost transport, acknowledged messages discarded on reconnect, unacknowledged QoS 1 failed instead of resent after a lost session | #596, #597, #602 |
| Stalls: keepalive on a half-open link and under application backpressure, receipts for a connection nothing would open, first connect before the broker is up, durable MQTT 5 session after a process restart | #598, #600, #601 |
| Final state and retry classification (`RECONNECTING`, `ConnectRefusedError`, `retry_refused`, *Session taken over* terminal) | #599 |
| Limits: MQTT 3.1.1 inbound limits the broker never learns, oversized publication, outbound window | #603 |
| Typed errors and results (`ConnectError`, `PublishRejectedError`, `SubscribeError`, `granted_qos`) | #604 |
| Routing: shared-subscription routes, split iterators, delivered topic aliases, manual acknowledgement order | #605 |
| Observability: connection age, last loss, callback failures, unrouted messages | #607 |
| Transports: happy eyeballs, WebSocket handshakes met in practice | #606 |
| Documentation: coming from Paho or aiomqtt, corrected examples, event loops, SQLite properties | #608 |
| Performance: a per-lot cost introduced by #597, found by the release qualification | #609 |

### Report point 8, reproduced

A 3.1.1 publisher sending 25 QoS 2 messages to a local Mosquitto 2.0.18
completed every receipt, and the trace showed 25 complete PUBLISH, PUBREC,
PUBREL and PUBCOMP exchanges, yet subscribers (MQTTium or `mosquitto_sub`)
received 20. With 3000 messages, QoS 2 still delivered 20 and QoS 1 1040.
`1.0.0` behaves the same. Mosquitto acknowledges and then drops QoS 1/2
messages beyond its default window of 20 from a 3.1.1 client, which cannot
announce a Receive Maximum; MQTTium used a 65,535-message window on 3.1.1.
With the new 3.1.1 default of 20, 3000 of 3000 messages arrive for both QoS
levels, and a Mosquitto integration test pins it.

### Not included

- Local input errors (a wildcard or null in a topic) still raise
  `ProtocolError`, not `ValueError`: the raise sites are shared with decoding
  peer packets.
- No `server_hostname` option for TLS: it changes the transport factory
  signature.
- No age of the oldest message awaiting a manual acknowledgement: it would put
  a timestamp on every message.

## Qualification of `cd2c4289`

| Evidence | Result |
| --- | --- |
| [CI 36577029558](https://github.com/yoch/mqttium/actions/runs/36577029558) and [ARM64 CI 36577029782](https://github.com/yoch/mqttium/actions/runs/36577029782) | Passed |
| [Soak and broker interoperability 36578370402](https://github.com/yoch/mqttium/actions/runs/36578370402) | Passed: Linux and macOS soaks for MQTT 3.1.1 and 5, EMQX 5.8.9 and HiveMQ CE 2026.5 |
| Strict ARM64 network gate vs 1.0.0, [36578366687](https://github.com/yoch/mqttium/actions/runs/36578366687) | Passed: QoS 1 receipt ACK throughput at windows 1, 20 and 64 within the gate per ABBA cycle, mostly 0.976–0.995 of 1.0.0 |
| ARM64 paired regression vs 1.0.0, [36577040928](https://github.com/yoch/mqttium/actions/runs/36577040928) | Passed: strict writer-capacity A/B 0.994 (QoS 0) and 0.990 (QoS 1), A/A 1.001 and 1.002; strict paced writer-latency A/B lag 1.001 at 2,500 and 1.000 at 10,000 msgs/s, A/A 0.998 and 1.000; advisory network sweep 0.981–1.001 |

### ARM64 paired microbenchmarks against 1.0.0

Median candidate/base throughput over 11 pairs (run 36577040928): receive
paths `ingress_engine_qos0_v5` 0.984 and `ingress_publish_qos1` 0.990;
callback delivery `effect_single_message_callback` 0.985 and
`delivery_callback` 0.964; iterator delivery `delivery_iterator` 0.987; every
other scenario 0.99–1.024.

### Performance found during qualification

The first qualification of `ef938eaa` (run 36552990604) measured
`effect_single_message_callback` at 0.904. Counting profiler-visible calls per
delivered message isolated it to #597, whose carryover bookkeeping ran for every
delivery lot: 41.22 calls per message against 37.22 in 1.0.0. #609 skips that
bookkeeping when a lot is fully delivered and restores 37.22; the scenario
returned to 0.985. `delivery_callback` executes the same calls per message as
1.0.0; its remaining 0.964 is attributed to code layout, not to added work, and
is accepted for the loss fixes it accompanies.
