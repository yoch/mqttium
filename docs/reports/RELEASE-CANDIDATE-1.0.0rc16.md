# Release candidate 1.0.0rc16 evidence

Date: 2026-09-24. Candidate runtime: `main` at
`c6dcae84ae3a5af0700663c19bcefc7a30dc6684`. The release commit changes only the
version string, the frozen changelog section, release-facing installation and
support text, and this report. Baseline for comparisons: `1.0.0rc15`
(`d57ab288910d163da1c75227cbaa2c055ce11169`).

RC16 keeps the RC15 native API. It is a pre-release
(`Development Status :: 4 - Beta`), not a final 1.0.

## Content

RC16 resolves the 39 findings of the rc15 formal runtime audit (tracker
[#547](https://github.com/yoch/mqttium/issues/547)). Each fix has a
deterministic regression that fails on RC15. Each structural cause has a TLA+
model whose RC15 configuration keeps its counterexample and whose repaired
configuration passes; TLC checks 49 configurations in CI.

| Cause | Pull requests |
| --- | --- |
| Formal-model infrastructure (pinned TLC, CI job, declared outcomes) | #546 |
| Cancellation ownership decided in one place | #549 |
| Inbound exchange completes only after the application owns the message (data loss) | #550 |
| Ingress lot ends at the first peer error and keeps its prefix | #551 |
| Atomic connection teardown and first-cause latch | #552 |
| Observed facts no longer wait behind blocked writes; owned AUTH task | #553, #554 |
| Resumed-session replay conflict (`SessionReplayError`) | #555 |
| Outbound writer, QoS 2 phases, send quota, failed receipts | #556, #557, #559, #560 |
| Inbound acknowledgement handoff | #558, #562 |
| Reconnect during `on_disconnect`; `stable_after` retries at once | #561 |

Performance follow-ups: #495 (SQLite batch refusal) and #563 (effect-kind
membership by identity).

## Qualification of `c6dcae84`

| Evidence | Result |
| --- | --- |
| [CI 36021110967](https://github.com/yoch/mqttium/actions/runs/36021110967) and [ARM64 CI 36021111023](https://github.com/yoch/mqttium/actions/runs/36021111023) | Passed |
| [Soak and broker interoperability 36021308762](https://github.com/yoch/mqttium/actions/runs/36021308762) | Passed: Linux and macOS soaks for MQTT 3.1.1 and 5, EMQX 5.8.9 and HiveMQ CE 2026.5 |
| Strict ARM64 network gate vs RC15, [36021312003](https://github.com/yoch/mqttium/actions/runs/36021312003) | Passed: QoS 1 receipt ACK throughput at windows 1, 20 and 64 within 0.94–1.07 of RC15 per ABBA cycle |
| Strict ARM64 open-loop gate vs RC15, [36025710297](https://github.com/yoch/mqttium/actions/runs/36025710297) | Passed; see below |
| ARM64 paired regression vs RC15 | PENDING |

### Measurements on the x86 development host

Throughput ratio of the candidate against RC15. Paired, interleaved rounds;
the noise is about ±5 %.

- Publication QoS 0 and QoS 1, receipts and codec: within noise.
- Inbound QoS 1 on the client: +3 %.
- Inbound QoS 2 on the client: −5 to −8 %. Engine alone: −13 %.

The inbound QoS 2 cost is correctness work, not overhead:
- the exchange completes after the delivery mark (#519, #520);
- the exchange identity (#534);
- the PUBCOMP handoff (#537, #541).

The maintainer accepted it; [#39](https://github.com/yoch/mqttium/issues/39)
records the analysis.

## Event-loop lag at saturation (#493)

[Issue #493](https://github.com/yoch/mqttium/issues/493) remains open for 1.0.
It concerns the p95 event-loop lag at 64-byte saturation of RC15 against RC14.
RC16 was checked only for a further regression against RC15, because the
campaign changed the acknowledgement receive path that #493 suspects.

Open-loop gate against RC15 (run 36025710297): per-load ratios of the candidate
over RC15, at matched offered rates.

| Protocol, payload | Load | Throughput | Loop lag p95 |
| --- | --- | --- | --- |
| 3.1.1, 64 B | 0.50 / 0.75 / 0.90 / 1.00 | 1.00 / 1.00 / 1.00 / 1.00 | 1.00 / 0.99 / 0.99 / 0.98 |
| 5, 64 B | 0.50 / 0.75 / 0.90 / 1.00 | 1.00 / 1.00 / 1.00 / 1.00 | 1.09 / 1.02 / 0.86 / 0.89 |
| 3.1.1, 4096 B | 0.50 / 0.75 / 0.90 / 1.00 | 1.00 / 1.00 / 1.00 / 1.00 | 1.02 / 1.00 / 0.84 / 0.82 |
| 5, 4096 B | 0.50 / 0.75 / 0.90 / 1.00 | 1.00 / 0.99 / 1.00 / 1.00 | 1.00 / 1.01 / 1.00 / 1.01 |

The runner preflight was eligible, including the frequency and throttling
checks added by #496. No cell exceeds the same-code A/A spread documented for
RC15 (up to about 1.12). The gate therefore shows no further lag regression
from RC15 to RC16.

It does not qualify #493 itself. The reliability caveats recorded for RC15
still apply: capacity calibration still moves between levels (the 3.1.1/64 B
baseline calibrated at about 21,000 msgs/s against 26,000 for the candidate
diagnostic). Fixed-rate comparison, profiling, and a decision on the RC14 gap
remain required before 1.0.

## Not performed for this candidate

- The local `benchmarks/local_release.py rc` profile. Remote CI, the ARM64
  runner and the publication workflow's validation build, wheel smoke and
  resilience smoke provide the evidence above instead.
- Multi-hour fuzzing and soak campaigns, and validated migration of real
  applications. They remain required promotion evidence for a final 1.0, not
  for this candidate.
