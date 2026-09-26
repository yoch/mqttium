# Release candidate 1.0.0rc17 evidence

Date: 2026-09-26. Candidate runtime: `main` at
`4782dfb7ff11ee008890b4eae39387ed91e7182e`. The release commit changes only the
version string, the frozen changelog section, release-facing installation and
support text, and this report. Baseline for comparisons: `1.0.0rc16`
(`681cc01523f56db81cca1943ee89ef2c3e5388b5`).

RC17 revises the RC16 native API before 1.0. The
[migration guide](../migration.md#changes-since-100rc16) lists every removed or
reshaped contract. It is a pre-release (`Development Status :: 4 - Beta`), not
a final 1.0.

## Content

The pre-1.0 sprint had three goals: keep only useful contracts, simplify the
code and harden it with formal models, and take a last performance pass
without trading away simplicity.

| Area | Pull requests |
| --- | --- |
| Public surface: the maintainer's decisions from the [surface review](API-SURFACE-REVIEW-2026-09-24.md) (dead and duplicate contracts, internal-state leaks, Will as `PublishMessage`, store methods Internal, statistics, builtin argument errors) | #569, #570, #571, #572, #573, #574, #575, #567 |
| Bugs found by reading or by the new models, each with a regression that fails on RC16 | #565 (sealed packet ids freed with subscription ids), #568 (sealed row after a failed cleanup delete), #578 (resumed QoS 1 PUBACK before the broker's resend), #583 (stored inbound QoS 1 rows count as Session State) |
| Simplification | #576 (send-quota slot owned by packet id), #577 (inbound Receive Maximum derived from its owners), #580 (named teardown steps; `_read_loop` 243 → 151 lines) |
| Formal models | #579 (last legacy-flag models become refinement models); new `PacketIdOwnership` and `AdmissionRollback`; `SendQuotaOwnership` and `InboundSession` rewritten against the implementation's predicates |
| Performance | #581 (received `Message` built through slot descriptors), #585 (no await on a settled effect pump) |
| Documentation and tooling | #566, #582, #584 |

Every model is now a refinement model: its RC15 or RC16 configuration keeps its
counterexample and its repaired configuration passes. TLC checks 58
configurations in CI.

Size of the runtime against RC16: `src/` grows by 29 lines, net of the new
fixes. `InboundSession` holds 25 state slots instead of 27, `OutboundSession`
19 instead of 20.

## Qualification of `4782dfb7`

| Evidence | Result |
| --- | --- |
| [CI 36266676868](https://github.com/yoch/mqttium/actions/runs/36266676868) and [ARM64 CI 36266676903](https://github.com/yoch/mqttium/actions/runs/36266676903) | Passed |
| [Soak and broker interoperability 36267800944](https://github.com/yoch/mqttium/actions/runs/36267800944) | Passed: Linux and macOS soaks for MQTT 3.1.1 and 5, EMQX 5.8.9 and HiveMQ CE 2026.5 |
| Strict ARM64 network gate vs RC16, [36270053544](https://github.com/yoch/mqttium/actions/runs/36270053544) | Passed: QoS 1 receipt ACK throughput at windows 1, 20 and 64 within 0.94–1.06 of RC16 per ABBA cycle; window 1 at 1.01–1.02 in 11 of 12 cycles |
| Strict ARM64 open-loop gate vs RC16, [36267803906](https://github.com/yoch/mqttium/actions/runs/36267803906) | Passed; see below |
| ARM64 paired regression vs RC16, [36267722844](https://github.com/yoch/mqttium/actions/runs/36267722844) | Passed: strict writer-capacity A/B 0.999 (QoS 0) and 0.987 (QoS 1), A/A 0.998 and 1.000; strict paced writer-latency A/B lag 1.002 at 2,500 and 1.000 at 10,000 msgs/s |

### ARM64 paired microbenchmarks against RC16

Median candidate/base throughput over 11 pairs (run 36267722844):

| Scenario | Ratio |
| --- | --- |
| `ingress_engine_qos0` | 1.153 |
| `ingress_engine_qos0_v5` | 1.137 |
| `ingress_publish_qos1` | 1.189 |
| `mqtt5_puback_reason_cycle` | 0.985 |
| `qos1_cycle_memory` | 0.990 |
| Every other scenario (encode, writer, publish, effects, delivery, receipts, persistence) | 0.992–1.013 |

The advisory network sweep (MQTT 3.1.1 and 5, 64 B and 4096 B, windows 1 to
128) measured QoS 1 receipt ACK throughput at 0.997–1.018 of RC16 with
unchanged p50 latency. One cell (3.1.1, 4096 B, window 1) had a candidate
variation of 9 %, above the advisory threshold; its ratio was 1.017.

### Measurements on the x86 development host

Paired, interleaved rounds; the noise is about ±5 %.

- Engine-level reception, QoS 0 and QoS 1: about +20 % (#581).
- Awaited QoS 0 `publish()`: about 6 % less CPU per message (#585). At network
  window 1, publisher CPU per message −3.7 % and throughput +3.6 %; larger
  windows were within this host's noise.
- Other paired scenarios: within noise.

## Event-loop lag at saturation (#493)

[Issue #493](https://github.com/yoch/mqttium/issues/493) remains open for 1.0.
RC17 was checked only for a further regression against RC16.

Open-loop gate against RC16 (run 36267803906): per-load ratios of the candidate
over RC16, at matched offered rates.

| Protocol, payload | Load | Throughput | Loop lag p95 |
| --- | --- | --- | --- |
| 3.1.1, 64 B | 0.50 / 0.75 / 0.90 / 1.00 | 1.00 / 1.01 / 1.00 / 1.00 | 1.00 / 1.01 / 1.02 / 1.12 |
| 5, 64 B | 0.50 / 0.75 / 0.90 / 1.00 | 0.99 / 1.00 / 1.00 / 1.00 | 0.96 / 1.01 / 1.07 / 0.99 |
| 3.1.1, 4096 B | 0.50 / 0.75 / 0.90 / 1.00 | 1.00 / 0.99 / 1.00 / 0.99 | 1.00 / 1.00 / 1.03 / 1.02 |
| 5, 4096 B | 0.50 / 0.75 / 0.90 / 1.00 | 1.00 / 1.00 / 1.00 / 1.00 | 0.96 / 1.00 / 1.03 / 1.07 |

The runner preflight was eligible. No cell exceeds the same-code A/A spread
documented for RC15 (up to about 1.12); the 3.1.1/64 B saturation cell sits at
that bound. The gate therefore shows no further lag regression from RC16 to
RC17. It does not qualify #493 itself, and the RC15 caveat still applies:
capacity calibration still moves between levels (5/4096 B calibrated at about
21,600 msgs/s for the baseline against 16,600 for the candidate diagnostic).

## Not performed for this candidate

- The local `benchmarks/local_release.py rc` profile. Remote CI, the ARM64
  runner and the publication workflow's validation build, wheel smoke and
  resilience smoke provide the evidence above instead.
- Multi-hour fuzzing and soak campaigns, and validated migration of real
  applications. They remain required promotion evidence for a final 1.0, not
  for this candidate.
