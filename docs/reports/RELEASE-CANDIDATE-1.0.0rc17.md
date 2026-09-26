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
| RUNS_PENDING | |

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

## Not performed for this candidate

- The local `benchmarks/local_release.py rc` profile. Remote CI, the ARM64
  runner and the publication workflow's validation build, wheel smoke and
  resilience smoke provide the evidence above instead.
- Multi-hour fuzzing and soak campaigns, and validated migration of real
  applications. They remain required promotion evidence for a final 1.0, not
  for this candidate.
