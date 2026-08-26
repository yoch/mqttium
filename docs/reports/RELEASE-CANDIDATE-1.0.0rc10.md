# Release candidate report — 1.0.0rc10

Candidate source before release metadata:
`df93e72f41384a923f5fd6b3acdac7297bbcfe0e` (`main`, 2026-08-26).
Release metadata commit: `afe8b837c7f9211d7465a911cddf485c88b82c79`.
Release-harness and final local-evidence commit:
`c3e6e341c8b3dd845349b965852e6c90fa6e5b57`.

## Scope

RC10 collects the reviewed correctness, persistence and runtime-fuzzing work
merged after `v1.0.0rc9`. Its principal changes are:

- consolidate enhanced-authentication state and add the Stable `auth_timeout`
  bound;
- complete stateful outbound MQTT 5 Topic Alias handling, including canonical
  durable replay across connection-scoped alias resets;
- harden connection, callback, writer, EffectPump, keepalive and reconnect
  ownership under adversarial interleavings;
- stream and revalidate inbound replay at bounded effect-batch boundaries;
- fix replay-parked outbound settlement resurrection found by the stateful
  invariant fuzzer;
- add the V1 generative runtime fuzzer, V2 two-window composition target and V3
  pressure/interleaving target, with mutation qualification and an ARM64 V1
  nightly;
- make runtime-fuzzer wall-clock deadlines explicit for shared low-priority
  runners while retaining strict CI defaults and independent invariants.

The only removed surface is Provisional
`IncrementalDecoder.process_packets_bounded`; no Stable API was removed or
changed incompatibly. The package version is `1.0.0rc10`; its release tag must
be exactly `v1.0.0rc10` and the GitHub release must be a prerelease.

## Runtime-fuzzing evidence

Two fresh x86-64 long campaigns cover the final runtime-fuzzer implementation:

- V3 pressure/interleaving at
  `0bae517e413fd5986f3060c609c92599bb2779c9`: 1,000,000 schedules × 48
  operations, zero failures, all mandatory pressure counters, 999,976 unique
  operation traces and 999,736 unique scheduling traces;
- V2 lifecycle composition at
  `0be97c8df28aa13cf05fc94d8114f7ce2bea8a8f`: 1,000,000 schedules × 48
  operations, zero failures, all six ownership pairs, all 64 release traces,
  999,963 unique operation traces and 999,598 unique scheduling traces.

V2 and V3 retain four- and eight-mutant qualification respectively. The
shared-runner timeout report records the discarded environmental attempts and
their replay procedure; no timeout artifact was counted as healthy evidence.

## Local quality and performance evidence

The clean local RC run against `v1.0.0rc9` recorded:

- Ruff format/lint, mypy and Bandit success;
- 1,434 unit tests passed with 90.16% source coverage, above the 87.36% floor;
- 16 mandatory Mosquitto integration tests passed;
- eligible preflights before both comparative phases;
- the paired micro gate completed successfully;
- the closed-loop network sweep completed in advisory mode. Its artifact is
  intentionally `invalid` under the low-level raw-arm CV rules in 15 receipt
  cells; those CV rules are diagnostic and are not a local release decision,
  as documented by the benchmarking contract.

The original strict open-loop artifact recorded zero failures but returned
`invalid` before confirmation because 15 point-ratio cells exceeded the old
bounded maximum of four. The corrected initial screen reuses necessary
conditions already present in the final verdict: the geometric mean must exceed
1.05 and the relative and additive 95% lower bounds must both clear their
no-effect boundary before same-code controls are spent.

The retained 32 initial ABBA cells were reevaluated without acquisition. The
separate result passed with zero throughput suspects and zero loop-lag
confirmation candidates. The original remains unchanged. Traceability values
are:

- original artifact SHA-256:
  `c5a513cbe9d478c3e3eaab36a2c5e179ecf17b1dfedca9cf44484873e2593c4b`;
- evaluator commit:
  `c3e6e341c8b3dd845349b965852e6c90fa6e5b57`;
- evaluator policy-source SHA-256:
  `70cdd87b86ec1cdba623ceef43d9e2e42442cc7009a97cdd792da8b4bb1d1d18`.

This loop-lag policy is a targeted regression detector, not an equivalence
claim. A noisy point ratio cannot fail the release by itself.

## Robustness and package evidence

The clean `rc-remainder` manifest at `c3e6e341` passed all 22 commands in
327.6 seconds under reduced process priority:

- exact hot-path call/allocation profiling;
- all 15 isolated memory scenarios and their versioned thresholds;
- application stress;
- 30-second forced-reconnect soaks for MQTT 3.1.1 and MQTT 5.

The MQTT 3.1.1 soak published and received 13,500 messages across 25 measured
cycles with 26 forced reconnects. MQTT 5 published and received 14,000 messages
across 26 measured cycles with 27 forced reconnects. Both ended with zero idle
violations and a stable resource assessment.

The same manifest validated project metadata, built one wheel and one sdist,
ran strict Twine and wheel-content checks, installed the wheel without runtime
dependencies, imported every packaged module and passed isolated TCP, TLS,
WebSocket, Unix, SQLite restart, Paho VERSION2 and clean-shutdown smokes.

Artifact SHA-256 values are:

- wheel: `0ffaa32220716af448bc82c427eb102b458949beaad014152e400e28422c4b61`;
- sdist: `1e1ea94066ecf7020eb91b6b1b79dfb5fab9a45c943a28dd79d4e2acfb347ea7`.

## Hosted candidate validation

The exact `c3e6e341` PR head passed repository quality, Python 3.11–3.14,
Linux soak for MQTT 3.1.1 and MQTT 5, resilience, fuzz, package, macOS 3.11 and
3.14, Windows 3.11 and 3.14, Codecov and the `CI required` aggregate. The
`Publish to PyPI` pull-request workflow also passed its build plus wheel/sdist
basic smokes and the installed-artifact resilience smoke. Publication jobs were
correctly skipped for a pull request.

## Release decision

RC10 is ready to merge after the evidence-report-only head passes its final PR
checks. The two million-schedule runtime campaigns found no product failure;
the strict local micro and corrected open-loop gates are green; robustness,
memory, reconnect soaks and installed distributions passed; and the exact
pre-report candidate passed the full hosted matrix. No runtime source changes
follow `c3e6e341`.
