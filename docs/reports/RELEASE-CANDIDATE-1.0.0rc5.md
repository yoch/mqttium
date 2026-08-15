# Release candidate report — 1.0.0rc5

Candidate source before release metadata: `478e3d7f` (`main`, 2026-08-15).

## Scope

RC5 collects the reviewed work merged after `1.0.0rc4` without changing the frozen Stable API or public defaults.

The release includes three scheduler/callback handoff improvements:

- #220 admits terminal QoS 1/2 `on_publish` callbacks directly to the bounded callback worker when capacity is immediately available, settling receipts without an intermediate EffectPump wake-up;
- #221 lets one already-eligible small inbound MESSAGE use the established bounded inline delivery path;
- #223 applies the analogous direct callback admission to native QoS 0 after atomic writer admission.

It also includes the six independently extracted findings from the hypothesis-driven performance audit:

- #225 tracks persisted inbound-store occupancy separately from Receive Maximum so the common empty durable inbound store performs no metadata lookup or SQLite SELECT per automatic QoS 1 PUBLISH;
- #226 reuses the MQTT 5 property table already encoded during immediate QoS 1/2 admission instead of repeating the mutation-safe cache-signature walk in the packet encoder;
- #227 reuses validated Topic Name bytes on immediate QoS 1/2 launch while retaining no discarded copy when the connected flow window is saturated;
- #228 carries the exact fresh decoded MQTT 5 property-table wire size only as ephemeral engine-effect metadata. Small and accounted delivery can reuse it without storing stale metadata on mutable `Properties` or in persistence;
- #229 decodes inbound MQTT 5 QoS 0 directly into the delivered `Message` while preserving #228's size handoff and skipping Topic Alias resolution only when the Topic Name is non-empty and no alias property is present. QoS 1/2 retain the generic field decoder.

No compiled acceleration experiment or audit-tracking branch is part of this candidate.

## Review and repository cleanup

Before cutting RC5, the repository-wide Cursor Bugbot history was rechecked. Twelve real historical findings were identified across #174, #178, #181, #204, #207, #208, #213 and #217. Every finding is fixed on current `main`; the remaining four unresolved historical review threads on #174, #181 and #204 were replied to with the corresponding current regression coverage and marked resolved.

The Bugbot note retained on tracking PR #222 came from an external Bugbot run. Its `_wire_size` concern applies to the tracking implementation, not to merged #228, whose decoded size metadata is ephemeral and never stored on mutable `Properties`. The completed tracking/evaluation PRs #222, #224, #203 and #189 were closed unmerged before the release branch was created. There were no open pull requests at the RC5 cut.

## Validation inherited from the merged heads

The latest runtime head, #229, was validated on its exact clean tree with:

- Ruff and mypy clean;
- 113 focused tests and 1,081 full unit tests;
- repository CI on Python 3.11, 3.12, 3.13 and 3.14 with Mosquitto integration;
- macOS and Windows unit suites;
- package and fuzz jobs;
- long fuzz smoke;
- MQTT 3.1.1 and MQTT 5 Linux reconnect/backpressure soaks.

#228 independently passed the same broad repository matrix on its final one-commit head, including 1,064 full unit tests and focused decoded-property delivery/accounting coverage. #227 also passed CI, long fuzz and finalization/interoperability before merge. Earlier changes #220/#221/#223 carried their own unit, integration, memory and performance controls.

The RC5 release PR is expected to run the repository CI plus the `Publish to PyPI` pull-request build/smoke jobs against the exact `1.0.0rc5` artifacts before merge. No local-runner result is claimed in this report.

## Performance evidence

The relevant claims are bounded to the paths that were measured; exact call-count elimination is preferred over extrapolating microbenchmarks to end-to-end throughput.

- #220: at the valid 5,000 msg/s callback load point, callback p50 moved from about 0.532 ms to 0.249 ms while completed-rate stayed neutral; the exact callback hot path used about 22.9% fewer calls.
- #221: source-isolated scheduler-boundary probe, median candidate/base 6.4588x with 11/11 positive pairs; this is explicitly not an end-to-end broker throughput forecast.
- #223: source-isolated native QoS 0 callback target, median 1.9474x with 11/11 positive pairs, while the no-callback control remained neutral.
- #225: the empty SQLite inbound steady state dropped from one SELECT per automatic QoS 1 PUBLISH to zero; same-host SQLite/memory timing moved from roughly 1.47x to 0.99x.
- #228: fresh decoded MQTT 5 IoT-property delivery was about 80% lower in the isolated target path, oversized decoded delivery about 37% lower, with application-built properties and MQTT 3.1.1 controls effectively neutral.
- #229: MQTT 5 QoS 0 empty-property handling was about 12% lower and the representative IoT property bag about 7–9% lower; the alias-resolution skip gave a smaller 2–5% improvement on QoS 1/2 while retaining their generic field decoder.

Absolute microsecond timings from non-release runners remain advisory. No benchmark threshold was weakened for RC5.

## Release decision

RC5 is a consolidation candidate: the runtime changes were already independently reviewed and merged, the historical Bugbot findings have been reconciled, and the active PR queue was empty at cut. Promotion of this branch depends on the release PR's artifact/CI gates remaining green.
