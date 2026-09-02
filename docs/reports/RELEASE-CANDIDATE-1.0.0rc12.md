# Release candidate report — 1.0.0rc12

Candidate source before release metadata:
`8b78ec19626e7cb155b1d5bb28a14f0744750c4f` (`main`, 2026-09-02).
Selected runtime head before merge:
`ce120da2982b0e5c03247efc1c3f5ac3337976f4`.
Release metadata commit:
`4e52384af0da6ae51547d1e29f60df99158feee3`.
Hosted-candidate and ARM64 workflow-alignment commit:
`1a2a80e58e1f19a0ad3d2e4eb241b2d081126352`.
Final pre-report candidate and release-harness commit:
`5f045f414cd8ab4d24db153e9424793a89f875a0`.

## Scope

RC12 contains the reviewed QoS 1 publication and callback-response work merged
after `v1.0.0rc11`:

- reuse one mutation-free QoS 1/2 publication preparation between resident
  writer preflight and protocol admission;
- preserve effect ordering when an inline synchronous `on_message` callback
  publishes reentrantly; and
- give fixed success PUBACK, PUBREC and PUBCOMP frames one eager permit that is
  independent from the application-data permit, with both permits bounded to
  one write of their kind per event-loop turn.

The package version is `1.0.0rc12`. Its release tag must be exactly
`v1.0.0rc12`, and the GitHub release must be marked as a prerelease.

## PR 417 disposition

[PR 417](https://github.com/yoch/mqttium/pull/417) was closed as superseded by
[PR 420](https://github.com/yoch/mqttium/pull/420). Three of its four commits
were integrated identically through PR 420. Its only unique commit replaced the
named six-field `_PreparedPublish` carrier with a positional tuple based on a
local synthetic admission benchmark.

That residual change is intentionally excluded. It had no controlled ARM64 or
network evidence, its benchmark did not exercise broker acknowledgements, and
positional unpacking weakened readability and type-assisted review in a
transaction-sensitive path. RC12 retains the `NamedTuple` representation.

## Hosted matrix and exact distributions

The exact runtime head `ce120da` passed the complete PR 420 matrix: repository
quality, Python 3.11–3.14, Linux MQTT 3.1.1 and MQTT 5 soaks, resilience, fuzz,
package validation, macOS 3.11/3.14, Windows 3.11/3.14, Codecov, Read the Docs,
and the `CI required` aggregate.

The exact final pre-report head `5f045f4` passed the same hosted matrix in
[CI run 33652259920](https://github.com/yoch/mqttium/actions/runs/33652259920),
[soak run 33652260020](https://github.com/yoch/mqttium/actions/runs/33652260020),
and [distribution run 33652259884](https://github.com/yoch/mqttium/actions/runs/33652259884).
The publication workflow built one wheel and one sdist, then passed wheel
smokes on Python 3.11–3.14, the Python 3.12 sdist smoke, and the
installed-artifact resilience smoke. Publication jobs were correctly skipped.

The downloaded Actions distributions have these SHA-256 values:

- wheel: `49fc4ff6b8adaedb285dda3564c617c4ba1c580a1abadd2722c6d4673aea702d`;
- sdist: `2a2f0836aeaa3af93d54f99240cbfe3fb0acb4f1138c50aa21357944f64bc1d5`.

## ARM64 decision evidence

[Tournament run 33595474985](https://github.com/yoch/mqttium/actions/runs/33595474985)
tested exact selected runtime commit `ce120da` on the dedicated ARM64 runner
with valid same-code controls. Candidate/base ratios were:

- callback-response ACK p50: 0.50488, a 49.51% reduction;
- callback-response throughput: 1.14233, a 14.23% increase;
- network capacity QoS 0: 0.98528, a 1.47% reduction; and
- network capacity QoS 1: 0.99847, a 0.15% reduction.

All decision-cell coefficients of variation were below 5%. Baseline A/A ratios
were 0.99752 for callback response and 1.00251 for its operation rate; network
capacity A/A ratios were 1.00024 at QoS 0 and 1.00000 at QoS 1. A 64-ACK burst
used one eager ACK write, confirming that the latency gain does not remove the
per-turn fairness bound.

The ordinary ARM64 CI run on merged source stopped before tests because its
workflow still required the runner's former Python 3.13. RC12 aligns all
system-Python ARM64 workflows and their maintained runner documentation with
Python 3.14. The repository's trust boundary permits the general self-hosted
workflow to run only from `main`, so this correction must be confirmed by the
automatic post-merge run before publication.

## Local candidate evidence

The local `rc` acquisition at `e942e57`, with `v1.0.0rc11` as its baseline,
passed quality, unit coverage, required Mosquitto integration, the controlled
micro suite, the strict 32-cell open-loop gate, call/allocation and memory
profiles, memory thresholds, application stress, both 30-second reconnect
soaks, distribution build, Twine, wheel-content, isolated installation,
`pip check`, and import of every packaged module. The generic closed-loop
network artifact was advisory and invalid because several cells exceeded its
CV limit; no conclusion in this report uses that artifact.

That run stopped at the first installed-wheel smoke because the release
harness read the expected version from an unrelated editable environment
(`1.0.0rc11`) instead of the isolated wheel it had just installed
(`1.0.0rc12`). Commit `5f045f4` corrects only that version source. The isolated
wheel reports `1.0.0rc12`, and the exact hosted distribution run above passes
all TCP, resilience, wheel and sdist smokes on the corrected commit.

A later local repetition was stopped once the host was no longer reserved.
Its partial results are not release evidence. No timing result acquired after
that point is cited. The accepted evidence therefore composes the complete
functional, robustness and strict open-loop results from `e942e57` with exact
hosted package validation at `5f045f4`; the intervening change is confined to
the release harness and does not alter MQTTium runtime code.

## Release decision

RC12 is recommended for merge. Tagging and prerelease publication remain
conditional only on the automatic post-merge ARM64 general CI passing on
`main`.

This report-only addition changes no runtime source. The exact distributions
above were built from `5f045f4`; the report commit therefore does not alter the
validated runtime package contents.
