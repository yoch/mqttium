# Release candidate report — 1.0.0rc12

Candidate source before release metadata:
`8b78ec19626e7cb155b1d5bb28a14f0744750c4f` (`main`, 2026-09-02).
Selected runtime head before merge:
`ce120da2982b0e5c03247efc1c3f5ac3337976f4`.
Release metadata commit:
`4e52384af0da6ae51547d1e29f60df99158feee3`.
Hosted-candidate and ARM64 workflow-alignment commit:
`1a2a80e58e1f19a0ad3d2e4eb241b2d081126352`.

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

The exact RC metadata/workflow head `1a2a80e` passed the same hosted matrix in
[CI run 33623673079](https://github.com/yoch/mqttium/actions/runs/33623673079),
[soak run 33623673895](https://github.com/yoch/mqttium/actions/runs/33623673895),
and [distribution run 33623672905](https://github.com/yoch/mqttium/actions/runs/33623672905).
The publication workflow built one wheel and one sdist, then passed wheel
smokes on Python 3.11–3.14, the Python 3.12 sdist smoke, and the
installed-artifact resilience smoke. Publication jobs were correctly skipped.

The downloaded Actions distributions have these SHA-256 values:

- wheel: `49fc4ff6b8adaedb285dda3564c617c4ba1c580a1abadd2722c6d4673aea702d`;
- sdist: `0019b6c725e0037a23b47a88aea02908168fb4d8d138f601e5889d581003a265`.

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

## Local validation boundary

No heavy local gate was run for this candidate because the available x86-64
host was under unrelated load and the maintainer explicitly reserved precise
validation for the ARM64 GitHub runner. The focused workflow-contract test
passed locally after the Python 3.14 alignment.

The hosted matrix and tournament qualify this preparation PR for merge, but do
not waive the maintained release procedure. Tagging and publication remain
blocked until the post-merge ARM64 CI passes and the clean local candidate gate
is completed on an eligible host with `v1.0.0rc11` as its approved baseline.

## Release decision

RC12 is recommended for merge as a prepared candidate. It is not yet approved
for tagging or prerelease publication. The two remaining publication gates are
the post-merge ARM64 general CI and the clean local candidate gate described
above.

This report-only addition changes no runtime source. The exact distributions
above were built from `1a2a80e`; the report commit therefore does not alter the
validated runtime package contents.
