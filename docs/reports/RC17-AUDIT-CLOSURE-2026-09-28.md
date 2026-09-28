# RC17 audit closure — 2026-09-28

## Decision and scope

**RC17 audit closed with an unresolved performance qualification finding.**
No new runtime defect was confirmed; #493 remains open. This closes the bounded
audit of the already-published RC17 candidate,
`c9bab1ad93dd2e875c07706aecdeaa69eb8dce89`. It does not prepare or authorize a
new release or certify a downstream production deployment. Runtime source is
unchanged by the audit follow-up; PR #588 contains documentation and benchmark
corrections together.

## Functional evidence

The [initial audit](RC17-STABLE-PROMOTION-AUDIT-2026-09-27.md) and
[long-campaign closure](RC17-LONG-CAMPAIGN-CLOSURE-2026-09-27.md) preserve
the exact-source evidence: 2,491 unit/project/integration/resilience tests,
93.49% combined
branch-aware coverage, 58 expected TLC outcomes, 37 million long-fuzz
iterations, the two protocol-specific two-hour Linux soaks, and ARM64 schedule
coverage. No new runtime defect was confirmed during this follow-up.

Those checks retain their named scope and environment. They are not a proof
of every application integration or a controlled local performance result.

## Documentation

The maintained guides now match the implemented reconnect-hook ordering,
renamed public fields and immutable result models. All 22 RC16-to-RC17
migration rows render in one table. The historical API-surface review is
included in the built site. Examples clarify admission/ACK deadlines, partial
batch failure, manual acknowledgement and MQTT 5 Wills. Release instructions
cover PyPI verification and the published-tag Read the Docs fallback.

The strict documentation build and project checks pass. These are corrections
in the PR's source tree; immutable published release tags were not moved.

## Performance evidence

The original small-payload +15–20% schedule-lag claim does not reproduce at
fixed rates below saturation. This does not prove general RC14/RC17 parity.

The expanded matrix exposed a 4096-byte completed-throughput failure under
the older rule. An instrumented repeat showed backlog before the final
observer join. Crucially, an identical-code RC17/RC17 campaign also failed
that rule, so a median-only “confirmed regression” verdict was not reliable.

The corrected harness enforces eligible preflights, keeps lag verdicts below
the measured saturation regime, requires same-code controls and confidence
bounds for throughput suspects, and treats inconclusive evidence as invalid.
It also retires completed observer tasks during acquisition instead of
retaining an entire sample's task graphs. Thresholds and completion guarantees
are unchanged; legacy observer retention remains an explicit diagnostic mode.

The strengthened rule, still using full observer retention, returned `invalid`
in [run 36401521973](https://github.com/yoch/mqttium/actions/runs/36401521973).
All six final preflights were eligible and 16,512,000 publications completed.
Three 4096-byte cells had A/B intervals crossing 0.97 and same-code controls
outside the ±2% budget. This confirms unresolved measurement uncertainty,
not a product-regression or parity verdict. The
[diagnosis report](LOOP-LAG-493-DIAGNOSIS-2026-09-27.md) retains the raw-result
digests and earlier failed acquisitions.

The final two acquisitions used harness
`79268e55dec1984316eb27893f5fa7488815ab27`; its full runtime source is identical
to the RC17 tag:

| Acquisition | Artifact verdict | Audit interpretation |
| --- | --- | --- |
| [A/A 36402364148](https://github.com/yoch/mqttium/actions/runs/36402364148) | Invalid | Four eligible final preflights; 9,600,000 publications completed. Same-code dispersion at MQTT 5 / 4096 B / 26k exceeds the equivalence budget. Observer retirement reduces bookkeeping but does not resolve that dispersion. |
| [RC14/RC17 36404229046](https://github.com/yoch/mqttium/actions/runs/36404229046) | Passed under its initial screen | Three eligible final preflights; 5,856,000 publications completed. The earlier 24k failure does not reproduce, but two 26k intervals remain too wide to qualify. Their passing medians incorrectly bypassed confirmation. |

The last finding is corrected in the PR: throughput confirmation is now
required if the initial median **or lower 95% bound** is below 0.97.
Deterministic replay selects both uncertain 26k cells, and a regression test
covers the observed passing-median case. This correction has not yet completed
a fresh dedicated acquisition. The historical artifact still says `passed`;
it is not silently relabelled or cited as final qualification.

This is evidence from one Pi 5, Python 3.14.7 and the recorded broker/workload.
Low-rate CPU measurements are diagnostic and do not establish a general 3%
CPU-equivalence claim. No publication loss was observed in these acquisitions.

## Disposition

The requested RC17 audit is complete with the performance reservation above.
[PR #588](https://github.com/yoch/mqttium/pull/588) retains all corrections and
evidence together, without a new release, tag change or runtime optimization.
The PR remains open for the unresolved measurement work; this report is not
an instruction to merge it or to promote RC17 to final 1.0.

[Issue #493](https://github.com/yoch/mqttium/issues/493) remains open. Before
claiming stable performance qualification, the final screen needs fresh paired
and same-code evidence with adequate precision. If saturation remains unstable,
its backlog and observer-scheduling regimes need further isolation; discarding
the 26k workload or relaxing the threshold would not resolve the finding.
A single later passing acquisition cannot erase the retained same-code failures.

The functional and documentation conclusions do not certify an end-to-end
downstream application migration. Final 1.0 metadata and exact-artifact
qualification also remain separate future work. Neither requires a production
deployment as part of this bounded RC17 audit.

## Earlier checkpoint conditions

| Item | RC17 audit disposition |
| --- | --- |
| G01 — long-duration evidence | Closed by the successful exact-source fuzz and soak campaigns. |
| D01–D04 — documentation findings | Corrected and validated in PR #588; published tags remain immutable. |
| G02 — performance measurement and #493 | Open finding: same-code saturation variance and an initial-screen defect prevent final qualification. The screen defect is corrected and regression-tested; a fresh campaign remains necessary. |
| G03 — complete downstream application migration | Not certified by this audit. Isolated compatibility checks do not establish an end-to-end production migration. |
| G04 — final 1.0 metadata and exact artifacts | Outside the requested RC17 audit scope; no new release is prepared or authorized here. |
