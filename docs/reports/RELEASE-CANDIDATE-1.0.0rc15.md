# Release candidate 1.0.0rc15 evidence

Date: 2026-09-23. Candidate runtime: `main` at
`ed529654cca2b712d525bea4665c62da948e0ffd`. The release commit changes only the
version string, the frozen changelog section, release-facing installation and
support text, and this report. Baseline for comparisons: `1.0.0rc14`
(`c194597bcf5af4951fbec2b560600eef3cb84b3c`).

RC15 is the first published candidate of the pre-v1 native API that
deliberately revises RC14 (see the [migration guide](../migration.md)). It is a
pre-release (`Development Status :: 4 - Beta`), not a final 1.0.

## Blockers closed before the cut

An external audit of `b9fad62` (2026-09-23) reported two functional defects and
one misleading qualification artifact. Each was reproduced on that commit,
fixed with permanent regressions that fail on the unfixed code, and merged with
green pre- and post-merge CI:

| Finding | Pull request | Merge |
| --- | --- | --- |
| One-shot topic iterables rejected after admission waited for earlier effects (regression from #470) | [#490](https://github.com/yoch/mqttium/pull/490) | `a3afa35` |
| A failed SQLite batch rollback left a reusable open transaction | [#491](https://github.com/yoch/mqttium/pull/491) | `5322e0d` |
| The local release manifest could report `passed` after a timeout or launch failure | [#492](https://github.com/yoch/mqttium/pull/492) | `ed52965` |

## Qualification of `ed52965`

| Evidence | Result |
| --- | --- |
| [CI 35843276430](https://github.com/yoch/mqttium/actions/runs/35843276430) and [ARM64 CI 35843276392](https://github.com/yoch/mqttium/actions/runs/35843276392) | Passed (lint, types, security, strict docs, unit and project tests with branch coverage, integration, resilience, fuzz, macOS and Windows) |
| [Soak and broker interoperability 35843636903](https://github.com/yoch/mqttium/actions/runs/35843636903) | Passed: Linux and macOS soaks for MQTT 3.1.1 and 5, EMQX 5.8.9 and HiveMQ CE 2026.5; asyncio tasks, file descriptors and RSS stable per cycle |
| Strict ARM64 network gate, [35843633602](https://github.com/yoch/mqttium/actions/runs/35843633602) | Invalid: the candidate same-code control exceeded its equivalence band at window 1; no A/B conclusion |
| Strict ARM64 open-loop gate vs RC14 | Two failed runs and one passed run; accepted finding below |

### Accepted open-loop finding

Read at matched target rates, the candidate's publisher event-loop lag (p95) is
about 15–20 % higher than RC14 at saturation for 64-byte payloads (about
+0.1–0.15 ms near 26,000 messages/s), for MQTT 3.1.1 and 5. Same-code A/A pairs
at those rates stay within 1.04–1.06. Throughput is unchanged, and no
difference is measurable below about 22,000 messages/s. Comparing earlier
commits against the candidate places the effect mainly in the lean-native
rewrite (`6fd09d8`, part of #457); the 2026-09-22/23 audit fixes add nothing
measurable.

The maintainer accepted this for RC15, in line with the rewrite's documented
trade-offs. [Issue #493](https://github.com/yoch/mqttium/issues/493) tracks the
investigation before 1.0.

The same analysis showed that the strict open-loop gate is not reliable on this
runner as currently operated: a same-code A/A run failed once in three, and
calibrated capacity moves between discrete levels, placing fractional loads in
different lag regimes. Its individual pass or fail verdicts are therefore not
cited as evidence in either direction; issue #493 also tracks making the
measurement reliable.

## Not performed for this candidate

- The local `benchmarks/local_release.py rc` profile was not run: the
  maintainer workstation was unavailable for benchmarking. Remote CI, the ARM64
  runner and the publication workflow's validation build, wheel smoke and
  resilience smoke provide the evidence above instead.
- Multi-hour fuzzing and soak campaigns, and validated migration of real
  applications, remain required promotion evidence for a final 1.0, not for
  this candidate.
