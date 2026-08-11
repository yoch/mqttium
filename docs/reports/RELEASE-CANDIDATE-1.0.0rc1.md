# Release candidate report — 1.0.0rc1

Candidate baseline: `fbf1887`. All candidate evidence is produced locally;
GitHub workflow runs are explicitly non-authoritative.

## Simplification

- `ApplicationDelivery` is the sole owner of application iterator/callback
  queues, delivery byte reservations, callback worker lifetime, reset/shutdown,
  and delivery statistics.
- The public Stable surface and constructor defaults are locked by
  `test_public_api_surface.py` and remain unchanged.
- `AsyncClient` fell from 2,148 to fewer than 1,800 lines (over 15%). Its
  constructor no longer carries a C901 exception, and `_apply_effect` is below
  complexity 30.
- Paired micro scenarios are independent registry entries; the former worker
  C901 dispatch is gone.

## Performance evidence

The retained local A/B baseline is `fbf1887`. Mode-specialised delivery was
introduced only after profiling showed that a generic controller boundary added
eight Python calls per accounted message. Eleven CPU-pinned pairs on the final
unaccounted delivery paths measured candidate/base medians of:

| Scenario | Candidate/base | Baseline CV |
| --- | ---: | ---: |
| callback | 1.160 | 6.22% (invalid for a numeric claim) |
| iterator | 1.120 | 3.88% |
| both | 1.098 | 3.62% |

The exact call/allocation profiler is part of the local runner. A targeted
strict open-loop run passed with completeness `1.0004` and candidate/base loop
lag `0.7767`. A complete valid `performance` profile is still required before
promotion to `1.0.0`; no exhaustive network gain is claimed.

## Issue #77

`paired_network.py` now persists policy, thresholds, runner eligibility,
status, failures, partial scenarios and Markdown before every controlled exit.
`advisory` is visible but returns zero; `strict` returns 1 for a regression and
2 for an invalid runner or sample. Worker non-zero exits, timeouts and malformed
output are classified as invalid measurements rather than escaping before the
report is written.

The original false-green behaviour is fixed. An overhead audit then found that
the subscriber reader shared the publisher's GIL and inflated delivery p50 from
about `0.76 ms` to `29.31 ms`. The reader now runs in a separate process, the
delivery observer no longer adds a second PUBACK stream, closed-loop cells
target a calibrated duration, and open-loop calibration uses the measured path.

A final strict A/A control on a preflight-eligible host rejected
`paired_network.py` as a release gate: its four representative baseline CVs
ranged from `8.49%` to `23.05%`, above the 5% validity ceiling, even though the
A/A medians remained near neutral. The tool remains an explicit advisory
diagnostic; local release decisions use the stable micro and open-loop gates.
Issue #77 can therefore close with its original false-green bug resolved and
the unreliable strict-gate option deliberately not adopted.

## Robustness and packaging

The local first-RC profile requires:

- Ruff, mypy, Bandit, 87.36% coverage and mandatory broker integration;
- memory thresholds, application stress and resource-aware soaks;
- strict local micro/open-loop measurements and an advisory closed-loop network
  diagnostic on an eligible machine;
- validated wheel/sdist plus isolated TCP, TLS, WebSocket, Unix, SQLite, Paho
  VERSION2 and clean-shutdown smokes.

The local `quick` manifest is green: 694 tests, 87.61% coverage, mandatory
integration, all memory thresholds, application stress and 30-second reconnect
soaks for both protocols. The isolated wheel passed every required transport,
SQLite, Paho and shutdown smoke. Python 3.11–3.14 and EMQX/HiveMQ are covered by
one final GitHub matrix after the source is clean. Multi-hour fuzz and soak runs
are deferred until after the first RC. Issue #77 no longer blocks the first RC:
its strict closed-loop gate was rejected by its own A/A control.
