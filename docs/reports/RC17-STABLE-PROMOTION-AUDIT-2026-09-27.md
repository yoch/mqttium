# RC17 stable-promotion audit — 2026-09-27

## Decision and scope

**Hold promotion to final 1.0.** The completed checks below are positive, but
long-duration qualification, the disposition of issue #493, and real-application
migration evidence are still outstanding. This is a dated audit checkpoint,
not a completed stable-release qualification certificate.

Audited candidate: `v1.0.0rc17`,
`c9bab1ad93dd2e875c07706aecdeaa69eb8dce89`, also local `HEAD` and the remote
`main` commit inspected for this audit. RC16 comparison baseline:
`681cc01523f56db81cca1943ee89ef2c3e5388b5`.

The review covers the RC16-to-RC17 changes to outbound admission and settlement,
packet-identifier and flow ownership, inbound replay and acknowledgement,
reader retirement, the settled-effect optimization, message construction,
public models, and the release/documentation contracts. It also reruns the
existing regression suites and checks retained external evidence. No new
runtime defect was confirmed in this bounded review. This is not an exhaustive
proof of the implementation or validation of an actual downstream application.

Tracked sources were unchanged during runtime testing. Pre-existing untracked
user files were left in place, including the earlier September 22 audit; its
historical defects are not asserted to remain present in RC17. Local artifacts
are retained under `/tmp/mqttium-rc17-audit-20260927/` and are not committed.

The tracked `src/` fingerprint is
`2208f46b0ec4ad2fecfe5f9ea3e5fcb1cc5c36d11f9bae167ef6de4a8f648c70`.
It hashes sorted tracked paths followed by a NUL, file bytes, and a NUL for
each entry. The evidence directory contains `source-identity.json`.

## Promotion conditions still open

| ID | Condition | Evidence and required closure |
| --- | --- | --- |
| G01 | Long-duration qualification is incomplete | The two campaigns launched below are running. Require successful completion, exact-source manifests, and retained artifact digests. Short soak results do not satisfy this condition. |
| G02 | Saturation latency and measurement reliability remain unresolved | [Issue #493](https://github.com/yoch/mqttium/issues/493) remains open. A reliable RC14-to-candidate comparison and an explicit fix-or-accept decision are still required. RC17-versus-RC16 results do not settle this question. |
| G03 | Real-application migration has not been validated | The RC17 evidence report explicitly lists this as missing. API-surface tests and shipped examples support the contract, but do not establish successful migration of a real application. Validate representative callback/iterator, receipt/batch, Will and persistence use before freezing 1.0. |
| G04 | Final release metadata and exact-artifact qualification remain future work | RC17 is correctly marked Beta and has an RC version. Final 1.0 requires a new reviewed version, changelog/support/classifier updates, and validation of the resulting exact release commit and artifacts. Merely removing the GitHub pre-release flag is insufficient. |

G02 must preserve the uncertainty recorded in the issue: the reported
15–20% lag difference is a working estimate, not a certified measurement.
The prior same-code A/A failures and multi-level capacity calibration prevent
using a passing RC17/RC16 comparison as proof of parity with RC14.

The complete `local_release.py rc` profile was not run on this workstation.
The evidence below is recorded by component; it must not be relabelled as a
passed integral RC manifest. Local profiling does not supply controlled
performance A/A and A/B evidence.

## Confirmed documentation findings

### D01 — P2: migration guidance retains the old reconnect-hook ordering

`docs/migration.md:221` says automatic retry waits for the current
`on_disconnect` hook and then rechecks user intent. The implementation sets
the reconnect-ready event before invoking the hook
(`src/mqttium/api/_lifecycle.py`, `LifecycleHooks._invoke`), and the current
API contract explicitly permits reconnect while that hook is running.
`test_reconnect_proceeds_while_disconnect_hook_runs` covers that behavior.

An application following the migration paragraph could incorrectly treat hook
completion as a reconnect barrier. Clarify that retry waits for the hook to
start, and that applications needing stronger ordering must own that ordering.
This is a documentation contradiction, not a newly reproduced runtime defect.

The same guide also retains obsolete vocabulary after its RC17 changes table:
`state, epoch` at line 94 and writer `max_*` at line 102. Align these with
`connections`, `message_limit` and `byte_limit` before the final API freeze.

### D02 — P3: twelve RC17 migration rows render as one paragraph

The blank line at `docs/migration.md:57` terminates the Markdown table.
Lines 58–69, including the Will conversion and store-method support changes,
render as a single paragraph containing pipe characters. This was confirmed
both in the strict local MkDocs build and on the deployed RC17 migration page.

Remove the blank line or start a proper second table. Verify the rendered
table, because `mkdocs build --strict` and the current source-link tests both
pass with this defect.

### D03 — P3: the published surface-review link returns a 404

`mkdocs.yml:20` excludes reports except an allowlist which omits
`API-SURFACE-REVIEW-2026-09-24.md`. Both the reports index and the RC17 evidence
report link to that page. The deployed destination
`/en/v1.0.0rc17/reports/API-SURFACE-REVIEW-2026-09-24/` displays
`404 - Not found`; the generated local target is also absent.

Include the historical report in the site to repair existing immutable-report
links, or introduce an appropriate maintained redirect. Keep its historical
body unchanged. MkDocs reports the excluded link at INFO level, so strict
mode alone does not detect this as a failing build.

### D04 — P3: the release procedure names an obsolete stable fallback

`docs/release-process.md:126` still says the fallback points to RC14.
The deployed `stable` URL was followed successfully in the browser during
this audit and resolves to `/en/v1.0.0rc17/`. Update the maintained procedure
to reflect the current setting or describe it without a stale fixed version.

## Completed checks

Local environment: Linux x86_64, Python 3.12.13; Ruff 0.16.1, mypy 2.3.0,
Bandit 1.9.4, pytest 9.1.1, Hypothesis 6.165.0 and MkDocs 1.6.1.

| Check | Result |
| --- | --- |
| Ruff format and lint over `src tests benchmarks tools` | Passed |
| mypy over `src/mqttium` | Passed |
| Bandit `-q -ll -r src` | Passed |
| Strict MkDocs build | Passed; the rendered defects above still exist |
| Unit, project, mandatory Mosquitto integration, and resilience suites | **2,491 passed**, no skips reported; 114.85 seconds pytest time |
| Combined-suite branch-aware coverage | **93.49%**, above 89%; this is combined-suite coverage, not unit-only coverage |
| Deterministic fuzz, seed 17, 20,000 iterations per codec/engine/WebSocket target | Passed: 60,000 iterations total |
| Hypothesis and stateful invariants, 6 seeds × 200 steps for stateful tests | **28 passed** |
| Runtime, composition and pressure fuzzer regression tests | **71 passed** |
| Pinned SHA-256-verified TLC | **58/58 expected outcomes matched**, including the declared historical counterexamples |
| Publication artifact validation, strict Twine check, wheel-content check | Passed on the already-built GitHub artifact |
| Wheel/source comparison | All **62 Python modules** byte-identical to the audited source; no mandatory runtime dependencies |
| Existing release runner's robustness phase | Passed: call/allocation profile, memory profile, all 13 memory thresholds, application stress, and 30-second reconnect soaks for MQTT 3.1.1 and 5 |

The robustness-only manifest is retained in `robustness/manifest.json` with
profile `audit-robustness-only`; it is not a full `rc` profile. These runs do
not replace the long soak or establish a performance comparison. The
property-heavy outbound scenario is within its threshold but has only about
0.96 MiB headroom (20.04 MiB measured, 21 MiB threshold) on this environment.

An initial sandboxed test attempt was interrupted after failures/timeouts;
the environment disallowed local sockets. The complete suite above was rerun
with local networking enabled and an audit-owned Mosquitto process, cleaned
up afterward. The initial TLC attempt could not write its default cache;
the successful run used a temporary cache and the pinned official JAR.
Those environmental attempts are retained separately and are not counted as
product failures or successful qualification.

### Invariant review anchors

| Invariant | Reviewed paths and regression evidence |
| --- | --- |
| Single writer | Writer/effect ownership remains in `WritePump`; writer and effect suites passed. |
| Receipts before wire | Settled-effect shortcut and receipt registration ordering reviewed; effect-drain, publish and receipt regressions passed. |
| Owned bytes | `_decoded_message` is used on decoded/store-owned data paths; decoder boundary and decoded-message regressions passed. |
| Packet identifiers distinct from flow slots | Per-mid `FlowControl` ownership and subscription cleanup reviewed; sealed-id and parked-quota regressions passed. |
| One state owner | Outbound rollback/sealing and inbound derived occupancy reviewed; transaction, store-failure, quota and session-state tests passed. |
| Callbacks outside critical sections | Reader retirement and lifecycle ownership reviewed; project lock-discipline and lifecycle tests passed. |
| No in-session retransmission | Replay changes remain connection/session recovery paths; replay and QoS phase tests passed. |

This mapping identifies review and test coverage; it does not claim every
possible interleaving has been proven correct.

## External qualification and source equivalence

The exact release commit has successful
[CI 36271960868](https://github.com/yoch/mqttium/actions/runs/36271960868) and
[ARM64 CI 36271960875](https://github.com/yoch/mqttium/actions/runs/36271960875).

The prior RC17 qualification used runtime commit
`4782dfb7ff11ee008890b4eae39387ed91e7182e`. Comparing it to the release commit
shows only release-facing documentation/metadata changes and the version
assignment in `src/mqttium/__init__.py`; other runtime, test, benchmark and
formal files are unchanged. Its retained results therefore support the
unchanged runtime paths, subject to their original scope.

The following archives were downloaded, their bytes checked against the
GitHub artifact SHA-256, and their result records inspected:

| Evidence | Result | Artifact SHA-256 |
| --- | --- | --- |
| [Strict network 36270053544](https://github.com/yoch/mqttium/actions/runs/36270053544) | `passed`, no recorded failures; RC16 baseline | `7fae58b75f037f5aec915fbbf618ab31cd790344a3e14c441b7241ece600f5fd` |
| [Open-loop 36267803906](https://github.com/yoch/mqttium/actions/runs/36267803906) | `passed`, no recorded failures; RC16 baseline, #493 caveats retained | `0679f2d6b04092d7cae78f3262add98ad9eb20038e29942464363294784feb15` |
| [Paired regression 36267722844](https://github.com/yoch/mqttium/actions/runs/36267722844) | Workflow passed; retained writer-capacity A/A and A/B records passed | `a1d86c051da0cf5fd754ac3dfd8ec469b8c11d70c4c7ec721f03eff5f3396bc1` |
| [Short soak/interoperability 36267800944](https://github.com/yoch/mqttium/actions/runs/36267800944) | Eight protocol/broker/platform results; each published and received 11,000 messages, with no idle/discrete-resource violations | Six archive digests retained in `retained-artifacts.json` |

The earlier soak results lasted approximately 8–17 seconds each. They must not
be cited as multi-hour evidence. Statistical gate decisions above were
inspected, not independently recalculated from all raw samples.

## Campaigns launched by this audit

Every run below was checked to resolve to the exact RC17 commit.

| Campaign | Configuration | Status at checkpoint |
| --- | --- | --- |
| [Long fuzz 36293924785](https://github.com/yoch/mqttium/actions/runs/36293924785) | Tag `v1.0.0rc17`; `rc-24-cpu-hours`; base seed 17; five 288-minute shards | Running; approximately 4 h 48 min per shard, 24 aggregate worker-hours, not measured CPU hours |
| [Soak/interoperability 36293926073](https://github.com/yoch/mqttium/actions/runs/36293926073) | Tag `v1.0.0rc17`; Linux 7,200 seconds per protocol; 500 messages/cycle; short macOS/EMQX/HiveMQ runs | Linux running; both macOS and both broker-interoperability jobs passed |
| [ARM64 runtime 36293927726](https://github.com/yoch/mqttium/actions/runs/36293927726) | Trusted `main`; 50,000 seeds × 32 operations | Passed: 50,000 seeds, 1,600,000 operations, zero failures |

The ARM64 manifest records seed interval `[3750000, 3800000)`, a clean source
tree, Python 3.14.7 and Raspberry Pi 5. Its archive SHA-256 is
`e4d5115ccdece0e95ddf0909b950f75426a29fa92ef054621e8c920a2335b83f`.
The schedule suite is bounded and does not replace the long deterministic fuzz
or resource-soak campaigns.

## Publication state and artifact identity

The GitHub RC17 pre-release and PyPI distributions are public. The
[PyPI workflow 36293641588](https://github.com/yoch/mqttium/actions/runs/36293641588)
has passed its build, wheel checks on Python 3.11–3.14, Python 3.12 sdist smoke,
resilience smoke, publishing job and post-publication PyPI verification.
It initially waited for the protected `pypi` environment during this audit,
then completed successfully. No publication approval was performed by this
audit. The public PyPI version endpoint was checked after completion: both
file digests match the already-audited build artifacts exactly.

The artifact downloaded for inspection is the same `python-distributions`
archive destined for the publishing job; no second release build was made.

| Artifact | SHA-256 |
| --- | --- |
| GitHub publication archive | `26326f7db90a4d94bb1720c4e5cd0edeb104b41e4210f924c01108226c6a0891` |
| `mqttium-1.0.0rc17-py3-none-any.whl` | `8da1cbf61a1564c9b72d1d4e7e5ff19b7e630586b6c0c82960d39d33f8d0bc53` |
| `mqttium-1.0.0rc17.tar.gz` | `fca7df82bafc6ed0b19e148c51a29af5b92ae8cdf79e9c54d651f02de3ca298a` |

For final 1.0, retain the successful long-campaign archives and migration
record, close the #493 decision, correct the maintained documentation, then
qualify the exact new release commit and its artifacts. Any intervening runtime
change requires qualification of the paths it affects. Record later outcomes
in a new dated follow-up rather than rewriting this checkpoint.
