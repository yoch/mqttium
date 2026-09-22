# Native API integration and release preparation — 2026-09-22

This is the integration checkpoint recorded at 10:45 UTC. It is not a release
authorization or a claim that the long qualification campaigns have finished.
Later campaign closure belongs in a new dated report, linked from the archive
index, without rewriting this checkpoint.

## Decision and immutable identities

PR [#457](https://github.com/yoch/mqttium/pull/457) was merged into `main` with
a merge commit after its required checks passed. Recommendation: release
**1.0.0rc15** first because the native API deliberately breaks RC14 contracts.
**1.0.0** remains a possible later decision once qualification is complete and
the maintainer accepts the final Stable API boundary.

No version change, tag, GitHub release or PyPI publication was made. The source
version and built artifact filenames still say `1.0.0rc14`; these preparation
artifacts must never be uploaded under that already-published version.

| Identity | Commit or tree |
| --- | --- |
| Published RC14 baseline | `c194597bcf5af4951fbec2b560600eef3cb84b3c` |
| Original reviewed #457 head | `0725be1cf096927b4bdc5e58ac2f2e4144a46292` |
| Measured final runtime | `8ff3acb5fce8f617013ec67a695c62c365d683c9` |
| Main/Codecov merged into #457 | `c4d8306e529fdc5ee0260276b83437c39e09a493` |
| Final #457 head | `3ba3880c8f81c27d567d33a99abbecfdd26e70fa` |
| #457 merge into main | `852d572a9ba20ad6cb0b3fb4d356cd241a092443` |
| Shared `src` tree for the five #457/runtime identities above | `caca4c48678b6335056dae0de57c60089de8d84f` |
| `tests/fuzz` tree at the long-fuzz start and integration | `b8f539f843865671fdf9ff7a2f27f067e4cee981` |

The finalization checkout was isolated from the maintainer's existing branch
and untracked files. After the workstation restriction, all benchmark work was
kept remote. An initially started local test/fuzz/soak batch was stopped when
the workstation was reserved; its partial output is not counted as a pass.
Remote Actions and the dedicated ARM64 runner provide the completed evidence.

The follow-up accompanying this report corrects three Internal-tier docstrings
in `packets/acks.py`, `packets/publish.py` and `protocol/engine.py`. Their ASTs
match the integrated source after removing module/function docstrings. This
changes the raw source fingerprint but no executable statements, signatures,
defaults or constants used by the runtime. Any subsequent behavioral change
requires qualification of its affected paths.

## Applied PR triage

| PR | Applied decision and reason |
| --- | --- |
| [#461](https://github.com/yoch/mqttium/pull/461) | Merged first as `05f07af9991f99865d5914d690b85659dfae4efe`. Codecov v7.1.0's annotated tag resolves to the pinned `0b35c9ecc4f0529d0eb674914510c22f85b196b4`; checks passed. |
| [#443](https://github.com/yoch/mqttium/pull/443) | Closed as superseded. The current reentrancy regression covers 1, 2, 3 and 5 messages, MQTT 3.1.1/5, QoS 0/1 and normal/eager task factories. Obsolete scheduler assertions were not restored. |
| [#456](https://github.com/yoch/mqttium/pull/456) | Closed as rejected, following its final evaluation. The first-inline experiment's batch-two regression and extra runtime complexity did not justify adoption. Branch, report and measurements remain available; no runtime was recovered. |
| [#457](https://github.com/yoch/mqttium/pull/457) | Merged with history preserved after verifying head `3ba3880`. The description records the native contract, actual validations and the publication boundary. |

Each decision was recorded on GitHub with its rationale. No original open PR
remained after this triage; a documentation/qualification follow-up carries this
checkpoint and the remaining infrastructure correction.

## Contract and documentation audit

The maintained English guides, reference tables, examples, support/security
policy, contribution guidance, provenance, changelog and RC14 migration now
describe the source API. Historical reports retain their original bodies.

| Contract | Verified documentation and implementation/test boundary |
| --- | --- |
| Synchronous callbacks | Inline on the delivering reader, outside protocol locks; routes frozen at the first connection attempt; asynchronous application work uses the iterator or owned tasks. Reentrancy, callback quantum and routed dispatch regressions cover these boundaries. |
| Lifecycle hooks | Separate asynchronous lifecycle ownership; hooks may await connection/client operations without waiting for their own triggering effect. Lifecycle takeover and stale-epoch tests remain authoritative. |
| Receipts | Publication receipts precede wire exposure; progressive batch admission retains a receipt for the committed prefix. Waiter, reconnect and aggregate-progress tests cover completion. |
| Memory pressure | Count/byte limits have distinct owners. `max_iterator_bytes=None` disables iterator byte accounting as well as its byte limit; occupancy/high-water byte counters remain zero. Finite limits retain exact logical accounting. |
| Reconnect and manual acknowledgement | Iterators retain their delivery generation across automatic reconnect. ACK handles identify the logical exchange. Manual QoS 1 PUBACK and final QoS 2 PUBCOMP wait for the application; PUBREC precedes QoS 2 delivery. |
| SQLite | Schema 5 accepts only fresh/current databases; incompatible formats are refused. The persistence guide retains explicit filesystem/DB-API/conversion exception boundaries and recovery caveats. |
| Support tiers | Native client operations, models, receipts and results: Stable. Statistics and the two supplied stores: Provisional. Engine, codecs, transport implementations, records and extension protocols: Internal. No tier was implicitly promoted. |

Source-installation instructions accompany the new API examples. Installing
the published RC14 package points readers to its versioned documentation.
Generated `llms` metadata no longer describes the whole API as experimental.
Strict performance gates remain distinct from exploratory/diagnostic campaigns;
the documented candidate command includes the required `--cpu` argument.

## Completed validation

| Evidence | Result and scope |
| --- | --- |
| [Final PR CI](https://github.com/yoch/mqttium/actions/runs/35715512921) | Passed: Ruff formatting/lint, mypy, Bandit, strict documentation, project checks, unit coverage, resilience, seeded/property/stateful/runtime fuzz and required Mosquitto integration. Unit/project coverage run: **1,901 passed, 92.19% coverage**. |
| Python/platform matrix in that CI | Python 3.11–3.14 on Linux; macOS and Windows on 3.11/3.14. Mandatory Mosquitto integration: **13 passed per Linux Python version**. Eager-task/platform-specific skips are not claimed as executed coverage. |
| [Main CI](https://github.com/yoch/mqttium/actions/runs/35716294202) | Passed after integration at `852d572`. |
| [Artifact validation](https://github.com/yoch/mqttium/actions/runs/35715512953) | Wheel on Python 3.11–3.14 and sdist on 3.12 installed in isolation. Minimal distribution contents, public imports, no runtime dependencies, TCP/TLS/WebSocket/Unix/SQLite and shutdown smokes passed. Publication/PyPI verification jobs were intentionally skipped. |
| [Main ARM64 CI](https://github.com/yoch/mqttium/actions/runs/35716294036) | Exact checkout `852d572`, system Python 3.14: **1,880 unit, 27 resilience and 13 mandatory integration tests passed**. |
| [Manual macOS and broker interoperability](https://github.com/yoch/mqttium/actions/runs/35715555414) | Both macOS protocol jobs and both EMQX 5.8.9/HiveMQ CE 2026.5 jobs passed at `3ba3880`; 20 cycles and 500 messages configured. The two Linux two-hour jobs in the same run were still running at this checkpoint. |
| [Remote memory and application stress](https://github.com/yoch/mqttium/actions/runs/35716636859) | Passed at `852d572`: application/persistence stress, isolated memory scenarios and existing thresholds. Hosted timing is diagnostic, not eligible performance evidence. |

The focused extended reentrancy/project check also passed locally before the
workstation restriction (53 tests). Later full-matrix counts above are the
qualification record. A successful workflow does not turn skipped jobs into
successful validation.

## Retained artifacts and performance applicability

The following downloaded artifact archives matched GitHub's SHA-256 digests:

| Artifact | SHA-256 |
| --- | --- |
| [Distributions, 10688253990](https://github.com/yoch/mqttium/actions/runs/35715512953/artifacts/10688253990) | `b35cfe11d7384042f0cd9512d5298911d555284ef10db56f6c1a64eb83db2b78` |
| [Memory/stress, 10689400847](https://github.com/yoch/mqttium/actions/runs/35716636859/artifacts/10689400847) | `c490a71f67bf7264683c652d64a07c767242743d1578416fd97872d959a4e108` |
| [Earlier strict network, 10418129954](https://github.com/yoch/mqttium/actions/runs/35021115957/artifacts/10418129954) | `42b40f68fd813b8f1b378d63330c1abf602a24507eb968237afcee2b6c23655c` |
| [Earlier paired controls, 10420436223](https://github.com/yoch/mqttium/actions/runs/35025727962/artifacts/10420436223) | `055a93e597bb558731a2a976ed5d227f22eeee36bcf814c47407824d7d9b0dc0` |

The preparation wheel's SHA-256 is
`9df4fa7fb373fbdff0a2268c3490823001eed06005b1ce0510f0bc0be933c315`;
the sdist's is
`bfd6df91118742dfe25e02aa9b9dc681d35bf909a80bac9f603adbd656f370f3`.
These identify validation artifacts, not a publishable release version.

The older strict network run passed at `fb95a142257524c9730de23be7c5ad5872ef14a8`;
the paired writer controls passed at `8d3bc3c3`. Their source differs from the
integrated runtime in `_delivery.py` and `api/stats.py`. Consequently they are
retained as earlier bounded evidence, not blanket qualification of the final
iterator path. Their failed/invalid prior attempts remain retained. No old
microbenchmark ratio or cross-campaign comparison is promoted to a current
product performance claim.

Independent usage evidence in benchmark-repository
[#41](https://github.com/yoch/mqtt-python-client-bench/pull/41) and
[#42](https://github.com/yoch/mqtt-python-client-bench/pull/42) measured runtime
`8ff3acb5`; exact `src` tree identity supports its applicability to `852d572`.
The former is a matrix without ABBA; the latter records unresolved same-code
noise and offer-limited subscription comparisons, not a certified speedup.
Fresh dedicated-runner
qualification is tracked below to close the final candidate's strict controls.
Generated measurements, distributions and logs remain outside source control.

## Deployed documentation

[Read the Docs build 34694398](https://app.readthedocs.org/projects/mqttium/builds/34694398/)
successfully deployed `852d572` as `latest`. Browser inspection verified the
rendered source installation guide, navigation, reference links, search results
and a legacy uppercase-page redirect. An HTTP check returned 200 for **44 URLs**:
the `llms.txt` Markdown targets, reference pages, redirects and published links.
Both `llms.txt` and `llms-full.txt` contain current source-API content.

The initial automated HTTP 403 and browser text-download restriction were
client-specific: direct curl checks succeeded. A cached pre-merge home page
was distinguished from the deployed build with a cache-busting request.

The existing RC14 tag was activated and built as
[`v1.0.0rc14`](https://mqttium.readthedocs.io/en/v1.0.0rc14/) in
[build 34694537](https://app.readthedocs.org/projects/mqttium/builds/34694537/).
Read the Docs excludes prereleases from its automatic stable version. The
project now has a non-forced HTTP 302 exact redirect, `/en/stable/*` to
`/en/v1.0.0rc14/:splat`, so existing stable links reach the published contract.
`latest` continues to follow `main`. Update that fallback after a future RC
publication, or remove it once a final-release stable version is available.

## Remaining promotion evidence at this checkpoint

1. [Long fuzz 35714320518](https://github.com/yoch/mqttium/actions/runs/35714320518):
   running at `c4d8306`, campaign `rc-24-cpu-hours`, base seed `20260922`.
   Five 288-minute shards provide 24 aggregate worker-hours, not a measured
   process CPU-time total. Source and fuzz trees match the integrated runtime.
   Require every shard and retained metadata/failure artifacts.
2. [Two-hour soaks 35715555414](https://github.com/yoch/mqttium/actions/runs/35715555414):
   Linux/Mosquitto MQTT 3.1.1 and 5 are running with 7,200 seconds per protocol.
   Require final completion, source identity and both JSON summaries.
3. [Paired ARM64 35716373609](https://github.com/yoch/mqttium/actions/runs/35716373609):
   running, RC14 versus `852d572`. Require eligible-runner strict writer-capacity
   and paced-latency A/A and A/B results; retain advisory/invalid cells separately.
4. Complete the baseline-anchored strict open-loop release gate on the dedicated
   ARM64 runner, using the existing full default matrix and unchanged policy.
   The accompanying workflow option makes this possible without local load.
5. The queued [ARM64 runtime fuzz](https://github.com/yoch/mqttium/actions/runs/35716645072)
   must finish its 50,000-seed, 32-step campaign. ARM64 workflows share one
   concurrency group; a replaced queued run is not a pass and must be dispatched
   again after the runner becomes available.
6. Validate the documentation/docstring follow-up in CI and record its exact
   SHA. Rebuild and validate distributions after the eventual release metadata
   change; those will be different artifacts from this preparation checkpoint.

## Prepared release notes and choice

Suggested common release-note text:

> MQTTium introduces its lean native API for Python 3.11–3.14, with no runtime
> dependencies. Delivery uses either a bounded asynchronous iterator or short
> synchronous callbacks. Lifecycle hooks have separate asynchronous ownership;
> publication uses receipts and progressive bounded batches. Reconnect,
> manual acknowledgements and SQLite schema 5 have explicit ownership and
> failure contracts. Read the RC14 migration guide before upgrading: Paho
> compatibility and one-shot helpers are removed, constructor/statistics names
> change, and historical SQLite formats are not upgraded automatically.

For **RC15**, describe this as a release candidate for migration feedback and
retain the Beta classifier. For **1.0.0**, describe the finalized Stable native
API and use Production/Stable only after the outstanding evidence and the
maintainer's version decision are complete. Statistics/stores remain Provisional
and extension plumbing remains Internal for either choice.

The release decision must change version, dated changelog, comparison links,
classifier, installation notices and release notes together, then validate the
exact commit, tag `v<version>` and publish through the existing build-once
Trusted Publishing workflow. This preparation does not trigger any of those
publication actions.
