# Release candidate report — 1.0.0rc13

Candidate source before release metadata:
`cf8f93d4026793f3c4b7bfd987f70b3a027a385d` (`origin/main`, 2026-09-04).

This PR prepares the candidate only. It does not create the `v1.0.0rc13`
tag or publish a GitHub/PyPI release.

## Scope

RC13 contains everything merged after `v1.0.0rc12` (`v1.0.0rc12...cf8f93d`,
13 commits):

- #428 — ingress failure semantics: local store/persistence failures during
  ingress propagate with their original exception instead of a
  peer-attributed `PROTOCOL_ERROR`; observed terminal broker outcomes still
  settle; a local-terminal failure fail-stops the client (no automatic
  reconnect or replay, explicit `connect()` refused, no new publish
  admission);
- #424 — MQTT 3.1 declared explicitly out of the support matrix (docs only;
  the Stable enum member is retained);
- #422 — restore the ordinary QoS 0 publication hot path, removing the
  generic prepared-publish carrier overhead;
- #423 — QoS 1/2 publication preparation as a plain tuple instead of a
  `NamedTuple` carrier;
- #427 — explicit success-ACK provenance (`SEND_ACK`) carried into the
  writer; ordinary writer admission no longer reclassifies ACK bytes;
- #425 — `InflightStore.batch()` atomicity and `take_effects()`
  ownership-transfer boundary documented (docs only);
- supporting tests (22 ingress-failure-semantics tests, ACK-provenance
  tests) and test-behavior alignments.

Classification:

- Stable observable behavior: #428 (with CHANGELOG migration note for
  direct `ProtocolEngine` consumers).
- Provisional contract: third-party stores must not report backend failures
  with `MQTTError` (#428 contract/docs; no SQLite runtime change).
- Internal/performance: #422, #423, #427 (`EffectKind` is Internal per
  `docs/api-stability.md`; validation, ordering and backpressure semantics
  unchanged).
- Documentation-only: #424, #425.

The package version is `1.0.0rc13`. Its release tag must be exactly
`v1.0.0rc13`, and the GitHub release must be marked as a prerelease.
The `Development Status :: 4 - Beta` classifier is unchanged; the move to
a stable/production classifier belongs to `1.0.0`.

## Issue 39 disposition

Issue #39 (native hot-path performance program) remains open as research.
Its full matrix is explicitly not an RC13/1.0 blocker. The targeted
same-machine gate below is the only performance evidence required for this
candidate; #39 may continue after 1.0.

## Performance readiness

Same-machine paired comparison on an 8-CPU Linux runner (Python 3.12),
`v1.0.0rc12` baseline vs `cf8f93d` candidate, fresh source-isolated workers
in ABBA order (`benchmarks/paired_regression.py`, `benchmarks/paired_qos1_rtt.py`):

- QoS 0 engine ingress: 1.0204;
- QoS 0 publish nowait: 1.3899;
- QoS 1 ingress publish: 0.9953;
- QoS 1 memory store cycle: 0.9574 first run, 1.0192 on a 13-pair re-run
  (the first-run shortfall came from high baseline outliers, 25.5k–29.0k
  ops/s against a stable candidate at ~26.5k ops/s; baseline CV 4–5%);
- writer try-enqueue: 1.2781;
- delivery callback: 1.0390; single-message callback effect: 1.0760;
- encode QoS 0 / QoS 1: 1.0095 / 1.0078;
- QoS 1 callback-response RTT: p50 0.9881, p95 1.0130, p99 1.0288,
  throughput 1.0055. The p50 baseline CV was ~17% (harness-flagged
  invalid), so no strong comparative claim is drawn from that cell; all
  cells lie within ±3%.

No material regression demonstrated. Memory guardrail
(`benchmarks/memory_profile.py` + `check_memory_thresholds.py`) on the
candidate: all scenarios within thresholds.

## Local candidate evidence

On the release-metadata tree (version `1.0.0rc13`):

- `ruff format --check`: 289 files already formatted; `ruff check`: clean;
- `mypy src/mqttium`: no issues in 63 source files;
- `bandit -q -ll -r src`: clean;
- `pytest tests/unit tests/project`: 1583 passed (Python 3.11);
- `python -m build`: wheel + sdist built; `twine check --strict`: passed;
  `check-wheel-contents`: OK; isolated wheel install reports
  `mqttium.__version__ == importlib.metadata.version("mqttium") ==
  "1.0.0rc13"`.

## Hosted evidence

The exact pre-metadata source `cf8f93d` passed on `main`:

- [CI run 33841507973](https://github.com/yoch/mqttium/actions/runs/33841507973);
- [ARM64 CI run 33841508003](https://github.com/yoch/mqttium/actions/runs/33841508003);
- [Runtime fuzz nightly run 33842207734](https://github.com/yoch/mqttium/actions/runs/33842207734).

Candidate-PR CI evidence: preparation PR #429, head `fc4ce42`, passed the
full hosted matrix:

- [CI run 33847229283](https://github.com/yoch/mqttium/actions/runs/33847229283)
  (quality, tests 3.11–3.14, Mosquitto integration, resilience,
  cross-platform macOS/Windows 3.11+3.14, fuzz, package, `CI required`);
- [distribution run 33847229284](https://github.com/yoch/mqttium/actions/runs/33847229284)
  (wheel/sdist build, smoke-basic 3.11–3.14, smoke-resilience; publish and
  verify-pypi correctly skipped);
- [soak run 33847229295](https://github.com/yoch/mqttium/actions/runs/33847229295)
  (linux-soak MQTT 3.1.1 and MQTT 5 passed);
- Read the Docs build for the PR passed.

## Release decision

RC13 is recommended for merge once the candidate-PR CI is green. Tagging
and prerelease publication remain conditional on the post-merge `main`
gates (CI required matrix, ARM64 CI, interoperability and soak
workflows), exactly as for RC12.

This preparation changes no runtime source: the only
`src/mqttium/**` change is the version number.
