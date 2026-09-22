# Release procedure

MQTTium publishes through PyPI Trusted Publishing. Creating a tag does not
publish anything. The normal trigger is publishing a GitHub release; recovery
can explicitly dispatch `publish.yml` in `publish` mode for an existing,
published release, from that same tag. Validation mode never publishes.

## Preparing the next release

The current source API is not yet published. Its version remains `1.0.0rc14`
while the next release is being prepared; never upload this tree under that
already-used version. Keep changes in `[Unreleased]`, record the candidate
commit and source fingerprint, and validate the current installation and
migration instructions before deciding between `1.0.0rc15` and `1.0.0`.

For the release cut, update `src/mqttium/__init__.py`, freeze the changelog
section and comparison links, and align README installation commands, security
and support status, documentation version notices and the development-status
classifier. RC15 remains a pre-release/Beta; a final 1.0 release uses the
Production/Stable classifier. Validate that exact reviewed release commit
before tagging. Preparation alone does not authorize publication.

## Local candidate gate

From a clean Linux worktree with Mosquitto, OpenSSL, and the project development
extras, run:

```bash
python -m pip install -e ".[dev,fuzz,security,release,docs,benchmark]"
python benchmarks/local_release.py rc --base-ref <approved-baseline> --cpu <eligible-cpu>
```

The runner writes commands, versions, durations, logs and result artifacts under
its versioned temporary result directory, manages Mosquitto with guaranteed cleanup, and
fails if a local quality, performance, memory, artifact, or smoke gate is
missing. An expected non-zero gate result exits without a Python traceback and
prints the retained command log and manifest paths; the gate log contains the
recorded failure or invalidation reason. Performance evidence remains local
because hosted timing is not stable enough for small regressions.

An open-loop run invalidated **only** because the old point-ratio screen
overflowed its bounded confirmation budget may be reevaluated without new
acquisition after a reviewed screening-policy correction:

```bash
python benchmarks/reevaluate_open_loop_release_gate.py \
  --input <retained-original.json> \
  --output <separate-reevaluated.json>
```

The reevaluator refuses other failure or invalidation classes, refuses an
overwritten source artifact, recomputes every scenario from retained initial
ABBA pairs, and remains invalid if any cell still needs confirmation. Its
separate artifact records the original invalidation, source-artifact digest,
evaluator commit, and policy-source digest. Retain both artifacts.

The same profile builds and validates the wheel and sdist, installs the wheel
without source-tree imports, imports every packaged module and exercises TCP,
TLS, WebSocket, Unix, SQLite restart and clean shutdown. It never
contacts PyPI.

Both distributions are minimal. The wheel carries `mqttium/` and its
`.dist-info/`; the source distribution carries `src/mqttium`, `README.md`,
`LICENSE`, `NOTICE` and `pyproject.toml`, which is what rebuilds the wheel.
Tests, benchmarks, documentation, examples and tooling stay in the repository.
`tools/ci/validate_distribution.py` fails the build if any of them reappear, so
a future release cannot quietly widen the distributions again.

Confirm the worktree is clean and prepare a release evidence report for the
exact candidate commit. If release metadata is changed after this preparation,
validate the resulting distributions again at the release cut.

Once the source and local manifest are final, run the GitHub matrix once for the
platform-specific Python 3.11–3.14 and EMQX/HiveMQ checks. Those checks validate
portability and interoperability; they do not replace local performance
evidence. Run the manual soak workflow to include macOS and EMQX/HiveMQ:
pull-request runs cover only the short Linux/Mosquitto checks. A skipped job
is not interoperability evidence. Multi-hour fuzzing and soak campaigns are
required promotion evidence for a stable release; see [Stability](stability.md).

Retain source and harness SHAs, configuration, run URLs, artifact digests and
outcomes. Earlier performance evidence can support unchanged paths only when
source equivalence and artifact availability are verified; a later runtime
change needs validation of the paths it affects. Cross-campaign comparisons
and diagnostic runs do not become strict A/A and A/B evidence.

Build the documentation with `mkdocs build --strict`, check rendered API pages,
search, redirects, relative links, `llms.txt` and `llms-full.txt`, then verify
Read the Docs `latest` after integration. Keep `stable` on the published line
until an appropriate release is available. A successful CI status is not a
substitute for inspecting the deployed documentation.

## Publish

1. Create tag `v<version>` on the reviewed commit.
2. Create a GitHub release for that tag. Mark alpha, beta, and release-candidate
   versions as pre-releases.
3. Review the release notes, then publish the GitHub release.

The publication workflow verifies that the tag belongs to `main` and matches the
package version, rebuilds and validates the distributions, installs the wheel in
an isolated environment, and passes that same Actions artifact to the protected
PyPI publishing job.

## Failure handling

A failure before the final PyPI step publishes nothing and can be corrected
before retrying. PyPI versions are immutable: if a version reaches PyPI, never
reuse it. Fix the problem, increment the version, and publish a new release.
