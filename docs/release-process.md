# Release procedure

MQTTium publishes through PyPI Trusted Publishing. Creating a tag does not
publish anything; publication starts only when a GitHub release is published.

## Local candidate gate

From a clean Linux worktree with Mosquitto, OpenSSL, and the project development
extras, run:

```bash
python benchmarks/local_release.py rc --base-ref <approved-baseline>
```

The runner writes commands, versions, durations, logs and result artifacts under
its versioned temporary result directory, manages Mosquitto with guaranteed cleanup, and
fails if a local quality, performance, memory, artifact, or smoke gate is
missing. Performance evidence remains local because hosted timing is not stable
enough for small regressions.

The same profile builds and validates the wheel and sdist, installs the wheel
without source-tree imports, imports every packaged module and exercises TCP,
TLS, WebSocket, Unix, SQLite restart, Paho VERSION2 and clean shutdown. It never
contacts PyPI.

Then update `src/mqttium/__init__.py`, `CHANGELOG.md`, status documentation, and
the development-status classifier as appropriate. Confirm the worktree is
clean and prepare a release evidence report for the exact candidate commit.

Once the source and local manifest are final, run the GitHub matrix once for the
platform-specific Python 3.11–3.14 and EMQX/HiveMQ checks. Those checks validate
portability and interoperability; they do not replace local performance
evidence. Multi-hour fuzzing and soak campaigns are required promotion evidence
for a stable release.

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
