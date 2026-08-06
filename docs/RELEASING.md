# Release procedure

MQTTium publishes through PyPI Trusted Publishing. Creating a tag does not
publish anything; publication starts only when a GitHub release is published.

## Prepare

1. Merge the reviewed changes into `main`.
2. Update `src/mqttium/__init__.py`, `CHANGELOG.md`, and the development-status
   classifier for the new version.
3. Run the normal CI and the required stability checks on the candidate commit.

## Rehearse

Run the **Prepublication audit** workflow with the exact candidate commit in
`ref` and, preferably, the expected tag such as `v0.2.0b1`.

The workflow builds the wheel and source distribution, validates them, installs
the wheel in an isolated environment, imports every packaged module, and keeps
the resulting distributions as a GitHub Actions artifact. It never contacts
PyPI.

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
