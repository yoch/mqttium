# Roadmap

MQTTium's native protocol, transport, persistence, and public API foundations
are implemented. Roadmap entries describe intended direction, not a support
promise or release date.

## Stable-release readiness

- Complete the source, documentation, test, workflow, and packaging review.
- Retain a clean local release manifest and exact artifact smokes.
- Retain Python 3.11–3.14, cross-platform, and broker-interoperability evidence.
- Complete the long-running fuzz, reconnect, backpressure, memory, and soak
  campaigns defined by [Stability](stability.md).
- Publish the Read the Docs `stable` and `latest` versions and verify their
  search, links, API reference, and machine-readable endpoints.
- Publish cross-client results only after the independent benchmark repository
  satisfies the evidence contract.

## Post-release priorities

- Improve the native `AsyncClient` from real service and gateway feedback.
- Expand cookbook and troubleshooting material from reproducible issues.
- Keep protocol conformance, hostile-broker tests, and persistence migrations
  current as defects are discovered.
- Extend broker, platform, and architecture validation where maintainable
  runners and concrete user demand exist.
- Add enhanced-authentication integrations only when broker demand and a safe
  credential boundary justify them.

## Paho compatibility policy

The Paho VERSION2 facade remains Provisional, tested, and de-emphasized. It is a
transition surface for existing synchronous applications, not a second product
direction. Add compatibility features only for demonstrated migration needs
with behavioral and lifecycle tests. Do not promise drop-in or performance
parity.

If maintenance cost or concurrency risk grows without corresponding adoption,
deprecation must follow the published API policy and include migration guidance.

## Documentation and ecosystem

- Maintain one authoritative Stable API reference and one compatibility matrix.
- Keep dated reports immutable and classify every report as current evidence,
  superseded, or retracted in the archive index.
- Keep project metadata, issue forms, support policy, and release instructions
  aligned with the current release line.
- Consider a custom documentation domain only after the default Read the Docs
  deployment is stable and indexed.

## Permanent quality gates

- Python 3.11–3.14 unit and Mosquitto integration coverage;
- cross-platform endpoint validation and ARM64 release evidence;
- Ruff, mypy, Bandit, and the configured coverage threshold;
- deterministic and Hypothesis fuzzing plus retained long campaigns;
- wheel, source distribution, metadata, and isolated-install validation;
- subscriber-confirmed transport and broker-interoperability tests;
- controlled performance comparisons with valid A/A controls;
- no generated benchmark, coverage, cache, or build artifacts tracked in Git.
