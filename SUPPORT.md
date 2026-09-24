# Support

MQTTium is an open-source project maintained on a best-effort basis. There is no
response-time, uptime, or commercial support guarantee.

## Before asking for help

1. Read [Getting Started](docs/getting-started.md) and
   [Troubleshooting](docs/troubleshooting.md).
2. Confirm the behaviour with the latest supported release in a clean virtual
   environment.
3. Check existing GitHub issues for the same MQTTium, broker, and transport
   combination.
4. Reduce the problem to a small executable example and remove secrets.

## Where to report

- **Reproducible incorrect behaviour:** use the structured GitHub bug form and
  [Reporting Issues](docs/reporting-issues.md).
- **Documentation gaps:** open a GitHub issue describing the task that could not
  be completed and the page you consulted.
- **Security vulnerabilities:** use the private process in
  [SECURITY.md](SECURITY.md), never a public issue.
- **Performance regressions:** include valid same-machine A/A and A/B evidence
  under the [Benchmarking Contract](docs/benchmarking.md).

General application design, broker administration, network operations, and
production incident response are outside the project's support scope unless a
minimal reproducer identifies MQTTium behaviour.

## Supported surface

The Stable native API follows SemVer and the documented deprecation policy.
`ClientStats`, its nested snapshots, `MemoryInflightStore`, and
`SqliteInflightStore` are Provisional: supported and tested, but changes require
a changelog entry and migration guidance. Engine, codec, transport and store
extension protocols are Internal and have no compatibility guarantee.
The Paho facade and one-shot helpers have been removed.

The latest published release, `1.0.0rc16`, keeps the API of `1.0.0rc15`, an
incompatible pre-v1 revision of `1.0.0rc14`. Use documentation matching your installed release. See the
[migration guide](docs/migration.md) before upgrading an application or database
from `1.0.0rc14`.

See [API Stability](docs/api-stability.md) and the
[Compatibility Matrix](docs/compatibility.md).
