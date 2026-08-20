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

The Stable native API follows SemVer. Provisional APIs—including persistence,
transports, diagnostics, advanced protocol integrations, and Paho
compatibility—remain tested but may evolve with a changelog entry and migration
guidance. Internal modules have no support guarantee.

See [API Stability](docs/api-stability.md) and the
[Compatibility Matrix](docs/compatibility.md).
