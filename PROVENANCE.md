# Provenance

MQTTium was initially developed as the independent `mqttnext/` subtree in
[`yoch/paho.mqtt.python`](https://github.com/yoch/paho.mqtt.python).

## Spin-out source

- source commit: `bcdff1a22889c90ec635f658aee13fb318038af0`;
- production-hardening line: pull request #6;
- measured core optimizations: pull request #8;
- aggregate `publish_many()` implementation: pull request #9.

The dedicated repository was created by extracting only the `mqttnext/`
subtree from that exact commit, moving it to the repository root, renaming the
Python distribution and import package to `mqttium`, and rebuilding the GitHub
workflows for a standalone repository. No `mqttnext` compatibility alias is
retained.

## Validation inherited from the source tree

At the time of the spin-out, the exact production code was validated with 245
unit tests, Mosquitto integration on Python 3.11/3.12/3.13, Ruff, mypy,
deterministic fuzzing, Hypothesis fuzzing, and an 80% coverage gate.

This repository has moved on since: the standalone gates run unit, project,
resilience and fuzz suites, and Mosquitto integration across Python 3.11, 3.12,
3.13 and 3.14, plus Bandit and distribution validation. See
[Contributing](CONTRIBUTING.md) for the current commands and
[Testing](docs/testing.md) for coverage and suite definitions. The counts above
describe the spin-out commit, not the current release candidate.

The final retained `publish_many()` benchmark decision was based on a paired
A/B run whose artifact digest was:

`sha256:3357e8f1928860ac1f42f74810fefea68adb29d04af50450cddbdc6fa43abefe`

## Licensing review

The initial file-level review was completed before the first package release:

- MQTTium's protocol, transport, persistence and public API implementations are
  original code distributed under Apache-2.0;
- the former Paho compatibility package implemented documented public behaviour
  without vendoring Paho source files; the current native API removes it;
- tests and documentation reference Paho and gmqtt for behavioural comparison
  and migration guidance only;
- the original trie matcher, whose structure followed Paho's matcher closely,
  was replaced with an independent flat-filter implementation before release;
- no generated coverage databases, benchmark results or upstream license files
  are included as package source.

References to Eclipse Paho and gmqtt are nominative interoperability and
provenance references. Their names and licenses do not apply to MQTTium's
independent implementation.
