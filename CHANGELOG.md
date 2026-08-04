# Changelog

All notable changes to MQTTium are documented here.

The format follows Keep a Changelog and versions follow Semantic Versioning.

## [Unreleased]

### Changed

- Coalesced Paho-compatible QoS 0/1/2 cross-thread publishing onto bounded
  network-loop batches, with atomic queue-size refusal and cancellation-safe
  MID handoff semantics.

## [0.1.0a1] - 2026-08-03

### Added

- Initial async-native MQTT 3.1.1 and MQTT 5 client spin-out.
- QoS 0/1/2, reconnect, session replay and multiple transports.
- In-memory and SQLite inflight persistence.
- Aggregate `publish_many()` pipeline with bounded memory.
- Additive Paho VERSION2 compatibility façade.
- Standalone Python 3.11–3.14 CI, fuzzing and Mosquitto integration tests.
- Wheel, source-distribution and isolated-install release validation.
- PEP 561 inline typing marker.
- PyPI Trusted Publishing workflow using GitHub OIDC.

### Changed

- Replaced the original Paho-shaped topic matcher with an independent
  flat-filter implementation before publication.
- Adopted PEP 639 license metadata and explicit Apache-2.0 package files.

### Removed

- Pre-spin-out comparative analysis and generated coverage data from the
  published source tree.
