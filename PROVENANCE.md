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

The exact production code was validated with 245 unit tests, Mosquitto
integration on Python 3.11/3.12/3.13, Ruff, mypy, deterministic fuzzing,
Hypothesis fuzzing, and an 80% coverage gate.

The final retained `publish_many()` benchmark decision was based on a paired
A/B run whose artifact digest was:

`sha256:3357e8f1928860ac1f42f74810fefea68adb29d04af50450cddbdc6fa43abefe`

## Licensing note

MQTTium is intended as an independent implementation and is distributed under
Apache-2.0. Initial development occurred inside a fork of Eclipse Paho. A
file-level provenance and license review should be completed before the first
public package release, particularly for compatibility-facing material.
