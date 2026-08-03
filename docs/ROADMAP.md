# Roadmap

The protocol, transport and compatibility foundations are implemented. This
roadmap contains only work that remains relevant after production hardening.
Completed phase checklists live in Git history and the pull-request discussion.

## Before a stable release

- [ ] Extract `mqttium/` into its dedicated repository and establish release ownership.
- [ ] Freeze and document the public API/compatibility policy for `0.x` releases.
- [ ] Add packaging smoke tests for wheel and source distributions.
- [ ] Run sustained reconnect/session/backpressure soak tests on Linux and macOS.
- [ ] Extend broker interoperability beyond Mosquitto to at least two independent implementations.
- [ ] Publish reproducible release benchmark artefacts from pinned runner profiles.

## Optional extensions

- [ ] Concrete enhanced-authentication plugins such as SCRAM where broker demand justifies them.
- [ ] Additional Paho compatibility surface only when backed by behavioural tests.
- [ ] Long-running fuzz campaigns and corpus retention outside normal pull-request CI.

## Permanent quality gates

- Python 3.11, 3.12 and 3.13 unit and Mosquitto integration tests;
- Ruff formatting/linting and mypy;
- at least 80% source coverage;
- deterministic plus Hypothesis fuzzing;
- subscriber-confirmed TCP, TLS and controlled WAN-profile benchmarks;
- no generated benchmark results or mutable CI workflows committed to source.
