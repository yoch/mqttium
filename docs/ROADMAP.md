# Roadmap

The protocol, transport and compatibility foundations are implemented. This
roadmap tracks work that remains relevant after the standalone spin-out.

## Completed release foundations

- [x] Extract MQTTium into a dedicated repository.
- [x] Establish standalone Python 3.11–3.14 CI.
- [x] Validate wheel and source-distribution contents.
- [x] Add isolated wheel-install and Mosquitto integration smoke tests.
- [x] Add reproducible benchmark workflows and an OIDC-based PyPI release workflow.
- [x] Complete the initial file-level provenance review.

## Before a stable release

- [ ] Freeze and document the public API and compatibility policy.
- [ ] Run sustained reconnect, session and backpressure soak tests on Linux and macOS.
- [ ] Extend broker interoperability beyond Mosquitto to at least two independent implementations.
- [ ] Publish reproducible release benchmark artefacts from pinned runner profiles.

## Remaining memory work

Carried over from [`MEMORY-PROFILE-FOLLOW-UP.md`](MEMORY-PROFILE-FOLLOW-UP.md),
which records what already shipped.

- [ ] Bound each ingress batch by bytes, not only by the 256-packet count.
- [ ] Specialised in-buffer PUBLISH decode, to remove the payload-sized copies
      large inbound messages still cost.
- [ ] Full QoS 2 phase-two compaction (release topic, payload and properties,
      not only the encoded frame).
- [ ] Benchmark the QoS 1 / pre-PUBREC frame policy: re-encode on retransmission
      versus a size-based policy.
- [ ] Complete the counter surface — effect queue, writer, decoder, WebSocket
      buffers, receipts, an outbound-budget high-water mark, and one unified
      stats snapshot. Constraint: no `logging`, per
      [`LOGGING.md`](LOGGING.md).
- [ ] Add memory-benchmark scenarios for the paths currently covered only by
      unit tests (property-heavy outbound, immediate refusal, cancellation
      around the commit point, Paho queue saturation, shared iterator/callback
      accounting, WebSocket byte-bounded batches, reconnect/epoch cleanup).
- [ ] Assert that outbound admission counters return to zero after a sustained
      load drains.

## Optional extensions

- [ ] Concrete enhanced-authentication plugins such as SCRAM where broker demand justifies them.
- [ ] Additional Paho compatibility surface only when backed by behavioural tests.
- [ ] Long-running fuzz campaigns and corpus retention outside normal pull-request CI.

## Permanent quality gates

- Python 3.11, 3.12, 3.13 and 3.14 unit and Mosquitto integration tests;
- Ruff formatting/linting and mypy;
- at least 80% source coverage;
- deterministic plus Hypothesis fuzzing;
- wheel, sdist and isolated-install validation;
- subscriber-confirmed TCP, TLS and controlled WAN-profile benchmarks;
- no generated benchmark, coverage or build artefacts committed to source.
