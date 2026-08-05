# Roadmap

The protocol, transport and compatibility foundations are implemented. This
roadmap tracks the evidence and optimisations still required after the
finalisation architecture pass.

## Completed release foundations

- [x] Extract MQTTium into a dedicated repository.
- [x] Establish standalone Python 3.11–3.14 CI.
- [x] Validate wheel and source-distribution contents.
- [x] Add isolated wheel-install and Mosquitto integration smoke tests.
- [x] Add reproducible benchmark workflows and an OIDC-based PyPI release workflow.
- [x] Complete the initial file-level provenance review.
- [x] Give `EffectPump`, `WritePump`, `InboundSession` and `OutboundSession`
      authoritative ownership of their runtime/protocol state.
- [x] Document the public API candidate and compatibility/deprecation policy.

## Stable-release campaign

Automation is implemented in `.github/workflows/finalization.yml`; successful
workflow definitions are not counted as evidence until their run artefacts are
retained and reviewed. Extended macOS and multi-broker campaigns are launched
manually. See [`STABILITY.md`](STABILITY.md).

- [ ] Retain successful reconnect, session and backpressure soak runs on Linux
      and macOS for MQTT 3.1.1 and MQTT 5.
- [ ] Retain successful interoperability runs against Mosquitto, EMQX and HiveMQ
      Community Edition.
- [ ] Publish reproducible release benchmark artefacts from pinned runner
      profiles.
- [ ] Review the evidence and promote the API candidate to the first non-alpha
      release.

## Remaining memory and performance work

Carried over from [`MEMORY-PROFILE-FOLLOW-UP.md`](MEMORY-PROFILE-FOLLOW-UP.md),
which records what already shipped.

- [x] Bound each ingress batch by bytes as well as the 256-packet count.
- [x] Provide one immutable stats snapshot for effect, writer, decoder,
      WebSocket, delivery, receipt and outbound-admission counters, including
      high-water marks.
- [x] Assert that outbound admission counters return to zero after sustained
      QoS 1 load drains.
- [ ] Specialised in-buffer PUBLISH decode, if profiling still shows
      payload-sized copies as a material peak after ingress batching.
- [x] Full QoS 2 phase-two compaction (release topic, payload and properties,
      not only the encoded frame).
- [ ] Benchmark the QoS 1 / pre-PUBREC frame policy: re-encode on retransmission
      versus a size-based policy.
- [ ] Add memory-benchmark scenarios for property-heavy outbound, immediate
      refusal, cancellation around commit, Paho saturation, shared delivery,
      WebSocket batching and reconnect/epoch cleanup.

## Optional extensions

- [ ] Extract inbound application delivery into a dedicated controller only if
      soak results or future delivery policies show a concrete ownership or
      maintenance benefit.
- [ ] Concrete enhanced-authentication plugins such as SCRAM where broker demand
      justifies them.
- [ ] Additional Paho compatibility surface only when backed by behavioural tests.
- [ ] Long-running fuzz campaigns and corpus retention outside normal pull-request CI.

## Permanent quality gates

- Python 3.11, 3.12, 3.13 and 3.14 unit and Mosquitto integration tests;
- Ruff formatting/linting and mypy;
- at least 80% source coverage;
- deterministic plus Hypothesis fuzzing;
- wheel, sdist and isolated-install validation;
- subscriber-confirmed TCP, TLS and controlled WAN-profile benchmarks;
- manually triggered reconnect/backpressure and multi-broker interoperability artefacts;
- no generated benchmark, coverage or build artefacts committed to source.
