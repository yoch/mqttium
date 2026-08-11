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

Performance and source validation run locally through
`benchmarks/local_release.py`. A final GitHub matrix supplies the Python-version
and independent-broker environments that are intentionally not reproduced
locally.
See [`STABILITY.md`](STABILITY.md) and the retained historical
[2026-08-05 campaign record](reports/STABLE-RELEASE-EVIDENCE-2026-08-05.md).

- [x] Retain successful reconnect, session and backpressure soak runs on Linux
      and macOS for MQTT 3.1.1 and MQTT 5.
- [x] Retain successful interoperability runs against Mosquitto, EMQX and HiveMQ
      Community Edition.
- [x] Publish reproducible release benchmark artefacts from pinned runner
      profiles.
- [ ] Complete and review the local `rc` manifest, then promote the candidate
      from `1.0.0rc1` to `1.0.0`.
- [ ] After the source is clean, run one Python 3.11–3.14 and EMQX/HiveMQ
      GitHub matrix for the first RC.
- [ ] Run the multi-hour fuzz and soak campaign after the first RC, before a
      later candidate or final `1.0.0` promotion.

## Remaining memory and performance work

Carried over from [`reports/MEMORY-PROFILE-FOLLOW-UP.md`](reports/MEMORY-PROFILE-FOLLOW-UP.md),
which records what already shipped.

- [x] Bound each ingress batch by bytes as well as the 256-packet count.
- [x] Provide one immutable stats snapshot for effect, writer, decoder,
      WebSocket, delivery, receipt and outbound-admission counters, including
      high-water marks.
- [x] Assert that outbound admission counters return to zero after sustained
      QoS 1 load drains.
- [x] Profile specialised in-buffer PUBLISH decoding after ingress batching.
      It did not reduce the remaining two-payload peak after the simple body-copy
      correction and is therefore not shipped; see
      [`reports/PUBLISH-DECODE-PROFILE.md`](reports/PUBLISH-DECODE-PROFILE.md).
- [x] Full QoS 2 phase-two compaction (release topic, payload and properties,
      not only the encoded frame).
- [x] Benchmark the QoS 1 / pre-PUBREC frame policy. Contiguous frames are
      re-encoded instead of retained; segmented frames retain their shared
      payload and patch only the DUP header. See
      [`reports/QOS1-FRAME-POLICY.md`](reports/QOS1-FRAME-POLICY.md).
- [x] Add memory-benchmark scenarios for property-heavy outbound, immediate
      refusal, cancellation around commit, Paho saturation, shared delivery,
      WebSocket batching and reconnect/epoch cleanup, with exact workload
      assertions and versioned allocation thresholds.

## Optional extensions

- [x] Extract inbound application delivery into a dedicated controller with
      measured mode-specialised admission and unchanged Stable API semantics.
- [ ] Concrete enhanced-authentication plugins such as SCRAM where broker demand
      justifies them.
- [ ] Additional Paho compatibility surface only when backed by behavioural tests.
- [x] Long-running fuzz campaigns and corpus retention outside normal pull-request CI.

## Permanent quality gates

- Python 3.11, 3.12, 3.13 and 3.14 unit and Mosquitto integration tests;
- Ruff formatting/linting and mypy;
- at least 87.36% source coverage;
- deterministic plus Hypothesis fuzzing;
- wheel, sdist and isolated-install validation;
- subscriber-confirmed TCP, TLS and controlled WAN-profile benchmarks;
- manually triggered reconnect/backpressure and multi-broker interoperability artefacts;
- no generated benchmark, coverage or build artefacts committed to source.
