# Rejected experiment: fused MQTT 3.1.1 QoS 0 ingress — 2026-08-20

## Question

Can the native subscriber hot path materially improve by fusing MQTT framing with the existing direct MQTT 3.1.1 QoS 0 PUBLISH decoder, avoiding the intermediate owned `RawPacket.remaining` body before constructing the application `Message`?

This experiment was motivated by the external `mqtt-python-client-bench` subscribe campaign, where MQTTium trails gmqtt by roughly 23–34% on broker-limited QoS 0 ingress points. Those cross-client numbers are hypotheses only: Mosquitto was saturated and the points are correctly tagged `inconclusive` / `broker_limited`.

## Base and experimental boundary

Remote base: `main@bf0d80ea0cb0fa9be2c22ac47bf2ddff2ca07fee`.

The local baseline was reconstructed from the CI distribution artifact for the exact tree that merged as that `main` commit; critical source blobs were checked against GitHub (`buffer.py`, `async_client.py`, `_publish.py`, `pyproject.toml`).

The experiment deliberately kept `Message`, `EngineEffect`, `EffectPump`, callback isolation and all public delivery semantics unchanged. Only framing/body materialisation was varied.

## Variants tried

1. **Transient-memoryview split.** The decoder copied a topic field and payload separately from the reusable buffer, returned an internal split packet, and let the existing protocol decoder validate and build `Message`.
   - 11 alternating local pairs on telemetry256: median candidate/base about **0.875**; **0/11** favoured the candidate.
   - Rejected immediately: removing copies did not repay memoryview/object/dispatch overhead for small frames.

2. **Decode topic while the memoryview is live.** This removed the second `unpack_utf8` from the protocol decoder.
   - 7 alternating pairs: median about **0.873**; **0/7** favoured the candidate.
   - Rejected for the same reason.

3. **No-memoryview split with a bounded one-entry `struct.Struct` payload copier.** `Struct.unpack_from()` copies directly from the decoder bytearray to immutable `bytes`; a one-entry size cache keeps memory bounded and favours common fixed-size telemetry while changing size safely.
   - This was the best implementation and is the one used for the final paired decision below.

## Final paired decision

A new target scenario measured the complete small-PUBLISH pipeline:

`feed -> IncrementalDecoder -> PUBLISH decode -> ProtocolEngine -> take_effects`

MQTT 3.1.1, exact topic, 256-byte payload, fresh source-isolated workers, 11 alternating pairs, worker pinned to CPU 3:

| Metric | Result |
| --- | ---: |
| Median candidate/base | **0.9898** |
| Range | 0.8786–1.0646 |
| Baseline CV | **3.77%** |
| Candidate CV | **4.60%** |
| Pairs favouring candidate | **5 / 11** |

The baseline satisfies the repository's 5% validity bound. The candidate does not meet the micro-gain acceptance rule (at least +2% and at least 8/11 favourable pairs); its median is slightly negative.

An engine-only QoS 0 guardrail also showed no compensating effect: one valid-base-CV run measured median **0.9850**, 4/11 favourable, with candidate CV above 5%, so no numeric regression claim is made from that cell. It is directionally consistent with the target result.

## Correctness validation

The best candidate preserved the existing engine error boundary: malformed UTF-8 falls back to the legacy owned-body path so validation still occurs inside `ProtocolEngine.handle_raw`; empty topics, wildcards and DUP are not rejected by framing; terminal engine states still ignore trailing buffered packets.

Focused regression coverage included fragmented input, Unicode topics, payload ownership across decoder-buffer reuse, malformed UTF-8 error routing, bounded batch byte accounting, and MQTT 5 remaining on the legacy path.

Local validation from the source-equivalent sdist:

- focused QoS 0 / decoder / ingress-neighbour tests: green;
- unit suite available in the sdist: **1,236 passed**;
- `tests/unit/test_spec_extractor.py` is not runnable from the distribution artifact because the repository-only `tools/` source is intentionally absent; repository CI remains the authoritative check for it.

## Decision

**Reject. Do not merge the runtime candidate.**

For small MQTT frames, CPython's existing C-level bytearray/bytes copies are cheap enough that replacing them with a Python-visible special representation does not buy throughput. The historical instruction to revisit direct framing/PUBLISH fusion when decoder CPU became material was followed; current evidence says this particular pure-Python boundary is not the next lever.

Do not re-propose a memoryview split, decoded-topic split, or single-size `struct.Struct` split without a materially different mechanism. The next investigation should target work represented by Python calls/objects after decode (effect/delivery handoff), where previous successful MQTTium optimisations have shown larger returns.
