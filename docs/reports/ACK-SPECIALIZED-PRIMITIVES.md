# Specialized acknowledgement codec primitives

Date: 2026-08-13

Base: branch `perf/specialized-codec-primitives` (successor to the three-byte
session fast-path checkpoint).

## Question

Can PUBACK/PUBREC/PUBREL/PUBCOMP decode be specialized per protocol version so
the common MQTT 5 three-byte reason-bearing body costs the same order of
Python work as MQTT 3.1.1's two-byte success body?

## Change

- `packets/_ack_v311.py` and `packets/_ack_v5.py` own encode/decode. The
  fast-path shapes (2 bytes; MQTT 5 3 bytes) are the body of the decoder, not
  a prelude that falls back to a generic helper.
- Absent properties return `None` (no empty `Properties()` on the three-byte
  form).
- `Pub*Packet` dataclasses are factories over those primitives.
- `OutboundSession` / `InboundSession` bind the version-specific decoder once
  at construction and drop per-packet `len`/`protocol` branches.

## Evidence (local micro)

`sys.setprofile` mqttium call counts per decode:

| Shape | Calls | Call stack |
| --- | --- | --- |
| v3.1.1 2-byte | 2 | `decode_puback_v311` → `_decode_mid` |
| v5 2-byte | 2 | `decode_puback_v5` → `_decode_ack_v5` |
| v5 3-byte `0x10` | 2 | same |
| v5 4-byte empty props | 5 | includes `decode_properties` |

Pre-change generic path was 5.0 (v3.1.1) / 6.0 (v5) mqttium calls for the same
ACK family. Criterion met: v5 three-byte is the same order as v3.1.1 two-byte
(1–2 calls, not 6).

`benchmarks/ack_specialized_decode.py` (200k ops × 7 repeats, this host):

| Shape | Primitive ops/s | Packet factory ops/s |
| --- | --- | --- |
| v5 3-byte `0x10` | ~2.53e6 | ~5.6e5 |
| v5 4-byte empty props | ~9.4e5 | ~3.7e5 |

The factory still allocates the dataclass; sessions call the primitive
directly and never pay that.

## Network

`paired_network.py` QoS 1 v5 vs 3.1.1 against Mosquitto remains the end-to-end
check for the original capacity gap. Hosted runners are advisory only; this
report does not claim a network median. The micro call-count criterion is the
wave-1 gate.

## Risks

- `EngineConfig.protocol` must not be mutated after session construction; the
  codec bindings are fixed at `__init__`. The former negotiation test that
  flipped protocol mid-flight was rewritten to construct MQTT 5 up front.
- Property-bearing acknowledgements still go through `decode_properties` and
  Request Problem Information validation.
