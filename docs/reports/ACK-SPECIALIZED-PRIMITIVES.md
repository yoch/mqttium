# Specialized acknowledgement codec primitives

Date: 2026-08-13

Base: `main` at `e4e4482`; candidate rebases and supersedes the concurrent
three-byte session fast path (#212) and specialized-codec branch (#213).

## Question

Can PUBACK/PUBREC/PUBREL/PUBCOMP decode be specialized per protocol version so
the common MQTT 5 three-byte reason-bearing body costs the same order of
Python work as MQTT 3.1.1's two-byte success body?

## Change

- `packets/_ack_v311.py` and `packets/_ack_v5.py` own direct encode/decode
  functions per packet type. The fast-path shapes (2 bytes; MQTT 5 3 bytes)
  contain no generic helper call.
- Absent properties return `None` (no empty `Properties()` on the three-byte
  form).
- `Pub*Packet` dataclasses are factories over those primitives.
- `OutboundSession` / `InboundSession` bind the version-specific decoder once
  at construction and drop per-packet `len`/`protocol` branches.

## Evidence (local micro)

MQTTium call depth per engine ACK decode:

| Shape | Calls | Call stack |
| --- | --- | --- |
| v3.1.1 2-byte | 1 | `decode_puback_v311` |
| v5 2-byte | 1 | `decode_puback_v5` |
| v5 3-byte `0x10` | 1 | `decode_puback_v5` |
| v5 4-byte empty props | property path | includes `decode_properties` |

Pre-change generic path was 5.0 (v3.1.1) / 6.0 (v5) MQTTium calls for the same
ACK family. The common forms now execute one direct decode primitive.

`benchmarks/ack_specialized_decode.py` (300k ops × 9 repeats, this host):

| Shape | Primitive ops/s | Packet factory ops/s |
| --- | --- | --- |
| v5 3-byte `0x10` | ~2.76e6 | ~5.92e5 |
| v5 4-byte empty props | ~1.01e6 | ~3.73e5 |

The factory still allocates the dataclass; sessions call the primitive directly
and never pay that. A paired full engine cycle (QoS 1 admission, PUBLISH encode,
MQTT 5 three-byte PUBACK and settlement) measured candidate/main **1.1654x**
(11/11 pairs favouring candidate; base CV 2.27%, candidate CV 3.12%). Against
#212 it measured **1.0723x** (10/11; CV 4.32% / 3.04%). Raw outputs remain build
artefacts under `/tmp` per the benchmark contract.

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
