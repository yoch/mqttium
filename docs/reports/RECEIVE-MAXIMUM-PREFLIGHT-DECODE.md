# Receive Maximum preflight without `PublishPacket.decode`

Date: 2026-08-13

Base commit: `d431834` (`main` after #198).

Supersedes the approach in PR #197, which was closed: a MID-only
`unpack_utf8` + `unpack_u16` preflight changed MQTT 5 protocol semantics.

## Question

Can the pending-handoff Receive Maximum preflight recover a packet identifier
without constructing a `PublishPacket` or copying the payload, without
changing which protocol violation wins?

## Established choice that this keeps

Auto-PUBACKs still count against Receive Maximum until the effect batch is
handed off. Duplicate-MID, cross-QoS reuse and handoff-release behaviour are
unchanged. MQTT 5 `PublishPacket.decode` validates the property table and the
received Topic Name before returning; a malformed follow-up must not become
DISCONNECT 0x93.

This is **not** a proposal to reuse Topic Name bytes on outbound QoS 1/2
(PR #178).

## Defect in #197

`_qos_publish_mid()` decoded Topic Name only far enough to locate the MID.
A wildcard Topic Name is valid MQTT UTF-8, so the preflight treated it as a
usable identifier. With `local_receive_maximum=1` and one pending auto-QoS1
PUBACK, an MQTT 5 QoS 1 PUBLISH whose Topic Name contains `+`:

| | `main` | #197 |
| --- | --- | --- |
| state | `CONNECTED` | `DISCONNECTED` |
| wire | none | `DISCONNECT 0x93` |
| diagnostic | `PUBLISH topic must not contain wildcards` | `Receive Maximum exceeded ... before pending PUBACK handoff` |

`PublishPacket.decode` on MQTT 5 does not return until it has decoded the
property table and validated the received topic. The truncated-UTF-8 pin in
#197 did not cover this.

## Change

`decode_publish_envelope` in `packets/publish.py` is the variable-header
check `PublishPacket.decode` already ran. Decode now slices the payload from
the offset that helper returns. The engine preflight calls the same function
and keeps only the MID.

A wildcard, unknown property identifier, truncated Topic Name or zero MID
therefore raises from the same checks decode uses, before Receive Maximum is
consulted. The payload is not copied.

## Impact

- **Correctness.** The reviewer's MQTT 5 wildcard differential is pinned, as
  are truncated UTF-8 and an unknown MQTT 5 property identifier. Valid
  pipelined QoS 1 still disconnects 0x93 when the window is full. MQTT 3.1.1
  decode does not validate wildcards in the packet layer; the preflight
  matches that (inbound specialised decoders still validate later).
- **Performance.** Measured on this host, 50 000 iterations, MQTT 5 QoS 1:

  | Payload | `PublishPacket.decode` | `decode_publish_envelope` | Ratio |
  | ---: | ---: | ---: | ---: |
  | 64 B | 2158 ns | 1095 ns | 1.97× |
  | 4 KiB | 2372 ns | 1139 ns | 2.08× |
  | 64 KiB | 3540 ns | 1137 ns | 3.11× |

  Envelope time is independent of payload size; decode is not. That is the
  payload copy. #197's MID-only helper was cheaper still (~0.43 µs to locate
  the MID) because it skipped property-table decode and topic validation.
  Those checks are required. This PR does not claim the ~+29% saturated
  preflight ratio measured on #197; hosted-runner wall-clock is not claimed.
  The evidence is the same class as the specialised inbound decoders: skip
  the packet object and the payload copy, keep the header contract.
- **API.** `decode_publish_envelope` is not added to `packets.__all__`.
  `ProtocolEngine` no longer imports `PublishPacket`.

## What this is not

It does not cache decoded fields for `InboundSession.on_publish`. The success
path still decodes the header twice (preflight + specialised inbound). The
expensive part that #197 identified — copying the payload into a
`PublishPacket` — happens once, in the inbound handler, as owned `Message`
bytes.
