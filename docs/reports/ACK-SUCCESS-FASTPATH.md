# Complete the success-ack fast path

Date: 2026-08-12

Base commit: `c34a949` (`1.0.0rc2`).

## Question

Should PUBREC, PUBREL and PUBCOMP use the same success/no-properties fast path
already shipped for PUBACK?

## Established choice that this keeps

PR #175 (`Fast-path success PUBACK encode and settle`) established that a
success acknowledgement with no properties is a fixed four-byte frame in both
protocol versions, and that constructing a packet dataclass only to recover a
MID is wasted work. `_encode_ack_with_reason` already returns that frame when
`reason_code == 0` and properties are empty. Inbound auto-PUBACK and outbound
PUBACK settle already skip the dataclass.

This PR extends that choice. It does not change reason-code validation, MQTT 5
property decode, orphan PUBREL 0x92, or the QoS 2 state machine.

## Defect

The common QoS 2 handshake is four success frames: PUBREC, PUBREL, PUBCOMP, and
the inbound PUBREC a subscriber sends. Each still constructed a packet object:

- inbound `_on_qos2` called `PubRecPacket(mid=mid).encode(...)` (652 ns) for a
  frame `_encode_ack_with_reason` would have built as four bytes (67 ns);
- inbound `on_pubrel` always called `PubRelPacket.decode` (750 ns) even when
  remaining length was 2, against 55 ns to read the MID;
- outbound `on_pubrec` / `on_pubcomp` did the same, while `on_puback` already
  had the two-byte remaining-length branch.

That is leftover inconsistency, not a measured rejection.

## Change

- `encode_success_ack(packet_type, mid, flags=0)` is the shared four-byte
  encoder in `packets/acks.py`. `_encode_ack_with_reason` uses it, so the
  generic packet classes and the sessions cannot drift.
- Inbound auto-PUBREC / PUBCOMP and manual ACK success frames call it directly.
- Outbound success PUBREL (including retained `encoded_pubrel`) calls it with
  `flags=0x02`.
- `on_pubrec`, `on_pubcomp` and `on_pubrel` take the two-byte remaining-length
  branch already used by `on_puback`. Longer bodies still use the generic
  decoder (reason codes, properties).

## Impact

- **Correctness.** Success frames are byte-identical to `Pub*Packet.encode()`.
  Reason-bearing MQTT 5 PUBREC/PUBACK still hit the full decoder (pinned).
  Orphan PUBREL with reason 0x92 is unchanged.
- **Performance.** Removes one dataclass construction per success QoS 2
  acknowledgement on both legs. Measured locally: encode 652 ns → 67 ns,
  decode 750 ns → 55 ns.
- **API.** `encode_success_ack` lives on `mqttium.packets.acks` and is not
  added to `packets.__all__`. No Stable-tier change.
- **What this is not.** It does not skip MID validation (`require_nonzero_mid`
  still runs). It does not change EffectPump SEND-first ordering.

Hosted-runner wall-clock is not claimed. The mechanism is the same redundant
packet construction #175 already removed for PUBACK.
