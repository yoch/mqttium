# Investigation — MQTT 5 peer `maximum_packet_size < 4`

Base: `main@0053fc99c85a52747addb873c7c2b7533928f0d3`

## Protocol constraint

A Server may legally advertise any non-zero MQTT 5 Maximum Packet Size. Once advertised, the Client MUST NOT send an MQTT Control Packet larger than that value. Independently, a receiver of QoS 1/2 traffic MUST send PUBACK/PUBREC/PUBCOMP as required by the QoS state machine. The minimal success form of each of those acknowledgements is four bytes.

Therefore a peer limit of 1–3 bytes is legal to advertise but incompatible with MQTTium guaranteeing its normal QoS receiver obligations. The Server is not blamed for a Protocol Error; this is a local capability mismatch.

## Existing cost

The current implementation keeps connection-scoped `_tiny_peer_packet_limit` state in `InboundSession`, dynamically rebinds automatic QoS 1 handling, and carries dedicated branches through QoS 1, QoS 2 and PUBREL processing so the failure happens only when a mandatory acknowledgement is eventually needed. A dedicated edge-case test module exists to preserve precedence among those branches.

## Proposed policy

Fail the successful MQTT 5 CONNACK locally when the negotiated Server Maximum Packet Size is below four bytes, before entering `CONNECTED`, replaying Session state, or admitting application traffic.

Use `MandatoryResponseTooLargeError` (a `PacketTooLargeError`) so the failure stays local and is never converted into a peer-attributed `PROTOCOL_ERROR` or DISCONNECT reason.

Keep generic outbound packet-size checks, including mandatory outbound PUBREL handling, as defensive invariants for direct engine consumers and manually constructed state.

## Success criteria

- remove `_tiny_peer_packet_limit` and its specialized inbound handler/branches;
- preserve exact-limit (`4`) QoS 1/2 behavior;
- preserve durable Session state if a resumed connection is rejected at CONNACK;
- runtime `connect()` fails terminally without reconnecting or sending a peer-blaming DISCONNECT;
- full CI and soak green on the exact final head.
