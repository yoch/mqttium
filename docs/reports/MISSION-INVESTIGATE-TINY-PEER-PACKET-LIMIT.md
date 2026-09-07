# Investigation — MQTT 5 peer `maximum_packet_size < 4`

Base: `main@0053fc99c85a52747addb873c7c2b7533928f0d3`

## Protocol constraint

A Server may legally advertise any non-zero MQTT 5 Maximum Packet Size. Once advertised, the Client MUST NOT send an MQTT Control Packet larger than that value. Independently, a receiver of QoS 1/2 traffic MUST send PUBACK/PUBREC/PUBCOMP as required by the QoS state machine. The minimal success form of each of those acknowledgements is four bytes.

Therefore a peer limit of 1–3 bytes is legal to advertise but incompatible with MQTTium guaranteeing its normal QoS receiver obligations. The Server is not blamed for a Protocol Error; this is a local capability mismatch.

## Previous cost

The previous implementation kept connection-scoped `_tiny_peer_packet_limit` state in `InboundSession`, dynamically rebound automatic QoS 1 handling, and carried dedicated branches through QoS 1, QoS 2 and PUBREL processing so the failure happened only when a mandatory acknowledgement was eventually needed. A dedicated edge-case test module existed to preserve precedence among those branches.

## Final policy

Fail the successful MQTT 5 CONNACK locally when the negotiated Server Maximum Packet Size is below four bytes, before entering `CONNECTED`, replaying Session state, starting keepalive ownership, or admitting application traffic.

Use `MandatoryResponseTooLargeError` (a `PacketTooLargeError`) so the failure stays local and is never converted into a peer-attributed `PROTOCOL_ERROR` or DISCONNECT reason. While CONNECT is pending, the reader propagates this original exception to the CONNACK waiter instead of leaving `connect()` to time out.

Generic outbound packet-size checks, including mandatory outbound PUBREL handling, remain as defensive invariants for direct engine consumers and manually constructed state.

## Result

- `_tiny_peer_packet_limit`, its connection-time binder, the automatic-QoS1 tiny variant and the inbound PUBREC/PUBCOMP tiny branches are removed;
- runtime implementation changes are net negative: 16 additions / 39 deletions across `async_client.py`, `engine.py` and `inbound.py` (−23 lines net);
- the 240-line tiny-peer precedence test module is removed and the remaining tests concentrate on the new negotiation boundary;
- limits 1, 2 and 3 fail locally during successful-CONNACK processing;
- limit 4 remains supported for automatic PUBACK, the complete QoS 2 PUBREC/PUBCOMP exchange, and manual PUBACK;
- durable QoS 2 state remains unchanged when a resumed Session is rejected by a tiny limit;
- runtime `connect()` receives the original local error, does not reconnect, does not start keepalive ownership, and does not send a peer-blaming DISCONNECT;
- focused qualification and the complete `tests/unit` suite passed on implementation commit `ead74b744ed389ff9a6d2110448a01f175815a26` before this documentation-only finalization.

Full CI and soak remain the final merge gate on the exact final head.
