"""Packets a broker must refuse: oversized, malformed framing, reserved types, invalid strings."""
import mqttwire as wire
from dsl import connect, mark, publish, quiet, recv, scenario, send, until_close
from expectations import CLOSED, at, expectation

for level in (4, 5):
    v = f"v{level}"

    scenario(
        f"errors/oversized-packet-{v}",
        doc="""A QoS 1 PUBLISH of 70 000 bytes with each broker's default limits: mqttiumd
        accepts packets up to 65 536 bytes, the reference up to the protocol maximum.""",
        steps=[connect("a", "e-big", level=level),
               publish("a", "big/t", b"x" * 70_000, qos=1, packet_id=1, level=level), recv("a"), quiet("a")],
    )

    scenario(
        f"errors/malformed-remaining-length-{v}",
        doc="A fixed header whose Remaining Length runs over a fifth byte: the connection is closed.",
        steps=[connect("a", "e-length", level=level),
               send("a", wire.raw(0x30, b"\xff\xff\xff\xff\x7f"), note="PUBLISH header with a five-byte Remaining Length"),
               until_close("a")],
    )

scenario(
    "errors/packet-over-announced-maximum-v5",
    doc="""Both brokers announce Maximum Packet Size 32 776 (the reference by
    `max_packet_size 32776`) and the client sends a PUBLISH of 70 000 bytes.
    Both answer DISCONNECT 0x95. The reference then closes with the rest of
    the packet unread, which resets the connection; mqttiumd reads and drops
    it and ends the stream. How the connection ends after the DISCONNECT is
    outside the protocol and depends on what was still in flight: the
    closure is compared as `any`.""",
    reference_config=["max_packet_size 32776"],
    steps=[connect("a", "e-toobig", level=5),
           publish("a", "big/t", b"x" * 70_000, qos=1, packet_id=1, level=5), until_close("a")],
    closure="any",
)

# The Maximum Packet Size mqttiumd announces: the size at which no PUBLISH
# exceeds its payload limit (32 768 bytes and eight bytes around them).
ANNOUNCED = 32_776


def sized_publish(total, packet_id):
    """A QoS 1 MQTT 5 PUBLISH that takes exactly `total` bytes on the wire."""
    data = wire.publish("big/t", b"x" * (total - 14), qos=1, packet_id=packet_id, level=5)
    if len(data) != total:
        raise ValueError(f"the PUBLISH takes {len(data)} bytes on the wire, not {total}")
    return data


scenario(
    "errors/packet-at-announced-maximum-v5",
    doc=f"""Both brokers announce Maximum Packet Size {ANNOUNCED} (the reference by
    `max_packet_size {ANNOUNCED}`). A PUBLISH that takes exactly that many bytes on the
    wire, fixed header included, is acknowledged; the next one, a byte larger, is answered
    DISCONNECT 0x95 and the connection is closed. How the connection ends after the
    DISCONNECT depends on what was still in flight: the closure is compared as `any`.""",
    reference_config=[f"max_packet_size {ANNOUNCED}"],
    steps=[
        connect("a", "e-exact", level=5), mark("connack"),
        send("a", sized_publish(ANNOUNCED, 1), note=f"PUBLISH of {ANNOUNCED} bytes on the wire"), recv("a"),
        mark("exact"),
        send("a", sized_publish(ANNOUNCED + 1, 2), note=f"PUBLISH of {ANNOUNCED + 1} bytes on the wire"),
        until_close("a"), mark("over"),
    ],
    closure="any",
    expectations=[expectation(
        "limit",
        statement="MQTT-4.13.1-1, with section 3.2.2.3.6: a packet larger than the announced Maximum Packet Size "
                  "is a Protocol Error with the Reason Code 0x95",
        requires=f"Where CONNACK announces Maximum Packet Size {ANNOUNCED}: a PUBLISH of exactly that size is "
                 "acknowledged, and one a byte larger is answered DISCONNECT 0x95 and the connection is closed",
        when=[at("connack", {"got": [{"type": "CONNACK", "properties": {"maximum_packet_size": ANNOUNCED}}]})],
        then=[at("exact", {"got": [{"type": "PUBACK", "packet_id": 1}], "end": "ok"}),
              at("over", {"got": [{"type": "DISCONNECT", "reason": 0x95}], "end": CLOSED})],
    )],
)

del level, v
