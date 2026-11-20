"""Packets a broker must refuse: oversized, malformed framing, reserved types, invalid strings."""
import mqttwire as wire
from dsl import connect, publish, quiet, recv, scenario, send, until_close

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
    doc="""Both brokers announce Maximum Packet Size 65 536 (the reference by
    `max_packet_size 65536`) and the client sends a PUBLISH of 70 000 bytes.""",
    reference_config=["max_packet_size 65536"],
    steps=[connect("a", "e-toobig", level=5),
           publish("a", "big/t", b"x" * 70_000, qos=1, packet_id=1, level=5), until_close("a")],
)

del level, v
