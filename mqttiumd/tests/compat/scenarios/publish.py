"""PUBLISH in both directions: QoS 0/1/2, downgrade, duplicates, invalid topics, flow control."""
import mqttwire as wire
from dsl import (ack, collect, connect, disconnect, mark, publish, publish_ack, publish_qos2, publish_sync, quiet, recv,
                 scenario, send, subscribe, tally, until_close)
from expectations import at, at_most, expectation

for level in (4, 5):
    v = f"v{level}"

    scenario(
        f"publish/qos0-{v}",
        doc="A QoS 0 message reaches a QoS 0 subscription once, with DUP 0 and no Packet Identifier.",
        steps=[
            connect("sub", "p-q0-sub", level=level), subscribe("sub", 1, [("q0/t", 0)], level=level),
            connect("pub", "p-q0-pub", level=level), publish_sync("pub", "q0/t", "zero", level=level),
            recv("sub"), quiet("sub"),
        ],
    )

    scenario(
        f"publish/qos1-{v}",
        doc="QoS 1 in and out: PUBACK to the publisher, PUBLISH to the subscriber, its PUBACK, then silence.",
        steps=[
            connect("sub", "p-q1-sub", level=level), subscribe("sub", 1, [("q1/t", 1)], level=level),
            connect("pub", "p-q1-pub", level=level), publish_ack("pub", "q1/t", "one", packet_id=21, level=level),
            recv("sub"), ack("sub"), quiet("sub"), quiet("pub"),
        ],
    )

    scenario(
        f"publish/qos2-{v}",
        doc="QoS 2 in and out: both four-packet exchanges played to their end, then silence.",
        steps=[
            connect("sub", "p-q2-sub", level=level), subscribe("sub", 1, [("q2/t", 2)], level=level),
            connect("pub", "p-q2-pub", level=level), publish_qos2("pub", "q2/t", "two", packet_id=22, level=level),
            recv("sub"), ack("sub", "pubrec"), recv("sub"), ack("sub", "pubcomp"), quiet("sub"), quiet("pub"),
        ],
    )

    scenario(
        f"publish/duplicate-qos2-publish-{v}",
        doc="""A QoS 2 PUBLISH is sent again with DUP 1 before PUBREL: it is acknowledged
        again and the subscriber still receives one message.""",
        steps=[
            connect("sub", "p-q2dup-sub", level=level), subscribe("sub", 1, [("q2dup/t", 0)], level=level),
            connect("pub", "p-q2dup-pub", level=level),
            publish("pub", "q2dup/t", "once", qos=2, packet_id=7, level=level), recv("pub"),
            publish("pub", "q2dup/t", "once", qos=2, packet_id=7, dup=True, level=level), recv("pub"),
            send("pub", wire.pubrel(7, level=level)), recv("pub"),
            recv("sub"), quiet("sub"),
        ],
    )

    scenario(
        f"publish/pubrel-retransmit-{v}",
        doc="""PUBREL is sent again after the QoS 2 exchange completed: it is answered by
        PUBCOMP again and the message is not delivered a second time.""",
        steps=[
            connect("sub", "p-rel-sub", level=level), subscribe("sub", 1, [("rel/t", 0)], level=level),
            connect("pub", "p-rel-pub", level=level), publish_qos2("pub", "rel/t", "once", packet_id=9, level=level),
            send("pub", wire.pubrel(9, level=level)), recv("pub"),
            recv("sub"), quiet("sub"), quiet("pub"),
        ],
    )

    scenario(
        f"publish/topic-with-wildcard-{v}",
        doc="A PUBLISH whose Topic Name contains the wildcard `#`: the connection is closed.",
        steps=[connect("a", "p-wild", level=level), publish("a", "wild/#", "x", level=level), until_close("a")],
    )

scenario(
    "publish/qos-downgrade-v4",
    doc="""A subscription granted QoS 1 receives a QoS 0, a QoS 1 and a QoS 2 message:
    each is delivered at the lower of the two QoS, in publication order.""",
    steps=[
        connect("sub", "p-down-sub"), subscribe("sub", 1, [("down/t", 1)]),
        connect("pub", "p-down-pub"),
        publish_sync("pub", "down/t", "published-qos0"),
        publish_ack("pub", "down/t", "published-qos1", packet_id=2),
        publish_qos2("pub", "down/t", "published-qos2", packet_id=3),
        recv("sub", 3), ack("sub", nth=0), ack("sub", nth=1), quiet("sub"),
    ],
)

scenario(
    "publish/duplicate-qos1-publish-v4",
    doc="""A QoS 1 PUBLISH is sent again with DUP 1 and the same Packet Identifier after
    its PUBACK: it is a new publication, acknowledged and delivered again, and DUP is
    not propagated to the subscriber.""",
    steps=[
        connect("sub", "p-dup-sub"), subscribe("sub", 1, [("dup/t", 1)]),
        connect("pub", "p-dup-pub"),
        publish_ack("pub", "dup/t", "at-least-once", packet_id=5),
        publish_ack("pub", "dup/t", "at-least-once", packet_id=5, dup=True),
        recv("sub", 2), ack("sub", nth=0), ack("sub", nth=1), quiet("sub"),
    ],
)

scenario(
    "publish/pipelined-qos1-v4",
    doc="""A MQTT 3.1.1 client writes twenty QoS 1 PUBLISH packets at once, before reading
    any PUBACK: MQTT 3.1.1 has no Receive Maximum, and all twenty are acknowledged, in
    the order of the PUBLISH packets (the transcript keeps whether all PUBACK came and
    whether their Packet Identifiers came as sent, not how many).""",
    steps=[
        connect("pub", "p-pipe-pub"),
        send("pub", b"".join(wire.publish("pipe/t", f"m{index}", qos=1, packet_id=index) for index in range(1, 21)),
             note="twenty QoS 1 PUBLISH on pipe/t, Packet Identifiers 1 to 20, in one write"),
        tally("pub", 20, kind="PUBACK", sequence=range(1, 21)),
    ],
)

scenario(
    "publish/burst-qos0-v4",
    doc="""One hundred QoS 0 messages written at once reach an online QoS 0 subscriber;
    the transcript keeps whether all came and whether those that came are in the order
    sent, not how many.""",
    steps=[
        connect("sub", "p-burst-sub"), subscribe("sub", 1, [("burst/t", 0)]),
        connect("pub", "p-burst-pub"),
        send("pub", b"".join(wire.publish("burst/t", f"m{index}") for index in range(100)),
             note="one hundred QoS 0 PUBLISH on burst/t in one write"),
        send("pub", wire.pingreq()), recv("pub"),
        tally("sub", 100, sequence=[f"m{index}" for index in range(100)]),
    ],
)

scenario(
    "publish/payload-40000-bytes-v5",
    doc="""A QoS 1 PUBLISH with a 40 000-byte payload, below the Maximum Packet Size
    either broker announces, is acknowledged and delivered.""",
    steps=[
        connect("sub", "p-40k-sub", level=5), subscribe("sub", 1, [("big40/t", 0)], level=5),
        connect("pub", "p-40k-pub", level=5),
        publish("pub", "big40/t", b"x" * 40_000, qos=1, packet_id=1, level=5), recv("pub"),
        collect("sub"), quiet("pub"),
    ],
)

scenario(
    "publish/qos0-with-dup-v5",
    doc="A QoS 0 PUBLISH with DUP 1, which the specification forbids.",
    steps=[connect("a", "p-dup0", level=5), publish("a", "dup0/t", "x", dup=True, level=5), until_close("a")],
)

scenario(
    "publish/no-matching-subscribers-v5",
    doc="A QoS 1 and a QoS 2 PUBLISH that match no subscription: the Reason Codes of PUBACK and PUBREC.",
    steps=[connect("a", "p-nobody", level=5), publish_ack("a", "nobody/t", "x", level=5),
           publish_qos2("a", "nobody/t", "y", packet_id=2, level=5)],
)

scenario(
    "publish/receive-maximum-inbound-v5",
    doc="""Both brokers announce Receive Maximum 8 (the reference by `max_inflight_messages
    8`). The client opens eight QoS 2 exchanges, reads each PUBREC, completes none and
    sends a ninth PUBLISH.""",
    reference_config=["max_inflight_messages 8"],
    steps=[
        connect("a", "p-rmax", level=5),
        [[publish("a", "rmax/t", f"m{index}", qos=2, packet_id=index, level=5), recv("a")] for index in range(1, 9)],
        publish("a", "rmax/t", "m9", qos=2, packet_id=9, level=5),
        until_close("a"),
    ],
)

scenario(
    "publish/receive-maximum-outbound-v5",
    doc="""The client announces Receive Maximum 2 and acknowledges nothing: of four QoS 1
    messages only two are sent to it; once it acknowledges them the next two follow.""",
    steps=[
        connect("sub", "p-window-sub", level=5, props={"receive_maximum": 2}),
        subscribe("sub", 1, [("window/t", 1)], level=5),
        connect("pub", "p-window-pub", level=5),
        [publish_ack("pub", "window/t", f"m{index}", packet_id=index, level=5) for index in range(1, 5)],
        recv("sub", 2), quiet("sub"),
        ack("sub", nth=0), ack("sub", nth=1),
        recv("sub", 2), ack("sub", nth=2), ack("sub", nth=3), quiet("sub"),
    ],
)

def limited(maximum_packet_size):
    """CONNECT options of the subscriber that keeps its session and
    accepts packets of at most `maximum_packet_size` bytes."""
    return {"clean": False, "props": {"session_expiry_interval": 3600, "maximum_packet_size": maximum_packet_size}}


for qos in (1, 2):
    published = publish_ack if qos == 1 else publish_qos2
    # The subscriber's side of one delivery, played to its end.
    answered = [ack("sub")] if qos == 1 else [ack("sub", "pubrec"), recv("sub"), ack("sub", "pubcomp")]

    scenario(
        f"publish/over-maximum-packet-size-qos{qos}-v5",
        doc=f"""A subscriber with a kept session announces Maximum Packet Size 64 and subscribes
        at QoS {qos}. A message of 100 bytes published at QoS {qos}, which would take 114 bytes
        on the wire, is accepted from its publisher and not sent to the subscriber, whose
        connection stays open; the next message, which fits, is delivered and acknowledged.
        The session then resumes with Maximum Packet Size 1024: nothing is redelivered, and
        the first PUBLISH it reads is a new message.""",
        steps=[
            connect("sub", f"p-limit{qos}-sub", level=5, **limited(64)),
            subscribe("sub", 1, [("limit/t", qos)], level=5),
            connect("pub", f"p-limit{qos}-pub", level=5),
            published("pub", "limit/t", b"x" * 100, packet_id=1, level=5),
            quiet("sub"), mark("large"),
            published("pub", "limit/t", "small", packet_id=2, level=5),
            recv("sub"), mark("small"), answered, quiet("sub"), mark("after-small"),
            disconnect("sub", level=5),
            connect("sub", f"p-limit{qos}-sub", level=5, **limited(1024)), mark("resumed"),
            quiet("sub"), mark("no-redelivery"),
            published("pub", "limit/t", "next", packet_id=3, level=5),
            recv("sub"), mark("next"), answered, quiet("sub"),
        ],
        expectations=[
            expectation(
                "discarded", statement="MQTT-3.1.2-25, with MQTT-3.1.2-24 for the size of what is sent",
                requires="Nothing is sent for the message that would take 114 bytes and the connection stays open; "
                         "the next message is delivered in a packet of at most 64 bytes, and nothing follows it",
                then=[at("large", {"got": [], "end": "quiet"}),
                      at("small", {"got": [{"type": "PUBLISH", "topic": "limit/t", "payload": "small", "dup": False,
                                            "wire_bytes": at_most(64)}], "end": "ok"}),
                      at("after-small", {"got": [], "end": "quiet"})]),
            expectation(
                "not-redelivered", statement="MQTT-3.1.2-25, with MQTT-3.2.2-3 for Session Present",
                requires="The resumed session (Session Present 1), which now accepts 1024 bytes, is sent nothing "
                         "for the discarded message: the first PUBLISH it reads is a new message, with DUP 0",
                then=[at("resumed", {"got": [{"type": "CONNACK", "session_present": True, "reason": 0}], "end": "ok"}),
                      at("no-redelivery", {"got": [], "end": "quiet"}),
                      at("next", {"got": [{"type": "PUBLISH", "topic": "limit/t", "payload": "next", "dup": False}],
                                  "end": "ok"})]),
        ],
    )

scenario(
    "publish/cross-version-v5-to-v4",
    version="3.1.1 + 5.0",
    doc="""A MQTT 5 publisher sets Message Expiry Interval and a MQTT 3.1.1 subscriber
    receives the message, without any property.""",
    steps=[
        connect("sub", "p-x54-sub", level=4), subscribe("sub", 1, [("x54/t", 1)], level=4),
        connect("pub", "p-x54-pub", level=5),
        publish_ack("pub", "x54/t", "from-v5", level=5, props={"message_expiry_interval": 600}),
        recv("sub"), ack("sub"), quiet("sub"),
    ],
)

del level, v, qos, published, answered
