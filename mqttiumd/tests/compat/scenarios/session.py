"""Persistent sessions: offline queueing and redelivery on reconnection."""
import mqttwire as wire
from dsl import (ack, close, connect, disconnect, persistent, publish, publish_ack, publish_qos2, publish_sync, quiet,
                 recv, scenario, send, sleep, subscribe, until_close)

for level in (4, 5):
    v = f"v{level}"

    scenario(
        f"session/offline-queue-qos1-{v}",
        doc="""Two QoS 1 messages published while a persistent session is offline are sent
        after CONNACK on reconnection, in order and with DUP 0; once acknowledged they are
        not sent again.""",
        steps=[
            connect("sub", "o-q1", level=level, **persistent(level)),
            subscribe("sub", 1, [("offq1/t", 1)], level=level),
            disconnect("sub", level=level),
            connect("pub", "o-q1-pub", level=level),
            publish_ack("pub", "offq1/t", "m1", packet_id=1, level=level),
            publish_ack("pub", "offq1/t", "m2", packet_id=2, level=level),
            connect("sub", "o-q1", level=level, expect=3, **persistent(level)),
            ack("sub", nth=0), ack("sub", nth=1), quiet("sub"),
            disconnect("sub", level=level),
            connect("sub", "o-q1", level=level, **persistent(level)), quiet("sub"),
        ],
    )

    scenario(
        f"session/offline-queue-qos2-{v}",
        doc="""A QoS 2 message published while a persistent session is offline is delivered
        on reconnection through the complete PUBLISH, PUBREC, PUBREL, PUBCOMP exchange.""",
        steps=[
            connect("sub", "o-q2", level=level, **persistent(level)),
            subscribe("sub", 1, [("offq2/t", 2)], level=level),
            disconnect("sub", level=level),
            connect("pub", "o-q2-pub", level=level),
            publish_qos2("pub", "offq2/t", "exactly-once", level=level),
            connect("sub", "o-q2", level=level, expect=2, **persistent(level)),
            ack("sub", "pubrec"), recv("sub"), ack("sub", "pubcomp"), quiet("sub"),
        ],
    )

    scenario(
        f"session/redelivery-qos1-{v}",
        doc="""A QoS 1 PUBLISH the client received and did not acknowledge before the
        connection broke is sent again on reconnection with DUP 1 and the same Packet
        Identifier.""",
        steps=[
            connect("sub", "o-r1", level=level, **persistent(level)),
            subscribe("sub", 1, [("redo1/t", 1)], level=level),
            connect("pub", "o-r1-pub", level=level),
            publish_ack("pub", "redo1/t", "unacknowledged", level=level),
            recv("sub"), close("sub"),
            connect("sub", "o-r1", level=level, expect=2, **persistent(level)),
            ack("sub"), quiet("sub"),
        ],
    )

    scenario(
        f"session/redelivery-pubrel-{v}",
        doc="""The client answered a QoS 2 PUBLISH with PUBREC, received PUBREL and lost the
        connection before PUBCOMP: on reconnection the broker sends PUBREL again, not the
        PUBLISH.""",
        steps=[
            connect("sub", "o-rel", level=level, **persistent(level)),
            subscribe("sub", 1, [("redo2/t", 2)], level=level),
            connect("pub", "o-rel-pub", level=level),
            publish_qos2("pub", "redo2/t", "released", level=level),
            recv("sub"), ack("sub", "pubrec"), recv("sub"), close("sub"),
            connect("sub", "o-rel", level=level, expect=2, **persistent(level)),
            ack("sub", "pubcomp"), quiet("sub"),
        ],
    )

    scenario(
        f"session/ninth-exchange-after-reconnection-{v}",
        doc="""Upstream ticket 3809. A session opens eight QoS 2 exchanges, reads each
        PUBREC, completes none and loses its connection. It comes back, sends no PUBREL and
        publishes a ninth message: what the broker answers and whether it keeps the
        connection. The session then comes back once more and sends PUBREL for the nine
        identifiers: which of the nine messages a subscriber has received in the end. Both
        brokers have a window of eight (the reference by `max_inflight_messages 8`).""",
        reference_config=["max_inflight_messages 8"],
        steps=[
            connect("sub", "o-ninth-sub", level=level), subscribe("sub", 1, [("ninth/t", 0)], level=level),
            connect("a", "o-ninth", level=level, **persistent(level)),
            [[publish("a", "ninth/t", f"m{index}", qos=2, packet_id=index, level=level), recv("a")]
             for index in range(1, 9)],
            close("a"),
            connect("a", "o-ninth", level=level, **persistent(level)),
            publish("a", "ninth/t", "m9", qos=2, packet_id=9, level=level),
            until_close("a"),
            close("a"),
            connect("a", "o-ninth", level=level, **persistent(level)),
            [[send("a", wire.pubrel(index, level=level)), recv("a")] for index in range(1, 10)],
            quiet("sub"),
        ],
    )

scenario(
    "session/redelivery-pubrel-after-expiry-v5",
    doc="""Upstream ticket 3744. A QoS 2 message with Message Expiry Interval 1 was
    delivered and answered with PUBREC; the client received PUBREL and lost the connection
    before PUBCOMP. It comes back after the interval has passed: the delivery had begun,
    and the broker still owes the PUBREL.""",
    steps=[
        connect("sub", "o-rel-exp", level=5, **persistent(5)),
        subscribe("sub", 1, [("redo2x/t", 2)], level=5),
        connect("pub", "o-rel-exp-pub", level=5),
        publish_qos2("pub", "redo2x/t", "released", level=5, props={"message_expiry_interval": 1}),
        recv("sub"), ack("sub", "pubrec"), recv("sub"), close("sub"),
        sleep(2.0),
        connect("sub", "o-rel-exp", level=5, expect=2, **persistent(5)),
        ack("sub", "pubcomp"), quiet("sub"),
    ],
)

scenario(
    "session/offline-qos0-v4",
    doc="""A QoS 0 message published while a persistent session with a QoS 1 subscription
    is offline: whether it is kept for the reconnection.""",
    steps=[
        connect("sub", "o-q0", clean=False),
        subscribe("sub", 1, [("offq0/t", 1)]),
        disconnect("sub"),
        connect("pub", "o-q0-pub"),
        publish_sync("pub", "offq0/t", "at-most-once"),
        connect("sub", "o-q0", clean=False), quiet("sub"),
    ],
)

del level, v
