"""Persistent sessions: offline queueing and redelivery on reconnection."""
from dsl import (ack, close, connect, disconnect, persistent, publish_ack, publish_qos2, publish_sync, quiet, recv,
                 scenario, subscribe)

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
