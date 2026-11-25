"""Will messages: QoS 0/1/2, retain, abrupt close, DISCONNECT with and without Will, takeover."""
import mqttwire as wire
from dsl import (ack, close, collect, connect, disconnect, mark, publish_sync, quiet, recv, scenario, send, subscribe)
from expectations import at, expectation


def acknowledge(conn, qos):
    """The subscriber's side of a delivery at `qos`."""
    if qos == 1:
        return [ack(conn)]
    if qos == 2:
        return [ack(conn, "pubrec"), recv(conn), ack(conn, "pubcomp")]
    return []


def abrupt(level, qos):
    v = f"v{level}"
    scenario(
        f"will/abrupt-close-qos{qos}-{v}",
        doc=f"""A client with a QoS {qos} Will closes its connection without DISCONNECT:
        the Will is published to a QoS 2 subscription at QoS {qos}.""",
        steps=[
            connect("sub", f"w-q{qos}-sub", level=level), subscribe("sub", 1, [("will/t", 2)], level=level),
            connect("w", f"w-q{qos}", level=level, will={"topic": "will/t", "payload": "gone", "qos": qos}),
            close("w"),
            recv("sub"), acknowledge("sub", qos), quiet("sub"),
        ],
    )


abrupt(4, 0)
abrupt(4, 1)
abrupt(5, 1)
abrupt(4, 2)
abrupt(5, 2)

for level in (4, 5):
    v = f"v{level}"

    scenario(
        f"will/retain-{v}",
        doc="""A QoS 1 Will with Will Retain 1 is published on abrupt close and kept as the
        retained message of its topic: a later subscriber receives it with RETAIN 1.""",
        steps=[
            connect("sub", "w-ret-sub", level=level), subscribe("sub", 1, [("willr/t", 1)], level=level),
            connect("w", "w-ret", level=level,
                    will={"topic": "willr/t", "payload": "gone", "qos": 1, "retain": True}),
            close("w"),
            recv("sub"), ack("sub"), quiet("sub"),
            connect("late", "w-ret-late", level=level),
            subscribe("late", 1, [("willr/t", 0)], level=level, expect=0), collect("late", sort=True),
        ],
    )

    scenario(
        f"will/disconnect-discards-will-{v}",
        doc="A normal DISCONNECT discards the Will: the subscriber receives only a later marker message.",
        steps=[
            connect("sub", "w-disc-sub", level=level), subscribe("sub", 1, [("willd/t", 0)], level=level),
            connect("w", "w-disc", level=level, will={"topic": "willd/t", "payload": "gone", "qos": 1}),
            disconnect("w", level=level),
            connect("pub", "w-disc-pub", level=level), publish_sync("pub", "willd/t", "marker", level=level),
            collect("sub"),
        ],
    )

scenario(
    "will/disconnect-with-will-v5",
    doc="DISCONNECT with Reason Code 0x04 (Disconnect with Will Message): the Will is published.",
    steps=[
        connect("sub", "w-dww-sub", level=5), subscribe("sub", 1, [("willw/t", 0)], level=5),
        connect("w", "w-dww", level=5, will={"topic": "willw/t", "payload": "gone", "qos": 1}),
        send("w", wire.disconnect(level=5, reason=0x04)), close("w", wait_peer=1.0),
        recv("sub"), quiet("sub"),
    ],
)

for reason, name in ((0x00, "normal"), (0x04, "with-will"), (0x80, "unspecified-error"),
                     (0x83, "implementation-error")):
    scenario(
        f"will/disconnect-{name}-expiry-zero-v5",
        doc=f"""A client with a Will and a session of sixty seconds sends DISCONNECT with
        Reason Code {reason:#04x} and Session Expiry Interval 0. What is recorded: whether
        the Will is published, and whether the session is still there when the same
        ClientID comes back with Clean Start 0.""",
        steps=[
            connect("sub", f"w-dr{reason:02x}-sub", level=5),
            subscribe("sub", 1, [(f"willr{reason:02x}/t", 0)], level=5),
            connect("w", f"w-dr{reason:02x}", level=5, props={"session_expiry_interval": 60},
                    will={"topic": f"willr{reason:02x}/t", "payload": "gone", "qos": 1}),
            send("w", wire.disconnect(level=5, reason=reason, props={"session_expiry_interval": 0})),
            close("w", wait_peer=1.0),
            collect("sub"), mark("will"),
            connect("w", f"w-dr{reason:02x}", level=5, clean=False, props={"session_expiry_interval": 60}),
            mark("resume"),
        ],
        expectations=[expectation(
            "will-and-session", statement="MQTT-3.1.2-8, MQTT-3.14.4-3, MQTT-3.2.2-3",
            requires=("Normal DISCONNECT discards the Will and expiry zero ends the session." if reason == 0 else
                      "The client receives the Will and expiry zero ends the publishing session."),
            then=[at("will", {"got": [] if reason == 0 else [{"type": "PUBLISH", "topic": f"willr{reason:02x}/t",
                                                              "payload": "gone", "qos": 0}], "end": "quiet"}),
                  at("resume", {"got": [{"type": "CONNACK", "session_present": False, "reason": 0}], "end": "ok"})])],
    )

scenario(
    "will/delay-interval-v5",
    doc="""A Will with Will Delay Interval 1 on a session that outlives the connection is
    published after an abrupt close (observed within 3 s, not timed).""",
    steps=[
        connect("sub", "w-delay-sub", level=5), subscribe("sub", 1, [("willdelay/t", 0)], level=5),
        connect("w", "w-delay", level=5, props={"session_expiry_interval": 60},
                will={"topic": "willdelay/t", "payload": "gone", "qos": 1, "props": {"will_delay_interval": 1}}),
        close("w"),
        recv("sub"), quiet("sub"),
    ],
)

scenario(
    "will/takeover-v5",
    doc="""A client with a Will is taken over by a new connection with the same ClientID
    and Clean Start 1: the Will of the first connection is published.""",
    steps=[
        connect("sub", "w-take-sub", level=5), subscribe("sub", 1, [("willt/t", 0)], level=5),
        connect("w", "w-take", level=5, will={"topic": "willt/t", "payload": "gone", "qos": 1}),
        connect("w2", "w-take", level=5),
        recv("sub"), quiet("sub"),
    ],
)

del level, v, reason, name
