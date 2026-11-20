"""CONNECT, CONNACK, PINGREQ, DISCONNECT: identifiers, flags, levels, credentials, takeover."""
import mqttwire as wire
from dsl import (ack, connect, disconnect, open_, persistent, publish_ack, publish_sync, quiet, recv, scenario,
                 send, sleep, subscribe, until_close)

for level in (4, 5):
    v = f"v{level}"

    scenario(
        f"connect/clean-start-{v}",
        doc="A first CONNECT with Clean Session/Clean Start 1 is accepted with Session Present 0.",
        steps=[connect("a", "c-clean", level=level), quiet("a")],
    )

    scenario(
        f"connect/session-resume-{v}",
        doc="""A kept session reports Session Present 0 when created and 1 when resumed, and
        its subscription still routes a message published after the reconnection.""",
        steps=[
            connect("a", "c-resume", level=level, **persistent(level)),
            subscribe("a", 1, [("resume/t", 1)], level=level),
            disconnect("a", level=level),
            connect("a", "c-resume", level=level, **persistent(level)),
            connect("pub", "c-resume-pub", level=level),
            publish_ack("pub", "resume/t", "after", level=level),
            recv("a"), ack("a"), quiet("a"),
        ],
    )

    scenario(
        f"connect/duplicate-connect-{v}",
        doc="A second CONNECT on an established connection is a protocol violation: the connection is closed.",
        steps=[connect("a", "c-twice", level=level), send("a", wire.connect("c-twice", level=level)),
               until_close("a")],
    )

    scenario(
        f"connect/username-password-{v}",
        doc="CONNECT with User Name and Password on a listener that accepts anonymous clients.",
        steps=[connect("a", "c-user", level=level, username="alice", password="secret"), quiet("a")],
    )

    scenario(
        f"connect/takeover-{v}",
        doc="""A second connection presents the ClientID of a connected client: the first
        connection is closed and the second stays usable.""",
        steps=[connect("a", "c-same", level=level), connect("b", "c-same", level=level),
               until_close("a"), quiet("b")],
    )

    scenario(
        f"connect/disconnect-{v}",
        doc="After DISCONNECT the broker closes the network connection without sending anything.",
        steps=[connect("a", "c-bye", level=level), send("a", wire.disconnect(level=level)), until_close("a")],
    )

scenario(
    "connect/clean-start-discards-session-v4",
    doc="""Clean Session 1 discards a kept session: Session Present 0, the old subscription
    no longer routes, and nothing is left for a later resume.""",
    steps=[
        connect("a", "c-discard", clean=False),
        subscribe("a", 1, [("discard/t", 1)]),
        disconnect("a"),
        connect("a", "c-discard", clean=True),
        connect("pub", "c-discard-pub"),
        publish_sync("pub", "discard/t", "nobody"),
        quiet("a"),
        disconnect("a"),
        connect("a", "c-discard", clean=False), quiet("a"),
    ],
)

scenario(
    "connect/empty-clientid-v4",
    doc="MQTT 3.1.1: a zero-length ClientID with Clean Session 1 is given a unique identifier and accepted.",
    steps=[connect("a", "", level=4, clean=True), quiet("a")],
)

scenario(
    "connect/empty-clientid-nonclean-v4",
    doc="MQTT 3.1.1: a zero-length ClientID with Clean Session 0 is refused with return code 2.",
    steps=[connect("a", "", level=4, clean=False), until_close("a")],
)

scenario(
    "connect/empty-clientid-v5",
    doc="MQTT 5: a zero-length ClientID is accepted and CONNACK carries an Assigned Client Identifier.",
    steps=[connect("a", "", level=5), quiet("a")],
)

scenario(
    "connect/bad-protocol-name-v4",
    doc="CONNECT with the protocol name MQTX and level 4.",
    steps=[open_("a"), send("a", wire.connect("c-name", level=4, protocol_name="MQTX")), until_close("a")],
)

scenario(
    "connect/bad-protocol-level-v4",
    doc="CONNECT with the protocol name MQTT and the unknown level 6: return code 1, then closed.",
    steps=[open_("a"), send("a", wire.connect("c-level", level=4, level_byte=6), note="CONNECT with protocol level 6"),
           until_close("a")],
)

scenario(
    "connect/legacy-mqtt31",
    version="3.1",
    doc="CONNECT of the older MQTT 3.1 (protocol name MQIsdp, level 3), outside both specifications.",
    steps=[open_("a"), send("a", wire.connect("c-mqtt31", level=4, level_byte=3, protocol_name="MQIsdp"),
                note="CONNECT with protocol name MQIsdp and level 3"),
           recv("a"), quiet("a")],
)

scenario(
    "connect/keepalive-zero-v4",
    doc="Keep Alive 0 turns the keep alive timeout off: a silent client is still served after 1.5 s.",
    steps=[connect("a", "c-nokeepalive", keepalive=0), sleep(1.5), quiet("a")],
)

scenario(
    "connect/keepalive-timeout-v4",
    doc="Keep Alive 1 and then silence: the broker closes the connection (observed within 5 s, not timed).",
    steps=[connect("a", "c-idle", keepalive=1), until_close("a", timeout=5.0)],
)

scenario(
    "connect/twenty-connections-v4",
    doc="Twenty clients connected at the same time are all accepted and the last one is served.",
    steps=[[connect(f"c{index:02d}", f"c-many-{index:02d}") for index in range(1, 21)], quiet("c20")],
)

scenario(
    "connect/user-property-v5",
    doc="A CONNECT carrying a User Property is accepted like one without.",
    steps=[connect("a", "c-userprop", level=5, props=[("user_property", ("k", "v"))]), quiet("a")],
)

scenario(
    "connect/ping-v4",
    doc="PINGREQ is answered by PINGRESP, twice in a row.",
    steps=[connect("a", "c-ping"), send("a", wire.pingreq()), recv("a"), send("a", wire.pingreq()), recv("a")],
)

scenario(
    "connect/connack-capabilities-v5",
    doc="The capabilities each broker advertises in a MQTT 5 CONNACK, compared as they are.",
    capabilities=True,
    steps=[connect("a", "c-caps", level=5)],
)

scenario(
    "connect/authentication-method-v5",
    doc="CONNECT with an Authentication Method neither broker supports.",
    steps=[open_("a", 5),
           send("a", wire.connect("c-auth", level=5, props={"authentication_method": "SCRAM-SHA-1"})),
           until_close("a")],
)

del level, v
