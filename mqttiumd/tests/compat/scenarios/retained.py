"""Retained messages: set, replace, clear, delivery to new subscribers, MQTT 5 retain options."""
from mqttwire import sub_options
from dsl import (ack, collect, connect, publish_ack, publish_qos2, publish_sync, quiet, recv, scenario, subscribe)


def store(conn, topic, payload, level, packet_id=1):
    """Publish a retained message and wait until the broker has taken it.

    MQTT 3.1.1 uses QoS 1 and its PUBACK. MQTT 5 uses QoS 0 and a PINGREQ
    round trip: a MQTT 5 PUBACK may say "no matching subscribers", which
    `publish/no-matching-subscribers-v5` compares and these do not.
    """
    if level == 4:
        return publish_ack(conn, topic, payload, retain=True, packet_id=packet_id, level=4)
    return publish_sync(conn, topic, payload, retain=True, level=5)


for level in (4, 5):
    v = f"v{level}"

    scenario(
        f"retained/set-and-deliver-{v}",
        doc="A retained message is sent, with RETAIN 1, to a subscription made afterwards.",
        steps=[
            connect("pub", "r-set-pub", level=level), store("pub", "set/t", "kept", level),
            connect("sub", "r-set-sub", level=level),
            subscribe("sub", 1, [("set/t", 0)], level=level, expect=2, sort=True), quiet("sub"),
        ],
    )



scenario(
    "retained/replace-v4",
    doc="A second retained message on a topic replaces the first: a new subscriber receives only the second.",
    steps=[
        connect("pub", "r-replace-pub"),
        store("pub", "replace/t", "one", 4), store("pub", "replace/t", "two", 4, packet_id=2),
        connect("sub", "r-replace-sub"),
        subscribe("sub", 1, [("replace/t", 0)], expect=0), collect("sub", sort=True),
    ],
)

scenario(
    "retained/clear-v4",
    doc="""A retained message with a zero-length payload removes the retained message
    of its topic: a new subscriber receives nothing.""",
    steps=[
        connect("pub", "r-clear-pub"),
        store("pub", "clear/t", "kept", 4), store("pub", "clear/t", b"", 4, packet_id=2),
        connect("sub", "r-clear-sub"),
        subscribe("sub", 1, [("clear/t", 0)], expect=0), collect("sub", sort=True),
    ],
)

scenario(
    "retained/live-delivery-clears-retain-flag-v4",
    doc="A message published with RETAIN 1 reaches an already established subscription with RETAIN 0.",
    steps=[
        connect("sub", "r-live-sub"), subscribe("sub", 1, [("live/t", 0)]),
        connect("pub", "r-live-pub"), publish_ack("pub", "live/t", "now", retain=True),
        recv("sub"), quiet("sub"),
    ],
)

scenario(
    "retained/wildcard-subscription-v4",
    doc="`tree/#` receives the retained messages of the parent and of every level below, and no other.",
    steps=[
        connect("pub", "r-tree-pub"),
        publish_ack("pub", "tree", "root", retain=True, packet_id=1),
        publish_ack("pub", "tree/a", "a", retain=True, packet_id=2),
        publish_ack("pub", "tree/c/d", "cd", retain=True, packet_id=3),
        publish_ack("pub", "other/a", "other", retain=True, packet_id=4),
        connect("sub", "r-tree-sub"),
        subscribe("sub", 1, [("tree/#", 0)], expect=0), collect("sub", sort=True),
    ],
)

scenario(
    "retained/qos-downgrade-v4",
    doc="A message retained at QoS 2 is sent to a subscription granted QoS 1 at QoS 1, with RETAIN 1.",
    steps=[
        connect("pub", "r-down-pub"), publish_qos2("pub", "rdown/t", "kept", retain=True),
        connect("sub", "r-down-sub"),
        subscribe("sub", 1, [("rdown/t", 1)], expect=2, sort=True), ack("sub"), quiet("sub"),
    ],
)

scenario(
    "retained/forty-topics-v4",
    doc="Forty topics each keep a retained message: a `forty/#` subscription made afterwards receives all forty.",
    steps=[
        connect("pub", "r-forty-pub"),
        [publish_ack("pub", f"forty/{index:02d}", f"r{index:02d}", retain=True, packet_id=index)
         for index in range(1, 41)],
        connect("sub", "r-forty-sub"),
        subscribe("sub", 1, [("forty/#", 0)], expect=0), collect("sub", sort=True),
    ],
)

scenario(
    "retained/retain-as-published-v5",
    doc="""Retain As Published 1: a message published with RETAIN 1 keeps the flag on
    live delivery, and one published with RETAIN 0 does not get it.""",
    steps=[
        connect("sub", "r-rap-sub", level=5),
        subscribe("sub", 1, [("rap/t", sub_options(0, rap=True))], level=5),
        connect("pub", "r-rap-pub", level=5),
        publish_ack("pub", "rap/t", "flagged", retain=True, level=5),
        publish_ack("pub", "rap/t", "plain", packet_id=2, level=5),
        recv("sub", 2), quiet("sub"),
    ],
)

for handling, doc in (
    (0, "Retain Handling 0: the retained message is sent at each SUBSCRIBE."),
    (1, "Retain Handling 1: the retained message is sent only when the subscription is new."),
    (2, "Retain Handling 2: the retained message is never sent at SUBSCRIBE."),
):
    scenario(
        f"retained/retain-handling-{handling}-v5",
        doc=doc,
        steps=[
            connect("pub", "r-rh-pub", level=5), store("pub", "rh/t", "kept", 5),
            connect("sub", "r-rh-sub", level=5),
            subscribe("sub", 1, [("rh/t", sub_options(0, rh=handling))], level=5, expect=0),
            collect("sub", sort=True),
            subscribe("sub", 2, [("rh/t", sub_options(0, rh=handling))], level=5, expect=0),
            collect("sub", sort=True),
        ],
    )

del level, v, handling, doc
