"""Retained messages: set, replace, clear, delivery to new subscribers, MQTT 5 retain options."""
from mqttwire import sub_options
from dsl import (ack, collect, connect, drain, mark, measure, publish_ack, publish_qos2, publish_sync, quiet, recv,
                 scenario, subscribe)
from expectations import at, expectation, in_any_order


def store(conn, topic, payload, level, packet_id=1):
    """Publish a retained message and wait until the broker has taken it.

    MQTT 3.1.1 uses QoS 1 and its PUBACK. MQTT 5 uses QoS 0 and a PINGREQ
    round trip: a MQTT 5 PUBACK says "no matching subscribers" or not with
    the subscriptions of the moment, which `publish/no-matching-subscribers-v5`
    compares and these do not.
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
        subscribe("sub", 1, [("replace/t", 0)], expect=0), collect("sub", sort=True, suback=1),
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
        subscribe("sub", 1, [("clear/t", 0)], expect=0), collect("sub", sort=True, suback=1),
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
        subscribe("sub", 1, [("tree/#", 0)], expect=0), collect("sub", sort=True, suback=1),
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

def many(prefix, count):
    """`{topic: payload}` of `count` retained messages under `prefix`."""
    return {f"{prefix}/{index:02d}": f"r{index:02d}" for index in range(1, count + 1)}


def keep(conn, topics):
    """Publish one retained message per topic at QoS 1, each after the
    PUBACK of the one before."""
    return [publish_ack(conn, topic, payload, retain=True, packet_id=index)
            for index, (topic, payload) in enumerate(topics.items(), 1)]


def stored(topics):
    """What a new subscription is owed of a store that holds `topics`."""
    deliveries = [{"type": "PUBLISH", "topic": topic, "payload": payload, "retain": True}
                  for topic, payload in topics.items()]
    return expectation(
        "store", statement="MQTT-3.3.1-6, with MQTT-3.3.1-5 for the store and MQTT-3.3.1-8 for the RETAIN flag",
        requires=f"The SUBACK and the {len(topics)} retained messages, one for each topic, with its payload and "
                 "RETAIN 1, each once and in any order; then nothing",
        then=[at("retained", {"got": in_any_order({"type": "SUBACK", "packet_id": 1}, *deliveries), "end": "ok"}),
              at("after", {"got": [], "end": "quiet"})])


# The two questions a subscription to many retained topics asks are kept
# apart. What the store holds is read by a subscriber at QoS 1 that
# acknowledges every delivery: nothing may be shed for it, so every
# message is owed whatever the timing. How many QoS 0 deliveries reach a
# subscriber is something else: a broker may shed them, the number depends
# on timing, and it is measured, not compared.
for count, word, doc in (
    (32, "thirty-two", "thirty-two is what the bounded profile of mqttiumd keeps"),
    (40, "forty", "forty is more than the bounded profile of mqttiumd keeps"),
):
    topics = many(word, count)
    scenario(
        f"retained/{word}-topics-qos1-v4",
        doc=f"""The content of the retained store: {word} topics each keep a retained message
        ({doc}), and a `{word}/#` subscription made afterwards at QoS 1 acknowledges every
        delivery. Each message is owed once, with RETAIN 1, in any order, and nothing after.""",
        steps=[
            connect("pub", f"r-{word}1-pub"), keep("pub", topics),
            connect("sub", f"r-{word}1-sub"),
            subscribe("sub", 1, [(f"{word}/#", 1)], expect=0),
            drain("sub", count, sort=True), mark("retained"), quiet("sub"), mark("after"),
        ],
        expectations=[stored(topics)],
    )

scenario(
    "retained/forty-topics-v4",
    doc="""QoS 0 deliveries of retained messages: forty topics each keep a retained message and
    a `forty/#` subscription is made afterwards at QoS 0. A broker may shed QoS 0 deliveries
    for a subscriber it cannot write to fast enough: each message that comes is one of the
    forty and comes once; how many come is measured, not compared.""",
    steps=[
        connect("pub", "r-forty-pub"), keep("pub", many("forty", 40)),
        connect("sub", "r-forty-sub"),
        subscribe("sub", 1, [("forty/#", 0)], expect=0),
        measure("sub", many("forty", 40).values(), ordered=False, window=1.0),
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
            collect("sub", sort=True, suback=1),
            subscribe("sub", 2, [("rh/t", sub_options(0, rh=handling))], level=5, expect=0),
            collect("sub", sort=True, suback=2),
        ],
    )

del level, v, handling, doc, count, word, topics
