"""SUBSCRIBE, SUBACK, UNSUBSCRIBE: grants, wildcards, `$` topics, invalid filters, MQTT 5 options."""
import mqttwire as wire
from mqttwire import sub_options
from dsl import (collect, connect, publish, publish_ack, publish_qos2, publish_sync, quiet, recv, scenario, send,
                 subscribe, until_close)


def matching(name, topic_filter, topics, doc):
    """One QoS 0 subscription, a list of QoS 0 publications, and what arrives."""
    scenario(
        f"subscribe/{name}-v4",
        doc=doc,
        steps=[
            connect("sub", f"s-{name}-sub"),
            subscribe("sub", 1, [(topic_filter, 0)]),
            connect("pub", f"s-{name}-pub"),
            [publish("pub", topic, f"on {topic}") for topic in topics[:-1]],
            publish_sync("pub", topics[-1], f"on {topics[-1]}"),
            collect("sub"),
        ],
    )


matching("plus-wildcard", "sport/+", ["sport", "sport/", "sport/tennis", "sport/tennis/player1", "sports/x"],
         "`sport/+` matches exactly one level, the empty one included, and not the parent.")
matching("hash-wildcard", "sport/#", ["sport", "sport/", "sport/tennis/player1", "sports", "other/sport"],
         "`sport/#` matches the parent level and every level below it.")

for level in (4, 5):
    v = f"v{level}"

    scenario(
        f"subscribe/qos-grants-{v}",
        doc="A SUBSCRIBE with three filters at QoS 0, 1 and 2: one granted QoS per filter, in order.",
        steps=[connect("a", "s-grants", level=level),
               subscribe("a", 0x1234, [("grants/0", 0), ("grants/1", 1), ("grants/2", 2)], level=level)],
    )


    scenario(
        f"subscribe/invalid-hash-not-last-{v}",
        doc="SUBSCRIBE with the invalid Topic Filter `bad/#/x`: the connection is closed.",
        steps=[connect("a", "s-hash", level=level), send("a", wire.subscribe(1, [("bad/#/x", 0)], level=level)),
               until_close("a")],
    )

    scenario(
        f"subscribe/overlapping-subscriptions-{v}",
        doc="""Two subscriptions of one client, granted QoS 0 and QoS 1, match one QoS 2
        message: how many copies are sent, and at which QoS.""",
        steps=[
            connect("sub", "s-overlap-sub", level=level),
            subscribe("sub", 1, [("ov/#", 0)], level=level), subscribe("sub", 2, [("ov/+", 1)], level=level),
            connect("pub", "s-overlap-pub", level=level), publish_qos2("pub", "ov/x", "once?", level=level),
            collect("sub", sort=True),
        ],
    )

scenario(
    "subscribe/dollar-topics-vs-wildcards-v4",
    doc="""Subscriptions to `#` and to `+/broker/version` receive a plain topic and
    nothing that begins with `$`: neither the reference's own `$SYS` tree nor a
    `$SYS/...` message a client publishes.""",
    steps=[
        connect("all", "s-dollar-all"), subscribe("all", 1, [("#", 0)]),
        connect("plus", "s-dollar-plus"),
        subscribe("plus", 1, [("+/broker/version", 0)]),
        connect("pub", "s-dollar-pub"),
        publish_sync("pub", "$SYS/broker/version", "from a client"),
        publish_sync("pub", "plain/broker/version", "plain"),
        collect("all"), collect("plus"), quiet("pub"),
    ],
)

scenario(
    "subscribe/sys-explicit-filter-v4",
    doc="""A subscription that names `$SYS/broker/version`: the reference keeps a retained
    version string there.""",
    steps=[connect("a", "s-sys"), subscribe("a", 1, [("$SYS/broker/version", 0)], expect=0),
           collect("a", sort=True)],
)

scenario(
    "subscribe/invalid-empty-filter-v5",
    doc="SUBSCRIBE with a zero-length Topic Filter: the connection is closed.",
    steps=[connect("a", "s-empty", level=5), send("a", wire.subscribe(1, [("", 0)], level=5)), until_close("a")],
)

scenario(
    "subscribe/twelve-filters-v4",
    doc="One SUBSCRIBE packet with twelve Topic Filters is answered by a SUBACK with twelve granted QoS.",
    steps=[connect("a", "s-twelve"), subscribe("a", 1, [(f"twelve/{index}", 0) for index in range(12)]), quiet("a")],
)

scenario(
    "subscribe/many-subscriptions-v4",
    doc="One client makes 150 subscriptions, five Topic Filters per SUBSCRIBE packet: all are granted.",
    steps=[connect("a", "s-many"),
           [subscribe("a", batch + 1, [(f"many/{batch}/{index}", 0) for index in range(5)]) for batch in range(30)],
           quiet("a")],
)

scenario(
    "subscribe/unsubscribe-v4",
    doc="""UNSUBSCRIBE removes exactly the filter named, compared character by character:
    `unsub/#` goes, `unsub/a` stays; an unknown filter is acknowledged too.""",
    steps=[
        connect("sub", "s-unsub-sub"), subscribe("sub", 1, [("unsub/a", 0), ("unsub/#", 0)]),
        send("sub", wire.unsubscribe(2, ["unsub/#"])), recv("sub"),
        send("sub", wire.unsubscribe(3, ["never/subscribed"])), recv("sub"),
        connect("pub", "s-unsub-pub"), publish("pub", "unsub/b", "gone"), publish_sync("pub", "unsub/a", "kept"),
        collect("sub"),
    ],
)

scenario(
    "subscribe/unsubscribe-v5",
    doc="""An UNSUBSCRIBE with an existing and an unknown filter: one Reason Code each, in
    order, then no more delivery.""",
    steps=[
        connect("sub", "s-unsub5-sub", level=5), subscribe("sub", 1, [("unsub5/known", 0)], level=5),
        send("sub", wire.unsubscribe(2, ["unsub5/known", "unsub5/unknown"], level=5)), recv("sub"),
        connect("pub", "s-unsub5-pub", level=5), publish_sync("pub", "unsub5/known", "late", level=5),
        quiet("sub"),
    ],
)

scenario(
    "subscribe/no-local-v5",
    doc="No Local: a client does not receive its own publication and receives another client's.",
    steps=[
        connect("a", "s-nolocal-a", level=5),
        subscribe("a", 1, [("nolocal/t", sub_options(0, no_local=True))], level=5),
        publish("a", "nolocal/t", "mine", level=5), quiet("a"),
        connect("b", "s-nolocal-b", level=5), publish_sync("b", "nolocal/t", "theirs", level=5),
        collect("a"),
    ],
)

scenario(
    "subscribe/shared-subscription-v5",
    doc="""A shared subscription `$share/group/shared/t` with one member receives the
    messages of `shared/t`, as does a plain subscription of another client.""",
    steps=[
        connect("m", "s-share-m", level=5), subscribe("m", 1, [("$share/group/shared/t", 1)], level=5),
        connect("p", "s-share-p", level=5), subscribe("p", 1, [("shared/t", 0)], level=5),
        connect("pub", "s-share-pub", level=5), publish_ack("pub", "shared/t", "for-the-group", level=5),
        collect("m"), collect("p"),
    ],
)

scenario(
    "subscribe/subscription-identifier-v5",
    doc="A Subscription Identifier given at SUBSCRIBE comes back in the matching PUBLISH.",
    steps=[
        connect("sub", "s-subid-sub", level=5),
        subscribe("sub", 1, [("subid/t", 0)], level=5, props={"subscription_identifier": 7}),
        connect("pub", "s-subid-pub", level=5), publish_sync("pub", "subid/t", "tagged", level=5),
        collect("sub"),
    ],
)

scenario(
    "subscribe/user-property-v5",
    doc="A SUBSCRIBE carrying a User Property is accepted like one without.",
    steps=[connect("a", "s-userprop", level=5),
           send("a", wire.subscribe(1, [("userprop/t", 0)], level=5, props=[("user_property", ("k", "v"))])),
           recv("a"), quiet("a")],
)

del level, v
