"""MQTT 5 PUBLISH properties: forwarding, Message Expiry, Topic Alias, Maximum Packet Size."""
from dsl import (ack, collect, connect, disconnect, persistent, publish, publish_ack, publish_sync, quiet, recv,
                 scenario, sleep, subscribe)



def forwarded(name, props, doc, payload="payload"):
    """One QoS 1 PUBLISH with `props` and what a QoS 1 subscriber receives."""
    scenario(
        f"properties/{name}-v5",
        doc=doc,
        steps=[
            connect("sub", f"x-{name}-sub", level=5), subscribe("sub", 1, [(f"{name}/t", 1)], level=5),
            connect("pub", f"x-{name}-pub", level=5),
            publish_ack("pub", f"{name}/t", payload, level=5, props=props),
            recv("sub"), ack("sub"), quiet("sub"),
        ],
    )


forwarded("user-properties",
          [("user_property", ("a", "1")), ("user_property", ("b", "2")), ("user_property", ("a", "3"))],
          "User Properties of a PUBLISH are forwarded to the subscriber, repeated names included, in order.")
forwarded("payload-format-indicator", [("payload_format_indicator", 1)],
          "Payload Format Indicator 1 on a UTF-8 payload is forwarded unaltered to the subscriber.")
forwarded("content-type", [("content_type", "application/json")],
          "The Content Type of a PUBLISH is forwarded unaltered to the subscriber.", payload='{"q":1}')
forwarded("response-topic", [("response_topic", "rr/reply"), ("correlation_data", b"\x00\x01corr")],
          "Response Topic and Correlation Data of a PUBLISH are forwarded unaltered to the subscriber.")

scenario(
    "properties/message-expiry-forwarded-v5",
    doc="""A PUBLISH with Message Expiry Interval 600 is forwarded at once with the
    property set to the remaining lifetime (compared as the interval 591..600).""",
    buckets={"message_expiry_interval": [0, 590, 600]},
    steps=[
        connect("sub", "x-exp-sub", level=5), subscribe("sub", 1, [("exp/t", 1)], level=5),
        connect("pub", "x-exp-pub", level=5),
        publish_ack("pub", "exp/t", "fresh", level=5, props={"message_expiry_interval": 600}),
        recv("sub"), ack("sub"), quiet("sub"),
    ],
)

scenario(
    "properties/message-expiry-elapsed-queued-v5",
    doc="""A QoS 1 message with Message Expiry Interval 1 queued for an offline session
    is not delivered when the session reconnects 2.5 s later; one without expiry is.""",
    steps=[
        connect("sub", "x-expq-sub", level=5, **persistent(5)), subscribe("sub", 1, [("expq/t", 1)], level=5),
        disconnect("sub", level=5),
        connect("pub", "x-expq-pub", level=5),
        publish_ack("pub", "expq/t", "short-lived", level=5, props={"message_expiry_interval": 1}),
        publish_ack("pub", "expq/t", "kept", packet_id=2, level=5),
        sleep(2.5),
        connect("sub", "x-expq-sub", level=5, **persistent(5)), collect("sub"),
    ],
)

scenario(
    "properties/message-expiry-elapsed-retained-v5",
    doc="A retained message with Message Expiry Interval 1 is not sent to a subscription made 2.5 s later.",
    steps=[
        connect("pub", "x-expr-pub", level=5),
        publish_sync("pub", "expr/t", "short-lived", retain=True, level=5, props={"message_expiry_interval": 1}),
        sleep(2.5),
        connect("sub", "x-expr-sub", level=5),
        subscribe("sub", 1, [("expr/t", 0)], level=5, expect=0), collect("sub", sort=True, suback=1),
    ],
)

scenario(
    "properties/topic-alias-v5",
    doc="""The client sets Topic Alias 1 on a PUBLISH and then publishes with the alias
    and a zero-length Topic Name: the subscriber receives both under the full topic.""",
    steps=[
        connect("sub", "x-alias-sub", level=5), subscribe("sub", 1, [("alias/t", 0)], level=5),
        connect("pub", "x-alias-pub", level=5),
        publish("pub", "alias/t", "first", level=5, props={"topic_alias": 1}),
        publish_sync("pub", "", "second", level=5, props={"topic_alias": 1}),
        collect("sub"), quiet("pub"),
    ],
)

scenario(
    "properties/client-maximum-packet-size-v5",
    doc="""The subscriber announces Maximum Packet Size 40: a PUBLISH that would exceed
    it is not sent to it, and a smaller one is.""",
    steps=[
        connect("sub", "x-mps-sub", level=5, props={"maximum_packet_size": 40}),
        subscribe("sub", 1, [("mps/t", 0)], level=5),
        connect("pub", "x-mps-pub", level=5),
        publish("pub", "mps/t", "x" * 100, level=5), publish_sync("pub", "mps/t", "small", level=5),
        collect("sub"),
    ],
)
