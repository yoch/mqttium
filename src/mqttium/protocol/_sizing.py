"""One definition of a publication's logical size.

Both directional sessions bound their pending-byte budgets by the same
quantity: payload + UTF-8 topic + MQTT 5 property table. The two had drifted
into separate copies that differed only in whether a trusted decode-time table
size was available, which is a difference in the caller, not in the formula.

A module-level function rather than a shared mixin method: it is called per
admitted publish and per inbound QoS 1/2 message, and a global lookup is
marginally cheaper than a bound-method call.
"""

from __future__ import annotations

from mqttium.codec.properties import PUBLISH, encode_properties
from mqttium.types import Properties


def publish_logical_size(
    is_v5: bool,
    topic: str,
    payload_size: int,
    properties: Properties | None,
    property_wire_size: int | None = None,
) -> int:
    """Bytes a publication counts against a pending-byte budget.

    `property_wire_size` is the exact table size observed while decoding a fresh
    MQTT 5 PUBLISH. When it is absent the table is re-encoded to measure it; the
    ASCII fast path avoids encoding the topic a second time either way.
    """
    property_bytes = 0
    if is_v5 and properties is not None and properties.values:
        if property_wire_size is None:
            property_bytes = len(encode_properties(properties, PUBLISH))
        else:
            property_bytes = property_wire_size
    topic_bytes = len(topic) if topic.isascii() else len(topic.encode("utf-8"))
    return payload_size + topic_bytes + property_bytes
