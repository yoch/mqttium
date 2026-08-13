"""Version-specialized MQTT SUBSCRIBE and UNSUBSCRIBE encode primitives.

Functions remain deliberately specialized by protocol version; grouping them
by packet family keeps navigation simple without adding hot-path dispatch.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from mqttium.codec.primitives import pack_u16, pack_utf8
from mqttium.codec.properties import SUBSCRIBE, UNSUBSCRIBE
from mqttium.enums import MQTTProtocolVersion, PacketType
from mqttium.errors import ProtocolError
from mqttium.packets._common import _require_outbound_mid, encode_frame
from mqttium.types import Properties
from mqttium.codec.properties import encode_properties


if TYPE_CHECKING:
    from mqttium.packets.subscription import Subscription


def encode_subscribe_v311(
    mid: int,
    subscriptions: tuple[Subscription, ...],
    properties: Properties | None = None,
) -> bytes:
    if not subscriptions:
        raise ProtocolError("SUBSCRIBE requires at least one filter")
    _require_outbound_mid(mid, SUBSCRIBE)
    body = bytearray(pack_u16(mid))
    for subscription in subscriptions:
        body.extend(pack_utf8(subscription.topic))
        body.append(subscription.options.encode_byte(MQTTProtocolVersion.MQTTv311))
    return encode_frame(PacketType.SUBSCRIBE, 0x02, body)


def encode_unsubscribe_v311(
    mid: int,
    topics: tuple[str, ...],
    properties: Properties | None = None,
) -> bytes:
    if not topics:
        raise ProtocolError("UNSUBSCRIBE requires at least one filter")
    _require_outbound_mid(mid, UNSUBSCRIBE)
    body = bytearray(pack_u16(mid))
    for topic in topics:
        body.extend(pack_utf8(topic))
    return encode_frame(PacketType.UNSUBSCRIBE, 0x02, body)


if TYPE_CHECKING:
    from mqttium.packets.subscription import Subscription


def encode_subscribe_v5(
    mid: int,
    subscriptions: tuple[Subscription, ...],
    properties: Properties | None = None,
) -> bytes:
    if not subscriptions:
        raise ProtocolError("SUBSCRIBE requires at least one filter")
    _require_outbound_mid(mid, SUBSCRIBE)
    body = bytearray(pack_u16(mid))
    body.extend(encode_properties(properties, SUBSCRIBE))
    for subscription in subscriptions:
        body.extend(pack_utf8(subscription.topic))
        body.append(subscription.options.encode_byte(MQTTProtocolVersion.MQTTv5))
    return encode_frame(PacketType.SUBSCRIBE, 0x02, body)


def encode_unsubscribe_v5(
    mid: int,
    topics: tuple[str, ...],
    properties: Properties | None = None,
) -> bytes:
    if not topics:
        raise ProtocolError("UNSUBSCRIBE requires at least one filter")
    _require_outbound_mid(mid, UNSUBSCRIBE)
    body = bytearray(pack_u16(mid))
    body.extend(encode_properties(properties, UNSUBSCRIBE))
    for topic in topics:
        body.extend(pack_utf8(topic))
    return encode_frame(PacketType.UNSUBSCRIBE, 0x02, body)
