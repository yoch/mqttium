"""Version-bound codec primitive table.

Constructed once from ``MQTTProtocolVersion`` so sessions and the engine never
branch on protocol per packet. Internal only — not part of ``packets.__all__``.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from functools import cache

from mqttium.enums import MQTTProtocolVersion
from mqttium.errors import ProtocolError
from mqttium.packets import _ack as ack
from mqttium.packets import _connack as connack
from mqttium.packets import _control as control
from mqttium.packets import _publish as publish
from mqttium.packets import _suback as suback
from mqttium.packets import _subscribe as subscribe
from mqttium.transport.writes import WriteItem
from mqttium.types import Properties


def _auth_requires_v5(_remaining: bytes) -> tuple[int, Properties | None]:
    raise ProtocolError("AUTH requires MQTT 5")


@dataclass(frozen=True, slots=True)
class CodecBindings:
    """One closed set of specialized encode/decode callables for a protocol."""

    protocol: MQTTProtocolVersion
    is_mqtt5: bool

    decode_puback: Callable[[bytes], tuple[int, int, Properties | None]]
    decode_pubrec: Callable[[bytes], tuple[int, int, Properties | None]]
    decode_pubrel: Callable[[bytes], tuple[int, int, Properties | None]]
    decode_pubcomp: Callable[[bytes], tuple[int, int, Properties | None]]
    encode_pubrel: Callable[..., bytes]

    decode_connack: Callable[[bytes], tuple[bool, int, Properties | None]]
    decode_suback: Callable[[bytes], tuple[int, tuple[int, ...], Properties | None]]
    decode_unsuback: Callable[[bytes], tuple[int, tuple[int, ...], Properties | None]]
    decode_disconnect: Callable[[bytes], tuple[int, Properties | None]]
    decode_auth: Callable[[bytes], tuple[int, Properties | None]]

    # PINGREQ carries no data and is byte-identical in every protocol
    # version, so the binding holds the frame rather than a callable. A
    # client never encodes PINGRESP, so there is no binding for it.
    pingreq_frame: bytes
    encode_disconnect: Callable[..., bytes]

    encode_publish_item: Callable[..., WriteItem]

    encode_subscribe: Callable[..., bytes]
    encode_unsubscribe: Callable[..., bytes]


@cache
def bind_codec(protocol: MQTTProtocolVersion) -> CodecBindings:
    """Bind every specialized primitive for *protocol* exactly once."""
    if protocol is MQTTProtocolVersion.MQTTv5:
        return CodecBindings(
            protocol=protocol,
            is_mqtt5=True,
            decode_puback=ack.decode_puback_v5,
            decode_pubrec=ack.decode_pubrec_v5,
            decode_pubrel=ack.decode_pubrel_v5,
            decode_pubcomp=ack.decode_pubcomp_v5,
            encode_pubrel=ack.encode_pubrel_v5,
            decode_connack=connack.decode_connack_v5,
            decode_suback=suback.decode_suback_v5,
            decode_unsuback=suback.decode_unsuback_v5,
            decode_disconnect=control.decode_disconnect_v5,
            decode_auth=control.decode_auth_v5,
            pingreq_frame=control.encode_pingreq(),
            encode_disconnect=control.encode_disconnect_v5,
            encode_publish_item=publish.encode_publish_item_v5,
            encode_subscribe=subscribe.encode_subscribe_v5,
            encode_unsubscribe=subscribe.encode_unsubscribe_v5,
        )
    # MQTT 3.1.1 and the MQTT 3.1 leftover share the 3.1.1 acknowledgement and
    # control shapes; PUBLISH ingress for 3.1 still uses PublishPacket.decode.
    return CodecBindings(
        protocol=protocol,
        is_mqtt5=False,
        decode_puback=ack.decode_puback_v311,
        decode_pubrec=ack.decode_pubrec_v311,
        decode_pubrel=ack.decode_pubrel_v311,
        decode_pubcomp=ack.decode_pubcomp_v311,
        encode_pubrel=ack.encode_pubrel_v311,
        decode_connack=connack.decode_connack_v311,
        decode_suback=suback.decode_suback_v311,
        decode_unsuback=suback.decode_unsuback_v311,
        decode_disconnect=control.decode_disconnect_v311,
        decode_auth=_auth_requires_v5,
        pingreq_frame=control.encode_pingreq(),
        encode_disconnect=control.encode_disconnect_v311,
        encode_publish_item=publish.encode_publish_item_v311,
        encode_subscribe=subscribe.encode_subscribe_v311,
        encode_unsubscribe=subscribe.encode_unsubscribe_v311,
    )
