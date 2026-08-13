"""Regressions for protocol/codec findings from the RC2 audit."""

from __future__ import annotations

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.vbi import MAX_VBI
from mqttium.enums import MQTTProtocolVersion, QoS
from mqttium.errors import MalformedPacketError, PacketTooLargeError, ProtocolError
from mqttium.packets import ConnectPacket, PubAckPacket, PublishPacket
from mqttium.packets.publish import encode_publish_item
from mqttium.protocol.outbound import OutboundSession
from mqttium.types import Properties

V311 = MQTTProtocolVersion.MQTTv311
V5 = MQTTProtocolVersion.MQTTv5


def _decode_publish_wire(wire: bytes, protocol: MQTTProtocolVersion) -> PublishPacket:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    [raw] = decoder.drain_packets()
    return PublishPacket.decode(raw.flags, raw.remaining, protocol)


def test_ufeff_round_trips_in_publish_topic_and_user_property() -> None:
    properties = Properties()
    properties.add_user_property("bom", "before\ufeffafter")
    packet = PublishPacket(
        topic="legal/\ufeff/topic",
        payload=b"payload",
        qos=QoS.AT_MOST_ONCE,
        retain=False,
        dup=False,
        properties=properties,
    )

    decoded = _decode_publish_wire(packet.encode(V5), V5)

    assert decoded.topic == packet.topic
    assert decoded.properties is not None
    assert decoded.properties.get("user_property") == [("bom", "before\ufeffafter")]


def test_v311_inbound_publish_rejects_empty_topic() -> None:
    with pytest.raises(MalformedPacketError, match="must not be empty"):
        PublishPacket.decode(0, b"\x00\x00payload", V311)


def test_v311_inbound_publish_rejects_wildcard_topic_at_packet_boundary() -> None:
    remaining = b"\x00\x05a/+/bpayload"
    with pytest.raises(MalformedPacketError, match="wildcards"):
        PublishPacket.decode(0, remaining, V311)


class _HugePayload:
    def __len__(self) -> int:
        return MAX_VBI


def test_oversized_publish_encoder_raises_typed_mqtt_error_before_materializing_payload() -> None:
    with pytest.raises(PacketTooLargeError, match="wire maximum"):
        encode_publish_item(
            "t",
            _HugePayload(),  # type: ignore[arg-type]
            qos=QoS.AT_MOST_ONCE,
            retain=False,
            dup=False,
            mid=None,
            properties=None,
            protocol=V311,
        )


def test_outbound_publish_size_precheck_uses_same_typed_wire_limit() -> None:
    with pytest.raises(PacketTooLargeError, match="wire maximum"):
        OutboundSession._publish_wire_size_from_parts(
            topic_bytes=1,
            wire_property_bytes=0,
            payload_size=MAX_VBI,
            qos=QoS.AT_MOST_ONCE,
        )


def test_connect_rejects_semantic_will_fields_without_will_topic() -> None:
    with pytest.raises(ProtocolError, match="Will payload/properties require a Will topic"):
        ConnectPacket(client_id="c", will_payload=b"not-empty").encode()

    properties = Properties()
    properties.set("will_delay_interval", 1)
    with pytest.raises(ProtocolError, match="Will payload/properties require a Will topic"):
        ConnectPacket(
            client_id="c",
            protocol=V5,
            will_properties=properties,
        ).encode()


def test_reason_only_v5_puback_normalizes_absent_properties() -> None:
    shortest = PubAckPacket.decode(b"\x00\x01", V5)
    explicit_reason = PubAckPacket.decode(b"\x00\x01\x00", V5)

    assert shortest == explicit_reason
    assert shortest.properties is None
    assert explicit_reason.properties is None
