"""Regression tests for strict MQTT packet parsing and encoding."""

from __future__ import annotations

import pytest

from mqttium.enums import MQTTProtocolVersion, QoS
from mqttium.errors import MalformedPacketError, ProtocolError
from mqttium.packets import (
    AuthPacket,
    ConnAckPacket,
    ConnectPacket,
    DisconnectPacket,
    PubAckPacket,
    PublishPacket,
    SubAckPacket,
    SubscribeOptions,
    SubscribePacket,
    Subscription,
    UnsubAckPacket,
)

V311 = MQTTProtocolVersion.MQTTv311
V5 = MQTTProtocolVersion.MQTTv5


def test_connack_rejects_trailing_bytes_and_reserved_flags() -> None:
    with pytest.raises(MalformedPacketError, match="trailing"):
        ConnAckPacket.decode(b"\x00\x00\x00", V311)
    with pytest.raises(MalformedPacketError, match="acknowledge flags"):
        ConnAckPacket.decode(b"\x02\x00", V311)


def test_connack_rejects_session_present_on_failure() -> None:
    with pytest.raises(MalformedPacketError, match="Session Present"):
        ConnAckPacket.decode(b"\x01\x05", V311)


def test_connack_v5_requires_exact_properties_body() -> None:
    assert ConnAckPacket.decode(b"\x00\x00\x00", V5).reason_code == 0
    with pytest.raises(MalformedPacketError, match="trailing"):
        ConnAckPacket.decode(b"\x00\x00\x00\x00", V5)


def test_puback_rejects_trailing_bytes_in_mqtt311() -> None:
    with pytest.raises(MalformedPacketError, match="trailing"):
        PubAckPacket.decode(b"\x00\x01\x00", V311)


def test_puback_rejects_invalid_v5_reason_code() -> None:
    with pytest.raises(MalformedPacketError, match="reason code"):
        PubAckPacket.decode(b"\x00\x01\x7f", V5)


def test_suback_requires_nonzero_mid_and_valid_reason_codes() -> None:
    with pytest.raises(MalformedPacketError, match="must not be 0"):
        SubAckPacket.decode(b"\x00\x00\x00", V311)
    with pytest.raises(MalformedPacketError, match="reason code"):
        SubAckPacket.decode(b"\x00\x01\x7f", V311)
    with pytest.raises(MalformedPacketError, match="missing reason"):
        SubAckPacket.decode(b"\x00\x01\x00", V5)


def test_unsuback_v5_requires_reason_payload() -> None:
    with pytest.raises(MalformedPacketError, match="missing reason"):
        UnsubAckPacket.decode(b"\x00\x01\x00", V5)


def test_disconnect_mqtt311_rejects_body() -> None:
    with pytest.raises(MalformedPacketError, match="trailing"):
        DisconnectPacket.decode(b"\x00", V311)


def test_auth_rejects_undefined_reason_code() -> None:
    with pytest.raises(MalformedPacketError, match="reason code"):
        AuthPacket.decode(b"\x7f", V5)


def test_qos0_publish_rejects_dup_on_encode_and_decode() -> None:
    with pytest.raises(ProtocolError, match="DUP"):
        PublishPacket(
            topic="t",
            payload=b"x",
            qos=QoS.AT_MOST_ONCE,
            retain=False,
            dup=True,
        ).encode(V311)
    with pytest.raises(MalformedPacketError, match="DUP"):
        PublishPacket.decode(0x08, b"\x00\x01t", V311)


def test_outbound_packet_identifiers_are_validated() -> None:
    with pytest.raises(ProtocolError, match="1..65535"):
        PubAckPacket(mid=0).encode(V311)
    with pytest.raises(ProtocolError, match="1..65535"):
        SubscribePacket(
            mid=0,
            subscriptions=(Subscription("t/#"),),
        ).encode(V311)


def test_subscribe_options_reject_qos3() -> None:
    with pytest.raises(ProtocolError, match="QoS"):
        SubscribeOptions(qos=3).encode_byte(V5)  # type: ignore[arg-type]


def test_connect_validates_keepalive_and_will_fields() -> None:
    with pytest.raises(ProtocolError, match="keepalive"):
        ConnectPacket(client_id="x", keepalive=65536).encode()
    with pytest.raises(ProtocolError, match="Will QoS"):
        ConnectPacket(client_id="x", will_qos=QoS.AT_LEAST_ONCE).encode()
