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
    with pytest.raises(MalformedPacketError, match="too short"):
        SubAckPacket.decode(b"\x00\x01", V311)
    with pytest.raises(MalformedPacketError, match="must not be 0"):
        SubAckPacket.decode(b"\x00\x00\x00", V311)
    with pytest.raises(MalformedPacketError, match="reason code"):
        SubAckPacket.decode(b"\x00\x01\x7f", V311)
    with pytest.raises(MalformedPacketError, match="missing reason"):
        SubAckPacket.decode(b"\x00\x01\x00", V5)


def test_suback_decodes_all_valid_reason_codes() -> None:
    v311 = SubAckPacket.decode(b"\x00\x07\x00\x01\x02\x80", V311)
    assert v311.mid == 7
    assert v311.reason_codes == (0, 1, 2, 0x80)

    v5 = SubAckPacket.decode(b"\x00\x08\x00\x00\x01\x02\x91", V5)
    assert v5.mid == 8
    assert v5.reason_codes == (0, 1, 2, 0x91)


def test_unsuback_v5_requires_reason_payload() -> None:
    with pytest.raises(MalformedPacketError, match="too short"):
        UnsubAckPacket.decode(b"\x00", V311)
    with pytest.raises(MalformedPacketError, match="trailing"):
        UnsubAckPacket.decode(b"\x00\x01\x00", V311)
    with pytest.raises(MalformedPacketError, match="missing reason"):
        UnsubAckPacket.decode(b"\x00\x01\x00", V5)
    with pytest.raises(MalformedPacketError, match="reason code"):
        UnsubAckPacket.decode(b"\x00\x01\x00\x7f", V5)


def test_unsuback_decodes_valid_mqtt5_reasons() -> None:
    packet = UnsubAckPacket.decode(b"\x00\x09\x00\x00\x11\x80", V5)
    assert packet.mid == 9
    assert packet.reason_codes == (0, 0x11, 0x80)


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


def test_connect_encodes_each_protocol_version_layout() -> None:
    """Pin the three CONNECT wire layouts, which share one encoder.

    They differ only in the Protocol Name/Level prefix and in whether property
    tables are present. MQTT 3.1 in particular had no encoding coverage while
    its encoder lived inline in ConnectPacket.encode.
    """
    fields = dict(
        client_id="c1",
        clean_start=True,
        keepalive=60,
        username="u",
        password=b"pw",
        will_topic="w/t",
        will_payload=b"bye",
        will_qos=QoS.AT_LEAST_ONCE,
        will_retain=True,
    )
    # Will + will QoS 1 + will retain + clean start + username + password.
    flags = 0xEE
    tail = b"\x00\x02c1" + b"\x00\x03w/t\x00\x03bye" + b"\x00\x01u" + b"\x00\x02pw"

    v31 = ConnectPacket(protocol=MQTTProtocolVersion.MQTTv31, **fields).encode()
    assert v31[2:11] == b"\x00\x06MQIsdp\x03"
    assert v31[11] == flags
    assert v31[12:14] == b"\x00\x3c"
    assert v31[14:] == tail

    v311 = ConnectPacket(protocol=V311, **fields).encode()
    assert v311[2:9] == b"\x00\x04MQTT\x04"
    assert v311[9] == flags
    assert v311[10:12] == b"\x00\x3c"
    assert v311[12:] == tail

    # MQTT 5 interleaves an empty CONNECT table after keepalive and an empty
    # WILL table before the Will Topic.
    v5 = ConnectPacket(protocol=V5, **fields).encode()
    assert v5[2:9] == b"\x00\x04MQTT\x05"
    assert v5[9] == flags
    assert v5[10:12] == b"\x00\x3c"
    assert (
        v5[12:]
        == b"\x00" + b"\x00\x02c1" + b"\x00" + b"\x00\x03w/t\x00\x03bye" + b"\x00\x01u\x00\x02pw"
    )
