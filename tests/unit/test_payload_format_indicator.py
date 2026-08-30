"""Outbound MQTT 5 Payload Format Indicator validation contracts."""

from __future__ import annotations

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import ProtocolError
from mqttium.packets import PublishPacket, encode_frame
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import Message, Properties

V5 = MQTTProtocolVersion.MQTTv5


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def _connected() -> ProtocolEngine:
    engine = ProtocolEngine(EngineConfig(client_id="pfi", protocol=V5))
    engine.begin_connect()
    _feed(engine, encode_frame(PacketType.CONNACK, 0, b"\x00\x00\x00"))
    engine.take_effects()
    return engine


def _pfi(value: int) -> Properties:
    return Properties({"payload_format_indicator": value})


@pytest.mark.parametrize("payload", [b"\xff\xfe", b"\xc3\x28"])
@pytest.mark.parametrize("qos", list(QoS))
def test_pfi_1_rejects_invalid_publish_payload_before_admission(payload: bytes, qos: QoS) -> None:
    engine = _connected()

    with pytest.raises(ProtocolError, match="Payload Format Indicator.*UTF-8"):
        engine.queue_publish("topic", payload, qos=qos, properties=_pfi(1))

    assert engine.take_effects() == []
    assert engine.outbound.pending_messages == 0


def test_pfi_1_rejects_invalid_offline_qos_publish_before_persistence() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="pfi-offline", protocol=V5))

    with pytest.raises(ProtocolError, match="Payload Format Indicator.*UTF-8"):
        engine.queue_publish("topic", b"\xff", qos=QoS.AT_LEAST_ONCE, properties=_pfi(1))

    assert engine.take_effects() == []
    assert engine.store.get_out(1) is None


def test_pfi_1_valid_publish_payload_is_emitted_unchanged() -> None:
    payload = "valid \N{SNOWMAN}".encode()
    engine = _connected()

    engine.queue_publish("topic", payload, properties=_pfi(1))
    send = next(effect.data for effect in engine.take_effects() if effect.kind is EffectKind.SEND)
    wire = send if isinstance(send, bytes) else b"".join(send)
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None

    decoded = PublishPacket.decode(raw.flags, raw.remaining, V5)
    assert decoded.payload == payload


@pytest.mark.parametrize("properties", [None, _pfi(0)])
def test_binary_publish_payload_remains_legal_without_pfi_1(
    properties: Properties | None,
) -> None:
    packet = PublishPacket(
        topic="topic",
        payload=b"\xff\xfe",
        qos=QoS.AT_MOST_ONCE,
        retain=False,
        dup=False,
        properties=properties,
    )

    assert packet.encode(V5)


def test_low_level_publish_encoder_rejects_invalid_pfi_1_payload() -> None:
    packet = PublishPacket(
        topic="topic",
        payload=b"\xff",
        qos=QoS.AT_MOST_ONCE,
        retain=False,
        dup=False,
        properties=_pfi(1),
    )

    with pytest.raises(ProtocolError, match="Payload Format Indicator.*UTF-8"):
        packet.encode(V5)


def test_will_pfi_1_rejects_invalid_utf8_before_connect_commit() -> None:
    engine = ProtocolEngine(
        EngineConfig(
            client_id="will-pfi",
            protocol=V5,
            will=Message(topic="will/topic", payload=b"\xff"),
            will_properties=_pfi(1),
        )
    )

    with pytest.raises(ProtocolError, match="Payload Format Indicator.*UTF-8"):
        engine.begin_connect()

    assert engine.take_effects() == []
