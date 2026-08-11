"""Passing regressions for the second independent protocol audit."""

from __future__ import annotations

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import ProtocolError
from mqttium.packets import SubscribeOptions, encode_frame
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import Message


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def _connected(
    protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv5,
) -> ProtocolEngine:
    engine = ProtocolEngine(EngineConfig(client_id="audit-followup", protocol=protocol))
    engine.begin_connect()
    body = b"\x00\x00\x00" if protocol is MQTTProtocolVersion.MQTTv5 else b"\x00\x00"
    _feed(engine, encode_frame(PacketType.CONNACK, 0, body))
    engine.take_effects()
    return engine


def _ack_frame(packet_type: PacketType, mid: int, reasons: list[int]) -> bytes:
    body = bytes((mid >> 8, mid & 0xFF, 0)) + bytes(reasons)
    return encode_frame(packet_type, 0, body)


@pytest.mark.parametrize("protocol", list(MQTTProtocolVersion))
def test_pingresp_rejects_a_nonzero_remaining_length(
    protocol: MQTTProtocolVersion,
) -> None:
    engine = _connected(protocol)

    _feed(engine, b"\xd0\x01\xff")
    effects = engine.take_effects()

    assert any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)
    assert not any(effect.kind is EffectKind.PINGRESP for effect in effects)


@pytest.mark.parametrize("topic", ["", "bad/#", "bad/+"])
def test_connect_rejects_an_invalid_will_topic_before_state_mutation(topic: str) -> None:
    engine = ProtocolEngine(
        EngineConfig(
            client_id="will-topic",
            will=Message(topic=topic, payload=b"bye"),
        )
    )

    with pytest.raises(ProtocolError, match="topic"):
        engine.begin_connect()

    assert engine.state.name == "NEW"
    assert engine.take_effects() == []


def test_shared_subscription_rejects_no_local_before_allocating_a_mid() -> None:
    engine = _connected()

    with pytest.raises(ProtocolError, match="No Local.*shared"):
        engine.queue_subscribe(
            (("$share/group/topic", SubscribeOptions(qos=QoS.AT_MOST_ONCE, no_local=True)),)
        )

    assert not engine._pending_sub_mids
    assert len(engine.packet_ids) == 0
    assert engine.take_effects() == []


@pytest.mark.parametrize(
    ("topic_count", "reasons"),
    [
        (1, [0, 1]),
        (2, [0]),
    ],
)
def test_suback_reason_count_must_match_the_request(
    topic_count: int,
    reasons: list[int],
) -> None:
    engine = _connected()
    topics = [f"topic/{index}" for index in range(topic_count)]
    mid = engine.queue_subscribe(topics)
    engine.take_effects()

    _feed(engine, _ack_frame(PacketType.SUBACK, mid, reasons))
    effects = engine.take_effects()

    assert any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)
    assert not any(effect.kind is EffectKind.SUBACK for effect in effects)
    assert mid in engine._pending_sub_mids


def test_unsuback_reason_count_must_match_the_request() -> None:
    engine = _connected()
    mid = engine.queue_unsubscribe("one/topic")
    engine.take_effects()

    _feed(engine, _ack_frame(PacketType.UNSUBACK, mid, [0, 0x11]))
    effects = engine.take_effects()

    assert any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)
    assert not any(effect.kind is EffectKind.UNSUBACK for effect in effects)
    assert mid in engine._pending_sub_mids


def test_matching_subscription_ack_consumes_its_request_shape() -> None:
    engine = _connected()
    mid = engine.queue_subscribe(["one", "two"])
    engine.take_effects()

    _feed(engine, _ack_frame(PacketType.SUBACK, mid, [0, 1]))

    assert any(effect.kind is EffectKind.SUBACK for effect in engine.take_effects())
    assert mid not in engine._pending_sub_mids
    assert mid not in engine._pending_sub_requests
