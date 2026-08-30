"""QoS protocol-response effects retain semantics through runtime admission."""

from __future__ import annotations

import sys

from mqttium.api import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PubRecPacket, PubRelPacket, PublishPacket, encode_frame
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.protocol.engine import ProtocolEngine


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def _connected_engine() -> ProtocolEngine:
    engine = ProtocolEngine(
        EngineConfig(
            client_id="protocol-response-eager",
            protocol=MQTTProtocolVersion.MQTTv5,
        )
    )
    engine.begin_connect()
    _feed(engine, encode_frame(PacketType.CONNACK, 0, b"\x00\x00\x00"))
    engine.take_effects()
    return engine


def test_protocol_response_kind_does_not_expand_generic_effects() -> None:
    ordinary = EngineEffect(EffectKind.SEND, b"data")
    response = EngineEffect(EffectKind.SEND_PROTOCOL_RESPONSE, b"ack")

    assert ordinary.kind is EffectKind.SEND
    assert response.kind is EffectKind.SEND_PROTOCOL_RESPONSE
    assert not hasattr(response, "__dict__")
    assert sys.getsizeof(response) == sys.getsizeof(ordinary)


def test_inbound_puback_is_marked_as_protocol_response() -> None:
    engine = _connected_engine()
    publish = PublishPacket(
        topic="response/qos1",
        payload=b"x",
        qos=QoS.AT_LEAST_ONCE,
        retain=False,
        dup=False,
        mid=7,
    )

    _feed(engine, publish.encode(MQTTProtocolVersion.MQTTv5))

    send = next(
        effect
        for effect in engine.take_effects()
        if effect.kind is EffectKind.SEND_PROTOCOL_RESPONSE
    )
    assert send.data[0] & 0xF0 == PacketType.PUBACK


def test_outbound_pubrel_retains_producer_batching() -> None:
    engine = _connected_engine()
    handle = engine.queue_publish("response/qos2", b"x", qos=QoS.EXACTLY_ONCE)
    engine.take_effects()

    _feed(
        engine,
        PubRecPacket(mid=handle.mid or 0).encode(MQTTProtocolVersion.MQTTv5),
    )

    send = next(effect for effect in engine.take_effects() if effect.kind is EffectKind.SEND)
    assert send.data[0] & 0xF0 == PacketType.PUBREL


def test_inbound_qos2_responses_are_marked_as_protocol_responses() -> None:
    engine = _connected_engine()
    publish = PublishPacket(
        topic="response/qos2",
        payload=b"x",
        qos=QoS.EXACTLY_ONCE,
        retain=False,
        dup=False,
        mid=9,
    )

    _feed(engine, publish.encode(MQTTProtocolVersion.MQTTv5))
    pubrec = next(
        effect
        for effect in engine.take_effects()
        if effect.kind is EffectKind.SEND_PROTOCOL_RESPONSE
    )
    assert pubrec.data[0] & 0xF0 == PacketType.PUBREC

    _feed(engine, PubRelPacket(mid=9).encode(MQTTProtocolVersion.MQTTv5))
    pubcomp = next(
        effect
        for effect in engine.take_effects()
        if effect.kind is EffectKind.SEND_PROTOCOL_RESPONSE
    )
    assert pubcomp.data[0] & 0xF0 == PacketType.PUBCOMP


def test_runtime_routes_marked_send_to_protocol_response_admission() -> None:
    client = AsyncClient(client_id="protocol-response-runtime")
    routed: list[bytes] = []
    ordinary: list[bytes] = []
    client._try_enqueue_protocol_response = lambda data, *, epoch=None: routed.append(data) is None
    client._try_enqueue_outbound = lambda data, *, epoch=None: ordinary.append(data) is None

    applied = client._apply_effect_inline(
        EngineEffect(EffectKind.SEND_PROTOCOL_RESPONSE, b"ack"),
        client._connection_epoch,
    )

    assert applied is True
    assert routed == [b"ack"]
    assert ordinary == []
