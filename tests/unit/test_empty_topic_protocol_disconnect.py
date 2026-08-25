"""MQTT 5 protocol errors should be announced before transport teardown."""

from __future__ import annotations

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PublishPacket, encode_frame
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import Properties


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def test_empty_topic_without_alias_sends_protocol_error_disconnect() -> None:
    engine = ProtocolEngine(
        EngineConfig(client_id="empty-topic", protocol=MQTTProtocolVersion.MQTTv5),
        MemoryInflightStore(),
    )
    engine.begin_connect()
    _feed(engine, encode_frame(PacketType.CONNACK, 0, b"\x00\x00\x00"))
    engine.take_effects()
    assert engine.state is ConnectionState.CONNECTED

    publish = PublishPacket(
        topic="",
        payload=b"invalid",
        qos=QoS.AT_MOST_ONCE,
        retain=False,
        dup=False,
        properties=Properties(),
    ).encode(MQTTProtocolVersion.MQTTv5)
    _feed(engine, publish)
    effects = engine.take_effects()

    assert effects[0].kind is EffectKind.SEND
    disconnect_item = effects[0].data
    disconnect = disconnect_item if isinstance(disconnect_item, bytes) else disconnect_item[0]
    assert (disconnect[0] & 0xF0) == PacketType.DISCONNECT.value
    assert disconnect[2] == 0x82
    assert any(effect.kind is EffectKind.DISCONNECTED for effect in effects)
    assert any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)
    assert engine.state is ConnectionState.DISCONNECTED
