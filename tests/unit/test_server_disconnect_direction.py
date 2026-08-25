"""DISCONNECT is server-to-client only starting with MQTT 5."""

from __future__ import annotations

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType
from mqttium.packets import encode_frame
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import DisconnectInfo, ProtocolEngine


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def _connected_engine(protocol: MQTTProtocolVersion) -> ProtocolEngine:
    engine = ProtocolEngine(
        EngineConfig(client_id="server-disconnect", protocol=protocol),
        MemoryInflightStore(),
    )
    engine.begin_connect()
    connack = b"\x00\x00\x00" if protocol is MQTTProtocolVersion.MQTTv5 else b"\x00\x00"
    _feed(engine, encode_frame(PacketType.CONNACK, 0, connack))
    engine.take_effects()
    assert engine.state is ConnectionState.CONNECTED
    return engine


def test_mqtt311_server_disconnect_is_a_protocol_error() -> None:
    engine = _connected_engine(MQTTProtocolVersion.MQTTv311)

    _feed(engine, encode_frame(PacketType.DISCONNECT, 0, b""))
    effects = engine.take_effects()

    assert any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)
    assert not any(effect.kind is EffectKind.DISCONNECTED for effect in effects)


def test_mqtt5_server_disconnect_remains_a_normal_broker_disconnect() -> None:
    engine = _connected_engine(MQTTProtocolVersion.MQTTv5)

    _feed(engine, encode_frame(PacketType.DISCONNECT, 0, b""))
    effects = engine.take_effects()

    assert not any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)
    disconnected = [effect.data for effect in effects if effect.kind is EffectKind.DISCONNECTED]
    assert len(disconnected) == 1
    assert isinstance(disconnected[0], DisconnectInfo)
    assert disconnected[0].from_broker is True
