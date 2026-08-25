"""Inbound Topic Alias validation must use the value advertised in CONNECT."""

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


def _connect(engine: ProtocolEngine) -> None:
    engine.begin_connect()
    _feed(engine, encode_frame(PacketType.CONNACK, 0, b"\x00\x00\x00"))
    engine.take_effects()
    assert engine.state is ConnectionState.CONNECTED


def _alias_publish(alias: int) -> bytes:
    return PublishPacket(
        topic="sensors/temp",
        payload=b"21.5",
        qos=QoS.AT_MOST_ONCE,
        retain=False,
        dup=False,
        properties=Properties({"topic_alias": alias}),
    ).encode(MQTTProtocolVersion.MQTTv5)


def test_connect_properties_topic_alias_maximum_is_the_inbound_limit() -> None:
    connect_properties = Properties({"topic_alias_maximum": 5})
    engine = ProtocolEngine(
        EngineConfig(
            client_id="alias-override",
            protocol=MQTTProtocolVersion.MQTTv5,
            connect_properties=connect_properties,
        ),
        MemoryInflightStore(),
    )

    _connect(engine)

    # The advertised value is connection state. Mutating the application-owned
    # Properties object after CONNECT must not change what the peer is allowed
    # to send on the already-established connection.
    connect_properties.set("topic_alias_maximum", 0)
    _feed(engine, _alias_publish(1))
    effects = engine.take_effects()

    assert not any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)
    messages = [
        effect.data
        for effect in effects
        if effect.kind in (EffectKind.MESSAGE, EffectKind.DECODED_MESSAGE)
    ]
    assert len(messages) == 1
    assert messages[0].topic == "sensors/temp"


def test_engine_config_topic_alias_maximum_is_used_when_connect_property_is_absent() -> None:
    engine = ProtocolEngine(
        EngineConfig(
            client_id="alias-config",
            protocol=MQTTProtocolVersion.MQTTv5,
            topic_alias_maximum=2,
        ),
        MemoryInflightStore(),
    )

    _connect(engine)
    _feed(engine, _alias_publish(2))
    effects = engine.take_effects()

    assert not any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)
    assert any(
        effect.kind in (EffectKind.MESSAGE, EffectKind.DECODED_MESSAGE) for effect in effects
    )


def test_alias_above_effective_advertised_maximum_is_still_rejected() -> None:
    engine = ProtocolEngine(
        EngineConfig(
            client_id="alias-bound",
            protocol=MQTTProtocolVersion.MQTTv5,
            connect_properties=Properties({"topic_alias_maximum": 2}),
            topic_alias_maximum=7,
        ),
        MemoryInflightStore(),
    )

    _connect(engine)
    _feed(engine, _alias_publish(3))
    effects = engine.take_effects()

    assert any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)
    sends = [effect.data for effect in effects if effect.kind is EffectKind.SEND]
    assert len(sends) == 1
    disconnect = sends[0] if isinstance(sends[0], bytes) else sends[0][0]
    assert (disconnect[0] & 0xF0) == PacketType.DISCONNECT.value
    assert disconnect[2] == 0x94
