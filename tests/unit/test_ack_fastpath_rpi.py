from __future__ import annotations

from mqttium.codec.buffer import IncrementalDecoder, RawPacket
from mqttium.codec.primitives import pack_u16, pack_utf8
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType
from mqttium.packets import PubCompPacket, PubRecPacket, PubRelPacket
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import Properties


def _raw(wire: bytes) -> RawPacket:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    packet = decoder.next_packet()
    assert packet is not None
    return packet


def _rpi0(engine: ProtocolEngine) -> None:
    engine._sent_request_problem_information = 0


def _assert_rejected(engine: ProtocolEngine) -> None:
    effects = engine.take_effects()
    assert engine.state is ConnectionState.DISCONNECTED
    assert any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)


def test_pubrec_long_form_still_enforces_request_problem_information() -> None:
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv5))
    engine.state = ConnectionState.CONNECTED
    handle = engine.queue_publish("ack/rpi", b"payload", qos=2)
    assert handle.mid is not None
    engine.take_effects()
    _rpi0(engine)
    engine.handle_raw(
        _raw(
            PubRecPacket(
                mid=handle.mid, properties=Properties(values={"reason_string": "forbidden"})
            ).encode(MQTTProtocolVersion.MQTTv5)
        )
    )
    _assert_rejected(engine)


def test_pubcomp_long_form_still_enforces_request_problem_information() -> None:
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv5))
    engine.state = ConnectionState.CONNECTED
    handle = engine.queue_publish("ack/rpi", b"payload", qos=2)
    assert handle.mid is not None
    engine.take_effects()
    engine.handle_raw(RawPacket(PacketType.PUBREC, 0, pack_u16(handle.mid)))
    engine.take_effects()
    _rpi0(engine)
    engine.handle_raw(
        _raw(
            PubCompPacket(
                mid=handle.mid, properties=Properties(values={"reason_string": "forbidden"})
            ).encode(MQTTProtocolVersion.MQTTv5)
        )
    )
    _assert_rejected(engine)


def test_pubrel_long_form_still_enforces_request_problem_information() -> None:
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv5))
    engine.state = ConnectionState.CONNECTED
    engine.handle_raw(
        RawPacket(PacketType.PUBLISH, 0x04, pack_utf8("ack/rpi") + pack_u16(9) + b"\x00payload")
    )
    engine.take_effects()
    _rpi0(engine)
    engine.handle_raw(
        _raw(
            PubRelPacket(
                mid=9, properties=Properties(values={"reason_string": "forbidden"})
            ).encode(MQTTProtocolVersion.MQTTv5)
        )
    )
    _assert_rejected(engine)
