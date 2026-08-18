from __future__ import annotations

import pytest

from mqttium.codec.buffer import RawPacket
from mqttium.codec.primitives import pack_u16
from mqttium.enums import (
    ConnectionState,
    MQTTProtocolVersion,
    OutboundQoSState,
    PacketType,
    QoS,
)
from mqttium.errors import MandatoryResponseTooLargeError
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine
from mqttium.protocol.negotiated import NegotiatedSettings


def _engine(limit: int | None = None) -> ProtocolEngine:
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv5))
    engine.state = ConnectionState.CONNECTED
    engine.negotiated = NegotiatedSettings(maximum_packet_size=limit)
    return engine


def _pubrec(mid: int) -> RawPacket:
    return RawPacket(PacketType.PUBREC, 0, pack_u16(mid))


def _start_qos2(engine: ProtocolEngine) -> int:
    handle = engine.queue_publish("a", b"", qos=QoS.EXACTLY_ONCE)
    assert handle.mid is not None
    engine.take_effects()
    record = engine.store.get_out(handle.mid)
    assert record is not None
    assert record.state is OutboundQoSState.WAIT_PUBREC
    return handle.mid


def test_unknown_pubrec_requires_five_byte_reason_pubrel() -> None:
    engine = _engine(4)

    with pytest.raises(
        MandatoryResponseTooLargeError,
        match=r"Mandatory PUBREL size 5 exceeds broker maximum_packet_size 4",
    ):
        engine.handle_raw(_pubrec(77))

    assert engine.take_effects() == []
    assert engine.state is ConnectionState.CONNECTED


def test_unknown_pubrec_reason_response_fits_at_five() -> None:
    engine = _engine(5)

    engine.handle_raw(_pubrec(77))

    sends = [effect.data for effect in engine.take_effects() if effect.kind is EffectKind.SEND]
    assert sends == [bytes.fromhex("6203004d92")]


def test_success_pubrel_capacity_is_checked_before_qos2_transition() -> None:
    engine = _engine()
    mid = _start_qos2(engine)
    engine.negotiated = NegotiatedSettings(maximum_packet_size=3)

    with pytest.raises(
        MandatoryResponseTooLargeError,
        match=r"Mandatory PUBREL size 4 exceeds broker maximum_packet_size 3",
    ):
        engine.handle_raw(_pubrec(mid))

    preserved = engine.store.get_out(mid)
    assert preserved is not None
    assert preserved.state is OutboundQoSState.WAIT_PUBREC
    assert engine.take_effects() == []


def test_tiny_limit_does_not_fail_pubrec_when_no_pubrel_would_be_sent() -> None:
    engine = _engine()
    mid = _start_qos2(engine)
    engine.handle_raw(_pubrec(mid))
    engine.take_effects()
    record = engine.store.get_out(mid)
    assert record is not None
    assert record.state is OutboundQoSState.WAIT_PUBCOMP

    engine.negotiated = NegotiatedSettings(maximum_packet_size=3)
    engine.handle_raw(_pubrec(mid))

    assert engine.take_effects() == []
    record = engine.store.get_out(mid)
    assert record is not None
    assert record.state is OutboundQoSState.WAIT_PUBCOMP
