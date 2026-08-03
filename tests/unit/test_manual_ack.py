"""manual_ack: deferred PUBACK / PUBCOMP until client.ack()."""

from __future__ import annotations

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import InboundQoSState, PacketType
from mqttium.packets import PubRelPacket, PublishPacket, encode_frame
from mqttium.protocol.engine import EffectKind, EngineConfig, ProtocolEngine


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    dec = IncrementalDecoder()
    dec.feed(wire)
    raw = dec.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def _connack() -> bytes:
    return encode_frame(PacketType.CONNACK, 0, bytes((0x00, 0x00)))


def _as_bytes(data: object) -> bytes:
    if isinstance(data, bytes):
        return data
    assert isinstance(data, tuple)
    return data[0] + data[1]


def _sends(effects: list) -> list[bytes]:
    return [_as_bytes(e.data) for e in effects if e.kind is EffectKind.SEND]


def test_manual_ack_defers_qos1_puback() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="c1", manual_ack=True))
    engine.begin_connect()
    _feed(engine, _connack())
    engine.take_effects()

    pub = PublishPacket(topic="t", payload=b"x", qos=1, retain=False, dup=False, mid=9).encode()
    _feed(engine, pub)
    effects = engine.take_effects()
    assert any(e.kind is EffectKind.MESSAGE for e in effects)
    assert _sends(effects) == []
    assert engine.store.get_in(9) is not None
    assert engine.store.get_in(9).state is InboundQoSState.WAIT_PUBACK

    engine.ack(9)
    effects = engine.take_effects()
    sends = _sends(effects)
    assert len(sends) == 1
    assert engine.store.get_in(9) is None


def test_manual_ack_qos2_pubrec_immediate_pubcomp_deferred() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="c1", manual_ack=True))
    engine.begin_connect()
    _feed(engine, _connack())
    engine.take_effects()

    pub = PublishPacket(topic="t", payload=b"x", qos=2, retain=False, dup=False, mid=3).encode()
    _feed(engine, pub)
    effects = engine.take_effects()
    assert any(e.kind is EffectKind.MESSAGE for e in effects)
    sends = _sends(effects)
    assert len(sends) == 1  # PUBREC
    assert engine.store.get_in(3).state is InboundQoSState.WAIT_PUBREL

    # App acks before PUBREL → mark ready.
    engine.ack(3)
    assert engine.take_effects() == []
    assert engine.store.get_in(3).user_acked is True

    _feed(engine, PubRelPacket(mid=3).encode())
    effects = engine.take_effects()
    sends = _sends(effects)
    assert len(sends) == 1  # PUBCOMP
    assert engine.store.get_in(3) is None


def test_manual_ack_qos2_ack_after_pubrel() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="c1", manual_ack=True))
    engine.begin_connect()
    _feed(engine, _connack())
    engine.take_effects()

    pub = PublishPacket(topic="t", payload=b"x", qos=2, retain=False, dup=False, mid=5).encode()
    _feed(engine, pub)
    engine.take_effects()

    _feed(engine, PubRelPacket(mid=5).encode())
    effects = engine.take_effects()
    assert _sends(effects) == []  # no PUBCOMP yet
    assert engine.store.get_in(5).state is InboundQoSState.WAIT_USER_ACK

    engine.ack(5)
    effects = engine.take_effects()
    assert len(_sends(effects)) == 1
    assert engine.store.get_in(5) is None
