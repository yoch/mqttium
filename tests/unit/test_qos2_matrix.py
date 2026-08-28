"""QoS 2 reconnect phase matrix (IMPLEMENTATION-GUIDE §12)."""

from __future__ import annotations

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import OutboundQoSState, PacketType
from mqttium.packets import PubCompPacket, PubRecPacket, PublishPacket, encode_frame
from mqttium.protocol.engine import EffectKind, EngineConfig, ProtocolEngine


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    dec = IncrementalDecoder()
    dec.feed(wire)
    engine.handle_raw(dec.next_packet())  # type: ignore[arg-type]


def _connack(session_present: bool) -> bytes:
    flags = 0x01 if session_present else 0x00
    return encode_frame(PacketType.CONNACK, 0, bytes((flags, 0x00)))


def _connect(engine: ProtocolEngine, session_present: bool) -> None:
    engine.begin_connect()
    _feed(engine, _connack(session_present))
    engine.take_effects()


def _as_bytes(data: object) -> bytes:
    if isinstance(data, bytes):
        return data
    assert isinstance(data, tuple)
    return data[0] + data[1]


def _drop_and_reconnect(engine: ProtocolEngine, session_present: bool) -> list[bytes]:
    engine.notify_transport_closed()
    engine.take_effects()
    engine.begin_connect()
    _feed(engine, _connack(session_present))
    return [
        _as_bytes(e.data)
        for e in engine.take_effects()
        if e.kind in (EffectKind.SEND, EffectKind.SEND_PROTOCOL_RESPONSE)
    ]


def test_qos2_loss_before_publish_clean_session() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="c", clean_start=True, local_receive_maximum=1))
    # Offline queue then clean connect.
    handle = engine.queue_publish("t", b"x", qos=2)
    sends = _drop_and_reconnect(engine, session_present=False)
    # Fresh session: never-sent QUEUED should still launch (not failed).
    assert len(sends) == 1
    assert engine.store.get_out(handle.mid or 0).state is OutboundQoSState.WAIT_PUBREC


def test_qos2_loss_after_publish_session_present() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="c", clean_start=False))
    _connect(engine, session_present=False)
    handle = engine.queue_publish("t", b"x", qos=2)
    mid = handle.mid
    assert mid is not None
    engine.take_effects()
    sends = _drop_and_reconnect(engine, session_present=True)
    assert len(sends) == 1
    dec = IncrementalDecoder()
    dec.feed(sends[0])
    raw = dec.next_packet()
    assert raw is not None
    pub = PublishPacket.decode(raw.flags, raw.remaining)
    assert pub.dup is True
    assert pub.mid == mid


def test_qos2_loss_after_pubrec_replays_pubrel() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="c", clean_start=False))
    _connect(engine, session_present=False)
    handle = engine.queue_publish("t", b"x", qos=2)
    mid = handle.mid
    assert mid is not None
    engine.take_effects()
    _feed(engine, PubRecPacket(mid=mid).encode())
    engine.take_effects()
    assert engine.store.get_out(mid).state is OutboundQoSState.WAIT_PUBCOMP

    sends = _drop_and_reconnect(engine, session_present=True)
    assert len(sends) == 1
    # Must be PUBREL, not PUBLISH.
    assert sends[0][0] & 0xF0 == PacketType.PUBREL


def test_qos2_loss_after_pubrel_before_pubcomp_session_present() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="c", clean_start=False))
    _connect(engine, session_present=False)
    handle = engine.queue_publish("t", b"x", qos=2)
    mid = handle.mid
    assert mid is not None
    engine.take_effects()
    _feed(engine, PubRecPacket(mid=mid).encode())
    engine.take_effects()  # PUBREL sent
    sends = _drop_and_reconnect(engine, session_present=True)
    assert len(sends) == 1
    assert sends[0][0] & 0xF0 == PacketType.PUBREL
    _feed(engine, PubCompPacket(mid=mid).encode())
    effects = engine.take_effects()
    assert any(e.kind is EffectKind.PUBLISH_COMPLETE for e in effects)


def test_qos2_loss_after_publish_clean_session_fails() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="c", clean_start=True))
    _connect(engine, session_present=False)
    handle = engine.queue_publish("t", b"x", qos=2)
    mid = handle.mid
    assert mid is not None
    engine.take_effects()
    engine.notify_transport_closed()
    engine.take_effects()
    engine.begin_connect()
    _feed(engine, _connack(session_present=False))
    effects = engine.take_effects()
    failed = [e for e in effects if e.kind is EffectKind.PUBLISH_FAILED]
    assert len(failed) == 1
    assert failed[0].data.mid == mid
