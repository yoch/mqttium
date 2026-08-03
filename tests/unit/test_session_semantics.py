"""Regression tests for planning-audit findings (engine level)."""

from __future__ import annotations

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import OutboundQoSState
from mqttium.packets import PubAckPacket, PublishPacket, encode_frame
from mqttium.enums import PacketType
from mqttium.protocol.engine import EffectKind, EngineConfig, ProtocolEngine


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    dec = IncrementalDecoder()
    dec.feed(wire)
    raw = dec.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def _connack(session_present: bool = False, reason_code: int = 0) -> bytes:
    flags = 0x01 if session_present else 0x00
    return encode_frame(PacketType.CONNACK, 0, bytes((flags, reason_code)))


def _as_bytes(data: object) -> bytes:
    if isinstance(data, bytes):
        return data
    assert isinstance(data, tuple)
    return data[0] + data[1]


def _take_sends(engine: ProtocolEngine) -> list[bytes]:
    return [_as_bytes(e.data) for e in engine.take_effects() if e.kind is EffectKind.SEND]


def test_offline_queue_survives_clean_connect() -> None:
    """QoS>0 published before connect must be sent after CONNACK, not dropped."""
    engine = ProtocolEngine(EngineConfig(client_id="c1"))
    handle = engine.queue_publish("offline/topic", b"queued", qos=1)
    assert _take_sends(engine) == []
    assert engine.store.get_out(handle.mid or 0) is not None

    engine.begin_connect()
    _feed(engine, _connack(session_present=False))
    effects = engine.take_effects()
    failed = [e for e in effects if e.kind is EffectKind.PUBLISH_FAILED]
    sends = [_as_bytes(e.data) for e in effects if e.kind is EffectKind.SEND]
    assert failed == []
    assert len(sends) == 1

    dec = IncrementalDecoder()
    dec.feed(sends[0])
    raw = dec.next_packet()
    assert raw is not None
    pub = PublishPacket.decode(raw.flags, raw.remaining)
    assert pub.mid == handle.mid
    assert pub.dup is False
    assert pub.payload == b"queued"

    # Completion still works end-to-end.
    _feed(engine, PubAckPacket(mid=handle.mid or 0).encode())
    effects = engine.take_effects()
    assert any(e.kind is EffectKind.PUBLISH_COMPLETE and e.data == handle.mid for e in effects)


def test_clean_reconnect_fails_inflight_keeps_queued() -> None:
    """Inflight from the old session fail on clean CONNACK; queued survive."""
    engine = ProtocolEngine(
        EngineConfig(client_id="c1", local_receive_maximum=1, max_outbound_inflight=1)
    )
    engine.begin_connect()
    _feed(engine, _connack(session_present=False))
    engine.take_effects()

    inflight = engine.queue_publish("a", b"1", qos=1)  # takes the only slot
    queued = engine.queue_publish("b", b"2", qos=1)  # stays QUEUED
    engine.take_effects()
    assert engine.store.get_out(queued.mid or 0).state is OutboundQoSState.QUEUED

    engine.notify_transport_closed()
    engine.take_effects()
    engine.begin_connect()
    _feed(engine, _connack(session_present=False))
    effects = engine.take_effects()

    failed_mids = [
        e.data.mid if hasattr(e.data, "mid") else e.data
        for e in effects
        if e.kind is EffectKind.PUBLISH_FAILED
    ]
    assert failed_mids == [inflight.mid]
    assert engine.store.get_out(inflight.mid or 0) is None
    assert not engine.packet_ids.in_use(inflight.mid or 0)

    # The queued message must have been launched on the fresh session.
    sends = [_as_bytes(e.data) for e in effects if e.kind is EffectKind.SEND]
    assert len(sends) == 1
    dec = IncrementalDecoder()
    dec.feed(sends[0])
    raw = dec.next_packet()
    assert raw is not None
    pub = PublishPacket.decode(raw.flags, raw.remaining)
    assert pub.mid == queued.mid
    assert engine.store.get_out(queued.mid or 0).state is OutboundQoSState.WAIT_PUBACK
