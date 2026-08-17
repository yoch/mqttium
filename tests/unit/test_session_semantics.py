"""Regression tests for planning-audit findings (engine level)."""

from __future__ import annotations

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import MQTTProtocolVersion, OutboundQoSState
from mqttium.errors import ProtocolError, SessionDiscardedError
from mqttium.packets import PubAckPacket, PublishPacket, encode_frame
from mqttium.enums import PacketType
from mqttium.protocol.engine import EffectKind, EngineConfig, ProtocolEngine
from mqttium.types import Properties


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


def _connack_v5(session_present: bool, receive_maximum: int | None = None) -> bytes:
    props = b""
    if receive_maximum is not None:
        props = bytes((0x21, receive_maximum >> 8, receive_maximum & 0xFF))
    body = bytes((0x01 if session_present else 0x00, 0x00)) + bytes((len(props),)) + props
    return bytes((0x20, len(body))) + body


def test_session_loss_with_blocked_replay_queue() -> None:
    """A clean-session CONNACK must also purge flow-blocked WAIT_* queue entries.

    `replay_session()` leaves retransmissions the broker's Receive Maximum
    window could not admit in `_queued` as WAIT_* entries. When the broker then
    reports Session Present 0 (the server-side session expired), those records
    are failed like every other unacknowledged publication — and their queue
    entries must go with them. A stale entry made `drain()` re-materialise a
    deleted record, double-release its byte reservation (an AssertionError
    surfaced as PROTOCOL_ERROR) and could retransmit a packet id the pool no
    longer owns.
    """
    connect_props = Properties()
    connect_props.set("session_expiry_interval", 60)
    engine = ProtocolEngine(
        EngineConfig(
            client_id="c1",
            protocol=MQTTProtocolVersion.MQTTv5,
            clean_start=False,
            connect_properties=connect_props,
        )
    )

    # Connection 1: launch three QoS 1 publications, never acknowledged.
    engine.begin_connect()
    _feed(engine, _connack_v5(session_present=False))
    engine.take_effects()
    handles = [engine.queue_publish(f"t/{i}", b"x" * 10, qos=1) for i in range(3)]
    engine.take_effects()
    engine.notify_transport_closed()
    engine.take_effects()

    # Connection 2: the broker resumes the session but caps Receive Maximum at
    # 2, so replay leaves the third record blocked in `_queued` as WAIT_PUBACK.
    engine.begin_connect()
    _feed(engine, _connack_v5(session_present=True, receive_maximum=2))
    engine.take_effects()
    blocked = [m for m in engine._queued if m.state is OutboundQoSState.WAIT_PUBACK]
    assert len(blocked) == 1
    assert blocked[0].mid == handles[2].mid

    # Connection 3: the server-side session expired — Session Present 0.
    engine.notify_transport_closed()
    engine.take_effects()
    engine.begin_connect()
    _feed(engine, _connack_v5(session_present=False))
    effects = engine.take_effects()

    assert not any(e.kind is EffectKind.PROTOCOL_ERROR for e in effects)
    failures = [e.data for e in effects if e.kind is EffectKind.PUBLISH_FAILED]
    assert sorted(f.mid for f in failures) == sorted(h.mid or 0 for h in handles)
    assert all(isinstance(f.reason, SessionDiscardedError) for f in failures)
    assert len(engine.outbound._queued) == 0
    outbound = engine.outbound
    assert outbound.pending_messages == 0
    assert outbound.pending_bytes == 0

    # The engine stays usable: a fresh publication launches on the new session.
    fresh = engine.queue_publish("t/fresh", b"y", qos=1)
    sends = [_as_bytes(e.data) for e in engine.take_effects() if e.kind is EffectKind.SEND]
    assert len(sends) == 1
    assert engine.store.get_out(fresh.mid or 0) is not None


def test_begin_connect_rejected_while_disconnecting() -> None:
    """A CONNECT attempted while the final DISCONNECT drains must be refused.

    Accepting it flipped the state machine to CONNECTING under a transport
    that is about to disappear.
    """
    engine = ProtocolEngine(EngineConfig(client_id="c1"))
    engine.begin_connect()
    _feed(engine, _connack(session_present=False))
    engine.take_effects()

    engine.begin_disconnect()
    with pytest.raises(ProtocolError, match="disconnecting"):
        engine.begin_connect()

    # After the transport actually closes, connecting again is legal.
    engine.notify_transport_closed()
    engine.take_effects()
    engine.begin_connect()
    _feed(engine, _connack(session_present=False))
    assert any(e.kind is EffectKind.CONNACK for e in engine.take_effects())
