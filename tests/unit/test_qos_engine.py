"""QoS state machine and flow-control tests (no network)."""

from __future__ import annotations

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import OutboundQoSState, PacketType, QoS
from mqttium.packets import (
    ConnAckPacket,
    PubAckPacket,
    PubCompPacket,
    PublishPacket,
    PubRecPacket,
    PubRelPacket,
    encode_frame,
)
from mqttium.protocol.engine import EffectKind, EngineConfig, ProtocolEngine
from mqttium.protocol.flow_control import FlowControl
from mqttium.protocol.packet_ids import PacketIdPool


def _feed_connack_ok(engine: ProtocolEngine, session_present: bool = False) -> None:
    flags = 0x01 if session_present else 0x00
    remaining = bytes((flags, 0x00))
    raw = IncrementalDecoder()
    raw.feed(encode_frame(PacketType.CONNACK, 0, remaining))
    packet = raw.next_packet()
    assert packet is not None
    engine.handle_raw(packet)


def _take_sends(engine: ProtocolEngine) -> list[bytes]:
    out: list[bytes] = []
    for e in engine.take_effects():
        if e.kind is EffectKind.SEND:
            data = e.data
            out.append(data if isinstance(data, bytes) else data[0] + data[1])
    return out


def test_packet_id_pool_independent_of_receive_maximum() -> None:
    pool = PacketIdPool()
    flow = FlowControl(limit=2)
    ids = [pool.allocate() for _ in range(5)]
    assert ids == [1, 2, 3, 4, 5]
    assert flow.available == 2
    assert pool.available == 65535 - 5


def test_qos1_roundtrip_with_decoder() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="c1"))
    engine.begin_connect()
    _feed_connack_ok(engine)
    engine.take_effects()

    handle = engine.queue_publish("sensors/1", b"42", qos=1)
    sends = _take_sends(engine)
    assert len(sends) == 1

    ack = PubAckPacket(mid=handle.mid or 0).encode()
    dec = IncrementalDecoder()
    dec.feed(ack)
    engine.handle_raw(dec.next_packet())  # type: ignore[arg-type]
    effects = engine.take_effects()
    assert any(e.kind is EffectKind.PUBLISH_COMPLETE and e.data == handle.mid for e in effects)
    assert engine.store.get_out(handle.mid or 0) is None
    assert not engine.packet_ids.in_use(handle.mid or 0)


def test_qos2_outbound_keeps_mid_until_pubcomp() -> None:
    """Regression against gmqtt bug: MID must survive PUBREC."""
    engine = ProtocolEngine(EngineConfig(client_id="c1"))
    engine.begin_connect()
    _feed_connack_ok(engine)
    engine.take_effects()

    handle = engine.queue_publish("exactly", b"once", qos=2)
    mid = handle.mid
    assert mid is not None
    _take_sends(engine)

    # PUBREC
    dec = IncrementalDecoder()
    dec.feed(PubRecPacket(mid=mid).encode())
    engine.handle_raw(dec.next_packet())  # type: ignore[arg-type]
    sends = _take_sends(engine)
    assert len(sends) == 1
    # Must still hold MID and store PUBREL phase.
    stored = engine.store.get_out(mid)
    assert stored is not None
    assert stored.state is OutboundQoSState.WAIT_PUBCOMP
    assert engine.packet_ids.in_use(mid)

    # PUBCOMP
    dec = IncrementalDecoder()
    dec.feed(PubCompPacket(mid=mid).encode())
    engine.handle_raw(dec.next_packet())  # type: ignore[arg-type]
    effects = engine.take_effects()
    assert any(e.kind is EffectKind.PUBLISH_COMPLETE for e in effects)
    assert engine.store.get_out(mid) is None
    assert not engine.packet_ids.in_use(mid)


def test_qos2_inbound_dedup() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="c1"))
    engine.begin_connect()
    _feed_connack_ok(engine)
    engine.take_effects()

    publish = PublishPacket(
        topic="in",
        payload=b"data",
        qos=QoS.EXACTLY_ONCE,
        retain=False,
        dup=False,
        mid=7,
    ).encode()
    dec = IncrementalDecoder()
    dec.feed(publish)
    engine.handle_raw(dec.next_packet())  # type: ignore[arg-type]
    effects = engine.take_effects()
    messages = [e for e in effects if e.kind is EffectKind.MESSAGE]
    assert len(messages) == 1
    sends = [e.data for e in effects if e.kind is EffectKind.SEND]
    assert len(sends) == 1  # PUBREC

    # Duplicate PUBLISH with DUP — must not redeliver.
    dup = PublishPacket(
        topic="in",
        payload=b"data",
        qos=QoS.EXACTLY_ONCE,
        retain=False,
        dup=True,
        mid=7,
    ).encode()
    dec = IncrementalDecoder()
    dec.feed(dup)
    engine.handle_raw(dec.next_packet())  # type: ignore[arg-type]
    effects = engine.take_effects()
    assert not any(e.kind is EffectKind.MESSAGE for e in effects)
    assert any(e.kind is EffectKind.SEND for e in effects)  # PUBREC again

    # PUBREL completes
    dec = IncrementalDecoder()
    dec.feed(PubRelPacket(mid=7).encode())
    engine.handle_raw(dec.next_packet())  # type: ignore[arg-type]
    effects = engine.take_effects()
    assert any(e.kind is EffectKind.SEND for e in effects)  # PUBCOMP
    assert engine.store.get_in(7) is None


def test_flow_control_queues_when_window_full() -> None:
    engine = ProtocolEngine(
        EngineConfig(client_id="c1", local_receive_maximum=1, max_outbound_inflight=1)
    )
    engine.begin_connect()
    _feed_connack_ok(engine)
    engine.take_effects()

    h1 = engine.queue_publish("a", b"1", qos=1)
    h2 = engine.queue_publish("b", b"2", qos=1)
    sends = _take_sends(engine)
    assert len(sends) == 1
    assert h1.mid == 1
    assert h2.mid == 2
    assert engine.store.get_out(2) is not None
    assert engine.store.get_out(2).state is OutboundQoSState.QUEUED

    # Complete first — second should launch.
    dec = IncrementalDecoder()
    dec.feed(PubAckPacket(mid=1).encode())
    engine.handle_raw(dec.next_packet())  # type: ignore[arg-type]
    sends = _take_sends(engine)
    assert len(sends) == 1
    assert engine.store.get_out(2).state is OutboundQoSState.WAIT_PUBACK


def test_session_present_replays_with_dup() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="c1", clean_start=False))
    engine.begin_connect()
    _feed_connack_ok(engine, session_present=False)
    engine.take_effects()
    handle = engine.queue_publish("re", b"x", qos=1)
    mid = handle.mid
    assert mid is not None
    _take_sends(engine)

    # Simulate drop then resume with session_present=1.
    engine.notify_transport_closed()
    engine.take_effects()
    engine.begin_connect()
    _feed_connack_ok(engine, session_present=True)
    sends = _take_sends(engine)
    assert len(sends) == 1
    dec = IncrementalDecoder()
    dec.feed(sends[0])
    raw = dec.next_packet()
    assert raw is not None
    pub = PublishPacket.decode(raw.flags, raw.remaining)
    assert pub.dup is True
    assert pub.mid == mid


def test_connack_helpers_parse() -> None:
    remaining = bytes((0x01, 0x00))
    ack = ConnAckPacket.decode(remaining)
    assert ack.session_present is True
    assert ack.reason_code == 0
