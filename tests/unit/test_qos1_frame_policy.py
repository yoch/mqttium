"""Retained PUBLISH frame policy at the transport segmentation boundary."""

from __future__ import annotations

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import OutboundQoSState, PacketType, QoS
from mqttium.packets import PublishPacket, encode_frame
from mqttium.protocol.engine import EffectKind, EngineConfig, ProtocolEngine
from mqttium.transport.writes import SEGMENT_THRESHOLD, WriteItem


def _feed_connack(engine: ProtocolEngine, *, session_present: bool) -> None:
    flags = 1 if session_present else 0
    decoder = IncrementalDecoder()
    decoder.feed(encode_frame(PacketType.CONNACK, 0, bytes((flags, 0))))
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def _take_send_items(engine: ProtocolEngine) -> list[WriteItem]:
    return [
        effect.data
        for effect in engine.take_effects()
        if effect.kind is EffectKind.SEND and isinstance(effect.data, (bytes, tuple))
    ]


def _as_bytes(item: WriteItem) -> bytes:
    return item if isinstance(item, bytes) else item[0] + item[1]


def _resume(engine: ProtocolEngine) -> list[WriteItem]:
    engine.notify_transport_closed()
    engine.take_effects()
    engine.begin_connect()
    _feed_connack(engine, session_present=True)
    return _take_send_items(engine)


@pytest.mark.parametrize("qos", [QoS.AT_LEAST_ONCE, QoS.EXACTLY_ONCE])
def test_contiguous_frame_is_not_retained_and_replay_reencodes_dup(qos: QoS) -> None:
    engine = ProtocolEngine(EngineConfig(client_id="frame-small", clean_start=False))
    engine.begin_connect()
    _feed_connack(engine, session_present=False)
    engine.take_effects()

    payload = b"z" * 4096
    handle = engine.queue_publish("frame/small", payload, qos=qos)
    mid = handle.mid
    assert mid is not None
    first = _take_send_items(engine)
    assert len(first) == 1
    assert isinstance(first[0], bytes)

    stored = engine.store.get_out(mid)
    assert stored is not None
    assert stored.encoded_publish is None
    expected_state = (
        OutboundQoSState.WAIT_PUBACK if qos is QoS.AT_LEAST_ONCE else OutboundQoSState.WAIT_PUBREC
    )
    assert stored.state is expected_state

    replay = _resume(engine)
    assert len(replay) == 1
    assert isinstance(replay[0], bytes)
    decoder = IncrementalDecoder()
    decoder.feed(_as_bytes(replay[0]))
    raw = decoder.next_packet()
    assert raw is not None
    packet = PublishPacket.decode(raw.flags, raw.remaining)
    assert packet.dup is True
    assert packet.mid == mid
    assert packet.topic == "frame/small"
    assert packet.payload == payload

    stored = engine.store.get_out(mid)
    assert stored is not None
    assert stored.encoded_publish is None


def test_segmented_frame_retains_shared_payload_and_reuses_dup_header() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="frame-large", clean_start=False))
    engine.begin_connect()
    _feed_connack(engine, session_present=False)
    engine.take_effects()

    payload = bytes(range(256)) * (SEGMENT_THRESHOLD // 256)
    handle = engine.queue_publish("frame/large", payload, qos=QoS.AT_LEAST_ONCE)
    mid = handle.mid
    assert mid is not None
    first_items = _take_send_items(engine)
    assert len(first_items) == 1
    first = first_items[0]
    assert isinstance(first, tuple)
    assert first[1] is payload
    assert first[0][0] & 0x08 == 0

    stored = engine.store.get_out(mid)
    assert stored is not None
    assert stored.encoded_publish is first

    second_items = _resume(engine)
    assert len(second_items) == 1
    second = second_items[0]
    assert isinstance(second, tuple)
    assert second is not first
    assert second[1] is payload
    assert second[0][0] & 0x08
    stored = engine.store.get_out(mid)
    assert stored is not None
    assert stored.encoded_publish is second

    third_items = _resume(engine)
    assert len(third_items) == 1
    third = third_items[0]
    assert third is second

    decoder = IncrementalDecoder(max_packet_size=2 * SEGMENT_THRESHOLD)
    decoder.feed(_as_bytes(third))
    raw = decoder.next_packet()
    assert raw is not None
    packet = PublishPacket.decode(raw.flags, raw.remaining)
    assert packet.dup is True
    assert packet.mid == mid
    assert packet.payload == payload
