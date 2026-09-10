from __future__ import annotations

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import ConnectionState, MQTTProtocolVersion, QoS
from mqttium.packets import PublishPacket


def _publish(topic: str, *, qos: QoS = QoS.AT_MOST_ONCE, mid: int | None = None) -> bytes:
    return PublishPacket(
        topic=topic,
        payload=b"x",
        qos=qos,
        retain=False,
        dup=False,
        mid=mid,
    ).encode(MQTTProtocolVersion.MQTTv311)


def _client(*, max_pending_callbacks: int = 1024) -> AsyncClient:
    client = AsyncClient(
        message_delivery="callback",
        max_pending_callbacks=max_pending_callbacks,
    )
    client._engine.state = ConnectionState.CONNECTED
    client.on_message = lambda _message: None
    return client


def test_record_inline_batch_uses_logical_message_count() -> None:
    client = _client()

    client._effect_pump.record_inline_batch(3)

    stats = client._effect_pump.stats()
    assert stats.batches == 1
    assert stats.multi_effect_batches == 1
    assert stats.enqueued == 3
    assert stats.applied == 3
    assert stats.inline_effects == 3


@pytest.mark.asyncio
async def test_record_inline_batch_unblocks_existing_drain_target() -> None:
    client = _client()
    client._effect_pump.enqueued = 2
    waiter = asyncio.create_task(client._effect_pump.drain())
    await asyncio.sleep(0)
    assert not waiter.done()

    client._effect_pump.record_inline_batch(2)

    await waiter


def test_decoder_header_peek_does_not_consume_data() -> None:
    client = _client()
    wire = _publish("one")
    client._decoder.feed(wire)

    assert client._decoder.next_header_byte == wire[0]
    assert client._decoder.buffered == len(wire)


def _publish_v5(topic: str, *, payload: bytes = b"x", properties=None) -> bytes:
    return PublishPacket(
        topic=topic,
        payload=payload,
        qos=QoS.AT_MOST_ONCE,
        retain=False,
        dup=False,
        properties=properties,
    ).encode(MQTTProtocolVersion.MQTTv5)


def _client_v5(*, max_pending_callbacks: int = 1024, topic_alias_maximum: int = 0) -> AsyncClient:
    client = AsyncClient(
        protocol=MQTTProtocolVersion.MQTTv5,
        message_delivery="callback",
        max_pending_callbacks=max_pending_callbacks,
        topic_alias_maximum=topic_alias_maximum,
    )
    client._engine.state = ConnectionState.CONNECTED
    client.on_message = lambda _message: None
    return client


def test_peek_packet_bounds_waits_for_fragment_and_does_not_consume() -> None:
    client = _client()
    wire = _publish("fragmented")
    split = len(wire) // 2
    client._decoder.feed(wire[:split])
    assert client._decoder.peek_packet_bounds() is None
    assert client._decoder.buffered == split

    client._decoder.feed(wire[split:])
    bounds = client._decoder.peek_packet_bounds()
    assert bounds is not None
    assert client._decoder.buffered == len(wire)
    _, body_start, body_end = bounds
    assert body_end > body_start


def test_peek_packet_bounds_enforces_packet_limit_like_next_packet() -> None:
    from mqttium.errors import PacketTooLargeError

    client = _client()
    wire = _publish("one")
    client._decoder.max_packet_size = len(wire) - 1
    client._decoder.feed(wire)

    with pytest.raises(PacketTooLargeError):
        client._decoder.peek_packet_bounds()


@pytest.mark.parametrize("remaining_length", [0, 1, 127, 128, 16_383, 16_384, 2_097_152])
def test_peek_packet_bounds_matches_next_packet_vbi_boundaries(remaining_length: int) -> None:
    from mqttium.codec.buffer import IncrementalDecoder
    from mqttium.codec.vbi import encode_vbi

    wire = b"\x30" + encode_vbi(remaining_length) + (b"x" * remaining_length)
    peek_decoder = IncrementalDecoder(max_packet_size=len(wire) + 1)
    ordinary_decoder = IncrementalDecoder(max_packet_size=len(wire) + 1)
    peek_decoder.feed(wire)
    ordinary_decoder.feed(wire)

    bounds = peek_decoder.peek_packet_bounds()
    packet = ordinary_decoder.next_packet()

    assert bounds is not None
    header, body_start, body_end = bounds
    assert header == 0x30
    assert body_end - body_start == remaining_length
    assert body_end == len(wire)
    assert packet is not None
    assert len(packet.remaining) == remaining_length
    assert peek_decoder.buffered == len(wire)


@pytest.mark.parametrize(
    "partial",
    [
        b"\x30\x80",
        b"\x30\x80\x80",
        b"\x30\x80\x80\x80",
    ],
)
def test_peek_packet_bounds_incomplete_vbi_matches_next_packet(partial: bytes) -> None:
    from mqttium.codec.buffer import IncrementalDecoder

    peek_decoder = IncrementalDecoder()
    ordinary_decoder = IncrementalDecoder()
    peek_decoder.feed(partial)
    ordinary_decoder.feed(partial)

    assert peek_decoder.peek_packet_bounds() is None
    assert ordinary_decoder.next_packet() is None


@pytest.mark.parametrize(
    "wire",
    [
        b"\x30\x80\x00",  # non-canonical zero
        b"\x30\x81\x00x",  # non-canonical one
        b"\x30\x80\x81\x00" + (b"x" * 128),  # non-canonical 128
        b"\x30\xff\xff\xff\xff",  # max header bytes, still incomplete
        b"\x30\xff\xff\xff\xff\x00",  # fifth VBI byte
    ],
)
def test_peek_packet_bounds_malformed_vbi_matches_next_packet(wire: bytes) -> None:
    from mqttium.codec.buffer import IncrementalDecoder
    from mqttium.errors import MalformedPacketError

    peek_decoder = IncrementalDecoder(max_packet_size=max(len(wire) + 1, 2))
    ordinary_decoder = IncrementalDecoder(max_packet_size=max(len(wire) + 1, 2))
    peek_decoder.feed(wire)
    ordinary_decoder.feed(wire)

    with pytest.raises(MalformedPacketError) as peek_error:
        peek_decoder.peek_packet_bounds()
    with pytest.raises(MalformedPacketError) as ordinary_error:
        ordinary_decoder.next_packet()
    assert str(peek_error.value) == str(ordinary_error.value)
