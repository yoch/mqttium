from __future__ import annotations

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PublishPacket, encode_frame
from mqttium.protocol.effects import EffectKind, EngineEffect


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


def test_pure_qos0_batch_stays_captured() -> None:
    client = _client()
    client._decoder.feed(_publish("one") + _publish("two"))

    handled, _, handoff, captured, sizes = client._process_direct_qos0_batch()

    assert handled == 2
    assert handoff is False
    assert [message.topic for message in captured] == ["one", "two"]
    assert client._engine._effects == []
    assert sizes is None


def test_mixed_qos1_batch_materializes_in_original_order() -> None:
    client = _client()
    client._decoder.feed(
        _publish("one") + _publish("two", qos=QoS.AT_LEAST_ONCE, mid=7) + _publish("three")
    )

    handled, _, _, captured, _ = client._process_direct_qos0_batch()

    assert handled == 3
    assert captured == []
    assert [effect.kind for effect in client._engine._effects] == [
        EffectKind.MESSAGE,
        EffectKind.SEND_ACK,
        EffectKind.MESSAGE,
        EffectKind.MESSAGE,
    ]
    assert [
        effect.data.topic for effect in client._engine._effects if effect.kind is EffectKind.MESSAGE
    ] == ["one", "two", "three"]


def test_non_publish_control_packet_materializes_prefix_before_effect() -> None:
    # Server DISCONNECT is valid only in MQTT 5. Keep this fast-path ordering
    # regression on a protocol where the packet is legal instead of relying on
    # the pre-MQTT-5 behavior this PR deliberately rejects.
    client = _client_v5()
    client._decoder.feed(
        _publish_v5("one") + encode_frame(PacketType.DISCONNECT, 0, b"") + _publish_v5("ignored")
    )

    handled, _, _, captured, _ = client._process_direct_qos0_batch()

    assert handled == 3
    assert captured == []
    assert [effect.kind for effect in client._engine._effects] == [
        EffectKind.MESSAGE,
        EffectKind.DISCONNECTED,
    ]
    assert client._engine._effects[0].data.topic == "one"


def test_invalid_qos0_packet_uses_engine_error_translation() -> None:
    client = _client()
    client._decoder.feed(_publish("one"))
    # PUBLISH QoS 0 with an incomplete UTF-8 topic length field.
    client._decoder._buf.extend(b"\x30\x01\x00")

    handled, _, _, captured, _ = client._process_direct_qos0_batch()

    assert handled == 2
    assert captured == []
    assert [effect.kind for effect in client._engine._effects] == [
        EffectKind.MESSAGE,
        EffectKind.PROTOCOL_ERROR,
    ]
    assert client._engine._effects[0].data.topic == "one"


def test_qos3_publish_uses_historical_protocol_error_path() -> None:
    client = _client()
    client._decoder.feed(_publish("one"))
    # Raw PUBLISH with QoS bits 0b11.
    client._decoder.feed(b"\x36\x03\x00\x01x")

    handled, _, _, captured, _ = client._process_direct_qos0_batch()

    assert handled == 2
    assert captured == []
    assert [effect.kind for effect in client._engine._effects] == [
        EffectKind.MESSAGE,
        EffectKind.PROTOCOL_ERROR,
    ]


def test_callback_capacity_refusal_falls_back_to_historical_effects() -> None:
    client = _client(max_pending_callbacks=1)
    client._decoder.feed(_publish("one") + _publish("two"))
    handled, _, _, captured, sizes = client._process_direct_qos0_batch()
    assert handled == 2

    assert (
        client._delivery.deliver_callback_messages_inline(captured, client.on_message, sizes)
        is False
    )
    client._engine._effects.extend(
        EngineEffect(EffectKind.MESSAGE, message, False, None) for message in captured
    )
    client._collect_effects_locked()

    assert len(client._effect_pump.pending) == 2
    assert client._effect_pump.enqueued == 2


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


def test_callback_messages_without_callback_are_consumed_inline() -> None:
    client = _client()
    client._decoder.feed(_publish("one"))
    _, _, _, captured, _ = client._process_direct_qos0_batch()

    assert client._delivery.deliver_callback_messages_inline(captured, None) is True


def test_single_callback_message_runs_inline_when_delivery_is_idle() -> None:
    client = _client()
    seen: list[str] = []
    client.on_message = lambda message: seen.append(message.topic)
    client._decoder.feed(_publish("one"))
    _, _, _, captured, sizes = client._process_direct_qos0_batch()

    assert client._delivery.deliver_callback_messages_inline(captured, client.on_message, sizes)
    assert seen == ["one"]
    assert client._callback_worker_task is None


def test_callback_messages_reject_non_small_message() -> None:
    client = _client()
    client._delivery.small_message_limit = 0
    client._decoder.feed(_publish("one"))
    _, _, _, captured, _ = client._process_direct_qos0_batch()

    assert client._delivery.deliver_callback_messages_inline(captured, client.on_message) is False


def test_direct_batch_honours_ingress_byte_bound() -> None:
    client = _client()
    client._max_ingress_batch_bytes = 7
    client._decoder.feed(_publish("one") + _publish("two"))

    handled, _, _, captured, _ = client._process_direct_qos0_batch()

    assert handled == 1
    assert [message.topic for message in captured] == ["one"]


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


def test_v5_qos0_without_properties_stays_captured() -> None:
    client = _client_v5()
    client._decoder.feed(_publish_v5("one") + _publish_v5("two"))

    handled, _, handoff, captured, sizes = client._process_direct_qos0_batch()

    assert handled == 2
    assert handoff is False
    assert [message.topic for message in captured] == ["one", "two"]
    assert [message.payload for message in captured] == [b"x", b"x"]
    assert sizes == [None, None]
    assert client._engine._effects == []


@pytest.mark.asyncio
async def test_v5_qos0_properties_preserve_decoded_size_for_delivery() -> None:
    from mqttium.types import Properties

    props = Properties()
    props.set("content_type", "application/json")
    props.set("user_property", [("schema", "telemetry.v1")])
    client = _client_v5()
    client._decoder.feed(_publish_v5("one", payload=b"payload", properties=props))

    handled, _, _, captured, sizes = client._process_direct_qos0_batch()

    assert handled == 1
    assert captured[0].properties == props
    assert sizes is not None and sizes[0] is not None
    assert client._delivery.deliver_callback_messages_inline(captured, client.on_message, sizes)
    await client._delivery.callback_queue.join()
    await client._delivery.shutdown_callbacks(drain=False)


def test_v5_topic_alias_replays_historical_stateful_path() -> None:
    from mqttium.types import Properties

    props = Properties()
    props.set("topic_alias", 1)
    client = _client_v5(topic_alias_maximum=4)
    client._decoder.feed(_publish_v5("aliased/topic", properties=props))

    handled, _, _, captured, sizes = client._process_direct_qos0_batch()

    assert handled == 1
    assert captured == []
    assert sizes == []
    effects = client._engine.take_effects()
    assert [effect.kind for effect in effects] == [EffectKind.DECODED_MESSAGE]
    assert effects[0].data.topic == "aliased/topic"


def test_v5_empty_topic_replays_historical_alias_path() -> None:
    from mqttium.types import Properties

    props = Properties()
    props.set("topic_alias", 1)
    client = _client_v5(topic_alias_maximum=4)
    client._decoder.feed(_publish_v5("seed", properties=props))
    client._engine.handle_raw(client._decoder.next_packet())
    client._engine.take_effects()

    client._decoder.feed(_publish_v5("", properties=props))
    handled, _, _, captured, sizes = client._process_direct_qos0_batch()

    assert handled == 1
    assert captured == []
    assert sizes == []
    effects = client._engine.take_effects()
    assert effects[0].data.topic == "seed"


def test_borrowed_payload_is_owned_after_decoder_reuse() -> None:
    client = _client_v5()
    payload = b"A" * 512
    client._decoder.feed(_publish_v5("one", payload=payload))
    _, _, _, captured, _ = client._process_direct_qos0_batch()
    message = captured[0]

    client._decoder.feed(_publish_v5("two", payload=b"B" * 512))
    client._process_direct_qos0_batch()

    assert message.payload == payload
    assert isinstance(message.payload, bytes)


def test_borrowed_unicode_topic_matches_historical_decoder() -> None:
    client = _client()
    client._decoder.feed(_publish("café/東京"))

    _, _, _, captured, _ = client._process_direct_qos0_batch()

    assert captured[0].topic == "café/東京"


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


def test_framing_error_materializes_captured_prefix_before_raising() -> None:
    from mqttium.errors import MalformedPacketError

    client = _client()
    client._decoder.feed(_publish("one") + b"\x30\x80\x00")

    with pytest.raises(MalformedPacketError):
        client._process_direct_qos0_batch()

    effects = client._engine.take_effects()
    assert [effect.kind for effect in effects] == [EffectKind.MESSAGE]
    assert effects[0].data.topic == "one"


def test_packet_too_large_materializes_captured_prefix_before_raising() -> None:
    from mqttium.errors import PacketTooLargeError

    client = _client()
    first = _publish("one")
    client._decoder.max_packet_size = len(first) + 1
    oversized = b"\x30\x20" + (b"x" * 32)
    client._decoder.feed(first + oversized)

    with pytest.raises(PacketTooLargeError):
        client._process_direct_qos0_batch()

    effects = client._engine.take_effects()
    assert [effect.kind for effect in effects] == [EffectKind.MESSAGE]
    assert effects[0].data.topic == "one"


def test_borrowed_null_topic_uses_historical_protocol_error() -> None:
    client = _client()
    # Topic is one NUL byte, a well-formed UTF-8 sequence forbidden by MQTT.
    client._decoder.feed(b"\x30\x03\x00\x01\x00")

    handled, _, _, captured, _ = client._process_direct_qos0_batch()

    assert handled == 1
    assert captured == []
    effects = client._engine.take_effects()
    assert [effect.kind for effect in effects] == [EffectKind.PROTOCOL_ERROR]
    assert "Null in UTF-8 data" in str(effects[0].data)


def test_borrowed_v5_malformed_properties_use_historical_error_path() -> None:
    client = _client_v5()
    # topic 'a', then a property length of two with only one byte available.
    client._decoder.feed(b"\x30\x05\x00\x01a\x02\x01")

    handled, _, _, captured, sizes = client._process_direct_qos0_batch()

    assert handled == 1
    assert captured == []
    assert sizes == []
    effects = client._engine.take_effects()
    assert [effect.kind for effect in effects] == [EffectKind.PROTOCOL_ERROR]


@pytest.mark.asyncio
async def test_auto_mode_with_callback_uses_direct_capture_without_losing_delivery() -> None:
    class _OneReadTransport:
        def __init__(self, wire: bytes) -> None:
            self.wire = wire
            self.closing = False

        async def read(self, _n: int = 65536) -> bytes:
            if self.wire:
                wire, self.wire = self.wire, b""
                return wire
            self.closing = True
            return b""

        def is_closing(self) -> bool:
            return self.closing

        async def close(self) -> None:
            self.closing = True

    client = AsyncClient(message_delivery="auto", max_pending_callbacks=8)
    client._engine.state = ConnectionState.CONNECTED
    seen: list[str] = []
    client.on_message = lambda message: seen.append(message.topic)
    client._transport = _OneReadTransport(_publish("compat/one") + _publish("compat/two"))

    await client._read_loop()
    await client._delivery.callback_queue.join()

    assert seen == ["compat/one", "compat/two"]
    assert client._effect_pump.stats().inline_effects == 2
    await client._delivery.shutdown_callbacks(drain=False)


@pytest.mark.asyncio
async def test_auto_mode_without_callback_keeps_iterator_delivery() -> None:
    client = AsyncClient(message_delivery="auto")
    message = PublishPacket(
        topic="iterator/one",
        payload=b"x",
        qos=QoS.AT_MOST_ONCE,
        retain=False,
        dup=False,
    )
    client._engine.state = ConnectionState.CONNECTED
    client._decoder.feed(message.encode(MQTTProtocolVersion.MQTTv311))

    # The read-loop gate must not select direct callback capture in this shape;
    # the historical engine path feeds the auto-mode iterator instead.
    handled, _, _ = client._process_ingress_batch()
    assert handled == 1
    client._collect_effects_locked()
    if client._effect_pump.pending:
        await client._drain_effects()

    delivered = await anext(client.messages())
    assert delivered.topic == "iterator/one"
