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

    handled, _, handoff, captured = client._process_direct_qos0_batch()

    assert handled == 2
    assert handoff is False
    assert [message.topic for message in captured] == ["one", "two"]
    assert client._engine._effects == []


def test_mixed_qos1_batch_materializes_in_original_order() -> None:
    client = _client()
    client._decoder.feed(
        _publish("one") + _publish("two", qos=QoS.AT_LEAST_ONCE, mid=7) + _publish("three")
    )

    handled, _, _, captured = client._process_direct_qos0_batch()

    assert handled == 3
    assert captured == []
    assert [effect.kind for effect in client._engine._effects] == [
        EffectKind.MESSAGE,
        EffectKind.SEND,
        EffectKind.MESSAGE,
        EffectKind.MESSAGE,
    ]
    assert [
        effect.data.topic for effect in client._engine._effects if effect.kind is EffectKind.MESSAGE
    ] == ["one", "two", "three"]


def test_non_publish_control_packet_materializes_prefix_before_effect() -> None:
    client = _client()
    client._decoder.feed(
        _publish("one") + encode_frame(PacketType.DISCONNECT, 0, b"") + _publish("ignored")
    )

    handled, _, _, captured = client._process_direct_qos0_batch()

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

    handled, _, _, captured = client._process_direct_qos0_batch()

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

    handled, _, _, captured = client._process_direct_qos0_batch()

    assert handled == 2
    assert captured == []
    assert [effect.kind for effect in client._engine._effects] == [
        EffectKind.MESSAGE,
        EffectKind.PROTOCOL_ERROR,
    ]


def test_callback_capacity_refusal_falls_back_to_historical_effects() -> None:
    client = _client(max_pending_callbacks=1)
    client._decoder.feed(_publish("one") + _publish("two"))
    handled, _, _, captured = client._process_direct_qos0_batch()
    assert handled == 2

    assert client._delivery.deliver_callback_messages_inline(captured, client.on_message) is False
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
    _, _, _, captured = client._process_direct_qos0_batch()

    assert client._delivery.deliver_callback_messages_inline(captured, None) is True


def test_callback_messages_reject_non_small_message() -> None:
    client = _client()
    client._delivery.small_message_limit = 0
    client._decoder.feed(_publish("one"))
    _, _, _, captured = client._process_direct_qos0_batch()

    assert client._delivery.deliver_callback_messages_inline(captured, client.on_message) is False


def test_direct_batch_honours_ingress_byte_bound() -> None:
    client = _client()
    client._max_ingress_batch_bytes = 7
    client._decoder.feed(_publish("one") + _publish("two"))

    handled, _, _, captured = client._process_direct_qos0_batch()

    assert handled == 1
    assert [message.topic for message in captured] == ["one"]


def test_decoder_header_peek_does_not_consume_data() -> None:
    client = _client()
    wire = _publish("one")
    client._decoder.feed(wire)

    assert client._decoder.next_header_byte == wire[0]
    assert client._decoder.buffered == len(wire)
