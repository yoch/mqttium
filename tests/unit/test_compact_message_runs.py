from __future__ import annotations

import pytest

from mqttium.api import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder, RawPacket
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import MalformedPacketError
from mqttium.packets import PublishPacket
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.protocol.engine import EngineConfig, ProtocolEngine


def _raw(topic: str = "run/topic", payload: bytes = b"x") -> RawPacket:
    decoder = IncrementalDecoder()
    decoder.feed(
        PublishPacket(
            topic=topic,
            payload=payload,
            qos=QoS.AT_MOST_ONCE,
            retain=False,
            dup=False,
        ).encode(MQTTProtocolVersion.MQTTv311)
    )
    return decoder.drain_packets()[0]


def _connected_engine() -> ProtocolEngine:
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv311))
    engine.state = ConnectionState.CONNECTED
    return engine


def _runtime_callback_client(*, max_pending_callbacks: int = 1024) -> AsyncClient:
    client = AsyncClient(
        message_delivery="callback",
        max_pending_callbacks=max_pending_callbacks,
    )
    client.on_message = lambda _message: None
    client._engine._enable_runtime_message_runs()
    client._engine.state = ConnectionState.CONNECTED
    return client


def test_public_engine_keeps_individual_message_effects() -> None:
    engine = _connected_engine()
    engine.handle_raw(_raw("one"))
    engine.handle_raw(_raw("two"))

    effects = engine.take_effects()

    assert len(effects) == 2
    assert all(type(effect) is EngineEffect for effect in effects)
    assert [effect.kind for effect in effects] == [EffectKind.MESSAGE, EffectKind.MESSAGE]
    assert [effect.data.topic for effect in effects] == ["one", "two"]


def test_callback_client_keeps_standard_engine_path_before_connection() -> None:
    client = AsyncClient(message_delivery="callback")
    client._engine.state = ConnectionState.CONNECTED
    client._engine.handle_raw(_raw("one"))
    client._engine.handle_raw(_raw("two"))

    effects = client._engine.take_effects()

    assert [effect.kind for effect in effects] == [EffectKind.MESSAGE, EffectKind.MESSAGE]


def test_runtime_promotes_only_after_second_adjacent_qos0() -> None:
    engine = _connected_engine()
    engine._enable_runtime_message_runs()

    engine.handle_raw(_raw("one"))
    assert len(engine._effects) == 1
    assert type(engine._effects[0]) is EngineEffect

    engine.handle_raw(_raw("two"))
    assert len(engine._effects) == 1
    run = engine._effects[0]
    assert run.kind is EffectKind.MESSAGE_RUN
    assert [message.topic for message in run.data] == ["one", "two"]

    engine.handle_raw(_raw("three"))
    assert engine._effects[0] is run
    assert [message.topic for message in run.data] == ["one", "two", "three"]


def test_collect_keeps_pure_run_compact_until_outside_lock() -> None:
    client = _runtime_callback_client()
    client._delivery.ensure_callback_worker = lambda: None  # type: ignore[method-assign]
    client._engine.handle_raw(_raw("one"))
    client._engine.handle_raw(_raw("two"))

    client._collect_effects_locked()

    assert len(client._pending_effects) == 1
    assert client._pending_effects[0].kind is EffectKind.MESSAGE_RUN
    assert client._callback_queue.empty()
    stats = client.stats().effects
    assert stats.pending == 1
    assert stats.enqueued == 2

    client._drain_effects_inline()

    assert not client._pending_effects
    job = client._callback_queue.get_nowait()
    assert [message.topic for message in job[1][0]] == ["one", "two"]
    client._callback_queue.task_done()
    client._delivery._release_callback_batch(2)
    assert client.stats().effects.pending == 0


def test_run_backpressure_falls_back_once_to_historical_effects() -> None:
    client = _runtime_callback_client(max_pending_callbacks=1)
    client._delivery.ensure_callback_worker = lambda: None  # type: ignore[method-assign]
    client._engine.handle_raw(_raw("one"))
    client._engine.handle_raw(_raw("two"))
    client._collect_effects_locked()
    client._effect_pump.schedule = lambda: None  # type: ignore[method-assign]

    client._drain_effects_inline()

    first_job = client._callback_queue.get_nowait()
    assert first_job[1][0].topic == "one"
    client._callback_queue.task_done()
    assert len(client._pending_effects) == 1
    remaining = client._pending_effects[0]
    assert type(remaining) is EngineEffect
    assert remaining.kind is EffectKind.MESSAGE
    assert remaining.data.topic == "two"
    stats = client.stats().effects
    assert stats.pending == 1
    assert stats.enqueued == 2
    assert stats.applied == 1


def test_mixed_run_and_send_materializes_before_effect_pump() -> None:
    client = AsyncClient(message_delivery="callback")
    client.on_message = lambda _message: None
    client._delivery.ensure_callback_worker = lambda: None  # type: ignore[method-assign]
    client._engine.state = ConnectionState.CONNECTED
    wire = b"".join(
        (
            PublishPacket(
                topic="one", payload=b"x", qos=QoS.AT_MOST_ONCE, retain=False, dup=False
            ).encode(MQTTProtocolVersion.MQTTv311),
            PublishPacket(
                topic="two", payload=b"x", qos=QoS.AT_MOST_ONCE, retain=False, dup=False
            ).encode(MQTTProtocolVersion.MQTTv311),
            PublishPacket(
                topic="three",
                payload=b"x",
                qos=QoS.AT_LEAST_ONCE,
                retain=False,
                dup=False,
                mid=7,
            ).encode(MQTTProtocolVersion.MQTTv311),
        )
    )
    client._decoder.feed(wire)

    handled, _decoded_bytes, handoff_required = client._process_ingress_batch()
    client._collect_effects_locked()

    assert handled == 3
    assert not handoff_required
    assert [effect.kind for effect in client._pending_effects] == [
        EffectKind.SEND,
        EffectKind.MESSAGE,
        EffectKind.MESSAGE,
        EffectKind.MESSAGE,
    ]
    assert [effect.data.topic for effect in list(client._pending_effects)[1:]] == [
        "one",
        "two",
        "three",
    ]
    assert client._effect_pump.reordered_batches == 1
    stats = client.stats().effects
    assert stats.pending == 4
    assert stats.enqueued == 4


def test_stale_compact_run_is_discarded_as_logical_effects() -> None:
    client = _runtime_callback_client()
    client._delivery.ensure_callback_worker = lambda: None  # type: ignore[method-assign]
    client._engine.handle_raw(_raw("one"))
    client._engine.handle_raw(_raw("two"))
    client._collect_effects_locked()
    client._effect_pump.pending_epoch = client._connection_epoch - 1

    client._drain_effects_inline()

    assert not client._pending_effects
    assert client._callback_queue.empty()
    assert client._effect_pump.enqueued == client._effect_pump.applied == 2


async def test_drain_waits_for_all_logical_messages_after_run_fallback() -> None:
    import asyncio

    client = _runtime_callback_client(max_pending_callbacks=1)
    client._delivery.ensure_callback_worker = lambda: None  # type: ignore[method-assign]
    client._engine.handle_raw(_raw("one"))
    client._engine.handle_raw(_raw("two"))
    client._collect_effects_locked()

    drain = asyncio.create_task(client._drain_effects())
    await asyncio.sleep(0)

    assert not drain.done()
    first_job = client._callback_queue.get_nowait()
    assert first_job[1][0].topic == "one"
    client._callback_queue.task_done()
    await asyncio.sleep(0)

    assert await asyncio.wait_for(drain, 1.0) is None
    second_job = client._callback_queue.get_nowait()
    assert second_job[1][0].topic == "two"
    client._callback_queue.task_done()
    stats = client.stats().effects
    assert stats.enqueued == stats.applied == 2


def test_single_packet_ingress_batch_stays_on_historical_message_path() -> None:
    client = AsyncClient(message_delivery="callback")
    client.on_message = lambda _message: None
    client._engine.state = ConnectionState.CONNECTED
    standard_handler = client._engine.inbound.handle_publish
    client._decoder.feed(
        PublishPacket(
            topic="one",
            payload=b"x",
            qos=QoS.AT_MOST_ONCE,
            retain=False,
            dup=False,
        ).encode(MQTTProtocolVersion.MQTTv311)
    )

    handled, _decoded_bytes, handoff_required = client._process_ingress_batch()

    assert handled == 1
    assert not handoff_required
    assert (
        client._engine._handlers_by_state[ConnectionState.CONNECTED][PacketType.PUBLISH]
        == standard_handler
    )
    assert len(client._engine._effects) == 1
    effect = client._engine._effects[0]
    assert effect.kind is EffectKind.MESSAGE
    assert effect.data.topic == "one"
    assert not client._engine._runtime_has_message_run


def test_two_packet_ingress_batch_stays_on_historical_path() -> None:
    client = AsyncClient(message_delivery="callback")
    client.on_message = lambda _message: None
    client._engine.state = ConnectionState.CONNECTED
    standard_handler = client._engine.inbound.handle_publish
    client._decoder.feed(
        b"".join(
            PublishPacket(
                topic=topic,
                payload=b"x",
                qos=QoS.AT_MOST_ONCE,
                retain=False,
                dup=False,
            ).encode(MQTTProtocolVersion.MQTTv311)
            for topic in ("one", "two")
        )
    )

    handled, _decoded_bytes, handoff_required = client._process_ingress_batch()

    assert handled == 2
    assert not handoff_required
    assert (
        client._engine._handlers_by_state[ConnectionState.CONNECTED][PacketType.PUBLISH]
        == standard_handler
    )
    assert len(client._engine._effects) == 2
    assert [effect.kind for effect in client._engine._effects] == [
        EffectKind.MESSAGE,
        EffectKind.MESSAGE,
    ]
    assert [effect.data.topic for effect in client._engine._effects] == ["one", "two"]
    assert not client._engine._runtime_has_message_run


def test_buffered_ingress_batch_promotes_then_restores_handler() -> None:
    client = AsyncClient(message_delivery="callback")
    client.on_message = lambda _message: None
    client._engine.state = ConnectionState.CONNECTED
    standard_handler = client._engine.inbound.handle_publish
    topics = tuple(f"topic-{index}" for index in range(12))
    client._decoder.feed(
        b"".join(
            PublishPacket(
                topic=topic,
                payload=b"x" * 64,
                qos=QoS.AT_MOST_ONCE,
                retain=False,
                dup=False,
            ).encode(MQTTProtocolVersion.MQTTv311)
            for topic in topics
        )
    )

    handled, _decoded_bytes, handoff_required = client._process_compact_ingress_batch()

    assert handled == len(topics)
    assert not handoff_required
    assert (
        client._engine._handlers_by_state[ConnectionState.CONNECTED][PacketType.PUBLISH]
        == standard_handler
    )
    assert len(client._engine._effects) == 1
    run = client._engine._effects[0]
    assert run.kind is EffectKind.MESSAGE_RUN
    assert [message.topic for message in run.data] == list(topics)
    assert not client._engine._runtime_has_message_run


def _raw_qos(topic: str, qos: QoS, *, mid: int) -> RawPacket:
    decoder = IncrementalDecoder()
    decoder.feed(
        PublishPacket(
            topic=topic,
            payload=b"x",
            qos=qos,
            retain=False,
            dup=False,
            mid=mid,
        ).encode(MQTTProtocolVersion.MQTTv311)
    )
    return decoder.drain_packets()[0]


def test_runtime_handler_preserves_qos1_and_qos2_paths() -> None:
    for qos in (QoS.AT_LEAST_ONCE, QoS.EXACTLY_ONCE):
        engine = _connected_engine()
        engine._enable_runtime_message_runs()

        engine.handle_raw(_raw_qos(f"qos-{int(qos)}", qos, mid=7))

        effects = engine.take_effects()
        assert [effect.kind for effect in effects] == [EffectKind.SEND, EffectKind.MESSAGE]
        assert effects[1].data.qos is qos


def test_runtime_handler_rejects_qos3() -> None:
    decoder = IncrementalDecoder()
    wire = bytearray(
        PublishPacket(
            topic="bad-qos",
            payload=b"x",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            dup=False,
            mid=7,
        ).encode(MQTTProtocolVersion.MQTTv311)
    )
    wire[0] = (wire[0] & 0xF0) | 0x06
    decoder.feed(bytes(wire))
    raw = decoder.drain_packets()[0]
    engine = _connected_engine()

    with pytest.raises(MalformedPacketError, match="QoS 3"):
        engine.inbound._on_publish_v311_runtime(raw)


def test_compact_mixed_batch_materializes_run_before_effect_pump() -> None:
    client = AsyncClient(message_delivery="callback")
    client.on_message = lambda _message: None
    client._engine.state = ConnectionState.CONNECTED
    client._decoder.feed(
        b"".join(
            (
                PublishPacket(
                    topic="one",
                    payload=b"x",
                    qos=QoS.AT_MOST_ONCE,
                    retain=False,
                    dup=False,
                ).encode(MQTTProtocolVersion.MQTTv311),
                PublishPacket(
                    topic="two",
                    payload=b"x",
                    qos=QoS.AT_MOST_ONCE,
                    retain=False,
                    dup=False,
                ).encode(MQTTProtocolVersion.MQTTv311),
                PublishPacket(
                    topic="three",
                    payload=b"x",
                    qos=QoS.AT_LEAST_ONCE,
                    retain=False,
                    dup=False,
                    mid=9,
                ).encode(MQTTProtocolVersion.MQTTv311),
            )
        )
    )

    handled, _decoded_bytes, handoff_required = client._process_compact_ingress_batch()

    assert handled == 3
    assert not handoff_required
    assert not client._engine._runtime_has_message_run
    assert [effect.kind for effect in client._engine._effects] == [
        EffectKind.MESSAGE,
        EffectKind.MESSAGE,
        EffectKind.SEND,
        EffectKind.MESSAGE,
    ]
    assert [
        effect.data.topic
        for effect in client._engine._effects
        if effect.kind is EffectKind.MESSAGE
    ] == ["one", "two", "three"]


def test_callback_run_without_callback_is_accepted_without_queueing() -> None:
    client = AsyncClient(message_delivery="callback")
    engine = _connected_engine()
    engine.handle_raw(_raw("one"))
    decoded = engine.take_effects()[0].data

    assert client._delivery.deliver_callback_run_inline([decoded], None)
    assert client._callback_queue.empty()


def test_callback_run_rejects_message_outside_small_budget() -> None:
    client = AsyncClient(message_delivery="callback")
    engine = _connected_engine()
    engine.handle_raw(_raw("large", payload=b"x" * (1024 * 1024)))
    decoded = engine.take_effects()[0].data

    assert not client._delivery.deliver_callback_run_inline([decoded], lambda _message: None)
    assert client._callback_queue.empty()


async def test_scheduled_flush_expands_message_run_before_delivery() -> None:
    client = _runtime_callback_client()
    client._delivery.ensure_callback_worker = lambda: None  # type: ignore[method-assign]
    client._engine.handle_raw(_raw("one"))
    client._engine.handle_raw(_raw("two"))
    client._collect_effects_locked()

    await client._effect_pump._run_scheduled()

    assert not client._pending_effects
    job = client._callback_queue.get_nowait()
    assert [message.topic for message in job[1][0]] == ["one", "two"]
    client._callback_queue.task_done()
    client._delivery._release_callback_batch(2)


def test_stale_message_run_inline_is_a_noop() -> None:
    client = _runtime_callback_client()
    engine = _connected_engine()
    engine.handle_raw(_raw("one"))
    message = engine.take_effects()[0].data

    assert client._apply_message_run_inline([message], client._connection_epoch - 1)
    assert client._callback_queue.empty()


async def test_message_run_cannot_escape_effect_pump() -> None:
    client = _runtime_callback_client()
    engine = _connected_engine()
    engine.handle_raw(_raw("one"))
    message = engine.take_effects()[0].data
    effect = EngineEffect(EffectKind.MESSAGE_RUN, [message], False, None)

    with pytest.raises(AssertionError, match="escaped EffectPump"):
        await client._apply_effect(effect, nowait=False, epoch=client._connection_epoch)
