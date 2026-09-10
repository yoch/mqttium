"""Loop-bound native publish admission and adapter-boundary contracts."""

from __future__ import annotations

import asyncio

import pytest

import mqttium.packets._publish as publish_v5_module
from mqttium.api import AsyncClient
from mqttium.api.models import PublishMessage
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import FlowControlError, ProtocolError
from mqttium.packets import PublishPacket
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.protocol.negotiated import NegotiatedSettings
from mqttium.types import Properties


def test_publish_nowait_requires_a_running_loop() -> None:
    client = AsyncClient()
    with pytest.raises(RuntimeError, match="event-loop thread"):
        client.publish_nowait("native/off-loop", b"x")


async def test_publish_nowait_rejects_a_different_running_loop() -> None:
    client = AsyncClient(max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED
    client.publish_nowait("native/owner", b"x", qos=0)

    errors: list[BaseException] = []

    def run_other_loop() -> None:
        async def attempt() -> None:
            try:
                client.publish_nowait("native/other-loop", b"x", qos=0)
            except BaseException as exc:
                errors.append(exc)

        asyncio.run(attempt())

    await asyncio.to_thread(run_other_loop)
    assert len(errors) == 1
    assert isinstance(errors[0], RuntimeError)
    assert "different event loop" in str(errors[0])


async def test_publish_nowait_registers_qos1_receipt() -> None:
    client = AsyncClient()
    client._engine.state = ConnectionState.CONNECTED
    receipt = client.publish_nowait("native/qos1", b"x", qos=1)
    assert receipt.qos is QoS.AT_LEAST_ONCE
    assert receipt.mid is not None
    assert client._receipts[receipt.mid] is receipt


async def test_publish_nowait_callback_uses_direct_writer_admission() -> None:
    client = AsyncClient(max_outbound_messages=512)
    client._engine.state = ConnectionState.CONNECTED
    seen: list[int | None] = []
    client.on_publish = lambda mid, _error: seen.append(mid)

    for _ in range(100):
        client.publish_nowait("native/qos0", b"x", qos=0)

    assert client.stats().writer.queued_messages == 100
    assert seen == []
    await client._delivery.callback_queue.join()
    assert seen == [None] * 100
    await client._delivery.shutdown_callbacks(drain=False)


async def test_qos0_callback_marks_writer_admission_not_transport_drain() -> None:
    client = AsyncClient(max_outbound_messages=1, max_outbound_bytes=1024)
    client._engine.state = ConnectionState.CONNECTED
    seen: list[tuple[int | None, BaseException | None]] = []
    client.on_publish = lambda mid, error: seen.append((mid, error))

    receipt = client.publish_nowait("native/qos0-boundary", b"first", qos=0)
    await client._delivery.callback_queue.join()

    assert receipt.is_done()
    assert seen == [(None, None)]
    assert client.stats().writer.queued_messages == 1
    queued_bytes = client.stats().writer.queued_bytes

    with pytest.raises(FlowControlError):
        client.publish_nowait("native/qos0-boundary", b"second", qos=0)

    assert client.stats().writer.queued_messages == 1
    assert client.stats().writer.queued_bytes == queued_bytes
    assert not client._engine.has_pending_effects
    assert not client._effect_pump.pending
    await client._delivery.shutdown_callbacks(drain=False)


def test_disconnect_metadata_boundary_is_private() -> None:
    assert not hasattr(AsyncClient, "last_disconnect")


async def test_await_publish_qos0_uses_engine_admission() -> None:
    client = AsyncClient(max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED

    receipt = await client.publish("native/await-qos0", b"x", qos=0)

    assert receipt.mid is None
    assert not client._engine.has_pending_effects
    assert isinstance(client._write_pump.queue.get_nowait(), bytes)


async def test_direct_qos0_path_commits_outbound_alias_after_writer_admission() -> None:
    client = AsyncClient(protocol=MQTTProtocolVersion.MQTTv5, max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED
    client._engine.negotiated = NegotiatedSettings(topic_alias_maximum=2)
    properties = Properties({"topic_alias": 1})

    await client.publish("canonical/topic", b"seed", properties=properties)
    await client.publish("", b"reuse", properties=properties)

    decoder = IncrementalDecoder()
    decoder.feed(client._write_pump.queue.get_nowait())
    decoder.feed(client._write_pump.queue.get_nowait())
    publishes = [
        PublishPacket.decode(raw.flags, raw.remaining, MQTTProtocolVersion.MQTTv5)
        for raw in decoder.drain_packets()
    ]
    assert [publish.topic for publish in publishes] == ["canonical/topic", ""]


async def test_refused_direct_qos0_write_does_not_establish_alias() -> None:
    client = AsyncClient(
        protocol=MQTTProtocolVersion.MQTTv5,
        max_outbound_messages=1,
        max_outbound_bytes=1024,
    )
    client._engine.state = ConnectionState.CONNECTED
    client._engine.negotiated = NegotiatedSettings(topic_alias_maximum=2)
    client._write_pump.queue.put_nowait(b"occupied")
    client._write_pump.queued_bytes = len(b"occupied")
    client._write_pump._admit_queued()
    properties = Properties({"topic_alias": 1})

    with pytest.raises(FlowControlError):
        client.publish_nowait("canonical/topic", b"seed", properties=properties)

    client._write_pump.discard()
    with pytest.raises(ProtocolError, match="Unknown outbound topic alias"):
        await client.publish("", b"reuse", properties=properties)


async def test_await_publish_qos0_callback_uses_worker_notifications() -> None:
    client = AsyncClient(max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED
    seen: list[tuple[int | None, BaseException | None]] = []
    client.on_publish = lambda mid, error: seen.append((mid, error))

    await client.publish("native/await-qos0", b"x", qos=0)

    assert seen == []
    await client._delivery.callback_queue.join()
    assert seen == [(None, None)]
    await client._delivery.shutdown_callbacks(drain=False)


async def test_publish_many_qos0_uses_engine_admission() -> None:
    client = AsyncClient(max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED

    receipt = await client.publish_many(
        [PublishMessage("native/batch", b"a", 0), PublishMessage("native/batch", b"b", 0)]
    )

    assert receipt.submitted == 2
    assert receipt.completed == 2
    assert not client._engine.has_pending_effects
    assert isinstance(client._write_pump.queue.get_nowait(), bytes)
    assert isinstance(client._write_pump.queue.get_nowait(), bytes)


async def test_publish_many_callback_uses_worker_notifications() -> None:
    client = AsyncClient(max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED
    seen: list[tuple[int | None, BaseException | None]] = []
    client.on_publish = lambda mid, error: seen.append((mid, error))

    receipt = await client.publish_many(
        [PublishMessage("native/batch", b"a", 0), PublishMessage("native/batch", b"b", 0)]
    )

    assert receipt.submitted == 2
    assert receipt.completed == 2
    assert client.stats().writer.queued_messages == 2
    assert seen == []
    await client._delivery.callback_queue.join()
    assert seen == [(None, None), (None, None)]
    await client._delivery.shutdown_callbacks(drain=False)


async def test_publish_many_mixed_qos_keeps_the_effect_path() -> None:
    """One non-QoS-0 request disqualifies the whole batch."""
    client = AsyncClient(max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED

    receipt = await client.publish_many(
        [PublishMessage("native/batch", b"a", 0), PublishMessage("native/batch", b"b", 1)]
    )

    assert receipt.submitted == 2
    assert client._effect_pump.batches > 0


@pytest.mark.parametrize("owner", ["engine", "pump"])
async def test_nowait_refuses_pending_effects_before_mutation(owner) -> None:
    client = AsyncClient()
    client._engine.state = ConnectionState.CONNECTED
    effect = EngineEffect(EffectKind.SEND, b"older")
    if owner == "engine":
        client._engine._emit(effect.kind, effect.data)
    else:
        client._effect_pump.pending.append(effect)
    with pytest.raises(FlowControlError):
        client.publish_nowait("t", b"x", qos=1)
    assert not client._engine.packet_ids
    assert not client._receipts
    assert client._engine.pending_outbound_messages == 0
    assert client._write_pump.queue.empty()


async def test_nowait_refuses_full_callback_queue_before_mutation() -> None:
    client = AsyncClient(max_pending_callbacks=1)
    client._engine.state = ConnectionState.CONNECTED
    client._delivery.callback_queue.put_nowait((lambda: None, (), None))
    client.on_publish = lambda *_: None
    with pytest.raises(FlowControlError):
        client.publish_nowait("t", b"x")
    assert client._write_pump.queue.empty()
    assert not client._engine.has_pending_effects
    assert not client._effect_pump.pending


async def test_direct_path_writer_refusal_does_not_enqueue_a_callback() -> None:
    """Writer admission remains the atomic boundary for callback completion."""
    client = AsyncClient(max_outbound_messages=1, max_pending_callbacks=8)
    client._engine.state = ConnectionState.CONNECTED
    seen: list[tuple[int | None, BaseException | None]] = []
    client.on_publish = lambda mid, error: seen.append((mid, error))

    client.publish_nowait("native/full", b"first", qos=0)
    with pytest.raises(FlowControlError):
        client.publish_nowait("native/full", b"second", qos=0)

    assert client.stats().writer.queued_messages == 1
    assert client._delivery.callback_queue.qsize() == 1
    assert not client._engine.has_pending_effects
    assert not client._effect_pump.pending
    await client._delivery.callback_queue.join()
    assert seen == [(None, None)]
    await client._delivery.shutdown_callbacks(drain=False)


async def test_publish_many_waits_for_callback_capacity_progressively() -> None:
    """Insufficient callback capacity must not split a batch across paths."""
    client = AsyncClient(max_outbound_messages=8, max_pending_callbacks=2)
    client._engine.state = ConnectionState.CONNECTED
    blocker_seen: list[str] = []
    client._delivery.callback_queue.put_nowait((lambda: blocker_seen.append("blocker"), (), None))
    seen: list[tuple[int | None, BaseException | None]] = []
    client.on_publish = lambda mid, error: seen.append((mid, error))

    receipt = await client.publish_many(
        [PublishMessage("native/batch", b"a", 0), PublishMessage("native/batch", b"b", 0)]
    )

    assert client._effect_pump.batches > 0
    await client._effect_pump.drain()
    await client._delivery.callback_queue.join()
    assert receipt.submitted == 2
    assert receipt.completed == 2
    assert blocker_seen == ["blocker"]
    assert seen == [(None, None), (None, None)]
    await client._delivery.shutdown_callbacks(drain=False)


async def test_publish_nowait_direct_path_encodes_mqtt5_properties(monkeypatch) -> None:
    properties = Properties()
    properties = Properties({**properties.values, "content_type": "application/json"})
    properties = Properties(
        {
            **properties.values,
            "user_property": (*properties.get("user_property", ()), ("source", "native-fast-path")),
        }
    )
    client = AsyncClient(protocol=MQTTProtocolVersion.MQTTv5, max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED
    original_encode = publish_v5_module.encode_publish_item_v5
    encode_calls = 0

    def counted_encode(*args, **kwargs):
        nonlocal encode_calls
        encode_calls += 1
        return original_encode(*args, **kwargs)

    monkeypatch.setattr(publish_v5_module, "encode_publish_item_v5", counted_encode)
    client._engine.outbound._encode_publish = publish_v5_module.encode_publish_item_v5

    receipt = client.publish_nowait(
        "native/mqtt5",
        b'{"value": 42}',
        qos=0,
        properties=properties,
    )

    assert receipt.mid is None
    assert encode_calls == 1
    assert not client._engine.has_pending_effects
    item = client._write_pump.queue.get_nowait()
    assert isinstance(item, bytes)
    decoder = IncrementalDecoder()
    decoder.feed(item)
    raw = decoder.next_packet()
    assert raw is not None
    assert raw.packet_type is PacketType.PUBLISH
    packet = PublishPacket.decode(raw.flags, raw.remaining, MQTTProtocolVersion.MQTTv5)
    assert packet.topic == "native/mqtt5"
    assert packet.payload == b'{"value": 42}'
    assert packet.properties == properties


@pytest.mark.parametrize("qos", [3, -1, 99])
async def test_invalid_qos_still_raises_value_error(qos: int) -> None:
    """Comparing before converting must not swallow an invalid level."""
    client = AsyncClient(max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED

    with pytest.raises(ValueError):
        await client.publish("native/invalid", b"x", qos=qos)
    with pytest.raises(ValueError):
        client.publish_nowait("native/invalid", b"x", qos=qos)


async def test_int_and_enum_qos0_both_take_the_direct_path() -> None:
    for level in (0, QoS.AT_MOST_ONCE):
        client = AsyncClient(max_outbound_messages=8)
        client._engine.state = ConnectionState.CONNECTED
        receipt = client.publish_nowait("native/qos0", b"x", qos=level)
        assert receipt.mid is None
