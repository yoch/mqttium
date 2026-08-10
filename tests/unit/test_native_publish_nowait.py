"""Loop-bound native publish admission and adapter-boundary contracts."""

from __future__ import annotations

import asyncio
import inspect

import pytest

import mqttium.compat.paho as paho_compat
import mqttium.protocol.outbound as outbound_module
from mqttium.api import AsyncClient
from mqttium.api.models import PublishMessage
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PublishPacket
from mqttium.protocol.effects import EffectKind, EngineEffect
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
    assert client._pop_publish_receipt(receipt.mid) is receipt


async def test_publish_nowait_coalesces_async_effect_flush(monkeypatch) -> None:
    client = AsyncClient(max_outbound_messages=512)
    client._engine.state = ConnectionState.CONNECTED
    seen: list[int | None] = []
    client.on_publish = lambda mid, _error: seen.append(mid)

    def direct_path_forbidden(*_args, **_kwargs):
        raise AssertionError("on_publish must retain the standard effect path")

    monkeypatch.setattr(type(client._engine.outbound), "prepare_qos0", direct_path_forbidden)

    for _ in range(100):
        client.publish_nowait("native/qos0", b"x", qos=0)

    assert client._effect_pump.enqueued > 0
    task = client._effect_flush_task
    assert task is not None
    assert not task.done()
    await task
    await client._callback_queue.join()
    assert seen == [None] * 100
    await client._shutdown_callback_worker(drain=False)


def test_paho_uses_the_async_client_adapter_boundary() -> None:
    source = inspect.getsource(paho_compat.Client)
    for forbidden in (
        "self._async._engine",
        "self._async._register_publish_receipt",
        "self._async._collect_effects_locked",
        "self._async._drain_effects_inline",
    ):
        assert forbidden not in source


def test_disconnect_metadata_boundary_is_private() -> None:
    assert not hasattr(AsyncClient, "last_disconnect")
    assert hasattr(AsyncClient, "_last_disconnect_info")


def _forbid_direct_path(client: AsyncClient, monkeypatch) -> None:
    """Make any entry into the direct QoS 0 path an explicit failure."""

    def direct_path_forbidden(*_args, **_kwargs):
        raise AssertionError("the direct QoS 0 path must stay disabled here")

    monkeypatch.setattr(type(client._engine.outbound), "prepare_qos0", direct_path_forbidden)


async def test_await_publish_qos0_uses_the_direct_path() -> None:
    client = AsyncClient(max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED

    receipt = await client.publish("native/await-qos0", b"x", qos=0)

    assert receipt.mid is None
    assert client._effect_pump.batches == 0
    assert client._effect_pump.enqueued == 0
    assert not client._engine.has_pending_effects
    assert isinstance(client._outbound.get_nowait(), bytes)


async def test_await_publish_qos0_keeps_the_effect_path_under_on_publish(monkeypatch) -> None:
    client = AsyncClient(max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED
    client.on_publish = lambda _mid, _error: None
    _forbid_direct_path(client, monkeypatch)

    await client.publish("native/await-qos0", b"x", qos=0)

    assert client._effect_pump.batches > 0
    await client._shutdown_callback_worker(drain=False)


async def test_publish_many_qos0_uses_the_direct_path() -> None:
    client = AsyncClient(max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED

    receipt = await client.publish_many(
        [PublishMessage("native/batch", b"a", 0), PublishMessage("native/batch", b"b", 0)]
    )

    assert receipt.submitted == 2
    assert receipt.completed == 2
    assert client._effect_pump.batches == 0
    assert not client._engine.has_pending_effects
    assert isinstance(client._outbound.get_nowait(), bytes)
    assert isinstance(client._outbound.get_nowait(), bytes)


async def test_publish_many_keeps_the_effect_path_under_on_publish(monkeypatch) -> None:
    client = AsyncClient(max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED
    client.on_publish = lambda _mid, _error: None
    _forbid_direct_path(client, monkeypatch)

    await client.publish_many([PublishMessage("native/batch", b"a", 0)])

    assert client._effect_pump.batches > 0
    await client._shutdown_callback_worker(drain=False)


async def test_publish_many_mixed_qos_keeps_the_effect_path(monkeypatch) -> None:
    """One non-QoS-0 request disqualifies the whole batch."""
    client = AsyncClient(max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED
    _forbid_direct_path(client, monkeypatch)

    receipt = await client.publish_many(
        [PublishMessage("native/batch", b"a", 0), PublishMessage("native/batch", b"b", 1)]
    )

    assert receipt.submitted == 2
    assert client._effect_pump.batches > 0


async def test_direct_path_is_gated_on_drained_effect_queues() -> None:
    """Both pending-effect gates must independently disable the direct path."""
    client = AsyncClient(max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED

    assert client._direct_qos0_ready() is True

    client._engine._emit(EffectKind.SUBACK, None)
    assert client._engine.has_pending_effects
    assert client._direct_qos0_ready() is False

    client._engine.take_effects()
    assert client._direct_qos0_ready() is True

    client._effect_pump.pending.append(EngineEffect(kind=EffectKind.SUBACK, data=None))
    assert client._direct_qos0_ready() is False

    client._effect_pump.pending.clear()
    assert client._direct_qos0_ready() is True


async def test_direct_path_returns_when_on_publish_is_cleared() -> None:
    """Installing a callback disables the fast path; clearing it restores it."""
    client = AsyncClient(max_outbound_messages=32)
    client._engine.state = ConnectionState.CONNECTED

    client.on_publish = lambda _mid, _error: None
    assert client._direct_qos0_ready() is False
    client.publish_nowait("native/gate", b"x", qos=0)
    assert client._effect_pump.enqueued > 0

    await client._drain_effects()
    await client._callback_queue.join()
    await client._shutdown_callback_worker(drain=False)

    client.on_publish = None
    assert client._direct_qos0_ready() is True
    before = client._effect_pump.enqueued
    client.publish_nowait("native/gate", b"x", qos=0)
    assert client._effect_pump.enqueued == before


async def test_publish_nowait_direct_path_encodes_mqtt5_properties(monkeypatch) -> None:
    properties = Properties()
    properties.set("content_type", "application/json")
    properties.add_user_property("source", "native-fast-path")
    client = AsyncClient(protocol=MQTTProtocolVersion.MQTTv5, max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED
    original_encode = outbound_module.encode_publish_item
    encode_calls = 0

    def counted_encode(*args, **kwargs):
        nonlocal encode_calls
        encode_calls += 1
        return original_encode(*args, **kwargs)

    monkeypatch.setattr(outbound_module, "encode_publish_item", counted_encode)

    receipt = client.publish_nowait(
        "native/mqtt5",
        b'{"value": 42}',
        qos=0,
        properties=properties,
    )

    assert receipt.mid is None
    assert encode_calls == 1
    assert client._effect_pump.batches == 0
    assert client._effect_pump.enqueued == 0
    assert not client._engine.has_pending_effects
    item = client._outbound.get_nowait()
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
