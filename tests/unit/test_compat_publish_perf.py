"""Regression tests for compatibility publish ordering and handoff behavior."""

from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest

from mqttium.api import AsyncClient
from mqttium.api.models import PublishBatchReceipt, PublishReceipt
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.compat.paho import CallbackAPIVersion, Client
from mqttium.enums import ConnectionState, MQTTProtocolVersion, OutboundQoSState, QoS
from mqttium.errors import ProtocolError
from mqttium.packets import PubAckPacket
from mqttium.protocol.engine import EffectKind, ProtocolEngine, PublishFailure
from mqttium.protocol.packet_ids import PacketIdPool
from mqttium.types import OutboundMessage


class _TracingPacketIds(PacketIdPool):
    def __init__(self, trace: list[tuple[str, Any]]) -> None:
        super().__init__()
        self._trace = trace

    def release(self, mid: int) -> None:
        self._trace.append(("release", mid))
        super().release(mid)


class _TracingEngine(ProtocolEngine):
    def __init__(self) -> None:
        self.trace: list[tuple[str, Any]] = []
        super().__init__()
        self.packet_ids = _TracingPacketIds(self.trace)

    def _emit(self, kind: EffectKind, data: Any = None) -> None:
        if kind in (EffectKind.PUBLISH_COMPLETE, EffectKind.PUBLISH_FAILED):
            self.trace.append(("emit", kind))
        super()._emit(kind, data)


def _prime_qos1(engine: ProtocolEngine) -> int:
    engine.state = ConnectionState.CONNECTED
    mid = engine.packet_ids.allocate()
    assert engine.flow.try_acquire()
    engine.store.put_out(
        OutboundMessage(
            mid=mid,
            topic="t",
            payload=b"x",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            state=OutboundQoSState.WAIT_PUBACK,
        )
    )
    engine.take_effects()
    return mid


def test_engine_puback_emits_completion_before_mid_release() -> None:
    engine = _TracingEngine()
    mid = _prime_qos1(engine)
    decoder = IncrementalDecoder()
    decoder.feed(PubAckPacket(mid=mid).encode())
    for raw in decoder.drain_packets():
        engine.handle_raw(raw)
    assert engine.trace[-2:] == [
        ("emit", EffectKind.PUBLISH_COMPLETE),
        ("release", mid),
    ]


def test_qos0_effect_completion_follows_send() -> None:
    engine = ProtocolEngine()
    engine.state = ConnectionState.CONNECTED
    handle = engine.queue_publish("t", b"x", qos=0)
    effects = engine.take_effects()
    assert handle.mid is None
    assert [effect.kind for effect in effects] == [
        EffectKind.SEND,
        EffectKind.PUBLISH_COMPLETE,
    ]
    assert effects[-1].data is None


@pytest.mark.asyncio
async def test_old_completion_cannot_settle_reused_mid() -> None:
    client = AsyncClient()
    client.on_publish = lambda *_args: None
    mid = 7
    old_receipt = PublishReceipt(mid=mid, qos=QoS.AT_LEAST_ONCE, _event=asyncio.Event())
    old_batch = PublishBatchReceipt()
    old_batch._register(mid)
    client._receipts[mid] = old_receipt
    client._batch_receipts[mid] = old_batch
    client._engine._emit(EffectKind.PUBLISH_COMPLETE, mid)
    with client._state_mutex:
        client._collect_effects_locked()
    assert old_receipt.is_done()
    assert old_batch.pending_count == 0
    effect = client._pending_effects.popleft()

    new_receipt = PublishReceipt(mid=mid, qos=QoS.AT_LEAST_ONCE, _event=asyncio.Event())
    new_batch = PublishBatchReceipt()
    new_batch._register(mid)
    client._receipts[mid] = new_receipt
    client._batch_receipts[mid] = new_batch

    await client._apply_effect(effect, nowait=False)
    await client._callback_queue.join()
    assert client._receipts[mid] is new_receipt
    assert client._batch_receipts[mid] is new_batch
    assert not new_receipt.is_done()
    assert new_batch.pending_count == 1
    await client._shutdown_callback_worker(drain=True)


@pytest.mark.asyncio
async def test_old_failure_cannot_fail_reused_mid() -> None:
    client = AsyncClient()
    client.on_publish = lambda *_args: None
    mid = 9
    old_receipt = PublishReceipt(mid=mid, qos=QoS.AT_LEAST_ONCE, _event=asyncio.Event())
    client._receipts[mid] = old_receipt
    failure = ProtocolError("old publish failed")
    client._engine._emit(
        EffectKind.PUBLISH_FAILED,
        PublishFailure(mid=mid, reason=failure),
    )
    with client._state_mutex:
        client._collect_effects_locked()
    assert old_receipt.is_done()
    assert old_receipt._error is failure
    effect = client._pending_effects.popleft()

    new_receipt = PublishReceipt(mid=mid, qos=QoS.AT_LEAST_ONCE, _event=asyncio.Event())
    client._receipts[mid] = new_receipt
    await client._apply_effect(effect, nowait=False)
    await client._callback_queue.join()
    assert client._receipts[mid] is new_receipt
    assert not new_receipt.is_done()
    assert new_receipt._error is None
    await client._shutdown_callback_worker(drain=True)


def test_off_loop_qos0_publish_uses_coalesced_loop_queue(monkeypatch: pytest.MonkeyPatch) -> None:
    class LoopStub:
        def __init__(self) -> None:
            self.calls: list[tuple[Any, tuple[Any, ...]]] = []

        def call_soon_threadsafe(self, callback: Any, *args: Any) -> None:
            self.calls.append((callback, args))

    client = Client(
        CallbackAPIVersion.VERSION2,
        client_id="perf",
        protocol=MQTTProtocolVersion.MQTTv311,
    )
    client._async._engine.state = ConnectionState.CONNECTED
    loop = LoopStub()
    client._loop = cast(asyncio.AbstractEventLoop, loop)
    flushes: list[bool] = []
    monkeypatch.setattr(client._async, "_schedule_effect_flush", lambda: flushes.append(True))
    monkeypatch.setattr(
        asyncio,
        "run_coroutine_threadsafe",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("QoS 0 must not use a coroutine handoff")
        ),
    )

    for _ in range(100):
        info = client.publish("perf/qos0", b"x", qos=0)
        assert info.mid is None
    assert len(loop.calls) == 1
    callback, args = loop.calls.pop()
    callback(*args)
    assert flushes == [True]
    assert [effect.kind for effect in client._async._pending_effects] == [
        *([EffectKind.SEND] * 100),
        *([EffectKind.PUBLISH_COMPLETE] * 100),
    ]
