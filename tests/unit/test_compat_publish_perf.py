"""Regression tests for compatibility publish ordering and handoff behavior."""

from __future__ import annotations

import asyncio
import threading
import time
from typing import Any, cast

import pytest

import mqttium.compat.paho as paho_compat
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
        self.outbound.packet_ids = _TracingPacketIds(self.trace)

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
    engine.outbound._reserve(engine.outbound.logical_size("t", b"x", None))
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
    client._register_publish_receipt(mid, old_receipt)
    client._register_batch_receipt(mid, old_batch)
    client._engine._emit(EffectKind.PUBLISH_COMPLETE, mid)
    client._collect_effects_locked()
    assert not old_receipt.is_done()
    assert client._pending_effects
    effect = client._pending_effects.popleft()

    new_receipt = PublishReceipt(mid=mid, qos=QoS.AT_LEAST_ONCE, _event=asyncio.Event())
    new_batch = PublishBatchReceipt()
    new_batch._register(mid)
    client._register_publish_receipt(mid, new_receipt)
    client._register_batch_receipt(mid, new_batch)

    await client._apply_effect(effect, nowait=False)
    await client._callback_queue.join()
    assert old_receipt.is_done()
    assert old_batch.pending_count == 0
    assert not new_receipt.is_done()
    assert new_batch.pending_count == 1
    assert client._pop_publish_receipt(mid) is new_receipt
    await client._shutdown_callback_worker(drain=True)


@pytest.mark.asyncio
async def test_old_failure_cannot_fail_reused_mid() -> None:
    client = AsyncClient()
    client.on_publish = lambda *_args: None
    mid = 9
    old_receipt = PublishReceipt(mid=mid, qos=QoS.AT_LEAST_ONCE, _event=asyncio.Event())
    client._register_publish_receipt(mid, old_receipt)
    failure = ProtocolError("old publish failed")
    client._engine._emit(
        EffectKind.PUBLISH_FAILED,
        PublishFailure(mid=mid, reason=failure),
    )
    client._collect_effects_locked()
    assert not old_receipt.is_done()
    effect = client._pending_effects.popleft()

    new_receipt = PublishReceipt(mid=mid, qos=QoS.AT_LEAST_ONCE, _event=asyncio.Event())
    client._register_publish_receipt(mid, new_receipt)
    await client._apply_effect(effect, nowait=False)
    await client._callback_queue.join()
    assert old_receipt.is_done()
    assert old_receipt._error is failure
    assert not new_receipt.is_done()
    assert new_receipt._error is None
    assert client._pop_publish_receipt(mid) is new_receipt
    await client._shutdown_callback_worker(drain=True)


def test_off_loop_qos0_publish_uses_coalesced_loop_queue(monkeypatch: pytest.MonkeyPatch) -> None:
    class LoopStub:
        def __init__(self) -> None:
            self.calls: list[tuple[Any, tuple[Any, ...]]] = []

        def is_running(self) -> bool:
            return True

        def call_soon_threadsafe(self, callback: Any, *args: Any) -> None:
            self.calls.append((callback, args))

        def call_soon(self, callback: Any, *args: Any) -> None:
            self.calls.append((callback, args))

    client = Client(
        CallbackAPIVersion.VERSION2,
        client_id="perf",
        protocol=MQTTProtocolVersion.MQTTv311,
    )
    client._async._engine.state = ConnectionState.CONNECTED
    loop = LoopStub()
    client._loop = cast(asyncio.AbstractEventLoop, loop)
    monkeypatch.setattr(client._async, "_drain_effects_inline", lambda: None)
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
    assert [effect.kind for effect in client._async._pending_effects] == [
        *([EffectKind.SEND] * 100),
        *([EffectKind.PUBLISH_COMPLETE] * 100),
    ]


@pytest.mark.parametrize("qos", [1, 2])
def test_off_loop_qosn_publish_avoids_coroutine_handoff(
    monkeypatch: pytest.MonkeyPatch,
    qos: int,
) -> None:
    client = Client(
        CallbackAPIVersion.VERSION2,
        client_id="perf-qos1",
        protocol=MQTTProtocolVersion.MQTTv311,
    )
    client.loop_start()
    try:
        client._async._engine.state = ConnectionState.CONNECTED
        monkeypatch.setattr(
            asyncio,
            "run_coroutine_threadsafe",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("QoS 1 publish must not use a coroutine handoff")
            ),
        )

        info = client.publish(f"perf/qos{qos}", b"x", qos=qos)
        assert info.mid is not None
        assert client._async._pop_publish_receipt(info.mid) is info._receipt
    finally:
        client.loop_stop()


def test_mixed_qos_requests_preserve_ingress_order(monkeypatch: pytest.MonkeyPatch) -> None:
    class LoopStub:
        def __init__(self) -> None:
            self.calls: list[tuple[Any, tuple[Any, ...]]] = []

        def is_running(self) -> bool:
            return True

        def call_soon_threadsafe(self, callback: Any, *args: Any) -> None:
            self.calls.append((callback, args))

        def call_soon(self, callback: Any, *args: Any) -> None:
            self.calls.append((callback, args))

    client = Client(CallbackAPIVersion.VERSION2, client_id="mixed-order")
    client._async._engine.state = ConnectionState.CONNECTED
    loop = LoopStub()
    client._loop = cast(asyncio.AbstractEventLoop, loop)
    order: list[str] = []
    original_queue_publish = client._async._engine.queue_publish

    def traced_queue_publish(topic: str, *args: Any, **kwargs: Any):
        order.append(topic)
        return original_queue_publish(topic, *args, **kwargs)

    monkeypatch.setattr(client._async._engine, "queue_publish", traced_queue_publish)
    monkeypatch.setattr(
        client,
        "_finalize_publish_effects",
        lambda: client._async._engine.take_effects(),
    )
    qos1_enqueued = threading.Event()
    original_enqueue = client._enqueue_publish_request

    def traced_enqueue(request) -> None:
        original_enqueue(request)
        if request.qos is QoS.AT_LEAST_ONCE:
            qos1_enqueued.set()

    monkeypatch.setattr(client, "_enqueue_publish_request", traced_enqueue)

    client.publish("mixed/first", b"0", qos=0)
    result: list[object] = []

    def publish_qos1() -> None:
        try:
            result.append(client.publish("mixed/second", b"1", qos=1))
        except BaseException as exc:
            result.append(exc)

    thread = threading.Thread(target=publish_qos1)
    thread.start()
    assert qos1_enqueued.wait(timeout=1.0)
    client.publish("mixed/third", b"0", qos=0)

    assert len(loop.calls) == 1
    callback, args = loop.calls.pop(0)
    callback(*args)
    thread.join(timeout=1.0)

    assert not thread.is_alive()
    assert len(result) == 1
    assert isinstance(result[0], paho_compat.MQTTMessageInfo)
    assert order == ["mixed/first", "mixed/second", "mixed/third"]


def test_publish_batches_are_count_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    class LoopStub:
        def __init__(self) -> None:
            self.calls: list[tuple[Any, tuple[Any, ...]]] = []

        def is_running(self) -> bool:
            return True

        def call_soon_threadsafe(self, callback: Any, *args: Any) -> None:
            self.calls.append((callback, args))

        def call_soon(self, callback: Any, *args: Any) -> None:
            self.calls.append((callback, args))

    client = Client(CallbackAPIVersion.VERSION2, client_id="bounded-batch")
    client._async._engine.state = ConnectionState.CONNECTED
    loop = LoopStub()
    client._loop = cast(asyncio.AbstractEventLoop, loop)
    committed: list[str] = []
    original_queue_publish = client._async._engine.queue_publish

    def traced_queue_publish(topic: str, *args: Any, **kwargs: Any):
        committed.append(topic)
        return original_queue_publish(topic, *args, **kwargs)

    monkeypatch.setattr(paho_compat, "_PUBLISH_BATCH_MAX_MESSAGES", 2)
    monkeypatch.setattr(client._async._engine, "queue_publish", traced_queue_publish)
    monkeypatch.setattr(
        client,
        "_finalize_publish_effects",
        lambda: client._async._engine.take_effects(),
    )

    for index in range(5):
        client.publish(f"batch/{index}", b"x", qos=0)
    assert len(loop.calls) == 1

    callbacks = 0
    while loop.calls:
        callback, args = loop.calls.pop(0)
        callback(*args)
        callbacks += 1

    assert callbacks == 3
    assert committed == [f"batch/{index}" for index in range(5)]


def test_publish_batches_are_byte_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    class LoopStub:
        def __init__(self) -> None:
            self.calls: list[tuple[Any, tuple[Any, ...]]] = []

        def is_running(self) -> bool:
            return True

        def call_soon_threadsafe(self, callback: Any, *args: Any) -> None:
            self.calls.append((callback, args))

        def call_soon(self, callback: Any, *args: Any) -> None:
            self.calls.append((callback, args))

    client = Client(CallbackAPIVersion.VERSION2, client_id="byte-bounded-batch")
    client._async._engine.state = ConnectionState.CONNECTED
    loop = LoopStub()
    client._loop = cast(asyncio.AbstractEventLoop, loop)
    committed: list[str] = []
    original_queue_publish = client._async._engine.queue_publish

    def traced_queue_publish(topic: str, *args: Any, **kwargs: Any):
        committed.append(topic)
        return original_queue_publish(topic, *args, **kwargs)

    monkeypatch.setattr(paho_compat, "_PUBLISH_BATCH_MAX_BYTES", 20)
    monkeypatch.setattr(client._async._engine, "queue_publish", traced_queue_publish)
    monkeypatch.setattr(
        client,
        "_finalize_publish_effects",
        lambda: client._async._engine.take_effects(),
    )

    for topic in ("a", "b", "c"):
        client.publish(topic, b"x" * 10, qos=0)
    assert len(loop.calls) == 1

    callbacks = 0
    while loop.calls:
        callback, args = loop.calls.pop(0)
        callback(*args)
        callbacks += 1

    assert callbacks == 3
    assert committed == ["a", "b", "c"]


def test_concurrent_qos1_publish_coalesces_loop_drains(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = Client(
        CallbackAPIVersion.VERSION2,
        client_id="perf-qos1-concurrent",
        protocol=MQTTProtocolVersion.MQTTv311,
    )
    drains = 0
    original_drain = client._drain_publish_requests

    def counted_drain() -> None:
        nonlocal drains
        drains += 1
        # Give concurrent producers a small window to join the same batch.
        time.sleep(0.002)
        original_drain()

    monkeypatch.setattr(client, "_drain_publish_requests", counted_drain)
    client.loop_start()
    try:
        client._async._engine.state = ConnectionState.CONNECTED
        workers = 8
        per_worker = 40
        barrier = threading.Barrier(workers)
        errors: list[BaseException] = []
        mids: list[int] = []
        result_lock = threading.Lock()

        def worker() -> None:
            try:
                barrier.wait()
                local: list[int] = []
                for _ in range(per_worker):
                    info = client.publish("perf/qos1", b"x", qos=1)
                    assert info.mid is not None
                    local.append(info.mid)
                with result_lock:
                    mids.extend(local)
            except BaseException as exc:
                with result_lock:
                    errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(workers)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10.0)

        assert all(not thread.is_alive() for thread in threads)
        assert errors == []
        assert len(mids) == workers * per_worker
        assert len(set(mids)) == workers * per_worker
        assert drains < workers * per_worker / 2
    finally:
        client.loop_stop()


def test_publish_on_network_thread_outside_callback_is_inline() -> None:
    client = Client(CallbackAPIVersion.VERSION2, client_id="network-thread")
    client.loop_start()
    try:
        client._async._engine.state = ConnectionState.CONNECTED
        done = threading.Event()
        result: dict[str, object] = {}

        def publish_on_loop() -> None:
            try:
                result["info"] = client.publish("network/thread", b"x", qos=1)
            except BaseException as exc:
                result["error"] = exc
            finally:
                done.set()

        assert client._loop is not None
        client._loop.call_soon_threadsafe(publish_on_loop)
        assert done.wait(timeout=2.0)
        assert "error" not in result
        info = result["info"]
        assert isinstance(info, paho_compat.MQTTMessageInfo)
        assert info.mid is not None
    finally:
        client.loop_stop()


def test_publish_timeout_cancels_before_admission(monkeypatch: pytest.MonkeyPatch) -> None:
    client = Client(CallbackAPIVersion.VERSION2, client_id="cancel-before-admission")
    client.loop_start()
    release = threading.Event()
    started = threading.Event()
    try:
        client._async._engine.state = ConnectionState.CONNECTED
        monkeypatch.setattr(paho_compat, "_PUBLISH_HANDOFF_TIMEOUT", 0.01)

        def block_loop() -> None:
            started.set()
            release.wait(timeout=2.0)

        assert client._loop is not None
        client._loop.call_soon_threadsafe(block_loop)
        assert started.wait(timeout=1.0)

        with pytest.raises(RuntimeError, match="before admission"):
            client.publish("timeout/cancelled", b"x", qos=1)
        release.set()
        time.sleep(0.05)

        assert client._async._engine.pending_outbound_messages == 0
        assert not client._async._engine.packet_ids
        assert not client._async._receipts
    finally:
        release.set()
        client.loop_stop()


def test_publish_timeout_after_admission_returns_authoritative_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = Client(CallbackAPIVersion.VERSION2, client_id="commit-after-timeout")
    client.loop_start()
    release = threading.Event()
    started = threading.Event()
    original_finalize = client._finalize_publish_effects
    try:
        client._async._engine.state = ConnectionState.CONNECTED
        monkeypatch.setattr(paho_compat, "_PUBLISH_HANDOFF_TIMEOUT", 0.01)

        def slow_finalize() -> None:
            started.set()
            release.wait(timeout=2.0)
            original_finalize()

        monkeypatch.setattr(client, "_finalize_publish_effects", slow_finalize)

        def release_later() -> None:
            assert started.wait(timeout=1.0)
            time.sleep(0.03)
            release.set()

        releaser = threading.Thread(target=release_later)
        releaser.start()
        info = client.publish("timeout/committed", b"x", qos=1)
        releaser.join(timeout=1.0)

        assert info.mid is not None
        assert client._async._engine.pending_outbound_messages == 1
    finally:
        release.set()
        client.loop_stop()


def test_loop_stop_fails_queued_qos1_publish(monkeypatch: pytest.MonkeyPatch) -> None:
    client = Client(CallbackAPIVersion.VERSION2, client_id="stop-pending")
    client.loop_start()
    release = threading.Event()
    started = threading.Event()
    errors: list[BaseException] = []
    try:
        client._async._engine.state = ConnectionState.CONNECTED
        monkeypatch.setattr(paho_compat, "_PUBLISH_HANDOFF_TIMEOUT", 2.0)

        def block_loop() -> None:
            started.set()
            release.wait(timeout=2.0)

        assert client._loop is not None
        client._loop.call_soon_threadsafe(block_loop)
        assert started.wait(timeout=1.0)

        def publish() -> None:
            try:
                client.publish("stop/pending", b"x", qos=1)
            except BaseException as exc:
                errors.append(exc)

        publisher = threading.Thread(target=publish)
        publisher.start()
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            if client._publish_spillover is not None or not client._publish_pending.empty():
                break
            time.sleep(0.001)
        else:
            raise AssertionError("publish request was not queued")

        releaser = threading.Thread(target=lambda: (time.sleep(0.05), release.set()))
        releaser.start()
        client.loop_stop()
        publisher.join(timeout=1.0)
        releaser.join(timeout=1.0)

        assert not publisher.is_alive()
        assert len(errors) == 1
        assert "stopped before publish admission" in str(errors[0])
        assert client._async._engine.pending_outbound_messages == 0
    finally:
        release.set()
        if client._thread is not None:
            client.loop_stop()
