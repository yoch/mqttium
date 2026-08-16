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
from mqttium.protocol.effects import EngineEffect
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


def test_publish_receipts_remain_fifo_across_multiple_mid_reuses() -> None:
    client = AsyncClient()
    receipts = [PublishReceipt(mid=7, qos=QoS.AT_LEAST_ONCE) for _ in range(3)]

    for receipt in receipts:
        client._register_publish_receipt(7, receipt)

    assert [client._pop_publish_receipt(7) for _ in receipts] == receipts
    assert 7 not in client._receipts


@pytest.mark.asyncio
async def test_old_completion_cannot_settle_reused_mid() -> None:
    client = AsyncClient()
    client.on_publish = lambda *_args: None
    mid = 7
    old_receipt = PublishReceipt(mid=mid, qos=QoS.AT_LEAST_ONCE)
    old_batch = PublishBatchReceipt()
    old_batch._register(mid)
    client._register_publish_receipt(mid, old_receipt)
    client._register_batch_receipt(mid, old_batch)
    effect = EngineEffect(EffectKind.PUBLISH_COMPLETE, mid)
    assert not old_receipt.is_done()

    new_receipt = PublishReceipt(mid=mid, qos=QoS.AT_LEAST_ONCE)
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
    old_receipt = PublishReceipt(mid=mid, qos=QoS.AT_LEAST_ONCE)
    client._register_publish_receipt(mid, old_receipt)
    failure = ProtocolError("old publish failed")
    effect = EngineEffect(
        EffectKind.PUBLISH_FAILED,
        PublishFailure(mid=mid, reason=failure),
    )
    assert not old_receipt.is_done()

    new_receipt = PublishReceipt(mid=mid, qos=QoS.AT_LEAST_ONCE)
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
        # The handle is returned before admission, so the receipt lands later.
        assert info._handoff is not None
        info._handoff.result(timeout=5.0)
        receipt = info._receipt
        assert receipt is not None
        assert receipt.mid is not None
        assert client._async._pop_publish_receipt(receipt.mid) is receipt
    finally:
        client.loop_stop()


def test_facade_mid_is_independent_of_the_wire_packet_id() -> None:
    """``MQTTMessageInfo.mid`` is a façade correlation id, not the packet id.

    Advancing the façade counter past the engine's frontier makes the two
    namespaces observably different, so nothing can pass by coincidence.
    """
    client = Client(CallbackAPIVersion.VERSION2, client_id="facade-mid")
    client.loop_start()
    try:
        client._async._engine.state = ConnectionState.CONNECTED
        for _ in range(10):
            client._next_facade_mid()

        info = client.publish("facade/mid", b"x", qos=1)
        assert info._handoff is not None
        info._handoff.result(timeout=5.0)

        receipt = info._receipt
        assert receipt is not None
        assert receipt.mid == 1
        assert info.mid == 11
    finally:
        client.loop_stop()


def test_facade_mids_resolve_fifo_when_a_packet_id_is_reused() -> None:
    """Reused packet identifiers must not cross their correlation ids."""
    client = Client(CallbackAPIVersion.VERSION2, client_id="facade-mid-fifo")
    client.on_publish = lambda *_args: None
    receipt = PublishReceipt(mid=7, qos=QoS.AT_LEAST_ONCE)
    client._register_facade_mid(receipt, 101)
    client._register_facade_mid(receipt, 102)

    assert client._resolve_facade_mid(7) == 101
    assert client._resolve_facade_mid(7) == 102
    assert not client._facade_mid_map
    # An identifier this façade never bound is reported as the engine gave it.
    assert client._resolve_facade_mid(7) == 7


def _settle_publish_on_loop(client: Client, mid: int) -> None:
    done = threading.Event()

    def settle() -> None:
        try:
            client._async._settle_publish(mid, None)
        finally:
            done.set()

    assert client._loop is not None
    client._loop.call_soon_threadsafe(settle)
    assert done.wait(timeout=5.0)


def test_single_producer_drains_batches_larger_than_one() -> None:
    """The defect Gap A fixes: one producer must fill a real batch.

    While ``publish()`` blocked on loop-side admission, the coalesced queue
    could only ever hold the one request its caller was waiting for.
    """
    client = Client(CallbackAPIVersion.VERSION2, client_id="single-producer-batch")
    batches: list[int] = []
    original_take = client._take_publish_batch

    def measured_take() -> Any:
        batch, has_more = original_take()
        batches.append(len(batch))
        return batch, has_more

    client._take_publish_batch = measured_take  # type: ignore[method-assign]
    client.loop_start()
    try:
        client._async._engine.state = ConnectionState.CONNECTED
        infos = [client.publish(f"batch/{index}", b"x", qos=1) for index in range(200)]
        for info in infos:
            assert info._handoff is not None
            info._handoff.result(timeout=5.0)

        assert max(batches) > 1
        assert len({info.mid for info in infos}) == 200
    finally:
        client.loop_stop()


def test_concurrent_publishers_get_unique_facade_mids() -> None:
    client = Client(CallbackAPIVersion.VERSION2, client_id="facade-mid-concurrent")
    client.loop_start()
    mids: list[int | None] = []
    errors: list[BaseException] = []
    lock = threading.Lock()
    try:
        client._async._engine.state = ConnectionState.CONNECTED

        def worker(worker_id: int) -> None:
            local: list[int | None] = []
            try:
                for index in range(40):
                    info = client.publish(f"concurrent/{worker_id}/{index}", b"x", qos=1)
                    local.append(info.mid)
            except BaseException as exc:  # pragma: no cover - failure path
                with lock:
                    errors.append(exc)
            finally:
                with lock:
                    mids.extend(local)

        threads = [threading.Thread(target=worker, args=(index,)) for index in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10.0)

        assert not errors
        assert len(mids) == 320
        assert len(set(mids)) == 320
    finally:
        client.loop_stop()


def test_facade_mid_map_is_cleared_when_the_callback_is_removed() -> None:
    client = Client(CallbackAPIVersion.VERSION2, client_id="facade-mid-cleanup")
    client.on_publish = lambda *_args: None
    client.loop_start()
    try:
        client._async._engine.state = ConnectionState.CONNECTED
        info = client.publish("cleanup/one", b"x", qos=1)
        assert info._handoff is not None
        info._handoff.result(timeout=5.0)
        assert client._facade_mid_map

        client.on_publish = None
        assert not client._facade_mid_map
    finally:
        client.loop_stop()


def test_facade_mid_map_is_cleared_when_the_loop_stops() -> None:
    client = Client(CallbackAPIVersion.VERSION2, client_id="facade-mid-stop")
    client.on_publish = lambda *_args: None
    client.loop_start()
    try:
        client._async._engine.state = ConnectionState.CONNECTED
        info = client.publish("stop/one", b"x", qos=1)
        assert info._handoff is not None
        info._handoff.result(timeout=5.0)
        assert client._facade_mid_map
    finally:
        client.loop_stop()
    assert not client._facade_mid_map


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

    def traced_enqueue(request) -> bool:
        accepted = original_enqueue(request)
        if accepted and request.qos is QoS.AT_LEAST_ONCE:
            qos1_enqueued.set()
        return accepted

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


def test_publish_enqueued_during_finalization_schedules_next_drain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class LoopStub:
        def __init__(self) -> None:
            self.calls: list[tuple[Any, tuple[Any, ...]]] = []

        def is_running(self) -> bool:
            return True

        def call_soon_threadsafe(self, callback: Any, *args: Any) -> None:
            self.calls.append((callback, args))

        def call_soon(self, callback: Any, *args: Any) -> None:
            self.calls.append((callback, args))

    client = Client(CallbackAPIVersion.VERSION2, client_id="tail-race")
    client._async._engine.state = ConnectionState.CONNECTED
    loop = LoopStub()
    client._loop = cast(asyncio.AbstractEventLoop, loop)
    committed: list[str] = []
    original_queue_publish = client._async._engine.queue_publish

    def traced_queue_publish(topic: str, *args: Any, **kwargs: Any):
        committed.append(topic)
        return original_queue_publish(topic, *args, **kwargs)

    enqueue_tail = True

    def finalize() -> None:
        nonlocal enqueue_tail
        client._async._engine.take_effects()
        if enqueue_tail:
            enqueue_tail = False
            client.publish("tail/second", b"x", qos=0)

    monkeypatch.setattr(client._async._engine, "queue_publish", traced_queue_publish)
    monkeypatch.setattr(client, "_finalize_publish_effects", finalize)

    client.publish("tail/first", b"x", qos=0)
    assert len(loop.calls) == 1
    callback, args = loop.calls.pop(0)
    callback(*args)
    assert len(loop.calls) == 1
    callback, args = loop.calls.pop(0)
    callback(*args)

    assert committed == ["tail/first", "tail/second"]
    assert not client._publish_drain_scheduled


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


def test_qosn_publish_does_not_wait_for_the_loop() -> None:
    """The producer thread never blocks on loop-side admission.

    This is the whole point of the façade MID namespace: with the loop wedged,
    ``publish()`` must still return a usable handle immediately, and admission
    must complete correctly once the loop is released.
    """
    client = Client(CallbackAPIVersion.VERSION2, client_id="nonblocking-qosn")
    client.loop_start()
    release = threading.Event()
    started = threading.Event()
    try:
        client._async._engine.state = ConnectionState.CONNECTED

        def block_loop() -> None:
            started.set()
            release.wait(timeout=5.0)

        assert client._loop is not None
        client._loop.call_soon_threadsafe(block_loop)
        assert started.wait(timeout=1.0)

        before = time.monotonic()
        info = client.publish("nonblocking/qos1", b"x", qos=1)
        elapsed = time.monotonic() - before

        assert info.mid is not None
        assert info.rc == 0
        # The loop stays blocked for seconds; anything near that means the
        # producer waited for admission.
        assert elapsed < 0.5
        # Nothing has been committed yet: the handle precedes the protocol state.
        assert client._async._engine.pending_outbound_messages == 0

        release.set()
        assert info._handoff is not None
        info._handoff.result(timeout=5.0)

        assert client._async._engine.pending_outbound_messages == 1
        assert info._receipt is not None
        assert client._pending_publish_requests == 0
        assert client._pending_publish_bytes == 0
    finally:
        release.set()
        client.loop_stop()


def test_publish_handoff_settles_only_after_effect_finalization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A caller never observes a completion the writer has not yet accepted."""
    client = Client(CallbackAPIVersion.VERSION2, client_id="settle-after-finalize")
    client.loop_start()
    release = threading.Event()
    started = threading.Event()
    original_finalize = client._finalize_publish_effects
    try:
        client._async._engine.state = ConnectionState.CONNECTED

        def slow_finalize() -> None:
            started.set()
            release.wait(timeout=5.0)
            original_finalize()

        monkeypatch.setattr(client, "_finalize_publish_effects", slow_finalize)

        info = client.publish("finalize/committed", b"x", qos=1)
        assert info.mid is not None
        assert started.wait(timeout=1.0)

        assert info._handoff is not None
        assert not info._handoff.done()

        release.set()
        info._handoff.result(timeout=5.0)
        assert client._async._engine.pending_outbound_messages == 1
    finally:
        release.set()
        client.loop_stop()


def test_loop_stop_fails_queued_qos1_publish() -> None:
    """A publication queued when the loop stops fails on its handle."""
    client = Client(CallbackAPIVersion.VERSION2, client_id="stop-pending")
    client.loop_start()
    release = threading.Event()
    started = threading.Event()
    try:
        client._async._engine.state = ConnectionState.CONNECTED

        def block_loop() -> None:
            started.set()
            release.wait(timeout=5.0)

        assert client._loop is not None
        client._loop.call_soon_threadsafe(block_loop)
        assert started.wait(timeout=1.0)

        info = client.publish("stop/pending", b"x", qos=1)
        assert info.mid is not None
        assert not client._publish_pending.empty() or client._publish_spillover is not None

        releaser = threading.Thread(target=lambda: (time.sleep(0.05), release.set()))
        releaser.start()
        client.loop_stop()
        releaser.join(timeout=2.0)

        with pytest.raises(RuntimeError, match="stopped before publish admission"):
            info.wait_for_publish(timeout=2.0)

        assert client._async._engine.pending_outbound_messages == 0
        assert client._pending_publish_requests == 0
        assert client._pending_publish_bytes == 0
    finally:
        release.set()
        if client._thread is not None:
            client.loop_stop()


@pytest.mark.asyncio
async def test_batch_receipt_settles_fifo_across_three_registrations() -> None:
    """Reusing one mid must settle its batch receipts in submission order."""
    client = AsyncClient()
    mid = 21
    batches = [PublishBatchReceipt() for _ in range(3)]
    for batch in batches:
        batch._register(mid)
        client._register_batch_receipt(mid, batch)

    for index, batch in enumerate(batches):
        assert client._pop_batch_receipt(mid) is batch
        remaining = batches[index + 1 :]
        assert all(other.pending_count == 1 for other in remaining)

    assert client._pop_batch_receipt(mid) is None
    assert mid not in client._batch_receipts


@pytest.mark.asyncio
async def test_batch_and_plain_receipts_share_a_reused_mid() -> None:
    client = AsyncClient()
    mid = 22
    batch = PublishBatchReceipt()
    batch._register(mid)
    client._register_batch_receipt(mid, batch)
    receipt = PublishReceipt(mid=mid, qos=QoS.AT_LEAST_ONCE)
    client._register_publish_receipt(mid, receipt)

    client._settle_publish(mid, None)

    assert receipt.is_done()
    assert batch.pending_count == 0
    assert not client._batch_receipts
    assert not client._receipts


@pytest.mark.asyncio
async def test_settling_skips_the_batch_table_when_no_batch_is_outstanding() -> None:
    """A client that never calls publish_many must not hash acked mids twice."""
    client = AsyncClient()
    mid = 23
    receipt = PublishReceipt(mid=mid, qos=QoS.AT_LEAST_ONCE)
    client._register_publish_receipt(mid, receipt)

    lookups = 0
    original = type(client)._pop_batch_receipt

    def counted(self, value):
        nonlocal lookups
        lookups += 1
        return original(self, value)

    client._pop_batch_receipt = counted.__get__(client)  # type: ignore[method-assign]
    client._settle_publish(mid, None)

    assert receipt.is_done()
    assert lookups == 0
