from pathlib import Path


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if text.count(old) != 1:
        raise SystemExit(f"{label}: expected exactly one match, got {text.count(old)}")
    return text.replace(old, new, 1)


paho_path = Path("src/mqttium/compat/paho.py")
paho = paho_path.read_text()
paho = replace_once(
    paho,
    "import threading\nfrom contextlib import suppress\nfrom collections.abc import Callable\n",
    "import threading\nfrom collections import deque\nfrom contextlib import suppress\nfrom collections.abc import Callable\n",
    "paho imports",
)
marker = "        self._topic_callbacks = TopicMatcher()\n        self._in_callback = False\n"
paho = replace_once(
    paho,
    marker,
    marker
    + "        self._qos0_pending: deque[tuple[str, bytes, bool]] = deque()\n"
    + "        self._qos0_lock = threading.Lock()\n"
    + "        self._qos0_drain_scheduled = False\n",
    "paho init",
)
start = paho.index("    def publish(\n")
end = paho.index("    def _dispatch_publish", start)
new_publish = '''    def _enqueue_qos0_publish(
        self,
        topic: str,
        payload: bytes,
        retain: bool,
    ) -> None:
        """Queue one QoS 0 request without handing control to the loop per call."""
        assert self._loop is not None
        schedule = False
        with self._qos0_lock:
            self._qos0_pending.append((topic, payload, retain))
            if not self._qos0_drain_scheduled:
                self._qos0_drain_scheduled = True
                schedule = True
        if schedule:
            self._loop.call_soon_threadsafe(self._drain_qos0_publishes)

    def _drain_qos0_publishes(self) -> None:
        """Move a coalesced QoS 0 batch into the engine on its owning loop."""
        with self._qos0_lock:
            batch = list(self._qos0_pending)
            self._qos0_pending.clear()
            self._qos0_drain_scheduled = False

        errors: list[BaseException] = []
        queued = False
        with self._async._state_mutex:
            for topic, payload, retain in batch:
                try:
                    self._async._engine.queue_publish(
                        topic,
                        payload,
                        qos=QoS.AT_MOST_ONCE,
                        retain=retain,
                    )
                except BaseException as exc:
                    errors.append(exc)
                else:
                    queued = True
            if queued:
                self._async._collect_effects_locked()

        if queued:
            self._async._schedule_effect_flush()
        for error in errors:
            self._async._spawn_callback(self._dispatch_publish, None, error)

    def publish(
        self,
        topic: str,
        payload: bytes | str = b"",
        qos: int = 0,
        retain: bool = False,
    ) -> MQTTMessageInfo:
        """Queue a publish without waiting for TCP writer progress.

        QoS 0 requests are coalesced in a thread-safe façade queue and consumed
        on the network loop. QoS 1/2 wait only for the loop to allocate the MID
        and register the receipt; effect draining remains asynchronous.
        """
        data = payload.encode("utf-8") if isinstance(payload, str) else payload
        requested_qos = QoS(qos)
        if self._loop is None:
            self.loop_start()
        assert self._loop is not None

        if requested_qos is QoS.AT_MOST_ONCE:
            receipt = PublishReceipt(mid=None, qos=requested_qos, _event=None)
            self._enqueue_qos0_publish(topic, data, retain)
            return MQTTMessageInfo(mid=None, _receipt=receipt, _loop=self._loop)

        if self._on_loop_in_callback():
            with self._async._state_mutex:
                handle = self._async._engine.queue_publish(
                    topic,
                    data,
                    qos=requested_qos,
                    retain=retain,
                )
                assert handle.mid is not None
                receipt = PublishReceipt(
                    mid=handle.mid,
                    qos=handle.qos,
                    _event=asyncio.Event(),
                )
                self._async._receipts[handle.mid] = receipt
                self._async._collect_effects_locked()
            self._async._schedule_effect_flush()
            return MQTTMessageInfo(mid=receipt.mid, _receipt=receipt, _loop=self._loop)

        handoff: dict[str, Any] = {}
        done = threading.Event()

        async def _queue() -> None:
            try:
                async with self._async._engine_lock:
                    with self._async._state_mutex:
                        handle = self._async._engine.queue_publish(
                            topic,
                            data,
                            qos=requested_qos,
                            retain=retain,
                        )
                        assert handle.mid is not None
                        receipt = PublishReceipt(
                            mid=handle.mid,
                            qos=handle.qos,
                            _event=asyncio.Event(),
                        )
                        self._async._receipts[handle.mid] = receipt
                        self._async._collect_effects_locked()
                        handoff["receipt"] = receipt
                self._async._schedule_effect_flush()
            except BaseException as exc:
                handoff["error"] = exc
            finally:
                done.set()

        fut = asyncio.run_coroutine_threadsafe(_queue(), self._loop)
        if not done.wait(timeout=5.0):
            fut.cancel()
            raise RuntimeError("publish handoff to event loop timed out")
        error = handoff.get("error")
        if error is not None:
            raise error
        receipt = handoff["receipt"]
        return MQTTMessageInfo(mid=receipt.mid, _receipt=receipt, _loop=self._loop)

'''
paho_path.write_text(paho[:start] + new_publish + paho[end:])

engine_path = Path("src/mqttium/protocol/engine.py")
engine = engine_path.read_text()
engine = replace_once(
    engine,
    "            self._send(wire)\n            return PublishHandle(mid=None, qos=qos)\n",
    "            self._send(wire)\n"
    "            # Completion follows SEND in the effect stream so compatibility\n"
    "            # callbacks cannot run before outbound queue acceptance.\n"
    "            self._emit(EffectKind.PUBLISH_COMPLETE, None)\n"
    "            return PublishHandle(mid=None, qos=qos)\n",
    "engine QoS0 completion",
)
engine_path.write_text(engine)

client_path = Path("src/mqttium/api/async_client.py")
client = client_path.read_text()
client = replace_once(
    client,
    '''        elif kind is EffectKind.PUBLISH_COMPLETE:
            mid: int = effect.data
            # Receipt is normally retired under ``_engine_lock`` in
            # ``_collect_effects_locked``. Keep a defensive settle for any
            # completion that bypassed that path.
            self._settle_outbound_locked(mid, error=None)
            if self.on_publish is not None:
                await self._enqueue_callback(self.on_publish, mid, None)
''',
    '''        elif kind is EffectKind.PUBLISH_COMPLETE:
            # Receipts were already settled atomically in _collect_effects_locked.
            # Re-settling here could target a newer publish that reused this MID.
            mid: int | None = effect.data
            if self.on_publish is not None:
                await self._enqueue_callback(self.on_publish, mid, None)
''',
    "AsyncClient completion settlement",
)
client = replace_once(
    client,
    '''        elif kind is EffectKind.PUBLISH_FAILED:
            failure: PublishFailure = effect.data
            self._settle_outbound_locked(failure.mid, error=failure.reason)
            if self.on_publish is not None:
                await self._enqueue_callback(self.on_publish, failure.mid, failure.reason)
''',
    '''        elif kind is EffectKind.PUBLISH_FAILED:
            failure: PublishFailure = effect.data
            if self.on_publish is not None:
                await self._enqueue_callback(self.on_publish, failure.mid, failure.reason)
''',
    "AsyncClient failure settlement",
)
client_path.write_text(client)

Path("tests/unit/test_compat_publish_perf.py").write_text(
    '''"""Regression tests for compatibility publish ordering and handoff behavior."""

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
'''
)

Path("tests/integration/test_compat_publish_perf.py").write_text(
    '''"""End-to-end compatibility publish regression tests."""

from __future__ import annotations

import asyncio
import threading
import time

import pytest

from mqttium.api import AsyncClient
from mqttium.compat.paho import CallbackAPIVersion, Client
from mqttium.protocol.reconnect import ReconnectPolicy


@pytest.mark.asyncio
async def test_compat_qos0_callbacks_and_delivery_are_exactly_once() -> None:
    count = 1_000
    topic = f"integration/compat-qos0/{time.time_ns()}"
    subscriber = AsyncClient(
        client_id=f"sub-{time.time_ns()}",
        reconnect=ReconnectPolicy(enabled=False),
    )
    await subscriber.connect("127.0.0.1", 11883, timeout=5.0)
    await subscriber.subscribe(topic, timeout=5.0)

    received: list[bytes] = []

    async def collect() -> None:
        async for message in subscriber.messages():
            if message.topic != topic:
                continue
            received.append(message.payload)
            if len(received) == count:
                return

    collector = asyncio.create_task(collect())
    publisher = Client(
        CallbackAPIVersion.VERSION2,
        client_id=f"pub-{time.time_ns()}",
    )
    completed = 0
    completed_event = threading.Event()
    completed_lock = threading.Lock()

    def on_publish(*_args: object) -> None:
        nonlocal completed
        with completed_lock:
            completed += 1
            if completed == count:
                completed_event.set()

    publisher.on_publish = on_publish
    await asyncio.to_thread(publisher.connect, "127.0.0.1", 11883)
    try:
        started = time.perf_counter()

        def submit() -> float:
            for index in range(count):
                publisher.publish(topic, str(index).encode(), qos=0)
            return count / (time.perf_counter() - started)

        submit_rate = await asyncio.to_thread(submit)
        assert await asyncio.to_thread(completed_event.wait, 10.0)
        await asyncio.wait_for(collector, timeout=10.0)
        assert completed == count
        assert len(received) == count
        assert submit_rate > 5_000, f"submit_rate={submit_rate:.0f} too low"
    finally:
        await asyncio.to_thread(publisher.disconnect)
        publisher.loop_stop()
        if not collector.done():
            collector.cancel()
            with pytest.raises(asyncio.CancelledError):
                await collector
        await subscriber.disconnect()
'''
)
