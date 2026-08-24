"""Concurrency red-team: EffectPump, WritePump, and AsyncClient teardown.

These schedules sit next to already-fixed races (eager unbind on writer
failure, EffectPump drain/failure ownership, cancelled-waiter wakeup
handoff). They pin the adjacent liveness holes closed by the 2026-08-24
red-team: waiter wakeup on writer failure, and collect-during-failing-close.
"""

from __future__ import annotations

import asyncio
from collections import deque
from typing import Any

import pytest

from mqttium.api._effects import EffectPump, StaleConnectionEffect
from mqttium.api._writer import WritePump
from mqttium.api.async_client import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import PacketType, QoS
from mqttium.packets import PublishPacket, encode_frame
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.protocol.reconnect import ReconnectPolicy


class _Engine:
    def __init__(self, effects: list[EngineEffect] | None = None) -> None:
        self.effects = list(effects or [])

    def take_effects(self) -> list[EngineEffect]:
        effects = self.effects
        self.effects = []
        return effects


class _Owner:
    def __init__(
        self,
        effects: list[EngineEffect] | None = None,
        *,
        failure: BaseException | None = None,
    ) -> None:
        self._connection_epoch = 1
        self._disconnect_exc: BaseException | None = None
        self._engine = _Engine(effects)
        self._connack_fut = None
        self.failure = failure
        self.applies = 0
        self.closed = 0
        self.close_entered = asyncio.Event()
        self.close_release = asyncio.Event()
        self.applied_payloads: list[bytes] = []

    def _apply_effect_inline(self, effect: EngineEffect, epoch: int) -> bool:
        del effect, epoch
        return False

    def _apply_message_effect_batch_inline(self, effects: deque[EngineEffect], epoch: int) -> int:
        del effects, epoch
        return 0

    def _apply_decoded_message_effect_batch_inline(
        self, effects: deque[EngineEffect], epoch: int
    ) -> int:
        del effects, epoch
        return 0

    async def _apply_effect(
        self,
        effect: EngineEffect,
        *,
        nowait: bool,
        epoch: int | None = None,
    ) -> None:
        del nowait, epoch
        self.applies += 1
        if self.failure is not None and effect.data == b"fail":
            raise self.failure
        self.applied_payloads.append(effect.data)

    async def _close_transport_after_connection_failure(self) -> None:
        self.closed += 1
        self.close_entered.set()
        await self.close_release.wait()

    def _settle_terminal_effect(self, effect: EngineEffect) -> None:
        del effect


async def _wait_until(predicate: Any, *, timeout: float = 1.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() >= deadline:
            raise AssertionError("condition was not met before timeout")
        await asyncio.sleep(0)


class _HoldThenFailTransport:
    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.fail = False
        self.writes: list[bytes] = []

    async def write(self, data: bytes) -> None:
        self.writes.append(data)
        self.entered.set()
        await self.release.wait()
        if self.fail:
            raise ConnectionResetError("writer failed")

    async def write_many(self, parts: list[bytes]) -> None:
        await self.write(b"".join(parts))

    async def read(self, n: int = 65536) -> bytes:
        del n
        await asyncio.Event().wait()
        return b""

    async def close(self) -> None:
        return None

    def is_closing(self) -> bool:
        return False


async def test_writer_failure_unblocks_enqueue_waiters() -> None:
    """A failed write must not leave producers parked on space forever.

    Adjacent to the already-fixed eager-unbind on writer failure: the failed
    batch releases resident capacity without notifying waiters, and
    AsyncClient._writer_failed does not advance the epoch.
    """

    async def on_failure(exc: BaseException) -> None:
        del exc

    pump = WritePump(max_bytes=1024, max_messages=1, on_failure=on_failure)
    transport = _HoldThenFailTransport()
    pump.start(transport)
    try:
        await pump.enqueue(b"hold")
        await asyncio.wait_for(transport.entered.wait(), timeout=1.0)
        waiter = asyncio.create_task(pump.enqueue(b"blocked"))
        await _wait_until(lambda: pump.waiters == 1)
        transport.fail = True
        transport.release.set()
        await _wait_until(lambda: pump.task is not None and pump.task.done())
        try:
            await asyncio.wait_for(waiter, timeout=0.5)
        except StaleConnectionEffect:
            pass
        assert pump.waiters == 0
    finally:
        transport.release.set()
        await pump.stop()


async def test_effect_failure_close_does_not_orphan_later_drain() -> None:
    """Effects collected during failing close must not park a later drain.

    Failure discards the current epoch, then awaits transport close while
    still holding the pump lock. A collect in that window is neither applied
    nor discarded, so drain() waits for progress that will never come.
    """
    failure = RuntimeError("first effect failed")
    owner = _Owner([EngineEffect(EffectKind.SEND, b"fail")], failure=failure)
    pump = EffectPump(owner)  # type: ignore[arg-type]
    pump.collect_from_engine()
    failing = asyncio.create_task(pump.drain())
    await asyncio.wait_for(owner.close_entered.wait(), timeout=1.0)

    owner._engine.effects = [EngineEffect(EffectKind.SEND, b"late")]
    pump.collect_from_engine()
    later = asyncio.create_task(pump.drain())
    try:
        await asyncio.wait_for(later, timeout=0.3)
        with pytest.raises(RuntimeError):
            await failing
        assert not pump.pending
        assert pump.waiters == 0
    finally:
        owner.close_release.set()
        await asyncio.gather(failing, later, return_exceptions=True)


class _Broker:
    """CONNACK broker that can hold, then fail, an application write."""

    def __init__(self) -> None:
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._decoder = IncrementalDecoder()
        self._gate = asyncio.Event()
        self._gate.set()
        self._fail = False
        self.write_entered = asyncio.Event()
        self._closing = False

    async def write(self, data: bytes) -> None:
        self._decoder.feed(data)
        for raw in self._decoder.drain_packets():
            if raw.packet_type is PacketType.CONNECT:
                self._rx.put_nowait(encode_frame(PacketType.CONNACK, 0, b"\x00\x00"))
                return
        self.write_entered.set()
        await self._gate.wait()
        if self._fail:
            raise ConnectionResetError("application write failed")

    async def read(self, n: int = 65536) -> bytes:
        del n
        return await self._rx.get()

    async def close(self) -> None:
        if not self._closing:
            self._closing = True
            self._rx.put_nowait(b"")

    def is_closing(self) -> bool:
        return self._closing

    def hold_application_writes(self) -> None:
        self.write_entered = asyncio.Event()
        self._gate = asyncio.Event()

    def fail_held_write(self) -> None:
        self._fail = True
        self._gate.set()

    def inject_qos1_publish(self) -> None:
        self._rx.put_nowait(
            PublishPacket(
                topic="in/q1",
                payload=b"nudge",
                qos=QoS.AT_LEAST_ONCE,
                retain=False,
                dup=False,
                mid=7,
            ).encode()
        )


async def test_writer_failure_does_not_deadlock_reader_puback_enqueue() -> None:
    """Reader-owned PUBACK enqueue must not hang when the writer fails.

    1. CONNECT / CONNACK
    2. QoS 0 publish occupies the only writer slot and blocks in write
    3. inbound QoS 1 PUBLISH requires a PUBACK SEND from the reader
    4. the held write fails
    5. teardown must reach quiescence
    """
    broker = _Broker()
    client = AsyncClient(
        client_id="redteam-writer-fail",
        max_outbound_messages=1,
        max_outbound_bytes=1024,
        reconnect=ReconnectPolicy(enabled=False),
    )

    async def factory(host: str, port: int, *, ssl: object = None) -> _Broker:
        del host, port, ssl
        return broker

    client._transport_factory = factory
    disconnected = asyncio.Event()
    client.on_disconnect = lambda _exc: disconnected.set()
    loop = asyncio.get_running_loop()
    reported: list[dict[str, Any]] = []
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: reported.append(context))
    try:
        await client.connect("fake", timeout=2.0)
        broker.hold_application_writes()
        published = asyncio.create_task(client.publish("out/q0", b"hold"))
        await asyncio.wait_for(broker.write_entered.wait(), timeout=1.0)
        broker.inject_qos1_publish()
        await _wait_until(lambda: client._write_pump.waiters >= 1)
        broker.fail_held_write()
        # The reader owns teardown. Calling disconnect() from this task would
        # wake waiters and hide a deadlock in which the reader is the waiter.
        await asyncio.wait_for(disconnected.wait(), timeout=1.0)
        await asyncio.wait_for(published, timeout=1.0)
        assert client._write_pump.waiters == 0
        assert not reported
    finally:
        loop.set_exception_handler(previous)
        try:
            await asyncio.wait_for(client.disconnect(), timeout=1.0)
        except (Exception, asyncio.CancelledError):
            pass
