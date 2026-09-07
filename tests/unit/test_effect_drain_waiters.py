from __future__ import annotations

import asyncio
from collections import deque

import pytest

from mqttium.api._effects import EffectPump
from mqttium.protocol.effects import EffectKind, EngineEffect


class _Engine:
    def __init__(self) -> None:
        self.effects = [EngineEffect(EffectKind.SEND, b"blocked")]

    def take_effects(self) -> list[EngineEffect]:
        effects = self.effects
        self.effects = []
        return effects


class _TrackingEvent(asyncio.Event):
    def __init__(self) -> None:
        super().__init__()
        self.active_waits = 0

    async def wait(self) -> bool:
        self.active_waits += 1
        try:
            return await super().wait()
        finally:
            self.active_waits -= 1


class _Owner:
    def __init__(self, release: asyncio.Event) -> None:
        self._connection_epoch = 1
        self._disconnect_exc: BaseException | None = None
        self._engine = _Engine()
        self._connack_fut = None
        self.release = release

    def _apply_effect_inline(self, effect: EngineEffect, epoch: int) -> bool:
        del effect, epoch
        return False

    def _apply_message_effect_batch_inline(self, effects: deque[EngineEffect], epoch: int) -> int:
        del effects, epoch
        return 0

    async def _apply_effect(
        self,
        effect: EngineEffect,
        *,
        nowait: bool,
        epoch: int | None = None,
    ) -> None:
        del effect, nowait, epoch
        await self.release.wait()

    async def _close_transport_after_connection_failure(self) -> None:
        return None

    def _settle_terminal_effect(self, effect: EngineEffect) -> None:
        del effect


async def _wait_until(predicate) -> None:
    for _ in range(100):
        if predicate():
            return
        await asyncio.sleep(0)
    raise AssertionError("condition did not become true")


async def test_cancelled_drain_cancels_its_progress_wait_without_orphan() -> None:
    release = asyncio.Event()
    owner = _Owner(release)
    pump = EffectPump(owner)  # type: ignore[arg-type]
    progress = _TrackingEvent()
    pump.progress = progress
    pump.collect_from_engine()

    drain = asyncio.create_task(pump.drain())
    await _wait_until(lambda: progress.active_waits == 1)

    try:
        drain.cancel()
        with pytest.raises(asyncio.CancelledError):
            await drain
        assert progress.active_waits == 0
        assert pump.waiters == 0
    finally:
        # Also cleans up the pre-fix shielded waiter when this regression test
        # is run against an older tree.
        progress.set()
        release.set()
        task = pump.task
        if task is not None:
            await task
