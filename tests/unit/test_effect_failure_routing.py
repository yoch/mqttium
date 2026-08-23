"""Deferred EffectPump failures keep deterministic call and connection owners."""

from __future__ import annotations

import asyncio
from collections import deque
from typing import Any

import pytest

from mqttium.api._effects import EffectPump
from mqttium.protocol.effects import EffectKind, EngineEffect


class _Engine:
    def __init__(self, effects: list[EngineEffect]) -> None:
        self.effects = effects

    def take_effects(self) -> list[EngineEffect]:
        effects = self.effects
        self.effects = []
        return effects


class _Owner:
    def __init__(
        self,
        effects: list[EngineEffect],
        failure: BaseException,
        *,
        block_first: asyncio.Event | None = None,
    ) -> None:
        self._connection_epoch = 7
        self._disconnect_exc: BaseException | None = None
        self._engine = _Engine(effects)
        self._connack_fut = None
        self.failure = failure
        self.block_first = block_first
        self.applies = 0
        self.closed = 0

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
        if effect.data == b"ok":
            if self.block_first is not None:
                await self.block_first.wait()
            return
        raise self.failure

    async def _close_transport_after_connection_failure(self) -> None:
        self.closed += 1

    def _settle_terminal_effect(self, effect: EngineEffect) -> None:
        del effect


async def _wait_idle(pump: EffectPump) -> None:
    for _ in range(100):
        if pump.task is None:
            return
        await asyncio.sleep(0)
    raise AssertionError("effect pump did not become idle")


async def _wait_for_waiters(pump: EffectPump, count: int) -> None:
    for _ in range(100):
        if pump.waiters == count:
            return
        await asyncio.sleep(0)
    raise AssertionError(f"effect pump did not reach {count} waiters")


async def test_unobserved_fatal_scheduled_failure_routes_to_connection_owner() -> None:
    failure = RuntimeError("deferred effect failed")
    owner = _Owner([EngineEffect(EffectKind.SEND, b"fail")], failure)
    pump = EffectPump(owner)  # type: ignore[arg-type]
    loop = asyncio.get_running_loop()
    contexts: list[dict[str, Any]] = []
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: contexts.append(context))
    try:
        pump.collect_from_engine()
        pump.schedule()
        await _wait_idle(pump)
    finally:
        loop.set_exception_handler(previous_handler)

    assert owner._disconnect_exc is failure
    assert owner.closed == 1
    assert contexts == []
    assert pump.error is None


async def test_waiting_drain_receives_original_failure_and_lifecycle_owns_connection() -> None:
    failure = RuntimeError("deferred effect failed")
    owner = _Owner([EngineEffect(EffectKind.SEND, b"fail")], failure)
    pump = EffectPump(owner)  # type: ignore[arg-type]

    pump.collect_from_engine()
    with pytest.raises(RuntimeError) as caught:
        await asyncio.wait_for(pump.drain(), timeout=1.0)
    await _wait_idle(pump)

    assert caught.value is failure
    assert owner._disconnect_exc is failure
    assert owner.closed == 1
    assert pump.error is None


async def test_two_concurrent_drains_receive_same_original_failure() -> None:
    failure = RuntimeError("shared deferred failure")
    release = asyncio.Event()
    owner = _Owner(
        [EngineEffect(EffectKind.SEND, b"fail")],
        failure,
        block_first=release,
    )

    # Block the failing apply explicitly so both drain() calls register first.
    async def blocked_failure(
        effect: EngineEffect, *, nowait: bool, epoch: int | None = None
    ) -> None:
        del effect, nowait, epoch
        await release.wait()
        raise failure

    owner._apply_effect = blocked_failure  # type: ignore[method-assign]
    pump = EffectPump(owner)  # type: ignore[arg-type]
    pump.collect_from_engine()

    first = asyncio.create_task(pump.drain())
    await _wait_for_waiters(pump, 1)
    second = asyncio.create_task(pump.drain())
    await _wait_for_waiters(pump, 2)
    release.set()

    results = await asyncio.gather(first, second, return_exceptions=True)
    assert results == [failure, failure]
    assert owner.closed == 1
    assert pump.error is None
    assert pump.waiters == 0


async def test_waiter_before_failing_effect_is_not_poisoned() -> None:
    failure = RuntimeError("second effect failed")
    release = asyncio.Event()
    owner = _Owner(
        [EngineEffect(EffectKind.SEND, b"ok")],
        failure,
        block_first=release,
    )
    pump = EffectPump(owner)  # type: ignore[arg-type]
    pump.collect_from_engine()

    before = asyncio.create_task(pump.drain())
    await _wait_for_waiters(pump, 1)

    owner._engine.effects = [EngineEffect(EffectKind.SEND, b"fail")]
    pump.collect_from_engine()
    including_failure = asyncio.create_task(pump.drain())
    await _wait_for_waiters(pump, 2)
    release.set()

    assert await asyncio.wait_for(before, timeout=1.0) is None
    with pytest.raises(RuntimeError) as caught:
        await asyncio.wait_for(including_failure, timeout=1.0)
    assert caught.value is failure
    assert pump.error is None

    # A later drain did not exist when the failure happened and must not inherit it.
    await asyncio.wait_for(pump.drain(), timeout=1.0)


async def test_unobserved_protocol_error_routes_to_connection_owner() -> None:
    failure = RuntimeError("peer protocol diagnostic")
    owner = _Owner(
        [EngineEffect(EffectKind.PROTOCOL_ERROR, "rude peer")],
        failure,
    )
    pump = EffectPump(owner)  # type: ignore[arg-type]
    loop = asyncio.get_running_loop()
    contexts: list[dict[str, Any]] = []
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: contexts.append(context))
    try:
        pump.collect_from_engine()
        pump.schedule()
        await _wait_idle(pump)
    finally:
        loop.set_exception_handler(previous_handler)

    assert owner._disconnect_exc is failure
    assert owner.closed == 1
    assert contexts == []
    assert pump.error is None
