"""A reentrant eager flusher must acquire its task identity before user code."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.protocol.effects import EffectKind, EngineEffect
from tests.support import ScriptedBrokerTransport, transport_factory
from tests.unit.test_first_inline_bursts import clean, effects
from tests.unit.test_first_inline_bursts import task_factory as task_factory


@pytest.mark.parametrize("count", [1, 2, 3, 8])
@pytest.mark.parametrize("shutdown", [False, True])
async def test_interrupted_eager_flush_keeps_awaiting_successor_owned(
    task_factory, count, shutdown
) -> None:
    client = AsyncClient(message_delivery="callback", keepalive=0)
    transport = ScriptedBrokerTransport()
    client._transport_factory = transport_factory(transport)
    pump = client._effect_pump
    entered = asyncio.Event()
    release = asyncio.Event()
    successors = []
    seen = []
    loop_errors = []
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: loop_errors.append(context))
    apply_effect = client._apply_effect

    async def paused_effect(effect, *, nowait, epoch):
        if effect.kind is EffectKind.PINGRESP:
            successors.append(asyncio.current_task())
            entered.set()
            await release.wait()
        return await apply_effect(effect, nowait=nowait, epoch=epoch)

    def callback(message):
        seen.append(message.payload)
        task = asyncio.current_task()
        assert task is not None
        task.cancel()
        raise asyncio.CancelledError("cancel first notification")

    try:
        await client.connect("memory-transport", timeout=1)
        client._apply_effect = paused_effect
        client.on_message = callback
        pump.pending = effects(count)
        pump.pending.append(EngineEffect(EffectKind.PINGRESP))
        pump.pending_epoch = client._connection_epoch
        pump.enqueued += count + 1
        pump.schedule()
        first = pump.task
        assert first is not None
        await asyncio.gather(first, return_exceptions=True)
        await asyncio.wait_for(entered.wait(), 1)
        successor = successors[0]
        assert successor is not None and not successor.done()
        assert first.cancelled()
        assert seen == [b"0"]
        # Assert during the suspension, not just after every task has finished.
        assert pump.task is successor
        if shutdown:
            await asyncio.wait_for(client.disconnect(), 1)
            assert successor.cancelled()
            assert not client.is_connected
        else:
            release.set()
            await asyncio.wait_for(asyncio.shield(successor), 1)
            await asyncio.sleep(0)
        assert pump.task is None
        assert not pump.pending
        assert pump.enqueued == pump.applied
        assert not loop_errors, loop_errors
    finally:
        release.set()
        await asyncio.gather(*[t for t in successors if t is not None], return_exceptions=True)
        await asyncio.wait_for(client.disconnect(), 1)
        await clean(client)
        loop.set_exception_handler(previous_handler)
