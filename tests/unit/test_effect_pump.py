from __future__ import annotations

from collections import deque

import pytest

from mqttium.api import AsyncClient
from mqttium.protocol.effects import EffectKind, EngineEffect


def test_effect_operations_are_bound_directly_to_the_pump() -> None:
    client = AsyncClient(client_id="effect-owner")

    assert client._collect_effects_locked.__self__ is client._effect_pump
    assert client._drain_effects_inline.__self__ is client._effect_pump
    assert client._schedule_effect_flush.__self__ is client._effect_pump
    assert client._drain_effects.__self__ is client._effect_pump
    assert client._discard_connection_effects.__self__ is client._effect_pump


def test_effect_diagnostic_attributes_are_views() -> None:
    client = AsyncClient(client_id="effect-views")

    assert client._pending_effects is client._effect_pump.pending
    assert client._pending_effect_epoch is client._effect_pump.pending_epoch
    assert client._effect_enqueued == client._effect_pump.enqueued
    assert client._effect_applied == client._effect_pump.applied
    assert client._effect_flush_task is client._effect_pump.task


@pytest.mark.asyncio
async def test_single_send_effect_bypasses_queue_and_accounting() -> None:
    client = AsyncClient(client_id="effect-fast-path")
    client._engine._emit(EffectKind.SEND, b"payload")

    client._collect_effects_locked()

    assert client._pending_effects == deque()
    assert client._effect_enqueued == 0
    assert client._effect_applied == 0
    assert client._outbound.get_nowait() == b"payload"
    client._outbound.task_done()


@pytest.mark.asyncio
async def test_discard_preserves_terminal_publish_order() -> None:
    client = AsyncClient(client_id="effect-discard")
    client._connection_epoch = 2
    complete = EngineEffect(EffectKind.PUBLISH_COMPLETE, 41)
    failed = EngineEffect(EffectKind.PUBLISH_FAILED, object())
    send = EngineEffect(EffectKind.SEND, b"stale")
    client._pending_effects.extend((send, complete, failed))
    client._effect_pump.pending_epoch = 1
    client._effect_pump.enqueued = 3

    client._discard_connection_effects()

    assert list(client._pending_effects) == [complete, failed]
    assert client._pending_effect_epoch == 2
    assert client._effect_applied == 1
