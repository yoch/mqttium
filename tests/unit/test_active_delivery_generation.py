from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.protocol.effects import EffectKind
from mqttium.types import Message


async def _wait_until(predicate) -> None:
    for _ in range(1000):
        if predicate():
            return
        await asyncio.sleep(0)
    raise AssertionError("condition did not become true")


def _client(max_iterator_bytes: int | None) -> AsyncClient:
    return AsyncClient(
        client_id="active-delivery-generation",
        max_iterator_messages=1,
        max_iterator_bytes=max_iterator_bytes,
    )


@pytest.mark.parametrize("max_iterator_bytes", [None, 1024])
async def test_waiting_admission_cannot_cross_stream_generation(
    max_iterator_bytes: int | None,
) -> None:
    client = _client(max_iterator_bytes)
    first = Message("first", b"1")
    assert client._delivery.accept(first, None) is None

    stale = Message("old/waiting", b"2")
    client._engine._emit(EffectKind.MESSAGE, stale)
    client._effect_pump.collect_from_engine()
    lane = asyncio.create_task(client._delivery_lane.drain())
    await _wait_until(lambda: client._delivery.waiters == 1)
    assert client._delivery_lane.active_count == 1

    old_generation = client._delivery._stream_generation
    client._delivery.close()
    client._delivery.reset_stream()

    assert client._delivery._stream_generation == old_generation + 1
    await asyncio.wait_for(lane, timeout=1)
    assert client._delivery.messages_queue.empty()
    assert client._delivery.waiters == 0
    assert client._delivery_lane.active_count == 0


@pytest.mark.parametrize("max_iterator_bytes", [None, 1024])
async def test_waiting_admission_cannot_cross_connection_epoch(
    max_iterator_bytes: int | None,
) -> None:
    client = _client(max_iterator_bytes)
    first = Message("first", b"1")
    assert client._delivery.accept(first, None) is None

    stale = Message("old/epoch", b"2")
    client._engine._emit(EffectKind.MESSAGE, stale)
    client._effect_pump.collect_from_engine()
    lane = asyncio.create_task(client._delivery_lane.drain())
    await _wait_until(lambda: client._delivery.waiters == 1)
    assert client._delivery_lane.active_count == 1
    old_epoch = client._connection_epoch

    # Hold the writer condition so _invalidate_connection_epoch() suspends
    # after epoch/admission invalidation. This makes the race deterministic.
    await client._write_pump.space.acquire()
    try:
        invalidating = asyncio.create_task(client._invalidate_connection_epoch())
        await _wait_until(lambda: client._connection_epoch == old_epoch + 1)

        stream = client.messages()
        assert await anext(stream) is first
        await asyncio.sleep(0)

        assert client._delivery.messages_queue.empty()
        await asyncio.wait_for(lane, timeout=1)
        assert client._delivery.waiters == 0
        assert client._delivery_lane.active_count == 0
        await stream.aclose()
    finally:
        client._write_pump.space.release()

    await asyncio.wait_for(invalidating, timeout=1)
