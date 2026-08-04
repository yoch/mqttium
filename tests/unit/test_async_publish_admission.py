"""Async publication behavior at the logical outbound admission boundary."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.errors import FlowControlError
from mqttium.protocol.engine import EffectKind, EngineEffect


async def _complete(client: AsyncClient, mid: int) -> None:
    client._engine._complete_outbound_record(mid)
    client._engine.packet_ids.release(mid)
    await client._apply_effect(
        EngineEffect(kind=EffectKind.PUBLISH_COMPLETE, data=mid),
        nowait=False,
    )


async def test_wait_mode_blocks_until_logical_capacity_is_released() -> None:
    client = AsyncClient(
        max_pending_outbound_messages=1,
        max_pending_outbound_bytes=None,
    )
    first = await client.publish("admission/first", b"one", qos=1)
    assert first.mid is not None

    second_task = asyncio.create_task(client.publish("admission/second", b"two", qos=1))
    await asyncio.sleep(0)

    assert not second_task.done()
    assert client._engine.pending_outbound_messages == 1
    assert len(client._receipts) == 1

    await _complete(client, first.mid)
    second = await asyncio.wait_for(second_task, timeout=1.0)

    assert second.mid is not None
    assert client._engine.pending_outbound_messages == 1
    assert len(client._receipts) == 1


async def test_nowait_rejection_is_atomic() -> None:
    client = AsyncClient(
        max_pending_outbound_messages=1,
        max_pending_outbound_bytes=None,
    )
    first = await client.publish("admission/first", b"one", qos=1)
    assert first.mid is not None
    before_ids = len(client._engine.packet_ids)
    before_records = list(client._engine.store.out_items())

    with pytest.raises(FlowControlError):
        await client.publish("admission/rejected", b"two", qos=1, nowait=True)

    assert client._engine.pending_outbound_messages == 1
    assert len(client._engine.packet_ids) == before_ids
    assert list(client._engine.store.out_items()) == before_records
    assert len(client._receipts) == 1
    assert not client._pending_effects


async def test_error_mode_refuses_without_waiting() -> None:
    client = AsyncClient(
        max_pending_outbound_messages=1,
        max_pending_outbound_bytes=None,
        publish_backpressure="error",
    )
    await client.publish("admission/first", b"one", qos=1)

    with pytest.raises(FlowControlError):
        await client.publish("admission/rejected", b"two", qos=1)

    assert client._engine.pending_outbound_messages == 1


async def test_cancellation_while_waiting_leaves_no_publication_state() -> None:
    client = AsyncClient(
        max_pending_outbound_messages=1,
        max_pending_outbound_bytes=None,
    )
    await client.publish("admission/first", b"one", qos=1)
    before_ids = len(client._engine.packet_ids)
    before_records = list(client._engine.store.out_items())

    waiting = asyncio.create_task(client.publish("admission/cancelled", b"two", qos=1))
    await asyncio.sleep(0)
    assert not waiting.done()
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting

    assert client._engine.pending_outbound_messages == 1
    assert len(client._engine.packet_ids) == before_ids
    assert list(client._engine.store.out_items()) == before_records
    assert len(client._receipts) == 1
