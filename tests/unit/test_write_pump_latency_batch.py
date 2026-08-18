from __future__ import annotations

import asyncio

import pytest

from mqttium.api._writer import WritePump


async def _no_failure(exc: BaseException) -> None:
    raise AssertionError(f"unexpected writer failure: {exc!r}")


def _pump() -> WritePump:
    return WritePump(max_bytes=1 << 20, max_messages=1024, on_failure=_no_failure)


def _bind(pump: WritePump, writes: list[bytes], *, accept: bool = True) -> None:
    def write_nowait(data: bytes) -> bool:
        if not accept:
            return False
        writes.append(data)
        return True

    pump._write_nowait = write_nowait
    pump._eager_armed = False


def _queue(pump: WritePump, *items: bytes | tuple[bytes, bytes]) -> None:
    for item in items:
        assert pump.try_enqueue(item)


@pytest.mark.asyncio
async def test_latency_batch_waits_for_item_or_byte_threshold() -> None:
    writes: list[bytes] = []
    pump = _pump()
    _bind(pump, writes)
    _queue(pump, *(b"x" * 64 for _ in range(31)))
    assert pump._try_flush_latency_batch() is False
    assert pump.queued_messages == 31
    assert writes == []
    pump.discard()


@pytest.mark.asyncio
async def test_latency_batch_flushes_at_32_small_frames_in_order() -> None:
    writes: list[bytes] = []
    pump = _pump()
    _bind(pump, writes)
    parts = [bytes([index]) * 16 for index in range(32)]
    _queue(pump, *parts)
    assert pump._try_flush_latency_batch() is True
    assert writes == [b"".join(parts)]
    assert pump.queued_messages == 0
    assert pump.queued_bytes == 0
    assert pump.batches == 1
    assert pump.batched_items == 32
    assert pump.batched_bytes == sum(map(len, parts))
    await asyncio.wait_for(pump.join(), timeout=0.1)


@pytest.mark.asyncio
async def test_latency_batch_flushes_on_byte_budget_before_item_cap() -> None:
    writes: list[bytes] = []
    pump = _pump()
    _bind(pump, writes)
    parts = [b"x" * 4096 for _ in range(6)]
    _queue(pump, *parts)
    assert pump._try_flush_latency_batch() is True
    assert writes == [b"".join(parts)]
    assert pump.queued_bytes == 0
    await asyncio.wait_for(pump.join(), timeout=0.1)


@pytest.mark.asyncio
async def test_latency_batch_transport_refusal_restores_exact_queue_and_join_accounting() -> None:
    writes: list[bytes] = []
    pump = _pump()
    _bind(pump, writes, accept=False)
    parts = [bytes([index]) * 16 for index in range(32)]
    _queue(pump, *parts)
    before_bytes = pump.queued_bytes
    assert pump._try_flush_latency_batch() is False
    assert pump.queued_bytes == before_bytes
    assert [pump.queue.get_nowait() for _ in range(32)] == parts
    for _ in parts:
        pump.queue.task_done()
    await asyncio.wait_for(pump.join(), timeout=0.1)


@pytest.mark.asyncio
async def test_latency_batch_segmented_item_restores_order_and_accounting() -> None:
    writes: list[bytes] = []
    pump = _pump()
    _bind(pump, writes)
    items: list[bytes | tuple[bytes, bytes]] = [b"a" * 4096] * 5 + [(b"h", b"p" * 4096)]
    _queue(pump, *items)
    before_bytes = pump.queued_bytes
    assert pump._try_flush_latency_batch() is False
    assert pump.queued_bytes == before_bytes
    assert [pump.queue.get_nowait() for _ in range(len(items))] == items
    for _ in items:
        pump.queue.task_done()
    await asyncio.wait_for(pump.join(), timeout=0.1)


@pytest.mark.asyncio
async def test_latency_batch_is_disabled_while_writer_or_waiter_owns_ordering() -> None:
    for state in ("writing", "waiter"):
        writes: list[bytes] = []
        pump = _pump()
        _bind(pump, writes)
        _queue(pump, *(b"x" * 4096 for _ in range(6)))
        if state == "writing":
            pump._writing = True
        else:
            pump.waiters = 1
        assert pump._try_flush_latency_batch() is False
        assert pump.queued_messages == 6
        assert writes == []
        pump.discard()
