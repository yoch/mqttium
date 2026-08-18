"""Writer-task byte quantum: FIFO leftover slot, not a second scheduler."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api._writer import (
    WritePump,
    _WRITER_BATCH_MAX_BYTES,
    _WRITER_BATCH_MAX_ITEMS,
)
from mqttium.transport.writes import item_size


class _RecordingTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[bytes, ...] | bytes]] = []

    async def write(self, data: bytes) -> None:
        self.calls.append(("write", data))

    async def write_many(self, parts: list[bytes]) -> None:
        self.calls.append(("many", tuple(parts)))

    async def read(self, n: int = 65536) -> bytes:
        return b""

    async def close(self) -> None:
        return None

    def is_closing(self) -> bool:
        return False

    @property
    def wire(self) -> list[bytes]:
        out: list[bytes] = []
        for kind, data in self.calls:
            if kind == "many":
                out.extend(data)
            else:
                out.append(data)
        return out


class _GatedTransport(_RecordingTransport):
    """Pause on the first write so tests can inspect the leftover slot."""

    def __init__(self) -> None:
        super().__init__()
        self.gate = asyncio.Event()
        self.entered = asyncio.Event()
        self._writes = 0

    async def write(self, data: bytes) -> None:
        self._writes += 1
        if self._writes == 1:
            self.entered.set()
            await self.gate.wait()
        await super().write(data)

    async def write_many(self, parts: list[bytes]) -> None:
        self._writes += 1
        if self._writes == 1:
            self.entered.set()
            await self.gate.wait()
        await super().write_many(parts)


class _EagerTransport(_RecordingTransport):
    def __init__(self) -> None:
        super().__init__()
        self.eager: list[bytes] = []

    def write_nowait(self, data: bytes) -> bool:
        self.eager.append(data)
        self.calls.append(("eager", data))
        return True


async def _no_failure(exc: BaseException) -> None:
    raise AssertionError(f"unexpected writer failure: {exc!r}")


def _pump() -> WritePump:
    return WritePump(max_bytes=1 << 20, max_messages=1024, on_failure=_no_failure)


def _chunk(tag: bytes, size: int) -> bytes:
    body = size - len(tag)
    assert body >= 0
    return tag + b"x" * body


def _park_held(pump: WritePump, item: bytes | tuple[bytes, bytes]) -> None:
    pump.queue.put_nowait(item)
    pump.queued_bytes += item_size(item)
    pump._held = pump.queue.get_nowait()


QUANTUM = _WRITER_BATCH_MAX_BYTES
HALF = QUANTUM // 2 + QUANTUM // 8  # 40 KiB when the quantum is 64 KiB
SLICE = QUANTUM // 10


@pytest.mark.asyncio
async def test_byte_cut_preserves_fifo_for_plain_and_segmented_items() -> None:
    transport = _RecordingTransport()
    pump = _pump()
    first = _chunk(b"A", HALF)
    header, payload = b"H", _chunk(b"B", HALF - 1)
    third = _chunk(b"C", HALF)
    pump.start(transport)
    try:
        assert pump.try_enqueue(first) is True
        assert pump.try_enqueue((header, payload)) is True
        assert pump.try_enqueue(third) is True
        await asyncio.wait_for(pump.join(), timeout=1.0)
    finally:
        await pump.stop()

    assert transport.wire == [first, header, payload, third]
    assert pump.batches == 3
    assert pump.batched_items == 3
    assert pump.batched_bytes == item_size(first) + item_size((header, payload)) + len(third)
    assert pump._held is None


@pytest.mark.asyncio
async def test_first_oversized_item_still_progresses() -> None:
    transport = _RecordingTransport()
    pump = _pump()
    huge = _chunk(b"H", QUANTUM + 4096)
    small = b"tail"
    pump.start(transport)
    try:
        assert pump.try_enqueue(huge) is True
        assert pump.try_enqueue(small) is True
        await asyncio.wait_for(pump.join(), timeout=1.0)
    finally:
        await pump.stop()

    assert transport.wire == [huge, small]
    assert pump.batches == 2
    assert pump.batched_items == 2
    assert pump._held is None
    assert pump.queued_bytes == 0


@pytest.mark.asyncio
async def test_held_item_is_neither_lost_nor_duplicated_across_batches() -> None:
    transport = _GatedTransport()
    pump = _pump()
    first = _chunk(b"1", HALF)
    second = _chunk(b"2", HALF)
    third = _chunk(b"3", HALF)
    pump.start(transport)
    try:
        assert pump.try_enqueue(first) is True
        assert pump.try_enqueue(second) is True
        assert pump.try_enqueue(third) is True
        await asyncio.wait_for(transport.entered.wait(), timeout=1.0)
        assert pump._held == second
        assert pump.queue.qsize() == 1
        assert pump.queued_messages == 2
        assert pump.queued_bytes == len(first) + len(second) + len(third)
        transport.gate.set()
        await asyncio.wait_for(pump.join(), timeout=1.0)
    finally:
        await pump.stop()

    assert transport.wire == [first, second, third]
    assert pump.batches == 3
    assert pump.batched_items == 3
    assert pump.batched_bytes == len(first) + len(second) + len(third)
    assert pump._held is None
    assert pump.queued_messages == 0


@pytest.mark.asyncio
async def test_quantum_split_clears_queue_accounting_and_join() -> None:
    transport = _RecordingTransport()
    pump = _pump()
    a = _chunk(b"a", HALF)
    b = _chunk(b"b", HALF)
    pump.start(transport)
    try:
        assert pump.try_enqueue(a) is True
        assert pump.try_enqueue(b) is True
        await asyncio.wait_for(pump.join(), timeout=1.0)
        assert pump.queued_bytes == 0
        assert pump.queued_messages == 0
        assert pump._held is None
        await asyncio.wait_for(pump.join(), timeout=0.1)
    finally:
        await pump.stop()

    assert pump.batches == 2
    assert pump.batched_items == 2
    assert pump.batched_bytes == len(a) + len(b)


@pytest.mark.asyncio
async def test_eager_does_not_overtake_a_held_item() -> None:
    pump = _pump()
    transport = _EagerTransport()
    pump._write_nowait = transport.write_nowait
    pump._eager_armed = True
    _park_held(pump, _chunk(b"h", HALF))
    assert pump.queued_messages == 1
    assert pump.queue.empty()

    assert pump.try_enqueue(b"later") is True
    assert transport.eager == []
    assert pump.eager_writes == 0
    assert pump.queued_messages == 2
    assert pump._held is not None
    assert pump.queue.get_nowait() == b"later"
    pump.queue.task_done()
    pump.discard()
    await asyncio.wait_for(pump.join(), timeout=0.1)


@pytest.mark.asyncio
async def test_enqueue_during_byte_cut_does_not_reorder_held() -> None:
    transport = _GatedTransport()
    pump = _pump()
    first = _chunk(b"A", HALF)
    held = _chunk(b"B", HALF)
    later = b"C"
    pump.start(transport)
    try:
        assert pump.try_enqueue(first) is True
        assert pump.try_enqueue(held) is True
        await asyncio.wait_for(transport.entered.wait(), timeout=1.0)
        assert pump._held == held
        assert pump.try_enqueue(later) is True
        assert pump.queued_messages == 2
        transport.gate.set()
        await asyncio.wait_for(pump.join(), timeout=1.0)
    finally:
        await pump.stop()

    assert transport.wire == [first, held, later]


@pytest.mark.asyncio
async def test_discard_clears_held_without_leaking_join() -> None:
    pump = _pump()
    _park_held(pump, b"held")
    pump.queue.put_nowait(b"queued")
    pump.queued_bytes += len(b"queued")
    assert pump.queued_messages == 2

    pump.discard()
    assert pump._held is None
    assert pump.queued_messages == 0
    assert pump.queued_bytes == 0
    await asyncio.wait_for(pump.join(), timeout=0.1)


@pytest.mark.asyncio
async def test_reset_drops_held_and_abandons_the_old_queue() -> None:
    pump = _pump()
    _park_held(pump, b"held")
    pump.queue.put_nowait(b"queued")
    pump.queued_bytes += len(b"queued")
    old = pump.queue
    pump.reset()
    assert pump._held is None
    assert pump.queue is not old
    assert pump.queued_messages == 0
    assert pump.queued_bytes == 0
    await asyncio.wait_for(pump.join(), timeout=0.1)


@pytest.mark.asyncio
async def test_mixed_sizes_split_or_coalesce_against_the_quantum() -> None:
    transport = _RecordingTransport()
    pump = _pump()
    left = _chunk(b"L", HALF)
    right = _chunk(b"R", HALF)
    pump.start(transport)
    try:
        assert pump.try_enqueue(left) is True
        assert pump.try_enqueue(right) is True
        await asyncio.wait_for(pump.join(), timeout=1.0)
        assert pump.batches == 2
        assert pump.batched_items == 2
        assert transport.wire == [left, right]

        slices = [_chunk(bytes([ord("a") + i]), SLICE) for i in range(10)]
        for item in slices:
            assert pump.try_enqueue(item) is True
        await asyncio.wait_for(pump.join(), timeout=1.0)
        assert pump.batches == 3
        assert pump.batched_items == 12
        assert transport.wire[2:] == slices
        assert sum(map(len, slices)) <= QUANTUM

        exact = _chunk(b"E", QUANTUM // 2)
        assert pump.try_enqueue(exact) is True
        assert pump.try_enqueue(exact) is True
        await asyncio.wait_for(pump.join(), timeout=1.0)
        assert pump.batches == 4
        assert pump.batched_items == 14
        assert transport.wire[-2:] == [exact, exact]
    finally:
        await pump.stop()


@pytest.mark.asyncio
async def test_item_ceiling_still_applies_under_the_byte_quantum() -> None:
    transport = _RecordingTransport()
    pump = WritePump(
        max_bytes=1 << 20,
        max_messages=_WRITER_BATCH_MAX_ITEMS + 64,
        on_failure=_no_failure,
    )
    pump.start(transport)
    try:
        for _ in range(_WRITER_BATCH_MAX_ITEMS + 1):
            assert pump.try_enqueue(b"x") is True
        await asyncio.wait_for(pump.join(), timeout=1.0)
    finally:
        await pump.stop()

    assert pump.batches == 2
    assert pump.batched_items == _WRITER_BATCH_MAX_ITEMS + 1


def test_can_enqueue_size_counts_the_held_item() -> None:
    pump = WritePump(max_bytes=1024, max_messages=1, on_failure=_no_failure)
    _park_held(pump, b"held")
    assert pump.queued_messages == 1
    assert pump.can_enqueue_size(1) is False
    assert pump.try_enqueue(b"x") is False
    pump.discard()


def test_latency_batch_refuses_while_held() -> None:
    writes: list[bytes] = []

    def write_nowait(data: bytes) -> bool:
        writes.append(data)
        return True

    pump = _pump()
    pump._write_nowait = write_nowait
    _park_held(pump, b"held")
    for _ in range(12):
        assert pump.try_enqueue(b"x" * 4096) is True
    assert pump._try_flush_latency_batch() is False
    assert writes == []
    assert pump._held == b"held"
    pump.discard()
