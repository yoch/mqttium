"""Resident writer message budget: admitted frames, not only ``qsize()``."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api._writer import WritePump


class _HeldWriteTransport:
    """Parks ``write`` / ``write_many`` on an Event so a batch stays in flight."""

    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.hold = asyncio.Event()
        self.parts: list[bytes] = []

    async def write(self, data: bytes) -> None:
        self.entered.set()
        await self.hold.wait()
        self.parts.append(data)

    async def write_many(self, parts: list[bytes]) -> None:
        self.entered.set()
        await self.hold.wait()
        self.parts.extend(parts)

    async def read(self, n: int = 65536) -> bytes:
        return b""

    async def close(self) -> None:
        return None

    def is_closing(self) -> bool:
        return False


class _EagerTransport:
    def __init__(self) -> None:
        self.written: list[bytes] = []

    def write_nowait(self, data: bytes) -> bool:
        self.written.append(data)
        return True

    async def write(self, data: bytes) -> None:
        self.written.append(data)

    async def write_many(self, parts: list[bytes]) -> None:
        self.written.extend(parts)

    async def read(self, n: int = 65536) -> bytes:
        return b""

    async def close(self) -> None:
        return None

    def is_closing(self) -> bool:
        return False


class _BoomTransport:
    def __init__(self) -> None:
        self.entered = asyncio.Event()

    async def write(self, data: bytes) -> None:
        self.entered.set()
        raise OSError("boom")

    async def write_many(self, parts: list[bytes]) -> None:
        self.entered.set()
        raise OSError("boom")

    async def read(self, n: int = 65536) -> bytes:
        return b""

    async def close(self) -> None:
        return None

    def is_closing(self) -> bool:
        return False


async def _no_failure(exc: BaseException) -> None:
    raise AssertionError(f"unexpected writer failure: {exc!r}")


def _pump(*, max_bytes: int = 1 << 20, max_messages: int = 1024) -> WritePump:
    return WritePump(max_bytes=max_bytes, max_messages=max_messages, on_failure=_no_failure)


async def _hold_after_fill(pump: WritePump, n: int) -> _HeldWriteTransport:
    for _ in range(n):
        assert pump.try_enqueue(b"x") is True
    transport = _HeldWriteTransport()
    pump.start(transport)
    await asyncio.wait_for(transport.entered.wait(), timeout=1.0)
    return transport


@pytest.mark.asyncio
async def test_resident_stays_at_cap_throughout_an_active_256_item_batch() -> None:
    pump = _pump(max_messages=256)
    transport = await _hold_after_fill(pump, 256)
    try:
        assert pump.queued_messages == 0
        assert pump.resident_messages == 256
        assert pump.try_enqueue(b"extra") is False
        assert pump.queued_messages == 0
        assert pump.resident_messages == 256

        transport.hold.set()
        await asyncio.wait_for(pump.join(), timeout=1.0)
        assert pump.resident_messages == 0
        assert pump.queued_messages == 0
        assert pump.queued_bytes == 0
        assert pump.try_enqueue(b"after") is True
        assert pump.resident_messages == 1
    finally:
        transport.hold.set()
        await pump.stop()


@pytest.mark.asyncio
async def test_qsize_drops_during_batch_but_resident_keeps_the_message_bound() -> None:
    pump = _pump(max_messages=300)
    transport = await _hold_after_fill(pump, 300)
    try:
        assert pump.queued_messages == 44
        assert pump.resident_messages == 300
        assert pump.stats().queued_messages == 44
        assert pump.try_enqueue(b"extra") is False
        assert pump.try_enqueue_many([b"a", b"b"]) is False
        assert pump.queued_messages == 44
        assert pump.resident_messages == 300
    finally:
        transport.hold.set()
        await pump.stop()


@pytest.mark.asyncio
async def test_try_enqueue_many_is_atomic_against_resident_while_batch_in_flight() -> None:
    pump = _pump(max_messages=10)
    transport = await _hold_after_fill(pump, 8)
    try:
        assert pump.queued_messages == 0
        assert pump.resident_messages == 8
        assert pump.try_enqueue_many([b"a", b"b", b"c"]) is False
        assert pump.queued_messages == 0
        assert pump.resident_messages == 8
        assert pump.queued_bytes == 8

        assert pump.try_enqueue_many([b"a", b"b"]) is True
        assert pump.queued_messages == 2
        assert pump.resident_messages == 10
        assert pump.try_enqueue(b"full") is False
        assert pump.try_enqueue_many([b"x"]) is False
    finally:
        transport.hold.set()
        await pump.stop()


@pytest.mark.asyncio
async def test_normal_write_completion_releases_resident_by_batch_len() -> None:
    pump = _pump(max_messages=16)
    for _ in range(5):
        assert pump.try_enqueue(b"x") is True
    assert pump.resident_messages == 5
    assert pump.queued_messages == 5

    transport = _HeldWriteTransport()
    pump.start(transport)
    await asyncio.wait_for(transport.entered.wait(), timeout=1.0)
    assert pump.queued_messages == 0
    assert pump.resident_messages == 5

    transport.hold.set()
    await asyncio.wait_for(pump.join(), timeout=1.0)
    assert pump.resident_messages == 0
    assert pump.queued_messages == 0
    await pump.stop()


@pytest.mark.asyncio
async def test_latency_microflush_success_releases_resident() -> None:
    writes: list[bytes] = []
    pump = _pump()

    def write_nowait(data: bytes) -> bool:
        writes.append(data)
        return True

    pump._write_nowait = write_nowait
    pump._eager_armed = False
    parts = [bytes([index]) * 16 for index in range(16)]
    for part in parts:
        assert pump.try_enqueue(part) is True
    assert pump.resident_messages == 16
    assert pump.queued_messages == 16

    assert pump._try_flush_latency_batch() is True
    assert pump.resident_messages == 0
    assert pump.queued_messages == 0
    assert pump.queued_bytes == 0
    await asyncio.wait_for(pump.join(), timeout=0.1)


def test_discard_releases_exactly_the_remaining_queued_items() -> None:
    pump = _pump(max_messages=8)
    assert pump.try_enqueue_many([b"a", b"b", b"c"]) is True
    assert pump.resident_messages == 3
    pump.discard()
    assert pump.resident_messages == 0
    assert pump.queued_messages == 0
    assert pump.queued_bytes == 0


def test_reset_zeros_resident_on_epoch_transition() -> None:
    pump = _pump(max_messages=8)
    assert pump.try_enqueue(b"keep") is True
    assert pump.resident_messages == 1
    pump.reset()
    assert pump.resident_messages == 0
    assert pump.queued_messages == 0
    assert pump.queued_bytes == 0


@pytest.mark.asyncio
async def test_discard_keeps_in_flight_resident_until_writer_finally() -> None:
    pump = _pump(max_messages=16)
    transport = await _hold_after_fill(pump, 10)
    try:
        assert pump.try_enqueue_many([b"q", b"w"]) is True
        assert pump.queued_messages == 2
        assert pump.resident_messages == 12
        pump.discard()
        assert pump.queued_messages == 0
        assert pump.resident_messages == 10
    finally:
        transport.hold.set()
        await pump.stop()
    assert pump.resident_messages == 0


@pytest.mark.asyncio
async def test_writer_cancel_mid_batch_releases_in_flight_resident() -> None:
    pump = _pump(max_messages=32)
    transport = await _hold_after_fill(pump, 8)
    assert pump.queued_messages == 0
    assert pump.resident_messages == 8
    await pump.stop()
    assert pump.resident_messages == 0
    assert pump.queued_bytes == 0
    transport.hold.set()


@pytest.mark.asyncio
async def test_writer_cancel_releases_in_flight_and_leaves_unextracted_queued() -> None:
    pump = _pump(max_messages=400)
    transport = await _hold_after_fill(pump, 300)
    assert pump.queued_messages == 44
    assert pump.resident_messages == 300
    await pump.stop()
    assert pump.resident_messages == 44
    assert pump.queued_messages == 44
    pump.discard()
    assert pump.resident_messages == 0
    assert pump.queued_messages == 0
    transport.hold.set()


@pytest.mark.asyncio
async def test_writer_failure_releases_in_flight_resident() -> None:
    failures: list[BaseException] = []

    async def record(exc: BaseException) -> None:
        failures.append(exc)

    pump = WritePump(max_bytes=1 << 20, max_messages=16, on_failure=record)
    for _ in range(4):
        assert pump.try_enqueue(b"x") is True
    transport = _BoomTransport()
    pump.start(transport)
    await asyncio.wait_for(transport.entered.wait(), timeout=1.0)
    for _ in range(8):
        await asyncio.sleep(0)
    assert pump.task is not None and pump.task.done()
    assert [type(exc).__name__ for exc in failures] == ["OSError"]
    assert pump.resident_messages == 0
    assert pump.queued_messages == 0


def test_oversized_single_item_only_when_resident_is_empty() -> None:
    pump = WritePump(max_bytes=1, max_messages=4, on_failure=_no_failure)
    assert pump.try_enqueue(b"oversized") is True
    assert pump.resident_messages == 1
    assert pump.try_enqueue(b"x") is False
    pump.discard()
    assert pump.resident_messages == 0
    assert pump.try_enqueue(b"still-oversized") is True


@pytest.mark.asyncio
async def test_oversized_item_refused_while_a_batch_is_in_flight() -> None:
    pump = WritePump(max_bytes=8, max_messages=8, on_failure=_no_failure)
    transport = await _hold_after_fill(pump, 4)
    try:
        assert pump.queued_messages == 0
        assert pump.resident_messages == 4
        assert pump.try_enqueue(b"way-too-big-for-the-byte-budget") is False
        assert pump.resident_messages == 4
    finally:
        transport.hold.set()
        await pump.stop()
    assert pump.resident_messages == 0
    assert pump.try_enqueue(b"way-too-big-for-the-byte-budget") is True


@pytest.mark.asyncio
async def test_eager_write_does_not_consume_resident_budget() -> None:
    transport = _EagerTransport()
    pump = _pump(max_messages=2)
    pump.start(transport)
    try:
        assert pump.try_enqueue(b"a") is True
        assert transport.written == [b"a"]
        assert pump.eager_writes == 1
        assert pump.resident_messages == 0
        assert pump.queued_messages == 0
        assert pump.queued_bytes == 0
        assert pump.try_enqueue(b"b") is True
        assert pump.resident_messages == 1
        assert pump.queued_messages == 1
    finally:
        await pump.stop()


def test_queued_messages_still_tracks_qsize_not_resident() -> None:
    pump = _pump(max_messages=8)
    assert pump.try_enqueue_many([b"a", b"b", b"c"]) is True
    assert pump.queued_messages == 3
    assert pump.stats().queued_messages == 3
    assert pump.resident_messages == 3
    pump.queue.get_nowait()
    assert pump.queued_messages == 2
    assert pump.stats().queued_messages == 2
    assert pump.resident_messages == 3
