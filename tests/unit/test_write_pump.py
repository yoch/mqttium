from __future__ import annotations

import asyncio

import pytest

from mqttium.api._effects import StaleConnectionEffect
from mqttium.api._writer import WritePump


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


async def _no_failure(exc: BaseException) -> None:
    raise AssertionError(f"unexpected writer failure: {exc!r}")


def test_write_pump_allows_one_oversized_item_only_when_empty() -> None:
    pump = WritePump(max_bytes=1, max_messages=2, on_failure=_no_failure)

    assert pump.try_enqueue(b"oversized") is True
    assert pump.try_enqueue(b"x") is False
    assert pump.queued_messages == 1
    assert pump.queued_bytes == len(b"oversized")


def test_write_pump_rejects_stale_epoch() -> None:
    pump = WritePump(max_bytes=1024, max_messages=2, on_failure=_no_failure)
    pump.epoch = 3

    with pytest.raises(StaleConnectionEffect):
        pump.try_enqueue(b"x", epoch=2)


@pytest.mark.asyncio
async def test_write_pump_preserves_batch_and_segment_order() -> None:
    transport = _RecordingTransport()
    pump = WritePump(max_bytes=1024, max_messages=8, on_failure=_no_failure)
    pump.start(transport)
    try:
        await pump.enqueue(b"a")
        await pump.enqueue(b"b")
        await pump.enqueue((b"header", b"payload"))
        await asyncio.wait_for(pump.join(), timeout=1.0)
    finally:
        await pump.stop()

    assert transport.calls == [
        ("many", (b"a", b"b")),
        ("write", b"header"),
        ("write", b"payload"),
    ]
    assert pump.queued_bytes == 0
    assert pump.last_outbound > 0


def test_write_pump_batch_admission_is_atomic_on_capacity_failure() -> None:
    pump = WritePump(max_bytes=10, max_messages=2, on_failure=_no_failure)
    assert pump.try_enqueue(b"12345") is True

    assert pump.try_enqueue_many([b"123", b"456"]) is False

    assert pump.queued_messages == 1
    assert pump.queued_bytes == 5
    assert pump.queue.get_nowait() == b"12345"


def test_write_pump_batch_admission_updates_exact_accounting() -> None:
    pump = WritePump(max_bytes=32, max_messages=4, on_failure=_no_failure)

    assert pump.try_enqueue_many([b"a", (b"head", b"payload")]) is True

    assert pump.queued_messages == 2
    assert pump.queued_bytes == 1 + len(b"head") + len(b"payload")
