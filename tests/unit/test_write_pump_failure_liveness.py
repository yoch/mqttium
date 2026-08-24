"""WritePump failures wake admission waiters without admitting dead work."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api._effects import StaleConnectionEffect
from mqttium.api._writer import WritePump


class _BlockedFailureTransport:
    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def write(self, data: bytes) -> None:
        del data
        self.entered.set()
        await self.release.wait()
        raise ConnectionResetError("controlled writer failure")

    async def read(self, n: int = 65536) -> bytes:
        del n
        await asyncio.Event().wait()
        return b""

    async def close(self) -> None:
        return None

    def is_closing(self) -> bool:
        return False


async def _wait_for_waiter(pump: WritePump) -> None:
    for _ in range(100):
        if pump.waiters == 1:
            return
        await asyncio.sleep(0)
    raise AssertionError("writer admission waiter did not park")


async def test_failed_write_invalidates_and_wakes_admission_waiter() -> None:
    failures: list[BaseException] = []

    async def on_failure(exc: BaseException) -> None:
        failures.append(exc)

    pump = WritePump(max_bytes=1024, max_messages=1, on_failure=on_failure)
    transport = _BlockedFailureTransport()
    pump.start(transport)
    initial_epoch = pump.epoch
    try:
        await pump.enqueue(b"active")
        await asyncio.wait_for(transport.entered.wait(), timeout=1)
        waiter = asyncio.create_task(pump.enqueue(b"must-not-be-admitted"))
        await _wait_for_waiter(pump)

        transport.release.set()
        with pytest.raises(StaleConnectionEffect):
            await asyncio.wait_for(waiter, timeout=1)

        assert pump.epoch == initial_epoch + 1
        assert pump.waiters == 0
        assert pump.queue.empty()
        assert pump.queued_bytes == 0
        assert pump.resident_messages == 0
        assert len(failures) == 1
        assert isinstance(failures[0], ConnectionResetError)
    finally:
        transport.release.set()
        await pump.stop()
