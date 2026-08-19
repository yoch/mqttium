"""Targeted writer-waiter wakeups: capacity release wakes n = slots freed."""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

from mqttium.api._effects import StaleConnectionEffect
from mqttium.api._writer import WritePump


class _SlowTransport:
    def __init__(self, delay: float = 0.005) -> None:
        self.delay = delay
        self.written: list[bytes] = []

    async def write(self, data: bytes) -> None:
        await asyncio.sleep(self.delay)
        self.written.append(data)

    async def write_many(self, parts: list[bytes]) -> None:
        await asyncio.sleep(self.delay)
        self.written.extend(parts)

    async def read(self, n: int = 65536) -> bytes:
        return b""

    async def close(self) -> None:
        return None

    def is_closing(self) -> bool:
        return False


class _CallbackTransport:
    """Runs a callback after each awaited write, before the writer notifies."""

    def __init__(self) -> None:
        self.written: list[bytes] = []
        self.eager: list[bytes] = []
        self.after_write: Callable[[], None] | None = None

    def write_nowait(self, data: bytes) -> bool:
        self.eager.append(data)
        return True

    async def write(self, data: bytes) -> None:
        self.written.append(data)
        if self.after_write is not None:
            self.after_write()

    async def write_many(self, parts: list[bytes]) -> None:
        self.written.extend(parts)
        if self.after_write is not None:
            self.after_write()

    async def read(self, n: int = 65536) -> bytes:
        return b""

    async def close(self) -> None:
        return None

    def is_closing(self) -> bool:
        return False


class _GatedTransport:
    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.written: list[bytes] = []

    async def write(self, data: bytes) -> None:
        self.entered.set()
        await self.release.wait()
        self.written.append(data)

    async def write_many(self, parts: list[bytes]) -> None:
        await self.write(b"".join(parts))

    async def read(self, n: int = 65536) -> bytes:
        return b""

    async def close(self) -> None:
        return None

    def is_closing(self) -> bool:
        return False


class _FailingTransport:
    async def write(self, data: bytes) -> None:
        raise ConnectionResetError("connection lost")

    async def write_many(self, parts: list[bytes]) -> None:
        raise ConnectionResetError("connection lost")

    async def read(self, n: int = 65536) -> bytes:
        return b""

    async def close(self) -> None:
        return None

    def is_closing(self) -> bool:
        return False


async def _no_failure(exc: BaseException) -> None:
    raise AssertionError(f"unexpected writer failure: {exc!r}")


def _pump(*, max_bytes: int = 1 << 20, max_messages: int) -> WritePump:
    return WritePump(max_bytes=max_bytes, max_messages=max_messages, on_failure=_no_failure)


async def _wait_until(predicate: Callable[[], bool], *, timeout: float = 1.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() >= deadline:
            raise AssertionError("condition was not met before timeout")
        await asyncio.sleep(0)


async def _park_waiters(pump: WritePump, payloads: list[bytes]) -> list[asyncio.Task[None]]:
    tasks = [asyncio.create_task(pump.enqueue(payload)) for payload in payloads]
    await _wait_until(lambda: pump.waiters == len(payloads))
    return tasks


@pytest.mark.asyncio
async def test_single_waiter_proceeds_when_capacity_is_released() -> None:
    pump = _pump(max_messages=1)
    assert pump.try_enqueue(b"hold") is True
    waiter = asyncio.create_task(pump.enqueue(b"next"))
    await _wait_until(lambda: pump.waiters == 1)

    pump.start(_SlowTransport(delay=0.0))
    try:
        await asyncio.wait_for(waiter, timeout=1.0)
        await asyncio.wait_for(pump.join(), timeout=1.0)
    finally:
        await pump.stop()

    assert pump.waiters == 0
    assert pump.queued_messages == 0


@pytest.mark.asyncio
async def test_many_waiters_all_complete_while_writer_drains() -> None:
    pump = _pump(max_messages=2)
    assert pump.try_enqueue(b"a") is True
    assert pump.try_enqueue(b"b") is True
    waiters = await _park_waiters(pump, [f"w{i}".encode() for i in range(8)])

    pump.start(_SlowTransport(delay=0.002))
    try:
        await asyncio.wait_for(asyncio.gather(*waiters), timeout=2.0)
        await asyncio.wait_for(pump.join(), timeout=2.0)
    finally:
        await pump.stop()

    assert pump.waiters == 0
    assert pump.queued_messages == 0


@pytest.mark.asyncio
async def test_capacity_release_notifies_at_most_the_freed_slots() -> None:
    pump = _pump(max_messages=2)
    assert pump.try_enqueue_many([b"a", b"b"]) is True
    waiters = await _park_waiters(pump, [f"w{i}".encode() for i in range(8)])

    notifies: list[int] = []
    real_notify = pump.space.notify

    def record_notify(n: int = 1) -> None:
        notifies.append(n)
        real_notify(n)

    pump.space.notify = record_notify  # type: ignore[method-assign]

    pump.start(_SlowTransport(delay=0.0))
    try:
        await asyncio.wait_for(asyncio.gather(*waiters), timeout=2.0)
        await asyncio.wait_for(pump.join(), timeout=1.0)
    finally:
        await pump.stop()

    assert notifies
    assert notifies[0] == 2
    assert all(n <= 2 for n in notifies)


@pytest.mark.asyncio
async def test_epoch_advance_wakes_all_waiters() -> None:
    pump = _pump(max_messages=1)
    assert pump.try_enqueue(b"hold") is True
    waiters = await _park_waiters(pump, [b"a", b"b", b"c", b"d"])

    await pump.advance_epoch(pump.epoch + 1)

    results = await asyncio.gather(*waiters, return_exceptions=True)
    assert all(isinstance(result, StaleConnectionEffect) for result in results)
    assert pump.waiters == 0


@pytest.mark.asyncio
async def test_discard_and_wake_unblocks_waiters() -> None:
    pump = _pump(max_messages=2)
    assert pump.try_enqueue_many([b"hold-a", b"hold-b"]) is True
    waiters = await _park_waiters(pump, [b"a", b"b"])

    pump.discard()
    await pump.wake_waiters()

    await asyncio.wait_for(asyncio.gather(*waiters), timeout=1.0)
    assert pump.waiters == 0
    assert pump.queued_messages == 2


@pytest.mark.asyncio
async def test_stop_then_epoch_advance_does_not_strand_waiters() -> None:
    pump = _pump(max_messages=1)
    transport = _GatedTransport()
    pump.start(transport)
    try:
        await pump.enqueue(b"inflight")
        await asyncio.wait_for(transport.entered.wait(), timeout=1.0)
        pump.try_enqueue(b"hold")
        assert pump.try_enqueue(b"overflow") is False
        waiters = await _park_waiters(pump, [b"a", b"b"])
        await pump.stop()
        await pump.advance_epoch(pump.epoch + 1)
        results = await asyncio.gather(*waiters, return_exceptions=True)
        assert all(isinstance(result, StaleConnectionEffect) for result in results)
        assert pump.waiters == 0
    finally:
        transport.release.set()
        await pump.stop()


@pytest.mark.asyncio
async def test_transport_failure_epoch_wake_does_not_strand_waiters() -> None:
    failures: list[BaseException] = []
    pump = WritePump(max_bytes=1024, max_messages=1, on_failure=_no_failure)

    async def on_failure(exc: BaseException) -> None:
        failures.append(exc)
        await pump.advance_epoch(pump.epoch + 1)

    pump.on_failure = on_failure
    assert pump.try_enqueue(b"hold") is True
    waiters = await _park_waiters(pump, [b"a", b"b", b"c"])
    pump.start(_FailingTransport())
    try:
        results = await asyncio.wait_for(
            asyncio.gather(*waiters, return_exceptions=True), timeout=1.0
        )
        assert all(isinstance(result, StaleConnectionEffect) for result in results)
        assert pump.waiters == 0
        assert [type(exc).__name__ for exc in failures] == ["ConnectionResetError"]
    finally:
        await pump.stop()


@pytest.mark.asyncio
async def test_cancelled_waiter_does_not_consume_the_only_wakeup() -> None:
    pump = _pump(max_messages=1)
    transport = _CallbackTransport()
    assert pump.try_enqueue(b"hold") is True
    waiter_a = asyncio.create_task(pump.enqueue(b"a"))
    await _wait_until(lambda: pump.waiters == 1)
    waiter_b = asyncio.create_task(pump.enqueue(b"b"))
    await _wait_until(lambda: pump.waiters == 2)

    loop = asyncio.get_running_loop()
    transport.after_write = lambda: loop.call_soon(waiter_a.cancel)
    pump.start(transport)
    try:
        await asyncio.wait_for(waiter_b, timeout=1.0)
        with pytest.raises(asyncio.CancelledError):
            await waiter_a
        await asyncio.wait_for(pump.join(), timeout=1.0)
    finally:
        await pump.stop()

    assert waiter_a.cancelled()
    assert pump.waiters == 0
    assert b"b" in transport.written


@pytest.mark.asyncio
async def test_cancelled_waiter_while_still_parked_does_not_strand_peer() -> None:
    pump = _pump(max_messages=1)
    assert pump.try_enqueue(b"hold") is True
    waiter_a = asyncio.create_task(pump.enqueue(b"a"))
    await _wait_until(lambda: pump.waiters == 1)
    waiter_b = asyncio.create_task(pump.enqueue(b"b"))
    await _wait_until(lambda: pump.waiters == 2)

    waiter_a.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter_a
    await _wait_until(lambda: pump.waiters == 1)

    pump.start(_SlowTransport(delay=0.0))
    try:
        await asyncio.wait_for(waiter_b, timeout=1.0)
        await asyncio.wait_for(pump.join(), timeout=1.0)
    finally:
        await pump.stop()

    assert pump.waiters == 0


@pytest.mark.asyncio
async def test_no_waiter_starves_while_capacity_keeps_being_released() -> None:
    pump = _pump(max_messages=2)
    assert pump.try_enqueue_many([b"a", b"b"]) is True
    payloads = [f"w{i:02d}".encode() for i in range(16)]
    waiters = await _park_waiters(pump, payloads)

    pump.start(_SlowTransport(delay=0.001))
    try:
        await asyncio.wait_for(asyncio.gather(*waiters), timeout=3.0)
        await asyncio.wait_for(pump.join(), timeout=1.0)
    finally:
        await pump.stop()

    assert pump.waiters == 0
    assert pump.enqueue_suspensions >= 16


@pytest.mark.asyncio
async def test_eager_writes_stay_disabled_while_waiters_exist() -> None:
    pump = _pump(max_messages=1)
    transport = _CallbackTransport()
    assert pump.try_enqueue(b"hold") is True
    waiter = asyncio.create_task(pump.enqueue(b"waiter"))
    await _wait_until(lambda: pump.waiters == 1)

    sneak_ok = False
    sneak_eager = -1

    def sneak() -> None:
        nonlocal sneak_ok, sneak_eager
        transport.after_write = None
        eager_before = pump.eager_writes
        sneak_ok = pump.try_enqueue(b"sneak")
        sneak_eager = pump.eager_writes - eager_before

    loop = asyncio.get_running_loop()
    transport.after_write = lambda: loop.call_soon(sneak)
    pump.start(transport)
    try:
        await asyncio.wait_for(waiter, timeout=1.0)
        await asyncio.wait_for(pump.join(), timeout=1.0)
    finally:
        await pump.stop()

    assert sneak_ok is True
    assert sneak_eager == 0
    assert transport.eager == []
    assert pump.waiters == 0
