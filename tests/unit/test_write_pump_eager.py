"""Regression tests for the eager (writer-task-bypassing) write path.

The pump may write straight to the transport when the writer task would only
add an event-loop turn. That relaxes the single-*task* writer rule to a single
*in-flight* write, so every test here exists to pin the part that did not
change: wire order, and the impossibility of interleaving.
"""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api._effects import StaleConnectionEffect
from mqttium.api._writer import WritePump


class _EagerTransport:
    """Records every write in call order, and can decline the eager path."""

    def __init__(self, *, accept_eager: bool = True) -> None:
        self.calls: list[tuple[str, bytes]] = []
        self.accept_eager = accept_eager
        self.in_flight = False
        self.max_in_flight = 0

    def write_nowait(self, data: bytes) -> bool:
        if not self.accept_eager:
            return False
        assert not self.in_flight, "eager write landed inside another write"
        self.calls.append(("eager", data))
        return True

    async def write(self, data: bytes) -> None:
        self._enter()
        try:
            await asyncio.sleep(0)
            self.calls.append(("write", data))
        finally:
            self._exit()

    async def write_many(self, parts: list[bytes]) -> None:
        self._enter()
        try:
            await asyncio.sleep(0)
            for part in parts:
                self.calls.append(("write", part))
        finally:
            self._exit()

    def _enter(self) -> None:
        self.in_flight = True
        self.max_in_flight = max(self.max_in_flight, 1)

    def _exit(self) -> None:
        self.in_flight = False

    async def read(self, n: int = 65536) -> bytes:
        return b""

    async def close(self) -> None:
        return None

    def is_closing(self) -> bool:
        return False

    @property
    def written(self) -> list[bytes]:
        return [data for _kind, data in self.calls]


class _PlainTransport(_EagerTransport):
    """A transport that offers no eager path at all, like WebSocket."""

    write_nowait = None  # type: ignore[assignment]


async def _no_failure(exc: BaseException) -> None:  # pragma: no cover - failure path
    raise AssertionError(f"unexpected writer failure: {exc!r}")


def _pump(**kwargs: int) -> WritePump:
    return WritePump(
        max_bytes=kwargs.get("max_bytes", 1 << 20),
        max_messages=kwargs.get("max_messages", 1024),
        on_failure=_no_failure,
    )


@pytest.mark.asyncio
async def test_eager_write_reaches_the_transport_without_a_loop_turn() -> None:
    transport = _EagerTransport()
    pump = _pump()
    pump.start(transport)
    try:
        assert pump.try_enqueue(b"a") is True
        # No await: the byte is already at the transport.
        assert transport.written == [b"a"]
        assert pump.queued_messages == 0
        assert pump.queued_bytes == 0
        assert pump.eager_writes == 1
        assert pump.eager_bytes == 1
    finally:
        await pump.stop()


@pytest.mark.asyncio
async def test_eager_write_updates_last_outbound_for_keepalive() -> None:
    transport = _EagerTransport()
    pump = _pump()
    pump.start(transport)
    try:
        assert pump.last_outbound == 0.0
        pump.try_enqueue(b"a")
        assert pump.last_outbound > 0.0
    finally:
        await pump.stop()


@pytest.mark.asyncio
async def test_eager_write_is_declined_while_the_queue_is_not_empty() -> None:
    """A queued frame is always owed first, so order forbids overtaking it."""
    transport = _EagerTransport()
    pump = _pump()
    # No writer task: nothing drains the queue, so it stays non-empty.
    pump._write_nowait = transport.write_nowait
    try:
        pump.queue.put_nowait(b"queued")
        pump.queued_bytes = len(b"queued")
        assert pump.try_enqueue(b"later") is True
        assert transport.written == []
        assert pump.queued_messages == 2
        assert pump.eager_writes == 0
    finally:
        pump.discard()


@pytest.mark.asyncio
async def test_eager_write_is_declined_while_a_write_is_in_flight() -> None:
    transport = _EagerTransport()
    pump = _pump()
    pump._write_nowait = transport.write_nowait
    pump._writing = True
    assert pump.try_enqueue(b"a") is True
    assert transport.written == []
    assert pump.queued_messages == 1
    assert pump.eager_writes == 0


@pytest.mark.asyncio
async def test_eager_write_is_declined_while_a_producer_is_waiting() -> None:
    """A producer suspended for queue space must not be overtaken."""
    transport = _EagerTransport()
    pump = _pump()
    pump._write_nowait = transport.write_nowait
    pump.waiters = 1
    assert pump.try_enqueue(b"a") is True
    assert transport.written == []
    assert pump.eager_writes == 0


@pytest.mark.asyncio
async def test_segmented_items_are_never_written_eagerly() -> None:
    transport = _EagerTransport()
    pump = _pump()
    pump._write_nowait = transport.write_nowait
    assert pump.try_enqueue((b"header", b"payload")) is True
    assert transport.written == []
    assert pump.queued_messages == 1
    assert pump.eager_writes == 0


@pytest.mark.asyncio
async def test_transport_without_write_nowait_never_takes_the_eager_path() -> None:
    transport = _PlainTransport()
    pump = _pump()
    pump.start(transport)
    try:
        assert pump.try_enqueue(b"a") is True
        assert pump.queued_messages == 1
        assert pump.eager_writes == 0
        await pump.join()
        assert transport.written == [b"a"]
    finally:
        await pump.stop()


@pytest.mark.asyncio
async def test_transport_declining_the_eager_write_falls_back_to_the_queue() -> None:
    """A drain is due, so the frame goes back to the task that can await one."""
    transport = _EagerTransport(accept_eager=False)
    pump = _pump()
    pump.start(transport)
    try:
        assert pump.try_enqueue(b"a") is True
        assert pump.eager_writes == 0
        assert pump.queued_messages == 1
        assert pump.queued_bytes == 1
        await pump.join()
        assert transport.written == [b"a"]
    finally:
        await pump.stop()


@pytest.mark.asyncio
async def test_wire_order_is_preserved_across_eager_and_queued_frames() -> None:
    transport = _EagerTransport()
    pump = _pump()
    pump.start(transport)
    try:
        # First frame is eager (queue empty, nothing in flight).
        pump.try_enqueue(b"1")
        # Segmented item is never eager, so it queues and wakes the writer.
        pump.try_enqueue((b"2a", b"2b"))
        # Queue is now non-empty, so this queues behind it.
        pump.try_enqueue(b"3")
        await pump.join()
        # And this one is eager again, after the queue drained.
        pump.try_enqueue(b"4")

        assert transport.written == [b"1", b"2a", b"2b", b"3", b"4"]
        assert transport.max_in_flight <= 1
    finally:
        await pump.stop()


@pytest.mark.asyncio
async def test_eager_write_cannot_land_inside_a_segmented_write() -> None:
    """The `_writing` flag exists for exactly this case.

    A segmented item is written as two consecutive awaits. A frame landing
    between them would be spliced into the middle of an MQTT packet.
    """
    transport = _EagerTransport()
    pump = _pump()
    pump.start(transport)
    try:
        pump.try_enqueue((b"header", b"payload"))
        # Interleave publishes with loop turns while the segmented write runs.
        for index in range(6):
            await asyncio.sleep(0)
            pump.try_enqueue(f"t{index}".encode())
        await pump.join()

        written = transport.written
        header_at = written.index(b"header")
        assert written[header_at + 1] == b"payload"
        assert transport.max_in_flight <= 1
        # Everything published afterwards followed the whole packet.
        assert written[header_at + 2 :] == [f"t{index}".encode() for index in range(6)]
    finally:
        await pump.stop()


@pytest.mark.asyncio
async def test_eager_writes_do_not_consume_queue_capacity() -> None:
    """They never enter the queue, so they must not be accounted against it."""
    transport = _EagerTransport()
    pump = _pump(max_messages=2, max_bytes=8)
    pump.start(transport)
    try:
        for _ in range(20):
            assert pump.try_enqueue(b"x") is True
        assert pump.queued_messages == 0
        assert pump.queued_bytes == 0
        assert pump.eager_writes == 20
        assert transport.written == [b"x"] * 20
    finally:
        await pump.stop()


@pytest.mark.asyncio
async def test_eager_path_still_refuses_a_stale_epoch() -> None:
    transport = _EagerTransport()
    pump = _pump()
    pump.start(transport)
    pump.epoch = 3
    try:
        with pytest.raises(StaleConnectionEffect):
            pump.try_enqueue(b"x", epoch=2)
        assert transport.written == []
    finally:
        await pump.stop()


@pytest.mark.asyncio
async def test_stopping_drops_the_eager_path_with_its_transport() -> None:
    """A frame must never reach a transport this pump no longer owns."""
    transport = _EagerTransport()
    pump = _pump()
    pump.start(transport)
    await pump.stop()

    assert pump._write_nowait is None
    assert pump.try_enqueue(b"after-stop") is True
    assert transport.written == []
    assert pump.queued_messages == 1


@pytest.mark.asyncio
async def test_batched_frames_are_not_counted_as_eager() -> None:
    """try_enqueue_many keeps its atomic all-or-nothing queue admission."""
    transport = _EagerTransport()
    pump = _pump()
    pump.start(transport)
    try:
        assert pump.try_enqueue_many([b"a", b"b", b"c"]) is True
        assert pump.eager_writes == 0
        assert pump.queued_messages == 3
        await pump.join()
        assert transport.written == [b"a", b"b", b"c"]
    finally:
        await pump.stop()
