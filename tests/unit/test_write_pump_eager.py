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


@pytest.mark.parametrize("packet_type", (0x40, 0x50, 0x70))
async def test_success_ack_and_data_have_independent_eager_permits(packet_type: int) -> None:
    transport = _EagerTransport()
    pump = _pump()
    pump.start(transport)
    try:
        ack = bytes((packet_type, 2, 0, 1))
        assert pump.try_enqueue_ack(ack) is True
        assert pump.try_enqueue(b"\x30\x00") is True

        assert transport.written == [ack, b"\x30\x00"]
        assert pump.queued_messages == 0
        assert pump.eager_writes == 2
    finally:
        await pump.stop()


async def test_success_ack_eager_burst_is_bounded_to_one_per_turn() -> None:
    transport = _EagerTransport()
    pump = _pump()
    pump.start(transport)
    try:
        for mid in range(1, 17):
            assert pump.try_enqueue_ack(b"\x40\x02" + mid.to_bytes(2, "big")) is True

        assert len(transport.written) == 1
        assert pump.queued_messages == 15
        assert pump.eager_writes == 1
    finally:
        await pump.stop()


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


async def test_eager_write_is_declined_while_the_queue_is_not_empty() -> None:
    """A queued frame is always owed first, so order forbids overtaking it."""
    transport = _EagerTransport()
    pump = _pump()
    # No writer task: nothing drains the queue, so it stays non-empty.
    pump._write_nowait = transport.write_nowait
    try:
        assert pump.try_enqueue(b"queued") is True
        assert pump.try_enqueue(b"later") is True
        assert transport.written == []
        assert pump.queued_messages == 2
        assert pump.eager_writes == 0
    finally:
        pump.discard()


async def test_eager_write_is_declined_while_a_write_is_in_flight() -> None:
    transport = _EagerTransport()
    pump = _pump()
    pump._write_nowait = transport.write_nowait
    pump._writing = True
    assert pump.try_enqueue(b"a") is True
    assert transport.written == []
    assert pump.queued_messages == 1
    assert pump.eager_writes == 0


async def test_eager_write_is_declined_while_a_producer_is_waiting() -> None:
    """A producer suspended for queue space must not be overtaken."""
    transport = _EagerTransport()
    pump = _pump()
    pump._write_nowait = transport.write_nowait
    pump.waiters = 1
    assert pump.try_enqueue(b"a") is True
    assert transport.written == []
    assert pump.eager_writes == 0


async def test_segmented_items_are_never_written_eagerly() -> None:
    transport = _EagerTransport()
    pump = _pump()
    pump._write_nowait = transport.write_nowait
    assert pump.try_enqueue((b"header", b"payload")) is True
    assert transport.written == []
    assert pump.queued_messages == 1
    assert pump.eager_writes == 0


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


async def test_only_first_write_of_a_synchronous_burst_is_eager() -> None:
    """A tight producer seeds once, then wakes the writer's batching path."""
    transport = _EagerTransport()
    pump = _pump(max_messages=32, max_bytes=64)
    pump.start(transport)
    try:
        for _ in range(20):
            assert pump.try_enqueue(b"x") is True

        # No yield yet: exactly the first frame bypassed the task. The rest are
        # visible together for one writer batch instead of 19 more eager writes.
        assert transport.written == [b"x"]
        assert pump.eager_writes == 1
        assert pump.queued_messages == 19
        assert pump.queued_bytes == 19

        await pump.join()
        assert transport.written == [b"x"] * 20
        assert pump.batches == 1
        assert pump.batched_items == 19
    finally:
        await pump.stop()


async def test_synchronous_burst_respects_queue_capacity_after_first_eager() -> None:
    """Eager is not an escape hatch from normal burst backpressure."""
    transport = _EagerTransport()
    pump = _pump(max_messages=2, max_bytes=8)
    pump.start(transport)
    try:
        assert pump.try_enqueue(b"1") is True  # eager
        assert pump.try_enqueue(b"2") is True  # queued
        assert pump.try_enqueue(b"3") is True  # queued, queue now full
        assert pump.try_enqueue(b"4") is False

        assert transport.written == [b"1"]
        assert pump.eager_writes == 1
        assert pump.queued_messages == 2
        assert pump.queued_bytes == 2

        await pump.join()
        assert transport.written == [b"1", b"2", b"3"]
    finally:
        await pump.stop()


async def test_paced_writes_rearm_eager_on_the_next_loop_turn() -> None:
    """Yielding between writes keeps the latency win that eager was added for."""
    transport = _EagerTransport()
    pump = _pump()
    pump.start(transport)
    try:
        for index in range(5):
            frame = str(index).encode()
            assert pump.try_enqueue(frame) is True
            # Each paced frame reaches the transport on the producer stack.
            assert transport.written == [str(i).encode() for i in range(index + 1)]
            assert pump.queued_messages == 0
            await asyncio.sleep(0)

        assert pump.eager_writes == 5
        assert pump.batches == 0
    finally:
        await pump.stop()


async def test_stale_rearm_cannot_arm_a_new_transport_generation() -> None:
    old = _EagerTransport()
    pump = _pump()
    pump.start(old)
    stale_generation = pump._eager_generation
    await pump.stop()

    new = _EagerTransport()
    pump.start(new)
    try:
        current_generation = pump._eager_generation
        pump._eager_armed = False

        # Model a delayed call_soon callback left by the old connection.
        pump._rearm_eager_if_idle(stale_generation)
        assert pump._eager_armed is False

        pump._rearm_eager_if_idle(current_generation)
        assert pump._eager_armed is True
    finally:
        await pump.stop()


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


class _FailingTransport(_EagerTransport):
    """Fails every awaited write, and records anything written after close."""

    def __init__(self) -> None:
        super().__init__()
        self.closed = False

    def write_nowait(self, data: bytes) -> bool:
        self.calls.append(("after-close" if self.closed else "eager", data))
        return True

    async def write(self, data: bytes) -> None:
        raise ConnectionResetError("connection lost")

    async def write_many(self, parts: list[bytes]) -> None:
        raise ConnectionResetError("connection lost")

    async def close(self) -> None:
        self.closed = True

    def is_closing(self) -> bool:
        return self.closed


async def test_writer_failure_drops_the_eager_binding() -> None:
    """A dead transport must not stay reachable through the eager path.

    The reconnect path does not call `stop()` — `AsyncClient` only stops the
    pump when it will *not* reconnect — so the pump has to release the binding
    itself as soon as the writer sees the transport fail. The connection epoch
    is the primary guard; this is the one that does not depend on how promptly
    the reader's teardown runs.
    """
    failures: list[BaseException] = []

    async def record(exc: BaseException) -> None:
        failures.append(exc)

    transport = _FailingTransport()
    pump = WritePump(max_bytes=1 << 20, max_messages=100, on_failure=record)
    pump.start(transport)

    # Force the awaited writer path, but use the pump's admission API so all
    # owned counters describe the frame the failing transport is about to see.
    pump._eager_armed = False
    assert pump.try_enqueue(b"boom") is True
    for _ in range(8):
        await asyncio.sleep(0)

    assert pump.task is not None and pump.task.done()
    assert [type(exc).__name__ for exc in failures] == ["ConnectionResetError"]
    await transport.close()

    # Same-epoch publish arriving during the outage, before reset()/start().
    assert pump.try_enqueue(b"during-outage") is True
    assert pump._write_nowait is None
    assert not [data for kind, data in transport.calls if kind == "after-close"]
    assert pump.queued_messages == 1


async def test_reset_drops_the_eager_binding_until_the_next_start() -> None:
    old = _EagerTransport()
    pump = _pump()
    pump.start(old)
    await pump.stop()

    pump.reset()
    assert pump._write_nowait is None
    assert pump.try_enqueue(b"between-connections") is True
    assert old.written == []

    new = _EagerTransport()
    pump.start(new)
    try:
        # The queued frame goes out on the new transport, the eager path is live
        # again, and the old transport is never touched.
        await pump.join()
        assert pump.try_enqueue(b"after-reconnect") is True
        assert new.written == [b"between-connections", b"after-reconnect"]
        assert old.written == []
    finally:
        await pump.stop()


async def test_discard_drops_the_eager_binding() -> None:
    transport = _EagerTransport()
    pump = _pump()
    pump.start(transport)
    await pump.stop()
    pump.start(transport)
    pump.discard()
    assert pump._write_nowait is None
    assert pump.try_enqueue(b"after-discard") is True
    assert transport.written == []
    await pump.stop()


async def test_large_payloads_fall_back_to_the_queued_path() -> None:
    """Payloads past SEGMENT_THRESHOLD keep exactly their pre-eager behaviour.

    Two thresholds disengage the eager path as frames grow, and both matter:
    a segmented `(header, payload)` item is never eager, and `write_nowait`
    declines once the socket buffer is above its high-water mark. Large
    publications therefore take the same path they always did.
    """

    class _BufferedTransport(_EagerTransport):
        def __init__(self) -> None:
            super().__init__()
            self.buffered = 0

        def write_nowait(self, data: bytes) -> bool:
            if self.buffered > 64 * 1024:
                return False
            self.buffered += len(data)
            return super().write_nowait(data)

        async def write(self, data: bytes) -> None:
            self.buffered = 0
            await super().write(data)

        async def write_many(self, parts: list[bytes]) -> None:
            self.buffered = 0
            await super().write_many(parts)

    transport = _BufferedTransport()
    pump = _pump(max_bytes=64 * 1024 * 1024, max_messages=10_000)
    pump.start(transport)
    try:
        # Segmented items never take the eager path, whatever the queue state.
        for _ in range(5):
            pump.try_enqueue((b"header", b"x" * (200 * 1024)))
        await pump.join()
        assert pump.eager_writes == 0

        # A small frame is eager again once the queue has drained.
        assert pump.try_enqueue(b"small") is True
        assert pump.eager_writes == 1
    finally:
        await pump.stop()
