"""Bounded ordered transport writer.

``WritePump`` owns the runtime write queue, byte/count backpressure, batching,
writer task and last successful write timestamp. ``AsyncClient`` keeps
transport lifecycle and decides how a writer failure affects protocol state.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable

from mqttium.api._effects import StaleConnectionEffect
from mqttium.errors import FlowControlError
from mqttium.transport._stream import AsyncTransport
from mqttium.transport.writes import WriteItem, item_size

WriterFailureHandler = Callable[[BaseException], Awaitable[None]]


class WritePump:
    """Serialize transport writes and own their bounded queue invariant."""

    def __init__(
        self,
        *,
        max_bytes: int,
        max_messages: int,
        on_failure: WriterFailureHandler,
    ) -> None:
        self.max_bytes = max_bytes
        self.max_messages = max_messages
        self.on_failure = on_failure
        self.queue: asyncio.Queue[WriteItem] = asyncio.Queue()
        self.queued_bytes = 0
        self.high_water_messages = 0
        self.high_water_bytes = 0
        self.space = asyncio.Condition()
        self.waiters = 0
        self.task: asyncio.Task[None] | None = None
        self.transport: AsyncTransport | None = None
        self.epoch = 0
        self.last_outbound = 0.0

    @property
    def queued_messages(self) -> int:
        return self.queue.qsize()

    def reset(self) -> None:
        """Start a new transport epoch with an empty queue."""
        self.queue = asyncio.Queue()
        self.queued_bytes = 0

    def start(self, transport: AsyncTransport) -> None:
        task = self.task
        if task is not None and not task.done():
            raise RuntimeError("WritePump is already running")
        self.transport = transport
        self.task = asyncio.create_task(self._run(), name="mqttium-writer")

    async def stop(self) -> None:
        task = self.task
        if task is None:
            self.transport = None
            return
        if task is asyncio.current_task():
            return
        if not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self.task = None
        self.transport = None

    async def join(self) -> None:
        await self.queue.join()

    async def advance_epoch(self, epoch: int) -> None:
        self.epoch = epoch
        await self.wake_waiters()

    async def wake_waiters(self) -> None:
        async with self.space:
            self.space.notify_all()

    def discard(self) -> None:
        while True:
            try:
                self.queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            else:
                self.queue.task_done()
        self.queued_bytes = 0

    def can_enqueue_size(
        self,
        size: int,
        *,
        queued_messages: int | None = None,
        queued_bytes: int | None = None,
    ) -> bool:
        messages = self.queue.qsize() if queued_messages is None else queued_messages
        bytes_used = self.queued_bytes if queued_bytes is None else queued_bytes
        if messages >= self.max_messages:
            return False
        return bytes_used + size <= self.max_bytes or (messages == 0 and bytes_used == 0)

    def try_enqueue(self, item: WriteItem, *, epoch: int | None = None) -> bool:
        if epoch is None:
            epoch = self.epoch
        if epoch != self.epoch:
            raise StaleConnectionEffect
        size = item_size(item)
        if not self.can_enqueue_size(size):
            return False
        self.queue.put_nowait(item)
        self.queued_bytes += size
        return True

    async def enqueue(
        self,
        item: WriteItem,
        *,
        nowait: bool = False,
        epoch: int | None = None,
    ) -> None:
        if epoch is None:
            epoch = self.epoch
        if self.try_enqueue(item, epoch=epoch):
            return
        size = item_size(item)
        if nowait:
            raise FlowControlError("Outbound backpressure limit reached")

        async with self.space:
            while True:
                if epoch != self.epoch:
                    raise StaleConnectionEffect
                messages_full = self.queue.qsize() >= self.max_messages
                # Allow a single oversized item into an empty queue (segmented
                # payloads can exceed max_bytes by the MQTT header).
                bytes_blocked = self.queued_bytes + size > self.max_bytes and not (
                    self.queued_bytes == 0 and self.queue.empty()
                )
                if not messages_full and not bytes_blocked:
                    break
                # Escape hatch: if the writer is idle but space accounting is
                # wedged, do not block protocol forever.
                if self.queue.empty() and self.queued_bytes == 0:
                    break
                self.waiters += 1
                try:
                    await self.space.wait()
                finally:
                    self.waiters -= 1
            if epoch != self.epoch:
                raise StaleConnectionEffect
            self.queue.put_nowait(item)
            self.queued_bytes += size

    async def _write_contiguous(self, transport: AsyncTransport, parts: list[bytes]) -> None:
        if not parts:
            return
        write_many = getattr(transport, "write_many", None)
        if write_many is not None:
            await write_many(parts)
        else:
            for part in parts:
                await transport.write(part)
        parts.clear()

    async def _run(self) -> None:
        transport = self.transport
        if transport is None:
            raise RuntimeError("WritePump started without a transport")
        queue = self.queue
        try:
            while True:
                first = await queue.get()
                queued_messages = queue.qsize() + 1
                if queued_messages > self.high_water_messages:
                    self.high_water_messages = queued_messages
                if self.queued_bytes > self.high_water_bytes:
                    self.high_water_bytes = self.queued_bytes
                batch: list[WriteItem] = [first]
                while len(batch) < 256:
                    try:
                        batch.append(queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break
                try:
                    # Coalesce contiguous small frames into one writelines call.
                    # Never await drain() after the batch (deadlocks vs reader ACK).
                    contiguous: list[bytes] = []
                    for data in batch:
                        if isinstance(data, tuple):
                            await self._write_contiguous(transport, contiguous)
                            for part in data:
                                await transport.write(part)
                        else:
                            contiguous.append(data)
                    await self._write_contiguous(transport, contiguous)
                    self.last_outbound = time.monotonic()
                finally:
                    released = 0
                    for data in batch:
                        released += item_size(data)
                        queue.task_done()
                    async with self.space:
                        self.queued_bytes = max(0, self.queued_bytes - released)
                        if self.waiters:
                            self.space.notify_all()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self.on_failure(exc)
