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
from mqttium.api.stats import WriterStats
from mqttium.transport._stream import AsyncTransport
from mqttium.transport.writes import WriteItem, item_size

WriterFailureHandler = Callable[[BaseException], Awaitable[None]]

_LATENCY_BATCH_MIN_ITEMS = 4
_LATENCY_BATCH_MAX_ITEMS = 16
_LATENCY_BATCH_TARGET_BYTES = 48 * 1024
# Writer-task quantum. Distinct from the latency-microbatch target above: that
# path flushes an already-queued QoS 1/2 burst through write_nowait; this cap
# only bounds how much the writer task will take from the queue in one turn.
_WRITER_BATCH_MAX_ITEMS = 256
_WRITER_BATCH_MAX_BYTES = 64 * 1024


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
        # Decision counters. The writer's batching strategy cannot be changed
        # honestly without knowing how often it actually batches, how large the
        # batches get, and how often a producer had to wait — so they are
        # recorded before any of that is tuned. All are plain increments on
        # paths that already allocate or await.
        self.batches = 0
        self.batched_items = 0
        self.batched_bytes = 0
        self.segmented_writes = 0
        self.enqueue_suspensions = 0
        self.eager_writes = 0
        self.eager_bytes = 0
        # True for the whole of the writer task's batch, including between the
        # two halves of a segmented write. Nothing may write to the transport
        # while it is set.
        self._writing = False
        # Resolved once per connection: absent on transports that do not offer
        # a non-awaiting write.
        self._write_nowait: Callable[[bytes], bool] | None = None
        # At most one eager write may happen before the event loop regains
        # control. A tight producer therefore seeds the transport immediately,
        # then queues the rest so the writer task can coalesce them. A paced
        # producer is re-armed on the next loop turn while the writer is idle.
        self._eager_armed = False
        # Delayed re-arm callbacks carry this generation. Lifecycle changes
        # invalidate it so a callback from an old transport can never arm a new
        # connection.
        self._eager_generation = 0
        # Item that did not fit the current writer batch. asyncio.Queue has no
        # peek/putleft: putting it back would append at the tail and break FIFO,
        # so the pump holds it as the first item of the next batch.
        self._held: WriteItem | None = None

    @property
    def queued_messages(self) -> int:
        # qsize() undercounts by one while a byte-quantum leftover is held: that
        # item was already get()'d, but it is still owed to the wire.
        return self.queue.qsize() + (1 if self._held is not None else 0)

    def stats(self) -> WriterStats:
        """Snapshot the queue and the batching decisions taken so far."""
        queued_messages = self.queued_messages
        return WriterStats(
            queued_messages=queued_messages,
            queued_bytes=self.queued_bytes,
            high_water_messages=max(self.high_water_messages, queued_messages),
            high_water_bytes=max(self.high_water_bytes, self.queued_bytes),
            max_messages=self.max_messages,
            max_bytes=self.max_bytes,
            waiters=self.waiters,
            last_outbound=self.last_outbound,
            batches=self.batches,
            batched_items=self.batched_items,
            batched_bytes=self.batched_bytes,
            segmented_writes=self.segmented_writes,
            enqueue_suspensions=self.enqueue_suspensions,
            eager_writes=self.eager_writes,
            eager_bytes=self.eager_bytes,
        )

    def _sample_high_water(self, queued_messages: int | None = None) -> None:
        messages = self.queued_messages if queued_messages is None else queued_messages
        if messages > self.high_water_messages:
            self.high_water_messages = messages
        if self.queued_bytes > self.high_water_bytes:
            self.high_water_bytes = self.queued_bytes

    def _drop_eager_binding(self) -> None:
        self._eager_generation += 1
        self._eager_armed = False
        self._write_nowait = None

    def _rearm_eager_if_idle(self, generation: int) -> None:
        if (
            generation == self._eager_generation
            and self._write_nowait is not None
            and not self._writing
            and self._held is None
            and self.queue.empty()
        ):
            self._eager_armed = True

    def reset(self) -> None:
        """Start a new transport epoch with an empty queue."""
        self._sample_high_water()
        # Abandon the leftover with the old queue: join() waiters of the
        # previous epoch are not transferred, matching items still sitting in
        # the replaced queue.
        self._held = None
        self.queue = asyncio.Queue()
        self.queued_bytes = 0
        # The next start() rebinds it. Until then there is no transport this
        # pump may write to.
        self._drop_eager_binding()
        self._writing = False

    def start(self, transport: AsyncTransport) -> None:
        task = self.task
        if task is not None and not task.done():
            raise RuntimeError("WritePump is already running")
        self.transport = transport
        self._writing = False
        self._eager_generation += 1
        self._write_nowait = getattr(transport, "write_nowait", None)
        self._eager_armed = self._write_nowait is not None
        self.task = asyncio.create_task(self._run(), name="mqttium-writer")

    async def stop(self) -> None:
        task = self.task
        # Invalidate the producer-side path before the first await. A pending
        # re-arm callback from this connection then becomes harmless even if the
        # writer task cancellation yields back to the loop.
        self._drop_eager_binding()
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
        self._writing = False

    async def join(self) -> None:
        await self.queue.join()

    async def advance_epoch(self, epoch: int) -> None:
        self.epoch = epoch
        await self.wake_waiters()

    async def wake_waiters(self) -> None:
        async with self.space:
            self.space.notify_all()

    def discard(self) -> None:
        self._sample_high_water()
        self._drop_eager_binding()
        self._writing = False
        held = self._held
        self._held = None
        if held is not None:
            # Already get()'d, so join() still owes a task_done.
            self.queue.task_done()
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
        messages = self.queued_messages if queued_messages is None else queued_messages
        bytes_used = self.queued_bytes if queued_bytes is None else queued_bytes
        if messages >= self.max_messages:
            return False
        return bytes_used + size <= self.max_bytes or (messages == 0 and bytes_used == 0)

    def refusal(
        self,
        size: int = 0,
        *,
        queued_messages: int | None = None,
        queued_bytes: int | None = None,
    ) -> str:
        """Name the bound that is refusing, for the error path only.

        The two bounds default to very different magnitudes (10 000 messages
        against 1 MiB), so a caller who sized the queue in messages meets the
        byte bound first as soon as payloads grow. Saying which one refused is
        the difference between a tunable and a mystery.

        The overrides mirror :meth:`can_enqueue_size`: a batch is admitted
        against running totals rather than against the live queue, so the two
        must be told the same state or this names the wrong bound.
        """
        messages = self.queued_messages if queued_messages is None else queued_messages
        bytes_used = self.queued_bytes if queued_bytes is None else queued_bytes
        if messages >= self.max_messages:
            return (
                "Outbound backpressure limit reached: "
                f"max_outbound_messages={self.max_messages} is full"
            )
        return (
            "Outbound backpressure limit reached: "
            f"max_outbound_bytes={self.max_bytes} would be exceeded by a "
            f"{size}-byte write with {bytes_used} bytes already queued"
        )

    def refusal_many(self, items: list[WriteItem]) -> str:
        """Name the bound that refused a batch, for the error path only.

        Replays the running totals ``try_enqueue_many`` admits against, so the
        reported bound is the one that stopped the batch rather than whatever
        the live queue happens to show.
        """
        messages = self.queued_messages
        bytes_used = self.queued_bytes
        for item in items:
            size = item_size(item)
            if not self.can_enqueue_size(
                size,
                queued_messages=messages,
                queued_bytes=bytes_used,
            ):
                return self.refusal(size, queued_messages=messages, queued_bytes=bytes_used)
            messages += 1
            bytes_used += size
        return self.refusal()

    def _try_write_eager(self, item: WriteItem) -> bool:
        """Write straight through when the writer task would only add a hop.

        Every queued frame costs one event-loop turn: the writer task has to be
        scheduled out of ``await queue.get()`` before a single byte moves. When
        nothing is queued and no write is in flight, that turn buys nothing.

        ``_eager_armed`` limits that shortcut to the first write before the loop
        regains control. A synchronous producer burst therefore wakes the writer
        on its second frame and lets the existing batch path do the throughput
        work, while a paced producer can still take the zero-hop path each time.

        Wire order is preserved because the original conditions still hold
        together, and each one is load-bearing:

        * ``_write_nowait`` is absent unless the transport's write is a plain
          buffer append;
        * ``_writing`` covers the writer task's whole batch — a segmented item
          is two consecutive writes, and a frame landing between them would
          corrupt the packet;
        * an empty queue is what makes this ordered: ``put_nowait`` leaves the
          item visible until the writer pops it, so a non-empty queue always
          means an earlier frame is still owed;
        * a leftover ``_held`` item is an earlier frame already taken from the
          queue for the next byte-capped batch, so it must not be overtaken;
        * a producer suspended in :meth:`enqueue` must not be overtaken;
        * segmented items are never written eagerly, for the reason above.

        None of the checks can be invalidated between them: this runs to
        completion without awaiting.
        """
        write_nowait = self._write_nowait
        if (
            not self._eager_armed
            or write_nowait is None
            or self._writing
            or self._held is not None
            or self.waiters
            or isinstance(item, tuple)
            or not self.queue.empty()
        ):
            return False
        if not write_nowait(item):
            return False
        self._eager_armed = False
        generation = self._eager_generation
        asyncio.get_running_loop().call_soon(self._rearm_eager_if_idle, generation)
        self.eager_writes += 1
        self.eager_bytes += len(item)
        self.last_outbound = time.monotonic()
        return True

    def try_enqueue(self, item: WriteItem, *, epoch: int | None = None) -> bool:
        if epoch is None:
            epoch = self.epoch
        if epoch != self.epoch:
            raise StaleConnectionEffect
        size = item_size(item)
        if not self.can_enqueue_size(size):
            return False
        if self._try_write_eager(item):
            return True
        self.queue.put_nowait(item)
        self.queued_bytes += size
        return True

    def _try_flush_latency_batch(self) -> bool:
        """Flush one short queued burst without waiting for the writer task.

        This is a latency shortcut for already-admitted QoS 1/2 publishes. It
        deliberately lives in the queue owner so byte accounting, task_done(),
        and restoration stay atomic with respect to the event loop. The caller
        decides *when* latency-sensitive traffic may use it; QoS 0 and nowait
        paths never call this method.

        Only the whole current queue is considered. That makes a declined
        ``write_nowait`` exactly restorable without moving earlier frames behind
        later ones. Segmented items are left to the writer task because joining
        them would defeat their copy-avoidance contract.
        """
        queued = self.queued_messages
        if (
            self._held is not None
            or queued < _LATENCY_BATCH_MIN_ITEMS
            or queued > _LATENCY_BATCH_MAX_ITEMS
            or (
                queued < _LATENCY_BATCH_MAX_ITEMS
                and self.queued_bytes < _LATENCY_BATCH_TARGET_BYTES
            )
        ):
            return False
        write_nowait = self._write_nowait
        if write_nowait is None or self._writing or self.waiters:
            return False

        self._sample_high_water(queued)
        items = [self.queue.get_nowait() for _ in range(queued)]
        if any(isinstance(item, tuple) for item in items):
            for _ in items:
                self.queue.task_done()
            for item in items:
                self.queue.put_nowait(item)
            return False

        parts = [item for item in items if isinstance(item, bytes)]
        combined = b"".join(parts)
        if not write_nowait(combined):
            for _ in items:
                self.queue.task_done()
            for item in items:
                self.queue.put_nowait(item)
            return False

        for _ in items:
            self.queue.task_done()
        self.queued_bytes -= len(combined)
        if self.queued_bytes != 0:
            raise AssertionError("writer queued-byte accounting mismatch after latency batch")
        self.batches += 1
        self.batched_items += queued
        self.batched_bytes += len(combined)
        self.last_outbound = time.monotonic()
        return True

    def try_enqueue_many(
        self,
        items: list[WriteItem],
        *,
        epoch: int | None = None,
    ) -> bool:
        """Atomically append a bounded ordered batch without suspending."""

        if epoch is None:
            epoch = self.epoch
        if epoch != self.epoch:
            raise StaleConnectionEffect
        if not items:
            return True

        messages = self.queued_messages
        bytes_used = self.queued_bytes
        for item in items:
            size = item_size(item)
            if not self.can_enqueue_size(
                size,
                queued_messages=messages,
                queued_bytes=bytes_used,
            ):
                return False
            messages += 1
            bytes_used += size

        for item in items:
            self.queue.put_nowait(item)
        self.queued_bytes = bytes_used
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
            raise FlowControlError(self.refusal(size))

        async with self.space:
            while True:
                if epoch != self.epoch:
                    raise StaleConnectionEffect
                messages_full = self.queued_messages >= self.max_messages
                # Allow a single oversized item into an empty queue (segmented
                # payloads can exceed max_bytes by the MQTT header).
                bytes_blocked = self.queued_bytes + size > self.max_bytes and not (
                    self.queued_bytes == 0 and self.queued_messages == 0
                )
                if not messages_full and not bytes_blocked:
                    break
                # Escape hatch: if the writer is idle but space accounting is
                # wedged, do not block protocol forever.
                if self.queued_messages == 0 and self.queued_bytes == 0:
                    break
                self.waiters += 1
                self.enqueue_suspensions += 1
                try:
                    await self.space.wait()
                finally:
                    self.waiters -= 1
            if epoch != self.epoch:
                raise StaleConnectionEffect
            self.queue.put_nowait(item)
            self.queued_bytes += size

    async def _write_contiguous(
        self,
        transport: AsyncTransport,
        write_many: Callable[[list[bytes]], Awaitable[None]] | None,
        parts: list[bytes],
    ) -> None:
        if not parts:
            return
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
        # Resolved once per writer task rather than per batch. The local dies
        # with the task, so unlike `_write_nowait` it needs no explicit
        # clearing: it can never outlive the transport it was resolved from.
        write_many = getattr(transport, "write_many", None)
        queue = self.queue
        try:
            while True:
                # Reaching the idle queue wait is an independent re-arm point:
                # it covers a batch that just drained even when no eager write
                # scheduled the producer-side next-turn callback.
                held = self._held
                if held is not None:
                    self._held = None
                    first = held
                else:
                    if queue.empty() and self._write_nowait is not None:
                        self._eager_armed = True
                    first = await queue.get()
                self._eager_armed = False
                self._sample_high_water(self.queued_messages + 1)
                batch: list[WriteItem] = [first]
                batch_bytes = item_size(first)
                while len(batch) < _WRITER_BATCH_MAX_ITEMS:
                    try:
                        item = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    size = item_size(item)
                    if batch_bytes + size > _WRITER_BATCH_MAX_BYTES:
                        # Already get()'d; putting it back would append at the
                        # tail. Hold it as the first item of the next batch.
                        self._held = item
                        break
                    batch.append(item)
                    batch_bytes += size
                self.batches += 1
                self.batched_items += len(batch)
                # Set before the first await and held across the whole batch:
                # this is what stops an eager write from landing between the two
                # halves of a segmented item.
                self._writing = True
                try:
                    # Coalesce contiguous small frames into one writelines call.
                    # Never await drain() after the batch (deadlocks vs reader ACK).
                    contiguous: list[bytes] = []
                    for data in batch:
                        if isinstance(data, tuple):
                            self.segmented_writes += 1
                            await self._write_contiguous(transport, write_many, contiguous)
                            for part in data:
                                await transport.write(part)
                        else:
                            contiguous.append(data)
                    await self._write_contiguous(transport, write_many, contiguous)
                    self.last_outbound = time.monotonic()
                finally:
                    self._writing = False
                    released = 0
                    for data in batch:
                        released += item_size(data)
                        queue.task_done()
                    self.batched_bytes += released
                    async with self.space:
                        self.queued_bytes = max(0, self.queued_bytes - released)
                        if self.waiters:
                            self.space.notify_all()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # The transport has failed and this task is giving up on it. Drop
            # the eager binding first: the reconnect path does not call stop()
            # (AsyncClient only stops the pump when it will not reconnect), so
            # without this a producer racing the reader's teardown could still
            # write straight into the dead transport.
            self._drop_eager_binding()
            self._writing = False
            await self.on_failure(exc)
