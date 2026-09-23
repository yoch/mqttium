"""Bounded application delivery: one iterator queue or synchronous callbacks."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from dataclasses import dataclass
from functools import partial
from typing import Any, Literal, cast

from mqttium.api.stats import DeliveryStats
from mqttium.enums import MQTTProtocolVersion
from mqttium.errors import MessageDeliveryError, MQTTError
from mqttium.protocol._sizing import publish_logical_size
from mqttium.types import Message

MessageDelivery = Literal["iterator", "callback"]


@dataclass(frozen=True, slots=True)
class MessageRoute:
    """Select synchronous callbacks without invoking application code."""

    select: Callable[[Message], Iterator[Callable[..., Any]]]


CallbackTarget = Callable[..., Any] | MessageRoute
IteratorQueueItem = Message | tuple[Message, int]
IteratorAcceptor = Callable[[Message, int | None], Awaitable[None] | None]
# Synchronous callback invocations, including route fan-out, charged between
# two cooperative yields of the delivering reader. The yield happens at a
# message boundary, so one message's routes always run contiguously.
_CALLBACK_QUANTUM = 128


class _DeliveryQueue(asyncio.Queue[IteratorQueueItem]):
    """Iterator queue without unused ``join()`` / ``task_done()`` bookkeeping."""

    def put_nowait(self, item: IteratorQueueItem) -> None:
        if self.full():
            raise asyncio.QueueFull
        self._put(item)
        self._wakeup_next(self._getters)  # type: ignore[attr-defined]


class ApplicationDelivery:
    """Own the message destination: a bounded iterator queue or direct callbacks.

    Callback mode invokes synchronous application callbacks from the reader
    that delivers the message, outside every protocol lock, so it needs no
    queue, worker task or byte reservation: the reader does not decode more
    until the current lot has been handed to the application. Iterator mode
    always applies its message-count bound. When ``max_iterator_bytes`` is
    finite it additionally charges exact logical bytes; ``None`` selects an
    unaccounted fast path and byte occupancy statistics remain zero.
    """

    def __init__(
        self,
        *,
        mode: MessageDelivery,
        protocol: MQTTProtocolVersion,
        max_iterator_messages: int,
        max_iterator_bytes: int | None,
        iterator_admission_timeout: float | None,
    ) -> None:
        self.mode = mode
        self.protocol = protocol
        self._is_v5 = protocol == MQTTProtocolVersion.MQTTv5
        self.max_iterator_messages = max_iterator_messages
        self.max_iterator_bytes = max_iterator_bytes
        self.pending_bytes = 0
        self.pending_high_water_bytes = 0
        self.space = asyncio.Event()
        self.waiters = 0
        self.messages_queue: asyncio.Queue[IteratorQueueItem] = _DeliveryQueue(
            max_iterator_messages
        )
        self.message_ready = asyncio.Event()
        self.closed = asyncio.Event()
        self._stream_generation = 0
        # Slow iterator admissions are not committed application messages yet.
        # Retire them independently when either their connection owner or the
        # application stream generation is replaced.
        self._admission_generation = 0
        self.iterator_admission_timeout = iterator_admission_timeout
        self.callback_invocations = 0
        self._since_yield = 0
        self._accept_iterator: IteratorAcceptor = (
            self._accept_iterator_unaccounted
            if max_iterator_bytes is None
            else self._accept_iterator_accounted
        )

    def stats(self) -> DeliveryStats:
        return DeliveryStats(
            iterator_queued=self.messages_queue.qsize(),
            iterator_limit=self.messages_queue.maxsize,
            iterator_bytes=self.pending_bytes,
            iterator_high_water_bytes=self.pending_high_water_bytes,
            iterator_byte_limit=self.max_iterator_bytes,
            waiters=self.waiters,
        )

    def reopen(self) -> None:
        self.closed.clear()

    def close(self) -> None:
        self.closed.set()
        self.message_ready.set()

    def logical_size(self, message: Message, property_wire_size: int | None = None) -> int:
        return publish_logical_size(
            self._is_v5,
            message.topic,
            len(message.payload),
            message.properties,
            property_wire_size,
        )

    def _wake_waiters(self) -> None:
        if self.waiters:
            self.space.set()

    def invalidate_waiting_admissions(self) -> None:
        """Retire uncommitted iterator handoffs from an older owner generation."""
        self._admission_generation += 1
        self._wake_waiters()

    def release(self, size: int) -> None:
        self.pending_bytes -= size
        self._wake_waiters()

    def _enqueue_accounted(self, message: Message, size: int) -> None:
        self.pending_bytes += size
        if self.pending_bytes > self.pending_high_water_bytes:
            self.pending_high_water_bytes = self.pending_bytes
        self.messages_queue.put_nowait((message, size))
        self.message_ready.set()

    def _enqueue_unaccounted(self, message: Message) -> None:
        self.messages_queue.put_nowait(message)
        self.message_ready.set()

    def accept(
        self,
        message: Message,
        callback: CallbackTarget | None,
        property_wire_size: int | None = None,
    ) -> Awaitable[None] | None:
        """Hand one message to its destination now, or return the waiting path.

        Callback mode runs every matching synchronous callback contiguously
        before returning, and yields an awaitable at this message boundary
        once the invocation budget is reached. Invocations beyond the budget
        stay charged to the next yield, so a wide fan-out cannot consume
        budget for free. Iterator mode dispatches through the constructor-bound
        accounted or unaccounted strategy and returns an awaitable only when
        queue or configured byte capacity is unavailable.
        """
        if self.mode == "callback":
            if callback is not None:
                if isinstance(callback, MessageRoute):
                    for selected in callback.select(message):
                        self.invoke_sync_isolated(selected, message)
                else:
                    self.invoke_sync_isolated(callback, message)
                if self._since_yield >= _CALLBACK_QUANTUM:
                    self._since_yield %= _CALLBACK_QUANTUM
                    return asyncio.sleep(0)
            return None
        return self._accept_iterator(message, property_wire_size)

    def _accept_iterator_unaccounted(
        self,
        message: Message,
        _property_wire_size: int | None = None,
    ) -> Awaitable[None] | None:
        if self.messages_queue.full():
            return self._accept_waiting_unaccounted(message, self._admission_generation)
        self._enqueue_unaccounted(message)
        return None

    def _accept_iterator_accounted(
        self,
        message: Message,
        property_wire_size: int | None = None,
    ) -> Awaitable[None] | None:
        # This is the bounded iterator hot path. Call the shared sizing primitive
        # directly rather than paying a forwarding method frame per message;
        # logical_size() remains the diagnostic/test surface for the same rule.
        size = publish_logical_size(
            self._is_v5,
            message.topic,
            len(message.payload),
            message.properties,
            property_wire_size,
        )
        limit = self.max_iterator_bytes
        assert limit is not None
        if size > limit:
            raise MessageDeliveryError(
                f"Message requires {size} delivery bytes, exceeding limit {limit}"
            )
        if self.pending_bytes + size > limit or self.messages_queue.full():
            return self._accept_waiting_accounted(message, size, self._admission_generation)
        self._enqueue_accounted(message, size)
        return None

    async def _accept_waiting_unaccounted(self, message: Message, generation: int) -> None:
        """Wait only for count capacity while this admission remains current."""
        try:
            async with asyncio.timeout(self.iterator_admission_timeout):
                while generation == self._admission_generation and self.messages_queue.full():
                    self.space.clear()
                    self.waiters += 1
                    try:
                        await self.space.wait()
                    finally:
                        self.waiters -= 1
        except TimeoutError as exc:
            raise MessageDeliveryError("Application delivery capacity timed out") from exc
        if generation != self._admission_generation:
            return
        self._enqueue_unaccounted(message)

    async def _accept_waiting_accounted(
        self,
        message: Message,
        size: int,
        generation: int,
    ) -> None:
        """Wait for byte/count capacity while this admission remains current."""
        limit = self.max_iterator_bytes
        assert limit is not None
        try:
            async with asyncio.timeout(self.iterator_admission_timeout):
                while generation == self._admission_generation and (
                    self.pending_bytes + size > limit or self.messages_queue.full()
                ):
                    self.space.clear()
                    self.waiters += 1
                    try:
                        await self.space.wait()
                    finally:
                        self.waiters -= 1
        except TimeoutError as exc:
            raise MessageDeliveryError("Application delivery capacity timed out") from exc
        if generation != self._admission_generation:
            return
        self._enqueue_accounted(message, size)

    def messages(self) -> AsyncIterator[Message]:
        generation = self._stream_generation
        if self.max_iterator_bytes is None:
            return self._messages_unaccounted(generation)
        return self._messages_accounted(generation)

    async def _messages_unaccounted(self, generation: int) -> AsyncIterator[Message]:
        if self.mode != "iterator":
            raise MQTTError("messages() requires message_delivery='iterator'")
        while generation == self._stream_generation:
            try:
                item = self.messages_queue.get_nowait()
            except asyncio.QueueEmpty:
                if self.closed.is_set():
                    return
                self.message_ready.clear()
                await self.message_ready.wait()
            else:
                self._wake_waiters()
                yield cast(Message, item)

    async def _messages_accounted(self, generation: int) -> AsyncIterator[Message]:
        if self.mode != "iterator":
            raise MQTTError("messages() requires message_delivery='iterator'")
        while generation == self._stream_generation:
            try:
                item = self.messages_queue.get_nowait()
            except asyncio.QueueEmpty:
                if self.closed.is_set():
                    return
                self.message_ready.clear()
                await self.message_ready.wait()
            else:
                message, size = cast(tuple[Message, int], item)
                self.release(size)
                yield message

    def reset_stream(self) -> None:
        if not self.closed.is_set():
            return
        self.invalidate_waiting_admissions()
        self._stream_generation += 1
        if self.max_iterator_bytes is None:
            while not self.messages_queue.empty():
                self.messages_queue.get_nowait()
            self._wake_waiters()
        else:
            while not self.messages_queue.empty():
                _message, size = cast(tuple[Message, int], self.messages_queue.get_nowait())
                self.release(size)
        # Wake iterators that belong to the retired stream before replacing it.
        self.message_ready.set()
        self.messages_queue = _DeliveryQueue(self.max_iterator_messages)
        self.message_ready = asyncio.Event()
        self.closed.clear()

    @staticmethod
    def _is_async_callback(callback: Callable[..., Any]) -> bool:
        while isinstance(callback, partial):
            callback = callback.func
        return (
            inspect.iscoroutinefunction(callback)
            or inspect.iscoroutinefunction(type(callback).__call__)
            or inspect.isasyncgenfunction(callback)
            or inspect.isasyncgenfunction(type(callback).__call__)
        )

    @classmethod
    def validate_message_callback(cls, callback: Callable[..., Any]) -> None:
        if not callable(callback):
            raise TypeError("callback must be callable")
        if cls._is_async_callback(callback):
            raise TypeError("message callbacks must be synchronous; use messages() for async work")

    @classmethod
    async def invoke(cls, callback: Callable[..., Any] | None, *args: Any) -> Any:
        if callback is None:
            return None
        if cls._is_async_callback(callback):
            return await callback(*args)
        result = callback(*args)
        if inspect.isawaitable(result):
            if inspect.iscoroutine(result):
                result.close()
            raise TypeError(
                "synchronous callbacks must not return awaitables; declare asynchronous callbacks with 'async def'"
            )
        return result

    @staticmethod
    def report_callback_error(callback: Callable[..., Any] | None, exc: BaseException) -> None:
        asyncio.get_running_loop().call_exception_handler(
            {
                "message": "mqttium user callback failed",
                "exception": exc,
                "callback": callback,
            }
        )

    def _propagate_callback_cancellation(
        self, callback: Callable[..., Any] | None, exc: asyncio.CancelledError
    ) -> None:
        task = asyncio.current_task()
        if task is None or task.cancelling():
            raise exc
        self.report_callback_error(callback, exc)

    def invoke_sync_isolated(self, callback: Callable[[Message], Any], message: Message) -> None:
        self.callback_invocations += 1
        self._since_yield += 1
        try:
            result = callback(message)
            if result is not None and inspect.isawaitable(result):
                if inspect.iscoroutine(result):
                    result.close()
                raise TypeError(
                    "message callbacks must not return awaitables; use messages() for async work"
                )
        except asyncio.CancelledError as exc:
            self._propagate_callback_cancellation(callback, exc)
        except Exception as exc:
            self.report_callback_error(callback, exc)
