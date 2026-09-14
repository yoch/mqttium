"""Bounded application delivery: one iterator queue or synchronous callbacks."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from typing import Any, Literal
from dataclasses import dataclass
from functools import partial

from mqttium.api.stats import DeliveryStats
from mqttium.codec.properties import PUBLISH, encode_properties
from mqttium.enums import MQTTProtocolVersion
from mqttium.errors import MessageDeliveryError, MQTTError
from mqttium.types import Message

MessageDelivery = Literal["iterator", "callback"]


@dataclass(frozen=True, slots=True)
class MessageRoute:
    """Select synchronous callbacks without invoking application code."""

    select: Callable[[Message], Iterator[Callable[..., Any]]]


CallbackTarget = Callable[..., Any] | MessageRoute
IteratorQueueItem = tuple[Message, int]
# Synchronous callback invocations, including route fan-out, charged between
# two cooperative yields of the delivering reader. The yield happens at a
# message boundary, so one message's routes always run contiguously.
_CALLBACK_QUANTUM = 128


class ApplicationDelivery:
    """Own the message destination: a bounded iterator queue or direct callbacks.

    Callback mode invokes synchronous application callbacks from the reader
    that delivers the message, outside every protocol lock, so it needs no
    queue, worker task or byte reservation: the reader does not decode more
    until the current lot has been handed to the application. Iterator mode
    parks messages in a bounded queue for an independent consumer and charges
    their logical size against ``max_iterator_bytes``.
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
        self.max_iterator_messages = max_iterator_messages
        self.max_iterator_bytes = max_iterator_bytes
        self.pending_bytes = 0
        self.pending_high_water_bytes = 0
        self.space = asyncio.Event()
        self.waiters = 0
        self.messages_queue: asyncio.Queue[IteratorQueueItem] = asyncio.Queue(max_iterator_messages)
        self.message_ready = asyncio.Event()
        self.closed = asyncio.Event()
        self._stream_generation = 0
        self.iterator_admission_timeout = iterator_admission_timeout
        self.callback_invocations = 0
        self._since_yield = 0

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
        if property_wire_size is None:
            property_wire_size = (
                len(encode_properties(message.properties, PUBLISH))
                if self.protocol == MQTTProtocolVersion.MQTTv5 and message.properties
                else 0
            )
        return len(message.payload) + len(message.topic.encode("utf-8")) + property_wire_size

    def release(self, size: int) -> None:
        self.pending_bytes -= size
        if self.waiters:
            self.space.set()

    def _enqueue(self, message: Message, size: int) -> None:
        self.pending_bytes += size
        if self.pending_bytes > self.pending_high_water_bytes:
            self.pending_high_water_bytes = self.pending_bytes
        self.messages_queue.put_nowait((message, size))
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
        budget for free. Iterator mode returns ``None`` after an immediate
        bounded enqueue and otherwise the coroutine that waits for queue and
        byte capacity.
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
        size = self.logical_size(message, property_wire_size)
        limit = self.max_iterator_bytes
        if limit is not None:
            if size > limit:
                raise MessageDeliveryError(
                    f"Message requires {size} delivery bytes, exceeding limit {limit}"
                )
            if self.pending_bytes + size > limit:
                return self._accept_waiting(message, size)
        if self.messages_queue.full():
            return self._accept_waiting(message, size)
        self._enqueue(message, size)
        return None

    async def _accept_waiting(self, message: Message, size: int) -> None:
        """Wait for byte and queue capacity under one shared deadline."""
        limit = self.max_iterator_bytes
        try:
            async with asyncio.timeout(self.iterator_admission_timeout):
                while (limit is not None and self.pending_bytes + size > limit) or (
                    self.messages_queue.full()
                ):
                    self.space.clear()
                    self.waiters += 1
                    try:
                        await self.space.wait()
                    finally:
                        self.waiters -= 1
        except TimeoutError as exc:
            raise MessageDeliveryError("Application delivery capacity timed out") from exc
        self._enqueue(message, size)

    def messages(self) -> AsyncIterator[Message]:
        return self._messages(self._stream_generation)

    async def _messages(self, generation: int) -> AsyncIterator[Message]:
        if self.mode != "iterator":
            raise MQTTError("messages() requires message_delivery='iterator'")
        while generation == self._stream_generation:
            try:
                message, size = self.messages_queue.get_nowait()
            except asyncio.QueueEmpty:
                if self.closed.is_set():
                    return
                self.message_ready.clear()
                await self.message_ready.wait()
            else:
                self.release(size)
                yield message

    def reset_stream(self) -> None:
        if not self.closed.is_set():
            return
        self._stream_generation += 1
        while not self.messages_queue.empty():
            _message, size = self.messages_queue.get_nowait()
            self.release(size)
        # Wake iterators that belong to the retired stream before replacing it.
        self.message_ready.set()
        self.messages_queue = asyncio.Queue(self.max_iterator_messages)
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
