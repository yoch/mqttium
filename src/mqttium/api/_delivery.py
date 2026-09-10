"""Bounded application delivery and serial user notifications."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterator, Callable
from typing import Any, Literal
from dataclasses import dataclass

from mqttium.api.stats import DeliveryStats
from mqttium.codec.properties import PUBLISH, encode_properties
from mqttium.enums import MQTTProtocolVersion
from mqttium.errors import MessageDeliveryError, MQTTError
from mqttium.types import Message

MessageDelivery = Literal["iterator", "callback"]


@dataclass(frozen=True, slots=True)
class _ClassifiedCallback:
    callback: Callable[..., Any]
    is_async: bool


CallbackTarget = Callable[..., Any] | _ClassifiedCallback
CallbackJob = tuple[CallbackTarget, tuple[Any, ...], int | None]
IteratorQueueItem = tuple[Message, int]


class ApplicationDelivery:
    """Own one destination per message, its byte charge, and callback worker."""

    def __init__(
        self,
        *,
        mode: MessageDelivery,
        protocol: MQTTProtocolVersion,
        max_pending_messages: int,
        max_pending_callbacks: int,
        max_pending_delivery_bytes: int | None,
        delivery_timeout: float | None,
        callback_shutdown_timeout: float,
    ) -> None:
        self.mode = mode
        self.protocol = protocol
        self.max_pending_messages = max_pending_messages
        self.max_pending_delivery_bytes = max_pending_delivery_bytes
        self.pending_bytes = 0
        self.pending_high_water_bytes = 0
        self.space = asyncio.Event()
        self.waiters = 0
        self.messages_queue: asyncio.Queue[IteratorQueueItem] = asyncio.Queue(max_pending_messages)
        self.callback_queue: asyncio.Queue[CallbackJob] = asyncio.Queue(max_pending_callbacks)
        self.message_ready = asyncio.Event()
        self.closed = asyncio.Event()
        self._stream_generation = 0
        self.callback_task: asyncio.Task[None] | None = None
        self._callback_stop = False
        self.delivery_timeout = delivery_timeout
        self.callback_shutdown_timeout = callback_shutdown_timeout

    def stats(self) -> DeliveryStats:
        return DeliveryStats(
            iterator_queued=self.messages_queue.qsize(),
            iterator_limit=self.messages_queue.maxsize,
            callback_queued=self.callback_queue.qsize(),
            callback_limit=self.callback_queue.maxsize,
            pending_bytes=self.pending_bytes,
            pending_high_water_bytes=self.pending_high_water_bytes,
            max_bytes=self.max_pending_delivery_bytes,
            waiters=self.waiters,
        )

    def reopen(self) -> None:
        if self._callback_stop:
            # A callback may reconnect while its worker is still active.
            # Retire old queued work before that same worker can resume.
            self._discard_callback_queue()
            self._callback_stop = False
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

    async def _reserve(self, size: int) -> None:
        limit = self.max_pending_delivery_bytes
        if limit is not None and size > limit:
            raise MessageDeliveryError(
                f"Message requires {size} delivery bytes, exceeding limit {limit}"
            )
        while limit is not None and self.pending_bytes + size > limit:
            self.space.clear()
            self.waiters += 1
            try:
                await self.space.wait()
            finally:
                self.waiters -= 1
        self.pending_bytes += size
        self.pending_high_water_bytes = max(self.pending_high_water_bytes, self.pending_bytes)

    def release(self, size: int) -> None:
        self.pending_bytes -= size
        if self.waiters:
            self.space.set()

    def try_accept(
        self,
        message: Message,
        callback: CallbackTarget | None,
        property_wire_size: int | None = None,
        *,
        size: int | None = None,
    ) -> bool:
        """Transfer one message to its bounded destination without suspending."""
        if self.mode == "callback" and callback is None:
            return True
        if size is None:
            size = self.logical_size(message, property_wire_size)
        limit = self.max_pending_delivery_bytes
        if limit is not None and size > limit:
            raise MessageDeliveryError(
                f"Message requires {size} delivery bytes, exceeding limit {limit}"
            )
        queue = self.messages_queue if self.mode == "iterator" else self.callback_queue
        if queue.full() or (limit is not None and self.pending_bytes + size > limit):
            return False
        if self.mode == "callback":
            self.ensure_callback_worker()
        self.pending_bytes += size
        self.pending_high_water_bytes = max(self.pending_high_water_bytes, self.pending_bytes)
        try:
            if self.mode == "iterator":
                self.messages_queue.put_nowait((message, size))
                self.message_ready.set()
            else:
                assert callback is not None
                self.callback_queue.put_nowait((callback, (message,), size))
        except BaseException:
            self.release(size)
            raise
        return True

    async def accept(
        self,
        message: Message,
        callback: CallbackTarget | None,
        property_wire_size: int | None = None,
    ) -> None:
        if self.mode == "callback" and callback is None:
            return
        size = self.logical_size(message, property_wire_size)
        if self.try_accept(message, callback, size=size):
            return
        reserved = False
        try:
            async with asyncio.timeout(self.delivery_timeout):
                await self._reserve(size)
                reserved = True
                if self.mode == "iterator":
                    await self.messages_queue.put((message, size))
                    self.message_ready.set()
                else:
                    assert callback is not None
                    self.ensure_callback_worker()
                    await self.callback_queue.put((callback, (message,), size))
                reserved = False  # the queue now owns the reservation
        except TimeoutError as exc:
            raise MessageDeliveryError("Application delivery capacity timed out") from exc
        finally:
            if reserved:
                self.release(size)

    async def messages(self) -> AsyncIterator[Message]:
        if self.mode != "iterator":
            raise MQTTError("messages() requires message_delivery='iterator'")
        generation = self._stream_generation
        while generation == self._stream_generation:
            try:
                message, size = self.messages_queue.get_nowait()
            except asyncio.QueueEmpty:
                if self.closed.is_set():
                    return
                self.message_ready.clear()
                await self.message_ready.wait()
            else:
                self.messages_queue.task_done()
                self.release(size)
                yield message

    def reset_stream(self) -> None:
        if not self.closed.is_set():
            return
        self._stream_generation += 1
        while not self.messages_queue.empty():
            _message, size = self.messages_queue.get_nowait()
            self.messages_queue.task_done()
            self.release(size)
        # Wake iterators that belong to the retired stream before replacing it.
        self.message_ready.set()
        self.messages_queue = asyncio.Queue(self.max_pending_messages)
        self.message_ready = asyncio.Event()
        self.closed.clear()

    def ensure_callback_worker(self) -> None:
        if self.callback_task is None or self.callback_task.done():
            self._callback_stop = False
            self.callback_task = asyncio.create_task(
                self._callback_worker(), name="mqttium-callbacks"
            )

    def try_enqueue_callback(self, callback: Callable[..., Any], *args: Any) -> bool:
        if self.callback_queue.full():
            return False
        self.ensure_callback_worker()
        self.callback_queue.put_nowait((callback, args, None))
        return True

    async def enqueue_callback(self, callback: Callable[..., Any], *args: Any) -> None:
        self.ensure_callback_worker()
        try:
            async with asyncio.timeout(self.delivery_timeout):
                await self.callback_queue.put((callback, args, None))
        except TimeoutError as exc:
            raise MessageDeliveryError("Callback delivery capacity timed out") from exc

    @staticmethod
    def _is_async_callback(callback: Callable[..., Any]) -> bool:
        return inspect.iscoroutinefunction(callback) or inspect.iscoroutinefunction(
            type(callback).__call__
        )

    @classmethod
    def classify(cls, callback: Callable[..., Any]) -> _ClassifiedCallback:
        """Capture invocation mode once for a frozen message route."""
        return _ClassifiedCallback(callback, cls._is_async_callback(callback))

    @classmethod
    async def invoke(cls, callback: CallbackTarget | None, *args: Any) -> Any:
        if callback is None:
            return None
        if isinstance(callback, _ClassifiedCallback):
            is_async = callback.is_async
            callback = callback.callback
        else:
            is_async = cls._is_async_callback(callback)
        if is_async:
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
    def report_callback_error(callback: CallbackTarget | None, exc: BaseException) -> None:
        if isinstance(callback, _ClassifiedCallback):
            callback = callback.callback
        asyncio.get_running_loop().call_exception_handler(
            {
                "message": "mqttium user callback failed",
                "exception": exc,
                "callback": callback,
            }
        )

    def _propagate_callback_cancellation(
        self, callback: CallbackTarget | None, exc: asyncio.CancelledError
    ) -> None:
        task = asyncio.current_task()
        if task is None or task.cancelling():
            raise exc
        self.report_callback_error(callback, exc)

    async def invoke_isolated(self, callback: CallbackTarget, *args: Any) -> None:
        try:
            await self.invoke(callback, *args)
        except asyncio.CancelledError as exc:
            self._propagate_callback_cancellation(callback, exc)
        except Exception as exc:
            self.report_callback_error(callback, exc)

    def _discard_callback_queue(self) -> None:
        while not self.callback_queue.empty():
            _callback, _args, size = self.callback_queue.get_nowait()
            if size is not None:
                self.release(size)
            self.callback_queue.task_done()

    async def _callback_worker(self) -> None:
        # Even an eager task factory must not invoke user code from an engine
        # critical section that synchronously enqueues a notification.
        await asyncio.sleep(0)
        try:
            while not self._callback_stop:
                callback, args, size = await self.callback_queue.get()
                try:
                    await self.invoke_isolated(callback, *args)
                finally:
                    if size is not None:
                        self.release(size)
                    self.callback_queue.task_done()
        finally:
            self._discard_callback_queue()

    async def shutdown_callbacks(self, *, drain: bool) -> None:
        task = self.callback_task
        if task is None:
            return
        if task is asyncio.current_task():
            self._callback_stop = True
            return
        if drain and not task.done():
            try:
                await asyncio.wait_for(self.callback_queue.join(), self.callback_shutdown_timeout)
            except TimeoutError:
                pass
        if not task.done():
            task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        self.callback_task = None
        self._discard_callback_queue()
