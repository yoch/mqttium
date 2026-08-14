"""Bounded application delivery and user-callback dispatch.

This controller owns every resource whose lifetime belongs to application
delivery.  It deliberately knows nothing about transports, reconnect policy or
the protocol engine; ``AsyncClient`` marks persisted messages delivered only
after this controller has accepted them.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any, Literal

from mqttium.api.stats import DeliveryStats
from mqttium.codec.properties import PUBLISH, encode_properties
from mqttium.enums import MQTTProtocolVersion
from mqttium.errors import MessageDeliveryError
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.types import Message

MessageDelivery = Literal["auto", "iterator", "callback", "both"]


class _SharedDeliveryReservation:
    """One exact byte reservation shared by callback and iterator delivery."""

    __slots__ = ("logical_bytes", "remaining")

    def __init__(self, logical_bytes: int) -> None:
        self.logical_bytes = logical_bytes
        self.remaining = 2


AccountedDeliveryToken = int | _SharedDeliveryReservation
DeliveryToken = AccountedDeliveryToken | None
CallbackJob = tuple[Callable[..., Any], tuple[Any, ...], DeliveryToken]
TrackedIteratorMessage = tuple[Message, AccountedDeliveryToken]
IteratorQueueItem = Message | TrackedIteratorMessage
MessageAcceptor = Callable[[Message, Callable[[Message], Any] | None], Awaitable[None] | None]
DecodedMessageAcceptor = Callable[
    [Message, Callable[[Message], Any] | None, int], Awaitable[None] | None
]


class _DeliveryQueue(asyncio.Queue[IteratorQueueItem]):
    """Queue for stream delivery without unused ``join()`` bookkeeping."""

    def put_nowait(self, item: IteratorQueueItem) -> None:
        if self.full():
            raise asyncio.QueueFull
        self._put(item)
        self._wakeup_next(self._getters)  # type: ignore[attr-defined]


def _budget_partition(
    *,
    mode: MessageDelivery,
    max_pending_messages: int,
    max_pending_callbacks: int,
    max_pending_delivery_bytes: int | None,
    maximum_packet_size: int,
) -> tuple[int, int | None, int | None]:
    if max_pending_delivery_bytes is None:
        return 0, None, None
    if mode == "callback":
        maximum_small_messages = max_pending_callbacks + 2
    elif mode == "iterator":
        maximum_small_messages = max_pending_messages + 1
    else:
        maximum_small_messages = max_pending_messages + max_pending_callbacks + 2
    small_budget = max_pending_delivery_bytes // 8
    small_limit = small_budget // maximum_small_messages
    accounted_limit = max_pending_delivery_bytes - small_budget
    minimum_single_message_capacity = min(max_pending_delivery_bytes, maximum_packet_size)
    if small_limit <= 0 or accounted_limit < minimum_single_message_capacity:
        return 0, 0, max_pending_delivery_bytes
    return small_budget, small_limit, accounted_limit


def _fits_small_limit(message: Message, limit: int | None, property_wire_size: int | None) -> bool:
    if property_wire_size is None:
        return False
    if limit is None:
        return True
    return limit > 0 and len(message.payload) + 4 * len(message.topic) + property_wire_size <= limit


def _is_small_delivery(
    message: Message,
    references: int,
    limit: int | None,
    decoded_property_wire_size: int | None,
) -> bool:
    if not references:
        return False
    if decoded_property_wire_size is not None:
        return _fits_small_limit(message, limit, decoded_property_wire_size)
    return limit is None or (
        limit > 0
        and not message.properties
        and len(message.payload) + 4 * len(message.topic) <= limit
    )


class ApplicationDelivery:
    """Own bounded message delivery, callbacks and their accounting."""

    def __init__(
        self,
        *,
        mode: MessageDelivery,
        protocol: MQTTProtocolVersion,
        max_pending_messages: int,
        max_pending_callbacks: int,
        max_pending_delivery_bytes: int | None,
        maximum_packet_size: int,
        delivery_timeout: float,
        callback_shutdown_timeout: float,
    ) -> None:
        self.mode = mode
        self.callback_mode = mode in ("auto", "callback", "both")
        self.iterator_mode = mode in ("iterator", "both")
        self.auto_mode = mode == "auto"
        self.protocol = protocol
        self.max_pending_messages = max_pending_messages
        self.max_pending_delivery_bytes = max_pending_delivery_bytes
        self.pending_bytes = 0
        self.pending_high_water_bytes = 0
        self.space = asyncio.Event()
        self.space.set()
        self.waiters = 0
        self.small_budget_bytes, self.small_message_limit, self.accounted_limit = _budget_partition(
            mode=mode,
            max_pending_messages=max_pending_messages,
            max_pending_callbacks=max_pending_callbacks,
            max_pending_delivery_bytes=max_pending_delivery_bytes,
            maximum_packet_size=maximum_packet_size,
        )
        self.messages_queue: asyncio.Queue[IteratorQueueItem] = _DeliveryQueue(
            maxsize=max_pending_messages
        )
        self.callback_queue: asyncio.Queue[CallbackJob] = asyncio.Queue(
            maxsize=max_pending_callbacks
        )
        self.message_ready = asyncio.Event()
        self.closed = asyncio.Event()
        self.callback_task: asyncio.Task[None] | None = None
        self.delivery_timeout = delivery_timeout
        self.callback_shutdown_timeout = callback_shutdown_timeout

    def acceptor(self) -> MessageAcceptor:
        """Return the mode-specialized admission strategy for this client."""
        if self.small_message_limit is None:
            return {
                "auto": self._accept_auto_fast,
                "iterator": self._accept_iterator_unaccounted,
                "callback": self._accept_callback_unaccounted,
                "both": self._accept_both_unaccounted,
            }[self.mode]
        return {
            "auto": self._accept_auto_fast,
            "iterator": self._accept_iterator_fast,
            "callback": self._accept_callback_fast,
            "both": self._accept_both_fast,
        }[self.mode]

    def decoded_acceptor(self) -> DecodedMessageAcceptor:
        """Return the mode-specialized admission path for a fresh decoded table."""
        return {
            "auto": self._accept_auto_decoded,
            "iterator": self._accept_iterator_decoded,
            "callback": self._accept_callback_decoded,
            "both": self._accept_both_decoded,
        }[self.mode]

    def _accept_iterator_unaccounted(
        self, message: Message, _callback: Callable[[Message], Any] | None
    ) -> Awaitable[None] | None:
        try:
            self.messages_queue.put_nowait(message)
        except asyncio.QueueFull:
            return self.put_message(message)
        self.message_ready.set()
        return None

    def _accept_callback_unaccounted(
        self, message: Message, callback: Callable[[Message], Any] | None
    ) -> Awaitable[None] | None:
        if callback is None:
            return None
        self.ensure_callback_worker()
        job: CallbackJob = (callback, (message,), None)
        try:
            self.callback_queue.put_nowait(job)
        except asyncio.QueueFull:
            return self.enqueue_callback_job_slow(job)
        return None

    def _accept_both_unaccounted(
        self, message: Message, callback: Callable[[Message], Any] | None
    ) -> Awaitable[None] | None:
        if callback is None:
            return self.accept(message, callback)
        try:
            self.messages_queue.put_nowait(message)
        except asyncio.QueueFull:
            return self.accept(message, callback)
        self.message_ready.set()
        self.ensure_callback_worker()
        job: CallbackJob = (callback, (message,), None)
        try:
            self.callback_queue.put_nowait(job)
        except asyncio.QueueFull:
            return self.enqueue_callback_job_slow(job)
        return None

    def _accept_iterator_fast(
        self, message: Message, _callback: Callable[[Message], Any] | None
    ) -> Awaitable[None] | None:
        limit = self.small_message_limit
        if (
            limit is not None
            and (
                limit <= 0
                or bool(message.properties)
                or len(message.payload) + 4 * len(message.topic) > limit
            )
        ) or self.messages_queue.full():
            return self.accept(message, None)
        self.messages_queue.put_nowait(message)
        self.message_ready.set()
        return None

    def _accept_callback_fast(
        self, message: Message, callback: Callable[[Message], Any] | None
    ) -> Awaitable[None] | None:
        if callback is None:
            return None
        limit = self.small_message_limit
        if (
            limit is not None
            and (
                limit <= 0
                or bool(message.properties)
                or len(message.payload) + 4 * len(message.topic) > limit
            )
        ) or self.callback_queue.full():
            return self.accept(message, callback)
        self.ensure_callback_worker()
        self.callback_queue.put_nowait((callback, (message,), None))
        return None

    def _accept_both_fast(
        self, message: Message, callback: Callable[[Message], Any] | None
    ) -> Awaitable[None] | None:
        if callback is None:
            return self.accept(message, callback)
        limit = self.small_message_limit
        if (
            (
                limit is not None
                and (
                    limit <= 0
                    or bool(message.properties)
                    or len(message.payload) + 4 * len(message.topic) > limit
                )
            )
            or self.messages_queue.full()
            or self.callback_queue.full()
        ):
            return self.accept(message, callback)
        self.messages_queue.put_nowait(message)
        self.message_ready.set()
        self.ensure_callback_worker()
        self.callback_queue.put_nowait((callback, (message,), None))
        return None

    def _accept_iterator_decoded(
        self,
        message: Message,
        _callback: Callable[[Message], Any] | None,
        property_wire_size: int,
    ) -> Awaitable[None] | None:
        if (
            not _fits_small_limit(message, self.small_message_limit, property_wire_size)
            or self.messages_queue.full()
        ):
            return self.accept(message, None, decoded_property_wire_size=property_wire_size)
        self.messages_queue.put_nowait(message)
        self.message_ready.set()
        return None

    def _accept_callback_decoded(
        self,
        message: Message,
        callback: Callable[[Message], Any] | None,
        property_wire_size: int,
    ) -> Awaitable[None] | None:
        if callback is None:
            return None
        if (
            not _fits_small_limit(message, self.small_message_limit, property_wire_size)
            or self.callback_queue.full()
        ):
            return self.accept(message, callback, decoded_property_wire_size=property_wire_size)
        self.ensure_callback_worker()
        self.callback_queue.put_nowait((callback, (message,), None))
        return None

    def _accept_both_decoded(
        self,
        message: Message,
        callback: Callable[[Message], Any] | None,
        property_wire_size: int,
    ) -> Awaitable[None] | None:
        if callback is None:
            return self.accept(message, callback, decoded_property_wire_size=property_wire_size)
        if (
            not _fits_small_limit(message, self.small_message_limit, property_wire_size)
            or self.messages_queue.full()
            or self.callback_queue.full()
        ):
            return self.accept(message, callback, decoded_property_wire_size=property_wire_size)
        self.messages_queue.put_nowait(message)
        self.message_ready.set()
        self.ensure_callback_worker()
        self.callback_queue.put_nowait((callback, (message,), None))
        return None

    def _accept_auto_decoded(
        self,
        message: Message,
        callback: Callable[[Message], Any] | None,
        property_wire_size: int,
    ) -> Awaitable[None] | None:
        if callback is None:
            return self._accept_iterator_decoded(message, None, property_wire_size)
        return self._accept_callback_decoded(message, callback, property_wire_size)

    def _accept_auto_fast(
        self, message: Message, callback: Callable[[Message], Any] | None
    ) -> Awaitable[None] | None:
        if callback is None:
            return self._accept_iterator_fast(message, None)
        return self._accept_callback_fast(message, callback)

    async def accept(
        self,
        message: Message,
        callback: Callable[[Message], Any] | None,
        *,
        decoded_property_wire_size: int | None = None,
    ) -> None:
        """Accept one message using one controller boundary on the hot path."""
        callback_delivery = callback is not None and self.callback_mode
        iterator_delivery = self.iterator_mode or (self.auto_mode and callback is None)
        references = int(iterator_delivery) + int(callback_delivery)
        small_delivery = _is_small_delivery(
            message,
            references,
            self.small_message_limit,
            decoded_property_wire_size,
        )
        if small_delivery:
            if iterator_delivery:
                try:
                    self.messages_queue.put_nowait(message)
                except asyncio.QueueFull:
                    await self.put_message(message)
                else:
                    self.message_ready.set()
            if callback_delivery:
                assert callback is not None
                self.ensure_callback_worker()
                job: CallbackJob = (callback, (message,), None)
                try:
                    self.callback_queue.put_nowait(job)
                except asyncio.QueueFull:
                    await self.enqueue_callback_job_slow(job)
            return

        token: DeliveryToken = None
        iterator_enqueued = False
        callback_enqueued = False
        if references:
            logical_bytes = self._reservable_size(
                message, decoded_property_wire_size=decoded_property_wire_size
            )
            token = self.try_reserve(logical_bytes, references)
            if token is None:
                token = await self.reserve_slow(logical_bytes, references)
        try:
            if iterator_delivery:
                item: IteratorQueueItem = (message, token) if token is not None else message
                try:
                    self.messages_queue.put_nowait(item)
                except asyncio.QueueFull:
                    await self.put_message(item)
                else:
                    self.message_ready.set()
                iterator_enqueued = True
            if callback_delivery:
                assert callback is not None
                self.ensure_callback_worker()
                job = (callback, (message,), token)
                try:
                    self.callback_queue.put_nowait(job)
                except asyncio.QueueFull:
                    await self.enqueue_callback_job_slow(job)
                callback_enqueued = True
        except BaseException:
            self._release_unqueued(
                token,
                references=references,
                iterator_enqueued=iterator_enqueued,
                callback_enqueued=callback_enqueued,
            )
            raise

    def _release_unqueued(
        self,
        token: DeliveryToken,
        *,
        references: int,
        iterator_enqueued: bool,
        callback_enqueued: bool,
    ) -> None:
        if token is None:
            return
        unqueued = references - int(iterator_enqueued) - int(callback_enqueued)
        for _ in range(unqueued):
            self.release_nowait(token)

    def stats(self) -> DeliveryStats:
        return DeliveryStats(
            iterator_queued=self.messages_queue.qsize(),
            iterator_limit=self.messages_queue.maxsize,
            callback_queued=self.callback_queue.qsize(),
            callback_limit=self.callback_queue.maxsize,
            pending_bytes=self.pending_bytes,
            pending_high_water_bytes=self.pending_high_water_bytes,
            accounted_limit=self.accounted_limit,
            small_budget_bytes=self.small_budget_bytes,
            small_message_limit=self.small_message_limit,
            waiters=self.waiters,
        )

    def reopen(self) -> None:
        self.closed.clear()

    def close(self) -> None:
        self.closed.set()
        self.message_ready.set()

    def _modes(self, callback: Callable[[Message], Any] | None) -> tuple[bool, bool]:
        callback_delivery = callback is not None and self.mode in ("auto", "callback", "both")
        iterator_delivery = self.mode in ("iterator", "both") or (
            self.mode == "auto" and callback is None
        )
        return callback_delivery, iterator_delivery

    def _is_small(self, message: Message, references: int) -> bool:
        limit = self.small_message_limit
        return bool(
            references
            and (
                limit is None
                or (
                    limit > 0
                    and not message.properties
                    and len(message.payload) + 4 * len(message.topic) <= limit
                )
            )
        )

    def _is_small_decoded(self, message: Message, references: int, property_wire_size: int) -> bool:
        return bool(
            references and _fits_small_limit(message, self.small_message_limit, property_wire_size)
        )

    def deliver_batch_inline(
        self,
        effects: deque[EngineEffect],
        callback: Callable[[Message], Any] | None,
    ) -> int:
        callback_delivery, iterator_delivery = self._modes(callback)
        if not callback_delivery and not iterator_delivery:
            return 0
        callback_worker_ready = False
        applied = 0
        for effect in effects:
            if effect.kind is not EffectKind.MESSAGE:
                break
            message: Message = effect.data
            if effect.requires_delivery_mark is not False:
                break
            property_wire_size = effect.decoded_property_wire_size
            if property_wire_size is None:
                if not self._is_small(message, 1):
                    break
            elif not self._is_small_decoded(message, 1, property_wire_size):
                break
            if iterator_delivery and self.messages_queue.full():
                break
            if callback_delivery and self.callback_queue.full():
                break
            if iterator_delivery:
                self.messages_queue.put_nowait(message)
            if callback_delivery:
                assert callback is not None
                if not callback_worker_ready:
                    self.ensure_callback_worker()
                    callback_worker_ready = True
                self.callback_queue.put_nowait((callback, (message,), None))
            applied += 1
        if applied and iterator_delivery:
            self.message_ready.set()
        return applied

    async def messages(self) -> AsyncIterator[Message]:
        while True:
            try:
                item = self.messages_queue.get_nowait()
                if isinstance(item, tuple):
                    message, token = item
                    self.release_nowait(token)
                    yield message
                else:
                    yield item
                continue
            except asyncio.QueueEmpty:
                if self.closed.is_set():
                    return
            self.message_ready.clear()
            if not self.messages_queue.empty() or self.closed.is_set():
                continue
            await self.message_ready.wait()

    def reset_stream(self) -> None:
        if not self.closed.is_set():
            return
        while True:
            try:
                item = self.messages_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if isinstance(item, tuple):
                _message, token = item
                self.release_nowait(token)
        self.messages_queue = _DeliveryQueue(maxsize=self.max_pending_messages)
        self.message_ready = asyncio.Event()
        self.closed.clear()

    async def put_message(self, item: IteratorQueueItem) -> None:
        try:
            self.messages_queue.put_nowait(item)
        except asyncio.QueueFull:
            try:
                await asyncio.wait_for(
                    self.messages_queue.put(item),
                    timeout=self.delivery_timeout,
                )
            except TimeoutError as exc:
                raise MessageDeliveryError(
                    f"Iterator delivery queue remained full for {self.delivery_timeout:.3f}s"
                ) from exc
        self.message_ready.set()

    def try_reserve(self, logical_bytes: int, references: int) -> DeliveryToken:
        limit = self.accounted_limit
        if limit is not None and self.pending_bytes + logical_bytes > limit:
            return None
        self.pending_bytes += logical_bytes
        self.pending_high_water_bytes = max(self.pending_high_water_bytes, self.pending_bytes)
        if references == 1:
            return logical_bytes
        return _SharedDeliveryReservation(logical_bytes)

    async def reserve_slow(self, logical_bytes: int, references: int) -> DeliveryToken:
        while True:
            self.waiters += 1
            try:
                self.space.clear()
                token = self.try_reserve(logical_bytes, references)
                if token is not None:
                    return token
                await self.space.wait()
            finally:
                self.waiters -= 1
            token = self.try_reserve(logical_bytes, references)
            if token is not None:
                return token

    def release_nowait(self, token: AccountedDeliveryToken) -> None:
        if isinstance(token, int):
            self.pending_bytes -= token
        else:
            if token.remaining <= 0:
                return
            token.remaining -= 1
            if token.remaining:
                return
            logical_bytes = token.logical_bytes
            token.logical_bytes = 0
            self.pending_bytes -= logical_bytes
        if self.waiters:
            self.space.set()

    async def release(self, token: AccountedDeliveryToken) -> None:
        self.release_nowait(token)

    def logical_size(
        self, message: Message, *, decoded_property_wire_size: int | None = None
    ) -> int:
        property_bytes = 0
        if self.protocol == MQTTProtocolVersion.MQTTv5 and message.properties:
            if decoded_property_wire_size is None:
                property_bytes = len(encode_properties(message.properties, PUBLISH))
            else:
                property_bytes = decoded_property_wire_size
        topic_bytes = (
            len(message.topic) if message.topic.isascii() else len(message.topic.encode("utf-8"))
        )
        return len(message.payload) + topic_bytes + property_bytes

    def _reservable_size(
        self, message: Message, *, decoded_property_wire_size: int | None = None
    ) -> int:
        logical_bytes = self.logical_size(
            message, decoded_property_wire_size=decoded_property_wire_size
        )
        limit = self.accounted_limit
        if limit is not None and logical_bytes > limit:
            raise MessageDeliveryError(
                f"Message requires {logical_bytes} delivery bytes, exceeding limit {limit}"
            )
        return logical_bytes

    def ensure_callback_worker(self) -> None:
        if self.callback_task is None or self.callback_task.done():
            self.callback_task = asyncio.create_task(
                self._callback_worker(), name="mqttium-callback-worker"
            )

    def spawn_callback(self, callback: Callable[..., Any], *args: Any) -> None:
        self.ensure_callback_worker()
        try:
            self.callback_queue.put_nowait((callback, args, None))
        except asyncio.QueueFull as exc:
            raise MessageDeliveryError("Callback delivery queue is full") from exc

    def try_enqueue_callback(self, callback: Callable[..., Any], *args: Any) -> bool:
        """Enqueue one callback without suspending or weakening the queue bound."""
        self.ensure_callback_worker()
        try:
            self.callback_queue.put_nowait((callback, args, None))
        except asyncio.QueueFull:
            return False
        return True

    def has_callback_capacity(self, count: int = 1) -> bool:
        """Whether ``count`` callbacks can be admitted without suspending."""
        maximum = self.callback_queue.maxsize
        return maximum <= 0 or self.callback_queue.qsize() + count <= maximum

    def enqueue_callback_repeated_nowait(
        self,
        callback: Callable[..., Any],
        args: tuple[Any, ...],
        count: int,
    ) -> None:
        """Enqueue a preflighted callback batch without yielding."""
        if not self.has_callback_capacity(count):
            raise RuntimeError("callback batch exceeds preflighted capacity")
        self.ensure_callback_worker()
        for _ in range(count):
            self.callback_queue.put_nowait((callback, args, None))

    async def enqueue_callback(
        self,
        callback: Callable[..., Any],
        *args: Any,
        delivery_token: DeliveryToken = None,
    ) -> None:
        self.ensure_callback_worker()
        job = (callback, args, delivery_token)
        try:
            self.callback_queue.put_nowait(job)
        except asyncio.QueueFull:
            await self.enqueue_callback_job_slow(job)

    async def enqueue_callback_job_slow(self, job: CallbackJob) -> None:
        try:
            await asyncio.wait_for(
                self.callback_queue.put(job),
                timeout=self.delivery_timeout,
            )
        except TimeoutError as exc:
            raise MessageDeliveryError(
                f"Callback delivery queue remained full for {self.delivery_timeout:.3f}s"
            ) from exc

    async def _callback_worker(self) -> None:
        while True:
            callback, args, token = await self.callback_queue.get()
            try:
                await self.invoke(callback, *args)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.report_callback_error(callback, exc)
            finally:
                if token is not None:
                    self.release_nowait(token)
                self.callback_queue.task_done()

    @staticmethod
    async def invoke(callback: Callable[..., Any] | None, *args: Any) -> Any:
        if callback is None:
            return None
        result = callback(*args)
        if isinstance(result, Awaitable):
            return await result
        return result

    @staticmethod
    def report_callback_error(
        callback: Callable[..., Any] | None,
        exc: BaseException,
    ) -> None:
        asyncio.get_running_loop().call_exception_handler(
            {
                "message": "mqttium user callback failed",
                "exception": exc,
                "callback": callback,
            }
        )

    async def shutdown_callbacks(self, *, drain: bool) -> None:
        task = self.callback_task
        if task is None:
            return
        if drain and not task.done():
            try:
                await asyncio.wait_for(
                    self.callback_queue.join(), timeout=self.callback_shutdown_timeout
                )
            except TimeoutError:
                pass
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self.callback_task = None
        while True:
            try:
                _callback, _args, token = self.callback_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if token is not None:
                self.release_nowait(token)
            self.callback_queue.task_done()
