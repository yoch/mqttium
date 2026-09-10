"""Bounded application delivery and user-callback dispatch.

This controller owns every resource whose lifetime belongs to application
delivery.  It deliberately knows nothing about transports, reconnect policy or
the protocol engine; ``AsyncClient`` marks persisted messages delivered only
after this controller has accepted them.
"""

from __future__ import annotations

import asyncio
import inspect
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable
from types import FunctionType, MethodType
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
        self._delivery_generation = 0
        self.callback_task: asyncio.Task[None] | None = None
        # True while either the worker or the opportunistic reader/effect path
        # is executing user code. It is also the reentrancy guard: a callback
        # that publishes or causes another delivery always falls back to the
        # bounded queue instead of nesting user callbacks.
        self._callback_active = False
        self._callback_state: Literal["open", "draining", "closed"] = "open"
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
        generation = self._delivery_generation
        try:
            self.messages_queue.put_nowait(message)
        except asyncio.QueueFull:
            return self.put_message(message, generation=generation)
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
        self.ensure_callback_worker()
        try:
            self.messages_queue.put_nowait(message)
        except asyncio.QueueFull:
            return self.accept(message, callback)
        self.message_ready.set()
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
        self.ensure_callback_worker()
        self.messages_queue.put_nowait(message)
        self.message_ready.set()
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
            return self.accept(message, None, property_wire_size)
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
            return self.accept(message, callback, property_wire_size)
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
            return self.accept(message, callback, property_wire_size)
        if (
            not _fits_small_limit(message, self.small_message_limit, property_wire_size)
            or self.messages_queue.full()
            or self.callback_queue.full()
        ):
            return self.accept(message, callback, property_wire_size)
        self.ensure_callback_worker()
        self.messages_queue.put_nowait(message)
        self.message_ready.set()
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

    async def _accept_small(
        self,
        message: Message,
        callback: Callable[[Message], Any] | None,
        iterator_delivery: bool,
    ) -> None:
        """Enqueue a message small enough to ride the unaccounted budget.

        No reservation is taken, so there is nothing to roll back: a failure
        here leaves only whatever the queues already accepted.
        """
        generation = self._delivery_generation
        if callback is not None:
            self.ensure_callback_worker()
        if iterator_delivery:
            try:
                self.messages_queue.put_nowait(message)
            except asyncio.QueueFull:
                await self.put_message(message, generation=generation)
            else:
                self.message_ready.set()
        if callback is not None:
            if generation != self._delivery_generation:
                raise MessageDeliveryError("Callback admission belongs to a retired generation")
            self.ensure_callback_worker()
            job: CallbackJob = (callback, (message,), None)
            try:
                self.callback_queue.put_nowait(job)
            except asyncio.QueueFull:
                await self.enqueue_callback_job_slow(job)

    async def accept(
        self,
        message: Message,
        callback: Callable[[Message], Any] | None,
        property_wire_size: int | None = None,
    ) -> None:
        """Accept one message using one controller boundary on the hot path.

        `property_wire_size` is the exact MQTT 5 property-table size observed
        while decoding a fresh PUBLISH; `None` means no trusted size is
        available and the table has to be re-encoded to size it. That is the
        only difference between the two entry points, so they share this body:
        the enqueue steps and the rollback below must not drift apart. Both are
        slow-path entries — `acceptor()` / `decoded_acceptor()` hand the common
        case to the specialised `_accept_*` acceptors.
        """
        generation = self._delivery_generation
        callback_delivery = callback is not None and self.callback_mode
        iterator_delivery = self.iterator_mode or (self.auto_mode and callback is None)
        references = int(iterator_delivery) + int(callback_delivery)
        if property_wire_size is None:
            small_delivery = bool(references and self._is_small(message))
        else:
            small_delivery = bool(
                references
                and _fits_small_limit(message, self.small_message_limit, property_wire_size)
            )
        if small_delivery:
            await self._accept_small(
                message, callback if callback_delivery else None, iterator_delivery
            )
            return

        token: DeliveryToken = None
        iterator_enqueued = False
        callback_enqueued = False
        if references:
            logical_bytes = self._reservable_size(message, property_wire_size)
            token = self.try_reserve(logical_bytes, references)
            if token is None:
                token = await self.reserve_slow(
                    logical_bytes,
                    references,
                    generation=generation,
                    require_callback_open=callback_delivery,
                )
        try:
            if callback_delivery and generation != self._delivery_generation:
                raise MessageDeliveryError("Callback admission belongs to a retired generation")
            if iterator_delivery:
                item: IteratorQueueItem = (message, token) if token is not None else message
                try:
                    self.messages_queue.put_nowait(item)
                except asyncio.QueueFull:
                    await self.put_message(item, generation=generation)
                else:
                    self.message_ready.set()
                iterator_enqueued = True
            if callback_delivery:
                assert callback is not None
                if generation != self._delivery_generation:
                    raise MessageDeliveryError("Callback admission belongs to a retired generation")
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
        if self._callback_state != "open":
            # The explicit reset already retired the prior delivery generation.
            # A callback which reconnects remains the sole worker incarnation.
            self._callback_state = "open"
            self.space.set()

    def close(self) -> None:
        self.closed.set()
        self.message_ready.set()
        self.space.set()

    def _modes(self, callback: Callable[[Message], Any] | None) -> tuple[bool, bool]:
        """Resolve the two delivery destinations from the modes cached at init."""
        callback_delivery = callback is not None and self.callback_mode
        iterator_delivery = self.iterator_mode or (self.auto_mode and callback is None)
        return callback_delivery, iterator_delivery

    def _is_small(self, message: Message) -> bool:
        limit = self.small_message_limit
        if limit is None:
            return True
        return (
            limit > 0
            and not message.properties
            and len(message.payload) + 4 * len(message.topic) <= limit
        )

    def _is_small_decoded(self, message: Message, property_wire_size: int) -> bool:
        return _fits_small_limit(message, self.small_message_limit, property_wire_size)

    def _enqueue_message_batch(
        self,
        callback: Callable[[Message], Any],
        messages: list[Message],
        *,
        iterator_delivery: bool,
    ) -> None:
        """Admit a preflighted prefix, with one ordinary queue entry per message."""
        self.ensure_callback_worker()
        if iterator_delivery:
            for message in messages:
                self.messages_queue.put_nowait(message)
            self.message_ready.set()
        put = self.callback_queue.put_nowait
        for message in messages:
            put((callback, (message,), None))

    def deliver_callback_messages_inline(
        self,
        messages: list[Message],
        callback: Callable[[Message], Any] | None,
        decoded_property_wire_sizes: list[int | None] | None = None,
    ) -> bool:
        if callback is None:
            return True
        if not self.has_callback_capacity(len(messages)):
            return False
        if decoded_property_wire_sizes is None:
            for message in messages:
                if not self._is_small(message):
                    return False
        else:
            if len(decoded_property_wire_sizes) != len(messages):
                raise AssertionError("decoded property sizes must align with messages")
            for message, wire_size in zip(messages, decoded_property_wire_sizes, strict=True):
                if wire_size is None:
                    if not self._is_small(message):
                        return False
                elif not self._is_small_decoded(message, wire_size):
                    return False
        self._enqueue_message_batch(callback, messages, iterator_delivery=False)
        return True

    def deliver_message_batch_inline(
        self,
        effects: deque[EngineEffect],
        callback: Callable[[Message], Any] | None,
    ) -> int:
        """Admit a consecutive eligible prefix; never run application code.

        The effect owner may finish admission synchronously without creating a
        flusher. User execution belongs exclusively to the callback worker.
        Iterator/callback capacity is preflighted once for the whole prefix.
        No user code or suspension can invalidate that preflight.
        Persisted deliveries and exact-byte reservations retain the slow path.
        """
        callback_delivery, iterator_delivery = self._modes(callback)
        if not callback_delivery and not iterator_delivery:
            return 0
        cb = callback if callback_delivery else None
        capacity = len(effects)
        if iterator_delivery:
            capacity = min(capacity, self.messages_queue.maxsize - self.messages_queue.qsize())
        if cb is not None:
            capacity = min(capacity, self.callback_queue.maxsize - self.callback_queue.qsize())
        ready = False
        applied = 0
        for effect in effects:
            if applied >= capacity:
                break
            if effect.kind not in (EffectKind.MESSAGE, EffectKind.DECODED_MESSAGE):
                break
            message: Message = effect.data
            size = effect.decoded_property_wire_size
            if effect.requires_delivery_mark or not (
                self._is_small(message) if size is None else self._is_small_decoded(message, size)
            ):
                break
            if cb is not None and not ready:
                self.ensure_callback_worker()
                ready = True
            if iterator_delivery:
                self.messages_queue.put_nowait(message)
            if cb is not None:
                self.callback_queue.put_nowait((cb, (message,), None))
            applied += 1
        if applied and iterator_delivery:
            self.message_ready.set()
        return applied

    async def messages(self) -> AsyncIterator[Message]:
        generation = self._delivery_generation
        while True:
            if generation != self._delivery_generation:
                return
            try:
                item = self.messages_queue.get_nowait()
                self.space.set()
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
        # An explicit connection takeover starts one new application-delivery
        # generation for both iterator and callback consumers. Active callback
        # code may finish; no unstarted notification or blocked admission from
        # the retired generation may cross into the replacement connection.
        self._delivery_generation += 1
        self._discard_callback_queue()
        while True:
            try:
                item = self.messages_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if isinstance(item, tuple):
                _message, token = item
                self.release_nowait(token)
        # Keep the queue/event objects stable. Slow admissions wait on the
        # controller-owned space event, so reset can wake every old producer;
        # the generation check rejects them before they enqueue stale work.
        self.message_ready.clear()
        self.closed.clear()
        self.space.set()

    async def put_message(self, item: IteratorQueueItem, *, generation: int | None = None) -> None:
        if generation is None:
            generation = self._delivery_generation
        try:
            async with asyncio.timeout(self.delivery_timeout):
                while True:
                    if generation != self._delivery_generation or self.closed.is_set():
                        raise MessageDeliveryError(
                            "Iterator admission belongs to a retired generation"
                        )
                    if not self.messages_queue.full():
                        self.messages_queue.put_nowait(item)
                        self.message_ready.set()
                        return
                    self.space.clear()
                    await self.space.wait()
        except TimeoutError as exc:
            raise MessageDeliveryError(
                f"Iterator delivery queue remained full for {self.delivery_timeout:.3f}s"
            ) from exc

    def try_reserve(self, logical_bytes: int, references: int) -> DeliveryToken:
        limit = self.accounted_limit
        if limit is not None and self.pending_bytes + logical_bytes > limit:
            return None
        self.pending_bytes += logical_bytes
        self.pending_high_water_bytes = max(self.pending_high_water_bytes, self.pending_bytes)
        if references == 1:
            return logical_bytes
        return _SharedDeliveryReservation(logical_bytes)

    async def reserve_slow(
        self,
        logical_bytes: int,
        references: int,
        *,
        generation: int,
        require_callback_open: bool,
    ) -> DeliveryToken:
        self.waiters += 1
        try:
            while True:
                if generation != self._delivery_generation or self.closed.is_set():
                    raise MessageDeliveryError("Delivery admission belongs to a retired generation")
                if require_callback_open and self._callback_state != "open":
                    raise MessageDeliveryError("Callback delivery generation is closing")
                self.space.clear()
                token = self.try_reserve(logical_bytes, references)
                if token is not None:
                    return token
                await self.space.wait()
        finally:
            self.waiters -= 1

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

    def logical_size(self, message: Message) -> int:
        property_bytes = 0
        if self.protocol == MQTTProtocolVersion.MQTTv5 and message.properties:
            property_bytes = len(encode_properties(message.properties, PUBLISH))
        topic_bytes = (
            len(message.topic) if message.topic.isascii() else len(message.topic.encode("utf-8"))
        )
        return len(message.payload) + topic_bytes + property_bytes

    def _decoded_logical_size(self, message: Message, property_wire_size: int) -> int:
        topic_bytes = (
            len(message.topic) if message.topic.isascii() else len(message.topic.encode("utf-8"))
        )
        return len(message.payload) + topic_bytes + property_wire_size

    def _reservable_size(self, message: Message, property_wire_size: int | None = None) -> int:
        """Logical size to reserve, refusing anything the budget can never hold.

        With a trusted decode-time table size, use it; otherwise re-encode the
        property table through `logical_size` (which is also the test seam).
        """
        logical_bytes = (
            self.logical_size(message)
            if property_wire_size is None
            else self._decoded_logical_size(message, property_wire_size)
        )
        limit = self.accounted_limit
        if limit is not None and logical_bytes > limit:
            raise MessageDeliveryError(
                f"Message requires {logical_bytes} delivery bytes, exceeding limit {limit}"
            )
        return logical_bytes

    def ensure_callback_worker(self) -> None:
        if self._callback_state != "open":
            raise MessageDeliveryError("Callback delivery is closing")
        self._start_callback_worker()

    def _start_callback_worker(self) -> None:
        task = self.callback_task
        if task is not None and task.done():
            self._callback_worker_done(task)
        if self._callback_state == "closed":
            raise MessageDeliveryError("Callback delivery is closed")
        if self.callback_task is None:
            coro = self._callback_worker()
            try:
                task = asyncio.create_task(coro, name="mqttium-callback-worker")
            except BaseException:
                coro.close()
                raise
            self.callback_task = task
            task.add_done_callback(self._callback_worker_done)

    def _callback_worker_done(self, task: asyncio.Task[None]) -> None:
        if self.callback_task is not task:
            return
        self.callback_task = None
        if not task.cancelled() and (exc := task.exception()) is not None:
            # An internal failure must not cause an infinite restart loop.
            self._callback_state = "closed"
            self._discard_callback_queue()
            self.report_callback_error(None, exc)
        elif self._callback_state != "closed" and not self.callback_queue.empty():
            # The controller, not a task incarnation, owns unstarted jobs.
            self._start_callback_worker()

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

    @staticmethod
    def _is_async_callback(callback: Callable[..., Any]) -> bool:
        """Recognise coroutine functions, including async callable objects."""
        if isinstance(callback, (FunctionType, MethodType)):
            return bool(callback.__code__.co_flags & inspect.CO_COROUTINE)
        return inspect.iscoroutinefunction(callback) or inspect.iscoroutinefunction(
            type(callback).__call__
        )

    def can_dispatch_callback_inline(self, callback: Callable[..., Any]) -> bool:
        """Whether a plain synchronous callback can run without a queue hop."""
        return (
            not self._callback_active
            and self.callback_queue.empty()
            and not self._is_async_callback(callback)
        )

    @staticmethod
    def _sync_awaitable_error(result: Any) -> TypeError:
        """Reject a sync callback that dynamically returned async work."""
        if inspect.iscoroutine(result):
            result.close()
        return TypeError(
            "synchronous callbacks must not return awaitables; "
            "declare asynchronous callbacks with 'async def'"
        )

    def run_sync_callback(self, callback: Callable[..., Any], *args: Any) -> None:
        """Invoke one declared-sync callback and isolate application failures."""
        try:
            result = callback(*args)
        except asyncio.CancelledError as exc:
            self._propagate_callback_cancellation(callback, exc)
        except Exception as exc:
            self.report_callback_error(callback, exc)
        else:
            if result is not None and inspect.isawaitable(result):
                self.report_callback_error(callback, self._sync_awaitable_error(result))

    def try_dispatch_callback_inline(self, callback: Callable[..., Any], *args: Any) -> bool:
        """Run one idle synchronous callback now, isolating application errors."""
        if not self.can_dispatch_callback_inline(callback):
            return False
        self.dispatch_callback_inline(callback, *args)
        return True

    def dispatch_callback_inline(self, callback: Callable[..., Any], *args: Any) -> None:
        """Invoke a callback after the caller established inline eligibility."""
        self._callback_active = True
        try:
            self.run_sync_callback(callback, *args)
        finally:
            self._callback_active = False

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
        generation = self._delivery_generation
        try:
            async with asyncio.timeout(self.delivery_timeout):
                while True:
                    if generation != self._delivery_generation:
                        raise MessageDeliveryError(
                            "Callback admission belongs to a retired generation"
                        )
                    self.ensure_callback_worker()
                    if not self.callback_queue.full():
                        self.callback_queue.put_nowait(job)
                        return
                    if asyncio.current_task() is self.callback_task:
                        raise MessageDeliveryError("A callback cannot wait for its own full queue")
                    self.space.clear()
                    await self.space.wait()
        except TimeoutError as exc:
            raise MessageDeliveryError(
                f"Callback delivery queue remained full for {self.delivery_timeout:.3f}s"
            ) from exc

    def _propagate_callback_cancellation(
        self,
        callback: Callable[..., Any] | None,
        exc: asyncio.CancelledError,
    ) -> None:
        """Propagate worker cancellation, but report callback self-cancellation."""
        task = asyncio.current_task()
        if task is None or task.cancelling():
            raise exc
        self.report_callback_error(callback, exc)

    def _discard_callback_queue(self) -> None:
        while not self.callback_queue.empty():
            _callback, _args, token = self.callback_queue.get_nowait()
            if token is not None:
                self.release_nowait(token)
            self.callback_queue.task_done()
        self.space.set()

    async def _callback_worker(self) -> None:
        # Cold entry only: register ownership before an eager factory can call
        # user code. No task is allocated for an individual notification.
        if self.callback_task is None or self.callback_task.done():
            await asyncio.sleep(0)
        queue = self.callback_queue
        task = asyncio.current_task()
        assert task is not None
        previous: Callable[..., Any] | None = None
        asynchronous = False
        while self._callback_state != "closed":
            if self._callback_state == "draining" and queue.empty():
                return
            job = await queue.get()
            generation = self._delivery_generation
            remaining = 1 + queue.qsize()
            while True:
                self.space.set()
                callback, args, token = job
                self._callback_active = True
                try:
                    if callback is not previous:
                        asynchronous = self._is_async_callback(callback)
                        previous = callback
                    if asynchronous:
                        try:
                            await callback(*args)
                        except asyncio.CancelledError as exc:
                            self._propagate_callback_cancellation(callback, exc)
                        except Exception as exc:
                            self.report_callback_error(callback, exc)
                    else:
                        self.run_sync_callback(callback, *args)
                finally:
                    self._callback_active = False
                    if token is not None:
                        self.release_nowait(token)
                    queue.task_done()
                del job, args, token
                if task.cancelling():
                    await asyncio.sleep(0)
                if self._callback_state == "closed":
                    return
                remaining -= 1
                if not remaining or generation != self._delivery_generation:
                    break
                job = queue.get_nowait()
            # Reentrant arrivals cannot extend a round indefinitely. All
            # unstarted jobs remain counted in the queue, never in a side list.
            if not queue.empty():
                await asyncio.sleep(0)

    @classmethod
    async def invoke(cls, callback: Callable[..., Any] | None, *args: Any) -> Any:
        if callback is None:
            return None
        if cls._is_async_callback(callback):
            return await callback(*args)
        result = callback(*args)
        if result is not None and inspect.isawaitable(result):
            raise cls._sync_awaitable_error(result)
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
        generation = self._delivery_generation
        if self._callback_state != "closed":
            self._callback_state = "draining" if drain else "closed"
        self.space.set()
        if self._callback_state == "closed":
            self._discard_callback_queue()
        if self.callback_task is asyncio.current_task():
            # An own-worker shutdown cannot join itself. Return to finish the
            # active notification; a reconnect may reopen the same consumer.
            return
        try:
            if drain and self._callback_state == "draining":
                if not self.callback_queue.empty():
                    self._start_callback_worker()
                try:
                    async with asyncio.timeout(self.callback_shutdown_timeout):
                        await self.callback_queue.join()
                except TimeoutError:
                    pass
        finally:
            if generation == self._delivery_generation:
                self._callback_state = "closed"
                self._discard_callback_queue()
                task = self.callback_task
                if task is not None:
                    if not task.done():
                        task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
