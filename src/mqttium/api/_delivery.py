"""Application delivery with batched high-rate message callbacks."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable
from typing import Any, cast

from mqttium.api._delivery_base import (
    AccountedDeliveryToken,
    ApplicationDelivery as _BaseApplicationDelivery,
    MessageDelivery,
    _fits_small_limit,
)
from mqttium.api.stats import DeliveryStats
from mqttium.codec.properties import PUBLISH, encode_properties
from mqttium.enums import MQTTProtocolVersion
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.types import Message


class _CallbackMessageBatchToken:
    __slots__ = ()


_CALLBACK_MESSAGE_BATCH = _CallbackMessageBatchToken()


class ApplicationDelivery(_BaseApplicationDelivery):
    """Batch adjacent callbacks while preserving the logical queue bound."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._callback_limit = self.callback_queue.maxsize
        self._callback_batch_reserved = 0

    # Preserve the historical monkeypatch seam on this module.
    def logical_size(self, message: Message) -> int:
        property_bytes = 0
        if self.protocol == MQTTProtocolVersion.MQTTv5 and message.properties:
            property_bytes = len(encode_properties(message.properties, PUBLISH))
        topic_bytes = (
            len(message.topic)
            if message.topic.isascii()
            else len(message.topic.encode("utf-8"))
        )
        return len(message.payload) + topic_bytes + property_bytes

    def stats(self) -> DeliveryStats:
        stats = super().stats()
        return DeliveryStats(
            iterator_queued=stats.iterator_queued,
            iterator_limit=stats.iterator_limit,
            callback_queued=self.callback_queue.qsize() + self._callback_batch_reserved,
            callback_limit=self._callback_limit,
            pending_bytes=stats.pending_bytes,
            pending_high_water_bytes=stats.pending_high_water_bytes,
            accounted_limit=stats.accounted_limit,
            small_budget_bytes=stats.small_budget_bytes,
            small_message_limit=stats.small_message_limit,
            waiters=stats.waiters,
        )

    def _reserve_batch(self, count: int) -> None:
        extra = count - 1
        if extra <= 0:
            return
        self._callback_batch_reserved += extra
        self.callback_queue._maxsize -= extra

    def _release_batch(self, count: int) -> None:
        extra = count - 1
        if extra <= 0:
            return
        assert self._callback_batch_reserved >= extra
        self._callback_batch_reserved -= extra
        self.callback_queue._maxsize += extra
        putters = self.callback_queue._putters
        for _ in range(min(extra, len(putters))):
            self.callback_queue._wakeup_next(putters)

    def _callback_capacity(self, iterator_delivery: bool) -> int:
        capacity = self.callback_queue.maxsize - self.callback_queue.qsize()
        if iterator_delivery:
            capacity = min(
                capacity,
                self.messages_queue.maxsize - self.messages_queue.qsize(),
            )
        return max(0, capacity)

    def _enqueue_batch(
        self,
        callback: Callable[[Message], Any],
        messages: list[Message],
        *,
        iterator_delivery: bool,
    ) -> None:
        if iterator_delivery:
            for message in messages:
                self.messages_queue.put_nowait(message)
            self.message_ready.set()
        self.ensure_callback_worker()
        if len(messages) == 1:
            self.callback_queue.put_nowait((callback, (messages[0],), None))
            return
        self.callback_queue.put_nowait((callback, (messages,), _CALLBACK_MESSAGE_BATCH))
        self._reserve_batch(len(messages))

    def deliver_batch_inline(
        self,
        effects: deque[EngineEffect],
        callback: Callable[[Message], Any] | None,
    ) -> int:
        callback_delivery, iterator_delivery = self._modes(callback)
        if not callback_delivery or len(effects) <= 1:
            return super().deliver_batch_inline(effects, callback)
        assert callback is not None
        capacity = self._callback_capacity(iterator_delivery)
        if capacity <= 0:
            return 0
        messages: list[Message] = []
        for effect in effects:
            if len(messages) >= capacity or effect.kind is not EffectKind.MESSAGE:
                break
            message: Message = effect.data
            if effect.requires_delivery_mark or not self._is_small(message):
                break
            messages.append(message)
        if len(messages) <= 1:
            return super().deliver_batch_inline(effects, callback)
        self._enqueue_batch(callback, messages, iterator_delivery=iterator_delivery)
        return len(messages)

    def deliver_decoded_batch_inline(
        self,
        effects: deque[EngineEffect],
        callback: Callable[[Message], Any] | None,
    ) -> int:
        callback_delivery, iterator_delivery = self._modes(callback)
        if not callback_delivery or len(effects) <= 1:
            return super().deliver_decoded_batch_inline(effects, callback)
        assert callback is not None
        capacity = self._callback_capacity(iterator_delivery)
        if capacity <= 0:
            return 0
        messages: list[Message] = []
        for effect in effects:
            if len(messages) >= capacity or effect.kind is not EffectKind.DECODED_MESSAGE:
                break
            message: Message = effect.data
            property_wire_size = effect.decoded_property_wire_size
            if (
                effect.requires_delivery_mark
                or property_wire_size is None
                or not self._is_small_decoded(message, property_wire_size)
            ):
                break
            messages.append(message)
        if len(messages) <= 1:
            return super().deliver_decoded_batch_inline(effects, callback)
        self._enqueue_batch(callback, messages, iterator_delivery=iterator_delivery)
        return len(messages)

    async def _callback_worker(self) -> None:
        while True:
            callback, args, token = await self.callback_queue.get()
            try:
                if token is _CALLBACK_MESSAGE_BATCH:
                    messages = args[0]
                    try:
                        for message in messages:
                            try:
                                await self.invoke(callback, message)
                            except asyncio.CancelledError:
                                raise
                            except Exception as exc:
                                self.report_callback_error(callback, exc)
                    finally:
                        self._release_batch(len(messages))
                    continue
                try:
                    await self.invoke(callback, *args)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.report_callback_error(callback, exc)
                finally:
                    if token is not None:
                        self.release_nowait(cast(AccountedDeliveryToken, token))
            finally:
                self.callback_queue.task_done()

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
                _callback, args, token = self.callback_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if token is _CALLBACK_MESSAGE_BATCH:
                self._release_batch(len(args[0]))
            elif token is not None:
                self.release_nowait(cast(AccountedDeliveryToken, token))
            self.callback_queue.task_done()
