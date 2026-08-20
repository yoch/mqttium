"""Ordered runtime application of protocol-engine effects.

``EffectPump`` owns the connection-scoped effect deque and the asynchronous
flusher. The client remains the interpreter of individual effects because it
owns transports, futures, receipts, callbacks and delivery queues.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Protocol, cast

from mqttium.api.stats import EffectStats
from mqttium.protocol.effects import EffectKind, EngineEffect

if TYPE_CHECKING:
    from mqttium.protocol.engine import ProtocolEngine


# Runtime-private physical marker. It is intentionally not an EffectKind member:
# ProtocolEngine and AsyncClient retain their closed/exhaustive protocol effect
# vocabulary, while generic MESSAGE delivery still sees this as a hard boundary.
_MESSAGE_BATCH_KIND = cast(EffectKind, object())


class _MessageEffectBatch:
    """Adjacent logical MESSAGE effects represented by one physical effect."""

    __slots__ = ("messages", "offset")

    def __init__(self, first: object, second: object) -> None:
        self.messages = [first, second]
        self.offset = 0


class _MessageBatchIterator:
    """Allocation-free iterator that exposes one reusable MESSAGE-effect proxy."""

    __slots__ = ("_batch", "_index", "_proxy")

    def __init__(self) -> None:
        self._batch: _MessageEffectBatch | None = None
        self._index = 0
        self._proxy = EngineEffect(EffectKind.MESSAGE, requires_delivery_mark=False)

    def bind(self, batch: _MessageEffectBatch | None) -> _MessageBatchIterator:
        self._batch = batch
        self._index = 0 if batch is None else batch.offset
        return self

    def __iter__(self) -> _MessageBatchIterator:
        return self

    def __next__(self) -> EngineEffect:
        batch = self._batch
        if batch is None or self._index >= len(batch.messages):
            raise StopIteration
        proxy = self._proxy
        proxy.data = batch.messages[self._index]
        self._index += 1
        return proxy


class _MessageBatchEffectView(deque[EngineEffect]):
    """Deque-shaped logical view consumed by the existing delivery fast path."""

    __slots__ = ("_batch", "_iterator")

    def __init__(self) -> None:
        super().__init__()
        self._batch: _MessageEffectBatch | None = None
        self._iterator = _MessageBatchIterator()

    def bind(self, batch: _MessageEffectBatch) -> _MessageBatchEffectView:
        self._batch = batch
        return self

    def __len__(self) -> int:
        batch = self._batch
        if batch is None:
            return 0
        return len(batch.messages) - batch.offset

    def __iter__(self) -> Iterator[EngineEffect]:
        return self._iterator.bind(self._batch)


class StaleConnectionEffect(Exception):
    """An effect was produced for a transport epoch that is no longer current."""


class EffectOwner(Protocol):
    _connection_epoch: int
    _engine: ProtocolEngine

    def _apply_effect_inline(self, effect: EngineEffect, epoch: int) -> bool: ...

    def _apply_message_effect_batch_inline(
        self, effects: deque[EngineEffect], epoch: int
    ) -> int: ...

    def _apply_decoded_message_effect_batch_inline(
        self, effects: deque[EngineEffect], epoch: int
    ) -> int: ...

    async def _apply_effect(
        self,
        effect: EngineEffect,
        *,
        nowait: bool,
        epoch: int | None = None,
    ) -> None: ...

    def _settle_terminal_effect(self, effect: EngineEffect) -> None: ...


class EffectPump:
    """Serialize engine effects without charging the single-effect fast path.

    A lone immediately-applicable effect is interpreted inline and never enters
    the deque or progress counters. Only genuinely asynchronous work is tagged
    with a connection epoch and owned by the scheduled flusher.
    """

    def __init__(self, owner: EffectOwner) -> None:
        self.owner = owner
        self.lock = asyncio.Lock()
        self.pending: deque[EngineEffect] = deque()
        self.pending_epoch = owner._connection_epoch
        self.enqueued = 0
        self.applied = 0
        self.pending_high_water = 0
        self.progress = asyncio.Event()
        self.waiters = 0
        self.error: BaseException | None = None
        self.task: asyncio.Task[None] | None = None
        self.flush_requested = False
        self.draining_inline = False
        self._message_batch_view = _MessageBatchEffectView()
        self._message_batch_slow_effect = EngineEffect(
            EffectKind.MESSAGE, requires_delivery_mark=False
        )
        # Decision counters. The SEND-first partition protects an ordering that
        # is easy to break and hard to debug, so before changing how batches are
        # represented it has to be clear how often several effects even arrive
        # together, and how often the inline fast path actually carries them.
        self.batches = 0
        self.multi_effect_batches = 0
        self.reordered_batches = 0
        self.inline_effects = 0
        self.apply_suspensions = 0

    def collect_from_engine(self) -> None:  # noqa: C901
        effects = self.owner._engine.take_effects()
        if not effects:
            return
        epoch = self.owner._connection_epoch
        self.batches += 1
        logical_count = len(effects)

        if self.pending and self.pending_epoch != epoch:
            self.discard_connection_effects()

        if logical_count == 1 and not self.pending:
            if self.owner._apply_effect_inline(effects[0], epoch):
                self.inline_effects += 1
                return

        if logical_count > 1:
            self.multi_effect_batches += 1
            # Partition SEND-first in one pass, and let that same pass answer
            # whether the batch needed it. The same pass also compacts adjacent
            # non-persisted MESSAGE effects; no second scan is added to ingress.
            # A private marker keeps compact runs opaque to generic delivery.
            sends: list[EngineEffect] = []
            others: list[EngineEffect] = []
            out_of_order = False
            compacted = False
            message_boundary = False
            for effect in effects:
                if effect.kind is EffectKind.SEND:
                    if others:
                        out_of_order = True
                    sends.append(effect)
                    message_boundary = True
                    continue

                if (
                    effect.kind is EffectKind.MESSAGE
                    and not effect.requires_delivery_mark
                    and effect.decoded_property_wire_size is None
                    and others
                    and not message_boundary
                ):
                    previous = others[-1]
                    if (
                        previous.kind is EffectKind.MESSAGE
                        and not previous.requires_delivery_mark
                        and previous.decoded_property_wire_size is None
                    ):
                        previous.kind = _MESSAGE_BATCH_KIND
                        previous.data = _MessageEffectBatch(previous.data, effect.data)
                        compacted = True
                        continue
                    if previous.kind is _MESSAGE_BATCH_KIND:
                        batch: _MessageEffectBatch = previous.data
                        batch.messages.append(effect.data)
                        compacted = True
                        continue
                others.append(effect)
                message_boundary = False

            if out_of_order:
                self.reordered_batches += 1
                effects = sends + others
            elif compacted:
                # When no SEND followed a non-SEND, every SEND is already a
                # prefix. Rebuild only because compaction removed physical
                # MESSAGE effects from the suffix.
                effects = others if not sends else sends + others
        if not self.pending:
            self.pending_epoch = epoch
        self.pending.extend(effects)
        self.enqueued += logical_count
        pending = self.enqueued - self.applied
        if pending > self.pending_high_water:
            self.pending_high_water = pending

    def stats(self) -> EffectStats:
        """Snapshot logical work while the deque may use compact batches."""
        pending = self.enqueued - self.applied
        return EffectStats(
            pending=pending,
            pending_high_water=max(self.pending_high_water, pending),
            enqueued=self.enqueued,
            applied=self.applied,
            waiters=self.waiters,
            batches=self.batches,
            multi_effect_batches=self.multi_effect_batches,
            reordered_batches=self.reordered_batches,
            inline_effects=self.inline_effects,
            apply_suspensions=self.apply_suspensions,
        )

    def _complete(self, count: int = 1) -> None:
        self.applied += count
        if self.waiters:
            self.progress.set()

    def _consume_batch(
        self,
        apply: Callable[[deque[EngineEffect], int], int],
        epoch: int,
    ) -> bool:
        """Apply a consecutive non-persisted small-message prefix.

        `apply` is the owner's MESSAGE or DECODED_MESSAGE batch acceptor; only
        which one is bound differs between the two kinds. One call per batch,
        not per message.
        """
        applied = apply(self.pending, epoch)
        if not applied:
            return False
        for _ in range(applied):
            self.pending.popleft()
        self.inline_effects += applied
        self._complete(applied)
        return True

    def _consume_message_batch(self, epoch: int) -> bool:
        effect = self.pending[0]
        batch: _MessageEffectBatch = effect.data
        remaining = len(batch.messages) - batch.offset
        applied = self.owner._apply_message_effect_batch_inline(
            self._message_batch_view.bind(batch), epoch
        )
        if not applied:
            return False
        if applied > remaining:
            raise RuntimeError("message batch acceptor consumed beyond the logical batch")
        batch.offset += applied
        self.inline_effects += applied
        self._complete(applied)
        if batch.offset == len(batch.messages):
            self.pending.popleft()
        return True

    def _slow_effect(self, effect: EngineEffect) -> EngineEffect:
        if effect.kind is not _MESSAGE_BATCH_KIND:
            return effect
        batch: _MessageEffectBatch = effect.data
        proxy = self._message_batch_slow_effect
        proxy.data = batch.messages[batch.offset]
        return proxy

    def _advance_head(self, effect: EngineEffect) -> None:
        if not self.pending or self.pending[0] is not effect:
            return
        if effect.kind is _MESSAGE_BATCH_KIND:
            batch: _MessageEffectBatch = effect.data
            batch.offset += 1
            if batch.offset < len(batch.messages):
                self._complete()
                return
        self.pending.popleft()
        self._complete()

    def drain_inline(self) -> None:
        if self.draining_inline or self.lock.locked():
            return
        if not self.pending:
            return
        epoch = self.pending_epoch
        if epoch != self.owner._connection_epoch:
            self.discard_connection_effects()
            if not self.pending:
                return
            epoch = self.pending_epoch
        self.draining_inline = True
        try:
            while self.pending:
                effect = self.pending[0]
                if effect.kind is _MESSAGE_BATCH_KIND:
                    if self._consume_message_batch(epoch):
                        continue
                    break
                if effect.kind is EffectKind.MESSAGE:
                    if self._consume_batch(self.owner._apply_message_effect_batch_inline, epoch):
                        continue
                    break
                if effect.kind is EffectKind.DECODED_MESSAGE:
                    if self._consume_batch(
                        self.owner._apply_decoded_message_effect_batch_inline, epoch
                    ):
                        continue
                    break
                if not self.owner._apply_effect_inline(effect, epoch):
                    break
                self.pending.popleft()
                self.inline_effects += 1
                self._complete()
        finally:
            self.draining_inline = False
        if self.pending:
            self.schedule()

    def schedule(self) -> None:
        self.flush_requested = True
        task = self.task
        if task is not None and not task.done():
            return
        task = asyncio.create_task(self._run_scheduled(), name="mqttium-effect-flush")
        self.task = task
        task.add_done_callback(self._done)

    async def _run_scheduled(self) -> None:  # noqa: C901
        async with self.lock:
            while True:
                self.flush_requested = False
                while self.pending:
                    effect = self.pending[0]
                    epoch = self.pending_epoch
                    if epoch != self.owner._connection_epoch:
                        self.discard_connection_effects()
                        continue
                    if effect.kind is _MESSAGE_BATCH_KIND:
                        if self._consume_message_batch(epoch):
                            continue
                    elif effect.kind is EffectKind.MESSAGE:
                        if self._consume_batch(
                            self.owner._apply_message_effect_batch_inline, epoch
                        ):
                            continue
                    elif effect.kind is EffectKind.DECODED_MESSAGE:
                        if self._consume_batch(
                            self.owner._apply_decoded_message_effect_batch_inline, epoch
                        ):
                            continue
                    apply_effect = self._slow_effect(effect)
                    try:
                        await self.owner._apply_effect(apply_effect, nowait=False, epoch=epoch)
                    except asyncio.CancelledError:
                        raise
                    except StaleConnectionEffect:
                        self._advance_head(effect)
                    except BaseException as exc:
                        self._advance_head(effect)
                        self.error = exc
                        connack_fut = getattr(self.owner, "_connack_fut", None)
                        if connack_fut is not None and not connack_fut.done():
                            connack_fut.set_exception(exc)
                        if self.waiters:
                            self.progress.set()
                        raise
                    else:
                        self._advance_head(effect)
                if not self.flush_requested:
                    return

    def _done(self, task: asyncio.Task[None]) -> None:
        if self.task is task:
            self.task = None
        try:
            task.result()
        except asyncio.CancelledError:
            return
        except Exception as exc:
            if not self.pending:
                # `self.error` exists to unblock a caller whose effects the
                # flush task failed to apply. The failing effect is popped and
                # counted before the raise, so an empty deque means applied has
                # caught up with enqueued: every waiter's target is already met
                # and no future drain() can be blocked by this failure. The
                # exception has no owner, and retaining it would hand a failure
                # caused by the peer to the next unrelated call that suspends in
                # drain(). A non-empty deque is the opposite case — work really
                # is stuck, so the error stays for whoever is waiting on it.
                self.error = None
            asyncio.get_running_loop().call_exception_handler(
                {
                    "message": "mqttium scheduled effect flush failed",
                    "exception": exc,
                    "task": task,
                }
            )

    async def drain(self, *, nowait: bool = False) -> None:
        self.drain_inline()
        if nowait:
            if self.pending:
                self.schedule()
            return
        target = self.enqueued
        if self.applied >= target:
            return
        self.waiters += 1
        self.apply_suspensions += 1
        try:
            while self.applied < target:
                if self.error is not None:
                    error = self.error
                    self.error = None
                    raise error
                self.progress.clear()
                self.schedule()
                if self.applied >= target:
                    break
                await asyncio.shield(self.progress.wait())
        finally:
            self.waiters -= 1

    def discard_connection_effects(self, *, settle_publish: bool = False) -> None:
        """Drop transport effects and preserve or settle terminal publishes."""
        retained: deque[EngineEffect] = deque()
        discarded = 0
        for effect in self.pending:
            if effect.kind not in (
                EffectKind.PUBLISH_COMPLETE,
                EffectKind.PUBLISH_FAILED,
            ):
                if effect.kind is _MESSAGE_BATCH_KIND:
                    batch: _MessageEffectBatch = effect.data
                    discarded += len(batch.messages) - batch.offset
                else:
                    discarded += 1
                continue
            if not settle_publish:
                retained.append(effect)
                continue
            self.owner._settle_terminal_effect(effect)
            discarded += 1

        self.pending = retained
        if retained:
            self.pending_epoch = self.owner._connection_epoch
        self.applied += discarded
        if self.waiters:
            self.progress.set()
