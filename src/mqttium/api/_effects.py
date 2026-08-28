"""Ordered runtime application of protocol-engine effects.

``EffectPump`` owns the connection-scoped effect deque and the asynchronous
flusher. The client remains the interpreter of individual effects because it
owns transports, futures, receipts, callbacks and delivery queues.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable
from typing import TYPE_CHECKING, Protocol

from mqttium.api.stats import EffectStats
from mqttium.protocol.effects import EffectKind, EngineEffect

if TYPE_CHECKING:
    from mqttium.protocol.engine import ProtocolEngine


class StaleConnectionEffect(Exception):
    """An effect was produced for a transport epoch that is no longer current."""


class EffectOwner(Protocol):
    _connection_epoch: int
    _disconnect_exc: BaseException | None
    _engine: ProtocolEngine

    def _apply_effect_inline(self, effect: EngineEffect, epoch: int) -> bool: ...

    def _apply_message_effect_batch_inline(
        self, effects: deque[EngineEffect], epoch: int
    ) -> int: ...

    def _apply_terminal_effect_batch_inline(
        self, effects: deque[EngineEffect], epoch: int
    ) -> int: ...

    async def _apply_effect(
        self,
        effect: EngineEffect,
        *,
        nowait: bool,
        epoch: int | None = None,
    ) -> None: ...

    async def _close_transport_after_connection_failure(self) -> None: ...

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
        self._next_waiter_id = 0
        self._waiter_targets: dict[int, int] = {}
        self._error_waiters: set[int] = set()
        self.task: asyncio.Task[None] | None = None
        self.flush_requested = False
        self.draining_inline = False
        self._failing_close = False
        # Decision counters. The SEND-first partition protects an ordering that
        # is easy to break and hard to debug, so before changing how batches are
        # represented it has to be clear how often several effects even arrive
        # together, and how often the inline fast path actually carries them.
        self.batches = 0
        self.multi_effect_batches = 0
        self.reordered_batches = 0
        self.inline_effects = 0
        self.apply_suspensions = 0

    def collect_from_engine(self) -> None:
        effects = self.owner._engine.take_effects()
        if not effects:
            return
        epoch = self.owner._connection_epoch
        self.batches += 1

        if self.pending and self.pending_epoch != epoch:
            self.discard_connection_effects()

        if self._failing_close:
            # A failing flush still owns the pump lock while connection close
            # is awaited. Effects collected in that window belong to the dead
            # connection and can never be applied; settle them immediately so
            # a later drain cannot wait for impossible progress.
            if not self.pending:
                self.pending_epoch = epoch
            self.pending.extend(effects)
            self.enqueued += len(effects)
            self.pending_high_water = max(self.pending_high_water, len(self.pending))
            self.discard_connection_effects(settle_publish=True)
            return

        if len(effects) == 1 and not self.pending:
            if self.owner._apply_effect_inline(effects[0], epoch):
                self.inline_effects += 1
                return

        if len(effects) > 1:
            self.multi_effect_batches += 1
            # Partition SEND-first in one pass, and let that same pass answer
            # whether the batch needed it: a SEND seen after a non-SEND is
            # exactly the condition. The two-generator form this replaces walked
            # the batch twice and always rebuilt the list, even for the common
            # batch that was already ordered.
            #
            # Splitting this into a detect pass followed by a partition pass
            # was tried and reverted: it saves two list allocations on an
            # already-ordered batch but adds a scan to the batch that must be
            # reordered, and the paired microbenchmark did not support the
            # trade. See docs/reports/PERFORMANCE-AUDIT-0.2.0b4.md.
            sends: list[EngineEffect] = []
            others: list[EngineEffect] = []
            out_of_order = False
            for effect in effects:
                if effect.kind is EffectKind.SEND:
                    if others:
                        out_of_order = True
                    sends.append(effect)
                else:
                    others.append(effect)
            if out_of_order:
                self.reordered_batches += 1
                effects = sends + others
        if not self.pending:
            self.pending_epoch = epoch
        self.pending.extend(effects)
        self.enqueued += len(effects)
        pending = len(self.pending)
        if pending > self.pending_high_water:
            self.pending_high_water = pending

    def record_inline_batch(self, count: int) -> None:
        """Record logical effects delivered without entering the pending deque."""
        self.batches += 1
        if count > 1:
            self.multi_effect_batches += 1
        self.enqueued += count
        self.applied += count
        self.inline_effects += count
        if self.waiters:
            self.progress.set()

    def stats(self) -> EffectStats:
        """Snapshot the deque and the ordering decisions taken so far."""
        pending = len(self.pending)
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

    def _complete(self) -> None:
        self.applied += 1
        if self.waiters:
            self.progress.set()

    def _consume_batch(
        self,
        apply: Callable[[deque[EngineEffect], int], int],
        epoch: int,
    ) -> bool:
        """Apply a consecutive effect prefix accepted by the owner.

        One owner call covers the whole accepted prefix, then the pump advances
        its ordered deque and progress counters without interpreting each
        effect again.
        """
        applied = apply(self.pending, epoch)
        if not applied:
            return False
        for _ in range(applied):
            self.pending.popleft()
            self.inline_effects += 1
            self._complete()
        return True

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
                kind = effect.kind
                if kind is EffectKind.MESSAGE or kind is EffectKind.DECODED_MESSAGE:
                    if self._consume_batch(self.owner._apply_message_effect_batch_inline, epoch):
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

    def drain_ingress_ack_batch_inline(self) -> None:
        """Apply an ingress batch ending in terminal publish results.

        The reader calls this only after decoding several packets and observing
        a terminal effect at the tail. Keeping that detection outside
        ``drain_inline`` leaves the general SEND/QoS 0 loop unchanged.
        """
        if self.draining_inline or self.lock.locked() or not self.pending:
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
                kind = effect.kind
                if kind is EffectKind.PUBLISH_COMPLETE or kind is EffectKind.PUBLISH_FAILED:
                    if self._consume_batch(self.owner._apply_terminal_effect_batch_inline, epoch):
                        continue
                    break
                # Message delivery has its own batch admission and may need
                # persistence marking. Let the established drain own it.
                if kind is EffectKind.MESSAGE or kind is EffectKind.DECODED_MESSAGE:
                    break
                if not self.owner._apply_effect_inline(effect, epoch):
                    break
                self.pending.popleft()
                self.inline_effects += 1
                self._complete()
        finally:
            self.draining_inline = False

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
                    kind = effect.kind
                    if kind is EffectKind.MESSAGE or kind is EffectKind.DECODED_MESSAGE:
                        if self._consume_batch(
                            self.owner._apply_message_effect_batch_inline, epoch
                        ):
                            continue
                    try:
                        await self.owner._apply_effect(effect, nowait=False, epoch=epoch)
                    except asyncio.CancelledError:
                        raise
                    except StaleConnectionEffect:
                        if self.pending and self.pending[0] is effect:
                            self.pending.popleft()
                            self._complete()
                    except Exception as exc:
                        if self.pending and self.pending[0] is effect:
                            self.pending.popleft()
                            self._complete()
                        failure_at = self.applied
                        owners = {
                            waiter_id
                            for waiter_id, target in self._waiter_targets.items()
                            if target >= failure_at
                        }
                        if owners:
                            self.error = exc
                            self._error_waiters = owners
                            self.progress.set()
                        connack_fut = getattr(self.owner, "_connack_fut", None)
                        if connack_fut is not None and not connack_fut.done():
                            connack_fut.set_exception(exc)

                        # Connection health always belongs to AsyncClient's
                        # reader-owned lifecycle. Active drain() calls still
                        # receive the same original exception below.
                        self.owner._disconnect_exc = exc
                        self._failing_close = True
                        try:
                            self.discard_connection_effects(settle_publish=True)
                            await self.owner._close_transport_after_connection_failure()
                            return
                        finally:
                            if self.pending:
                                self.discard_connection_effects(settle_publish=True)
                            self._failing_close = False
                    else:
                        if self.pending and self.pending[0] is effect:
                            self.pending.popleft()
                            self._complete()
                if not self.flush_requested:
                    return

    def _done(self, task: asyncio.Task[None]) -> None:
        owned = self.task is task
        if owned:
            self.task = None
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            if not self.pending and not self._error_waiters:
                # No current drain() owns a failure anymore. Do not retain an
                # unowned exception for a later unrelated operation.
                self.error = None
            asyncio.get_running_loop().call_exception_handler(
                {
                    "message": "mqttium scheduled effect flush failed",
                    "exception": exc,
                    "task": task,
                }
            )
        if owned and self.flush_requested and self.pending:
            self.schedule()

    async def drain(self, *, nowait: bool = False) -> None:
        self.drain_inline()
        if nowait:
            if self.pending:
                self.schedule()
            return
        target = self.enqueued
        if self.applied >= target:
            return

        waiter_id = self._next_waiter_id
        self._next_waiter_id += 1
        self._waiter_targets[waiter_id] = target
        self.waiters += 1
        self.apply_suspensions += 1
        try:
            while True:
                if waiter_id in self._error_waiters:
                    assert self.error is not None
                    raise self.error
                if self.applied >= target:
                    return
                self.progress.clear()
                self.schedule()
                if waiter_id in self._error_waiters:
                    assert self.error is not None
                    raise self.error
                if self.applied >= target:
                    return
                await asyncio.shield(self.progress.wait())
        finally:
            self._waiter_targets.pop(waiter_id, None)
            self._error_waiters.discard(waiter_id)
            self.waiters -= 1
            if self.error is not None and not self._error_waiters:
                self.error = None

    def discard_connection_effects(self, *, settle_publish: bool = False) -> None:
        """Drop transport effects and preserve or settle terminal publishes."""
        retained: deque[EngineEffect] = deque()
        discarded = 0
        for effect in self.pending:
            if effect.kind not in (
                EffectKind.PUBLISH_COMPLETE,
                EffectKind.PUBLISH_FAILED,
            ):
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
