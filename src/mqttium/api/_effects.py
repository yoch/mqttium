"""Ordered runtime application of protocol-engine effects.

``EffectPump`` owns the connection-scoped effect deque and the asynchronous
flusher. The client remains the interpreter of individual effects because it
owns transports, futures, receipts, callbacks and delivery queues.
"""

from __future__ import annotations

import asyncio
from collections import deque
from typing import TYPE_CHECKING, Protocol

from mqttium.api.stats import EffectStats
from mqttium.enums import ConnectionState
from mqttium.protocol.effects import EffectKind, EngineEffect

if TYPE_CHECKING:
    from mqttium.protocol.engine import ProtocolEngine


class StaleConnectionEffect(Exception):
    """An effect was produced for a transport epoch that is no longer current."""


class EffectOwner(Protocol):
    _connection_epoch: int
    _engine: ProtocolEngine

    def _apply_effect_inline(self, effect: EngineEffect, epoch: int) -> bool: ...

    def _apply_message_effect_batch_inline(
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
        self.pending_epoch: int | None = None
        self.enqueued = 0
        self.applied = 0
        self.pending_high_water = 0
        self.progress = asyncio.Event()
        self.waiters = 0
        self.error: BaseException | None = None
        self.task: asyncio.Task[None] | None = None
        self.flush_requested = False
        self.draining_inline = False
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

    def drain_inline(self) -> None:
        if self.draining_inline or self.lock.locked():
            return
        epoch = self.pending_epoch
        if epoch is None:
            return
        if epoch != self.owner._connection_epoch:
            self.discard_connection_effects()
            epoch = self.pending_epoch
            if epoch is None:
                return
        self.draining_inline = True
        try:
            while self.pending:
                effect = self.pending[0]
                if not self.owner._apply_effect_inline(effect, epoch):
                    break
                self.pending.popleft()
                self.inline_effects += 1
                self._complete()
            if not self.pending:
                self.pending_epoch = None
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
                    if epoch is None:
                        self.pending.clear()
                        break
                    if epoch != self.owner._connection_epoch:
                        self.discard_connection_effects()
                        continue
                    if effect.kind is EffectKind.MESSAGE:
                        applied_inline = self.owner._apply_message_effect_batch_inline(
                            self.pending, epoch
                        )
                        if applied_inline:
                            for _ in range(applied_inline):
                                self.pending.popleft()
                                self.inline_effects += 1
                                self._complete()
                            if not self.pending:
                                self.pending_epoch = None
                            continue
                    try:
                        await self.owner._apply_effect(effect, nowait=False, epoch=epoch)
                    except asyncio.CancelledError:
                        raise
                    except StaleConnectionEffect:
                        if self.pending and self.pending[0] is effect:
                            self.pending.popleft()
                            self._complete()
                    except BaseException as exc:
                        if self.pending and self.pending[0] is effect:
                            self.pending.popleft()
                            self._complete()
                        self.error = exc
                        if self.owner._engine.state is ConnectionState.CONNECTING:
                            connack_fut = getattr(self.owner, "_connack_fut", None)
                            if connack_fut is not None and not connack_fut.done():
                                connack_fut.set_exception(exc)
                        if self.waiters:
                            self.progress.set()
                        raise
                    else:
                        if self.pending and self.pending[0] is effect:
                            self.pending.popleft()
                            self._complete()
                    if not self.pending:
                        self.pending_epoch = None
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
                discarded += 1
                continue
            if not settle_publish:
                retained.append(effect)
                continue
            self.owner._settle_terminal_effect(effect)
            discarded += 1

        self.pending = retained
        self.pending_epoch = self.owner._connection_epoch if retained else None
        self.applied += discarded
        if self.waiters:
            self.progress.set()
