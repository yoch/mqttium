"""Ordered runtime application of protocol-engine effects.

``EffectPump`` owns the connection-scoped effect deque and the asynchronous
flusher. The client remains the interpreter of individual effects because it
owns transports, futures, receipts, callbacks and delivery queues.
"""

from __future__ import annotations

import asyncio
from collections import deque
from typing import TYPE_CHECKING, Protocol

from mqttium.protocol.effects import EffectKind, EngineEffect

if TYPE_CHECKING:
    from mqttium.api._delivery_lane import DeliveryLane
    from mqttium.protocol.engine import ProtocolEngine


# Reused on the effect hot path. Keeping the three members in one prebuilt
# tuple avoids constructing a tuple for every single effect and for every
# non-wire member of a batch; tuple membership is also cheaper here than a
# three-way Enum identity chain.
_DELIVERY_EFFECT_KINDS = (
    EffectKind.MESSAGE,
    EffectKind.DECODED_MESSAGE,
    EffectKind.CONTINUE_INBOUND_REPLAY,
)


def _partition_effects(
    effects: list[EngineEffect],
) -> tuple[list[EngineEffect], list[EngineEffect] | None, bool]:
    """Preserve wire/result order while separating reader-owned delivery."""
    sends: list[EngineEffect] = []
    others: list[EngineEffect] = []
    # Most protocol batches contain no application delivery. Allocate this
    # third list only when the batch actually needs the delivery lane.
    deliveries: list[EngineEffect] | None = None
    reordered = False
    for effect in effects:
        kind = effect.kind
        if kind is EffectKind.SEND or kind is EffectKind.SEND_ACK:
            if others or deliveries:
                reordered = True
            sends.append(effect)
        elif kind in _DELIVERY_EFFECT_KINDS:
            if deliveries is None:
                deliveries = []
            deliveries.append(effect)
        else:
            others.append(effect)
    if reordered or deliveries:
        effects = sends + others
    return effects, deliveries, reordered


class StaleConnectionEffect(Exception):
    """An effect was produced for a transport epoch that is no longer current."""


class EffectOwner(Protocol):
    _connection_epoch: int
    _disconnect_exc: BaseException | None
    _engine: ProtocolEngine
    _delivery_lane: DeliveryLane

    def _apply_effect_inline(self, effect: EngineEffect, epoch: int) -> bool: ...

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

        deliveries: list[EngineEffect] | None = None
        if len(effects) == 1:
            effect = effects[0]
            if effect.kind in _DELIVERY_EFFECT_KINDS:
                self.owner._delivery_lane.collect(effects, epoch, self.enqueued)
                self.pending_high_water = max(
                    self.pending_high_water,
                    len(self.pending) + self.owner._delivery_lane.outstanding,
                )
                return
            if not self.pending and self.owner._apply_effect_inline(effect, epoch):
                self.inline_effects += 1
                return

        if len(effects) > 1:
            self.multi_effect_batches += 1
            effects, deliveries, reordered = _partition_effects(effects)
            self.reordered_batches += reordered
        if not self.pending:
            self.pending_epoch = epoch
        self.pending.extend(effects)
        self.enqueued += len(effects)
        if deliveries:
            self.owner._delivery_lane.collect(deliveries, epoch, self.enqueued)
        self.pending_high_water = max(
            self.pending_high_water,
            len(self.pending) + self.owner._delivery_lane.outstanding,
        )

    def counters(self) -> dict[str, int]:
        """Deque occupancy and the scheduling decisions taken so far.

        Maintainer diagnostics for tests and benchmarks; not part of the
        application snapshot, which describes queues the application can size.
        """
        lane = self.owner._delivery_lane
        pending = len(self.pending) + lane.outstanding
        return {
            "pending": pending,
            "pending_high_water": max(self.pending_high_water, pending),
            "enqueued": self.enqueued + lane.enqueued,
            "applied": self.applied + lane.applied,
            "waiters": self.waiters,
            "batches": self.batches,
            "multi_effect_batches": self.multi_effect_batches,
            "reordered_batches": self.reordered_batches,
            "inline_effects": self.inline_effects,
            "apply_suspensions": self.apply_suspensions,
        }

    def _complete(self) -> None:
        self.applied += 1
        if self.waiters:
            self.progress.set()

    def drain_inline(self, *, target: int | None = None) -> None:
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
            while self.pending and (target is None or self.applied < target):
                effect = self.pending[0]
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

    async def drain(self, *, nowait: bool = False, target: int | None = None) -> None:
        self.drain_inline(target=target)
        if nowait:
            if self.pending:
                self.schedule()
            return
        if target is None:
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
                await self.progress.wait()
                if waiter_id in self._error_waiters:
                    # Preserve cancellation priority during the failing-close
                    # handoff without spawning a shield-owned waiter task.
                    await asyncio.sleep(0)
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
