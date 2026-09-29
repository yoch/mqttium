"""Reader-owned, bounded delivery work separated from protocol progress."""

from __future__ import annotations

from collections import deque
from collections.abc import Awaitable
from typing import TYPE_CHECKING, Protocol

import asyncio

from mqttium.protocol.effects import EffectKind, EngineEffect

if TYPE_CHECKING:
    from mqttium.api._delivery import ApplicationDelivery
    from mqttium.api._effects import EffectPump


def survives_connection(effect: EngineEffect) -> bool:
    """Whether a delivery must still reach the application after its connection.

    A message that needs no delivery mark (QoS 0, automatically acknowledged
    QoS 1) has no remaining protocol owner: the broker will not resend it.
    Marked deliveries and replay continuation stay connection scoped; session
    state owns them.
    """
    kind = effect.kind
    return (
        kind is EffectKind.MESSAGE or kind is EffectKind.DECODED_MESSAGE
    ) and not effect.requires_delivery_mark


class DeliveryOwner(Protocol):
    _connection_epoch: int
    _effect_pump: EffectPump
    _delivery: ApplicationDelivery

    def _apply_delivery_effect(
        self, effect: EngineEffect, epoch: int
    ) -> Awaitable[object] | None: ...

    def _flush_released_completions(self) -> None: ...


class DeliveryLane:
    """Hold the current ingress/replay lot; only its reader drains this lane.

    A collection retains its own protocol fence. Later publications do not
    extend that fence. The reader finishes delivery before decoding another
    lot, and replay continuation lives behind the messages it depends on.

    Retiring a connection never drops an already-acknowledged message: it
    moves to ``carryover`` in arrival order, and the client hands it over once
    the old reader has stopped.
    """

    def __init__(self, owner: DeliveryOwner) -> None:
        self.owner = owner
        self.pending: deque[tuple[int, int, deque[EngineEffect]]] = deque()
        self.carryover: deque[EngineEffect] = deque()
        self.pending_count = 0
        self.active_count = 0
        self.enqueued = 0
        self.applied = 0
        self.high_water = 0

    def collect(self, effects: list[EngineEffect], epoch: int, protocol_target: int) -> None:
        self.pending.append((epoch, protocol_target, deque(effects)))
        self.pending_count += len(effects)
        self.enqueued += len(effects)
        self.high_water = max(self.high_water, self.pending_count)

    async def drain(self) -> None:
        owner = self.owner
        while self.pending:
            epoch, target, effects = self.pending.popleft()
            # Remove ownership from the lane before suspending. Its reader now
            # owns this lot; cancellation unwinds any active byte reservation.
            self.pending_count -= len(effects)
            self.active_count = len(effects)
            try:
                if epoch == owner._connection_epoch:
                    await owner._effect_pump.drain(target=target)
                while effects:
                    effect = effects.popleft()
                    if epoch != owner._connection_epoch and not survives_connection(effect):
                        self.active_count -= 1
                        self.applied += 1
                        continue
                    # Immediate handoff is the common case and creates no
                    # coroutine; only waiting work returns something to await.
                    pending = owner._apply_delivery_effect(effect, epoch)
                    if pending is not None:
                        try:
                            await pending
                        except asyncio.CancelledError:
                            # An iterator admission cancelled before commit
                            # did not deliver; a callback yield already did.
                            if owner._delivery.mode == "iterator" and survives_connection(effect):
                                effects.appendleft(effect)
                            raise
                    self.active_count -= 1
                    self.applied += 1
                # Completions released by this bounded lot leave together. A
                # lot waiting for application capacity holds them back, which
                # carries that backpressure to the broker's send quota.
                if epoch == owner._connection_epoch:
                    owner._flush_released_completions()
            finally:
                kept = 0
                if effects:
                    # A lot interrupted by cancellation is the oldest
                    # outstanding work: its acknowledged remainder goes ahead
                    # of any carryover. A fully delivered lot, the common
                    # case, skips this (#597 cost a list and a deque call per
                    # lot on every single-message delivery).
                    survivors = [effect for effect in effects if survives_connection(effect)]
                    self.carryover.extendleft(reversed(survivors))
                    kept = len(survivors)
                    self.pending_count += kept
                self.applied += self.active_count - kept
                self.active_count = 0

    @property
    def outstanding(self) -> int:
        return self.pending_count + self.active_count

    def discard(self) -> None:
        """Retire queued lots, keeping their already-acknowledged messages."""
        kept = 0
        for _epoch, _target, effects in self.pending:
            for effect in effects:
                if survives_connection(effect):
                    self.carryover.append(effect)
                    kept += 1
        queued = sum(len(effects) for _epoch, _target, effects in self.pending)
        self.applied += queued - kept
        self.pending_count -= queued - kept
        self.pending.clear()

    def take_carryover(self) -> list[EngineEffect]:
        """Remove and return the retired connections' acknowledged messages."""
        carried = list(self.carryover)
        self.carryover.clear()
        self.pending_count -= len(carried)
        self.applied += len(carried)
        return carried
