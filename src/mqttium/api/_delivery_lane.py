"""Reader-owned, bounded delivery work separated from protocol progress."""

from __future__ import annotations

from collections import deque
from typing import TYPE_CHECKING, Protocol

from mqttium.protocol.effects import EngineEffect

if TYPE_CHECKING:
    from mqttium.api._effects import EffectPump


class DeliveryOwner(Protocol):
    _connection_epoch: int
    _effect_pump: EffectPump

    async def _apply_delivery_effect(self, effect: EngineEffect, epoch: int) -> None: ...


class DeliveryLane:
    """Hold the current ingress/replay lot; only its reader drains this lane.

    A collection retains its own protocol fence. Later publications do not
    extend that fence. The reader finishes delivery before decoding another
    lot, and replay continuation lives behind the messages it depends on.
    """

    def __init__(self, owner: DeliveryOwner) -> None:
        self.owner = owner
        self.pending: deque[tuple[int, int, deque[EngineEffect]]] = deque()
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
        while self.pending:
            epoch, target, effects = self.pending.popleft()
            # Remove ownership from the lane before suspending. Its reader now
            # owns this lot; cancellation unwinds any active byte reservation.
            self.pending_count -= len(effects)
            self.active_count = len(effects)
            try:
                if epoch != self.owner._connection_epoch:
                    continue
                await self.owner._effect_pump.drain(target=target)
                while effects:
                    if epoch != self.owner._connection_epoch:
                        break
                    effect = effects.popleft()
                    await self.owner._apply_delivery_effect(effect, epoch)
                    self.active_count -= 1
                    self.applied += 1
            finally:
                self.applied += self.active_count
                self.active_count = 0

    @property
    def outstanding(self) -> int:
        return self.pending_count + self.active_count

    def discard(self) -> None:
        self.applied += self.pending_count
        self.pending_count = 0
        self.pending.clear()
