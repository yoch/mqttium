"""Outbound QoS 1/2 inflight window (Receive Maximum).

Independent from PacketIdPool: a broker may advertise Receive Maximum=10 while
packet identifiers still span 1..65535.

Each slot is owned by the packet identifier of the exchange whose PUBLISH it
admitted on this connection. Releasing by owner makes a release by an exchange
that holds no slot (a replayed PUBREL, a parked or sealed exchange) a no-op
instead of freeing a slot another exchange owns (#545).
"""

from __future__ import annotations


class FlowControl:
    __slots__ = ("_limit", "_holders")

    def __init__(self, limit: int = 65535) -> None:
        self._limit = max(1, limit)
        self._holders: set[int] = set()

    @property
    def limit(self) -> int:
        return self._limit

    @limit.setter
    def limit(self, value: int) -> None:
        self._limit = max(1, value)

    @property
    def inflight(self) -> int:
        return len(self._holders)

    @property
    def available(self) -> int:
        return max(0, self._limit - len(self._holders))

    def holds(self, mid: int) -> bool:
        return mid in self._holders

    def try_acquire(self, mid: int) -> bool:
        holders = self._holders
        count = len(holders)
        if count >= self._limit:
            return False
        holders.add(mid)
        if len(holders) == count:
            raise AssertionError(f"send quota slot acquired twice by mid={mid}")
        return True

    def release(self, mid: int) -> bool:
        """Free the slot `mid` owns; whether it owned one."""
        holders = self._holders
        if mid in holders:
            holders.remove(mid)
            return True
        return False

    def reset(self) -> None:
        self._holders.clear()

    def apply_broker_receive_maximum(
        self, receive_maximum: int, local_outbound: int | None = None
    ) -> None:
        # Outbound window is bounded by the *broker's* Receive Maximum
        # ([MQTT-4.9.0-1]); an optional local cap may throttle further.
        limit = max(1, receive_maximum)
        if local_outbound is not None:
            limit = max(1, min(limit, local_outbound))
        self._limit = limit
