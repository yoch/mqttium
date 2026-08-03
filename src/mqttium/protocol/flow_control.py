"""Outbound QoS 1/2 inflight window (Receive Maximum).

Independent from PacketIdPool: a broker may advertise Receive Maximum=10 while
packet identifiers still span 1..65535.
"""

from __future__ import annotations


class FlowControl:
    __slots__ = ("_limit", "_inflight")

    def __init__(self, limit: int = 65535) -> None:
        self._limit = max(1, limit)
        self._inflight = 0

    @property
    def limit(self) -> int:
        return self._limit

    @limit.setter
    def limit(self, value: int) -> None:
        self._limit = max(1, value)

    @property
    def inflight(self) -> int:
        return self._inflight

    @property
    def available(self) -> int:
        return max(0, self._limit - self._inflight)

    def try_acquire(self) -> bool:
        if self._inflight >= self._limit:
            return False
        self._inflight += 1
        return True

    def release(self) -> None:
        if self._inflight > 0:
            self._inflight -= 1

    def reset(self) -> None:
        self._inflight = 0

    def apply_broker_receive_maximum(
        self, receive_maximum: int, local_max: int, local_outbound: int | None = None
    ) -> None:
        # Outbound window is bounded by the *broker's* Receive Maximum
        # ([MQTT-4.9.0-1]); an optional local cap may throttle further.
        limit = max(1, receive_maximum)
        if local_outbound is not None:
            limit = max(1, min(limit, local_outbound))
        self._limit = limit
