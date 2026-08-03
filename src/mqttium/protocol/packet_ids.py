"""Outbound MQTT packet identifier allocator.

Receive Maximum MUST NOT shrink this space — it is enforced by FlowControl.
Inbound QoS 2 identifiers live in a separate namespace and must never be freed here.
"""

from __future__ import annotations

from contextlib import suppress

from mqttium.errors import FlowControlError


class PacketIdPool:
    __slots__ = ("_used", "_free", "_next", "_size")

    def __init__(self) -> None:
        # MQTT packet identifiers are 1..65535 inclusive.
        self._used: set[int] = set()
        self._free: list[int] = []
        self._next = 1  # next never-issued id
        self._size = 65535

    def __len__(self) -> int:
        return len(self._used)

    @property
    def available(self) -> int:
        return self._size - len(self._used)

    def allocate(self) -> int:
        if len(self._used) >= self._size:
            raise FlowControlError("No free MQTT packet identifiers")
        if self._free:
            mid = self._free.pop()
            self._used.add(mid)
            return mid
        # Skip ids reserved via hydrate (store) that precede _next.
        while self._next <= self._size and self._next in self._used:
            self._next += 1
        if self._next <= self._size:
            mid = self._next
            self._next += 1
            self._used.add(mid)
            return mid
        # All ids have been issued at least once; scan for a hole.
        for candidate in range(1, self._size + 1):
            if candidate not in self._used:
                self._used.add(candidate)
                return candidate
        raise FlowControlError("No free MQTT packet identifiers")

    def reserve(self, mid: int) -> None:
        """Mark an existing MID as in-use (e.g. hydrated from persistence)."""
        if mid < 1 or mid > self._size:
            raise ValueError(f"Invalid packet id {mid}")
        self._used.add(mid)
        # Keep free-list coherent if this id was previously released.
        with suppress(ValueError):
            self._free.remove(mid)

    def release(self, mid: int) -> None:
        if mid in self._used:
            self._used.discard(mid)
            self._free.append(mid)

    def in_use(self, mid: int) -> bool:
        return mid in self._used

    def clear(self) -> None:
        self._used.clear()
        self._free.clear()
        self._next = 1
