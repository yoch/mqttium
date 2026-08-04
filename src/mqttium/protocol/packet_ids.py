"""Outbound MQTT packet identifier allocator.

Receive Maximum MUST NOT shrink this space — it is enforced by FlowControl.
Inbound QoS 2 identifiers live in a separate namespace and must never be freed here.
"""

from __future__ import annotations

from mqttium.errors import FlowControlError


class PacketIdPool:
    """Allocate MQTT packet identifiers without retaining every live MID.

    Identifiers below ``_next`` have crossed the allocation frontier and are in
    use unless recorded in ``_free_one`` or ``_free_many``. Identifiers at or above
    the frontier have not been issued unless they appear in ``_reserved``.

    Normal sequential allocation therefore needs no per-identifier container.
    The common release-then-allocate path uses one scalar free slot and only
    creates sets when multiple holes or out-of-order reservations coexist.
    """

    __slots__ = ("_free_count", "_free_many", "_free_one", "_next", "_reserved")

    _MAX_ID = 65_535

    def __init__(self) -> None:
        self._free_count = 0
        self._free_one = 0
        self._free_many: set[int] | None = None
        self._reserved: set[int] | None = None
        self._next = 1

    def __len__(self) -> int:
        reserved_count = 0 if self._reserved is None else len(self._reserved)
        return self._next - 1 - self._free_count + reserved_count

    @property
    def available(self) -> int:
        return self._MAX_ID - len(self)

    @property
    def _used(self) -> set[int]:
        """Materialize live MIDs for invariant/debug tooling.

        Production paths never retain this compatibility view. Existing fuzz
        checks historically inspected the old private set directly.
        """
        used = set(range(1, self._next))
        if self._free_one:
            used.discard(self._free_one)
        if self._free_many is not None:
            used.difference_update(self._free_many)
        if self._reserved is not None:
            used.update(self._reserved)
        return used

    @property
    def _free(self) -> list[int]:
        """Materialize released MIDs for legacy memory-cleanup tests."""
        free = [] if not self._free_one else [self._free_one]
        if self._free_many is not None:
            free.extend(self._free_many)
        return free

    def allocate(self) -> int:
        mid = self._free_one
        if mid:
            self._free_one = 0
            self._free_count -= 1
            return mid

        free = self._free_many
        if free:
            mid = free.pop()
            if not free:
                self._free_many = None
            self._free_count -= 1
            return mid

        mid = self._next
        reserved = self._reserved
        if reserved is not None:
            while mid in reserved:
                reserved.remove(mid)
                mid += 1
            if not reserved:
                self._reserved = None
            self._next = mid

        if mid > self._MAX_ID:
            raise FlowControlError("No free MQTT packet identifiers")
        self._next = mid + 1
        return mid

    def reserve(self, mid: int) -> None:
        """Mark an existing MID as in-use (e.g. hydrated from persistence)."""
        if mid < 1 or mid > self._MAX_ID:
            raise ValueError(f"Invalid packet id {mid}")

        frontier = self._next
        if mid < frontier:
            if self._free_one == mid:
                self._free_one = 0
                self._free_count -= 1
                return
            free = self._free_many
            if free is not None and mid in free:
                free.remove(mid)
                self._free_count -= 1
                if not free:
                    self._free_many = None
            return

        if mid == frontier:
            frontier += 1
            reserved = self._reserved
            if reserved is not None:
                while frontier in reserved:
                    reserved.remove(frontier)
                    frontier += 1
                if not reserved:
                    self._reserved = None
            self._next = frontier
            return

        reserved = self._reserved
        if reserved is None:
            self._reserved = {mid}
        else:
            reserved.add(mid)

    def release(self, mid: int) -> None:
        if mid < 1 or mid > self._MAX_ID:
            return

        frontier = self._next
        if mid < frontier:
            if self._free_one == mid:
                return
            free = self._free_many
            if free is not None and mid in free:
                return
            if not self._free_one:
                self._free_one = mid
            elif free is None:
                self._free_many = {mid}
            else:
                free.add(mid)
            free_count = self._free_count + 1
            self._free_count = free_count
            if free_count == frontier - 1 and self._reserved is None:
                self.clear()
            return

        reserved = self._reserved
        if reserved is None or mid not in reserved:
            return
        reserved.remove(mid)
        if reserved:
            return
        self._reserved = None
        if self._free_count == frontier - 1:
            self.clear()

    def in_use(self, mid: int) -> bool:
        if mid < 1 or mid > self._MAX_ID:
            return False
        if mid < self._next:
            if self._free_one == mid:
                return False
            free = self._free_many
            return free is None or mid not in free
        reserved = self._reserved
        return reserved is not None and mid in reserved

    def clear(self) -> None:
        self._free_count = 0
        self._free_one = 0
        self._free_many = None
        self._reserved = None
        self._next = 1
