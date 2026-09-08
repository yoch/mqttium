"""Bounded incremental MQTT frame decoder.

The decoder owns reusable contiguous storage.  Pull transports copy received
bytes through :meth:`feed`; selector TCP may instead receive directly into a
window returned by :meth:`writable_window` and publish the written extent with
:meth:`commit`.

Storage invariants:
- ``_start:_end`` is the live interval; ``len(_buf)`` is capacity.
- mutable storage never escapes the decoder as application data.
- reallocation always allocates a new bytearray and swaps it in; an outstanding
  memoryview of an older slab can therefore never make growth raise BufferError.
- large, incomplete frames may reserve their known final extent once their
  Remaining Length is available.  They do not reserve an additional receive
  quantum merely to make the *next* get_buffer() possible.
"""

from __future__ import annotations

from dataclasses import dataclass

from mqttium.codec.vbi import decode_vbi
from mqttium.enums import PacketType
from mqttium.errors import MalformedPacketError, PacketTooLargeError

DEFAULT_MAX_PACKET_SIZE = 16 * 1024 * 1024

# Pull-only clients should not pay the selector hot-path reserve.  A direct
# selector ingress asks for 64 KiB initially and can promote independently.
_MIN_CAPACITY = 4 * 1024
_RECONNECT_CAPACITY = 64 * 1024
# Once a connection has demonstrated sustained/bulk traffic, keeping 256 KiB is
# intentional: it avoids grow/shrink churn while still being half the old #447
# unconditional 512-KiB floor.
_STEADY_CAPACITY = 256 * 1024
# A known incomplete frame at/above this size is allocated to its exact final
# frame extent.  Direct ingress pauses once such a frame becomes actionable, so
# no speculative tail is required after the final byte.
_EXACT_FRAME_THRESHOLD = 512 * 1024
# Give an oversized slab back only after a real run of small complete drains.
# Repeated large frames reset the counter and therefore do not thrash.
_OVERSIZE_RETENTION_DRAINS = 32
_VIEW_COPY_THRESHOLD = 4096


@dataclass(slots=True, frozen=True)
class RawPacket:
    """One decoded MQTT frame (owned bytes, safe to retain)."""

    packet_type: PacketType
    flags: int
    remaining: bytes


class IncrementalDecoder:
    __slots__ = (
        "_buf",
        "_start",
        "_end",
        "_max_packet_size",
        "_high_water",
        "_growth_count",
        "_shrink_count",
        "_compaction_count",
        "_capacity_peak",
        "_small_drain_streak",
        "_storage_generation",
    )

    def __init__(self, max_packet_size: int = DEFAULT_MAX_PACKET_SIZE) -> None:
        if max_packet_size < 1:
            raise ValueError("max_packet_size too small")
        self._buf = bytearray()
        self._start = 0
        self._end = 0
        self._max_packet_size = max_packet_size
        self._high_water = 0
        self._growth_count = 0
        self._shrink_count = 0
        self._compaction_count = 0
        self._capacity_peak = 0
        self._small_drain_streak = 0
        self._storage_generation = 0

    @property
    def buffered(self) -> int:
        return self._end - self._start

    @property
    def capacity(self) -> int:
        return len(self._buf)

    @property
    def capacity_peak(self) -> int:
        return self._capacity_peak

    @property
    def growth_count(self) -> int:
        return self._growth_count

    @property
    def shrink_count(self) -> int:
        return self._shrink_count

    @property
    def compaction_count(self) -> int:
        return self._compaction_count

    @property
    def storage_generation(self) -> int:
        return self._storage_generation

    @property
    def next_header_byte(self) -> int | None:
        return self._buf[self._start] if self._start < self._end else None

    @property
    def high_water(self) -> int:
        return self._high_water

    @property
    def max_packet_size(self) -> int:
        return self._max_packet_size

    @max_packet_size.setter
    def max_packet_size(self, value: int) -> None:
        if value < 1:
            raise ValueError("max_packet_size too small")
        self._max_packet_size = value

    def _reallocate(self, capacity: int) -> None:
        live = self._end - self._start
        if capacity < live:
            raise AssertionError("decoder reallocation below live extent")
        old = self._buf
        old_capacity = len(old)
        replacement = bytearray(capacity)
        if live:
            # Copy without resizing either object.  A memoryview retained by a
            # diagnostic traceback still owns `old`; swapping `self._buf` is
            # safe and the stale view can no longer block decoder growth.
            view = memoryview(old)
            try:
                replacement[:live] = view[self._start : self._end]
            finally:
                view.release()
        self._buf = replacement
        self._start = 0
        self._end = live
        self._storage_generation += 1
        if capacity > old_capacity:
            self._growth_count += 1
        elif capacity < old_capacity:
            self._shrink_count += 1
        if capacity > self._capacity_peak:
            self._capacity_peak = capacity

    def _compact(self) -> None:
        if self._start == 0:
            return
        live = self._end - self._start
        if not live:
            self._start = 0
            self._end = 0
            return
        view = memoryview(self._buf)
        try:
            # Equal-length assignment is an in-place move, never a resize.
            self._buf[:live] = view[self._start : self._end]
        finally:
            view.release()
        self._start = 0
        self._end = live
        self._compaction_count += 1

    def _known_incomplete_frame_total(self) -> int | None:
        """Return the head frame's final byte extent when safely knowable.

        This helper is allocation policy only.  Malformed/non-canonical/oversize
        input is deliberately left for ``peek_packet_bounds``/``next_packet`` so
        protocol errors keep their normal reader-task ordering.
        """
        available = self._end - self._start
        if available < 2:
            return None
        try:
            remaining, rl_end = decode_vbi(self._buf, self._start + 1, end=self._end)
        except MalformedPacketError:
            return None
        total = (rl_end - self._start) + remaining
        if total > self._max_packet_size or available >= total:
            return None
        return total

    def _ensure(self, need: int, *, exact_frame_total: int | None = None) -> None:
        if need <= 0:
            raise ValueError("decoder writable window must be positive")
        if len(self._buf) - self._end >= need:
            return
        if self._start:
            self._compact()
            if len(self._buf) - self._end >= need:
                return

        live = self._end
        required = live + need
        if exact_frame_total is not None:
            required = max(required, exact_frame_total)

        capacity = len(self._buf)
        if capacity == 0:
            capacity = max(_MIN_CAPACITY, need)
        while capacity < required:
            capacity *= 2

        if exact_frame_total is not None and exact_frame_total >= _EXACT_FRAME_THRESHOLD:
            # The transport will pause when this now-complete large head becomes
            # actionable.  Reserving another receive quantum is therefore both
            # unnecessary and exactly what caused #447's 8 MiB -> 16 MiB case.
            capacity = max(required, exact_frame_total)
        else:
            # For ordinary growth, geometric expansion is retained for allocator
            # stability but cannot run away to ~2x a legal maximum frame merely
            # because a future receive window was requested.
            ceiling = self._max_packet_size + need
            if required <= ceiling and capacity > ceiling:
                capacity = ceiling
            capacity = max(capacity, required)

        self._reallocate(capacity)

    def writable_window(self, preferred: int = _RECONNECT_CAPACITY) -> memoryview:
        """Expose decoder-owned receive storage for one ``recv_into`` callback.

        A large fragmented head whose Remaining Length is already known reserves
        its final frame extent once, but the returned receive window remains
        bounded by ``preferred``.  The caller must not assume a stable slab
        identity across calls.
        """
        if preferred <= 0:
            raise ValueError("preferred receive window must be positive")
        total = self._known_incomplete_frame_total()
        live = self._end - self._start
        if total is not None and total >= _EXACT_FRAME_THRESHOLD:
            remaining = total - live
            offered = min(preferred, remaining)
            self._ensure(offered, exact_frame_total=total)
        else:
            offered = preferred
            self._ensure(offered)
        return memoryview(self._buf)[self._end : self._end + offered]

    def commit(self, nbytes: int) -> None:
        if nbytes <= 0:
            if nbytes < 0:
                raise ValueError("negative decoder commit")
            return
        end = self._end + nbytes
        if end > len(self._buf):
            raise ValueError("decoder commit exceeds writable window")
        self._end = end
        buffered = end - self._start
        if buffered > self._high_water:
            self._high_water = buffered

    def _retire_after_drain(self, extent: int) -> None:
        capacity = len(self._buf)
        if capacity <= _STEADY_CAPACITY:
            self._small_drain_streak = 0
            return
        if extent > _STEADY_CAPACITY:
            # A stream of genuinely large frames benefits from retaining the
            # slab; do not shrink and regrow on every packet.
            self._small_drain_streak = 0
            return
        self._small_drain_streak += 1
        if self._small_drain_streak >= _OVERSIZE_RETENTION_DRAINS:
            self._small_drain_streak = 0
            self._reallocate(_STEADY_CAPACITY)

    def _fully_consumed(self, extent: int) -> None:
        self._start = 0
        self._end = 0
        self._retire_after_drain(extent)

    def feed(self, data: bytes | bytearray | memoryview) -> None:
        """Copy bytes from a pull transport into the same decoder slab."""
        size = len(data)
        if not size:
            return
        self._ensure(size)
        end = self._end
        self._buf[end : end + size] = data
        self._end = end + size
        buffered = self._end - self._start
        if buffered > self._high_water:
            self._high_water = buffered

    def clear(self) -> None:
        """Reset connection state and release connection-specific oversize growth."""
        self._start = 0
        self._end = 0
        self._small_drain_streak = 0
        capacity = len(self._buf)
        if capacity > _RECONNECT_CAPACITY:
            self._reallocate(_RECONNECT_CAPACITY)

    def peek_packet_bounds(self) -> tuple[int, int, int] | None:
        buf = self._buf
        start = self._start
        available = self._end - start
        if available < 2:
            return None

        header = buf[start]
        first = buf[start + 1]
        if first < 0x80:
            remaining_length = first
            rl_end = start + 2
        elif available >= 3 and buf[start + 2] < 0x80:
            remaining_length = (first & 0x7F) | (buf[start + 2] << 7)
            if remaining_length < 128:
                raise MalformedPacketError("Non-canonical Variable Byte Integer")
            rl_end = start + 3
        else:
            try:
                remaining_length, rl_end = decode_vbi(buf, start + 1, end=self._end)
            except MalformedPacketError:
                if available >= 5:
                    raise
                if all(buf[start + i] & 0x80 for i in range(1, available)):
                    return None
                raise

        fixed_header_len = rl_end - start
        total = fixed_header_len + remaining_length
        if total > self._max_packet_size:
            raise PacketTooLargeError(
                f"Packet size {total} exceeds maximum {self._max_packet_size}"
            )
        if available < total:
            return None
        return header, start + fixed_header_len, start + total

    def head_frame_ready(self) -> bool:
        """Whether decoding/error handling can make progress without more input."""
        try:
            return self.peek_packet_bounds() is not None
        except (MalformedPacketError, PacketTooLargeError):
            return True

    def consume_peeked_packet(self, body_end: int) -> None:
        if not (self._start < body_end <= self._end):
            raise AssertionError("invalid decoder packet boundary")
        self._start = body_end
        if body_end == self._end:
            self._fully_consumed(body_end)

    def next_packet(self) -> RawPacket | None:
        bounds = self.peek_packet_bounds()
        if bounds is None:
            return None
        header, body_start, body_end = bounds
        remaining_length = body_end - body_start
        if remaining_length >= _VIEW_COPY_THRESHOLD:
            view = memoryview(self._buf)
            try:
                body = bytes(view[body_start:body_end])
            finally:
                view.release()
        else:
            body = bytes(self._buf[body_start:body_end])
        self._start = body_end
        if body_end == self._end:
            self._fully_consumed(body_end)
        return RawPacket(
            packet_type=PacketType.from_byte(header),
            flags=header & 0x0F,
            remaining=body,
        )

    def drain_packets(self, limit: int = 100) -> list[RawPacket]:
        packets: list[RawPacket] = []
        for _ in range(limit):
            packet = self.next_packet()
            if packet is None:
                break
            packets.append(packet)
        return packets
