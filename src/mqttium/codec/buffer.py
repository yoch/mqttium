"""Bounded incremental MQTT frame decoder.

Design constraints (from Paho perf audit + gmqtt critique):
- Fixed-capacity reusable storage with read/write offsets and in-place compaction
- Contiguous decode via indices / unpack_from when a full packet is present
- Never expose a memoryview into the reusable storage to callers
- Enforce a maximum packet size early (after Remaining Length)

The storage is a slab the decoder owns outright: `_start` is the read offset,
`_end` the write offset, and `len(_buf)` is the capacity rather than the amount
of live data. Nothing on the hot path reallocates -- compaction is a memmove
inside the existing slab, and the slab only grows for a frame larger than its
capacity. That is what lets a transport hand `writable_window()` straight to
`socket.recv_into()`, so received bytes are never copied through an
intermediate buffer before reaching the decoder.
"""

from __future__ import annotations

from dataclasses import dataclass

from mqttium.codec.vbi import decode_vbi
from mqttium.enums import PacketType
from mqttium.errors import MalformedPacketError, PacketTooLargeError

# Default local ceiling before CONNACK negotiation (256 MiB is the MQTT max).
DEFAULT_MAX_PACKET_SIZE = 16 * 1024 * 1024
# Steady-state slab size. Large enough that a saturated receive path batches
# several frames per wakeup. The slab grows past it only for a frame larger than
# it, and is retired again only after _OVERSIZE_RETENTION drains that stayed
# inside it -- not as soon as that frame is consumed. See _retire_oversize.
DEFAULT_CAPACITY = 256 * 1024
# Smallest window worth offering a receiver; also the compaction trigger.
_MIN_WINDOW = 16 * 1024
# Most one recv_into() may be offered, whatever the slab's size. Storage
# capacity and receive quantum are separate concerns.
RECEIVE_QUANTUM = DEFAULT_CAPACITY
# Fully drained slabs larger than DEFAULT_CAPACITY are retired only after this
# many consecutive drains that did not need the extra room. Giving the memory
# back immediately costs two reallocations per frame for a stream of frames just
# over capacity, which is the allocator churn this design exists to remove.
_OVERSIZE_RETENTION = 64
# Body size from which the body is copied through a memoryview instead of
# `bytes(bytearray[a:b])` — see next_packet. Paired end-to-end decode, alternated
# in-process over 11 repeats: 1 KiB 0.996, 4 KiB 0.989, 8 KiB 1.027, 16 KiB
# 1.021. So 4 KiB gives up ~1% throughput to stop allocating a second full copy
# of every payload, and everything larger gains on both counts.
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
        "_view",
        "_start",
        "_end",
        "_capacity",
        "_idle_drains",
        "_max_packet_size",
        "_high_water",
    )

    def __init__(self, max_packet_size: int = DEFAULT_MAX_PACKET_SIZE) -> None:
        if max_packet_size < 1:
            raise ValueError("max_packet_size too small")
        # Allocated lazily so an idle or never-connected client costs nothing.
        self._buf = bytearray()
        self._view = memoryview(self._buf)
        self._start = 0
        self._end = 0
        self._capacity = 0
        self._idle_drains = 0
        self._max_packet_size = max_packet_size
        self._high_water = 0

    @property
    def buffered(self) -> int:
        return self._end - self._start

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def next_header_byte(self) -> int | None:
        """Return the next fixed-header byte without consuming buffered data."""
        return self._buf[self._start] if self._start < self._end else None

    def _reallocate(self, capacity: int) -> None:
        live = self._end - self._start
        grown = bytearray(capacity)
        if live:
            grown[0:live] = self._view[self._start : self._end]
        # Release only after the copy: the old view is the source.
        self._view.release()
        self._buf = grown
        self._view = memoryview(grown)
        self._capacity = capacity
        self._start = 0
        self._end = live

    def _ensure(self, need: int) -> None:
        """Guarantee `need` writable bytes after `_end`, without reallocating if possible."""
        if self._capacity - self._end >= need:
            return
        live = self._end - self._start
        if self._capacity - live >= need:
            # In-place memmove inside the existing slab; capacity is untouched,
            # so neither this nor the following write reallocates.
            self._buf[0:live] = self._view[self._start : self._end]
            self._start = 0
            self._end = live
            return
        capacity = self._capacity or DEFAULT_CAPACITY
        while capacity - live < need:
            capacity *= 2
        # Doubling alone can reach twice max_packet_size: a frame just over a
        # doubling step leaves a tail too small for the next window and doubles
        # again. Framing rejects anything larger than max_packet_size, so one
        # maximum frame plus a window is all the slab can ever use. `need` still
        # wins, because feed() may be handed more than that in one call.
        ceiling = self._max_packet_size + _MIN_WINDOW
        if capacity > ceiling:
            capacity = max(ceiling, live + need)
        self._reallocate(capacity)

    def _retire_oversize(self, extent: int) -> None:
        """Give back an oversized slab, but only once it has stopped being used.

        ``extent`` is how far into the slab this drain actually reached. Using
        the reallocation that created the slab instead would be wrong: after the
        first oversized frame the slab is already large enough, so every later
        oversized frame fits without reallocating and would look idle. That
        retires the slab on a fixed period and re-grows on the very next frame.
        """
        if extent > DEFAULT_CAPACITY:
            self._idle_drains = 0
            return
        self._idle_drains += 1
        if self._idle_drains >= _OVERSIZE_RETENTION:
            self._idle_drains = 0
            self._reallocate(DEFAULT_CAPACITY)

    def _reset(self) -> None:
        """Rewind a fully consumed slab."""
        extent = self._end
        self._start = 0
        self._end = 0
        if self._capacity > DEFAULT_CAPACITY:
            self._retire_oversize(extent)

    def writable_window(self, need: int = _MIN_WINDOW) -> memoryview:
        """Return writable storage for a receiver to fill, then `commit()`.

        The window is a view into the decoder's own slab, so bytes written into
        it are already where the parser expects them. It is never empty, which
        `asyncio.BufferedProtocol.get_buffer()` requires. Callers must drop the
        view before the next call, since an outstanding export would block the
        slab from being replaced on the grow path.

        Capped at `RECEIVE_QUANTUM`: a slab left large by an earlier oversized
        frame would otherwise overshoot the receiver's high water in proportion
        to retained capacity, since that check only runs afterwards.
        """
        self._ensure(need)
        end = self._end
        limit = end + (need if need > RECEIVE_QUANTUM else RECEIVE_QUANTUM)
        if limit > self._capacity:
            limit = self._capacity
        return self._view[end:limit]

    def commit(self, nbytes: int) -> None:
        """Publish `nbytes` written into the window returned by `writable_window()`."""
        if nbytes <= 0:
            return
        end = self._end + nbytes
        if end > self._capacity:
            raise ValueError("commit exceeds the window that was handed out")
        self._end = end
        buffered = end - self._start
        if buffered > self._high_water:
            self._high_water = buffered

    def peek_packet_bounds(self) -> tuple[int, int, int] | None:
        """Return ``(header, body_start, body_end)`` for the next complete frame.

        The frame is not consumed and the reusable storage is not exposed. This
        internal hot-path primitive deliberately mirrors ``next_packet`` framing
        so the generic decoder does not pay an extra Python call per packet.
        """
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
                remaining_length, rl_end = decode_vbi(buf, start + 1)
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
        body_start = start + fixed_header_len
        return header, body_start, start + total

    def head_frame_ready(self) -> bool:
        """Whether the reader can make progress without receiving more bytes.

        A malformed or oversize header counts as ready: the reader must run so
        it can surface the protocol error, rather than leaving the connection
        paused forever waiting for bytes it will never accept.
        """
        try:
            return self.peek_packet_bounds() is not None
        except (MalformedPacketError, PacketTooLargeError):
            return True

    def consume_peeked_packet(self, body_end: int) -> None:
        """Commit a frame previously returned by :meth:`peek_packet_bounds`."""
        assert self._start < body_end <= self._end
        self._start = body_end
        if self._start == self._end:
            self._reset()

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

    def feed(self, data: bytes | bytearray | memoryview) -> None:
        """Copy `data` into the slab.

        Kept for transports that cannot write into `writable_window()` directly:
        TLS, WebSocket, and any non-selector event loop.
        """
        nbytes = len(data)
        if not nbytes:
            return
        end = self._end
        if self._capacity - end < nbytes:
            self._ensure(nbytes)
            end = self._end
        # Slice assignment inside the slab: a memcpy, never a resize.
        self._buf[end : end + nbytes] = data
        end += nbytes
        self._end = end
        buffered = end - self._start
        if buffered > self._high_water:
            self._high_water = buffered

    def clear(self) -> None:
        """Rewind for a new connection, dropping any oversized-frame growth."""
        self._start = 0
        self._end = 0
        self._idle_drains = 0
        if self._capacity > DEFAULT_CAPACITY:
            self._reallocate(DEFAULT_CAPACITY)

    def next_packet(self) -> RawPacket | None:
        buf = self._buf
        start = self._start
        available = self._end - start
        if available < 2:
            return None

        header = buf[start]
        try:
            remaining_length, rl_end = decode_vbi(buf, start + 1)
        except MalformedPacketError:
            # Incomplete VBI — need more bytes unless clearly malformed length.
            if available >= 5:
                raise
            # Distinguish "need more" from "too long": if we have continuation
            # bits on all available bytes and < 5 total header bytes, wait.
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

        packet_type = PacketType.from_byte(header)
        flags = header & 0x0F
        body_start = start + fixed_header_len
        body_end = start + total
        # Copy the body out so callers never alias the reusable storage.
        #
        # `bytes(buf[a:b])` copies twice: slicing a bytearray builds another
        # bytearray, which `bytes()` then copies again. Going through the slab's
        # own memoryview copies once.
        #
        # Why not just `buf[a:b]`? It is indeed a single copy, and on its own the
        # fastest of the three. But it is a *mutable* bytearray, and that escapes
        # one level further: `PublishPacket.decode` slices the payload straight
        # out of it, so `Message.payload` would become a bytearray — unhashable,
        # and mutable by the application while the inflight store holds the same
        # object. Converting it back costs exactly the copy just saved; measured
        # end to end, paired and alternated, that variant is 1.04x to 1.17x
        # slower. A memoryview is the only single-copy route to immutable bytes.
        #
        # The slice is transient and only ever produces owned bytes, so no
        # memoryview of the reusable storage is ever handed out.
        #
        # Only worth it above `_VIEW_COPY_THRESHOLD`: the memoryview object costs
        # more than the second copy saves on small frames, and small frames are
        # the hot path.
        if remaining_length >= _VIEW_COPY_THRESHOLD:
            body = bytes(self._view[body_start:body_end])
        else:
            body = bytes(buf[body_start:body_end])
        self._start = body_end
        if body_end == self._end:
            self._start = 0
            self._end = 0
            if self._capacity > DEFAULT_CAPACITY:
                self._retire_oversize(body_end)
        return RawPacket(packet_type=packet_type, flags=flags, remaining=body)

    def drain_packets(self, limit: int = 100) -> list[RawPacket]:
        packets: list[RawPacket] = []
        for _ in range(limit):
            packet = self.next_packet()
            if packet is None:
                break
            packets.append(packet)
        return packets
