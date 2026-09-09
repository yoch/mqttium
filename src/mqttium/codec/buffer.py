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
# Steady-state receive size once a connection is demonstrably busy.
DEFAULT_CAPACITY = 256 * 1024
# Allocation floor. A decoder that only ever sees small frames -- an idle
# connection, a short-lived client, the TLS/WebSocket feed() path -- never grows
# past what it is actually handed.
_MIN_CAPACITY = 16 * 1024
# What the receive path asks for before anything is known about the peer.
_INITIAL_WINDOW = 64 * 1024
# Consecutive completely-filled windows before offering a larger one.
_PROMOTE_AFTER = 4
# A single frame above this is evidence the workload needs a large slab.
_LARGE_FRAME = DEFAULT_CAPACITY
# Above this, size the slab to the head frame's exact extent instead of doubling
# past it. Doubling wastes up to a whole frame on multi-MiB traffic.
_EXACT_FRAME_THRESHOLD = 512 * 1024
# Smallest window worth offering a receiver; also the compaction trigger.
_MIN_WINDOW = 16 * 1024
# Ceiling for the adaptive receive window. Storage capacity and receive quantum
# are separate concerns.
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
        "_window_peak",
        "_drain_had_large_frame",
        "_offered",
        "_target_window",
        "_full_fills",
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
        self._window_peak = 0
        self._drain_had_large_frame = False
        self._offered = 0
        self._target_window = _INITIAL_WINDOW
        self._full_fills = 0
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

    def _ensure(self, need: int, exact_total: int | None = None) -> None:
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
        capacity = self._capacity or _MIN_CAPACITY
        if exact_total is not None:
            # The head frame's extent is known, so size to it instead of
            # doubling: a frame just over a step would otherwise cost a whole
            # extra frame of slab.
            capacity = exact_total if exact_total > live + need else live + need
        elif need >= _EXACT_FRAME_THRESHOLD:
            # feed() was handed this much in one call, so it is the exact need.
            capacity = live + need
        else:
            while capacity - live < need:
                capacity *= 2
        # Doubling alone can reach twice max_packet_size: a frame just over a
        # doubling step leaves a tail too small for the next window and doubles
        # again. Framing rejects anything larger than max_packet_size, so one
        # maximum frame plus a window is all the slab can ever use. `need` still
        # wins, because feed() may be handed more than that in one call.
        ceiling = self._max_packet_size + RECEIVE_QUANTUM
        if capacity > ceiling:
            capacity = max(ceiling, live + need)
        self._reallocate(capacity)

    def _retire_oversize(self, extent: int) -> None:
        """Shrink an oversized slab once large frames stop arriving.

        Two different things must not be confused. A drain that consumed a
        genuinely large *frame* is evidence the workload still needs the room,
        and resets the window. Aggregate pressure from coalesced small frames is
        not: it routinely exceeds `DEFAULT_CAPACITY` without any single frame
        doing so, and counting it as use pins a multi-MiB slab for a workload
        that needs a few hundred KiB.
        """
        if self._drain_had_large_frame:
            self._drain_had_large_frame = False
            self._idle_drains = 0
            self._window_peak = 0
            return
        if extent > self._window_peak:
            self._window_peak = extent
        self._idle_drains += 1
        if self._idle_drains < _OVERSIZE_RETENTION:
            return
        floor = self._target_window
        target = _MIN_CAPACITY if _MIN_CAPACITY > floor else floor
        while target < self._window_peak:
            target *= 2
        self._idle_drains = 0
        self._window_peak = 0
        if target < self._capacity:
            self._reallocate(target)

    def _known_frame_total(self) -> int | None:
        """Head frame's byte extent when already knowable. Allocation policy only.

        Malformed and oversize input is left to the framing methods so protocol
        errors keep their normal ordering.
        """
        if self._end - self._start < 2:
            return None
        try:
            remaining, rl_end = decode_vbi(self._buf, self._start + 1, self._end)
        except MalformedPacketError:
            return None
        total = (rl_end - self._start) + remaining
        return None if total > self._max_packet_size else total

    def _reset(self) -> None:
        """Rewind a fully consumed slab."""
        extent = self._end
        self._start = 0
        self._end = 0
        if self._capacity > self._target_window:
            self._retire_oversize(extent)

    def writable_window(self, preferred: int = _MIN_WINDOW) -> memoryview:
        """Return writable storage for a receiver to fill, then `commit()`.

        `preferred` is an upper bound, not a floor: the window is always
        non-empty, which is all `get_buffer()` requires, but it is shortened to
        what an incomplete head frame still needs so the slab is never grown
        past the frame it is receiving.

        The window is a view into the decoder's own slab, so bytes written into
        it are already where the parser expects them. It is never empty, which
        `asyncio.BufferedProtocol.get_buffer()` requires. Callers must drop the
        view before the next call, since an outstanding export would block the
        slab from being replaced on the grow path.

        Capped at `RECEIVE_QUANTUM`: a slab left large by an earlier oversized
        frame would otherwise overshoot the receiver's high water in proportion
        to retained capacity, since that check only runs afterwards.
        """
        want = self._target_window
        if preferred > want:
            want = preferred
        exact: int | None = None
        if self._capacity - self._end < want:
            # Only when this call is about to grow, so the framing read stays
            # off the steady-state path. Doing it here rather than once the slab
            # is already large is what lets a cold large frame reserve its
            # extent at once instead of climbing there one doubling at a time.
            total = self._known_frame_total()
            if total is not None:
                # Only an *incomplete* head has a remainder, so this never
                # shrinks the window for ordinary complete-frame traffic. Asking
                # for more than the head still needs is what grows the slab past
                # the frame it is receiving.
                exact = total
                remaining = total - (self._end - self._start)
                if 0 < remaining < want:
                    want = remaining
        self._ensure(want, exact)
        end = self._end
        limit = end + want
        if limit > self._capacity:
            limit = self._capacity
        self._offered = limit - end
        return self._view[end:limit]

    def commit(self, nbytes: int) -> None:
        """Publish `nbytes` written into the window returned by `writable_window()`."""
        if nbytes <= 0:
            return
        offered = self._offered
        if nbytes > offered:
            raise ValueError("commit exceeds the window that was handed out")
        self._offered = 0
        # A completely filled window means the peer had at least that much
        # waiting; a few in a row is the signal to offer more.
        if nbytes == offered and self._target_window < RECEIVE_QUANTUM:
            self._full_fills += 1
            if self._full_fills >= _PROMOTE_AFTER:
                self._full_fills = 0
                self._target_window *= 2
        else:
            self._full_fills = 0
        end = self._end + nbytes
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
                remaining_length, rl_end = decode_vbi(buf, start + 1, self._end)
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
        if body_end - self._start > _LARGE_FRAME:
            self._drain_had_large_frame = True
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
        self._window_peak = 0
        self._drain_had_large_frame = False
        self._offered = 0
        self._target_window = _INITIAL_WINDOW
        self._full_fills = 0
        if self._capacity > _INITIAL_WINDOW:
            self._reallocate(_INITIAL_WINDOW)

    def next_packet(self) -> RawPacket | None:
        buf = self._buf
        start = self._start
        available = self._end - start
        if available < 2:
            return None

        header = buf[start]
        try:
            remaining_length, rl_end = decode_vbi(buf, start + 1, self._end)
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
        if total > _LARGE_FRAME:
            self._drain_had_large_frame = True
        if body_end == self._end:
            self._start = 0
            self._end = 0
            if self._capacity > self._target_window:
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
