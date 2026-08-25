"""Bounded incremental MQTT frame decoder.

Design constraints (from Paho perf audit + gmqtt critique):
- Reusable bytearray buffer with read offset and bounded compaction
- Contiguous decode via indices / unpack_from when a full packet is present
- Never expose a memoryview into the reusable buffer to callers
- Enforce a maximum packet size early (after Remaining Length)
"""

from __future__ import annotations

from dataclasses import dataclass

from mqttium.codec.vbi import decode_vbi
from mqttium.enums import PacketType
from mqttium.errors import MalformedPacketError, PacketTooLargeError

# Default local ceiling before CONNACK negotiation (256 MiB is the MQTT max).
DEFAULT_MAX_PACKET_SIZE = 16 * 1024 * 1024
_COMPACT_THRESHOLD = 64 * 1024
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
    __slots__ = ("_buf", "_start", "_max_packet_size", "_high_water")

    def __init__(self, max_packet_size: int = DEFAULT_MAX_PACKET_SIZE) -> None:
        if max_packet_size < 2:
            raise ValueError("max_packet_size too small")
        self._buf = bytearray()
        self._start = 0
        self._max_packet_size = max_packet_size
        self._high_water = 0

    @property
    def buffered(self) -> int:
        return len(self._buf) - self._start

    @property
    def next_header_byte(self) -> int | None:
        """Return the next fixed-header byte without consuming buffered data."""
        return self._buf[self._start] if self._start < len(self._buf) else None

    def peek_packet_bounds(self) -> tuple[int, int, int] | None:
        """Return ``(header, body_start, body_end)`` for the next complete frame.

        The frame is not consumed and the reusable buffer is not exposed. This
        internal hot-path primitive deliberately mirrors ``next_packet`` framing
        so the generic decoder does not pay an extra Python call per packet.
        """
        buf = self._buf
        start = self._start
        available = len(buf) - start
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

    def consume_peeked_packet(self, body_end: int) -> None:
        """Commit a frame previously returned by :meth:`peek_packet_bounds`."""
        assert self._start < body_end <= len(self._buf)
        self._start = body_end
        if self._start == len(self._buf):
            self._buf.clear()
            self._start = 0

    @property
    def high_water(self) -> int:
        return self._high_water

    @property
    def max_packet_size(self) -> int:
        return self._max_packet_size

    @max_packet_size.setter
    def max_packet_size(self, value: int) -> None:
        if value < 2:
            raise ValueError("max_packet_size too small")
        self._max_packet_size = value

    def feed(self, data: bytes | bytearray | memoryview) -> None:
        if not data:
            return
        if self._start > _COMPACT_THRESHOLD:
            del self._buf[: self._start]
            self._start = 0
        self._buf.extend(data)
        buffered = len(self._buf) - self._start
        if buffered > self._high_water:
            self._high_water = buffered

    def clear(self) -> None:
        self._buf.clear()
        self._start = 0

    def next_packet(self) -> RawPacket | None:
        buf = self._buf
        start = self._start
        available = len(buf) - start
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
        # Copy the body out so callers never alias the reusable buffer.
        #
        # `bytes(buf[a:b])` copies twice: slicing a bytearray builds another
        # bytearray, which `bytes()` then copies again. Going through a
        # memoryview copies once.
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
        # The view is transient and released before the buffer is touched, so no
        # memoryview of the reusable buffer is ever handed out — the owned-bytes
        # invariant is unchanged.
        #
        # Only worth it above `_VIEW_COPY_THRESHOLD`: the memoryview object costs
        # more than the second copy saves on small frames, and small frames are
        # the hot path.
        if remaining_length >= _VIEW_COPY_THRESHOLD:
            view = memoryview(buf)
            try:
                body = bytes(view[body_start:body_end])
            finally:
                view.release()
        else:
            body = bytes(buf[body_start:body_end])
        self._start = body_end
        if self._start == len(self._buf):
            # Reuse the buffer object (avoid allocating a fresh bytearray).
            self._buf.clear()
            self._start = 0
        return RawPacket(packet_type=packet_type, flags=flags, remaining=body)

    def drain_packets(self, limit: int = 100) -> list[RawPacket]:
        packets: list[RawPacket] = []
        for _ in range(limit):
            packet = self.next_packet()
            if packet is None:
                break
            packets.append(packet)
        return packets
