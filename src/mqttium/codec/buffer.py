"""Bounded incremental MQTT frame decoder.

Design constraints (from Paho perf audit + gmqtt critique):
- Reusable bytearray buffer with read offset and bounded compaction
- Contiguous decode via indices / unpack_from when a full packet is present
- Never expose a memoryview into the reusable buffer to callers
- Enforce a maximum packet size early (after Remaining Length)
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from mqttium.codec.vbi import decode_vbi
from mqttium.enums import PacketType
from mqttium.errors import MalformedPacketError, PacketTooLargeError

# Default local ceiling before CONNACK negotiation (256 MiB is the MQTT max).
DEFAULT_MAX_PACKET_SIZE = 16 * 1024 * 1024
_COMPACT_THRESHOLD = 64 * 1024


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
        if self._start and self._start > _COMPACT_THRESHOLD:
            self._compact()
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
        # Copy body out so callers never alias the reusable buffer.
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

    def process_packets(self, callback: Callable[[RawPacket], None], limit: int = 100) -> int:
        """Decode up to *limit* packets, invoking *callback* without a list alloc."""
        n = 0
        for _ in range(limit):
            packet = self.next_packet()
            if packet is None:
                break
            callback(packet)
            n += 1
        return n

    def _compact(self) -> None:
        if self._start <= 0:
            return
        del self._buf[: self._start]
        self._start = 0
