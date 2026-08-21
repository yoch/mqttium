"""WebSocket transport for MQTT (binary frames, MQTT subprotocol)."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import os
import struct
from contextlib import suppress
from typing import Any
from urllib.parse import urlparse

from mqttium.transport._stream import write_buffer_needs_drain
from mqttium.transport.stats import TransportStats

_MAX_HANDSHAKE_BYTES = 64 * 1024
_MAX_CONTROL_PAYLOAD = 125
_MAX_PENDING_CONTROL = 16
_MAX_COALESCED_PAYLOAD = 256 * 1024
_XOR_TABLES: tuple[bytes, ...] | None = None


class WebSocketTransport:
    """Minimal MQTT-over-WebSocket client transport (RFC 6455 binary frames)."""

    __slots__ = (
        "_reader",
        "_writer",
        "_recv_buf",
        "_closing",
        "_pending_control",
        "_max_frame_size",
        "_max_write_batch_bytes",
        "_fragment",
    )

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        max_frame_size: int = 16 * 1024 * 1024,
        max_write_batch_bytes: int = 1 * 1024 * 1024,
    ) -> None:
        if max_frame_size <= 0:
            raise ValueError("max_frame_size must be positive")
        if max_write_batch_bytes <= 0:
            raise ValueError("max_write_batch_bytes must be positive")
        self._reader = reader
        self._writer = writer
        self._recv_buf = bytearray()
        self._closing = False
        self._pending_control: list[bytes] = []
        self._max_frame_size = max_frame_size
        self._max_write_batch_bytes = max_write_batch_bytes
        # Reassembled fragmented binary message (FIN=0 sequence).
        self._fragment: bytearray | None = None

    @classmethod
    async def connect(
        cls,
        url: str,
        *,
        ssl: Any = None,
        extra_headers: dict[str, str] | None = None,
        timeout: float = 30.0,
        max_frame_size: int = 16 * 1024 * 1024,
        max_write_batch_bytes: int = 1 * 1024 * 1024,
    ) -> WebSocketTransport:
        host, port, path, use_ssl = _parse_websocket_endpoint(url, ssl)
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        request = _build_handshake_request(host, port, path, key, extra_headers)
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port, ssl=use_ssl),
            timeout=timeout,
        )
        try:
            writer.write(request)
            await writer.drain()
            head, leftover = await _read_handshake_response(reader, timeout)
            _validate_handshake_response(head, key)
        except BaseException:
            await _close_stream_writer(writer)
            raise
        transport = cls(
            reader,
            writer,
            max_frame_size=max_frame_size,
            max_write_batch_bytes=max_write_batch_bytes,
        )
        transport._recv_buf.extend(leftover)
        return transport

    async def write(self, data: bytes) -> None:
        await self._flush_control()
        frame = _mask_client_frame(0x2, data)  # binary
        self._writer.write(frame)
        if write_buffer_needs_drain(self._writer):
            await self._writer.drain()

    async def write_many(self, parts: list[bytes]) -> None:
        if not parts:
            return
        await self._flush_control()
        # A WebSocket binary message may contain several consecutive MQTT
        # Control Packets. Coalesce them before framing so one batch pays for
        # one random mask and one RFC 6455 header, while retaining scatter/gather
        # writes for the already-owned MQTT byte strings.
        batch: list[bytes] = []
        payload_bytes = 0
        for part in parts:
            projected = payload_bytes + len(part)
            if batch and (
                _masked_frame_size(projected) > self._max_write_batch_bytes
                or projected > self._max_frame_size
                or projected > _MAX_COALESCED_PAYLOAD
            ):
                await self._write_payload_batch(batch, payload_bytes)
                payload_bytes = 0
                projected = len(part)
            batch.append(part)
            payload_bytes = projected
            # One MQTT packet larger than a configured batch/frame bound is
            # still emitted alone, matching the previous write_many contract.
            if (
                _masked_frame_size(payload_bytes) >= self._max_write_batch_bytes
                or payload_bytes >= self._max_frame_size
                or payload_bytes >= _MAX_COALESCED_PAYLOAD
            ):
                await self._write_payload_batch(batch, payload_bytes)
                payload_bytes = 0
        if batch:
            await self._write_payload_batch(batch, payload_bytes)

    async def _write_payload_batch(self, parts: list[bytes], payload_bytes: int) -> None:
        payload = parts[0] if len(parts) == 1 else b"".join(parts)
        mask = os.urandom(4)
        # Header and masked payload are separate TCP write segments but together
        # form one RFC 6455 binary frame. This avoids a final header+payload copy.
        self._writer.writelines(
            [
                _client_frame_header(0x2, payload_bytes, mask),
                _mask_payload(payload, mask),
            ]
        )
        parts.clear()
        if write_buffer_needs_drain(self._writer):
            await self._writer.drain()

    async def drain(self) -> None:
        await self._writer.drain()

    async def read(self, n: int = 65536) -> bytes:
        while True:
            payload = self._try_extract_application_payload()
            if self._pending_control:
                # RFC 6455 requires a pong as soon as practical. Flush it before
                # waiting for any more network or application traffic.
                await self._flush_control()
                if payload is None:
                    continue
            if payload is not None:
                return payload
            chunk = await self._reader.read(n)
            if not chunk:
                self._closing = True
                return b""
            self._recv_buf.extend(chunk)

    async def close(self) -> None:
        self._closing = True
        try:
            # Queue the close frame, then close the stream before the first
            # cancellation point. asyncio transports flush already-buffered
            # bytes while closing; cancellation must never leave the TCP stream
            # open merely because drain()/wait_closed() was interrupted.
            with suppress(Exception):
                self._writer.write(_mask_client_frame(0x8, b""))
            await _close_stream_writer(self._writer)
        finally:
            # No buffered frame can be reused after the underlying stream is
            # closed. Release connection-scoped storage immediately even when
            # the transport object remains referenced by the client.
            self._recv_buf.clear()
            self._pending_control.clear()
            self._fragment = None

    def is_closing(self) -> bool:
        return self._closing or self._writer.is_closing()

    @property
    def pending_write_bytes(self) -> int:
        transport = self._writer.transport
        return 0 if transport is None else transport.get_write_buffer_size()

    @property
    def buffered_read_bytes(self) -> int:
        return len(self._recv_buf)

    @property
    def fragmented_read_bytes(self) -> int:
        return 0 if self._fragment is None else len(self._fragment)

    @property
    def pending_control_frames(self) -> int:
        return len(self._pending_control)

    @property
    def pending_control_bytes(self) -> int:
        return sum(map(len, self._pending_control))

    def stats(self) -> TransportStats:
        return TransportStats(
            kind=type(self).__name__,
            closing=self.is_closing(),
            pending_write_bytes=self.pending_write_bytes,
            buffered_read_bytes=self.buffered_read_bytes,
            fragmented_read_bytes=self.fragmented_read_bytes,
            pending_control_frames=self.pending_control_frames,
            pending_control_bytes=self.pending_control_bytes,
        )

    async def _flush_control(self) -> None:
        if not self._pending_control:
            return
        self._writer.writelines(self._pending_control)
        self._pending_control.clear()
        if write_buffer_needs_drain(self._writer):
            await self._writer.drain()

    def _try_extract_application_payload(self) -> bytes | None:  # noqa: C901
        """Extract the next binary MQTT payload and process control frames."""
        while True:
            parsed = _parse_frame(
                self._recv_buf,
                self._max_frame_size,
                expect_masked=False,
            )
            if parsed is None:
                return None
            fin, opcode, raw = parsed
            if opcode == 0x8:  # close
                self._closing = True
                return b""
            if opcode == 0x9:  # ping → immediate pong on the next read-loop turn
                if len(self._pending_control) >= _MAX_PENDING_CONTROL:
                    raise ConnectionError("Too many pending WebSocket control replies")
                self._pending_control.append(_mask_client_frame(0xA, raw))
                return None
            if opcode == 0xA:  # pong — ignore
                continue
            if opcode == 0x2:
                if self._fragment is not None:
                    raise ConnectionError(
                        "WebSocket binary frame started before fragmented message completed"
                    )
                if not fin:
                    self._fragment = bytearray(raw)
                    continue
                if not raw:
                    continue
                return raw
            if opcode == 0x0:  # continuation
                if self._fragment is None:
                    raise ConnectionError("WebSocket continuation without start frame")
                self._fragment.extend(raw)
                if len(self._fragment) > self._max_frame_size:
                    raise ConnectionError("WebSocket fragmented message too large")
                if fin:
                    payload = bytes(self._fragment)
                    self._fragment = None
                    if not payload:
                        continue
                    return payload
                continue
            # _parse_frame rejects every non-MQTT application opcode.
            raise AssertionError(f"unreachable WebSocket opcode {opcode}")


def _parse_websocket_endpoint(url: str, ssl: Any) -> tuple[str, int, str, Any]:
    parsed = urlparse(url)
    if parsed.scheme not in ("ws", "wss"):
        raise ValueError(f"Unsupported WebSocket URL scheme: {parsed.scheme}")
    if parsed.scheme == "wss" and ssl is False:
        raise ValueError("wss:// requires TLS (ssl=False would downgrade silently)")
    host = parsed.hostname or "localhost"
    port = parsed.port or (443 if parsed.scheme == "wss" else 80)
    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"
    use_ssl = ssl if ssl is not None else (True if parsed.scheme == "wss" else None)
    return host, port, path, use_ssl


def _build_handshake_request(
    host: str,
    port: int,
    path: str,
    key: str,
    extra_headers: dict[str, str] | None,
) -> bytes:
    host_header = f"[{host}]" if ":" in host and not host.startswith("[") else host
    headers = [
        f"GET {path} HTTP/1.1",
        f"Host: {host_header}:{port}",
        "Upgrade: websocket",
        "Connection: Upgrade",
        f"Sec-WebSocket-Key: {key}",
        "Sec-WebSocket-Version: 13",
        "Sec-WebSocket-Protocol: mqtt",
    ]
    for name, value in (extra_headers or {}).items():
        if any(char in name or char in value for char in ("\r", "\n")):
            raise ValueError("extra_headers must not contain CR/LF")
        headers.append(f"{name}: {value}")
    return ("\r\n".join(headers) + "\r\n\r\n").encode("ascii")


async def _read_handshake_response(
    reader: asyncio.StreamReader,
    timeout: float,
) -> tuple[bytes, bytes]:
    buffer = bytearray()
    delimiter = b"\r\n\r\n"
    async with asyncio.timeout(timeout):
        while delimiter not in buffer:
            chunk = await reader.read(4096)
            if not chunk:
                raise ConnectionError("WebSocket handshake closed early")
            buffer.extend(chunk)
            if len(buffer) > _MAX_HANDSHAKE_BYTES:
                raise ConnectionError("WebSocket handshake headers too large")
    head, _, leftover = bytes(buffer).partition(delimiter)
    return head, leftover


def _validate_handshake_response(head: bytes, key: str) -> None:
    lines = head.split(b"\r\n")
    status_line = lines[0].decode("ascii", errors="replace")
    parts = status_line.split(" ", 2)
    if len(parts) < 2 or parts[1] != "101":
        raise ConnectionError(f"WebSocket handshake failed: {status_line}")
    headers = _parse_http_headers(lines[1:])
    accept_source = (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()
    # RFC 6455 mandates SHA-1 here as a non-security protocol transform.
    expected = base64.b64encode(hashlib.sha1(accept_source, usedforsecurity=False).digest()).decode(
        "ascii"
    )
    if headers.get("sec-websocket-accept") != expected:
        raise ConnectionError("WebSocket accept key mismatch")
    if headers.get("upgrade", "").lower() != "websocket":
        raise ConnectionError("WebSocket handshake missing Upgrade: websocket")
    connection_tokens = {
        token.strip().lower() for token in headers.get("connection", "").split(",") if token.strip()
    }
    if "upgrade" not in connection_tokens:
        raise ConnectionError("WebSocket handshake missing Connection: Upgrade")
    if headers.get("sec-websocket-protocol", "").lower() != "mqtt":
        raise ConnectionError("WebSocket subprotocol 'mqtt' not negotiated")


def _parse_http_headers(lines: list[bytes]) -> dict[str, str]:
    headers: dict[str, str] = {}
    for line in lines:
        if b":" not in line:
            continue
        raw_name, _, raw_value = line.partition(b":")
        headers[raw_name.decode("latin1").strip().lower()] = raw_value.decode("latin1").strip()
    return headers


async def _close_stream_writer(writer: asyncio.StreamWriter) -> None:
    writer.close()
    with suppress(Exception):
        await writer.wait_closed()


def _xor_tables() -> tuple[bytes, ...]:
    """Return the lazily-built byte translation tables used for masking."""

    global _XOR_TABLES
    tables = _XOR_TABLES
    if tables is None:
        tables = tuple(bytes(value ^ key for value in range(256)) for key in range(256))
        _XOR_TABLES = tables
    return tables


def _mask_payload(payload: bytes, mask: bytes) -> bytes:
    """Apply one RFC 6455 mask without a Python loop per payload byte."""

    tables = _xor_tables()
    masked = bytearray(len(payload))
    for offset, key in enumerate(mask):
        masked[offset::4] = payload[offset::4].translate(tables[key])
    return bytes(masked)


def _masked_frame_size(payload_size: int) -> int:
    if payload_size < 126:
        return payload_size + 6
    if payload_size < 65536:
        return payload_size + 8
    return payload_size + 14


def _client_frame_header(opcode: int, payload_size: int, mask: bytes) -> bytes:
    header = bytearray()
    header.append(0x80 | (opcode & 0x0F))
    if payload_size < 126:
        header.append(0x80 | payload_size)
    elif payload_size < 65536:
        header.append(0x80 | 126)
        header.extend(struct.pack("!H", payload_size))
    else:
        header.append(0x80 | 127)
        header.extend(struct.pack("!Q", payload_size))
    header.extend(mask)
    return bytes(header)


def _mask_client_frame(
    opcode: int,
    payload: bytes,
    *,
    mask: bytes | None = None,
) -> bytes:
    if mask is None:
        mask = os.urandom(4)
    elif len(mask) != 4:
        raise ValueError("WebSocket mask must contain exactly four bytes")
    return _client_frame_header(opcode, len(payload), mask) + _mask_payload(payload, mask)


def _parse_frame(  # noqa: C901
    buf: bytearray,
    max_frame_size: int,
    *,
    expect_masked: bool | None = None,
) -> tuple[bool, int, bytes] | None:
    """Consume one RFC 6455 frame and return ``(fin, opcode, payload)``.

    ``expect_masked=False`` is used for server→client frames and rejects masked
    server frames. ``expect_masked=True`` is useful for server-side tests parsing
    client frames. ``None`` accepts either direction for low-level fuzzing only.
    """
    if len(buf) < 2:
        return None
    b0 = buf[0]
    b1 = buf[1]
    if b0 & 0x70:
        raise ConnectionError("WebSocket RSV bits set without negotiated extension")
    fin = bool(b0 & 0x80)
    masked = bool(b1 & 0x80)
    if expect_masked is not None and masked is not expect_masked:
        direction = "masked" if expect_masked else "unmasked"
        raise ConnectionError(f"Expected {direction} WebSocket frame")

    opcode = b0 & 0x0F
    if opcode not in (0x0, 0x2, 0x8, 0x9, 0xA):
        if opcode == 0x1:
            raise ConnectionError("MQTT over WebSocket requires binary frames")
        raise ConnectionError(f"Unsupported WebSocket opcode 0x{opcode:x}")

    ln = b1 & 0x7F
    pos = 2
    if ln == 126:
        if len(buf) < 4:
            return None
        ln = struct.unpack_from("!H", buf, 2)[0]
        if ln < 126:
            raise ConnectionError("Non-canonical WebSocket frame length")
        pos = 4
    elif ln == 127:
        if len(buf) < 10:
            return None
        ln = struct.unpack_from("!Q", buf, 2)[0]
        if ln & (1 << 63):
            raise ConnectionError("Invalid 64-bit WebSocket frame length (MSB set)")
        if ln < 65536:
            raise ConnectionError("Non-canonical WebSocket frame length")
        pos = 10
    if ln > max_frame_size:
        raise ConnectionError(f"WebSocket frame {ln} exceeds max {max_frame_size}")
    if opcode in (0x8, 0x9, 0xA):
        if ln > _MAX_CONTROL_PAYLOAD:
            raise ConnectionError("WebSocket control frame payload too large")
        if not fin:
            raise ConnectionError("WebSocket control frame must not be fragmented")
        if opcode == 0x8 and ln == 1:
            raise ConnectionError("WebSocket close frame payload must not be one byte")

    mask_len = 4 if masked else 0
    total = pos + mask_len + ln
    if len(buf) < total:
        return None
    mask = bytes(buf[pos : pos + mask_len]) if masked else b""
    start = pos + mask_len
    raw = bytes(buf[start:total])
    del buf[:total]
    if masked:
        raw = bytes(b ^ mask[i % 4] for i, b in enumerate(raw))
    return fin, opcode, raw
