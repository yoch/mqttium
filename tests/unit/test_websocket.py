"""WebSocket transport unit tests (handshake + strict RFC 6455 framing)."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import struct

import pytest

from mqttium.transport.websocket import WebSocketTransport, _mask_client_frame, _parse_frame


def _server_accept(key: str) -> str:
    return base64.b64encode(
        hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()
    ).decode("ascii")


def _server_frame(
    opcode: int,
    payload: bytes,
    *,
    fin: bool = True,
    rsv: int = 0,
) -> bytes:
    """Build an unmasked server→client frame."""
    header = bytearray()
    header.append((0x80 if fin else 0) | (rsv & 0x70) | (opcode & 0x0F))
    ln = len(payload)
    if ln < 126:
        header.append(ln)
    elif ln < 65536:
        header.append(126)
        header.extend(struct.pack("!H", ln))
    else:
        header.append(127)
        header.extend(struct.pack("!Q", ln))
    return bytes(header) + payload


def _decode_client_batch(batch: list[bytes], max_frame_size: int = 1 << 20) -> bytes:
    frame = bytearray(b"".join(batch))
    parsed = _parse_frame(frame, max_frame_size, expect_masked=True)
    assert parsed is not None
    assert frame == b""
    return parsed[2]


@pytest.mark.asyncio
async def test_websocket_binary_roundtrip_and_immediate_pong() -> None:
    loop = asyncio.get_running_loop()
    got_mqtt = loop.create_future()
    got_pong = loop.create_future()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        req = b""
        while b"\r\n\r\n" not in req:
            chunk = await reader.read(4096)
            if not chunk:
                return
            req += chunk
        key_line = next(
            line
            for line in req.decode("latin1").split("\r\n")
            if line.lower().startswith("sec-websocket-key:")
        )
        key = key_line.split(":", 1)[1].strip()
        accept = _server_accept(key)
        resp = (
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: keep-alive, Upgrade\r\n"
            f"Sec-WebSocket-Accept: {accept}\r\n"
            "Sec-WebSocket-Protocol: mqtt\r\n"
            "\r\n"
        )
        writer.write(resp.encode("ascii"))
        await writer.drain()
        # Send a ping followed immediately by a binary MQTT-looking payload.
        writer.write(_server_frame(0x9, b"ping-payload"))
        writer.write(_server_frame(0x2, b"\x30\x02\x00\x00"))
        await writer.drain()

        buf = bytearray()
        while not (got_pong.done() and got_mqtt.done()):
            chunk = await reader.read(4096)
            if not chunk:
                break
            buf.extend(chunk)
            while True:
                parsed = _parse_frame(
                    buf,
                    16 * 1024 * 1024,
                    expect_masked=True,
                )
                if parsed is None:
                    break
                fin, opcode, payload = parsed
                if opcode == 0xA and not got_pong.done():
                    got_pong.set_result(payload)
                if opcode == 0x2 and not got_mqtt.done():
                    got_mqtt.set_result(payload)
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]
    try:
        transport = await WebSocketTransport.connect(f"ws://{host}:{port}/mqtt")
        data = await transport.read()
        assert data == b"\x30\x02\x00\x00"

        # The pong must be emitted by read(), not deferred until the next
        # application write.
        assert await asyncio.wait_for(got_pong, timeout=2.0) == b"ping-payload"

        await transport.write(b"\x10\x00")  # dummy CONNECT fragment
        assert await asyncio.wait_for(got_mqtt, timeout=2.0) == b"\x10\x00"
        await transport.close()
    finally:
        server.close()
        await server.wait_closed()


def test_mask_frame_roundtrip_parse() -> None:
    frame = _mask_client_frame(0x2, b"abc")
    buf = bytearray(frame)
    fin, opcode, payload = _parse_frame(
        buf,
        16 * 1024 * 1024,
        expect_masked=True,
    )
    assert fin is True
    assert opcode == 0x2
    assert payload == b"abc"
    assert buf == b""


def test_rejects_masked_server_frame() -> None:
    buf = bytearray(_mask_client_frame(0x2, b"abc"))
    with pytest.raises(ConnectionError, match="unmasked"):
        _parse_frame(buf, 1024, expect_masked=False)


def test_rejects_text_and_reserved_opcodes() -> None:
    with pytest.raises(ConnectionError, match="binary"):
        _parse_frame(bytearray(_server_frame(0x1, b"text")), 1024, expect_masked=False)
    with pytest.raises(ConnectionError, match="opcode"):
        _parse_frame(bytearray(_server_frame(0x3, b"x")), 1024, expect_masked=False)


def test_rejects_rsv_bits_without_extension() -> None:
    with pytest.raises(ConnectionError, match="RSV"):
        _parse_frame(
            bytearray(_server_frame(0x2, b"x", rsv=0x40)),
            1024,
            expect_masked=False,
        )


def test_rejects_non_canonical_frame_lengths() -> None:
    # Payload length 1 encoded using the 16-bit extended form.
    buf = bytearray(b"\x82\x7e\x00\x01x")
    with pytest.raises(ConnectionError, match="Non-canonical"):
        _parse_frame(buf, 1024, expect_masked=False)


def test_rejects_new_data_frame_during_fragmentation() -> None:
    transport = WebSocketTransport(None, None)  # type: ignore[arg-type]
    transport._recv_buf.extend(
        _server_frame(0x2, b"part-1", fin=False) + _server_frame(0x2, b"part-2", fin=True)
    )
    with pytest.raises(ConnectionError, match="fragmented message"):
        transport._try_extract_application_payload()


def test_reassembles_binary_fragmentation_with_interleaved_pong() -> None:
    transport = WebSocketTransport(None, None)  # type: ignore[arg-type]
    transport._recv_buf.extend(
        _server_frame(0x2, b"part-1", fin=False)
        + _server_frame(0xA, b"ignored")
        + _server_frame(0x0, b"part-2", fin=True)
    )
    assert transport._try_extract_application_payload() == b"part-1part-2"


@pytest.mark.asyncio
async def test_invalid_extra_headers_are_rejected_before_connect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opened = False

    async def open_connection(*args, **kwargs):
        nonlocal opened
        opened = True
        raise AssertionError("connection should not be opened")

    monkeypatch.setattr(asyncio, "open_connection", open_connection)
    with pytest.raises(ValueError, match="CR/LF"):
        await WebSocketTransport.connect(
            "ws://localhost/mqtt",
            extra_headers={"X-Test": "bad\r\nInjected: value"},
        )
    assert opened is False


@pytest.mark.asyncio
async def test_failed_handshake_closes_stream() -> None:
    peer_closed = asyncio.Event()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.readuntil(b"\r\n\r\n")
        writer.write(b"HTTP/1.1 403 Forbidden\r\n\r\n")
        await writer.drain()
        assert await reader.read() == b""
        peer_closed.set()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    assert server.sockets
    host, port = server.sockets[0].getsockname()[:2]
    try:
        with pytest.raises(ConnectionError, match="403 Forbidden"):
            await WebSocketTransport.connect(f"ws://{host}:{port}/mqtt")
        await asyncio.wait_for(peer_closed.wait(), timeout=2.0)
    finally:
        server.close()
        await server.wait_closed()


class _WriteBufferTransport:
    def get_write_buffer_size(self) -> int:
        return 0


class _BatchWriter:
    def __init__(self) -> None:
        self.transport = _WriteBufferTransport()
        self.batches: list[list[bytes]] = []

    def writelines(self, frames: list[bytes]) -> None:
        self.batches.append(list(frames))

    async def drain(self) -> None:
        return None


async def test_write_many_bounds_masked_frames_by_bytes() -> None:
    writer = _BatchWriter()
    transport = WebSocketTransport(
        None,  # type: ignore[arg-type]
        writer,  # type: ignore[arg-type]
        max_write_batch_bytes=160,
    )
    parts = [b"x" * 100, b"y" * 100, b"z" * 20]

    await transport.write_many(parts)

    assert len(writer.batches) == 2
    assert all(sum(map(len, batch)) <= 160 for batch in writer.batches)
    assert [_decode_client_batch(batch) for batch in writer.batches] == [
        parts[0],
        parts[1] + parts[2],
    ]


async def test_write_many_coalesces_mqtt_packets_into_one_websocket_message() -> None:
    writer = _BatchWriter()
    transport = WebSocketTransport(
        None,  # type: ignore[arg-type]
        writer,  # type: ignore[arg-type]
        max_write_batch_bytes=256,
    )
    parts = [b"a" * 40, b"b" * 40, b"c" * 40, b"d" * 200]

    await transport.write_many(parts)

    assert len(writer.batches) == 2
    assert _decode_client_batch(writer.batches[0]) == b"".join(parts[:3])
    assert _decode_client_batch(writer.batches[1]) == parts[3]
    assert all(sum(map(len, batch)) <= 256 for batch in writer.batches)


async def test_write_many_does_not_coalesce_beyond_max_frame_size() -> None:
    writer = _BatchWriter()
    transport = WebSocketTransport(
        None,  # type: ignore[arg-type]
        writer,  # type: ignore[arg-type]
        max_frame_size=100,
        max_write_batch_bytes=512,
    )
    parts = [b"a" * 60, b"b" * 60, b"c" * 20]

    await transport.write_many(parts)

    assert [_decode_client_batch(batch) for batch in writer.batches] == [
        parts[0],
        parts[1] + parts[2],
    ]


async def test_write_many_emits_one_oversized_mqtt_packet_alone() -> None:
    writer = _BatchWriter()
    transport = WebSocketTransport(
        None,  # type: ignore[arg-type]
        writer,  # type: ignore[arg-type]
        max_frame_size=64,
        max_write_batch_bytes=64,
    )
    part = b"x" * 100

    await transport.write_many([part])

    assert len(writer.batches) == 1
    assert _decode_client_batch(writer.batches[0]) == part


@pytest.mark.parametrize("payload_size", [0, 1, 2, 3, 4, 5, 125, 126, 127, 65_535, 65_536])
def test_vectorized_mask_matches_rfc_xor_at_length_boundaries(payload_size: int) -> None:
    payload = bytes(range(251)) * (payload_size // 251) + bytes(range(payload_size % 251))
    mask = b"\x12\x34\x56\x78"

    frame = _mask_client_frame(0x2, payload, mask=mask)
    header_size = 2 if payload_size < 126 else 4 if payload_size < 65_536 else 10
    assert frame[header_size : header_size + 4] == mask
    assert frame[header_size + 4 :] == bytes(
        value ^ mask[index & 3] for index, value in enumerate(payload)
    )

    parsed = _parse_frame(
        bytearray(frame),
        max(payload_size, 1),
        expect_masked=True,
    )
    assert parsed == (True, 0x2, payload)


def test_mask_frame_rejects_non_four_byte_test_mask() -> None:
    with pytest.raises(ValueError, match="four bytes"):
        _mask_client_frame(0x2, b"payload", mask=b"bad")
