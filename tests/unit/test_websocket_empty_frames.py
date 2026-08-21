"""Empty WebSocket application messages must not look like stream EOF."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.transport.websocket import WebSocketTransport


class _Writer:
    transport = None

    def is_closing(self) -> bool:
        return False


def _server_frame(opcode: int, payload: bytes, *, fin: bool = True) -> bytes:
    assert len(payload) < 126
    return bytes(((0x80 if fin else 0) | opcode, len(payload))) + payload


@pytest.mark.parametrize(
    "empty_message",
    [
        _server_frame(0x2, b""),
        _server_frame(0x2, b"", fin=False) + _server_frame(0x0, b"", fin=True),
    ],
)
async def test_read_skips_empty_binary_message_without_signalling_eof(
    empty_message: bytes,
) -> None:
    reader = asyncio.StreamReader()
    transport = WebSocketTransport(reader, _Writer())  # type: ignore[arg-type]
    mqtt_payload = b"\x30\x00"

    reader.feed_data(empty_message + _server_frame(0x2, mqtt_payload))

    assert await asyncio.wait_for(transport.read(), timeout=1.0) == mqtt_payload
    assert not transport.is_closing()

    reader.feed_eof()
    assert await asyncio.wait_for(transport.read(), timeout=1.0) == b""
    assert transport.is_closing()
