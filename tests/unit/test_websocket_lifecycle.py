"""WebSocket close, IPv6 handshake, and total-timeout lifecycle contracts."""

from __future__ import annotations

import asyncio
import base64
import hashlib

import pytest

from mqttium.transport.websocket import (
    WebSocketTransport,
    _build_handshake_request,
    _parse_websocket_endpoint,
    _read_handshake_response,
    _validate_handshake_response,
)


class _CancellableCloseWriter:
    def __init__(self) -> None:
        self.entered_wait = asyncio.Event()
        self.closed = False
        self.writes: list[bytes] = []
        self.transport = self
        self.aborted = asyncio.Event()

    def write(self, data: bytes) -> None:
        self.writes.append(data)

    async def drain(self) -> None:
        self.entered_wait.set()
        await asyncio.Event().wait()

    def close(self) -> None:
        self.closed = True

    def abort(self) -> None:
        # A real asyncio transport schedules connection_lost on abort, which
        # releases the shared wait_closed future even if its peer never reads.
        self.closed = True
        self.aborted.set()

    async def wait_closed(self) -> None:
        self.entered_wait.set()
        await self.aborted.wait()

    def is_closing(self) -> bool:
        return self.closed


async def test_cancelled_websocket_close_has_already_closed_stream() -> None:
    writer = _CancellableCloseWriter()
    transport = WebSocketTransport(None, writer)  # type: ignore[arg-type]

    task = asyncio.create_task(transport.close())
    await asyncio.wait_for(writer.entered_wait.wait(), timeout=1.0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1.0)

    assert writer.closed
    assert writer.aborted.is_set()
    assert writer.writes  # the close frame was queued before stream closure


def test_ipv6_handshake_host_header_is_bracketed() -> None:
    request = _build_handshake_request("::1", 8080, "/mqtt", "key", None)
    assert b"\r\nHost: [::1]:8080\r\n" in request
    assert b"\r\nHost: ::1:8080\r\n" not in request


class _DribblingReader:
    def __init__(self) -> None:
        self._chunks = iter((b"HTTP/1.1 101 Switching\r\n", b"Upgrade: websocket\r\n", b"\r\n"))

    async def read(self, n: int) -> bytes:
        del n
        await asyncio.sleep(0.04)
        return next(self._chunks, b"")


async def test_handshake_timeout_is_total_not_per_chunk() -> None:
    reader = _DribblingReader()
    loop = asyncio.get_running_loop()
    started = loop.time()

    with pytest.raises(TimeoutError):
        await _read_handshake_response(reader, timeout=0.05)  # type: ignore[arg-type]

    assert loop.time() - started < 0.10


def test_host_header_omits_the_scheme_default_port() -> None:
    request = _build_handshake_request(
        "broker.example", 443, "/mqtt", "key", None, default_port=443
    )
    assert b"\r\nHost: broker.example\r\n" in request
    request = _build_handshake_request(
        "broker.example", 8443, "/mqtt", "key", None, default_port=443
    )
    assert b"\r\nHost: broker.example:8443\r\n" in request


def _upgrade_response(key: str, subprotocol: str | None) -> bytes:
    accept = base64.b64encode(
        hashlib.sha1(
            (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode(), usedforsecurity=False
        ).digest()
    )
    lines = [
        b"HTTP/1.1 101 Switching Protocols",
        b"Upgrade: websocket",
        b"Connection: Upgrade",
        b"Sec-WebSocket-Accept: " + accept,
    ]
    if subprotocol is not None:
        lines.append(b"Sec-WebSocket-Protocol: " + subprotocol.encode())
    return b"\r\n".join(lines)


@pytest.mark.parametrize("subprotocol", ["mqtt", "MQTT", "mqttv3.1", None])
def test_mqtt_or_absent_subprotocol_is_accepted(subprotocol: str | None) -> None:
    _validate_handshake_response(_upgrade_response("key", subprotocol), "key")


def test_foreign_subprotocol_is_refused() -> None:
    with pytest.raises(ConnectionError, match="not MQTT"):
        _validate_handshake_response(_upgrade_response("key", "chat"), "key")


@pytest.mark.parametrize(
    "url", ["ws://user:secret@broker.example/mqtt", "wss://user@broker.example/mqtt"]
)
def test_credentials_in_the_url_are_refused(url: str) -> None:
    with pytest.raises(ValueError, match="credentials"):
        _parse_websocket_endpoint(url, None)
