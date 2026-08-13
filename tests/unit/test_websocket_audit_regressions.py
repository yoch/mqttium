"""Regressions for WebSocket transport findings from the RC2 audit."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.transport.websocket import (
    WebSocketTransport,
    _build_handshake_request,
    _read_handshake_response,
)


class _CancellableCloseWriter:
    def __init__(self) -> None:
        self.entered_wait = asyncio.Event()
        self.closed = False
        self.writes: list[bytes] = []

    def write(self, data: bytes) -> None:
        self.writes.append(data)

    async def drain(self) -> None:
        self.entered_wait.set()
        await asyncio.Event().wait()

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        self.entered_wait.set()
        await asyncio.Event().wait()

    def is_closing(self) -> bool:
        return self.closed


@pytest.mark.asyncio
async def test_cancelled_websocket_close_has_already_closed_stream() -> None:
    writer = _CancellableCloseWriter()
    transport = WebSocketTransport(None, writer)  # type: ignore[arg-type]

    task = asyncio.create_task(transport.close())
    await asyncio.wait_for(writer.entered_wait.wait(), timeout=1.0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert writer.closed
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


@pytest.mark.asyncio
async def test_handshake_timeout_is_total_not_per_chunk() -> None:
    reader = _DribblingReader()
    loop = asyncio.get_running_loop()
    started = loop.time()

    with pytest.raises(TimeoutError):
        await _read_handshake_response(reader, timeout=0.05)  # type: ignore[arg-type]

    assert loop.time() - started < 0.10
