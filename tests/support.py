"""Shared in-memory transport utilities for unit tests."""

from __future__ import annotations

import asyncio


class QueueTransport:
    """Queue-backed transport with thread-safe inbound injection."""

    def __init__(self) -> None:
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._closing = False
        self._loop: asyncio.AbstractEventLoop | None = None

    def push_rx(self, data: bytes) -> None:
        """Inject bytes safely from either the client loop or another thread."""
        loop = self._loop
        if loop is None:
            self._rx.put_nowait(data)
            return
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None
        if running_loop is loop:
            self._rx.put_nowait(data)
        else:
            loop.call_soon_threadsafe(self._rx.put_nowait, data)

    async def read(self, n: int = 65536) -> bytes:
        del n
        self._loop = asyncio.get_running_loop()
        return await self._rx.get()

    async def close(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._closing = True
        self.push_rx(b"")

    def is_closing(self) -> bool:
        return self._closing


def feed_engine(engine: object, wire: bytes) -> None:
    """Decode one complete packet and pass it to a protocol engine."""
    from mqttium.codec.buffer import IncrementalDecoder

    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)  # type: ignore[attr-defined]


def write_item_bytes(data: object) -> bytes:
    """Flatten the bytes/segmented-write representation used by SEND effects."""
    if isinstance(data, bytes):
        return data
    assert isinstance(data, tuple)
    return data[0] + data[1]
