"""Writer failure cleanup when transport close also fails."""

from __future__ import annotations

import asyncio

from mqttium.api.async_client import AsyncClient
from mqttium.enums import ConnectionState


class _WriteAndCloseFailTransport:
    def __init__(self) -> None:
        self.write_error = OSError("primary write failure")
        self.close_error = OSError("secondary close failure")
        self.close_calls = 0

    async def write(self, data: bytes) -> None:
        del data
        raise self.write_error

    async def write_many(self, parts: list[bytes]) -> None:
        del parts
        raise self.write_error

    async def read(self, n: int = 65536) -> bytes:
        del n
        return b""

    async def close(self) -> None:
        self.close_calls += 1
        raise self.close_error

    def is_closing(self) -> bool:
        return False


async def test_secondary_close_failure_does_not_escape_writer_failure_path() -> None:
    transport = _WriteAndCloseFailTransport()
    client = AsyncClient(client_id="writer-close-failure")
    client._transport = transport
    client._engine.state = ConnectionState.CONNECTED
    client._write_pump.start(transport)

    assert client._try_enqueue_outbound(b"trigger")
    writer = client._writer_task
    assert writer is not None

    await asyncio.wait_for(writer, timeout=1.0)

    assert writer.exception() is None
    assert client._disconnect_exc is transport.write_error
    assert transport.close_calls >= 1
