"""Reject invalid factory results before sending CONNECT or starting tasks."""

from __future__ import annotations

import pytest

from mqttium.api import AsyncClient
from tests.support import ScriptedBrokerTransport, transport_factory


class _WriteOnly:
    def __init__(self) -> None:
        self.written: list[bytes] = []
        self.closing = False

    async def write(self, data: bytes) -> None:
        self.written.append(data)

    async def write_many(self, parts: list[bytes]) -> None:
        self.written.extend(parts)

    async def close(self) -> None:
        self.closing = True

    def is_closing(self) -> bool:
        return self.closing


class _Both(_WriteOnly):
    async def read(self, n: int = 65536) -> bytes:
        raise AssertionError("must fail before reading")

    def attach_decoder(self, decoder: object) -> None:
        raise AssertionError("must fail before attaching")

    async def receive(self) -> bool:
        raise AssertionError("must fail before receiving")


@pytest.mark.parametrize("transport_type", [_WriteOnly, _Both])
async def test_invalid_receive_capability_fails_locally_and_client_can_retry(
    transport_type,
) -> None:
    transport = transport_type()
    client = AsyncClient("invalid-capability")
    client._transport_factory = transport_factory(transport)
    try:
        with pytest.raises(TypeError, match="exactly one receive capability"):
            await client.connect("unused", 0, timeout=1)
        assert transport.closing
        assert transport.written == []
        assert client._reader_task is None
        valid = ScriptedBrokerTransport()
        client._transport_factory = transport_factory(valid)
        await client.connect("unused", 0, timeout=1)
        assert client.is_connected
    finally:
        await client.disconnect()
