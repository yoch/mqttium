"""Regression coverage for enhanced-auth handler failures during CONNECT."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType
from mqttium.packets import AuthPacket
from mqttium.types import Properties


class _AuthChallengeTransport:
    def __init__(self) -> None:
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._decoder = IncrementalDecoder()
        self._closing = False

    async def write(self, data: bytes | tuple[bytes, bytes]) -> None:
        wire = data if isinstance(data, bytes) else data[0] + data[1]
        self._decoder.feed(wire)
        for raw in self._decoder.drain_packets():
            if raw.packet_type is PacketType.CONNECT:
                props = Properties()
                props.set("authentication_method", "demo")
                self._rx.put_nowait(
                    AuthPacket(reason_code=0x18, properties=props).encode(
                        MQTTProtocolVersion.MQTTv5
                    )
                )

    async def read(self, n: int = 65536) -> bytes:
        return await self._rx.get()

    async def close(self) -> None:
        self._closing = True
        self._rx.put_nowait(b"")

    def is_closing(self) -> bool:
        return self._closing


async def test_auth_handler_exception_propagates_from_connect() -> None:
    transport = _AuthChallengeTransport()

    async def bad_auth(_packet: AuthPacket) -> None:
        raise RuntimeError("auth boom")

    props = Properties()
    props.set("authentication_method", "demo")
    client = AsyncClient(
        client_id="auth-failure",
        protocol=MQTTProtocolVersion.MQTTv5,
        connect_properties=props,
        auth_handler=bad_auth,
    )

    async def factory(
        host: str,
        port: int,
        *,
        ssl: object = None,
    ) -> _AuthChallengeTransport:
        return transport

    client._transport_factory = factory

    with pytest.raises(RuntimeError, match="auth boom"):
        await client.connect("fake", timeout=0.5)

    assert transport.is_closing()
    assert client.state is ConnectionState.DISCONNECTED
    assert client._transport is None
