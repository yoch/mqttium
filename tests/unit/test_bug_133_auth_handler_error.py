"""Reproduction for GitHub issue #133."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import MQTTProtocolVersion, PacketType
from mqttium.errors import MQTTTimeoutError
from mqttium.packets import AuthPacket
from mqttium.types import Properties


class _AuthTransport:
    def __init__(self) -> None:
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._decoder = IncrementalDecoder()
        self._closing = False

    async def write(self, data: bytes | tuple[bytes, bytes]) -> None:
        payload = data if isinstance(data, bytes) else data[0] + data[1]
        self._decoder.feed(payload)
        for raw in self._decoder.drain_packets():
            if raw.packet_type is PacketType.CONNECT:
                props = Properties()
                props.set("authentication_method", "demo")
                self._rx.put_nowait(AuthPacket(reason_code=0x18, properties=props).encode())

    async def read(self, n: int = 65536) -> bytes:
        del n
        return await self._rx.get()

    async def close(self) -> None:
        self._closing = True
        self._rx.put_nowait(b"")

    def is_closing(self) -> bool:
        return self._closing


@pytest.mark.xfail(
    strict=True,
    reason="https://github.com/yoch/mqttium/issues/133 — auth_handler errors become CONNACK timeout",
)
async def test_auth_handler_error_fails_connect_immediately() -> None:
    transport = _AuthTransport()

    async def bad_auth(packet: object) -> None:
        del packet
        raise RuntimeError("auth boom")

    client = AsyncClient(
        client_id="bug133",
        protocol=MQTTProtocolVersion.MQTTv5,
        connect_properties=Properties(values={"authentication_method": "demo"}),
        auth_handler=bad_auth,
    )

    async def factory(host: str, port: int, *, ssl: object = None) -> _AuthTransport:
        del host, port, ssl
        return transport

    client._transport_factory = factory  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="auth boom"):
        await asyncio.wait_for(client.connect("fake", timeout=2.0), timeout=0.8)

    # Guard against the current failure mode specifically.
    assert not isinstance(
        getattr(getattr(client, "_disconnect_exc", None), "__class__", type(None))(),
        MQTTTimeoutError,
    )
