"""Reproduction for GitHub issue #115.

``ack()`` after a transport drop must not delete durable inbound state without
sending PUBACK/PUBCOMP to the broker.
"""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import NotConnectedError, ProtocolError
from mqttium.packets import PublishPacket, encode_frame
from mqttium.persistence.memory import MemoryInflightStore


class _DropTransport:
    def __init__(self) -> None:
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._decoder = IncrementalDecoder()
        self._closing = False
        self.written: list[bytes] = []

    async def write(self, data: bytes | tuple[bytes, bytes]) -> None:
        payload = data if isinstance(data, bytes) else data[0] + data[1]
        self.written.append(payload)
        self._decoder.feed(payload)
        for raw in self._decoder.drain_packets():
            if raw.packet_type is PacketType.CONNECT:
                self._rx.put_nowait(encode_frame(PacketType.CONNACK, 0, b"\x00\x00\x00"))

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
    reason="https://github.com/yoch/mqttium/issues/115 — ack after disconnect drops store without PUBACK",
)
async def test_ack_after_disconnect_preserves_inbound_or_fails() -> None:
    store = MemoryInflightStore()
    transport = _DropTransport()
    held: dict[str, object] = {}
    got = asyncio.Event()
    client = AsyncClient(
        client_id="bug115",
        protocol=MQTTProtocolVersion.MQTTv5,
        clean_start=False,
        manual_ack=True,
        message_delivery="callback",
        store=store,
    )

    def on_message(message: object) -> None:
        held["msg"] = message
        got.set()

    client.on_message = on_message

    async def factory(host: str, port: int, *, ssl: object = None) -> _DropTransport:
        del host, port, ssl
        return transport

    client._transport_factory = factory  # type: ignore[method-assign]
    await client.connect("fake", timeout=2.0)
    transport._rx.put_nowait(
        PublishPacket(
            topic="t",
            payload=b"p",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            dup=False,
            mid=7,
        ).encode(MQTTProtocolVersion.MQTTv5)
    )
    await asyncio.wait_for(got.wait(), timeout=2.0)
    assert store.get_in(7) is not None

    await transport.close()
    await asyncio.sleep(0.15)
    assert store.get_in(7) is not None

    with pytest.raises((NotConnectedError, ProtocolError, RuntimeError)):
        await client.ack(held["msg"])  # type: ignore[arg-type]

    assert store.get_in(7) is not None
