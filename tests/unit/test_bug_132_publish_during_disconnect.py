"""Reproduction for GitHub issue #132."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import MQTTProtocolVersion, PacketType
from mqttium.packets import PublishPacket, encode_frame
from mqttium.persistence.memory import MemoryInflightStore


class _SessionTransport:
    def __init__(self, *, session_present: bool = False) -> None:
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._decoder = IncrementalDecoder()
        self._closing = False
        self.written: list[bytes] = []
        self.session_present = session_present

    async def write(self, data: bytes | tuple[bytes, bytes]) -> None:
        payload = data if isinstance(data, bytes) else data[0] + data[1]
        self.written.append(payload)
        self._decoder.feed(payload)
        for raw in self._decoder.drain_packets():
            if raw.packet_type is PacketType.CONNECT:
                flags = 0x01 if self.session_present else 0x00
                self._rx.put_nowait(encode_frame(PacketType.CONNACK, 0, bytes([flags, 0x00, 0x00])))

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
    reason="https://github.com/yoch/mqttium/issues/132 — failed disconnect-time publish is replayed",
)
async def test_failed_publish_during_disconnect_is_not_replayed() -> None:
    store = MemoryInflightStore()
    transport = _SessionTransport()
    client = AsyncClient(
        client_id="bug132",
        protocol=MQTTProtocolVersion.MQTTv5,
        clean_start=False,
        store=store,
    )

    async def factory(host: str, port: int, *, ssl: object = None) -> _SessionTransport:
        del host, port, ssl
        return transport

    client._transport_factory = factory  # type: ignore[method-assign]
    await client.connect("fake", timeout=2.0)

    async with client._lifecycle_lock:
        packet = client._engine.begin_disconnect(0)
        try:
            client.publish_nowait("late/qos1", b"x", qos=1)
            admitted = True
        except Exception:
            admitted = False
        await client._enqueue_outbound(packet)

    await client._force_close()

    # A publish that the caller observed as failed (or that occurred while
    # disconnecting) must not remain durable for session replay.
    assert not admitted or list(store.out_items()) == []

    transport2 = _SessionTransport(session_present=True)

    async def factory2(host: str, port: int, *, ssl: object = None) -> _SessionTransport:
        del host, port, ssl
        return transport2

    client._transport_factory = factory2  # type: ignore[method-assign]
    await client.connect("fake", timeout=2.0)
    await asyncio.sleep(0.2)
    decoder = IncrementalDecoder()
    pubs: list[str] = []
    for chunk in transport2.written:
        decoder.feed(chunk)
        for raw in decoder.drain_packets():
            if raw.packet_type is PacketType.PUBLISH:
                pubs.append(
                    PublishPacket.decode(
                        raw.flags, raw.remaining, MQTTProtocolVersion.MQTTv5
                    ).topic
                )
    assert "late/qos1" not in pubs
    await client._force_close()
