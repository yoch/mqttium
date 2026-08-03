"""Tests for helpers.publish / helpers.subscribe (fake broker)."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.primitives import pack_u16
from mqttium.enums import PacketType, QoS
from mqttium.helpers import publish as publish_helper
from mqttium.helpers import subscribe as subscribe_helper
from mqttium.packets import PubAckPacket, PublishPacket, encode_frame


class FakeBrokerTransport:
    def __init__(self, *, push_publish: bytes | None = None) -> None:
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._decoder = IncrementalDecoder()
        self._closing = False
        self._push_publish = push_publish

    async def write(self, data: bytes) -> None:
        self._decoder.feed(data)
        for raw in self._decoder.drain_packets():
            if raw.packet_type is PacketType.CONNECT:
                self._rx.put_nowait(encode_frame(PacketType.CONNACK, 0, b"\x00\x00"))
            elif raw.packet_type is PacketType.PUBLISH:
                pub = PublishPacket.decode(raw.flags, raw.remaining)
                if pub.qos == QoS.AT_LEAST_ONCE and pub.mid is not None:
                    self._rx.put_nowait(PubAckPacket(mid=pub.mid).encode())
            elif raw.packet_type is PacketType.SUBSCRIBE:
                mid = int.from_bytes(raw.remaining[:2], "big")
                self._rx.put_nowait(encode_frame(PacketType.SUBACK, 0, pack_u16(mid) + bytes([0])))
                if self._push_publish is not None:
                    self._rx.put_nowait(self._push_publish)

    async def read(self, n: int = 65536) -> bytes:
        return await self._rx.get()

    async def close(self) -> None:
        self._closing = True
        self._rx.put_nowait(b"")

    def is_closing(self) -> bool:
        return self._closing


@pytest.mark.asyncio
async def test_publish_single_and_multiple(monkeypatch: pytest.MonkeyPatch) -> None:
    async def factory(host: str, port: int, *, ssl=None):
        return FakeBrokerTransport()

    from mqttium.api import async_client as ac

    monkeypatch.setattr(ac.TcpTransport, "connect", staticmethod(factory))

    await publish_helper.single("t/1", b"a", qos=1, hostname="fake", keepalive=5)
    await publish_helper.multiple(
        [
            {"topic": "t/2", "payload": b"b", "qos": 1},
            ("t/3", b"c", 0, False),
        ],
        hostname="fake",
        keepalive=5,
    )


@pytest.mark.asyncio
async def test_subscribe_simple(monkeypatch: pytest.MonkeyPatch) -> None:
    inbound = PublishPacket(topic="news/1", payload=b"hi", qos=0, retain=False, dup=False).encode()

    async def factory(host: str, port: int, *, ssl=None):
        return FakeBrokerTransport(push_publish=inbound)

    from mqttium.api import async_client as ac

    monkeypatch.setattr(ac.TcpTransport, "connect", staticmethod(factory))

    msg = await subscribe_helper.simple("news/#", hostname="fake", timeout=2.0, keepalive=5)
    assert not isinstance(msg, list)
    assert msg.topic == "news/1"
    assert msg.payload == b"hi"
