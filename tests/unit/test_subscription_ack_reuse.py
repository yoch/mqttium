"""Deferred request results must settle before their identifiers are reused."""

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PublishPacket, encode_frame
from tests.support import QueueTransport, wait_until


class _PausedRequestTransport(QueueTransport):
    def __init__(self, protocol: MQTTProtocolVersion) -> None:
        super().__init__()
        self.protocol = protocol
        self.decoder = IncrementalDecoder()
        self.requests: list[int] = []
        self.gate = asyncio.Event()
        self.gate.set()
        self.write_held = asyncio.Event()

    async def write(self, data: bytes) -> None:
        if not self.gate.is_set():
            self.write_held.set()
        await self.gate.wait()
        self.decoder.feed(data)
        for raw in self.decoder.drain_packets():
            if raw.packet_type is PacketType.CONNECT:
                self.push_rx(encode_frame(PacketType.CONNACK, 0, b"\x00\x00" + self.properties))
            elif raw.packet_type in (PacketType.SUBSCRIBE, PacketType.UNSUBSCRIBE):
                self.requests.append(int.from_bytes(raw.remaining[:2], "big"))

    @property
    def properties(self) -> bytes:
        return b"\x00" if self.protocol is MQTTProtocolVersion.MQTTv5 else b""

    def acknowledgement(self, mid: int, unsubscribe: bool) -> bytes:
        reasons = b"\x00" if not unsubscribe or self.properties else b""
        kind = PacketType.UNSUBACK if unsubscribe else PacketType.SUBACK
        return encode_frame(kind, 0, mid.to_bytes(2, "big") + self.properties + reasons)

    async def close(self) -> None:
        self.gate.set()
        await super().close()


@pytest.mark.parametrize("protocol", [MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5])
@pytest.mark.parametrize("unsubscribe", [False, True])
async def test_deferred_ack_cannot_complete_reused_identifier(protocol, unsubscribe) -> None:
    transport = _PausedRequestTransport(protocol)
    client = AsyncClient(
        "request-reuse", protocol=protocol, keepalive=0, max_write_queue_messages=1
    )

    async def factory(*args, **kwargs):
        return transport

    client._transport_factory = factory
    await client.connect("unused")
    request = client.unsubscribe if unsubscribe else client.subscribe
    tasks = []
    try:
        first = asyncio.create_task(request("first"))
        tasks.append(first)
        await wait_until(lambda: len(transport.requests) == 1)
        mid = transport.requests[0]
        transport.gate.clear()
        await client.publish("block", b"x")
        await transport.write_held.wait()
        incoming = PublishPacket(
            topic="incoming", payload=b"x", qos=QoS.AT_LEAST_ONCE, mid=7, retain=False, dup=False
        ).encode(protocol)
        transport.push_rx(transport.acknowledgement(mid, unsubscribe) + incoming)
        await wait_until(lambda: client._write_pump.waiters == 1)
        second = asyncio.create_task(request("second"))
        tasks.append(second)
        await wait_until(lambda: client._effect_pump.waiters >= 2)
        transport.gate.set()
        await wait_until(lambda: len(transport.requests) == 2)
        assert transport.requests == [mid, mid]
        assert not second.done(), "The second exchange has not received an acknowledgement"
        assert (await asyncio.wait_for(first, 1)).mid == mid
        transport.push_rx(transport.acknowledgement(mid, unsubscribe))
        assert (await asyncio.wait_for(second, 1)).mid == mid
    finally:
        transport.gate.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await client.disconnect()
