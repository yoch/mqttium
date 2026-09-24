"""A peer error inside an ingress lot keeps the valid prefix and ends the lot.

Packets decoded before a malformed, oversized or protocol-violating packet
were observed: their outcomes are committed, acknowledged and delivered as if
the lot had ended there. Nothing after the fatal packet is processed. The
result must not depend on how the peer's bytes were split into reads.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import BrokerDisconnectError, MalformedPacketError, ProtocolError
from mqttium.packets import PubAckPacket, PublishPacket, encode_frame
from mqttium.persistence.sqlite import SqliteInflightStore
from tests.support import ScriptedBrokerTransport, transport_factory, wait_until

MALFORMED = b"\x00\x00"  # reserved packet type 0


def _acks(transport: ScriptedBrokerTransport, first_byte: int) -> list[int]:
    return [
        int.from_bytes(data[2:4], "big")
        for data in transport.written
        if data and data[0] == first_byte and len(data) >= 4
    ]


class _TrailingBroker(ScriptedBrokerTransport):
    """Answers a QoS 1 PUBLISH with PUBACK and a malformed packet in one read."""

    def handle_packet(self, raw) -> None:
        if raw.packet_type is PacketType.PUBLISH:
            publish = PublishPacket.decode(raw.flags, raw.remaining, self.protocol)
            assert publish.mid is not None
            self.push_rx(PubAckPacket(mid=publish.mid).encode(self.protocol) + MALFORMED)
            return
        super().handle_packet(raw)


async def test_malformed_packet_after_puback_keeps_the_committed_outcome(tmp_path: Path) -> None:
    # #511: the malformed trailer used to roll back the PUBACK's store delete
    # and fail the receipt before its already-observed completion was applied.
    path = tmp_path / "prefix.db"
    store = SqliteInflightStore(path)
    transport = _TrailingBroker()
    disconnects: list[BaseException | None] = []
    client = AsyncClient("prefix-ack", store=store, clean_start=False, keepalive=0)
    client.on_disconnect = disconnects.append
    client._transport_factory = transport_factory(transport)
    await client.connect("fake")
    try:
        receipt = await client.publish("prefix/ack", b"x", qos=QoS.AT_LEAST_ONCE)
        await asyncio.wait_for(receipt.wait(), 1)
        await wait_until(lambda: disconnects != [])
        assert isinstance(disconnects[0], MalformedPacketError)
        assert receipt.mid is not None
        assert store.out_meta(receipt.mid) is None
        mid = receipt.mid
    finally:
        await client.disconnect()
    reopened = SqliteInflightStore(path)
    try:
        assert reopened.out_meta(mid) is None
    finally:
        reopened.close()


class _SubackTrailingBroker(ScriptedBrokerTransport):
    """Answers SUBSCRIBE with SUBACK and a malformed packet in one read."""

    def handle_packet(self, raw) -> None:
        if raw.packet_type is PacketType.SUBSCRIBE:
            mid = int.from_bytes(raw.remaining[:2], "big")
            self.push_rx(
                encode_frame(PacketType.SUBACK, 0, mid.to_bytes(2, "big") + b"\x01") + MALFORMED
            )
            return
        super().handle_packet(raw)


async def test_malformed_packet_after_suback_returns_the_subscription() -> None:
    # #511 variant: SUBACK observed in the same lot as the malformed packet.
    transport = _SubackTrailingBroker()
    client = AsyncClient("prefix-suback", keepalive=0)
    client._transport_factory = transport_factory(transport)
    await client.connect("fake")
    try:
        result = await asyncio.wait_for(client.subscribe("prefix/+", qos=QoS.AT_LEAST_ONCE), 1)
        assert list(result.reason_codes) == [1]
    finally:
        await client.disconnect()


@pytest.mark.parametrize("coalesced", [True, False])
async def test_protocol_error_keeps_acked_prefix_and_stops_the_lot(coalesced: bool) -> None:
    # #513: a QoS 1 message acknowledged before the violation must reach the
    # application; a PUBLISH after the violation must be neither acknowledged
    # nor delivered. Coalesced and split reads must agree.
    transport = ScriptedBrokerTransport()
    disconnects: list[BaseException | None] = []
    client = AsyncClient("prefix-protocol", keepalive=0)
    client.on_disconnect = disconnects.append
    client._transport_factory = transport_factory(transport)
    await client.connect("fake")
    stream = client.messages()

    def publish(mid: int) -> bytes:
        return PublishPacket(
            topic=f"prefix/{mid}",
            payload=b"x",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            dup=False,
            mid=mid,
        ).encode()

    violation = encode_frame(PacketType.CONNACK, 0, b"\x00\x00")  # CONNACK while connected
    chunks = [publish(1), violation, publish(2)]
    try:
        if coalesced:
            transport.push_rx(b"".join(chunks))
        else:
            for chunk in chunks:
                transport.push_rx(chunk)
        await wait_until(lambda: disconnects != [])
        assert isinstance(disconnects[0], ProtocolError)
        first = await asyncio.wait_for(anext(stream), 1)
        assert first.mid == 1
        assert _acks(transport, 0x40) == [1]
        assert client._delivery.messages_queue.empty()
    finally:
        await stream.aclose()
        await client.disconnect()


async def test_bytes_after_broker_disconnect_keep_the_broker_reason() -> None:
    # After the broker's DISCONNECT nothing on the connection is processed:
    # a malformed trailer must neither replace the broker's reason nor make
    # the client send a DISCONNECT of its own.
    transport = ScriptedBrokerTransport(protocol=MQTTProtocolVersion.MQTTv5)
    disconnects: list[BaseException | None] = []
    client = AsyncClient("broker-reason", protocol=MQTTProtocolVersion.MQTTv5, keepalive=0)
    client.on_disconnect = disconnects.append
    client._transport_factory = transport_factory(transport)
    await client.connect("fake")
    try:
        transport.push_rx(encode_frame(PacketType.DISCONNECT, 0, b"\x8b\x00") + MALFORMED)
        await wait_until(lambda: disconnects != [])
        assert isinstance(disconnects[0], BrokerDisconnectError)
        assert disconnects[0].reason_code == 0x8B
        assert not [d for d in transport.written if d and d[0] == 0xE0]
    finally:
        await client.disconnect()


async def test_fatal_disconnect_is_never_sent_after_the_engine_ended_the_connection() -> None:
    # The engine's own DISCONNECT (or its terminal state) is final: the
    # runtime's normative DISCONNECT must not follow it on an open transport.
    transport = ScriptedBrokerTransport(protocol=MQTTProtocolVersion.MQTTv5)
    client = AsyncClient("single-disconnect", protocol=MQTTProtocolVersion.MQTTv5, keepalive=0)
    client._transport_factory = transport_factory(transport)
    await client.connect("fake")
    try:
        client._engine._protocol_disconnect(0x82)
        client._engine.take_effects()
        written = len(transport.written)
        await client._send_fatal_disconnect(ProtocolError("late"))
        assert len(transport.written) == written
    finally:
        await client.disconnect()
