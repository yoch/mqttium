"""Bound repeated-property amplification before CONNECT or packet delivery."""

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.codec import properties as codec
from mqttium.codec.primitives import pack_utf8
from mqttium.codec.vbi import encode_vbi
from mqttium.enums import MQTTProtocolVersion, PacketType
from mqttium.errors import MalformedPacketError, PacketTooLargeError, ProtocolError
from mqttium.packets import ConnAckPacket, PublishPacket, encode_frame
from mqttium.types import Properties
from tests.support import ScriptedBrokerTransport, transport_factory, wait_until

LIMIT = 1024


class PropertyConnackTransport(ScriptedBrokerTransport):
    def __init__(self, properties):
        super().__init__(protocol=MQTTProtocolVersion.MQTTv5)
        self.properties = properties

    def handle_packet(self, raw):
        if raw.packet_type is PacketType.CONNECT:
            body = b"\x00\x00" + codec.encode_properties(self.properties, codec.CONNACK)
            self.push_rx(encode_frame(PacketType.CONNACK, 0, body))
        else:
            super().handle_packet(raw)


@pytest.mark.parametrize("count", [0, 1, LIMIT])
async def test_connack_at_or_below_budget_preserves_order(count):
    pairs = tuple((str(index), f"value-{index}") for index in range(count))
    transport = PropertyConnackTransport(Properties({"user_property": pairs}))
    client = AsyncClient("property-budget", protocol=MQTTProtocolVersion.MQTTv5, keepalive=0)
    client._transport_factory = transport_factory(transport)
    try:
        connack = await asyncio.wait_for(client.connect("unused"), 1)
        assert connack.reason_code == 0
        assert connack.properties.get("user_property", ()) == pairs
        assert client.is_connected
    finally:
        await client.disconnect()
        await wait_until(lambda: not any(client._running_tasks().values()))


async def test_over_budget_connack_fails_before_success():
    transport = PropertyConnackTransport(Properties({"user_property": [("", "")] * (LIMIT + 1)}))
    client = AsyncClient("over-budget", protocol=MQTTProtocolVersion.MQTTv5, keepalive=0)
    client._transport_factory = transport_factory(transport)
    connected = []
    client.on_connect = lambda *_: connected.append(True)
    try:
        with pytest.raises(ProtocolError, match="decode budget"):
            # The watchdog is shorter than the MQTT timeout: a lost failure
            # or successful CONNACK cannot masquerade as prompt rejection.
            await asyncio.wait_for(client.connect("unused", timeout=10), 1)
        assert not connected
        assert not client.is_connected
        assert transport.is_closing()
    finally:
        await client.disconnect()
        await wait_until(lambda: not any(client._running_tasks().values()))


@pytest.mark.parametrize("packet", [codec.CONNACK, codec.PUBLISH, codec.SUBACK, codec.AUTH])
def test_budget_applies_in_the_shared_decoder(packet):
    wire = codec.encode_properties(Properties({"user_property": [("", "")] * (LIMIT + 1)}), packet)
    with pytest.raises(ProtocolError, match="decode budget"):
        codec.decode_properties(wire, 0, packet)


def test_publish_packet_uses_combined_repeatable_budget():
    pairs = tuple((str(index), "value") for index in range(LIMIT // 2))
    identifiers = tuple(range(1, LIMIT // 2 + 1))
    properties = Properties({"user_property": pairs, "subscription_identifier": identifiers})
    table = codec.encode_properties(properties, codec.PUBLISH)
    remaining = pack_utf8("topic") + table + b"payload"
    packet = PublishPacket.decode(0, remaining, MQTTProtocolVersion.MQTTv5)
    assert packet.payload == b"payload"
    assert packet.properties.get("user_property") == pairs
    assert packet.properties.get("subscription_identifier") == identifiers
    properties = Properties(
        {"user_property": pairs + (("extra", "value"),), "subscription_identifier": identifiers}
    )
    remaining = pack_utf8("topic") + codec.encode_properties(properties, codec.PUBLISH)
    with pytest.raises(ProtocolError, match="decode budget"):
        PublishPacket.decode(0, remaining, MQTTProtocolVersion.MQTTv5)


def test_over_budget_value_is_not_decoded_or_retained(monkeypatch):
    unpack = codec.unpack_utf8
    calls = 0

    def counted_unpack(buf, offset):
        nonlocal calls
        calls += 1
        return unpack(buf, offset)

    monkeypatch.setattr(codec, "unpack_utf8", counted_unpack)
    # The first excess property has no value. The budget must reject it before
    # a value decoder could report truncation or allocate another string pair.
    body = b"\x26\x00\x00\x00\x00" * LIMIT + b"\x26"
    with pytest.raises(ProtocolError, match="decode budget") as caught:
        ConnAckPacket.decode(b"\x00\x00" + encode_vbi(len(body)) + body, MQTTProtocolVersion.MQTTv5)
    assert calls == 2 * LIMIT
    traceback = caught.value.__traceback__
    while traceback is not None and traceback.tb_frame.f_code.co_name != "decode_properties":
        traceback = traceback.tb_next
    assert traceback is not None
    frame = traceback.tb_frame.f_locals
    assert frame["seen"] == {}
    assert not any(isinstance(value, list) and len(value) > 1 for value in frame.values())


async def test_wire_packet_limit_still_rejects_below_count_budget():
    transport = PropertyConnackTransport(Properties({"user_property": [("key", "v" * 128)]}))
    client = AsyncClient(
        "byte-budget", protocol=MQTTProtocolVersion.MQTTv5, keepalive=0, maximum_packet_size=64
    )
    client._transport_factory = transport_factory(transport)
    try:
        with pytest.raises(PacketTooLargeError):
            await asyncio.wait_for(client.connect("unused", timeout=10), 1)
        assert not client.is_connected
        assert client._decoder.max_packet_size == 64
    finally:
        await client.disconnect()
        await wait_until(lambda: not any(client._running_tasks().values()))


def test_existing_empty_and_singleton_validation_is_preserved():
    empty, end = codec.decode_properties(b"\x00", 0, codec.PUBLISH)
    assert end == 1 and not empty
    assert codec.decode_properties(b"\x00", 0, codec.CONNACK)[0] is empty
    with pytest.raises(MalformedPacketError, match="Duplicate"):
        codec.decode_properties(b"\x04\x01\x00\x01\x00", 0, codec.PUBLISH)
    with pytest.raises(MalformedPacketError, match="Duplicate"):
        codec.decode_properties(b"\x04\x0b\x01\x0b\x02", 0, codec.SUBSCRIBE)
