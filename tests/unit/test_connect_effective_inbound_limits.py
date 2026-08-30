"""CONNECT-advertised inbound limits are the limits enforced at runtime."""

from __future__ import annotations

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.primitives import unpack_utf8
from mqttium.codec.properties import CONNECT, decode_properties
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import PacketTooLargeError
from mqttium.packets import PublishPacket, encode_frame
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import Properties
from tests.support import ScriptedBrokerTransport, transport_factory


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def _connect_properties(wire: bytes) -> Properties:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None and raw.packet_type is PacketType.CONNECT
    _, pos = unpack_utf8(raw.remaining, 0)
    pos += 1  # protocol level
    pos += 1  # CONNECT flags
    pos += 2  # keepalive
    properties, _ = decode_properties(raw.remaining, pos, CONNECT)
    return properties


def _qos1_publish(mid: int) -> bytes:
    return PublishPacket(
        topic="in",
        payload=b"x",
        qos=QoS.AT_LEAST_ONCE,
        retain=False,
        dup=False,
        mid=mid,
    ).encode(MQTTProtocolVersion.MQTTv5)


def test_connect_receive_maximum_override_is_the_inbound_limit() -> None:
    connect_properties = Properties({"receive_maximum": 10})
    engine = ProtocolEngine(
        EngineConfig(
            client_id="rm-override",
            protocol=MQTTProtocolVersion.MQTTv5,
            local_receive_maximum=2,
            manual_ack=True,
            connect_properties=connect_properties,
        )
    )

    connect = engine.begin_connect()
    assert _connect_properties(connect).get("receive_maximum") == 10
    _feed(engine, encode_frame(PacketType.CONNACK, 0, b"\x00\x00\x00"))
    engine.take_effects()

    # The application-owned property bag is no longer connection state after
    # CONNECT: enforcement must keep using the value that went on the wire.
    connect_properties.set("receive_maximum", 1)
    for mid in range(1, 11):
        _feed(engine, _qos1_publish(mid))
        effects = engine.take_effects()
        assert not any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)

    assert engine.inbound.stats().receive_maximum == 10
    assert engine.state is ConnectionState.CONNECTED

    _feed(engine, _qos1_publish(11))
    effects = engine.take_effects()
    assert engine.state is ConnectionState.DISCONNECTED
    disconnects = [effect.data for effect in effects if effect.kind is EffectKind.SEND]
    assert disconnects
    disconnect = disconnects[-1]
    assert isinstance(disconnect, bytes)
    assert disconnect[2] == 0x93


def test_connect_maximum_packet_size_override_is_the_decoder_limit() -> None:
    client = AsyncClient(
        client_id="mps-override",
        protocol=MQTTProtocolVersion.MQTTv5,
        maximum_packet_size=32,
        connect_properties=Properties({"maximum_packet_size": 64}),
    )

    connect = client._engine.begin_connect()
    assert _connect_properties(connect).get("maximum_packet_size") == 64
    assert client._decoder.max_packet_size == 64

    peer_packet = encode_frame(PacketType.PUBLISH, 0, b"x" * 35)
    assert 32 < len(peer_packet) <= 64
    client._decoder.feed(peer_packet)
    assert client._decoder.next_packet() is not None


def test_connect_maximum_packet_size_one_is_enforced_exactly() -> None:
    client = AsyncClient(
        protocol=MQTTProtocolVersion.MQTTv5,
        connect_properties=Properties({"maximum_packet_size": 1}),
    )

    assert client._decoder.max_packet_size == 1
    client._decoder.feed(encode_frame(PacketType.PINGRESP, 0, b""))
    with pytest.raises(PacketTooLargeError, match="exceeds maximum 1"):
        client._decoder.next_packet()


async def test_connect_resnapshots_mutated_maximum_packet_size_property() -> None:
    connect_properties = Properties({"maximum_packet_size": 64})
    client = AsyncClient(
        client_id="mps-snapshot",
        protocol=MQTTProtocolVersion.MQTTv5,
        maximum_packet_size=32,
        connect_properties=connect_properties,
    )
    transport = ScriptedBrokerTransport(protocol=MQTTProtocolVersion.MQTTv5)
    client._transport_factory = transport_factory(transport)

    connect_properties.set("maximum_packet_size", 96)
    await client.connect("fake", 1883, timeout=1.0)
    assert client._decoder.max_packet_size == 96

    connect_properties.set("maximum_packet_size", 16)
    assert client._decoder.max_packet_size == 96
    await client.disconnect()
