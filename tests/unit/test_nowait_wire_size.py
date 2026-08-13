from __future__ import annotations

import pytest

import mqttium.protocol.outbound as outbound_module
from mqttium.api import AsyncClient
from mqttium.enums import ConnectionState, MQTTProtocolVersion, QoS
from mqttium.packets import PublishPacket
from mqttium.packets.publish import encode_publish_item
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from mqttium.transport.writes import item_size
from mqttium.types import Properties


def _properties(protocol: MQTTProtocolVersion, rich: bool) -> Properties | None:
    if protocol is MQTTProtocolVersion.MQTTv311 or not rich:
        return None
    properties = Properties()
    properties.add_user_property("source", "nowait-wire-size")
    properties.set("content_type", "application/octet-stream")
    return properties


@pytest.mark.parametrize("protocol", list(MQTTProtocolVersion))
@pytest.mark.parametrize("qos", list(QoS))
@pytest.mark.parametrize(
    ("topic", "payload_size", "rich_properties"),
    [
        ("bench/nowait", 0, False),
        ("capteur/température", 64, False),
        ("bench/vbi-boundary", 127, True),
        ("bench/segmented", 1 << 20, True),
    ],
)
def test_publish_wire_size_matches_encoded_frame(
    protocol: MQTTProtocolVersion,
    qos: QoS,
    topic: str,
    payload_size: int,
    rich_properties: bool,
) -> None:
    properties = _properties(protocol, rich_properties)
    engine = ProtocolEngine(EngineConfig(protocol=protocol))
    actual = PublishPacket(
        topic=topic,
        payload=b"x" * payload_size,
        qos=qos,
        retain=True,
        dup=False,
        mid=None if qos is QoS.AT_MOST_ONCE else 17,
        properties=properties,
    ).encode_write_item(protocol)
    assert engine.outbound.publish_wire_size(
        topic,
        payload_size,
        qos,
        properties,
    ) == item_size(actual)


async def test_publish_nowait_encodes_only_the_real_frame(monkeypatch) -> None:
    packet_calls = 0
    item_calls = 0
    original_packet = PublishPacket.encode_write_item
    original_item = outbound_module.encode_publish_item

    def counted_packet(
        self: PublishPacket,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    ):
        nonlocal packet_calls
        packet_calls += 1
        return original_packet(self, protocol)

    def counted_item(*args, **kwargs):
        nonlocal item_calls
        item_calls += 1
        return original_item(*args, **kwargs)

    monkeypatch.setattr(PublishPacket, "encode_write_item", counted_packet)
    monkeypatch.setattr(outbound_module, "encode_publish_item", counted_item)
    client = AsyncClient(max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED

    receipt = client.publish_nowait("bench/nowait", b"payload", qos=1)

    assert receipt.mid is not None
    assert packet_calls == 0
    assert item_calls == 1
    assert client._effect_flush_task is None


def test_qos1_launch_frame_matches_publish_packet() -> None:
    """Dropping the packet object must not change the bytes on the wire."""
    engine = ProtocolEngine(EngineConfig())
    engine.state = ConnectionState.CONNECTED
    handle = engine.queue_publish("bench/launch", b"payload", qos=QoS.AT_LEAST_ONCE, retain=True)
    effects = engine.take_effects()
    sent = next(effect.data for effect in effects if effect.kind is EffectKind.SEND)
    expected = encode_publish_item(
        "bench/launch",
        b"payload",
        qos=QoS.AT_LEAST_ONCE,
        retain=True,
        dup=False,
        mid=handle.mid,
        properties=None,
    )
    assert sent == expected
    assert (
        sent
        == PublishPacket(
            topic="bench/launch",
            payload=b"payload",
            qos=QoS.AT_LEAST_ONCE,
            retain=True,
            dup=False,
            mid=handle.mid,
            properties=None,
        ).encode_write_item()
    )
