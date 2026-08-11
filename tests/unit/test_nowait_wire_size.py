from __future__ import annotations

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import ConnectionState, MQTTProtocolVersion, QoS
from mqttium.packets import PublishPacket
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
    import mqttium.protocol.outbound as outbound_module
    from mqttium.packets.publish import encode_publish_item as original

    calls = 0

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(outbound_module, "encode_publish_item", counted)
    client = AsyncClient(max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED

    receipt = client.publish_nowait("bench/nowait", b"payload", qos=1)

    assert receipt.mid is not None
    assert calls == 1
    assert client._effect_flush_task is None
