from dataclasses import replace
from pathlib import Path

import pytest

import mqttium.protocol.outbound as outbound_module
from mqttium.api.async_client import AsyncClient
from mqttium.enums import MQTTProtocolVersion
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from mqttium.types import Properties


def test_async_client_exposes_outbound_window() -> None:
    client = AsyncClient(max_outbound_inflight=7)
    assert client._engine.config.max_outbound_inflight == 7


def test_async_client_rejects_invalid_outbound_window() -> None:
    with pytest.raises(ValueError, match="max_outbound_inflight"):
        AsyncClient(max_outbound_inflight=0)


def test_comparative_benchmark_uses_equivalent_public_contracts() -> None:
    root = Path(__file__).resolve().parents[2]
    compare = (root / "benchmarks" / "compare_libs.py").read_text()
    sprint = (root / "benchmarks" / "perf_sprint.py").read_text()
    assert "max_outbound_inflight=WINDOW" in compare
    assert "max_inflight_messages_set(WINDOW)" in compare
    assert "pending.pop(0)" in compare
    assert "no equivalent public per-publish completion contract" in compare
    assert "wait_empty" not in compare
    assert "132" not in compare
    assert "max_outbound_inflight=outbound_window" in sprint
    assert "local_receive_maximum=local_rm" not in sprint


def test_realworld_benchmark_confirms_payload_identity() -> None:
    root = Path(__file__).resolve().parents[2]
    realworld = (root / "benchmarks" / "realworld.py").read_text()
    assert "mosquitto_sub" in realworld
    assert "sorted(sequences) != list(range(args.count))" in realworld
    assert "publisher_ack_msg_s" in realworld
    assert "delivered_msg_s" in realworld
    assert "latency_p99_ms" in realworld


def test_publish_admission_encodes_properties_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """The wire-size check and the logical budget share one property encode.

    Both need the same topic and property measurements; computing them twice was
    pure duplicated allocation on the QoS 1/2 publish path whenever the broker
    advertised a maximum packet size.
    """
    calls = 0
    original = outbound_module.encode_properties

    def counting(properties: object, packet_type: object) -> bytes:
        nonlocal calls
        calls += 1
        return original(properties, packet_type)

    monkeypatch.setattr(outbound_module, "encode_properties", counting)

    properties = Properties()
    properties.add_user_property("source", "contract")
    engine = ProtocolEngine(
        EngineConfig(protocol=MQTTProtocolVersion.MQTTv5, max_pending_outbound_bytes=None)
    )
    engine.negotiated = replace(engine.negotiated, maximum_packet_size=1_000_000)

    engine.queue_publish("contract/topic", b"payload", qos=1, properties=properties)
    assert calls == 1

    calls = 0
    engine.queue_publish("contract/topic", b"payload", qos=1)
    assert calls == 0, "an empty property table needs no encode"


def test_connected_qos1_launch_reuses_cached_property_body(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sizing and the actual MQTT 5 frame encode walk each property only once."""
    import mqttium.codec.properties as properties_module
    from mqttium.enums import ConnectionState

    calls = 0
    original = properties_module._encode_value

    def counting(prop_type: object, value: object) -> bytes:
        nonlocal calls
        calls += 1
        return original(prop_type, value)

    monkeypatch.setattr(properties_module, "_encode_value", counting)
    properties = Properties()
    properties.add_user_property("source", "contract")
    properties.set("content_type", "application/octet-stream")
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv5))
    engine.state = ConnectionState.CONNECTED

    engine.queue_publish("contract/topic", b"payload", qos=1, properties=properties)

    assert calls == 2


async def test_nowait_publish_encodes_properties_once(monkeypatch) -> None:
    """Admission must not re-encode a property table queue_publish will encode."""
    import mqttium.protocol.outbound as outbound_module
    from mqttium.api import AsyncClient
    from mqttium.enums import ConnectionState

    original = outbound_module.encode_properties
    calls = 0

    def counting(properties: object, packet_type: object) -> bytes:
        nonlocal calls
        calls += 1
        return original(properties, packet_type)

    monkeypatch.setattr(outbound_module, "encode_properties", counting)

    properties = Properties()
    properties.add_user_property("source", "contract")
    client = AsyncClient(protocol=MQTTProtocolVersion.MQTTv5, max_outbound_messages=64)
    client._engine.state = ConnectionState.CONNECTED
    client._engine.negotiated = replace(client._engine.negotiated, maximum_packet_size=1_000_000)

    client.publish_nowait("contract/topic", b"payload", qos=1, properties=properties)

    assert calls == 1


async def test_nowait_admission_still_refuses_when_the_writer_is_loaded() -> None:
    """Skipping the preview must only apply while the queue is genuinely empty."""
    from mqttium.api import AsyncClient
    from mqttium.enums import ConnectionState
    from mqttium.errors import FlowControlError

    client = AsyncClient(max_outbound_messages=10_000, max_outbound_bytes=2048)
    client._engine.state = ConnectionState.CONNECTED

    client.publish_nowait("contract/full", b"x" * 1800, qos=0)
    with pytest.raises(FlowControlError):
        client.publish_nowait("contract/full", b"x" * 1800, qos=0)


def test_success_acks_encode_to_the_fixed_four_byte_frame() -> None:
    """Every specialized success encoder produces the fixed wire frame."""
    from mqttium.enums import PacketType
    from mqttium.packets.acks import PubAckPacket, PubCompPacket, PubRecPacket, PubRelPacket

    cases = (
        (PubAckPacket, PacketType.PUBACK, 0),
        (PubRecPacket, PacketType.PUBREC, 0),
        (PubRelPacket, PacketType.PUBREL, 0x02),
        (PubCompPacket, PacketType.PUBCOMP, 0),
    )
    for cls, packet_type, flags in cases:
        for protocol in (MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5):
            for mid in (1, 255, 256, 65535):
                encoded = cls(mid=mid).encode(protocol)
                assert encoded == bytes((int(packet_type) | flags, 2, mid >> 8, mid & 0xFF)), (
                    cls,
                    protocol,
                    mid,
                )
                assert len(encoded) == 4


def test_acks_with_reason_or_properties_keep_the_full_encoding() -> None:
    """Only the zero-reason, no-property case may use the shortest frame."""
    from mqttium.packets.acks import PubAckPacket

    with_reason = PubAckPacket(mid=9, reason_code=0x10).encode(MQTTProtocolVersion.MQTTv5)
    assert len(with_reason) > 4
    assert with_reason[2:4] == (9).to_bytes(2, "big")

    properties = Properties()
    properties.set("reason_string", "no matching subscribers")
    with_props = PubAckPacket(mid=9, properties=properties).encode(MQTTProtocolVersion.MQTTv5)
    assert len(with_props) > 4

    # An empty table is not "properties" and must still take the shortcut.
    assert len(PubAckPacket(mid=9, properties=Properties()).encode()) == 4


def test_publish_encoder_accepts_int_and_enum_identically() -> None:
    """Skipping the enum call for a real member must not change any output."""
    from mqttium.enums import QoS
    from mqttium.packets.publish import encode_publish_item

    for level, mid in ((0, None), (1, 7), (2, 7)):
        for protocol in (MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5):
            as_int = encode_publish_item(
                "a/b",
                b"p",
                qos=level,
                retain=False,
                dup=False,
                mid=mid,
                properties=None,
                protocol=protocol,
            )
            as_enum = encode_publish_item(
                "a/b",
                b"p",
                qos=QoS(level),
                retain=False,
                dup=False,
                mid=mid,
                properties=None,
                protocol=protocol,
            )
            assert as_int == as_enum


@pytest.mark.parametrize("bad", [3, -1, "0", None, 1.5])
def test_publish_encoder_still_rejects_invalid_qos(bad: object) -> None:
    from mqttium.errors import ProtocolError
    from mqttium.packets.publish import encode_publish_item

    with pytest.raises(ProtocolError):
        encode_publish_item("a/b", b"p", qos=bad, retain=False, dup=False, mid=1, properties=None)
