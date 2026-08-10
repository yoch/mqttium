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
