from dataclasses import replace
from pathlib import Path

import pytest

import mqttium.protocol.engine as engine_module
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
    original = engine_module.encode_properties

    def counting(properties: object, packet_type: object) -> bytes:
        nonlocal calls
        calls += 1
        return original(properties, packet_type)

    monkeypatch.setattr(engine_module, "encode_properties", counting)

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
