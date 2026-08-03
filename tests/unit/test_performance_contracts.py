from pathlib import Path

import pytest

from mqttium.api.async_client import AsyncClient


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
