from __future__ import annotations

import sys
from pathlib import Path

import pytest

from mqttium.enums import MQTTProtocolVersion

_BENCHMARKS = Path(__file__).resolve().parents[2] / "benchmarks"
if str(_BENCHMARKS) not in sys.path:
    sys.path.insert(0, str(_BENCHMARKS))

import hotpath_recon as recon  # noqa: E402
from hotpath_recon import (  # noqa: E402
    _run_engine_qos1_ingress,
    _run_qos0_publish,
    _run_qos1_inbound_reply,
    _run_qos1_publish,
)


def test_cpu_times_without_posix_resource(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(recon, "resource", None)
    assert recon._cpu_times() == (0.0, 0.0)


def test_engine_qos1_ingress_is_send_ack_plus_message() -> None:
    result = _run_engine_qos1_ingress(protocol=MQTTProtocolVersion.MQTTv311, count=200)
    assert result["effects_per_ingress"] == 2
    assert result["operations"] == 200
    assert result["ops_per_s"] > 0


async def test_qos0_publish_direct_path_dominates_at_burst_one() -> None:
    result = await _run_qos0_publish(
        protocol=MQTTProtocolVersion.MQTTv311,
        burst=1,
        count=80,
        warmup=8,
        instrument=True,
    )
    counters = result["counters"]
    assert counters["qos0_direct_hit_rate"] == 1.0
    assert counters["eager_data_hit_rate"] == 1.0
    assert result["operations"] == 80


async def test_qos0_burst_eager_is_one_per_turn() -> None:
    result = await _run_qos0_publish(
        protocol=MQTTProtocolVersion.MQTTv311,
        burst=8,
        count=80,
        warmup=8,
        instrument=True,
    )
    counters = result["counters"]
    assert counters["qos0_direct_hit_rate"] == 1.0
    assert counters["eager_data_hits"] == 10
    assert counters["eager_data_misses"] == 70
    assert counters["eager_data_hit_rate"] == 0.125


async def test_qos1_inbound_reply_serial_uses_uniform_worker() -> None:
    result = await _run_qos1_inbound_reply(
        protocol=MQTTProtocolVersion.MQTTv311,
        outstanding=1,
        count=40,
        warmup=4,
        coalesce=False,
        instrument=True,
    )
    counters = result["counters"]
    assert counters["qos1_v311_field_decodes"] == 40
    assert counters["callback_inline_rate"] == 0.0
    assert counters["send_ack_effects"] == 40
    assert counters["message_effects"] == 40
    assert counters["effect_multi_batches"] == 40
    # The queued responder emits SEND after the original MESSAGE drain ended;
    # SEND and terminal ACK now each use an independent single-effect admission.
    assert counters["effect_collect_single_inline"] == 80
    assert result["operations"] == 40


async def test_qos1_coalesced_inbound_keeps_window() -> None:
    result = await _run_qos1_inbound_reply(
        protocol=MQTTProtocolVersion.MQTTv311,
        outstanding=8,
        count=64,
        warmup=8,
        coalesce=True,
        instrument=True,
    )
    assert result["operations"] == 64
    counters = result["counters"]
    assert counters["qos1_v311_field_decodes"] == 64
    assert counters["send_ack_effects"] == 64
    assert counters["message_effects"] == 64
    assert counters["effect_multi_batches"] > 0
    assert counters["callback_inline_rate"] == 0.0
    # Writer batching remains a separate scheduling policy.
    assert 0.0 < counters["eager_data_hit_rate"] < 1.0
    assert 0.0 < counters["eager_ack_hit_rate"] < 1.0


async def test_qos1_publish_completes_windowed_receipts() -> None:
    result = await _run_qos1_publish(
        protocol=MQTTProtocolVersion.MQTTv5,
        outstanding=4,
        count=32,
        warmup=4,
        instrument=True,
    )
    assert result["operations"] == 32
    assert result["ops_per_s"] > 0
    counters = result["counters"]
    assert counters["qos0_direct_attempts"] == 32
    assert counters["qos0_direct_hits"] == 0
