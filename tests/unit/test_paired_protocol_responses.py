"""The protocol-response benchmark must measure the branch it names."""

from __future__ import annotations

from benchmarks.paired_protocol_responses import (
    _EXPECTED_TYPES,
    _build_response_effects,
    _run_phase,
)
from mqttium.enums import PacketType


def test_benchmark_corpus_comes_from_all_four_engine_transitions() -> None:
    effects = _build_response_effects()

    assert [PacketType.from_byte(effect.data[0]).name for effect in effects] == list(
        _EXPECTED_TYPES
    )
    assert all(effect.protocol_response is True for effect in effects)


async def test_candidate_benchmark_reaches_idle_transport_inline_and_keeps_fifo() -> None:
    result = await _run_phase(40)

    assert result.immediate_writes == result.count == 40
    assert result.queued_writes == 0
    assert result.marked_effect_types == list(_EXPECTED_TYPES)
    assert result.wire_packet_types == list(_EXPECTED_TYPES)
    assert result.fifo_segment_check is True
