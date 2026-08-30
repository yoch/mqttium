"""The protocol-response benchmark must measure the branch it names."""

from __future__ import annotations

from benchmarks.paired_protocol_responses import (
    _EXPECTED_TYPES,
    _build_response_effects,
    _run_phase,
)
from mqttium.enums import PacketType
from mqttium.protocol.effects import EffectKind


def test_benchmark_corpus_is_limited_to_inbound_protocol_responses() -> None:
    effects = _build_response_effects()

    assert [PacketType.from_byte(effect.data[0]).name for effect in effects] == list(
        _EXPECTED_TYPES
    )
    assert PacketType.PUBREL.name not in _EXPECTED_TYPES
    assert all(effect.kind is EffectKind.SEND_PROTOCOL_RESPONSE for effect in effects)


async def test_candidate_benchmark_reaches_idle_transport_inline_and_keeps_fifo() -> None:
    result = await _run_phase(39)

    assert result.immediate_writes == result.count == 39
    assert result.queued_writes == 0
    assert result.marked_effect_types == list(_EXPECTED_TYPES)
    assert result.wire_packet_types == list(_EXPECTED_TYPES)
    assert result.fifo_segment_check is True
