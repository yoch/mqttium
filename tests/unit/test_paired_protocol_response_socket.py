"""Contracts for the real-socket protocol-response benchmark corpus."""

from __future__ import annotations

from benchmarks.paired_protocol_response_socket import _cv, _publish


def test_qos1_probe_publish_has_its_packet_id_before_the_payload() -> None:
    wire = _publish("probe/inbound", b"payload", qos=1, mid=0x1234)

    assert wire[0] == 0x32
    assert wire[1] == len(wire) - 2
    assert wire[2:4] == len(b"probe/inbound").to_bytes(2, "big")
    assert wire[4:17] == b"probe/inbound"
    assert wire[17:19] == b"\x12\x34"
    assert wire[19:] == b"payload"


def test_probe_variability_is_zero_for_identical_samples() -> None:
    assert _cv([10.0, 10.0, 10.0]) == 0.0
