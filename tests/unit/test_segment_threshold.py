"""Regression coverage for the PUBLISH segmentation boundary."""

from __future__ import annotations

from mqttium.enums import MQTTProtocolVersion, QoS
from mqttium.packets.publish import encode_publish_item
from mqttium.transport.writes import SEGMENT_THRESHOLD


def _encode(payload: bytes):
    return encode_publish_item(
        "segment/topic",
        payload,
        qos=QoS.AT_MOST_ONCE,
        retain=False,
        dup=False,
        mid=None,
        properties=None,
        protocol=MQTTProtocolVersion.MQTTv311,
    )


def test_segment_threshold_is_128_kib() -> None:
    assert SEGMENT_THRESHOLD == 128 * 1024


def test_publish_below_threshold_stays_contiguous() -> None:
    item = _encode(b"x" * (SEGMENT_THRESHOLD - 1))

    assert isinstance(item, bytes)


def test_publish_at_threshold_is_segmented_without_payload_copy() -> None:
    payload = b"x" * SEGMENT_THRESHOLD
    item = _encode(payload)

    assert isinstance(item, tuple)
    assert item[1] is payload
