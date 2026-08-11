"""Reproduction for GitHub issue #131."""

from __future__ import annotations

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import encode_properties
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import PacketTooLargeError
from mqttium.packets import PublishPacket, encode_frame
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import Properties


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def _connect_with_mps(engine: ProtocolEngine, maximum_packet_size: int) -> None:
    engine.begin_connect()
    props = Properties()
    props.set("maximum_packet_size", maximum_packet_size)
    body = bytearray([0x00, 0x00])
    body.extend(encode_properties(props, "CONNACK"))
    _feed(engine, encode_frame(PacketType.CONNACK, 0, body))
    engine.take_effects()


@pytest.mark.xfail(
    strict=True,
    reason="https://github.com/yoch/mqttium/issues/131 — PINGREQ ignores maximum_packet_size",
)
def test_queue_ping_enforces_maximum_packet_size() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="bug131", protocol=MQTTProtocolVersion.MQTTv5))
    _connect_with_mps(engine, 1)
    with pytest.raises(PacketTooLargeError):
        engine.queue_ping()


@pytest.mark.xfail(
    strict=True,
    reason="https://github.com/yoch/mqttium/issues/131 — PUBACK ignores maximum_packet_size",
)
def test_autoack_puback_enforces_maximum_packet_size() -> None:
    engine = ProtocolEngine(
        EngineConfig(client_id="bug131", protocol=MQTTProtocolVersion.MQTTv5, manual_ack=False)
    )
    _connect_with_mps(engine, 3)
    with pytest.raises(PacketTooLargeError):
        _feed(
            engine,
            PublishPacket(
                topic="t",
                payload=b"p",
                qos=QoS.AT_LEAST_ONCE,
                retain=False,
                dup=False,
                mid=7,
            ).encode(MQTTProtocolVersion.MQTTv5),
        )
