"""Reproduction for GitHub issue #126."""

from __future__ import annotations

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import encode_properties
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PublishPacket, encode_frame
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


@pytest.mark.xfail(
    strict=True,
    reason="https://github.com/yoch/mqttium/issues/126 — auto-ACK bypasses Receive Maximum",
)
def test_autoack_qos1_respects_local_receive_maximum() -> None:
    engine = ProtocolEngine(
        EngineConfig(
            client_id="bug126",
            protocol=MQTTProtocolVersion.MQTTv5,
            manual_ack=False,
            local_receive_maximum=1,
        )
    )
    engine.begin_connect()
    body = bytearray([0x00, 0x00])
    body.extend(encode_properties(None, "CONNACK"))
    _feed(engine, encode_frame(PacketType.CONNACK, 0, body))
    engine.take_effects()

    for mid in (1, 2):
        _feed(
            engine,
            PublishPacket(
                topic="t",
                payload=bytes([mid]),
                qos=QoS.AT_LEAST_ONCE,
                retain=False,
                dup=False,
                mid=mid,
            ).encode(MQTTProtocolVersion.MQTTv5),
        )
    effects = engine.take_effects()
    messages = [e.data.payload for e in effects if e.kind is EffectKind.MESSAGE]
    assert messages == [bytes([1])]
    assert any(e.kind is EffectKind.DISCONNECTED for e in effects)
    assert engine.state is ConnectionState.DISCONNECTED
