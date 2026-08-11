"""Reproduction for GitHub issue #88.

MQTT 5 Response Topic must not contain wildcard characters
(``[MQTT-3.3.2-14]``). Both outbound publish and inbound delivery currently
accept ``+`` / ``#``.
"""

from __future__ import annotations

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import ProtocolError
from mqttium.packets import PublishPacket, encode_frame
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import Properties


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def _connect(engine: ProtocolEngine) -> None:
    engine.begin_connect()
    _feed(engine, encode_frame(PacketType.CONNACK, 0, b"\x00\x00\x00"))
    engine.take_effects()


@pytest.mark.xfail(
    strict=True,
    reason="https://github.com/yoch/mqttium/issues/88 — response_topic wildcards not rejected",
)
def test_outbound_response_topic_rejects_wildcards() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="bug88", protocol=MQTTProtocolVersion.MQTTv5))
    _connect(engine)
    props = Properties()
    props.set("response_topic", "reply/#")
    with pytest.raises(ProtocolError, match="(?i)response.?topic|wildcard"):
        engine.queue_publish("request", b"x", qos=0, properties=props)


@pytest.mark.xfail(
    strict=True,
    reason="https://github.com/yoch/mqttium/issues/88 — response_topic wildcards not rejected",
)
def test_inbound_response_topic_with_wildcard_is_protocol_error() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="bug88", protocol=MQTTProtocolVersion.MQTTv5))
    _connect(engine)
    props = Properties()
    props.set("response_topic", "reply/+")
    _feed(
        engine,
        PublishPacket(
            topic="req",
            payload=b"y",
            qos=QoS.AT_MOST_ONCE,
            retain=False,
            dup=False,
            properties=props,
        ).encode(MQTTProtocolVersion.MQTTv5),
    )
    effects = engine.take_effects()
    assert not any(e.kind is EffectKind.MESSAGE for e in effects)
    assert engine.state is ConnectionState.DISCONNECTED or any(
        e.kind is EffectKind.PROTOCOL_ERROR for e in effects
    )
