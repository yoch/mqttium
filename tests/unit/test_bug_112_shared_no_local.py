"""Reproduction for GitHub issue #112.

MQTT 5 forbids No Local=1 on a Shared Subscription ([MQTT-3.8.3-4]).
"""

from __future__ import annotations

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import encode_properties
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import ProtocolError
from mqttium.packets import SubscribeOptions, encode_frame
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.engine import ProtocolEngine


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


@pytest.mark.xfail(
    strict=True,
    reason="https://github.com/yoch/mqttium/issues/112 — shared No Local accepted",
)
def test_shared_subscribe_with_no_local_is_protocol_error() -> None:
    engine = ProtocolEngine(
        EngineConfig(client_id="bug112", protocol=MQTTProtocolVersion.MQTTv5)
    )
    engine.begin_connect()
    body = bytearray([0x00, 0x00])
    body.extend(encode_properties(None, "CONNACK"))
    _feed(engine, encode_frame(PacketType.CONNACK, 0, body))
    engine.take_effects()

    with pytest.raises(ProtocolError, match="(?i)no.?local|shared"):
        engine.queue_subscribe(
            (("$share/g/t", SubscribeOptions(qos=QoS.AT_MOST_ONCE, no_local=True)),)
        )
