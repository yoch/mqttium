"""Structural contracts for the inbound protocol-state owner."""

from __future__ import annotations

import pytest

from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType
from mqttium.packets import _ack, _publish
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.engine import ProtocolEngine
from mqttium.protocol.inbound import InboundSession


@pytest.mark.parametrize(
    ("protocol", "publish_handler"),
    [
        (MQTTProtocolVersion.MQTTv31, InboundSession._on_publish_v31),
        (MQTTProtocolVersion.MQTTv311, InboundSession._on_publish_v311),
        (MQTTProtocolVersion.MQTTv5, InboundSession._on_publish_v5),
    ],
)
def test_publish_and_pubrel_dispatch_directly_to_inbound_session(
    protocol: MQTTProtocolVersion,
    publish_handler,
) -> None:
    engine = ProtocolEngine(EngineConfig(protocol=protocol))
    connected_handlers = engine._handlers_by_state[ConnectionState.CONNECTED]

    publish = connected_handlers[PacketType.PUBLISH]
    pubrel = connected_handlers[PacketType.PUBREL]

    assert publish.__self__ is engine.inbound
    assert publish.__func__ is publish_handler
    assert pubrel.__self__ is engine.inbound
    assert pubrel.__func__ is InboundSession.on_pubrel


def test_legacy_inbound_diagnostics_are_views_not_duplicate_state() -> None:
    engine = ProtocolEngine()

    assert engine._topic_aliases is engine.inbound._aliases
    assert engine._recovered_inbound_mids is engine.inbound._recovered_mids

    engine._inbound_inflight = 3
    assert engine.inbound._inflight == 3
    assert engine._inbound_inflight == 3


@pytest.mark.parametrize(
    ("protocol", "decode_puback", "decode_pubrel", "publish_encoder"),
    [
        (
            MQTTProtocolVersion.MQTTv311,
            _ack.decode_puback_v311,
            _ack.decode_pubrel_v311,
            _publish.encode_publish_item_v311,
        ),
        (
            MQTTProtocolVersion.MQTTv5,
            _ack.decode_puback_v5,
            _ack.decode_pubrel_v5,
            _publish.encode_publish_item_v5,
        ),
    ],
)
def test_hot_codec_functions_are_bound_once(
    protocol: MQTTProtocolVersion,
    decode_puback,
    decode_pubrel,
    publish_encoder,
) -> None:
    engine = ProtocolEngine(EngineConfig(protocol=protocol))

    assert engine.outbound._decode_puback is decode_puback
    assert engine.inbound._decode_pubrel is decode_pubrel
    assert engine.outbound._encode_publish is publish_encoder
