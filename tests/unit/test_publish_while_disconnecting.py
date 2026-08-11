"""Regression coverage for publication admission during disconnect."""

from __future__ import annotations

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType
from mqttium.errors import NotConnectedError
from mqttium.packets import encode_frame
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.engine import ProtocolEngine


def _connected_engine() -> tuple[ProtocolEngine, MemoryInflightStore]:
    store = MemoryInflightStore()
    engine = ProtocolEngine(
        EngineConfig(
            client_id="disconnect-publish",
            protocol=MQTTProtocolVersion.MQTTv5,
            clean_start=False,
        ),
        store,
    )
    engine.begin_connect()
    decoder = IncrementalDecoder()
    decoder.feed(encode_frame(PacketType.CONNACK, 0, b"\x00\x00\x00"))
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)
    engine.take_effects()
    assert engine.state is ConnectionState.CONNECTED
    return engine, store


def test_qos1_publish_is_rejected_after_disconnect_begins() -> None:
    engine, store = _connected_engine()
    engine.begin_disconnect()

    with pytest.raises(NotConnectedError, match="disconnecting"):
        engine.queue_publish("late/qos1", b"x", qos=1)

    assert engine.state is ConnectionState.DISCONNECTING
    assert tuple(store.out_items()) == ()
    assert engine.pending_outbound_messages == 0
    assert len(engine.packet_ids) == 0


def test_publish_many_is_rejected_after_disconnect_begins() -> None:
    engine, store = _connected_engine()
    engine.begin_disconnect()

    with pytest.raises(NotConnectedError, match="disconnecting"):
        engine.queue_publish_many([("late/qos1", b"x", 1, False, None)])

    assert tuple(store.out_items()) == ()
    assert engine.pending_outbound_messages == 0
    assert len(engine.packet_ids) == 0
