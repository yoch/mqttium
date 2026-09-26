"""A resumed QoS 1 row is acknowledged only after the broker's resend.

The application may acknowledge a replayed QoS 1 message before the broker's
resend of that PUBLISH arrives. A PUBACK sent then precedes the resend, which
the receiver must treat as a new message [MQTT-4.3.2-5] that the broker no
longer counts: the phantom exchange held a Receive Maximum slot (a false
DISCONNECT 0x93) and answered a later message reusing the identifier with the
old payload. Found by formal/models/InboundSession.tla.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import encode_properties
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PublishPacket, encode_frame
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import Message, Properties


def _feed(engine: ProtocolEngine, wire: bytes) -> list:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)
    return engine.take_effects()


def _connack(*, present: bool) -> bytes:
    body = bytearray([0x01 if present else 0x00, 0x00])
    body.extend(encode_properties(None, "CONNACK"))
    return encode_frame(PacketType.CONNACK, 0, body)


def _publish(mid: int, payload: bytes, *, dup: bool = False) -> bytes:
    return PublishPacket(
        topic="race", payload=payload, qos=QoS.AT_LEAST_ONCE, retain=False, dup=dup, mid=mid
    ).encode(MQTTProtocolVersion.MQTTv5)


def _pubacks(effects: list) -> list[int]:
    return [
        int.from_bytes(effect.data[2:4], "big")
        for effect in effects
        if effect.kind is EffectKind.SEND_ACK and effect.data[0] >> 4 == 4
    ]


def _messages(effects: list) -> list[Message]:
    return [effect.data for effect in effects if isinstance(effect.data, Message)]


def _resumed_engine(store: MemoryInflightStore | SqliteInflightStore) -> ProtocolEngine:
    """Deliver mid=1, lose the connection, resume: the row is replayed."""
    engine = ProtocolEngine(
        EngineConfig(
            client_id="resend-race",
            protocol=MQTTProtocolVersion.MQTTv5,
            clean_start=False,
            manual_ack=True,
            max_inbound_inflight=1,
            connect_properties=Properties({"session_expiry_interval": 60}),
        ),
        store,
    )
    engine.begin_connect()
    _feed(engine, _connack(present=False))
    _feed(engine, _publish(1, b"first"))
    engine.notify_transport_closed()
    engine.take_effects()
    engine.begin_connect()
    replayed = _messages(_feed(engine, _connack(present=True)))
    assert engine.state is ConnectionState.CONNECTED
    assert [message.payload for message in replayed] == [b"first"]
    return engine


def _stores(tmp_path: Path) -> list[MemoryInflightStore | SqliteInflightStore]:
    return [MemoryInflightStore(), SqliteInflightStore(tmp_path / "race.db")]


@pytest.mark.parametrize("backend", [0, 1])
def test_puback_of_a_resumed_row_waits_for_the_resend(tmp_path: Path, backend: int) -> None:
    engine = _resumed_engine(_stores(tmp_path)[backend])
    engine.ack(1)
    # The broker has not resent yet: no PUBACK may precede its resend.
    assert _pubacks(engine.take_effects()) == []
    effects = _feed(engine, _publish(1, b"first", dup=True))
    assert _pubacks(effects) == [1]
    assert _messages(effects) == []  # already acknowledged: not delivered again
    assert engine.store.in_count() == 0
    assert engine.inbound._inflight == 0


@pytest.mark.parametrize("backend", [0, 1])
def test_resend_race_leaves_no_phantom_slot(tmp_path: Path, backend: int) -> None:
    engine = _resumed_engine(_stores(tmp_path)[backend])
    engine.ack(1)
    engine.take_effects()
    _feed(engine, _publish(1, b"first", dup=True))
    # The broker received that PUBACK: its quota is free again (Receive
    # Maximum 1), so a new PUBLISH is legal and must be admitted.
    effects = _feed(engine, _publish(2, b"second"))
    assert engine.state is ConnectionState.CONNECTED
    assert [message.payload for message in _messages(effects)] == [b"second"]


@pytest.mark.parametrize("backend", [0, 1])
def test_reused_identifier_after_resend_race_delivers_the_new_message(
    tmp_path: Path, backend: int
) -> None:
    engine = _resumed_engine(_stores(tmp_path)[backend])
    engine.ack(1)
    engine.take_effects()
    _feed(engine, _publish(1, b"first", dup=True))
    # The broker completed mid=1 and reuses it for a new message.
    effects = _feed(engine, _publish(1, b"new"))
    assert [(message.payload, message.dup) for message in _messages(effects)] == [(b"new", False)]
