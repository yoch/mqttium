"""Enhanced authentication method/data sequencing (#231).

Covers #231, #233, #241 and #244 as one state machine: initial enhanced
authentication, idle connected state, and client-initiated re-authentication.
"""

from __future__ import annotations

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import encode_properties
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType
from mqttium.errors import ProtocolError
from mqttium.packets import encode_frame
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import Properties


def _feed(engine: ProtocolEngine, wire: bytes) -> list:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)
    return engine.take_effects()


def _engine() -> ProtocolEngine:
    return ProtocolEngine(
        EngineConfig(
            client_id="auth-regression",
            protocol=MQTTProtocolVersion.MQTTv5,
            accept_auth=True,
            connect_properties=Properties(values={"authentication_method": "demo"}),
        )
    )


def _connack(method: str | None = "demo") -> bytes:
    props = None if method is None else Properties(values={"authentication_method": method})
    body = bytearray([0x00, 0x00])
    body.extend(encode_properties(props, "CONNACK"))
    return encode_frame(PacketType.CONNACK, 0, body)


def _auth(reason: int, method: str | None = "demo") -> bytes:
    props = None if method is None else Properties(values={"authentication_method": method})
    body = bytearray([reason])
    body.extend(encode_properties(props, "AUTH"))
    return encode_frame(PacketType.AUTH, 0, body)


def _connect(engine: ProtocolEngine) -> None:
    engine.begin_connect()
    effects = _feed(engine, _connack())
    assert engine.state is ConnectionState.CONNECTED
    assert not any(e.kind is EffectKind.PROTOCOL_ERROR for e in effects)


def _assert_protocol_disconnect(engine: ProtocolEngine, effects: list) -> None:
    assert engine.state is ConnectionState.DISCONNECTED
    assert not any(e.kind is EffectKind.AUTH for e in effects)
    assert any(e.kind is EffectKind.PROTOCOL_ERROR for e in effects)
    assert any(e.kind is EffectKind.DISCONNECTED for e in effects)


def test_successful_connack_requires_authentication_method() -> None:
    engine = _engine()
    engine.begin_connect()

    effects = _feed(engine, _connack(method=None))

    _assert_protocol_disconnect(engine, effects)


def test_initial_auth_requires_authentication_method() -> None:
    engine = _engine()
    engine.begin_connect()

    effects = _feed(engine, _auth(0x18, method=None))

    _assert_protocol_disconnect(engine, effects)


def test_auth_success_is_invalid_during_initial_authentication() -> None:
    engine = _engine()
    engine.begin_connect()

    effects = _feed(engine, _auth(0x00))

    _assert_protocol_disconnect(engine, effects)


@pytest.mark.parametrize("reason_code", [0x00, 0x18])
def test_server_auth_is_invalid_while_connected_auth_is_idle(reason_code: int) -> None:
    engine = _engine()
    _connect(engine)

    effects = _feed(engine, _auth(reason_code))

    _assert_protocol_disconnect(engine, effects)


def test_client_initiated_reauthentication_roundtrip() -> None:
    engine = _engine()
    _connect(engine)

    engine.queue_auth(reason_code=0x19)
    start_effects = engine.take_effects()
    assert any(e.kind is EffectKind.SEND for e in start_effects)

    challenge_effects = _feed(engine, _auth(0x18))
    assert engine.state is ConnectionState.CONNECTED
    assert any(e.kind is EffectKind.AUTH for e in challenge_effects)
    assert not any(e.kind is EffectKind.PROTOCOL_ERROR for e in challenge_effects)

    engine.queue_auth(reason_code=0x18)
    continue_effects = engine.take_effects()
    assert any(e.kind is EffectKind.SEND for e in continue_effects)

    success_effects = _feed(engine, _auth(0x00))
    assert engine.state is ConnectionState.CONNECTED
    assert any(e.kind is EffectKind.AUTH for e in success_effects)
    assert not any(e.kind is EffectKind.PROTOCOL_ERROR for e in success_effects)

    with pytest.raises(ProtocolError, match="active re-authentication"):
        engine.queue_auth(reason_code=0x18)


def test_duplicate_client_reauthenticate_is_rejected_locally() -> None:
    engine = _engine()
    _connect(engine)
    engine.queue_auth(reason_code=0x19)
    engine.take_effects()

    with pytest.raises(ProtocolError, match="already in progress"):
        engine.queue_auth(reason_code=0x19)
