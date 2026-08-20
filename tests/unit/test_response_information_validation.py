"""Response Information negotiation and unsolicited-property validation (#235)."""

from __future__ import annotations

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import encode_properties
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType
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


def _connack(*, response_information: str | None = "resp/") -> bytes:
    props = Properties()
    if response_information is not None:
        props.set("response_information", response_information)
    body = bytearray([0x00, 0x00])
    body.extend(encode_properties(props, "CONNACK"))
    return encode_frame(PacketType.CONNACK, 0, body)


def _engine(request: int | None) -> tuple[ProtocolEngine, Properties | None]:
    props = None
    if request is not None:
        props = Properties(values={"request_response_information": request})
    return (
        ProtocolEngine(
            EngineConfig(
                client_id="bug235",
                protocol=MQTTProtocolVersion.MQTTv5,
                connect_properties=props,
            )
        ),
        props,
    )


def _assert_rejected(engine: ProtocolEngine, effects: list) -> None:
    assert engine.state is ConnectionState.DISCONNECTED
    assert any(e.kind is EffectKind.PROTOCOL_ERROR for e in effects)
    assert any(e.kind is EffectKind.DISCONNECTED for e in effects)


def test_connack_response_information_rejected_when_request_absent() -> None:
    engine, _ = _engine(None)
    engine.begin_connect()
    _assert_rejected(engine, _feed(engine, _connack()))


def test_connack_response_information_rejected_when_request_zero() -> None:
    engine, _ = _engine(0)
    connect = engine.begin_connect()
    assert b"\x19\x00" in connect
    _assert_rejected(engine, _feed(engine, _connack()))


def test_connack_response_information_accepted_when_request_one() -> None:
    engine, _ = _engine(1)
    engine.begin_connect()
    effects = _feed(engine, _connack())
    assert engine.state is ConnectionState.CONNECTED
    assert not any(e.kind is EffectKind.PROTOCOL_ERROR for e in effects)


def test_connect_snapshot_is_not_changed_by_later_properties_mutation() -> None:
    engine, props = _engine(0)
    assert props is not None
    engine.begin_connect()
    props.set("request_response_information", 1)
    _assert_rejected(engine, _feed(engine, _connack()))
