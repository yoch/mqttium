"""Reproduction for GitHub issue #113.

SUBACK/UNSUBACK reason-code count must match the number of topic filters in
the corresponding request (MQTT 5 §3.9 / §3.11).
"""

from __future__ import annotations

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import encode_properties
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import encode_frame
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def _connect(engine: ProtocolEngine) -> None:
    engine.begin_connect()
    body = bytearray([0x00, 0x00])
    body.extend(encode_properties(None, "CONNACK"))
    _feed(engine, encode_frame(PacketType.CONNACK, 0, body))
    engine.take_effects()


def _suback(mid: int, reasons: list[int]) -> bytes:
    body = bytes([(mid >> 8) & 0xFF, mid & 0xFF]) + b"\x00" + bytes(reasons)
    return encode_frame(PacketType.SUBACK, 0, body)


def _unsuback(mid: int, reasons: list[int]) -> bytes:
    body = bytes([(mid >> 8) & 0xFF, mid & 0xFF]) + b"\x00" + bytes(reasons)
    return encode_frame(PacketType.UNSUBACK, 0, body)


def _assert_refused(engine: ProtocolEngine, effects: list) -> None:
    assert not any(e.kind in (EffectKind.SUBACK, EffectKind.UNSUBACK) for e in effects)
    assert engine.state is ConnectionState.DISCONNECTED or any(
        e.kind is EffectKind.PROTOCOL_ERROR for e in effects
    )


@pytest.mark.xfail(
    strict=True,
    reason="https://github.com/yoch/mqttium/issues/113 — SUBACK length not correlated",
)
def test_suback_with_too_many_reason_codes_is_protocol_error() -> None:
    engine = ProtocolEngine(
        EngineConfig(client_id="bug113", protocol=MQTTProtocolVersion.MQTTv5)
    )
    _connect(engine)
    mid = engine.queue_subscribe("one/topic")
    engine.take_effects()
    _feed(engine, _suback(mid, [0, 1]))
    _assert_refused(engine, engine.take_effects())


@pytest.mark.xfail(
    strict=True,
    reason="https://github.com/yoch/mqttium/issues/113 — SUBACK length not correlated",
)
def test_suback_with_too_few_reason_codes_is_protocol_error() -> None:
    engine = ProtocolEngine(
        EngineConfig(client_id="bug113", protocol=MQTTProtocolVersion.MQTTv5)
    )
    _connect(engine)
    mid = engine.queue_subscribe([("a", QoS.AT_MOST_ONCE), ("b", QoS.AT_MOST_ONCE)])
    engine.take_effects()
    _feed(engine, _suback(mid, [0]))
    _assert_refused(engine, engine.take_effects())


@pytest.mark.xfail(
    strict=True,
    reason="https://github.com/yoch/mqttium/issues/113 — UNSUBACK length not correlated",
)
def test_unsuback_with_too_many_reason_codes_is_protocol_error() -> None:
    engine = ProtocolEngine(
        EngineConfig(client_id="bug113", protocol=MQTTProtocolVersion.MQTTv5)
    )
    _connect(engine)
    mid = engine.queue_unsubscribe("one/topic")
    engine.take_effects()
    _feed(engine, _unsuback(mid, [0, 0x11]))
    _assert_refused(engine, engine.take_effects())
