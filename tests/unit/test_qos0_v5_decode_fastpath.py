from __future__ import annotations

import pytest

import mqttium.protocol.inbound as inbound_module
from mqttium.codec.buffer import RawPacket
from mqttium.codec.primitives import pack_u16, pack_utf8
from mqttium.codec.properties import PUBLISH, encode_properties
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import MalformedPacketError
from mqttium.packets import PublishPacket
from mqttium.packets._publish import decode_qos0_message_v5
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from mqttium.protocol.inbound import InboundSession
from mqttium.types import Message, Properties


def _connected(*, alias_maximum: int = 0) -> ProtocolEngine:
    engine = ProtocolEngine(
        EngineConfig(
            protocol=MQTTProtocolVersion.MQTTv5,
            client_id="probe",
            topic_alias_maximum=alias_maximum,
        )
    )
    engine.state = ConnectionState.CONNECTED
    return engine


def _raw(
    topic: str,
    payload: bytes,
    properties: Properties | None = None,
    *,
    flags: int = 0,
    mid: int | None = None,
) -> RawPacket:
    table = encode_properties(properties, PUBLISH) if properties is not None else b"\x00"
    body = pack_utf8(topic)
    if mid is not None:
        body += pack_u16(mid)
    return RawPacket(PacketType.PUBLISH, flags, body + table + payload)


def _message_effect(effects: list[EngineEffect]) -> EngineEffect:
    messages = [
        effect
        for effect in effects
        if effect.kind in (EffectKind.MESSAGE, EffectKind.DECODED_MESSAGE)
    ]
    assert len(messages) == 1
    return messages[0]


@pytest.mark.parametrize(
    ("topic", "payload", "retain"),
    [
        ("bench/telemetry", b"payload", False),
        ("capteur/température", b"", True),
        ("a/b/c", b"x" * 4096, False),
    ],
)
def test_v5_qos0_direct_decode_matches_generic_packet(
    topic: str,
    payload: bytes,
    retain: bool,
) -> None:
    raw = _raw(topic, payload, flags=0x01 if retain else 0)
    message, property_wire_size = decode_qos0_message_v5(raw)
    packet = PublishPacket.decode(raw.flags, raw.remaining, MQTTProtocolVersion.MQTTv5)

    assert message.topic == packet.topic == topic
    assert message.payload == packet.payload == payload
    assert message.qos is packet.qos is QoS.AT_MOST_ONCE
    assert message.retain is packet.retain is retain
    assert message.dup is packet.dup is False
    assert message.mid is packet.mid is None
    assert message.properties is not None
    assert packet.properties is not None
    assert message.properties.values == packet.properties.values == {}
    assert property_wire_size == 1


@pytest.mark.parametrize("size", [1, 200])
def test_v5_qos0_direct_decode_preserves_properties_and_exact_wire_size(size: int) -> None:
    properties = Properties()
    properties.set("correlation_data", b"x" * size)
    table = encode_properties(properties, PUBLISH)
    raw = _raw("bench/v5/props", b"body", properties)

    message, property_wire_size = decode_qos0_message_v5(raw)
    packet = PublishPacket.decode(raw.flags, raw.remaining, MQTTProtocolVersion.MQTTv5)

    assert message.properties is not None
    assert packet.properties is not None
    assert message.properties.values == packet.properties.values
    assert property_wire_size == len(table)
    assert message.payload == b"body"


def test_v5_qos0_engine_skips_generic_decoder_and_preserves_size_handoff(monkeypatch) -> None:
    calls = 0
    original = inbound_module.decode_publish_fields_v5

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(inbound_module, "decode_publish_fields_v5", counted)
    properties = Properties()
    properties.set("content_type", "text/plain")
    table = encode_properties(properties, PUBLISH)
    engine = _connected()
    engine.handle_raw(_raw("bench/direct", b"payload", properties))

    effect = _message_effect(engine.take_effects())
    assert effect.kind is EffectKind.DECODED_MESSAGE
    assert effect.decoded_property_wire_size == len(table)
    assert isinstance(effect.data, Message)
    assert effect.data.topic == "bench/direct"
    assert effect.data.payload == b"payload"
    assert calls == 0


def test_v5_qos0_empty_table_keeps_generic_message_effect() -> None:
    engine = _connected()
    engine.handle_raw(_raw("bench/empty", b"payload"))
    effect = _message_effect(engine.take_effects())

    assert effect.kind is EffectKind.MESSAGE
    assert effect.decoded_property_wire_size is None
    assert effect.data.properties is not None
    assert not effect.data.properties


def test_v5_qos0_alias_free_topic_skips_resolution(monkeypatch) -> None:
    calls = 0
    original = InboundSession._resolve_topic_fields

    def counted(self, topic, props):
        nonlocal calls
        calls += 1
        return original(self, topic, props)

    monkeypatch.setattr(InboundSession, "_resolve_topic_fields", counted)
    engine = _connected()
    engine.handle_raw(_raw("bench/no-alias", b"x"))
    effect = _message_effect(engine.take_effects())

    assert effect.kind is EffectKind.MESSAGE
    assert effect.data.topic == "bench/no-alias"
    assert calls == 0


def test_v5_qos0_non_alias_properties_skip_resolution_and_keep_hint(monkeypatch) -> None:
    calls = 0
    original = InboundSession._resolve_topic_fields

    def counted(self, topic, props):
        nonlocal calls
        calls += 1
        return original(self, topic, props)

    monkeypatch.setattr(InboundSession, "_resolve_topic_fields", counted)
    properties = Properties()
    properties.set("content_type", "text/plain")
    table = encode_properties(properties, PUBLISH)
    engine = _connected()
    engine.handle_raw(_raw("bench/typed", b"x", properties))
    effect = _message_effect(engine.take_effects())

    assert calls == 0
    assert effect.kind is EffectKind.DECODED_MESSAGE
    assert effect.decoded_property_wire_size == len(table)


@pytest.mark.parametrize(
    ("qos", "flags"),
    [(QoS.AT_LEAST_ONCE, 0x02), (QoS.EXACTLY_ONCE, 0x04)],
)
def test_v5_qos12_alias_free_topic_skips_resolution_but_keeps_generic_decoder(
    monkeypatch, qos: QoS, flags: int
) -> None:
    field_calls = 0
    resolve_calls = 0
    original_fields = inbound_module.decode_publish_fields_v5
    original_resolve = InboundSession._resolve_topic_fields

    def counted_fields(*args, **kwargs):
        nonlocal field_calls
        field_calls += 1
        return original_fields(*args, **kwargs)

    def counted_resolve(self, topic, props):
        nonlocal resolve_calls
        resolve_calls += 1
        return original_resolve(self, topic, props)

    monkeypatch.setattr(inbound_module, "decode_publish_fields_v5", counted_fields)
    monkeypatch.setattr(InboundSession, "_resolve_topic_fields", counted_resolve)
    engine = _connected()
    engine.handle_raw(_raw("bench/qos12", b"payload", flags=flags, mid=9))
    effect = _message_effect(engine.take_effects())

    assert effect.data.qos is qos
    assert field_calls == 1
    assert resolve_calls == 0


@pytest.mark.parametrize(
    ("flags", "remaining", "match"),
    [
        (0x08, pack_utf8("bench/dup") + b"\x00x", "must not set DUP"),
        (0, pack_utf8("bench/+") + b"\x00x", "wildcards"),
    ],
)
def test_v5_qos0_direct_decode_preserves_validation(
    flags: int, remaining: bytes, match: str
) -> None:
    with pytest.raises(MalformedPacketError, match=match):
        decode_qos0_message_v5(RawPacket(PacketType.PUBLISH, flags, remaining))


def test_v5_qos0_empty_topic_without_alias_is_protocol_error() -> None:
    engine = _connected()
    engine.handle_raw(_raw("", b"x"))
    effects = engine.take_effects()

    assert any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)
    assert not any(
        effect.kind in (EffectKind.MESSAGE, EffectKind.DECODED_MESSAGE) for effect in effects
    )


def test_v5_qos0_topic_alias_establish_and_reuse_preserve_hint() -> None:
    engine = _connected(alias_maximum=10)
    properties = Properties()
    properties.set("topic_alias", 1)
    table = encode_properties(properties, PUBLISH)

    engine.handle_raw(_raw("sensors/temp", b"first", properties))
    first = _message_effect(engine.take_effects())
    assert engine.inbound._aliases == {1: "sensors/temp"}
    assert first.kind is EffectKind.DECODED_MESSAGE
    assert first.decoded_property_wire_size == len(table)

    engine.handle_raw(_raw("", b"second", properties))
    second = _message_effect(engine.take_effects())
    assert second.data.topic == "sensors/temp"
    assert second.data.payload == b"second"
    assert second.kind is EffectKind.DECODED_MESSAGE
    assert second.decoded_property_wire_size == len(table)


@pytest.mark.parametrize(("alias", "maximum"), [(2, 1)])
def test_v5_qos0_invalid_topic_alias_never_delivers(alias: int, maximum: int) -> None:
    properties = Properties()
    properties.set("topic_alias", alias)
    engine = _connected(alias_maximum=maximum)
    engine.handle_raw(_raw("sensors/temp", b"x", properties))
    effects = engine.take_effects()

    assert any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)
    assert not any(
        effect.kind in (EffectKind.MESSAGE, EffectKind.DECODED_MESSAGE) for effect in effects
    )


def test_v5_qos0_zero_topic_alias_on_wire_never_delivers() -> None:
    # Topic Alias is property id 0x23 with a two-byte integer value. The
    # outbound encoder correctly refuses zero, so construct the malformed peer
    # packet directly to exercise inbound validation.
    remaining = pack_utf8("sensors/temp") + b"\x03\x23\x00\x00" + b"x"
    engine = _connected(alias_maximum=10)
    engine.handle_raw(RawPacket(PacketType.PUBLISH, 0, remaining))
    effects = engine.take_effects()

    assert any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)
    assert not any(
        effect.kind in (EffectKind.MESSAGE, EffectKind.DECODED_MESSAGE) for effect in effects
    )
