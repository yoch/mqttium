"""Safety and accounting tests for decoded MQTT 5 small-delivery sizing."""

from __future__ import annotations

import pytest

import mqttium.api._delivery as delivery_module
from mqttium.api.async_client import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import PUBLISH, encode_properties
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PublishPacket, encode_frame
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import Message, Properties


def _iot_properties() -> Properties:
    properties = Properties()
    properties.set("content_type", "application/octet-stream")
    properties.set("payload_format_indicator", 1)
    properties.set("message_expiry_interval", 60)
    properties.add_user_property("device", "probe")
    properties.add_user_property("site", "lab")
    return properties


def _connected_engine() -> ProtocolEngine:
    engine = ProtocolEngine(
        EngineConfig(protocol=MQTTProtocolVersion.MQTTv5, client_id="probe")
    )
    engine.begin_connect()
    decoder = IncrementalDecoder()
    decoder.feed(encode_frame(PacketType.CONNACK, 0, b"\x00\x00\x00"))
    connack = decoder.next_packet()
    assert connack is not None
    engine.handle_raw(connack)
    engine.take_effects()
    return engine


def _fresh_effect(
    properties: Properties | None,
    *,
    qos: QoS = QoS.AT_MOST_ONCE,
    mid: int | None = None,
    payload: bytes = b"hello",
) -> tuple[ProtocolEngine, EngineEffect]:
    engine = _connected_engine()
    packet = PublishPacket(
        topic="sensors/t",
        payload=payload,
        qos=qos,
        retain=False,
        dup=False,
        mid=mid,
        properties=properties,
    )
    decoder = IncrementalDecoder()
    decoder.feed(packet.encode(MQTTProtocolVersion.MQTTv5))
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)
    effects = [effect for effect in engine.take_effects() if effect.kind is EffectKind.MESSAGE]
    assert len(effects) == 1
    return engine, effects[0]


@pytest.mark.parametrize("mode", ["iterator", "callback", "both", "auto"])
async def test_small_decoded_properties_use_small_reserve(mode: str) -> None:
    _engine, effect = _fresh_effect(_iot_properties())
    message = effect.data
    assert isinstance(message, Message)
    assert message.properties is not None
    assert effect.decoded_property_wire_size == len(
        encode_properties(message.properties, PUBLISH)
    )

    client = AsyncClient(
        message_delivery=mode,  # type: ignore[arg-type]
        protocol=MQTTProtocolVersion.MQTTv5,
        client_id="probe",
    )
    seen: list[Message] = []
    if mode in ("callback", "both"):
        client.on_message = seen.append

    await client._apply_effect(effect, nowait=False)

    assert client.pending_delivery_bytes == 0
    if mode in ("iterator", "both", "auto"):
        assert client._messages.get_nowait() is message
    if mode in ("callback", "both"):
        await client._callback_queue.join()
        assert seen == [message]
        await client._shutdown_callback_worker(drain=False)


async def test_small_decoded_properties_do_not_reencode(monkeypatch) -> None:
    _engine, effect = _fresh_effect(_iot_properties())

    def fail_encode(*_args: object, **_kwargs: object) -> bytes:
        raise AssertionError("delivery must use the trusted decode-time size")

    monkeypatch.setattr(delivery_module, "encode_properties", fail_encode)
    client = AsyncClient(
        message_delivery="iterator",
        protocol=MQTTProtocolVersion.MQTTv5,
        client_id="probe",
    )

    await client._apply_effect(effect, nowait=False)

    assert client.pending_delivery_bytes == 0


async def test_oversized_decoded_properties_remain_accounted() -> None:
    properties = Properties()
    properties.set("correlation_data", b"x" * 200)
    _engine, effect = _fresh_effect(properties)
    client = AsyncClient(
        message_delivery="iterator",
        protocol=MQTTProtocolVersion.MQTTv5,
        client_id="probe",
    )

    await client._apply_effect(effect, nowait=False)

    item = client._messages.get_nowait()
    assert isinstance(item, tuple)
    assert client.pending_delivery_bytes > 0


async def test_application_built_properties_remain_accounted() -> None:
    message = Message(topic="sensors/t", payload=b"hello", properties=_iot_properties())
    effect = EngineEffect(EffectKind.MESSAGE, message)
    assert effect.decoded_property_wire_size is None
    client = AsyncClient(
        message_delivery="iterator",
        protocol=MQTTProtocolVersion.MQTTv5,
        client_id="probe",
    )

    await client._apply_effect(effect, nowait=False)

    item = client._messages.get_nowait()
    assert isinstance(item, tuple)
    assert client.pending_delivery_bytes > 0


def test_empty_property_table_keeps_normal_effect_path() -> None:
    _engine, effect = _fresh_effect(None)
    assert effect.decoded_property_wire_size is None


async def test_replay_drops_hint_after_direct_properties_mutation() -> None:
    engine, first = _fresh_effect(_iot_properties(), qos=QoS.EXACTLY_ONCE, mid=7)
    message = first.data
    assert isinstance(message, Message)
    assert message.properties is not None
    assert first.decoded_property_wire_size is not None

    # MemoryInflightStore retains the same Properties object. A later replay
    # must not reuse decode metadata after arbitrary direct mutation.
    message.properties.values["correlation_data"] = b"x" * 200

    engine.inbound.replay_session()
    replay = [effect for effect in engine.take_effects() if effect.kind is EffectKind.MESSAGE]
    assert len(replay) == 1
    replay_effect = replay[0]
    assert replay_effect.decoded_property_wire_size is None

    client = AsyncClient(
        message_delivery="iterator",
        protocol=MQTTProtocolVersion.MQTTv5,
        client_id="probe",
    )
    await client._apply_effect(replay_effect, nowait=False)
    item = client._messages.get_nowait()
    assert isinstance(item, tuple)
    assert client.pending_delivery_bytes > 0
