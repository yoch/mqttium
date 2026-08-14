"""Decoded MQTT 5 property sizes may use the count-bounded small reserve."""

from __future__ import annotations

from collections import deque

import pytest

import mqttium.api._delivery as delivery_module
from mqttium.api._delivery import _fits_small_limit
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
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv5, client_id="probe"))
    engine.begin_connect()
    decoder = IncrementalDecoder()
    decoder.feed(encode_frame(PacketType.CONNACK, 0, b"\x00\x00\x00"))
    connack = decoder.next_packet()
    assert connack is not None
    engine.handle_raw(connack)
    engine.take_effects()
    return engine


def _fresh_message_effect(
    properties: Properties | None,
    *,
    qos: QoS = QoS.AT_MOST_ONCE,
    payload: bytes = b"hello",
    mid: int | None = None,
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
    effects = engine.take_effects()
    message_effects = [effect for effect in effects if effect.kind is EffectKind.MESSAGE]
    assert len(message_effects) == 1
    return engine, message_effects[0]


@pytest.mark.parametrize("mode", ["iterator", "callback", "both"])
async def test_decoded_iot_properties_use_small_delivery_in_all_modes(mode: str) -> None:
    _engine, effect = _fresh_message_effect(_iot_properties())
    message = effect.data
    assert isinstance(message, Message)
    assert message.properties is not None
    assert effect.decoded_property_wire_size == len(encode_properties(message.properties, PUBLISH))

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
    if mode in ("iterator", "both"):
        assert client._messages.get_nowait() is message
    if mode in ("callback", "both"):
        await client._callback_queue.join()
        assert seen == [message]
        await client._shutdown_callback_worker(drain=False)


async def test_decoded_small_properties_do_not_reencode_for_delivery(monkeypatch) -> None:
    _engine, effect = _fresh_message_effect(_iot_properties())

    def fail_encode(*_args: object, **_kwargs: object) -> bytes:
        raise AssertionError("small decoded delivery must not encode properties")

    monkeypatch.setattr(delivery_module, "encode_properties", fail_encode)
    client = AsyncClient(
        message_delivery="iterator",
        protocol=MQTTProtocolVersion.MQTTv5,
        client_id="probe",
    )

    await client._apply_effect(effect, nowait=False)

    assert client.pending_delivery_bytes == 0


async def test_application_built_properties_stay_accounted() -> None:
    client = AsyncClient(
        message_delivery="iterator",
        protocol=MQTTProtocolVersion.MQTTv5,
        client_id="probe",
    )
    message = Message(topic="sensors/t", payload=b"hello", properties=_iot_properties())
    effect = EngineEffect(EffectKind.MESSAGE, message)
    assert effect.decoded_property_wire_size is None

    await client._apply_effect(effect, nowait=False)

    item = client._messages.get_nowait()
    assert isinstance(item, tuple)
    assert item[0] is message
    assert client.pending_delivery_bytes > 0


async def test_decoded_oversized_properties_stay_accounted(monkeypatch) -> None:
    properties = Properties()
    properties.set("correlation_data", b"x" * 200)
    _engine, effect = _fresh_message_effect(properties)
    assert effect.decoded_property_wire_size is not None

    def fail_encode(*_args: object, **_kwargs: object) -> bytes:
        raise AssertionError("fresh decoded accounted delivery must reuse the known table size")

    monkeypatch.setattr(delivery_module, "encode_properties", fail_encode)
    client = AsyncClient(
        message_delivery="iterator",
        protocol=MQTTProtocolVersion.MQTTv5,
        client_id="probe",
    )
    await client._apply_effect(effect, nowait=False)

    item = client._messages.get_nowait()
    assert isinstance(item, tuple)
    assert client.pending_delivery_bytes > 0


def test_inbound_logical_size_reuses_fresh_decoded_table(monkeypatch) -> None:
    engine, effect = _fresh_message_effect(_iot_properties())
    message = effect.data
    assert isinstance(message, Message)
    assert message.properties is not None
    property_wire_size = effect.decoded_property_wire_size
    assert property_wire_size is not None

    def fail_encode(*_args: object, **_kwargs: object) -> bytes:
        raise AssertionError("fresh inbound sizing must reuse the decoded table size")

    monkeypatch.setattr("mqttium.protocol.inbound.encode_properties", fail_encode)
    size = engine.inbound.logical_size(
        message.topic, message.payload, message.properties, property_wire_size
    )
    assert size == len(message.payload) + len(message.topic) + property_wire_size


def test_empty_decoded_property_table_keeps_generic_path() -> None:
    _engine, effect = _fresh_message_effect(Properties())
    assert effect.decoded_property_wire_size is None


def test_decoded_property_size_honors_small_limit_boundary() -> None:
    _engine, effect = _fresh_message_effect(_iot_properties())
    message = effect.data
    assert isinstance(message, Message)
    property_size = effect.decoded_property_wire_size
    assert property_size is not None
    footprint = len(message.payload) + 4 * len(message.topic) + property_size

    assert _fits_small_limit(message, footprint, property_size)
    assert not _fits_small_limit(message, footprint - 1, property_size)
    assert not _fits_small_limit(message, footprint, None)


def test_decoded_properties_can_join_inline_small_delivery_batch() -> None:
    _engine, first = _fresh_message_effect(_iot_properties())
    _engine, second = _fresh_message_effect(_iot_properties())
    client = AsyncClient(
        message_delivery="iterator",
        protocol=MQTTProtocolVersion.MQTTv5,
        client_id="probe",
    )
    effects = deque([first, second])

    applied = client._apply_message_effect_batch_inline(effects, client._connection_epoch)

    assert applied == 2
    assert client.pending_delivery_bytes == 0
    assert client._messages.get_nowait() is first.data
    assert client._messages.get_nowait() is second.data


async def test_replay_never_reuses_fresh_decoded_property_size_after_direct_mutation() -> None:
    engine, first = _fresh_message_effect(_iot_properties(), qos=QoS.EXACTLY_ONCE, mid=7)
    message = first.data
    assert isinstance(message, Message)
    assert message.properties is not None
    assert first.decoded_property_wire_size is not None

    # MemoryInflightStore keeps the same Properties object. This is exactly the
    # mutation shape the encoding cache promises to tolerate, and it must not
    # make a decode-time delivery hint stale on replay.
    message.properties.values["correlation_data"] = b"x" * 200

    engine.inbound.replay_session()
    replay = [effect for effect in engine.take_effects() if effect.kind is EffectKind.MESSAGE]
    assert len(replay) == 1
    replay_effect = replay[0]
    assert replay_effect.decoded_property_wire_size is None
    replay_message = replay_effect.data
    assert isinstance(replay_message, Message)
    assert replay_message.properties is message.properties

    client = AsyncClient(
        message_delivery="iterator",
        protocol=MQTTProtocolVersion.MQTTv5,
        client_id="probe",
    )
    await client._apply_effect(replay_effect, nowait=False)
    item = client._messages.get_nowait()
    assert isinstance(item, tuple)
    assert client.pending_delivery_bytes > 0
