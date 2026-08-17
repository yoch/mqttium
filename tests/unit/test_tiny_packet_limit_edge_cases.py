"""Edge cases for the MQTT 5 tiny peer packet-limit inbound specialization."""

from __future__ import annotations

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import CONNACK, encode_properties
from mqttium.enums import InboundQoSState, MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import MandatoryResponseTooLargeError
from mqttium.packets import PublishPacket, encode_frame
from mqttium.packets._ack import encode_pubrel_success
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import InboundMessage, Properties


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def _connack(maximum_packet_size: int) -> bytes:
    properties = Properties()
    properties.set("maximum_packet_size", maximum_packet_size)
    body = bytearray((0, 0))
    body.extend(encode_properties(properties, CONNACK))
    return encode_frame(PacketType.CONNACK, 0, body)


def _engine(**config: object) -> ProtocolEngine:
    engine = ProtocolEngine(
        EngineConfig(
            client_id="tiny-edge",
            protocol=MQTTProtocolVersion.MQTTv5,
            **config,
        )
    )
    engine.begin_connect()
    _feed(engine, _connack(3))
    engine.take_effects()
    return engine


def _publish(
    qos: QoS,
    *,
    mid: int = 7,
    topic: str = "edge/topic",
    payload: bytes = b"payload",
    properties: Properties | None = None,
) -> bytes:
    return PublishPacket(
        topic=topic,
        payload=payload,
        qos=qos,
        retain=False,
        dup=False,
        mid=None if qos is QoS.AT_MOST_ONCE else mid,
        properties=properties,
    ).encode(MQTTProtocolVersion.MQTTv5)


def _record(mid: int, state: InboundQoSState, *, user_acked: bool = False) -> InboundMessage:
    return InboundMessage(
        mid=mid,
        topic="edge/topic",
        payload=b"payload",
        qos=QoS.EXACTLY_ONCE if state is not InboundQoSState.WAIT_PUBACK else QoS.AT_LEAST_ONCE,
        retain=False,
        state=state,
        user_acked=user_acked,
        logical_size=17,
    )


def _protocol_errors(engine: ProtocolEngine) -> list[str]:
    return [
        str(effect.data)
        for effect in engine.take_effects()
        if effect.kind is EffectKind.PROTOCOL_ERROR
    ]


def test_tiny_limit_resolves_topic_alias_before_autoack_failure() -> None:
    engine = _engine(topic_alias_maximum=1)
    alias = Properties()
    alias.set("topic_alias", 1)

    _feed(engine, _publish(QoS.AT_MOST_ONCE, topic="aliased/topic", properties=alias))
    engine.take_effects()

    with pytest.raises(MandatoryResponseTooLargeError):
        _feed(engine, _publish(QoS.AT_LEAST_ONCE, topic="", properties=alias))

    assert engine.store.get_in(7) is None
    assert engine.inbound._inflight == 0


def test_tiny_qos1_preserves_packet_id_collision_error() -> None:
    engine = _engine()
    record = _record(7, InboundQoSState.WAIT_PUBREL)
    engine.store.put_in(record)
    engine.inbound._remember_inbound()
    engine.inbound._inflight = 1

    _feed(engine, _publish(QoS.AT_LEAST_ONCE))

    errors = _protocol_errors(engine)
    assert errors and "reuses mid=7" in errors[-1]
    assert engine.store.get_in(7) is record


def test_tiny_qos2_preserves_packet_id_collision_error() -> None:
    engine = _engine()
    record = _record(7, InboundQoSState.WAIT_PUBACK)
    engine.store.put_in(record)
    engine.inbound._remember_inbound()
    engine.inbound._inflight = 1

    _feed(engine, _publish(QoS.EXACTLY_ONCE))

    errors = _protocol_errors(engine)
    assert errors and "reuses mid=7" in errors[-1]
    assert engine.store.get_in(7) is record


def test_tiny_qos2_rejects_mid_pending_auto_qos1_before_size_failure() -> None:
    engine = _engine()
    engine.inbound._pending_auto_qos1_mids.add(7)

    _feed(engine, _publish(QoS.EXACTLY_ONCE))

    errors = _protocol_errors(engine)
    assert errors and "QoS 1 PUBACK is pending" in errors[-1]
    assert engine.store.get_in(7) is None


def test_tiny_limit_preserves_receive_maximum_error_precedence() -> None:
    engine = _engine(local_receive_maximum=1)
    engine.inbound._inflight = 1

    _feed(engine, _publish(QoS.AT_LEAST_ONCE))

    errors = _protocol_errors(engine)
    assert errors and errors[-1] == "Receive Maximum exceeded"
    assert engine.store.get_in(7) is None


def test_tiny_limit_preserves_inbound_byte_quota_error_precedence() -> None:
    engine = _engine(max_pending_inbound_bytes=1)

    _feed(engine, _publish(QoS.EXACTLY_ONCE, payload=b"too-large-for-quota"))

    errors = _protocol_errors(engine)
    assert errors and errors[-1] == "Pending inbound byte limit reached"
    assert engine.store.get_in(7) is None
    assert engine.inbound._pending_bytes == 0


def test_tiny_pubrel_manual_wait_user_ack_is_idempotent() -> None:
    engine = _engine(manual_ack=True)
    record = _record(7, InboundQoSState.WAIT_USER_ACK)
    engine.store.put_in(record)
    engine.inbound._remember_inbound()
    engine.inbound._inflight = 1

    _feed(engine, encode_pubrel_success(7))

    assert engine.take_effects() == []
    assert engine.store.get_in(7) is record
    assert record.state is InboundQoSState.WAIT_USER_ACK


def test_tiny_pubrel_manual_transition_happens_without_pubcomp() -> None:
    engine = _engine(manual_ack=True)
    record = _record(7, InboundQoSState.WAIT_PUBREL)
    engine.store.put_in(record)
    engine.inbound._remember_inbound()
    engine.inbound._inflight = 1

    _feed(engine, encode_pubrel_success(7))

    assert engine.take_effects() == []
    transitioned = engine.store.get_in(7)
    assert transitioned is not None
    assert transitioned.state is InboundQoSState.WAIT_USER_ACK


def test_tiny_pubrel_preserves_invalid_state_protocol_error() -> None:
    engine = _engine()
    record = _record(7, InboundQoSState.WAIT_PUBACK)
    engine.store.put_in(record)
    engine.inbound._remember_inbound()
    engine.inbound._inflight = 1

    _feed(engine, encode_pubrel_success(7))

    errors = _protocol_errors(engine)
    assert errors and "invalid state" in errors[-1]
    assert engine.store.get_in(7) is record


def test_tiny_qos1_duplicate_pending_autoack_still_fails_locally() -> None:
    engine = _engine()
    engine.inbound._pending_auto_qos1_mids.add(7)

    with pytest.raises(MandatoryResponseTooLargeError):
        _feed(engine, _publish(QoS.AT_LEAST_ONCE))

    assert engine.inbound._pending_auto_qos1_mids == {7}
    assert engine.store.get_in(7) is None


def test_tiny_qos2_duplicate_wait_pubrel_preserves_exchange() -> None:
    engine = _engine()
    record = _record(7, InboundQoSState.WAIT_PUBREL)
    engine.store.put_in(record)
    engine.inbound._remember_inbound()
    engine.inbound._inflight = 1

    with pytest.raises(MandatoryResponseTooLargeError):
        _feed(engine, _publish(QoS.EXACTLY_ONCE))

    assert engine.store.get_in(7) is record
    assert record.state is InboundQoSState.WAIT_PUBREL


def test_tiny_unknown_pubrel_fails_locally_without_state() -> None:
    engine = _engine()

    with pytest.raises(MandatoryResponseTooLargeError):
        _feed(engine, encode_pubrel_success(77))

    assert engine.store.get_in(77) is None
    assert engine.take_effects() == []
