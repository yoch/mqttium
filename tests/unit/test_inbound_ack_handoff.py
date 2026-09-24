"""A completed inbound QoS 2 exchange owns its slot and identifier until handoff.

The broker counts a QoS 2 PUBLISH against the client's Receive Maximum, and
keeps its packet identifier in use, until it receives the PUBCOMP
[MQTT-3.3.4-9] [MQTT-2.2.1-4]. A PUBCOMP still inside the engine batch cannot
have reached it, so a PUBLISH decoded before that batch is handed off must not
be admitted into the freed slot (#537) or under the freed identifier (#541).
This mirrors the automatic QoS 1 PUBACK handoff.
"""

from __future__ import annotations

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import ProtocolError
from mqttium.packets import PublishPacket, PubRelPacket, encode_frame
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from tests.support import (
    ScriptedBrokerTransport,
    feed_engine,
    mark_delivered_messages,
    transport_factory,
    wait_until,
)

V5 = MQTTProtocolVersion.MQTTv5


def _publish(mid: int, qos: QoS = QoS.EXACTLY_ONCE) -> bytes:
    return PublishPacket(
        topic=f"in/{mid}", payload=b"x", qos=qos, retain=False, dup=False, mid=mid
    ).encode(V5)


def _pubrel(mid: int) -> bytes:
    return PubRelPacket(mid=mid).encode(V5)


def _engine(receive_maximum: int) -> ProtocolEngine:
    engine = ProtocolEngine(
        EngineConfig(client_id="handoff", protocol=V5, max_inbound_inflight=receive_maximum)
    )
    engine.begin_connect()
    engine.take_effects()
    feed_engine(engine, encode_frame(PacketType.CONNACK, 0, b"\x00\x00\x00"))
    engine.take_effects()
    return engine


def _delivered_exchange(engine: ProtocolEngine, mid: int) -> None:
    """PUBLISH handed off and delivered: its PUBREL completes immediately."""
    feed_engine(engine, _publish(mid))
    mark_delivered_messages(engine, engine.take_effects())


def _refusal(engine: ProtocolEngine, wire: bytes) -> tuple[str, list[int]]:
    """Feed ``wire``; return the peer error and the DISCONNECT reasons it caused."""
    feed_engine(engine, wire)
    effects = engine.take_effects()
    errors = [e.data for e in effects if e.kind is EffectKind.PROTOCOL_ERROR]
    assert len(errors) == 1 and isinstance(errors[0], ProtocolError), effects
    reasons = [e.data.reason_code for e in effects if e.kind is EffectKind.DISCONNECTED]
    return str(errors[0]), reasons


def test_publish_before_pubcomp_handoff_exceeds_receive_maximum() -> None:
    # #537: PUBLISH(1), PUBREL(1), PUBLISH(2) with Receive Maximum 1.
    engine = _engine(1)
    _delivered_exchange(engine, 1)
    feed_engine(engine, _pubrel(1))  # PUBCOMP(1) is still in this batch

    error, reasons = _refusal(engine, _publish(2))
    assert "Receive Maximum exceeded" in error
    assert reasons == [0x93]


def test_publish_after_pubcomp_handoff_uses_the_released_slot() -> None:
    engine = _engine(1)
    _delivered_exchange(engine, 1)
    feed_engine(engine, _pubrel(1))
    assert engine.inbound._autoack_handoff_required
    effects = engine.take_effects()  # PUBCOMP(1) leaves the engine
    assert [e.kind for e in effects] == [EffectKind.SEND_ACK]
    assert engine.inbound.stats().inflight == 0

    feed_engine(engine, _publish(2))
    assert engine.inbound.stats().inflight == 1


@pytest.mark.parametrize("qos", [QoS.AT_LEAST_ONCE, QoS.EXACTLY_ONCE])
def test_identifier_reuse_before_pubcomp_handoff_is_refused(qos: QoS) -> None:
    # #541: spare Receive Maximum, same identifier before PUBCOMP handoff.
    engine = _engine(10)
    _delivered_exchange(engine, 1)
    feed_engine(engine, _pubrel(1))

    error, reasons = _refusal(engine, _publish(1, qos))
    assert "awaiting PUBCOMP handoff" in error
    assert reasons == [0x82]


def test_publish_after_pubrel_awaiting_delivery_is_refused() -> None:
    # PUBREL arrived before the application owned the message (#520): the
    # exchange is in phase 2, so the same PUBLISH again is a violation, not a
    # retransmission to acknowledge with another PUBREC.
    engine = _engine(10)
    feed_engine(engine, _publish(1))
    feed_engine(engine, _pubrel(1))

    error, reasons = _refusal(engine, _publish(1))
    assert "after PUBREL" in error
    assert reasons == [0x82]


def test_repeated_pubrel_before_handoff_releases_one_slot() -> None:
    engine = _engine(2)
    _delivered_exchange(engine, 1)
    feed_engine(engine, _pubrel(1))
    feed_engine(engine, _pubrel(1))  # answered again, owns nothing more

    effects = engine.take_effects()
    assert [e.kind for e in effects] == [EffectKind.SEND_ACK, EffectKind.SEND_ACK]
    assert engine.inbound.stats().inflight == 0
    feed_engine(engine, _publish(1))  # legal once PUBCOMP has left
    assert engine.inbound.stats().inflight == 1


async def test_client_refuses_identifier_reuse_in_the_pubrel_read() -> None:
    broker = ScriptedBrokerTransport(protocol=V5)
    client = AsyncClient(
        "handoff",
        protocol=V5,
        keepalive=0,
        max_inbound_inflight=10,
        message_delivery="callback",
    )
    client._transport_factory = transport_factory(broker)
    disconnects: list[BaseException | None] = []
    client.on_disconnect = disconnects.append
    received: list[int | None] = []
    client.on_message = lambda message: received.append(message.mid)
    await client.connect("fake")
    try:
        broker.push_rx(_publish(1))
        await wait_until(lambda: received == [1])
        broker.push_rx(_pubrel(1) + _publish(1))
        await wait_until(lambda: disconnects != [])
        assert isinstance(disconnects[0], ProtocolError)
        assert received == [1]
    finally:
        await client.disconnect()


def test_manual_qos1_reuse_before_manual_pubcomp_handoff_is_refused() -> None:
    engine = ProtocolEngine(
        EngineConfig(client_id="handoff", protocol=V5, max_inbound_inflight=10, manual_ack=True)
    )
    engine.begin_connect()
    engine.take_effects()
    feed_engine(engine, encode_frame(PacketType.CONNACK, 0, b"\x00\x00\x00"))
    engine.take_effects()
    feed_engine(engine, _publish(1))
    mark_delivered_messages(engine, engine.take_effects())
    feed_engine(engine, _pubrel(1))
    engine.take_effects()
    engine.ack(1)  # PUBCOMP(1) is in the batch

    error, reasons = _refusal(engine, _publish(1, QoS.AT_LEAST_ONCE))
    assert "awaiting PUBCOMP handoff" in error
    assert reasons == [0x82]
